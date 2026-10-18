#!/usr/bin/env python3
"""Compare Immich's current GeoNames reverse geocoder (re-implemented from
immich/server/src/repositories/map.repository.ts:154-210 + 323-398) with the
geocoder's admin-minimal set queried through grid_query.

Usage (on the box): /ssd/shrink/venv/bin/python compare.py   (writes comparison.jsonl)
"""
import json
import math
import os
import random
import subprocess
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
GEO = HERE / "geodata"
GRID_QUERY = Path("/ssd/shrink/target/release/examples/grid_query")
DATASET = Path(os.environ.get("DATASET", "/ssd/shrink/idx/smallest"))
OUT = os.environ.get("OUT", "comparison.jsonl")

# postgres earthdistance: earth() = 6378168 m; earth_box(p, r) = cube_enlarge(p, gc_to_sec(r), 3)
EARTH = 6378168.0
MAX_DIST = 25_000.0  # constants.ts:58 reverseGeocodeMaxDistance
CHORD = 2 * EARTH * math.sin(MAX_DIST / (2 * EARTH))


def ecef(lat, lon):
    la, lo = math.radians(lat), math.radians(lon)
    return (EARTH * math.cos(la) * math.cos(lo), EARTH * math.cos(la) * math.sin(lo), EARTH * math.sin(la))


def load_admin(path):
    m = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.rstrip("\n").split("\t")
            m[s[0]] = s[1]
    return m


def load_country_names():
    a2, a3 = {}, {}
    with open(GEO / "countryInfo.txt", encoding="utf-8") as f:
        for line in f:
            if line.startswith("#"):
                continue
            s = line.rstrip("\n").split("\t")
            if len(s) < 5 or not s[4].strip():
                continue
            if s[0]:
                a2[s[0]] = s[4].strip()
            if s[1]:
                a3[s[1]] = s[4].strip()
    return a2, a3


def load_cities(admin1):
    rows = []
    with open(GEO / "cities500.txt", encoding="utf-8") as f:
        for line in f:
            s = line.rstrip("\n").split("\t")
            # map.repository.ts:341
            if (s[7] == "PPLX" and s[8] != "AU") or s[7] == "PPLH":
                continue
            rows.append({
                "id": int(s[0]), "name": s[1], "lat": float(s[4]), "lon": float(s[5]),
                "fcode": s[7], "cc": s[8], "pop": int(s[14] or 0),
                "admin1": admin1.get(f"{s[8]}.{s[10]}"),
            })
    return rows


class CityIndex:
    """3D grid over ECEF coords, cell = chord length, so an earth_box query touches 27 cells."""

    def __init__(self, rows):
        self.rows = rows
        self.grid = defaultdict(list)
        self.xyz = []
        for i, r in enumerate(rows):
            p = ecef(r["lat"], r["lon"])
            self.xyz.append(p)
            self.grid[tuple(int(math.floor(c / CHORD)) for c in p)].append(i)

    def nearest(self, lat, lon):
        p = ecef(lat, lon)
        key = tuple(int(math.floor(c / CHORD)) for c in p)
        best, best_d = None, None
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    for i in self.grid.get((key[0] + dx, key[1] + dy, key[2] + dz), ()):
                        q = self.xyz[i]
                        # earth_box @> point: inside the axis-aligned cube of half-side CHORD
                        if abs(q[0] - p[0]) > CHORD or abs(q[1] - p[1]) > CHORD or abs(q[2] - p[2]) > CHORD:
                            continue
                        d = (q[0] - p[0]) ** 2 + (q[1] - p[1]) ** 2 + (q[2] - p[2]) ** 2
                        if best_d is None or d < best_d:
                            best, best_d = i, d
        if best is None:
            return None, None
        chord = math.sqrt(best_d)
        return self.rows[best], 2 * EARTH * math.asin(min(1.0, chord / (2 * EARTH)))


def load_ne():
    """map.repository.ts:259-293: one row per polygon outer ring, insertion order."""
    d = json.load(open(GEO / "ne_10m_admin_0_countries.geojson", encoding="utf-8"))
    rings = []
    for feat in d["features"]:
        g = feat["geometry"]
        for entry in g["coordinates"]:
            ring = entry[0] if g["type"] == "MultiPolygon" else entry
            xs = [pt[0] for pt in ring]
            ys = [pt[1] for pt in ring]
            rings.append((feat["properties"]["ADM0_A3"], ring, (min(xs), min(ys), max(xs), max(ys))))
            if g["type"] == "Polygon":
                break
    return rings


def pip(x, y, ring):
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def immich_geonames(lat, lon, idx, rings, a2, a3):
    city, dist = idx.nearest(lat, lon)
    if city:
        return {"source": "cities500", "city": city["name"], "state": city["admin1"],
                "country": a2.get(city["cc"]), "cc": city["cc"], "dist_m": round(dist),
                "fcode": city["fcode"], "geonameid": city["id"], "glat": city["lat"], "glon": city["lon"]}
    for a3code, ring, (x0, y0, x1, y1) in rings:
        if x0 <= lon <= x1 and y0 <= lat <= y1 and pip(lon, lat, ring):
            return {"source": "naturalearth", "city": None, "state": None, "country": a3.get(a3code), "a3": a3code}
    return {"source": "none", "city": None, "state": None, "country": None}


def map_geocoder(addr, a2):
    """Prototype mapping (immich-geocoder geocoding.repository.ts:66-80), with country from
    the GeoNames countryInfo name for the code (what upstream #30199 standardised on)."""
    cc = (addr.get("country_code") or "").upper()
    return {
        "city": addr.get("city") or addr.get("town") or addr.get("village") or addr.get("hamlet") or addr.get("municipality"),
        "state": addr.get("state") or addr.get("province"),
        "country_raw": addr.get("country"),
        "country": a2.get(cc) or addr.get("country"),
        "cc": cc or None,
    }


def norm(s):
    if s is None:
        return None
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.casefold().replace("-", " ").replace("'", "").split())


BORDER_CASES = [
    # (label, lat, lon)
    ("Weehawken NJ", 40.769, -74.02),
    ("Hoboken NJ", 40.7440, -74.0324),
    ("Fort Lee NJ (GW bridge)", 40.8509, -73.9701),
    ("London Westminster (Big Ben)", 51.5007, -0.1246),
    ("London Camden", 51.5390, -0.1426),
    ("London Croydon", 51.3762, -0.0982),
    ("London City", 51.5155, -0.0922),
    ("London Greenwich", 51.4826, -0.0077),
    ("London Hackney", 51.5450, -0.0553),
    ("London Kensington", 51.5014, -0.1919),
    ("London Wembley (Brent)", 51.5560, -0.2796),
    ("London Heathrow (Hillingdon)", 51.4700, -0.4543),
    ("London Richmond", 51.4613, -0.3037),
    ("Sapporo", 43.0618, 141.3545),
    ("Fukuoka", 33.5902, 130.4017),
    ("Nagoya", 35.1815, 136.9066),
    ("Manhattan", 40.7484, -73.9857),
    ("Brooklyn", 40.6782, -73.9442),
    ("Jersey City (Hudson edge)", 40.7178, -74.0431),
    ("Tokyo Shibuya", 35.6595, 139.7005),
    ("Tokyo Shinjuku", 35.6938, 139.7034),
    ("Osaka", 34.6937, 135.5023),
    ("Kyoto", 35.0116, 135.7681),
    ("Hokkaido rural", 43.5, 142.9),
    ("Seoul", 37.5665, 126.9780),
    ("Beijing", 39.9042, 116.4074),
    ("Hong Kong Central", 22.2819, 114.1580),
    ("Macau", 22.1987, 113.5439),
    ("Taipei", 25.0330, 121.5654),
    ("Singapore", 1.2839, 103.8515),
    ("Detroit", 42.3314, -83.0458),
    ("Windsor ON", 42.3149, -83.0364),
    ("El Paso", 31.7619, -106.4850),
    ("Ciudad Juarez", 31.6904, -106.4245),
    ("Basel", 47.5596, 7.5886),
    ("Kehl/Strasbourg Rhine", 48.5734, 7.8000),
    ("Baarle-Hertog enclave", 51.4386, 4.9301),
    ("Vatican", 41.9022, 12.4539),
    ("Monaco", 43.7384, 7.4246),
    ("Gibraltar", 36.1408, -5.3536),
    ("San Marino", 43.9424, 12.4578),
    ("Pristina Kosovo", 42.6629, 21.1655),
    ("Laayoune W Sahara", 27.1418, -13.1875),
    ("Simferopol Crimea", 44.9521, 34.1024),
    ("North Nicosia", 35.1856, 33.3823),
    ("Jerusalem Old City", 31.7767, 35.2345),
    ("Ramallah", 31.9038, 35.2034),
    ("Srinagar", 34.0837, 74.7973),
    ("San Juan PR", 18.4655, -66.1057),
    ("Nuuk", 64.1814, -51.6941),
    ("Longyearbyen", 78.2232, 15.6267),
    ("Reykjavik", 64.1466, -21.9426),
    ("Uluru (outback)", -25.3444, 131.0369),
    ("Outback NT empty", -21.0, 133.0),
    ("Sahara empty", 23.0, 12.0),
    ("Denali", 63.0692, -151.0070),
    ("Yellowstone Old Faithful", 44.4605, -110.8281),
    ("Grand Canyon South Rim", 36.0544, -112.1401),
    ("Mid-Atlantic ocean", 30.0, -40.0),
    ("Offshore Sydney 10km", -33.85, 151.40),
    ("Antarctica McMurdo", -77.8419, 166.6863),
    ("Everest base camp", 28.0026, 86.8528),
    ("Machu Picchu", -13.1631, -72.5450),
    ("Santorini Oia", 36.4618, 25.3753),
    ("Zermatt", 46.0207, 7.7491),
    ("Bali Ubud", -8.5069, 115.2625),
    ("Dubai Marina", 25.0805, 55.1403),
    ("Paris Eiffel", 48.8584, 2.2945),
    ("Berlin Mitte", 52.5200, 13.4050),
    ("Amsterdam", 52.3676, 4.9041),
    ("Brussels", 50.8503, 4.3517),
    ("Sydney Opera House", -33.8568, 151.2153),
    ("Mexico City", 19.4326, -99.1332),
    ("Sao Paulo", -23.5505, -46.6333),
    ("Buenos Aires", -34.6037, -58.3816),
    ("Cape Town", -33.9249, 18.4241),
    ("Lagos", 6.5244, 3.3792),
    ("Cairo", 30.0444, 31.2357),
    ("Mumbai", 19.0760, 72.8777),
    ("Delhi", 28.6139, 77.2090),
    ("Moscow", 55.7558, 37.6173),
    ("Istanbul Beyoglu", 41.0369, 28.9850),
]


def jitter(lat, lon, rng, max_m=2000.0):
    d = math.sqrt(rng.random()) * max_m
    b = rng.random() * 2 * math.pi
    dlat = d * math.cos(b) / 111_320.0
    dlon = d * math.sin(b) / (111_320.0 * max(0.01, math.cos(math.radians(lat))))
    return round(lat + dlat, 6), round(((lon + dlon + 180) % 360) - 180, 6)


def build_samples(cities, rng):
    by_cc = defaultdict(list)
    for c in cities:
        by_cc[c["cc"]].append(c)
    samples = []
    # A: country-stratified, 2 random places per country code
    for cc in sorted(by_cc):
        for c in rng.sample(by_cc[cc], min(2, len(by_cc[cc]))):
            lat, lon = jitter(c["lat"], c["lon"], rng)
            samples.append(("A_strat", f"{cc}:{c['name']}", lat, lon))
    # B: population-weighted (where photos get taken), 300 places
    weights = [max(c["pop"], 500) for c in cities]
    for c in rng.choices(cities, weights=weights, k=300):
        lat, lon = jitter(c["lat"], c["lon"], rng)
        samples.append(("B_popw", f"{c['cc']}:{c['name']}", lat, lon))
    # C: known border / edge cases (not jittered)
    for label, lat, lon in BORDER_CASES:
        samples.append(("C_edge", label, lat, lon))
    # D: population-weighted, exactly at the GeoNames point (no jitter) - agreement ceiling
    for c in rng.choices(cities, weights=weights, k=300):
        samples.append(("D_popw_exact", f"{c['cc']}:{c['name']}", c["lat"], c["lon"]))
    return samples


def run_grid_query(samples):
    inp = "".join(f"{lat} {lon}\n" for _, _, lat, lon in samples)
    out = subprocess.run([str(GRID_QUERY), str(DATASET)], input=inp, capture_output=True, text=True, check=True).stdout
    res = []
    for line in out.splitlines():
        lat, lon, js = line.split(" ", 2)
        res.append(json.loads(js))
    assert len(res) == len(samples), (len(res), len(samples))
    return res


def main():
    rng = random.Random(20261013)
    admin1 = load_admin(GEO / "admin1CodesASCII.txt")
    a2, a3 = load_country_names()
    cities = load_cities(admin1)
    print(f"cities500 rows after Immich filter: {len(cities)}", file=sys.stderr)
    idx = CityIndex(cities)
    rings = load_ne()
    print(f"naturalearth rows: {len(rings)}", file=sys.stderr)
    samples = build_samples(cities, rng)
    gq = run_grid_query(samples)
    rows = []
    for (group, label, lat, lon), g in zip(samples, gq):
        old = immich_geonames(lat, lon, idx, rings, a2, a3)
        addr = g.get("address") or {}
        new = map_geocoder(addr, a2)
        rows.append({"group": group, "label": label, "lat": lat, "lon": lon, "old": old, "new": new, "addr": addr})
    with open(HERE / OUT, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"wrote {len(rows)} rows", file=sys.stderr)


if __name__ == "__main__":
    main()
