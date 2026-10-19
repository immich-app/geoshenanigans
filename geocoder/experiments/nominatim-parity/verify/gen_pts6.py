"""verify6: fresh query pools, seeds 61906 (parity) and 61907 (reader); no earlier track's points.
Method as the reader track's gen_pts.py (re-implemented here), edges from planet-261018b
(/ssd/bugs2/sets/base, main builder): the max set's uncapped rings and the recommended q2.5 rings.

usage: python -I gen_pts6.py <out dir>
  par30k.txt  30,000 = 22,500 photo-like (GeoNames cities500, weight population^0.7, exponential
              offset mean 2 km, uniform direction) + 7,500 area-uniform on land (Natural Earth 10m)
  rd30k.txt   30,000 = 6,000 photo-like + 3,000 land + 3,000 sphere (lat -60..84)
              + 9,000 beside parallel-following borders (edges >= 50 km, |dlat| <= 0.05 deg,
                levels 2-8 of the max set; drawn by length x lune depth; latitude uniform from 2 km
                beyond the straight edge to 2 km beyond the great-circle arc)
              + 9,000 within +-200 m of L2 / L4 / L8 borders (3,000 each; half max rings, half
                recommended rings; edge drawn by length, perpendicular offset uniform)
"""
import json
import math
import sys

import numpy as np

OUT = sys.argv[1].rstrip("/") + "/"
NE = "/ssd/loc2city/ne/ne_10m_admin_0_countries.geojson"
CITIES = "/ssd/search/pg/geodata/cities500.txt"
SETS = "/ssd/bugs2/sets/base/"
KM_PER_DEG = 111.32
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
SCALE = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}


def write(name, lat, lng):
    open(OUT + name, "w").write("".join(f"{a:.7f} {b:.7f}\n" for a, b in zip(lat, lng)))
    print(name, len(lat))


def sphere(rng, n, lat0=-60.0, lat1=84.0):
    z = rng.uniform(math.sin(math.radians(lat0)), math.sin(math.radians(lat1)), n)
    return np.degrees(np.arcsin(z)), rng.uniform(-180, 180, n)


def pip(rlat, rlng, plat, plng):
    inside = np.zeros(len(plat), bool)
    x1, y1, x2, y2 = rlng, rlat, np.roll(rlng, -1), np.roll(rlat, -1)
    for k in range(0, len(rlat), 2048):
        a1, b1, a2, b2 = x1[k:k + 2048, None], y1[k:k + 2048, None], x2[k:k + 2048, None], y2[k:k + 2048, None]
        cond = (b1 > plat[None, :]) != (b2 > plat[None, :])
        with np.errstate(divide="ignore", invalid="ignore"):
            xint = a1 + (plat[None, :] - b1) * (a2 - a1) / (b2 - b1)
        inside ^= (np.count_nonzero(cond & (plng[None, :] < xint), axis=0) % 2).astype(bool)
    return inside


CITY = []
for line in open(CITIES, encoding="utf-8"):
    c = line.split("\t")
    CITY.append((float(c[4]), float(c[5]), max(1, int(c[14] or 0))))
CITY_W = np.array([c[2] for c in CITY], float) ** 0.7
CITY_LL = np.array([(c[0], c[1]) for c in CITY])


def photo(rng, n):
    pick = rng.choice(len(CITY), n, p=CITY_W / CITY_W.sum())
    cl = CITY_LL[pick]
    d = rng.exponential(2.0, n) / 111.2
    t = rng.uniform(0, 2 * math.pi, n)
    lat = cl[:, 0] + d * np.cos(t)
    lng = cl[:, 1] + d * np.sin(t) / np.maximum(0.05, np.cos(np.radians(cl[:, 0])))
    return lat, (lng + 180) % 360 - 180


RINGS = []
for f in json.load(open(NE))["features"]:
    g = f["geometry"]
    for poly in (g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]]):
        RINGS.append([np.array(r, float) for r in poly])


def land(rng, n):
    lat_out, lng_out = [], []
    while sum(len(x) for x in lat_out) < n:
        la, lo = sphere(rng, 20000)
        ins = np.zeros(len(la), bool)
        for poly in RINGS:
            o = poly[0]
            m = (lo >= o[:, 0].min()) & (lo <= o[:, 0].max()) & (la >= o[:, 1].min()) & (la <= o[:, 1].max())
            if not m.any():
                continue
            idx = np.flatnonzero(m)
            r = np.zeros(len(idx), bool)
            for ring in poly:
                r ^= pip(ring[:, 1], ring[:, 0], la[idx], lo[idx])
            ins[idx] |= r
        lat_out.append(la[ins]); lng_out.append(lo[ins])
    return np.concatenate(lat_out)[:n], np.concatenate(lng_out)[:n]


def edges(set_dir, levels):
    p = np.fromfile(set_dir + "/admin_polygons.bin", PDT)
    v = np.memmap(set_dir + "/admin_vertices.bin", np.uint8, "r")
    out = [[], [], [], []]
    keep = np.flatnonzero(np.isin(p["lvl"], levels) & (p["name"] != 0xFFFFFFFF) & (p["cnt"] >= 3) & (p["area"] > 0))
    for i in keep:
        off, n = int(p["off"][i]), int(p["cnt"][i])
        enc = int(v[off])
        blat, blng = np.frombuffer(v[off + 2:off + 10].tobytes(), "<f4").astype(np.float64)
        body = v[off + 10:]
        if enc == 4:
            a = np.frombuffer(body[:8 * n].tobytes(), "<f4").reshape(-1, 2).astype(np.float64)
            lat, lng = a[:, 0], a[:, 1]
        else:
            a = np.frombuffer(body[:(8 if enc == 3 else 4) * n].tobytes(), "<u4" if enc == 3 else "<u2").reshape(-1, 2).astype(np.float64)
            lat, lng = blat + a[:, 0] * SCALE[enc], blng + a[:, 1] * SCALE[enc]
        out[0].append(lat); out[1].append(lng); out[2].append(np.roll(lat, -1)); out[3].append(np.roll(lng, -1))
    alat, alng, blat, blng = (np.concatenate(x) for x in out)
    assert np.all(np.abs(alat) <= 90.0001) and np.all(np.abs(alng) <= 180.0001), "bad decode"
    return alat, alng, blat, blng


def unit(lat, lng):
    la, lo = np.radians(lat), np.radians(lng)
    return np.stack([np.cos(la) * np.cos(lo), np.cos(la) * np.sin(lo), np.sin(la)], -1)


def gc_lat(alat, alng, blat, blng, lng):
    n = np.cross(unit(alat, alng), unit(blat, blng))
    lo = np.radians(lng)
    return np.degrees(np.arctan(-(n[..., 0] * np.cos(lo) + n[..., 1] * np.sin(lo)) / n[..., 2]))


# --- parity pool
r1 = np.random.default_rng(61906)
pla, plo = photo(r1, 22500)
lla, llo = land(r1, 7500)
write("par30k.txt", np.r_[pla, lla], np.r_[plo, llo])

# --- reader pool
r2 = np.random.default_rng(61907)
pla, plo = photo(r2, 6000)
lla, llo = land(r2, 3000)
sla, slo = sphere(r2, 3000)
alat, alng, blat, blng = edges(SETS + "max", [2, 3, 4, 5, 6, 7, 8])
coslat = np.cos(np.radians((alat + blat) / 2))
dlng = np.abs(blng - alng)
length = np.hypot((blat - alat) * KM_PER_DEG, dlng * KM_PER_DEG * coslat)
par = np.flatnonzero((length >= 50) & (np.abs(blat - alat) <= 0.05) & (dlng < 180) & (np.abs(alat) < 80))
mid = (alng[par] + blng[par]) / 2
depth = np.abs(gc_lat(alat[par], alng[par], blat[par], blng[par], mid) - (alat[par] + blat[par]) / 2) * KM_PER_DEG
print(f"parallel edges {len(par)}, length {length[par].sum():.0f} km, lune depth max {depth.max():.2f} km", file=sys.stderr)
wgt = length[par] * np.maximum(depth, 0.05)
e = par[r2.choice(len(par), 9000, p=wgt / wgt.sum())]
tt = r2.uniform(0, 1, 9000)
lng = alng[e] + tt * (blng[e] - alng[e])
plat = alat[e] + tt * (blat[e] - alat[e])
glat = gc_lat(alat[e], alng[e], blat[e], blng[e], lng)
lo_, hi_ = np.minimum(plat, glat) - 2 / KM_PER_DEG, np.maximum(plat, glat) + 2 / KM_PER_DEG
par_lat, par_lng = r2.uniform(lo_, hi_), lng
bl, bg = [], []
for level in (2, 4, 8):
    for src, m in (("max", 1500), ("recommended", 1500)):
        alat, alng, blat, blng = edges(SETS + src, [level])
        coslat = np.cos(np.radians((alat + blat) / 2))
        dy = (blat - alat) * KM_PER_DEG
        dx = (blng - alng) * KM_PER_DEG * coslat
        length = np.hypot(dx, dy)
        ok = np.flatnonzero((length > 0) & (np.abs(blng - alng) < 180) & (np.abs(alat) < 85))
        e = ok[r2.choice(len(ok), m, p=length[ok] / length[ok].sum())]
        tt = r2.uniform(0, 1, m)
        dist = r2.uniform(-0.2, 0.2, m)
        nx, ny = -dy[e] / length[e], dx[e] / length[e]
        bl.append(alat[e] + tt * (blat[e] - alat[e]) + ny * dist / KM_PER_DEG)
        bg.append(alng[e] + tt * (blng[e] - alng[e]) + nx * dist / (KM_PER_DEG * coslat[e]))
write("rd30k.txt", np.r_[pla, lla, sla, par_lat, np.concatenate(bl)], np.r_[plo, llo, slo, par_lng, np.concatenate(bg)])
