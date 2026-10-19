"""reader track: fresh query pools (seed 20261020; none taken from earlier tracks' pools).

usage: python -I gen_pts.py <out dir>
Writes "lat lng" lines, 7 decimals:
  general30k.txt  30,000 = 10,000 photo-like (GeoNames cities500, city weight population^0.7,
                  exponential offset mean 2 km, uniform direction) + 10,000 area-uniform on land
                  (Natural Earth 10m countries) + 10,000 area-uniform on the sphere, lat -60..84
  parallel10k.txt 10,000 beside admin borders that follow a parallel: ring edges (levels 2-8, max
                  set = uncapped rings, the geometry the S2 covering was built from) at least 50 km
                  long whose latitude changes by at most 0.05 deg; edge drawn by length x lune depth,
                  position uniform along it, latitude uniform from 2 km on the far side of the
                  straight (planar) edge to 2 km past the great-circle edge
  border10k.txt   10,000 within +-200 m of L2 / L4 / L8 borders (3,334 / 3,333 / 3,333), edge
                  drawn by length, offset perpendicular and uniform in distance; per level the
                  first half from the max (uncapped) rings, the second from the recommended (q2.5)
"""
import json
import math
import sys

import numpy as np

OUT = sys.argv[1].rstrip("/") + "/"
SEED = 20261020
nrng = np.random.default_rng(SEED)
NE = "/ssd/loc2city/ne/ne_10m_admin_0_countries.geojson"
CITIES = "/ssd/search/pg/geodata/cities500.txt"
SETS = "/ssd/search-prod/rg/sets-orig/"
KM_PER_DEG = 111.32
EARTH_KM = 6371.0
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
SCALE = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}


def write(name, lat, lng):
    lat, lng = np.asarray(lat), np.asarray(lng)
    open(OUT + name, "w").write("".join(f"{a:.7f} {b:.7f}\n" for a, b in zip(lat, lng)))
    print(name, len(lat))


def sphere(n, lat0=-60.0, lat1=84.0):
    z = nrng.uniform(math.sin(math.radians(lat0)), math.sin(math.radians(lat1)), n)
    return np.degrees(np.arcsin(z)), nrng.uniform(-180, 180, n)


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


def edges(set_dir, levels):
    """All ring edges of the given admin levels: (a_lat, a_lng, b_lat, b_lng, level)."""
    p = np.fromfile(set_dir + "/admin_polygons.bin", PDT)
    v = np.memmap(set_dir + "/admin_vertices.bin", np.uint8, "r")
    out = [[], [], [], [], []]
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
        out[4].append(np.full(n, p["lvl"][i]))
    return [np.concatenate(x) for x in out]


def unit(lat, lng):
    la, lo = np.radians(lat), np.radians(lng)
    return np.stack([np.cos(la) * np.cos(lo), np.cos(la) * np.sin(lo), np.sin(la)], -1)


def gc_lat(alat, alng, blat, blng, lng):
    n = np.cross(unit(alat, alng), unit(blat, blng))
    lo = np.radians(lng)
    return np.degrees(np.arctan(-(n[..., 0] * np.cos(lo) + n[..., 1] * np.sin(lo)) / n[..., 2]))


# --- general30k
cities = []
for line in open(CITIES, encoding="utf-8"):
    c = line.split("\t")
    cities.append((float(c[4]), float(c[5]), max(1, int(c[14] or 0))))
w = np.array([c[2] for c in cities], float) ** 0.7
pick = nrng.choice(len(cities), 10000, p=w / w.sum())
cl = np.array([(cities[i][0], cities[i][1]) for i in pick])
d = nrng.exponential(2.0, 10000) / 111.2
t = nrng.uniform(0, 2 * math.pi, 10000)
photo_lat = cl[:, 0] + d * np.cos(t)
photo_lng = cl[:, 1] + d * np.sin(t) / np.maximum(0.05, np.cos(np.radians(cl[:, 0])))
photo_lng = (photo_lng + 180) % 360 - 180

ne = json.load(open(NE))
rings = []
for f in ne["features"]:
    g = f["geometry"]
    polys = g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]]
    for poly in polys:
        rings.append([np.array(r, float) for r in poly])
land_lat, land_lng = [], []
while sum(len(x) for x in land_lat) < 10000:
    la, lo = sphere(20000)
    ins = np.zeros(len(la), bool)
    for poly in rings:
        o = poly[0]
        m = (lo >= o[:, 0].min()) & (lo <= o[:, 0].max()) & (la >= o[:, 1].min()) & (la <= o[:, 1].max())
        if not m.any():
            continue
        idx = np.flatnonzero(m)
        r = np.zeros(len(idx), bool)
        for ring in poly:
            r ^= pip(ring[:, 1], ring[:, 0], la[idx], lo[idx])
        ins[idx] |= r
    land_lat.append(la[ins]); land_lng.append(lo[ins])
land_lat, land_lng = np.concatenate(land_lat)[:10000], np.concatenate(land_lng)[:10000]
sph_lat, sph_lng = sphere(10000)
write("general30k.txt", np.r_[photo_lat, land_lat, sph_lat], np.r_[photo_lng, land_lng, sph_lng])

# --- parallel10k (max set rings = uncapped)
alat, alng, blat, blng, lvl = edges(SETS + "max", [2, 3, 4, 5, 6, 7, 8])
coslat = np.cos(np.radians((alat + blat) / 2))
dlng = np.abs(blng - alng)
length = np.hypot((blat - alat) * KM_PER_DEG, dlng * KM_PER_DEG * coslat)
par = np.flatnonzero((length >= 50) & (np.abs(blat - alat) <= 0.05) & (dlng < 180) & (np.abs(alat) < 80))
mid_lng = (alng[par] + blng[par]) / 2
depth = np.abs(gc_lat(alat[par], alng[par], blat[par], blng[par], mid_lng) - (alat[par] + blat[par]) / 2) * KM_PER_DEG
print(f"parallel edges {len(par)}, total length {length[par].sum():.0f} km, lune depth max {depth.max():.2f} km", file=sys.stderr)
wgt = length[par] * np.maximum(depth, 0.05)
e = par[nrng.choice(len(par), 10000, p=wgt / wgt.sum())]
tt = nrng.uniform(0, 1, 10000)
lng = alng[e] + tt * (blng[e] - alng[e])
plat = alat[e] + tt * (blat[e] - alat[e])
glat = gc_lat(alat[e], alng[e], blat[e], blng[e], lng)
lo_, hi_ = np.minimum(plat, glat) - 2 / KM_PER_DEG, np.maximum(plat, glat) + 2 / KM_PER_DEG
write("parallel10k.txt", nrng.uniform(lo_, hi_), lng)

# --- border10k
out_lat, out_lng = [], []
for level, n in ((2, 3334), (4, 3333), (8, 3333)):
    for src, m in (("max", (n + 1) // 2), ("recommended", n // 2)):
        alat, alng, blat, blng, _ = edges(SETS + src, [level])
        coslat = np.cos(np.radians((alat + blat) / 2))
        dy = (blat - alat) * KM_PER_DEG
        dx = (blng - alng) * KM_PER_DEG * coslat
        length = np.hypot(dx, dy)
        ok = np.flatnonzero((length > 0) & (np.abs(blng - alng) < 180) & (np.abs(alat) < 85))
        e = ok[nrng.choice(len(ok), m, p=length[ok] / length[ok].sum())]
        tt = nrng.uniform(0, 1, m)
        dist = nrng.uniform(-0.2, 0.2, m)
        nx, ny = -dy[e] / length[e], dx[e] / length[e]
        out_lat.append(alat[e] + tt * (blat[e] - alat[e]) + ny * dist / KM_PER_DEG)
        out_lng.append(alng[e] + tt * (blng[e] - alng[e]) + nx * dist / (KM_PER_DEG * coslat[e]))
write("border10k.txt", np.concatenate(out_lat), np.concatenate(out_lng))
