#!/usr/bin/env python3
"""Verifier's FRESH 20k-point sample (seed 777013, never used by the measured run).

 photo  10,000: around place nodes of the FULL set (idx/max/place_nodes.bin), uniform in a 2 km disc;
                5,000 around city/town/suburb/neighbourhood/quarter, 5,000 around village/hamlet.
 border 10,000: within +-200 m of the TRUTH (idx/max, uncapped) admin rings. Each point draws its
                level uniformly from 2..10, then a ring edge of that level with probability
                proportional to its length, a uniform position on the edge and a perpendicular
                offset uniform in [-200, 200] m. Rings are decoded the way the reader decodes them
                (f32(f64(bbox_min_f32) + q * scale)); only reader-visible rings (area > 0, L2 needs cc).
Writes vfy/pts/fresh.txt ("lat lng") and vfy/pts/fresh.lbl (photo_big / photo_small / border_L<n>)."""
import collections
import numpy as np

D = "/ssd/shrink/idx/max"
OUT = "/ssd/shrink/grid/vfy/pts"
NO = 0xFFFFFFFF
H = 10
SEED = 777013
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
NDT = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name", "<u4"), ("type", "u1"), ("p1", "u1"), ("p2", "<u2"),
                ("parent", "<u4")])
SCALE = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}
M = 111320.0
rng = np.random.default_rng(SEED)
pts, lbl = [], []

# ---- photo-like
nodes = np.fromfile(f"{D}/place_nodes.bin", NDT)
for name, types in (("photo_big", (0, 1, 3, 5, 6)), ("photo_small", (2, 4))):
    pool = np.nonzero(np.isin(nodes["type"], types))[0]
    pick = pool[rng.integers(0, len(pool), 5000)]
    r = 2000.0 * np.sqrt(rng.random(5000))
    b = rng.random(5000) * 2 * np.pi
    la = nodes["lat"][pick].astype(np.float64) + r * np.cos(b) / M
    lo = nodes["lng"][pick].astype(np.float64) + r * np.sin(b) / (M * np.cos(np.radians(la)))
    pts += list(zip(la, lo))
    lbl += [name] * 5000

# ---- borders
p = np.fromfile(f"{D}/admin_polygons.bin", PDT)
vb = np.memmap(f"{D}/admin_vertices.bin", np.uint8, "r")
levels = np.arange(2, 11)
per_level = collections.Counter(rng.choice(levels, 10000).tolist())
for L in levels.tolist():
    n = per_level[L]
    sel = (p["cnt"] > 0) & (p["off"] != NO) & (p["lvl"] == L) & (p["area"] > 0)
    if L == 2:
        sel &= p["cc"] != 0
    lat_parts, lng_parts, first = [], [], []
    pos = 0
    for off, cnt in zip(p["off"][sel].tolist(), p["cnt"][sel].tolist()):
        enc = int(vb[off])
        bml = float(np.frombuffer(vb, "<f4", 1, off + 2)[0])
        bmg = float(np.frombuffer(vb, "<f4", 1, off + 6)[0])
        q = np.frombuffer(vb, "<u4" if enc == 3 else "<u2", 2 * cnt, off + H).reshape(cnt, 2).astype(np.float64)
        s = SCALE[enc]
        lat_parts.append((bml + q[:, 0] * s).astype(np.float32))
        lng_parts.append((bmg + q[:, 1] * s).astype(np.float32))
        first.append(pos)
        pos += cnt
    lat = np.concatenate(lat_parts).astype(np.float64)
    lng = np.concatenate(lng_parts).astype(np.float64)
    del lat_parts, lng_parts
    is_last = np.zeros(len(lat), bool)
    is_last[np.array(first[1:] + [len(lat)]) - 1] = True
    a = np.nonzero(~is_last)[0]
    cl = np.cos(np.radians((lat[a] + lat[a + 1]) / 2))
    dy = (lat[a + 1] - lat[a]) * M
    dx = (lng[a + 1] - lng[a]) * M * cl
    length = np.hypot(dx, dy)
    ok = length > 0
    a, cl, dy, dx, length = a[ok], cl[ok], dy[ok], dx[ok], length[ok]
    k = rng.choice(len(a), size=n, p=length / length.sum())
    t = rng.random(n)
    o = rng.uniform(-200.0, 200.0, n)
    ux, uy = dx[k] / length[k], dy[k] / length[k]
    la = lat[a[k]] + t * (lat[a[k] + 1] - lat[a[k]]) + o * ux / M          # normal = (-uy, ux) in (x, y)
    lo = lng[a[k]] + t * (lng[a[k] + 1] - lng[a[k]]) - o * uy / (M * cl[k])
    pts += list(zip(la, lo))
    lbl += [f"border_L{L}"] * n
    print(f"L{L}: rings {int(sel.sum()):,} vertices {len(lat):,} edges {len(a):,} -> {n} points", flush=True)
    del lat, lng, a, cl, dy, dx, length

with open(f"{OUT}/fresh.txt", "w") as f:
    for la, lo in pts:
        f.write(f"{la:.7f} {lo:.7f}\n")
with open(f"{OUT}/fresh.lbl", "w") as f:
    f.write("\n".join(lbl) + "\n")
print("wrote", len(pts), collections.Counter(lbl))
