#!/usr/bin/env python3
"""Border test points within +-200 m of the TRUTH (uncapped, idx/max) admin boundaries.

Same method as /ssd/shrink/vertices/gen_points.py, but the rings come from the uncapped
set: decode every reader-visible L2 / L4 / L8 ring exactly as the reader does
(f32(f64(bbox_min_f32) + q * scale)); L2 rings need an ISO country code (the reader skips
cc == 0 at level 2) and every ring needs area > 0 (the reader skips area <= 0). Pick ring
edges with probability proportional to their length in metres, a uniform position along
the edge and a perpendicular offset uniform in [-200, 200] m.
Writes pts/L2.txt, pts/L4.txt, pts/L8.txt ("lat lng", 20k each, seeded 20261013)."""
import numpy as np

D = "/ssd/shrink/idx/max"
OUT = "/ssd/shrink/grid/pts"
NO = 0xFFFFFFFF
H = 10
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
SCALE = np.array([1e-5, 1e-4, 1e-6, 1e-7])

p = np.fromfile(f"{D}/admin_polygons.bin", PDT)
vb = np.memmap(f"{D}/admin_vertices.bin", np.uint8, "r")
rng = np.random.default_rng(20261013)


def decode(offs, cnts):
    enc = np.asarray(vb[offs]).astype(np.int64)
    assert np.isin(enc, [0, 1, 2, 3]).all()
    w = np.where(enc == 3, 4, 2)
    bml = np.asarray(vb[(offs[:, None] + np.arange(2, 6)).ravel()]).view("<f4").astype(np.float64)
    bmg = np.asarray(vb[(offs[:, None] + np.arange(6, 10)).ravel()]).view("<f4").astype(np.float64)
    vstart = np.concatenate([[0], np.cumsum(cnts)[:-1]])
    vp = np.repeat(np.arange(len(offs)), cnts)
    k = np.arange(int(cnts.sum())) - vstart[vp]
    wv = w[vp]
    pos = offs[vp] + H + k * 2 * wv
    del k
    out = []
    for comp, bm in ((0, bml), (1, bmg)):
        v = np.zeros(len(pos), np.int64)
        for b in range(4):
            s = b < wv
            v[s] |= np.asarray(vb[pos[s] + comp * wv[s] + b]).astype(np.int64) << (8 * b)
        out.append((bm[vp] + v * SCALE[enc][vp]).astype(np.float32).astype(np.float64))
    return out[0], out[1], vstart


for name, lvl in (("L2", 2), ("L4", 4), ("L8", 8)):
    sel = (p["cnt"] > 0) & (p["off"] != NO) & (p["lvl"] == lvl) & (p["area"] > 0)
    if lvl == 2:
        sel &= p["cc"] != 0
    offs, cnts = p["off"][sel].astype(np.int64), p["cnt"][sel].astype(np.int64)
    lat, lng, vstart = decode(offs, cnts)
    last = np.zeros(len(lat), bool)
    last[vstart + cnts - 1] = True
    a = np.nonzero(~last)[0]
    b = a + 1
    la, ga, lb, gb = lat[a], lng[a], lat[b], lng[b]
    coslat = np.cos(np.radians((la + lb) / 2))
    dy = (lb - la) * 111320.0
    dx = (gb - ga) * 111320.0 * coslat
    L = np.hypot(dx, dy)
    s = np.nonzero(L > 0)[0]
    w = L[s] / L[s].sum()
    pick = s[rng.choice(len(s), size=20000, p=w)]
    t = rng.random(len(pick))
    off = rng.uniform(-200, 200, len(pick))
    ux, uy = dx[pick] / L[pick], dy[pick] / L[pick]
    nx, ny = -uy, ux
    plat = la[pick] + t * (lb[pick] - la[pick]) + off * ny / 111320.0
    plng = ga[pick] + t * (gb[pick] - ga[pick]) + off * nx / (111320.0 * coslat[pick])
    with open(f"{OUT}/{name}.txt", "w") as f:
        for x, y in zip(plat, plng):
            f.write(f"{x:.7f} {y:.7f}\n")
    print(f"{name}: rings {len(offs):,} vertices {len(lat):,} edges in pool {len(s):,} "
          f"total length {L[s].sum() / 1000:,.0f} km -> {len(pick):,} points", flush=True)
    del lat, lng, la, ga, lb, gb, coslat, dx, dy, L, s, w, a, b, last
