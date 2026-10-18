#!/usr/bin/env python3
"""Border test points within +-50 m of the exact admin boundaries the uncapped tier ships.

Same method as /ssd/shrink/grid/gen_borders.py, but the rings are the exact 0.1 m DP rings
(dumps/admin_dp.e7, OSM's 1e-7 integers, aligned with quality/uncapped/admin_polygons.bin) and the
offset is +-50 m. Reader-visible rings only (the reader skips area <= 0, and cc == 0 at level 2).
Edges are picked with probability proportional to their length in metres, a uniform position
along the edge and a perpendicular offset uniform in [-50, 50] m.
Writes pts/L<level>.txt ("lat lng", 20k each, seeded 20261014)."""
import numpy as np

DU = "/ssd/precision/build/dumps"
SH = "/ssd/precision/build/out-nocap/planet/quality/uncapped"
OUT = "/ssd/precision/tiers/pts"
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
p = np.fromfile(f"{SH}/admin_polygons.bin", PDT)
idx = np.fromfile(f"{DU}/admin_dp.e7.idx", "<u8").astype(np.int64)
v = np.memmap(f"{DU}/admin_dp.e7", "<i4", "r").reshape(-1, 2)
assert len(idx) == len(p) + 1
rng = np.random.default_rng(20261014)
OFF = 50.0

for lvl in (2, 4, 6, 8, 10):
    sel = (p["lvl"] == lvl) & (p["area"] > 0)
    if lvl == 2:
        sel &= p["cc"] != 0
    rings = np.nonzero(sel)[0]
    a_idx = np.concatenate([np.arange(idx[r], idx[r + 1] - 1) for r in rings])  # edge start vertices
    la, ga = v[a_idx, 0] * 1e-7, v[a_idx, 1] * 1e-7
    lb, gb = v[a_idx + 1, 0] * 1e-7, v[a_idx + 1, 1] * 1e-7
    coslat = np.cos(np.radians((la + lb) / 2))
    dy = (lb - la) * 111320.0
    dx = (gb - ga) * 111320.0 * coslat
    L = np.hypot(dx, dy)
    s = np.nonzero((L > 0) & (np.abs(gb - ga) < 180))[0]  # no antimeridian-wrapping edges
    w = L[s] / L[s].sum()
    pick = s[rng.choice(len(s), size=20000, p=w)]
    t = rng.random(len(pick))
    off = rng.uniform(-OFF, OFF, len(pick))
    ux, uy = dx[pick] / L[pick], dy[pick] / L[pick]
    nx, ny = -uy, ux
    plat = la[pick] + t * (lb[pick] - la[pick]) + off * ny / 111320.0
    plng = ga[pick] + t * (gb[pick] - ga[pick]) + off * nx / (111320.0 * coslat[pick])
    with open(f"{OUT}/L{lvl}.txt", "w") as f:
        for x, y in zip(plat, plng):
            f.write(f"{x:.7f} {y:.7f}\n")
    print(f"L{lvl}: rings {len(rings):,} edges {len(a_idx):,} in pool {len(s):,} "
          f"total length {L[s].sum() / 1000:,.0f} km -> {len(pick):,} points", flush=True)
