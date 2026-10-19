#!/usr/bin/env python3
"""bboxshift.py <variant min dir> <dump dir> : today's vertex packing (vertex_pack.h pack_polygon)
stores bbox_min as f32 but the u16/u32 offsets relative to the f64 minimum, so a reader decoding
f32(bbox_min) + k * scale is shifted by (f64 min - f32 min) on every vertex of the polygon. Measures,
for the uncapped (scheme none) admin-minimal polygons, the decoded-vs-dump error per vertex and the
per-polygon constant shift implied by the stored f32 minimum. Points to the size of the effect only."""
import struct
import sys

import numpy as np

vd, dump = sys.argv[1], sys.argv[2]
POLY = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                 ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("holes", "<u2")])
polys = np.fromfile(f"{vd}/admin_polygons.bin", dtype=POLY)
verts = np.fromfile(f"{vd}/admin_vertices.bin", dtype=np.uint8)
dpol = np.fromfile(f"{dump}/admin_polygons.raw", dtype=POLY)
dv = np.fromfile(f"{dump}/admin_vertices.raw", dtype="<f4").reshape(-1, 2)
kept = np.nonzero((dpol["lvl"] >= 2) & (dpol["lvl"] <= 8) & (dpol["cnt"] >= 3))[0]
assert len(kept) == len(polys)
rng = np.random.default_rng(1)
SC = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}
shifts, errs = {e: [] for e in SC}, {e: [] for e in SC}
for s in rng.choice(len(polys), 20000, replace=False):
    p, d = polys[s], dpol[kept[s]]
    o = int(p["off"])
    enc = int(verts[o])
    if enc not in SC or p["cnt"] != d["cnt"]:
        continue
    bml, bmg = (float(x) for x in verts[o + 2:o + 10].view("<f4"))
    src = dv[int(d["off"]):int(d["off"]) + int(d["cnt"])].astype(np.float64)
    mn = src.min(axis=0)
    shifts[enc].append(max(abs(mn[0] - bml), abs(mn[1] - bmg)))
    n = int(p["cnt"])
    w = 4 if enc == 3 else 2
    raw = verts[o + 10:o + 10 + n * 2 * w].view("<u4" if w == 4 else "<u2").reshape(n, 2).astype(np.float64)
    dec = np.stack([np.float64(bml) + raw[:, 0] * SC[enc], np.float64(bmg) + raw[:, 1] * SC[enc]], axis=1)
    errs[enc].append(np.abs(dec - src).max())
for e in SC:
    if shifts[e]:
        print(f"encoding {e} (grid {SC[e]:g} deg): {len(shifts[e])} polygons; f64-f32 min shift max {max(shifts[e]):.2e} "
              f"median {np.median(shifts[e]):.2e} deg; decoded-vs-dump error max {max(errs[e]):.2e} deg (half grid {SC[e] / 2:.1e})")
