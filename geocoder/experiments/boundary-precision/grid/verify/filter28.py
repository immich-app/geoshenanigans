#!/usr/bin/env python3
"""Independent L2-8 filter (verifier): keep live rings with 2 <= admin_level <= 8, copy their
blocks verbatim back to back in slot order; other live slots get vertex_count 0 and
vertex_offset NO_DATA; dead slots untouched. Written without reading gridvar's filter code path.
usage: filter28.py <src_dir> <out_dir>  -> prints one JSON line"""
import json, sys
import numpy as np

src, out = sys.argv[1:3]
NO = 0xFFFFFFFF
H = 10
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
p = np.fromfile(f"{src}/admin_polygons.bin", PDT)
vb = np.fromfile(f"{src}/admin_vertices.bin", np.uint8)
live = (p["cnt"] > 0) & (p["off"] != NO)
sel = live & (p["lvl"] >= 2) & (p["lvl"] <= 8)
idx = np.nonzero(sel)[0]
off = p["off"][idx].astype(np.int64)
cnt = p["cnt"][idx].astype(np.int64)
enc = vb[off]
assert np.isin(enc, [0, 1, 2, 3]).all()
size = H + cnt * np.where(enc == 3, 8, 4)
new_off = np.concatenate([[0], np.cumsum(size)[:-1]])
total = int(size.sum())
assert total < NO
buf = np.empty(total, np.uint8)
for o, s, n in zip(off.tolist(), size.tolist(), new_off.tolist()):
    buf[n:n + s] = vb[o:o + s]
q = p.copy()
drop = live & ~sel
q["cnt"][drop] = 0
q["off"][drop] = NO
q["off"][idx] = new_off.astype(np.uint32)
q.tofile(f"{out}/admin_polygons.bin")
buf.tofile(f"{out}/admin_vertices.bin")
print(json.dumps({"src": src, "rings": int(len(idx)), "vertices": int(cnt.sum()), "bytes": total}))
