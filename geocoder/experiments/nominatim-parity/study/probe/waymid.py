"""waymid.py <flat dir> <way index> [poly id ...] : the street's vertex average (builder's way
parent point) and its length midpoint (Nominatim's get_center_point: ST_LineInterpolatePoint(0.5)
in lon/lat degrees), and which of the given admin polygons contain each."""
import sys

import numpy as np

sys.path.insert(0, "/ssd/parity/probe")
from geo import Admin, pip  # noqa: E402

d, wi = sys.argv[1], int(sys.argv[2])
hdr = np.memmap(f"{d}/street_ways.bin", np.dtype([("off", "<u4"), ("cnt", "<u2"), ("pad", "<u2"), ("name", "<u4")]), "r")[wi]
nodes = np.memmap(f"{d}/street_nodes.bin", "<f4", "r").reshape(-1, 2)[int(hdr["off"]):int(hdr["off"]) + int(hdr["cnt"])]
lat, lng = nodes[:, 0].astype(np.float64), nodes[:, 1].astype(np.float64)
f32 = np.float32
s_lat, s_lng = f32(0), f32(0)
for a, b in nodes:
    s_lat, s_lng = f32(s_lat + a), f32(s_lng + b)
avg = (float(s_lat / f32(len(nodes))), float(s_lng / f32(len(nodes))))
seg = np.hypot(np.diff(lat), np.diff(lng))
half = seg.sum() / 2
k = int(np.searchsorted(np.cumsum(seg), half))
before = np.cumsum(seg)[k - 1] if k > 0 else 0.0
t = (half - before) / seg[k] if seg[k] > 0 else 0.0
mid = (lat[k] + t * (lat[k + 1] - lat[k]), lng[k] + t * (lng[k + 1] - lng[k]))
print("nodes", len(nodes), "vertex average", avg, "length midpoint", mid)
import os
adm = Admin(d, os.environ.get("SIDECAR"))
for pid in [int(x) for x in sys.argv[3:]]:
    la, lo = adm.verts(pid)
    r = pip(la, lo, np.array([avg[0], mid[0]]), np.array([avg[1], mid[1]]))
    print("poly", pid, "contains average", bool(r[0]), "contains midpoint", bool(r[1]))
