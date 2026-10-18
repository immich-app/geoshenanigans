"""find_name.py <set dir> <name>...: place nodes and polygons whose string equals a name (debug)."""
import sys

import numpy as np

d = sys.argv[1].rstrip("/") + "/"
NO = 0xFFFFFFFF
pn_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("pt", "u1"), ("p1", "u1"), ("p2", "<u2"), ("parent", "<u4")])
ap_dt = np.dtype([("voff", "<u4"), ("vcnt", "<u4"), ("name_id", "<u4"), ("level", "u1"), ("ovr", "u1"), ("p2", "u1"), ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
s = open(d + "strings_core.bin", "rb").read()
pn = np.fromfile(d + "place_nodes.bin", dtype=pn_dt)
ap = np.fromfile(d + "admin_polygons.bin", dtype=ap_dt)
for name in sys.argv[2:]:
    b = name.encode() + b"\0"
    offs = []
    i = s.find(b)
    while i >= 0:
        if i == 0 or s[i - 1] == 0:
            offs.append(i)
        i = s.find(b, i + 1)
    print(name, "string offsets", offs)
    for o in offs:
        for j in np.nonzero(pn["name_id"] == o)[0].tolist():
            print("  node", j, pn[j])
        for j in np.nonzero(ap["name_id"] == o)[0].tolist():
            print("  poly", j, ap[j])
