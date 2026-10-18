#!/usr/bin/env python3
"""E4 cost variants: keep a hidden node's name_id (a) always, (b) only when it differs from its
claiming boundary's name or no boundary of the set claims it. zstd-19 of place_nodes.bin."""
import subprocess

import numpy as np

Z = "/ssd/shrink/zstd157/bin/zstd"
S = "/ssd/search-prod/sets/"
NO = 0xFFFFFFFF
pn_dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name_id", "<u4"), ("pt", "u1"), ("p1", "u1"), ("p2", "<u2"), ("parent", "<u4")])
ap_dt = np.dtype([("voff", "<u4"), ("vcnt", "<u4"), ("name_id", "<u4"), ("level", "u1"), ("ovr", "u1"), ("p2", "u1"), ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])


def z(arr):
    return len(subprocess.run([Z, "-19", "-T16", "-c", "-q", "-"], input=arr.tobytes(), capture_output=True, check=True).stdout)


for s in ("minimal", "recommended"):
    e3 = np.fromfile(S + s + "/place_nodes.e3.bin", dtype=pn_dt)
    e4 = np.fromfile(S + s + "/place_nodes.bin", dtype=pn_dt)
    ap = np.fromfile(S + s + "/admin_polygons.bin", dtype=ap_dt)
    hidden = (e4["p1"] & 2) != 0
    linked = hidden & ((e4["p1"] & 1) != 0)
    claimer_name = np.full(len(e4), NO, dtype=np.uint32)
    claimer_name[linked] = ap["name_id"][e4["parent"][linked]]
    same = linked & (claimer_name == e4["name_id"])
    b = e4.copy()
    b["name_id"][same] = NO
    print(s, "hidden", int(hidden.sum()), "linked", int(linked.sum()), "same name as claimer", int(same.sum()),
          "unclaimed in this set", int((hidden & ~linked).sum()))
    print("  zstd e3", z(e3), "e4 all", z(e4), "e4 differing/unclaimed only", z(b))
