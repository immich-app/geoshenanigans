#!/usr/bin/env python3
"""Largest rings per level (vertex_count, bytes a point-in-polygon test decodes) of candidates'
admin-minimal sets: max and p99 vertices per level, and the 5 largest L2 rings with their names.
usage: ringsizes.py <name> ..."""
import sys
import numpy as np

PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("holes", "<u2")])
core = open("/ssd/tiers2/sets/base/smallest/strings_core.bin", "rb").read()


def name(i):
    e = core.index(b"\0", i)
    return core[i:e].decode("utf-8", "replace")


for n in sys.argv[1:]:
    p = np.fromfile(f"/ssd/tiers2/var/{n}/min/admin_polygons.bin", PDT)
    live = p["cnt"] > 0
    out = []
    for l in range(2, 9):
        c = p["cnt"][live & (p["lvl"] == l)]
        out.append(f"L{l} max {c.max():,} p99 {int(np.percentile(c, 99)):,}")
    l2 = np.nonzero(live & (p["lvl"] == 2))[0]
    top = l2[np.argsort(-p["cnt"][l2].astype(np.int64))[:5]]
    print(n, "|", "; ".join(out), "| largest L2:", ", ".join(f"{name(int(p['name'][i]))} {p['cnt'][i]:,}" for i in top))
