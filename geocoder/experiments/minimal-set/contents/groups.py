#!/usr/bin/env python3
"""Largest admin features of the admin-minimal set, rings grouped by (name, level, country code of
the ring, override) as a stand-in for the OSM relation (the minimal dir ships no osm_ids sidecar),
plus per-level ring-size distribution.

Usage: groups.py <minimal dir>
"""
import os
import sys
from collections import defaultdict

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from inventory import load_polys  # noqa: E402

mdir = sys.argv[1]
p, v, live, has, enc, nbytes = load_polys(mdir)
sb = np.fromfile(f"{mdir}/strings_core.bin", dtype=np.uint8)


def name(off):
    return sb[off:off + 120].tobytes().split(b"\0", 1)[0].decode("utf-8", "replace")


g = defaultdict(lambda: [0, 0, 0])
for i in np.flatnonzero(live):
    k = (int(p["name"][i]), int(p["level"][i]), int(p["cc"][i]))
    g[k][0] += 1
    g[k][1] += int(p["vcnt"][i])
    g[k][2] += int(nbytes[i])
tot = int(nbytes.sum())
print("top 15 features by vertex bytes (rings grouped by name+level+cc):")
for (nm, L, cc), (r, vc, b) in sorted(g.items(), key=lambda kv: -kv[1][2])[:15]:
    ccs = (chr(cc >> 8) + chr(cc & 0xFF)) if cc else ""
    print(f"  L{L} {name(nm)[:40]:40s} {ccs:2s} rings {r:6d} vertices {vc:8d} bytes {b:9d} ({b / tot * 100:.2f}%)")
print("\nper level: features, rings, rings/feature max, vertices per ring p50/p90/p99/max, bytes in top 1% rings")
for L in range(2, 9):
    m = live & (p["level"] == L)
    feats = [x for k, x in g.items() if k[1] == L]
    vc = np.sort(p["vcnt"][m])
    nb = np.sort(nbytes[m])[::-1]
    top = nb[:max(1, len(nb) // 100)].sum()
    print(f"  L{L}: features {len(feats):7d} rings {int(m.sum()):7d} max rings/feature {max(f[0] for f in feats):5d} "
          f"verts p50 {int(np.percentile(vc, 50))} p90 {int(np.percentile(vc, 90))} p99 {int(np.percentile(vc, 99))} "
          f"max {int(vc.max())}; top-1% rings hold {top / nb.sum() * 100:.0f}% of level bytes")
