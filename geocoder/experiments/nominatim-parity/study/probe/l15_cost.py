"""l15_cost.py <out dir> <side> [census.json] : live admin polygons of one build tier
(planet/<side>), by level, and the place areas (level 15) by place type: count, vertices, area.
With census.json, also the polygons only build B has (only_b) by place type."""
import collections
import json
import struct
import sys

import numpy as np

PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("holes", "<u2")])
PTO = {0: "-", 1: "city", 2: "town", 3: "village", 4: "suburb", 5: "neighbourhood", 6: "quarter", 7: "state",
       8: "province", 9: "region", 10: "county", 11: "district", 12: "borough", 13: "hamlet", 14: "municipality"}
out, side = sys.argv[1], sys.argv[2]
d = f"{out}/planet/{side}"
polys = np.fromfile(f"{d}/admin_polygons.bin", PDT)
raw = open(f"{d}/admin_polygons.osm_ids", "rb").read()
only_b = set(json.load(open(sys.argv[3]))["only_b"]) if len(sys.argv) > 3 else set()
by_pto, new_by_pto = collections.defaultdict(lambda: [0, 0, 0.0]), collections.defaultdict(lambda: [0, 0, 0.0])
live = polys["cnt"] > 0
print(f"{side}: live polygons {int(live.sum())}, vertices {int(polys['cnt'][live].sum())}")
for i, p in enumerate(polys):
    if p["cnt"] == 0 or p["lvl"] != 15:
        continue
    ty, fl, _, _, sid = struct.unpack_from("<BBBBQ", raw, 12 + 12 * i)
    key = f"r{sid >> 16}" if ty == 3 else f"w{sid}"
    for tab in [by_pto] + ([new_by_pto] if key in only_b else []):
        t = tab[PTO.get(int(p["pto"]), int(p["pto"]))]
        t[0] += 1
        t[1] += int(p["cnt"])
        t[2] += float(p["area"])
print("  L15 by place type: count, vertices, area (deg^2)")
for k, (n, v, a) in sorted(by_pto.items(), key=lambda kv: -kv[1][1]):
    print(f"    {k:14s} {n:7d} {v:10d} {a:10.2f}")
if only_b:
    print("  of them only in build B (census only_b):")
    for k, (n, v, a) in sorted(new_by_pto.items(), key=lambda kv: -kv[1][1]):
        print(f"    {k:14s} {n:7d} {v:10d} {a:10.2f}")
