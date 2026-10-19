"""dropped_ids.py <out A> <out B> <ids.txt> <summary.json> : OSM relations/ways whose admin polygons
(quality/uncapped sidecar, every level) or POI records (poi/all sidecar) build A has and build B
lacks, and the reverse; writes osmium getid ids (r<id>/w<id>) of all of them and a summary."""
import collections
import json
import struct
import sys

import numpy as np

PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("holes", "<u2")])


def admin(out):
    d = f"{out}/planet/quality/uncapped"
    polys = np.fromfile(f"{d}/admin_polygons.bin", PDT)
    raw = open(f"{d}/admin_polygons.osm_ids", "rb").read()
    res = {}
    for i, p in enumerate(polys):
        if p["cnt"] == 0:
            continue
        ty, fl, _, _, sid = struct.unpack_from("<BBBBQ", raw, 12 + 12 * i)
        if fl & 1:
            continue
        key = f"r{sid >> 16}" if ty == 3 else ("w" if ty == 2 else f"t{ty}:") + str(sid)
        res.setdefault(key, (int(p["lvl"]), int(p["pto"])))
    return res


def poi(out):
    raw = open(f"{out}/planet/poi/all/poi_records.osm_ids", "rb").read()
    n = struct.unpack_from("<III", raw, 0)[2]
    res = set()
    for i in range(n):
        ty, fl, _, _, sid = struct.unpack_from("<BBBBQ", raw, 12 + 12 * i)
        if not fl & 1 and ty == 3:
            res.add(f"r{sid >> 16}")  # relation_id << 16 | ring_index
    return res


a, b = sys.argv[1], sys.argv[2]
aa, ab = admin(a), admin(b)
pa, pb = poi(a), poi(b)
only_a = {k: v for k, v in aa.items() if k not in ab}
only_b = {k: v for k, v in ab.items() if k not in aa}
summary = {
    "admin_only_a": len(only_a), "admin_only_b": len(only_b),
    "admin_only_a_by_level": dict(collections.Counter(v[0] for v in only_a.values())),
    "admin_only_b_by_level": dict(collections.Counter(v[0] for v in only_b.values())),
    "admin_only_a_by_type": dict(collections.Counter(k[0] for k in only_a)),
    "poi_rel_only_a": len(pa - pb), "poi_rel_only_b": len(pb - pa),
}
ids = sorted({k for k in list(only_a) + list(only_b) if k[0] in "rw"} | (pa - pb) | (pb - pa))
open(sys.argv[3], "w").write("".join(i + "\n" for i in ids))
json.dump({"summary": summary, "only_a": only_a, "only_b": only_b, "poi_only_a": sorted(pa - pb),
           "poi_only_b": sorted(pb - pa)}, open(sys.argv[4], "w"))
print(json.dumps(summary))
