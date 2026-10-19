"""Admin polygon census of a builder output dir: per (admin_level, place_type_override) the
polygon count and packed vertex bytes; optional osm relation ids to print.

usage: python -I polycount.py <dir with admin_polygons.bin/.osm_ids/admin_vertices.bin> [rel_id...]
"""
import os
import struct
import sys
from collections import defaultdict

REC = struct.Struct("<IIIBBBBfHH")
d = sys.argv[1]
want = {int(x) for x in sys.argv[2:]}
polys = open(f"{d}/admin_polygons.bin", "rb").read()
ids = open(f"{d}/admin_polygons.osm_ids", "rb").read()
magic, ver, count = struct.unpack_from("<III", ids, 0)
n = len(polys) // 24
assert count == n, (count, n)
recs = [REC.unpack_from(polys, i * 24) for i in range(n)]
offs = sorted({r[0] for r in recs if r[1] > 0 and r[0] != 0xFFFFFFFF})
vsize = os.path.getsize(f"{d}/admin_vertices.bin")
nxt = {o: (offs[i + 1] if i + 1 < len(offs) else vsize) for i, o in enumerate(offs)}
agg = defaultdict(lambda: [0, 0])
for i, r in enumerate(recs):
    off, vc, name_id, lvl, pto = r[0], r[1], r[2], r[3], r[4]
    if vc == 0:
        continue
    ot, fl, tier, _, sid = struct.unpack_from("<BBBBQ", ids, 12 + i * 12)
    b = nxt[off] - off
    agg[(lvl, pto)][0] += 1
    agg[(lvl, pto)][1] += b
    if ot == 3 and sid in want:
        print("want", sid, "slot", i, "lvl", lvl, "pto", pto, "verts", vc, "bytes", b, "area", r[7], "cc", r[8])
for k in sorted(agg):
    print(k, agg[k][0], agg[k][1])
