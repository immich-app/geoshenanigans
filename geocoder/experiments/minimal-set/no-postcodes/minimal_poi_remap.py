#!/usr/bin/env python3
"""dirs/minimal_poi_remap: minimal_poi with poi_records.parent_poly_id
translated from admin-mode slots to admin-minimal slots (match on name_id,
admin_level, country_code, unique only; parents admin-minimal drops (L9/L10)
-> NO_DATA, which the chain check passes, lib.rs:2334). Shows what a builder
that remapped POI parents for admin-minimal would return.
"""
import collections, mmap, os, struct

REC, MIN = "/ssd/shrink/idx/recommended", "/ssd/shrink/idx/smallest"
SRC = "/ssd/shrink/no-postcodes/dirs/minimal_poi"
OUT = "/ssd/shrink/no-postcodes/dirs/minimal_poi_remap"
NO_DATA = 0xFFFFFFFF
POLY = struct.Struct("<IIIBBBBfHH")


def table(p):
    b = open(p, "rb").read()
    return [POLY.unpack_from(b, i * 24) for i in range(len(b) // 24)]


rec, mn = table(f"{REC}/admin_polygons.bin"), table(f"{MIN}/admin_polygons.bin")
key = lambda r: (r[2], r[3], r[8])
mkeys = collections.defaultdict(list)
for i, r in enumerate(mn):
    if r[0] != NO_DATA:
        mkeys[key(r)].append(i)
os.makedirs(OUT, exist_ok=True)
for n in os.listdir(SRC):
    if n != "poi_records.bin" and not os.path.exists(f"{OUT}/{n}"):
        os.link(f"{SRC}/{n}", f"{OUT}/{n}")
pois = bytearray(open(f"{REC}/poi_records.bin", "rb").read())
c = collections.Counter()
for i in range(len(pois) // 36):
    p, = struct.unpack_from("<I", pois, i * 36 + 32)
    if p == NO_DATA or p >= len(rec):
        c["NO_DATA / out of range"] += 1
        continue
    cand = mkeys.get(key(rec[p]), [])
    new = cand[0] if len(cand) == 1 else NO_DATA
    c["mapped" if new != NO_DATA else f"unmapped L{rec[p][3]}" if not cand else "ambiguous"] += 1
    struct.pack_into("<I", pois, i * 36 + 32, new)
open(f"{OUT}/poi_records.bin", "wb").write(pois)
print(dict(c.most_common()))
