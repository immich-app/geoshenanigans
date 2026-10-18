#!/usr/bin/env python3
"""How much of strings_core.bin is postal-boundary (admin_level 11) names?
L11 polygons sit in admin_polygons, whose names the builder marks CORE
(builder/src/parsed_data.h:529), so postal codes ride in the core tier every
mode ships (admin-minimal included, same sha256). Counts strings referenced
by an L11 polygon and by no other admin polygon / place node / POI major
record, and measures zstd -18 of strings_core with those strings dropped.
Lower bound on other consumers: POI notable/all and street-mode records can
also reference core strings; not checked (UNVERIFIED).
"""
import json, mmap, os, struct, subprocess

REC = "/ssd/shrink/idx/recommended"
OUT = "/ssd/shrink/no-postcodes/out/strings_core_no_l11.bin"
NO_DATA = 0xFFFFFFFF


def mm(p):
    fh = open(p, "rb")
    return fh, mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)


_, polys = mm(f"{REC}/admin_polygons.bin")
_, places = mm(f"{REC}/place_nodes.bin")
_, pois = mm(f"{REC}/poi_records.bin")
_, core = mm(f"{REC}/strings_core.bin")
l11, other = set(), set()
for i in range(len(polys) // 24):
    vo, vc, nid, lvl = struct.unpack_from("<IIIB", polys, i * 24)
    if vo == NO_DATA or nid == NO_DATA:
        continue
    (l11 if lvl == 11 else other).add(nid)
for i in range(len(places) // 20):
    other.add(struct.unpack_from("<I", places, i * 20 + 8)[0])
for i in range(len(pois) // 36):
    nid, = struct.unpack_from("<I", pois, i * 36 + 16)
    pst, ppc = struct.unpack_from("<II", pois, i * 36 + 24)
    other.update((nid, pst))
    # parent_postcode_id is the L11 polygon's name_id itself: a postcode use
only = sorted(o for o in l11 - other if o < len(core))
dropped = 0
skip = {}
for o in only:
    end = core.find(b"\0", o)
    skip[o] = end + 1
    dropped += end + 1 - o
with open(OUT, "wb") as f:
    pos = 0
    for o in only:
        f.write(core[pos:o])
        pos = skip[o]
    f.write(core[pos:])


def z(p):
    return len(subprocess.run(["zstd", "-18", "-T16", "-c", p], stdout=subprocess.PIPE, check=True).stdout)


MiB = 1 << 20
print(f"L11 polygon names {len(l11)}, referenced by nothing else {len(only)}, bytes {dropped} ({dropped / MiB:.2f} MiB raw)")
a, b = z(f"{REC}/strings_core.bin"), z(OUT)
print(f"strings_core zstd -18: {a / MiB:.2f} -> {b / MiB:.2f} MiB (saves {(a - b) / MiB:.2f} MiB)")
os.remove(OUT)
