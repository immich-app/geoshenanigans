#!/usr/bin/env python3
"""Cross-references from POI major records into the admin/string files.

- parent_poly_id is a slot in the admin-mode admin_polygons.bin (POI files are
  shared by full/no-addresses/admin, builder/src/build_index.cpp:1467-1471).
  admin-minimal compacts its own polygon table (cell_index.cpp
  write_admin_minimal_polygons) and POI files are not remapped, so check how
  each POI's parent lands in admin-minimal's table.
- name_id / parent_street_id / parent_postcode_id: which string tier each
  lands in (strings_layout.json), i.e. which file must be present to resolve it.
- Road diffs between recommended and minimal_poi outputs: how many sit next to
  a POI whose parent slot means a different polygon in admin-minimal.
"""
import collections, json, mmap, struct

REC, MIN = "/ssd/shrink/idx/recommended", "/ssd/shrink/idx/smallest"
NO_DATA = 0xFFFFFFFF
POI = struct.Struct("<ffIIIBBBBIII")
POLY = struct.Struct("<IIIBBBBfHH")


def mm(p):
    f = open(p, "rb")
    return mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)


recp, minp, pois = mm(f"{REC}/admin_polygons.bin"), mm(f"{MIN}/admin_polygons.bin"), mm(f"{REC}/poi_records.bin")
nrec, nmin, npoi = len(recp) // 24, len(minp) // 24, len(pois) // POI.size
layout = json.load(open(f"{REC}/strings_layout.json"))["tiers"]


def tier(off):
    if off == NO_DATA:
        return "NO_DATA"
    for t in layout:
        if t["start"] <= off < t["end"]:
            return t["name"]
    return "out-of-range"


parent = collections.Counter()
tiers = {k: collections.Counter() for k in ("name_id", "parent_street_id", "parent_postcode_id")}
for i in range(npoi):
    lat, lng, vo, vc, nid, cat, tr, fl, imp, pst, ppc, ppoly = POI.unpack_from(pois, i * POI.size)
    if nid == NO_DATA and vo == NO_DATA:
        parent["tombstone slot"] += 1
        continue
    tiers["name_id"][tier(nid)] += 1
    tiers["parent_street_id"][tier(pst)] += 1
    tiers["parent_postcode_id"][tier(ppc)] += 1
    if ppoly == NO_DATA:
        parent["parent NO_DATA (chain check passes)"] += 1
    elif ppoly >= nmin:
        parent["slot >= admin-minimal table size (accessor None, check passes)"] += 1
    else:
        a, b = POLY.unpack_from(recp, ppoly * 24), POLY.unpack_from(minp, ppoly * 24)
        if b[0] == NO_DATA:
            parent["slot is an admin-minimal tombstone (check passes)"] += 1
        elif (a[2], a[3], a[8]) == (b[2], b[3], b[8]):
            parent["same polygon (name, level, cc)"] += 1
        else:
            parent["DIFFERENT polygon in admin-minimal (PIP tests the wrong shape)"] += 1
live = npoi - parent["tombstone slot"]
print(f"POI major slots {npoi} ({live} live); admin polygon slots: admin-mode {nrec}, admin-minimal {nmin}")
for k, v in parent.most_common():
    print(f"  {k}: {v} ({100 * v / npoi:.1f}%)")
for k, c in tiers.items():
    print(f"  {k} string tier:", dict(c.most_common()))
