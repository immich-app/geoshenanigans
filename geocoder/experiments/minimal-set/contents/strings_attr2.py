#!/usr/bin/env python3
"""Second-pass attribution of strings_core.bin strings that admin-minimal does not reference.

Adds consumers from the planet's poi/all poi_records.bin (name_id, parent_street_id,
parent_postcode_id; 36 B records, builder/src/types.h:865-905) and no-addresses
street_ways.bin (12 B WayHeader, name_id at +8, types.h:10-18) to the class bitmask
inventory.py saved, then splits the "not used by admin-minimal" bytes by consumer.

A string with no consumer at all lands in core by construction
(string_home_tier: mask == 0 -> tier 0, builder/src/parsed_data.h:226-234).

Usage: strings_attr2.py <minimal dir> <strcls.npy> <poi_all_poi_records.bin> <street_ways.bin> <out json>
"""
import json
import sys

import numpy as np

NO_DATA = 0xFFFFFFFF


def main():
    mdir, clsf, poif, waysf, out = sys.argv[1:6]
    sb = np.fromfile(f"{mdir}/strings_core.bin", dtype=np.uint8)
    nul = np.flatnonzero(sb == 0)
    starts = np.concatenate(([0], nul[:-1] + 1)).astype(np.int64)
    nbytes = (nul - starts + 1).astype(np.int64)
    ns = len(starts)
    cls = np.load(clsf).astype(np.uint32)
    core_end = len(sb)

    def mark(offs, bit):
        offs = offs[(offs != NO_DATA) & (offs < core_end)].astype(np.int64)
        i = np.searchsorted(starts, offs)
        ok = (i < ns) & (starts[np.minimum(i, ns - 1)] == offs)
        np.bitwise_or.at(cls, i[ok], bit)
        return int(ok.sum())

    poi = np.memmap(poif, dtype=np.uint32, mode="r").reshape(-1, 9)
    # u32 words: 0 lat, 1 lng, 2 voff, 3 vcnt, 4 name, 5 cat/tier/flags/imp, 6 street, 7 postcode, 8 parent
    tier = (poi[:, 5] >> 8) & 0xFF
    R = {"poi_all_records": int(len(poi))}
    for t in (1, 2, 3):
        m = tier == t
        R[f"poi_tier{t}_name_refs_in_core"] = mark(np.asarray(poi[m, 4]), 1 << (12 + t))
    R["poi_street_refs_in_core"] = mark(np.asarray(poi[:, 6]), 1 << 16)
    R["poi_postcode_refs_in_core"] = mark(np.asarray(poi[:, 7]), 1 << 17)
    ways = np.memmap(waysf, dtype=np.uint32, mode="r").reshape(-1, 3)
    R["street_ways"] = int(len(ways))
    R["street_name_refs_in_core"] = mark(np.asarray(ways[:, 2]), 1 << 18)

    used_min = (cls & 3) != 0
    other_admin = (cls & (4 | 16 | 128 | 256 | 512 | 1024)) != 0
    other_place = (cls & 8) != 0
    poi_any = (cls & ((1 << 13) | (1 << 14) | (1 << 15) | 32)) != 0
    street = (cls & ((1 << 16) | (1 << 18))) != 0
    pc = (cls & ((1 << 17) | 64)) != 0
    rest = ~used_min
    groups = {}

    def take(name, m):
        nonlocal rest
        mm = rest & m
        groups[name] = {"strings": int(mm.sum()), "bytes": int(nbytes[mm].sum())}
        rest = rest & ~m

    take("admin L9/L10/L11/L15 names (admin mode only)", other_admin)
    take("suburb/neighbourhood/quarter node names", other_place)
    take("POI names and/or streets they share (poi tiers 1-3)", poi_any & street)
    take("POI names only (poi tiers 1-3)", poi_any)
    take("street names only (no POI consumer seen)", street)
    take("postcodes", pc)
    groups["no consumer found (orphans or addr/interp-only)"] = {"strings": int(rest.sum()),
                                                                "bytes": int(nbytes[rest].sum())}
    R["unused_by_minimal_split"] = groups
    R["unused_by_minimal_total"] = {"strings": int((~used_min).sum()), "bytes": int(nbytes[~used_min].sum())}
    rng = np.random.default_rng(7)
    idx = np.flatnonzero(rest)
    pick = np.sort(rng.choice(idx, size=min(40, len(idx)), replace=False)) if len(idx) else []
    R["orphan_sample"] = [sb[starts[i]:starts[i] + nbytes[i] - 1].tobytes().decode("utf-8", "replace") for i in pick]
    np.save(out + ".strcls2.npy", cls)
    json.dump(R, open(out, "w"), indent=1, ensure_ascii=False)
    print(json.dumps(R, indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()
