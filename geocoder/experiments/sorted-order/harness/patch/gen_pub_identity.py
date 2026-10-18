"""Identity sidecars for the PUBLISHED (strategy-2 stable slot) days, so the v8
tools can run on them too: in a published file record id == slot, and a slot
live on two days holds the same OSM entity (sort step, checked against the
osm_ids sidecars). Key = record index for a live record, 0xFFFFFFFF (never
matches) for a tombstone. Tombstone content is the strategy-2 fill the sort
step's canon_sort recognises (none of them referenced on these days):
  street_ways  (u32 off, u16 cnt, u16 pad, u32 name)  = 0, 0, 0, NO_DATA
  interp_ways  (off, cnt|pad, street, start, end, type|pad) = 0, 0, NO_DATA, 0, 0, 0
  addr_points  (lat, lng, hn, street, parent, voff, vcnt) = 0, 0, 0, 0, NO_DATA, NO_DATA, 0

Usage: gen_pub_identity.py DAY_FULL_DIR OUT_DIR"""
import os
import sys

import numpy as np

NO = 0xFFFFFFFF
TOMB = {
    "street": ("street_ways.bin", [0, 0, NO]),
    "interp": ("interp_ways.bin", [0, 0, NO, 0, 0, 0]),
    "addr": ("addr_points.bin", [0, 0, 0, 0, NO, NO, 0]),
}
src, out = sys.argv[1], sys.argv[2]
os.makedirs(out, exist_ok=True)
for fam, (fname, tomb) in TOMB.items():
    m = np.memmap(os.path.join(src, fname), dtype=np.uint32, mode="r")
    rows = m.reshape(-1, len(tomb))
    t = np.array(tomb, dtype=np.uint32)
    n = rows.shape[0]
    key = np.arange(n, dtype=np.uint32)
    dead = 0
    step = 1 << 24
    for b in range(0, n, step):
        z = (rows[b:b + step] == t).all(axis=1)
        key[b:b + step][z] = NO
        dead += int(z.sum())
    key.tofile(os.path.join(out, fam + ".slot.u32"))
    print(f"{fam}: {n} records, {dead} tombstones")
