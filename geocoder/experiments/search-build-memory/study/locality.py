"""locality.py <set dir> <kind: place|poi>: page-level locality of the index build's random reads.

For each record in file order: the 4 KiB page of its name string and of its parent polygon. Prints
distinct pages touched and the LRU miss count for caches of several sizes (pages)."""
import json
import struct
import sys
from collections import OrderedDict

import numpy as np

d, kind = sys.argv[1], sys.argv[2]
lay = json.load(open(d + "/strings_layout.json"))["tiers"]
if kind == "place":
    raw = np.fromfile(d + "/place_nodes.bin", dtype=np.uint8).reshape(-1, 20)
    name = raw[:, 8:12].copy().view("<u4").ravel()
    parent = raw[:, 16:20].copy().view("<u4").ravel()
else:
    raw = np.memmap(d + "/poi_records.bin", dtype=np.uint8, mode="r")
    rec = 36
    raw = raw.reshape(-1, rec)
    n = raw.shape[0]
    step = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    raw = raw[::step]
    name = raw[:, 16:20].copy().view("<u4").ravel()
    parent = None
NO = 0xFFFFFFFF
name = name[name != NO]
# string id -> (tier, byte offset) -> global page id
starts = np.array([t["start"] for t in lay], dtype=np.int64)
tier = np.searchsorted(starts, name.astype(np.int64), side="right") - 1
off = name.astype(np.int64) - starts[tier]
page = tier * (1 << 40) + off // 4096
print(f"{kind}: {len(name)} named records, distinct string pages {len(np.unique(page))}")
same = np.mean(page[1:] == page[:-1])
near = np.mean(np.abs(np.diff(page)) <= 1)
print(f"  consecutive same page {same:.3f}, within 1 page {near:.3f}")


def lru(seq, cap):
    c = OrderedDict()
    miss = 0
    for p in seq:
        if p in c:
            c.move_to_end(p)
        else:
            miss += 1
            c[p] = 1
            if len(c) > cap:
                c.popitem(last=False)
    return miss


sample = page[: min(len(page), 2_000_000)].tolist()
for cap in (1024, 2560, 5120):
    print(f"  LRU {cap} pages ({cap * 4 // 1024} MiB): misses {lru(sample, cap)} of {len(sample)}")
if parent is not None:
    p = parent[parent != NO].astype(np.int64) * 24 // 4096
    print(f"  parent polygon pages distinct {len(np.unique(p))}; LRU 1024: {lru(p[:2_000_000].tolist(), 1024)} misses")
