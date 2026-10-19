"""namecache.py <set dir> <max tier>: the POI key phase's name reads, over every record it indexes, in
record order. Prints string pages touched, page misses of an LRU page cache of a few sizes, and the
misses left when a small LRU cache of recently seen name ids sits in front (a hit needs no string
read). Answers: are the 4 KiB major faults of a 32 MiB build reads of shared names far back in the
string pool, which a name-id cache would absorb?"""
import json
import sys
from collections import OrderedDict

import numpy as np

d, max_tier = sys.argv[1], int(sys.argv[2])
lay = json.load(open(d + "/strings_layout.json"))["tiers"]
raw = np.memmap(d + "/poi_records.bin", dtype=np.uint8, mode="r").reshape(-1, 36)
name = raw[:, 16:20].copy().view("<u4").ravel()
tier = raw[:, 21]
flags = raw[:, 22]
keep = (name != 0xFFFFFFFF) & (flags & 0x08 == 0) & (tier <= max_tier)
name = name[keep]
print(f"records indexed {len(name)}, distinct name ids {len(np.unique(name))}")
starts = np.array([t["start"] for t in lay], dtype=np.int64)
t = np.searchsorted(starts, name.astype(np.int64), side="right") - 1
page = (t.astype(np.int64) << 40) + (name.astype(np.int64) - starts[t]) // 4096
print(f"distinct string pages {len(np.unique(page))}")
_, counts = np.unique(name, return_counts=True)
counts = np.sort(counts)[::-1]
for k in (1000, 10000, 100000):
    print(f"  top {k} names cover {counts[:k].sum() / len(name):.3f} of records")


def run(pages, names, page_cap, name_cap):
    pc, nc = OrderedDict(), OrderedDict()
    miss = 0
    for p, n in zip(pages, names):
        if name_cap:
            if n in nc:
                nc.move_to_end(n)
                continue
            nc[n] = 1
            if len(nc) > name_cap:
                nc.popitem(last=False)
        if p in pc:
            pc.move_to_end(p)
        else:
            miss += 1
            pc[p] = 1
            if len(pc) > page_cap:
                pc.popitem(last=False)
    return miss


pages, names = page.tolist(), name.tolist()
for page_cap in (1024, 2048):
    for name_cap in (0, 4096, 65536):
        m = run(pages, names, page_cap, name_cap)
        print(f"  page LRU {page_cap * 4 // 1024} MiB, name-id LRU {name_cap}: string page misses {m} ({m / len(names):.3f} per record)")
