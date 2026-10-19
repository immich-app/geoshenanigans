#!/usr/bin/env python3
"""time_table.py [tsv] : µs per query from timeq3.sh rows (reader label pool n best load us), one row per
set label, columns per reader: warm photo-like (p50k), warm country-border (L2), cold first 2,000."""
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "/ssd/tiers3/cmp/time.tsv"
t = defaultdict(dict)
readers, labels = [], []
for line in open(path):
    r, lab, pool, n, best, load, us = line.split("\t")
    t[lab][(r, pool)] = float(us)
    if r not in readers:
        readers.append(r)
    if lab not in labels:
        labels.append(lab)
hdr = ["set"] + [f"{r} {p}" for r in readers for p in ("p50k", "L2", "cold2k")]
print("| " + " | ".join(hdr) + " |")
print("|" + "---|" * len(hdr))
for lab in labels:
    print("| " + " | ".join([lab] + [f"{t[lab].get((r, p), float('nan')):.0f}" for r in readers for p in ("p50k", "L2", "cold2k")]) + " |")
