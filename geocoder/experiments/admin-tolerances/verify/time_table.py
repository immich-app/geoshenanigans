#!/usr/bin/env python3
"""time_table.py <time tsv> : timing.sh rows as one line per label: warm photo-like / warm L2 border /
cold, microseconds per query (label reader set pool points best_s load_s us_per_query)."""
import sys
from collections import defaultdict

rows = defaultdict(dict)
for line in open(sys.argv[1]):
    label, reader, d, pool, n, best, load, us = line.rstrip("\n").split("\t")
    kind = "cold" if pool == "cold" else ("L2" if pool.endswith("_L2") else "photo")
    rows[label][kind] = float(us)
for label, v in rows.items():
    print(f"{label:28s} photo {v.get('photo', float('nan')):7.1f}  L2 {v.get('L2', float('nan')):7.1f}  cold {v.get('cold', float('nan')):7.1f}")
