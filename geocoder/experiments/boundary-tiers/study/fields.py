#!/usr/bin/env python3
"""Per address key: share of points whose value differs from the truth (unc_<set>), one pool.
usage: fields.py <set> <pool> <label> ...  -> one line per label (keys changed on >= 0.01%)"""
import json
import sys
from collections import Counter

st, pool = sys.argv[1:3]


def load(label):
    return [json.loads(l.split(" ", 2)[2]).get("address", {}) for l in open(f"/ssd/tiers2/q/{label}_{st}/{pool}.txt")]


t = load("unc")
for label in sys.argv[3:]:
    o = load(label)
    c = Counter()
    for a, b in zip(t, o):
        for k in set(a) | set(b):
            if a.get(k) != b.get(k):
                c[k] += 1
    n = len(t)
    print(label, " ".join(f"{k} {100 * v / n:.2f}" for k, v in c.most_common() if 100 * v / n >= 0.01))
