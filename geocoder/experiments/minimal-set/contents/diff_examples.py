#!/usr/bin/env python3
"""Examples of Immich (country, state, city) differences between two geocoder dirs on the same points.

Usage: diff_examples.py <grid_query> <dir A> <dir B> <coords.txt> [country filter] [max examples]
"""
import json
import subprocess
import sys
from collections import Counter

gq, da, db, coords = sys.argv[1:5]
ccf = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] != "-" else None
nmax = int(sys.argv[6]) if len(sys.argv) > 6 else 12
text = open(coords).read()


def run(d):
    r = subprocess.run([gq, d], input=text.encode(), stdout=subprocess.PIPE, check=True)
    return [json.loads(line.split(" ", 2)[2]).get("address", {}) for line in r.stdout.decode().splitlines()]


def immich(a):
    cc = a.get("country_code")
    city = next(((k, a[k]) for k in ("city", "town", "village", "hamlet", "municipality") if a.get(k)), None)
    return (cc, a.get("state") or a.get("province"), city)


A, B = run(da), run(db)
pts = text.splitlines()
kinds = Counter()
shown = 0
for p, a, b in zip(pts, A, B):
    ia, ib = immich(a), immich(b)
    if ia == ib or (ccf and a.get("country_code") != ccf):
        continue
    kinds[(a.get("country_code"), (ia[2] or ("none",))[0], (ib[2] or ("none",))[0])] += 1
    if shown < nmax:
        shown += 1
        print(p, "\n   A:", json.dumps(a, ensure_ascii=False), "\n   B:", json.dumps(b, ensure_ascii=False))
print("(cc, A city field, B city field):", kinds.most_common(15))
