#!/usr/bin/env python3
"""Print answer differences between two labels for one pool: the address fields that differ.
usage: diffs.py <label a> <label b> <pool> [max] [field filter: city|state|country]"""
import json
import sys
from collections import Counter

a, b, pool = sys.argv[1:4]
mx = int(sys.argv[4]) if len(sys.argv) > 4 else 20
flt = sys.argv[5] if len(sys.argv) > 5 else None
NONADMIN = ("road", "house_number", "postcode", "landmark")


def imm(ad):
    c = lambda *k: next((ad[x] for x in k if ad.get(x) is not None), None)
    return {"city": c("city", "town", "village", "hamlet", "municipality"), "state": c("state", "province"),
            "country": ad.get("country_code")}


A = [l.rstrip("\n").split(" ", 2) for l in open(f"/ssd/tiers2/q/{a}/{pool}.txt")]
B = [l.rstrip("\n").split(" ", 2) for l in open(f"/ssd/tiers2/q/{b}/{pool}.txt")]
n = 0
cc = Counter()
for x, y in zip(A, B):
    if x[2] == y[2]:
        continue
    ax, ay = json.loads(x[2]).get("address", {}), json.loads(y[2]).get("address", {})
    ix, iy = imm(ax), imm(ay)
    if flt and ix[flt] == iy[flt]:
        continue
    if not flt and ix == iy:
        continue
    cc[ax.get("country_code")] += 1
    if n < mx:
        keys = sorted(set(ax) | set(ay))
        d = {k: (ax.get(k), ay.get(k)) for k in keys if ax.get(k) != ay.get(k) and k not in NONADMIN}
        print(f"{x[0]} {x[1]} cc={ax.get('country_code')}: {d}")
    n += 1
print(f"total {n}; by truth country: {cc.most_common(15)}")
