"""diff_fields.py <set> <reader A> <reader B>: for query_json answers that differ, count the
address fields that differ as (field, A value -> B value)."""
import collections
import json
import sys

s, a, b = sys.argv[1:4]
R = "/ssd/reader/out/"
la = open(f"{R}{s}.{a}.q").read().splitlines()
lb = open(f"{R}{s}.{b}.q").read().splitlines()
c = collections.Counter()
for x, y in zip(la, lb):
    if x == y:
        continue
    da = json.loads(x.split(" ", 2)[2]).get("address") or {}
    db = json.loads(y.split(" ", 2)[2]).get("address") or {}
    for k in sorted(set(da) | set(db)):
        if da.get(k) != db.get(k):
            c[(k, da.get(k), db.get(k))] += 1
for (k, va, vb), n in c.most_common():
    print(f"{n:5d}  {k}: {va} -> {vb}")
