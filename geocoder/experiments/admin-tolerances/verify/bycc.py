#!/usr/bin/env python3
"""bycc.py <pool> <truth> <a> <b> <field city|state|country> : discordant points of a field between labels
a and b (wrong in one only, against the truth), grouped by the truth's country code, with examples."""
import json
import sys
from collections import Counter

pool, truth, a, b, field = sys.argv[1:6]
Q = "/ssd/verify5/q"
FI = {"city": 0, "state": 1, "country": 2}[field]


def fields(js):
    ad = json.loads(js).get("address", {})
    cc = ad.get("country_code")
    country = cc.upper() if cc else ad.get("country")
    if not country:
        return (None, None, None)
    city = next((ad[k] for k in ("city", "town", "village", "hamlet", "municipality") if ad.get(k) is not None), None)
    state = next((ad[k] for k in ("state", "province") if ad.get(k) is not None), None)
    return (city, state, country)


def load(lab):
    return [l.rstrip("\n").split(" ", 2) for l in open(f"{Q}/{lab}/{pool}.txt")]


t, x, y = load(truth), load(a), load(b)
ca, cb = Counter(), Counter()
ex = {}
for (la, ln, tj), (_, _, aj), (_, _, bj) in zip(t, x, y):
    tf, af, bf = fields(tj), fields(aj), fields(bj)
    wa, wb = af[FI] != tf[FI], bf[FI] != tf[FI]
    if wa == wb:
        continue
    k = tf[2]
    (ca if wa else cb)[k] += 1
    ex.setdefault((k, wa), []).append((la, ln, tf[FI], af[FI], bf[FI]))
print(f"{field}: wrong only in {a}: {sum(ca.values())}, only in {b}: {sum(cb.values())}")
for k, n in (ca + cb).most_common(12):
    print(f"  {k}: only {a} {ca[k]}, only {b} {cb[k]}; e.g. {(ex.get((k, True)) or ex.get((k, False)))[0]}")
