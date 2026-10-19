#!/usr/bin/env python3
"""cc_rates.py <pts file> <truth label> <cc> <label>... : per-label wrong rates for points whose truth
country is <cc>, split by the pool's level column when present (field per level: L2 country, L4 state,
others city) and overall city/state/country."""
import json
import os
import sys
from collections import defaultdict

pts, truth, cc = sys.argv[1:4]
labels = sys.argv[4:]
pool = os.path.basename(pts)[:-4]
Q = "/ssd/verify5/q"


def fields(js):
    ad = json.loads(js).get("address", {})
    c = ad.get("country_code")
    country = c.upper() if c else ad.get("country")
    if not country:
        return (None, None, None)
    city = next((ad[k] for k in ("city", "town", "village", "hamlet", "municipality") if ad.get(k) is not None), None)
    state = next((ad[k] for k in ("state", "province") if ad.get(k) is not None), None)
    return (city, state, country)


lv = [l.split()[2] if len(l.split()) > 2 else "-" for l in open(pts)]
t = [fields(l.split(" ", 2)[2]) for l in open(f"{Q}/{truth}/{pool}.txt")]
for lab in labels:
    o = [fields(l.split(" ", 2)[2]) for l in open(f"{Q}/{lab}/{pool}.txt")]
    acc = defaultdict(lambda: [0, 0, 0, 0])
    for l, a, b in zip(lv, t, o):
        if a[2] != cc:
            continue
        for g in (l, "all"):
            r = acc[g]
            r[0] += 1
            r[1] += a[0] != b[0]
            r[2] += a[1] != b[1]
            r[3] += a[2] != b[2]
    print(lab, " ".join(f"{g}: n={r[0]} city {100*r[1]/r[0]:.2f} state {100*r[2]/r[0]:.2f} country {100*r[3]/r[0]:.2f}" for g, r in sorted(acc.items()) if r[0]))
