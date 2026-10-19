#!/usr/bin/env python3
"""nompick.py <pool path> <a label> <b label> <field> <level|-> <n> <seed> <tag> <truth label> : n random points of the pool
(optionally only one border level) where labels a and b give a different Immich <field>; prints
"<tag>_<i> lat lng" lines for napi-style querying, and writes the two answers to <tag>.pick.json."""
import json
import os
import random
import sys

pool, a, b, field, level, n, seed, tag, truth = sys.argv[1:10]
FI = {"city": 0, "state": 1, "country": 2}[field]
name = os.path.basename(pool)[:-4]


def fields(js):
    ad = json.loads(js).get("address", {})
    cc = ad.get("country_code")
    country = cc.upper() if cc else ad.get("country")
    if not country:
        return (None, None, None)
    city = next((ad[k] for k in ("city", "town", "village", "hamlet", "municipality") if ad.get(k) is not None), None)
    state = next((ad[k] for k in ("state", "province") if ad.get(k) is not None), None)
    return (city, state, country)


lv = [l.split()[2] if len(l.split()) > 2 else "-" for l in open(pool)]
A = [l.rstrip("\n").split(" ", 2) for l in open(f"/ssd/verify5/q/{a}/{name}.txt")]
B = [l.rstrip("\n").split(" ", 2) for l in open(f"/ssd/verify5/q/{b}/{name}.txt")]
T = [l.rstrip("\n").split(" ", 2) for l in open(f"/ssd/verify5/q/{truth}/{name}.txt")]
cand = [i for i in range(len(A)) if (level == "-" or lv[i] == level) and fields(A[i][2])[FI] != fields(B[i][2])[FI]]
random.Random(int(seed)).shuffle(cand)
pick = cand[:int(n)]
info = {}
for k, i in enumerate(pick):
    print(f"{tag}_{k} {A[i][0]} {A[i][1]}")
    info[f"{tag}_{k}"] = {"a": fields(A[i][2]), "b": fields(B[i][2]), "truth": fields(T[i][2]), "a_label": a, "b_label": b, "field": field}
json.dump({"candidates": len(cand), "points": info}, open(f"/ssd/verify5/nom/{tag}.pick.json", "w"), ensure_ascii=False)
