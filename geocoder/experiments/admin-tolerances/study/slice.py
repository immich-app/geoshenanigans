#!/usr/bin/env python3
"""slice.py --truth <label> --set <set> --pools B2,B4,photo --cc CN,PL,UA <label>... : Immich-field error
(country / state / city / any) per truth country code, for points whose truth answer is in one of the
given countries (or whose answer under the label is: --either). Also counts truth points per country."""
import argparse
import json
from collections import Counter, defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("--truth", required=True)
ap.add_argument("--set", default="smallest")
ap.add_argument("--q", default="/ssd/tiers3/q")
ap.add_argument("--pools", default="B2,B4,B8,photo")
ap.add_argument("--cc", default="CN,PL,UA")
ap.add_argument("labels", nargs="+")
a = ap.parse_args()
CC = set(a.cc.split(","))


def imm(js):
    ad = json.loads(js).get("address", {})
    co = lambda *ks: next((ad[k] for k in ks if ad.get(k) is not None), None)  # noqa: E731
    cc = ad.get("country_code")
    return {"city": co("city", "town", "village", "hamlet", "municipality"), "state": co("state", "province"),
            "country": cc.upper() if cc else ad.get("country")}


def load(lab, pool):
    return [line.rstrip("\n").split(" ", 2)[2] for line in open(f"{a.q}/{lab}_{a.set}/{pool}.txt")]


for pool in a.pools.split(","):
    t = [imm(j) for j in load(a.truth, pool)]
    n = Counter(x["country"] for x in t if x["country"] in CC)
    print(f"== {pool} ({a.set}): truth points per country {dict(n)}")
    for lab in a.labels:
        o = [imm(j) for j in load(lab, pool)]
        err = defaultdict(Counter)
        for ti, oi in zip(t, o):
            c = ti["country"]
            if c not in CC:
                continue
            for f in ("country", "state", "city"):
                err[c][f] += ti[f] != oi[f]
            err[c]["any"] += ti != oi
        print(f"  {lab:<12} " + "  ".join(
            f"{c}: cc {100 * err[c]['country'] / n[c]:.2f} st {100 * err[c]['state'] / n[c]:.2f} ct {100 * err[c]['city'] / n[c]:.2f}"
            for c in sorted(n)))
