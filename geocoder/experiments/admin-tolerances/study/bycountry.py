#!/usr/bin/env python3
"""bycountry.py [--set smallest] [--q /ssd/tiers3/q] [--truth unc] <A> <B> <pool> [field] : points wrong
(Immich field, default any) under A only / B only, by the truth's country code (top 10 each)."""
import argparse
import json
from collections import Counter

ap = argparse.ArgumentParser()
ap.add_argument("--set", default="smallest")
ap.add_argument("--q", default="/ssd/tiers3/q")
ap.add_argument("--truth", default="unc")
ap.add_argument("A")
ap.add_argument("B")
ap.add_argument("pool")
ap.add_argument("field", nargs="?", default="any")
a = ap.parse_args()


def imm(js):
    ad = json.loads(js).get("address", {})
    co = lambda *ks: next((ad[k] for k in ks if ad.get(k) is not None), None)  # noqa: E731
    cc = ad.get("country_code")
    return {"city": co("city", "town", "village", "hamlet", "municipality"), "state": co("state", "province"),
            "country": cc.upper() if cc else ad.get("country")}


def load(lab):
    return [line.rstrip("\n").split(" ", 2)[2] for line in open(f"{a.q}/{lab}_{a.set}/{a.pool}.txt")]


t, x, y = load(a.truth), load(a.A), load(a.B)
ca, cb = Counter(), Counter()
for tj, xj, yj in zip(t, x, y):
    ti, xi, yi = imm(tj), imm(xj), imm(yj)
    wa = xi != ti if a.field == "any" else xi[a.field] != ti[a.field]
    wb = yi != ti if a.field == "any" else yi[a.field] != ti[a.field]
    if wa and not wb:
        ca[ti["country"]] += 1
    if wb and not wa:
        cb[ti["country"]] += 1
print(f"{a.pool}:{a.field} wrong only under {a.A}: {sum(ca.values())}  top {ca.most_common(10)}")
print(f"{a.pool}:{a.field} wrong only under {a.B}: {sum(cb.values())}  top {cb.most_common(10)}")
