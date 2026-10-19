#!/usr/bin/env python3
"""paired.py [--truth unc] [--set smallest] [--q /ssd/tiers3/q] <A> <B> <pool:field>... : paired comparison
of two labels against the truth: points where the Immich field is wrong under A only / B only, and the
two-sided exact sign-test p-value (binomial, p = 0.5) on those discordant points."""
import argparse
import json
from math import comb

ap = argparse.ArgumentParser()
ap.add_argument("--truth", default="unc")
ap.add_argument("--set", default="smallest")
ap.add_argument("--q", default="/ssd/tiers3/q")
ap.add_argument("--excl", default="", help="skip points whose truth country is in this comma list")
ap.add_argument("A")
ap.add_argument("B")
ap.add_argument("items", nargs="+")
a = ap.parse_args()


def immich(js):
    ad = json.loads(js).get("address", {})
    cc = ad.get("country_code")
    country = cc.upper() if cc else ad.get("country")

    def co(*ks):
        return next((ad[k] for k in ks if ad.get(k) is not None), None)
    return {"city": co("city", "town", "village", "hamlet", "municipality"), "state": co("state", "province"), "country": country}


def load(label, pool):
    return [line.rstrip("\n").split(" ", 2)[2] for line in open(f"{a.q}/{label}_{a.set}/{pool}.txt")]


def sign_p(x, y):
    n = x + y
    if n == 0:
        return 1.0
    k = min(x, y)
    p = sum(comb(n, i) for i in range(k + 1)) / 2 ** n * 2
    return min(1.0, p)


cache = {}
for it in a.items:
    pool, fld = it.split(":")
    if pool not in cache:
        cache[pool] = (load(a.truth, pool), load(a.A, pool), load(a.B, pool))
    t, x, y = cache[pool]
    onlyA = onlyB = both = 0
    skip = set(a.excl.split(",")) - {""}
    n = 0
    for tj, xj, yj in zip(t, x, y):
        ti = immich(tj)
        if ti["country"] in skip:
            continue
        n += 1
        wa = fld == "any" and immich(xj) != ti or fld != "any" and immich(xj)[fld] != ti[fld]
        wb = fld == "any" and immich(yj) != ti or fld != "any" and immich(yj)[fld] != ti[fld]
        onlyA += wa and not wb
        onlyB += wb and not wa
        both += wa and wb
    print(f"{pool}:{fld}  n {n}  {a.A} {100 * (onlyA + both) / n:.3f}%  {a.B} {100 * (onlyB + both) / n:.3f}%  "
          f"only {a.A} {onlyA}  only {a.B} {onlyB}  p {sign_p(onlyA, onlyB):.3g}")
