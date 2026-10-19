#!/usr/bin/env python3
"""exgr.py [--truth unc] [--set smallest] [--pool photo] <label>... : Immich-field error on a pool with
and without the points whose truth country is GR (Greek islands: the state flips between the
coincident 'Aegean' and 'South/North Aegean' island rings by stored ring area, a reader tie-break,
not a border error). Prints any / city / state / country %, all points and excluding GR."""
import argparse
import json

ap = argparse.ArgumentParser()
ap.add_argument("--truth", default="unc")
ap.add_argument("--set", default="smallest")
ap.add_argument("--pool", default="photo")
ap.add_argument("--q", default="/ssd/tiers3/q")
ap.add_argument("labels", nargs="+")
a = ap.parse_args()


def imm(js):
    ad = json.loads(js).get("address", {})
    co = lambda *ks: next((ad[k] for k in ks if ad.get(k) is not None), None)  # noqa: E731
    cc = ad.get("country_code")
    return {"city": co("city", "town", "village", "hamlet", "municipality"), "state": co("state", "province"),
            "country": cc.upper() if cc else ad.get("country")}


def load(lab):
    return [imm(line.rstrip("\n").split(" ", 2)[2]) for line in open(f"{a.q}/{lab}_{a.set}/{a.pool}.txt")]


t = load(a.truth)
keep = [x["country"] != "GR" for x in t]
for lab in a.labels:
    o = load(lab)
    out = []
    for sel in (None, True):
        n = sum(1 for k in keep if sel is None or k)
        c = {"any": 0, "city": 0, "state": 0, "country": 0}
        for ti, oi, k in zip(t, o, keep):
            if sel and not k:
                continue
            c["any"] += ti != oi
            for f in ("city", "state", "country"):
                c[f] += ti[f] != oi[f]
        out.append(" ".join(f"{f} {100 * v / n:.3f}" for f, v in c.items()))
    print(f"{lab:<8} all: {out[0]}   | excl GR: {out[1]}")
