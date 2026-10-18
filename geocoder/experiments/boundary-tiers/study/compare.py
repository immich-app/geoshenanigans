#!/usr/bin/env python3
"""Answer changes of a candidate vs the truth, per pool, from /ssd/tiers2/q/<label>/<pool>.txt.

immich  = (city, state, country) with the Immich prototype mapper (geocoding.repository.ts on
          feat/geoshenanigans-geocoder): city = city ?? town ?? village ?? hamlet ?? municipality,
          state = state ?? province, country = country_code
admin   = the address object without road / house_number / postcode / landmark
usage: compare.py <truth label> <label> [pool ...]   -> /ssd/tiers2/cmp/<label>.json + one line per pool
"""
import json
import os
import sys
from collections import Counter

Q = "/ssd/tiers2/q"
NONADMIN = ("road", "house_number", "postcode", "landmark")
POOLS = ["places", "p200k", "L2", "L4", "L8", "uniform100k", "random5k"]


def coalesce(a, *keys):
    return next((a[k] for k in keys if a.get(k) is not None), None)


def parse(js):
    ad = json.loads(js).get("address", {})
    imm = (coalesce(ad, "city", "town", "village", "hamlet", "municipality"), coalesce(ad, "state", "province"),
           ad.get("country_code"))
    adm = tuple(sorted((k, v) for k, v in ad.items() if k not in NONADMIN))
    return imm, adm


def load(label, pool):
    with open(f"{Q}/{label}/{pool}.txt") as f:
        return [line.rstrip("\n").split(" ", 2) for line in f]


def compare(truth, label, pools):
    res = {}
    for p in pools:
        t, o = load(truth, p), load(label, p)
        assert len(t) == len(o) and all(a[:2] == b[:2] for a, b in zip(t, o)), (label, p)
        c = Counter()
        ex = []
        for a, b in zip(t, o):
            if a[2] == b[2]:
                continue
            c["json"] += 1
            ta, oa = parse(a[2]), parse(b[2])
            if ta[0] != oa[0]:
                c["immich"] += 1
                for k, f in enumerate(("city", "state", "country")):
                    c[f] += ta[0][k] != oa[0][k]
                if len(ex) < 4:
                    ex.append(f"{a[0]} {a[1]}: {ta[0]} -> {oa[0]}")
            c["admin"] += ta[1] != oa[1]
        res[p] = {"n": len(t), **{k: c[k] for k in ("json", "immich", "city", "state", "country", "admin")}, "examples": ex}
    return res


if __name__ == "__main__":
    truth, label = sys.argv[1:3]
    pools = sys.argv[3:] or POOLS
    r = compare(truth, label, pools)
    os.makedirs("/ssd/tiers2/cmp", exist_ok=True)
    json.dump({"truth": truth, "label": label, "res": r}, open(f"/ssd/tiers2/cmp/{label}.json", "w"), indent=1)
    print(label + " vs " + truth + ": " + "  ".join(
        f"{p} imm {100 * v['immich'] / v['n']:.2f}% (ci {100 * v['city'] / v['n']:.2f} st {100 * v['state'] / v['n']:.2f} "
        f"co {100 * v['country'] / v['n']:.2f})" for p, v in r.items()))
