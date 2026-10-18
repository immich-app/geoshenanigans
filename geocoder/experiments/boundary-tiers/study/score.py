#!/usr/bin/env python3
"""Accuracy of candidates vs the truth (uncapped-with-holes, standard reader) from the stored answers
q/<label>_<set>/<pool>.txt; one JSON per label+set (cmp/<label>_<set>.score.json).

Immich fields (prototype mapper): city = city ?? town ?? village ?? hamlet ?? municipality,
state = state ?? province, country = country_code.
Per pool: n, immich (any of the three changed), city, state, country, cc_wrong (both answers name a
country and they differ), cc_none (one names a country, the other none), admin (any admin field), subcity (suburb,
neighbourhood, quarter, city_district, borough or city_block), postcode.
usage: score.py <set> <label> ...      (<label>_<set> answers; truth unc_<set>)
"""
import json
import os
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

Q = "/ssd/tiers2/q"
NONADMIN = ("road", "house_number", "postcode", "landmark")
SUBCITY = ("suburb", "neighbourhood", "quarter", "city_district", "borough", "city_block")
POOLS = ["places", "p200k", "L2", "L4", "L8", "uniform100k", "random5k"]


def coalesce(a, *keys):
    return next((a[k] for k in keys if a.get(k) is not None), None)


def parse(js):
    ad = json.loads(js).get("address", {})
    imm = (coalesce(ad, "city", "town", "village", "hamlet", "municipality"), coalesce(ad, "state", "province"),
           ad.get("country_code"))
    return imm, tuple(sorted((k, v) for k, v in ad.items() if k not in NONADMIN))


def load(label, pool):
    with open(f"{Q}/{label}/{pool}.txt") as f:
        return [line.rstrip("\n").split(" ", 2) for line in f]


def one(args):
    st, label = args
    truth = f"unc_{st}"
    res = {}
    for p in POOLS:
        t, o = load(truth, p), load(f"{label}_{st}", p)
        assert len(t) == len(o) and all(a[:2] == b[:2] for a, b in zip(t, o)), (label, p)
        c = Counter()
        for a, b in zip(t, o):
            if a[2] == b[2]:
                continue
            ta, oa = parse(a[2]), parse(b[2])
            if ta[0] != oa[0]:
                c["immich"] += 1
                for k, f in enumerate(("city", "state", "country")):
                    c[f] += ta[0][k] != oa[0][k]
                x, y = ta[0][2], oa[0][2]
                if x != y:
                    c["cc_wrong" if (x and y) else "cc_none"] += 1
            c["admin"] += ta[1] != oa[1]
            ja, jb = json.loads(a[2]).get("address", {}), json.loads(b[2]).get("address", {})
            c["subcity"] += any(ja.get(k) != jb.get(k) for k in SUBCITY)
            c["postcode"] += ja.get("postcode") != jb.get("postcode")
        res[p] = {"n": len(t), **{k: c[k] for k in ("immich", "city", "state", "country", "cc_wrong", "cc_none", "admin", "subcity", "postcode")}}
    json.dump(res, open(f"/ssd/tiers2/cmp/{label}_{st}.score.json", "w"), indent=1)
    return label, res


if __name__ == "__main__":
    st = sys.argv[1]
    labels = sys.argv[2:]
    with ProcessPoolExecutor(int(os.environ.get("JOBS", "12"))) as ex:
        for label, r in ex.map(one, [(st, l) for l in labels]):
            print(f"{label}_{st}: " + "  ".join(f"{p} {100 * v['immich'] / v['n']:.2f}" for p, v in r.items()), flush=True)
