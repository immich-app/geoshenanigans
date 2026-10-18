#!/usr/bin/env python3
"""Answer changes of each configuration vs a base (normally T = truth, uncapped), per pool,
from q/<name>.<pool>.jsonl (runq.sh). Same point files for every configuration.

immich   = (city, state, country) with the prototype mapper (immich feat/geoshenanigans-geocoder
           geocoding.repository.ts): city = city ?? town ?? village ?? hamlet ?? municipality,
           state = state ?? province, country = country_code
admin    = the address object without road / house_number / postcode / landmark (every other key:
           country, country_code, state, province, region, county, municipality, city, town, village,
           hamlet, suburb, neighbourhood, quarter, borough, district, ... whatever the reader emits)
other    = road, house_number, postcode, landmark separately (to check they do not depend on the
           admin boundaries; postcode depends on postal_* which follow the quality dir)
usage: compare.py <base> <name> ... [--json out.json]"""
import json, sys
from collections import Counter

POOLS = ["places", "random", "L2", "L4", "L8", "p200k"]
NONADMIN = ("road", "house_number", "postcode", "landmark")
argv = sys.argv[1:]
out_json = None
if "--json" in argv:
    i = argv.index("--json"); out_json = argv[i + 1]; argv = argv[:i] + argv[i + 2:]
base, names = argv[0], argv[1:]


def coalesce(a, *keys):
    return next((a[k] for k in keys if a.get(k) is not None), None)


def load(name, pool):
    rows = []
    with open(f"/ssd/shrink/grid/q/{name}.{pool}.jsonl") as f:
        for line in f:
            a, b, js = line.rstrip("\n").split(" ", 2)
            rows.append(((a, b), js))
    return rows


def parse(js):
    j = json.loads(js)
    ad = j.get("address", {})
    imm = (coalesce(ad, "city", "town", "village", "hamlet", "municipality"), coalesce(ad, "state", "province"),
           ad.get("country_code"))
    adm = tuple(sorted((k, v) for k, v in ad.items() if k not in NONADMIN))
    return imm, adm, tuple(ad.get(k) for k in NONADMIN), ad


res = {}
keys_seen = Counter()
for p in POOLS:
    b = load(base, p)
    bp = [parse(js) for _, js in b]
    for _, _, _, ad in bp:
        keys_seen.update(ad.keys())
    for n in names:
        o = load(n, p)
        assert len(o) == len(b) and all(x[0] == y[0] for x, y in zip(b, o)), (n, p)
        N = len(b)
        c = Counter()
        ex = []
        for i in range(N):
            if b[i][1] == o[i][1]:
                continue
            c["json"] += 1
            bi, oi = bp[i], parse(o[i][1])
            if bi[0] != oi[0]:
                c["immich"] += 1
                for k, f in enumerate(("city", "state", "country")):
                    c[f] += bi[0][k] != oi[0][k]
                if len(ex) < 3:
                    ex.append(f"{b[i][0][0]} {b[i][0][1]}: {bi[0]} -> {oi[0]}")
            if bi[1] != oi[1]:
                c["admin"] += 1
            for k, f in enumerate(NONADMIN):
                c[f] += bi[2][k] != oi[2][k]
        res.setdefault(n, {})[p] = {"n": N, **{k: c[k] for k in ("json", "immich", "city", "state", "country", "admin") + NONADMIN},
                                    "examples": ex}

if out_json:
    json.dump({"base": base, "res": res, "address_keys_in_base": dict(keys_seen)}, open(out_json, "w"), indent=1)
print(f"base {base}; address keys seen in base answers: {dict(keys_seen.most_common())}")
print("| config | pool | n | JSON changed | Immich changed | city | state | country | admin fields changed | road | house_number | postcode | landmark |")
print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for n in names:
    for p in POOLS:
        r = res[n][p]
        N = r["n"]
        pct = lambda k: f"{r[k] / N:.2%}"
        print(f"| {n} | {p} | {N:,} | {pct('json')} | {pct('immich')} | {pct('city')} | {pct('state')} | {pct('country')} | "
              f"{pct('admin')} | {pct('road')} | {pct('house_number')} | {pct('postcode')} | {pct('landmark')} |")
