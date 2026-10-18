#!/usr/bin/env python3
"""Answer changes of each configuration against a base, per pool, from q/<name>.<pool>.jsonl (runq.sh).
Same field split as /ssd/shrink/grid/compare.py:
  immich = (city, state, country) with the prototype mapper (city = city ?? town ?? village ?? hamlet ??
           municipality, state = state ?? province, country = country_code)
  admin  = the address object without road / house_number / postcode / landmark
  other  = road, house_number, postcode, landmark separately; "addr" = road or house_number changed
usage: compare.py --pools "p200k L2 ..." <base> <name> ... [--json out.json]"""
import json
import sys
from collections import Counter

NONADMIN = ("road", "house_number", "postcode", "landmark")
argv = sys.argv[1:]
out_json = None
pools = None
if "--json" in argv:
    i = argv.index("--json"); out_json = argv[i + 1]; argv = argv[:i] + argv[i + 2:]
if "--pools" in argv:
    i = argv.index("--pools"); pools = argv[i + 1].split(); argv = argv[:i] + argv[i + 2:]
base, names = argv[0], argv[1:]


def coalesce(a, *keys):
    return next((a[k] for k in keys if a.get(k) is not None), None)


def load(name, pool):
    rows = []
    with open(f"/ssd/precision/tiers/q/{name}.{pool}.jsonl") as f:
        for line in f:
            a, b, js = line.rstrip("\n").split(" ", 2)
            rows.append(((a, b), js))
    return rows


def parse(js):
    ad = json.loads(js).get("address", {})
    imm = (coalesce(ad, "city", "town", "village", "hamlet", "municipality"), coalesce(ad, "state", "province"),
           ad.get("country_code"))
    adm = tuple(sorted((k, v) for k, v in ad.items() if k not in NONADMIN))
    return imm, adm, tuple(ad.get(k) for k in NONADMIN)


res = {}
for p in pools:
    b = load(base, p)
    bp = [parse(js) for _, js in b]
    for n in names:
        o = load(n, p)
        assert len(o) == len(b) and all(x[0] == y[0] for x, y in zip(b, o)), (n, p)
        c = Counter()
        ex = []
        for i in range(len(b)):
            if b[i][1] == o[i][1]:
                continue
            c["json"] += 1
            bi, oi = bp[i], parse(o[i][1])
            if bi[0] != oi[0]:
                c["immich"] += 1
            if bi[1] != oi[1]:
                c["admin"] += 1
                if len(ex) < 3:
                    ex.append(f"{b[i][0][0]} {b[i][0][1]}: {dict(set(bi[1]) - set(oi[1]))} -> {dict(set(oi[1]) - set(bi[1]))}")
            for k, f in enumerate(NONADMIN):
                c[f] += bi[2][k] != oi[2][k]
            c["addr"] += bi[2][0] != oi[2][0] or bi[2][1] != oi[2][1]
        res.setdefault(n, {})[p] = {"n": len(b), **{k: c[k] for k in ("json", "immich", "admin", "addr") + NONADMIN},
                                    "examples": ex}

if out_json:
    json.dump({"base": base, "res": res}, open(out_json, "w"), indent=1)
print(f"base {base}")
print("| config | pool | n | JSON changed | Immich (city/state/country) changed | admin fields changed | road or house_number | road | house_number | postcode | landmark |")
print("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
for n in names:
    for p in pools:
        r = res[n][p]
        N = r["n"]
        pct = lambda k: f"{r[k] / N:.3%} ({r[k]:,})"
        print(f"| {n} | {p} | {N:,} | {pct('json')} | {pct('immich')} | {pct('admin')} | {pct('addr')} | {pct('road')} | "
              f"{pct('house_number')} | {pct('postcode')} | {pct('landmark')} |")
