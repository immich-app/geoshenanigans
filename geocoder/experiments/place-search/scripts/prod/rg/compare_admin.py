#!/usr/bin/env python3
"""compare_admin.py: find_admin answers (admin_dump) of main vs pip2 (8dbb58d) vs head (66a5ad0) on the
1,199,563 near-edge points; compares the common prefix when a run is incomplete. Per set: points,
differing points by pool (edge +-1.5 km / long edges +-15 km), fields that differ, examples."""
import collections
import os

R = "/ssd/search-prod/rg/"
N_EDGE = 999845
FIELDS = ["country_code", "country", "state", "province", "region", "state_district", "county", "district", "municipality", "city",
          "town", "village", "hamlet", "city_district", "borough", "suburb", "quarter", "neighbourhood", "city_block"]
SETS = ["orig-minimal", "orig-recommended", "orig-max", "t2-q25-smallest", "t2-q25-recommended", "t2-uni75t8rc10-smallest",
        "t2-uni75t8rc10-recommended"]
pts = open(R + "pts/edge2k_1000000_200000.txt").read().splitlines() + open(R + "pts/long15k_1000000_200000.txt").read().splitlines()
print("| set | readers | points compared | differ | edge +-1.5 km | long edges +-15 km | fields (points) |")
print("|---|---|---:|---:|---:|---:|---|")
ex = {}
for s in SETS:
    runs = {r: R + f"out/{s}.{r}.admin" for r in ("main", "pip2", "head")}
    lines = {r: open(p).read().splitlines() for r, p in runs.items() if os.path.exists(p)}
    for a, b in (("main", "pip2"), ("main", "head")):
        if a not in lines or b not in lines:
            continue
        n = min(len(lines[a]), len(lines[b]))
        # an unfinished run's last line can be partial
        if n < len(pts):
            n -= 1
        d = [i for i in range(n) if lines[a][i] != lines[b][i]]
        flds = collections.Counter()
        for i in d:
            x, y = lines[a][i].split("|"), lines[b][i].split("|")
            flds.update(FIELDS[k] for k in range(len(FIELDS)) if x[k] != y[k])
        e = sum(1 for i in d if i < N_EDGE)
        print(f"| {s} | {a} vs {b} | {n} | {len(d)} | {e} | {len(d) - e} | {', '.join(f'{k} {v}' for k, v in flds.most_common(5)) or '-'} |")
        ex[(s, a, b)] = [(pts[i], lines[a][i], lines[b][i]) for i in d[:3]]
print()
for (s, a, b), rows in ex.items():
    for p, x, y in rows:
        xs, ys = x.split("|"), y.split("|")
        diff = "; ".join(f"{FIELDS[k]}: {xs[k]} -> {ys[k]}" for k in range(len(FIELDS)) if xs[k] != ys[k])
        print(f"- {s} {a} vs {b} @ {p}: {diff}")
