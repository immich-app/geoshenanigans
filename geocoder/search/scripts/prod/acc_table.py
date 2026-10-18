#!/usr/bin/env python3
"""Accuracy table across result dirs: top-1 / top-5 per category, plus ALL and ALL without the
categories a smaller tier deliberately lacks (nonenglish: other-language names; landmark: POIs)."""
import collections
import json
import sys

R = "/ssd/search-prod/results/"
runs = sys.argv[1:]
data = {r: json.load(open(R + r + "/per_query.json")) for r in runs}
first = data[runs[0]]
cats = list(collections.OrderedDict((p["grp"], 1) for p in first))
rows = cats + ["ALL", "ALL excl. nonenglish+landmark"]


def score(ps, key):
    n = len(ps)
    t1 = sum(1 for p in ps if p[key] is not None and p[key] <= 1)
    t5 = sum(1 for p in ps if p[key] is not None and p[key] <= 5)
    return f"{100 * t1 / n:.0f} / {100 * t5 / n:.0f}"


def sel(ps, row):
    if row == "ALL":
        return ps
    if row.startswith("ALL excl"):
        return [p for p in ps if p["grp"] not in ("nonenglish", "landmark")]
    return [p for p in ps if p["grp"] == row]


print("| category | n | GeoNames | prototype | " + " | ".join(runs) + " |")
print("|---|---:|---:|---:|" + "---:|" * len(runs))
for row in rows:
    ps0 = sel(first, row)
    cells = [score(ps0, "base_rank"), score(ps0, "proto_rank")] + [score(sel(data[r], row), "rank") for r in runs]
    print(f"| {row} | {len(ps0)} | " + " | ".join(cells) + " |")
