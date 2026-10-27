#!/usr/bin/env python3
"""Examples per category where one system is top-1 and the other is not.
  examples.py <per_query.json> [per category]"""
import json
import sys

per = json.load(open(sys.argv[1]))
k = int(sys.argv[2]) if len(sys.argv) > 2 else 3


def fmt(rs):
    out = []
    for r in rs[:2]:
        ctx = ", ".join(x for x in (r.get("admin1"), r.get("admin2")) if x)
        out.append(f"{r['name']} ({ctx})")
    return "; ".join(out) or "(none)"


for who, mine, other in (("OURS", "ours_rank", "base_rank"), ("GEONAMES", "base_rank", "ours_rank")):
    seen = {}
    for p in per:
        if p[mine] == 1 and p[other] != 1 and seen.get(p["cat"], 0) < k:
            seen[p["cat"]] = seen.get(p["cat"], 0) + 1
            print(f"{who}\t{p['cat']}\t{p['q']}\tours r={p['ours_rank']}: {fmt(p['ours'])}\tGN r={p['base_rank']}: {fmt(p['base'])}")
