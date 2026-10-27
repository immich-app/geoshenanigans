#!/usr/bin/env python3
"""timing_table.py: rg_timing.sh results as a markdown table (per-point fastest of 3 passes, µs),
with the admin ring vertices read per find_admin from count/ (rg_count, p50k / L2)."""
import json
import os

R = "/ssd/search-prod/rg/"
SETS = ["orig-minimal", "orig-recommended", "orig-max", "t2-q25-smallest", "t2-uni75t8rc10-smallest", "t2-q25-recommended",
        "t2-uni75t8rc10-recommended"]


def load(p):
    try:
        return json.loads(open(p).read().strip().splitlines()[-1])
    except (FileNotFoundError, IndexError, ValueError):
        return None


print("| set | pool | call | main p50 / p99 / mean µs | 8dbb58d p50 / p99 / mean | head p50 / p99 / mean | mean main→8dbb58d | mean main→head | ring vertices read per find_admin main / 8dbb58d / head |")
print("|---|---|---|---|---|---|---:|---:|---|")
for s in SETS:
    for pool in ("p50k", "L2"):
        verts = []
        for r in ("main", "pip2", "head"):
            c = load(R + f"count/{s}.{r}.{pool}.json")
            verts.append(f"{c['find_admin_vertices']:,.0f}" if c else "-")
        for mode in ("admin", "query"):
            d = {r: load(R + f"timing/{s}.{pool}.{mode}.{r}.json") for r in ("main", "pip2", "head")}
            if not all(d.values()):
                continue
            cell = lambda x: f"{x['p50_us']:.0f} / {x['p99_us']:.0f} / {x['mean_us']:.0f}"
            m = d["main"]["mean_us"]
            print(f"| {s} | {pool} | {'find_admin' if mode == 'admin' else 'query_json'} | {cell(d['main'])} | {cell(d['pip2'])} | {cell(d['head'])} | "
                  f"{m / d['pip2']['mean_us']:.2f}x | {m / d['head']['mean_us']:.2f}x | {' / '.join(verts) if mode == 'admin' else ''} |")
