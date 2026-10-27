#!/usr/bin/env python3
"""compare.py: reverse-geocode answer differences between grid_query outputs (eq_run.sh), per
comparison and per point pool; the address fields that differ; a few examples of each pair."""
import collections
import json
import sys

R = "/ssd/search-prod/rg/out/"
POOLS = [("L2 +-200m", 20000), ("L4 +-200m", 20000), ("L8 +-200m", 20000), ("pts_40k", 40000), ("pts_grid", 10790),
         ("edge2k", 14999), ("long15k", 4992)]
PAIRS = [(f"orig-{s}.main", f"orig-{s}.head") for s in ("minimal", "recommended", "max")]
PAIRS += [(f"orig-{s}.main", f"emu-{s}.head") for s in ("minimal", "recommended", "max")]
PAIRS += [(f"orig-{s}.main", f"emu-{s}.main") for s in ("minimal", "recommended", "max")]
PAIRS += [(f"t2-{v}-{st}.main", f"t2-{v}-{st}.head") for v in ("q25", "uni75t8rc10") for st in ("smallest", "recommended")]
if len(sys.argv) > 1:
    PAIRS = [tuple(a.split(",")) for a in sys.argv[1:]]
pool_of = []
for name, n in POOLS:
    pool_of += [name] * n


def fields(line):
    d = json.loads(line.split(" ", 2)[2])
    out = {f"address.{k}": v for k, v in (d.get("address") or {}).items()}
    for k, v in d.items():
        if k != "address":
            out[k] = v
    return out


print("| answers A | answers B | points | identical | differ | differ by pool | fields that differ (points) |")
print("|---|---|---:|---:|---:|---|---|")
examples = {}
for a, b in PAIRS:
    la = open(R + a + ".jsonl").read().splitlines()
    lb = open(R + b + ".jsonl").read().splitlines()
    assert len(la) == len(lb) == len(pool_of), (a, b, len(la), len(lb), len(pool_of))
    by_pool = collections.Counter()
    by_field = collections.Counter()
    ex = []
    for i, (x, y) in enumerate(zip(la, lb)):
        if x == y:
            continue
        by_pool[pool_of[i]] += 1
        fx, fy = fields(x), fields(y)
        diff = sorted(k for k in set(fx) | set(fy) if fx.get(k) != fy.get(k))
        by_field.update(diff)
        if len(ex) < 6:
            ex.append((pool_of[i], x.split(" ", 2)[:2], {k: (fx.get(k), fy.get(k)) for k in diff if k != "places"}))
    nd = sum(by_pool.values())
    pools = ", ".join(f"{k} {v}" for k, v in by_pool.most_common()) or "-"
    flds = ", ".join(f"{k} {v}" for k, v in by_field.most_common(6)) or "-"
    print(f"| {a} | {b} | {len(la)} | {len(la) - nd} | {nd} | {pools} | {flds} |")
    examples[(a, b)] = ex
print()
for (a, b), ex in examples.items():
    if not ex:
        continue
    print(f"### {a} vs {b}")
    for pool, ll, d in ex:
        print(f"- [{pool}] {ll[0]} {ll[1]}: " + "; ".join(f"{k}: {u!r} -> {v!r}" for k, (u, v) in d.items())[:400])
    print()
