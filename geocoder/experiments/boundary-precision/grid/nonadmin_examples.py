"""Examples where road / house_number / postcode / landmark differ between two configurations
that share every non-admin file (e.g. T vs unc_g4: same postal_* files, only admin_* differ).
usage: nonadmin_examples.py <base> <other> [max per field]"""
import json, sys
POOLS = ["places", "random", "L2", "L4", "L8", "p200k"]
base, other = sys.argv[1], sys.argv[2]
M = int(sys.argv[3]) if len(sys.argv) > 3 else 4
for f in ("road", "house_number", "postcode", "landmark"):
    shown = 0
    tot = 0
    for p in POOLS:
        b = open(f"/ssd/shrink/grid/q/{base}.{p}.jsonl").read().splitlines()
        o = open(f"/ssd/shrink/grid/q/{other}.{p}.jsonl").read().splitlines()
        for x, y in zip(b, o):
            if x == y:
                continue
            jb = json.loads(x.split(" ", 2)[2]); jo = json.loads(y.split(" ", 2)[2])
            ab, ao = jb.get("address", {}), jo.get("address", {})
            if ab.get(f) == ao.get(f):
                continue
            tot += 1
            if shown < M:
                shown += 1
                keys = sorted(set(ab) | set(ao))
                diff = {k: (ab.get(k), ao.get(k)) for k in keys if ab.get(k) != ao.get(k)}
                print(f"[{f}] {p} {x.split(' ', 2)[0]} {x.split(' ', 2)[1]}: {diff}")
    print(f"[{f}] total differing points over all pools: {tot}")
