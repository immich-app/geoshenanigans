#!/usr/bin/env python3
"""cmp_q.py TAGS A.jsonl B.jsonl [EXAMPLES]: per query class, how many answers differ between two runs.

Lines correspond by position (same input). Fields: road, house_number, postcode (address object), any
address field, and the whole JSON line. Prints a markdown table and up to EXAMPLES differing road /
house-number examples.
"""
import json
import sys
from collections import Counter, defaultdict

tags = open(sys.argv[1]).read().split()
a_path, b_path = sys.argv[2], sys.argv[3]
nex = int(sys.argv[4]) if len(sys.argv) > 4 else 0
FIELDS = ["road", "house_number", "postcode"]
n = Counter()
diff = defaultdict(Counter)
ex = []
with open(a_path) as fa, open(b_path) as fb:
    for i, (la, lb) in enumerate(zip(fa, fb)):
        t = tags[i]
        n[t] += 1
        n["all"] += 1
        if la == lb:
            continue
        pa, pb = la.split(" ", 2), lb.split(" ", 2)
        if pa[:2] != pb[:2]:
            raise SystemExit(f"line {i}: coordinates differ")
        ja, jb = json.loads(pa[2]), json.loads(pb[2])
        aa, ab = ja.get("address", {}), jb.get("address", {})
        kinds = ["json"]
        if aa != ab:
            kinds.append("address")
        for f in FIELDS:
            if aa.get(f) != ab.get(f):
                kinds.append(f)
        for k in kinds:
            diff[t][k] += 1
            diff["all"][k] += 1
        if len(ex) < nex and ("road" in kinds or "house_number" in kinds):
            ex.append((t, pa[0], pa[1], {f: aa.get(f) for f in FIELDS}, {f: ab.get(f) for f in FIELDS}))
cols = ["json", "address"] + FIELDS
print("| class | queries | " + " | ".join(f"{c} differs" for c in cols) + " |")
print("|---|---:|" + "---:|" * len(cols))
for t in list(dict.fromkeys(tags)) + ["all"]:
    cells = [f"{diff[t][c]:,} ({100.0 * diff[t][c] / n[t]:.3f}%)" for c in cols]
    print(f"| {t} | {n[t]:,} | " + " | ".join(cells) + " |")
for e in ex:
    print("example", *e)
