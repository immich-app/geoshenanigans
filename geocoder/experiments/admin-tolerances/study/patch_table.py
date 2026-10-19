#!/usr/bin/env python3
"""patch_table.py <name>... : daily patch D -> D1 per scheme from cmp/patch_<name>.tsv (dir, bytes, diff rc,
verify verdict) -> markdown rows: admin-minimal and quality/q2.5 patch KiB, verify, and the D1 admin
file sizes (raw) the patch updates."""
import os
import sys

print("| scheme | admin-minimal patch KiB | verify | quality/q2.5 patch KiB | verify |")
print("|---|---|---|---|---|")
for name in sys.argv[1:]:
    p = f"/ssd/tiers3/cmp/patch_{name}.tsv"
    if not os.path.exists(p):
        continue
    row = {}
    for line in open(p):
        f = line.rstrip("\n").split("\t")
        row[f[0]] = (int(f[1]) / 1024, f[3] if len(f) > 3 else "")
    m, q = row.get("min", (0, "")), row.get("q", (0, ""))
    print(f"| {name} | {m[0]:.1f} | {m[1]} | {q[0]:.1f} | {q[1]} |")
