#!/usr/bin/env python3
"""planned.py <label>... : planned (layout Bg) zstd MB of each set's 4 admin files = Bg admin_vertices zstd
(cmp/bg_<label>.tsv) + zstd of the other 3 files (cmp/size_<label>.tsv), as tiers2 sizes.py; delta vs the
first label. MB = 1e6 bytes."""
import sys

T = "/ssd/tiers3/cmp"
OTHER = {"min": ["min/admin_polygons.bin", "min/admin_cells.bin", "min/admin_entries.bin"],
         "q": ["q/admin_polygons.bin", "q/postal_vertices.bin", "q/postal_polygons.bin"]}
rows = []
for lab in sys.argv[1:]:
    bg = {l.split("\t")[0]: int(l.split("\t")[2]) for l in open(f"{T}/bg_{lab}.tsv")}
    sz = {l.split("\t")[0]: int(l.split("\t")[2]) for l in open(f"{T}/size_{lab}.tsv")}
    rows.append((lab, {p: bg[p] + sum(sz[f] for f in OTHER[p]) for p in ("min", "q")}))
ref = rows[0][1]
for lab, v in rows:
    print(f"{lab:<8} admin-minimal planned {v['min'] / 1e6:6.1f} MB ({(v['min'] / ref['min'] - 1) * 100:+.1f}%)   "
          f"quality dir planned {v['q'] / 1e6:6.1f} MB ({(v['q'] / ref['q'] - 1) * 100:+.1f}%)")
