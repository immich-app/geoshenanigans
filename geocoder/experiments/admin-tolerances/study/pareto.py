#!/usr/bin/env python3
"""pareto.py <prefix> <label>... : measured Pareto set over (admin-minimal raw bytes, country B2, state B4,
city B7, city B8, photo city) of the labels <prefix><label> (score_*.json, size_*.tsv, smallest set);
marks each label Pareto (P) or dominated (by whom)."""
import json
import sys

pre, labs = sys.argv[1], sys.argv[2:]
T = "/ssd/tiers3/cmp"
rows = {}
for lab in labs:
    acc = json.load(open(f"{T}/score_{pre}{lab}.json"))["smallest"]["acc"]
    raw = sum(int(l.split("\t")[1]) for l in open(f"{T}/size_{pre}{lab}.tsv") if l.startswith("min/"))
    pct = lambda p, f: 100 * acc[p].get(f, 0) / acc[p]["n"]  # noqa: E731
    rows[lab] = (raw / 1e6, pct("B2", "country"), pct("B4", "state"), pct("B7", "city"), pct("B8", "city"), pct("photo", "city"))
for lab, v in rows.items():
    dom = [o for o, w in rows.items() if o != lab and all(a <= b for a, b in zip(w, v)) and any(a < b for a, b in zip(w, v))]
    print(f"{lab:<6} raw {v[0]:6.1f}  cc {v[1]:5.2f}  st {v[2]:5.2f}  B7 {v[3]:4.2f}  B8 {v[4]:4.2f}  photo city {v[5]:.3f}  "
          f"{'P' if not dom else 'dominated by ' + ','.join(dom)}")
