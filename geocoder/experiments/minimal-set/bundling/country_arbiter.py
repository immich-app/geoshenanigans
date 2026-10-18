#!/usr/bin/env python3
"""For every country mismatch (A+B), ask Natural Earth 10m (independent polygons) which country
contains the point, to split geocoder country changes into territory-policy vs location fixes."""
import json
from pathlib import Path
from compare import load_ne, pip, load_country_names, HERE

TERRITORY = {"AS", "GU", "MP", "PR", "VI", "UM", "HK", "MO", "AW", "CW", "SX", "BQ", "GF", "GP", "MQ", "RE", "YT",
             "PF", "NC", "WF", "PM", "BL", "MF", "TF", "AX", "SJ", "CC", "CX", "NF", "HM", "IO", "GL", "FO", "GI", "BM",
             "KY", "TC", "VG", "MS", "AI", "FK", "SH", "PN", "GG", "JE", "IM", "TK", "NU", "CK"}
rings = load_ne()
a2, a3 = load_country_names()
rows = [json.loads(l) for l in open(HERE / "comparison.jsonl", encoding="utf-8")]
ab = [r for r in rows if r["group"] in ("A_strat", "B_popw")]
mm = [r for r in ab if r["old"]["country"] != r["new"]["country"]]
cls = {}
for r in mm:
    ne = None
    for code, ring, (x0, y0, x1, y1) in rings:
        if x0 <= r["lon"] <= x1 and y0 <= r["lat"] <= y1 and pip(r["lon"], r["lat"], ring):
            ne = a3.get(code, code)
            break
    occ = r["old"].get("cc")
    if r["new"]["country"] is None:
        k = "geocoder: no country"
    elif occ in TERRITORY and r["new"]["cc"] != occ:
        k = "territory reported as parent country"
    elif ne == r["new"]["country"]:
        k = "geocoder agrees with Natural Earth (location fix)"
    else:
        k = "other"
    cls.setdefault(k, []).append((r["label"], r["old"]["country"], r["new"]["country"], ne))
print(f"country mismatches A+B: {len(mm)} of {len(ab)}")
for k, v in cls.items():
    print(f"  {k}: {len(v)}")
    for x in v[:12]:
        print(f"     {x}")
