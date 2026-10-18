#!/usr/bin/env python3
"""Natural Earth 10m arbiter for the country mismatches in comparison_land.jsonl (random_land.py)."""
import json
from compare import HERE, load_country_names, load_ne, pip

rings = load_ne()
a2, a3 = load_country_names()
rows = [json.loads(line) for line in open(HERE / "comparison_land.jsonl", encoding="utf-8")]
agree = tot = 0
for r in rows:
    if r["old"]["country"] == r["new"]["country"]:
        continue
    ne = next((a3.get(c, c) for c, ring, (x0, y0, x1, y1) in rings
               if x0 <= r["lon"] <= x1 and y0 <= r["lat"] <= y1 and pip(r["lon"], r["lat"], ring)), None)
    tot += 1
    agree += ne == r["new"]["country"]
    print(r["lat"], r["lon"], "GeoNames", r["old"]["country"], "geocoder", r["new"]["country"], "NE", ne)
print("geocoder agrees with NE:", agree, "of", tot)
