#!/usr/bin/env python3
"""Group E: uniformly random points on land (inside a Natural Earth country ring, Antarctica excluded),
to see how often today's 25 km nearest-cities500 lookup falls back to country-only, and what the
geocoder returns there. Writes comparison_land.jsonl and prints a summary."""
import json
import math
import random
from collections import Counter

from compare import (GEO, CityIndex, immich_geonames, load_admin, load_cities, load_country_names, load_ne,
                     map_geocoder, pip, run_grid_query, HERE)

rng = random.Random(7)
admin1 = load_admin(GEO / "admin1CodesASCII.txt")
a2, a3 = load_country_names()
idx = CityIndex(load_cities(admin1))
rings = load_ne()
samples = []
while len(samples) < 400:
    lat = math.degrees(math.asin(rng.uniform(-1, 1)))
    lon = rng.uniform(-180, 180)
    if lat < -60:
        continue
    if any(x0 <= lon <= x1 and y0 <= lat <= y1 and pip(lon, lat, ring) for _, ring, (x0, y0, x1, y1) in rings):
        samples.append(("E_land", "random land", round(lat, 5), round(lon, 5)))
gq = run_grid_query(samples)
rows = []
for (group, label, lat, lon), g in zip(samples, gq):
    addr = g.get("address") or {}
    rows.append({"group": group, "label": label, "lat": lat, "lon": lon,
                 "old": immich_geonames(lat, lon, idx, rings, a2, a3), "new": map_geocoder(addr, a2), "addr": addr})
with open(HERE / "comparison_land.jsonl", "w", encoding="utf-8") as f:
    for r in rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")
n = len(rows)
print(f"random land points: {n}")
print("GeoNames source:", Counter(r["old"]["source"] for r in rows).most_common())
for f in ("country", "state", "city"):
    print(f"{f}: GeoNames non-null {sum(1 for r in rows if r['old'][f])}, geocoder non-null {sum(1 for r in rows if r['new'][f])}, "
          f"equal {sum(1 for r in rows if r['old'][f] == r['new'][f])}")
fb = [r for r in rows if r["old"]["source"] != "cities500"]
print(f"country-only fallback rows: {len(fb)}; of those geocoder gives state {sum(1 for r in fb if r['new']['state'])}, "
      f"city {sum(1 for r in fb if r['new']['city'])}, country {sum(1 for r in fb if r['new']['country'])}; "
      f"country equal {sum(1 for r in fb if r['new']['country'] == r['old']['country'])}")
cm = [r for r in rows if r["old"]["country"] != r["new"]["country"]]
print(f"country mismatches: {len(cm)}")
for r in cm[:25]:
    print(f"  ({r['lat']},{r['lon']}) GeoNames={r['old']['country']!r} [{r['old']['source']}] geocoder={r['new']['country']!r} addr={json.dumps(r['addr'], ensure_ascii=False)[:160]}")
