#!/usr/bin/env python3
"""Dense grids over Manhattan (Weehawken effect) and inner London (borough vs 'Greater London'),
plus central Tokyo (prefecture): today's GeoNames answer vs the geocoder's, tallied."""
from collections import Counter

from compare import (GEO, CityIndex, immich_geonames, load_admin, load_cities, load_country_names, load_ne,
                     map_geocoder, run_grid_query)

admin1 = load_admin(GEO / "admin1CodesASCII.txt")
a2, a3 = load_country_names()
idx = CityIndex(load_cities(admin1))
rings = load_ne()

GRIDS = {
    # name: (lat0, lat1, lon0, lon1, steps)
    "Manhattan below 96th St (land+river)": (40.700, 40.790, -74.015, -73.935, 20),
    "Inner London": (51.45, 51.56, -0.25, 0.05, 20),
    "Central Tokyo (23 wards)": (35.60, 35.75, 139.65, 139.85, 15),
}
for name, (la0, la1, lo0, lo1, k) in GRIDS.items():
    samples = [("grid", name, round(la0 + (la1 - la0) * i / (k - 1), 5), round(lo0 + (lo1 - lo0) * j / (k - 1), 5))
               for i in range(k) for j in range(k)]
    gq = run_grid_query(samples)
    old = Counter()
    new = Counter()
    pair = Counter()
    for (_, _, lat, lon), g in zip(samples, gq):
        o = immich_geonames(lat, lon, idx, rings, a2, a3)
        nw = map_geocoder(g.get("address") or {}, a2)
        old[(o["city"], o["state"])] += 1
        new[(nw["city"], nw["state"])] += 1
        pair[(o["state"], nw["state"])] += 1
    print(f"## {name}: {len(samples)} grid points")
    print("  GeoNames (city, state) top:", old.most_common(12))
    print("  geocoder (city, state) top:", new.most_common(12))
    print("  (GeoNames state, geocoder state):", pair.most_common(6))
