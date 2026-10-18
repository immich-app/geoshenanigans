#!/usr/bin/env python3
"""Capitals / city-states: today's GeoNames (city | state) vs the geocoder's, with the raw address keys,
to see where the geocoder leaves state empty."""
import json

from compare import (GEO, CityIndex, immich_geonames, load_admin, load_cities, load_country_names, load_ne,
                     map_geocoder, run_grid_query)

PTS = [
    ("Hamburg", 53.5511, 9.9937), ("Bremen", 53.0793, 8.8017), ("Vienna", 48.2082, 16.3738),
    ("Shanghai", 31.2304, 121.4737), ("Tianjin", 39.1255, 117.1901), ("Chongqing", 29.5630, 106.5516),
    ("Busan", 35.1796, 129.0756), ("Saint Petersburg", 59.9343, 30.3351), ("Prague", 50.0755, 14.4378),
    ("Bucharest", 44.4268, 26.1025), ("Washington DC", 38.8977, -77.0365), ("Madrid", 40.4168, -3.7038),
    ("Kuala Lumpur", 3.1390, 101.6869), ("Jakarta", -6.2088, 106.8456), ("Bangkok", 13.7563, 100.5018),
    ("Hanoi", 21.0278, 105.8342), ("Lisbon", 38.7223, -9.1393), ("Oslo", 59.9139, 10.7522),
    ("Stockholm", 59.3293, 18.0686), ("Copenhagen", 55.6761, 12.5683), ("Dublin", 53.3498, -6.2603),
    ("Budapest", 47.4979, 19.0402), ("Warsaw", 52.2297, 21.0122), ("Rome", 41.9028, 12.4964),
    ("Athens", 37.9838, 23.7275), ("Zagreb", 45.8150, 15.9819), ("Riga", 56.9496, 24.1052),
    ("Tallinn", 59.4370, 24.7536), ("Helsinki", 60.1699, 24.9384), ("Bern", 46.9480, 7.4474),
    ("Edinburgh", 55.9533, -3.1883), ("Cardiff", 51.4816, -3.1791), ("Belfast", 54.5973, -5.9301),
    ("Manchester", 53.4808, -2.2426), ("Toronto", 43.6532, -79.3832), ("Vancouver", 49.2827, -123.1207),
    ("Los Angeles", 34.0522, -118.2437), ("San Francisco", 37.7749, -122.4194), ("Chicago", 41.8781, -87.6298),
    ("Honolulu", 21.3069, -157.8583), ("Melbourne", -37.8136, 144.9631), ("Auckland", -36.8485, 174.7633),
    ("Rio de Janeiro", -22.9068, -43.1729), ("Lima", -12.0464, -77.0428), ("Bogota", 4.7110, -74.0721),
    ("Nairobi", -1.2921, 36.8219), ("Johannesburg", -26.2041, 28.0473), ("Marrakesh", 31.6295, -7.9811),
    ("Tel Aviv", 32.0853, 34.7818), ("Bangalore", 12.9716, 77.5946), ("Kathmandu", 27.7172, 85.3240),
    ("Manila", 14.5995, 120.9842), ("Ho Chi Minh City", 10.8231, 106.6297), ("Barcelona", 41.3851, 2.1734),
    ("Munich", 48.1351, 11.5820), ("Florence", 43.7696, 11.2558), ("Venice", 45.4408, 12.3155),
    ("Nice", 43.7102, 7.2620), ("Lyon", 45.7640, 4.8357), ("Kyiv", 50.4501, 30.5234), ("Istanbul", 41.0082, 28.9784),
]
admin1 = load_admin(GEO / "admin1CodesASCII.txt")
a2, a3 = load_country_names()
idx = CityIndex(load_cities(admin1))
rings = load_ne()
samples = [("cap", n, la, lo) for n, la, lo in PTS]
gq = run_grid_query(samples)
null_state = 0
for (_, n, la, lo), g in zip(samples, gq):
    addr = g.get("address") or {}
    o = immich_geonames(la, lo, idx, rings, a2, a3)
    nw = map_geocoder(addr, a2)
    null_state += nw["state"] is None
    flag = "STATE-NULL " if nw["state"] is None else ""
    print(f"{flag}{n}: GeoNames=({o['city']} | {o['state']} | {o['country']}) geocoder=({nw['city']} | {nw['state']} | {nw['country']}) "
          f"keys={json.dumps({k: v for k, v in addr.items() if k not in ('country', 'country_code')}, ensure_ascii=False)}")
print(f"geocoder state null: {null_state} of {len(PTS)}")
