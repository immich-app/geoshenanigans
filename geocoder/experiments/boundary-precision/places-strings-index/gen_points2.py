#!/usr/bin/env python3
"""200k photo-like query points, deterministic:
  70k random major-tier POIs (recommended set) jittered +-0.0027 deg (~300 m)
  70k random place nodes of ALL types (recommended set) jittered +-0.0027 deg
  60k random place nodes jittered +-0.03 deg (~3 km, rural / between settlements)
usage: gen_points2.py > points200k.txt"""
import mmap, random, struct
IDX = "/ssd/shrink/idx/recommended"
rng = random.Random(20261013)
def sample(path, size, n, j):
    with open(path, "rb") as f:
        m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    count = len(m) // size
    out = []
    while len(out) < n:
        i = rng.randrange(count)
        lat, lng = struct.unpack_from("<ff", m, i * size)
        if lat == 0 and lng == 0: continue
        out.append((lat + rng.uniform(-j, j), lng + rng.uniform(-j, j)))
    return out
pts = sample(f"{IDX}/poi_records.bin", 36, 70000, 0.0027) + sample(f"{IDX}/place_nodes.bin", 20, 70000, 0.0027) + sample(f"{IDX}/place_nodes.bin", 20, 60000, 0.03)
for lat, lng in pts:
    lat = max(-89.9, min(89.9, lat)); lng = ((lng + 180) % 360) - 180
    print(f"{lat:.6f} {lng:.6f}")
