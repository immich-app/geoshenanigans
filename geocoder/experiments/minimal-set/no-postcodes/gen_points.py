#!/usr/bin/env python3
"""Query points: the named landmarks, then N random admin-mode place nodes and
N random major POIs (each jittered up to ~300 m, photo-like), then M uniform
random points in lat [-56, 72]. Deterministic seed.
usage: gen_points.py N M > points.txt
"""
import mmap, random, struct, sys

IDX = "/ssd/shrink/idx/recommended"
N, M = int(sys.argv[1]), int(sys.argv[2])
rng = random.Random(20261013)
NAMED = [(48.8584, 2.2945), (40.758, -73.9855), (40.769, -74.02), (51.93, 6.76),
         (35.658, 139.7016), (51.47, -0.488), (-25.3444, 131.0369)]
for lat, lng in NAMED:
    print(f"{lat:.6f} {lng:.6f}")


def sample(path, size, n):
    with open(path, "rb") as f:
        m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    count = len(m) // size
    for i in rng.sample(range(count), n):
        lat, lng = struct.unpack_from("<ff", m, i * size)
        yield lat + rng.uniform(-0.0027, 0.0027), lng + rng.uniform(-0.0027, 0.0027)


for lat, lng in sample(f"{IDX}/place_nodes.bin", 20, N):
    print(f"{lat:.6f} {lng:.6f}")
for lat, lng in sample(f"{IDX}/poi_records.bin", 36, N):
    print(f"{lat:.6f} {lng:.6f}")
for _ in range(M):
    print(f"{rng.uniform(-56, 72):.6f} {rng.uniform(-180, 180):.6f}")
