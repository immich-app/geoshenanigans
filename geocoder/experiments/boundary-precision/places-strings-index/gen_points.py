#!/usr/bin/env python3
"""Sample photo-like query points: N random admin-mode place nodes and N
random tier-1 (major) POIs, each jittered by up to ~300 m. Deterministic seed.

usage: gen_points.py N > points.txt
"""
import mmap, random, struct, sys

IDX = '/ssd/shrink/idx/recommended'
N = int(sys.argv[1])
rng = random.Random(20261013)


def sample(path, size, n):
    with open(path, 'rb') as f:
        m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    count = len(m) // size
    for i in rng.sample(range(count), n):
        lat, lng = struct.unpack_from('<ff', m, i * size)
        yield lat + rng.uniform(-0.0027, 0.0027), lng + rng.uniform(-0.0027, 0.0027)


for lat, lng in sample(f'{IDX}/place_nodes.bin', 20, N):
    print(f'{lat:.6f} {lng:.6f}')
for lat, lng in sample(f'{IDX}/poi_records.bin', 36, N):
    print(f'{lat:.6f} {lng:.6f}')
