#!/usr/bin/env python3
"""photo2.py <planet dir> <n> <seed> : photo-like points as tiers2-verify photo.py (35% major-tier POIs
jittered +-0.0027 deg, 35% place nodes +-0.0027 deg, 30% place nodes +-0.03 deg) from any build's
planet dir. Output "lat lng kind" per line."""
import mmap
import random
import struct
import sys

P, n, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
rng = random.Random(seed)


def sample(path, size, k, j, kind):
    with open(path, "rb") as f:
        m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    count = len(m) // size
    out = []
    while len(out) < k:
        lat, lng = struct.unpack_from("<ff", m, rng.randrange(count) * size)
        if lat == 0 and lng == 0:
            continue
        out.append((lat + rng.uniform(-j, j), lng + rng.uniform(-j, j), kind))
    return out


a, b = int(n * 0.35), int(n * 0.35)
pts = (sample(f"{P}/poi/major/poi_records.bin", 36, a, 0.0027, "poi")
       + sample(f"{P}/admin/place_nodes.bin", 20, b, 0.0027, "place")
       + sample(f"{P}/admin/place_nodes.bin", 20, n - a - b, 0.03, "rural"))
rng.shuffle(pts)
for lat, lng, kind in pts:
    lat = max(-89.9, min(89.9, lat))
    lng = ((lng + 180) % 360) - 180
    print(f"{lat:.6f} {lng:.6f} {kind}")
