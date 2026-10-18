#!/usr/bin/env python3
"""Answer-check points for the geo index / POI / strings track, deterministic (seed 20261013):
  20k random addr_points jittered +-30 m (lat/lng +-0.00027 deg)
  10k random street nodes jittered +-30 m
  10k random interpolation nodes jittered +-30 m
  15k random POIs (all tier) jittered +-300 m
  10k random polygon POIs' first vertex jittered +-20 m (polygon border / inside behaviour)
usage: gen_points.py > pts_track.txt"""
import mmap
import random
import struct

IDX = "/ssd/shrink/idx/max"
rng = random.Random(20261013)


def mm(name):
    with open(f"{IDX}/{name}", "rb") as f:
        return mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)


def sample(name, size, n, j, off=0):
    m = mm(name)
    count = len(m) // size
    out = []
    while len(out) < n:
        i = rng.randrange(count)
        lat, lng = struct.unpack_from("<ff", m, i * size + off)
        if lat == 0 and lng == 0:
            continue
        out.append((lat + rng.uniform(-j, j), lng + rng.uniform(-j, j)))
    return out


def poly_poi_points(n, j):
    recs = mm("poi_records.bin")
    verts = mm("poi_vertices.bin")
    count = len(recs) // 36
    out = []
    scale = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}
    while len(out) < n:
        i = rng.randrange(count)
        vo, vc = struct.unpack_from("<II", recs, i * 36 + 8)
        if vc == 0 or vo == 0xFFFFFFFF:
            continue
        enc = verts[vo]
        bl, bg = struct.unpack_from("<ff", verts, vo + 2)
        k = rng.randrange(vc)
        if enc == 3:
            ql, qg = struct.unpack_from("<II", verts, vo + 10 + 8 * k)
        else:
            ql, qg = struct.unpack_from("<HH", verts, vo + 10 + 4 * k)
        lat, lng = bl + ql * scale[enc], bg + qg * scale[enc]
        out.append((lat + rng.uniform(-j, j), lng + rng.uniform(-j, j)))
    return out


pts = (sample("addr_points.bin", 28, 20000, 0.00027)
       + sample("street_nodes.bin", 8, 10000, 0.00027)
       + sample("interp_nodes.bin", 8, 10000, 0.00027)
       + sample("poi_records.bin", 36, 15000, 0.0027)
       + poly_poi_points(10000, 0.00018))
for lat, lng in pts:
    lat = max(-89.9, min(89.9, lat))
    lng = ((lng + 180) % 360) - 180
    print(f"{lat:.6f} {lng:.6f}")
