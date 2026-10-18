#!/usr/bin/env python3
"""Boundary-stress points for the reverse-geocode equivalence run (seed 20261019), from the
planet-261006 q2.5 admin rings, levels 2-8 (the rings the recommended set ships):
  edge2k.txt   15,000 points within +-2 km of a ring edge (edge drawn by length), offset
               perpendicular, uniform in distance: covers the band where a q2.5 ring (tolerance
               up to 1,250 m at L2) leaves the original outline the S2 covering was built from
  long15k.txt   5,000 points within +-15 km of the middle third of edges longer than 50 km: where a
               planar ring edge and the geodesic edge S2 covers part by kilometres
Usage: gen_edge_pts.py <out dir> [n_edge n_long km_edge seed] (defaults 15000 5000 2 20261019; files get
the counts as a suffix when they are given)"""
import sys

import numpy as np

P = "/ssd/precision/build/out-nocap/planet/quality/q2.5/"
OUT = sys.argv[1].rstrip("/") + "/"
N_EDGE, N_LONG, KM_EDGE, SEED = (int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), int(sys.argv[5])) if len(sys.argv) > 5 else (15000, 5000, 2.0, 20261019)
SUFFIX = f"_{N_EDGE}_{N_LONG}" if len(sys.argv) > 5 else ""
KM_PER_DEG = 111.32
ap_dt = np.dtype([("voff", "<u4"), ("vcnt", "<u4"), ("name_id", "<u4"), ("level", "u1"), ("ovr", "u1"), ("p2", "u1"), ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
STRIDE = {0: 4, 1: 4, 2: 4, 3: 8, 4: 8}
SCALE = {0: 1e-5, 1: 1e-4, 2: 1e-6, 3: 1e-7}

rng = np.random.default_rng(SEED)
ap = np.fromfile(P + "admin_polygons.bin", dtype=ap_dt)
vb = np.memmap(P + "admin_vertices.bin", dtype=np.uint8, mode="r")
keep = np.nonzero((ap["level"] >= 2) & (ap["level"] <= 8) & (ap["area"] > 0) & (ap["vcnt"] >= 3))[0]

a_lat, a_lng, b_lat, b_lng = [], [], [], []
for i in keep:
    off, n = int(ap["voff"][i]), int(ap["vcnt"][i])
    enc = int(vb[off])
    h = np.frombuffer(vb[off + 2:off + 10].tobytes(), dtype="<f4").astype(np.float64)
    raw = vb[off + 10:off + 10 + n * STRIDE[enc]].tobytes()
    if enc == 4:
        v = np.frombuffer(raw, dtype="<f4").reshape(n, 2).astype(np.float64)
    else:
        v = np.frombuffer(raw, dtype="<u4" if enc == 3 else "<u2").reshape(n, 2) * SCALE[enc] + h
    w = np.roll(v, -1, axis=0)
    a_lat.append(v[:, 0]); a_lng.append(v[:, 1]); b_lat.append(w[:, 0]); b_lng.append(w[:, 1])
a_lat, a_lng, b_lat, b_lng = (np.concatenate(x) for x in (a_lat, a_lng, b_lat, b_lng))
coslat = np.cos(np.radians((a_lat + b_lat) / 2))
dy = (b_lat - a_lat) * KM_PER_DEG
dx = (b_lng - a_lng) * KM_PER_DEG * coslat
length = np.hypot(dx, dy)
ok = (length > 0) & (np.abs(b_lng - a_lng) < 180)
print(f"rings {len(keep)} edges {len(length)} usable {int(ok.sum())} longer than 50 km {int((ok & (length > 50)).sum())}", file=sys.stderr)


def points(idx, t, dist_km):
    nx, ny = -dy[idx] / length[idx], dx[idx] / length[idx]  # unit normal, km frame
    lat = a_lat[idx] + t * (b_lat[idx] - a_lat[idx]) + ny * dist_km / KM_PER_DEG
    lng = a_lng[idx] + t * (b_lng[idx] - a_lng[idx]) + nx * dist_km / (KM_PER_DEG * coslat[idx])
    good = (np.abs(lat) < 85) & (np.abs(lng) <= 180)
    return lat[good], lng[good]


cand = np.nonzero(ok)[0]
p = length[cand] / length[cand].sum()
idx = rng.choice(cand, size=N_EDGE, p=p)
lat, lng = points(idx, rng.random(N_EDGE), rng.uniform(-KM_EDGE, KM_EDGE, N_EDGE))
np.savetxt(OUT + f"edge2k{SUFFIX}.txt", np.c_[lat, lng], fmt="%.7f")
long_ = np.nonzero(ok & (length > 50))[0]
idx = rng.choice(long_, size=N_LONG)
lat, lng = points(idx, rng.uniform(1 / 3, 2 / 3, N_LONG), rng.uniform(-15, 15, N_LONG))
np.savetxt(OUT + f"long15k{SUFFIX}.txt", np.c_[lat, lng], fmt="%.7f")
print("written", file=sys.stderr)
