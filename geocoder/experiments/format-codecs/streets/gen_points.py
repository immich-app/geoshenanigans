#!/usr/bin/env python3
"""Query points for the street/interp answer checks: 20k jittered +-30 m
around random addr_points, 10k around random street nodes, 10k around random
interp nodes (TIGER areas). Seeded; writes q/pts_{addr,street,interp}.txt."""
import numpy as np
I = "/ssd/shrink/idx/max"
rng = np.random.default_rng(20261013)
def jitter(lat, lng, n):
    m = 30.0
    dlat = rng.uniform(-m, m, n) / 111320.0
    dlng = rng.uniform(-m, m, n) / (111320.0 * np.maximum(np.cos(np.radians(lat)), 0.05))
    return lat + dlat, lng + dlng
def write(name, lat, lng):
    with open(f"/ssd/shrink/max/streets/q/pts_{name}.txt", "w") as f:
        for a, b in zip(lat, lng): f.write(f"{a:.6f} {b:.6f}\n")
ap = np.memmap(f"{I}/addr_points.bin", np.uint8, "r").reshape(-1, 28)
i = np.sort(rng.choice(len(ap), 20000, replace=False))
ll = ap[i, 0:8].copy().view("<f4").reshape(-1, 2).astype(np.float64)
write("addr", *jitter(ll[:, 0], ll[:, 1], len(i)))
for name, f in (("street", "street_nodes.bin"), ("interp", "interp_nodes.bin")):
    nd = np.memmap(f"{I}/{f}", "<f4", "r").reshape(-1, 2)
    i = np.sort(rng.choice(len(nd), 10000, replace=False))
    ll = nd[i].astype(np.float64)
    write(name, *jitter(ll[:, 0], ll[:, 1], len(i)))
