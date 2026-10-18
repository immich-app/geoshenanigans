#!/usr/bin/env python3
"""Photo-like test points: 20k points around place nodes of the minimal set.
Half around city/town nodes (uniform in a 5 km disc), half around
village/hamlet nodes (2 km disc). Seeded. Writes pts_places.txt."""
import numpy as np
rng = np.random.default_rng(20261013)
dt = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("name", "<u4"), ("type", "u1"), ("p1", "u1"), ("p2", "<u2"), ("parent", "<u4")])
p = np.fromfile("/ssd/shrink/idx/smallest/place_nodes.bin", dtype=dt)
print("place nodes", len(p), "by type", np.bincount(p["type"]))
out = []
for types, radius in (((0, 1), 5000.0), ((2, 4), 2000.0)):
    pool = np.nonzero(np.isin(p["type"], types))[0]
    pick = pool[rng.integers(0, len(pool), 10000)]
    r = radius * np.sqrt(rng.random(10000)); b = rng.random(10000) * 2 * np.pi
    la = p["lat"][pick].astype(np.float64) + r * np.cos(b) / 111320.0
    lo = p["lng"][pick].astype(np.float64) + r * np.sin(b) / (111320.0 * np.cos(np.radians(la)))
    out.append(np.stack([la, lo], 1))
pts = np.concatenate(out)
np.savetxt("pts_places.txt", pts, fmt="%.7f")
print("wrote", len(pts))
