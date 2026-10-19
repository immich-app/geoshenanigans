#!/usr/bin/env python3
"""v5photo.py <planet dir> <n> <seed> <mix> : verify5's own photo-like sampler.
mix "tiers": the tiers2/3 recipe (35% major POIs +-0.0027 deg, 35% place nodes +-0.0027 deg,
             30% place nodes +-0.03 deg), reimplemented with numpy and a new seed
mix "addr" : address points (full/addr_points.bin, 28-byte records) +-0.0005 deg, a population-weighted
             proxy for where people live and take photos (not used by tiers3 or arcs)
Output "lat lng kind"."""
import sys

import numpy as np

P, n, seed, mix = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
rng = np.random.default_rng(seed)


def take(path, size, k, jitter, kind):
    m = np.memmap(path, dtype=np.uint8, mode="r")
    count = len(m) // size
    out = []
    got = 0
    while got < k:
        idx = rng.integers(0, count, size=(k - got) * 2)
        rec = np.stack([np.frombuffer(m[i * size:i * size + 8].tobytes(), dtype="<f4") for i in idx])
        rec = rec[~((rec[:, 0] == 0) & (rec[:, 1] == 0))][:k - got]
        out.append(rec.astype(np.float64))
        got += len(rec)
    a = np.concatenate(out)
    a += rng.uniform(-jitter, jitter, size=a.shape)
    return [(la, ln, kind) for la, ln in a]


if mix == "tiers":
    a = b = int(n * 0.35)
    pts = (take(f"{P}/poi/major/poi_records.bin", 36, a, 0.0027, "poi")
           + take(f"{P}/admin/place_nodes.bin", 20, b, 0.0027, "place")
           + take(f"{P}/admin/place_nodes.bin", 20, n - a - b, 0.03, "rural"))
else:
    pts = take(f"{P}/full/addr_points.bin", 28, n, 0.0005, "addr")
order = rng.permutation(len(pts))
for i in order:
    la, ln, kind = pts[i]
    la = max(-89.9, min(89.9, la))
    ln = ((ln + 180) % 360) - 180
    print(f"{la:.6f} {ln:.6f} {kind}")
