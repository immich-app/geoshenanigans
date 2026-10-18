#!/usr/bin/env python3
"""Today's way records for the same 8 x 0.75% slices nodeenc/nodedl sample."""
import numpy as np
for kind, f, S in (("street", "street_ways.bin", 12), ("interp", "interp_ways.bin", 24)):
    b = np.memmap(f"/ssd/shrink/idx/max/{f}", np.uint8, "r")
    W = len(b) // S
    per = int(W * 0.06 / 8)
    parts = []
    for s in range(8):
        w0 = int(W * (s + 0.5) / 8) - per // 2
        parts.append(np.asarray(b[w0 * S:(w0 + per) * S]))
    np.concatenate(parts).tofile(f"/ssd/shrink/max/streets/dl/{kind}/today_ways.sample")
    print(kind, W, per * 8)
