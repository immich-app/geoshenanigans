#!/usr/bin/env python3
"""Exact-tie queries for interpolation lines (seeded).

gen_interp_ties.py DATE OUT_DIR
  pts_ities.txt: queries placed exactly on vertices that two or more live
  interpolation lines share, where the lines differ in street or TIGER
  postcode (query_geo then keeps whichever it lists first): up to 6000 such
  vertices, the vertex itself (written with full float precision, so the
  distance to every line through it is exactly 0) plus one point 10 m away
  in a random direction.
"""
import sys
import numpy as np

date, OUT = sys.argv[1], sys.argv[2]
D = f'/ssd/shrink/days/{date}/planet/full'
rng = np.random.default_rng(20261015)
IW = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad1', '<u2'), ('street', '<u4'),
               ('start', '<u4'), ('end', '<u4'), ('interp', 'u1'), ('pad4', 'u1', 3)])
iw = np.fromfile(f'{D}/interp_ways.bin', IW)
nodes = np.fromfile(f'{D}/interp_nodes.bin', '<u4').reshape(-1, 2)
pc = np.fromfile(f'{D}/interp_postcodes.bin', '<u4')
cnt = iw['cnt'].astype(np.int64)
owner = np.repeat(np.arange(len(iw), dtype=np.int64), cnt)
# nodes are packed in slot order (canon_sort checks it), so node j belongs to owner[j]
assert len(owner) == len(nodes)
key = (nodes[:, 0].astype(np.uint64) << np.uint64(32)) | nodes[:, 1].astype(np.uint64)
order = np.lexsort((owner, key))
k, o = key[order], owner[order]
same = k[1:] == k[:-1]
other = same & (o[1:] != o[:-1])
# adjacent pairs in a vertex run that the reader can tell apart
diff = other & ((iw['street'][o[1:]] != iw['street'][o[:-1]]) | (pc[o[1:]] != pc[o[:-1]]))
cand = np.unique(k[1:][diff])
print('shared vertices with distinguishable lines', len(cand))
pick = cand[rng.choice(len(cand), min(6000, len(cand)), replace=False)]
lat = (pick >> np.uint64(32)).astype(np.uint32).view(np.float32).astype(np.float64)
lng = (pick & np.uint64(0xFFFFFFFF)).astype(np.uint32).view(np.float32).astype(np.float64)
ang = rng.uniform(0, 2 * np.pi, len(pick))
lat2 = lat + np.sin(ang) * 10 / 111320.0
lng2 = lng + np.cos(ang) * 10 / (111320.0 * np.cos(np.radians(lat)))
with open(f'{OUT}/pts_ities.txt', 'w') as f:
    for a, b, c, d in zip(lat, lng, lat2, lng2):
        f.write(f'{float(a)!r} {float(b)!r}\n{c:.8f} {d:.8f}\n')
print('points', 2 * len(pick))
