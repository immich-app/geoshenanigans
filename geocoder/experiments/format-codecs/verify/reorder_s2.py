"""Experiment: order street / interp ways along the S2 cell curve (leaf cell id of the first node)
instead of the streets track's lat/lng Hilbert key, so the way order follows the geo index's own row
order (geo_cells rows are sorted by S2 cell id). Ties: name/street id, node count, old index (as the
track's reorder.py). Writes, in today's format, <out>/<kind>_ways.bin, <kind>_nodes.bin and
remap_<kind>.u32 (old -> new); sidecars are not written (sizes of those follow the order).
usage: reorder_s2.py street|interp <out_dir>"""
import os
import sys
import numpy as np
sys.path.insert(0, "/ssd/shrink/max/addresses")
from common import cell_of_points

SRC = "/ssd/shrink/idx/max"
kind, OUT = sys.argv[1], sys.argv[2]
os.makedirs(OUT, exist_ok=True)
WAY = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad', '<u2'), ('name', '<u4')])
IW = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad1', '<u2'), ('name', '<u4'), ('start', '<u4'), ('end', '<u4'),
               ('interp', 'u1'), ('pad4', 'u1', 3)])
wf, nf, dt = (('street_ways.bin', 'street_nodes.bin', WAY) if kind == 'street' else
              ('interp_ways.bin', 'interp_nodes.bin', IW))
ways = np.fromfile(f'{SRC}/{wf}', dt)
nodes = np.fromfile(f'{SRC}/{nf}', '<u4').reshape(-1, 2)
W, N = len(ways), len(nodes)
off = ways['off'].astype(np.int64)
cnt = ways['cnt'].astype(np.int64)
live = cnt > 0
f = nodes.view(np.float32)
lat = f[np.minimum(off, N - 1), 0].astype(np.float64)
lng = f[np.minimum(off, N - 1), 1].astype(np.float64)
key = np.full(W, np.iinfo(np.uint64).max, np.uint64)
key[live] = cell_of_points(lat[live], lng[live], level=30)
perm = np.lexsort((np.arange(W), cnt, ways['name'].astype(np.int64), key))
remap = np.empty(W, np.uint32)
remap[perm] = np.arange(W, dtype=np.uint32)
remap.tofile(f'{OUT}/remap_{kind}.u32')
nw = ways[perm].copy()
ncnt = cnt[perm]
noff = np.concatenate([[0], np.cumsum(ncnt)[:-1]])
nw['off'] = noff.astype(np.uint32)
nw.tofile(f'{OUT}/{wf}')
src = np.repeat(off[perm] - noff, ncnt) + np.arange(N, dtype=np.int64)
nodes[src].tofile(f'{OUT}/{nf}')
print(f'{kind}: {W} ways, {N} nodes reordered along the S2 curve; remap written')
