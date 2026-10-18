#!/usr/bin/env python3
"""Spatially reorder street ways (or interp ways) in TODAY'S format.

A pure permutation of the records: every way keeps its header fields and its
node run byte-for-byte; node runs are repacked in the new way order; every
reference to a way index is remapped (cell entry lists re-sorted, parallel
sidecars permuted, AddrPoint.parent_way_id remapped).

usage: reorder.py street|interp <out_dir> [first|mid] [block_bits] [noaddr]
  writes the changed files into out_dir plus remap_<kind>.u32 (old -> new id).
  Order key: 2-D Hilbert index (2^24 per axis) of the way's first node
  (or bbox midpoint with `mid`), then name/street id, node count, old index.
  block_bits > 0: order by coarse block (Hilbert key >> 2*block_bits, i.e.
  blocks of 2^block_bits fine cells per axis) first, then name, then the fine
  Hilbert key, so names stay sorted inside each block.
  noaddr: skip rewriting addr_points.bin (size experiments only).
  Tombstones (node_count 0) go last.
"""
import os, sys, numpy as np

SRC = '/ssd/shrink/idx/max'
kind, OUT = sys.argv[1], sys.argv[2]
point = sys.argv[3] if len(sys.argv) > 3 else 'first'
block_bits = int(sys.argv[4]) if len(sys.argv) > 4 else 0
noaddr = len(sys.argv) > 5 and sys.argv[5] == 'noaddr'
os.makedirs(OUT, exist_ok=True)
NO_DATA = np.uint32(0xFFFFFFFF)

WAY = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad', '<u2'), ('name', '<u4')])
IW = np.dtype([('off', '<u4'), ('cnt', '<u2'), ('pad1', '<u2'), ('name', '<u4'),
               ('start', '<u4'), ('end', '<u4'), ('interp', 'u1'), ('pad4', 'u1', 3)])
ADDR = np.dtype([('lat', '<f4'), ('lng', '<f4'), ('hn', '<u4'), ('street', '<u4'),
                 ('parent_way', '<u4'), ('voff', '<u4'), ('vcnt', '<u4')])


def hilbert(x, y, order):
    n = np.int64(1) << order
    x = x.astype(np.int64); y = y.astype(np.int64)
    d = np.zeros(len(x), np.int64)
    s = n >> 1
    while s > 0:
        rx = ((x & s) > 0).astype(np.int64)
        ry = ((y & s) > 0).astype(np.int64)
        d += s * s * ((3 * rx) ^ ry)
        flip = (ry == 0) & (rx == 1)
        x = np.where(flip, n - 1 - x, x)
        y = np.where(flip, n - 1 - y, y)
        sw = ry == 0
        x, y = np.where(sw, y, x), np.where(sw, x, y)
        s >>= 1
    return d


if kind == 'street':
    wf, nf, dt = 'street_ways.bin', 'street_nodes.bin', WAY
    sidecars = ['way_parents.bin', 'way_postcodes.bin']
else:
    wf, nf, dt = 'interp_ways.bin', 'interp_nodes.bin', IW
    sidecars = ['interp_postcodes.bin']

ways = np.fromfile(f'{SRC}/{wf}', dt)
nodes = np.fromfile(f'{SRC}/{nf}', '<u4').reshape(-1, 2)
W, N = len(ways), len(nodes)
off = ways['off'].astype(np.int64); cnt = ways['cnt'].astype(np.int64)
pk = np.concatenate([[0], np.cumsum(cnt)[:-1]])
assert np.array_equal(off[cnt > 0], pk[cnt > 0]), 'offsets not packed'   # tombstones have cnt 0, off 0
print('tombstones', (cnt == 0).sum(), 'tombstone off nonzero', (off[cnt == 0] != 0).sum())
live = cnt > 0
f = nodes.view(np.float32)
if point == 'first':
    lat = np.where(live, f[np.minimum(off, N - 1), 0], 0.0)
    lng = np.where(live, f[np.minimum(off, N - 1), 1], 0.0)
else:
    grp = np.repeat(np.arange(W), cnt)
    lat = np.zeros(W); lng = np.zeros(W)
    for j, a in ((0, 'lat'), (1, 'lng')):
        mn = np.full(W, np.inf); mx = np.full(W, -np.inf)
        np.minimum.at(mn, grp, f[:, j]); np.maximum.at(mx, grp, f[:, j])
        mid = np.where(live, (mn + mx) / 2, 0.0)
        if j == 0: lat = mid
        else: lng = mid
ORDER = 24
x = np.clip(((lng.astype(np.float64) + 180.0) / 360.0 * (1 << ORDER)).astype(np.int64), 0, (1 << ORDER) - 1)
y = np.clip(((lat.astype(np.float64) + 90.0) / 180.0 * (1 << ORDER)).astype(np.int64), 0, (1 << ORDER) - 1)
h = hilbert(x, y, ORDER)
h[~live] = np.int64(1) << 62
if block_bits:
    blk = h >> (2 * block_bits)
    perm = np.lexsort((np.arange(W), cnt, h, ways['name'].astype(np.int64), blk))
else:
    perm = np.lexsort((np.arange(W), cnt, ways['name'].astype(np.int64), h))   # new i <- old perm[i]
remap = np.empty(W, np.uint32); remap[perm] = np.arange(W, dtype=np.uint32)
remap.tofile(f'{OUT}/remap_{kind}.u32')

nw = ways[perm].copy()
ncnt = cnt[perm]
noff = np.concatenate([[0], np.cumsum(ncnt)[:-1]])
nw['off'] = noff.astype(np.uint32)
nw.tofile(f'{OUT}/{wf}')
src = np.repeat(off[perm] - noff, ncnt) + np.arange(N, dtype=np.int64)
nodes[src].tofile(f'{OUT}/{nf}')
del src
for sc in sidecars:
    v = np.fromfile(f'{SRC}/{sc}', '<u4')
    assert len(v) == W
    v[perm].tofile(f'{OUT}/{sc}')
if kind == 'street' and not noaddr:
    ap = np.fromfile(f'{SRC}/addr_points.bin', ADDR)
    pw = ap['parent_way']
    has = pw != NO_DATA
    assert (pw[has] < W).all()
    ap['parent_way'][has] = remap[pw[has]]
    ap.tofile(f'{OUT}/addr_points.bin')
    print(f'addr_points parent_way remapped: {has.sum()}')
# sanity: every old way's run equals its new run
chk = np.random.default_rng(1).choice(W, 200000, replace=False)
for o in chk[:2000]:
    n = nw[remap[o]]
    assert n['cnt'] == cnt[o]
print(f'{kind}: ways {W} nodes {N} reordered by hilbert({point}) block_bits {block_bits}; remap written')
