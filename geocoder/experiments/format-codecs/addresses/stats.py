#!/usr/bin/env python3
"""Field ranges, dead records and layout facts for the address files.

Reads /ssd/shrink/idx/max (read-only). Prints facts every later encoding
relies on (value ranges, monotone offsets, one cell per point, ...).
"""
import json, sys, numpy as np

D = '/ssd/shrink/idx/max'
NO = np.uint32(0xFFFFFFFF)

ap = np.fromfile(f'{D}/addr_points.bin', np.uint32).reshape(-1, 7)
N = len(ap)
print('addr_points', N)
lat = ap[:, 0].view(np.float32); lng = ap[:, 1].view(np.float32)
hn, st, pw, vo, vc = (ap[:, k] for k in range(2, 7))

lay = json.load(open(f'{D}/strings_layout.json'))['tiers']
for t in lay:
    print('tier', t['name'], t['start'], t['end'], t['end'] - t['start'])

def tier_of(x):
    out = np.full(len(x), -1, np.int8)
    for k, t in enumerate(lay):
        out[(x >= t['start']) & (x < t['end'])] = k
    out[x == NO] = 9
    return out

for name, col in (('housenumber_id', hn), ('street_id', st)):
    tt = tier_of(col)
    print(name, 'tiers', {int(k): int(v) for k, v in zip(*np.unique(tt, return_counts=True))})
    live = col[col != NO]
    print(name, 'min', live.min(), 'max', live.max(), 'distinct', len(np.unique(col)))

ways = (1 << 64)
nw = np.fromfile(f'{D}/street_ways.bin', np.uint8).size // 12
print('street_ways', nw)
print('parent_way_id NO_DATA', int((pw == NO).sum()), 'max live', pw[pw != NO].max() if (pw != NO).any() else None,
      'distinct', len(np.unique(pw)))
print('vertex_count dist: zero', int((vc == 0).sum()), 'max', vc.max())
for b in (255, 256, 65535):
    print('  vc >', b, int((vc > b).sum()))
print('vertex_offset NO_DATA', int((vo == NO).sum()), 'where vc==0 & vo!=NO', int(((vc == 0) & (vo != NO)).sum()),
      'where vc>0 & vo==NO', int(((vc > 0) & (vo == NO)).sum()))

# tombstones: zeroed record with parent_way NO_DATA and vertex_offset NO_DATA
tomb = (ap[:, 0] == 0) & (ap[:, 1] == 0) & (hn == 0) & (st == 0) & (pw == NO) & (vo == NO) & (vc == 0)
print('tombstones', int(tomb.sum()))

# vertex_offset is a prefix sum of polygon byte sizes in record order?
av = np.fromfile(f'{D}/addr_vertices.bin', np.uint8)
print('addr_vertices bytes', len(av))
poly = vc > 0
pi = np.nonzero(poly)[0]
offs = vo[pi].astype(np.int64)
enc = av[offs]
stride = np.where(enc >= 3, 8, 4)
size = 10 + vc[pi].astype(np.int64) * stride
expect = np.concatenate([[0], np.cumsum(size)[:-1]])
print('polygons', len(pi), 'offsets == prefix sum:', bool(np.array_equal(expect, offs)),
      'end == file size:', int(expect[-1] + size[-1]) == len(av))
print('encodings', {int(k): int(v) for k, v in zip(*np.unique(enc, return_counts=True))})
print('pad byte nonzero', int((av[offs + 1] != 0).sum()))
vcp = vc[pi]
qs = np.percentile(vcp, [50, 90, 99, 99.9, 100])
print('vertex_count percentiles (polygons)', qs)

# Entries: u16 count + u32 ids per non-empty cell
ae = np.fromfile(f'{D}/addr_entries.bin', np.uint8)
gc = np.fromfile(f'{D}/geo_cells.bin', np.uint8).reshape(-1, 20)
cell = gc[:, 0:8].copy().view(np.uint64).ravel()
aoff = gc[:, 12:16].copy().view(np.uint32).ravel()
print('geo_cells rows', len(gc), 'sorted', bool(np.all(cell[1:] > cell[:-1])))
soff = gc[:, 8:12].copy().view(np.uint32).ravel()
ioff = gc[:, 16:20].copy().view(np.uint32).ravel()
has_a = aoff != NO
print('rows with addr', int(has_a.sum()), 'addr-only rows', int((has_a & (soff == NO) & (ioff == NO)).sum()))
o = aoff[has_a].astype(np.int64)
print('addr offsets increasing in cell order', bool(np.all(o[1:] > o[:-1])), 'unique', len(np.unique(o)))
cnt = ae[o].astype(np.int64) | (ae[o + 1].astype(np.int64) << 8)
ends = o + 2 + 4 * cnt
print('entries cover file exactly', bool(np.array_equal(np.sort(o)[1:], np.sort(ends)[:-1])) , int(ends.max()) == len(ae))
print('total entries', int(cnt.sum()), 'max per cell', int(cnt.max()), 'cells with count 0', int((cnt == 0).sum()))
# gather all ids
starts = o + 2
idx = np.repeat(starts, cnt) + 4 * (np.arange(cnt.sum()) - np.repeat(np.cumsum(cnt) - cnt, cnt))
ids = (ae[idx].astype(np.uint32) | (ae[idx + 1].astype(np.uint32) << 8) |
       (ae[idx + 2].astype(np.uint32) << 16) | (ae[idx + 3].astype(np.uint32) << 24))
refs = np.bincount(ids, minlength=N)
print('ids >= N', int((ids >= N).sum()), 'records referenced 0x', int((refs == 0).sum()), '1x', int((refs == 1).sum()),
      '>1x', int((refs > 1).sum()))
print('unreferenced that are tombstones', int((tomb & (refs == 0)).sum()))
print('ids sorted within cells', bool(np.all(np.diff(ids.astype(np.int64))[np.diff(np.repeat(np.arange(len(cnt)), cnt)) == 0] > 0)))
np.save('/ssd/shrink/max/addresses/addr_cell_of_id.npy', np.repeat(cell[has_a], cnt)[np.argsort(ids, kind='stable')] if (refs == 1).all() else np.zeros(1))

ac = np.fromfile(f'{D}/addr_postcodes.bin', np.uint32)
print('addr_postcodes', len(ac), 'NO_DATA', int((ac == NO).sum()), 'tiers',
      {int(k): int(v) for k, v in zip(*np.unique(tier_of(ac), return_counts=True))}, 'distinct', len(np.unique(ac)))
ic = np.fromfile(f'{D}/interp_postcodes.bin', np.uint32)
print('interp_postcodes', len(ic), 'NO_DATA', int((ic == NO).sum()), 'tiers',
      {int(k): int(v) for k, v in zip(*np.unique(tier_of(ic), return_counts=True))}, 'distinct', len(np.unique(ic)))
ni = np.fromfile(f'{D}/interp_ways.bin', np.uint8).size // 24
print('interp_ways', ni)

# exact duplicate records (all fields but vertex_offset) and duplicate polygons
key = np.ascontiguousarray(ap[:, [0, 1, 2, 3, 4, 6]]).view(np.dtype((np.void, 24))).ravel()
u = np.unique(key)
print('distinct records ignoring vertex_offset', len(u), 'of', N)
