#!/usr/bin/env python3
"""Today's-format variant S: addr points renumbered into cell order.

- every (cell, id) entry of addr_entries becomes one record, sorted by
  (geo_cells row, old id): each cell's list is then a run of consecutive ids
  and the reader visits the same points in the same order as today;
- duplicate (cell, id) entries (same point listed twice in one cell) dropped;
- unreferenced records (strategy-2 tombstones) dropped;
- the 47 points listed by two cells are stored once per cell;
- addr_vertices rewritten in the new record order (blobs byte-identical),
  vertex_offset = new prefix sum; addr_postcodes follows the records;
- geo_cells keeps every row, only the addr offset column changes.

Writes <out>/ as a full index dir (other files hardlinked from idx/max).
"""
import os, sys, numpy as np
sys.path.insert(0, '/ssd/shrink/max/addresses')
from common import geo_cells, write_lists, NO

D = '/ssd/shrink/idx/max'
W = '/ssd/shrink/max/addresses'
OUT = sys.argv[1] if len(sys.argv) > 1 else f'{W}/v_sorted'
os.makedirs(OUT, exist_ok=True)

z = np.load(f'{W}/entries.npz')
cells, ids = z['cells'], z['ids'].astype(np.int64)
# entries are in geo_cells row order already (offsets increase with rows);
# within a cell ids are non-decreasing, duplicates adjacent.
keep = np.ones(len(ids), bool)
keep[1:] = ~((cells[1:] == cells[:-1]) & (ids[1:] == ids[:-1]))
cells, old = cells[keep], ids[keep]
M = len(old)
print('records after renumbering', M)

ap = np.fromfile(f'{D}/addr_points.bin', np.uint32).reshape(-1, 7)
new = ap[old].copy()
pc = np.fromfile(f'{D}/addr_postcodes.bin', np.uint32)[old]

# vertices: copy each polygon blob in the new order
av = np.fromfile(f'{D}/addr_vertices.bin', np.uint8)
vc = new[:, 6].astype(np.int64)
poly = vc > 0
src = new[poly, 5].astype(np.int64)
enc = av[src]
size = 10 + vc[poly] * np.where(enc >= 3, 8, 4)
dst = np.concatenate([[0], np.cumsum(size)[:-1]])
total = int(size.sum())
first = np.repeat(dst, size)
gidx = np.arange(total) - first + np.repeat(src, size)
nv = av[gidx]
new[poly, 5] = dst.astype(np.uint32)
new[~poly, 5] = NO
print('vertex bytes', total, 'was', len(av))

# entries: one run per cell, geo_cells addr column re-pointed
gcell, s, a, i = geo_cells(D)
has = a != NO
row_cells = gcell[has]
starts = np.searchsorted(cells, row_cells, 'left')
ends = np.searchsorted(cells, row_cells, 'right')
cnt = ends - starts
assert cnt.min() > 0 and cnt.sum() == M
blob, offs = write_lists(cnt, np.arange(M, dtype=np.uint32))
newa = a.copy(); newa[has] = offs.astype(np.uint32)

new.tofile(f'{OUT}/addr_points.bin')
pc.tofile(f'{OUT}/addr_postcodes.bin')
nv.tofile(f'{OUT}/addr_vertices.bin')
blob.tofile(f'{OUT}/addr_entries.bin')
gc = np.fromfile(f'{D}/geo_cells.bin', np.uint8).reshape(-1, 20)
gc[:, 12:16] = newa.view(np.uint8).reshape(-1, 4)
gc.tofile(f'{OUT}/geo_cells.bin')
del gc
np.save(f'{W}/sorted_old_ids.npy', old.astype(np.uint32))
np.save(f'{W}/sorted_cells.npy', cells)
for f in os.listdir(D):
    p = f'{OUT}/{f}'
    if not os.path.exists(p):
        os.link(f'{D}/{f}', p)
print('done', OUT)
