#!/usr/bin/env python3
"""addr_entries structure: duplicate ids within a cell, ids in several cells,
whether a point sits in the cell that lists it. Saves cell/id pairs."""
import numpy as np, sys
sys.path.insert(0, '/ssd/shrink/max/addresses')
from common import load_entries, cell_of_points

D = '/ssd/shrink/idx/max'
W = '/ssd/shrink/max/addresses'
cells, ids, rowidx = load_entries(D)   # one element per entry, in file order
print('entries', len(ids))
pair = cells.astype(np.uint64)  # cell id
# duplicates (cell,id)
key = np.stack([cells, ids.astype(np.uint64)], 1)
o = np.lexsort((ids, cells))
dup = np.all(key[o][1:] == key[o][:-1], axis=1)
print('duplicate (cell,id) entries', int(dup.sum()))
# distinct cells per id
k2 = key[o][np.concatenate([[True], ~dup])]
ucnt = np.bincount(k2[:, 1].astype(np.int64))
print('ids in >1 distinct cell', int((ucnt > 1).sum()), 'max distinct cells per id', int(ucnt.max()))
np.savez(f'{W}/entries.npz', cells=cells, ids=ids)

ap = np.fromfile(f'{D}/addr_points.bin', np.uint32).reshape(-1, 7)
lat = ap[:, 0].view(np.float32).astype(np.float64); lng = ap[:, 1].view(np.float32).astype(np.float64)
own = cell_of_points(lat[ids], lng[ids])
print('entries whose cell == cell(lat,lng) of point', int((own == cells).sum()), 'of', len(ids))
multi = ucnt[ids] > 1
print('multi-cell ids: entries matching own cell', int((own[multi] == cells[multi]).sum()), 'of', int(multi.sum()))
vc = ap[:, 6]
print('multi-cell ids with polygon', int((vc[np.nonzero(ucnt > 1)[0]] > 0).sum()))
single = ~multi
print('single-cell entries matching own cell', int((own[single] == cells[single]).sum()), 'of', int(single.sum()))
