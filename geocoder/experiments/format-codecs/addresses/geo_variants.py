#!/usr/bin/env python3
"""addr_entries replaced by id ranges (records in cell order, v_sorted):
  G1  geo_cells.addr = first addr id of the row's cell (cumulative, never
      NO_DATA); count = next row's value - this one (last row: record count).
      addr_entries.bin deleted.
  G2  geo_cells without the addr column and without rows that only held
      addresses (16-byte rows) + addr_cells.bin: (u64 cell, u32 first id)
      per cell with addresses plus a sentinel row (cell=u64 max, first=N).
Checks both reproduce v_sorted's per-cell id lists exactly."""
import sys, numpy as np
sys.path.insert(0, '/ssd/shrink/max/addresses')
from common import geo_cells, read_lists, NO

W = '/ssd/shrink/max/addresses'
S = f'{W}/v_sorted'
O = f'{W}/geo'
import os; os.makedirs(O, exist_ok=True)
cell, s, a, i = geo_cells(S)
blob = np.fromfile(f'{S}/addr_entries.bin', np.uint8)
has = a != NO
cnt, ids = read_lists(blob, a[has])
N = int(cnt.sum())
first = ids[np.cumsum(cnt) - cnt]
assert np.array_equal(ids, np.repeat(first, cnt) + (np.arange(N) - np.repeat(np.cumsum(cnt) - cnt, cnt)).astype(np.uint32)), 'lists are not runs'
assert np.all(first[1:] == first[:-1] + cnt[:-1]) and first[0] == 0
# G1
row_cnt = np.zeros(len(cell), np.int64); row_cnt[has] = cnt
cum = (np.cumsum(row_cnt) - row_cnt).astype(np.uint32)
gc = np.fromfile(f'{S}/geo_cells.bin', np.uint8).reshape(-1, 20)
gc[:, 12:16] = cum.view(np.uint8).reshape(-1, 4)
gc.tofile(f'{O}/g1_geo_cells.bin')
# decode check G1
c2 = np.diff(np.append(cum.astype(np.int64), N))
assert np.array_equal(c2[has], cnt) and np.all(c2[~has] == 0) and np.array_equal(cum[has], first)
# G2
keep = (s != NO) | (i != NO)
g2 = np.concatenate([gc[keep, 0:12], gc[keep, 16:20]], 1)
g2.tofile(f'{O}/g2_geo_cells.bin')
del gc, g2
ac = np.zeros(int(has.sum()) + 1, dtype=[('c', '<u8'), ('f', '<u4')])
ac['c'][:-1] = cell[has]; ac['c'][-1] = np.uint64(0xFFFFFFFFFFFFFFFF)
ac['f'][:-1] = first; ac['f'][-1] = N
ac.tofile(f'{O}/g2_addr_cells.bin')
# columnar variant of the same directory (cells column then firsts column)
np.concatenate([np.ascontiguousarray(ac["c"]).view(np.uint8), np.ascontiguousarray(ac["f"]).view(np.uint8)]).tofile(f'{O}/g2_addr_cells_col.bin')
print('rows', len(cell), 'addr rows', int(has.sum()), 'G2 rows kept', int(keep.sum()), 'N', N)
