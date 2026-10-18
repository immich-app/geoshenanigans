#!/usr/bin/env python3
"""u8 per address point: 1 when the point lies in a 0.005-deg cell within one cell (3 x 3) of a test
point of the given pools, else 0. Used to write exact (encoding 3) outlines only near the test points,
so the exact-outline variant stays under the 4 GiB u32 offset limit of addr_vertices.bin; the reader's
address search (level-17 cell plus neighbours, ~150 m) never reaches past 0.005 deg (>= 270 m at 60N).
usage: near_mask.py OUT.u8 pool.txt ..."""
import sys
import numpy as np

C = 0.005
out, pools = sys.argv[1], sys.argv[2:]
keys = set()
for p in pools:
    a = np.loadtxt(p, dtype=np.float64, ndmin=2)
    ci, cj = np.floor(a[:, 0] / C).astype(np.int64), np.floor(a[:, 1] / C).astype(np.int64)
    for di in (-1, 0, 1):
        for dj in (-1, 0, 1):
            keys.update(((ci + di) * 100000 + (cj + dj)).tolist())
k = np.array(sorted(keys), np.int64)
pt = np.memmap("/ssd/precision/build/dumps/addr_points.e7", "<i4", "r").reshape(-1, 2)
idx = np.fromfile("/ssd/precision/build/dumps/addr_vertices.e7.idx", "<u8").astype(np.int64)
cnt = np.diff(idx)
mask = np.zeros(len(pt), np.uint8)
CH = 20_000_000
for a in range(0, len(pt), CH):
    q = np.asarray(pt[a:a + CH]).astype(np.float64) * 1e-7
    kk = np.floor(q[:, 0] / C).astype(np.int64) * 100000 + np.floor(q[:, 1] / C).astype(np.int64)
    mask[a:a + CH] = np.isin(kk, k)
mask[cnt == 0] = 0
mask.tofile(out)
print(f"cells {len(k):,}; points with outline marked {int(mask.sum()):,}; their vertices {int(cnt[mask == 1].sum()):,} "
      f"(+4 B each as encoding 3: {int(cnt[mask == 1].sum()) * 4 / 1e6:,.0f} MB)")
