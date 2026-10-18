#!/usr/bin/env python3
"""Whole-file column layouts of the Tier B addr_points (tb/abs2/addr_points.bin:
lat|lng|hn|st|pw|vo, N each) -> cols/*.bin, one file per column variant, so
zall.sh gives per-column -18 sizes. Also the download-only stream (no vo,
lat/lng as zigzag deltas of the f32 order key in 5 byte planes) with an
exact inverse check."""
import os, sys, numpy as np
sys.path.insert(0, '/ssd/shrink/max/addresses')
from codecs_addr import fkey, zz, unzz

W = '/ssd/shrink/max/addresses'
O = f'{W}/cols'
os.makedirs(O, exist_ok=True)
col = np.fromfile(f'{W}/tb/abs2/addr_points.bin', np.uint32)
N = len(col) // 6
col = col.reshape(6, N)
names = ['lat', 'lng', 'hn', 'st', 'pw', 'vo']
batch = sys.argv[1]  # 'll' (lat/lng variants) or 'ids' (the u32 id columns)
for k, nm in enumerate(names):
    if (k < 2) != (batch == 'll'):
        continue
    col[k].tofile(f'{O}/{nm}.bin')
    col[k].view(np.uint8).reshape(N, 4).T.tofile(f'{O}/{nm}_planes.bin')
for k, nm in enumerate(names[:2] if batch == 'll' else []):
    v = col[k]
    (v >> 16).astype('<u2').tofile(f'{O}/{nm}_hi16.bin')
    (v & 0xffff).astype('<u2').tofile(f'{O}/{nm}_lo16.bin')
    d = np.diff(fkey(v), prepend=0)
    z = zz(d)
    assert z.max() < 2**40
    z.astype('<u8').view(np.uint8).reshape(N, 8)[:, :5].T.tofile(f'{O}/{nm}_dl_dzz5.bin')
    # inverse check
    back = np.cumsum(unzz(z))
    k2 = back.astype(np.int64)
    rec = np.where(k2 & 0x80000000, k2 ^ 0x80000000, 0xFFFFFFFF - k2).astype(np.uint32)
    assert np.array_equal(rec, v), nm
print('columns written', N)
