#!/usr/bin/env python3
"""PAX layouts of the Tier B addr_points on the sorted 10% sample: records in
blocks of B, each block holding its B lat | B lng | B hn | B st | B pw | B vo
(so one record's fields share a page), optionally lat/lng as byte planes
inside the block. Also the plain columnar layout of the same sample, the
row-24 layout and the col6+planes layout, for scaling to the whole file."""
import os, sys, numpy as np
W = '/ssd/shrink/max/addresses'
z = np.load(f'{W}/samp_sorted.npz')
sel = z['sel']
col = np.memmap(f'{W}/tb/abs2/addr_points.bin', np.uint32, 'r')
N = len(col) // 6
cols = np.stack([np.asarray(col[k * N:(k + 1) * N][sel]) for k in range(6)])  # 6 x n
n = cols.shape[1]
O = f'{W}/exp/pax'
os.makedirs(O, exist_ok=True)
def w(name, arr):
    np.ascontiguousarray(arr).view(np.uint8).tofile(f'{O}/{name}.bin')

w('col6', cols)
w('row24', cols.T)
pl = [cols[k].view(np.uint8).reshape(n, 4).T for k in range(2)]
w('col6_llplanes', np.concatenate([pl[0].ravel(), pl[1].ravel()] + [cols[k].view(np.uint8) for k in range(2, 6)]))
for B in (170, 1024, 4096):
    nb = -(-n // B)
    pad = np.zeros((6, nb * B), np.uint32); pad[:, :n] = cols
    blocks = pad.reshape(6, nb, B).transpose(1, 0, 2)  # nb x 6 x B
    w(f'pax{B}', blocks)
    # lat/lng as 4 byte planes inside each block
    b8 = pad.view(np.uint8).reshape(6, nb, B, 4)
    ll = b8[0:2].transpose(1, 0, 3, 2).reshape(nb, -1)        # nb x (2*4*B)
    rest = b8[2:6].transpose(1, 0, 2, 3).reshape(nb, -1)      # nb x (4*B*4)
    w(f'pax{B}_llplanes', np.concatenate([ll, rest], 1))
print('sample records', n)
