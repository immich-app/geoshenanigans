#!/usr/bin/env python3
"""Cut a representative sample of an index dir's address files: 10 evenly
spread contiguous slices of 1% of the records each, polygons included.
Saves samp_<tag>.npz (records, postcodes, polygon blobs as arrays)."""
import sys, numpy as np
src, tag = sys.argv[1], sys.argv[2]
W = '/ssd/shrink/max/addresses'
ap = np.fromfile(f'{src}/addr_points.bin', np.uint32).reshape(-1, 7)
pc = np.fromfile(f'{src}/addr_postcodes.bin', np.uint32)
N = len(ap)
L = N // 100
sel = np.concatenate([np.arange(k * N // 10, k * N // 10 + L) for k in range(10)])
rec = ap[sel].copy()
av = np.fromfile(f'{src}/addr_vertices.bin', np.uint8)
vc = rec[:, 6].astype(np.int64)
poly = vc > 0
off = rec[poly, 5].astype(np.int64)
enc = av[off]
size = 10 + vc[poly] * np.where(enc >= 3, 8, 4)
first = np.concatenate([[0], np.cumsum(size)[:-1]])
g = np.arange(int(size.sum())) - np.repeat(first, size) + np.repeat(off, size)
blob = av[g]
rec[poly, 5] = first.astype(np.uint32)   # offsets into the sample blob
np.savez(f'{W}/samp_{tag}.npz', rec=rec, pc=pc[sel], blob=blob, sel=sel)
print(tag, 'records', len(rec), 'of', N, 'polygons', int(poly.sum()), 'blob', len(blob), 'of', len(av))
