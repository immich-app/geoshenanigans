#!/usr/bin/env python3
"""Item 1: counts, encodings, levels, delta magnitudes from decoded.npz."""
import numpy as np
z = np.load('decoded.npz')
enc, cnt, level, vpoly = z['enc'], z['cnt'], z['level'], z['vpoly']
q_lat, q_lng, lat, lng = z['q_lat'], z['q_lng'], z['lat'], z['lng']
N, P = len(q_lat), len(cnt)
FILE = 160644112
print(f'polygons {P}  vertices {N}  raw B/vertex {FILE/N:.3f}  header bytes {10*P} ({10*P/FILE:.1%})')
print(f'mean verts/poly {N/P:.1f}  median {np.median(cnt):.0f}  p99 {np.percentile(cnt,99):.0f}  max {cnt.max()}')
stride = np.where(enc <= 2, 4, 8)
print('\nencoding  polys  vertices  bytes(incl hdr)')
for e, nm in enumerate(['0 u16@1e-5', '1 u16@1e-4', '2 u16@1e-6', '3 u32@1e-7']):
    s = enc == e
    print(f'{nm:12s} {s.sum():8d} {cnt[s].sum():10d} {int((10 + cnt[s]*stride[s]).sum()):11d}')
print('\nlevel  polys  vertices  share')
for L in np.unique(level):
    s = level == L
    print(f'{L:5d} {s.sum():7d} {cnt[s].sum():10d} {cnt[s].sum()/N:6.1%}')

# closed ring: first == last stored?
first = z['vstart']; last = first + cnt - 1
closed = (q_lat[first] == q_lat[last]) & (q_lng[first] == q_lng[last])
print(f'\nrings stored closed (last==first): {closed.sum()} of {P}')

# sequential deltas within polygon, in each polygon's own grid units
same = np.ones(N, bool); same[first] = False
dl = np.diff(q_lat, prepend=0)[same]; dg = np.diff(q_lng, prepend=0)[same]
def zz(x): return np.where(x >= 0, 2*x, -2*x - 1)
def bits(x):
    x = zz(x).astype(np.uint64)
    b = np.zeros(len(x), np.int64)
    v = x.copy()
    while np.any(v):
        nz = v > 0; b[nz] += 1; v >>= np.uint64(1)
    return b
bl, bg = bits(dl), bits(dg)
m = np.maximum(bl, bg)
print(f'\nsequential deltas (n={len(dl)}), zigzag bit-length of max(|dlat|,|dlng|) per vertex:')
for t in (4, 8, 12, 16, 20, 24, 32):
    print(f'  <= {t:2d} bits: {np.mean(m <= t):6.1%}   (per-component <= {t}: {np.mean(np.concatenate([bl,bg]) <= t):6.1%})')
# per-encoding view (only u16 encodings matter)
for e in (0, 1, 3):
    s = (enc[vpoly] == e)[same]
    if s.sum():
        print(f'  enc {e}: median component bits {np.median(np.concatenate([bl[s], bg[s]])):.0f},'
              f' <=8 bits {np.mean(np.concatenate([bl[s], bg[s]]) <= 8):.1%}, <=12 {np.mean(np.concatenate([bl[s], bg[s]]) <= 12):.1%}')
# LEB128 varint size estimate of zigzag deltas
def vlen(b): return np.maximum(1, (b + 6) // 7)
first_bytes = sum(int(vlen(bits(q_lat[first]-0)).sum()) for _ in [0])  # first vertex absolute
vb = int(vlen(bl).sum() + vlen(bg).sum()) + int(vlen(bits(q_lat[first])).sum() + vlen(bits(q_lng[first])).sum())
print(f'\nzigzag LEB128 varint estimate (same grids, same headers): {vb + 10*P} bytes = {(vb+10*P)/N:.2f} B/vertex')
