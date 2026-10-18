#!/usr/bin/env python3
"""Stack lossless re-encodings on top of a (lossy) variant.

  encode_variant.py <decoded.npz> <tag> [G]

Writes stack/<tag>.<fmt>.bin for:
  shuffle  : per-polygon native grid, wrapping zigzag deltas, fixed width,
             byte planes over the whole file, headers in their own block
  varint   : per-polygon native grid, open rings, zigzag LEB128 deltas,
             lat stream | lng stream, headers in their own block
  gvarint  : (G given) ONE global grid of G degrees, open rings, no per-polygon
             header (first vertex zigzag-delta from the previous polygon's
             first vertex), lat stream | lng stream
  gshuffle : (G given) global grid, open rings, zigzag deltas at 16 bits where
             the whole polygon fits else 32, byte planes, 1 width byte/polygon
"""
import os, sys, numpy as np
src, tag = sys.argv[1], sys.argv[2]
G = float(sys.argv[3]) if len(sys.argv) > 3 else None
z = np.load(src)
enc, cnt, vstart, vpoly = z['enc'], z['cnt'], z['vstart'], z['vpoly']
q_lat, q_lng = z['q_lat'], z['q_lng']
P, N = len(cnt), len(q_lat)
os.makedirs('stack', exist_ok=True)

def zz(x):
    x = x.astype(np.int64); return np.where(x >= 0, 2 * x, -2 * x - 1).astype(np.uint64)
def varint(v):
    v = v.astype(np.uint64); ln = np.ones(len(v), np.int64); t = v >> np.uint64(7)
    while np.any(t): ln += t > 0; t >>= np.uint64(7)
    Lm = int(ln.max()); mat = np.zeros((len(v), Lm), np.uint8)
    for k in range(Lm):
        mat[:, k] = ((v >> np.uint64(7 * k)) & np.uint64(0x7f)).astype(np.uint8) | np.where(ln > k + 1, 0x80, 0).astype(np.uint8)
    return mat[np.arange(Lm)[None, :] < ln[:, None]]
def header():
    h = np.zeros((P, 10), np.uint8); h[:, 0] = enc
    h[:, 2:6] = z['bmin_lat'].view(np.uint8).reshape(-1, 4); h[:, 6:10] = z['bmin_lng'].view(np.uint8).reshape(-1, 4)
    return h.ravel()
def planes(vals, width):
    b = vals.astype('<u2' if width == 2 else '<u4').view(np.uint8).reshape(-1, width)
    return [b[:, k] for k in range(width)]
def write(fmt, arr):
    arr.tofile(f'stack/{tag}.{fmt}.bin'); print(f'{tag}.{fmt} raw {len(arr)}')

wide = (enc == 3)[vpoly]
# native shuffle
def wrap(q, bits):
    d = np.diff(q, prepend=0); d[vstart] = q[vstart]; m = 1 << bits
    d = np.mod(d, m); return zz(np.where(d >= m // 2, d - m, d))
dl = np.where(wide, wrap(q_lat, 32), wrap(q_lat, 16)); dg = np.where(wide, wrap(q_lng, 32), wrap(q_lng, 16))
write('shuffle', np.concatenate([header()] + planes(dl[~wide], 2) + planes(dg[~wide], 2) + planes(dl[wide], 4) + planes(dg[wide], 4)))
# native varint, open rings
keep = np.ones(N, bool); keep[vstart + cnt - 1] = False
def seq(q):
    d = np.diff(q, prepend=0); d[vstart] = q[vstart]; return d[keep]
write('varint', np.concatenate([header(), varint(zz(seq(q_lat))), varint(zz(seq(q_lng)))]))
if G:
    A = np.round(z['lat'].astype(np.float64) / G).astype(np.int64)[keep]
    B = np.round(z['lng'].astype(np.float64) / G).astype(np.int64)[keep]
    m = cnt - 1; st = np.concatenate([[0], np.cumsum(m)[:-1]])
    def gd(Q):
        d = np.diff(Q, prepend=0); f = Q[st]; d[st] = np.diff(f, prepend=0); return d
    a, b = gd(A), gd(B)
    write('gvarint', np.concatenate([varint(zz(a)), varint(zz(b))]))
    poly = np.repeat(np.arange(P), m)
    big = np.zeros(P, bool)
    np.logical_or.at(big, poly, (np.abs(a) > 32767) | (np.abs(b) > 32767))
    w = big[poly]
    write('gshuffle', np.concatenate([big.astype(np.uint8)] + planes(zz(a[~w]), 2) + planes(zz(b[~w]), 2) + planes(zz(a[w]), 4) + planes(zz(b[w]), 4)))
