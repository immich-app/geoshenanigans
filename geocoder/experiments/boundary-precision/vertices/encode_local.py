#!/usr/bin/env python3
"""Per-polygon (patch-compatible) fixed-width layouts: each polygon keeps one
contiguous block = 10-byte header + its vertices, but inside the block the
values are wrapping zigzag deltas stored as byte planes:
  e3d: [lat lo bytes][lat hi bytes][lng lo bytes][lng hi bytes]  (u32: 4+4 planes)
  e3e: same planes but on absolute values (today's numbers, only reordered)
Raw size is identical to today; only zstd sees a difference."""
import numpy as np
z = np.load('decoded.npz')
enc, cnt, vstart, vpoly = z['enc'], z['cnt'], z['vstart'], z['vpoly']
q_lat, q_lng = z['q_lat'], z['q_lng']
P, N = len(cnt), len(q_lat)
wide = (enc == 3)
W = np.where(wide, 4, 2)                      # bytes per component
def zz(x): x = x.astype(np.int64); return np.where(x >= 0, 2 * x, -2 * x - 1)
def wrap(q, bits):
    d = np.diff(q, prepend=0); d[vstart] = q[vstart]; m = 1 << bits
    d = np.mod(d, m); return zz(np.where(d >= m // 2, d - m, d))
def build(vl, vg, name):
    out = np.zeros(10 * P + int((2 * W * cnt).sum()), np.uint8)
    blk = 10 + 2 * W * cnt
    off = np.concatenate([[0], np.cumsum(blk)[:-1]])
    h = np.zeros((P, 10), np.uint8); h[:, 0] = enc
    h[:, 2:6] = z['bmin_lat'].view(np.uint8).reshape(-1, 4); h[:, 6:10] = z['bmin_lng'].view(np.uint8).reshape(-1, 4)
    out[(off[:, None] + np.arange(10)).ravel()] = h.ravel()
    k = np.arange(N) - vstart[vpoly]          # vertex index inside polygon
    c = cnt[vpoly]; w = W[vpoly]; base = off[vpoly] + 10
    for comp, vals in ((0, vl), (1, vg)):
        v = vals.astype(np.uint64)
        for byte in range(4):
            sel = byte < w
            plane = comp * w + byte            # plane index inside the block
            pos = base + plane * c + k
            out[pos[sel]] = ((v[sel] >> np.uint64(8 * byte)) & np.uint64(0xff)).astype(np.uint8)
    out.tofile(f'enc/{name}.bin'); print(name, len(out))
w32 = wide[vpoly]
build(np.where(w32, wrap(q_lat, 32), wrap(q_lat, 16)), np.where(w32, wrap(q_lng, 32), wrap(q_lng, 16)), 'e3d_local_wrapdelta_planes')
build(q_lat, q_lng, 'e3e_local_abs_planes')
