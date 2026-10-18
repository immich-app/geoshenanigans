#!/usr/bin/env python3
"""Second round of footprint encodings on the sorted sample:
  bmin predicted from the point minus the mean q (closing vertex included,
  like the builder's centroid) -> residual in ulps;
  rectangles (4 stored vertices): 4th vertex as parallelogram residual."""
import os, sys, numpy as np
sys.path.insert(0, '/ssd/shrink/max/addresses')
from codecs_addr import *

W = '/ssd/shrink/max/addresses'
z = np.load(f'{W}/samp_sorted.npz')
rec, blob = z['rec'], z['blob']
O = f'{W}/exp/vert2'
os.makedirs(O, exist_ok=True)
def w(name, arr):
    np.ascontiguousarray(arr).view(np.uint8).tofile(f'{O}/{name}.bin')

p = Polys(rec, blob)
P = p.P
print('open count distribution', np.bincount(np.minimum(p.ocnt, 12)))


def bmin_pred(plat_bits, ql, vs, cnt):
    """f32(point - mean(q incl. closing vertex) * 1e-6), all in f64."""
    s = np.add.reduceat(ql.astype(np.int64), vs)  # closed ring sum (stored incl closing)
    mean = s.astype(np.float64) / cnt.astype(np.float64)
    return (plat_bits.view(np.float32).astype(np.float64) - mean * 1e-6).astype(np.float32).view(np.uint32)


pl = bmin_pred(p.plat, p.ql, p.vs, p.cnt)
pg = bmin_pred(p.plng, p.qg, p.vs, p.cnt)
rl = fkey(p.bl) - fkey(pl); rg = fkey(p.bg) - fkey(pg)
for nm, r in (('lat', rl), ('lng', rg)):
    a = np.abs(r)
    print(f'bmin {nm} residual ulps: |r|<=1 {np.mean(a <= 1):.4f} <=3 {np.mean(a <= 3):.4f} <128 {np.mean(a < 128):.4f} '
          f'pct50/99/99.9 {np.percentile(a, [50, 99, 99.9])} max {a.max()}')
bpv, bpl = varint(zz(np.stack([rl, rg], 1).ravel()))
bp_len = bpl[0::2] + bpl[1::2]
w('h_bpred_var', bpv)
bv, bvl = p.hdr_bmin_varint()
w('h_bvar', bv)
# i8 pair with escape (tag bit) else raw f32 pair
small = (np.abs(rl) < 128) & (np.abs(rg) < 128)
b8 = np.zeros((P, 8), np.uint8)
b8[:, 0] = rl.astype(np.int8).view(np.uint8); b8[:, 1] = rg.astype(np.int8).view(np.uint8)
b8[:, 0:4] = np.where(small[:, None], b8[:, 0:4], p.bl.view(np.uint8).reshape(P, 4))
b8[:, 4:8] = np.where(small[:, None], 0, p.bg.view(np.uint8).reshape(P, 4))
l8 = np.where(small, 2, 8)
m8 = np.arange(8)[None, :] < l8[:, None]
w('h_bpred_i8', b8[m8])
print('bpred i8 fits', small.mean())

encb = p.enc.astype(np.uint8)
cntv, cntl = varint(p.ocnt)
wa, abs_s, abs_sl = p.abs_width(split=True)
tag = (encb | ((wa == 2) << 2) | (small << 3)).astype(np.uint8)
short, bm16, bm16l = p.hdr_bmin_delta()
one = np.ones(P, np.int64)
w('v3_b16_abssplit', interleave([(tag, one), (cntv, cntl), (bm16, bm16l), (abs_s, abs_sl)], P)[0])
w('v6_bpredvar_abssplit', interleave([(tag, one), (cntv, cntl), (bpv, bp_len), (abs_s, abs_sl)], P)[0])
w('v6_bpredi8_abssplit', interleave([(tag, one), (cntv, cntl), (b8[m8], l8), (abs_s, abs_sl)], P)[0])
wb, abs_b, abs_bl = p.abs_width(split=False)
w('v6_bpredi8_abs', interleave([(tag, one), (cntv, cntl), (b8[m8], l8), (abs_b, abs_bl)], P)[0])
# header fields grouped: tag|cnt|bmin first for all? (not random access) -> skip
# rectangles: 4 stored vertices, 4th as parallelogram residual (zigzag u8 when small)
k = p.keep
ql = p.ql[k].astype(np.int64); qg = p.qg[k].astype(np.int64)
oc = p.ocnt
vs = np.concatenate([[0], np.cumsum(oc)[:-1]])
rect = oc == 4
r4l = ql[vs[rect] + 3] - (ql[vs[rect] + 2] + ql[vs[rect]] - ql[vs[rect] + 1])
r4g = qg[vs[rect] + 3] - (qg[vs[rect] + 2] + qg[vs[rect]] - qg[vs[rect] + 1])
a = np.maximum(np.abs(r4l), np.abs(r4g))
print('rectangles', int(rect.sum()), 'of', P, '4th-vertex residual |r|<=1', np.mean(a <= 1), '<=3', np.mean(a <= 3), '<64', np.mean(a < 64))
# rect variant: for rect polygons with |r|<64 store 3 vertices abs + residual pair (zz u8 each)
rr = np.zeros(P, bool); rr[np.nonzero(rect)[0][a < 64]] = True
keep2 = np.ones(len(ql), bool)
keep2[vs[rr] + 3] = False
oc2 = oc - rr
# rebuild abs split payload on the reduced vertex set
vp = np.repeat(np.arange(P), oc)[keep2]
ql2, qg2 = ql[keep2], qg[keep2]
vs2 = np.concatenate([[0], np.cumsum(oc2)[:-1]])
idx2 = np.arange(len(ql2)) - np.repeat(vs2, oc2)
span = np.maximum(np.maximum.reduceat(p.ql, p.vs), np.maximum.reduceat(p.qg, p.vs))
wd = np.where(span < 256, 1, 2)
size = 2 * oc2 * wd
pstart = np.concatenate([[0], np.cumsum(size)[:-1]])
out = np.zeros(int(size.sum()), np.uint8)
wv = wd[vp]; ocv = oc2[vp]; ps = pstart[vp]
bl_ = ps + idx2 * wv; bg_ = ps + ocv * wv + idx2 * wv
out[bl_] = ql2 & 0xff; out[bg_] = qg2 & 0xff
two = wv == 2
out[bl_[two] + 1] = ql2[two] >> 8; out[bg_[two] + 1] = qg2[two] >> 8
res = np.zeros((P, 2), np.uint8)
ri = np.nonzero(rr)[0]
amask = a < 64
res[ri, 0] = zz(r4l[amask]).astype(np.uint8); res[ri, 1] = zz(r4g[amask]).astype(np.uint8)
rlen = np.where(rr, 2, 0)
rm = np.arange(2)[None, :] < rlen[:, None]
tag2 = (tag | (rr.astype(np.uint8) << 4)).astype(np.uint8)
w('v7_bpredi8_abssplit_rect', interleave([(tag2, one), (cntv, cntl), (b8[m8], l8), (out, size), (res[rm], rlen)], P)[0])
print('done')
