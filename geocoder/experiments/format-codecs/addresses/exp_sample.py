#!/usr/bin/env python3
"""Write candidate encodings of a sample (samp_<tag>.npz) to exp/<tag>/*.bin.
Sizes then come from zstd -18 -T1 per file (zall.sh)."""
import os, sys, numpy as np
sys.path.insert(0, '/ssd/shrink/max/addresses')
from codecs_addr import *

tag = sys.argv[1]
W = '/ssd/shrink/max/addresses'
z = np.load(f'{W}/samp_{tag}.npz')
rec, pc, blob = z['rec'], z['pc'], z['blob']
O = f'{W}/exp/{tag}'
os.makedirs(O, exist_ok=True)
def w(name, arr):
    np.ascontiguousarray(arr).view(np.uint8).tofile(f'{O}/{name}.bin')

N = len(rec)
# ----------------------------------------------------------- addr_points
w('p0_row28', rec)
for k, nm in enumerate(['lat', 'lng', 'hn', 'st', 'pw', 'vo', 'vc']):
    w(f'pc_{nm}', rec[:, k].copy())
for k, nm in enumerate(['lat', 'lng']):
    b = rec[:, k].copy().view(np.uint8).reshape(-1, 4)
    w(f'pc_{nm}_planes', b.T.copy())
for k, nm in ((2, 'hn'), (3, 'st')):
    u, inv = np.unique(rec[:, k], return_inverse=True)
    w(f'pc_{nm}_u24', inv.astype('<u4').view(np.uint8).reshape(-1, 4)[:, :3].copy())
    w(f'pc_{nm}_dict', u)
w('pc_pc', pc)
w('pc_pc_planes', pc.copy().view(np.uint8).reshape(-1, 4).T.copy())
# download-only transforms
for k, nm in enumerate(['lat', 'lng']):
    d = np.diff(fkey(rec[:, k]), prepend=0)
    zd = zz(d)
    assert zd.max() < 2**40
    w(f'pd_{nm}_dzz_planes', zd.astype('<u8').view(np.uint8).reshape(-1, 8)[:, :5].T.copy())
    b, _ = varint(zd); w(f'pd_{nm}_dzz_varint', b)
for k, nm in ((2, 'hn'), (3, 'st'), (4, 'pw')):
    d = np.diff(rec[:, k].astype(np.int64), prepend=0)
    b, _ = varint(zz(d)); w(f'pd_{nm}_dzz_varint', b)
    w(f'pd_{nm}_dzz_planes', zz(d).astype('<u8').view(np.uint8).reshape(-1, 8)[:, :5].T.copy())
d = np.diff(pc.astype(np.int64), prepend=0)
b, _ = varint(zz(d)); w('pd_pc_dzz_varint', b)

# ----------------------------------------------------------- addr_vertices
p = Polys(rec, blob)
P = p.P
print('polygons', P, 'closed', int(p.closed.sum()))
w('v0_today', p.today()[0])
assert np.array_equal(p.today()[0], blob), 'today re-encode mismatch'
cntv, cntl = varint(p.ocnt)
short, bm16, bm16l = p.hdr_bmin_delta()
bmv, bmvl = p.hdr_bmin_varint()
bmr, bmrl = p.hdr_bmin_raw()
print('bmin short', float(short.mean()))
encb = p.enc.astype(np.uint8)
# today header + open ring u16 (count still in record)
hdr10 = np.zeros((P, 10), np.uint8); hdr10[:, 0] = p.enc
hdr10[:, 2:10] = bmr.reshape(P, 8)
k = p.keep
v16 = np.stack([p.ql[k], p.qg[k]], 1).astype('<u2').view(np.uint8).ravel()
w('v1_today_open', interleave([(hdr10.ravel(), np.full(P, 10)), (v16, 4 * p.ocnt)], P)[0])
# tag = enc | width<<2 | short<<3
wa, abs_b, abs_l = p.abs_width()
tag_abs = (encb | ((wa == 2) << 2) | (short << 3)).astype(np.uint8)
w('v2_tag_cnt_b16_abs', interleave([(tag_abs, np.ones(P)), (cntv, cntl), (bm16, bm16l), (abs_b, abs_l)], P)[0])
wa2, abs_s, abs_sl = p.abs_width(split=True)
w('v3_tag_cnt_b16_abssplit', interleave([(tag_abs, np.ones(P)), (cntv, cntl), (bm16, bm16l), (abs_s, abs_sl)], P)[0])
w('v3r_tag_cnt_braw_abs', interleave([(tag_abs, np.ones(P)), (cntv, cntl), (bmr, bmrl), (abs_b, abs_l)], P)[0])
for pred in ('seq', 'pgram'):
    vb, vl = p.varint_payload(pred)
    w(f'v4_tag_cnt_bvar_var_{pred}', interleave([(encb, np.ones(P)), (cntv, cntl), (bmv, bmvl), (vb, vl)], P)[0])
    for planes in (True, False):
        wr, rb, rl = p.fixed_res_payload(pred, planes)
        tg = (encb | ((wr == 2) << 2) | (short << 3)).astype(np.uint8)
        nm = f'v5_tag_cnt_b16_fix_{pred}_{"planes" if planes else "inter"}'
        w(nm, interleave([(tg, np.ones(P)), (cntv, cntl), (bm16, bm16l), (rb, rl)], P)[0])
    # download-only: separate streams over the whole file
    rlat, rlng, idx, oc = p.residuals(pred)
    for nm2, arr in (('tag', encb), ('cnt', cntv), ('bvar', bmv)):
        w(f'vd_{pred}_{nm2}', arr)
    w(f'vd_{pred}_lat_var', varint(zz(rlat))[0]); w(f'vd_{pred}_lng_var', varint(zz(rlng))[0])
    for nm2, r in (('lat', rlat), ('lng', rlng)):
        zr = zz(r).astype('<u2')
        w(f'vd_{pred}_{nm2}_planes', zr.view(np.uint8).reshape(-1, 2).T.copy())
print('written', O)
