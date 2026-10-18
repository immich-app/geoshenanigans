#!/usr/bin/env python3
"""Download-only regroupings of the abs2 footprint stream (sample): the same
fields, but each kind in its own stream so zstd sees like with like.
  s4 : tags | counts | bmin bytes | payloads (per polygon lat-then-lng)
  s6 : tags | counts | bmin | u8 lat | u8 lng | u16 lat lo,hi | u16 lng lo,hi
       (payload bytes of narrow and wide polygons split, byte planes for u16)"""
import os, sys, numpy as np
sys.path.insert(0, '/ssd/shrink/max/addresses')
from codecs_addr import *
from tierb import bmin_residual

W = '/ssd/shrink/max/addresses'
z = np.load(f'{W}/samp_sorted.npz')
rec, blob = z['rec'], z['blob']
O = f'{W}/exp/vert3'
os.makedirs(O, exist_ok=True)
def w(name, arr):
    np.ascontiguousarray(arr).view(np.uint8).tofile(f'{O}/{name}.bin')

p = Polys(rec, blob)
P = p.P
rl, rg = bmin_residual(p)
small = (np.abs(rl) < 128) & (np.abs(rg) < 128)
b8 = np.zeros((P, 8), np.uint8)
b8[:, 0] = rl.astype(np.int8).view(np.uint8); b8[:, 1] = rg.astype(np.int8).view(np.uint8)
raw = np.zeros((P, 8), np.uint8)
raw[:, 0:4] = p.bl.view(np.uint8).reshape(P, 4); raw[:, 4:8] = p.bg.view(np.uint8).reshape(P, 4)
b8 = np.where(small[:, None], b8, raw)
bml = np.where(small, 2, 8)
bm = b8[np.arange(8)[None, :] < bml[:, None]]
wa, pay, payl = p.abs_width(split=True)
tag = (p.enc.astype(np.uint8) | ((wa == 2).astype(np.uint8) << 2) | (small.astype(np.uint8) << 3) | (p.closed.astype(np.uint8) << 4)).astype(np.uint8)
cntv, cntl = varint(p.ocnt)
one = np.ones(P, np.int64)
w('abs2_ondisk', interleave([(tag, one), (cntv, cntl), (bm, bml), (pay, payl)], P)[0])
w('s4', np.concatenate([tag, cntv, bm, pay]))
k = p.keep
vp = np.repeat(np.arange(P), p.ocnt)
ql, qg = p.ql[k], p.qg[k]
nar = (wa == 1)[vp]
parts = [tag, cntv, bm, ql[nar].astype(np.uint8), qg[nar].astype(np.uint8)]
for q in (ql[~nar], qg[~nar]):
    parts += [(q & 0xff).astype(np.uint8), (q >> 8).astype(np.uint8)]
w('s6', np.concatenate(parts))
print('written', P)
