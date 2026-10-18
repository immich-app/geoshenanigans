"""Download-only An-open for poi_vertices.bin: An (all headers in one block, wrapping zigzag
deltas, byte planes over the whole file) with every ring's closing vertex dropped (all 8,625,257
POI rings are closed in q space; the decoder re-appends the first vertex). Decoded back to
today's file and compared byte-for-byte.
usage: poi_vert2.py"""
import os
import sys
import numpy as np
sys.path.insert(0, os.path.dirname(__file__))
import layouts as L
from lists import *

POI_DT = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("vo", "<u4"), ("vc", "<u4"), ("name_id", "<u4"), ("c", "u1"),
                   ("t", "u1"), ("f", "u1"), ("imp", "u1"), ("ps", "<u4"), ("pc", "<u4"), ("pp", "<u4")])
p = np.fromfile(f"{D}/poi_records.bin", POI_DT)
poly = (p["vc"] > 0) & (p["vo"] != NO)
off = p["vo"][poly].astype(np.int64)
cnt = p["vc"][poly].astype(np.int64)
vb = np.fromfile(f"{D}/poi_vertices.bin", np.uint8)
enc, w, vstart, vpoly, k, wv, q = L.read_today(vb, off, cnt)
last = vstart + cnt - 1
assert np.all((q[0][last] == q[0][vstart]) & (q[1][last] == q[1][vstart]))
keep = np.ones(len(k), bool)
keep[last] = False
cnt2 = cnt - 1
vstart2 = np.concatenate([[0], np.cumsum(cnt2)[:-1]])
wv2 = w[np.repeat(np.arange(len(cnt2)), cnt2)]
H = 10
parts = [vb[(off[:, None] + np.arange(H)).ravel()]]
z = [L.wrap_delta(x[keep], vstart2, wv2) for x in q]
for width_ in (2, 4):
    s = wv2 == width_
    for comp in range(2):
        for b in range(width_):
            parts.append(((z[comp][s] >> (8 * b)) & 0xFF).astype(np.uint8))
ab = np.concatenate(parts)
os.makedirs(f"{W}/poi_vert", exist_ok=True)
ab.tofile(f"{W}/poi_vert/Anopen.bin")
# decode
P = len(cnt)
hdr = ab[:P * H].reshape(P, H)
w_d = L.width(hdr[:, 0].astype(np.int64))
vpoly2 = np.repeat(np.arange(P), cnt2)
wvd = w_d[vpoly2]
pos = P * H
zz_ = [np.zeros(len(vpoly2), np.int64), np.zeros(len(vpoly2), np.int64)]
for width_ in (2, 4):
    s = wvd == width_
    n = int(s.sum())
    for comp in range(2):
        for b in range(width_):
            zz_[comp][s] |= ab[pos:pos + n].astype(np.int64) << (8 * b)
            pos += n
assert pos == len(ab)
qo = [L.unwrap_delta(x, vstart2, vpoly2, wvd) for x in zz_]
full = []
for comp in range(2):
    f = np.zeros(int(cnt.sum()), np.int64)
    f[np.repeat(vstart, cnt2) + (np.arange(len(vpoly2)) - vstart2[vpoly2])] = qo[comp]
    f[vstart + cnt - 1] = f[vstart]
    full.append(f)
toff = np.concatenate([[0], np.cumsum(H + 2 * w_d * cnt)[:-1]])
assert np.array_equal(toff, off)
out = np.zeros(len(vb), np.uint8)
out[(off[:, None] + np.arange(H)).ravel()] = hdr.ravel()
vp = np.repeat(np.arange(P), cnt)
kk = np.arange(int(cnt.sum())) - vstart[vp]
L.write_today(out, off, kk, vp, w_d[vp], full)
same = bool(np.array_equal(out, vb))
print(f"Anopen: {len(ab):,} bytes; decode == today's poi_vertices.bin byte-for-byte: {same}")
assert same
