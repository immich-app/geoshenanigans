"""poi_vertices.bin re-encodings, lossless (every polygon's header and q values round-trip; the
decoder rebuilds today's file and is compared byte-for-byte over the WHOLE file).
Layouts (functions from /ssd/shrink/stack/layouts.py, copied to scripts/layouts.py unchanged):
  B   per-polygon block at today's offset and size: header verbatim, wrapping zigzag deltas at the
      polygon's own width as byte planes inside the block (on-disk, random access per polygon)
  Bg  B + per-polygon divisor of the q values in the header pad byte
  An  all headers in one block, deltas as byte planes over the whole file (download-only)
Also: closure statistics (first vertex == last vertex in q space), the B-open variant (closing
vertex dropped when equal to the first, 1 flag bit; decoder restores it) and its zstd input.
usage: poi_vert.py"""
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
assert np.all(np.diff(off) > 0)
vb = np.fromfile(f"{D}/poi_vertices.bin", np.uint8)
out = f"{W}/poi_vert"
os.makedirs(out, exist_ok=True)

enc, w, vstart, vpoly, k, wv, q = L.read_today(vb, off, cnt)
last = vstart + cnt - 1
closed = (q[0][last] == q[0][vstart]) & (q[1][last] == q[1][vstart]) & (cnt > 1)
print(f"polygons {len(cnt):,} vertices {len(k):,}; rings closed in q space: {int(closed.sum()):,} ({closed.mean()*100:.2f}%)", flush=True)
print("vertex count == 1:", int((cnt == 1).sum()), " == 2:", int((cnt == 2).sum()), " min", int(cnt.min()))
del k, vpoly, wv

for fmt in ["B", "Bg", "An"]:
    b = L.ENC[fmt](vb, off, cnt)
    b.tofile(f"{out}/{fmt}.bin")
    back = L.DEC[fmt](b, off, cnt)
    same = len(back) == len(vb) and bool(np.array_equal(back, vb))
    print(f"{fmt}: {len(b):,} bytes; decode == today's poi_vertices.bin byte-for-byte: {same}", flush=True)
    assert same
    del b, back

# B-open: drop the closing vertex of closed rings; header pad byte bit 0 = "closing vertex omitted".
keep = np.ones(len(q[0]), bool)
keep[last[closed]] = False
cnt2 = cnt - closed.astype(np.int64)
w = L.width(enc)
size2 = 10 + 2 * w * cnt2
off2 = np.concatenate([[0], np.cumsum(size2)[:-1]])
vstart2 = np.concatenate([[0], np.cumsum(cnt2)[:-1]])
vpoly2 = np.repeat(np.arange(len(cnt2)), cnt2)
k2 = np.arange(int(cnt2.sum())) - vstart2[vpoly2]
wv2 = w[vpoly2]
q2 = [q[0][keep], q[1][keep]]
ob = np.zeros(int(size2.sum()), np.uint8)
hp_old = (off[:, None] + np.arange(10)).ravel()
hp_new = (off2[:, None] + np.arange(10)).ravel()
ob[hp_new] = vb[hp_old]
assert np.all(vb[off + 1] == 0)
ob[off2 + 1] = closed.astype(np.uint8)
c2 = cnt2[vpoly2]
base = off2[vpoly2] + 10
for comp in range(2):
    z = L.wrap_delta(q2[comp], vstart2, wv2)
    L.put_planes(ob, lambda bb, s: base[s] + (comp * wv2[s] + bb) * c2[s] + k2[s], z, wv2)
ob.tofile(f"{out}/Bopen.bin")
# decode B-open back to today's bytes
z = [np.zeros(len(k2), np.int64) for _ in range(2)]
for comp in range(2):
    for bb in range(4):
        s = bb < wv2
        z[comp][s] |= ob[base[s] + (comp * wv2[s] + bb) * c2[s] + k2[s]].astype(np.int64) << (8 * bb)
qq = [L.unwrap_delta(zc, vstart2, vpoly2, wv2) for zc in z]
cl = ob[off2 + 1].astype(bool)
full = [np.zeros(int(cnt.sum()), np.int64) for _ in range(2)]
vstart_r = np.concatenate([[0], np.cumsum(cnt)[:-1]])
pos = np.repeat(vstart_r, cnt2) + k2
for comp in range(2):
    full[comp][pos] = qq[comp]
    full[comp][vstart_r[cl] + cnt[cl] - 1] = full[comp][vstart_r[cl]]
rec = np.zeros_like(vb)
rec[hp_old] = ob[hp_new]
rec[off + 1] = 0
vpoly_r = np.repeat(np.arange(len(cnt)), cnt)
k_r = np.arange(int(cnt.sum())) - vstart_r[vpoly_r]
L.write_today(rec, off, k_r, vpoly_r, w[vpoly_r], full)
same = bool(np.array_equal(rec, vb))
print(f"Bopen: {len(ob):,} bytes; decode == today's poi_vertices.bin byte-for-byte: {same}", flush=True)
assert same
