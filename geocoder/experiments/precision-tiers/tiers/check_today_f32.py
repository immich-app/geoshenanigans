#!/usr/bin/env python3
"""Where the today's-format exact variant (encoding 3) does not decode to f32(exact): is it only where
f32 is finer than 1e-7 (|coordinate| < 2 deg), and how far from the exact value is the decode there?
usage: check_today_f32.py <today dir> <rings.e7> <rings.e7.idx>"""
import sys
import numpy as np

d, e7, ix = sys.argv[1:4]
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
p = np.fromfile(f"{d}/admin_polygons.bin", PDT)
vb = np.memmap(f"{d}/admin_vertices.bin", np.uint8, "r")
idx = np.fromfile(ix, "<u8").astype(np.int64)
ex = np.memmap(e7, "<i4", "r").reshape(-1, 2)
tot = bad = bad_far = 0
worst_far = 0.0
worst_near = 0.0
CH = 50_000
for r0 in range(0, len(p), CH):
    r1 = min(len(p), r0 + CH)
    off = p["off"][r0:r1].astype(np.int64)
    cnt = p["cnt"][r0:r1].astype(np.int64)
    assert (np.asarray(vb[off]) == 3).all()
    hdr = np.asarray(vb[(off[:, None] + np.arange(2, 10)).ravel()]).reshape(-1, 8).copy()
    bl = hdr[:, 0:4].view("<f4").ravel().astype(np.float64)
    bg = hdr[:, 4:8].view("<f4").ravel().astype(np.float64)
    vp = np.repeat(np.arange(r1 - r0), cnt)
    k = np.arange(int(cnt.sum())) - np.repeat(np.concatenate([[0], np.cumsum(cnt)[:-1]]), cnt)
    pos = off[vp] + 10 + 8 * k
    raw = np.asarray(vb[(pos[:, None] + np.arange(8)).ravel()]).reshape(-1, 8).copy()
    q = raw.view("<u4").reshape(-1, 2).astype(np.float64)
    dec = np.stack([(bl[vp] + q[:, 0] * 1e-7).astype(np.float32), (bg[vp] + q[:, 1] * 1e-7).astype(np.float32)], 1)
    exa = np.asarray(ex[idx[r0]:idx[r1]]).astype(np.float64) * 1e-7
    f32 = exa.astype(np.float32)
    diff = dec != f32
    tot += diff.size
    bad += int(diff.sum())
    far = diff & (np.abs(exa) >= 2.0)
    bad_far += int(far.sum())
    if far.any():
        worst_far = max(worst_far, float(np.abs(dec.astype(np.float64) - exa)[far].max()))
    near = diff & (np.abs(exa) < 2.0)
    if near.any():
        worst_near = max(worst_near, float(np.abs(dec.astype(np.float64) - exa)[near].max()))
print(f"coordinates {tot:,}: decode != f32(exact) {bad:,} ({bad / tot:.2%}); of those with |coord| >= 2 deg: {bad_far:,} "
      f"(worst |decode - exact| there {worst_far:.3g} deg); below 2 deg worst |decode - exact| {worst_near:.3g} deg")
