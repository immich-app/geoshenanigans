#!/usr/bin/env python3
"""dbg_arc.py <arc dir> <lat> <lng> <cc> <level> : per-arc parity of the polygons of one country code
and level at a point, with and without the bbox skip (float32 as the reader)."""
import json
import struct
import sys

import numpy as np

d, lat, lng, cc, lvl = sys.argv[1], np.float32(sys.argv[2]), np.float32(sys.argv[3]), sys.argv[4], int(sys.argv[5])
POLY = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                 ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("holes", "<u2")])
polys = np.fromfile(f"{d}/admin_polygons.bin", dtype=POLY)
idx = np.fromfile(f"{d}/arc_index.bin", dtype="<u4")
av = open(f"{d}/arc_vertices.bin", "rb").read()
pa = open(f"{d}/poly_arcs.bin", "rb").read()
q = json.load(open(f"{d}/arc_meta.json"))["q"]
code = (ord(cc[0]) << 8) | ord(cc[1])


def f32(x):
    return np.float32(np.float64(x) * q)


def arc(a):
    b = av[idx[a]:idx[a + 1]]
    fl = b[0]
    w = 1 << (fl & 3)
    la0, ln0 = struct.unpack_from("<ii", b, 1)
    hdr = 9
    bb = None
    if fl & 0x80:
        bb = struct.unpack_from("<iiii", b, 9)
        hdr = 25
    m = (len(b) - hdr) // (2 * w)
    las, lns = [la0], [ln0]
    for i in range(m):
        zl = sum(b[hdr + k * m + i] << (8 * k) for k in range(w))
        zg = sum(b[hdr + (w + k) * m + i] << (8 * k) for k in range(w))
        las.append(las[-1] + ((zl >> 1) ^ -(zl & 1)))
        lns.append(lns[-1] + ((zg >> 1) ^ -(zg & 1)))
    return [f32(x) for x in las], [f32(x) for x in lns], bb


def parity(las, lns):
    ins = False
    for i in range(1, len(las)):
        vi, vj = (las[i], lns[i]), (las[i - 1], lns[i - 1])
        if (vi[1] > lng) != (vj[1] > lng):
            x = np.float32(np.float32(np.float32(vj[0] - vi[0]) * np.float32(lng - vi[1])) / np.float32(vj[1] - vi[1])) + vi[0]
            if lat < np.float32(x):
                ins = not ins
    return ins


for k, p in enumerate(polys):
    if p["lvl"] != lvl or p["cc"] != code or p["off"] == 0xFFFFFFFF:
        continue
    rings = int(p["holes"]) + 1
    pos = int(p["off"])
    tot, tot_skip = False, False
    nar = 0
    for r in range(rings):
        (cnt,) = struct.unpack_from("<I", pa, pos)
        pos += 4
        for _ in range(cnt):
            (ref,) = struct.unpack_from("<I", pa, pos)
            pos += 4
            a = ref >> 1
            las, lns, bb = arc(a)
            par = parity(las, lns)
            skip = False
            if bb:
                mnl, mxl, mng, mxg = (f32(v) for v in bb)
                skip = lng < mng or lng >= mxg or lat > mxl + np.float32(1e-4)
                real = (min(las), max(las), min(lns), max(lns))
                if (mnl, mxl, mng, mxg) != real:
                    print(f"  arc {a}: stored bbox {(mnl, mxl, mng, mxg)} != decoded {real}")
            if skip and par:
                print(f"  arc {a}: SKIPPED but parity 1 (n={len(las)}, bbox {bb})")
            tot ^= par
            tot_skip ^= (par and not skip)
            nar += 1
    print(f"polygon slot {k} area {p['area']}: {nar} arcs, parity {tot}, with bbox skip {tot_skip}")
