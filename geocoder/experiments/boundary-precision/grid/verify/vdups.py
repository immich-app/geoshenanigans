#!/usr/bin/env python3
"""Consecutive equal (q_lat, q_lng) pairs per storage encoding in one admin_vertices set.
usage: vdups.py <dir>"""
import sys
import numpy as np

d = sys.argv[1]
NO = 0xFFFFFFFF
H = 10
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("rest", "V16")])
p = np.fromfile(f"{d}/admin_polygons.bin", PDT)
vb = np.memmap(f"{d}/admin_vertices.bin", np.uint8, "r")
live = (p["cnt"] > 0) & (p["off"] != NO)
dups = {e: 0 for e in range(4)}
verts = {e: 0 for e in range(4)}
for off, cnt in zip(p["off"][live].tolist(), p["cnt"][live].tolist()):
    e = int(vb[off])
    q = np.frombuffer(vb, "<u4" if e == 3 else "<u4", cnt, off + H) if e != 3 else np.frombuffer(vb, "<u8", cnt, off + H)
    dups[e] += int(np.count_nonzero(q[1:] == q[:-1]))
    verts[e] += cnt
print(d, "consecutive duplicate vertices by storage enc (0=1e-5,1=1e-4,2=1e-6,3=1e-7):", dups, "vertices:", verts)
