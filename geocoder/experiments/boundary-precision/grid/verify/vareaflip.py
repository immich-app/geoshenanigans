#!/usr/bin/env python3
"""Slots whose stored area changes sign class (<= 0 vs > 0) between source and variant.
usage: vareaflip.py <src_dir> <var_dir>"""
import sys
import numpy as np

sd, vd = sys.argv[1:3]
NO = 0xFFFFFFFF
H = 10
SC = [1e-5, 1e-4, 1e-6, 1e-7]
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
ps, pv = np.fromfile(f"{sd}/admin_polygons.bin", PDT), np.fromfile(f"{vd}/admin_polygons.bin", PDT)
vs, vv = np.memmap(f"{sd}/admin_vertices.bin", np.uint8, "r"), np.memmap(f"{vd}/admin_vertices.bin", np.uint8, "r")


def ring(vb, off, cnt):
    enc = int(vb[off])
    bml = float(np.frombuffer(vb, "<f4", 1, off + 2)[0]); bmg = float(np.frombuffer(vb, "<f4", 1, off + 6)[0])
    q = np.frombuffer(vb, "<u4" if enc == 3 else "<u2", 2 * cnt, off + H).reshape(cnt, 2).astype(np.float64)
    return enc, (bml + q[:, 0] * SC[enc]).astype(np.float32).astype(np.float64), (bmg + q[:, 1] * SC[enc]).astype(np.float32).astype(np.float64)


def shoelace(x, y):
    x = x - x[0]; y = y - y[0]
    return abs(float(np.sum(x[:-1] * y[1:] - x[1:] * y[:-1]))) / 2


live = (ps["cnt"] > 0) & (ps["off"] != NO)
for i in np.nonzero(live & ((ps["area"] > 0) != (pv["area"] > 0)))[0]:
    es, sla, slo = ring(vs, int(ps["off"][i]), int(ps["cnt"][i]))
    ev, vla, vlo = ring(vv, int(pv["off"][i]), int(pv["cnt"][i]))
    span = max(np.ptp(sla) * 111320, np.ptp(slo) * 111320 * np.cos(np.radians(sla[0])))
    print(f"slot {i} L{ps['lvl'][i]} cc {ps['cc'][i]} | src cnt {ps['cnt'][i]} area {ps['area'][i]:.3e} own shoelace {shoelace(sla, slo):.3e} "
          f"span {span:.0f} m | var cnt {pv['cnt'][i]} area {pv['area'][i]:.3e} own shoelace {shoelace(vla, vlo):.3e} "
          f"| first src verts {list(zip(sla.round(6).tolist(), slo.round(6).tolist()))[:4]}")
