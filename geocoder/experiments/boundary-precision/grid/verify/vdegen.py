#!/usr/bin/env python3
"""Rings whose decoded variant geometry has zero shoelace area while the source's stored area is > 0.
usage: vdegen.py <src_dir> <var_dir> <G>  -> one line per such ring:
slot, level, cc, src cnt/enc/stored area/own shoelace, var cnt/enc/stored area, exact integer shoelace on the
G grid (round(v/G)), distinct vertices, a few vertices."""
import sys
import numpy as np

sd, vd, G = sys.argv[1], sys.argv[2], float(sys.argv[3])
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
    lat = (bml + q[:, 0] * SC[enc]).astype(np.float32).astype(np.float64)
    lng = (bmg + q[:, 1] * SC[enc]).astype(np.float32).astype(np.float64)
    return enc, lat, lng


def shoelace(x, y):
    x = x - x[0]; y = y - y[0]
    return abs(float(np.sum(x[:-1] * y[1:] - x[1:] * y[:-1]))) / 2


live = (pv["cnt"] > 0) & (pv["off"] != NO) & (ps["area"] > 0)
n = 0
for i in np.nonzero(live)[0]:
    if pv["cnt"][i] > 400:   # degenerate snapped rings are tiny
        continue
    e, la, lo = ring(vv, int(pv["off"][i]), int(pv["cnt"][i]))
    if shoelace(la, lo) > 0:
        continue
    es, sla, slo = ring(vs, int(ps["off"][i]), int(ps["cnt"][i]))
    a = np.rint(la / G).astype(np.int64); b = np.rint(lo / G).astype(np.int64)
    exact = int(np.sum(a[:-1] * b[1:] - a[1:] * b[:-1]))
    distinct = len(set(zip(la.tolist(), lo.tolist())))
    span_m = max(np.ptp(sla) * 111320, np.ptp(slo) * 111320 * np.cos(np.radians(sla[0])))
    n += 1
    print(f"slot {i} L{ps['lvl'][i]} cc {ps['cc'][i]} | src cnt {ps['cnt'][i]} enc {es} area {ps['area'][i]:.3e} shoelace {shoelace(sla, slo):.3e} "
          f"bbox span {span_m:.1f} m | var cnt {pv['cnt'][i]} enc {e} stored area {pv['area'][i]:.3e} exact grid 2*area {exact} "
          f"distinct {distinct} | var verts {list(zip(la.round(6).tolist(), lo.round(6).tolist()))[:6]}")
print("rings:", n)
