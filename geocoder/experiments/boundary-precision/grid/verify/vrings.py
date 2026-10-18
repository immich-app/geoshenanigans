#!/usr/bin/env python3
"""Verifier ring checks: variant vs its source, slot by slot, decoded exactly as the reader does.

usage: vrings.py <name> <src_dir> <var_dir> <grid_deg or 0>   -> one JSON line
  meta      : every AdminPolygon field except vertex_offset/vertex_count/area is unchanged
  live      : the same slots are live (vertex_count > 0, offset != NO_DATA)
  area      : stored area > 0 in the variant wherever it is > 0 in the source; own shoelace on the
              decoded f32 vertices > 0 for every such ring
  closed    : decoded first vertex == decoded last vertex for every live ring
  distinct  : >= 3 distinct consecutive positions (closing vertex excluded) for every live ring
  dups      : consecutive equal decoded vertices (source vs variant)
  grid      : rings whose storage grid s is finer than G: all q multiples of G/s (on the global grid,
              relative to the exact double bbox min) vs not (= pre-snap geometry kept)
  disp      : rings with an unchanged vertex count: max movement of a decoded vertex (metres,
              equirectangular), max |dlat|, |dlng|"""
import json, sys
import numpy as np

name, sd, vd, G = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4])
NO = 0xFFFFFFFF
H = 10
SC = np.array([1e-5, 1e-4, 1e-6, 1e-7])
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("p4", "<u2")])
ps = np.fromfile(f"{sd}/admin_polygons.bin", PDT)
pv = np.fromfile(f"{vd}/admin_polygons.bin", PDT)
vs = np.memmap(f"{sd}/admin_vertices.bin", np.uint8, "r")
vv = np.memmap(f"{vd}/admin_vertices.bin", np.uint8, "r")
r = {"name": name, "grid": G, "slots": len(ps)}
assert len(ps) == len(pv)
r["meta_equal"] = bool(all(np.array_equal(ps[f], pv[f]) for f in ("name", "lvl", "pto", "p2", "p3", "cc", "p4")))
ls = (ps["cnt"] > 0) & (ps["off"] != NO)
lv = (pv["cnt"] > 0) & (pv["off"] != NO)
r["live_src"], r["live_var"], r["live_same_slots"] = int(ls.sum()), int(lv.sum()), bool(np.array_equal(ls, lv))
r["v_src"], r["v_var"] = int(ps["cnt"][ls].astype(np.int64).sum()), int(pv["cnt"][lv].astype(np.int64).sum())
r["area_le0_src"], r["area_le0_var"] = int((ls & ~(ps["area"] > 0)).sum()), int((lv & ~(pv["area"] > 0)).sum())
r["area_pos_src_le0_var"] = int((ls & (ps["area"] > 0) & ~(pv["area"] > 0)).sum())
r["area_le0_same_slots"] = bool(np.array_equal(ls & ~(ps["area"] > 0), lv & ~(pv["area"] > 0)))


def decode(vb, offs, cnts):
    """reader decode of consecutive blocks; returns enc per ring, q (2 x N int64), f32 lat/lng as f64, vstart."""
    lo = int(offs[0])
    enc0 = np.asarray(vb[offs]).astype(np.int64)
    w = np.where(enc0 == 3, 4, 2)
    hi = int(offs[-1] + H + cnts[-1] * 2 * w[-1])
    b = np.asarray(vb[lo:hi])
    o = offs - lo
    enc = b[o].astype(np.int64)
    bml = b[(o[:, None] + np.arange(2, 6)).ravel()].view("<f4").astype(np.float64)
    bmg = b[(o[:, None] + np.arange(6, 10)).ravel()].view("<f4").astype(np.float64)
    vstart = np.concatenate([[0], np.cumsum(cnts)[:-1]])
    vp = np.repeat(np.arange(len(cnts)), cnts)
    k = np.arange(int(cnts.sum())) - vstart[vp]
    wv = w[vp]
    pos = o[vp] + H + k * 2 * wv
    q = np.zeros((2, len(pos)), np.int64)
    for c in range(2):
        for by in range(4):
            s = by < wv
            q[c, s] |= b[pos[s] + c * wv[s] + by].astype(np.int64) << (8 * by)
    sc = SC[enc][vp]
    lat = (bml[vp] + q[0] * sc).astype(np.float32).astype(np.float64)
    lng = (bmg[vp] + q[1] * sc).astype(np.float32).astype(np.float64)
    return enc, q, lat, lng, vstart, vp


idx = np.nonzero(ls & lv)[0]
acc = dict(open_var=0, open_src=0, distinct_lt3_var=0, shoelace_le0_var_where_src_area_pos=0, dups_src=0, dups_var=0,
           finer_than_G=0, finer_on_grid=0, finer_off_grid=0, eqcount_rings=0, disp_max_m=0.0, dlat_max=0.0,
           dlng_max=0.0, count_lt4_var=0, enc_changed=0)
CH = 20000
for c0 in range(0, len(idx), CH):
    sl = idx[c0:c0 + CH]
    res = {}
    for tag, P, VB in (("s", ps, vs), ("v", pv, vv)):
        offs, cnts = P["off"][sl].astype(np.int64), P["cnt"][sl].astype(np.int64)
        assert np.all(np.diff(offs) > 0)
        enc, q, lat, lng, vstart, vp = decode(VB, offs, cnts)
        last = vstart + cnts - 1
        closed = (lat[vstart] == lat[last]) & (lng[vstart] == lng[last])
        same = np.zeros(len(lat), bool)
        same[1:] = (lat[1:] == lat[:-1]) & (lng[1:] == lng[:-1])
        same[vstart] = False
        dups = np.add.reduceat(same.astype(np.int64), vstart)
        res[tag] = dict(enc=enc, q=q, lat=lat, lng=lng, vstart=vstart, vp=vp, cnts=cnts, closed=closed, dups=dups, last=last)
    s, v = res["s"], res["v"]
    acc["open_var"] += int((~v["closed"]).sum())
    acc["open_src"] += int((~s["closed"]).sum())
    acc["dups_src"] += int(s["dups"].sum())
    acc["dups_var"] += int(v["dups"].sum())
    acc["count_lt4_var"] += int((v["cnts"] < 4).sum())
    acc["enc_changed"] += int((s["enc"] != v["enc"]).sum())
    # distinct consecutive positions, closing vertex excluded
    runs = v["cnts"] - v["dups"] - v["closed"].astype(np.int64)
    acc["distinct_lt3_var"] += int((runs < 3).sum())
    # shoelace on decoded f32 vertices, relative to the ring's first vertex
    x = v["lat"] - v["lat"][v["vstart"]][v["vp"]]
    y = v["lng"] - v["lng"][v["vstart"]][v["vp"]]
    t = np.zeros(len(x))
    t[:-1] = x[:-1] * y[1:] - x[1:] * y[:-1]
    t[v["last"]] = 0.0
    a2 = np.abs(np.add.reduceat(t, v["vstart"]))
    srcpos = ps["area"][sl] > 0
    acc["shoelace_le0_var_where_src_area_pos"] += int((srcpos & ~(a2 > 0)).sum())
    if G > 0:
        sc = SC[v["enc"]]
        finer = sc < G * (1 - 1e-9)
        kk = np.maximum(np.rint(G / sc).astype(np.int64), 1)
        mult = ((v["q"][0] % kk[v["vp"]]) == 0) & ((v["q"][1] % kk[v["vp"]]) == 0)
        allm = np.logical_and.reduceat(mult, v["vstart"])
        acc["finer_than_G"] += int(finer.sum())
        acc["finer_on_grid"] += int((finer & allm).sum())
        acc["finer_off_grid"] += int((finer & ~allm).sum())
    eq = s["cnts"] == v["cnts"]
    acc["eqcount_rings"] += int(eq.sum())
    if eq.any():
        me, mv = eq[s["vp"]], eq[v["vp"]]
        dla = np.abs(s["lat"][me] - v["lat"][mv])
        dln = np.abs(s["lng"][me] - v["lng"][mv])
        dm = np.hypot(dla * 111320.0, dln * 111320.0 * np.cos(np.radians(s["lat"][me])))
        acc["disp_max_m"] = max(acc["disp_max_m"], float(dm.max()))
        acc["dlat_max"] = max(acc["dlat_max"], float(dla.max()))
        acc["dlng_max"] = max(acc["dlng_max"], float(dln.max()))
r.update(acc)
print(json.dumps(r), flush=True)
