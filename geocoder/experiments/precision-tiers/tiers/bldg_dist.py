#!/usr/bin/env python3
"""Address distance answers against each test point's own building, per building variant, against the
exact outline in f64. The reader's address distance is polygon_distance_sq (server/src/lib.rs): 0 when
the point is inside the outline, else the nearest edge distance (equirectangular at the query
latitude); here the same in f64 metres (R = 6,371,008.8 m) for:
  exact     OSM's outline, f64                                 (reference)
  exact32   f32(exact): what an exact tier shows today's reader
  k20       today's capped outline (out-base), as the reader decodes it (f32)
  unc       today's grid, uncapped (out-nocap), as the reader decodes it (f32)
  mx        Maximum: exact snapped to the global 1e-6 grid, f64 (a reader decoding to f64/i32)
  mx32      Maximum as today's reader decodes it (work/bldg/today_b, f32)
Per pool and variant: share of points whose inside / outside status differs from exact, and
|distance - exact distance| quantiles (m). Pools: pts/<pool>.txt with work/pts2/<pool>.ids (the
source outline's point index in out-nocap/planet/full).
usage: bldg_dist.py bT bA bG"""
import sys
import numpy as np

T = "/ssd/precision/tiers"
B = "/ssd/precision/build"
DU = f"{B}/dumps"
R = 6371008.8
SCALE = np.array([1e-5, 1e-4, 1e-6, 1e-7])
ADT = np.dtype([("lat", "<f4"), ("lng", "<f4"), ("hn", "<u4"), ("st", "<u4"), ("pw", "<u4"), ("voff", "<u4"), ("vcnt", "<u4")])
idx = np.fromfile(f"{DU}/addr_vertices.e7.idx", "<u8").astype(np.int64)
ev = np.memmap(f"{DU}/addr_vertices.e7", "<i4", "r").reshape(-1, 2)


class Today:
    def __init__(self, d):
        self.p = np.memmap(f"{d}/addr_points.bin", ADT, "r")
        self.v = np.memmap(f"{d}/addr_vertices.bin", np.uint8, "r")

    def ring(self, i):
        r = self.p[i]
        o, n = int(r["voff"]), int(r["vcnt"])
        h = np.asarray(self.v[o:o + 10])
        e = int(h[0])
        bl, bg = h[2:6].view("<f4")[0], h[6:10].view("<f4")[0]
        w = 4 if e == 3 else 2
        q = np.asarray(self.v[o + 10:o + 10 + 2 * w * n]).view("<u4" if w == 4 else "<u2").reshape(-1, 2).astype(np.float64)
        lat = (np.float64(bl) + q[:, 0] * SCALE[e]).astype(np.float32).astype(np.float64)
        lng = (np.float64(bg) + q[:, 1] * SCALE[e]).astype(np.float32).astype(np.float64)
        return np.stack([lat, lng], 1)


def sidecar(path):
    m = np.memmap(path, np.uint8, "r")
    hdr = np.asarray(m[:12]).view("<u4")
    assert hdr[0] == 0xD0510EAD and hdr[1] == 1
    rec = np.asarray(m[12:]).reshape(-1, 12)
    assert len(rec) == hdr[2]
    typ = rec[:, 0].astype(np.uint64)
    sid = rec[:, 4:12].copy().view("<u8").ravel()
    return (typ << np.uint64(56)) | sid


def inside(lat, lng, r):  # ray casting as the reader, in f64
    la, ga = r[:, 0], r[:, 1]
    lb, gb = np.roll(la, 1), np.roll(ga, 1)
    c = (ga > lng) != (gb > lng)
    with np.errstate(divide="ignore", invalid="ignore"):
        x = (lb - la) * (lng - ga) / (gb - ga) + la
    return bool(np.count_nonzero(c & (lat < x)) % 2)


def dist_m(lat, lng, r):
    if inside(lat, lng, r):
        return 0.0
    cl = np.cos(np.radians(lat))
    ax, ay = r[:, 0], r[:, 1]
    bx, by = np.roll(ax, -1), np.roll(ay, -1)
    dx, dy = bx - ax, by - ay
    L = dx * dx + dy * dy
    with np.errstate(divide="ignore", invalid="ignore"):
        t = np.where(L == 0, 0.0, np.clip(((lat - ax) * dx + (lng - ay) * dy) / L, 0, 1))
    px, py = ax + t * dx, ay + t * dy
    d2 = np.radians(lat - px) ** 2 + (np.radians(lng - py) * cl) ** 2
    return float(np.sqrt(d2.min()) * R)


base, nocap, mxd = Today(f"{B}/out-base/planet/full"), Today(f"{B}/out-nocap/planet/full"), Today(f"{T}/work/bldg/today_b")
kn = sidecar(f"{B}/out-nocap/planet/full/addr_points.osm_ids")
kb = sidecar(f"{B}/out-base/planet/full/addr_points.osm_ids")
order = np.argsort(kb, kind="stable")
kbs = kb[order]
VARS = ("exact32", "k20", "unc", "mx", "mx32")
print("| pool | variant | points | inside/outside differs from exact | abs distance error p50 m | p90 m | p99 m | max m |")
print("|---|---|---:|---:|---:|---:|---:|---:|")
for pool in sys.argv[1:]:
    pts = np.loadtxt(f"{T}/pts/{pool}.txt", ndmin=2)
    ids = np.loadtxt(f"{T}/work/pts2/{pool}.ids", dtype=np.int64, ndmin=1)
    assert len(pts) == len(ids)
    j = np.searchsorted(kbs, kn[ids])
    ok = (j < len(kbs)) & (kbs[np.minimum(j, len(kbs) - 1)] == kn[ids])
    bidx = order[np.minimum(j, len(kbs) - 1)]
    res = {v: ([], []) for v in VARS}
    skipped = 0
    for (lat, lng), i, b, good in zip(pts, ids, bidx, ok):
        if not good:
            skipped += 1
            continue
        ve = np.asarray(ev[idx[i]:idx[i + 1]]).astype(np.int64)
        ex = ve.astype(np.float64) * 1e-7
        sn = np.where(ve >= 0, (ve + 5) // 10, -((-ve + 5) // 10)) * 10  # tiers.cpp snap(v, 10) x 10
        rings = {"exact32": ex.astype(np.float32).astype(np.float64), "k20": base.ring(b), "unc": nocap.ring(i),
                 "mx": sn.astype(np.float64) * 1e-7, "mx32": mxd.ring(i)}
        d0, in0 = dist_m(lat, lng, ex), inside(lat, lng, ex)
        for v, r in rings.items():
            res[v][0].append(inside(lat, lng, r) != in0)
            res[v][1].append(abs(dist_m(lat, lng, r) - d0))
    for v in VARS:
        flip, err = np.array(res[v][0]), np.array(res[v][1])
        q = np.quantile(err, [0.5, 0.9, 0.99])
        print(f"| {pool} | {v} | {len(err):,} | {flip.mean():.3%} ({int(flip.sum())}) | {q[0]:.3f} | {q[1]:.3f} | {q[2]:.3f} | {err.max():.3f} |",
              flush=True)
    if skipped:
        print(f"| {pool} | (points whose address point is not in the capped build: {skipped}) | | | | | | |")
