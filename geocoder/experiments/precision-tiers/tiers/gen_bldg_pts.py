#!/usr/bin/env python3
"""Test points near building outlines (exact uncapped outlines, dumps/addr_vertices.e7).

Pools (seeded 20261014), each point: a random outline of the pool, one of its edges picked with
probability proportional to its length, a uniform position along it, a perpendicular offset
uniform in [-15, 15] m (so about half the points fall inside the outline):
  bT  20k: outlines with more than 20 vertices (closing node included), the ones the cap trims
  bA  20k: any outline (uniform over outlines)
  bG   2k: the outlines whose span is >= 65535 x 1e-6 deg (today's u16 @ 1e-6 cannot hold them;
           they ship on the 1.1 m / 11 m grids), outline weighted by its edge count
Writes pts/bT.txt, pts/bA.txt, pts/bG.txt (+ .ids: the source outline's address-point index in
out-nocap/planet/full, one per line) and prints pool sizes."""
import numpy as np

DU = "/ssd/precision/build/dumps"
import sys
OUT = sys.argv[1] if len(sys.argv) > 1 else "/ssd/precision/tiers/pts"
idx = np.fromfile(f"{DU}/addr_vertices.e7.idx", "<u8").astype(np.int64)
v = np.memmap(f"{DU}/addr_vertices.e7", "<i4", "r").reshape(-1, 2)
cnt = np.diff(idx)
has = np.nonzero(cnt > 0)[0]
rng = np.random.default_rng(20261014)
OFF = 15.0
print(f"points {len(cnt):,} with outline {len(has):,} vertices {int(cnt.sum()):,}")
big = has[cnt[has] > 20]
print(f"outlines with > 20 vertices: {len(big):,} ({len(big) / len(has):.2%}), vertices {int(cnt[big].sum()):,}")

# span per outline (exact), vectorised over all outline vertices in chunks
span = np.zeros(len(cnt), np.int64)
CH = 5_000_000
for a in range(0, len(has), CH):
    o = has[a:a + CH]
    lo, hi = idx[o[0]], idx[o[-1] + 1]
    vv = np.asarray(v[lo:hi]).astype(np.int64)
    starts = idx[o] - lo
    mn = np.minimum.reduceat(vv, starts, axis=0)
    mx = np.maximum.reduceat(vv, starts, axis=0)
    span[o] = (mx - mn).max(axis=1)
giant = has[span[has] >= 65535 * 10]
print(f"outlines spanning >= 65535e-6 deg: {len(giant):,}; largest span {span.max() * 1e-7:.4f} deg")


def points(pool, n, weights=None):
    pick = rng.choice(pool, size=n, p=weights)
    out = []
    for o in pick:
        r = np.asarray(v[idx[o]:idx[o + 1]]).astype(np.float64) * 1e-7
        la, ga, lb, gb = r[:-1, 0], r[:-1, 1], r[1:, 0], r[1:, 1]
        coslat = np.cos(np.radians((la + lb) / 2))
        dy = (lb - la) * 111320.0
        dx = (gb - ga) * 111320.0 * coslat
        L = np.hypot(dx, dy)
        if L.sum() <= 0:
            continue
        e = rng.choice(len(L), p=L / L.sum())
        t = rng.random()
        off = rng.uniform(-OFF, OFF)
        nx, ny = -dy[e] / L[e], dx[e] / L[e]
        out.append((la[e] + t * (lb[e] - la[e]) + off * ny / 111320.0,
                    ga[e] + t * (gb[e] - ga[e]) + off * nx / (111320.0 * coslat[e]), int(o)))
    return out


for name, pool, n, w in (("bT", big, 20000, None), ("bA", has, 20000, None),
                         ("bG", giant, 2000, cnt[giant] / cnt[giant].sum())):
    pts = points(pool, n, w)
    with open(f"{OUT}/{name}.txt", "w") as f:
        for x, y, _ in pts:
            f.write(f"{x:.7f} {y:.7f}\n")
    with open(f"{OUT}/{name}.ids", "w") as f:
        for _, _, o in pts:
            f.write(f"{o}\n")
    print(f"{name}: {len(pts):,} points from a pool of {len(pool):,} outlines", flush=True)
