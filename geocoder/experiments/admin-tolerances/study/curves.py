#!/usr/bin/env python3
"""curves.py <tsv>... : per-level tierscan curves. For each level and algorithm (aniso = builder DP,
iso = isotropic DP): tolerance, bytes, vertices, mismatch % on the level's own border pool (B<L>),
on photo-like (photo300k, per 10k points) and on the verify pools. Prints one table per level."""
import sys
from collections import defaultdict

rows = defaultdict(dict)  # spec -> pool -> row
meta = {}
for path in sys.argv[1:]:
    for line in open(path):
        f = line.rstrip("\n").split("\t")
        spec, level, npoly, nv, nb, pool = f[0], int(f[1]), int(f[2]), int(f[3]), int(f[4]), f[5]
        n, th, mm, gap, extra, swap, ov = map(int, f[6:13])
        rows[spec][pool] = dict(n=n, th=th, mm=mm, gap=gap, extra=extra, swap=swap, ov=ov)
        meta[spec] = (level, nv, nb)


def kv(spec):
    d = {}
    for it in spec.split(";"):
        k, v = it.split("=")
        d[k] = v
    return d


by_level = defaultdict(list)
for spec, (level, nv, nb) in meta.items():
    d = kv(spec)
    if "none" in d:
        continue
    algo = ("iso" if d.get("iso") == "1" else "aniso") + ("" if d.get("cap", "0") == "0.1" else f"/cap{d.get('cap', '0')}")
    if "rel" in d:
        algo += f"/rel{d['rel']}"
    by_level[level].append((algo, float(d.get("t", "nan")), nv, nb, spec))

for level in sorted(by_level):
    print(f"== L{level}  (B{level}: own border pool, mismatch %; photo: per 10k photo-like points)")
    print(f"{'algo':<14}{'t m':>7}{'vertices':>11}{'MB':>8}{'B%':>7}{'gap':>6}{'ovl':>5}{'photo/10k':>10}{'vb%':>7}")
    for algo, t, nv, nb, spec in sorted(by_level[level], key=lambda r: (r[0], r[1])):
        r = rows[spec]
        b = r.get(f"B{level}")
        ph = r.get("photo")
        vb = r.get("vborder")
        print(f"{algo:<14}{t:>7g}{nv:>11}{nb / 1e6:>8.2f}{100 * b['mm'] / b['n']:>7.2f}{b['gap']:>6}{b['ov']:>5}"
              f"{1e4 * ph['mm'] / ph['n']:>10.1f}{100 * vb['mm'] / vb['n']:>7.2f}")
