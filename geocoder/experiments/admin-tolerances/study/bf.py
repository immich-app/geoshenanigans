#!/usr/bin/env python3
"""bf.py : exhaustive constrained search over per-level tolerances (planet-261006 tierscan curves,
additive relevance model as frontier.py). For every L7/L8 pair in L78 and every combination of
L2..L6 options (aniso + cap 0.1, plus iso + cap 0.1 with --iso): bytes and estimated Immich errors.
Feasible: bytes <= budget (q2.5+cap's L2-8 bytes x (1 + slack)), estimated B7 city, B8 city and
photo city <= q2.5+cap's (all levels summed, so a coarser L5/L6 that moves city answers counts).
Prints the best feasible schemes for each objective: cs (B2 country + B4 state), photo (photo any),
border (mean any over B2..B8), and the Pareto set of (cs, photo).
usage: bf.py [--iso] [--slack 0] [--cityslack 0] <tierscan tsv>..."""
import argparse
import itertools
import json
from collections import defaultdict

import numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("--iso", default="", help="levels allowed the iso option, e.g. 6,7,8")
ap.add_argument("--slack", type=float, default=0.0)
ap.add_argument("--cityslack", type=float, default=0.0, help="allowed relative city increase")
ap.add_argument("--rel", default="/ssd/tiers3/cmp/relevance.json")
ap.add_argument("--top", type=int, default=8)
ap.add_argument("tsv", nargs="+")
a = ap.parse_args()
Q25 = {2: 1250, 3: 500, 4: 250, 5: 125, 6: 75, 7: 50, 8: 37.5}
rel = json.load(open(a.rel))
specs = defaultdict(dict)
meta = {}
for path in a.tsv:
    for line in open(path):
        f = line.rstrip("\n").split("\t")
        specs[f[0]][f[5]] = (int(f[6]), int(f[8]))
        meta[f[0]] = (int(f[1]), int(f[4]))


def kv(spec):
    return dict(it.split("=") for it in spec.split(";"))


KEYS = [("B2", "country"), ("B4", "state"), ("B7", "city"), ("B8", "city"), ("photo", "city"), ("photo", "any")] + \
       [(f"B{k}", "any") for k in range(2, 9)]


def est(spec):
    L = meta[spec][0]
    r = rel[str(L)]
    out = []
    for pool, fld in KEYS:
        n, mm = specs[spec][pool]
        grp = "photo_" if pool == "photo" else "border_"
        out.append(r[grp + fld] * mm / n)
    return np.array(out)


opts = defaultdict(list)
for spec, (L, nb) in meta.items():
    if L > 8:
        continue
    d = kv(spec)
    if d.get("none") == "1" or d.get("cap") != "0.1" or "rel" in d or "relx" in d:
        continue
    iso = d.get("iso") == "1"
    if iso and str(L) not in a.iso.split(","):
        continue
    opts[L].append((spec, nb / 1e6, est(spec)))
ref_specs = {L: [s for s, nb, e in opts[L] if "iso" not in kv(s) and float(kv(s)["t"]) == Q25[L]][0] for L in Q25}
ref_b = sum(meta[s][1] for s in ref_specs.values()) / 1e6
ref_e = sum(est(s) for s in ref_specs.values())
budget = ref_b * (1 + a.slack)
lim = ref_e[[2, 3, 4]] * (1 + a.cityslack)
print(f"q2.5+cap: {ref_b:.2f} MB; est B2cc {100 * ref_e[0]:.2f} B4st {100 * ref_e[1]:.2f} B7ct {100 * ref_e[2]:.2f} "
      f"B8ct {100 * ref_e[3]:.2f} photo city {100 * ref_e[4]:.3f} photo any {100 * ref_e[5]:.3f}; budget {budget:.2f} MB")

lv = [2, 3, 4, 5, 6]
B = [np.array([o[1] for o in opts[L]]) for L in lv]
E = [np.stack([o[2] for o in opts[L]]) for L in lv]
names = [[o[0] for o in opts[L]] for L in lv]
# all combinations of L2..L6 via broadcasting
grids = np.meshgrid(*[np.arange(len(b)) for b in B], indexing="ij")
idx = [g.ravel() for g in grids]
b26 = sum(B[i][idx[i]] for i in range(5))
e26 = sum(E[i][idx[i]] for i in range(5))
rows = []
for s7, b7, e7 in opts[7]:
    for s8, b8, e8 in opts[8]:
        bt = b26 + b7 + b8
        et = e26 + e7 + e8
        ok = (bt <= budget) & np.all(et[:, [2, 3, 4]] <= lim + 1e-12, axis=1)
        for k in np.nonzero(ok)[0]:
            rows.append((bt[k], et[k], [names[i][idx[i][k]] for i in range(5)] + [s7, s8]))
print(f"feasible combinations: {len(rows)}")


def fmt(r):
    bt, et, ch = r
    sch = " ".join(f"{kv(s)['L']}:{kv(s)['t']}{'i' if kv(s).get('iso') == '1' else ''}" for s in ch)
    brd = et[6:13].mean()
    return (f"{bt:7.2f} MB  {sch:<48} B2cc {100 * et[0]:5.2f} B4st {100 * et[1]:5.2f} B7ct {100 * et[2]:4.2f} "
            f"B8ct {100 * et[3]:4.2f} phct {100 * et[4]:.3f} phany {100 * et[5]:.3f} brd {100 * brd:5.2f}")


for obj, key in (("cs", lambda r: r[1][0] + r[1][1]), ("photo", lambda r: r[1][5]), ("border", lambda r: r[1][6:13].mean()),
                 ("cs+10photo", lambda r: r[1][0] + r[1][1] + 10 * r[1][5])):
    print(f"== best by {obj}")
    for r in sorted(rows, key=key)[:a.top]:
        print(fmt(r))
# Pareto (cs, photo)
print("== Pareto (country+state, photo any)")
pr = sorted(rows, key=lambda r: (r[1][0] + r[1][1], r[1][5]))
best = 9
for r in pr:
    if r[1][5] < best - 1e-6:
        best = r[1][5]
        print(fmt(r))
