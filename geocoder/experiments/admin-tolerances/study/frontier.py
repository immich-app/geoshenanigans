#!/usr/bin/env python3
"""frontier.py : Pareto frontier of per-level tolerance schemes from tierscan curves (planet-261006).

Per level L (2..8) every tierscan spec of that level is an option with bytes_L (packed vertex bytes of
the level's reader-visible polygons) and per-pool level-L pick mismatches. Relevance r_L[field] =
Immich-field changes of the single-level real-reader run sl<L> (only L simplified at q2.5+cap, the
rest truth) / tierscan level-L mismatches of the same spec, summed over pools (B2..B10, photo).
Estimated Immich error of a scheme on pool P, field f: sum_L r_L[f] x mm_L(P) / n(P) (additive; it
double counts points that flip at two levels, so it is an upper estimate on coincident borders).

Objectives (lower is better):
  border = mean over P in B2..B8 of the estimated any-field error (the verify "border any" metric)
  photo  = estimated any-field error on the photo-like pool
  cs     = country error on B2 + state error on B4 (what the flat scheme fixed)
Constraints: B8 city <= q2.5+cap's, B7 city <= q2.5+cap's (estimated the same way); --free78 lifts them.
Exact frontier by DP over bytes bins (0.05 MB). Prints the frontier near the q2.5+cap budget.
usage: frontier.py --obj border|photo|cs|mix [--algo aniso|iso|any] [--free78] <tierscan tsv>..."""
import argparse
import json
import math
from collections import defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("--obj", default="mix")
ap.add_argument("--algo", default="aniso")
ap.add_argument("--free78", action="store_true")
ap.add_argument("--mu", type=float, default=0, help="penalty weight on B7 + B8 + 10 x photo city (all levels)")
ap.add_argument("--rel", default="/ssd/tiers3/cmp/relevance.json")
ap.add_argument("--lo", type=float, default=150)
ap.add_argument("--hi", type=float, default=175)
ap.add_argument("--step", type=float, default=1.0)
ap.add_argument("tsv", nargs="+")
a = ap.parse_args()

Q25 = {2: 1250, 3: 500, 4: 250, 5: 125, 6: 75, 7: 50, 8: 37.5}
rel = json.load(open(a.rel))  # {"2": {"any": r, "city": r, "state": r, "country": r}, ...}
specs = defaultdict(dict)
meta = {}
for path in a.tsv:
    for line in open(path):
        f = line.rstrip("\n").split("\t")
        spec, level, nb, pool = f[0], int(f[1]), int(f[4]), f[5]
        n, mm = int(f[6]), int(f[8])
        specs[spec][pool] = (n, mm)
        meta[spec] = (level, nb)


def kv(spec):
    return dict(it.split("=") for it in spec.split(";"))


def family(d):
    if d.get("none") == "1":
        return None
    fam = "iso" if d.get("iso") == "1" else "aniso"
    if d.get("cap") != "0.1" or "rel" in d or "relx" in d:
        return fam + "*"
    return fam


opts = defaultdict(list)  # level -> [(bytes, spec, est dict)]
for spec, (level, nb) in meta.items():
    if level > 8:
        continue
    d = kv(spec)
    fam = family(d)
    if fam is None or (a.algo != "any" and fam != a.algo):
        continue
    r = rel[str(level)]
    est = {}
    for pool, (n, mm) in specs[spec].items():
        grp = "photo_" if pool in ("photo", "vphoto") else "border_"
        for fld in ("any", "city", "state", "country"):
            est[(pool, fld)] = r[grp + fld] * mm / n
    opts[level].append((nb / 1e6, spec, est))

base = {L: next(o for o in opts[L] if kv(o[1]).get("t") == str(Q25[L]) or float(kv(o[1]).get("t", -1)) == Q25[L]) for L in Q25} \
    if a.algo != "iso" else None


def cost(est):
    border = sum(est.get((f"B{k}", "any"), 0) for k in range(2, 9)) / 7
    photo = est.get(("photo", "any"), 0)
    cs = est.get(("B2", "country"), 0) + est.get(("B4", "state"), 0)
    city = est.get(("B7", "city"), 0) + est.get(("B8", "city"), 0) + 10 * est.get(("photo", "city"), 0)
    return {"border": border, "photo": photo, "cs": cs, "mix": border + 10 * photo}[a.obj] + a.mu * city


Q25B = {}
for L in (7, 8):
    q = [o for o in meta if meta[o][0] == L and kv(o).get("cap") == "0.1" and "iso" not in kv(o) and "rel" not in kv(o)
         and "relx" not in kv(o) and float(kv(o).get("t", -1)) == Q25[L]][0]
    Q25B[L] = rel[str(L)]["border_city"] * specs[q][f"B{L}"][1] / specs[q][f"B{L}"][0]

STEP = 0.05
levels = sorted(opts)
best = {0: (0.0, [])}
for L in levels:
    nxt = {}
    for b, (c, ch) in best.items():
        for nb, spec, est in opts[L]:
            if not a.free78 and L in (7, 8) and est.get((f"B{L}", "city"), 0) > Q25B[L] + 1e-12:
                continue
            k = b + int(round(nb / STEP))
            cc = c + cost(est)
            if k not in nxt or cc < nxt[k][0]:
                nxt[k] = (cc, ch + [(L, spec, est, nb)])
    best = nxt
# Pareto filter
pts = sorted(best.items())
front, cmin = [], math.inf
for k, (c, ch) in pts:
    if c < cmin - 1e-12:
        cmin = c
        front.append((k * STEP, c, ch))


def summary(ch):
    tot = defaultdict(float)
    for L, spec, est, nb in ch:
        for key, v in est.items():
            tot[key] += v
    return tot


print(f"objective {a.obj}, algo {a.algo}, L7/L8 {'free' if a.free78 else 'at most q2.5+cap city error'}")
print(f"{'MB':>7} {'cost':>7}  {'scheme (per level t m)':<52} {'B2 cc':>6} {'B4 st':>6} {'B7 ct':>6} {'B8 ct':>6} {'brd':>6} {'photo':>6} {'ph ct':>6}")
last = -1e9
for mb, c, ch in front:
    if mb < a.lo or mb > a.hi or mb - last < a.step:
        continue
    last = mb
    s = summary(ch)
    sch = " ".join(f"{L}:{kv(spec).get('t', spec)}{'i' if family(kv(spec)) == 'iso' else ''}{'' if family(kv(spec)) in ('aniso', 'iso') else '*'}" for L, spec, est, nb in ch)
    brd = sum(s[(f"B{k}", "any")] for k in range(2, 9)) / 7
    print(f"{mb:7.2f} {c * 100:7.3f}  {sch:<52} {100 * s[('B2', 'country')]:6.2f} {100 * s[('B4', 'state')]:6.2f} "
          f"{100 * s[('B7', 'city')]:6.2f} {100 * s[('B8', 'city')]:6.2f} {100 * brd:6.2f} {100 * s[('photo', 'any')]:6.3f} {100 * s[('photo', 'city')]:6.3f}")
if base:
    s = summary([(L, o[1], o[2], o[0]) for L, o in base.items()])
    brd = sum(s[(f"B{k}", "any")] for k in range(2, 9)) / 7
    print(f"q2.5+cap reference: {sum(o[0] for o in base.values()):.2f} MB  B2 cc {100 * s[('B2', 'country')]:.2f}  B4 st {100 * s[('B4', 'state')]:.2f}  "
          f"B7 ct {100 * s[('B7', 'city')]:.2f}  B8 ct {100 * s[('B8', 'city')]:.2f}  brd {100 * brd:.2f}  photo {100 * s[('photo', 'any')]:.3f}  photo city {100 * s[('photo', 'city')]:.3f}")
