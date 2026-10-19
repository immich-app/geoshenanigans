#!/usr/bin/env python3
"""v5score.py <pts dir> <truth label> <label>... [--pair a:b ...] : verify5's own scorer.
Immich fields exactly as immich-src mapGeocoderAddress (geocoding.repository.ts): country =
getName(upper(country_code)) || address.country (compared here by upper code, else name; no country ->
the whole result is null), state = state ?? province, city = city ?? town ?? village ?? hamlet ??
municipality. A point is wrong for a field when the label's field differs from the truth label's.
Border pools (3rd column = admin level) are split by level (B2..B8); others by pool.
--pair a:b : paired comparison per group and field, McNemar exact two-sided p (binomial on discordant).
Prints a JSON object {label: {group: {n, city, state, country, any (percent)}}, pairs: ...}."""
import json
import math
import os
import sys
from collections import defaultdict

args = [a for a in sys.argv[1:]]
pairs = []
while "--pair" in args:
    i = args.index("--pair")
    pairs.append(args[i + 1].split(":"))
    del args[i:i + 2]
pts_dir, truth, labels = args[0], args[1], args[2:]
Q = "/ssd/verify5/q"


def fields(js):
    ad = json.loads(js).get("address", {})
    cc = ad.get("country_code")
    country = cc.upper() if cc else ad.get("country")
    if not country:
        return (None, None, None)
    city = next((ad[k] for k in ("city", "town", "village", "hamlet", "municipality") if ad.get(k) is not None), None)
    state = next((ad[k] for k in ("state", "province") if ad.get(k) is not None), None)
    return (city, state, country)


def load(label, pool):
    with open(f"{Q}/{label}/{pool}.txt") as f:
        return [line.rstrip("\n").split(" ", 2) for line in f]


pools = sorted(p[:-4] for p in os.listdir(pts_dir) if p.endswith(".txt"))
groups_of = {}
for p in pools:
    with open(f"{pts_dir}/{p}.txt") as f:
        cols = [line.split() for line in f]
    groups_of[p] = [[p, f"{p}:B{c[2]}"] if len(c) > 2 and c[2].isdigit() else [p] for c in cols]

wrong = {}  # label -> pool -> list of (city, state, country, any) bools
for lab in labels + [truth]:
    wrong[lab] = {}
for p in pools:
    t = load(truth, p)
    tf = [fields(x[2]) for x in t]
    for lab in labels:
        o = load(lab, p)
        assert len(o) == len(t), (lab, p)
        w = []
        for (ta, tn, _), (oa, on, oj), f in zip(t, o, tf):
            assert ta == oa and tn == on, (lab, p, ta, oa)
            g = fields(oj)
            c, s, k = g[0] != f[0], g[1] != f[1], g[2] != f[2]
            w.append((c, s, k, c or s or k))
        wrong[lab][p] = w

out = {}
FN = ("city", "state", "country", "any")
for lab in labels:
    acc = defaultdict(lambda: [0, 0, 0, 0, 0])
    for p in pools:
        for gs, w in zip(groups_of[p], wrong[lab][p]):
            for g in gs:
                a = acc[g]
                a[0] += 1
                for i in range(4):
                    a[i + 1] += w[i]
    out[lab] = {g: {"n": a[0], **{FN[i]: round(100 * a[i + 1] / a[0], 3) for i in range(4)}} for g, a in sorted(acc.items())}


def mcnemar(b, c):
    n = b + c
    if n == 0:
        return 1.0
    k = min(b, c)
    # exact two-sided binomial p, log-space
    lp = [math.lgamma(n + 1) - math.lgamma(i + 1) - math.lgamma(n - i + 1) - n * math.log(2) for i in range(k + 1)]
    m = max(lp)
    return min(1.0, 2 * math.exp(m) * sum(math.exp(x - m) for x in lp))


pr = {}
for a, b in pairs:
    res = defaultdict(lambda: [[0, 0] for _ in range(4)])
    for p in pools:
        for gs, wa, wb in zip(groups_of[p], wrong[a][p], wrong[b][p]):
            for g in gs:
                for i in range(4):
                    if wa[i] and not wb[i]:
                        res[g][i][0] += 1
                    elif wb[i] and not wa[i]:
                        res[g][i][1] += 1
    pr[f"{a}:{b}"] = {g: {FN[i]: {"only_a": v[i][0], "only_b": v[i][1], "p": float(f"{mcnemar(*v[i]):.3g}")} for i in range(4)}
                      for g, v in sorted(res.items())}
out["pairs"] = pr
print(json.dumps(out, indent=1))
