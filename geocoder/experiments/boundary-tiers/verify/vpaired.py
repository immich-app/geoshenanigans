#!/usr/bin/env python3
"""vpaired.py <set> <A> <B> : paired comparison of two schemes on the fresh20k sample, per slice and
Immich field: points where only A differs from the truth vs points where only B does (discordant
pairs), with an exact two-sided sign-test p-value."""
import json
import math
import sys

V = "/ssd/tiers2-verify"
st, A, B = sys.argv[1:4]
slices = [f"border_L{l.split()[2]}" for l in open(f"{V}/pts/border10k.meta")] + \
         [f"photo_{l.split()[2]}" for l in open(f"{V}/pts/photo10k.meta")]


def imm(js):
    a = json.loads(js).get("address", {})
    cc = a.get("country_code")
    co = lambda *ks: next((a[k] for k in ks if a.get(k) is not None), None)  # noqa: E731
    return co("city", "town", "village", "hamlet", "municipality"), co("state", "province"), cc.upper() if cc else a.get("country")


def load(s):
    return [imm(line.split(" ", 2)[2]) for line in open(f"{V}/q/{s}_{st}/fresh20k.txt")]


t, a, b = load("unc"), load(A), load(B)


def sign_p(x, y):
    n = x + y
    if n == 0:
        return 1.0
    k = min(x, y)
    return min(1.0, 2 * sum(math.comb(n, i) for i in range(k + 1)) / 2 ** n)


groups = {}
for i, sl in enumerate(slices):
    for g in (sl, sl.split("_")[0] + ("_L6-8" if sl in ("border_L6", "border_L7", "border_L8") else ""), "all"):
        groups.setdefault(g, []).append(i)
print(f"{st}: {A} vs {B}; per field: only-{A}-wrong / only-{B}-wrong (sign-test p)")
for g in sorted(groups):
    idx = groups[g]
    cells = []
    for k, f in enumerate(("city", "state", "country")):
        x = sum(a[i][k] != t[i][k] and b[i][k] == t[i][k] for i in idx)
        y = sum(b[i][k] != t[i][k] and a[i][k] == t[i][k] for i in idx)
        cells.append(f"{f} {x}/{y} (p={sign_p(x, y):.2g})")
    anyx = sum(a[i] != t[i] and b[i] == t[i] for i in idx)
    anyy = sum(b[i] != t[i] and a[i] == t[i] for i in idx)
    print(f"{g} n={len(idx)}: any {anyx}/{anyy} (p={sign_p(anyx, anyy):.2g}) | " + " | ".join(cells))
