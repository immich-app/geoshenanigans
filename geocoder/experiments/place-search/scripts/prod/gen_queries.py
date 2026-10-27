#!/usr/bin/env python3
"""10,000 timing queries (seed 20261018): the 1,311 evaluation queries plus GeoNames cities500
names as typed: whole names, prefixes (3-6 chars), one-edit typos, "name, state"."""
import random

G = "/ssd/search/pg/geodata/"
OUT = "/ssd/search-prod/tmp/q10k.txt"
rng = random.Random(20261018)
admin1 = {}
for line in open(G + "admin1CodesASCII.txt", encoding="utf-8"):
    c = line.rstrip("\n").split("\t")
    admin1[c[0]] = c[1]
rows = []
for line in open(G + "cities500.txt", encoding="utf-8"):
    c = line.split("\t")
    rows.append((c[1], admin1.get(c[8] + "." + c[10], "")))
evalq = [l.rstrip("\n") for l in open("/ssd/search-prod/tmp/q1311.txt", encoding="utf-8") if l.strip()]


def typo(s):
    i = rng.randrange(1, len(s))
    op = rng.choice("dsi")
    ch = rng.choice("aeinorstl")
    if op == "d":
        return s[:i] + s[i + 1:]
    if op == "s":
        return s[:i] + ch + s[i + 1:]
    return s[:i] + ch + s[i:]


out = list(evalq)
for _ in range(4189):
    out.append(rng.choice(rows)[0])
for _ in range(2000):
    n = rng.choice(rows)[0]
    out.append(n[: rng.randint(3, 6)])
while len(out) < 4189 + 2000 + 1000 + len(evalq):
    n = rng.choice(rows)[0]
    if len(n) >= 5:
        out.append(typo(n))
while len(out) < 10000:
    n, a = rng.choice(rows)
    if a:
        out.append(f"{n}, {a}")
rng.shuffle(out)
open(OUT, "w", encoding="utf-8").write("\n".join(out) + "\n")
print(len(out))
