#!/usr/bin/env python3
"""Held-out queries (never looked at while tuning): seeded samples of the geodata_places rows
Immich imports (cities500, PPLX outside AU and PPLH dropped), expected answer = the GeoNames
point. GeoNames' own names and coordinates, so the set leans towards the baseline.
  heldout_big  150 places with population >= 100000, query = name, tolerance 15 km
  heldout_ctx  150 places with 1000 <= population < 100000, query = "name, admin1 name",
               tolerance 8 km
  make_heldout.py [seed] [out]  (defaults: 20261018, /ssd/search/qset/queries_heldout.tsv;
  the second sample, seed 20261019 -> queries_heldout2.tsv, was drawn after the tuning that
  looked at the first one)."""
import random
import sys

GEO = "/ssd/search/pg/geodata/"
SEED = int(sys.argv[1]) if len(sys.argv) > 1 else 20261018
OUT = sys.argv[2] if len(sys.argv) > 2 else "/ssd/search/qset/queries_heldout.tsv"
N = 150


def load_admin(p):
    m = {}
    for line in open(p, encoding="utf-8"):
        s = line.rstrip("\n").split("\t")
        m[s[0]] = s[1]
    return m


a1 = load_admin(GEO + "admin1CodesASCII.txt")
big, small = [], []
for line in open(GEO + "cities500.txt", encoding="utf-8"):
    s = line.rstrip("\n").split("\t")
    if (s[7] == "PPLX" and s[8] != "AU") or s[7] == "PPLH":
        continue
    pop = int(s[14] or 0)
    row = (s[1], float(s[4]), float(s[5]), s[8], a1.get(f"{s[8]}.{s[10]}"))
    if pop >= 100_000:
        big.append(row)
    elif pop >= 1000 and row[4]:
        small.append(row)
rng = random.Random(SEED)
TAG = "" if SEED == 20261018 else "h2_"
with open(OUT, "w", encoding="utf-8") as o:
    o.write("# category\tquery\texpected\tlat\tlon\ttol_km\talternates\n")
    for name, lat, lon, cc, adm in rng.sample(big, N):
        o.write(f"{TAG}heldout_big\t{name}\t{name} ({cc}, GeoNames)\t{lat}\t{lon}\t15\t\n")
    for name, lat, lon, cc, adm in rng.sample(small, N):
        o.write(f"{TAG}heldout_ctx\t{name}, {adm}\t{name}, {adm} ({cc}, GeoNames)\t{lat}\t{lon}\t8\t\n")
print("big pool", len(big), "small pool", len(small))
