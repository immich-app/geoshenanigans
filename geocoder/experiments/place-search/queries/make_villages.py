#!/usr/bin/env python3
"""Village rows of the evaluation set: places GeoNames (geodata_places, as Immich imports it)
lacks. Two halves:
  famous  hand-picked small places (coordinates from memory), kept only when no
          geodata_places row with that name (name or alternateNames, unaccented,
          case-folded) lies within 10 km; the expected point is checked against the corpus.
  random  seeded sample (seed 20261017) of corpus village/hamlet nodes, one per country, whose
          folded name occurs exactly once in the corpus and nowhere in geodata_places.
Writes /ssd/search/qset/queries_villages.tsv (same columns as queries_manual.tsv)."""
import collections
import math
import random
import unicodedata

import psycopg

FAMOUS = [
    ("Gásadalur", 62.1106, -7.4339), ("Saksun", 62.2486, -7.1767), ("Gjógv", 62.3256, -6.9458),
    ("Kirkjubøur", 61.9561, -6.7967), ("Manarola", 44.1067, 9.7286), ("Corniglia", 44.1197, 9.7114),
    ("Vernazza", 44.1350, 9.6847), ("Riomaggiore", 44.0997, 9.7376), ("Reine", 67.9326, 13.0890),
    ("Hamnøy", 67.9431, 13.1317), ("Grise Fiord", 76.4180, -82.8940), ("Supai", 36.2369, -112.6893),
    ("Castle Combe", 51.4936, -2.2289), ("Bibury", 51.7588, -1.8327), ("Clovelly", 50.9988, -4.3989),
    ("Polperro", 50.3313, -4.5170), ("Mousehole", 50.0838, -5.5381), ("Port Isaac", 50.5933, -4.8297),
    ("Staithes", 54.5590, -0.7920), ("Robin Hood's Bay", 54.4344, -0.5340), ("Aberdaron", 52.8045, -4.7110),
    ("Plockton", 57.3369, -5.6522), ("Grasmere", 54.4590, -3.0240), ("Hawkshead", 54.3750, -2.9980),
    ("Lacock", 51.4152, -2.1197), ("Portmeirion", 52.9134, -4.0984), ("Kinderdijk", 51.8838, 4.6370),
    ("Giethoorn", 52.7400, 6.0789), ("Hallstatt", 47.5622, 13.6493), ("Gimmelwald", 46.5459, 7.8917),
    ("Mürren", 46.5590, 7.8925), ("Wengen", 46.6083, 7.9225), ("Ushguli", 42.9167, 43.0144),
    ("Perast", 42.4865, 18.6984), ("Sveti Stefan", 42.2561, 18.8919), ("Ittoqqortoormiit", 70.4846, -21.9667),
    ("Edinburgh of the Seven Seas", -37.0675, -12.3105), ("Ny-Ålesund", 78.9236, 11.9306),
    ("Whittier", 60.7731, -148.6839), ("Milford Sound", -44.6714, 167.9256), ("Oia", 36.4618, 25.3753),
    ("Göreme", 38.6431, 34.8289), ("Ogimachi", 36.2578, 136.9061), ("Vík í Mýrdal", 63.4186, -19.0060),
    ("Oymyakon", 63.4608, 142.7858), ("Tórshavn", 62.0107, -6.7741),
]
FAMOUS_TOL_KM = 3
RANDOM_TOL_KM = 3
RANDOM_N = 30
SEED = 20261017


def fold(s):
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c)).casefold().strip()


def km(a, b, c, d):
    p = math.pi / 180
    h = math.sin((c - a) * p / 2) ** 2 + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(h))


# geodata_places: folded name -> points (name and every alternate name)
gn = collections.defaultdict(list)
with psycopg.connect("host=127.0.0.1 port=55432 user=postgres password=search dbname=immich") as conn:
    for name, alt, lat, lon in conn.execute('SELECT name, "alternateNames", latitude, longitude FROM geodata_places'):
        for n in [name] + (alt.split(",") if alt else []):
            if n:
                gn[fold(n)].append((lat, lon))

# corpus: folded primary-or-any name -> rows
corpus_names = collections.Counter()
by_name = collections.defaultdict(list)
cands = []
with open("/ssd/search/data/corpus.tsv", encoding="utf-8", errors="surrogateescape") as f:
    for line in f:
        c = line.rstrip("\n").split("\t")
        names = [kv.partition("=")[2] for kv in c[11].split("\x1f")]
        prim = next((kv.partition("=")[2] for kv in c[11].split("\x1f") if kv.startswith("name=")), None)
        for n in {fold(x) for x in names}:
            corpus_names[n] += 1
        if prim:
            by_name[fold(prim)].append((float(c[3]), float(c[4]), c[0], c[2]))
            if c[0] == "P" and c[2] in ("village", "hamlet") and 6 <= len(prim) <= 24 and len(names) == 1:
                cands.append((prim, float(c[3]), float(c[4]), c[12]))

out = open("/ssd/search/qset/queries_villages.tsv", "w", encoding="utf-8")
log = open("/ssd/search/qset/villages_check.txt", "w", encoding="utf-8")
kept = 0
for name, lat, lon in FAMOUS:
    f = fold(name)
    near_gn = [p for p in gn.get(f, []) if km(lat, lon, *p) < 10]
    near_corpus = [r for r in by_name.get(f, []) if km(lat, lon, r[0], r[1]) < 5]
    status = "in GeoNames" if near_gn else ("ok" if near_corpus else "NOT IN CORPUS near point")
    log.write(f"famous\t{name}\t{status}\tgeonames_near={len(near_gn)}\tcorpus_near={near_corpus[:2]}\n")
    if near_gn:
        continue
    kept += 1
    out.write(f"village\t{name}\t{name} (famous, not in GeoNames)\t{lat}\t{lon}\t{FAMOUS_TOL_KM}\t\n")

rng = random.Random(SEED)
rng.shuffle(cands)
per_cc = set()
picked = 0
for name, lat, lon, cc in cands:
    if picked >= RANDOM_N:
        break
    f = fold(name)
    if cc in per_cc or not cc or corpus_names[f] != 1 or f in gn:
        continue
    per_cc.add(cc)
    picked += 1
    out.write(f"village\t{name}\t{name} ({cc}, random OSM village/hamlet)\t{lat:.6f}\t{lon:.6f}\t{RANDOM_TOL_KM}\t\n")
    log.write(f"random\t{name}\t{cc}\t{lat:.5f},{lon:.5f}\n")
out.close()
log.write(f"famous kept {kept} of {len(FAMOUS)}; random {picked}\n")
log.close()
print(open("/ssd/search/qset/villages_check.txt").read())
