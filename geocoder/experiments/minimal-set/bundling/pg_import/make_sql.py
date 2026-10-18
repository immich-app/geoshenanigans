#!/usr/bin/env python3
"""Emit the rows Immich imports (map.repository.ts:296-415, 258-294) as SQL that mimics its load
pattern: 5000-row multi-row INSERTs split over 9 concurrent streams (map.repository.ts:359-379),
plus the naturalearth rows. Also prints row counts."""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
GEO = HERE.parent / "geodata"
STREAMS = 9
BATCH = 5000


def load_admin(p):
    m = {}
    for line in open(p, encoding="utf-8"):
        s = line.rstrip("\n").split("\t")
        m[s[0]] = s[1]
    return m


def q(v):
    if v is None:
        return "NULL"
    return "'" + v.replace("'", "''") + "'"


a1 = load_admin(GEO / "admin1CodesASCII.txt")
a2 = load_admin(GEO / "admin2Codes.txt")
total = kept = 0
rows = []
for line in open(GEO / "cities500.txt", encoding="utf-8"):
    total += 1
    s = line.rstrip("\n").split("\t")
    if (s[7] == "PPLX" and s[8] != "AU") or s[7] == "PPLH":
        continue
    kept += 1
    rows.append("(" + ",".join([s[0], q(s[1]), s[5], s[4], q(s[8]), q(s[10]), q(s[11]), q(s[18]),
                                q(a1.get(f"{s[8]}.{s[10]}")), q(a2.get(f"{s[8]}.{s[10]}.{s[11]}")), q(s[3])]) + ")")
cols = '(id,name,longitude,latitude,"countryCode","admin1Code","admin2Code","modificationDate","admin1Name","admin2Name","alternateNames")'
outs = [open(HERE / f"places_{i}.sql", "w", encoding="utf-8") for i in range(STREAMS)]
for b, start in enumerate(range(0, len(rows), BATCH)):
    outs[b % STREAMS].write(f"INSERT INTO geodata_places {cols} VALUES\n" + ",\n".join(rows[start:start + BATCH]) + ";\n")
for o in outs:
    o.close()

d = json.load(open(GEO / "ne_10m_admin_0_countries.geojson", encoding="utf-8"))
ne = []
for f in d["features"]:
    g = f["geometry"]
    for entry in g["coordinates"]:
        ring = entry[0] if g["type"] == "MultiPolygon" else entry
        poly = "(" + ", ".join(f"({p[0]},{p[1]})" for p in ring) + ")"
        ne.append("(" + ",".join([q(f["properties"]["ADMIN"]), q(f["properties"]["ADM0_A3"]), q(f["properties"]["TYPE"]), q(poly)]) + ")")
        if g["type"] == "Polygon":
            break
with open(HERE / "ne.sql", "w", encoding="utf-8") as o:
    o.write("INSERT INTO naturalearth_countries (admin,admin_a3,type,coordinates) VALUES\n" + ",\n".join(ne) + ";\n")

n1 = sum(1 for _ in open(GEO / "admin1CodesASCII.txt", encoding="utf-8"))
n2 = sum(1 for _ in open(GEO / "admin2Codes.txt", encoding="utf-8"))
nc = sum(1 for line in open(GEO / "countryInfo.txt", encoding="utf-8") if not line.startswith("#"))
print(f"cities500 lines {total}; geodata_places rows after PPLX/PPLH filter {kept}; batches {-(-kept // BATCH)}")
print(f"admin1 rows {n1}; admin2 rows {n2}; countryInfo rows {nc}")
print(f"naturalearth features {len(d['features'])}; naturalearth_countries rows (one per polygon outer ring) {len(ne)}")
