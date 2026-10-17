#!/usr/bin/env python3
"""geodata_places rows exactly as Immich imports them (map.repository.ts:324-360: cities500,
PPLX outside AU and PPLH dropped, admin1/admin2 names joined), as 5000-row INSERTs.
Adapted from /ssd/shrink/bundling/pg_import/make_sql.py (geodata only)."""
from pathlib import Path

HERE = Path(__file__).resolve().parent
GEO = HERE / "geodata"
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
with open(HERE / "places.sql", "w", encoding="utf-8") as o:
    for start in range(0, len(rows), BATCH):
        o.write(f"INSERT INTO geodata_places {cols} VALUES\n" + ",\n".join(rows[start:start + BATCH]) + ";\n")
print(f"cities500 lines {total}; geodata_places rows {kept}")
