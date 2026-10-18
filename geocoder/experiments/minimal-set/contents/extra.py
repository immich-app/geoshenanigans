#!/usr/bin/env python3
"""Extra breakdowns for the admin-minimal inventory.

1. Admin vertex bytes per (level, output field) with the server's default rank table
   (server/admin_levels.json "default"; per-country rows ignored) and place_type_override
   taking precedence (server/src/lib.rs place_type_to_field / rank_to_field).
2. strings_core bytes needed by each place type alone (strings no other minimal record uses).
3. Small-ring statistics.

Usage: extra.py <minimal dir>
"""
import os
import sys
from collections import defaultdict

import numpy as np

sys.path.insert(0, os.path.dirname(__file__))
from inventory import load_polys, load_strings, PLACE_DT, NO_DATA, OVERRIDE  # noqa: E402

DEFAULT_RANK = {2: 4, 3: 6, 4: 8, 5: 10, 6: 12, 7: 14, 8: 16}


def rank_field(r):
    return {4: "country", 6: "region", 8: "state", 10: "state_district", 12: "county", 14: "municipality",
            16: "city"}.get(r - (r % 2))


IMMICH = {"country": "country", "state": "state", "province": "state", "city": "city", "town": "city",
          "village": "city", "hamlet": "city", "municipality": "city (last fallback)"}

mdir = sys.argv[1]
p, v, live, has, enc, nbytes = load_polys(mdir)
agg = defaultdict(lambda: [0, 0])
imm = defaultdict(lambda: [0, 0])
for L in range(2, 9):
    m = live & (p["level"] == L)
    for o in np.unique(p["pto"][m]):
        mm = m & (p["pto"] == o)
        field = OVERRIDE[int(o)] if o else rank_field(DEFAULT_RANK[L])
        agg[(L, field)][0] += int(mm.sum())
        agg[(L, field)][1] += int(nbytes[mm].sum())
        key = (L, IMMICH.get(field, "not used by Immich"))
        imm[key][0] += int(mm.sum())
        imm[key][1] += int(nbytes[mm].sum())
tot = int(nbytes.sum())
print("level  field (default ranks + override)            rings      vertex bytes   share")
for (L, f), (c, b) in sorted(agg.items(), key=lambda kv: (kv[0][0], -kv[1][1])):
    print(f"L{L:<5d}{f:40s}{c:>8,d}{b:>16,d}{b / tot * 100:7.2f}%")
print("\nlevel  Immich use (default ranks + override)       rings      vertex bytes   share")
for (L, f), (c, b) in sorted(imm.items(), key=lambda kv: (kv[0][0], -kv[1][1])):
    print(f"L{L:<5d}{f:40s}{c:>8,d}{b:>16,d}{b / tot * 100:7.2f}%")

# small rings
for lim in (4, 8, 16):
    m = live & (p["vcnt"] <= lim)
    print(f"rings with <= {lim} vertices: {int(m.sum()):,} ({int(nbytes[m].sum()):,} vertex bytes)")

# strings needed per place type alone
sb, starts, lens = load_strings(f"{mdir}/strings_core.bin")
ns = len(starts)
pn = np.fromfile(f"{mdir}/place_nodes.bin", dtype=PLACE_DT)
users = np.zeros(ns, dtype=np.uint8)


def mark(offs, bit):
    offs = offs[offs != NO_DATA]
    i = np.searchsorted(starts, offs)
    np.bitwise_or.at(users, i, bit)


mark(p["name"][live], 1)
for t, bit in [(0, 2), (1, 4), (2, 8), (4, 16)]:
    mark(pn["name"][pn["pt"] == t], bit)
nb = lens + 1
for nm, bit in [("admin", 1), ("city", 2), ("town", 4), ("village", 8), ("hamlet", 16)]:
    only = users == bit
    print(f"strings used only by {nm:8s}: {int(only.sum()):>9,d} strings {int(nb[only].sum()):>12,d} B")
print(f"strings used by minimal at all: {int((users != 0).sum()):,} / {int(nb[users != 0].sum()):,} B")
