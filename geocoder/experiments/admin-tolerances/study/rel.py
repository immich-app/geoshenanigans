#!/usr/bin/env python3
"""rel.py <tierscan tsv>... : relevance of each admin level's pick mismatches for the Immich fields.
For level L: cmp/score_sl<L>.json (real reader, only L simplified at q2.5+cap, the rest truth; vs the
truth answers) against the tierscan mismatches of spec L=<L>;t=<q2.5 m>;cap=0.1 on the same pools
(B2..B10, photo). r_L[field] = sum of Immich changes / sum of level-L mismatches.
-> cmp/relevance.json and a table (also per pool for the level's own border pool and photo)."""
import json
import sys
from collections import defaultdict

T = "/ssd/tiers3"
Q25 = {2: "1250", 3: "500", 4: "250", 5: "125", 6: "75", 7: "50", 8: "37.5"}
POOLS = [f"B{k}" for k in range(2, 11)] + ["photo"]
mm = defaultdict(dict)
for path in sys.argv[1:]:
    for line in open(path):
        f = line.rstrip("\n").split("\t")
        mm[f[0]][f[5]] = (int(f[6]), int(f[8]))
out = {}
print("level | r any | r city | r state | r country | own pool: mism / any / city / state / country | photo: mism / any")
for L, t in Q25.items():
    spec = f"L={L};t={t};cap=0.1"
    sc = json.load(open(f"{T}/cmp/score_sl{L}.json"))["smallest"]["acc"]
    r = {}
    for grp, pools in (("", POOLS), ("border_", POOLS[:-1]), ("photo_", ["photo"])):
        tot = defaultdict(int)
        for p in pools:
            tot["mm"] += mm[spec][p][1]
            for fld in ("any", "city", "state", "country"):
                tot[fld] += sc.get(p, {}).get(fld, 0)
        for fld in ("any", "city", "state", "country"):
            r[grp + fld] = tot[fld] / tot["mm"] if tot["mm"] else 0
    out[str(L)] = r
    own, ph = sc.get(f"B{L}", {}), sc.get("photo", {})
    print(f"L{L} | {r['any']:.2f} | {r['city']:.2f} | {r['state']:.2f} | {r['country']:.2f} | "
          f"{mm[spec][f'B{L}'][1]} / {own.get('any', 0)} / {own.get('city', 0)} / {own.get('state', 0)} / {own.get('country', 0)} | "
          f"{mm[spec]['photo'][1]} / {ph.get('any', 0)}")
json.dump(out, open(f"{T}/cmp/relevance.json", "w"), indent=1)
