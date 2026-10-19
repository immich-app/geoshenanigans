"""score6.py <api.jsonl> <pool> : score verify6's answers against Nominatim's on the API points.
Each name is <preset>_<step>_<line> (line in /ssd/verify6/pts/<pool>.txt, 1-based); answers come from
/ssd/verify6/q/<label>_<preset>/<pool>.txt. Mismatch = size of the symmetric difference of the
field=value sets over the place fields (country .. city_block, as the parity track's score.py).
Prints per point Nominatim / before-step / after-step / B / T and a summary: exact matches and
better / worse / same of T vs B, and of each step's after vs before."""
import json
import sys
from collections import defaultdict

PLACE = ["country", "state", "region", "province", "state_district", "county", "district", "municipality", "city",
         "town", "village", "hamlet", "city_block", "city_district", "borough", "suburb", "quarter", "neighbourhood"]
STEPS = ["B", "D", "S1", "S2", "S3", "S4", "S5", "T"]
V = "/ssd/verify6/"
api_path, pool = sys.argv[1:3]
cache = {}


def ans(label, preset, line):
    key = (label, preset)
    if key not in cache:
        cache[key] = [json.loads(l.split(" ", 2)[2]).get("address") or {} for l in open(f"{V}q/{label}_{preset}/{pool}.txt", encoding="utf-8")]
    return cache[key][line - 1]


def fs(a):
    return {(k, a[k]) for k in PLACE if a.get(k)}


def fmt(a):
    return ", ".join(f"{k}={a[k]}" for k in PLACE if a.get(k))


tot = defaultdict(lambda: [0, 0, 0])
exact = defaultdict(int)
miss = defaultdict(int)
n = 0
print("| point | Nominatim | B | T | step before -> after |")
print("|---|---|---|---|---|")
for l in open(api_path, encoding="utf-8"):
    p = json.loads(l)
    if "address" not in p["r"]:
        print(f"| {p['name']} | API error {p['r']} | | | |")
        continue
    preset, step, line = p["name"].rsplit("_", 2)
    line = int(line)
    nset = fs(p["r"]["address"])
    m = {lab: len(fs(ans(lab, preset, line)) ^ nset) for lab in STEPS}
    n += 1
    for lab in ("B", "T"):
        exact[lab] += m[lab] == 0
        miss[lab] += m[lab]
    k = 0 if m["T"] < m["B"] else 1 if m["T"] > m["B"] else 2
    tot["T vs B"][k] += 1
    tot[f"T vs B ({step})"][k] += 1
    stepcell = ""
    if step in STEPS[1:]:
        prev = STEPS[STEPS.index(step) - 1]
        k = 0 if m[step] < m[prev] else 1 if m[step] > m[prev] else 2
        tot[f"step {step}: after vs before"][k] += 1
        stepcell = f"{prev} ({m[prev]}) {fmt(ans(prev, preset, line))} -> {step} ({m[step]}) {fmt(ans(step, preset, line))}"
    print(f"| {p['name']} {p['lat']:.5f},{p['lng']:.5f} | {fmt(p['r']['address'])} | ({m['B']}) {fmt(ans('B', preset, line))} | "
          f"({m['T']}) {fmt(ans('T', preset, line))} | {stepcell} |")
print(f"\n{n} points scored. exact B {exact['B']}, T {exact['T']}; mismatched fields B {miss['B']}, T {miss['T']}")
print("| comparison | better | worse | same |\n|---|---:|---:|---:|")
for k, v in sorted(tot.items()):
    print(f"| {k} | {v[0]} | {v[1]} | {v[2]} |")
