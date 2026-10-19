"""deg_sample.py pick | score <api.jsonl> : the degree-distance experiment (deg6.sh) against Nominatim.
pick : 15 random (seed 7) par30k points whose recommended answer differs between the tip (T) and the
       degree-distance variant (DEG) -> /ssd/verify6/api/deg_pts.txt
score: place-field mismatches (as score6.py) of T and DEG vs Nominatim; better / worse / same of DEG."""
import json
import random
import sys

V = "/ssd/verify6/"
PLACE = ["country", "state", "region", "province", "state_district", "county", "district", "municipality", "city",
         "town", "village", "hamlet", "city_block", "city_district", "borough", "suburb", "quarter", "neighbourhood"]
load = lambda lab: [json.loads(l.split(" ", 2)[2]).get("address") or {} for l in open(f"{V}q/{lab}_recommended/par30k.txt", encoding="utf-8")]
T, D = load("T"), load("DEG")
pts = open(V + "pts/par30k.txt").read().splitlines()
if sys.argv[1] == "pick":
    diff = [i for i in range(len(T)) if T[i] != D[i]]
    lat_abs = [abs(float(pts[i].split()[0])) for i in diff]
    print(len(diff), "differ; median |lat|", sorted(lat_abs)[len(lat_abs) // 2])
    with open(V + "api/deg_pts.txt", "w") as f:
        for i in random.Random(7).sample(diff, 15):
            f.write(f"deg_{i + 1} {pts[i]}\n")
    sys.exit(0)
fs = lambda a: {(k, a[k]) for k in PLACE if a.get(k)}
fmt = lambda a: ", ".join(f"{k}={a[k]}" for k in PLACE if a.get(k))
b = w = s = 0
for l in open(sys.argv[2], encoding="utf-8"):
    p = json.loads(l)
    i = int(p["name"].split("_")[1]) - 1
    n = fs(p["r"].get("address", {}))
    mt, md = len(fs(T[i]) ^ n), len(fs(D[i]) ^ n)
    b += md < mt; w += md > mt; s += md == mt
    print(f"| {p['name']} | {fmt(p['r'].get('address', {}))} | ({mt}) {fmt(T[i])} | ({md}) {fmt(D[i])} |")
print(f"\nDEG vs T against Nominatim: better {b}, worse {w}, same {s}")
