"""score.py <api.jsonl> <fields: place|all> <label>=<grid_query>:<set dir> [...]
Runs each server on the API points and scores its address against Nominatim's.
place fields: country .. neighbourhood (the admin/place ladder); all: those + road, postcode.
Per point and server: mismatches = size of the symmetric difference of the field=value sets.
Prints per point a row (Nominatim | each server, '*' marks an exact match) and a summary:
exact matches, total mismatches, and for each server after the first: better / worse / same
against the first.
"""
import json
import subprocess
import sys

PLACE = ["country", "state", "region", "province", "state_district", "county", "district", "municipality", "city",
         "town", "village", "hamlet", "city_district", "borough", "suburb", "quarter", "neighbourhood", "city_block"]
api = [json.loads(l) for l in open(sys.argv[1], encoding="utf-8")]
fields = PLACE + (["road", "postcode"] if sys.argv[2] == "all" else [])
servers = []
for spec in sys.argv[3:]:
    label, rest = spec.split("=", 1)
    gq, d = rest.split(":", 1)
    inp = "".join(f"{p['lat']} {p['lng']}\n" for p in api)
    out = subprocess.run([gq, d], input=inp, capture_output=True, text=True, check=True).stdout.splitlines()
    servers.append((label, [json.loads(l.split(" ", 2)[2]).get("address") or {} for l in out]))


def fs(a):
    return {(k, a[k]) for k in fields if a.get(k)}


def fmt(a):
    return ", ".join(f"{k}={a[k]}" for k in fields if a.get(k))


exact = [0] * len(servers)
miss = [0] * len(servers)
cmp = [[0, 0, 0] for _ in servers]
print("| point | Nominatim | " + " | ".join(l for l, _ in servers) + " |")
print("|---" * (2 + len(servers)) + "|")
for i, p in enumerate(api):
    n = p["r"].get("address", {})
    nset = fs(n)
    m = [len(fs(ans[i]) ^ nset) for _, ans in servers]
    for s in range(len(servers)):
        exact[s] += m[s] == 0
        miss[s] += m[s]
        if s:
            cmp[s][0 if m[s] < m[0] else 1 if m[s] > m[0] else 2] += 1
    cells = [("*" if m[s] == 0 else f"({m[s]}) ") + fmt(servers[s][1][i]) for s in range(len(servers))]
    print(f"| {p['name']} | {fmt(n)} | " + " | ".join(cells) + " |")
print()
for s, (label, _) in enumerate(servers):
    extra = f", vs {servers[0][0]}: better {cmp[s][0]}, worse {cmp[s][1]}, same {cmp[s][2]}" if s else ""
    print(f"{label}: exact {exact[s]}/{len(api)}, mismatched fields {miss[s]}{extra}")
