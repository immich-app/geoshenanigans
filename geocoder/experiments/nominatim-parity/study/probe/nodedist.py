"""nodedist.py <parity_probe> <set dir> <api.jsonl> [name substring] : for each API point, the
place nodes (suburb/hamlet/quarter/neighbourhood within 3 km) with their distance and offset
(north/east metres) from the query, marked N when Nominatim's answer names them, A when ours does."""
import json
import subprocess
import sys

probe, d, apif = sys.argv[1:4]
sub = sys.argv[4] if len(sys.argv) > 4 else ""
api = [json.loads(l) for l in open(apif, encoding="utf-8") if sub in l]
inp = "".join(f"{p['lat']} {p['lng']}\n" for p in api)
out = subprocess.run([probe, d], input=inp, capture_output=True, text=True, check=True).stdout.splitlines()
import math
for p, line in zip(api, out):
    j = json.loads(line)
    n = p["r"].get("address", {})
    a = j["answer"]
    print(p["name"], p["lat"], p["lng"], "| cb", j["places"]["current_boundary"])
    for x in j["places"]["nearby_nodes"]:
        if x["type"] not in ("suburb", "hamlet", "quarter", "neighbourhood") or x["dist_m"] > 3000:
            continue
        dn = (x["lat"] - p["lat"]) * 111320
        de = (x["lng"] - p["lng"]) * 111320 * math.cos(math.radians(p["lat"]))
        mark = ("N" if x["name"] in n.values() else "-") + ("A" if x["name"] in a.values() else "-")
        print(f"   {mark} {x['type']:13s} {x['dist_m']:7.0f} m  N{dn:+7.0f} E{de:+7.0f}  {x['name']}")
