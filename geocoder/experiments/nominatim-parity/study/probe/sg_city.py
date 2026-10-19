"""sg_city.py <q root> <pool,pool,...> <label,label,...> : per label and preset, the answers with
country_code SG and how many of them carry city=Singapore (and any city at all).
Answers are runq.sh output: <q root>/<label>_<preset>/<pool>.txt ("lat lng <json>" lines)."""
import json
import os
import sys

qroot, pools, labels = sys.argv[1], sys.argv[2].split(","), sys.argv[3].split(",")
print("| label | preset | SG answers | city=Singapore | no city |")
print("|---|---|---:|---:|---:|")
for lab in labels:
    for s in ("smallest", "recommended", "max"):
        n = city_sg = no_city = 0
        for p in pools:
            path = f"{qroot}/{lab}_{s}/{p}.txt"
            if not os.path.exists(path):
                continue
            for line in open(path, encoding="utf-8"):
                a = (json.loads(line.split(" ", 2)[2]).get("address") or {})
                if (a.get("country_code") or "").upper() != "SG":
                    continue
                n += 1
                city_sg += a.get("city") == "Singapore"
                no_city += "city" not in a
        print(f"| {lab} | {s} | {n} | {city_sg} | {no_city} |")
