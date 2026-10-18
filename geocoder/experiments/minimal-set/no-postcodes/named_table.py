#!/usr/bin/env python3
"""Markdown table of the named points across variants, Immich prototype
mapping (city = city ?? town ?? village ?? hamlet ?? municipality, state =
state ?? province, country code, landmark) plus postcode and road."""
import json

OUT = "/ssd/shrink/no-postcodes/out"
VARS = ["recommended", "rec_nopc", "admin_nopc_nopoi", "smallest", "minimal_poi"]
NAMES = ["Eiffel Tower", "Times Square", "Weehawken", "rural NL", "Shibuya", "Heathrow T5", "Uluru"]


def load(v):
    return [json.loads(l.split(" ", 2)[2]) for l in open(f"{OUT}/{v}.p207.jsonl")][:7]


def cell(r):
    a = r.get("address", {})
    city = a.get("city") or a.get("town") or a.get("village") or a.get("hamlet") or a.get("municipality")
    parts = [city or "-", a.get("state") or a.get("province") or "-", a.get("country_code") or "-",
             "lm=" + (a.get("landmark") or "-"), "pc=" + (a.get("postcode") or "-"), "road=" + (a.get("road") or "-")]
    return " / ".join(parts)


rows = {v: load(v) for v in VARS}
print("| point | " + " | ".join(VARS) + " |")
print("|" + "---|" * (len(VARS) + 1))
for i, n in enumerate(NAMES):
    print(f"| {n} | " + " | ".join(cell(rows[v][i]) for v in VARS) + " |")
