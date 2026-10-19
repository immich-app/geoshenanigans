#!/usr/bin/env python3
"""nom3.py <set> <pool> <field: country|state|city> <A> <B> <n> [seed] : n points of /ssd/tiers3 pool
answers (q/<label>_<set>/<pool>.txt) where schemes A and B give a different Immich <field>; asks the public
Nominatim /reverse (jsonv2, addressdetails, zoom 18, Accept-Language en), >= 2 s between calls, and
counts which side Nominatim agrees with (country by code; state / city by case-insensitive name, Immich's
city = city ?? town ?? village ?? hamlet ?? municipality on both sides). Also the truth (unc).
-> cmp/nom3_<A>_<B>_<pool>_<field>.json and a summary line."""
import json
import random
import sys
import time
import urllib.parse
import urllib.request

T = "/ssd/tiers3"
UA = "geoshenanigans-tiers3-study/1.0 (zack@futo.org)"
st, pool, fld, A, B, n = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], int(sys.argv[6])
seed = int(sys.argv[7]) if len(sys.argv) > 7 else 3301


def imm(ad):
    co = lambda *ks: next((ad[k] for k in ks if ad.get(k) is not None), None)  # noqa: E731
    cc = ad.get("country_code")
    v = {"country": cc.lower() if cc else None, "state": co("state", "province"),
         "city": co("city", "town", "village", "hamlet", "municipality")}[fld]
    return v.lower() if isinstance(v, str) else v


def load(lab):
    return [line.rstrip("\n").split(" ", 2) for line in open(f"{T}/q/{lab}_{st}/{pool}.txt")]


a, b, t = load(A), load(B), load("unc")
get = lambda row: imm(json.loads(row[2]).get("address", {}))  # noqa: E731
diff = [i for i in range(len(a)) if get(a[i]) != get(b[i])]
rng = random.Random(seed)
rows, agree = [], {"A": 0, "B": 0, "truth": 0, "neither": 0}
for i in sorted(rng.sample(diff, min(n, len(diff)))):
    lat, lng = a[i][0], a[i][1]
    url = "https://nominatim.openstreetmap.org/reverse?" + urllib.parse.urlencode(
        {"lat": lat, "lon": lng, "format": "jsonv2", "addressdetails": 1, "zoom": 18})
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            nom = json.loads(r.read().decode())
    except Exception as e:  # noqa: BLE001
        nom = {"error": str(e)}
    time.sleep(2.0)
    nv = imm(nom.get("address", {}))
    va, vb, vt = get(a[i]), get(b[i]), get(t[i])
    agree["A"] += nv == va
    agree["B"] += nv == vb
    agree["truth"] += nv == vt
    agree["neither"] += nv not in (va, vb)
    rows.append({"i": i, "lat": lat, "lng": lng, A: va, B: vb, "truth": vt, "nominatim": nv})
json.dump(rows, open(f"{T}/cmp/nom3_{A}_{B}_{pool}_{fld}.json", "w"), ensure_ascii=False, indent=1)
print(f"{pool}:{fld} {A} vs {B}: {len(diff)} disagreeing points, {len(rows)} asked; Nominatim agrees with "
      f"{A} {agree['A']}, {B} {agree['B']}, truth {agree['truth']}, neither {agree['neither']}")
