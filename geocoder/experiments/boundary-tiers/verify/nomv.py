#!/usr/bin/env python3
"""nomv.py <set> <A> <B> <n> : n points of the verify fresh20k sample (seeded) where schemes A and B
give a different Immich country or state; asks the public Nominatim /reverse (jsonv2, addressdetails,
zoom 10 = the admin area containing the point, Accept-Language en), >= 2 s between calls, and prints
each side's (cc, state), the truth's, and Nominatim's. -> cmp/nomv_<A>_<B>_<set>.json"""
import json
import random
import sys
import time
import urllib.parse
import urllib.request

V = "/ssd/tiers2-verify"
UA = "geoshenanigans-tiers2-verify/1.0 (+https://github.com/immich-app/geoshenanigans)"
st, A, B, n = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])


def imm(js):
    ad = json.loads(js).get("address", {})
    return ((ad.get("country_code") or "").lower() or None,
            next((ad[k] for k in ("state", "province") if ad.get(k) is not None), None))


def load(s):
    return [line.rstrip("\n").split(" ", 2) for line in open(f"{V}/q/{s}_{st}/fresh20k.txt")]


a, b, t = load(A), load(B), load("unc")
diff = [i for i in range(len(a)) if imm(a[i][2]) != imm(b[i][2])]
rng = random.Random(918273)
rows = []
for i in sorted(rng.sample(diff, min(n, len(diff)))):
    lat, lng = a[i][0], a[i][1]
    url = "https://nominatim.openstreetmap.org/reverse?" + urllib.parse.urlencode(
        {"lat": lat, "lon": lng, "format": "jsonv2", "addressdetails": 1, "zoom": 10})
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en"})
    with urllib.request.urlopen(req, timeout=30) as r:
        nom = json.loads(r.read().decode())
    time.sleep(2.0)
    ad = nom.get("address", {})
    row = {"i": i, "slice": "border" if i < 10000 else "photo", "lat": lat, "lng": lng, A: imm(a[i][2]), B: imm(b[i][2]),
           "truth": imm(t[i][2]), "nominatim": ((ad.get("country_code") or None), ad.get("state") or ad.get("province")),
           "display": nom.get("display_name")}
    rows.append(row)
    print(json.dumps(row, ensure_ascii=False), flush=True)
json.dump(rows, open(f"{V}/cmp/nomv_{A}_{B}_{st}.json", "w"), ensure_ascii=False, indent=1)
print(f"{len(diff)} disagreeing points in the sample; {len(rows)} asked")
