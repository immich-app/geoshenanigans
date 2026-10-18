#!/usr/bin/env python3
"""Which candidate agrees with the public Nominatim where two candidates disagree?

Picks points where <label A> and <label B> (same set) give a different Immich country or state,
stratified over pools (seeded), asks nominatim.openstreetmap.org /reverse (format=jsonv2,
addressdetails=1, Accept-Language en) at zoom 10 (admin area containing the point) and zoom 18
(the default: nearest object), >= 1.6 s between calls, and records whose country_code / state
matches. Also records the truth (uncapped-with-holes, standard reader) answer for each point.
usage: nomcheck.py <set> <label A> <label B> <per-pool n> <pool> ... -> cmp/nom_<A>_<B>_<set>.json
"""
import json
import random
import sys
import time
import urllib.parse
import urllib.request

UA = "geoshenanigans-tiers2/1.0 (+https://github.com/immich-app/geoshenanigans)"
st, A, B, per = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
pools = sys.argv[5:]
Q = "/ssd/tiers2/q"


def imm(js):
    ad = json.loads(js).get("address", {})
    c = lambda *k: next((ad[x] for x in k if ad.get(x) is not None), None)
    return {"cc": (ad.get("country_code") or "").lower() or None, "state": c("state", "province")}


def get(params):
    url = "https://nominatim.openstreetmap.org/reverse?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                out = json.loads(r.read().decode())
            time.sleep(1.6)
            return out
        except Exception as e:  # noqa: BLE001
            time.sleep(5)
            err = str(e)
    return {"error": err}


rng = random.Random(20261018)
res = []
for p in pools:
    a = [l.rstrip("\n").split(" ", 2) for l in open(f"{Q}/{A}_{st}/{p}.txt")]
    b = [l.rstrip("\n").split(" ", 2) for l in open(f"{Q}/{B}_{st}/{p}.txt")]
    t = [l.rstrip("\n").split(" ", 2) for l in open(f"{Q}/unc_{st}/{p}.txt")]
    diff = [i for i in range(len(a)) if imm(a[i][2]) != imm(b[i][2])]
    for i in sorted(rng.sample(diff, min(per, len(diff)))):
        lat, lng = float(a[i][0]), float(a[i][1])
        row = {"pool": p, "lat": lat, "lng": lng, "A": imm(a[i][2]), "B": imm(b[i][2]), "truth": imm(t[i][2])}
        for z in (10, 18):
            r = get({"lat": lat, "lon": lng, "format": "jsonv2", "addressdetails": 1, "zoom": z})
            ad = r.get("address", {}) if isinstance(r, dict) else {}
            row[f"nom{z}"] = {"cc": ad.get("country_code"), "state": ad.get("state") or ad.get("province"),
                              "display": r.get("display_name") if isinstance(r, dict) else None}
        res.append(row)
        print(json.dumps(row, ensure_ascii=False), flush=True)
json.dump(res, open(f"/ssd/tiers2/cmp/nom_{A}_{B}_{st}.json", "w"), ensure_ascii=False, indent=1)


def agree(x, n):
    return x["cc"] == n["cc"] and (x["state"] == n["state"] or not n["state"])


for z in (10, 18):
    ca = sum(agree(r["A"], r[f"nom{z}"]) for r in res)
    cb = sum(agree(r["B"], r[f"nom{z}"]) for r in res)
    ct = sum(agree(r["truth"], r[f"nom{z}"]) for r in res)
    cca = sum(r["A"]["cc"] == r[f"nom{z}"]["cc"] for r in res)
    ccb = sum(r["B"]["cc"] == r[f"nom{z}"]["cc"] for r in res)
    print(f"zoom {z}: {len(res)} points; country+state agree: {A} {ca}, {B} {cb}, truth {ct}; country agree: {A} {cca}, {B} {ccb}")
