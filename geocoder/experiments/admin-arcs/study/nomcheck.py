#!/usr/bin/env python3
"""nomcheck.py <set> <label A> <label B> <per-pool n> <pool> ... : where two candidates give a
different Immich (city, state, country), which one agrees with the public Nominatim?

Points sampled per pool (seeded) among the disagreements; nominatim.openstreetmap.org /reverse
(format=jsonv2, addressdetails=1, Accept-Language en) at zoom 10 and zoom 18, >= 1.6 s between
calls. Immich mapping on both sides: city = city ?? town ?? village ?? hamlet ?? municipality,
state = state ?? province, country = country_code. Also records the truth answer.
-> /ssd/arcs/cmp/nom_<A>_<B>_<set>.json and a summary.
"""
import json
import random
import sys
import time
import urllib.parse
import urllib.request

UA = "geoshenanigans-arcs-study/1.0 (+https://github.com/immich-app/geoshenanigans)"
st, A, B, per = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
pools = sys.argv[5:]
Q = "/ssd/arcs/q"


def imm_ad(ad):
    c = lambda *k: next((ad[x] for x in k if ad.get(x) is not None), None)  # noqa: E731
    return {"city": c("city", "town", "village", "hamlet", "municipality"), "state": c("state", "province"),
            "cc": (ad.get("country_code") or "").lower() or None}


def imm(js):
    return imm_ad(json.loads(js).get("address", {}))


def get(params):
    url = "https://nominatim.openstreetmap.org/reverse?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Language": "en"})
    err = ""
    for _ in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                out = json.loads(r.read().decode())
            time.sleep(1.6)
            return out
        except Exception as e:  # noqa: BLE001
            err = str(e)
            time.sleep(5)
    return {"error": err}


rng = random.Random(20261019)
res = []
for p in pools:
    a = [line.rstrip("\n").split(" ", 2) for line in open(f"{Q}/{A}_{st}/{p}.txt")]
    b = [line.rstrip("\n").split(" ", 2) for line in open(f"{Q}/{B}_{st}/{p}.txt")]
    t = [line.rstrip("\n").split(" ", 2) for line in open(f"{Q}/unc_{st}/{p}.txt")]
    diff = [i for i in range(len(a)) if imm(a[i][2]) != imm(b[i][2])]
    for i in sorted(rng.sample(diff, min(per, len(diff)))):
        lat, lng = float(a[i][0]), float(a[i][1])
        row = {"pool": p, "lat": lat, "lng": lng, "A": imm(a[i][2]), "B": imm(b[i][2]), "truth": imm(t[i][2])}
        for z in (10, 18):
            r = get({"lat": lat, "lon": lng, "format": "jsonv2", "addressdetails": 1, "zoom": z})
            row[f"nom{z}"] = imm_ad(r.get("address", {}) if isinstance(r, dict) else {})
        res.append(row)
        print(json.dumps(row, ensure_ascii=False), flush=True)
json.dump(res, open(f"/ssd/arcs/cmp/nom_{A}_{B}_{st}.json", "w"), ensure_ascii=False, indent=1)


def fields_differ(r):
    return [f for f in ("city", "state", "cc") if r["A"][f] != r["B"][f]]


for z in (10, 18):
    ca = cb = ct = 0
    for r in res:
        n = r[f"nom{z}"]
        fs = fields_differ(r)
        ca += all(r["A"][f] == n[f] for f in fs)
        cb += all(r["B"][f] == n[f] for f in fs)
        ct += all(r["truth"][f] == n[f] for f in fs)
    print(f"NOM zoom {z}: {len(res)} points; on the fields where they differ Nominatim agrees with {A} {ca}, {B} {cb}, truth {ct}")
