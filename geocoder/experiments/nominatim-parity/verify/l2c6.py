"""l2c6.py list | score <api.jsonl> : loc-to-city checks whose pass state differs between verify6's
base (B) and tip (T) runs (/ssd/verify6/l2c/<label>/eval.json), per preset.
list : prints each changed check (expectation, tiers, B and T city-ish fields) and writes the distinct
       points to /ssd/verify6/api/l2c_pts.txt ("l2c_<n> lat lng").
score: for each changed check point, whether B's and T's city/town/village fields equal Nominatim's
       (API, zoom 18) and which loc-to-city expectation Nominatim itself would meet."""
import json
import sys

V = "/ssd/verify6/"
F = ("city", "town", "village", "hamlet", "municipality", "suburb", "county", "state", "country_code")
CTV = ("city", "town", "village")
checks = {c["id"]: c for c in json.load(open("/ssd/transport-verify/l2c/checks_v.json", encoding="utf-8"))}
ev = {lab: json.load(open(f"{V}l2c/{lab}/eval.json"))["results"] for lab in ("B", "T")}
pts = open(V + "pts/l2c.txt").read().splitlines()
pidx = {}
for i, p in enumerate(pts):
    a, b = (float(x) for x in p.split()[:2])
    pidx.setdefault((round(a, 5), round(b, 5)), i)
ans = {}
for lab in ("B", "T"):
    for s in ("smallest", "recommended", "max"):
        ans[(lab, s)] = [json.loads(l.split(" ", 2)[2]).get("address") or {} for l in open(f"{V}q/{lab}_{s}/l2c.txt", encoding="utf-8")]
changed = []
for key in sorted(ev["B"]):
    pb, pt = ev["B"][key]["pass_"], ev["T"][key]["pass_"]
    if pb != pt:
        s, cid = key.split("|", 1)
        lat, lng = checks[cid]["at"]
        changed.append((s, cid, pb, pt, pidx[(round(lat, 5), round(lng, 5))]))
mode = sys.argv[1]
if mode == "list":
    names = {}
    for s, cid, pb, pt, i in changed:
        c = checks[cid]
        print(f"{s} {cid} '{c['name']}' expect city={c['city']}: B {'pass' if pb else 'FAIL'} -> T {'pass' if pt else 'FAIL'}")
        for lab in ("B", "T"):
            a = ans[(lab, s)][i]
            print(f"   {lab}: {json.dumps({k: a[k] for k in F if k in a}, ensure_ascii=False)}")
        names.setdefault(i, f"l2c_{i + 1}")
    with open(V + "api/l2c_pts.txt", "w") as f:
        for i, n in sorted(names.items()):
            f.write(f"{n} {pts[i]}\n")
    print(len(changed), "changed checks,", len(names), "distinct points")
else:
    api = {int(json.loads(l)["name"].split("_")[1]) - 1: json.loads(l)["r"].get("address", {}) for l in open(sys.argv[2], encoding="utf-8")}
    eqB = eqT = 0
    print("| preset | check | expect | Nominatim city/town/village | B | T | B=N | T=N |")
    print("|---|---|---|---|---|---|---|---|")
    for s, cid, pb, pt, i in changed:
        n = api[i]
        nv = {k: n[k] for k in CTV if k in n}
        b = {k: ans[("B", s)][i][k] for k in CTV if k in ans[("B", s)][i]}
        t = {k: ans[("T", s)][i][k] for k in CTV if k in ans[("T", s)][i]}
        eqB += b == nv
        eqT += t == nv
        exp = checks[cid]["city"]
        print(f"| {s} | {checks[cid]['name']} | {exp.get('v') if exp else '-'} | {nv} | {b} | {t} | {b == nv} | {t == nv} |")
    print(f"\n{len(changed)} changed checks: city/town/village equal Nominatim's: B {eqB}, T {eqT}")
