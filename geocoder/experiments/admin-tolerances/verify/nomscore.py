#!/usr/bin/env python3
"""nomscore.py <tag>... : for each picked point (nom/<tag>.pick.json) and its public Nominatim answer
(nom/<tag>.jsonl, napi.py format), the Immich field mapped the same way, counted as: Nominatim equals
label a / label b / neither; and equals the truth or not."""
import json
import sys
from collections import Counter

for tag in sys.argv[1:]:
    pick = json.load(open(f"/ssd/verify5/nom/{tag}.pick.json"))
    pts = pick["points"]
    c = Counter()
    rows = []
    for line in open(f"/ssd/verify5/nom/{tag}.jsonl", encoding="utf-8"):
        r = json.loads(line)
        p = pts[r["name"]]
        ad = r["r"].get("address", {}) or {}
        cc = ad.get("country_code")
        country = cc.upper() if cc else ad.get("country")
        city = next((ad[k] for k in ("city", "town", "village", "hamlet", "municipality") if ad.get(k) is not None), None)
        state = next((ad[k] for k in ("state", "province") if ad.get(k) is not None), None)
        nf = {"city": city, "state": state, "country": country}[p["field"]]
        fi = {"city": 0, "state": 1, "country": 2}[p["field"]]
        a, b, t = p["a"][fi], p["b"][fi], p["truth"][fi]
        k = "a" if nf == a else "b" if nf == b else "neither"
        c[k] += 1
        c["truth_agrees"] += nf == t
        rows.append((r["name"], r["lat"], r["lng"], nf, a, b, t))
    first = next(iter(pts.values()))
    print(f"{tag}: {first['field']}  a={first['a_label']} b={first['b_label']}  candidates {pick['candidates']}  "
          f"Nominatim = a {c['a']}, = b {c['b']}, neither {c['neither']}; = truth {c['truth_agrees']} of {sum(c[x] for x in ('a', 'b', 'neither'))}")
    for row in rows:
        print("   ", row)
