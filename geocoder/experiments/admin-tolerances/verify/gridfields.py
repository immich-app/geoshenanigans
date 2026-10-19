#!/usr/bin/env python3
"""gridfields.py <truth label> <label>... : Immich-field wrong counts per enclave grid (bugs2 grids/all.txt
order), with the most common (truth -> label) city substitutions."""
import json
import sys
from collections import Counter, defaultdict

sys.path.insert(0, "/ssd/verify5")
names = [l.split()[0] for l in open(__import__("os").environ.get("GRIDS", "/ssd/bugs2/grids/all.txt"))]


def fields(js):
    ad = json.loads(js).get("address", {})
    cc = ad.get("country_code")
    country = cc.upper() if cc else ad.get("country")
    if not country:
        return (None, None, None)
    city = next((ad[k] for k in ("city", "town", "village", "hamlet", "municipality") if ad.get(k) is not None), None)
    state = next((ad[k] for k in ("state", "province") if ad.get(k) is not None), None)
    return (city, state, country)


def load(lab):  # noqa
    return [fields(l.split(" ", 2)[2]) for l in open(f"/ssd/verify5/q/{lab}/grids.txt")]


t = load(sys.argv[1])
for lab in sys.argv[2:]:
    o = load(lab)
    per = defaultdict(Counter)
    subs = Counter()
    for g, a, b in zip(names, t, o):
        per[g]["n"] += 1
        for i, f in enumerate(("city", "state", "country")):
            if a[i] != b[i]:
                per[g][f] += 1
        if a[0] != b[0]:
            subs[(g, a[0], b[0])] += 1
    print(lab, " ".join(f"{g}:c{c['city']}/s{c['state']}/k{c['country']}" for g, c in sorted(per.items()) if c["city"] + c["state"] + c["country"]))
    print("   top city subs:", subs.most_common(4))
