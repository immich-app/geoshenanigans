"""mech6.py <preset> : mechanism of the builder step (D: base data -> tip data, server bc2e758 both) on
par30k. For every point whose address the builder step changed, admin_probe (verify6 build at 64183ef)
on base and tip data: the admin/place/postal polygons whose ring holds the point (cell + 8 neighbours)
and the street's admin_parents chain, with OSM ids from the admin_polygons.osm_ids sidecar.
Classes: pip (a polygon at the point was added or dropped), chain (only the street's parent chain
changed), other. Writes /ssd/verify6/cmp/mech_<preset>.json (per point: class, added, dropped) and
/ssd/verify6/cmp/mech_<preset>_ids.txt (OSM ids of every added / dropped polygon) and prints a summary."""
import json
import subprocess
import sys
from collections import Counter

V = "/ssd/verify6/"
preset = sys.argv[1]
SIDE = {"smallest": "admin-minimal", "recommended": "quality/q2.5", "max": "quality/uncapped"}
pts = open(V + "pts/par30k.txt").read().splitlines()
idx = json.load(open(f"{V}cmp/steps_{preset}.json"))["D"]
inp = "".join(pts[i] + "\n" for i in idx)


def probe(data):
    out = subprocess.run([V + "bin/admin_probe.64183ef", f"{V}sets/{data}/{preset}",
                          f"{V}build/out-{data}/planet/{SIDE[preset]}/admin_polygons.osm_ids"],
                         input=inp, capture_output=True, text=True, check=True).stdout.splitlines()
    return [json.loads(l) for l in out]


ans = {lab: [json.loads(l.split(" ", 2)[2]).get("address") or {} for l in open(f"{V}q/{lab}_{preset}/par30k.txt", encoding="utf-8")]
       for lab in ("B", "D")}
pb, pt = probe("base"), probe("tip")
key = lambda p: (p.get("osm"), p.get("level"), p.get("name"))
res, cls, ids = [], Counter(), Counter()
for j, i in enumerate(idx):
    a, b = pb[j], pt[j]
    sa, sb = {key(p) for p in a["pip"]}, {key(p) for p in b["pip"]}
    dropped, added = sorted(sa - sb, key=str), sorted(sb - sa, key=str)
    ca = [key(p) for p in a["chain"]]
    cb = [key(p) for p in b["chain"]]
    c = "pip" if dropped or added else "chain" if ca != cb else "other"
    cls[c] += 1
    for k in dropped:
        ids[("dropped", k[0], k[1])] += 1
    for k in added:
        ids[("added", k[0], k[1])] += 1
    if c == "chain":
        for k in set(ca) - set(cb):
            ids[("chain_dropped", k[0], k[1])] += 1
        for k in set(cb) - set(ca):
            ids[("chain_added", k[0], k[1])] += 1
    res.append({"line": i + 1, "class": c, "dropped": dropped, "added": added,
                "chain_base": ca if c == "chain" else None, "chain_tip": cb if c == "chain" else None,
                "B": ans["B"][i], "D": ans["D"][i]})
json.dump(res, open(f"{V}cmp/mech_{preset}.json", "w"), ensure_ascii=False)
with open(f"{V}cmp/mech_{preset}_ids.txt", "w") as f:
    for (kind, osm, lvl), n in sorted(ids.items(), key=lambda x: -x[1]):
        f.write(f"{kind}\t{osm}\t{lvl}\t{n}\n")
print(preset, "builder-step changes", len(idx), dict(cls))
lv = Counter((k[0], k[2]) for k in ids)
print("distinct polygons by kind/level:", dict(sorted(lv.items(), key=str)))
