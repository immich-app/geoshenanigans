"""explain_label.py <preset> <build A> <build B> <explain_builder out.json> <out.json>
Second pass over explain_builder's UNEXPLAINED rows: same polygons hold the point in both builds,
so does one of them carry another place label (place_type_override, from the place node linked to
it) in build B? Classes:
  label moved      a polygon holding the point (pip or street chain) has another pto in B
  UNEXPLAINED      anything else (listed)
Also tallies the (old pto -> new pto) pairs. pto values are AdminPlaceType (types.h).
"""
import collections
import json
import subprocess
import sys

P = "/ssd/parity"
SIDE = {"smallest": "admin-minimal", "recommended": "quality/q2.5", "max": "quality/uncapped"}
PTO = {0: "-", 1: "city", 2: "town", 3: "village", 4: "suburb", 5: "neighbourhood", 6: "quarter", 7: "state",
       8: "province", 9: "region", 10: "county", 11: "district", 12: "borough", 13: "hamlet", 14: "municipality"}


def probe(build, preset, pts):
    d = f"{P}/sets/{build}/{preset}"
    side = f"{P}/build/out-{build}/planet/{SIDE[preset]}/admin_polygons.osm_ids"
    inp = "".join(f"{a} {b}\n" for a, b in pts)
    out = subprocess.run([f"{P}/bin/admin_probe.base", d, side], input=inp, capture_output=True, text=True,
                         check=True).stdout
    return [json.loads(l) for l in out.splitlines()]


def labels(j):
    return {(p.get("osm") or f"id{p['id']}").split("/")[0]: (p["pto"], p["level"], p["name"])
            for p in j["pip"] + j.get("chain", [])}


def main():
    preset, ba, bb, inp, outp = sys.argv[1:6]
    rows = [r for r in json.load(open(inp))["unexplained"]]
    pts = [tuple(r["pt"]) for r in rows]
    ja, jb = probe(ba, preset, pts), probe(bb, preset, pts)
    classes, pairs, bad = collections.Counter(), collections.Counter(), []
    for r, a, b in zip(rows, ja, jb):
        la, lb = labels(a), labels(b)
        moved = [(k, la[k], lb[k]) for k in la.keys() & lb.keys() if la[k][0] != lb[k][0]]
        if moved:
            classes["label moved"] += 1
            for k, (pa, lvl, name), (pb, _, _) in moved:
                pairs[f"L{lvl} {PTO.get(pa, pa)} -> {PTO.get(pb, pb)}"] += 1
            r["moved"] = [(k, la[k][2], PTO.get(la[k][0]), PTO.get(lb[k][0])) for k, *_ in moved]
        else:
            classes["UNEXPLAINED"] += 1
            bad.append(r)
    res = {"preset": preset, "rows": len(rows), "classes": dict(classes), "pto_changes": dict(pairs.most_common()),
           "examples": [r for r in rows if "moved" in r][:10], "unexplained": bad}
    json.dump(res, open(outp, "w"), ensure_ascii=False, indent=1)
    print(json.dumps({k: res[k] for k in ("preset", "rows", "classes", "pto_changes")}, ensure_ascii=False))


main()
