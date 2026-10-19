"""explain_poi.py <preset> <build A> <build B> <label pass out.json> <census.json> <objs.opl> <out.json>
(parity_probe.s6: the base probe predates the primary-feature debug; no server commit changes the
primary feature.) Third pass over the builder rows still unexplained after explain_label.py: does the answer's POI
side (landmark, or the primary POI that gives road / postcode) come from a POI relation only one
build imports (census poi_only_a / poi_only_b, names from the OPL extract)? Classes:
  POI relation dropped   A's landmark or primary POI names a relation only build A has
  POI relation added     B's landmark or primary POI names a relation only build B has
  UNEXPLAINED            anything else
"""
import collections
import json
import subprocess
import sys

P = "/ssd/parity"


def probe(build, preset, pts):
    inp = "".join(f"{a} {b}\n" for a, b in pts)
    out = subprocess.run([f"{P}/bin/parity_probe.s6", f"{P}/sets/{build}/{preset}"], input=inp,
                         capture_output=True, text=True, check=True).stdout
    return [json.loads(l) for l in out.splitlines()]


def decode_opl(s):
    # OPL escapes non-alphanumerics as %<hex>% ; decode those runs
    out, i = [], 0
    while i < len(s):
        if s[i] == "%":
            j = s.index("%", i + 1)
            out.append(chr(int(s[i + 1:j], 16)))
            i = j + 1
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


def main():
    preset, ba, bb, inp, census, opl, outp = sys.argv[1:8]
    rows = json.load(open(inp))["unexplained"]
    c = json.load(open(census))
    only_a, only_b = set(c["poi_only_a"]), set(c["poi_only_b"])
    names_a, names_b = set(), set()
    for line in open(opl, encoding="utf-8"):
        oid = line.split(" ", 1)[0]
        if oid not in only_a and oid not in only_b:
            continue
        for part in line.rstrip("\n").split(" "):
            if part.startswith("T"):
                for kv in part[1:].split(","):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        if decode_opl(k) in ("name", "name:en"):
                            (names_a if oid in only_a else names_b).add(decode_opl(v))
    pts = [tuple(r["pt"]) for r in rows]
    ja, jb = probe(ba, preset, pts), probe(bb, preset, pts)
    classes, bad = collections.Counter(), []
    for r, a, b in zip(rows, ja, jb):
        poi_a = ((a.get("primary") or {}).get("poi") or {}).get("name")
        poi_b = ((b.get("primary") or {}).get("poi") or {}).get("name")
        la, lb = a["answer"].get("landmark"), b["answer"].get("landmark")
        if {la, poi_a} & names_a:
            cls = "POI relation dropped"
        elif {lb, poi_b} & names_b:
            cls = "POI relation added"
        else:
            cls = "UNEXPLAINED"
            r["poi"] = {"a": [la, poi_a], "b": [lb, poi_b]}
            bad.append(r)
        classes[cls] += 1
    res = {"preset": preset, "rows": len(rows), "classes": dict(classes), "unexplained": bad}
    json.dump(res, open(outp, "w"), ensure_ascii=False, indent=1)
    print(json.dumps({k: res[k] for k in ("preset", "rows", "classes")}, ensure_ascii=False))
    for r in bad[:12]:
        print("  ", json.dumps(r, ensure_ascii=False)[:300])


main()
