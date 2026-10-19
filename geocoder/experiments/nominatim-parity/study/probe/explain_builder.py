"""Explain every changed answer between two builds queried by the same server.

usage: python -I explain_builder.py <preset> <build A> <build B> <answers A> <answers B> <out.json>
  build X: name under /ssd/parity (out dir /ssd/parity/build/out-X, sets /ssd/parity/sets/X/<preset>)
  answers: grid_query output of the same points on each build's set.

Each changed answer (address dict) is classed by the first rule that fits:
  containment   the polygons holding the point (admin_probe pip, any level) or the street's parent
                chain (max) differ, and every polygon in the difference is one only build A or only
                build B has (identity: relation id or way id from the osm_ids sidecar)
  place node    same polygons; a changed place field's old value names a place node only A lists
                near the point, or its new value one only B lists (a node's link to a relation that
                one build lacks hides or shows it)
  postcode      only the postcode changed, and the old value is the code of a postal polygon only A
                has or the new value one only B has
  UNEXPLAINED   anything else
Writes counts, per-class examples, the responsible polygons and every unexplained row.
"""
import collections
import json
import os
import struct
import subprocess
import sys

import numpy as np

P = "/ssd/parity"
SIDE = {"smallest": "admin-minimal", "recommended": "quality/q2.5", "max": "quality/uncapped"}
PLACE = {"city", "town", "village", "suburb", "hamlet", "quarter", "neighbourhood"}
PDT = np.dtype([("off", "<u4"), ("cnt", "<u4"), ("name", "<u4"), ("lvl", "u1"), ("pto", "u1"), ("p2", "u1"),
                ("p3", "u1"), ("area", "<f4"), ("cc", "<u2"), ("holes", "<u2")])


class Strings:
    def __init__(self, d):
        self.tiers = []
        for t in json.load(open(f"{d}/strings_layout.json"))["tiers"]:
            f = f"{d}/{t['file']}"
            m = np.memmap(f, np.uint8, "r") if os.path.exists(f) and os.path.getsize(f) else None
            self.tiers.append((t["start"], t["end"], m))

    def get(self, off):
        for start, end, m in self.tiers:
            if start <= off < end and m is not None:
                lo = e = off - start
                while e < len(m) and m[e] != 0:
                    e += 1
                return bytes(m[lo:e]).decode("utf-8", "replace")
        return ""


def identities(out, preset):
    """slot -> (identity, level, name) of every live admin polygon of the build's preset."""
    d = f"{out}/planet/{SIDE[preset]}"
    polys = np.fromfile(f"{d}/admin_polygons.bin", PDT)
    raw = open(f"{d}/admin_polygons.osm_ids", "rb").read()
    strings = Strings(f"{P}/sets/{os.path.basename(out)[4:]}/{preset}")
    res = {}
    for i, p in enumerate(polys):
        if p["cnt"] == 0:
            continue
        ty, fl, _, _, sid = struct.unpack_from("<BBBBQ", raw, 12 + 12 * i)
        if fl & 1:
            continue
        ident = f"r{sid >> 16}" if ty == 3 else f"t{ty}:{sid}"
        res.setdefault(ident, (int(p["lvl"]), strings.get(int(p["name"]))))
    return res


def run(binary, d, extra, pts):
    if not pts:
        return []
    inp = "".join(f"{a} {b}\n" for a, b in pts)
    out = subprocess.run([binary, d] + extra, input=inp, capture_output=True, text=True, check=True).stdout
    return [json.loads(l) for l in out.splitlines()]


def ident(p):
    return p.get("osm", "").split("/")[0] or f"id{p.get('id')}"


def load(path):
    out = []
    for line in open(path, encoding="utf-8"):
        a, b, j = line.split(" ", 2)
        out.append(((a, b), json.loads(j).get("address") or {}))
    return out


def main():
    preset, ba, bb, ansa, ansb, outp = sys.argv[1:7]
    oa, ob = f"{P}/build/out-{ba}", f"{P}/build/out-{bb}"
    sa, sb = f"{P}/sets/{ba}/{preset}", f"{P}/sets/{bb}/{preset}"
    ia, ib = identities(oa, preset), identities(ob, preset)
    only_a = {k: v for k, v in ia.items() if k not in ib}
    only_b = {k: v for k, v in ib.items() if k not in ia}
    A, B = load(ansa), load(ansb)
    changed = [(p, a, b) for (p, a), (_, b) in zip(A, B) if a != b]
    pts = [p for p, _, _ in changed]
    side = SIDE[preset]
    pa = run(f"{P}/bin/admin_probe.base", sa, [f"{oa}/planet/{side}/admin_polygons.osm_ids"], pts)
    pb = run(f"{P}/bin/admin_probe.base", sb, [f"{ob}/planet/{side}/admin_polygons.osm_ids"], pts)
    classes, examples, responsible, unexplained = collections.Counter(), collections.defaultdict(list), collections.Counter(), []
    need_nodes = []
    for k, ((pt, a, b), ja, jb) in enumerate(zip(changed, pa, pb)):
        f = sorted(x for x in set(a) | set(b) if a.get(x) != b.get(x))
        ra = {ident(p) for p in ja["pip"]} | {ident(p) for p in ja.get("chain", [])}
        rb = {ident(p) for p in jb["pip"]} | {ident(p) for p in jb.get("chain", [])}
        diff = ra ^ rb
        row = {"pt": pt, "fields": f, "a": {x: a.get(x) for x in f}, "b": {x: b.get(x) for x in f}}
        if diff:
            if all(r in only_a or r in only_b for r in diff):
                cls = "containment"
                row["why"] = [("-" if r in only_a else "+") + f"{r} L{(only_a.get(r) or only_b.get(r))[0]} "
                              f"{(only_a.get(r) or only_b.get(r))[1]}" for r in sorted(diff)]
                for w in row["why"]:
                    responsible[w] += 1
            else:
                cls = "UNEXPLAINED containment"
                row["why"] = sorted(diff)
        elif f == ["postcode"]:
            codes_a = {v[1] for v in only_a.values() if v[0] == 11}
            codes_b = {v[1] for v in only_b.values() if v[0] == 11}
            cls = "postcode" if (a.get("postcode") in codes_a or b.get("postcode") in codes_b) else "UNEXPLAINED postcode"
        elif set(f) & PLACE:
            need_nodes.append((k, row))
            continue
        else:
            cls = "UNEXPLAINED"
        classes[cls] += 1
        (unexplained if cls.startswith("UNEXPLAINED") else examples[cls]).append(row)
    if need_nodes:
        npts = [changed[k][0] for k, _ in need_nodes]
        na = run(f"{P}/bin/parity_probe.base", sa, [], npts)
        nb = run(f"{P}/bin/parity_probe.base", sb, [], npts)
        for (k, row), ja, jb in zip(need_nodes, na, nb):
            key = lambda n: (n["type"], n["name"], round(n["lat"], 6), round(n["lng"], 6))
            sa_ = {key(n) for n in ja["places"].get("nearby_nodes", [])}
            sb_ = {key(n) for n in jb["places"].get("nearby_nodes", [])}
            gone = {n[1] for n in sa_ - sb_}
            new = {n[1] for n in sb_ - sa_}
            ok = any(row["a"].get(x) in gone or row["b"].get(x) in new for x in row["fields"] if x in PLACE)
            cls = "place node" if ok else "UNEXPLAINED place"
            row["why"] = {"nodes_only_a": sorted(gone)[:5], "nodes_only_b": sorted(new)[:5]}
            classes[cls] += 1
            (unexplained if cls.startswith("UNEXPLAINED") else examples[cls]).append(row)
    res = {"preset": preset, "builds": [ba, bb], "points": len(A), "changed": len(changed), "classes": dict(classes),
           "polygons_only_a": len(only_a), "polygons_only_b": len(only_b),
           "responsible_top": responsible.most_common(25),
           "examples": {c: v[:5] for c, v in examples.items()}, "unexplained": unexplained}
    json.dump(res, open(outp, "w"), ensure_ascii=False, indent=1)
    print(json.dumps({k: res[k] for k in ("preset", "builds", "points", "changed", "classes", "polygons_only_a",
                                          "polygons_only_b")}, ensure_ascii=False))


main()
