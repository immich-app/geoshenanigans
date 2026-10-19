"""mech_s7.py <answers A> <answers B> <set dir> <parity_probe B> <admin_probe> <out.json>
Mechanism check for 64183ef (rank-16 place nodes gated by the current boundary of rank 16 instead
of the smallest admin_level 7-8 polygon holding the query). Per changed answer, from the probes:
  gate_new  the current boundary of rank 16 at the query (parity_probe B, places.current_boundary)
  gate_old  the smallest admin_level 7/8 polygon holding the query (admin_probe pip)
Per changed field:
  city/town/village removed   its node is outside gate_new          -> "outside the rank-16 boundary"
  city/town/village added     its node is outside gate_old          -> "was outside the L7-8 gate"
                              or another rank-16 field went         -> "competitor gated out"
                              or there was no rank-16 field before  -> "slot freed" (A's node gated)
  any other place field       a rank-16 field changed               -> "cascade"
  else                                                               -> UNEXPLAINED
Polygons are matched by admin_probe id; gate_new by name among the query's pip polygons.
"""
import collections
import json
import subprocess
import sys

RANK16 = ("city", "town", "village")
PLACE = ("city", "town", "village", "suburb", "hamlet", "quarter", "neighbourhood")


def load(path):
    out = []
    for line in open(path, encoding="utf-8"):
        a, b, j = line.split(" ", 2)
        out.append(((a, b), json.loads(j).get("address") or {}))
    return out


def run(binary, d, pts):
    if not pts:
        return []
    inp = "".join(f"{a} {b}\n" for a, b in pts)
    out = subprocess.run([binary, d], input=inp, capture_output=True, text=True, check=True).stdout
    return [json.loads(l) for l in out.splitlines()]


def main():
    ansa, ansb, setdir, pprobe, aprobe, outp = sys.argv[1:7]
    A, B = load(ansa), load(ansb)
    changed = [(p, a, b) for (p, a), (_, b) in zip(A, B) if a != b]
    pts = [p for p, _, _ in changed]
    pj = run(pprobe, setdir, pts)
    aj = run(aprobe, setdir, pts)
    # nodes whose containment we need
    need = {}
    rows = []
    for (p, a, b), j, adm in zip(changed, pj, aj):
        nodes = j["places"]["nearby_nodes"]

        def node(t, name):
            m = [n for n in nodes if n["type"] == t and n["name"] == name]
            return min(m, key=lambda n: n["dist_m"]) if m else None

        pip = adm["pip"]
        l78 = [q for q in pip if q.get("level") in (7, 8)]
        gate_old = min(l78, key=lambda q: q["area"])["id"] if l78 else None
        gname = j["places"]["current_boundary"].get("16")
        named = [q for q in pip if q.get("name") == gname]
        gate_new = min(named, key=lambda q: q["area"])["id"] if named else None
        fields = [x for x in PLACE if a.get(x) != b.get(x)]
        info = []
        for x in fields:
            v_old, v_new = a.get(x), b.get(x)
            n_old = node(x, v_old) if v_old and x in RANK16 else None
            n_new = node(x, v_new) if v_new and x in RANK16 else None
            for n in (n_old, n_new):
                if n:
                    need[(round(n["lat"], 6), round(n["lng"], 6))] = None
            info.append((x, v_old, v_new, n_old, n_new))
        rows.append((p, a, b, gate_old, gate_new, gname, info))
    keys = list(need)
    for k, res in zip(keys, run(aprobe, setdir, [(f"{la:.6f}", f"{ln:.6f}") for la, ln in keys])):
        need[k] = {q["id"] for q in res["pip"]}

    def holds(gate, n):
        return gate is None or gate in need[(round(n["lat"], 6), round(n["lng"], 6))]

    reasons, bad = collections.Counter(), []
    for p, a, b, gate_old, gate_new, gname, info in rows:
        r16_changed = any(x in RANK16 for x, *_ in info)
        r16_removed = any(x in RANK16 and vo and not vn for x, vo, vn, _, _ in info)
        had_r16 = any(a.get(x) for x in RANK16)
        for x, vo, vn, no, nn in info:
            kind = "added" if vo is None else "removed" if vn is None else "changed"
            why = None
            if x in RANK16:
                if vo and no and not holds(gate_new, no):
                    why = "outside the rank-16 boundary"
                elif vn and nn and not holds(gate_old, nn):
                    why = "was outside the L7-8 gate"
                elif vn and r16_removed:
                    why = "competitor gated out"
                elif vn and not had_r16:
                    why = "slot freed"
                elif vo and not vn and any(b.get(y) for y in RANK16 if y != x):
                    why = "competitor gated in"
            elif r16_changed:
                why = "cascade"
            if why is None:
                why = "UNEXPLAINED"
                bad.append({"pt": p, "field": x, "a": vo, "b": vn, "gate_old": gate_old, "gate_new": gname})
            reasons[f"{x} {kind}: {why}"] += 1
    res = {"changed": len(changed), "reasons": dict(reasons.most_common()), "unexplained": len(bad),
           "unexplained_rows": bad[:200]}
    json.dump(res, open(outp, "w"), ensure_ascii=False, indent=1)
    print(json.dumps({"step": "rank16 gate", "changed": len(changed), "unexplained fields": len(bad)}))
    for k, v in reasons.most_common(20):
        print("  ", v, k)


main()
