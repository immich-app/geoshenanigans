"""Check the mechanism behind each changed answer of the subcity (c99516b) and rank16 (9171a8e)
server commits, from the place nodes near the point.

usage: python -I mech_server.py <answers A> <answers B> <subcity|rank16> <set dir> <out.json> <probe A> <probe B>
(probe X: the parity_probe binary built with server X, whose place-node picks show anchors an
admin field hides in the answer)
Per changed place field (old value from A, new from B), with the nearest node of that type and name
(parity_probe nearby_nodes):
  outside own area   a removed node whose place_node_fuzzy_area (half side r by type) misses the
                     point: max(|north|, |east|) > r
  weighed            rank16 only: a removed city/town/village while B has another rank-16 node
                     whose distance x weight (city 0.25, town 0.5, village 1) is smaller
  cascade            a field whose node is inside its area, in an answer where a node of a lower
                     rank was removed, added or changed (the fuzzy area it sits in moved)
  competitor         an added field whose rank shares its slot with a removed field (suburb/hamlet)
  own area corner    an added node inside its fuzzy area (a square) but beyond the old circular radius
  UNEXPLAINED        anything else
"""
import collections
import json
import math
import subprocess
import sys

R = {"city": 15000, "town": 4000, "village": 2000, "suburb": 2000, "hamlet": 1000, "quarter": 1000, "neighbourhood": 500}
# the radii the server used before c99516b / 9171a8e, in metres (0.16 / 0.08 / 0.04 / 0.02 degrees)
OLD = {"city": 17811, "town": 8906, "village": 4453, "suburb": 2226, "hamlet": 2226, "quarter": 2226, "neighbourhood": 2226}
RANK = {"city": 16, "town": 16, "village": 16, "suburb": 20, "hamlet": 20, "quarter": 22, "neighbourhood": 24}
W = {"city": 0.25, "town": 0.5, "village": 1.0}


def load(path):
    out = []
    for line in open(path, encoding="utf-8"):
        a, b, j = line.split(" ", 2)
        out.append(((a, b), json.loads(j).get("address") or {}))
    return out


A, B = load(sys.argv[1]), load(sys.argv[2])
step, setdir, outp, probe_a, probe_b = sys.argv[3:8]
changed = [(p, a, b) for (p, a), (_, b) in zip(A, B) if a != b]
inp = "".join(f"{p[0]} {p[1]}\n" for p, _, _ in changed)
probe = subprocess.run([probe_b, setdir], input=inp, capture_output=True, text=True, check=True).stdout.splitlines()
probe0 = subprocess.run([probe_a, setdir], input=inp, capture_output=True, text=True, check=True).stdout.splitlines()
reasons, bad = collections.Counter(), []
for (p, a, b), line, line0 in zip(changed, probe, probe0):
    j = json.loads(line)
    picks_b, picks_a = j["places"]["result"], json.loads(line0)["places"]["result"]
    lat, lng = float(p[0]), float(p[1])
    nodes = j["places"]["nearby_nodes"]

    def node(t, name):
        m = [n for n in nodes if n["type"] == t and n["name"] == name]
        return min(m, key=lambda n: n["dist_m"]) if m else None

    def outside(t, n):
        dn = (n["lat"] - lat) * 111320
        de = (n["lng"] - lng) * 111320 * math.cos(math.radians(n["lat"]))
        return max(abs(dn), abs(de)) > R[t]

    fields = [x for x in R if a.get(x) != b.get(x)]
    ranks_moved = {RANK[x] for x in fields} | {RANK[x] for x in R if picks_a.get(x) != picks_b.get(x)}
    for x in fields:
        why = None
        if a.get(x) is not None:  # removed or replaced
            n = node(x, a[x])
            if n and outside(x, n):
                why = "outside own area"
            elif step == "rank16" and RANK[x] == 16:
                others = [(y, node(y, b[y])) for y in ("city", "town", "village") if b.get(y) and y != x]
                if n and any(m and m["dist_m"] * W[y] < n["dist_m"] * W[x] for y, m in others):
                    why = "weighed"
        if why is None and b.get(x) is not None:
            n = node(x, b[x])
            if n and not outside(x, n) and RANK[x] == 20 and any(RANK[y] == 20 and y != x for y in fields):
                why = "competitor"
        if why is None and b.get(x) is not None:
            n = node(x, b[x])
            if n and not outside(x, n) and n["dist_m"] > OLD[x]:
                why = "own area corner beyond the old radius"
        if why is None and any(r < RANK[x] for r in ranks_moved):
            why = "cascade"
        if why is None and step == "rank16" and RANK[x] == 16 and b.get(x) is not None:
            others = [(y, node(y, a[y])) for y in ("city", "town", "village") if a.get(y) and y != x]
            n = node(x, b[x])
            if n and any(m and (outside(y, m) or m["dist_m"] * W[y] > n["dist_m"] * W[x]) for y, m in others):
                why = "weighed"
        if why is None:
            why = "UNEXPLAINED"
            bad.append({"pt": p, "field": x, "a": a.get(x), "b": b.get(x)})
        reasons[f"{x} {'added' if a.get(x) is None else 'removed' if b.get(x) is None else 'changed'}: {why}"] += 1
res = {"step": step, "changed": len(changed), "reasons": dict(reasons.most_common()),
       "unexplained": len(bad), "unexplained_rows": bad[:200]}
json.dump(res, open(outp, "w"), ensure_ascii=False, indent=1)
print(json.dumps({"step": step, "changed": len(changed), "unexplained fields": len(bad)}))
for k, v in reasons.most_common(14):
    print("  ", v, k)
