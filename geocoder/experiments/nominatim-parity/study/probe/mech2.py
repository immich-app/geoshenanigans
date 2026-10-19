"""mech2.py <answers A> <answers B> <quarter|half> <set dir> <probe A> <probe B> <out.json>
Mechanism check for the changed answers of the two server steps mech_server.py leaves out:
  quarter  984e668: quarter (rank 22) and neighbourhood (24) get a slot each; before, the nearer
           of the two took one shared slot, after, the quarter is accepted first and its fuzzy
           area (half side 1000/sqrt2 m at this step) must hold the neighbourhood.
  half     7e8e9bb: an accepted node's fuzzy area grows from radius/sqrt2 to radius half side.
Per changed field of {suburb, hamlet, quarter, neighbourhood} (old value from A, new from B), with
the probe's place-node picks (places.result) and nearby nodes (lat/lng) of each server:
  quarter step
    split slot          the field shows up beside the other one A already had (shared slot gone)
    outside quarter     a neighbourhood dropped while B accepts a quarter whose area misses it
  half step
    wider area          an added node inside B's anchor area (half side r) but outside A's (r/sqrt2);
                        the anchor is a lower-rank node B's answer or picks name
    wider area, anchor hidden by an admin field
                        as above, the anchor the nearest suburb/hamlet/quarter node, which an
                        admin field of its rank hides in the answer
    cascade             a field whose anchor (a lower-rank pick) changed between A and B
    competitor          suburb/hamlet: the rank-20 slot went to the other type
  UNEXPLAINED           anything else
"""
import collections
import json
import math
import subprocess
import sys

R = {"city": 15000, "town": 4000, "village": 2000, "suburb": 2000, "hamlet": 1000, "quarter": 1000,
     "neighbourhood": 1000}
RANK = {"city": 16, "town": 16, "village": 16, "suburb": 20, "hamlet": 20, "quarter": 22, "neighbourhood": 24}
FIELDS = ("suburb", "hamlet", "quarter", "neighbourhood")


def load(path):
    out = []
    for line in open(path, encoding="utf-8"):
        a, b, j = line.split(" ", 2)
        out.append(((a, b), json.loads(j).get("address") or {}))
    return out


def probe(binary, setdir, pts):
    inp = "".join(f"{p[0]} {p[1]}\n" for p in pts)
    out = subprocess.run([binary, setdir], input=inp, capture_output=True, text=True, check=True).stdout
    return [json.loads(l) for l in out.splitlines()]


def node(j, t, name):
    m = [n for n in j["places"]["nearby_nodes"] if n["type"] == t and n["name"] == name]
    return min(m, key=lambda n: n["dist_m"]) if m else None


def inside(anchor, n, half):
    dn = (n["lat"] - anchor["lat"]) * 111320
    de = (n["lng"] - anchor["lng"]) * 111320 * math.cos(math.radians(anchor["lat"]))
    return max(abs(dn), abs(de)) <= half


def anchors(j, ans, x):
    """Candidate anchors of field x: the place nodes of lower rank that the answer or the probe's
    picks name (the probe's picks come from find_places without the admin rank-16 entry, the
    answer's fields may come from boundaries, so both are tried): [(type, node)]."""
    picks = j["places"]["result"]
    out = []
    for t in ("city", "town", "village", "suburb", "hamlet", "quarter"):
        if RANK[t] >= RANK[x]:
            continue
        for name in {ans.get(t), picks.get(t)} - {None}:
            n = node(j, t, name)
            if n:
                out.append((t, n))
    return out


def hidden_anchors(j, x):
    """The nearest suburb, hamlet and quarter node within 0.02 degrees (the server's radius
    before c99516b): an accepted node an admin field of the same rank hides in the answer."""
    out = []
    for t in ("suburb", "hamlet", "quarter"):
        if RANK[t] >= RANK[x]:
            continue
        m = [n for n in j["places"]["nearby_nodes"] if n["type"] == t and n["dist_m"] <= 2226]
        if m:
            out.append((t, min(m, key=lambda n: n["dist_m"])))
    return out


def main():
    ansa, ansb, step, setdir, pa_bin, pb_bin, outp = sys.argv[1:8]
    A, B = load(ansa), load(ansb)
    changed = [(p, a, b) for (p, a), (_, b) in zip(A, B) if a != b]
    pts = [p for p, _, _ in changed]
    ja_all, jb_all = probe(pa_bin, setdir, pts), probe(pb_bin, setdir, pts)
    reasons, bad = collections.Counter(), []
    for (p, a, b), ja, jb in zip(changed, ja_all, jb_all):
        fields = [x for x in FIELDS if a.get(x) != b.get(x)]
        picks_a, picks_b = ja["places"]["result"], jb["places"]["result"]
        for x in fields:
            kind = "added" if a.get(x) is None else "removed" if b.get(x) is None else "changed"
            why = None
            if step == "quarter":
                other = "neighbourhood" if x == "quarter" else "quarter"
                if kind == "added" and a.get(other) is not None and x in ("quarter", "neighbourhood"):
                    why = "split slot"
                elif kind == "removed" and x == "neighbourhood":
                    # the quarter node accepted: the answer's (when a node) or the probe's pick (an
                    # answer quarter may come from a boundary, or be hidden by a dense admin chain)
                    n = node(ja, "neighbourhood", a["neighbourhood"])
                    qs = [node(jb, "quarter", q) for q in {b.get("quarter"), picks_b.get("quarter")} - {None}]
                    if n and any(q and not inside(q, n, 1000 / math.sqrt(2)) for q in qs):
                        why = "outside quarter"
                elif kind == "added" and x == "quarter" and b.get("neighbourhood") is None and \
                        a.get("neighbourhood") is None and picks_a.get("neighbourhood") is not None:
                    why = "split slot"  # A's neighbourhood pick hidden by an admin field
            else:
                if x in ("suburb", "hamlet") and kind == "added":
                    sib = "hamlet" if x == "suburb" else "suburb"
                    if a.get(sib) is not None and b.get(sib) is None:
                        why = "competitor"
                if why is None and kind in ("added", "changed"):
                    n = node(jb, x, b[x])
                    wider = lambda cands: n and any(inside(an, n, R[t]) and not inside(an, n, R[t] / math.sqrt(2))
                                                    for t, an in cands)
                    if wider(anchors(jb, b, x)):
                        why = "wider area"
                    elif wider(hidden_anchors(jb, x)):
                        why = "wider area, anchor hidden by an admin field"
                if why is None and x in ("suburb", "hamlet") and kind == "removed":
                    sib = "hamlet" if x == "suburb" else "suburb"
                    if b.get(sib) is not None and a.get(sib) is None:
                        why = "competitor"
                if why is None:
                    lower = [t for t in RANK if RANK[t] < RANK[x]]
                    if any(picks_a.get(t) != picks_b.get(t) or a.get(t) != b.get(t) for t in lower):
                        why = "cascade"
            if why is None:
                why = "UNEXPLAINED"
                bad.append({"pt": p, "field": x, "a": a.get(x), "b": b.get(x), "picks_a": picks_a, "picks_b": picks_b})
            reasons[f"{x} {kind}: {why}"] += 1
    res = {"step": step, "changed": len(changed), "reasons": dict(reasons.most_common()),
           "unexplained": len(bad), "unexplained_rows": bad[:200]}
    json.dump(res, open(outp, "w"), ensure_ascii=False, indent=1)
    print(json.dumps({"step": step, "changed": len(changed), "unexplained fields": len(bad)}))
    for k, v in reasons.most_common(16):
        print("  ", v, k)


main()
