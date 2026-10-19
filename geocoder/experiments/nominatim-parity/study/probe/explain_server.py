"""Class the changed answers of one server commit on the same data by the fields it may touch.

usage: python -I explain_server.py <answers A> <answers B> <step> <out.json>
steps and the address fields each may change (by construction of the commit):
  quarter   984e668  quarter, neighbourhood (rank 22/24 slots, cascade between them)
  half      7e8e9bb  suburb, hamlet, quarter, neighbourhood (fuzzy area of an accepted node)
  subcity   c99516b  suburb, hamlet, quarter, neighbourhood (candidacy by own fuzzy area)
  rank16    9171a8e  city, town, village + the place fields their fuzzy area holds
  coarse    509845b  state, region, province (boundaries at the street point)
A changed answer touching any other field is UNEXPLAINED. Also counts added / removed / changed
per field.
"""
import collections
import json
import sys

ALLOWED = {
    "quarter": {"quarter", "neighbourhood"},
    "half": {"suburb", "hamlet", "quarter", "neighbourhood"},
    "subcity": {"suburb", "hamlet", "quarter", "neighbourhood"},
    "rank16": {"city", "town", "village", "suburb", "hamlet", "quarter", "neighbourhood"},
    "coarse": {"state", "region", "province"},
}


def load(path):
    out = []
    for line in open(path, encoding="utf-8"):
        a, b, j = line.split(" ", 2)
        out.append(((a, b), json.loads(j).get("address") or {}))
    return out


A, B = load(sys.argv[1]), load(sys.argv[2])
step, outp = sys.argv[3], sys.argv[4]
allowed = ALLOWED[step]
classes, kinds, bad = collections.Counter(), collections.Counter(), []
for (p, a), (_, b) in zip(A, B):
    if a == b:
        continue
    f = sorted(x for x in set(a) | set(b) if a.get(x) != b.get(x))
    for x in f:
        kinds[f"{x} {'added' if x not in a else 'removed' if x not in b else 'changed'}"] += 1
    if set(f) <= allowed:
        classes[f"{step}: " + ",".join(f)] += 1
    else:
        classes["UNEXPLAINED"] += 1
        bad.append({"pt": p, "fields": f, "a": {x: a.get(x) for x in f}, "b": {x: b.get(x) for x in f}})
res = {"step": step, "points": len(A), "changed": sum(classes.values()), "unexplained": classes["UNEXPLAINED"],
       "classes": dict(classes.most_common()), "kinds": dict(kinds.most_common()), "unexplained_rows": bad}
json.dump(res, open(outp, "w"), ensure_ascii=False, indent=1)
print(json.dumps({k: res[k] for k in ("step", "points", "changed", "unexplained")}), dict(kinds.most_common(8)))
