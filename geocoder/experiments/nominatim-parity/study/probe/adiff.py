"""adiff.py <answers A> <answers B> [examples] : changed answers between two grid_query outputs
of the same points: total, by set of changed address fields, by (field, kind) where kind is
added / removed / changed, and up to <examples> example rows per field set (JSON lines)."""
import collections
import json
import sys


def load(path):
    out = []
    for line in open(path, encoding="utf-8"):
        a, b, j = line.split(" ", 2)
        out.append(((a, b), json.loads(j).get("address") or {}))
    return out


A, B = load(sys.argv[1]), load(sys.argv[2])
nex = int(sys.argv[3]) if len(sys.argv) > 3 else 3
assert len(A) == len(B)
by_set = collections.Counter()
by_field = collections.Counter()
ex = collections.defaultdict(list)
changed = 0
for (pa, a), (pb, b) in zip(A, B):
    assert pa == pb
    if a == b:
        continue
    changed += 1
    f = tuple(sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k)))
    by_set[f] += 1
    for k in f:
        kind = "added" if k not in a else "removed" if k not in b else "changed"
        by_field[(k, kind)] += 1
    if len(ex[f]) < nex:
        ex[f].append({"pt": pa, "a": {k: a.get(k) for k in f}, "b": {k: b.get(k) for k in f}})
print(json.dumps({"points": len(A), "changed": changed}))
for f, n in by_set.most_common():
    print(n, ",".join(f))
print("by field:")
for (k, kind), n in sorted(by_field.items(), key=lambda x: -x[1]):
    print("  ", n, k, kind)
print("examples:")
for f, rows in ex.items():
    for r in rows:
        print(json.dumps(r, ensure_ascii=False))
