"""sample.py <answers A> <answers B> <prefix> <n per class> [seed] : a sample of changed points,
classed by the set of changed fields, spread over countries (round robin by country code, random
order within), n per class -> "name lat lng" lines on stdout (name = prefix_class_k)."""
import collections
import json
import random
import sys


def load(path):
    out = []
    for line in open(path, encoding="utf-8"):
        a, b, j = line.split(" ", 2)
        out.append(((a, b), json.loads(j).get("address") or {}))
    return out


A, B = load(sys.argv[1]), load(sys.argv[2])
prefix, n = sys.argv[3], int(sys.argv[4])
rng = random.Random(int(sys.argv[5]) if len(sys.argv) > 5 else 1)
classes = collections.defaultdict(lambda: collections.defaultdict(list))
for (p, a), (_, b) in zip(A, B):
    if a == b:
        continue
    f = "+".join(sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k)))
    classes[f][(a.get("country_code") or b.get("country_code") or "??").lower()].append(p)
for f, by_cc in sorted(classes.items(), key=lambda kv: -sum(len(v) for v in kv[1].values())):
    pools = {cc: rng.sample(v, len(v)) for cc, v in by_cc.items()}
    order = rng.sample(sorted(pools), len(pools))
    picked = []
    while len(picked) < n and any(pools.values()):
        for cc in order:
            if pools[cc] and len(picked) < n:
                picked.append((cc, pools[cc].pop()))
    for k, (cc, (lat, lng)) in enumerate(picked):
        print(f"{prefix}_{f.replace(',', '+')}_{k}_{cc} {lat} {lng}")
