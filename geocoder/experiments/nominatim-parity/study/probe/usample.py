"""usample.py <answers A> <answers B> <prefix> <n> <seed> : a uniform random sample of the points whose
answers differ (address dict), "name lat lng" lines (name = prefix_k_cc)."""
import json
import random
import sys

pa, pb, prefix, n, seed = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
changed = []
with open(pa, encoding="utf-8") as fa, open(pb, encoding="utf-8") as fb:
    for la, lb in zip(fa, fb):
        x, y, ja = la.split(" ", 2)
        _, _, jb = lb.split(" ", 2)
        a, b = json.loads(ja).get("address") or {}, json.loads(jb).get("address") or {}
        if a != b:
            changed.append((x, y, (a.get("country_code") or b.get("country_code") or "xx").lower()))
for k, (x, y, cc) in enumerate(random.Random(seed).sample(changed, min(n, len(changed)))):
    print(f"{prefix}_{k}_{cc} {x} {y}")
