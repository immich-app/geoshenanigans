"""pq_an.py : verify6 attribution of the parity changes on par30k (pq.sh answers).
For each preset: answers changed base (B) -> tip (T), then per step of the walk B -> D -> S1 .. S5 -> T
the changed answers, the set of address fields each change touches (signature) and the changes that
touch a field outside the step's scope. Writes /ssd/verify6/cmp/steps_<preset>.json (indices per step)
and prints markdown tables."""
import json
import random
from collections import Counter

V = "/ssd/verify6/"
STEPS = ["B", "D", "S1", "S2", "S3", "S4", "S5", "T"]
NAME = {"D": "builder fff988c+253eb9a+0094397+3726c0e+f7096b0", "S1": "984e668", "S2": "7e8e9bb", "S3": "c99516b",
        "S4": "9171a8e", "S5": "509845b", "T": "64183ef"}
NODE = {"city", "town", "village", "suburb", "hamlet", "quarter", "neighbourhood"}
SCOPE = {"D": None, "S1": {"quarter", "neighbourhood"}, "S2": NODE, "S3": NODE, "S4": NODE,
         "S5": {"state", "region", "province"}, "T": NODE}
PHOTO = 22500


def load(label, preset, pool="par30k"):
    out = []
    for line in open(f"{V}q/{label}_{preset}/{pool}.txt", encoding="utf-8"):
        parts = line.rstrip("\n").split(" ", 2)
        out.append(json.loads(parts[2]) if len(parts) == 3 else {})
    return out


def addr(a):
    return {k: v for k, v in (a.get("address") or {}).items()}


def sig(x, y):
    return frozenset(k for k in set(x) | set(y) if x.get(k) != y.get(k))


for preset in ("smallest", "recommended", "max"):
    A = {l: load(l, preset) for l in STEPS}
    n = len(A["B"])
    ad = {l: [addr(a) for a in A[l]] for l in STEPS}
    whole = sum(json.dumps(A["B"][i], sort_keys=True) != json.dumps(A["T"][i], sort_keys=True) for i in range(n))
    chg = [i for i in range(n) if ad["B"][i] != ad["T"][i]]
    print(f"\n## {preset}: {n} points; address changed B->T {len(chg)} ({len(chg) / n:.1%}; photo part "
          f"{sum(i < PHOTO for i in chg) / PHOTO:.1%}, land part {sum(i >= PHOTO for i in chg) / (n - PHOTO):.1%}); whole answer changed {whole}")
    cnt = Counter()
    for i in chg:
        for k in sig(ad["B"][i], ad["T"][i]):
            cnt[k] += 1
    print("fields changed B->T:", ", ".join(f"{k} {v}" for k, v in cnt.most_common()))
    res = {"changed_BT": chg}
    print("| step | commit | changed | outside scope | top signatures |")
    print("|---|---|---:|---:|---|")
    for a, b in zip(STEPS, STEPS[1:]):
        idx = [i for i in range(n) if ad[a][i] != ad[b][i]]
        sigs = Counter(sig(ad[a][i], ad[b][i]) for i in idx)
        out = [i for i in idx if SCOPE[b] is not None and not sig(ad[a][i], ad[b][i]) <= SCOPE[b]]
        res[b] = idx
        res[b + "_out"] = out
        top = "; ".join(f"{'+'.join(sorted(s))} {c}" for s, c in sigs.most_common(6))
        print(f"| {a}->{b} | {NAME[b]} | {len(idx)} | {len(out) if SCOPE[b] is not None else '-'} | {top} |")
        for i in out[:5]:
            print(f"    OUT {b} line {i + 1}: {json.dumps(ad[a][i], ensure_ascii=False)} -> {json.dumps(ad[b][i], ensure_ascii=False)}")
    # net: changes that a later step reverted
    sets = [set(res[s]) for s in STEPS[1:]]
    per_point_steps = Counter(sum(i in st for st in sets) for i in chg)
    print("B->T changed points by number of steps that touched them:", dict(sorted(per_point_steps.items())))
    json.dump(res, open(f"{V}cmp/steps_{preset}.json", "w"))
