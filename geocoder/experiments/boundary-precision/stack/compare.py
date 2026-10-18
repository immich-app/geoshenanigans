"""Answer changes of a set vs the baseline, per pool, from q/<name>.<pool>.jsonl (runq.sh).
Immich fields (prototype mapper): city = city ?? town ?? village ?? hamlet ?? municipality,
state = state ?? province, country = country_code.
usage: compare.py <base> <other> [examples per pool]"""
import json, sys

POOLS = ["places", "random", "L2", "L4", "L8", "all", "p200k", "ablate3"]
base, other = sys.argv[1], sys.argv[2]
NEX = int(sys.argv[3]) if len(sys.argv) > 3 else 2


def coalesce(a, *keys):
    return next((a[k] for k in keys if a.get(k) is not None), None)


def immich(js):
    a = json.loads(js).get("address", {})
    return (coalesce(a, "city", "town", "village", "hamlet", "municipality"), coalesce(a, "state", "province"),
            a.get("country_code"))


def rows(name, pool):
    with open(f"/ssd/shrink/stack/q/{name}.{pool}.jsonl") as f:
        return [l.rstrip("\n").split(" ", 2) for l in f]


print(f"| pool | points | full JSON changed | Immich fields changed | city | state | country |")
print(f"|---|---:|---:|---:|---:|---:|---:|")
examples = []
tot = [0, 0, 0]
for p in POOLS:
    b, o = rows(base, p), rows(other, p)
    assert len(b) == len(o) and all(x[:2] == y[:2] for x, y in zip(b, o)), p
    n = len(b)
    full = [i for i in range(n) if b[i][2] != o[i][2]]
    bi = {i: immich(b[i][2]) for i in full}
    oi = {i: immich(o[i][2]) for i in full}
    imm = [i for i in full if bi[i] != oi[i]]
    per = [sum(bi[i][k] != oi[i][k] for i in imm) for k in range(3)]
    tot[0] += n; tot[1] += len(full); tot[2] += len(imm)
    print(f"| {p} | {n:,} | {len(full):,} ({len(full) / n:.3%}) | {len(imm):,} ({len(imm) / n:.3%}) | {per[0]:,} | {per[1]:,} | {per[2]:,} |")
    for i in imm[:NEX]:
        examples.append(f"{p} {b[i][0]} {b[i][1]}: {bi[i]} -> {oi[i]}")
    for i in [i for i in full if i not in set(imm)][:1]:
        bj, oj = json.loads(b[i][2]), json.loads(o[i][2])
        diff = {k: (bj.get("address", {}).get(k), oj.get("address", {}).get(k))
                for k in set(bj.get("address", {})) | set(oj.get("address", {}))
                if bj.get("address", {}).get(k) != oj.get("address", {}).get(k)}
        if not diff:
            diff = {k: (bj.get(k), oj.get(k)) for k in set(bj) | set(oj) if bj.get(k) != oj.get(k)}
        examples.append(f"{p} {b[i][0]} {b[i][1]} (JSON only): {diff}")
print(f"| **all pools** | {tot[0]:,} | {tot[1]:,} ({tot[1] / tot[0]:.3%}) | {tot[2]:,} ({tot[2] / tot[0]:.3%}) | | | |")
print("\nexamples:")
for e in examples:
    print(" ", e)
