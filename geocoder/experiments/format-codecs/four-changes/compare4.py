"""Answer changes of a set vs a baseline, per pool, from q/<name>.<pool>.jsonl (runq4.sh).
Full JSON = the whole grid_query output line. Immich fields (prototype mapper): city = city ?? town ??
village ?? hamlet ?? municipality, state = state ?? province, country = country_code.
Also counts road / house_number / postcode diffs (street/address answers).
usage: compare4.py <base> <other> [examples]"""
import json, os, sys

base, other = sys.argv[1], sys.argv[2]
NEX = int(sys.argv[3]) if len(sys.argv) > 3 else 2
POOLS = ["addr", "street", "interp", "poi", "p22k", "ablate3", "random", "places", "L2", "L4", "L8", "all", "p200k"]
Q = "/ssd/shrink/max/four-changes/q"


def coalesce(a, *keys):
    return next((a[k] for k in keys if a.get(k) is not None), None)


def fields(js):
    a = json.loads(js).get("address", {})
    return (coalesce(a, "city", "town", "village", "hamlet", "municipality"), coalesce(a, "state", "province"),
            a.get("country_code"), a.get("road"), a.get("house_number"), a.get("postcode"))


def rows(name, pool):
    with open(f"{Q}/{name}.{pool}.jsonl") as f:
        return [l.rstrip("\n").split(" ", 2) for l in f]


print(f"### {other} vs {base}")
print("| pool | points | full JSON changed | Immich city/state/country changed | city | state | country | road | house_number | postcode |")
print("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
tot = [0, 0, 0]
ex = []
for p in POOLS:
    if not (os.path.exists(f"{Q}/{base}.{p}.jsonl") and os.path.exists(f"{Q}/{other}.{p}.jsonl")):
        continue
    b, o = rows(base, p), rows(other, p)
    assert len(b) == len(o) and all(x[:2] == y[:2] for x, y in zip(b, o)), f"{p}: line mismatch {len(b)} vs {len(o)}"
    n = len(b)
    full = [i for i in range(n) if b[i][2] != o[i][2]]
    fb = {i: fields(b[i][2]) for i in full}
    fo = {i: fields(o[i][2]) for i in full}
    per = [sum(fb[i][k] != fo[i][k] for i in full) for k in range(6)]
    imm = [i for i in full if fb[i][:3] != fo[i][:3]]
    tot[0] += n; tot[1] += len(full); tot[2] += len(imm)
    print(f"| {p} | {n:,} | {len(full):,} ({len(full) / n:.3%}) | {len(imm):,} ({len(imm) / n:.3%}) | " + " | ".join(f"{x:,}" for x in per) + " |")
    for i in full[:NEX]:
        bj, oj = json.loads(b[i][2]), json.loads(o[i][2])
        ba, oa = bj.get("address", {}), oj.get("address", {})
        d = {k: (ba.get(k), oa.get(k)) for k in set(ba) | set(oa) if ba.get(k) != oa.get(k)}
        if not d:
            d = {k: (bj.get(k), oj.get(k)) for k in set(bj) | set(oj) if bj.get(k) != oj.get(k)}
        ex.append(f"{p} {b[i][0]} {b[i][1]}: {d}")
print(f"| **all pools** | {tot[0]:,} | {tot[1]:,} ({tot[1] / max(tot[0], 1):.3%}) | {tot[2]:,} ({tot[2] / max(tot[0], 1):.3%}) | | | | | | |")
if ex:
    print("\nexamples:")
    for e in ex:
        print(" ", e)
