"""eq_cmp.py <reader A> <reader B> [sets...]: lines that differ between two readers' answers
(eq_run.sh outputs), per set, tool (q = query_json, a = find_admin) and point pool; the first
differing lines of each set are printed below the table."""
import sys

R = "/ssd/reader/out/"
POOLS = [("general30k", 30000), ("parallel10k", 10000), ("border10k", 10000)]
SETS = ["o-minimal", "o-recommended", "o-max", "b-smallest", "b-recommended", "b-max", "u-smallest", "u-recommended", "r1-smallest", "r1-recommended"]
a, b = sys.argv[1], sys.argv[2]
sets = sys.argv[3:] or SETS
pool_of = [name for name, n in POOLS for _ in range(n)]
print(f"| set | tool | points | differ ({a} vs {b}) | " + " | ".join(n for n, _ in POOLS) + " |")
print("|---|---|---:|---:|" + "---:|" * len(POOLS))
examples = []
for s in sets:
    for t in ("q", "a"):
        la = open(f"{R}{s}.{a}.{t}").read().splitlines()
        lb = open(f"{R}{s}.{b}.{t}").read().splitlines()
        assert len(la) == len(lb) == len(pool_of), (s, t, len(la), len(lb))
        diff = [i for i in range(len(la)) if la[i] != lb[i]]
        per = {n: 0 for n, _ in POOLS}
        for i in diff:
            per[pool_of[i]] += 1
        print(f"| {s} | {'query_json' if t == 'q' else 'find_admin'} | {len(la)} | {len(diff)} | " + " | ".join(str(per[n]) for n, _ in POOLS) + " |")
        examples += [(s, t, i, la[i][:300], lb[i][:300]) for i in diff[:3]]
for s, t, i, x, y in examples:
    print(f"\n{s} {t} line {i + 1}\n  {a}: {x}\n  {b}: {y}")
