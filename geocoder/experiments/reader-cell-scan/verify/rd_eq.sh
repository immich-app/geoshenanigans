#!/bin/bash
# rd_eq.sh : reader answers on verify6's fresh pool rd30k (6k photo, 3k land, 3k sphere, 9k beside
# parallel-following borders, 9k within +-200 m of L2/L4/L8 borders) for the planet-261018b q2.5
# sets of the main builder (/ssd/bugs2/sets/base/{smallest,recommended,max}), readers main f48ddc4,
# 66a5ad0 and the perf/reader-cell-scan tip 9b16065 (verify6's own builds), tools grid_query
# (query_json, q) and admin_dump (find_admin, a). Out /ssd/verify6/rd/out/<set>.<rev>.<q|a>.
# Low priority, cores 48-63 (not timing).
set -u
V=/ssd/verify6
O=$V/rd/out
mkdir -p $O
for s in smallest recommended max; do
  for r in f48ddc4 9b16065 66a5ad0; do
    for t in q a; do echo "$s $r $t"; done
  done
done | xargs -P 12 -L 1 bash -c '
  s=$0 r=$1 t=$2
  exe=/ssd/verify6/bin/grid_query.$r; [ "$t" = a ] && exe=/ssd/verify6/bin/admin_dump.$r
  nice -n 15 taskset -c 48-63 $exe /ssd/bugs2/sets/base/$s < /ssd/verify6/pts/rd30k.txt > /ssd/verify6/rd/out/$s.$r.$t 2> /ssd/verify6/rd/out/$s.$r.$t.err
  echo "$s $r $t rc=$? lines=$(wc -l < /ssd/verify6/rd/out/$s.$r.$t)"'
/ssd/shrink/venv/bin/python -I - << 'EOF'
POOLS = [("photo", 6000), ("land", 3000), ("sphere", 3000), ("parallel", 9000), ("border", 9000)]
O = "/ssd/verify6/rd/out/"
pool = [n for n, k in POOLS for _ in range(k)]
print("| set | tool | reader vs main f48ddc4 | lines | differ | " + " | ".join(n for n, _ in POOLS) + " |")
print("|---|---|---|---:|---:|" + "---:|" * len(POOLS))
for s in ("smallest", "recommended", "max"):
    for t in ("q", "a"):
        m = open(f"{O}{s}.f48ddc4.{t}").read().splitlines()
        for r in ("9b16065", "66a5ad0"):
            x = open(f"{O}{s}.{r}.{t}").read().splitlines()
            assert len(m) == len(x) == 30000, (s, t, r, len(m), len(x))
            d = [i for i in range(30000) if m[i] != x[i]]
            per = {n: 0 for n, _ in POOLS}
            for i in d:
                per[pool[i]] += 1
            print(f"| {s} | {'query_json' if t == 'q' else 'find_admin'} | {r} | 30000 | {len(d)} | " + " | ".join(str(per[n]) for n, _ in POOLS) + " |")
EOF
echo RD_EQ_DONE
