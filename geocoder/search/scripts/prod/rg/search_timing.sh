#!/usr/bin/env bash
# Place-search latency with the branch head reader (66a5ad0, query-cell-only find_admin) vs the
# 8dbb58d reader (place-search.pip2), same protocol as scripts/timing3.sh (memwatch, dropped caches
# before each run, core 0, nice -20), under TIMING.lock after rg_timing.sh.
set -uo pipefail
R=/ssd/search-prod
G=$R/rg
W="/ssd/shrink/venv/bin/python $R/scripts/memwatch.py"
O=$G/timing-search
L=/ssd/shrink/sortpatch/TIMING.lock
N="nice -n -20"
until [ -f $G/timing/DONE ]; do sleep 30; done
until (set -o noclobber; echo "search-prod search_timing $(date -Is)" > $L) 2> /dev/null; do sleep 30; done
trap 'grep -q search-prod $L && rm -f $L' EXIT
mkdir -p $O
cd $R
drop() { sync; echo 3 > /proc/sys/vm/drop_caches; sleep 2; }
query() { # tag binary set idx cgroup-limit warm|cold queries
  local tag=$1 bin=$2 set=$3 idx=$4 lim=$5 mode=$6 q=$7
  local wrap=()
  [ "$lim" != "none" ] && wrap=(systemd-run --scope -q -p MemoryMax=$lim -p MemorySwapMax=0)
  local args=(--iso extras/$set/iso_codes.tsv --bench 1 --lat-out $O/q-$tag.lat)
  [ "$mode" = cold ] && args+=(--no-warmup)
  drop
  $W $O/q-$tag.mem.json -- "${wrap[@]}" $N taskset -c 0 $bin query sets/$set idx/$idx "${args[@]}" < $q > $O/q-$tag.json 2> $O/q-$tag.log
}
for b in head:$R/target/release/place-search pip2:$G/bin/place-search.pip2; do
  IFS=: read -r v bin <<< "$b"
  query min-warm.$v $bin minimal min-1c none warm tmp/q10k.txt
  query rec-warm.$v $bin recommended rec-1c none warm tmp/q10k.txt
  query rec-cold.$v $bin recommended rec-1c none cold tmp/q10k.txt
  query max-warm.$v $bin max max-1c none warm tmp/q10k.txt
  query rec-cg32M-cold.$v $bin recommended rec-1c 32M cold tmp/q1k.txt
  query min-cg32M-cold.$v $bin minimal min-1c 32M cold tmp/q1k.txt
done
echo done > $O/DONE
