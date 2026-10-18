#!/usr/bin/env bash
# Reverse-geocode latency, main reader (f48ddc4) vs 8dbb58d (pip2: the answer-preserving reader
# commits) vs branch head (66a5ad0, adds the query-cell-only scan), under
# /ssd/shrink/sortpatch/TIMING.lock: rg_bench (one warm pass, 3 timed passes, fastest per point),
# pinned to core 2 at nice -20, readers interleaved per (set, pool, mode). Pools: the first 50k
# of tiers2 p200k (photo-like) and tiers2 L2 (20k, +-200 m of country borders). Modes: admin
# (find_admin alone) and query (query_json, the server's answer).
# Waits for eq_run.sh (out/DONE) and sizes_sets.py (logs/sizes.md) so the box is quiet.
set -uo pipefail
R=/ssd/search-prod/rg
T=/ssd/tiers2
L=/ssd/shrink/sortpatch/TIMING.lock
O=$R/timing
until [ -f $R/out/DONE ] && [ -f $R/out/DONE_pip2 ] && [ -s $R/logs/sizes.md ]; do sleep 30; done
until (set -o noclobber; echo "search-prod rg_timing $(date -Is)" > $L) 2> /dev/null; do sleep 30; done
trap 'grep -q search-prod $L && rm -f $L' EXIT
mkdir -p $O
declare -A DIR=(
  [orig-minimal]=$R/sets-orig/minimal [orig-recommended]=$R/sets-orig/recommended [orig-max]=$R/sets-orig/max
  [t2-q25-smallest]=$T/sets/q25/smallest [t2-q25-recommended]=$T/sets/q25/recommended
  [t2-uni75t8rc10-smallest]=$T/sets/uni75t8rc10/smallest [t2-uni75t8rc10-recommended]=$T/sets/uni75t8rc10/recommended
)
for set in orig-minimal orig-recommended orig-max t2-q25-smallest t2-uni75t8rc10-smallest t2-q25-recommended t2-uni75t8rc10-recommended; do
  for pool in p50k L2; do
    for mode in admin query; do
      for r in main pip2 head; do
        nice -n -20 taskset -c 2 $R/bin/rg_bench.$r ${DIR[$set]} $mode 3 < $R/pts/$pool.txt > $O/$set.$pool.$mode.$r.json 2> $O/$set.$pool.$mode.$r.err
      done
    done
  done
done
echo done > $O/DONE
