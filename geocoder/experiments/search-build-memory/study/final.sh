#!/bin/bash
# final.sh <log>: under TIMING.lock, on the private set copies (psets), page cache of the set evicted
# before each run: the final binary (place-search.s8 = d54f85d) with default arguments (no --mem-mb)
# on min / rec / max inside a 32 MiB cgroup and unconstrained (runb.sh: memwatch3 peaks, files vs
# reference), then diag.py runs of max in the cgroup and unconstrained (where the time goes).
set -u
S=/ssd/searchbuild
LOCK=/ssd/shrink/sortpatch/TIMING.lock
log=$1
export SETS=$S/psets RUNS=$S/runs3
mkdir -p $RUNS
until (set -o noclobber; echo "searchbuild timing $(date -Iseconds)" > $LOCK) 2>/dev/null; do sleep 30; done
echo "LOCK taken $(date -Iseconds)" >> $log
trap 'rm -f $LOCK; echo "LOCK released $(date -Iseconds)" >> $log' EXIT
ev() { python3 -I $S/evict.py --no-links $SETS/$1 $S/extras/$1 >> $log 2>&1; }
for spec in min:minimal:cg32 rec:recommended:cg32 max:max:cg32 min:minimal:free rec:recommended:free max:max:free; do
  IFS=: read -r cfg d mode <<< "$spec"
  ev $d
  $S/runb.sh s8 $cfg $mode 40 >> $log 2>&1
  echo "DONE s8:$spec $(date -Iseconds)" >> $log
done
for mode in cg32 free; do
  ev max
  $S/diagrun.sh s8 max $mode >> $log 2>&1
  echo "DONE diag:max:$mode $(date -Iseconds)" >> $log
done
