#!/bin/bash
# advexp.sh <log>: under TIMING.lock, max (and rec) in a 32 MiB cgroup with the experiment binary
# place-search.adv (HEAD + mk_adv.py) and several madvise choices for the streamed files, sampled by
# diag.py; set page cache evicted before each run. Then the same choices unconstrained for max.
set -u
S=/ssd/searchbuild
LOCK=/ssd/shrink/sortpatch/TIMING.lock
log=$1
export SETS=$S/psets RUNS=$S/runs3
until (set -o noclobber; echo "searchbuild timing $(date -Iseconds)" > $LOCK) 2>/dev/null; do sleep 30; done
echo "LOCK taken $(date -Iseconds)" >> $log
trap 'rm -f $LOCK; echo "LOCK released $(date -Iseconds)" >> $log' EXIT
ev() { python3 -I $S/evict.py --no-links $SETS/$1 $S/extras/$1 >> $log 2>&1; }
run() {  # cfg mode label envs
  case $1 in min) d=minimal ;; rec) d=recommended ;; max) d=max ;; esac
  ev $d
  LABEL=$3 ENVS="$4" $S/diagrun.sh adv $1 $2 >> $log 2>&1
}
run max cg32 a1 "ADV_REC=seq ADV_STR=normal"
run max cg32 a2 "ADV_REC=seq ADV_STR=seq"
run max cg32 a3 "ADV_REC=normal ADV_STR=normal"
run rec cg32 a1 "ADV_REC=seq ADV_STR=normal"
run rec cg32 a0 ""
run max free a1 "ADV_REC=seq ADV_STR=normal"
