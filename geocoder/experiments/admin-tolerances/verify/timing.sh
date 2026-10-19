#!/bin/bash
# timing.sh <out tsv> <p50k pool> <L2 pool> <cold pool> <label>=<reader>:<set dir>... : single-thread
# reverse-geocode latency under /ssd/shrink/sortpatch/TIMING.lock (waits; HOLDS_LOCK=1 if the caller
# has it), pinned to core 11, reader = /ssd/verify5/bin/grid_query.<reader>.
#   warm: one untimed pass, then best of 3 timed passes over the pool, minus the best of 3 one-point
#         runs (process start + index load), per query
#   cold: page cache dropped, one-point run; dropped again, one pass over the cold pool; difference
#         per query
# Rows: label reader set pool points best_s load_s us_per_query
set -u
out=$1 p50=$2 l2=$3 cold=$4; shift 4
V=/ssd/verify5
L=/ssd/shrink/sortpatch/TIMING.lock
if [ -z "${HOLDS_LOCK:-}" ]; then
  until (set -o noclobber; echo "verify5 timing $(date -Is)" > $L) 2> /dev/null; do sleep 15; done
  trap 'rm -f $L' EXIT
fi
head -1 $p50 > $V/tmp/one.txt
run() { local s e; s=$(date +%s.%N); taskset -c 11 "$1" "$2" < "$3" > /dev/null 2>&1; e=$(date +%s.%N); awk -v s=$s -v e=$e 'BEGIN{printf "%.4f", e-s}'; }
best3() { local b=999999 x; for r in 1 2 3; do x=$(run "$1" "$2" "$3"); b=$(awk -v a=$b -v x=$x 'BEGIN{print (x<a)?x:a}'); done; echo $b; }
for item in "$@"; do
  label=${item%%=*}; rest=${item#*=}; r=${rest%%:*}; d=${rest#*:}
  GQ=$V/bin/grid_query.$r
  load=$(best3 $GQ $d $V/tmp/one.txt)
  for pool in $p50 $l2; do
    run $GQ $d $pool > /dev/null
    b=$(best3 $GQ $d $pool)
    n=$(wc -l < $pool)
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%.1f\n' $label $r $d $(basename $pool .txt) $n $b $load $(awk -v b=$b -v l=$load -v n=$n 'BEGIN{print (b-l)*1e6/n}') >> $out
  done
  sync; echo 3 > /proc/sys/vm/drop_caches; cl=$(run $GQ $d $V/tmp/one.txt)
  sync; echo 3 > /proc/sys/vm/drop_caches; cb=$(run $GQ $d $cold)
  n=$(wc -l < $cold)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%.1f\n' $label $r $d cold $n $cb $cl $(awk -v b=$cb -v l=$cl -v n=$n 'BEGIN{print (b-l)*1e6/n}') >> $out
done
echo TIMING_DONE
