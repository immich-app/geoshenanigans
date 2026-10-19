#!/bin/bash
# final.sh (parity resume) : replaces the tail of after3.sh. Waits for analyze.sh, runs analyze2.sh,
# then under TIMING.lock (p200k, core 2, warm, runs alternated):
#   DATA  base server (bc2e758) on the base data vs the tip data (r4): smallest, recommended, 3 runs
#   SRV   tip server 64183ef (s7) vs 509845b (s6) on the tip data: smallest, recommended 3 runs,
#         max 1 run each (bench.sh has base vs s6, 3 runs each, max included)
P=/ssd/parity
L=/ssd/shrink/sortpatch/TIMING.lock
until grep -q ANALYZE_DONE $P/logs/analyze.log 2>/dev/null; do sleep 10; done
echo "ANALYZE_DONE $(date -Is)"
$P/analyze2.sh > $P/logs/analyze2.log 2>&1
echo "ANALYZE2 $(date -Is) $(tail -1 $P/logs/analyze2.log)"
until ( set -o noclobber; echo "parity $(date -Is)" > $L ) 2>/dev/null; do sleep 2; done
echo "LOCKED $(date -Is)"
t() {  # t <label> <binary> <set> : one timed run
  local t0 t1
  t0=$(date +%s.%N)
  taskset -c 2 $2 $3 < $P/pts/p200k.txt > /dev/null 2>&1
  t1=$(date +%s.%N)
  echo "$1 $(awk "BEGIN { printf \"%.2f\", $t1 - $t0 }")"
}
for s in smallest recommended; do
  taskset -c 2 $P/bin/grid_query.base $P/sets/r4/$s < $P/pts/p200k.txt > /dev/null 2>&1   # warm
  taskset -c 2 $P/bin/grid_query.base $P/sets/base/$s < $P/pts/p200k.txt > /dev/null 2>&1
  for r in 1 2 3; do
    for d in base r4; do t "DATA $s $d run$r" $P/bin/grid_query.base $P/sets/$d/$s; done
  done
  for r in 1 2 3; do
    for v in s6 s7; do t "SRV $s $v run$r" $P/bin/grid_query.$v $P/sets/r4/$s; done
  done
done
taskset -c 2 $P/bin/grid_query.s7 $P/sets/r4/max < $P/pts/p200k.txt > /dev/null 2>&1   # warm
for v in s6 s7; do t "SRV max $v run1" $P/bin/grid_query.$v $P/sets/r4/max; done
rm -f $L
echo "UNLOCKED $(date -Is)"
echo "FINAL_DONE $(date -Is)"
