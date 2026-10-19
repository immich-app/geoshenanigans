#!/bin/bash
# after3.sh (parity resume) : replaces after.sh / after2.sh. Once bench.sh is done:
#  (last) under TIMING.lock, the data effect on query time: the base server (bc2e758) on the base
#     data vs the tip data (r4), smallest and recommended, p200k, core 2, warm, 3 runs alternated
#     (bench.sh timed the servers on the same data only);
#  2. every query (queries.sh) plus B0 / T6 on the verify4 30k pools (v4photo v4land v4cpu v4changed);
#  3. archive builds of the server commits (server_revs.sh), then analyze.sh.
P=/ssd/parity
L=/ssd/shrink/sortpatch/TIMING.lock
until grep -q BENCH_DONE $P/logs/bench.log 2>/dev/null; do sleep 10; done
echo "START $(date -Is)"
for p in photo land cpu changed; do
  [ -f $P/pts/v4$p.txt ] || cp /ssd/verify4/enc/pts/$p.txt $P/pts/v4$p.txt
done
JOBS=12 $P/queries.sh > $P/logs/queries.log 2>&1
echo "QUERIES $(date -Is) $(tail -1 $P/logs/queries.log)"
for l in B0 T6; do
  for s in smallest recommended max; do
    d=r4; v=s6
    [ $l = B0 ] && { d=base; v=base; }
    JOBS=12 $P/runq.sh ${l}_$s $P/sets/$d/$s $P/bin/grid_query.$v v4photo v4land v4cpu v4changed
  done
done > $P/logs/queries_v4.log 2>&1
echo "QUERIES_V4 $(date -Is) $(tail -1 $P/logs/queries_v4.log)"
$P/server_revs.sh > $P/logs/server_revs.log 2>&1
echo "SERVER_REVS $(date -Is)"
$P/analyze.sh > $P/logs/analyze.log 2>&1
echo "ANALYZE $(date -Is)"
until ( set -o noclobber; echo "parity $(date -Is)" > $L ) 2>/dev/null; do sleep 2; done
echo "LOCKED $(date -Is)"
for s in smallest recommended; do
  taskset -c 2 $P/bin/grid_query.base $P/sets/r4/$s < $P/pts/p200k.txt > /dev/null 2>&1   # warm
  taskset -c 2 $P/bin/grid_query.base $P/sets/base/$s < $P/pts/p200k.txt > /dev/null 2>&1
  for r in 1 2 3; do
    for d in base r4; do
      t0=$(date +%s.%N)
      taskset -c 2 $P/bin/grid_query.base $P/sets/$d/$s < $P/pts/p200k.txt > /dev/null 2>&1
      t1=$(date +%s.%N)
      echo "DATA $s $d run$r $(awk "BEGIN { printf \"%.2f\", $t1 - $t0 }")"
    done
  done
done
rm -f $L
echo "UNLOCKED $(date -Is)"
echo "AFTER_DONE $(date -Is)"
