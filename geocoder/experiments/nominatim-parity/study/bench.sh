#!/bin/bash
# bench.sh : query time of the base (bc2e758) and tip (509845b) servers on the tip data, p200k,
# one process pinned to core 2, warm cache, runs alternated, 3 each; under TIMING.lock.
L=/ssd/shrink/sortpatch/TIMING.lock
P=/ssd/parity
until grep -q SETS_DONE $P/logs/pipeline.log; do sleep 5; done
# take the lock atomically (noclobber), polling every 2 s
until ( set -o noclobber; echo "parity $(date -Is)" > $L ) 2>/dev/null; do sleep 2; done
echo "LOCKED $(date -Is)"
for s in smallest recommended max; do
  taskset -c 2 $P/bin/grid_query.base $P/sets/r4/$s < $P/pts/p200k.txt > /dev/null 2>&1   # warm
  for r in 1 2 3; do
    for v in base s6; do
      t0=$(date +%s.%N)
      taskset -c 2 $P/bin/grid_query.$v $P/sets/r4/$s < $P/pts/p200k.txt > /dev/null 2>&1
      t1=$(date +%s.%N)
      echo "$s $v run$r $(awk "BEGIN { printf \"%.2f\", $t1 - $t0 }")"
    done
  done
done
echo "BENCH_RUNS_DONE $(date -Is)"
# Census of the OSM objects the builds disagree on (a full planet read, so still under the lock).
PY="/ssd/shrink/venv/bin/python -I"
mkdir -p $P/census
$PY $P/probe/dropped_ids.py $P/build/out-base $P/build/out-r1 $P/census/ids_base_r1.txt $P/census/base_r1.json
$PY $P/probe/dropped_ids.py $P/build/out-r1 $P/build/out-r4 $P/census/ids_r1_r4.txt $P/census/r1_r4.json
sort -u $P/census/ids_base_r1.txt $P/census/ids_r1_r4.txt > $P/census/ids.txt
osmium getid -O -i $P/census/ids.txt /ssd/bugs2/planet/planet-261018b.osm.pbf -f opl -o $P/census/objs.opl > $P/census/getid.log 2>&1
echo "GETID rc=$? $(wc -l < $P/census/objs.opl) objects of $(wc -l < $P/census/ids.txt)"
rm -f $L
$PY $P/probe/tag_census.py $P/census/objs.opl $P/census/base_r1.json $P/census/r1_r4.json > $P/census/census.txt
echo "BENCH_DONE $(date -Is)"
