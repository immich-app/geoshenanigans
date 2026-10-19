#!/bin/bash
# pipeline2.sh (parity resume) : 8896cf5 (no place areas of address rank 0), measured like the rest.
#  1. builder from a git archive of 8896cf5 -> bin/build-index.r5 (tests run)
#  2. under TIMING.lock: one cold planet build of planet-261018b -> build/out-r5; sets/r5
#  3. queries: D5 = r5 data x base server, T9 = r5 data x s7 server (64183ef): p200k l2c grids + v4 pools;
#     loc-to-city for both
#  4. explain_builder r4 -> r5 (+ label pass), polygons only in r4 by place type, sizes r4 -> r5
#  5. under TIMING.lock: query time, base server on r4 vs r5 data (smallest, recommended 3 runs each)
#     and s7 on r4 vs r5 max (1 run each)
set -u
P=/ssd/parity
L=/ssd/shrink/sortpatch/TIMING.lock
PY="/ssd/shrink/venv/bin/python -I"
export TMPDIR=$P/tmp
log() { echo "$(date -Is) $*"; }
rev=8896cf5
d=$P/src-$rev; rm -rf $d; mkdir -p $d
git -C $P/repo archive $rev geocoder/builder | tar -x -C $d
mkdir -p $d/geocoder/server
rm -rf $P/bbuild-r5a
$P/builder_build.sh r5a $d
cmp $P/bin/build-index.r5a $P/bin/build-index.r5 && log "archive builder = working-tree builder"
until ( set -o noclobber; echo "parity $(date -Is)" > $L ) 2>/dev/null; do sleep 2; done
log LOCKED
$P/run_planet.sh r5 $P/bin/build-index.r5a
rm -f $L
log "BUILD_DONE $(tail -1 $P/build/r5/run.txt)"
$PY $P/probe/assemble_sets.py $P/build/out-r5 $P/sets/r5
log SETS_DONE
export JOBS=16
for s in smallest recommended max; do
  $P/runq.sh D5_$s $P/sets/r5/$s $P/bin/grid_query.base p200k l2c grids v4photo v4land v4cpu v4changed
  $P/runq.sh T9_$s $P/sets/r5/$s $P/bin/grid_query.s7 p200k l2c grids v4photo v4land v4cpu v4changed
done > $P/logs/q_r5.log 2>&1
$P/l2c.sh D5 >> $P/logs/q_r5.log 2>&1
$P/l2c.sh T9 >> $P/logs/q_r5.log 2>&1
log QUERIES_DONE
C=$P/cmp
q() { echo $P/q/$1_$2/$3.txt; }
for s in smallest recommended max; do
  $PY $P/probe/explain_builder.py $s r4 r5 $(q D4 $s p200k) $(q D5 $s p200k) $C/builder_rank0.$s.json
  $PY $P/probe/explain_label.py $s r4 r5 $C/builder_rank0.$s.json $C/builder_rank0_label.$s.json
  $PY $P/probe/adiff.py $(q D4 $s p200k) $(q D5 $s p200k) 0 | sed -n '1,3p;/by field/,/examples/p' > $C/step_rank0.$s.txt
  $PY $P/probe/adiff.py $(q B0 $s p200k) $(q T9 $s p200k) 0 | sed -n '1,3p;/by field/,/examples/p' > $C/total_T9.$s.txt
done > $C/explain_rank0.txt 2>&1
$PY $P/probe/dropped_ids.py $P/build/out-r4 $P/build/out-r5 $P/census/ids_r4_r5.txt $P/census/r4_r5.json > $C/census_r4_r5.txt 2>&1
$PY $P/probe/l15_cost.py $P/build/out-r5 quality/q2.5 > $C/l15_r5.txt 2>&1
$P/sizes.sh $P/sets/r4 $P/sets/r5 > $C/sizes_r4_r5.tsv 2> $C/sizes_r4_r5.err
$PY $P/probe/sizesum.py $C/sizes_r4_r5.tsv > $C/sizes_r4_r5.txt
log ANALYSIS_DONE
until ( set -o noclobber; echo "parity $(date -Is)" > $L ) 2>/dev/null; do sleep 2; done
log LOCKED2
t() {  # t <label> <binary> <set> : one timed run
  local t0 t1
  t0=$(date +%s.%N)
  taskset -c 2 $2 $3 < $P/pts/p200k.txt > /dev/null 2>&1
  t1=$(date +%s.%N)
  echo "$1 $(awk "BEGIN { printf \"%.2f\", $t1 - $t0 }")"
}
for s in smallest recommended; do
  taskset -c 2 $P/bin/grid_query.base $P/sets/r4/$s < $P/pts/p200k.txt > /dev/null 2>&1   # warm
  taskset -c 2 $P/bin/grid_query.base $P/sets/r5/$s < $P/pts/p200k.txt > /dev/null 2>&1
  for r in 1 2 3; do
    for dd in r4 r5; do t "DATA2 $s $dd run$r" $P/bin/grid_query.base $P/sets/$dd/$s; done
  done
done
for dd in r4 r5; do t "DATA2 max $dd run1 (s7)" $P/bin/grid_query.s7 $P/sets/$dd/max; done
rm -f $L
log UNLOCKED2
echo PIPELINE2_DONE
