#!/bin/bash
# final3.sh (parity resume) : the final tip (9fe43d0 data r5 x 64183ef server s7 = T9) against
# bc2e758 (B0): Nominatim point sets, Singapore, then under TIMING.lock end-to-end query time
# (p200k, core 2, warm, alternated): smallest, recommended 3 runs each, max 1 run each after reading both sets into the page cache.
P=/ssd/parity
L=/ssd/shrink/sortpatch/TIMING.lock
PY="/ssd/shrink/venv/bin/python -I"
C=$P/cmp
for f in fixed24 extra trace l2c6 l2c7 s7 uni typ mini lbl misc mecca; do
  for s in smallest recommended max; do
    echo "## $f $s"
    $PY $P/probe/score.py $P/nominatim/${f}_api.jsonl place B0=$P/bin/grid_query.base:$P/sets/base/$s T7=$P/bin/grid_query.s7:$P/sets/r4/$s T9=$P/bin/grid_query.s7:$P/sets/r5/$s | tail -3
  done
done > $C/nominatim_summary_T9.txt 2>&1
for s in smallest recommended max; do
  $PY $P/probe/score.py $P/nominatim/fixed24_api.jsonl place B0=$P/bin/grid_query.base:$P/sets/base/$s T9=$P/bin/grid_query.s7:$P/sets/r5/$s > $C/nominatim_fixed24_${s}_T9.md
  $PY $P/probe/score.py $P/nominatim/fixed24_api.jsonl all B0=$P/bin/grid_query.base:$P/sets/base/$s T9=$P/bin/grid_query.s7:$P/sets/r5/$s > $C/nominatim_fixed24_${s}_T9_all.md
done
$PY $P/probe/sg_city.py $P/q p200k,v4photo,v4land,v4cpu,v4changed B0,T7,T9 > $C/sg_city_T9.md 2>&1
echo "SCORES_DONE $(date -Is)"
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
  taskset -c 2 $P/bin/grid_query.base $P/sets/base/$s < $P/pts/p200k.txt > /dev/null 2>&1   # warm
  taskset -c 2 $P/bin/grid_query.s7 $P/sets/r5/$s < $P/pts/p200k.txt > /dev/null 2>&1
  for r in 1 2 3; do
    t "E2E $s B0 run$r" $P/bin/grid_query.base $P/sets/base/$s
    t "E2E $s tip run$r" $P/bin/grid_query.s7 $P/sets/r5/$s
  done
done
# max: both sets read into the page cache first (251 GiB RAM, 33 GiB each), then one run each
cat $P/sets/base/max/* $P/sets/r5/max/* > /dev/null
t "E2E max B0 run1" $P/bin/grid_query.base $P/sets/base/max
t "E2E max tip run1" $P/bin/grid_query.s7 $P/sets/r5/max
rm -f $L
echo "UNLOCKED $(date -Is)"
echo FINAL3_DONE
