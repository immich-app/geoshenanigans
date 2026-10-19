#!/bin/bash
# pt_time.sh : parity end-to-end query time under TIMING.lock, base (grid_query.bc2e758 on
# sets/base) against tip (grid_query.64183ef on sets/tip), single process pinned to core 2, warm
# (one untimed pass of each first). smallest and recommended: the parity track's p200k points
# (/ssd/parity/pts/p200k.txt), 3 alternating runs each. max: verify6's par30k, both sets read into
# the page cache first, 2 alternating runs each.
set -u
V=/ssd/verify6
L=/ssd/shrink/sortpatch/TIMING.lock
O=$V/cmp/pt_time.txt
until ( set -o noclobber; echo "verify6 parity timing $(date -Is)" > $L ) 2>/dev/null; do sleep 10; done
echo "LOCKED $(date -Is)" > $O
t() {  # label binary set pts
  local t0 t1
  t0=$(date +%s.%N)
  taskset -c 2 $2 $3 < $4 > /dev/null 2>&1
  t1=$(date +%s.%N)
  echo "$1 $(awk "BEGIN { printf \"%.2f\", $t1 - $t0 }")" >> $O
}
P200=/ssd/parity/pts/p200k.txt
for s in smallest recommended; do
  taskset -c 2 $V/bin/grid_query.bc2e758 $V/sets/base/$s < $P200 > /dev/null 2>&1
  taskset -c 2 $V/bin/grid_query.64183ef $V/sets/tip/$s < $P200 > /dev/null 2>&1
  for r in 1 2 3; do
    t "$s p200k base run$r" $V/bin/grid_query.bc2e758 $V/sets/base/$s $P200
    t "$s p200k tip run$r" $V/bin/grid_query.64183ef $V/sets/tip/$s $P200
  done
done
cat $V/sets/base/max/* $V/sets/tip/max/* > /dev/null
for r in 1 2; do
  t "max par30k base run$r" $V/bin/grid_query.bc2e758 $V/sets/base/max $V/pts/par30k.txt
  t "max par30k tip run$r" $V/bin/grid_query.64183ef $V/sets/tip/max $V/pts/par30k.txt
done
rm -f $L
echo "UNLOCKED $(date -Is)" >> $O
echo PT_TIME_DONE
