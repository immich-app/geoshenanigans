#!/bin/bash
# eqr1.sh <name> ... : the reader with search-prod's four admin commits (target-r) against today's
# reader (target) on the same per-polygon set: answer lines that differ, per pool and set.
set -u
cd /ssd/arcs
for n in "$@"; do
  for s in smallest recommended; do
    GQ=/ssd/arcs/target-r/release/examples/grid_query ./runq.sh ${n}r1_$s sets/$n/$s > /dev/null
    out="EQR1 $n $s:"
    for p in p200k places L2 L4 L8 uniform100k random5k grids l2c fresh20k; do
      d=$(paste -d '\n' q/${n}_$s/$p.txt q/${n}r1_$s/$p.txt | awk 'NR%2==1{a=$0} NR%2==0{if(a!=$0)d++} END{print d+0}')
      out="$out $p $d"
    done
    echo "$out"
  done
done
echo EQR1_DONE
