#!/bin/bash
# eqr3.sh <label>=<set name>:<reference answers name> ... : the integer-straddle reader (target-r3)
# on a set against reference answers (today's reader on the per-polygon or decoded files):
# answer lines that differ over every pool, both sets.
set -u
cd /ssd/arcs
for spec in "$@"; do
  label=${spec%%=*}; rest=${spec#*=}; name=${rest%%:*}; ref=${rest#*:}
  for s in smallest recommended; do
    GQ=/ssd/arcs/target-r3/release/examples/grid_query ./runq.sh ${label}_$s sets/$name/$s > /dev/null
    tot=0; diff=0
    for p in p200k places L2 L4 L8 uniform100k random5k grids l2c fresh20k; do
      d=$(paste -d '\n' q/${ref}_$s/$p.txt q/${label}_$s/$p.txt | awk 'NR%2==1{a=$0} NR%2==0{if(a!=$0)d++} END{print d+0}')
      tot=$((tot + $(wc -l < q/${ref}_$s/$p.txt))); diff=$((diff + d))
    done
    echo "EQR3 $label ($name vs $ref) $s: $diff of $tot answer lines differ"
  done
done
echo EQR3_DONE
