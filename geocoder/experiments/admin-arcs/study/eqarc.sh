#!/bin/bash
# eqarc.sh <arc name> ... : the arc reader (target-r grid_query on sets/<name>a, the arc format)
# against today's reader on the same arcs decoded into today's files (q/<name>_<set>): identical
# answer lines per pool. Also the per-polygon reader fix (target-r on sets/<name>) for q25 / r50c.
set -u
cd /ssd/arcs
for n in "$@"; do
  for s in smallest recommended; do
    GQ=/ssd/arcs/target-r/release/examples/grid_query ./runq.sh ${n}a_$s sets/${n}a/$s > /dev/null
    tot=0; diff=0
    for p in p200k places L2 L4 L8 uniform100k random5k grids l2c fresh20k; do
      d=$(paste -d '\n' q/${n}_$s/$p.txt q/${n}a_$s/$p.txt | awk 'NR%2==1{a=$0} NR%2==0{if(a!=$0)d++} END{print d+0}')
      t=$(wc -l < q/${n}_$s/$p.txt)
      tot=$((tot + t)); diff=$((diff + d))
    done
    echo "EQARC $n $s: $diff of $tot answer lines differ (arc reader vs today's reader on the decoded arcs)"
  done
done
echo EQARC_DONE
