#!/bin/bash
# workq.sh <set: smallest|recommended> <label>=<set name> ... : decoded admin geometry per query
# (GC_ARC_STATS=1 of the target-r grid_query: per-polygon point_in_encoded_polygon calls and
# vertices; arc decodes, bbox skips, cache hits, arc vertices), pools p50k and L2.
# -> cmp/work_<set>.tsv (label, pool, points, the counters)
set -u
st=$1; shift
A=/ssd/arcs
mkdir -p $A/tmp/t
head -50000 $A/pts/p200k.txt > $A/tmp/t/p50k.txt
cp $A/pts/L2.txt $A/tmp/t/L2.txt
for spec in "$@"; do
  label=${spec%%=*}; name=${spec#*=}
  for pool in p50k L2; do
    n=$(wc -l < $A/tmp/t/$pool.txt)
    s=$(GC_ARC_STATS=1 $A/target-r/release/examples/grid_query $A/sets/$name/$st < $A/tmp/t/$pool.txt 2>&1 > /dev/null | grep ARC_STATS)
    printf '%s\t%s\t%s\t%s\n' "$label" "$pool" "$n" "$s" >> $A/cmp/work_$st.tsv
  done
done
echo WORKQ_DONE
