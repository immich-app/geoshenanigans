#!/bin/bash
# queries.sh : every answer set the attribution needs, after pipeline.sh, for pools p200k, l2c
# and grids, each preset:
#   B0  base data x base server        D1  r1 data (type gate) x base server
#   D4  tip data x base server         T2a T2b T3a T3b T6  tip data x each server commit
# (T6 = the tip server 509845b). Then loc-to-city for every label.
set -u
P=/ssd/parity
export JOBS=${JOBS:-24}
declare -A DATA=([B0]=base [D1]=r1 [D4]=r4 [T2a]=r4 [T2b]=r4 [T3a]=r4 [T3b]=r4 [T6]=r4)
declare -A SRV=([B0]=base [D1]=base [D4]=base [T2a]=s2a [T2b]=s2b [T3a]=s3a [T3b]=s3b [T6]=s6)
for l in B0 D1 D4 T2a T2b T3a T3b T6; do
  for s in smallest recommended max; do
    $P/runq.sh ${l}_$s $P/sets/${DATA[$l]}/$s $P/bin/grid_query.${SRV[$l]} p200k l2c grids
  done
  $P/l2c.sh $l
done
echo QUERIES_DONE
