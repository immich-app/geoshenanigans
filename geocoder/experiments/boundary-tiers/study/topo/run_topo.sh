#!/bin/bash
# run_topo.sh : the vertices track's shared-arc estimator (topo_uncapped, + TOPO_UNI) on the
# uncapped-WITH-HOLES quality dir (var/unc/q, L2-8 taken by the tool), q2.5 and uniform 75 m.
# Outputs topo/out_<tag>/ and logs topo/<tag>.log. 16 threads each, one after the other.
set -u
cd /ssd/tiers2/topo
export OMP_NUM_THREADS=16
for tag in q25 uni75; do
  rm -rf out_$tag; mkdir -p out_$tag
  if [ $tag = uni75 ]; then export TOPO_UNI=75; else unset TOPO_UNI; fi
  /usr/bin/time -v ./topo_uncapped /ssd/tiers2/var/unc/q out_$tag 1e-5 repair runsonly > $tag.log 2>&1
  echo "TOPO_DONE $tag rc=$?"
done
