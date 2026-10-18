#!/bin/bash
# Four-changes today's-format variants (change 1, P1, P2) for both presets, answered on the fresh 20k points.
set -uo pipefail
cd /ssd/shrink/max/verify/fc
export TMPDIR=/ssd/shrink/tmp
P=/ssd/shrink/venv/bin/python
for s in max1 withaddr1; do
  $P -u postal.py p1 sets/$s sets/${s}p1
  $P -u postal.py p2 sets/$s sets/${s}p2
done
cd /ssd/shrink/max/verify
S=/ssd/shrink/max/verify/fc/sets
scripts/runq_v.sh fc_max1 $S/max1 base_max &
scripts/runq_v.sh fc_max1p1 $S/max1p1 base_max &
scripts/runq_v.sh fc_max1p2 $S/max1p2 base_max &
scripts/runq_v.sh fc_wa1 $S/withaddr1 base_wa &
scripts/runq_v.sh fc_wa1p1 $S/withaddr1p1 base_wa &
scripts/runq_v.sh fc_wa1p2 $S/withaddr1p2 base_wa &
wait
# P1 alone on today's files (no change 1): postal_* simply absent
mkdir -p work/p1only_max
for f in /ssd/shrink/idx/max/*; do b=$(basename $f); case $b in postal_*) ;; *) ln -f $f work/p1only_max/$b;; esac; done
scripts/runq_v.sh p1only_max work/p1only_max base_max
echo DONE
