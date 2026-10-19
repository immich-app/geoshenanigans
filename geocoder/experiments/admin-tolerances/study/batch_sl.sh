#!/bin/bash
# batch_sl.sh : single-level relevance runs on planet-261006 admin-minimal: only level L at its
# q2.5+cap tolerance, every other level at truth geometry; real reader on every pool, so the
# Immich-field changes on B<L> / photo / vfresh are level L's alone.
set -u
cd /ssd/tiers3
for L in 2 3 4 5 6 7 8; do
  SKIPQ=1 ./mkset6.sh sl$L "levels=$L-$L;rest=none;scale=2.5;cap=0.1" > /dev/null
  ./runq.sh sl${L}_smallest sets6/sl$L/smallest
done
echo BATCH_SL_DONE
