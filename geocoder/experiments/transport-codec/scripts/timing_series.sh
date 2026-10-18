#!/bin/bash
# Clean decode timing series on a quiet box (each run under TIMING.lock, core 63):
# plain zstd -d of the published files vs tt (vertex counts read back from the decoded
# records file) vs tt2 (records + vertices in one pass), warm and small-device.
# Every tt/tt2 output is compared byte for byte with the source set.
set -u
cd /ssd/transport
SRC=/ssd/shrink/idx/max
chk() { local bad=0; for f in $SRC/*; do cmp -s $f planet/dec/$(basename $f) || { echo "DIFF $f"; bad=1; }; done; echo "verify $1: $([ $bad = 0 ] && echo PASS || echo FAIL)"; rm -rf planet/dec; }
for run in "plain planet/plain_zst" "tt planet/best_4m" "tt2 planet/best_4m"; do
  set -- $run
  PREWARM=/ssd/transport/$2 scripts/timed.sh q_dec_$1_warm warm planet/dec scripts/dec_day.sh $1 $2 planet/dec
  chk q_dec_$1_warm
done
for run in "plain planet/plain_zst" "tt2 planet/best_4m" "tt planet/best_4m"; do
  set -- $run
  scripts/timed.sh q_dec_$1_small small planet/dec scripts/dec_day.sh $1 $2 planet/dec
  chk q_dec_$1_small
done
echo SERIES DONE
