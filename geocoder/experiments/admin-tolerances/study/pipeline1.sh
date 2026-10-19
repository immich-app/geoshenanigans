#!/bin/bash
# pipeline1.sh : under TIMING.lock: planet-261017 (planet-261006 + daily diffs 5130-5148, pbf-apply, as
# /ssd/bugs2/planet/apply2.sh does for 261018b = + 5149), then the two consecutive baseline builds with
# the admin dump: D = 261017 cold, D+1 = 261018b warm (--prev-output D). Baseline = q2.5 + cap 0.1.
set -u
T=/ssd/tiers3
L=/ssd/shrink/sortpatch/TIMING.lock
log() { echo "$(date -Is) $*"; }
until (set -o noclobber; echo "tiers3 pipeline1 $(date -Is)" > $L) 2> /dev/null; do sleep 20; done
trap 'rm -f $L' EXIT
log LOCKED
export TMPDIR=$T/tmp
P=$T/planet/planet-261017.osm.pbf
if [ ! -s $P ]; then
  cd /ssd/bugs2/planet
  /usr/bin/time -v /ssd/precision/build/bin-base/pbf-apply /ssd/work/planet-261006.osm.pbf $(for s in $(seq 5130 5148); do echo diff-$s.osc.gz; done) -o $P --replication-timestamp 2026-10-17T00:00:00Z --replication-sequence 5148 > $T/planet/apply.log 2>&1
  log "APPLY rc=$?"
fi
cd $T
./run_build.sh D $P - - $T/dumpD
./run_build.sh D1 /ssd/bugs2/planet/planet-261018b.osm.pbf $T/build/out-D - $T/dumpD1
log PIPELINE1_DONE
