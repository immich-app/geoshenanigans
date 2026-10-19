#!/bin/bash
# pipeline_all.sh : everything that needs TIMING.lock, in one hold:
#   1. planet-261017 = planet-261006 + daily diffs 5130-5148 (pbf-apply, as bugs2 apply2.sh makes
#      261018b = + 5149), so D = 261017 and D1 = 261018b are consecutive days
#   2. build D (cold) and D1 (warm, --prev-output D), baseline q2.5 + cap 0.1, admin dumps
#   3. D1 builds for each "name scheme" line of /ssd/tiers3/cands_build.txt (read when reached),
#      warm from D, iso-capable builder (bin-builder2): builder time + byte identity with tiervar
#   4. timeq3.sh over each "label=dir" line of /ssd/tiers3/cands_time.txt (both readers)
set -u
T=/ssd/tiers3
L=/ssd/shrink/sortpatch/TIMING.lock
log() { echo "$(date -Is) $*"; }
until (set -o noclobber; echo "tiers3 pipeline_all $(date -Is)" > $L) 2> /dev/null; do sleep 5; done
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
[ -f build/D/run.txt ] && grep -q "rc=0" build/D/run.txt || ./run_build.sh D $P - - $T/dumpD
log D_DONE
[ -f build/D1/run.txt ] && grep -q "rc=0" build/D1/run.txt || ./run_build.sh D1 /ssd/bugs2/planet/planet-261018b.osm.pbf $T/build/out-D - $T/dumpD1
log D1_DONE
mapfile -t builds < $T/cands_build.txt
for line in "${builds[@]}"; do
  name=${line%% *}; scheme=${line#* }
  [ -z "$name" ] && continue
  BIN=$T/bin-builder2/build-index ./run_build.sh D1_$name /ssd/bugs2/planet/planet-261018b.osm.pbf $T/build/out-D "$scheme"
  # keep only what the comparisons read
  find $T/build/out-D1_$name -mindepth 1 -maxdepth 1 ! -name planet -exec rm -rf {} +
  find $T/build/out-D1_$name/planet -mindepth 1 -maxdepth 1 ! -name admin-minimal ! -name quality -exec rm -rf {} +
  log "BUILT D1_$name"
done
items=()
while read -r it; do [ -n "$it" ] && items+=("$it"); done < $T/cands_time.txt
[ ${#items[@]} -gt 0 ] && HOLDS_LOCK=1 ./timeq3.sh $T/cmp/time.tsv "${items[@]}"
log PIPELINE_ALL_DONE
