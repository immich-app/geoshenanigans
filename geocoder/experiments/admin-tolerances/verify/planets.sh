#!/bin/bash
# planets.sh : verify5 planet builds under TIMING.lock, with verify5's own builders from the committed
# sources and the nightly's arguments (as tiers3 run_build.sh: 96 GiB scope, no swap, all cores,
# GC_MEMORY_RESERVE_MIB=12288, cold page cache before each build):
#   qD1   bq  (4f87ab6, q2.5 + cap)  planet-261018b warm from tiers3 out-D  (reproduces tiers3 out-D1?)
#   r1D1  br1 (bdac8cd, r1)          planet-261018b warm from tiers3 out-D  (r1 at planet scale)
#   r1D   br1                        planet-261017  cold                    (patch day 0)
#   r1D1w br1                        planet-261018b warm from out-r1D       (patch day 1)
# Each output keeps planet/ only. Logs /ssd/verify5/build/<name>/.
set -u
V=/ssd/verify5
W=$V/build
L=/ssd/shrink/sortpatch/TIMING.lock
P17=/ssd/tiers3/planet/planet-261017.osm.pbf
P18=/ssd/bugs2/planet/planet-261018b.osm.pbf
log() { echo "$(date -Is) $*"; }
until (set -o noclobber; echo "verify5 planets $(date -Is)" > $L) 2> /dev/null; do sleep 20; done
trap 'rm -f $L' EXIT
log LOCKED
export TMPDIR=$V/tmp
build() {  # build <name> <bin> <pbf> <prev|->
  local name=$1 bin=$2 pbf=$3 prev=$4
  local out=$W/out-$name d=$W/$name
  [ -f $d/run.txt ] && grep -q "rc=0" $d/run.txt && { log "SKIP $name"; return; }
  local EXTRA="--tiger-data /ssd/work/tiger-data.tar.gz --wikidata-sitelinks /ssd/work/qid_sitelinks.bin --external-postcodes /ssd/review/fixval2/gb_postcodes.csv.gz"
  [ "$prev" != - ] && EXTRA="$EXTRA --prev-output $prev"
  rm -rf "$out" "$d"; mkdir -p "$d"
  cat > "$d/inner.sh" << EOS
#!/bin/bash
/usr/bin/time -v $bin $out $pbf --continents --multi-output --multi-quality $EXTRA > $d/build.log 2>&1
rc=\$?
own=/sys/fs/cgroup\$(sed -n "s/^0:://p" /proc/self/cgroup)
cat \$own/memory.peak > $d/memory.peak
exit \$rc
EOS
  chmod +x "$d/inner.sh"
  echo "$name bin=$bin pbf=$pbf prev=$prev" > "$d/args.txt"
  sync; echo 3 > /proc/sys/vm/drop_caches
  echo "start $(date +%s) $(date -Is)" > "$d/run.txt"
  local s=$(date +%s.%N)
  systemd-run --quiet --scope --slice=verify5.slice --unit=verify5-$name-$$ -p MemoryMax=96G -p MemorySwapMax=0 \
    env GC_MEMORY_RESERVE_MIB=12288 TMPDIR=$TMPDIR "$d/inner.sh"
  local rc=$?
  local e=$(date +%s.%N)
  echo "end $(date +%s) rc=$rc wall=$(awk "BEGIN { printf \"%.1f\", $e - $s }")" >> "$d/run.txt"
  find $out -mindepth 1 -maxdepth 1 ! -name planet -exec rm -rf {} +
  log "BUILT $name rc=$rc"
}
build qD1 $V/bin-bq/build-index $P18 /ssd/tiers3/build/out-D
build r1D1 $V/bin-br1/build-index $P18 /ssd/tiers3/build/out-D
build r1D $V/bin-br1/build-index $P17 -
build r1D1w $V/bin-br1/build-index $P18 $W/out-r1D
log PLANETS_DONE
