#!/bin/bash
# run_build.sh <name> <pbf> <prev output|-> <GC_TIER_SCHEME|-> [dump dir] : one cold-cache planet build
# with the tiers3 builder (fix/admin-nesting + cap 9b55d28 + scheme override, /ssd/tiers3/bin-builder)
# and the nightly's arguments/inputs (as /ssd/bugs2/run_planet.sh): 96 GiB scope without swap, all
# cores, GC_MEMORY_RESERVE_MIB=12288; --prev-output <prev> like the nightly's warm build.
# Output /ssd/tiers3/build/out-<name>, logs /ssd/tiers3/build/<name>/. Caller holds TIMING.lock.
set -u
W=/ssd/tiers3/build
name=$1 pbf=$2 prev=$3 scheme=$4 dump=${5:-}
bin=${BIN:-/ssd/tiers3/bin-builder/build-index}
out=$W/out-$name
d=$W/$name
export TMPDIR=/ssd/tiers3/tmp
EXTRA="--tiger-data /ssd/work/tiger-data.tar.gz --wikidata-sitelinks /ssd/work/qid_sitelinks.bin --external-postcodes /ssd/review/fixval2/gb_postcodes.csv.gz"
[ "$prev" != - ] && EXTRA="$EXTRA --prev-output $prev"
ENVS="GC_MEMORY_RESERVE_MIB=12288 TMPDIR=$TMPDIR"
[ "$scheme" != - ] && ENVS="$ENVS GC_TIER_SCHEME=$scheme"
[ -n "$dump" ] && { rm -rf "$dump"; ENVS="$ENVS GC_TIER_DUMP=$dump"; }
rm -rf "$out" "$d"
mkdir -p "$d" "$TMPDIR"
cat > "$d/inner.sh" << EOS
#!/bin/bash
/usr/bin/time -v $bin $out $pbf --continents --multi-output --multi-quality $EXTRA > $d/build.log 2>&1
rc=\$?
own=/sys/fs/cgroup\$(sed -n "s/^0:://p" /proc/self/cgroup)
cat \$own/memory.peak > $d/memory.peak
exit \$rc
EOS
chmod +x "$d/inner.sh"
echo "$name pbf=$pbf prev=$prev scheme=$scheme dump=$dump bin=$bin" > "$d/args.txt"
sync
echo 3 > /proc/sys/vm/drop_caches
echo "start $(date +%s) $(date -Is)" > "$d/run.txt"
start=$(date +%s.%N)
systemd-run --quiet --scope --slice=tiers3.slice --unit=tiers3-$name-$$ -p MemoryMax=96G -p MemorySwapMax=0 \
  env $ENVS "$d/inner.sh"
rc=$?
end=$(date +%s.%N)
echo "end $(date +%s) rc=$rc wall=$(awk "BEGIN { printf \"%.1f\", $end - $start }")" >> "$d/run.txt"
echo "RUN_DONE $name rc=$rc"
