#!/bin/bash
# run_planet.sh <name> <build-index> [dump dir] : one cold planet build of planet-261006 with the
# nightly's arguments and inputs (as /ssd/bugs/run_planet.sh): 96 GiB scope without swap, all
# cores, GC_MEMORY_RESERVE_MIB=12288. Takes /ssd/shrink/sortpatch/TIMING.lock (waits if held).
# Output /ssd/tiers2/build/out-<name>, logs /ssd/tiers2/build/<name>/. With a dump dir, the
# builder writes its final planet admin arrays there (GC_TIER_DUMP, tiers2-dump branch).
set -u
W=/ssd/tiers2/build
name=$1
bin=$2
dump=${3:-}
out=$W/out-$name
d=$W/$name
L=/ssd/shrink/sortpatch/TIMING.lock
until (set -o noclobber; echo "tiers2 $(date -Is)" > $L) 2> /dev/null; do sleep 5; done
trap 'rm -f $L' EXIT
export TMPDIR=/ssd/tiers2/tmp
EXTRA=(--tiger-data /ssd/work/tiger-data.tar.gz
       --wikidata-sitelinks /ssd/work/qid_sitelinks.bin
       --external-postcodes /ssd/review/fixval2/gb_postcodes.csv.gz)
rm -rf "$out" "$d"
mkdir -p "$d" "$TMPDIR"
cat > "$d/inner.sh" << EOS
#!/bin/bash
/usr/bin/time -v $bin $out /ssd/work/planet-261006.osm.pbf --continents --multi-output --multi-quality ${EXTRA[*]} > $d/build.log 2>&1
rc=\$?
own=/sys/fs/cgroup\$(sed -n 's/^0:://p' /proc/self/cgroup)
cat \$own/memory.peak > $d/memory.peak
exit \$rc
EOS
chmod +x "$d/inner.sh"
sync
echo 3 > /proc/sys/vm/drop_caches
echo "start $(date +%s) $(date -Is)" > "$d/run.txt"
start=$(date +%s.%N)
systemd-run --quiet --scope --slice=tiers2.slice --unit=tiers2-$name -p MemoryMax=96G -p MemorySwapMax=0 \
  env GC_MEMORY_RESERVE_MIB=12288 TMPDIR=$TMPDIR ${dump:+GC_TIER_DUMP=$dump} "$d/inner.sh"
rc=$?
end=$(date +%s.%N)
echo "end $(date +%s) rc=$rc wall=$(awk "BEGIN { printf \"%.1f\", $end - $start }")" >> "$d/run.txt"
echo "RUN_DONE $name rc=$rc"
