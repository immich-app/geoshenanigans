#!/bin/bash
# run_planet.sh <name> <build-index> : one cold planet build of planet-261018b (as
# /ssd/bugs2/run_planet.sh: nightly arguments and inputs, 96 GiB scope without swap, all cores,
# GC_MEMORY_RESERVE_MIB=12288). Output /ssd/parity/build/out-<name>, logs
# /ssd/parity/build/<name>/. Caller holds TIMING.lock.
set -u
W=/ssd/parity/build
name=$1
bin=$2
out=$W/out-$name
d=$W/$name
export TMPDIR=/ssd/parity/tmp
PBF=/ssd/bugs2/planet/planet-261018b.osm.pbf
EXTRA=(--tiger-data /ssd/work/tiger-data.tar.gz
       --wikidata-sitelinks /ssd/work/qid_sitelinks.bin
       --external-postcodes /ssd/review/fixval2/gb_postcodes.csv.gz)
rm -rf "$out" "$d"
mkdir -p "$d" "$TMPDIR"
unit=parity-$name
cat > "$d/inner.sh" << EOS
#!/bin/bash
/usr/bin/time -v $bin $out $PBF --continents --multi-output --multi-quality ${EXTRA[*]} > $d/build.log 2>&1
rc=\$?
own=/sys/fs/cgroup\$(sed -n "s/^0:://p" /proc/self/cgroup)
cat \$own/memory.peak > $d/memory.peak
cat \$own/memory.events > $d/memory.events
exit \$rc
EOS
chmod +x "$d/inner.sh"
sync
echo 3 > /proc/sys/vm/drop_caches
echo "start $(date +%s) $(date -Is)" > "$d/run.txt"
start=$(date +%s.%N)
systemd-run --quiet --scope --slice=parity.slice --unit=$unit -p MemoryMax=96G -p MemorySwapMax=0 \
  env GC_MEMORY_RESERVE_MIB=12288 TMPDIR=$TMPDIR "$d/inner.sh"
rc=$?
end=$(date +%s.%N)
echo "end $(date +%s) rc=$rc wall=$(awk "BEGIN { printf \"%.1f\", $end - $start }")" >> "$d/run.txt"
if [ -d "$out" ]; then
  (cd "$out" && find . -type f -printf "%s\t%P\n" | sort -k2) > "$d/files.tsv"
fi
echo "RUN_DONE $name rc=$rc"
