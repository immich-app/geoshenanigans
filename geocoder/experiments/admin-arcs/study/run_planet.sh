#!/bin/bash
# run_planet.sh <name> <pbf> : one cold planet build (arcs-dump branch, fix/admin-nesting + dump
# hooks) with the nightly's arguments and inputs (as /ssd/tiers2/run_planet.sh): 96 GiB scope
# without swap, all cores, GC_MEMORY_RESERVE_MIB=12288, GC_TIER_DUMP + GC_ARC_DUMP on.
# Takes /ssd/shrink/sortpatch/TIMING.lock (waits if held). Output /ssd/arcs/build/out-<name>,
# dumps /ssd/arcs/build/<name>/{dump,arcdump}, logs /ssd/arcs/build/<name>/.
# Afterwards drops the continent outputs and the quality tiers nobody uses (keeps planet/{admin,
# admin-minimal,full,poi/major,quality/uncapped,quality/q2.5}).
set -u
W=/ssd/arcs/build
name=$1
pbf=$2
bin=/ssd/arcs/bin/build-index
out=$W/out-$name
d=$W/$name
L=/ssd/shrink/sortpatch/TIMING.lock
until (set -o noclobber; echo "arcs planet-$name $(date -Is)" > $L) 2> /dev/null; do sleep 5; done
trap 'rm -f $L' EXIT
export TMPDIR=/ssd/arcs/tmp
EXTRA=(--tiger-data /ssd/work/tiger-data.tar.gz
       --wikidata-sitelinks /ssd/work/qid_sitelinks.bin
       --external-postcodes /ssd/review/fixval2/gb_postcodes.csv.gz)
rm -rf "$out" "$d"
mkdir -p "$d/dump" "$d/arcdump" "$TMPDIR"
cat > "$d/inner.sh" << EOS
#!/bin/bash
/usr/bin/time -v $bin $out $pbf --continents --multi-output --multi-quality ${EXTRA[*]} > $d/build.log 2>&1
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
systemd-run --quiet --scope --slice=arcs.slice --unit=arcs-$name -p MemoryMax=96G -p MemorySwapMax=0 \
  env GC_MEMORY_RESERVE_MIB=12288 TMPDIR=$TMPDIR GC_TIER_DUMP=$d/dump GC_ARC_DUMP=$d/arcdump "$d/inner.sh"
rc=$?
end=$(date +%s.%N)
echo "end $(date +%s) rc=$rc wall=$(awk "BEGIN { printf \"%.1f\", $end - $start }")" >> "$d/run.txt"
rm -f $L
trap - EXIT
# trim
for x in "$out"/*; do [ "$(basename "$x")" = planet ] || rm -rf "$x"; done
for q in "$out"/planet/quality/*; do case $(basename "$q") in uncapped|q2.5) ;; *) rm -rf "$q" ;; esac; done
for x in "$out"/planet/*; do case $(basename "$x") in admin|admin-minimal|full|poi|quality) ;; *) rm -rf "$x" ;; esac; done
for x in "$out"/planet/poi/*; do [ "$(basename "$x")" = major ] || rm -rf "$x"; done
du -sh "$out" "$d" >> "$d/run.txt"
echo "RUN_DONE $name rc=$rc"
