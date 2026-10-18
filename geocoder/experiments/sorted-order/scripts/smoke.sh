#!/bin/bash
# smoke.sh OLD NEW : today's geocoder-diff / geocoder-patch on the SORTED
# planet/full dirs (format check, warm, untimed lock): diff in a 140 GiB
# memory scope, apply to an isolated copy (link_client_files.sh), verify
# byte for byte (verify_patch.sh). Mirror tree = sorted days + q2.5 (the
# differ borrows admin polygons from ../quality/q2.5, as in CI).
set -u
old=$1 new=$2
W=/ssd/shrink/sortpatch/sort; BB=/ssd/shrink/sortpatch/baseline
TOOLS=/ssd/shrink/repo/geocoder/builder/tools
export PATH=/ssd/shrink/zstd157/bin:$PATH TMPDIR=/ssd/shrink/tmp
for d in $old $new; do
  T=$W/tree/$d/planet
  [ -d $T/full ] && continue
  mkdir -p $T/quality $T/poi
  cp -al $W/days/$d/planet/full $T/full
  cp -al $W/days/$d/planet/quality/uncapped $T/quality/uncapped
  cp -al $W/days/$d/planet/poi/all $T/poi/all
  cp -al $BB/tree/$d/planet/quality/q2.5 $T/quality/q2.5
done
S=$W/smoke/$old-$new; rm -rf $S; mkdir -p $S/old $S/new
until [ $(( $(awk "/MemAvailable/ {print \$2}" /proc/meminfo) / 1048576 )) -ge 150 ]; do echo waiting for memory; sleep 30; done
/usr/bin/time -v -o $S/diff.time systemd-run --scope --quiet -p MemoryMax=140G -p MemorySwapMax=0 \
  taskset -c 32-47 $BB/bin/geocoder-diff $W/tree/$old/planet/full $W/tree/$new/planet/full -o $S/patch.gcpatch 2> $S/diff.log
echo "diff rc=$? size=$(stat -c%s $S/patch.gcpatch 2>/dev/null) raw=$(zstd -dc $S/patch.gcpatch | wc -c)"
grep -E "Elapsed|Maximum resident|User time|System time" $S/diff.time
$TOOLS/link_client_files.sh $W/tree/$old/planet/full $S/old/variant
/usr/bin/time -v -o $S/apply.time taskset -c 47 $BB/bin/geocoder-patch $S/old/variant $S/patch.gcpatch -o $S/new/ 2> $S/apply.log
echo "apply rc=$?"
grep -E "Elapsed|Maximum resident|User time|System time" $S/apply.time
$TOOLS/verify_patch.sh $W/tree/$new/planet/full $S/new | tail -3
