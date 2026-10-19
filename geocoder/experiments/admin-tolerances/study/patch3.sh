#!/bin/bash
# patch3.sh <name> <minimal scheme> [quality scheme (default: minimal + t9..t11 = 37.5)] : daily patch
# of one tier scheme between the consecutive builds D (planet-261017) and D1 (planet-261018b, warm,
# --prev-output D). Each day's admin-minimal and quality/q2.5 are re-made from that build's own
# admin dump with tiervar3-n (admin-minimal in the builder's warm slots, TIERVAR_MIN_SLOTS), the
# other files of the dir linked from the build, then geocoder-diff D -> D1 per dir, geocoder-patch
# applied to a copy of D and compared byte for byte with D1 (verify_patch.sh).
# -> /ssd/tiers3/pv/<name>/{min,q}.gcpatch, cmp/patch_<name>.tsv (dir, patch bytes, verify).
set -u
T=/ssd/tiers3
name=$1
ms=$2
qs=${3:-"$2;t9=37.5;t10=37.5;t11=37.5"}
P=$T/pv/$name
TOOLS=$T/repo/geocoder/builder/tools
export OMP_NUM_THREADS=${THREADS:-16} TMPDIR=$T/tmp
rm -rf $P
for day in D D1; do
  out=$T/build/out-$day/planet
  V=$P/$day/planet
  mkdir -p $V/admin-minimal $V/quality/q2.5
  ln -s $out/full $V/full
  TIERVAR_MIN_SLOTS=$out/admin-minimal/admin_polygons.bin $T/bin/tiervar3-n minimal $T/dump$day $out/admin $V/admin-minimal "$ms" > $P/$day.min.json
  rm -f $V/admin-minimal/admin_parents.bin
  $T/bin/tiervar3-n quality $T/dump$day $V/quality/q2.5 "$qs" > $P/$day.q.json
  for sub in admin-minimal quality/q2.5; do
    for f in $out/$sub/*; do b=$(basename $f); [ -e $V/$sub/$b ] || ln $f $V/$sub/$b; done
  done
done
: > $T/cmp/patch_$name.tsv
for sub in admin-minimal quality/q2.5; do
  tag=${sub%%/*}; [ $tag = quality ] && tag=q || tag=min
  /usr/bin/time -f "%e s %M KiB" $T/bin-builder/geocoder-diff $P/D/planet/$sub $P/D1/planet/$sub -o $P/$tag.gcpatch > $P/$tag.diff.log 2>&1
  rc=$?
  w=$(mktemp -d $T/tmp/pv-XXXXXX)
  mkdir -p $w/old $w/new
  $TOOLS/link_client_files.sh $P/D/planet/$sub $w/old/variant
  $T/bin-builder/geocoder-patch $w/old/variant $P/$tag.gcpatch -o $w/new/ > $P/$tag.patch.log 2>&1
  v=$($TOOLS/verify_patch.sh $P/D1/planet/$sub $w/new 2>&1 | tail -1)
  rm -rf $w
  printf "%s\t%s\t%s\t%s\n" $tag "$(stat -c%s $P/$tag.gcpatch 2>/dev/null || echo 0)" "diff_rc=$rc" "$v" >> $T/cmp/patch_$name.tsv
done
echo "PATCH_DONE $name"
