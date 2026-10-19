#!/bin/bash
# patch.sh <label> <old build out> <new build out> : the real daily patch between two real planet builds
# for admin-minimal and quality/q2.5 (geocoder-diff of verify5's br1 build), applied with geocoder-patch
# to the old variant's client files alone (link_client_files.sh) and verified byte for byte against the
# new one (verify_patch.sh) -> /ssd/verify5/pv/<label>/{min,q}.gcpatch, cmp/patch_<label>.tsv.
set -u
V=/ssd/verify5
label=$1; old=$2/planet; new=$3/planet
TOOLS=$V/wt-br1/geocoder/builder/tools
P=$V/pv/$label
rm -rf $P; mkdir -p $P
export TMPDIR=$V/tmp
: > $V/cmp/patch_$label.tsv
for sub in admin-minimal quality/q2.5; do
  tag=${sub%%/*}; [ $tag = quality ] && tag=q || tag=min
  /usr/bin/time -f "%e s %M KiB" $V/bin-br1/geocoder-diff $old/$sub $new/$sub -o $P/$tag.gcpatch > $P/$tag.diff.log 2>&1
  rc=$?
  w=$(mktemp -d $V/tmp/pv-XXXXXX)
  mkdir -p $w/new
  $TOOLS/link_client_files.sh $old/$sub $w/old
  $V/bin-br1/geocoder-patch $w/old $P/$tag.gcpatch -o $w/new/ > $P/$tag.patch.log 2>&1
  v=$($TOOLS/verify_patch.sh $new/$sub $w/new 2>&1 | tail -1)
  rm -rf $w
  printf "%s\t%s\t%s\t%s\n" $tag "$(stat -c%s $P/$tag.gcpatch 2>/dev/null || echo 0)" "diff_rc=$rc" "$v" >> $V/cmp/patch_$label.tsv
done
cat $V/cmp/patch_$label.tsv
echo "PATCH_DONE $label"
