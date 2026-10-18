#!/bin/bash
# link_day.sh DATE: complete days/DATE (sorted files already written in
# planet/full) with hardlinks of every other published file. The published
# patch.gcpatch files are left out: they patch the published files, not these.
set -eu
d=$1; S=/ssd/shrink/days/$d; O=/ssd/shrink/sortpatch/sort/days/$d
ln -f $S/configurations.json $O/configurations.json
for v in planet/full planet/quality/uncapped planet/poi/all; do
  mkdir -p $O/$v
  for f in $S/$v/*; do
    b=$(basename $f)
    [ "$b" = patch.gcpatch ] && continue
    [ -e $O/$v/$b ] && continue
    ln $f $O/$v/$b
  done
done
cd $O && find . -type f | sort | while read f; do
  n=$(stat -c %h $f); s=$(stat -c %s $f)
  if [ "$n" -gt 1 ]; then echo "link $s $f"; else echo "NEW  $s $f"; fi
done
