#!/bin/bash
# perfam.sh: per-family warm runs on 1 core (one gc process per family, so its maxrss is that family's
# peak memory): encode from the sorted 10-13 day, decode from enc/2026-10-13. Phase lines -> log.
set -u
C=/ssd/shrink/sortpatch/codecs
S=/ssd/shrink/sortpatch/sort/days/2026-10-13/planet
E=$C/enc/2026-10-13/planet
F=$C/f
export TMPDIR=/ssd/shrink/tmp
var() { case $1 in admin) echo quality/uncapped ;; poi) echo poi/all ;; *) echo full ;; esac; }
for fam in streets interps way_parents way_postcodes interp_postcodes gxc addr admin poi; do
  v=$(var $fam)
  rm -rf $F; mkdir -p $F/e $F/d
  echo "## encode $fam"
  taskset -c 63 $C/bin/gc-encode $S/$v $F/e --only $fam 2>&1 | grep -E "^phase ($fam|total)"
  echo "## decode $fam"
  taskset -c 63 $C/bin/gc-decode $E/$v $F/d --only $fam 2>&1 | grep -E "^phase ($fam|total)"
done
rm -rf $F
