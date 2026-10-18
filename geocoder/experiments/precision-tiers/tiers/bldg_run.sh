#!/bin/bash
# Building tier variants: AB7 encodings (+ full decode / index verification), gc abs2 + col of
# today's capped and uncapped outlines (+ byte-exact round trips), today's-format Maximum for grid_query.
set -euo pipefail
T=/ssd/precision/tiers; B=/ssd/precision/build; DU=$B/dumps
X=$T/bin/tiers; GC=/ssd/shrink/sortpatch/codecs/bin/gc
W=$T/work/bldg; mkdir -p $W
export TMPDIR=/ssd/precision/tmp
log(){ echo "== $(date +%T) $*"; }
PT=$DU/addr_points.e7; V=$DU/addr_vertices.e7; VI=$DU/addr_vertices.e7.idx
for div in 10 1; do
  for opt in "" "--planes" "--delta" "--planes --delta"; do
    tag=d$div$(echo "$opt" | tr -d ' -')
    log ab-enc $tag
    $X ab-enc $PT $V $VI $div $W/$tag $opt
    $X ab-verify $PT $V $VI $div $W/$tag $opt
  done
done
log today-bldg 1e-6
$X today-bldg $B/out-nocap/planet/full/addr_points.bin $V $VI 10 $W/today_b
for s in base nocap; do
  log gc addr $s
  mkdir -p $W/gc_src_$s
  ln -f $B/out-$s/planet/full/addr_points.bin $B/out-$s/planet/full/addr_vertices.bin $W/gc_src_$s/
  $GC encode $W/gc_src_$s $W/gc_$s --only addr
  $GC decode $W/gc_$s $W/gc_dec_$s --only addr
  for f in addr_points.bin addr_vertices.bin; do
    cmp $W/gc_dec_$s/$f $B/out-$s/planet/full/$f && echo "ROUNDTRIP gc $s $f byte-identical"
  done
  rm -rf $W/gc_dec_$s
done
log done
