#!/bin/bash
# Boundary tier variants: snap, today's-format dirs for grid_query, PB7 encodings + round trips,
# gc layout B of today's shipped tier + round trip, vertex error stats.
set -euo pipefail
T=/ssd/precision/tiers; B=/ssd/precision/build; DU=$B/dumps
X=$T/bin/tiers; GC=/ssd/shrink/sortpatch/codecs/bin/gc
W=$T/work/admin; mkdir -p $W $T/logs
export TMPDIR=/ssd/precision/tmp
SH=$B/out-nocap/planet/quality/uncapped
log(){ echo "== $(date +%T) $*"; }
log snap 1e-5
$X snap $DU/admin_dp.e7 $DU/admin_dp.e7.idx 100 $W/b.e7 $W/b.e7.idx
ln -sf $DU/admin_dp.e7 $W/c.e7; ln -sf $DU/admin_dp.e7.idx $W/c.e7.idx
ln -sf $DU/admin_raw.e7 $W/d.e7; ln -sf $DU/admin_raw.e7.idx $W/d.e7.idx
log admin-err
$X admin-err $SH/admin_polygons.bin $SH/admin_vertices.bin $DU/admin_dp.e7 $DU/admin_dp.e7.idx 100
for v in b c d; do
  log today-admin $v
  $X today-admin $W/$v.e7 $W/$v.e7.idx $SH/admin_polygons.bin $W/today_$v
done
for v in b c d; do
  for opt in "" "--single" "--nogcd" "--order2"; do
    tag=pb${opt#--}
    log pb-enc $v $opt
    $X pb-enc $W/$v.e7 $W/$v.e7.idx $SH/admin_polygons.bin $W/${v}.$tag.blocks $W/${v}.$tag.polys $opt
    $X pb-dec $W/${v}.$tag.blocks $W/${v}.$tag.polys $W/rt.e7 $W/rt.e7.idx
    if cmp $W/rt.e7 $W/$v.e7 && cmp $W/rt.e7.idx $W/$v.e7.idx; then echo "ROUNDTRIP $v $tag byte-identical"; else echo "ROUNDTRIP $v $tag FAILED"; fi
    rm -f $W/rt.e7 $W/rt.e7.idx
  done
done
log gc layout B of today
mkdir -p $W/a_src; ln -f $SH/admin_polygons.bin $SH/admin_vertices.bin $W/a_src/
$GC encode $W/a_src $W/a_gc --only admin
$GC decode $W/a_gc $W/a_dec --only admin
cmp $W/a_dec/admin_vertices.bin $SH/admin_vertices.bin && echo "ROUNDTRIP a layoutB byte-identical"
rm -rf $W/a_dec
log done
