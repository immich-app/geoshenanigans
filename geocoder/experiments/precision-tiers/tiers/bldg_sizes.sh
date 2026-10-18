#!/bin/bash
# zstd -18 sizes of the building encodings: AB7 blobs (+ i2 / i1 indexes) for both grids and the four
# layouts, and gc abs2 + addr_points.col of today's capped (base) and uncapped (nocap) outlines, with
# the col file split into the poly_offset planes (the last 4 N bytes) and the rest.
set -euo pipefail
T=/ssd/precision/tiers; W=$T/work/bldg
cd $T
for s in base nocap; do
  col=$W/gc_$s/addr_points.col
  n=$(( $(stat -c %s $col) / 24 ))
  head -c $(( 20 * n )) $col > $W/gc_$s/col_without_vo.bin
  tail -c $(( 4 * n )) $col > $W/gc_$s/col_vo.bin
done
./zs.sh $W/gc_base/addr_vertices.abs2 $W/gc_base/addr_points.col $W/gc_base/col_without_vo.bin $W/gc_base/col_vo.bin \
        $W/gc_nocap/addr_vertices.abs2 $W/gc_nocap/addr_points.col $W/gc_nocap/col_vo.bin \
        /ssd/precision/build/out-base/planet/full/addr_vertices.bin /ssd/precision/build/out-nocap/planet/full/addr_vertices.bin
for t in d10 d10planes d10delta d10planesdelta d1 d1planes d1delta d1planesdelta; do ./zs.sh $W/$t.blobs; done
./zs.sh $W/d10.i2 $W/d10.i1
