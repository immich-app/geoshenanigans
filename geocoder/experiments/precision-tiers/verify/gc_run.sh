#!/bin/bash
GC=/ssd/shrink/sortpatch/codecs/bin/gc; B=/ssd/precision/build; cd /ssd/precision/verify/gc
$GC encode a_src a_gc --only admin && $GC decode a_gc a_dec --only admin && cmp a_dec/admin_vertices.bin a_src/admin_vertices.bin && cmp a_dec/admin_polygons.bin a_src/admin_polygons.bin && echo "ROUNDTRIP admin layoutB byte-identical"
rm -rf a_dec
$GC encode base_src gc_base --only addr && $GC decode gc_base gc_dec --only addr && cmp gc_dec/addr_vertices.bin base_src/addr_vertices.bin && cmp gc_dec/addr_points.bin base_src/addr_points.bin && echo "ROUNDTRIP addr abs2 byte-identical"
rm -rf gc_dec
col=gc_base/addr_points.col; n=$(( $(stat -c %s $col) / 24 )); tail -c $(( 4 * n )) $col > gc_base/col_vo.bin
echo GC_DONE
