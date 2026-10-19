#!/bin/bash
# arcs_regen.sh : re-generate the arcs track's best configuration (af37cb8 = flat 37.5 m + cap 0.1 +
# chunk bboxes of 8 refs) with verify5's build of the committed arcs tool (arcs-reader e566648,
# tools/arcs_study/arcs.cpp) from the arcs track's d1006 dump + arcdump, then compare byte for byte
# with /ssd/arcs/var/af37cb8 and measure raw / zstd 1.5.7 -18 -T1 sizes.
set -u
V=/ssd/verify5
A=/ssd/arcs
D=$A/build/d1006
B=$A/build/out-d1006/planet
O=$V/arcs/af37cb8
Z=/ssd/shrink/zstd157/bin/zstd
S="uni=37.5;cap=0.1;chunk=8"
export OMP_NUM_THREADS=32 TMPDIR=$V/tmp
rm -rf $O; mkdir -p $O/min $O/q
/usr/bin/time -f "%e s %M KB" -o $O/min.time nice -n 5 $V/bin/arcs minimal $D/dump $D/arcdump $B/admin $O/min "$S" > $O/min.json
/usr/bin/time -f "%e s %M KB" -o $O/q.time nice -n 5 $V/bin/arcs quality $D/dump $D/arcdump - $O/q "$S" > $O/q.json
for part in min q; do
  for sub in arc dec; do
    for f in $O/$part/$sub/*; do
      b=$(basename $f)
      cmp -s $f $A/var/af37cb8/$part/$sub/$b && r=IDENTICAL || r=DIFFERENT
      printf "%s\t%s/%s\t%s\t%s\n" $part $sub $b $(stat -c%s $f) $r
    done
  done
done > $V/cmp/arcs_regen_cmp.tsv
: > $V/cmp/arcs_regen_size.tsv
for f in min/arc/arc_vertices.bin min/arc/arc_index.bin min/arc/poly_arcs.bin min/arc/admin_polygons.bin min/dec/admin_cells.bin min/dec/admin_entries.bin \
         q/arc/arc_vertices.bin q/arc/arc_index.bin q/arc/poly_arcs.bin q/arc/admin_polygons.bin q/arc/postal_polygons.bin q/arc/poly_arcs_v.bin min/arc/poly_arcs_v.bin; do
  printf "%s\t%s\t%s\n" $f $(stat -c%s $O/$f) $($Z -18 -T1 -q -c $O/$f | wc -c) >> $V/cmp/arcs_regen_size.tsv &
done
wait
echo ARCS_REGEN_DONE
