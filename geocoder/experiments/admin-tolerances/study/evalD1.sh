#!/bin/bash
# evalD1.sh "<name>|<minimal scheme>[|<quality scheme>]"... : the schemes on the newer planet (D1 = planet-261018b,
# fix/admin-nesting + cap, warm build): pools from D1's own data (borders2 on dumpD1: 20k points per
# level L2..L8, photo-like 100k from the D1 build, the bugs2 enclave grids, the loc-to-city checks),
# truth = D1unc (admin-minimal L2-8 uncapped / quality uncapped), reader = fix/admin-nesting
# (bin/grid_query.nest). Per scheme: mksetD1.sh, answers, sizes (cmp/size_D1<name>.tsv), score3.py.
set -u
T=/ssd/tiers3
cd $T
GQ=$T/bin/grid_query.nest
P=$T/rptsD1
Z=/ssd/shrink/zstd157/bin/zstd
if [ ! -s $P/photo.txt ]; then
  mkdir -p $P
  $T/bin/borders2 $T/dumpD1 20000 3201 2 8 > $T/pts/bordD1.txt 2> $T/logs/bordD1.log
  for l in 2 3 4 5 6 7 8; do awk -v l=$l '$3==l' $T/pts/bordD1.txt > $P/B$l.txt; done
  /ssd/shrink/venv/bin/python -I photo2.py $T/build/out-D1/planet 100000 3202 > $P/photo.txt
  cut -d' ' -f2,3 /ssd/bugs2/grids/all.txt > $P/grids.txt
  cp $T/rpts/l2c.txt $P/l2c.txt
fi
if [ ! -f q/D1unc_recommended/photo.txt ]; then
  ./mksetD1.sh unc none > /dev/null
  for s in smallest recommended; do PTS=$P JOBS=16 ./runq.sh D1unc_$s setsD1/unc/$s $GQ; done
fi
for item in "$@"; do
  IFS='|' read -r name scheme qscheme <<< "$item"
  if [ -n "$qscheme" ]; then ./mksetD1.sh $name "$scheme" "$qscheme" > /dev/null; else ./mksetD1.sh $name "$scheme" > /dev/null; fi
  for s in smallest recommended; do PTS=$P JOBS=12 ./runq.sh D1${name}_$s setsD1/$name/$s $GQ; done
  : > cmp/size_D1$name.tsv
  for f in min/admin_vertices.bin min/admin_polygons.bin min/admin_cells.bin min/admin_entries.bin \
           q/admin_vertices.bin q/admin_polygons.bin q/postal_vertices.bin q/postal_polygons.bin; do
    p=varD1/$name/$f
    printf "%s\t%s\t%s\n" "$f" "$(stat -c%s $p)" "$($Z -18 -T1 -q -c $p | wc -c)" >> cmp/size_D1$name.tsv &
  done
  wait
  /ssd/shrink/venv/bin/python -I score3.py --truth D1unc --grids /ssd/bugs2/grids --meta /nonexistent D1$name
done
echo EVALD1_DONE
