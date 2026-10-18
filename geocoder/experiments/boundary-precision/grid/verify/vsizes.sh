#!/bin/bash
# Verifier: sizes + round trips for name=dir pairs.
#  - independent numpy L2-8 filter (filter28.py), cmp'd against gridvar filter
#  - layout Bg encoded by the stack reference (layouts.py), cmp'd against bgcodec encode
#  - Bg decoded by layouts.py AND by bgcodec, each cmp'd byte for byte with today's file
#  - zstd 1.5.7 -18 -T1 of today full / today L2-8 / Bg full / Bg L2-8 (done later by vzst.sh)
# usage: vsizes.sh name=dir ...   (sequential inside; run several instances for parallelism)
set -uo pipefail
export TMPDIR=/ssd/shrink/tmp
V=/ssd/shrink/grid/vfy
PY=/ssd/shrink/venv/bin/python
L=/ssd/shrink/stack/layouts.py
GV=/ssd/shrink/grid/gridvar
BC=/ssd/shrink/grid/bgcodec
for nd in "$@"; do
  name=${nd%%=*}; dir=${nd#*=}
  log=$V/logs/sizes_$name.log
  : > $log
  mkdir -p $V/l28/$name $V/l28g/$name
  echo "filter28: $($PY $V/filter28.py $dir $V/l28/$name)" >> $log
  echo "gridvar filter: $($GV filter $dir $V/l28g/$name 2 8)" >> $log
  for f in admin_polygons.bin admin_vertices.bin; do
    cmp -s $V/l28/$name/$f $V/l28g/$name/$f && echo "l28 $f filter28 == gridvar filter: IDENTICAL" >> $log || echo "l28 $f filter28 vs gridvar filter: DIFFERENT" >> $log
  done
  rm -rf $V/l28g/$name
  for scope in full l28; do
    if [ $scope = full ]; then sd=$dir; else sd=$V/l28/$name; fi
    av=$(realpath $sd/admin_vertices.bin)
    py=$V/bg/$name.$scope.Bg
    echo "$scope today: $av $(stat -c %s $av) B" >> $log
    $PY $L encode Bg $sd $py >> $log 2>&1
    $BC encode $sd $av $V/bg/$name.$scope.c >> $log 2>&1
    cmp -s $py $V/bg/$name.$scope.c && echo "$scope Bg: layouts.py encode == bgcodec encode: IDENTICAL" >> $log || echo "$scope Bg: layouts.py vs bgcodec encode: DIFFERENT" >> $log
    rm -f $V/bg/$name.$scope.c
    $PY $L decode Bg $sd $py $V/bg/$name.$scope.dec >> $log 2>&1
    cmp -s $V/bg/$name.$scope.dec $av && echo "$scope Bg: layouts.py decode == today: IDENTICAL" >> $log || echo "$scope Bg: layouts.py decode vs today: DIFFERENT" >> $log
    rm -f $V/bg/$name.$scope.dec
    $BC decode $sd $py $V/bg/$name.$scope.dec >> $log 2>&1
    cmp -s $V/bg/$name.$scope.dec $av && echo "$scope Bg: bgcodec decode == today: IDENTICAL" >> $log || echo "$scope Bg: bgcodec decode vs today: DIFFERENT" >> $log
    rm -f $V/bg/$name.$scope.dec
    echo "$scope Bg raw: $(stat -c %s $py) B" >> $log
  done
  echo "DONE $name" >> $log
done
