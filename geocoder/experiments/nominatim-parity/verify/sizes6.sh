#!/bin/bash
# sizes6.sh : download size change base -> tip per preset: for each file of the preset (verify6's sets),
# raw bytes, and zstd -19 bytes of both versions where they differ (zstd 1.5.7, -T16). Prints per
# preset raw and zstd -19 delta in bytes. Niced.
set -u
V=/ssd/verify6
Z=/ssd/shrink/zstd157/bin/zstd
for s in smallest recommended max; do
  dr=0; dz=0; nd=0
  for f in $(ls $V/sets/tip/$s); do
    a=$(readlink -f $V/sets/tip/$s/$f); b=$(readlink -f $V/sets/base/$s/$f)
    ra=$(stat -c %s $a); rb=$(stat -c %s $b)
    dr=$((dr + ra - rb))
    if ! cmp -s $a $b; then
      za=$(nice $Z -19 -T16 -q -c $a | wc -c); zb=$(nice $Z -19 -T16 -q -c $b | wc -c)
      dz=$((dz + za - zb)); nd=$((nd + 1))
      echo "$s $f raw $rb -> $ra zst $zb -> $za"
    fi
  done
  echo "SIZE $s files_differing=$nd raw_delta=$dr zst19_delta=$dz"
done
echo SIZES_DONE
