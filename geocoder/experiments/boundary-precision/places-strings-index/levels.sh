#!/usr/bin/env bash
# zstd 1.5.7 level sweep on the six shipped files of this track (single thread each, 6 in parallel).
Z=/ssd/shrink/places-strings-index/zstd157/bin/zstd
cd /ssd/shrink/idx/smallest
for f in place_nodes.bin strings_core.bin admin_cells.bin admin_entries.bin place_cells.bin place_entries.bin; do
  ( echo "$f -18 $($Z -18 -T1 -c -q $f | wc -c) -19 $($Z -19 -T1 -c -q $f | wc -c) -22L $($Z --ultra -22 --long=27 -T1 -c -q $f | wc -c)" ) &
done
wait
