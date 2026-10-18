#!/bin/bash
# Whole-file zstd -18 -T16 of the change-1 files (max1 / withaddr1) and of today's counterparts,
# then remove the four-changes variant sets.
set -uo pipefail
V=/ssd/shrink/max/verify
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/shrink/tmp
cd $V
zs() { for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c) >> $V/work/zst_T16.tsv; done; }
S=fc/sets
for f in place_nodes.bin strings_core.bin admin_cells.bin admin_entries.bin admin_polygons.bin place_cells.bin place_entries.bin addr_points.bin; do
  zs $S/max1/$f /ssd/shrink/idx/max/$f
done
zs $S/withaddr1/admin_polygons.bin /ssd/shrink/idx/withaddr/admin_polygons.bin
rm -rf fc/sets work/p1only_max
echo DONE
