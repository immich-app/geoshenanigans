#!/bin/bash
# sizes.sh <name>... : raw bytes and zstd 1.5.7 -18 -T1 bytes of each set's 4 admin-minimal admin files
# (smallest: admin_vertices, admin_polygons, admin_cells, admin_entries) and the quality dir's 4 files
# (recommended: admin_vertices, admin_polygons, postal_vertices, postal_polygons), files followed through
# the set's symlinks -> /ssd/verify5/cmp/size_<name>.tsv (part file raw zstd) + a total line per part.
V=/ssd/verify5
Z=/ssd/shrink/zstd157/bin/zstd
one() {
  local name=$1 part=$2 f=$3 st
  [ $part = min ] && st=smallest || st=recommended
  local p=$(readlink -f $V/sets/$name/$st/$f)
  printf "%s\t%s\t%s\t%s\n" $part $f $(stat -c%s $p) $($Z -18 -T1 -q -c $p | wc -c)
}
export -f one; export V Z
for name in "$@"; do
  for f in admin_vertices.bin admin_polygons.bin admin_cells.bin admin_entries.bin; do echo "$name min $f"; done
  for f in admin_vertices.bin admin_polygons.bin postal_vertices.bin postal_polygons.bin; do echo "$name q $f"; done
done | nice -n 19 xargs -P ${PAR:-6} -L 1 bash -c 'one $0 $1 $2 >> $V/tmp/size_$0.part'
for name in "$@"; do
  sort $V/tmp/size_$name.part > $V/cmp/size_$name.tsv; rm -f $V/tmp/size_$name.part
  awk -F'\t' -v n=$name '{r[$1]+=$3; z[$1]+=$4} END {printf "SIZE %s  min raw %.1f zstd %.1f MB | q raw %.1f zstd %.1f MB\n", n, r["min"]/1e6, z["min"]/1e6, r["q"]/1e6, z["q"]/1e6}' $V/cmp/size_$name.tsv
done
