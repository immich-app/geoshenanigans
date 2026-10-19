#!/bin/bash
# planned.sh <name>... : the planned layout Bg size (tiers2 method, /ssd/shrink/grid/bgcodec) of each
# per-polygon set: Bg-encode admin_vertices (and postal_vertices for the quality dir) with the set's
# polygon file, decode + cmp round trip, zstd 1.5.7 -18 -T1 of the Bg file; planned = Bg zstd + zstd of
# the other files (from cmp/size_<name>.tsv, sizes.sh). -> cmp/bg_<name>.tsv and one PLANNED line.
V=/ssd/verify5
BG=/ssd/shrink/grid/bgcodec
Z=/ssd/shrink/zstd157/bin/zstd
one() {  # one <name> <part min|q> <vertex file> <polygon file>
  local name=$1 part=$2 st
  [ $part = min ] && st=smallest || st=recommended
  local d=$V/tmp/bg-$name-$part-$3; rm -rf $d; mkdir -p $d
  ln -s $(readlink -f $V/sets/$name/$st/$4) $d/admin_polygons.bin
  local verts=$(readlink -f $V/sets/$name/$st/$3)
  $BG encode $d $verts $d/v.bg > /dev/null 2>&1 && $BG decode $d $d/v.bg $d/v.dec > /dev/null 2>&1
  cmp -s $verts $d/v.dec && rt=IDENTICAL || rt=DIFFERENT
  printf "%s\t%s\t%s\t%s\t%s\n" $part $3 $(stat -c%s $d/v.bg) $($Z -18 -T1 -q -c $d/v.bg | wc -c) $rt
  rm -rf $d
}
export -f one; export V BG Z
for name in "$@"; do
  echo "$name min admin_vertices.bin admin_polygons.bin"
  echo "$name q admin_vertices.bin admin_polygons.bin"
  echo "$name q postal_vertices.bin postal_polygons.bin"
done | xargs -P ${PAR:-12} -L 1 bash -c 'one $0 $1 $2 $3 >> $V/tmp/bg_$0.part'
for name in "$@"; do
  sort $V/tmp/bg_$name.part > $V/cmp/bg_$name.tsv; rm -f $V/tmp/bg_$name.part
  awk -F'\t' -v n=$name 'FNR==NR {bg[$1" "$2]=$4; rt=rt" "$5; next} {k=$1" "$2; z[$1]+= (k in bg) ? bg[k] : $4}
     END {printf "PLANNED %s  min %.1f MB | q %.1f MB (round trips:%s)\n", n, z["min"]/1e6, z["q"]/1e6, rt}' \
     $V/cmp/bg_$name.tsv $V/cmp/size_$name.tsv
done
