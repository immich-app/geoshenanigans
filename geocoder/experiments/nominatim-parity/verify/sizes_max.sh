#!/bin/bash
# sizes_max.sh : the max preset of sizes6.sh in parallel for the files sizes6.sh had not reached
# (same method: raw bytes, zstd 1.5.7 -19 of both versions where they differ).
V=/ssd/verify6
Z=/ssd/shrink/zstd157/bin/zstd
done_files=$(grep "^max " $V/logs/sizes6.log | cut -d" " -f2)
for f in $(ls $V/sets/tip/max); do
  echo "$done_files" | grep -qx "$f" && continue
  echo $f
done | xargs -P 8 -I{} bash -c "
  a=\$(readlink -f $V/sets/tip/max/{}); b=\$(readlink -f $V/sets/base/max/{})
  ra=\$(stat -c %s \$a); rb=\$(stat -c %s \$b)
  if cmp -s \$a \$b; then echo \"max {} raw \$rb -> \$ra same\"; else
  za=\$(nice $Z -19 -T8 -q -c \$a | wc -c); zb=\$(nice $Z -19 -T8 -q -c \$b | wc -c); echo \"max {} raw \$rb -> \$ra zst \$zb -> \$za\"; fi" > $V/logs/sizes_max.log
( grep "^max " $V/logs/sizes6.log; cat $V/logs/sizes_max.log ) | awk "{r+=\$6-\$4; if (\$7==\"zst\") {z+=\$10-\$8; n++}} END {print \"SIZE max files_differing=\" n \" raw_delta=\" r \" zst19_delta=\" z}" >> $V/logs/sizes_max.log
echo SIZES_MAX_DONE >> $V/logs/sizes_max.log
