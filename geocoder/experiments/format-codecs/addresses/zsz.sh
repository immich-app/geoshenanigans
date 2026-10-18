#!/bin/bash
# zsz.sh <file>... : zstd 1.5.7 -18 -T16 size of each file (bytes), streamed, nothing kept
Z=/ssd/shrink/zstd157/bin/zstd
for f in "$@"; do
  printf "%s\t%s\t%s\n" "$(basename $f)" "$(stat -L -c %s $f)" "$($Z -18 -T${T:-16} -q -c "$f" | wc -c)"
done
