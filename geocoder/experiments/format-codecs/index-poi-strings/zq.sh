#!/bin/bash
# Whole-file zstd 1.5.7 -18 -T16 sizes (publish level; -T16 reproduced every published size
# byte-exactly, out/baseline_T16.tsv). Appends "label<TAB>path<TAB>raw<TAB>zst<TAB>secs" to
# out/sizes.tsv. One queue at a time (flock) so at most 16 compression threads run.
# usage: zq.sh label=path [label=path ...]
Z=/ssd/shrink/zstd157/bin/zstd
O=/ssd/shrink/max/index-poi-strings/out/sizes.tsv
exec 9> /ssd/shrink/max/index-poi-strings/tmp/zq.lock
flock 9
for a in "$@"; do
  label=${a%%=*}; p=${a#*=}
  t=$(date +%s.%N)
  s=$($Z -18 -T16 -q -c "$p" | wc -c)
  printf "%s\t%s\t%s\t%s\t%.0f\n" "$label" "$p" "$(stat -L -c %s "$p")" "$s" "$(echo "$(date +%s.%N) - $t" | bc)" >> $O
done
