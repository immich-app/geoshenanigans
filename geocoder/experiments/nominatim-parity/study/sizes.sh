#!/bin/bash
# sizes.sh <before sets dir> <after sets dir> : per preset and file, raw bytes before/after and,
# for files whose bytes differ, zstd -19 sizes (zstd 1.5.7, -T16) -> TSV on stdout:
# set file raw_before raw_after zst_before zst_after (zst columns "same" when identical)
set -euo pipefail
B=$1
A=$2
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/parity/tmp
printf 'set\tfile\traw_before\traw_after\tzst_before\tzst_after\n'
for s in smallest recommended max; do
  for f in $(ls "$A/$s"); do
    a=$(readlink -f "$A/$s/$f")
    b=$(readlink -f "$B/$s/$f")
    ra=$(stat -c %s "$a")
    rb=$(stat -c %s "$b")
    if cmp -s "$a" "$b"; then
      printf '%s\t%s\t%s\t%s\tsame\tsame\n' "$s" "$f" "$rb" "$ra"
    else
      za=$($Z -19 -T16 -q -c "$a" | wc -c)
      zb=$($Z -19 -T16 -q -c "$b" | wc -c)
      printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$s" "$f" "$rb" "$ra" "$zb" "$za"
    fi
  done
done
