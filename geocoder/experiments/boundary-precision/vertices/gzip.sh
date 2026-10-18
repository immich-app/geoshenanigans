#!/bin/bash
# gzip -9 size (what a default Docker layer push uses) of the main candidates.
cd "$(dirname "$0")"
for f in orig e3c_wrapdelta_shuffle e2_varint_split_open e1_varint_inter_open "$@"; do
  echo "$f $(gzip -9 -c enc/$f.bin | wc -c)" &
done
wait
