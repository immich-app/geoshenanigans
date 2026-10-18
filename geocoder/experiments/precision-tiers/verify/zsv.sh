#!/bin/bash
# zsv.sh LIST OUT: for each file in LIST: path, raw bytes, zstd 1.5.7 -18 -T16 whole-file bytes (4 at a time)
Z=/ssd/shrink/zstd157/bin/zstd
one() { printf "%s\t%s\t%s\n" "$1" $(stat -L -c %s "$1") $($Z -18 -T16 -q -c "$1" | wc -c); }
export -f one; export Z
xargs -P 4 -I{} bash -c "one {}" < "$1" > "$2"
echo ZS_DONE >> "$2.done"
