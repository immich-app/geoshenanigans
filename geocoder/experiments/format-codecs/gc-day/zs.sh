#!/bin/bash
# zs.sh FILE... : path, raw bytes, zstd 1.5.7 -18 -T16 whole-file bytes (one line per file), the
# same measurement as the earlier tracks' zst_T16.tsv. Run it pinned to 16 cores.
Z=/ssd/shrink/zstd157/bin/zstd
for f; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c); done
