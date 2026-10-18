#!/usr/bin/env bash
# Stage 1: stats + encodings (zstd 1.5.7 -18, = compress_index.py), then variants + grid_query comparisons.
set -uo pipefail
cd /ssd/shrink/places-strings-index
export TMPDIR=/ssd/shrink/tmp
PY=/ssd/shrink/venv/bin/python
$PY -u place_encode.py > work/place_encode.txt 2>&1 &
$PY -u index_encode.py > work/index_encode.txt 2>&1 &
$PY -u strings_stats.py > work/strings_stats.txt 2>&1
$PY -u make_variants.py > work/make_variants.txt 2>&1
$PY -u compare_variants.py V1 V2 V3 V4 V5 V6 > work/compare_variants.txt 2>&1
wait
echo STAGE1 DONE > work/stage1.done
