#!/usr/bin/env bash
# Stage 2: admin index rebuild bench (with fallback), more variants, per-subset answer diffs, option sizes.
set -uo pipefail
cd /ssd/shrink/places-strings-index
export TMPDIR=/ssd/shrink/tmp
PY=/ssd/shrink/venv/bin/python
./run_s2_bench.sh > work/s2_bench.txt 2>&1 &
S2=$!
$PY -u admin_dedup_inplace.py > work/admin_dedup.txt 2>&1
$PY -u slot_stable.py > work/slot_stable.txt 2>&1
$PY -u options.py > work/options.txt 2>&1 &
wait $S2
$PY -u make_variants2.py > work/make_variants2.txt 2>&1
$PY -u compare2.py V1 V3 V4 V5 V6 V7 V9 V10 V11 V12 > work/compare2.txt 2>&1
wait
echo STAGE2 DONE > work/stage2.done
