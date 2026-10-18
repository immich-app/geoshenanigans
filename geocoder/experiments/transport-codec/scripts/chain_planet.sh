#!/bin/bash
# After planet/best.json exists: best config on the published order (whole + 4 MiB kept for the
# decode timing), then the sorted order (the 13 files canon_sort rewrites): plain zstd, as described,
# column search, best whole + 4 MiB. Unchanged files reuse the published-order runs.
set -eu
cd /ssd/transport
export TMPDIR=/ssd/transport/tmp
P=/ssd/shrink/venv/bin/python
S13=addr_entries.bin,addr_points.bin,addr_postcodes.bin,addr_vertices.bin,interp_entries.bin,interp_nodes.bin,interp_postcodes.bin,interp_ways.bin,street_entries.bin,street_nodes.bin,street_ways.bin,way_parents.bin,way_postcodes.bin
until [ -s planet/best.json ]; do sleep 30; done
echo "$(date +%T) best whole"
$P -I scripts/run_cfg.py /ssd/shrink/idx/max planet/best_whole planet/best.json 0 --jobs 16 > logs/planet_best_whole.log 2>&1
echo "$(date +%T) best 4m"
$P -I scripts/run_cfg.py /ssd/shrink/idx/max planet/best_4m planet/best.json 4194304 --jobs 16 --keep > logs/planet_best_4m.log 2>&1
echo "$(date +%T) sorted plain"
$P -I scripts/run_cfg.py planet/sorted_flat planet/s_plain13 raw 0 --jobs 16 --files $S13 > logs/planet_s_plain.log 2>&1
echo "$(date +%T) sorted theirs"
$P -I scripts/run_cfg.py planet/sorted_flat planet/s_theirs13 theirs 0 --jobs 16 --files $S13 > logs/planet_s_theirs.log 2>&1
echo "$(date +%T) sorted colsearch"
$P -I scripts/colsearch.py planet/sorted_flat planet/scols planet/best_sorted13.json --jobs 40 --files $S13 > logs/planet_s_colsearch.log 2>&1
$P -I scripts/merge_results.py planet/best_sorted.json planet/best.json planet/best_sorted13.json
echo "$(date +%T) sorted best whole"
$P -I scripts/run_cfg.py planet/sorted_flat planet/s_best13 planet/best_sorted.json 0 --jobs 16 --files $S13 > logs/planet_s_best.log 2>&1
echo "$(date +%T) sorted best 4m"
$P -I scripts/run_cfg.py planet/sorted_flat planet/s_best4m13 planet/best_sorted.json 4194304 --jobs 16 --files $S13 > logs/planet_s_best4m.log 2>&1
echo "$(date +%T) sorted theirs 4m"
$P -I scripts/run_cfg.py planet/sorted_flat planet/s_theirs4m13 theirs 4194304 --jobs 16 --files $S13 > logs/planet_s_theirs4m.log 2>&1
echo "$(date +%T) CHAIN DONE"
