#!/bin/bash
# perf_run.sh MODE   (untimed CPU profile of planet/full; MODE sync: core 40, threads: cores 40-43)
# Writes /ssd/memory/perf/<MODE>.{data,txt}: perf report by shared object and symbol.
set -u
MODE=$1
M=/ssd/memory I=$M/inst/enc/2026-10-12 P=/ssd/shrink/sortpatch/patch/out/v8/2026-10-13
BIN=${BIN:-$M/bin/geocoder-patch-m2}
O=$M/perfout
mkdir -p $M/perf $M/tmp
rm -rf "$O"; mkdir -p "$O"
export TMPDIR=$M/tmp
case $MODE in
  sync) cores=40; args="" ;;
  threads) cores=40-43; args="--threads" ;;
esac
taskset -c $cores perf record -F 499 -o $M/perf/$MODE.data -- "$BIN" "$I/planet/full" "$P/planet_full/patch.gcpatch" -o "$O/" $args 2> $M/perf/$MODE.log
perf report -i $M/perf/$MODE.data --no-children --sort dso,sym --stdio 2> /dev/null | grep -v "^#" | grep -v "^$" | head -120 > $M/perf/$MODE.txt
perf report -i $M/perf/$MODE.data --no-children --sort comm,dso --stdio 2> /dev/null | grep -v "^#" | grep -v "^$" | head -30 > $M/perf/$MODE.dso.txt
rm -rf "$O"
echo "perf $MODE done"
