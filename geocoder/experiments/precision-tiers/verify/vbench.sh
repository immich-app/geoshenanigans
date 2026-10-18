#!/bin/bash
P=/ssd/precision; S=$P/streets
until (set -o noclobber; echo "verify-bench" > $P/BUILD.lock) 2> /dev/null; do sleep 15; done
trap "rm -f $P/BUILD.lock" EXIT
uptime > logs/bench_uptime.txt
$S/bin/bench $S/work/enc $S/sorted warm 2000000 101 > logs/bench_warm.log 2>&1
uptime >> logs/bench_uptime.txt
$S/bin/bench $S/work/enc $S/sorted cold 3000 103 > logs/bench_cold.log 2>&1
uptime >> logs/bench_uptime.txt
echo BENCH_DONE >> logs/bench_cold.log
