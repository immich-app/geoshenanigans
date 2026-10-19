#!/bin/bash
# chain.sh : verify6 after the planet builds. Waits for PLANET_DONE, then the untimed work in
# parallel (tests6, rd_eq, pq), then the timed windows one after another, each under TIMING.lock
# (sb, rd_time, pt_time), then sizes6.
set -u
V=/ssd/verify6
export HOME=/root PATH=/root/.cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
S=$V/scripts
log() { echo "$(date -Is) $*"; }
until grep -q PLANET_DONE $V/logs/planet.log 2>/dev/null; do sleep 10; done
log PLANET_SEEN
$S/tests6.sh > $V/logs/tests6.log 2>&1 &
$S/rd_eq.sh > $V/logs/rd_eq.log 2>&1 &
JOBS=32 $S/pq.sh > $V/logs/pq.log 2>&1 &
wait
log UNTIMED_DONE
$S/sb.sh > $V/logs/sb.log 2>&1; log SB_EXIT
$S/rd_time.sh > $V/logs/rd_time.log 2>&1; log RD_TIME_EXIT
$S/pt_time.sh > $V/logs/pt_time.log 2>&1; log PT_TIME_EXIT
$S/sizes6.sh > $V/logs/sizes6.log 2>&1; log SIZES_EXIT
log CHAIN_DONE
