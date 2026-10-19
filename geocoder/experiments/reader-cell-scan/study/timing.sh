#!/usr/bin/env bash
# timing.sh: reverse-geocode latency of main (f48ddc4), pip2 (8dbb58d), head (66a5ad0) and fix3
# (perf/reader-cell-scan 90b1292; fix2 = 382dc69 on the small sets for attribution), under
# /ssd/shrink/sortpatch/TIMING.lock. rg_bench, single thread, core 2, nice -20, readers
# interleaved per (set, pool, mode):
#   warm: one untimed pass, 3 timed passes, fastest per point
#   cold: the set's files evicted from the page cache (posix_fadvise DONTNEED on every file the
#         set dir links to), then one timed pass, each point once
# Modes: admin (Index::find_admin alone) and query (MultiIndex::query_json, the server's answer).
# Sets: planet-261018b base build (q2.5 scheme: smallest, recommended, max) and tiers2
# uni75t8rc10 (flat 75 m, L8 50 m + ring cap: smallest, recommended). Pools: photo-like and
# +-200 m of L2/L4/L8 borders, 10k each (5k for max).
set -uo pipefail
R=/ssd/reader
L=/ssd/shrink/sortpatch/TIMING.lock
O=$R/timing
mkdir -p $O
until (set -o noclobber; echo "reader timing $(date -Is)" > $L) 2> /dev/null; do sleep 30; done
trap 'grep -q "^reader" $L && rm -f $L' EXIT
echo "LOCKED $(date -Is)"
evict() { /ssd/shrink/venv/bin/python -I -c '
import os, sys
d = sys.argv[1]
for f in sorted(os.listdir(d)):
    p = os.path.realpath(os.path.join(d, f))
    fd = os.open(p, os.O_RDONLY)
    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    os.close(fd)
' "$1"; }
run() { # set dir pool readers...
  local set=$1 dir=$2 pool=$3; shift 3
  for mode in admin query; do
    for r in "$@"; do
      nice -n -20 taskset -c 2 $R/bin/rg_bench.$r $dir $mode 3 < $R/pts/$pool.txt > $O/$set.$pool.$mode.warm.$r.json 2> $O/$set.$pool.$mode.warm.$r.err
    done
    for r in "$@"; do
      sync; evict $dir; sleep 1
      nice -n -20 taskset -c 2 $R/bin/rg_bench.$r $dir $mode 0 < $R/pts/$pool.txt > $O/$set.$pool.$mode.cold.$r.json 2> $O/$set.$pool.$mode.cold.$r.err
    done
    echo "$(date -Is) $set $pool $mode"
  done
}
for pool in photo10k border10k; do
  run b-smallest /ssd/bugs2/sets/base/smallest $pool main pip2 head fix2 fix3
  run b-recommended /ssd/bugs2/sets/base/recommended $pool main pip2 head fix2 fix3
  run u-smallest /ssd/tiers2/sets/uni75t8rc10/smallest $pool main pip2 head fix3
  run u-recommended /ssd/tiers2/sets/uni75t8rc10/recommended $pool main pip2 head fix3
done
for pool in photo5k border5k; do
  run b-max /ssd/bugs2/sets/base/max $pool main pip2 head fix3
done
echo "TIMING_DONE $(date -Is)"
