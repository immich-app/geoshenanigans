#!/usr/bin/env bash
# Final latency run under the box's timing lock: waits for /ssd/shrink/sortpatch/TIMING.lock to
# be free, takes it (noclobber), runs run_all.sh with the given arguments, releases it.
#   timed_run.sh <out dir> <query tsv>...
set -uo pipefail
LOCK=/ssd/shrink/sortpatch/TIMING.lock
until (set -o noclobber; echo "search $(date -Iseconds)" > "$LOCK") 2>/dev/null; do sleep 10; done
trap 'rm -f "$LOCK"' EXIT
echo "lock taken $(date -Iseconds); load: $(cut -d' ' -f1-3 /proc/loadavg)"
/ssd/search/qset/run_all.sh "$@"
echo "done $(date -Iseconds); load: $(cut -d' ' -f1-3 /proc/loadavg)"
