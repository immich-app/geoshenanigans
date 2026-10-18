#!/usr/bin/env bash
until (set -o noclobber; echo search-prod > /ssd/shrink/sortpatch/TIMING.lock) 2>/dev/null; do sleep 30; done
/ssd/search-prod/scripts/timing3.sh > /ssd/search-prod/logs/timing3.log 2>&1
grep -q search-prod /ssd/shrink/sortpatch/TIMING.lock && rm -f /ssd/shrink/sortpatch/TIMING.lock
