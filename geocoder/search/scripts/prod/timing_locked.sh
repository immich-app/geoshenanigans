#!/usr/bin/env bash
/ssd/search-prod/scripts/timing.sh > /ssd/search-prod/logs/timing.log 2>&1
grep -q search-prod /ssd/shrink/sortpatch/TIMING.lock && rm -f /ssd/shrink/sortpatch/TIMING.lock
