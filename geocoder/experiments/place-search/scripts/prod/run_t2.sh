#!/usr/bin/env bash
/ssd/search-prod/scripts/timing2.sh > /ssd/search-prod/logs/timing2.log 2>&1
grep -q search-prod /ssd/shrink/sortpatch/TIMING.lock && rm -f /ssd/shrink/sortpatch/TIMING.lock
