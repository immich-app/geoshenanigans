#!/bin/bash
# After run_all.sh: redo 10-12 -> 10-13 planet/full applies against a private
# copy of the old dir (the first ones shared page cache with another step).
B=/ssd/shrink/sortpatch/baseline
until grep -q "BASELINE DONE" $B/run_all.log; do sleep 30; done
for prof in p8 p1 small; do
  bash $B/apply_one.sh 2026-10-12 2026-10-13 planet/full $prof
done
echo "=== RERUN DONE $(date -u +%FT%TZ)"
