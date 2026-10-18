#!/bin/bash
# Baseline: diff + apply (3 profiles) for both day pairs x three variants.
# nohup bash run_all.sh > run_all.log 2>&1 &
set -u
B=/ssd/shrink/sortpatch/baseline
cd $B
echo "=== baseline start $(date -u +%FT%TZ)"
for pair in "2026-10-12 2026-10-13" "2026-10-11 2026-10-12"; do
  for v in planet/quality/uncapped planet/poi/all planet/full; do
    echo "--- $pair $v $(date +%T)"
    bash $B/diff_one.sh $pair $v
    set -- $pair
    for prof in p8 p1 small; do
      # Applies use the published patch and the days dirs; keep a passing one.
      if grep -q 'verify="PASS' "$B/out/$2/${v//\//_}/apply-$prof.result" 2> /dev/null; then
        echo "kept: $(cat "$B/out/$2/${v//\//_}/apply-$prof.result")"; continue
      fi
      bash $B/apply_one.sh $pair $v $prof
    done
    df -h /ssd | tail -1
  done
done
echo "=== BASELINE DONE $(date -u +%FT%TZ)"
