#!/bin/bash
# Today's tools (v7) on the canonical sorted days: diff + apply (3 profiles)
# for both day pairs x three variants. A variant whose patch is byte-identical
# to the published one (the sort step left quality/uncapped and poi/all in
# today's order, same inodes) has the baseline step's apply numbers already.
# nohup bash run_v7.sh > logs/run_v7.log 2>&1 &
set -u
B=/ssd/shrink/sortpatch/patch
cd $B
echo "=== v7 on sorted days start $(date -u +%FT%TZ)"
for pair in "2026-10-12 2026-10-13" "2026-10-11 2026-10-12"; do
  set -- $pair
  for v in planet/quality/uncapped planet/poi/all planet/full; do
    echo "--- v7 $pair $v $(date +%T)"
    bash $B/diff_one.sh v7 $1 $2 $v
    O=$B/out/v7/$2/${v//\//_}
    if grep -q 'identical_to_published=yes' "$O/diff.result"; then
      echo "skip applies: patch identical to published (baseline step measured it)"; continue
    fi
    for prof in p8 p1 small; do
      if grep -q 'verify="PASS' "$O/apply-$prof.result" 2> /dev/null; then echo "kept: $(cat "$O/apply-$prof.result")"; continue; fi
      bash $B/apply_one.sh v7 $1 $2 $v $prof
    done
    df -h /ssd | tail -1
  done
done
echo "=== V7 DONE $(date -u +%FT%TZ)"
