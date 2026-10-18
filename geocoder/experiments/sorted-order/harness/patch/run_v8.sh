#!/bin/bash
# The identity-matched v8 tools on the canonical sorted days: diff + apply
# (3 profiles) for both day pairs x three variants, same method as run_v7.sh.
# Every timing run takes TIMING.lock, so this can run beside run_v7.sh.
# nohup bash run_v8.sh > logs/run_v8.log 2>&1 < /dev/null &
set -u
B=/ssd/shrink/sortpatch/patch
cd $B
echo "=== v8 on sorted days start $(date -u +%FT%TZ)"
for pair in "2026-10-12 2026-10-13" "2026-10-11 2026-10-12"; do
  set -- $pair
  for v in planet/quality/uncapped planet/poi/all planet/full; do
    echo "--- v8 $pair $v $(date +%T)"
    O=$B/out/v8/$2/${v//\//_}
    [ -f "$O/diff.result" ] && grep -q "rc=0" "$O/diff.result" && echo "kept: $(cat $O/diff.result)" || bash $B/diff_one.sh v8 $1 $2 $v
    for prof in p8 p1 small; do
      if grep -q 'verify="PASS' "$O/apply-$prof.result" 2> /dev/null; then echo "kept: $(cat "$O/apply-$prof.result")"; continue; fi
      bash $B/apply_one.sh v8 $1 $2 $v $prof
    done
    df -h /ssd | tail -1
  done
done
echo "=== V8 DONE $(date -u +%FT%TZ)"
