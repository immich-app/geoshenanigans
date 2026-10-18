#!/bin/bash
# After run_v8.sh: (1) the v8 tools on the PUBLISHED stable-slot days (v8s,
# slot identity), cold diff + 1-core apply, to separate the format's effect
# from the canonical order's; (2) re-run every timing whose SMT siblings were
# busy (rerun_busy.sh).
# nohup bash run_after.sh > logs/run_after.log 2>&1 < /dev/null &
set -u
B=/ssd/shrink/sortpatch/patch
cd $B
until grep -q "=== V8 DONE" logs/run_v8.log; do sleep 30; done
echo "=== after start $(date -u +%FT%TZ)"
for pair in "2026-10-12 2026-10-13" "2026-10-11 2026-10-12"; do
  set -- $pair
  O=$B/out/v8s/$2/planet_full
  echo "--- v8s $pair $(date +%T)"
  grep -q "rc=0" "$O/diff.result" 2> /dev/null || bash $B/diff_one.sh v8s $1 $2 planet/full
  grep -q 'verify="PASS' "$O/apply-p1.result" 2> /dev/null || bash $B/apply_one.sh v8s $1 $2 planet/full p1
done
bash $B/rerun_busy.sh
echo "=== AFTER DONE $(date -u +%FT%TZ)"
