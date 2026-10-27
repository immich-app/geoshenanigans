#!/bin/bash
# Timed runs, round 1: the in-process patcher on all profiles, then the baseline under the same harness.
cd /ssd/memory
for spec in "p1c new" "smallc new" "p8w new" "p8w newpar" "smallc base" "p8w basepar"; do
  echo "=== $(date +%T) $spec"
  bash scripts/mrun.sh $spec
done
echo "=== $(date +%T) done"
