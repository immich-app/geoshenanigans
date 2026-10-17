#!/bin/bash
# Round 3: the small device with codec threads (overlap of IO waits on one core). Waits for round 2.
cd /ssd/memory
while pgrep -f "scripts/queue2.sh" > /dev/null; do sleep 20; done
for spec in "smallc new2t"; do
  echo "=== $(date +%T) $spec"
  bash scripts/mrun.sh $spec
done
echo "=== $(date +%T) done"
