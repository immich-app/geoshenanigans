#!/bin/bash
# Round 4: the baseline small-device run again (the first one shared its page cache with an untimed
# smoke run of mine for quality / poi). Waits for round 3.
cd /ssd/memory
while pgrep -f "scripts/queue3.sh" > /dev/null; do sleep 20; done
for spec in "smallc base rerun"; do
  echo "=== $(date +%T) $spec"
  bash scripts/mrun.sh $spec
done
echo "=== $(date +%T) done"
