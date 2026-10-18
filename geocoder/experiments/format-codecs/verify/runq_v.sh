#!/bin/bash
# runq_v.sh <name> <dir> [baseline-name] : grid_query the fresh 20k verifier points against <dir>
# -> q/<name>.jsonl; if a baseline name is given, compare full JSON line by line and log the count.
set -uo pipefail
cd /ssd/shrink/max/verify
GQ=/ssd/shrink/target/release/examples/grid_query
n=$1; d=$2; b=${3:-}
t0=$(date +%s)
$GQ "$d" < pts_v20k.txt > q/$n.jsonl 2> q/$n.err
rc=$?
lines=$(wc -l < q/$n.jsonl)
msg="$n rc=$rc lines=$lines secs=$(( $(date +%s) - t0 )) dir=$d"
if [ -n "$b" ]; then
  if cmp -s q/$b.jsonl q/$n.jsonl; then msg="$msg | vs $b: IDENTICAL (0 diffs)"
  else msg="$msg | vs $b: $(diff <(cat -n q/$b.jsonl) <(cat -n q/$n.jsonl) | grep -c '^<') lines differ"; fi
fi
echo "$msg" | tee -a logs/runq_v.log
