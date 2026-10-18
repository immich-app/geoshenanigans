#!/bin/bash
# runq.sh <label> <set dir> : grid_query answers for every pool in /ssd/tiers2/pts on one flat set,
# chunks of 25k points, JOBS (default 16) processes at a time -> /ssd/tiers2/q/<label>/<pool>.txt.
# CHAIN=1 runs the chain reader mode (GC_CHAIN_ADMIN=1, candidate g). POOLS limits the pools.
# grid_query: /ssd/tiers2/target (tiers2-chain-reader = fix/admin-holes + the env-gated chain mode).
set -euo pipefail
label=$1
dir=$2
GQ=${GQ:-/ssd/tiers2/target/release/examples/grid_query}
Q=/ssd/tiers2/q/$label
C=/ssd/tiers2/q/chunks
mkdir -p "$Q" "$C"
export TMPDIR=/ssd/tiers2/tmp
pools=${POOLS:-$(ls /ssd/tiers2/pts | sed 's/\.txt$//')}
for p in $pools; do
  [ -f "$C/$p.done" ] && continue
  split -d -l 25000 -a 3 "/ssd/tiers2/pts/$p.txt" "$C/$p."
  touch "$C/$p.done"
done
jobs=()
for p in $pools; do for c in $(cd $C && ls $p.[0-9][0-9][0-9]); do jobs+=("$c"); done; done
export GC_CHAIN_ADMIN=${CHAIN:-0}
printf '%s\n' "${jobs[@]}" | xargs -P "${JOBS:-16}" -I{} sh -c "$GQ $dir < $C/{} > $Q/{}.part 2> $Q/{}.err"
for p in $pools; do
  cat $(ls $Q/$p.[0-9][0-9][0-9].part | sort) > "$Q/$p.txt"
  rm -f $Q/$p.[0-9][0-9][0-9].part $Q/$p.[0-9][0-9][0-9].err
  [ "$(wc -l < "$Q/$p.txt")" = "$(wc -l < /ssd/tiers2/pts/$p.txt)" ] || echo "SHORT $label $p"
done
echo "RUNQ_DONE $label"
