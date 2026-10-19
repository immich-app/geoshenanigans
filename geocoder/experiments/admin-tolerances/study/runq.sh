#!/bin/bash
# runq.sh <label> <set dir> [grid_query] : answers for every pool in $PTS (default /ssd/tiers3/rpts)
# on one flat set, 20k-point chunks, JOBS (default 16) processes -> /ssd/tiers3/q/<label>/<pool>.txt.
# Default reader: tiers2's grid_query (fix/admin-holes 97c732c + env-gated chain mode, run plain:
# GC_CHAIN_ADMIN=0), the reader the tiers2 verify numbers came from. POOLS limits the pools.
set -euo pipefail
label=$1
dir=$2
GQ=${3:-/ssd/tiers2/target/release/examples/grid_query}
PTS=${PTS:-/ssd/tiers3/rpts}
Q=/ssd/tiers3/q/$label
C=$PTS/chunks
mkdir -p "$Q" "$C"
export TMPDIR=/ssd/tiers3/tmp GC_CHAIN_ADMIN=0
pools=${POOLS:-$(cd $PTS && ls *.txt | sed 's/\.txt$//')}
for p in $pools; do
  [ -f "$C/$p.done" ] && continue
  split -d -l 20000 -a 3 "$PTS/$p.txt" "$C/$p."
  touch "$C/$p.done"
done
jobs=()
for p in $pools; do for c in $(cd $C && ls $p.[0-9][0-9][0-9]); do jobs+=("$c"); done; done
printf '%s\n' "${jobs[@]}" | xargs -P "${JOBS:-16}" -I{} sh -c "$GQ $dir < $C/{} > $Q/{}.part 2> $Q/{}.err"
for p in $pools; do
  cat $(ls $Q/$p.[0-9][0-9][0-9].part | sort) > "$Q/$p.txt"
  rm -f $Q/$p.[0-9][0-9][0-9].part $Q/$p.[0-9][0-9][0-9].err
  [ "$(wc -l < "$Q/$p.txt")" = "$(wc -l < $PTS/$p.txt)" ] || echo "SHORT $label $p"
done
echo "RUNQ_DONE $label"
