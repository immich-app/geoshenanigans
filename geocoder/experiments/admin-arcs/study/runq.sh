#!/bin/bash
# runq.sh <label> <set dir> : grid_query answers for every pool in /ssd/arcs/pts on one flat set,
# chunks of 25k points, JOBS (default 16) processes -> /ssd/arcs/q/<label>/<pool>.txt.
# GQ selects the reader (default: fix/admin-nesting's grid_query in /ssd/arcs/target).
set -euo pipefail
label=$1
dir=$2
GQ=${GQ:-/ssd/arcs/target/release/examples/grid_query}
Q=/ssd/arcs/q/$label
C=/ssd/arcs/q/chunks
mkdir -p "$Q" "$C"
export TMPDIR=/ssd/arcs/tmp
pools=${POOLS:-"p200k places L2 L4 L8 uniform100k random5k grids l2c fresh20k"}
for p in $pools; do
  [ -f "$C/$p.done" ] && continue
  split -d -l 25000 -a 3 "/ssd/arcs/pts/$p.txt" "$C/$p."
  touch "$C/$p.done"
done
jobs=()
for p in $pools; do for c in $(cd $C && ls $p.[0-9][0-9][0-9]); do jobs+=("$c"); done; done
printf '%s\n' "${jobs[@]}" | xargs -P "${JOBS:-16}" -I{} sh -c "nice -n 10 $GQ $dir < $C/{} > $Q/{}.part 2> $Q/{}.err"
for p in $pools; do
  cat $(ls $Q/$p.[0-9][0-9][0-9].part | sort) > "$Q/$p.txt"
  rm -f $Q/$p.[0-9][0-9][0-9].part $Q/$p.[0-9][0-9][0-9].err
  [ "$(wc -l < "$Q/$p.txt")" = "$(wc -l < /ssd/arcs/pts/$p.txt)" ] || echo "SHORT $label $p"
done
echo "RUNQ_DONE $label"
