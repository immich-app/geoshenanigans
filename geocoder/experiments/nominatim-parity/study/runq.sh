#!/bin/bash
# runq.sh <label> <set dir> <grid_query binary> [pool...] : answers for each pool of
# /ssd/parity/pts (default all) -> /ssd/parity/q/<label>/<pool>.txt, chunks of 25k points,
# JOBS (default 12) at a time, niced. As /ssd/bugs2/runq.sh.
set -euo pipefail
label=$1; dir=$2; GQ=$3; shift 3
P=/ssd/parity
pools=("$@")
[ ${#pools[@]} -eq 0 ] && pools=($(ls $P/pts | sed "s/\.txt$//"))
Q=$P/q/$label; C=$P/q/chunks; W=$P/q/parts/$label
mkdir -p "$Q" "$C" "$W"
export TMPDIR=$P/tmp
jobs=()
for p in "${pools[@]}"; do
  if [ ! -f "$C/$p.done" ]; then split -d -l 25000 -a 3 $P/pts/$p.txt "$C/$p."; touch "$C/$p.done"; fi
  for c in $(ls $C | grep -E "^$p\.[0-9]{3}$"); do jobs+=("$c"); done
done
printf "%s\n" "${jobs[@]}" | xargs -P "${JOBS:-12}" -I{} sh -c "nice -n 10 $GQ $dir < $C/{} > $W/{}.part 2> $W/{}.err"
for p in "${pools[@]}"; do
  cat $(ls $W/$p.[0-9][0-9][0-9].part | sort) > "$Q/$p.txt"
  [ "$(wc -l < "$Q/$p.txt")" = "$(wc -l < $P/pts/$p.txt)" ] || echo "SHORT $label $p"
done
echo "RUNQ_DONE $label"
