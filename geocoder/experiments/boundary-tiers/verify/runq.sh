#!/bin/bash
# runq.sh : grid_query (repo@97c732c, plain reader, built here) for pools fresh20k + grids on every
# scheme x set -> q/<scheme>_<set>/<pool>.txt ; 5000-line chunks, 16 processes.
set -euo pipefail
V=/ssd/tiers2-verify
GQ=$V/target/release/examples/grid_query
mkdir -p $V/q/chunks
for p in fresh20k grids; do split -d -l 5000 -a 3 $V/pts/$p.txt $V/q/chunks/$p.; done
jobs=()
for s in ${SCHEMES:-unc q25 cap10 rec small}; do for st in smallest recommended; do
  mkdir -p $V/q/${s}_$st
  for c in $(cd $V/q/chunks && ls); do jobs+=("$s $st $c"); done
done; done
printf "%s\n" "${jobs[@]}" | xargs -P 16 -L 1 sh -c "$GQ $V/sets/\$0/\$1 < $V/q/chunks/\$2 > $V/q/\$0_\$1/\$2.part 2> $V/q/\$0_\$1/\$2.err"
for s in ${SCHEMES:-unc q25 cap10 rec small}; do for st in smallest recommended; do
  for p in fresh20k grids; do
    cat $(ls $V/q/${s}_$st/$p.[0-9][0-9][0-9].part | sort) > $V/q/${s}_$st/$p.txt
    [ "$(wc -l < $V/q/${s}_$st/$p.txt)" = "$(wc -l < $V/pts/$p.txt)" ] || echo "SHORT ${s}_$st $p"
  done
  rm -f $V/q/${s}_$st/*.part
done; done
echo RUNQ_DONE
