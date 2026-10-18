#!/bin/bash
# grid_query answers for every point pool against the named sets.
# usage: runq.sh <name>=<dir> ...   -> q/<name>.<pool>.jsonl (+ .err)
# Pools: vertices track (random 5k, places 20k, L2/L4/L8/all 20k border points),
# p200k (places-strings-index gen_points2.py), ablate3 (35k photo-like strata).
set -euo pipefail
cd /ssd/shrink/stack
GQ=/ssd/shrink/target/release/examples/grid_query
V=/ssd/shrink/vertices
declare -A POOL=(
  [random]=$V/pts_random.txt [places]=$V/pts_places.txt [L2]=$V/pts_L2.txt [L4]=$V/pts_L4.txt
  [L8]=$V/pts_L8.txt [all]=$V/pts_all.txt
  [p200k]=/ssd/shrink/places-strings-index/work/points.txt [ablate3]=/ssd/shrink/tmp/ablate3/coords.txt)
mkdir -p q
jobs=()
for nd in "$@"; do
  for p in "${!POOL[@]}"; do jobs+=("${nd%%=*} ${nd#*=} $p ${POOL[$p]}"); done
done
export GQ
printf '%s\n' "${jobs[@]}" | xargs -P 16 -L 1 bash -c '"$GQ" "$1" < "$3" > "q/$0.$2.jsonl" 2> "q/$0.$2.err"'
for nd in "$@"; do wc -l q/${nd%%=*}.*.jsonl | tail -1; cat q/${nd%%=*}.*.err; done
