#!/bin/bash
# grid_query answers for every point pool against the named sets.
# usage: runq.sh <name>=<dir> ...   -> q/<name>.<pool>.jsonl (+ .err)
# Pools (pts/): places 20k (vertices/gen_places.py), random 5k (vertices/pts_random.txt),
# L2/L4/L8 20k each (gen_borders.py, from the TRUTH rings), p200k (gen_points2.py, run as
# 8 chunks of 25k and concatenated in order). Parallelism: JOBS (default 8) grid_query processes.
set -euo pipefail
cd /ssd/shrink/grid
GQ=/ssd/shrink/target/release/examples/grid_query
mkdir -p q pts/chunks
[ -f pts/chunks/p200k.07 ] || split -d -n l/8 -a 2 pts/p200k.txt pts/chunks/p200k.
jobs=()
for nd in "$@"; do
  n=${nd%%=*}; d=${nd#*=}
  for p in places random L2 L4 L8; do jobs+=("$n $d $p pts/$p.txt"); done
  for c in 00 01 02 03 04 05 06 07; do jobs+=("$n $d p200k.$c pts/chunks/p200k.$c"); done
done
export GQ
printf '%s\n' "${jobs[@]}" | xargs -P ${JOBS:-8} -L 1 bash -c '"$GQ" "$1" < "$3" > "q/$0.$2.jsonl" 2> "q/$0.$2.err"'
for nd in "$@"; do
  n=${nd%%=*}
  cat q/$n.p200k.0?.jsonl > q/$n.p200k.jsonl && rm q/$n.p200k.0?.jsonl
  cat q/$n.p200k.0?.err > q/$n.p200k.err && rm q/$n.p200k.0?.err
  echo "$n: $(cat q/$n.*.jsonl | wc -l) answers; stderr bytes $(cat q/$n.*.err | wc -c)"
done
