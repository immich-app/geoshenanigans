#!/bin/bash
# grid_query answers for the named point pools against the named flat dirs.
# usage: POOLS="p200k L2 ..." runq.sh <name>=<dir> ...   -> q/<name>.<pool>.jsonl (+ .err)
# Every pool is split into 10k-line chunks run as separate grid_query processes (JOBS at a time,
# default 24), then concatenated in order. grid_query: built from /ssd/precision/repo (main f48ddc4 +
# the instrumentation commit, server/ unchanged since 5caed45).
set -euo pipefail
T=/ssd/precision/tiers
cd $T
GQ=$T/target/release/examples/grid_query
POOLS=${POOLS:-"p200k L2 L4 L6 L8 L10 places random"}
mkdir -p q pts/chunks
jobs=()
for p in $POOLS; do
  [ -f pts/chunks/$p.000 ] || split -d -a 3 -l 10000 pts/$p.txt pts/chunks/$p.
done
for nd in "$@"; do
  n=${nd%%=*}; d=${nd#*=}
  for p in $POOLS; do
    for c in pts/chunks/$p.[0-9][0-9][0-9]; do jobs+=("$n $d $p.${c##*.} $c"); done
  done
done
export GQ
printf '%s\n' "${jobs[@]}" | xargs -P ${JOBS:-24} -L 1 bash -c '"$GQ" "$1" < "$3" > "q/$0.$2.part" 2> "q/$0.$2.perr"'
for nd in "$@"; do
  n=${nd%%=*}
  for p in $POOLS; do
    cat q/$n.$p.[0-9][0-9][0-9].part > q/$n.$p.jsonl && rm q/$n.$p.[0-9][0-9][0-9].part
    cat q/$n.$p.[0-9][0-9][0-9].perr > q/$n.$p.err && rm q/$n.$p.[0-9][0-9][0-9].perr
  done
  echo "$n: $(cat q/$n.*.jsonl | wc -l) answers; stderr bytes $(cat q/$n.*.err | wc -c)"
done
