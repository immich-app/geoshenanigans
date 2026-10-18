#!/bin/bash
# runq.sh NAME PTSFILE: grid_query answers of flat/1013-pub and flat/1013-sorted for PTSFILE,
# 8 chunks per dir in parallel on cores 32-47, then a whole-file / per-line full-JSON comparison.
set -eu
V=/ssd/shrink/sortpatch/verify; GQ=/ssd/shrink/target/release/examples/grid_query
export TMPDIR=/ssd/shrink/tmp
name=$1 pts=$2
T=$V/q/tmp-$name; rm -rf $T; mkdir -p $T
split -n l/8 -d $pts $T/in.
i=0
for kind in pub sorted; do
  for c in $T/in.*; do
    n=${c##*.}
    taskset -c $((32 + i % 16)) $GQ $V/flat/1013-$kind < $c > $T/$kind.$n 2> $T/$kind.$n.err &
    i=$((i + 1))
  done
done
wait
for kind in pub sorted; do cat $T/$kind.[0-9]* > $V/q/$name.$kind.jsonl; done
np=$(wc -l < $V/q/$name.pub.jsonl); ns=$(wc -l < $V/q/$name.sorted.jsonl); nin=$(wc -l < $pts)
if cmp -s $V/q/$name.pub.jsonl $V/q/$name.sorted.jsonl; then v=IDENTICAL; else v="DIFF lines: $(diff $V/q/$name.pub.jsonl $V/q/$name.sorted.jsonl | grep -c '^<')"; fi
echo "$name: input $nin, answers pub $np sorted $ns: $v"
rm -rf $T
