#!/bin/bash
# runq.sh DATE POOL : grid_query answers of the published and the sorted day
# (each variant set hardlinked into one flat dir) for pts/DATE/pts_POOL.txt,
# 8 chunks per dir in parallel on cores 32-47; then a full-JSON diff.
set -eu
d=$1; pool=$2
W=/ssd/shrink/sortpatch/sort; GQ=/ssd/shrink/target/release/examples/grid_query
export TMPDIR=/ssd/shrink/tmp
for kind in pub sorted; do
  F=$W/flat/$d-$kind
  if [ ! -d $F ]; then
    mkdir -p $F
    src=/ssd/shrink/days/$d/planet; [ $kind = sorted ] && src=$W/days/$d/planet
    for v in full quality/uncapped poi/all; do
      for f in $src/$v/*; do b=$(basename $f); [ "$b" = patch.gcpatch ] && continue; [ -e $F/$b ] || ln $f $F/$b; done
    done
  fi
done
mkdir -p $W/q/tmp; rm -f $W/q/tmp/$d.$pool.*
split -n l/8 -d $W/pts/$d/pts_$pool.txt $W/q/tmp/$d.$pool.in.
t0=$(date +%s)
i=0
for kind in pub sorted; do
  for c in $W/q/tmp/$d.$pool.in.*; do
    n=${c##*.}
    taskset -c $((32 + i % 16)) $GQ $W/flat/$d-$kind < $c > $W/q/tmp/$d.$pool.$kind.$n 2> $W/q/tmp/$d.$pool.$kind.$n.err &
    i=$((i + 1))
  done
done
wait
for kind in pub sorted; do cat $W/q/tmp/$d.$pool.$kind.[0-9]* > $W/q/$d.$pool.$kind.jsonl; done
echo "$d $pool: $(wc -l < $W/q/$d.$pool.pub.jsonl) / $(wc -l < $W/q/$d.$pool.sorted.jsonl) lines in $(( $(date +%s) - t0 )) s"
if cmp -s $W/q/$d.$pool.pub.jsonl $W/q/$d.$pool.sorted.jsonl; then echo "IDENTICAL"; else
  echo "DIFF lines: $(diff $W/q/$d.$pool.pub.jsonl $W/q/$d.$pool.sorted.jsonl | grep -c "^<")"; fi
