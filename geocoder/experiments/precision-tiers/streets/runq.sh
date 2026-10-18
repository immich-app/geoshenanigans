#!/bin/bash
# runq.sh FLATDIR NAME CORES: grid_query answers of FLATDIR for q/pts.txt, one chunk per core, into q/NAME.jsonl.
set -eu
S=/ssd/precision/streets; GQ=${GQ:-/ssd/shrink/target/release/examples/grid_query}
export TMPDIR=/ssd/precision/tmp
dir=$1 name=$2 cores=$3
T=$S/q/tmp-$name; rm -rf $T; mkdir -p $T
first=${cores%-*} last=${cores#*-}
k=$((last - first + 1))
split -n l/$k -d -a 3 $S/q/pts.txt $T/in.
i=0
start=$(date +%s)
for c in $T/in.*; do
  n=${c##*.}
  taskset -c $((first + i)) $GQ $dir < $c > $T/out.$n 2> $T/err.$n &
  i=$((i + 1))
done
wait
cat $T/out.* > $S/q/$name.jsonl
echo "$name: $(wc -l < $S/q/$name.jsonl) answers of $(wc -l < $S/q/pts.txt) in $(( $(date +%s) - start )) s"
rm -rf $T
