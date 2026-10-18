#!/bin/bash
# grid_query over work/pts_all.txt (87,007 points) against <dir>, 8 chunks in parallel;
# writes q/<name>.jsonl (input order). usage: runq.sh <dir> <name>
set -euo pipefail
W=/ssd/shrink/max/index-poi-strings
Q=/ssd/shrink/target/release/examples/grid_query
DIR=$1; NAME=$2
mkdir -p $W/q $W/tmp
export TMPDIR=/ssd/shrink/tmp
split -n l/8 -d $W/work/pts_all.txt $W/tmp/$NAME.part.
t0=$(date +%s.%N)
for p in $W/tmp/$NAME.part.*; do
  "$Q" "$DIR" < "$p" > "$p.out" 2> "$p.err" &
done
wait
cat $W/tmp/$NAME.part.0?.out > $W/q/$NAME.jsonl
cat $W/tmp/$NAME.part.0?.err > $W/q/$NAME.err
rm -f $W/tmp/$NAME.part.*
echo "$NAME: $(wc -l < $W/q/$NAME.jsonl) lines, $(echo "$(date +%s.%N) - $t0" | bc) s"
