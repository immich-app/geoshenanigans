#!/bin/bash
# v5q.sh <reader> <label> <set dir> <pool file>... : grid_query answers for each pool (10k-point
# chunks, JOBS processes, nice 10) -> /ssd/verify5/q/<label>/<pool basename>. Reader =
# /ssd/verify5/bin/grid_query.<reader> (or a path).
set -euo pipefail
r=$1; label=$2; dir=$3; shift 3
GQ=$r; [ -f "$GQ" ] && [ -x "$GQ" ] || GQ=/ssd/verify5/bin/grid_query.$r
Q=/ssd/verify5/q/$label
C=$(mktemp -d /ssd/verify5/tmp/q-XXXXXX)
mkdir -p $Q
for p in "$@"; do
  b=$(basename $p .txt)
  split -d -l 10000 -a 3 $p $C/$b.
done
ls $C | xargs -P ${JOBS:-16} -I{} sh -c "nice -n ${NICE:-10} $GQ $dir < $C/{} > $C/{}.out 2> $C/{}.err"
for p in "$@"; do
  b=$(basename $p .txt)
  cat $(ls $C/$b.[0-9][0-9][0-9].out | sort) > $Q/$b.txt
  [ "$(wc -l < $Q/$b.txt)" = "$(wc -l < $p)" ] || { echo "SHORT $label $b"; cat $C/$b.*.err | tail -3; }
done
rm -rf $C
echo "V5Q_DONE $label"
