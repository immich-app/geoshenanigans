#!/usr/bin/env bash
# eq_run.sh <reader>... : answers of each reader on the 50k reader-track points (general30k,
# parallel10k, border10k, in that order) for every set: grid_query (query_json, the server's
# answer) and admin_dump (find_admin alone). Out: /ssd/reader/out/<set>.<reader>.{q,a}
# 20 jobs at a time on cores 24-63 (not timing: the box is shared).
set -uo pipefail
R=/ssd/reader
SETS="o-minimal=/ssd/search-prod/rg/sets-orig/minimal
o-recommended=/ssd/search-prod/rg/sets-orig/recommended
o-max=/ssd/search-prod/rg/sets-orig/max
b-smallest=/ssd/bugs2/sets/base/smallest
b-recommended=/ssd/bugs2/sets/base/recommended
b-max=/ssd/bugs2/sets/base/max
u-smallest=/ssd/tiers2/sets/uni75t8rc10/smallest
u-recommended=/ssd/tiers2/sets/uni75t8rc10/recommended"
cat $R/pts/general30k.txt $R/pts/parallel10k.txt $R/pts/border10k.txt > $R/pts/all50k.txt
for line in $SETS; do
  for r in "$@"; do
    echo "${line%%=*} ${line#*=} $r q"
    echo "${line%%=*} ${line#*=} $r a"
  done
done | xargs -P 20 -L 1 bash -c '
  set=$0 dir=$1 r=$2 t=$3
  exe=/ssd/reader/bin/grid_query.$r
  [ "$t" = a ] && exe=/ssd/reader/bin/admin_dump.$r
  taskset -c 24-63 $exe $dir < /ssd/reader/pts/all50k.txt > /ssd/reader/out/$set.$r.$t 2> /ssd/reader/out/$set.$r.$t.err
  echo "$set $r $t rc=$? lines=$(wc -l < /ssd/reader/out/$set.$r.$t)"'
echo EQ_DONE
