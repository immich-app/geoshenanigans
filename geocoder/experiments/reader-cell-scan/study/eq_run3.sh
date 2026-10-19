#!/usr/bin/env bash
# eq_run3.sh <reader>... : answers of each reader on the 50k reader-track points (general30k,
# parallel10k, border10k, in that order) for every set below: grid_query (query_json, the server's
# answer) and admin_dump (find_admin alone). Out: /ssd/reader/out/<set>.<reader>.{q,a}; a
# (set, reader, tool) whose output already has 50,000 lines is kept. Low priority, 4 jobs on
# cores 56-63 (not timing: the box is shared).
#   o-*  search-prod sets-orig (planet-261006, q2.5)
#   b-*  bugs2 base (planet-261018b, main builder, q2.5): today's data
#   u-*  tiers2 uni75t8rc10 (flat 75 m, L8 50 m, ring cap)
#   r1-* tiers3 setsD1/r1 (planet-261018b D1 build, r1 tolerances)
set -uo pipefail
R=/ssd/reader
SETS="o-minimal=/ssd/search-prod/rg/sets-orig/minimal
o-recommended=/ssd/search-prod/rg/sets-orig/recommended
o-max=/ssd/search-prod/rg/sets-orig/max
b-smallest=/ssd/bugs2/sets/base/smallest
b-recommended=/ssd/bugs2/sets/base/recommended
b-max=/ssd/bugs2/sets/base/max
u-smallest=/ssd/tiers2/sets/uni75t8rc10/smallest
u-recommended=/ssd/tiers2/sets/uni75t8rc10/recommended
r1-smallest=/ssd/tiers3/setsD1/r1/smallest
r1-recommended=/ssd/tiers3/setsD1/r1/recommended"
for line in $SETS; do
  for r in "$@"; do
    for t in q a; do
      f=$R/out/${line%%=*}.$r.$t
      [ -f "$f" ] && [ "$(wc -l < "$f")" = 50000 ] && continue
      echo "${line%%=*} ${line#*=} $r $t"
    done
  done
done | xargs -P 4 -L 1 bash -c '
  set=$0 dir=$1 r=$2 t=$3
  exe=/ssd/reader/bin/grid_query.$r
  [ "$t" = a ] && exe=/ssd/reader/bin/admin_dump.$r
  nice -n 19 taskset -c 56-63 $exe $dir < /ssd/reader/pts/all50k.txt > /ssd/reader/out/$set.$r.$t 2> /ssd/reader/out/$set.$r.$t.err
  echo "$set $r $t rc=$? lines=$(wc -l < /ssd/reader/out/$set.$r.$t)"'
echo EQ_DONE
