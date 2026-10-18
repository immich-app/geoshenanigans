#!/bin/bash
# work.sh <set> <name> ... : machine-independent point-in-polygon work per query: admin_verts calls
# and decoded admin vertices per query (counting grid_query, tiers2-count-reader), on the first 50k
# points of p200k and on L2. Chain labels (ending in g) run with GC_CHAIN_ADMIN=1.
# Output lines: name pool points decodes/query vertices/query
set -u
st=$1; shift
T=/ssd/tiers2
GQ=$T/target-c/release/examples/grid_query
head -50000 $T/pts/p200k.txt > $T/tmp/p50k.txt
for name in "$@"; do
  g=$name; chain=0
  if [[ $name == *g && -d $T/sets/${name%g} ]]; then g=${name%g}; chain=1; fi
  for pool in p50k L2; do
    f=$T/tmp/p50k.txt; [ $pool = L2 ] && f=$T/pts/L2.txt
    n=$(wc -l < $f)
    r=$(GC_CHAIN_ADMIN=$chain nice -n 19 $GQ $T/sets/$g/$st < $f 2>&1 > /dev/null | grep admin_decodes)
    set -- $r
    echo "$name $pool $n $(awk -v a=$2 -v b=$4 -v n=$n 'BEGIN{printf "%.1f %.0f", a/n, b/n}')"
  done
done
