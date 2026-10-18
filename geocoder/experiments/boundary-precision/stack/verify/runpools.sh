#!/bin/bash
# re-run grid_query on the 8 earlier pools for baseline / stack / v11 / snaponly into verify/qp/
set -uo pipefail
cd /ssd/shrink/stack/verify
mkdir -p qp
GQ=/ssd/shrink/target/release/examples/grid_query; V=/ssd/shrink/vertices
declare -A POOL=([random]=$V/pts_random.txt [places]=$V/pts_places.txt [L2]=$V/pts_L2.txt [L4]=$V/pts_L4.txt [L8]=$V/pts_L8.txt [all]=$V/pts_all.txt [p200k]=/ssd/shrink/places-strings-index/work/points.txt [ablate3]=/ssd/shrink/tmp/ablate3/coords.txt)
declare -A SET=([base]=/ssd/shrink/idx/smallest [stack]=/ssd/shrink/stack/todays-format [v11]=/ssd/shrink/stack/verify/sets/v11 [snaponly]=/ssd/shrink/stack/verify/snaponly)
for s in "${!SET[@]}"; do for p in "${!POOL[@]}"; do echo "$s ${SET[$s]} $p ${POOL[$p]}"; done; done | xargs -P 14 -L 1 bash -c "$GQ \$1 < \$3 > qp/\$0.\$2.jsonl 2> qp/\$0.\$2.err"
echo done > runpools.done
