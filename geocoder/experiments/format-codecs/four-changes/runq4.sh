#!/bin/bash
# grid_query answers for point pools against named sets -> q/<name>.<pool>.jsonl (+ .err)
# usage: POOLS="addr street ..." runq4.sh <parallel> <name>=<dir> ...   (POOLS default: all)
# Pools: addr 20k / street 10k / interp 5k / poi 5k (gen_points4.py, +-30 m around records),
# p22k (no-postcodes), ablate3 35k, vertices track random 5k / places 20k / L2 L4 L8 all 20k border
# points, p200k (places-strings-index gen_points2.py).
set -uo pipefail
cd /ssd/shrink/max/four-changes
GQ=/ssd/shrink/target/release/examples/grid_query
V=/ssd/shrink/vertices
declare -A POOL=(
  [addr]=work/pts_addr.txt [street]=work/pts_street.txt [interp]=work/pts_interp.txt [poi]=work/pts_poi.txt
  [p22k]=/ssd/shrink/no-postcodes/points22k.txt [ablate3]=/ssd/shrink/tmp/ablate3/coords.txt
  [random]=$V/pts_random.txt [places]=$V/pts_places.txt [L2]=$V/pts_L2.txt [L4]=$V/pts_L4.txt
  [L8]=$V/pts_L8.txt [all]=$V/pts_all.txt [p200k]=/ssd/shrink/places-strings-index/work/points.txt)
P=$1; shift
pools=${POOLS:-"addr street interp poi p22k ablate3 random places L2 L4 L8 all p200k"}
mkdir -p q
jobs=()
for nd in "$@"; do
  for p in $pools; do jobs+=("${nd%%=*} ${nd#*=} $p ${POOL[$p]}"); done
done
export GQ
printf '%s\n' "${jobs[@]}" | xargs -P $P -L 1 bash -c '"$GQ" "$1" < "$3" > "q/$0.$2.jsonl" 2> "q/$0.$2.err"; echo "$0 $2 rc=$? $(wc -l < q/$0.$2.jsonl) lines"'
