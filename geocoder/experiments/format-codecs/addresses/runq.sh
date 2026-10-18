#!/bin/bash
# runq.sh <name> <dir> : grid_query answers for the address pools -> q/<name>.<pool>.jsonl
# Pools: addr20k exact5k street10k interp5k (gen_points.py), points22k (no-postcodes),
# ablate3 35k (photo-like strata).
set -euo pipefail
cd /ssd/shrink/max/addresses
GQ=/ssd/shrink/target/release/examples/grid_query
mkdir -p q
name=$1; dir=$2
declare -A POOL=(
  [addr20k]=pts_addr20k.txt [exact5k]=pts_exact5k.txt [street10k]=pts_street10k.txt
  [interp5k]=pts_interp5k.txt [p22k]=/ssd/shrink/no-postcodes/points22k.txt
  [ablate3]=/ssd/shrink/tmp/ablate3/coords.txt)
for p in "${!POOL[@]}"; do echo "$p ${POOL[$p]}"; done \
  | xargs -P ${JOBS:-6} -L 1 bash -c '"'"$GQ"'" "'"$dir"'" < "$1" > "q/'"$name"'.$0.jsonl" 2> "q/'"$name"'.$0.err"'
wc -l q/$name.*.jsonl | tail -1
