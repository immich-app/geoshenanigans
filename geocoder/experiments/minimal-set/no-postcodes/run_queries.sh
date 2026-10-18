#!/usr/bin/env bash
# Run grid_query over a points file against every test dir in parallel; one
# jsonl per dir. usage: run_queries.sh <points.txt> <suffix>
set -euo pipefail
W=/ssd/shrink/no-postcodes
Q=/ssd/shrink/target/release/examples/grid_query
PTS=$1; SUF=$2
declare -A DIRS=(
  [recommended]=/ssd/shrink/idx/recommended
  [smallest]=/ssd/shrink/idx/smallest
  [rec_nopc]=$W/dirs/rec_nopc
  [rec_nopc_keepstr]=$W/dirs/rec_nopc_keepstr
  [admin_nopc_nopoi]=$W/dirs/admin_nopc_nopoi
  [admin_nopoi]=$W/dirs/admin_nopoi
  [minimal_poi]=$W/dirs/minimal_poi
)
[ -d $W/dirs/rec_nopc_nol11 ] && DIRS[rec_nopc_nol11]=$W/dirs/rec_nopc_nol11
for name in "${!DIRS[@]}"; do
  ( t0=$(date +%s.%N)
    "$Q" "${DIRS[$name]}" < "$PTS" > "$W/out/$name.$SUF.jsonl" 2> "$W/out/$name.$SUF.err"
    echo "$name: rc=$? $(echo "$(date +%s.%N) - $t0" | bc) s, $(wc -l < "$W/out/$name.$SUF.jsonl") lines" ) &
done
wait
