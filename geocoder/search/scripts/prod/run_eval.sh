#!/usr/bin/env bash
# One accuracy run: run_eval.sh <set> <index dir> <out dir> [extra query args...]
# Queries: the prototype's five sets (dev manual + villages, held-out 1-3), 1,311 queries.
set -euo pipefail
R=/ssd/search-prod
Q=/ssd/search/qset
SET=$1
IDX=$2
OUT=$3
shift 3
mkdir -p "$OUT"
FILES="$Q/queries_manual.tsv $Q/queries_villages.tsv $Q/queries_heldout.tsv $Q/queries_heldout2.tsv $Q/queries_heldout3.tsv"
cat $FILES | grep -v '^#' | grep -v '^$' | cut -f2 > "$OUT/queries.txt"
$R/target/release/place-search query "$R/sets/$SET" "$IDX" --iso "$R/extras/$SET/iso_codes.tsv" "$@" < "$OUT/queries.txt" > "$OUT/ours.jsonl" 2> "$OUT/query.err"
/ssd/shrink/venv/bin/python $R/scripts/eval.py "$OUT/ours.jsonl" "$OUT" $FILES
