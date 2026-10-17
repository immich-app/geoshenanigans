#!/usr/bin/env bash
# One evaluation run: ours (results + bench) and the GeoNames baseline, scored by run_eval.py.
#   run_all.sh <out dir> <query tsv>...
set -euo pipefail
Q=/ssd/search/qset
D=/ssd/search/data
BIN=/ssd/search/target/release/place-search
OUT=$1
shift
mkdir -p "$OUT"
cat "$@" | grep -v '^#' | grep -v '^$' | cut -f2 > "$OUT/queries.txt"
export RAYON_NUM_THREADS=20
/usr/bin/time -v $BIN $D/corpus.tsv $D/context_aliases.tsv < "$OUT/queries.txt" > "$OUT/ours.jsonl" 2> "$OUT/ours_build.txt"
$BIN $D/corpus.tsv $D/context_aliases.tsv --bench 5 < "$OUT/queries.txt" > "$OUT/ours_bench.json" 2> "$OUT/ours_bench.err"
/ssd/search/venv/bin/python $Q/run_eval.py "$OUT/ours.jsonl" "$OUT" "$@"
cat "$OUT/ours_bench.json"
