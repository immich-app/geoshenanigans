#!/bin/bash
# zstd every enc/*.bin (plus orig.bin) at -18 (what compress_index.py ships),
# -19 and --ultra -22 --long=27, single-threaded for deterministic sizes.
# Usage: compress.sh <dir> ; writes <dir>/sizes.tsv
set -euo pipefail
D=${1:-enc}
OUTD=$D/zst
mkdir -p "$OUTD"
job() {
  f=$1; lvl=$2; b=$(basename "$f" .bin)
  case $lvl in
    18) args="-18" ;;
    19) args="-19" ;;
    22) args="--ultra -22 --long=27" ;;
  esac
  zstd $args -T1 -q -f -c "$f" > "$OUTD/$b.$lvl.zst"
  printf '%s\t%s\t%s\t%s\n' "$b" "$lvl" "$(stat -c %s "$f")" "$(stat -c %s "$OUTD/$b.$lvl.zst")"
}
export -f job; export OUTD
for f in "$D"/*.bin; do for l in ${LEVELS:-18 19 22}; do echo "$f $l"; done; done \
  | xargs -P ${JOBS:-18} -n 2 bash -c 'job "$0" "$1"' | sort > "$D/sizes.tsv"
cat "$D/sizes.tsv"
