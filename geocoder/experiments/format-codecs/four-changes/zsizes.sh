#!/bin/bash
# zstd 1.5.7 -18 sizes of whole files, compressed by path like builder/tools/compress_index.py.
# usage: zsizes.sh <threads per job: 1|16> <parallel jobs> <file>...   -> appends to work/zst_T<t>.tsv
# Each line: realpath \t raw bytes \t zstd bytes \t seconds. Files already in the tsv (same realpath
# and size) are skipped.
set -uo pipefail
cd /ssd/shrink/max/four-changes
export TMPDIR=/ssd/shrink/tmp
T=$1; P=$2; shift 2
OUT=work/zst_T$T.tsv
touch $OUT
todo=()
for f in "$@"; do
  r=$(realpath "$f"); s=$(stat -c %s "$r")
  grep -qP "^\Q$r\E\t$s\t" $OUT || todo+=("$r")
done
printf '%s\n' "${todo[@]}" | sort -u | xargs -r -P $P -I{} bash -c '
  t0=$(date +%s); z=$(/ssd/shrink/zstd157/bin/zstd -18 -T'$T' -q -c "{}" | wc -c)
  printf "%s\t%s\t%s\t%s\n" "{}" "$(stat -c %s "{}")" "$z" "$(( $(date +%s) - t0 ))" >> '$OUT
