#!/bin/bash
# compress.sh: sizes of a search name file per name set and scope.
# Level files from places.py / poi_strings.py in /ssd/precision/names/files:
#   L0 search places, Lminor suburb/quarter/neighbourhood nodes, Ladmin unlinked
#   admin boundaries, L1/L2/L3 POI tiers 1/2/3.
# Two layouts per (set, scope):
#   fm   feature-major: one line per feature, its distinct strings joined by
#        0x1f, features in osm-id order (line number = feature index)
#   dict the distinct strings of the scope, LC_ALL=C sorted, one per line
# Output: out/compress.tsv  set scope layout lines raw_bytes zstd18_bytes
set -eu
F=/ssd/precision/names/files
S=/ssd/precision/names/scopes
O=/ssd/precision/names/out
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/precision/tmp
mkdir -p $S
declare -A SCOPES=(
  [places]="L0"
  [places+minor]="L0 Lminor"
  [places+minor+admin]="L0 Lminor Ladmin"
  [places+major]="L0 L1"
  [places+notable]="L0 L1 L2"
  [places+all_pois]="L0 L1 L2 L3"
)
jobs=$S/jobs.txt
: > $jobs
for set in base top10 all; do
  for scope in "${!SCOPES[@]}"; do
    fm=""; dc=""
    for lv in ${SCOPES[$scope]}; do fm="$fm $F/fm_${set}_${lv}.txt"; dc="$dc $F/dict_${set}_${lv}.txt"; done
    echo "$set|$scope|fm|cat $fm" >> $jobs
    echo "$set|$scope|dict|cat $dc | LC_ALL=C sort -u -S 4G" >> $jobs
  done
done
run_one() {
  IFS='|' read -r set scope layout cmd <<< "$1"
  f=$S/${layout}_${set}_${scope}.txt
  eval "$cmd" > "$f"
  lines=$(wc -l < "$f")
  raw=$(stat -c %s "$f")
  z=$($Z -18 -T1 -q -c "$f" | wc -c)
  rm -f "$f"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$set" "$scope" "$layout" "$lines" "$raw" "$z"
}
export -f run_one
export S Z
printf 'set\tscope\tlayout\tlines\traw_bytes\tzstd18_bytes\n' > $O/compress.tsv
tr '\n' '\0' < $jobs | xargs -0 -P 36 -I{} bash -c 'run_one "$@"' _ {} | sort >> $O/compress.tsv
echo COMPRESS_DONE
