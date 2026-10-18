#!/bin/bash
# dec_day.sh MODE IN OUT : decode a whole downloaded set, one file after another (single process at a time).
#   MODE plain: IN/<name>.zst            -> zstd -d (1.5.7 CLI)
#   MODE tt   : IN/<name>.tt.zst         -> tt dec (zstd stream + inverse transform, in-process);
#               vertex files after their records file, reading the counts from the decoded records
set -eu
mode=$1 in=$2 out=$3
mkdir -p "$out"
Z=/ssd/shrink/zstd157/bin/zstd
TT=/ssd/transport/bin/tt
declare -A SIDE=(
  [addr_vertices.bin]="addr_points.bin:28:24"
  [poi_vertices.bin]="poi_records.bin:36:12"
  [admin_vertices.bin]="admin_polygons.bin:24:4"
  [postal_vertices.bin]="postal_polygons.bin:24:4"
)
if [ "$mode" = plain ]; then
  for z in "$in"/*.zst; do
    n=$(basename "$z" .zst)
    $Z -d -q -f "$z" -o "$out/$n"
  done
  exit 0
fi
if [ "$mode" = tt2 ]; then
  # records + vertex file in one pass (tt dec2): the decoded records are never read back
  declare -A REC=()
  for v in "${!SIDE[@]}"; do REC[${SIDE[$v]%%:*}]=$v; done
  for z in "$in"/*.tt.zst; do
    n=$(basename "$z" .tt.zst)
    [ -n "${SIDE[$n]:-}" ] && continue
    if [ -n "${REC[$n]:-}" ] && [ -e "$in/${REC[$n]}.tt.zst" ]; then
      v=${REC[$n]}
      $TT dec2 "$z" "$out/$n" "$in/$v.tt.zst" "$out/$v" "${SIDE[$v]##*:}" 2> /dev/null
      continue
    fi
    $TT dec "$z" "$out/$n"
  done
  exit 0
fi
later=()
for z in "$in"/*.tt.zst; do
  n=$(basename "$z" .tt.zst)
  if [ -n "${SIDE[$n]:-}" ]; then later+=("$n"); continue; fi
  $TT dec "$z" "$out/$n"
done
for n in "${later[@]}"; do
  $TT dec "$in/$n.tt.zst" "$out/$n" --side "$out/${SIDE[$n]}"
done
