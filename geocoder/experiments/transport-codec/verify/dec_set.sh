#!/bin/bash
# dec_set.sh MODE TT IN OUT : decode a downloaded set one file after another (one process at a time).
#   plain: IN/<name>.zst -> zstd -d (1.5.7 CLI)
#   tt2  : IN/<name>.tt.zst -> TT dec, records + vertex file pairs through TT dec2 (only .tt.zst inputs)
set -eu
mode=$1 TT=$2 in=$3 out=$4
mkdir -p "$out"
declare -A VOF=([addr_points.bin]="addr_vertices.bin:24" [poi_records.bin]="poi_vertices.bin:12"
  [admin_polygons.bin]="admin_vertices.bin:4" [postal_polygons.bin]="postal_vertices.bin:4")
declare -A ISV=([addr_vertices.bin]=1 [poi_vertices.bin]=1 [admin_vertices.bin]=1 [postal_vertices.bin]=1)
if [ "$mode" = plain ]; then
  for z in "$in"/*.zst; do
    /ssd/shrink/zstd157/bin/zstd -d -q -f "$z" -o "$out/$(basename "$z" .zst)"
  done
  exit 0
fi
for z in "$in"/*.tt.zst; do
  n=$(basename "$z" .tt.zst)
  [ -n "${ISV[$n]:-}" ] && continue
  if [ -n "${VOF[$n]:-}" ]; then
    v=${VOF[$n]%%:*} off=${VOF[$n]##*:}
    "$TT" dec2 "$z" "$out/$n" "$in/$v.tt.zst" "$out/$v" "$off" 2>> "$out.dec2.log"
  else
    "$TT" dec "$z" "$out/$n"
  fi
done
