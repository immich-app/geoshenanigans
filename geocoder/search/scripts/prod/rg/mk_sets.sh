#!/usr/bin/env bash
# Original-file test sets: the search-prod sets/ with the three record files the builder
# emulation rewrote (admin_polygons, place_nodes, poi_records) pointed back at the shipped
# planet-261006 files. sets/ itself keeps the emulated copies.
set -euo pipefail
P=/ssd/precision/build/out-nocap/planet
S=/ssd/search-prod/sets
O=/ssd/search-prod/rg/sets-orig
declare -A ORIG=(
  [minimal/admin_polygons.bin]=$P/admin-minimal/admin_polygons.bin
  [minimal/place_nodes.bin]=$P/admin-minimal/place_nodes.bin
  [recommended/admin_polygons.bin]=$P/quality/q2.5/admin_polygons.bin
  [recommended/place_nodes.bin]=$P/admin/place_nodes.bin
  [recommended/poi_records.bin]=$P/poi/major/poi_records.bin
  [max/admin_polygons.bin]=$P/quality/uncapped/admin_polygons.bin
  [max/place_nodes.bin]=$P/full/place_nodes.bin
  [max/poi_records.bin]=$P/poi/all/poi_records.bin
)
for s in minimal recommended max; do
  rm -rf "${O:?}/$s"
  mkdir -p $O/$s
  for f in $S/$s/*; do
    b=$(basename "$f")
    [ "$b" = place_nodes.e3.bin ] && continue
    if [ -n "${ORIG[$s/$b]:-}" ]; then
      ln -s "${ORIG[$s/$b]}" $O/$s/$b
    elif [ -L "$f" ]; then
      ln -s "$(readlink "$f")" $O/$s/$b
    else
      echo "unexpected regular file $f" >&2
      exit 1
    fi
  done
done
for k in "${!ORIG[@]}"; do
  if cmp -s "$S/$k" "${ORIG[$k]}"; then echo "same  $k"; else echo "differs $k (emulated builder change)"; fi
done
