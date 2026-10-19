#!/bin/bash
# mkset6.sh <name> <minimal scheme> [quality scheme (default: minimal scheme + t9..t11 = 37.5)] :
# one tier candidate on planet-261006 (tiers2 dump, fix/admin-holes 97c732c) with tiervar3-h.
#   var6/<name>/min  admin-minimal admin_* (+ min.json stats), var6/<name>/q quality dir (+ q.json)
#   sets6/<name>/smallest     = tiers2 base smallest with the variant's admin_*
#   sets6/<name>/recommended  = tiers2 base recommended with the variant's admin_* + postal_*
# "unc" builds the truth sets (quality/uncapped files for recommended). SKIPQ=1: smallest only.
set -euo pipefail
T=/ssd/tiers3
B=/ssd/tiers2/build/out-dump/planet
name=$1
ms=$2
qs=${3:-"$2;t9=37.5;t10=37.5;t11=37.5"}
V=$T/var6/$name
export OMP_NUM_THREADS=${THREADS:-16}
mkdir -p "$V/min" "$V/q"
farm() {  # farm <base set> <dest>
  rm -rf "$2"; mkdir -p "$2"
  for f in "$1"/*; do ln -s "$(readlink -f "$f")" "$2/$(basename "$f")"; done
}
$T/bin/tiervar3-h minimal /ssd/tiers2/dump $B/admin "$V/min" "$ms" > "$V/min.json"
rm -f "$V/min/admin_parents.bin"
farm /ssd/tiers2/sets/base/smallest $T/sets6/$name/smallest
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin; do
  ln -sfn "$V/min/$f" "$T/sets6/$name/smallest/$f"
done
if [ -z "${SKIPQ:-}" ]; then
  farm /ssd/tiers2/sets/base/recommended $T/sets6/$name/recommended
  if [ "$name" = unc ]; then
    for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do
      ln -sfn "$B/quality/uncapped/$f" "$T/sets6/$name/recommended/$f"
    done
  else
    $T/bin/tiervar3-h quality /ssd/tiers2/dump "$V/q" "$qs" > "$V/q.json"
    for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do
      ln -sfn "$V/q/$f" "$T/sets6/$name/recommended/$f"
    done
  fi
fi
echo "MKSET6_DONE $name"
