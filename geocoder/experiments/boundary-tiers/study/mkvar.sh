#!/bin/bash
# mkvar.sh <name> <scheme> [quality scheme (default: same)] : one tier candidate.
#   var/<name>/min  tiervar minimal (admin-minimal admin_* + admin_parents.bin), stats min.json
#   var/<name>/q    tiervar quality (quality-dir admin_* + postal_*), stats q.json
#   sets/<name>/smallest     = sets/base/smallest with the variant's admin_* (+ admin_parents.bin)
#   sets/<name>/recommended  = sets/base/recommended with the variant's admin_polygons/vertices +
#                              postal_* (+ planet/full/admin_parents.bin, same slot space)
# SKIPQ=1 skips the quality dir (smallest only). OMP threads: THREADS (default 20).
# TIERS2_NEST=<nestcheck flags>: both sets' admin_parents.bin leave out the parent of every polygon
# whose chain doesn't nest (the chain reader then uses point-in-polygon for it).
set -euo pipefail
T=/ssd/tiers2
name=$1
scheme=$2
qscheme=${3:-$2}
B=$T/build/out-dump/planet
V=$T/var/$name
export OMP_NUM_THREADS=${THREADS:-20}
mkdir -p "$V/min" "$V/q"
$T/tiervar minimal $T/dump $B/admin "$V/min" "$scheme" > "$V/min.json"
farm() {  # farm <base set> <dest>
  rm -rf "$2"; mkdir -p "$2"
  for f in "$1"/*; do ln -s "$(readlink -f "$f")" "$2/$(basename "$f")"; done
}
farm $T/sets/base/smallest $T/sets/$name/smallest
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin admin_parents.bin; do
  ln -sfn "$V/min/$f" "$T/sets/$name/smallest/$f"
done
if [ -z "${SKIPQ:-}" ]; then
  $T/tiervar quality $T/dump "$V/q" "$qscheme" > "$V/q.json"
  farm $T/sets/base/recommended $T/sets/$name/recommended
  for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do
    ln -sfn "$V/q/$f" "$T/sets/$name/recommended/$f"
  done
  if [ -n "${TIERS2_NEST:-}" ]; then
    $T/tiervar parents $T/dump "$TIERS2_NEST" "$V/q/admin_parents.bin" > "$V/parents.json"
    ln -sfn "$V/q/admin_parents.bin" "$T/sets/$name/recommended/admin_parents.bin"
  else
    ln -sfn $B/full/admin_parents.bin "$T/sets/$name/recommended/admin_parents.bin"
  fi
fi
echo "MKVAR_DONE $name"
