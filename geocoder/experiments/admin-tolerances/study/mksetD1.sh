#!/bin/bash
# mksetD1.sh <name> <minimal scheme> [quality scheme (default: minimal + t9..t11 = 37.5)] : one tier
# scheme on the newer planet (planet-261018b, build D1 = fix/admin-nesting + cap, warm from D) with
# tiervar3-n from D1's own admin dump (admin-minimal in the builder's warm slots). Sets = the D1 build's
# presets (bugs2 assemble_sets.py) with the scheme's admin-minimal / quality q2.5 files.
# "unc" = truth: admin-minimal L2-8 uncapped; recommended with quality/uncapped.
set -euo pipefail
T=/ssd/tiers3
O=$T/build/out-D1/planet
name=$1
ms=$2
qs=${3:-"$2;t9=37.5;t10=37.5;t11=37.5"}
V=$T/varD1/$name
export OMP_NUM_THREADS=${THREADS:-16}
if [ ! -d $T/setsD1/base ]; then
  /ssd/shrink/venv/bin/python -I /ssd/bugs2/probe/assemble_sets.py $T/build/out-D1 $T/setsD1/base > /dev/null
fi
mkdir -p "$V/min" "$V/q"
farm() { rm -rf "$2"; mkdir -p "$2"; for f in "$1"/*; do ln -s "$(readlink -f "$f")" "$2/$(basename "$f")"; done; }
TIERVAR_MIN_SLOTS=$O/admin-minimal/admin_polygons.bin $T/bin/tiervar3-n minimal $T/dumpD1 $O/admin "$V/min" "$ms" > "$V/min.json"
rm -f "$V/min/admin_parents.bin"
farm $T/setsD1/base/smallest $T/setsD1/$name/smallest
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin; do ln -sfn "$V/min/$f" "$T/setsD1/$name/smallest/$f"; done
farm $T/setsD1/base/recommended $T/setsD1/$name/recommended
if [ "$name" = unc ]; then
  for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -sfn "$O/quality/uncapped/$f" "$T/setsD1/$name/recommended/$f"; done
else
  $T/bin/tiervar3-n quality $T/dumpD1 "$V/q" "$qs" > "$V/q.json"
  for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -sfn "$V/q/$f" "$T/setsD1/$name/recommended/$f"; done
fi
echo "MKSETD1_DONE $name"
