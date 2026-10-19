#!/bin/bash
# mkset.sh <name> <build out dir> [tiervar minimal scheme] [tiervar quality scheme] : verify5 flat sets
# /ssd/verify5/sets/<name>/{smallest,recommended} assembled the client's way (bugs2 assemble_sets.py,
# 2026-10-13 manifest) from a build. With schemes: the 4 admin-minimal admin files and the quality
# dir's admin/postal polygons + vertices are replaced by tiers3's tiervar3-n output from that build's
# dump (/ssd/tiers3/dumpD1, admin-minimal in the build's warm slots). "none" = uncapped truth; the
# recommended truth then takes the build's own quality/uncapped files.
set -euo pipefail
V=/ssd/verify5
name=$1; out=$2; ms=${3:-}; qs=${4:-}
S=$V/sets/$name
rm -rf $S; mkdir -p $S
/ssd/shrink/venv/bin/python -I /ssd/bugs2/probe/assemble_sets.py $out $S > /dev/null
rm -rf $S/max
[ -z "$ms" ] && { echo "MKSET_DONE $name"; exit 0; }
O=$out/planet
T=$V/var/$name
rm -rf $T; mkdir -p $T/min $T/q
export OMP_NUM_THREADS=${THREADS:-16}
TIERVAR_MIN_SLOTS=$O/admin-minimal/admin_polygons.bin /ssd/tiers3/bin/tiervar3-n minimal /ssd/tiers3/dumpD1 $O/admin $T/min "$ms" > $T/min.json
rm -f $T/min/admin_parents.bin
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin; do ln -sfn $T/min/$f $S/smallest/$f; done
if [ "$ms" = none ]; then
  for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -sfn $O/quality/uncapped/$f $S/recommended/$f; done
else
  /ssd/tiers3/bin/tiervar3-n quality /ssd/tiers3/dumpD1 $T/q "$qs" > $T/q.json
  for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -sfn $T/q/$f $S/recommended/$f; done
fi
echo "MKSET_DONE $name"
