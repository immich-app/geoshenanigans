#!/bin/bash
# mkv.sh <name> <min scheme> <quality scheme> : my own variant build (verify track) with bin/tiervar
# (compiled here from tiervar.cpp against repo@97c732c headers). Sets farm the base sets of /ssd/tiers2.
set -euo pipefail
V=/ssd/tiers2-verify
B=/ssd/tiers2/build/out-dump/planet
n=$1; ms=$2; qs=$3
export OMP_NUM_THREADS=20
mkdir -p $V/var/$n/min $V/var/$n/q
$V/bin/tiervar minimal /ssd/tiers2/dump $B/admin $V/var/$n/min "$ms" > $V/var/$n/min.json
$V/bin/tiervar quality /ssd/tiers2/dump $V/var/$n/q "$qs" > $V/var/$n/q.json
rm -f $V/var/$n/min/admin_parents.bin
for s in smallest recommended; do
  d=$V/sets/$n/$s; rm -rf $d; mkdir -p $d
  for f in /ssd/tiers2/sets/base/$s/*; do ln -s "$(readlink -f "$f")" "$d/$(basename "$f")"; done
done
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin; do ln -sfn $V/var/$n/min/$f $V/sets/$n/smallest/$f; done
for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -sfn $V/var/$n/q/$f $V/sets/$n/recommended/$f; done
echo "MKV_DONE $n"
