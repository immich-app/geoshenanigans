#!/bin/bash
# cand6.sh <name> <minimal scheme> [quality scheme] : one planet-261006 candidate end to end: mkset6.sh,
# real-reader answers on every pool for both sets (runq.sh), sizes (raw + zstd 1.5.7 -18 -T1 of the
# 4 admin files per set -> cmp/size_<name>.tsv), score3.py -> cmp/score_<name>.json. Log logs/c_<name>.log.
set -u
T=/ssd/tiers3
cd $T
name=$1
Z=/ssd/shrink/zstd157/bin/zstd
{
  ./mkset6.sh "$@"
  for s in smallest recommended; do JOBS=${JOBS:-12} ./runq.sh ${name}_$s sets6/$name/$s; done
  : > cmp/size_$name.tsv
  for f in min/admin_vertices.bin min/admin_polygons.bin min/admin_cells.bin min/admin_entries.bin \
           q/admin_vertices.bin q/admin_polygons.bin q/postal_vertices.bin q/postal_polygons.bin; do
    p=var6/$name/$f
    printf "%s\t%s\t%s\n" "$f" "$(stat -c%s $p)" "$($Z -18 -T1 -q -c $p | wc -c)" >> cmp/size_$name.tsv &
  done
  wait
  /ssd/shrink/venv/bin/python -I score3.py $name
} > logs/c_$name.log 2>&1
echo "CAND_DONE $name"
