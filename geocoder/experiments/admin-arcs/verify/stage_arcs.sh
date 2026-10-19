#!/bin/bash
# stage_arcs.sh : planet-261006 (arcs track's d1006 build, fix/admin-nesting + dump hooks) sets and
# answers for the arcs check, with verify5's regenerated af37cb8 (arcs_regen.sh).
#   d6base  = the build's own files (per-polygon q2.5, no cap), client selection (assemble_sets.py)
#   d6unc   = truth: arcs track's tiervar "none" admin-minimal (cold slots) + the build's quality/uncapped
#   d6r50c, d6f37 = per-polygon 75 m / L8 50 m + cap and flat 37.5 m (arcs track's tiervar)
#   d6af    = af37cb8 decoded back to today's files (arcs tool dec/)
#   d6afa   = af37cb8 in the arc format; admin_vertices.bin and postal_vertices.bin replaced by 16-byte
#             0xFF dummies so any read of them would fail or change answers
# Answers: nest (fix/admin-nesting reader) on every per-polygon / decoded set; arcs reader (r5,
# arcs-reader e566648) on d6afa and on d6base.
set -u
V=/ssd/verify5
A=/ssd/arcs
B=$A/build/out-d1006
D=$A/build/d1006/dump
cd $V
until grep -q ARCS_REGEN_DONE logs/arcs_regen.log 2>/dev/null; do sleep 20; done
asm() { rm -rf sets/$1; mkdir -p sets/$1; /ssd/shrink/venv/bin/python -I /ssd/bugs2/probe/assemble_sets.py $B sets/$1 > /dev/null; rm -rf sets/$1/max; }
export OMP_NUM_THREADS=16
asm d6base
for n in d6unc d6r50c d6f37; do
  case $n in
    d6unc) ms=none; qs=none ;;
    d6r50c) ms="uni=75;t8=50;cap=0.1"; qs="uni=75;t8=50;t9=37.5;t10=37.5;t11=37.5;t12=37.5;t13=37.5;t14=37.5;t15=37.5;cap=0.1" ;;
    d6f37) ms="uni=37.5"; qs="uni=37.5" ;;
  esac
  rm -rf var/$n; mkdir -p var/$n/min var/$n/q
  $A/tiervar minimal $D $B/planet/admin var/$n/min "$ms" > var/$n/min.json
  asm $n
  for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin; do ln -sfn $V/var/$n/min/$f sets/$n/smallest/$f; done
  if [ $n = d6unc ]; then
    for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -sfn $B/planet/quality/uncapped/$f sets/$n/recommended/$f; done
  else
    $A/tiervar quality $D var/$n/q "$qs" > var/$n/q.json
    for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -sfn $V/var/$n/q/$f sets/$n/recommended/$f; done
  fi
done
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin; do
  cmp -s var/d6unc/min/$f $A/var/unc/min/$f && echo "unc $f IDENTICAL to arcs track's" || echo "unc $f DIFFERENT"
done
X=$V/arcs/af37cb8
asm d6af
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin; do ln -sfn $X/min/dec/$f sets/d6af/smallest/$f; done
for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -sfn $X/q/dec/$f sets/d6af/recommended/$f; done
rm -rf sets/d6afa; mkdir -p sets/d6afa
head -c 16 /dev/zero | tr '\0' '\377' > $V/tmp/dummy16.bin
for st in smallest recommended; do
  part=$([ $st = smallest ] && echo min || echo q)
  cp -P -r sets/d6af/$st sets/d6afa/$st
  for f in arc_vertices.bin arc_index.bin poly_arcs.bin arc_meta.json admin_polygons.bin; do ln -sfn $X/$part/arc/$f sets/d6afa/$st/$f; done
  ln -sfn $V/tmp/dummy16.bin sets/d6afa/$st/admin_vertices.bin
done
ln -sfn $X/q/arc/postal_polygons.bin sets/d6afa/recommended/postal_polygons.bin
ln -sfn $V/tmp/dummy16.bin sets/d6afa/recommended/postal_vertices.bin
P="pts/d6/fresh/*.txt pts/d6/big/*.txt"
for n in d6unc d6base d6r50c d6f37 d6af; do
  for st in smallest recommended; do JOBS=24 ./v5q.sh nest ${n}_$st sets/$n/$st $P; done
done
for st in smallest recommended; do
  JOBS=24 ./v5q.sh arcs d6afa_r5_$st sets/d6afa/$st $P
  JOBS=24 ./v5q.sh arcs d6base_r5_$st sets/d6base/$st $P
  JOBS=24 ./v5q.sh nest3 d6base_n3_$st sets/d6base/$st $P
done
echo STAGE_ARCS_DONE
