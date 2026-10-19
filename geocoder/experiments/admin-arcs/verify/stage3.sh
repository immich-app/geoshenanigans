#!/bin/bash
# stage3.sh : arcs against per-polygon q2.5+cap and r1 on the SAME planet-261006 build (arcs track's
# d1006 dump, fix/admin-nesting + dump hooks), all with tiers3's tiervar3-n (cold admin-minimal slots):
#   d6qcap = "scale=2.5;cap=0.1"   (checked byte for byte against the arcs track's own q25c)
#   d6r1   = the r1 table of feat/admin-tier-tolerances (+ t9..t15 for the quality dir)
# Sets assembled as stage_arcs.sh did for d6r50c / d6f37. Then nest-reader answers on verify5's fresh
# d6 pools, raw/zstd sizes, planned Bg sizes, and same-level gaps/overlaps (the arcs track's committed
# cover tool, e566648, on verify5's own 70k border points) for every d6 set against d6unc.
# Runs beside another track's timing: nice 19, few processes.
set -u
V=/ssd/verify5
A=/ssd/arcs
B=$A/build/out-d1006
D=$A/build/d1006/dump
TV=/ssd/tiers3/bin/tiervar3-n
cd $V
export OMP_NUM_THREADS=8 TMPDIR=$V/tmp
asm() { rm -rf sets/$1; mkdir -p sets/$1; /ssd/shrink/venv/bin/python -I /ssd/bugs2/probe/assemble_sets.py $B sets/$1 > /dev/null; rm -rf sets/$1/max; }
R1="t2=82;t3=400;t4=90;t5=175;t6=175;t7=54;t8=45;cap=0.1;iso=1;cityovr=50"
Q9=";t9=42;t10=46;t11=47;t15=47"
for n in d6qcap d6r1; do
  case $n in
    d6qcap) ms="scale=2.5;cap=0.1"; qs="scale=2.5;cap=0.1" ;;
    d6r1) ms="$R1"; qs="$R1$Q9" ;;
  esac
  rm -rf var/$n; mkdir -p var/$n/min var/$n/q
  /usr/bin/time -f "%e s %M KB" -o var/$n/min.time nice -n 19 $TV minimal $D $B/planet/admin var/$n/min "$ms" > var/$n/min.json
  /usr/bin/time -f "%e s %M KB" -o var/$n/q.time nice -n 19 $TV quality $D var/$n/q "$qs" > var/$n/q.json
  asm $n
  for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin; do ln -sfn $V/var/$n/min/$f sets/$n/smallest/$f; done
  for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -sfn $V/var/$n/q/$f sets/$n/recommended/$f; done
  echo "MKSET3_DONE $n"
done
for f in admin_cells.bin admin_entries.bin admin_polygons.bin admin_vertices.bin; do
  cmp -s var/d6qcap/min/$f $A/var/q25c/min/$f && echo "d6qcap min/$f IDENTICAL to arcs q25c" || echo "d6qcap min/$f DIFFERENT from arcs q25c"
done
for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do
  cmp -s var/d6qcap/q/$f $A/var/q25c/q/$f && echo "d6qcap q/$f IDENTICAL to arcs q25c" || echo "d6qcap q/$f DIFFERENT from arcs q25c"
done
P="pts/d6/fresh/*.txt pts/d6/big/*.txt"
for n in d6qcap d6r1; do
  for st in smallest recommended; do NICE=19 JOBS=8 ./v5q.sh nest ${n}_$st sets/$n/$st $P; done
done
./sizes.sh d6qcap d6r1 > logs/sizes3.log 2>&1
PAR=4 ./planned.sh d6qcap d6r1 >> logs/sizes3.log 2>&1
cat logs/sizes3.log
# gaps / overlaps: committed cover tool, verify5's border points without the level column
[ -x bin/cover ] || g++ -O3 -march=native -fopenmp -std=c++20 wt-arcs/geocoder/builder/tools/arcs_study/cover.cpp -o bin/cover
cut -d" " -f1,2 pts/d6/big/bb70k.txt > pts/d6/bb70k_2col.txt
for n in d6unc d6base d6qcap d6r1 d6r50c d6f37 d6af; do
  nice -n 19 bin/cover sets/d6unc/smallest sets/$n/smallest pts/d6/bb70k_2col.txt > cmp/cover_$n.json
  echo "COVER $n $(cat cmp/cover_$n.json)"
done
echo STAGE3_DONE
