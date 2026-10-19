#!/bin/bash
# stage2.sh : after planets.sh. Byte checks of the real builds, sets, sizes, answers, reader
# equivalence, real daily patches, then the timing runs (one lock hold).
set -u
V=/ssd/verify5
cd $V
until grep -q PLANETS_DONE logs/planets.log 2>/dev/null; do sleep 30; done
until grep -q STAGE1_DONE logs/stage1.log 2>/dev/null; do sleep 30; done
T1=/ssd/tiers3/build/out-D1/planet
B=$V/build
{
  echo "# qD1 (verify5 bq 4f87ab6 build) vs tiers3 out-D1, every planet file of admin-minimal, quality/q2.5, admin"
  for sub in admin-minimal quality/q2.5 quality/uncapped admin; do
    for f in $T1/$sub/*; do b=$(basename $f); cmp -s $f $B/out-qD1/planet/$sub/$b && r=IDENTICAL || r=DIFFERENT; echo "qD1 $sub/$b $r"; done
  done
  echo "# r1D1 (verify5 br1 bdac8cd build) vs tiervar3-n r1 (= tiers3 varD1/r1)"
  for f in admin_vertices.bin admin_polygons.bin admin_cells.bin admin_entries.bin; do
    cmp -s $B/out-r1D1/planet/admin-minimal/$f var/r1T/min/$f && r=IDENTICAL || r="DIFFERENT $(cmp $B/out-r1D1/planet/admin-minimal/$f var/r1T/min/$f 2>&1 | head -1)"; echo "r1D1 admin-minimal/$f $r"
  done
  for f in admin_vertices.bin admin_polygons.bin postal_vertices.bin postal_polygons.bin; do
    cmp -s $B/out-r1D1/planet/quality/q2.5/$f var/r1T/q/$f && r=IDENTICAL || r="DIFFERENT $(cmp $B/out-r1D1/planet/quality/q2.5/$f var/r1T/q/$f 2>&1 | head -1)"; echo "r1D1 quality/q2.5/$f $r"
  done
  echo "# r1D1 vs qD1: files outside the tolerance that should not change"
  for sub in admin admin-minimal quality/uncapped; do
    for f in $B/out-qD1/planet/$sub/*; do b=$(basename $f); cmp -s $f $B/out-r1D1/planet/$sub/$b || echo "r1D1 vs qD1 differs: $sub/$b"; done
  done
} > cmp/bytecheck.txt 2>&1
./mkset.sh qcapB $B/out-qD1
./mkset.sh r1B $B/out-r1D1
./sizes.sh qcapB r1B > logs/sizes2.log 2>&1
P="pts/D1/fresh/*.txt pts/D1/big/*.txt"
for s in qcapB r1B; do for st in smallest recommended; do JOBS=24 ./v5q.sh nest D1_${s}_$st sets/$s/$st $P; done; done
# reader equivalence on the fresh 20k (and the big pools): main / psp / pspr / nest3 against nest and each other
for s in qcapB r1B; do for st in smallest recommended; do
  for r in main psp pspr nest3; do JOBS=24 ./v5q.sh $r eq_${r}_${s}_$st sets/$s/$st $P; done
done; done
./patch.sh r1 $B/out-r1D $B/out-r1D1w > logs/patch_r1.log 2>&1
./patch.sh base /ssd/tiers3/build/out-D /ssd/tiers3/build/out-D1 > logs/patch_base.log 2>&1
./patch.sh qB /ssd/tiers3/build/out-D $B/out-qD1 > logs/patch_qB.log 2>&1
echo STAGE2_PRE_DONE
until grep -q STAGE_ARCS_DONE logs/stage_arcs.log 2>/dev/null; do sleep 30; done
L=/ssd/shrink/sortpatch/TIMING.lock
until (set -o noclobber; echo "verify5 timing $(date -Is)" > $L) 2> /dev/null; do sleep 15; done
trap 'rm -f $L' EXIT
export HOLDS_LOCK=1
S=$V/sets
items=()
for st in smallest recommended; do
  for r in main psp pspr nest nest3; do
    items+=("D1_q_${st}_$r=$r:$S/qcapB/$st" "D1_r1_${st}_$r=$r:$S/r1B/$st")
  done
done
./timing.sh cmp/time_D1.tsv pts/time/D1_p50k.txt pts/time/D1_L2.txt pts/time/D1_cold2k.txt "${items[@]}"
items=()
for st in smallest recommended; do
  items+=("d6_q25_${st}_r0=nest:$S/d6base/$st" "d6_q25_${st}_n3=nest3:$S/d6base/$st" "d6_q25_${st}_r5=arcs:$S/d6base/$st"
          "d6_af_${st}_r5=arcs:$S/d6afa/$st" "d6_r50c_${st}_r0=nest:$S/d6r50c/$st" "d6_r50c_${st}_r5=arcs:$S/d6r50c/$st")
done
./timing.sh cmp/time_d6.tsv pts/time/d6_p50k.txt pts/time/d6_L2.txt pts/time/d6_cold10k.txt "${items[@]}"
echo STAGE2_DONE
