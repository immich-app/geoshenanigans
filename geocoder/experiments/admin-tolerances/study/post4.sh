#!/bin/bash
# post4.sh : the clean builder change (feat/admin-tier-tolerances, bin-final, no env) against tiervar3-n
# with scheme r1 on an Oceania extract: the study builder (bin-builder2, baseline) dumps the extract's
# final admin arrays (GC_TIER_DUMP), tiervar3-n re-simplifies them at r1, the final builder builds the
# extract; admin-minimal (cold slots) and quality/q2.5 admin files must be byte-identical.
set -u
T=/ssd/tiers3
cd $T
R1="t2=82;t3=400;t4=90;t5=175;t6=175;t7=54;t8=45;cap=0.1;iso=1;cityovr=50"
Q=";t9=42;t10=46;t11=47;t15=47"
PBF=/ssd/work/oceania.osm.pbf
export TMPDIR=$T/tmp OMP_NUM_THREADS=16
rm -rf build/oc-base build/oc-final2 dumpOC tmp/ocr1
GC_TIER_DUMP=$T/dumpOC nice -n 10 bin-builder2/build-index build/oc-base $PBF --multi-output --multi-quality > logs/oc-base.log 2>&1
echo "base rc=$?"
nice -n 10 bin-final/build-index build/oc-final2 $PBF --multi-output --multi-quality > logs/oc-final2.log 2>&1
echo "final rc=$?"
mkdir -p tmp/ocr1/min tmp/ocr1/q
bin/tiervar3-n minimal dumpOC build/oc-base/planet/admin tmp/ocr1/min "$R1" > /dev/null
bin/tiervar3-n quality dumpOC tmp/ocr1/q "$R1$Q" > /dev/null
same=0; diff=0
for f in admin_cells admin_entries admin_polygons admin_vertices; do
  if cmp -s tmp/ocr1/min/$f.bin build/oc-final2/planet/admin-minimal/$f.bin; then same=$((same+1)); else diff=$((diff+1)); echo "DIFF admin-minimal/$f.bin"; fi
done
for f in admin_polygons admin_vertices postal_polygons postal_vertices; do
  if cmp -s tmp/ocr1/q/$f.bin build/oc-final2/planet/quality/q2.5/$f.bin; then same=$((same+1)); else diff=$((diff+1)); echo "DIFF quality/q2.5/$f.bin"; fi
done
cmp -s build/oc-base/planet/quality/q2.5/admin_vertices.bin build/oc-final2/planet/quality/q2.5/admin_vertices.bin && echo "WARNING final q2.5 equals baseline" || echo "final q2.5 differs from the baseline (expected)"
echo "OCEANIA final builder vs tiervar r1: identical $same, different $diff"
echo POST4_DONE
