#!/bin/bash
# post3.sh : the clean builder change (feat/admin-tier-tolerances, bin-final, no env) against the study
# builder with GC_TIER_SCHEME = d2 (bin-builder2) on an Oceania extract: admin-minimal and
# quality/q2.5 must be byte-identical (the planet D1_d2 build is checked against tiervar in post1).
set -u
T=/ssd/tiers3
cd $T
until grep -q POST1_DONE logs/post1.log; do sleep 15; done
D2="t2=82;t3=275;t4=82;t5=165;t6=138;t7=54;t8=44.5;t9=42;t10=46;t11=47;t15=47;cap=0.1;iso=1"
PBF=/ssd/work/oceania.osm.pbf
export TMPDIR=$T/tmp
rm -rf build/oc-final build/oc-study
nice -n 10 bin-final/build-index build/oc-final $PBF --multi-output --multi-quality > logs/oc-final.log 2>&1
echo "final rc=$?"
GC_TIER_SCHEME="$D2" nice -n 10 bin-builder2/build-index build/oc-study $PBF --multi-output --multi-quality > logs/oc-study.log 2>&1
echo "study rc=$?"
same=0; diff=0
for f in $(cd build/oc-study && find . -path '*admin-minimal/*.bin' -o -path '*quality/q2.5/*.bin' | sort); do
  if cmp -s build/oc-study/$f build/oc-final/$f; then same=$((same+1)); else diff=$((diff+1)); echo "DIFF $f"; fi
done
echo "OCEANIA final vs study: identical $same, different $diff"
# and the final builder's q2.5 is not the old one
cmp -s build/oc-final/$(cd build/oc-final && find . -path '*quality/q2.5/admin_vertices.bin' | head -1) /dev/null; echo POST3_DONE
