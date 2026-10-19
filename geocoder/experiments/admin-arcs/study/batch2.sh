#!/bin/bash
# batch2.sh : day-to-day change (patch study). The same schemes on the d1005 build (planet-261005,
# the day before planet-261006), files only, then patchcmp.py of each pair (old = d1005, new = d1006).
set -u
cd /ssd/arcs
L9="t9=37.5;t10=37.5;t11=37.5;t12=37.5;t13=37.5;t14=37.5;t15=37.5"
P=(
  "q25c|aq25c|scale=2.5;cap=0.1|scale=2.5;cap=0.1"
  "r50c|ar50c|uni=75;t8=50;cap=0.1|uni=75;t8=50;$L9;cap=0.1"
  "r75c|ar75c|uni=75;cap=0.1|uni=75;$L9;cap=0.1"
)
for p in "${P[@]}"; do
  IFS='|' read -r pp arc ms qs <<< "$p"
  BUILD=d1005 SKIPSETS=1 ./mkvar.sh d1005_$pp pp "$ms" "$qs" > logs/d1005_$pp.log 2>&1
  BUILD=d1005 SKIPSETS=1 ./mkvar.sh d1005_$arc arc "$ms" "$qs" > logs/d1005_$arc.log 2>&1
  /ssd/shrink/venv/bin/python -I patchcmp.py var/d1005_$arc var/$arc var/d1005_$pp var/$pp build/d1005/dump build/d1006/dump > cmp/patch_$pp.json
  echo "PATCH $pp $(cat cmp/patch_$pp.json)"
done
echo BATCH2_DONE
