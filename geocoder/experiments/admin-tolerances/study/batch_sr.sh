#!/bin/bash
# batch_sr.sh : role-aware size-first scheme sr1 (as s5i, with city-named polygons kept at the L8
# tolerance), both planets, both sets, and its daily patch.
cd /ssd/tiers3
Q=";t9=42;t10=46;t11=47;t15=47"
SR="t2=90;t3=400;t4=100;t5=200;t6=200;t7=82;t8=66;cap=0.1;iso=1;cityovr=66"
./evalD1.sh "sr1|$SR|$SR$Q" > logs/evalD1_sr.log 2>&1 &
JOBS=10 ./cand6.sh sr1 "$SR" "$SR$Q"
./patch3.sh sr1 "$SR" "$SR$Q" > /dev/null
./bg3.sh sr1 > /dev/null
wait
echo BATCH_SR_DONE
