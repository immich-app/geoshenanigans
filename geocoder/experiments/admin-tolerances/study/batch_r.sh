#!/bin/bash
# batch_r.sh : role-aware variants of d2 (cityovr: polygons the reader names as city / town / village,
# place_type_override 1-3, keep a city tolerance whatever their level; China's county-level cities
# are level 6). Paid for by coarser country/state/L5/L6. Both planets, both sets.
cd /ssd/tiers3
Q=";t9=42;t10=46;t11=47;t15=47"
R1="t2=82;t3=400;t4=90;t5=175;t6=175;t7=54;t8=45;cap=0.1;iso=1;cityovr=50"
R2="t2=90;t3=400;t4=100;t5=200;t6=200;t7=54;t8=44.5;cap=0.1;iso=1;cityovr=44.5"
./evalD1.sh "r1|$R1|$R1$Q" "r2|$R2|$R2$Q" > logs/evalD1_r.log 2>&1 &
JOBS=10 ./cand6.sh r1 "$R1" "$R1$Q"
JOBS=10 ./cand6.sh r2 "$R2" "$R2$Q"
wait
echo BATCH_R_DONE
