#!/bin/bash
# batch_c1.sh : first real-reader candidates from the tierscan frontier (planet-261006, aniso DP + cap 0.1,
# L9+ 37.5 m in the quality dir). Two candidates at a time.
cd /ssd/tiers3
run() { JOBS=12 ./cand6.sh "$@"; }
run c1 "t2=37.5;t3=150;t4=50;t5=125;t6=200;t7=50;t8=37.5;cap=0.1" &
run c2 "t2=125;t3=400;t4=75;t5=75;t6=150;t7=50;t8=37.5;cap=0.1"
wait
run c3 "t2=30;t3=125;t4=45;t5=200;t6=200;t7=50;t8=37.5;cap=0.1" &
run c5 "t2=75;t3=250;t4=75;t5=150;t6=125;t7=50;t8=37.5;cap=0.1"
wait
run c4 "t2=60;t3=150;t4=60;t5=125;t6=175;t7=50;t8=37.5;cap=0.1" &
run s1 "t2=37.5;t3=125;t4=45;t5=90;t6=150;t7=75;t8=60;cap=0.1"
wait
run s2 "t2=75;t3=250;t4=75;t5=150;t6=200;t7=60;t8=50;cap=0.1"
echo BATCH_C1_DONE
