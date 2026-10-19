#!/bin/bash
# batch_c2.sh : second real-reader batch around c5 (planet-261006): finer L5 for photo-like state
# answers, an isotropic (iso=1) version of c5 at the same bytes, and size-first variants.
cd /ssd/tiers3
run() { JOBS=12 ./cand6.sh "$@"; }
run c6 "t2=90;t3=400;t4=90;t5=100;t6=125;t7=50;t8=37.5;cap=0.1" &
run c7 "t2=75;t3=250;t4=75;t5=100;t6=150;t7=50;t8=37.5;cap=0.1"
wait
run c9 "t2=75;t3=250;t4=75;t5=125;t6=140;t7=50;t8=37.5;cap=0.1" &
run c5i "t2=82;t3=275;t4=82;t5=165;t6=138;t7=54;t8=45;cap=0.1;iso=1"
wait
run s3 "t2=75;t3=250;t4=75;t5=100;t6=125;t7=60;t8=50;cap=0.1" &
run s4 "t2=75;t3=250;t4=75;t5=100;t6=150;t7=75;t8=60;cap=0.1"
wait
echo BATCH_C2_DONE
