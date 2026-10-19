#!/bin/bash
# batch_c3b.sh : the iso candidates again with their quality-dir L9-L11 tolerances matched to aniso
# 37.5 m in bytes (iso: L9 42, L10 45, L11 42); smallest is unchanged by this.
cd /ssd/tiers3
until grep -q BATCH_C3_DONE logs/batch_c3.log; do sleep 10; done
Q=";t9=42;t10=45;t11=42"
run() { JOBS=12 ./cand6.sh "$1" "$2" "$2$Q"; }
run c5i "t2=82;t3=275;t4=82;t5=165;t6=138;t7=54;t8=45;cap=0.1;iso=1" &
run d1 "t2=82;t3=275;t4=82;t5=165;t6=138;t7=54;t8=44.5;cap=0.1;iso=1"
wait
run s3i "t2=82;t3=275;t4=82;t5=110;t6=138;t7=66;t8=55;cap=0.1;iso=1" &
run s5i "t2=82;t3=275;t4=82;t5=165;t6=165;t7=82;t8=66;cap=0.1;iso=1"
wait
echo BATCH_C3B_DONE
