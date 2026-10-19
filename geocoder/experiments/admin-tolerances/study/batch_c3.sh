#!/bin/bash
# batch_c3.sh : final real-reader candidates (planet-261006), isotropic DP (iso=1) + cap 0.1:
#   d1  = c5i with L8 44.5 m (L8 city back to q2.5+cap's on B8)
#   s3i = size-first, iso version of s3 (L7 66, L8 55)
#   s5i = smaller size-first (L6 165, L7 82, L8 66)
cd /ssd/tiers3
run() { JOBS=12 ./cand6.sh "$@"; }
run d1 "t2=82;t3=275;t4=82;t5=165;t6=138;t7=54;t8=44.5;cap=0.1;iso=1" &
run s3i "t2=82;t3=275;t4=82;t5=110;t6=138;t7=66;t8=55;cap=0.1;iso=1"
wait
run s5i "t2=82;t3=275;t4=82;t5=165;t6=165;t7=82;t8=66;cap=0.1;iso=1"
echo BATCH_C3_DONE
