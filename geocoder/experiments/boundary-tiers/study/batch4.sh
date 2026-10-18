#!/bin/bash
# batch4.sh : uniform 75 m with L8 kept at q2.5's 37.5 m (+ sub-city levels at 37.5 m), cap 0.1.
set -u
cd /ssd/tiers2
R="t9=37.5;t10=37.5;t11=37.5;t12=37.5;t13=37.5;t14=37.5;t15=37.5"
nice -n 19 ./cand.sh uni75t837rc10 "uni=75;t8=37.5;$R;cap=0.1"
JOBS=8 nice -n 19 /ssd/shrink/venv/bin/python -I sizes.py uni75t837rc10
for s in smallest recommended; do JOBS=2 nice -n 19 /ssd/shrink/venv/bin/python -I score.py $s uni75t837rc10 > cmp/score_b4_$s.log; done
nice -n 19 ./l2c.sh uni75t837rc10 > logs/l2c_b4.log 2>&1
echo BATCH4_DONE
