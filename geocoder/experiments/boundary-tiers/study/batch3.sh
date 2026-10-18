#!/bin/bash
# batch3.sh : uniform 75 on the city-and-above levels with the recommended set's sub-city levels
# (L9 borough, L10 quarter, L11 postal, L15 place areas) kept at q2.5's 37.5 m, and the matched
# 11 m grid on the 50 m-L8 variant. Log: logs/batch3.log
set -u
cd /ssd/tiers2
R="t9=37.5;t10=37.5;t11=37.5;t12=37.5;t13=37.5;t14=37.5;t15=37.5"
C=(
  "uni75rc10|uni=75;$R;cap=0.1"
  "uni75t8rc10|uni=75;t8=50;$R;cap=0.1"
  "uni75t8g4c10|uni=75;t8=50;grid=1e-4;cap=0.1"
  "uni75t8rg4c10|uni=75;t8=50;$R;grid=1e-4;cap=0.1"
)
names=""
for c in "${C[@]}"; do
  n=${c%%|*}; s=${c#*|}
  nice -n 19 ./cand.sh "$n" "$s"
  names="$names $n"
done
JOBS=8 nice -n 19 /ssd/shrink/venv/bin/python -I sizes.py $names
for s in smallest recommended; do JOBS=4 nice -n 19 /ssd/shrink/venv/bin/python -I score.py $s $names > cmp/score_b3_$s.log; done
nice -n 19 ./l2c.sh $names > logs/l2c_b3.log 2>&1
echo BATCH3_DONE
