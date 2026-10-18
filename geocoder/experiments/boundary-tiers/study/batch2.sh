#!/bin/bash
# batch2.sh : uniform-75 neighbourhood + caps, and the chain reader with the nesting flags
# (TIERS2_NEST, nestcheck at 2 m). Log: logs/batch2.log
set -u
cd /ssd/tiers2
C=(
  "uni75c10|uni=75;cap=0.1"
  "uni75c05|uni=75;cap=0.05"
  "uni60|uni=60"
  "uni60c10|uni=60;cap=0.1"
  "uni90c10|uni=90;cap=0.1"
  "uni100c10|uni=100;cap=0.1"
  "uni75t8c10|uni=75;t8=50;cap=0.1"
  "uni75g2c10|uni=75;grid=2e-4;cap=0.1"
  "uni75g4c10|uni=75;grid=1e-4;cap=0.1"
)
N=(
  "uncn|none"
  "cap10n|scale=2.5;cap=0.1"
  "cg2n|scale=2.5;t2=2500;t3=1000;t4=500;cap=0.1"
  "cg4n|scale=2.5;t2=5000;t3=2000;t4=1000;cap=0.1"
  "uni75c10n|uni=75;cap=0.1"
)
names=""
for c in "${C[@]}"; do
  n=${c%%|*}; s=${c#*|}
  nice -n 19 ./cand.sh "$n" "$s"
  names="$names $n"
done
for c in "${N[@]}"; do
  n=${c%%|*}; s=${c#*|}
  TIERS2_NEST=/ssd/tiers2/nest2.flags WITHCHAIN=1 nice -n 19 ./cand.sh "$n" "$s"
  names="$names $n"
done
JOBS=10 nice -n 19 /ssd/shrink/venv/bin/python -I sizes.py $names
echo BATCH2_DONE
