#!/bin/bash
# batch1.sh : the first candidate batch (cand.sh each, then sizes.py for all). Log: logs/batch1.log
set -u
cd /ssd/tiers2
C=(
  "uni37|uni=37.5"
  "uni50|uni=50"
  "uni75|uni=75"
  "uni100|uni=100"
  "uni37g4|uni=37.5;grid=1e-4"
  "uni75g2|uni=75;grid=2e-4"
  "q25g4|scale=2.5;grid=1e-4"
  "max250|scale=2.5;max=250"
  "max150|scale=2.5;max=150"
  "max100|scale=2.5;max=100"
  "max250c|scale=2.5;max=250;cap=0.1"
  "max100c|scale=2.5;max=100;cap=0.1"
  "cap05|scale=2.5;cap=0.05"
  "cap20|scale=2.5;cap=0.2"
  "rel1|rel=0.001,37.5,250"
)
G=(
  "cg2|scale=2.5;t2=2500;t3=1000;t4=500;cap=0.1"
  "cg4|scale=2.5;t2=5000;t3=2000;t4=1000;cap=0.1"
)
names=""
for c in "${C[@]}"; do
  n=${c%%|*}; s=${c#*|}
  ./cand.sh "$n" "$s"
  names="$names $n"
done
for c in "${G[@]}"; do
  n=${c%%|*}; s=${c#*|}
  WITHCHAIN=1 ./cand.sh "$n" "$s"
  names="$names $n"
done
JOBS=16 /ssd/shrink/venv/bin/python -I sizes.py q25 cap10 unc $names
echo BATCH1_DONE
