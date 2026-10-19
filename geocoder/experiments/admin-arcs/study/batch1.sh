#!/bin/bash
# batch1.sh : the truth, the per-polygon baselines and the arc candidates on the d1006 build,
# then their sizes. Candidates: name|kind|admin-minimal scheme|quality-dir scheme.
set -u
cd /ssd/arcs
L9="t9=37.5;t10=37.5;t11=37.5;t12=37.5;t13=37.5;t14=37.5;t15=37.5"
C=(
  "unc|pp|none|none"
  "q25|pp|scale=2.5|scale=2.5"
  "q25c|pp|scale=2.5;cap=0.1|scale=2.5;cap=0.1"
  "f37|pp|uni=37.5|uni=37.5"
  "r50c|pp|uni=75;t8=50;cap=0.1|uni=75;t8=50;$L9;cap=0.1"
  "r75c|pp|uni=75;cap=0.1|uni=75;$L9;cap=0.1"
  "aq25|arc|scale=2.5|scale=2.5"
  "aq25c|arc|scale=2.5;cap=0.1|scale=2.5;cap=0.1"
  "af37|arc|uni=37.5|uni=37.5"
  "af37c|arc|uni=37.5;cap=0.1|uni=37.5;cap=0.1"
  "ar50c|arc|uni=75;t8=50;cap=0.1|uni=75;t8=50;$L9;cap=0.1"
  "ar75c|arc|uni=75;cap=0.1|uni=75;$L9;cap=0.1"
  "af50c|arc|uni=50;cap=0.1|uni=50;$L9;cap=0.1"
  "ar37c|arc|uni=75;t8=37.5;cap=0.1|uni=75;t8=37.5;$L9;cap=0.1"
  "ar50cg4|arc|uni=75;t8=50;cap=0.1;q=1e-4|uni=75;t8=50;$L9;cap=0.1;q=1e-4"
  "af37cg4|arc|uni=37.5;cap=0.1;q=1e-4|uni=37.5;cap=0.1;q=1e-4"
)
names=""
for c in "${C[@]}"; do
  IFS='|' read -r n k ms qs <<< "$c"
  if [ -f logs/$n.done ]; then names="$names $n"; continue; fi
  ./cand.sh "$n" "$k" "$ms" "$qs" && touch logs/$n.done
  grep -E "SCORE|L2C|SHORT" logs/$n.log
  names="$names $n"
done
JOBS=8 /ssd/shrink/venv/bin/python -I sizes.py $names > cmp/sizes_batch1.txt 2>&1
cat cmp/sizes_batch1.txt
echo BATCH1_DONE
