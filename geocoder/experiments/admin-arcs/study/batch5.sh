#!/bin/bash
# batch5.sh : timing of r3 (r1 + the integer straddle test, both formats; same answers) with an r1
# control in the same lock hold. Output cmp/time5_<set>.tsv via timeq.sh (it appends to
# cmp/time_<set>.tsv; the rows are copied out afterwards).
set -u
cd /ssd/arcs
for st in smallest recommended; do
  before=$(wc -l < cmp/time_$st.tsv)
  ./timeq.sh $st q25_r1b=r1:q25 q25_r3=r3:q25 r50c_r3=r3:r50c r75c_r3=r3:r75c f37_r3=r3:f37 \
    aq25c_r3=r3:aq25ca af37c_r3=r3:af37ca ar50c_r3=r3:ar50ca ar75c_r3=r3:ar75ca
  tail -n +$((before + 1)) cmp/time_$st.tsv > cmp/time5_$st.tsv
done
echo BATCH5_DONE
