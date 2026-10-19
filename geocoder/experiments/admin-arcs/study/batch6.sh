#!/bin/bash
# batch6.sh : timing of r5 (r3 + chunk bboxes in poly_arcs, chunk = 8 refs) on af37cb8 with r3
# controls in the same lock hold -> cmp/time6_<set>.tsv.
set -u
cd /ssd/arcs
for st in smallest recommended; do
  before=$(wc -l < cmp/time_$st.tsv)
  ./timeq.sh $st q25_r3b=r3:q25 r50c_r3b=r3:r50c af37c_r3b=r3:af37ca af37cb8_r5=r5:af37cb8a
  tail -n +$((before + 1)) cmp/time_$st.tsv > cmp/time6_$st.tsv
done
echo BATCH6_DONE
