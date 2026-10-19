#!/bin/bash
# batch4.sh : read cost, single-thread timing under the lock (work counters: workq.sh, run before).
#   r0 = today's reader (fix/admin-nesting), r1 = + search-prod's three answer-preserving admin
#   reader commits (decode only for PIP, PIP on encoded rings, specialised loop), r2 = r1's
#   binary on the arc format (sets/<name>a).
set -u
cd /ssd/arcs
for st in smallest recommended; do
  ./timeq.sh $st q25_r0=r0:q25 r50c_r0=r0:r50c q25_r1=r1:q25 r50c_r1=r1:r50c r75c_r1=r1:r75c f37_r1=r1:f37 \
    af37c_dec_r1=r1:af37c aq25c_r2=r2:aq25ca af37c_r2=r2:af37ca af37cnb_r2=r2:af37cnba ar50c_r2=r2:ar50ca ar75c_r2=r2:ar75ca
done
echo BATCH4_DONE
