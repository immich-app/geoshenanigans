#!/bin/bash
# master.sh : after batch1 and the d1005 planet build, run the gaps/overlaps (batch3), the patch
# study (batch2) and last the read cost (batch4: timing wants the box quiet from this track).
set -u
cd /ssd/arcs
until grep -q BATCH1_DONE logs/batch1.out 2>/dev/null; do sleep 30; done
./batch3.sh q25 q25c f37 r50c r75c aq25 aq25c af37 af37c ar50c ar75c af50c ar37c ar50cg4 af37cg4 > logs/batch3.out 2>&1
until grep -q "RUN_DONE d1005" build/planets.nohup.log 2>/dev/null; do sleep 30; done
./mkbase.sh d1005 > /dev/null 2>&1
./batch2.sh > logs/batch2.out 2>&1
./batch4.sh > logs/batch4.out 2>&1
echo MASTER_DONE >> logs/batch4.out
