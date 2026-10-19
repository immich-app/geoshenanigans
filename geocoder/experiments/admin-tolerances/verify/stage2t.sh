#!/bin/bash
# stage2t.sh : the timing tail of stage2.sh (same items), restarted with a 1 s lock poll so it is not
# starved by tracks that poll more often. One lock hold for both planets' timing runs.
set -u
V=/ssd/verify5
cd $V
L=/ssd/shrink/sortpatch/TIMING.lock
until (set -o noclobber; echo "verify5 timing $(date -Is)" > $L) 2> /dev/null; do sleep 1; done
trap 'grep -q "^verify5" $L && rm -f $L' EXIT
echo "LOCKED $(date -Is)"
export HOLDS_LOCK=1
S=$V/sets
rm -f cmp/time_D1.tsv cmp/time_d6.tsv
items=()
for st in smallest recommended; do
  for r in main psp pspr nest nest3; do
    items+=("D1_q_${st}_$r=$r:$S/qcapB/$st" "D1_r1_${st}_$r=$r:$S/r1B/$st")
  done
done
./timing.sh cmp/time_D1.tsv pts/time/D1_p50k.txt pts/time/D1_L2.txt pts/time/D1_cold2k.txt "${items[@]}"
echo "D1_TIMED $(date -Is)"
items=()
for st in smallest recommended; do
  items+=("d6_q25_${st}_r0=nest:$S/d6base/$st" "d6_q25_${st}_n3=nest3:$S/d6base/$st" "d6_q25_${st}_r5=arcs:$S/d6base/$st"
          "d6_af_${st}_r5=arcs:$S/d6afa/$st" "d6_r50c_${st}_r0=nest:$S/d6r50c/$st" "d6_r50c_${st}_r5=arcs:$S/d6r50c/$st"
          "d6_r1_${st}_r0=nest:$S/d6r1/$st" "d6_r1_${st}_n3=nest3:$S/d6r1/$st")
done
./timing.sh cmp/time_d6.tsv pts/time/d6_p50k.txt pts/time/d6_L2.txt pts/time/d6_cold10k.txt "${items[@]}"
echo "STAGE2T_DONE $(date -Is)"
