#!/bin/bash
# vtime.sh: the verifier's timing queue (box only; nohup bash vtime.sh > logs/vtime.log 2>&1 &).
#  T1 v8 diff 10-12 -> 10-13 planet/full on the sorted tree (identity sidecars), cold, cores 56-63
#  T2 today's diff of the same pair on the published tree (baseline tree, q2.5 sibling), cold, 56-63
#  T3 v8 patch applied (geocoder-patch8) to a private byte copy of sorted 10-12 planet/full, cold, core 63,
#     systemd scope (IO + memory accounting); every output file cmp'd with sorted 10-13 planet/full
# Each holds TIMING.lock and waits for quiet SMT siblings (24-31); caches dropped right before.
set -u
source /ssd/shrink/sortpatch/patch/common.sh   # lock / drop_caches / sibling helpers, time_fields
MY_TAG="verify-$$"
V=/ssd/shrink/sortpatch/verify
SORT=/ssd/shrink/sortpatch/sort
D8=/ssd/shrink/sortpatch/patch/bin/geocoder-diff8
P8=/ssd/shrink/sortpatch/patch/bin/geocoder-patch8
D7=/ssd/shrink/sortpatch/baseline/bin/geocoder-diff
O=$V/out/timing; mkdir -p $O
step() { echo "=== $(date +%T) $*"; }

if [ ! -f $O/diff8.result ]; then
step T1 diff8
wait_quiet_siblings; lock_acquire "verify diff8 1213 full"; wait_quiet_siblings; drop_caches
s0=$(sib_snapshot)
/usr/bin/time -v -o $O/diff8.time taskset -c 56-63 $D8 $SORT/tree/2026-10-12/planet/full $SORT/tree/2026-10-13/planet/full \
  -o $O/diff8.gcpatch --identity $SORT/meta/2026-10-12 $SORT/meta/2026-10-13 2> $O/diff8.log
rc=$?; s1=$(sib_snapshot); lock_release
echo "DIFF8 rc=$rc $(time_fields $O/diff8.time) sib=$(sib_busy_pct "$s0" "$s1")% size=$(stat -c%s $O/diff8.gcpatch) same_as_patch_step=$(cmp -s $O/diff8.gcpatch /ssd/shrink/sortpatch/patch/out/v8/2026-10-13/planet_full/patch.gcpatch && echo yes || echo no)" | tee $O/diff8.result
fi

if [ ! -f $O/diff7.result ]; then
step T2 diff7
BT=/ssd/shrink/sortpatch/baseline/tree
wait_quiet_siblings; lock_acquire "verify diff7 1213 full"; wait_quiet_siblings; drop_caches
s0=$(sib_snapshot)
/usr/bin/time -v -o $O/diff7.time taskset -c 56-63 $D7 $BT/2026-10-12/planet/full $BT/2026-10-13/planet/full -o $O/diff7.gcpatch 2> $O/diff7.log
rc=$?; s1=$(sib_snapshot); lock_release
echo "DIFF7 rc=$rc $(time_fields $O/diff7.time) sib=$(sib_busy_pct "$s0" "$s1")% size=$(stat -c%s $O/diff7.gcpatch) same_as_published=$(cmp -s $O/diff7.gcpatch /ssd/shrink/days/2026-10-13/planet/full/patch.gcpatch && echo yes || echo no)" | tee $O/diff7.result
fi

if [ ! -f $O/apply8.result ]; then
step T3 apply8
P=$V/private/2026-10-12/full
if [ ! -f $P.done ]; then
  rm -rf $P; mkdir -p $P
  find $SORT/days/2026-10-12/planet/full -maxdepth 1 -type f ! -name '.*' ! -name '*.osm_ids' ! -name '*.gcpatch' ! -name '*.zst' -exec cp --reflink=never {} $P/ \;
  sync; touch $P.done
fi
W=$V/work/apply8; rm -rf $W; mkdir -p $W/new $W/tmp
$TOOLS/link_client_files.sh $P $W/old
wait_quiet_siblings; lock_acquire "verify apply8 1213 full p1"; wait_quiet_siblings; drop_caches
s0=$(sib_snapshot)
r=$(systemd-run --scope --quiet -p IOAccounting=yes -p MemoryAccounting=yes env TMPDIR=$W/tmp \
    bash /ssd/shrink/sortpatch/baseline/scope_run.sh $O/apply8-p1 63 $W/new $W/tmp $P8 $W/old $O/diff8.gcpatch -o $W/new/ 2>&1 | tail -1)
s1=$(sib_snapshot)
# byte comparison, every file, still under the lock
REF=$SORT/days/2026-10-13/planet/full
la=$(cd $REF && find . -maxdepth 1 -type f ! -name '*.gcpatch' | sort); lb=$(cd $W/new && find . -maxdepth 1 -type f | sort)
same=0; bad=0; bytes=0
[ "$la" = "$lb" ] || { echo "file lists differ"; diff <(echo "$la") <(echo "$lb"); bad=$((bad + 1)); }
for f in $la; do if cmp -s $REF/$f $W/new/$f; then same=$((same + 1)); bytes=$((bytes + $(stat -c%s $REF/$f))); else bad=$((bad + 1)); echo "DIFFERS $f"; fi; done
vp=$($TOOLS/verify_patch.sh $REF $W/new | tail -2 | tr '\n' ' ')
rm -rf $W
lock_release
echo "APPLY8 p1 $(time_fields $O/apply8-p1.time) $r sib=$(sib_busy_pct "$s0" "$s1")% cmp: $same identical ($bytes B), $bad differ; verify_patch: $vp" | tee $O/apply8.result
fi
step done
