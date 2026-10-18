#!/bin/bash
# vpost.sh: the verifier's second queue (box only; nohup bash vpost.sh > logs/vpost.log 2>&1 &).
#  P1 canon_sort of published 10-13 into a scratch dir: cold, 8 threads, cores 56-63, TIMING.lock;
#     then its 13 files + slot maps compared with the sort step's day / meta
#  P2 identity: published slot -> osm id across 10-11 -> 10-12 -> 10-13 (untimed, cores 48-55)
#  P3 gc-decode of the server's encoded 10-13 == the sorted 10-13 day (untimed)
#  P4 gc-encode of the sorted 10-13 day == the server's encoded 10-13 (untimed)
#  P5 zstd 1.5.7 -18 -T16 of every file of the encoded 10-13 set (cores 32-47)
set -u
source /ssd/shrink/sortpatch/patch/common.sh
MY_TAG="verify-post-$$"
V=/ssd/shrink/sortpatch/verify
SORT=/ssd/shrink/sortpatch/sort
CODECS=/ssd/shrink/sortpatch/codecs
T=$V/scratch; mkdir -p $T
step() { echo "=== $(date +%T) $*"; }
# cmpdirs A B: same file list (recursive, minus *.gcpatch) and every file byte-identical
cmpdirs() {
  local A=$1 B=$2 la lb f same=0 link=0 bad=0 bytes=0
  la=$(cd $A && find . -type f ! -name '*.gcpatch' | sort); lb=$(cd $B && find . -type f ! -name '*.gcpatch' | sort)
  [ "$la" = "$lb" ] || { echo "FILE LISTS DIFFER"; diff <(echo "$la") <(echo "$lb") | head; bad=1; }
  for f in $la; do
    [ -f $B/$f ] || continue
    if [ "$(stat -c %i $A/$f)" = "$(stat -c %i $B/$f)" ]; then link=$((link + 1)); continue; fi
    if cmp -s $A/$f $B/$f; then same=$((same + 1)); bytes=$((bytes + $(stat -c %s $A/$f))); else echo "DIFFERS $f"; bad=1; fi
  done
  echo "cmp $A vs $B: $same files byte-identical ($bytes B), $link same inode, $([ $bad = 0 ] && echo PASS || echo FAIL)"
}

step P1 canon_sort cold
O=$T/sort1013; rm -rf $O; mkdir -p $O/full $O/meta
wait_quiet_siblings; lock_acquire "verify canon_sort 1013 cold"; wait_quiet_siblings; drop_caches
s0=$(sib_snapshot)
/usr/bin/time -v -o $V/out/timing/sort.time taskset -c 56-63 $SORT/bin/canon_sort /ssd/shrink/days/2026-10-13/planet/full $O/full $O/meta --threads 8 > $V/out/timing/sort.log 2>&1 &
tp=$!; peak=0
while kill -0 $tp 2> /dev/null; do
  for p in $(pgrep -P $tp); do a=$(awk '/^RssAnon/ {print $2}' /proc/$p/status 2> /dev/null); [ -n "$a" ] && [ "$a" -gt "$peak" ] && peak=$a; done
  sleep 0.25
done
wait $tp; rc=$?; s1=$(sib_snapshot); lock_release
echo "SORT rc=$rc $(time_fields $V/out/timing/sort.time) anon_peak_mib=$((peak / 1024)) sib=$(sib_busy_pct "$s0" "$s1")%" | tee $V/out/timing/sort.result
n=0; bad=0
for f in $(cd $O/full && ls); do n=$((n + 1)); cmp -s $O/full/$f $SORT/days/2026-10-13/planet/full/$f || { bad=$((bad + 1)); echo "SORT DIFFERS $f"; }; done
for f in street interp addr; do n=$((n + 1)); cmp -s $O/meta/$f.slot.u32 $SORT/meta/2026-10-13/$f.slot.u32 || { bad=$((bad + 1)); echo "SLOTMAP DIFFERS $f"; }; done
echo "SORT re-run vs sort step: $n files compared, $bad differ" | tee -a $V/out/timing/sort.result
rm -rf $O

step P2 identity
for pair in "2026-10-11 2026-10-12" "2026-10-12 2026-10-13"; do
  taskset -c 48-55 /ssd/shrink/venv/bin/python $V/src/vident.py $pair
done 2>&1 | tee $V/out/ident.txt

step P3 decode
O=$T/dec1013; rm -rf $O
GC=$CODECS/bin/gc-decode taskset -c 48-55 bash $CODECS/dec_day.sh $CODECS/enc/2026-10-13 $O > $V/logs/dec1013.log 2>&1; echo "decode rc=$?"
cmpdirs $O $SORT/days/2026-10-13 | tee $V/out/dec1013.cmp
rm -rf $O

step P4 encode
O=$T/enc1013; rm -rf $O
GC=$CODECS/bin/gc-encode taskset -c 48-55 bash $CODECS/enc_day.sh $SORT/days/2026-10-13 $O > $V/logs/enc1013.log 2>&1; echo "encode rc=$?"
cmpdirs $O $CODECS/enc/2026-10-13 | tee $V/out/enc1013.cmp
rm -rf $O

step P5 zstd sizes
Z=/ssd/shrink/zstd157/bin/zstd
for f in $(cd $CODECS/enc/2026-10-13 && find . -type f | sort); do
  printf "%s\t%s\t%s\n" $f $(stat -L -c %s $CODECS/enc/2026-10-13/$f) $(taskset -c 32-47 $Z -18 -T16 -q -c $CODECS/enc/2026-10-13/$f | wc -c)
done > $V/out/zst_enc1013.tsv
awk -F'\t' '{r += $2; z += $3; n++} END {printf "ZSTD encoded 10-13: %d files, raw %d B (%.2f GB), zstd -18 %d B (%.1f MiB)\n", n, r, r / 1e9, z, z / 2^20}' $V/out/zst_enc1013.tsv | tee $V/out/zst_enc1013.sum
rmdir $T 2> /dev/null
step POST-DONE
