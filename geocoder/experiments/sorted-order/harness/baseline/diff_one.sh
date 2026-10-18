#!/bin/bash
# Usage: diff_one.sh OLD_DATE NEW_DATE VARIANT
# Server side of today's patches: geocoder-diff OLD/VARIANT -> NEW/VARIANT,
# run in the mirror tree (build_tree.sh: the days' dirs hardlinked plus
# planet/quality/q2.5), so a variant borrows string tiers from ../full or
# ../../full and admin polygons from quality/q2.5 exactly as in CI; output to
# this step's dir.
#  1. timed: cold cache, pinned to cores 56-63, /usr/bin/time -v, TIMING.lock held
#  2. section probe: the instrumented copy (bin/geocoder-diff-sec), warm, untimed
# Then compares both patches with the published one and slices the sections.
set -u
source /ssd/shrink/sortpatch/baseline/common.sh
old=$1 new=$2 v=$3
safe=${v//\//_}
O=$B/out/$new/$safe
mkdir -p "$O"
rm -f "$O"/patch*.gcpatch
OLD=$B/tree/$old/$v NEW=$B/tree/$new/$v
pub=$NEW/patch.gcpatch

# A planet/full diff peaks near 115 GiB: wait for room.
need_gib=$([ "$v" = planet/full ] && echo 150 || echo 40)
until [ $(( $(awk '/MemAvailable/ {print $2}' /proc/meminfo) / 1048576 )) -ge $need_gib ]; do
  echo "  [$(ts)] waiting for ${need_gib} GiB available"; sleep 30
done

lock_acquire "diff $old->$new $v"
drop_caches
s0=$(sib_snapshot)
/usr/bin/time -v -o "$O/diff.time" taskset -c 56-63 $B/bin/geocoder-diff "$OLD" "$NEW" -o "$O/patch.gcpatch" 2> "$O/diff.log"
rc=$?
s1=$(sib_snapshot)
# Warm instrumented run right after (inputs still cached): section offsets.
/usr/bin/time -v -o "$O/diff-sec.time" taskset -c 56-63 $B/bin/geocoder-diff-sec "$OLD" "$NEW" -o "$O/patch-sec.gcpatch" 2> "$O/diff-sec.log"
lock_release

same_pub=$(cmp -s "$O/patch.gcpatch" "$pub" && echo yes || echo no)
same_sec=$(cmp -s "$O/patch.gcpatch" "$O/patch-sec.gcpatch" && echo yes || echo no)
raw_ours=$(zstd -dc "$O/patch.gcpatch" | wc -c)
raw_pub=$(zstd -dc "$pub" | wc -c)
same_raw=$(cmp -s <(zstd -dc "$O/patch.gcpatch") <(zstd -dc "$pub") && echo yes || echo no)
echo "DIFF $old->$new $v rc=$rc $(time_fields "$O/diff.time") sib_busy=$(sib_busy_pct "$s0" "$s1")% \
ours_zst=$(stat -c%s "$O/patch.gcpatch") pub_zst=$(stat -c%s "$pub") ours_raw=$raw_ours pub_raw=$raw_pub \
identical_zst=$same_pub identical_raw=$same_raw sec_same=$same_sec | warm-sec: $(time_fields "$O/diff-sec.time")" | tee "$O/diff.result"
/ssd/shrink/venv/bin/python $B/section_sizes2.py "$O/diff-sec.log" "$O/patch.gcpatch" > "$O/sections.txt" 2>&1
cat "$O/sections.txt"
rm -f "$O/patch-sec.gcpatch"
