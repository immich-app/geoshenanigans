#!/bin/bash
# Usage: diff_one.sh TOOL OLD_DATE NEW_DATE VARIANT
# geocoder-diff between two CANONICAL SORTED days (sort step's mirror tree:
# sorted days hardlinked plus planet/quality/q2.5, so variants borrow string
# tiers / admin polygons exactly as in CI). Same method as the baseline step:
#  1. timed: cold cache, cores 56-63, /usr/bin/time -v, TIMING.lock held
#  2. section probe: instrumented copy of the same tool, warm, untimed
# Then compares the two patches (and, for a variant the sort step left in
# today's order, the published patch) and slices sections.
set -u
source /ssd/shrink/sortpatch/patch/common.sh
tool=$1 old=$2 new=$3 v=$4
safe=${v//\//_}
O=$B/out/$tool/$new/$safe
mkdir -p "$O"
rm -f "$O"/patch*.gcpatch
OLD=$(tree_of $tool)/$old/$v NEW=$(tree_of $tool)/$new/$v
pub=/ssd/shrink/days/$new/$v/patch.gcpatch
extra=$(diff_extra $tool $old $new)

need_gib=$([ "$v" = planet/full ] && echo 80 || echo 20)
until [ $(( $(awk '/MemAvailable/ {print $2}' /proc/meminfo) / 1048576 )) -ge $need_gib ]; do
  echo "  [$(ts)] waiting for ${need_gib} GiB available"; sleep 30
done

wait_quiet_siblings
lock_acquire "diff $tool $old->$new $v"
wait_quiet_siblings
drop_caches
s0=$(sib_snapshot)
/usr/bin/time -v -o "$O/diff.time" taskset -c 56-63 $(diff_bin $tool) "$OLD" "$NEW" -o "$O/patch.gcpatch" $extra 2> "$O/diff.log"
rc=$?
s1=$(sib_snapshot)
/usr/bin/time -v -o "$O/diff-sec.time" taskset -c 56-63 $(diff_sec_bin $tool) "$OLD" "$NEW" -o "$O/patch-sec.gcpatch" $extra 2> "$O/diff-sec.log"
lock_release
# The post-processing below stays off the timing cores (56-63).
taskset -p -c 0-15 $$ > /dev/null

same_sec=$(cmp -s "$O/patch.gcpatch" "$O/patch-sec.gcpatch" && echo yes || echo no)
raw_ours=$(zstd -dc "$O/patch.gcpatch" | wc -c)
same_pub=$(cmp -s "$O/patch.gcpatch" "$pub" && echo yes || echo no)
echo "DIFF $tool $old->$new $v rc=$rc $(time_fields "$O/diff.time") sib_busy=$(sib_busy_pct "$s0" "$s1")% \
zst=$(stat -c%s "$O/patch.gcpatch") raw=$raw_ours pub_zst=$(stat -c%s "$pub") identical_to_published=$same_pub sec_same=$same_sec \
| warm-sec: $(time_fields "$O/diff-sec.time")" | tee "$O/diff.result"
/ssd/shrink/venv/bin/python $B/section_sizes2.py "$O/diff-sec.log" "$O/patch.gcpatch" > "$O/sections.txt" 2>&1
cat "$O/sections.txt"
rm -f "$O/patch-sec.gcpatch"
