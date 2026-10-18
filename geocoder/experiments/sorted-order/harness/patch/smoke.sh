#!/bin/bash
# Usage: smoke.sh TOOL OLD NEW VARIANT [TAG]
# Untimed correctness run of a tool set (sorted days; v8s: published days):
# instrumented diff (cores 32-47), apply (core 48) into a scratch dir, verify
# against the new day, delete the output, section breakdown. Holds
# TIMING.lock through the IO-heavy part so it neither disturbs nor is
# disturbed by a timing run.
set -u
source /ssd/shrink/sortpatch/patch/common.sh
tool=$1 old=$2 new=$3 v=$4 tag=${5:-smoke}
safe=${v//\//_}
S=$B/smoke/$tool-$tag/$new/$safe
T=$(tree_of $tool) D=$(days_of $tool)
mkdir -p "$S"
rm -rf "$S/new" "$S/old" "$S/tmp"; mkdir -p "$S/tmp"
extra=$(diff_extra $tool $old $new)
lock_acquire "smoke $tool $old->$new $v"
/usr/bin/time -v -o "$S/diff.time" taskset -c 32-47 $(diff_sec_bin $tool) "$T/$old/$v" "$T/$new/$v" -o "$S/patch.gcpatch" $extra 2> "$S/diff.log"
echo "diff rc=$? $(time_fields "$S/diff.time") zst=$(stat -c%s "$S/patch.gcpatch")"
"$TOOLS/link_client_files.sh" "$D/$old/$v" "$S/old"
TMPDIR=$S/tmp /usr/bin/time -v -o "$S/apply.time" taskset -c 48 $(patch_bin $tool) "$S/old" "$S/patch.gcpatch" -o "$S/new/" 2> "$S/apply.log"
echo "apply rc=$? $(time_fields "$S/apply.time")"
"$TOOLS/verify_patch.sh" "$D/$new/$v" "$S/new" | tail -5 | tee "$S/verify.txt"
rm -rf "$S/new" "$S/old" "$S/tmp"
lock_release
/ssd/shrink/venv/bin/python $B/section_sizes2.py "$S/diff.log" "$S/patch.gcpatch" > "$S/sections.txt" 2>&1
