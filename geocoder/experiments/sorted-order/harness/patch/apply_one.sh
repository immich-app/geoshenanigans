#!/bin/bash
# Usage: apply_one.sh TOOL OLD_DATE NEW_DATE VARIANT PROFILE
# Client side, same method as the baseline step's apply_one.sh: this step's
# patch (out/TOOL/NEW/VARIANT/patch.gcpatch) applied by TOOL's geocoder-patch
# to an isolated copy of the SORTED OLD/VARIANT (link_client_files.sh symlinks
# into a private byte copy, so no other step's reads warm its page cache),
# cold cache, TIMING.lock held, systemd scope for memory and md4 IO
# accounting; then verify_patch.sh against the sorted NEW/VARIANT and delete.
# PROFILE: p8 cores 56-63 | p1 core 63 | small core 63, 1 GiB incl. page
#          cache, no swap, md4 100 MB/s read+write, 150 read IOPS
set -u
source /ssd/shrink/sortpatch/patch/common.sh
tool=$1 old=$2 new=$3 v=$4 prof=$5
safe=${v//\//_}
O=$B/out/$tool/$new/$safe
W=$B/work/$tool-$safe-$prof
rm -rf "$W"; mkdir -p "$W/new" "$W/tmp"
# v8s applies to the PUBLISHED days: its own private copies.
DAYS=$(days_of $tool)
PRIV=$B/private; [ $tool = v8s ] && PRIV=$B/private-pub
P=$PRIV/$old/$v
if [ ! -f "$PRIV/$old/$safe.done" ]; then
  lock_acquire "copy $tool $old $v (untimed)"
  # Re-checked under the lock: two drivers may share a private copy.
  if [ ! -f "$PRIV/$old/$safe.done" ]; then
    rm -rf "$P"; mkdir -p "$P"
    find "$DAYS/$old/$v" -maxdepth 1 -type f ! -name '.*' ! -name '*.osm_ids' ! -name '*.gcpatch' ! -name '*.zst' \
      -exec cp --reflink=never {} "$P/" \;
    sync
    touch "$PRIV/$old/$safe.done"
  fi
  lock_release
fi
"$TOOLS/link_client_files.sh" "$P" "$W/old"
patch=$O/patch.gcpatch
props=()
case $prof in
  p8) cores=56-63 ;;
  p1) cores=63 ;;
  small) cores=63
    props=(-p MemoryMax=1G -p MemorySwapMax=0 -p "IOReadBandwidthMax=/dev/md4 100M"
           -p "IOWriteBandwidthMax=/dev/md4 100M" -p "IOReadIOPSMax=/dev/md4 150") ;;
  *) echo "unknown profile $prof"; exit 1 ;;
esac
need_kb=$(( $(du -skL "$W/old" | awk '{print $1}') + 41943040 ))
until [ "$(df -k --output=avail /ssd | tail -1)" -ge "$need_kb" ]; do
  echo "  [$(ts)] waiting for disk: need $((need_kb / 1048576)) GiB free"; sleep 60
done

wait_quiet_siblings
lock_acquire "apply $tool $old->$new $v $prof"
wait_quiet_siblings
drop_caches
s0=$(sib_snapshot)
r=$(systemd-run --scope --quiet -p IOAccounting=yes -p MemoryAccounting=yes "${props[@]}" \
      env TMPDIR="$W/tmp" bash /ssd/shrink/sortpatch/baseline/scope_run.sh "$O/apply-$prof" "$cores" "$W/new" "$W/tmp" \
      $(patch_bin $tool) "$W/old" "$patch" -o "$W/new/" 2>&1 | tail -1)
s1=$(sib_snapshot)
[ -z "$r" ] && r="killed"
# Verify (reads ~2x the variant) and delete still under the lock, so another
# driver's timing run never overlaps this IO.
verdict=$("$TOOLS/verify_patch.sh" "$DAYS/$new/$v" "$W/new" | tail -3 | tr '\n' ' ')
rm -rf "$W"
lock_release
echo "APPLY $tool $old->$new $v $prof $(time_fields "$O/apply-$prof.time") $r sib_busy=$(sib_busy_pct "$s0" "$s1")% verify=\"$verdict\"" \
  | tee "$O/apply-$prof.result"
