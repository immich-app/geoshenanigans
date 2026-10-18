#!/bin/bash
# Usage: apply_one.sh OLD_DATE NEW_DATE VARIANT PROFILE
# Client side of today's patches: the PUBLISHED NEW/VARIANT/patch.gcpatch
# applied by geocoder-patch to an isolated copy of OLD/VARIANT (symlinks to
# its client files only, link_client_files.sh, as generate_patches.sh's verify
# stage does), cold cache, TIMING.lock held, in a systemd scope for memory and
# md4 IO accounting; then verify_patch.sh against NEW/VARIANT and delete.
# The symlinks point at a private byte copy of OLD/VARIANT (private/, its own
# inodes): other steps read the days' files (hardlinks share one page cache),
# which made a "cold" run read 1.4 of 31 GB from disk.
# PROFILE: p8    pinned to cores 56-63, no limits
#          p1    pinned to core 63, no limits
#          small pinned to core 63, 1 GiB incl. page cache, no swap,
#                md4 100 MB/s read and write, 150 read IOPS
set -u
source /ssd/shrink/sortpatch/baseline/common.sh
old=$1 new=$2 v=$3 prof=$4
safe=${v//\//_}
O=$B/out/$new/$safe
mkdir -p "$O"
W=$B/work/$safe-$prof
rm -rf "$W"; mkdir -p "$W/old" "$W/new" "$W/tmp"
P=$B/private/$old/$v
if [ ! -f "$B/private/$old/$safe.done" ]; then
  lock_acquire "copy $old $v (untimed)"
  rm -rf "$P"; mkdir -p "$P"
  find "$DAYS/$old/$v" -maxdepth 1 -type f ! -name '.*' ! -name '*.osm_ids' ! -name '*.gcpatch' ! -name '*.zst' \
    -exec cp --reflink=never {} "$P/" \;
  sync
  lock_release
  touch "$B/private/$old/$safe.done"
fi
"$TOOLS/link_client_files.sh" "$P" "$W/old/variant"
patch=$DAYS/$new/$v/patch.gcpatch
props=()
case $prof in
  p8) cores=56-63 ;;
  p1) cores=63 ;;
  small) cores=63
    props=(-p MemoryMax=1G -p MemorySwapMax=0 -p "IOReadBandwidthMax=/dev/md4 100M"
           -p "IOWriteBandwidthMax=/dev/md4 100M" -p "IOReadIOPSMax=/dev/md4 150") ;;
  *) echo "unknown profile $prof"; exit 1 ;;
esac
# Free space check: the output is a full copy of the variant dir.
need_kb=$(( $(du -skL "$W/old/variant" | awk '{print $1}') + 41943040 ))
until [ "$(df -k --output=avail /ssd | tail -1)" -ge "$need_kb" ]; do
  echo "  [$(ts)] waiting for disk: need $((need_kb / 1048576)) GiB free"; sleep 60
done

lock_acquire "apply $old->$new $v $prof"
drop_caches
s0=$(sib_snapshot)
r=$(systemd-run --scope --quiet -p IOAccounting=yes -p MemoryAccounting=yes "${props[@]}" \
      env TMPDIR="$W/tmp" bash $B/scope_run.sh "$O/apply-$prof" "$cores" "$W/new" "$W/tmp" \
      $B/bin/geocoder-patch "$W/old/variant" "$patch" -o "$W/new/" 2>&1 | tail -1)
s1=$(sib_snapshot)
lock_release
[ -z "$r" ] && r="killed"
verdict=$("$TOOLS/verify_patch.sh" "$DAYS/$new/$v" "$W/new" | tail -1)
echo "APPLY $old->$new $v $prof $(time_fields "$O/apply-$prof.time") $r sib_busy=$(sib_busy_pct "$s0" "$s1")% verify=\"$verdict\"" \
  | tee "$O/apply-$prof.result"
rm -rf "$W"
