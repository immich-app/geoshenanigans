#!/bin/bash
# run_update2.sh PROFILE STYLE OLD NEW [TAG]  (run_update.sh plus STYLE stream|streampar: supdate.sh)
# One timed client update, OLD -> NEW, of the planet maximum-quality set (planet/full,
# planet/quality/uncapped, planet/poi/all):
#   STYLE seq|par    : the encoded installed set (update.sh: decode -> v8 patch -> encode)
#   STYLE today-seq|today-par : today's raw installed set, the published patch (baseline tools)
#   PROFILE p8w   : cores 56-63, warm (installed set and patches read into the page cache first)
#           p1c   : core 63, cold cache
#           smallc: core 63, cold cache, 1 GiB memory incl. page cache, no swap, md4 100 MB/s read and
#                   write, 150 read IOPS
# The installed set is a private byte copy (inst/<tag>/<OLD>, made once, untimed) so no other dir's
# reads warm its page cache. Holds TIMING.lock for the run, the byte-identity check against the
# server's NEW set and the cleanup. Results: out/<OLD>-<NEW>/<PROFILE>-<STYLE>[-TAG]/.
set -u
source /ssd/shrink/sortpatch/e2e/common.sh
PROFILE=$1 STYLE=$2 OLD=$3 NEW=$4 TAG=${5:-}
R=$E/out/$OLD-$NEW/$PROFILE-$STYLE${TAG:+-$TAG}
W=$E/work/$PROFILE-$STYLE
N=$E/newset/$PROFILE-$STYLE
mkdir -p "$R"; rm -f "$R"/*
case $STYLE in
  today-*) kind=raw; SRC=/ssd/shrink/days/$OLD; REF=/ssd/shrink/days/$NEW ;;
  *) kind=enc; SRC=$ENC/$OLD; REF=$ENC/$NEW ;;
esac
I=$E/inst/$kind/$OLD
if [ ! -f "$I.done" ]; then
  need_free_gb 40
  lock_acquire "e2e copy installed $kind $OLD (untimed)"
  rm -rf "$I"
  copied=1
  for v in $VARIANTS; do
    mkdir -p "$I/$v" && find "$SRC/$v" -maxdepth 1 -type f ! -name '.*' ! -name '*.osm_ids' ! -name '*.gcpatch' ! -name '*.zst' \
      -exec cp --reflink=never -t "$I/$v/" {} + || copied=0
  done
  # (a set marked done is reused by every later run)
  [ $copied = 1 ] || { echo "copying the installed set $SRC failed"; rm -rf "$I"; exit 1; }
  sync; touch "$I.done"
  lock_release
fi
case $PROFILE in
  p8w) cores=56-63; props=() ;;
  p1c) cores=63; props=() ;;
  smallc) cores=63
    props=(-p MemoryMax=1G -p MemorySwapMax=0 -p "IOReadBandwidthMax=/dev/md4 100M"
           -p "IOWriteBandwidthMax=/dev/md4 100M" -p "IOReadIOPSMax=/dev/md4 150") ;;
  *) echo "unknown profile $PROFILE"; exit 1 ;;
esac
need_free_gb 90
wait_quiet_siblings
lock_acquire "e2e update $PROFILE $STYLE $OLD->$NEW"
wait_quiet_siblings
rm -rf "$W" "$N"
if [ $PROFILE = p8w ]; then
  sync
  find "$I" -type f -print0 | xargs -0 -P 8 -n 4 cat > /dev/null
  [ $kind = enc ] && cat $PATCHES/$NEW/*/patch.gcpatch > /dev/null || cat $REF/planet/*/patch.gcpatch $REF/planet/*/*/patch.gcpatch > /dev/null 2>&1
else
  drop_caches
fi
s0=$(sib_snapshot)
case $STYLE in
  today-*) PDIR=$REF ;;
  *) PDIR=$PATCHES/$NEW ;;
esac
case $STYLE in stream*) inner=supdate.sh ;; *) inner=update.sh ;; esac
systemd-run --scope --quiet -p IOAccounting=yes -p MemoryAccounting=yes "${props[@]}" \
  bash $E/$inner "$STYLE" "$cores" "$I" "$PDIR" "$N" "$W" "$R" > "$R/update.out" 2>&1
rc=$?
s1=$(sib_snapshot)
# byte identity against the server's NEW set (same file list, every file cmp'd)
verdict=PASS; nf=0; nb=0
for v in $VARIANTS; do
  la=$(cd "$REF/$v" && find . -maxdepth 1 -type f ! -name '.*' ! -name '*.osm_ids' ! -name '*.gcpatch' ! -name '*.zst' | sort)
  lb=$(cd "$N/$v" 2> /dev/null && find . -maxdepth 1 -type f | sort)
  if [ "$la" != "$lb" ]; then verdict=FAIL; echo "file lists differ in $v" >> "$R/verify.txt"; diff <(echo "$la") <(echo "$lb") | head >> "$R/verify.txt"; fi
  for f in $la; do
    [ -f "$N/$v/$f" ] || continue
    if cmp -s "$REF/$v/$f" "$N/$v/$f"; then nf=$((nf + 1)); nb=$((nb + $(stat -c %s "$N/$v/$f"))); else verdict=FAIL; echo "DIFFERS $v/$f" >> "$R/verify.txt"; fi
  done
done
echo "$verdict: $nf files byte-identical to $REF ($nb bytes)" >> "$R/verify.txt"
rm -rf "$W" "$N"
lock_release
echo "UPDATE $PROFILE $STYLE $OLD->$NEW rc=$rc sib_busy=$(sib_busy_pct "$s0" "$s1")% verify=\"$(tail -1 $R/verify.txt)\"" | tee "$R/result.txt"
grep -E '^(KIND|ROLE|TOTAL)' "$R/update.out" | tee -a "$R/result.txt"
[ $rc = 0 ] && [ $verdict = PASS ]  # the exit status: the update ran and every file matched
