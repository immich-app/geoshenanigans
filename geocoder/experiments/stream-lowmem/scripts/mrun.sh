#!/bin/bash
# mrun.sh PROFILE STYLE [TAG]   (memory track; box only)
# One timed client update 10-12 -> 10-13 of the planet maximum set (planet/full,
# planet/quality/uncapped, planet/poi/all) on the encoded installed set, with an external memory
# sampler (memsample.py on core 40, outside the scope: per-process RssAnon, cgroup anon / kernel).
#   STYLE base      : the e2e prototype (supdate.sh stream: ~20 processes through FIFOs)
#         basepar   : the same, variants in parallel (supdate.sh streampar)
#         new       : inproc.sh (the in-process patcher, variants one after another)
#         newpar    : inproc.sh, variants in parallel
#   PROFILE p8w (cores 56-63, warm) | p1c (core 63, cold) | smallc (core 63, cold, 1 GiB incl. page
#           cache, no swap, md4 100 MB/s r/w, 150 read IOPS)
# Holds /ssd/shrink/sortpatch/TIMING.lock for the run, cmp's every file against the server's
# encoded 10-13 and deletes the new set. Results: /ssd/memory/out/<PROFILE>-<STYLE>[-TAG]/
set -u
M=/ssd/memory
PROFILE=$1 STYLE=$2 TAG=${3:-}
OLD=${OLD:-2026-10-12} NEW=${NEW:-2026-10-13}
ENC=/ssd/shrink/sortpatch/codecs/enc
PATCHES=/ssd/shrink/sortpatch/patch/out/v8
LOCK=/ssd/shrink/sortpatch/TIMING.lock
VARIANTS="planet/full planet/quality/uncapped planet/poi/all"
R=$M/out/$PROFILE-$STYLE${TAG:+-$TAG}
W=$M/work/$PROFILE-$STYLE N=$M/newset/$PROFILE-$STYLE I=$M/inst/enc/$OLD
MY_TAG="memory-$$"
export TMPDIR=$M/tmp
mkdir -p "$R" "$TMPDIR"; rm -f "$R"/*
ts() { date +%T; }
lock_acquire() {
  local waited=0
  until (set -o noclobber; echo "$MY_TAG $(date +%s) $1" > "$LOCK") 2> /dev/null; do
    [ $((waited % 300)) -eq 0 ] && echo "  [$(ts)] waiting for TIMING.lock: $(cat "$LOCK" 2> /dev/null)"
    sleep 5; waited=$((waited + 5))
  done
}
lock_release() { [ -f "$LOCK" ] && [ "$(awk '{print $1}' "$LOCK")" = "$MY_TAG" ] && rm -f "$LOCK"; }
trap lock_release EXIT
sib_snapshot() { awk '/^cpu(2[4-9]|3[01]) / {busy += $2 + $3 + $4 + $7 + $8; tot += $2 + $3 + $4 + $5 + $6 + $7 + $8} END {print busy, tot}' /proc/stat; }
sib_busy_pct() { read b0 t0 <<< "$1"; read b1 t1 <<< "$2"; awk -v b=$((b1 - b0)) -v t=$((t1 - t0)) 'BEGIN {printf "%.0f", (t > 0 ? 100 * b / t : 0)}'; }

if [ ! -f "$I.done" ]; then
  lock_acquire "memory: copy installed enc $OLD (untimed)"
  rm -rf "$I"
  copied=1
  for v in $VARIANTS; do
    mkdir -p "$I/$v" && find "$ENC/$OLD/$v" -maxdepth 1 -type f ! -name '.*' -exec cp --reflink=never -t "$I/$v/" {} + || copied=0
  done
  # (a set marked done is reused by every later run)
  [ $copied = 1 ] || { echo "copying the installed set $ENC/$OLD failed"; rm -rf "$I"; exit 1; }
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
case $STYLE in
  base) inner=(bash /ssd/shrink/sortpatch/e2e/supdate.sh stream "$cores") ;;
  basepar) inner=(bash /ssd/shrink/sortpatch/e2e/supdate.sh streampar "$cores") ;;
  new) inner=(env PATCHX=$M/bin/geocoder-patch-m bash $M/scripts/inproc.sh seq "$cores") ;;
  newpar) inner=(env PATCHX=$M/bin/geocoder-patch-m bash $M/scripts/inproc.sh par "$cores") ;;
  new2) inner=(env PATCHX=$M/bin/geocoder-patch-m2 bash $M/scripts/inproc.sh seq "$cores") ;;
  new2par) inner=(env PATCHX=$M/bin/geocoder-patch-m2 bash $M/scripts/inproc.sh par "$cores") ;;
  new2t) inner=(env PATCHX=$M/bin/geocoder-patch-m2 PATCH_ARGS=--threads bash $M/scripts/inproc.sh seq "$cores") ;;
  new2tpar) inner=(env PATCHX=$M/bin/geocoder-patch-m2 PATCH_ARGS=--threads bash $M/scripts/inproc.sh par "$cores") ;;
  new3) inner=(env PATCHX=$M/bin/geocoder-patch-m3 bash $M/scripts/inproc.sh seq "$cores") ;;
  new3t) inner=(env PATCHX=$M/bin/geocoder-patch-m3 PATCH_ARGS=--threads bash $M/scripts/inproc.sh seq "$cores") ;;
  new3tpar) inner=(env PATCHX=$M/bin/geocoder-patch-m3 PATCH_ARGS=--threads bash $M/scripts/inproc.sh par "$cores") ;;
  new4) inner=(env PATCHX=$M/bin/geocoder-patch-m4 bash $M/scripts/inproc.sh seq "$cores") ;;
  new4t) inner=(env PATCHX=$M/bin/geocoder-patch-m4 PATCH_ARGS=--threads bash $M/scripts/inproc.sh seq "$cores") ;;
  new4tpar) inner=(env PATCHX=$M/bin/geocoder-patch-m4 PATCH_ARGS=--threads bash $M/scripts/inproc.sh par "$cores") ;;
  *) echo "unknown style $STYLE"; exit 1 ;;
esac
# Waits (up to 2 h) until the SMT siblings of the timing cores (24-31) are under 10% busy.
wait_quiet_siblings() {
  local waited=0 a b pct
  while [ $waited -lt 7200 ]; do
    a=$(sib_snapshot); sleep 3; b=$(sib_snapshot)
    pct=$(sib_busy_pct "$a" "$b")
    [ "$pct" -lt 10 ] && return 0
    [ $((waited % 300)) -eq 0 ] && echo "  [$(ts)] waiting for quiet siblings 24-31 (${pct}% busy)"
    sleep 27; waited=$((waited + 30))
  done
  echo "  [$(ts)] siblings still busy after 2 h, timing anyway"
}
wait_quiet_siblings
lock_acquire "memory: update $PROFILE $STYLE $OLD->$NEW"
wait_quiet_siblings
rm -rf "$W" "$N"
if [ $PROFILE = p8w ]; then
  sync
  find "$I" -type f -print0 | xargs -0 -P 8 -n 4 cat > /dev/null
  cat $PATCHES/$NEW/*/patch.gcpatch > /dev/null
else
  sync; echo 3 > /proc/sys/vm/drop_caches
fi
unit=memtrack-$$-$RANDOM
cgdir=/sys/fs/cgroup/system.slice/$unit.scope
taskset -c 40 /ssd/shrink/venv/bin/python -I $M/scripts/memsample.py "$cgdir" "$R/mem" 0.1 > "$R/mem.out" 2>&1 &
spid=$!
s0=$(sib_snapshot)
systemd-run --scope --quiet --unit=$unit -p IOAccounting=yes -p MemoryAccounting=yes "${props[@]}" \
  "${inner[@]}" "$I" "$PATCHES/$NEW" "$N" "$W" "$R" > "$R/update.out" 2>&1
rc=$?
s1=$(sib_snapshot)
touch "$R/mem.stop"; wait $spid
REF=$ENC/$NEW
verdict=PASS; nf=0; nb=0
for v in $VARIANTS; do
  la=$(cd "$REF/$v" && find . -maxdepth 1 -type f ! -name '.*' | sort)
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
echo "MEM $(cat $R/mem.out)" | tee -a "$R/result.txt"
grep -E '^(STAGE|ROLE|TOTAL)' "$R/update.out" | tee -a "$R/result.txt"
[ $rc = 0 ] && [ $verdict = PASS ]  # the exit status: the update ran and every file matched
