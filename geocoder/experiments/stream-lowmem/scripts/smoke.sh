#!/bin/bash
# smoke.sh VARIANT [BIN]  untimed correctness check of the in-process patcher (core 40)
set -u
v=$1 BIN=${2:-/ssd/memory/build-x/geocoder-patch}
M=/ssd/memory I=$M/inst/enc/2026-10-12 REF=/ssd/shrink/sortpatch/codecs/enc/2026-10-13
P=/ssd/shrink/sortpatch/patch/out/v8/2026-10-13
sv=${v//\//_}
O=$M/smoke/$sv
rm -rf "$O"; mkdir -p "$O" $M/tmp
export TMPDIR=$M/tmp PATH=/ssd/shrink/zstd157/bin:$PATH
/usr/bin/time -f "wall=%e user=%U sys=%S maxrss_kib=%M" taskset -c 40 "$BIN" "$I/$v" "$P/$sv/patch.gcpatch" -o "$O/" 2> "$M/logs/smoke.$sv.log"
echo "rc=$?"; tail -3 "$M/logs/smoke.$sv.log"
bad=0; n=0
for f in $(cd "$REF/$v" && ls); do n=$((n+1)); cmp -s "$REF/$v/$f" "$O/$f" || { echo "DIFFERS $f"; bad=1; }; done
for f in $(cd "$O" && ls); do [ -e "$REF/$v/$f" ] || { echo "EXTRA $f"; bad=1; }; done
echo "smoke $v: $n files, $([ $bad = 0 ] && echo PASS || echo FAIL)"
