#!/bin/bash
# smoke.sh VARIANT [BIN]  untimed correctness check of the in-process patcher (core 40): patches the
# encoded installed set I/VARIANT with P/<VARIANT, / as _>/patch.gcpatch and cmp's the new set
# against the server's encoded REF/VARIANT. Env: M (work root), I, REF, P, CORES, EXTRA (e.g. --threads).
set -u
v=$1 BIN=${2:-/ssd/memory/build-x/geocoder-patch}
M=${M:-/ssd/memory} I=${I:-/ssd/memory/inst/enc/2026-10-12} REF=${REF:-/ssd/shrink/sortpatch/codecs/enc/2026-10-13}
P=${P:-/ssd/shrink/sortpatch/patch/out/v8/2026-10-13}
sv=${v//\//_}
O=$M/smoke/$sv
rm -rf "$O"; mkdir -p "$O" $M/tmp
export TMPDIR=$M/tmp PATH=/ssd/shrink/zstd157/bin:$PATH
/usr/bin/time -f "wall=%e user=%U sys=%S maxrss_kib=%M" taskset -c ${CORES:-40} "$BIN" "$I/$v" "$P/$sv/patch.gcpatch" -o "$O/" ${EXTRA:-} 2> "$M/logs/smoke.$sv.log"
echo "rc=$?"; tail -3 "$M/logs/smoke.$sv.log"
bad=0; n=0
for f in $(cd "$REF/$v" && ls); do n=$((n+1)); cmp -s "$REF/$v/$f" "$O/$f" || { echo "DIFFERS $f"; bad=1; }; done
for f in $(cd "$O" && ls); do [ -e "$REF/$v/$f" ] || { echo "EXTRA $f"; bad=1; }; done
echo "smoke $v: $n files, $([ $bad = 0 ] && echo PASS || echo FAIL)"
