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
# (a missing set or patch would compare nothing and pass)
for f in "$I/$v" "$REF/$v" "$P/$sv/patch.gcpatch"; do [ -e "$f" ] || { echo "smoke $v: no $f, FAIL"; exit 1; }; done
rm -rf "$O"; mkdir -p "$O" $M/tmp $M/logs
export TMPDIR=$M/tmp PATH=/ssd/shrink/zstd157/bin:$PATH
/usr/bin/time -f "wall=%e user=%U sys=%S maxrss_kib=%M" taskset -c ${CORES:-40} "$BIN" "$I/$v" "$P/$sv/patch.gcpatch" -o "$O/" ${EXTRA:-} 2> "$M/logs/smoke.$sv.log"
rc=$?
echo "rc=$rc"; tail -3 "$M/logs/smoke.$sv.log"
bad=0; n=0
[ $rc = 0 ] || bad=1
for f in $(cd "$REF/$v" && ls); do n=$((n+1)); cmp -s "$REF/$v/$f" "$O/$f" || { echo "DIFFERS $f"; bad=1; }; done
for f in $(cd "$O" && ls); do [ -e "$REF/$v/$f" ] || { echo "EXTRA $f"; bad=1; }; done
[ $n -gt 0 ] || { echo "no reference files in $REF/$v"; bad=1; }
echo "smoke $v: $n files, $([ $bad = 0 ] && echo PASS || echo FAIL)"
[ $bad = 0 ]
