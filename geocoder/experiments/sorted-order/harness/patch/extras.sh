#!/bin/bash
# Untimed extra measurements (each under TIMING.lock via smoke.sh/with_lock.sh):
#  1. identity sidecars for the published days (gen_pub_identity.py)
#  2. v8c: v8 tools on the sorted days WITHOUT identity (content matching)
#  3. v8s: v8 tools on the PUBLISHED stable-slot days with slot identity
# nohup bash extras.sh > logs/extras.log 2>&1 < /dev/null &
set -u
B=/ssd/shrink/sortpatch/patch
cd $B
for d in 2026-10-11 2026-10-12 2026-10-13; do
  [ -f $B/meta-pub/$d/addr.slot.u32 ] || bash $B/with_lock.sh "pub identity $d" \
    /ssd/shrink/venv/bin/python $B/gen_pub_identity.py /ssd/shrink/days/$d/planet/full $B/meta-pub/$d
done
for pair in "2026-10-12 2026-10-13" "2026-10-11 2026-10-12"; do
  set -- $pair
  for tool in v8c v8s; do
    echo "--- $tool $pair $(date +%T)"
    if grep -q PASS $B/smoke/$tool-smoke/$2/planet_full/verify.txt 2> /dev/null; then echo "kept: $(cat $B/smoke/$tool-smoke/$2/planet_full/verify.txt)"; continue; fi
    bash $B/smoke.sh $tool $1 $2 planet/full
  done
done
echo "=== EXTRAS DONE $(date -u +%FT%TZ)"
