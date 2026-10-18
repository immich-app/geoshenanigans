#!/bin/bash
# Round 6: m4 (decoder thread override fixed): threaded smoke, threaded timed runs. Waits for round 5.
cd /ssd/memory
while pgrep -f "scripts/queue5.sh" > /dev/null; do sleep 20; done
cp build-t/geocoder-patch bin/geocoder-patch-m4
ok=1
for v in planet/poi/all planet/quality/uncapped planet/full; do
  r=$(CORES=40-43 EXTRA=--threads bash scripts/smoke.sh $v bin/geocoder-patch-m4 | tail -1); echo "threads: $r"; case "$r" in *PASS) ;; *) ok=0 ;; esac
  cp logs/smoke.${v//\//_}.log logs/smoke4-thr.${v//\//_}.log
done
r=$(bash scripts/smoke.sh planet/poi/all bin/geocoder-patch-m4 | tail -1); echo "sync: $r"; case "$r" in *PASS) ;; *) ok=0 ;; esac
rm -rf smoke
[ $ok = 1 ] || { echo "smoke failed, no timed runs"; exit 1; }
for spec in "p8w new4t" "p8w new4tpar" "p1c new4t"; do
  echo "=== $(date +%T) $spec"
  bash scripts/mrun.sh $spec
done
echo "=== $(date +%T) done"
