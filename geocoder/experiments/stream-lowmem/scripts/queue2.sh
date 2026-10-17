#!/bin/bash
# Round 2: smoke tests of the experimental build (libzstd, POI big blocks by planes, --threads),
# then timed runs of it. Waits for round 1 to finish.
cd /ssd/memory
while pgrep -f "scripts/queue1.sh" > /dev/null; do sleep 20; done
cp build-t/geocoder-patch bin/geocoder-patch-m2
ok=1
for v in planet/poi/all planet/quality/uncapped planet/full; do
  r=$(bash scripts/smoke.sh $v bin/geocoder-patch-m2 | tail -1); echo "sync: $r"; case "$r" in *PASS) ;; *) ok=0 ;; esac
  cp logs/smoke.${v//\//_}.log logs/smoke2-sync.${v//\//_}.log
done
for v in planet/poi/all planet/quality/uncapped; do
  r=$(GCS_BIG_BLOCK=1024 bash scripts/smoke.sh $v bin/geocoder-patch-m2 | tail -1); echo "big1024: $r"; case "$r" in *PASS) ;; *) ok=0 ;; esac
  cp logs/smoke.${v//\//_}.log logs/smoke2-big.${v//\//_}.log
done
for v in planet/poi/all planet/quality/uncapped planet/full; do
  r=$(CORES=40-43 EXTRA=--threads bash scripts/smoke.sh $v bin/geocoder-patch-m2 | tail -1); echo "threads: $r"; case "$r" in *PASS) ;; *) ok=0 ;; esac
  cp logs/smoke.${v//\//_}.log logs/smoke2-thr.${v//\//_}.log
done
rm -rf smoke
[ $ok = 1 ] || { echo "smoke failed, no timed runs"; exit 1; }
for spec in "p1c new2" "p8w new2t" "p8w new2tpar" "smallc new2" "p1c new2t" "p8w new2"; do
  echo "=== $(date +%T) $spec"
  bash scripts/mrun.sh $spec
done
echo "=== $(date +%T) done"
