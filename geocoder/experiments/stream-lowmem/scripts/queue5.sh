#!/bin/bash
# Round 5: m3 (adaptive dictionary table, coupled decoder reads capped under --threads): smoke, timed
# runs, then the untimed CPU profiles. Waits for round 4.
cd /ssd/memory
while pgrep -f "scripts/queue4.sh" > /dev/null; do sleep 20; done
cp build-t/geocoder-patch bin/geocoder-patch-m3
ok=1
for v in planet/poi/all planet/quality/uncapped planet/full; do
  r=$(bash scripts/smoke.sh $v bin/geocoder-patch-m3 | tail -1); echo "sync: $r"; case "$r" in *PASS) ;; *) ok=0 ;; esac
  cp logs/smoke.${v//\//_}.log logs/smoke3-sync.${v//\//_}.log
done
r=$(CORES=40-43 EXTRA=--threads bash scripts/smoke.sh planet/full bin/geocoder-patch-m3 | tail -1); echo "threads: $r"; case "$r" in *PASS) ;; *) ok=0 ;; esac
cp logs/smoke.planet_full.log logs/smoke3-thr.planet_full.log
rm -rf smoke
[ $ok = 1 ] || { echo "smoke failed, no timed runs"; exit 1; }
for spec in "p1c new3" "p8w new3t" "p8w new3tpar" "smallc new3"; do
  echo "=== $(date +%T) $spec"
  bash scripts/mrun.sh $spec
done
for m in sync threads; do BIN=/ssd/memory/bin/geocoder-patch-m3 bash scripts/perf_run.sh $m; done
echo "=== $(date +%T) done"
