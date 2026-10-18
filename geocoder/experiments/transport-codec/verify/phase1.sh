#!/bin/bash
# phase1: build tt from the reviewed source, planet max best/4 MiB transport set (kept for the
# round trip and decode timing), plain zstd -18 of the same files (baseline + manifest check),
# Oceania 10-17 reproduction (theirs/best x whole/4 MiB, all 32 files).
set -eu
V=/ssd/transport-verify
export TMPDIR=$V/tmp
P=/ssd/shrink/venv/bin/python
mkdir -p $V/bin $V/src $V/logs
cp /ssd/transport/scripts/tt.cpp $V/src/tt.cpp
sha256sum /ssd/transport/scripts/tt.cpp $V/src/tt.cpp > $V/logs/tt_src.sha
g++ -O2 -std=c++17 -I/ssd/transport/zstd-src/zstd-1.5.7/lib $V/src/tt.cpp \
  /ssd/transport/zstd-src/zstd-1.5.7/lib/libzstd.a -o $V/bin/tt
echo "$(date +%T) built"
O=/ssd/transport/oceania/orig
for cfg in theirs best; do
  c=$cfg; [ $cfg = best ] && c=/ssd/transport/oceania/best.json
  for b in 0 4194304; do
    $P -I $V/scripts/enc_set.py $O $V/oceania/${cfg}_$b $c $b --jobs 32 > $V/logs/oc_${cfg}_$b.log 2>&1
  done
done
echo "$(date +%T) oceania done"
$P -I $V/scripts/enc_set.py /ssd/shrink/idx/max $V/planet/best_4m /ssd/transport/planet/best.json 4194304 \
  --jobs 16 --keep > $V/logs/planet_best_4m.log 2>&1
echo "$(date +%T) planet best_4m done"
$P -I $V/scripts/plain_set.py /ssd/shrink/idx/max $V/planet/plain_zst /ssd/shrink/data/configurations-2026-10-13.json \
  planet/full/,planet/quality/uncapped/,planet/poi/all/ --jobs 16 > $V/logs/planet_plain.log 2>&1
echo "$(date +%T) PHASE1 DONE"
