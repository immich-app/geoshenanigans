#!/bin/bash
# Fetch the planet POI-all records and street ways (for strings_core attribution only).
set -euo pipefail
export TMPDIR=/ssd/shrink/tmp
B=https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/2026-10-13
cd /ssd/shrink/contents/extra
for f in poi/all/poi_records.bin no-addresses/street_ways.bin; do
  out=$(echo $f | tr / _)
  curl -sf "$B/planet/$f.zst" | zstd -d -q -o "$out"
  ls -la "$out"
done
echo FETCH_DONE
