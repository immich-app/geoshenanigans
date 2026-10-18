#!/bin/bash
# Mirror tree for the diffs: the days' three variant dirs hardlinked, plus
# planet/quality/q2.5 fetched from the published build (zstd -d, sha256 checked
# against configurations.json). geocoder-diff borrows admin_polygons/vertices
# for planet/full and planet/poi/all from ../quality/q2.5 (geocoder_diff.cpp
# t_admin fallback), as it does in CI's full tree; without it the admin id
# remap is empty and the patch differs from the published one.
set -euo pipefail
B=/ssd/shrink/sortpatch/baseline
BASE=https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds
ZSTD=/ssd/shrink/zstd157/bin/zstd
for d in 2026-10-11 2026-10-12 2026-10-13; do
  T=$B/tree/$d/planet
  mkdir -p "$T/quality" "$T/poi"
  for v in full quality/uncapped poi/all; do
    [ -d "$T/$v" ] || cp -al "/ssd/shrink/days/$d/planet/$v" "$T/$v"
  done
  mkdir -p "$T/quality/q2.5"
  for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do
    out=$T/quality/q2.5/$f
    want=$(/ssd/shrink/venv/bin/python -c "import json,sys; print(json.load(open('/ssd/shrink/days/$d/configurations.json'))['files']['planet/quality/q2.5/$f']['sha256'])")
    if [ ! -f "$out" ] || [ "$(sha256sum "$out" | cut -d' ' -f1)" != "$want" ]; then
      curl -sSL --fail --retry 3 "$BASE/$d/planet/quality/q2.5/$f.zst" | $ZSTD -d -q -f -o "$out"
    fi
    got=$(sha256sum "$out" | cut -d' ' -f1)
    [ "$got" = "$want" ] && echo "ok $d q2.5/$f" || { echo "SHA MISMATCH $d $f"; exit 1; }
  done
done
du -sh --apparent-size $B/tree/*/planet/quality/q2.5
