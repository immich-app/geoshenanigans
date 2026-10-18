#!/bin/bash
# Download planet quality dirs (admin+postal polygons/vertices) of build 2026-10-13,
# verify sha256 of the decompressed file against the manifest, keep raw files in dl/<q>/.
set -euo pipefail
cd /ssd/shrink/grid
export TMPDIR=/ssd/shrink/tmp
Z=/ssd/shrink/zstd157/bin/zstd
M=/ssd/shrink/data/configurations-2026-10-13.json
U=https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/2026-10-13/planet/quality
one() {
  q=$1; f=$2
  mkdir -p dl/$q
  want=$(/ssd/shrink/venv/bin/python -c "import json,sys; print(json.load(open(\"$M\"))[\"files\"][\"planet/quality/$q/$f\"][\"sha256\"])")
  curl -fsS --retry 5 "$U/$q/$f.zst" | $Z -d -q -c > dl/$q/$f.part
  got=$(sha256sum dl/$q/$f.part | cut -d" " -f1)
  if [ "$got" = "$want" ]; then mv dl/$q/$f.part dl/$q/$f; echo "OK $q/$f $got"; else echo "SHA MISMATCH $q/$f got $got want $want"; fi
}
export -f one; export Z M U
for q in ${QS:-q1 q0.5 q2 q1.5 q0.2}; do for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do echo "$q $f"; done; done | xargs -P 4 -L 1 bash -c "one \"\$@\"" _
