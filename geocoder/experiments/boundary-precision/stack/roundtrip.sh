#!/bin/bash
# Encode todays-format/admin_vertices.bin in each transfer layout, decode it back with the
# decoder alone (admin_polygons.bin + encoded file), and cmp/sha256 against today's file.
# Encoded files land in work/layouts/<fmt>.bin; decoded copies are deleted after the cmp.
set -euo pipefail
cd /ssd/shrink/stack
PY=/ssd/shrink/venv/bin/python
T=todays-format
mkdir -p work/layouts
rt() {
  f=$1
  $PY layouts.py encode "$f" $T work/layouts/$f.bin
  $PY layouts.py decode "$f" $T work/layouts/$f.bin work/layouts/$f.decoded
  if cmp $T/admin_vertices.bin work/layouts/$f.decoded; then r=IDENTICAL; else r=DIFFERENT; fi
  echo "$f round trip: $r | today $(sha256sum < $T/admin_vertices.bin | cut -c1-16) decoded $(sha256sum < work/layouts/$f.decoded | cut -c1-16) | encoded $(stat -c %s work/layouts/$f.bin) B vs today $(stat -c %s $T/admin_vertices.bin) B"
  rm work/layouts/$f.decoded
}
export -f rt; export PY T
printf '%s\n' ${FMTS:-B Bg A An} | xargs -P 4 -I{} bash -c 'rt {}'
