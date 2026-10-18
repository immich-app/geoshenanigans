#!/bin/bash
# Layout B encode + decode + byte cmp for the remaining admin_vertices variants:
#   withaddr1s (11 m grid), max1p2 / withaddr1p2 / withaddr1sp2 (postal copy dropped from admin files).
# B file -> work/B/<set>_B.bin; decoded copy deleted after the cmp. Runs <= 2 at a time.
set -uo pipefail
cd /ssd/shrink/max/four-changes
export TMPDIR=/ssd/shrink/tmp
rt() {
  s=$1; P=/ssd/shrink/venv/bin/python
  $P -u layoutB4.py encode sets/$s sets/$s/admin_vertices.bin work/B/${s}_B.bin
  $P -u layoutB4.py decode sets/$s work/B/${s}_B.bin work/B/${s}_B.decoded
  if cmp sets/$s/admin_vertices.bin work/B/${s}_B.decoded; then r=IDENTICAL; else r=DIFFERENT; fi
  echo "$s B round trip: $r | today $(sha256sum < sets/$s/admin_vertices.bin | cut -c1-16) decoded $(sha256sum < work/B/${s}_B.decoded | cut -c1-16) | B $(stat -c %s work/B/${s}_B.bin) B vs today $(stat -c %s sets/$s/admin_vertices.bin) B"
  rm -f work/B/${s}_B.decoded
}
export -f rt
printf '%s\n' "$@" | xargs -P 2 -I{} bash -c 'rt {}'
