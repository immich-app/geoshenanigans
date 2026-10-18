#!/bin/bash
# sizes.sh: zstd -18 -T16 of every file of the encoded 10-13 day (gc outputs and the hardlinked
# unchanged files) and of the PAX170 address layout; PAX170 round trip check. Cores 16-31.
set -uo pipefail
C=/ssd/shrink/sortpatch/codecs
E=$C/try/enc/2026-10-13
D=/ssd/shrink/sortpatch/sort/days/2026-10-13/planet/full
export TMPDIR=/ssd/shrink/tmp
echo "== pax170 round trip"; date
mkdir -p $C/try/pax/enc $C/try/pax/dec
$C/bin/gc-encode $D $C/try/pax/enc --addr pax170 --only addr
$C/bin/gc-decode $C/try/pax/enc $C/try/pax/dec
for f in addr_points.bin addr_vertices.bin; do cmp $D/$f $C/try/pax/dec/$f && echo "pax170 round trip: $f byte-identical"; done
echo "== zstd"; date
$C/zs.sh $C/try/pax/enc/addr_points.pax170 > $C/logs/zst_pax.tsv
rm -rf $C/try/pax/dec
$C/zs.sh $(find $E -type f | sort) > $C/logs/zst_enc_1013.tsv
echo DONE; date
