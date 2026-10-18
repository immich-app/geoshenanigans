#!/bin/bash
# Format parity with the earlier research encoders/decoders, on the sorted 10-13 day:
#  streets/interps: the streets track's waypack (C++) on the same input -> groups, nodes2 and the
#                   split_cols.py columns must equal gc's files byte for byte
#  admin:           the verifier's independent layoutb.c encoder -> must equal admin_vertices.B
#  poi vertices:    the verifier's independent bopendec.c on gc's poi_vertices.bopen (+ today's
#                   poi_records) -> must equal today's poi_vertices.bin
#  addresses:       the verifier's independent abs2dec.c on gc's addr_vertices.abs2 (+ gc's col
#                   columns un-planed) -> must equal today's addr_points.bin and addr_vertices.bin
#  q2.5 admin:      gc round trip on quality/q2.5 (not part of the published day layout)
set -uo pipefail
C=/ssd/shrink/sortpatch/codecs
D=/ssd/shrink/sortpatch/sort/days/2026-10-13/planet
E=$C/try/enc/2026-10-13/planet
O=$C/parity
P=/ssd/shrink/venv/bin/python
V=/ssd/shrink/max/verify
export TMPDIR=/ssd/shrink/tmp
mkdir -p $O $C/bin
cd $O
ok() { if cmp -s "$1" "$2"; then echo "SAME  $3"; else echo "DIFF  $3 ($1 vs $2)"; fi; }
g++ -O2 -o $C/bin/ref_waypack $V/st/waypack.cpp && gcc -O2 -o $C/bin/ref_layoutb $V/scripts/layoutb.c \
  && gcc -O2 -o $C/bin/ref_bopendec $V/scripts/bopendec.c && gcc -O2 -o $C/bin/ref_abs2dec $V/scripts/abs2dec.c || exit 1
echo "== streets (waypack)"; date
$C/bin/ref_waypack enc 12 $D/full/street_ways.bin $D/full/street_nodes.bin 16 s_g16
ok s_g16.groups $E/full/street.groups street.groups
ok s_g16.nodes2 $E/full/street.nodes2 street.nodes2
$P -u $V/scripts/split_cols.py street s_g16.ways2 s_cols
for c in cnt code name; do ok s_cols.$c $E/full/street.$c street.$c; done
rm -f s_g16.* s_cols.*
echo "== interps (waypack)"; date
$C/bin/ref_waypack enc 24 $D/full/interp_ways.bin $D/full/interp_nodes.bin 16 i_g16
ok i_g16.groups $E/full/interp.groups interp.groups
ok i_g16.nodes2 $E/full/interp.nodes2 interp.nodes2
$P -u $V/scripts/split_cols.py interp i_g16.ways2 i_cols
for c in cntcode street start span interp; do ok i_cols.$c $E/full/interp.$c interp.$c; done
rm -f i_g16.* i_cols.*
echo "== admin (layoutb.c)"; date
$C/bin/ref_layoutb enc $D/quality/uncapped/admin_polygons.bin $D/quality/uncapped/admin_vertices.bin adminB.ref
ok adminB.ref $E/quality/uncapped/admin_vertices.B admin_vertices.B
rm -f adminB.ref
echo "== poi vertices (bopendec.c)"; date
$C/bin/ref_bopendec $D/poi/all/poi_records.bin $E/poi/all/poi_vertices.bopen poiv.dec
ok poiv.dec $D/poi/all/poi_vertices.bin "bopendec(gc bopen) == today's poi_vertices.bin"
rm -f poiv.dec
echo "== addresses (abs2dec.c)"; date
$P -u $C/unplane_addr.py $E/full/addr_points.col tb_cols.bin
$C/bin/ref_abs2dec tb_cols.bin $E/full/addr_vertices.abs2 ap.dec av.dec
ok ap.dec $D/full/addr_points.bin "abs2dec(gc) == today's addr_points.bin"
ok av.dec $D/full/addr_vertices.bin "abs2dec(gc) == today's addr_vertices.bin"
rm -f tb_cols.bin ap.dec av.dec
echo "== q2.5 admin round trip"; date
Q=/ssd/shrink/sortpatch/sort/tree/2026-10-13/planet/quality/q2.5
mkdir -p q25e q25d
$C/bin/gc-encode $Q q25e && $C/bin/gc-decode q25e q25d && $C/cmp_day.sh $Q q25d
rm -rf q25e q25d
echo DONE; date
