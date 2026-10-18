#!/bin/bash
# Index/POI/strings track re-run in /ssd/shrink/max/verify/ix (track scripts copied, paths rewritten):
# clean + dedup geo index (today's format), block index gx adj 128 -> independent full decode vs today;
# best_tf today's-format stack (clean+dedup geo, POI R1 + POI list dedup, admin list dedup, strings purge)
# -> fresh 20k answers; POI B-open -> independent decode cmp; whole-file zstd -18 -T16.
set -uo pipefail
V=/ssd/shrink/max/verify
IX=$V/ix
I=/ssd/shrink/idx/max
P=/ssd/shrink/venv/bin/python
Z=/ssd/shrink/zstd157/bin/zstd
T=/ssd/shrink/max/index-poi-strings
export TMPDIR=/ssd/shrink/tmp
mkdir -p $IX/work $IX/out
cd $IX
for f in lists.py make_clean.py make_dedup_lib.py geo_gx.py poi_rec.py strings_purge.py strings_fields.py layouts.py poi_vert.py poi_vert2.py l10_idx.py; do
  sed -e "s#/ssd/shrink/max/index-poi-strings/work#$IX/work#g" -e "s#/ssd/shrink/max/index-poi-strings/out#$IX/out#g" $T/scripts/$f > $f
done
grep -l "index-poi-strings" *.py && { echo "path rewrite incomplete"; exit 1; }
# skip the non-dedup v_clean geo_cells copy (8 GB, not needed here)
sed -i 's|^g1.tofile(f"{out1}/geo_cells.bin")|pass  # verifier: v_clean geo_cells not written|' make_clean.py
grep -n "verifier" make_clean.py || { echo "patch failed"; exit 1; }
ln -sf $T/work/str_starts.npy work/str_starts.npy
ln -sf $T/work/str_ref_any.npy work/str_ref_any.npy
zs() { for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c) >> $V/work/zst_T16.tsv; done; }
echo "== make_clean"; date
$P -u make_clean.py
rm -rf work/v_clean
echo "== gx adj 128"; date
$P -u geo_gx.py adj 128
$V/work/gxdec 128 work/gx_adj_128 $I && echo "gx: independent full decode matches today's geo index (repeated ids dropped)"
zs work/gx_adj_128/gx_rows.bin work/gx_adj_128/gx_top.bin work/gx_adj_128/street_entries.bin work/gx_adj_128/interp_entries.bin work/gx_adj_128/addr_entries.bin
rm -rf work/gx_adj_128 work/clean_*.npz
echo "== POI records + admin lists + strings purge"; date
$P -u poi_rec.py
zs work/poi_rec/R3_aos32.bin work/poi_rec/R3_soa.bin work/poi_rec/R3_dl.bin work/v_poi_dg/poi_entries.bin work/v_poi_dg/poi_cells.bin $I/poi_records.bin
rm -rf work/poi_rec
$P -u l10_idx.py
zs work/l10/admin_bitmap.bin work/l10/admin_rank.bin work/l10/admin_offsets_dg.bin work/l10/poi_bitmap.bin work/l10/poi_rank.bin work/l10/poi_offsets_dg.bin
$P -u strings_purge.py
zs work/v_strpurge/strings_core.bin work/v_strpurge/strings_street.bin work/v_strpurge/strings_postcode.bin work/v_admin_dg/admin_entries.bin work/v_admin_dg/admin_cells.bin
echo "== best_tf"; date
D=work/best_tf; mkdir -p $D
for f in $I/*; do ln -f "$f" "$D/"; done
for f in work/v_clean_dg/* work/v_poi_dg/* work/v_admin_dg/* work/v_strpurge/*; do ln -f "$f" "$D/$(basename $f)"; done
ls -la $D | head -50
(cd $V && scripts/runq_v.sh ix_best_tf $IX/$D base_max)
zs work/v_clean_dg/geo_cells.bin work/v_clean_dg/street_entries.bin work/v_clean_dg/interp_entries.bin work/v_clean_dg/addr_entries.bin
rm -rf $D work/v_clean_dg work/v_strpurge work/v_poi_r1
echo "== POI vertices"; date
$P -u poi_vert.py
$V/work/bopendec $I/poi_records.bin work/poi_vert/Bopen.bin work/poi_vert/Bopen.dec
cmp work/poi_vert/Bopen.dec $I/poi_vertices.bin && echo "poi B-open: independent decode == today's poi_vertices.bin (byte-identical)"
rm -f work/poi_vert/Bopen.dec
zs work/poi_vert/Bopen.bin work/poi_vert/B.bin $I/poi_vertices.bin
rm -f work/poi_vert/B.bin work/poi_vert/Bg.bin work/poi_vert/An.bin
$P -u poi_vert2.py
zs work/poi_vert/Anopen.bin
rm -rf work/poi_vert
echo DONE; date
