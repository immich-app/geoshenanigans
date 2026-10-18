#!/bin/bash
# Addresses track re-run in /ssd/shrink/max/verify/ad (track scripts copied, paths rewritten):
# Tier A cell-ordered set (today's format) + strings_street purge -> fresh 20k answers; Tier B abs2
# encode -> independent C decode cmp; col / pax170 / download-only builds; whole-file zstd -T16.
set -uo pipefail
V=/ssd/shrink/max/verify
AD=$V/ad
I=/ssd/shrink/idx/max
P=/ssd/shrink/venv/bin/python
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/shrink/tmp
mkdir -p $AD
# streets re-run leftovers (keep only the two remap tables, needed by the combined measurements)
rm -rf $V/st/fin $V/st/pk $V/st/side $V/st/dirs
find $V/st/reo -type f ! -name 'remap_*.u32' -delete
cd $AD
for f in common.py codecs_addr.py entries_stats.py build_sorted.py purge_street.py tierb.py final_build.py geo_variants.py; do
  sed 's#/ssd/shrink/max/addresses#/ssd/shrink/max/verify/ad#g' /ssd/shrink/max/addresses/$f > $f
done
grep -l "max/addresses" *.py && { echo "path rewrite incomplete"; exit 1; }
zs() { for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c) >> $V/work/zst_T16.tsv; done; }
echo "== entries + Tier A"; date
$P -u entries_stats.py
$P -u build_sorted.py $AD/v_sorted
(cd $V && scripts/runq_v.sh ad_sorted $AD/v_sorted base_max)
zs v_sorted/addr_points.bin v_sorted/addr_vertices.bin v_sorted/addr_entries.bin v_sorted/addr_postcodes.bin \
   $I/addr_points.bin $I/addr_vertices.bin $I/addr_entries.bin $I/addr_postcodes.bin
echo "== G1/G2"; date
$P -u geo_variants.py
zs geo/g1_geo_cells.bin geo/g2_geo_cells.bin geo/g2_addr_cells_col.bin
rm -f geo/*.bin v_sorted/geo_cells.bin v_sorted/addr_entries.bin entries.npz
echo "== strings_street purge"; date
$P -u purge_street.py $AD/v_pstreet
(cd $V && scripts/runq_v.sh ad_pstreet $AD/v_pstreet base_max)
zs v_pstreet/strings_street.bin
rm -rf v_pstreet
echo "== Tier B abs2"; date
$P -u tierb.py encode abs2 tb/abs2 2>&1 | grep -v "^chunk"
$P -u tierb.py decode abs2 tb/abs2
$V/work/abs2dec tb/abs2/addr_points.bin tb/abs2/addr_vertices.bin tb/dec_points.bin tb/dec_vertices.bin
cmp tb/dec_points.bin v_sorted/addr_points.bin && echo "abs2: independent decode == Tier A addr_points.bin (byte-identical)"
cmp tb/dec_vertices.bin v_sorted/addr_vertices.bin && echo "abs2: independent decode == Tier A addr_vertices.bin (byte-identical)"
rm -f tb/dec_*.bin
zs tb/abs2/addr_vertices.bin
rm -rf v_sorted
echo "== final layouts"; date
for n in col pax170 dlpts dlverts; do
  $P -u final_build.py $n && zs final/$n.bin
  rm -f final/$n.bin
done
echo "== parent_way_id in the streets track's spatial way order (both tracks combined)"; date
$P -u $V/scripts/pw_remap.py tb/abs2/addr_points.bin $V/st/reo/street/remap_street.u32 tb/abs2pw/addr_points.bin
sed -e "s#tb/abs2/addr_points.bin#tb/abs2pw/addr_points.bin#" -e "s#O = f'{W}/final'#O = f'{W}/final_pw'#" final_build.py > final_build_pw.py
grep -c "abs2pw\|final_pw" final_build_pw.py
for n in col pax170 dlpts; do
  $P -u final_build_pw.py $n && zs final_pw/$n.bin
  rm -f final_pw/$n.bin
done
rm -rf tb
echo DONE; date
