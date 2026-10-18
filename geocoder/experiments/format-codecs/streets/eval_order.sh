#!/bin/bash
# eval_order.sh <reo_dir> <tag> : whole-file sizes of the street group for one way order
#   packed ways (waypack G16) + entries (raw/gv, inline singles, dedup; cells sample)
#   + way_parents / way_postcodes (today u32 and u24 / u16-dict repacks)
# Writes eval/<tag>.tsv (file raw zst). Uses idx/max geo_cells + street_entries + remap.
set -e
cd /ssd/shrink/max/streets
R=$1; T=$2
O=eval/$T; mkdir -p $O
I=/ssd/shrink/idx/max
./waypack enc 12 $R/street_ways.bin $R/street_nodes.bin 16 $O/s_g16 > $O/waypack.log || true
REMAP=""; [ -f $R/remap_street.u32 ] && REMAP=$R/remap_street.u32
NOCELLS=1 SAMPLE=1 INLINE=1 DEDUP=1 ./entenc $I/geo_cells.bin 0 $I/street_entries.bin raw $O/se_raw_in_dd.bin $REMAP > $O/se_raw.log
NOCELLS=1 SAMPLE=1 INLINE=1 DEDUP=1 ./entenc $I/geo_cells.bin 0 $I/street_entries.bin gv $O/se_gv_in_dd.bin $REMAP > $O/se_gv.log
rm -f $O/se_raw_in_dd.bin.todaycells.sample $O/se_gv_in_dd.bin.todaycells.sample
/ssd/shrink/venv/bin/python sidecars.py $R $O/side > $O/side.log
T=${ZT:-12} ./zwhole.sh $O/zwhole.tsv $O/s_g16.ways2 $O/s_g16.groups $O/s_g16.nodes2 \
  $R/way_parents.bin $R/way_postcodes.bin $O/side/way_parents.bin.u24 $O/side/way_postcodes.bin.u16dict \
  $O/se_raw_in_dd.bin $O/se_gv_in_dd.bin
Z=/ssd/shrink/zstd157/bin/zstd
for f in $O/se_raw_in_dd.bin.cells.sample $O/se_gv_in_dd.bin.cells.sample; do
  printf "%s\t%s\t%s\n" $f $(stat -c %s $f) $($Z -18 -T${ZT:-12} -q -c $f | wc -c) >> $O/zwhole.tsv
done
cat $O/waypack.log $O/se_raw.log $O/se_gv.log $O/zwhole.tsv
