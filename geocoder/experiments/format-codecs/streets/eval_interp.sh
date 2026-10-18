#!/bin/bash
# eval_interp.sh <dir> <tag> : whole-file sizes of the interp group for one interp-way order
#   packed interp ways (waypack G16) + interp_entries (raw/gv, inline singles, dedup; cells sample)
#   + interp_postcodes (u32 and u16-dict). Writes eval/<tag>/zwhole.tsv
set -e
cd /ssd/shrink/max/streets
R=$1; T=$2
O=eval/$T; mkdir -p $O
I=/ssd/shrink/idx/max
./waypack enc 24 $R/interp_ways.bin $R/interp_nodes.bin 16 $O/i_g16 > $O/waypack.log || true
REMAP=""; [ -f $R/remap_interp.u32 ] && REMAP=$R/remap_interp.u32
NOCELLS=1 SAMPLE=1 INLINE=1 DEDUP=1 ./entenc $I/geo_cells.bin 2 $I/interp_entries.bin raw $O/ie_raw_in_dd.bin $REMAP > $O/ie_raw.log
NOCELLS=1 SAMPLE=1 INLINE=1 DEDUP=1 ./entenc $I/geo_cells.bin 2 $I/interp_entries.bin gv $O/ie_gv_in_dd.bin $REMAP > $O/ie_gv.log
rm -f $O/ie_raw_in_dd.bin.todaycells.sample $O/ie_gv_in_dd.bin.todaycells.sample
/ssd/shrink/venv/bin/python sidecars.py $R $O/side > $O/side.log
T=${ZT:-12} ./zwhole.sh $O/zwhole.tsv $O/i_g16.ways2 $O/i_g16.groups $O/i_g16.nodes2 \
  $R/interp_postcodes.bin $O/side/interp_postcodes.bin.u16dict $O/ie_raw_in_dd.bin $O/ie_gv_in_dd.bin
Z=/ssd/shrink/zstd157/bin/zstd
for f in $O/ie_raw_in_dd.bin.cells.sample $O/ie_gv_in_dd.bin.cells.sample; do
  printf "%s\t%s\t%s\n" $f $(stat -c %s $f) $($Z -18 -T${ZT:-12} -q -c $f | wc -c) >> $O/zwhole.tsv
done
cat $O/waypack.log $O/ie_raw.log $O/ie_gv.log $O/zwhole.tsv
