#!/bin/bash
# Streets track re-run: spatial reorder (today's format) -> answers on fresh 20k points; packed ways
# (waypack) -> independent decoder cmp; sidecars; cell lists (entenc gv + inline + dedup) -> independent
# list check; whole-file zstd -18 -T16 of everything. Work dir /ssd/shrink/max/verify/st.
set -uo pipefail
V=/ssd/shrink/max/verify
ST=$V/st
I=/ssd/shrink/idx/max
P=/ssd/shrink/venv/bin/python
Z=/ssd/shrink/zstd157/bin/zstd
export TMPDIR=/ssd/shrink/tmp
mkdir -p $ST/reo $ST/pk $ST/side $ST/fin $ST/dirs
cd $ST
cp /ssd/shrink/max/streets/{reorder.py,sidecars.py,waypack.cpp,entenc.cpp} .
g++ -O2 -o waypack waypack.cpp && g++ -O2 -o entenc entenc.cpp || exit 1
zs() { for f in "$@"; do printf "%s\t%s\t%s\n" "$f" $(stat -L -c %s "$f") $($Z -18 -T16 -q -c "$f" | wc -c) >> $V/work/zst_T16.tsv; done; }
echo "== reorder"; date
$P -u reorder.py street reo/street & $P -u reorder.py interp reo/interp & wait
for k in street interp; do
  if cmp -s reo/$k/remap_$k.u32 /ssd/shrink/max/streets/reo/$k/remap_$k.u32; then echo "$k remap == track's remap table"; else echo "$k remap DIFFERS from track"; fi
done
echo "== raw remapped entries (today's format)"
./entenc $I/geo_cells.bin 0 $I/street_entries.bin raw reo/street/street_entries.bin reo/street/remap_street.u32
cmp -s reo/street/street_entries.bin.cells $I/geo_cells.bin && echo "street: geo_cells unchanged by raw remap" || echo "street: geo_cells CHANGED"
rm -f reo/street/street_entries.bin.cells
./entenc $I/geo_cells.bin 2 $I/interp_entries.bin raw reo/interp/interp_entries.bin reo/interp/remap_interp.u32
cmp -s reo/interp/interp_entries.bin.cells $I/geo_cells.bin && echo "interp: geo_cells unchanged by raw remap" || echo "interp: geo_cells CHANGED"
rm -f reo/interp/interp_entries.bin.cells
echo "== query today's-format reorder (both)"; date
D=dirs/reo_both; mkdir -p $D
for f in $I/*; do ln -f "$f" "$D/"; done
for f in reo/street/*.bin reo/interp/*.bin; do b=$(basename $f); [ -e "$I/$b" ] && ln -f "$f" "$D/$b"; done
ls -la $D | grep -E "street_|interp_|way_|addr_points"
(cd $V && scripts/runq_v.sh st_reo_both $ST/$D base_max)
zs reo/street/street_nodes.bin reo/street/street_ways.bin reo/street/way_parents.bin reo/street/way_postcodes.bin \
   reo/street/street_entries.bin reo/street/addr_points.bin reo/interp/interp_nodes.bin reo/interp/interp_ways.bin \
   reo/interp/interp_postcodes.bin reo/interp/interp_entries.bin
rm -rf $D reo/street/addr_points.bin
echo "== packed ways"; date
./waypack enc 12 reo/street/street_ways.bin reo/street/street_nodes.bin 16 pk/s_g16
./waypack enc 24 reo/interp/interp_ways.bin reo/interp/interp_nodes.bin 16 pk/i_g16
$V/work/wdec street 16 pk/s_g16 pk/s_dec.ways pk/s_dec.nodes
cmp pk/s_dec.nodes reo/street/street_nodes.bin && echo "street: independent decode == reordered street_nodes.bin (byte-identical)"
cmp pk/s_dec.ways reo/street/street_ways.bin && echo "street: independent decode == reordered street_ways.bin (byte-identical)"
$V/work/wdec interp 16 pk/i_g16 pk/i_dec.ways pk/i_dec.nodes
cmp pk/i_dec.nodes reo/interp/interp_nodes.bin && echo "interp: independent decode == reordered interp_nodes.bin (byte-identical)"
cmp pk/i_dec.ways reo/interp/interp_ways.bin && echo "interp: independent decode == reordered interp_ways.bin (byte-identical)"
rm -f pk/*_dec.*
$P -u $V/scripts/split_cols.py street pk/s_g16.ways2 pk/s_cols
$P -u $V/scripts/split_cols.py interp pk/i_g16.ways2 pk/i_cols
zs pk/s_g16.nodes2 pk/s_g16.groups pk/s_cols.cnt pk/s_cols.code pk/s_cols.name \
   pk/i_g16.nodes2 pk/i_g16.groups pk/i_cols.cntcode pk/i_cols.street pk/i_cols.start pk/i_cols.span pk/i_cols.interp
echo "== sidecars"; date
$P -u sidecars.py reo/street side
$P -u sidecars.py reo/interp side
zs side/way_parents.bin.u24 side/way_postcodes.bin.u16dict side/way_postcodes.bin.dict side/interp_postcodes.bin.u16dict side/interp_postcodes.bin.dict
rm -rf reo/street/street_nodes.bin reo/interp/interp_nodes.bin pk/*.nodes2
echo "== cell lists"; date
INLINE=1 DEDUP=1 ./entenc $I/geo_cells.bin 0 $I/street_entries.bin gv fin/street_entries.gv reo/street/remap_street.u32
CELLS_IN=fin/street_entries.gv.cells INLINE=1 DEDUP=1 ./entenc $I/geo_cells.bin 2 $I/interp_entries.bin gv fin/interp_entries.gv reo/interp/remap_interp.u32
rm -f fin/street_entries.gv.cells; mv fin/interp_entries.gv.cells fin/geo_cells.new
$V/work/entdec $I/geo_cells.bin 0 $I/street_entries.bin reo/street/remap_street.u32 fin/geo_cells.new fin/street_entries.gv
$V/work/entdec $I/geo_cells.bin 2 $I/interp_entries.bin reo/interp/remap_interp.u32 fin/geo_cells.new fin/interp_entries.gv
zs fin/geo_cells.new fin/street_entries.gv fin/interp_entries.gv $I/geo_cells.bin
echo DONE; date
