#!/bin/bash
# Final geo_cells effect: street column (spatial way order, inline singles, dedup, gv lists)
# then interp column (same) stacked via CELLS_IN; whole-file zstd of today's vs new geo_cells.
set -e
cd /ssd/shrink/max/streets
I=/ssd/shrink/idx/max
mkdir -p final
INLINE=1 DEDUP=1 ./entenc $I/geo_cells.bin 0 $I/street_entries.bin gv final/street_entries.gv_in_dd.bin reo/street/remap_street.u32 > final/street_entries.log
CELLS_IN=final/street_entries.gv_in_dd.bin.cells INLINE=1 DEDUP=1 ./entenc $I/geo_cells.bin 2 $I/interp_entries.bin gv final/interp_entries.gv_in_dd.bin reo/interp/remap_interp.u32 > final/interp_entries.log
rm -f /ssd/shrink/max/streets/final/street_entries.gv_in_dd.bin.cells
mv final/interp_entries.gv_in_dd.bin.cells final/geo_cells.new.bin
cat final/street_entries.log final/interp_entries.log
T=${ZT:-12} ./zwhole.sh final/zwhole.tsv final/geo_cells.new.bin final/street_entries.gv_in_dd.bin final/interp_entries.gv_in_dd.bin
grep -q "idx/max/geo_cells.bin" final/zwhole.tsv || T=${ZT:-12} ./zwhole.sh final/zwhole.tsv $I/geo_cells.bin
cat final/zwhole.tsv
