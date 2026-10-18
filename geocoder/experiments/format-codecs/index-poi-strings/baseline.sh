#!/bin/bash
# today's files, zstd 1.5.7 -18 -T16 (whole file), vs published size_zst (-18 -T1)
Z=/ssd/shrink/zstd157/bin/zstd
D=/ssd/shrink/idx/max
O=/ssd/shrink/max/index-poi-strings/out/baseline_T16.tsv
: > $O
for f in poi_meta.json strings_layout.json poi_cells.bin admin_cells.bin admin_entries.bin strings_addr.bin strings_postcode.bin strings_core.bin strings_street.bin strings_poi.bin poi_entries.bin poi_vertices.bin poi_records.bin addr_entries.bin interp_entries.bin street_entries.bin geo_cells.bin; do
  t=$(date +%s.%N)
  s=$($Z -18 -T16 -q -c $D/$f | wc -c)
  e=$(date +%s.%N)
  printf "%s\t%s\t%s\t%.1f\n" $f $(stat -c %s $D/$f) $s $(echo "$e - $t" | bc) >> $O
done
echo done >> $O
