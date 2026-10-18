#!/bin/bash
# Remove the large intermediates of the streets track; keep scripts, logs, tsv
# results, query outputs and the two remap tables (old -> new way id).
W=/ssd/shrink/max/streets
rm -rf $W/dirs/reo_street $W/dirs/reo_interp $W/dirs/reo_both
rm -f $W/reo/street/street_nodes.bin $W/reo/street/street_ways.bin $W/reo/street/street_entries.bin \
      $W/reo/street/addr_points.bin $W/reo/street/way_parents.bin $W/reo/street/way_postcodes.bin
rm -f $W/reo/interp/interp_nodes.bin $W/reo/interp/interp_ways.bin $W/reo/interp/interp_entries.bin \
      $W/reo/interp/interp_postcodes.bin
rm -f $W/eval/today/s_g16.* $W/eval/o1/s_g16.* $W/eval/itoday/i_g16.* $W/eval/ireo/i_g16.*
rm -f $W/wp/o1_g32.ways2 $W/wp/o1_g32.groups $W/wp/o1_g32.nodes2 $W/wp/ireo_g32.ways2 $W/wp/ireo_g32.groups $W/wp/ireo_g32.nodes2
rm -f $W/final/geo_cells.new.bin $W/final/street_entries.gv_in_dd.bin $W/final/interp_entries.gv_in_dd.bin
rm -rf $W/final/side_s $W/final/side_i $W/side
du -sh $W
df -h /ssd | tail -1
