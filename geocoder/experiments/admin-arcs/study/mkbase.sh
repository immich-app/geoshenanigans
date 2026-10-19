#!/bin/bash
# mkbase.sh [build name, default d1006] : base flat sets from a planet build, as /ssd/tiers2/sets/base:
#   smallest    = planet/admin-minimal/*
#   recommended = planet/admin/{cells,entries,place_*,postcode_*,strings_core,strings_postcode}
#                 + planet/poi/major/* + planet/quality/q2.5/{admin,postal}_{polygons,vertices}
set -euo pipefail
A=/ssd/arcs
b=${1:-d1006}
B=$A/build/out-$b/planet
S=$A/sets/base
[ "$b" = d1006 ] || S=$A/sets/base-$b
rm -rf $S; mkdir -p $S/smallest $S/recommended
for f in $B/admin-minimal/*; do ln -s "$f" $S/smallest/; done
R=$S/recommended
for f in admin_cells.bin admin_entries.bin place_cells.bin place_entries.bin place_nodes.bin postcode_centroid_cells.bin \
         postcode_centroid_entries.bin postcode_centroids.bin strings_core.bin strings_postcode.bin; do ln -s $B/admin/$f $R/; done
for f in poi_cells.bin poi_entries.bin poi_meta.json poi_records.bin poi_vertices.bin strings_layout.json strings_poi.bin; do ln -s $B/poi/major/$f $R/; done
for f in admin_polygons.bin admin_vertices.bin postal_polygons.bin postal_vertices.bin; do ln -s $B/quality/q2.5/$f $R/; done
ls -la $S/smallest $R
