#!/usr/bin/env bash
# Build the test dirs as hardlink farms of the flat 2026-10-13 sets (no extra
# disk): recommended minus postcode files, minus POI, minimal plus POI.
set -euo pipefail
W=/ssd/shrink/no-postcodes
REC=/ssd/shrink/idx/recommended
MIN=/ssd/shrink/idx/smallest
D=$W/dirs
PC_OPT="postcode_centroids.bin postcode_centroid_cells.bin postcode_centroid_entries.bin postal_polygons.bin postal_vertices.bin"
PC_ALL="$PC_OPT strings_postcode.bin"
POI="poi_cells.bin poi_entries.bin poi_records.bin poi_vertices.bin poi_meta.json strings_poi.bin"
rm -rf "$D"; mkdir -p "$D"
mk() { # name src exclude...
  local name=$1 src=$2; shift 2
  mkdir -p "$D/$name"
  for f in "$src"/*; do
    b=$(basename "$f")
    case " $* " in *" $b "*) continue;; esac
    ln "$f" "$D/$name/$b"
  done
}
mk rec_nopc "$REC" $PC_ALL
mk rec_nopc_keepstr "$REC" $PC_OPT
mk admin_nopc_nopoi "$REC" $PC_ALL $POI
mk admin_nopoi "$REC" $POI
mk minimal_poi "$MIN"
for f in $POI; do ln "$REC/$f" "$D/minimal_poi/$f"; done
for d in "$D"/*; do echo "$(basename $d): $(ls $d | wc -l) files, $(du -cb $d/* | tail -1 | cut -f1) bytes raw"; done
