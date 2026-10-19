#!/bin/bash
# mk_psets.sh: private copies of the set files the index build reads, so no other track's page
# cache (other tracks map the same /ssd/precision files) is charged outside the 32 MiB cgroup
# while counting as the build's own. Files the build never reads stay symlinks. Copies run
# throttled (60 MB/s) to keep other tracks' timing undisturbed.
set -eu
S=/ssd/searchbuild
P=$S/psets
copy_max="place_nodes.bin admin_polygons.bin admin_vertices.bin admin_cells.bin admin_entries.bin place_cells.bin place_entries.bin poi_records.bin poi_meta.json strings_layout.json strings_core.bin strings_poi.bin strings_street.bin strings_addr.bin strings_postcode.bin"
for s in minimal recommended max; do
  mkdir -p $P/$s
  for f in $S/sets/$s/*; do
    b=$(basename $f); real=$(readlink -f $f)
    rm -f $P/$s/$b
    if [ $s != max ] || [[ " $copy_max " == *" $b "* ]]; then
      systemd-run --scope -q -p "IOReadBandwidthMax=/dev/md4 60M" -p "IOWriteBandwidthMax=/dev/md4 60M" cp $real $P/$s/$b
    else
      ln -s $real $P/$s/$b
    fi
  done
  echo "psets $s: $(du -sh $P/$s | cut -f1) copied"
done
for s in minimal recommended max; do
  for f in $P/$s/*; do [ -L $f ] && continue; cmp -s $f $(readlink -f $S/sets/$s/$(basename $f)) || echo "COPY DIFF $f"; done
done
echo PSETS_DONE
