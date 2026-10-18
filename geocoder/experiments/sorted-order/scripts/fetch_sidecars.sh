#!/bin/bash
# fetch the strategy-2 osm_ids sidecars (street/addr/interp) for one date
set -eu
B=https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds
d=$1; out=/ssd/shrink/sortpatch/sort/sidecars/$d; mkdir -p $out
for f in street_ways addr_points interp_ways; do
  curl -sSf --retry 5 -o $out/$f.osm_ids.part $B/$d/planet/full/$f.osm_ids && mv $out/$f.osm_ids.part $out/$f.osm_ids
  echo ok $d $f $(stat -c%s $out/$f.osm_ids)
done
echo DONE $d
