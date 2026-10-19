#!/bin/bash
# osm6.sh : tags of the builder step changed polygons from planet-261018b (osmium getid, no
# referenced objects), as OPL; then osm6.py check. Plus every relation of an area type with
# place=state/province/region that is not an admin/postal boundary (the f7096b0 set), with its tags.
set -u
V=/ssd/verify6
PBF=/ssd/bugs2/planet/planet-261018b.osm.pbf
nice osmium getid -i $V/osm/ids.txt $PBF -o $V/osm/rels.osm.pbf -O --no-progress
osmium cat $V/osm/rels.osm.pbf -f opl -O -o $V/osm/rels.opl
echo "found $(wc -l < $V/osm/rels.opl) of $(wc -l < $V/osm/ids.txt)"
/ssd/shrink/venv/bin/python -I $V/scripts/osm6.py check > $V/cmp/osm6_check.md
nice osmium tags-filter $PBF r/place=state,province,region -o $V/osm/place_r0.osm.pbf -O --no-progress -R
osmium cat $V/osm/place_r0.osm.pbf -f opl -O -o $V/osm/place_r0.opl
echo OSM6_DONE
