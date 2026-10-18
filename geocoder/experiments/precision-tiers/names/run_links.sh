#!/bin/bash
# run_links.sh: one cold planet build of planet-261006 with the instrumented
# builder's --link-dumps, the nightly's arguments and inputs and cap 0 (the
# build step's out-nocap configuration), so place_nodes / admin polygons can be
# compared byte for byte with /ssd/precision/build/out-nocap.
# Output /ssd/precision/names/out-links (deleted after the comparison), logs and
# links.tsv in /ssd/precision/names/run-links.
set -u
P=/ssd/precision
N=$P/names
out=$N/out-links
d=$N/run-links
bin=$N/build-index.links
export TMPDIR=$P/tmp

until (set -o noclobber; echo "names:links" > $P/BUILD.lock) 2> /dev/null; do sleep 15; done
trap 'rm -f $P/BUILD.lock' EXIT

rm -rf "$out" "$d"
mkdir -p "$d" "$TMPDIR"
echo "start $(date -Is)" > "$d/run.txt"
start=$(date +%s.%N)
systemd-run --quiet --scope --slice=precision.slice --unit=prec-names-links -p MemoryMax=96G -p MemorySwapMax=0 \
  env GC_MEMORY_RESERVE_MIB=12288 TMPDIR=$TMPDIR \
  /usr/bin/time -v $bin $out /ssd/work/planet-261006.osm.pbf --continents --multi-output --multi-quality \
    --tiger-data /ssd/work/tiger-data.tar.gz \
    --wikidata-sitelinks /ssd/work/qid_sitelinks.bin \
    --external-postcodes /ssd/review/fixval2/gb_postcodes.csv.gz \
    --building-outline-cap 0 --link-dumps $d > $d/build.log 2>&1
rc=$?
end=$(date +%s.%N)
echo "end $(date -Is) rc=$rc wall=$(awk "BEGIN { printf \"%.1f\", $end - $start }")" >> "$d/run.txt"
for f in full/place_nodes.bin full/place_nodes.osm_ids admin/place_nodes.bin quality/uncapped/admin_polygons.bin \
         quality/uncapped/admin_polygons.osm_ids poi/all/poi_records.bin poi/all/poi_records.osm_ids; do
  if cmp -s "$out/planet/$f" "$P/build/out-nocap/planet/$f"; then echo "same $f"; else echo "DIFF $f"; fi
done >> "$d/run.txt"
echo "RUN_DONE rc=$rc" >> "$d/run.txt"
