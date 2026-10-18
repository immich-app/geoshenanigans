#!/bin/bash
# cleanup.sh : drop the large regenerable intermediates of /ssd/tiers2 (keeps the dump, scripts,
# key candidates' files and answers, every cmp/ result). Regenerate a candidate with
# ./mkvar.sh <name> <scheme> (~10 s) and its answers with ./runq.sh.
set -u
cd /ssd/tiers2
KEEPV="q25 cap10 cap10n uni75c10 uni75t8c10 uni75rc10 uni75t8rc10 uni75t8g4c10 uni75t8rg4c10"
KEEPQ="unc uncg uncng q25 q25g cap10 cap10g cap10n cap10ng uni75 uni75c10 uni75t8c10 uni75rc10 uni75t8rc10 uni75t8g4c10 uni75t8rg4c10 uni37 uni60c10 uni100c10"
P=build/out-dump/planet
mv $P/full/admin_parents.bin $P/admin_parents.bin.keep
rm -rf $P/full $P/no-addresses $P/poi/all $P/poi/notable $P/quality/q0.2 $P/quality/q0.5 $P/quality/q1 $P/quality/q1.5 $P/quality/q2
mkdir -p $P/full && mv $P/admin_parents.bin.keep $P/full/admin_parents.bin
for d in var/*; do n=$(basename $d); [[ " $KEEPV " == *" $n "* ]] || { rm -rf "$d" "sets/$n"; }; done
for d in q/*; do
  n=$(basename $d); n=${n%_smallest}; n=${n%_recommended}
  [ "$(basename $d)" = chunks ] && { rm -rf "$d"; continue; }
  [[ " $KEEPQ " == *" $n "* ]] || rm -rf "$d"
done
rm -rf topo/out_q25 topo/out_uni75
mkdir -p bin-gq && cp target-c/release/examples/grid_query bin-gq/grid_query.count && cp target/release/examples/grid_query bin-gq/grid_query.chain
rm -rf target-c cargo tmp/*
du -sh /ssd/tiers2
echo CLEANUP_DONE
