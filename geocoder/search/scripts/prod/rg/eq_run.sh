#!/usr/bin/env bash
# Reverse-geocode answers (grid_query = query_json per point), main reader (f48ddc4) vs branch
# head reader (66a5ad0), on one 130,781-point pool:
#   tiers2 L2 / L4 / L8 (+-200 m of country / state / city borders, 20,000 each), search-prod
#   pts_40k (40,000) and pts_grid (10,790), edge2k (14,999) and long15k (4,992) from gen_edge_pts.py
# Sets: sets-orig (shipped planet-261006 files) and sets (builder emulation) with both readers;
# tiers2 q25 and uni75t8rc10 (smallest, recommended) with both readers. find_admin alone
# (admin_dump) on 1,199,563 points near q2.5 ring edges (edge2k/long15k _1000000_200000: +-1.5 km of
# L2-L8 edges, +-15 km of edges over 50 km) for orig sets and tiers2 sets. Then admin ring work per
# query (rg_count, instrumented copies) on the first 50k of tiers2 p200k and on L2.
set -uo pipefail
R=/ssd/search-prod/rg
T=/ssd/tiers2
P=$R/pts
cat $T/pts/L2.txt $T/pts/L4.txt $T/pts/L8.txt /ssd/search-prod/tmp/pts_40k.txt /ssd/search-prod/tmp/pts_grid.txt \
  $P/edge2k.txt $P/long15k.txt > $P/pool.txt
head -50000 $T/pts/p200k.txt > $P/p50k.txt
cp $T/pts/L2.txt $P/L2.txt
mkdir -p $R/out $R/count
run() { # tag reader set-dir
  $R/bin/grid_query.$2 "$3" < $P/pool.txt > $R/out/$1.jsonl 2> $R/out/$1.err
}
admin() { # tag reader set-dir
  cat $P/edge2k_1000000_200000.txt $P/long15k_1000000_200000.txt | $R/bin/admin_dump.$2 "$3" > $R/out/$1.admin 2> $R/out/$1.admin.err
}
count() { # tag reader set-dir pool
  $R/bin/rg_count.$2-instr "$3" < $P/$4.txt > $R/count/$1.$4.json 2> $R/count/$1.$4.err
}
for s in minimal recommended max; do
  for r in main head; do
    run orig-$s.$r $r $R/sets-orig/$s &
    admin orig-$s.$r $r $R/sets-orig/$s &
    run emu-$s.$r $r /ssd/search-prod/sets/$s &
  done
done
for v in q25 uni75t8rc10; do
  for st in smallest recommended; do
    for r in main head; do
      run t2-$v-$st.$r $r $T/sets/$v/$st &
      admin t2-$v-$st.$r $r $T/sets/$v/$st &
    done
  done
done
wait
for s in minimal recommended max; do
  for r in main head; do
    for pool in p50k L2; do count orig-$s.$r $r $R/sets-orig/$s $pool & done
  done
done
for v in q25 uni75t8rc10; do
  for st in smallest recommended; do
    for r in main head; do
      for pool in p50k L2; do count t2-$v-$st.$r $r $T/sets/$v/$st $pool & done
    done
  done
done
wait
echo done > $R/out/DONE
