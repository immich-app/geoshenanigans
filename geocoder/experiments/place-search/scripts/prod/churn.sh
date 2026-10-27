#!/usr/bin/env bash
# Day-to-day churn of the derived index (published planet max sets 2026-10-12 -> 2026-10-13,
# built without the search flags: every named ring is a boundary entity, no label links).
# Index patch sizes: zstd 1.5.7 --patch-from (-19 --long), per file.
set -euo pipefail
R=/ssd/search-prod
Z=/ssd/shrink/zstd157/bin/zstd
cd $R
mkdir -p results/churn tmp/build-days
for d in 2026-10-12 2026-10-13; do
  if [ ! -f idx/day-$d/search_meta.json ]; then
    target/release/place-search build days/$d idx/day-$d --threads 8 --mem-mb 256 --tmp tmp/build-days > logs/build-day-$d.log 2>&1
  fi
done
A=idx/day-2026-10-12
B=idx/day-2026-10-13
{
  for f in places.fst places.post places.pfx.fst places.pfx places.bnd poi.fst poi.post poi.pfx.fst poi.pfx; do
    full=$($Z -19 -T16 --long=31 -c -q $B/$f | wc -c)
    patch=$($Z -19 -T16 --long=31 --patch-from=$A/$f -c -q $B/$f | wc -c)
    echo "$f raw $(stat -c %s $B/$f) zstd $full patch-from $patch"
  done
  target/release/place-search diff $A $B places
  target/release/place-search diff $A $B poi
} | tee results/churn/index_churn.txt
