#!/usr/bin/env bash
# Estimate the OCI layer size of the minimal set: tar the dir as COPY would, compress like a registry layer.
set -euo pipefail
cd "$(dirname "$0")"
SRC=/ssd/shrink/idx
OUT=layer_size.out
: > "$OUT"
echo "tar_raw $(tar -cf - -C "$SRC" smallest | wc -c)" | tee -a "$OUT"
for lvl in 6 9; do
  echo "gzip-$lvl $(tar -cf - -C "$SRC" smallest | gzip -$lvl | wc -c)" | tee -a "$OUT"
done
for lvl in 3 19; do
  echo "zstd-$lvl $(tar -cf - -C "$SRC" smallest | zstd -q -T16 -$lvl -c | wc -c)" | tee -a "$OUT"
done
echo "zstd-19-long27 $(tar -cf - -C "$SRC" smallest | zstd -q -T16 --long=27 -19 -c | wc -c)" | tee -a "$OUT"
for f in "$SRC"/smallest/*; do
  echo "gzip6_file $(basename "$f") $(stat -c %s "$f") $(gzip -6 -c "$f" | wc -c)" | tee -a "$OUT"
done
# same for today's geodata dir as COPY would ship it (cities500.txt unzipped, + countryInfo + geojson)
echo "geodata_tar_raw $(tar -cf - -C geodata --exclude=cities500.zip . | wc -c)" | tee -a "$OUT"
echo "geodata_gzip-6 $(tar -cf - -C geodata --exclude=cities500.zip . | gzip -6 | wc -c)" | tee -a "$OUT"
echo "geodata_zstd-19 $(tar -cf - -C geodata --exclude=cities500.zip . | zstd -q -T16 -19 -c | wc -c)" | tee -a "$OUT"
