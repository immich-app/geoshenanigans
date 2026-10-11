#!/usr/bin/env bash
# Usage: fetch_previous_index.sh <published build prefix> <dest dir> <status file>
#
# Fetches a published build's client files for patch generation: every file
# but the .bin ones (never published) and the strategy-2 *.osm_ids sidecars
# (the build fetches those itself, earlier), then decompresses each .bin.zst
# in place. The build runs meanwhile, so this starts with it and the patch
# step waits for <status file>, written last with the exit status.
#
# Env: S3_ENDPOINT and the AWS_* credentials s5cmd reads.
set -uo pipefail

src=$1
dest=$2
status=$3
rm -f "$status"

fetch() {
  local start
  start=$(date +%s)
  s5cmd --endpoint-url "$S3_ENDPOINT" --retry-count 5 --numworkers 32 cp --concurrency 8 \
    --exclude "*.bin" --exclude "*.osm_ids" "$src/*" "$dest/" > /dev/null || return 1
  echo "[old-output] Downloaded $(find "$dest" -type f | wc -l) files in $(( $(date +%s) - start ))s"
  start=$(date +%s)
  find "$dest" -name "*.bin.zst" -print0 | xargs -0 -r -n1 -P "$(nproc)" zstd -d -f --rm --quiet || return 1
  echo "[old-output] Decompressed in $(( $(date +%s) - start ))s"
}

fetch
echo $? > "$status"
