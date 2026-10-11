#!/usr/bin/env bash
# Usage: fetch_previous_index.sh <published build prefix> <dest dir> <status file>
#
# Fetches a published build's client files for patch generation: every file
# but the .bin ones (never published) and the strategy-2 *.osm_ids sidecars
# (the build fetches those itself, earlier). Each .bin.zst is decompressed as
# it streams in, so the compressed copy never touches the disk: on a runner
# whose disk is the bottleneck that is 2 x ~40 GB less I/O beside the build.
# The build runs meanwhile, so this starts with it and the patch step waits
# for <status file>, written last with the exit status.
#
# Env: S3_ENDPOINT and the AWS_* credentials s5cmd reads.
set -uo pipefail

src=${1%/}
dest=$2
status=$3
rm -f "$status"

# fetch_one <key under src>
fetch_one() {
  local key=$1
  mkdir -p "$dest/$(dirname "$key")"
  case "$key" in
    *.bin.zst)
      s5cmd --endpoint-url "$S3_ENDPOINT" --retry-count 5 cat "$src/$key" \
        | zstd -d -q -f -o "$dest/${key%.zst}" ;;
    *)
      s5cmd --endpoint-url "$S3_ENDPOINT" --retry-count 5 cp "$src/$key" "$dest/$key" > /dev/null ;;
  esac
}
export -f fetch_one
export src dest

fetch() {
  local start keys
  start=$(date +%s)
  keys=$(s5cmd --endpoint-url "$S3_ENDPOINT" --retry-count 5 ls "$src/*" | awk '{print $NF}' \
    | sed "s|^$src/||" | grep -v -E '\.(bin|osm_ids)$') || return 1
  [ -n "$keys" ] || return 1
  # Each worker streams one object at a time; 16 of them fill the link.
  printf '%s\n' "$keys" | xargs -r -P 16 -I{} bash -c 'fetch_one "$1"' _ {} || return 1
  echo "[old-output] Fetched $(printf '%s\n' "$keys" | wc -l) files in $(( $(date +%s) - start ))s"
}

fetch
echo $? > "$status"
