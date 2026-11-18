#!/usr/bin/env bash
# Usage: fetch_previous_index.sh <published build prefix> <dest dir> <status file> \
#          <geocoder-transport> <its build's status file>
#
# Fetches a published build's client files for patch generation: every file
# but the .bin ones (never published), the strategy-2 *.osm_ids sidecars
# (the build fetches those itself, earlier) and the place search indexes
# under <region>/search/ (no patches: clients rebuild them). Each .bin.zst is
# decompressed as it streams in, so the compressed copy never touches the
# disk: on a runner whose disk is the bottleneck that is 2 x ~40 GB less I/O
# beside the build. Each transport unit (*.tt.zst) is decoded into its files
# the same way, with geocoder-transport, whose build runs beside this one:
# a unit waits for that build's status file (its exit status, written last)
# and fails the fetch if the build did.
# The build runs meanwhile, so this starts with it and the patch step waits
# for <status file>, written last with the exit status.
#
# Env: S3_ENDPOINT and the AWS_* credentials s5cmd reads.
set -uo pipefail

src=${1%/}
dest=$2
status=$3
transport=$4
transport_status=$5
rm -f "$status"

# transport_built <key>: waits for geocoder-transport's build, at most 30
# minutes; fails unless it succeeded.
transport_built() {
  local waited=0
  until [ -s "$transport_status" ]; do
    if [ "$waited" -ge 1800 ]; then
      echo "[old-output] geocoder-transport was not built within 30 minutes: can't decode $1"
      return 1
    fi
    sleep 1
    waited=$((waited + 1))
  done
  [ "$(cat "$transport_status")" = 0 ] && return 0
  echo "[old-output] geocoder-transport failed to build: can't decode $1"
  return 1
}

# fetch_one <key under src>
fetch_one() {
  local key=$1 out
  local s5=(s5cmd --endpoint-url "$S3_ENDPOINT" --retry-count 5)
  mkdir -p "$dest/$(dirname "$key")"
  case "$key" in
    *.bin.zst)
      out=$dest/${key%.zst}
      "${s5[@]}" cat "$src/$key" 2> /dev/null | zstd -d -q -f -o "$out" 2> /dev/null && return 0
      # A stream cut mid-object can't be retried in the pipe: fetch the
      # whole file (s5cmd retries that) and decompress it instead.
      echo "[old-output] retrying $key as a download"
      "${s5[@]}" cp "$src/$key" "$out.zst" > /dev/null && zstd -d -q -f --rm "$out.zst" ;;
    *.tt.zst)
      transport_built "$key" || return 1
      out=$dest/$(dirname "$key")
      # A unit's files are renamed into place only once all of them decoded.
      "${s5[@]}" cat "$src/$key" 2> /dev/null | "$transport" decode - "$out" > /dev/null 2>&1 && return 0
      echo "[old-output] retrying $key as a download"
      "${s5[@]}" cp "$src/$key" "$dest/$key" > /dev/null && "$transport" decode "$dest/$key" "$out" > /dev/null \
        && rm -f "$dest/$key" ;;
    *)
      "${s5[@]}" cp "$src/$key" "$dest/$key" > /dev/null ;;
  esac
}
export -f fetch_one transport_built
export src dest transport transport_status

fetch() {
  local start keys
  start=$(date +%s)
  keys=$(s5cmd --endpoint-url "$S3_ENDPOINT" --retry-count 5 ls "$src/*" | awk '{print $NF}' \
    | sed "s|^$src/||" | grep -v -E '\.(bin|osm_ids)$|^[^/]+/search/') || return 1
  [ -n "$keys" ] || return 1
  # Each worker streams one object at a time; 16 of them fill the link.
  printf '%s\n' "$keys" | xargs -r -P 16 -I{} bash -c 'fetch_one "$1"' _ {} || return 1
  echo "[old-output] Fetched $(printf '%s\n' "$keys" | wc -l) files in $(( $(date +%s) - start ))s"
}

fetch
echo $? > "$status"
