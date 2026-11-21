#!/usr/bin/env bash
# Usage: place_search_indexes.sh <output dir> <place-search> <its build's status file> <seconds>
#
# The nightly's place search indexes (build_search_indexes.py), built in the
# background from build-index's exit, beside compression and the patches.
# Waits for place-search's build (its status file, the exit status written
# last; at most 30 minutes), then builds the indexes, stopped past <seconds>.
# Compression waits for this script's own status before it takes their files
# (compress_index.py --late-search), so a build that hangs must end; one
# stopped may have left an index half-written, so every index goes with it
# (their clients build theirs on the device). The exit status is
# build_search_indexes.py's, 124 past <seconds>, or 1 when place-search was
# not built.
set -uo pipefail

out=$1
binary=$2
build_status=$3
limit=$4
start=$(date +%s)

waited=0
until [ -s "$build_status" ]; do
  if [ "$waited" -ge 1800 ]; then
    echo "::error::place-search was not built within 30 minutes; no place search indexes"
    exit 1
  fi
  sleep 1
  waited=$((waited + 1))
done
if [ "$(cat "$build_status")" != 0 ]; then
  echo "::error::Building place-search failed; no place search indexes"
  exit 1
fi

status=0
timeout -k 60 "$limit" python3 "$(dirname "$0")/build_search_indexes.py" "$out" "$binary" || status=$?
if [ "$status" = 124 ] || [ "$status" = 137 ]; then
  status=124
  rm -rf "$out"/*/search
  echo "::error::The place search indexes took over ${limit}s and were stopped; none is published"
fi
echo "Place search indexes done in $(($(date +%s) - start))s (exit status $status)"
exit "$status"
