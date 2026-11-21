#!/usr/bin/env bash
# Usage: place_search_indexes.sh <output dir> <place-search> <its build's status file> <seconds>
#
# The nightly's place search indexes (build_search_indexes.py), built in the
# background from build-index's exit, beside compression and the patches.
# Waits for place-search's build (its status file, the exit status written
# last; at most 30 minutes), then builds the indexes, stopped past <seconds>.
# Compression waits for this script's own status before it takes their files
# (compress_index.py --late-search), so a build that hangs must end; one
# stopped, or ended by a signal, may have left an index half-written, so every
# index goes with it (their clients build theirs on the device). Their
# scratch (the selections' file sets and place-search's spills) goes in a dir
# of this script's own under $TMPDIR, removed however the build ends: a
# stopped build_search_indexes.py removes none of its own. The exit status is
# build_search_indexes.py's, 124 past <seconds>, 128 + the signal that ended
# it, or 1 when place-search was not built or the scratch dir could not be
# made.
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

if ! scratch=$(mktemp -d "${TMPDIR:-/tmp}/place-search-indexes-XXXXXX"); then
  echo "::error::No scratch dir for the place search indexes; none is built"
  exit 1
fi
status=0
TMPDIR=$scratch timeout -k 60 "$limit" python3 "$(dirname "$0")/build_search_indexes.py" "$out" "$binary" || status=$?
rm -rf "$scratch"
if [ "$status" = 124 ] || [ "$status" = 137 ]; then
  status=124
  rm -rf "$out"/*/search
  echo "::error::The place search indexes took over ${limit}s and were stopped; none is published"
elif [ "$status" -gt 128 ]; then
  rm -rf "$out"/*/search
  echo "::error::The place search indexes' build was ended by signal $((status - 128)); none is published"
fi
echo "Place search indexes done in $(($(date +%s) - start))s (exit status $status)"
exit "$status"
