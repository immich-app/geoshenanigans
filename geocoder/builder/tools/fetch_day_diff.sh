#!/usr/bin/env bash
# Usage: fetch_day_diff.sh <sequence> <out> <seconds>
#
# The day's replication diff <sequence> (planet.openstreetmap.org's
# replication/day), which the nightly's download step fetches once the PBF is
# down, and Apply replication diffs when that left none: written to <out>
# once whole (gzip -t), through <out>.part. At most <seconds>, its retries and
# the check included: a server that accepts the connection and then stalls
# must not hold up the step. The exit status is 0 when <out> is the diff.
set -uo pipefail

seq=$1
out=$2
limit=$3
start=$(date +%s)

path=$(printf "%03d/%03d/%03d" $((seq / 1000000)) $(((seq / 1000) % 1000)) $((seq % 1000)))
# One deadline for the fetch and its check: timeout ends them both.
# The inner script expands its own arguments ($0 the partial file, $1 the URL).
# shellcheck disable=SC2016
if timeout "$limit" bash -c 'curl -sL --fail --retry 5 --retry-delay 30 --retry-all-errors -o "$0" "$1" && gzip -t "$0"' \
     "$out.part" "https://planet.openstreetmap.org/replication/day/$path.osc.gz"; then
  if ! mv -T -- "$out.part" "$out"; then
    rm -f "$out.part"
    echo "::warning::[diff] Could not move the diff of seq $seq into place"
    exit 1
  fi
  echo "[diff] seq $seq: $(( $(stat -c%s "$out") / 1024 / 1024 )) MiB in $(($(date +%s) - start))s"
  exit 0
fi
rm -f "$out.part"
echo "::warning::[diff] Fetching the diff of seq $seq failed or took over ${limit}s"
exit 1
