#!/usr/bin/env bash
# Usage: fetch_day_diff.sh <sequence> <out> <seconds>
#
# The day's replication diff <sequence> (planet.openstreetmap.org's
# replication/day), which the nightly's download step fetches beside the PBF
# for Apply replication diffs: written to <out> once whole (gzip -t), through
# <out>.part. At most <seconds>, its retries included: a server that accepts
# the connection and then stalls must not hold up the step, and Apply
# replication diffs fetches the diff itself when this left none. The exit
# status is 0 when <out> is the diff.
set -uo pipefail

seq=$1
out=$2
limit=$3
start=$(date +%s)

path=$(printf "%03d/%03d/%03d" $((seq / 1000000)) $(((seq / 1000) % 1000)) $((seq % 1000)))
if timeout "$limit" curl -sL --fail --retry 5 --retry-delay 30 --retry-all-errors -o "$out.part" \
     "https://planet.openstreetmap.org/replication/day/$path.osc.gz" \
   && gzip -t "$out.part"; then
  mv "$out.part" "$out"
  echo "[diff] seq $seq: $(( $(stat -c%s "$out") / 1024 / 1024 )) MiB in $(($(date +%s) - start))s"
  exit 0
fi
rm -f "$out.part"
echo "::warning::[diff] Fetching the diff of seq $seq failed or took over ${limit}s; Apply replication diffs fetches it"
exit 1
