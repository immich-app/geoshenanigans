#!/usr/bin/env bash
# Prints the deployment key an environment's tiles worker is serving right now, read from the response header of
# its TileJSON. Git records what should be live; this is what is.
#
#   tiles-served-key.sh <dev|prod>
#
# It asks the worker's own hostname, tiles.<env>.immich.cloud, which always points at the worker. Prod's public one,
# tiles.immich.cloud, moves to Bunny at the cutover, which sends no deployment key (deployment/modules/bunny/tiles).
set -euo pipefail

env="${1:?usage: $0 <dev|prod>}"
host="tiles.$env.immich.cloud"

headers=$(curl -fsS --retry 3 -D - -o /dev/null "https://$host/v1.json")
key=$(tr -d '\r' <<<"$headers" | awk -F': ' 'tolower($1) == "pmtiles-deployment-key" { print $2 }')
if ! [[ "$key" =~ ^[0-9]{8}$ ]]; then
  echo "https://$host/v1.json has no deployment key header" >&2
  exit 1
fi
echo "$key"
