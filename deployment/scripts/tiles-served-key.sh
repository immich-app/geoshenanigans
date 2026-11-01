#!/usr/bin/env bash
# Prints the deployment key an environment's tiles worker is serving right now, read from the response header of
# its TileJSON. Git records what should be live; this is what is.
#
#   tiles-served-key.sh <dev|prod>
set -euo pipefail

env="${1:?usage: $0 <dev|prod>}"
if [ "$env" = prod ]; then
  host=tiles.immich.cloud
else
  host="tiles.$env.immich.cloud"
fi

headers=$(curl -fsS --retry 3 -D - -o /dev/null "https://$host/v1.json")
key=$(tr -d '\r' <<<"$headers" | awk -F': ' 'tolower($1) == "pmtiles-deployment-key" { print $2 }')
if ! [[ "$key" =~ ^[0-9]{8}$ ]]; then
  echo "https://$host/v1.json has no deployment key header" >&2
  exit 1
fi
echo "$key"
