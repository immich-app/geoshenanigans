#!/bin/bash
set -euo pipefail

current_branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$current_branch" != "main" ]; then
  echo "Not on main branch. Current branch is $current_branch. Exiting."
  exit 0
fi

json_file="$(dirname "$0")/../modules/cloudflare/tiles-worker/tiles.tfvars.json"

pmtiles_deployment_key=$(jq -r '.pmtiles_deployment_key' "$json_file")

if [ -z "$pmtiles_deployment_key" ] || [ "$pmtiles_deployment_key" == "null" ]; then
  echo "Failed to extract pmtiles_deployment_key"
  exit 1
fi
echo "pmtiles_deployment_key: $pmtiles_deployment_key"

rclone delete "tigris:tiles-geo" --exclude "/$pmtiles_deployment_key/**"
