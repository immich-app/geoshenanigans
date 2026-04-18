#!/bin/bash
set -euo pipefail

env="${1:-}"
if [ -z "$env" ]; then
  echo "Usage: $0 <env>"
  exit 1
fi

current_branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$current_branch" != "main" ]; then
  echo "Not on main branch. Current branch is $current_branch. Exiting."
  exit 0
fi

bucket="tiles-geo"
if [ "$env" != "prod" ]; then
  bucket="tiles-geo-${env}"
fi

tfvars_file="$(dirname "$0")/../modules/cloudflare/tiles-worker/tiles.${env}.tfvars.json"
if [ ! -f "$tfvars_file" ]; then
  echo "tfvars file not found: $tfvars_file"
  exit 1
fi

pmtiles_deployment_key=$(jq -r '.pmtiles_deployment_key' "$tfvars_file")

if [ -z "$pmtiles_deployment_key" ] || [ "$pmtiles_deployment_key" == "null" ]; then
  echo "Failed to extract pmtiles_deployment_key from $tfvars_file"
  exit 1
fi
echo "env=$env bucket=$bucket pmtiles_deployment_key=$pmtiles_deployment_key"

rclone delete "tigris:${bucket}" --exclude "/$pmtiles_deployment_key/**"
