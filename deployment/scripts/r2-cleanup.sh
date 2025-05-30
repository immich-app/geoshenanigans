#!/bin/bash

# Get the current git branch
current_branch=$(git rev-parse --abbrev-ref HEAD)

# Check if the current branch is main
if [ "$current_branch" != "main" ]; then
  echo "Not on main branch. Current branch is $current_branch. Exiting."
  exit 0
fi

# Define an array of strings
regions=("apac" "eeur" "enam" "wnam" "weur" "oc")

# Define the path to the JSON file
json_file="$(dirname "$0")/../modules/cloudflare/tiles-worker/tiles.tfvars.json"

# Extract the value of pmtiles_deployment_key using jq
pmtiles_deployment_key=$(cat "$json_file" | jq -r '.pmtiles_deployment_key')

# Check if the value was extracted correctly
if [ -z "$pmtiles_deployment_key" ] || [ "$pmtiles_deployment_key" == "null" ]; then
  echo "Failed to extract pmtiles_deployment_key"
  exit 1
else
  echo "pmtiles_deployment_key: $pmtiles_deployment_key"
fi

# Loop over each string in the array
for region in "${regions[@]}"; do
  echo "Processing region: $region"
  rclone delete "r2:tiles-$region" --exclude "/$pmtiles_deployment_key/"
done
