#!/bin/bash

# Check if an argument is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <kv_namespace_id>"
  exit 1
fi

# Use the input argument
kv_namespace_id=$1

# Define the path to the JSON file
json_file="../modules/cloudflare/tiles-worker/tiles.tfvars.json"

# Extract the value of pmtiles_deployment_key using jq
pmtiles_deployment_key=$(cat "$json_file" | jq -r '.pmtiles_deployment_key')

# Check if the value was extracted correctly
if [ -z "$pmtiles_deployment_key" ] || [ "$pmtiles_deployment_key" == "null" ]; then
  echo "Failed to extract pmtiles_deployment_key"
  exit 1
else
  echo "pmtiles_deployment_key: $pmtiles_deployment_key"
fi

wrangler kv:key list --namespace-id=$kv_namespace_id > KEYS.json
cat KEYS.json | jq -c "[.[] | select( .name | contains(\"$pmtiles_deployment_key\") | not) | to_entries[] | .value]" | jq > PURGE.json
wrangler kv:bulk delete --namespace-id=$kv_namespace_id PURGE.json
rm -f KEYS.json PURGE.json
