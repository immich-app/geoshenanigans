#!/usr/bin/env bash
# Deletes tile deployments nobody needs any more from an environment's own area of the tiles-geo bucket: prod owns
# the unprefixed area, every other environment owns "<env>/". It keeps the key every environment's worker is
# serving (main's storage_prefix is what should be live, not proof of where the worker reads, e.g. after a failed
# activation) and, for each environment main says reads from this area, the key main records and the key that one
# replaced (for a quick rollback). Prefixes that aren't deployment keys are never touched, and an area nobody reads
# is left alone. Run it under the owner's deploy lock: only that environment publishes into its area, so nothing is
# in flight and anything else, such as a failed deploy's upload, can go.
# Expects an rclone remote named "tigris" (see deployment/tiles-deploy.env); tiles-cleanup.test.sh covers it.
#
#   tiles-cleanup.sh <dev|prod> [--dry-run]
set -euo pipefail

usage="usage: $0 <dev|prod> [--dry-run]"
if [ $# -lt 1 ] || [ $# -gt 2 ] || { [ $# -eq 2 ] && [ "$2" != --dry-run ]; }; then
  echo "$usage" >&2
  exit 2
fi
env=$1
dry_run="${2:-}"
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
if [ "$(git rev-parse --is-shallow-repository)" = true ]; then
  echo "Needs full git history to find previous deployments"
  exit 1
fi

if [ "$env" = prod ]; then
  area=""
else
  area="$env/"
fi

keep=()
readers=0
for tfvars in deployment/modules/cloudflare/tiles-worker/tiles.*.tfvars.json; do
  reader=$(basename "$tfvars" .tfvars.json)
  reader=${reader#tiles.}
  served=$(deployment/scripts/tiles-served-key.sh "$reader")
  keep+=("$served")
  if [ "$(jq -r .storage_prefix "$tfvars")" != "$area" ]; then
    echo "$reader serves $served"
    continue
  fi
  live=$(jq -r .pmtiles_deployment_key "$tfvars")
  if ! [[ "$live" =~ ^[0-9]{8}$ ]]; then
    echo "Live key in $tfvars is '$live', not YYYYMMDD"
    exit 1
  fi
  previous=""
  for commit in $(git log --format=%H -- "$tfvars"); do
    key=$(git show "$commit:$tfvars" | jq -r .pmtiles_deployment_key)
    if [ "$key" != "$live" ]; then
      previous=$key
      break
    fi
  done
  echo "$reader reads tiles-geo/$area: served=$served live=$live previous=${previous:-none}"
  keep+=("$live" "$previous")
  readers=$((readers + 1))
done

if [ "$readers" -eq 0 ]; then
  echo "No environment reads tiles-geo/$area; leaving it alone"
  exit 0
fi

remote="tigris:tiles-geo/$area"
# A plain assignment so set -e stops the script when the listing fails.
dirs=$(rclone lsf --dirs-only "$remote")
for dir in $dirs; do
  key=${dir%/}
  if ! [[ "$key" =~ ^[0-9]{8}$ ]]; then
    continue
  fi
  if [[ " ${keep[*]} " == *" $key "* ]]; then
    echo "keep $key"
  elif [ "$dry_run" = --dry-run ]; then
    echo "would delete $key"
  else
    echo "delete $key"
    rclone purge "$remote$key"
  fi
done
