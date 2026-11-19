#!/usr/bin/env bash
# Puts Worker secrets on a tiles worker through Cloudflare's API: terraform_data.bunny_secrets runs it after every apply
# (workers.tf), so that the Bunny account key and the Globalping token never sit in OpenTofu's state.
#
#   ACCOUNT_ID=<account> SCRIPT=tiles-prod SECRETS="BUNNY_API_KEY GLOBALPING_TOKEN" CLOUDFLARE_API_TOKEN=<token> \
#     BUNNY_API_KEY=<value> GLOBALPING_TOKEN=<value> put-worker-secrets.sh
#
# Each secret's value is the environment variable of its name. Neither the values nor the token go on a command line.
set -euo pipefail

: "${ACCOUNT_ID:?}" "${SCRIPT:?}" "${SECRETS:?}" "${CLOUDFLARE_API_TOKEN:?}"

for name in $SECRETS; do
  if ! [[ "$name" =~ ^[A-Z][A-Z0-9_]*$ ]]; then
    echo "Not a secret name: $name" >&2
    exit 1
  fi
  if [ -z "${!name:-}" ]; then
    echo "$name is empty: not putting it on $SCRIPT" >&2
    exit 1
  fi
  # Each PUT makes a new version of the script with the secret and deploys it.
  response=$(jq -n --arg name "$name" '{name: $name, text: env[$name], type: "secret_text"}' |
    curl --silent --show-error --retry 3 -X PUT \
      -H @<(printf 'Authorization: Bearer %s\n' "$CLOUDFLARE_API_TOKEN") -H 'Content-Type: application/json' \
      --data-binary @- "https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID/workers/scripts/$SCRIPT/secrets")
  if ! jq -e '.success == true' <<<"$response" > /dev/null 2>&1; then
    echo "Could not put $name on $SCRIPT: $(jq -c '.errors' <<<"$response" 2>/dev/null || echo 'no API response')" >&2
    exit 1
  fi
  echo "Put $name on $SCRIPT"
done
