#!/usr/bin/env bash
# Hands the D1 proxy URL and this environment's token (outputs of the tiles module) to the later steps of a GitHub
# Actions job.
# Needs OP_SERVICE_ACCOUNT_TOKEN and ENVIRONMENT, like the other terragrunt runs.
set -euo pipefail

cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)/deployment/modules/cloudflare/tiles"
output() {
  op run --env-file=../../../.env -- terragrunt run --non-interactive -- output "$@"
}

url=$(output -raw d1_proxy_url)
token=$(output -raw "d1_proxy_token_$ENVIRONMENT")
# tofu prints warnings to stdout when an output is missing, so check what came back.
if ! [[ "$url" =~ ^https://[a-z0-9.-]+$ ]] || ! [[ "$token" =~ ^[A-Za-z0-9]{64}$ ]]; then
  echo "Unexpected D1 proxy outputs from the tiles module for $ENVIRONMENT" >&2
  exit 1
fi
echo "::add-mask::$token"
{
  echo "D1_PROXY_URL=$url"
  echo "D1_PROXY_TOKEN=$token"
} >> "$GITHUB_ENV"
