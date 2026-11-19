#!/usr/bin/env bash
# Puts Worker secrets on a tiles worker through Cloudflare's API: terraform_data.bunny_secrets runs it after every apply
# (workers.tf), so that the Bunny account key and the Globalping token never sit in OpenTofu's state.
#
#   ACCOUNT_ID=<account> SCRIPT=tiles-prod SECRETS="BUNNY_API_KEY GLOBALPING_TOKEN" CLOUDFLARE_API_TOKEN=<token> \
#     PLANNED_AT=<the plan's time, RFC 3339 UTC> BUNNY_API_KEY=<value> GLOBALPING_TOKEN=<value> put-worker-secrets.sh
#
# Each secret's value is the environment variable of its name. Neither the values nor the token go on a command line.
#
# An apply that uploads the script has just deployed a version without these secrets (workers.tf). Should putting one
# fail, the worker would run without its log reader or probe until a later apply, so before putting any this notes the
# deployment that served before that upload, if the upload dropped a secret that one had; when a PUT then fails it
# deploys that version again, whose code, bindings and secrets (a version keeps the values it was deployed with) are
# what served before the apply, and fails the apply. The next apply's refresh sees the older code, so it uploads the
# script again and puts the secrets. The upload is the latest deployment only if it is the one deployment made since
# the plan (PLANNED_AT): an apply that uploaded nothing (as after an earlier one that uploaded and could put no secret)
# goes back to nothing, since the deployment before the latest then served before an earlier apply, not this one.
set -euo pipefail

: "${ACCOUNT_ID:?}" "${SCRIPT:?}" "${SECRETS:?}" "${CLOUDFLARE_API_TOKEN:?}" "${PLANNED_AT:?}"
api="https://api.cloudflare.com/client/v4/accounts/$ACCOUNT_ID/workers/scripts/$SCRIPT"

# An RFC 3339 UTC time as seconds since the epoch, fractions kept (Cloudflare's created_on has them; fromdateiso8601
# doesn't take them); nothing for anything else.
epoch='def epoch: capture("^(?<s>[^.]+?)(?<f>\\.[0-9]+)?(?:Z|\\+00:00)$") | (.s + "Z" | fromdateiso8601) + ("0" + .f | tonumber);'
if ! jq -en --arg at "$PLANNED_AT" "$epoch"' $at | epoch' > /dev/null 2>&1; then
  echo "PLANNED_AT is not an RFC 3339 UTC time: $PLANNED_AT" >&2
  exit 1
fi

# Every name and value is checked before anything changes.
for name in $SECRETS; do
  if ! [[ "$name" =~ ^[A-Z][A-Z0-9_]*$ ]]; then
    echo "Not a secret name: $name" >&2
    exit 1
  fi
  if [ -z "${!name:-}" ]; then
    echo "$name is empty: not putting it on $SCRIPT" >&2
    exit 1
  fi
done

# cloudflare METHOD PATH: Cloudflare's answer for the script's PATH, the body on stdin for anything but GET.
cloudflare() {
  local body=()
  if [ "$1" != GET ]; then
    body=(-H 'Content-Type: application/json' --data-binary @-)
  fi
  # ${body[@]+...}: bash before 4.4 calls an empty array unset.
  curl --silent --show-error --retry 3 -X "$1" \
    -H @<(printf 'Authorization: Bearer %s\n' "$CLOUDFLARE_API_TOKEN") ${body[@]+"${body[@]}"} "$api$2"
}

succeeded() {
  jq -e '.success == true' <<<"$1" > /dev/null 2>&1
}

# An answer's errors, for a message.
errors() {
  local found
  if found=$(jq -ce '.errors' <<<"$1" 2> /dev/null) && [ -n "$found" ]; then
    echo "$found"
  else
    echo 'no API response'
  fi
}

# The names of a version's secrets, one a line; fails when the version can't be read.
secret_names() {
  local version
  version=$(cloudflare GET "/versions/$1") && succeeded "$version" &&
    jq -r '.result.resources.bindings[]? | select(.type == "secret_text") | .name' <<<"$version"
}

# The version that served before the apply's upload (the latest deployment before it; the first listed, the latest,
# serves now), if the latest dropped one of the secrets that version had; nothing when the latest isn't this apply's
# upload (the one deployment since the plan), either can't be read or one splits its traffic.
version_to_restore() {
  local deployments pair live previous live_secrets previous_secrets name
  deployments=$(cloudflare GET /deployments) && succeeded "$deployments" || return 0
  pair=$(jq -er --arg planned "$PLANNED_AT" "$epoch"'
    ($planned | epoch) as $planned
    | .result.deployments
    | (.[1:] | map({versions, at: (.created_on | epoch)})) as $before
    | select((.[0].created_on | epoch) >= $planned and ($before | length) > 0 and all($before[]; .at < $planned))
    | [.[0], ($before | max_by(.at))] | map(.versions | select(length == 1) | .[0].version_id) | select(length == 2)
    | join(" ")' <<<"$deployments") || return 0
  live=${pair% *}
  previous=${pair#* }
  live_secrets=$(secret_names "$live") && previous_secrets=$(secret_names "$previous") || return 0
  for name in $SECRETS; do
    if ! grep -qxF -- "$name" <<<"$live_secrets" && grep -qxF -- "$name" <<<"$previous_secrets"; then
      echo "$previous"
      return 0
    fi
  done
}

restore=$(version_to_restore 2> /dev/null) || restore=""
if [ -n "$restore" ]; then
  echo "Should a secret not go on, $SCRIPT goes back to version $restore, which served before this apply's upload"
fi

for name in $SECRETS; do
  # Each PUT makes a new version of the script with the secret and deploys it.
  response=$(jq -n --arg name "$name" '{name: $name, text: env[$name], type: "secret_text"}' | cloudflare PUT /secrets) ||
    response=""
  if succeeded "$response"; then
    echo "Put $name on $SCRIPT"
    continue
  fi
  echo "Could not put $name on $SCRIPT: $(errors "$response")" >&2
  if [ -z "$restore" ]; then
    echo "Nothing to go back to: $SCRIPT keeps the secrets it has now, without those not put, until an apply puts them" >&2
    exit 1
  fi
  # force: Cloudflare otherwise refuses to go back to a version whose secrets have changed since, as the upload's have.
  response=$(jq -n --arg version "$restore" --arg name "$name" '{
      strategy: "percentage",
      versions: [{version_id: $version, percentage: 100}],
      annotations: {"workers/message": "put-worker-secrets.sh could not put \($name): the version that served before the apply"}
    }' | cloudflare POST '/deployments?force=true') || response=""
  if succeeded "$response"; then
    echo "Deployed version $restore of $SCRIPT again, which served before this apply with its secrets; run the apply again" >&2
  else
    echo "Could not deploy version $restore of $SCRIPT again: $(errors "$response"); it runs without the secrets not put until an apply puts them" >&2
  fi
  exit 1
done
