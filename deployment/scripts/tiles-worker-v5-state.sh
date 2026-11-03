#!/usr/bin/env bash
# Moves an environment's tiles-worker state off Cloudflare provider v4's custom domains. v4 attached the worker's
# hostnames as cloudflare_workers_domain, a type v5 doesn't have, so the module can't plan while those entries are in
# state, and neither moved blocks nor state mv can change a resource's type. Removing them from state detaches nothing:
# the import blocks in workers.tf adopt the live domains on the next apply. See the module's MIGRATION.md.
#
#   apply    Removes the v4 entries from state. Run it right before applying the module, under the same concurrency
#            group. Once they're gone it does nothing, so a run whose apply failed recovers by running again.
#   preview  Plans the module against a local copy of the state without the v4 entries, which is what apply and the
#            deploy's apply will do, and fails if that plan destroys or replaces anything. The remote state isn't
#            touched. The copy holds the worker's secrets, so it's only ever written encrypted, and the plan isn't
#            saved. Once the entries are gone it does nothing and the module plans as usual. In GitHub Actions it sets
#            the step output pending to true when it planned the module, so the job's own plan can leave it out.
#
# Run it like the terragrunt jobs: ENVIRONMENT set, under op run --env-file=deployment/.env.
#
#   tiles-worker-v5-state.sh apply|preview
set -euo pipefail

usage="usage: $0 apply|preview"
if [ $# -ne 1 ]; then
  echo "$usage" >&2
  exit 2
fi
mode=$1
# What terragrunt.hcl picks the var file and state schema by; deployment/.env sets it from ENVIRONMENT.
env=${TF_VAR_env:?set ENVIRONMENT and run under op run --env-file=deployment/.env}
module_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../modules/cloudflare/tiles-worker" && pwd)
tofu=${TG_TF_PATH:-tofu}
old_type=cloudflare_workers_domain
umask 077

tg() {
  (cd "$module_dir" && terragrunt run --non-interactive -- "$@")
}

# Terragrunt's auto-init doesn't notice a pg backend's schema changing, so a checkout that last ran another environment
# would read and write that environment's state.
tg_init() {
  group "terragrunt init"
  tg init -input=false -reconfigure >&2
  endgroup
}

# Addresses of the v4 entries in the state on stdin, as state list prints them. An environment without state pulls
# nothing, which has none (state list fails there).
old_entries() {
  jq -r --arg type "$old_type" '
    .resources[]? | select(.mode == "managed" and .type == $type and (.module // "") == "") | . as $r | .instances[]
    | "\($r.type).\($r.name)" + (if .index_key == null then "" else "[\(.index_key | tojson)]" end)'
}

group() {
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    echo "::group::$1"
  fi
}

endgroup() {
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    echo "::endgroup::"
  fi
}

set_pending() {
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "pending=$1" >> "$GITHUB_OUTPUT"
  fi
}

apply() {
  local found
  local -a old
  tg_init
  # Piped, so the state's secrets never touch disk.
  found=$(tg state pull | old_entries)
  mapfile -t old <<<"$found"
  if [ -z "$found" ]; then
    echo "$env: no $old_type entries in state, nothing to migrate"
    return
  fi
  echo "$env: removing ${#old[@]} v4 custom domain entries from state; the apply imports the live domains"
  printf '  %s\n' "${old[@]}"
  tg state rm -lock-timeout=1m "${old[@]}"
  found=$(tg state pull | old_entries)
  if [ -n "$found" ]; then
    echo "::error::$env state still has $old_type entries after state rm" >&2
    exit 1
  fi
  echo "$env: state migrated"
}

# OpenTofu encryption for preview's copy of the state, its backups and any plan, under a passphrase drawn for this run.
# The passphrase only ever lives in this process's memory and the environment of the tofu commands it runs, so what
# they write can't be read once the script is gone, and enforced stops tofu writing either in plaintext. Built with
# builtins, so the passphrase is never in a command line.
encryption_config() {
  local passphrase
  passphrase=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    echo "::add-mask::$passphrase"
  fi
  printf -v encryption '%s\n' \
    'key_provider "pbkdf2" "run" {' \
    "  passphrase = \"$passphrase\"" \
    '}' \
    'method "aes_gcm" "run" {' \
    '  keys = key_provider.pbkdf2.run' \
    '}' \
    'state {' \
    '  method   = method.aes_gcm.run' \
    '  enforced = true' \
    '}' \
    'plan {' \
    '  method   = method.aes_gcm.run' \
    '  enforced = true' \
    '}'
}

# tofu on preview's copy of the module. Only these commands get the encryption: terragrunt's, on the real state, don't.
copy_tofu() {
  TF_ENCRYPTION=$encryption "$tofu" "$@"
}

preview() {
  local state found events deletes status=0
  local -a old
  tg_init
  # The state holds the worker's secrets, so it stays in memory until tofu encrypts it, and goes through pipes rather
  # than here-strings, which bash can back with a temp file.
  state=$(tg state pull)
  found=$(printf '%s\n' "$state" | old_entries)
  mapfile -t old <<<"$found"
  if [ -z "$found" ]; then
    echo "$env: no $old_type entries in state, the module plans as usual"
    set_pending false
    return
  fi

  work=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/tiles-worker-v5.XXXXXX")
  trap 'rm -rf "$work"' EXIT
  encryption_config
  # A copy of the module whose backend is a local state in this private directory, so nothing here can write the
  # real one.
  mkdir "$work/module"
  tar -C "$module_dir" --exclude=./.terraform --exclude=./.terragrunt-cache -cf - . | tar -C "$work/module" -xf -
  jq -n --arg path "$work/terraform.tfstate" '{terraform: {backend: {local: {path: $path}}}}' \
    > "$work/module/backend_override.tf.json"
  cd "$work/module"
  group "$tofu init"
  copy_tofu init -input=false
  endgroup
  printf '%s\n' "$state" | copy_tofu state push - > /dev/null

  # The same removal apply makes, here on the copy.
  copy_tofu state rm "${old[@]}" > /dev/null
  echo "$env: the deploy removes these v4 custom domain entries from state before its apply:"
  printf '  %s\n' "${old[@]}"
  echo "$env: planning the module against a copy of the state without them (the remote state is unchanged)"
  # Not saved: a plan file would hold the worker's secrets. The JSON messages name each resource and its action but
  # carry no attribute values. The var file is the one terragrunt.hcl passes.
  events=$(copy_tofu plan -input=false -lock=false -json -var-file="tiles.$env.tfvars.json") || status=$?
  # Each planned change, the totals, and any warnings or errors.
  printf '%s\n' "$events" | jq -r '
    select(.type == "planned_change" or .type == "change_summary" or .type == "diagnostic")
    | .["@message"] + (.diagnostic.detail // "" | if . == "" then "" else "\n" + . end)'
  if [ "$status" -ne 0 ]; then
    echo "::error::the migrated $env plan failed" >&2
    exit "$status"
  fi

  deletes=$(printf '%s\n' "$events" | jq -r '
    select(.type == "planned_change" and (.change.action == "delete" or .change.action == "replace"))
    | .change.resource.addr')
  if [ -n "$deletes" ]; then
    echo "::error::the migrated $env plan destroys or replaces resources:" >&2
    echo "$deletes" >&2
    exit 1
  fi
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    echo "::notice::tiles-worker $env: the v5 state migration is pending; the plan above is what the deploy applies"
  fi
  set_pending true
}

case "$mode" in
  apply) apply ;;
  preview) preview ;;
  *)
    echo "$usage" >&2
    exit 2
    ;;
esac
