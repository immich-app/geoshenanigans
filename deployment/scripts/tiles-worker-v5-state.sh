#!/usr/bin/env bash
# Moves an environment's tiles-worker state off Cloudflare provider v4's custom domains. v4 attached the worker's
# hostnames as cloudflare_workers_domain, a type v5 doesn't have, so the module can't plan while those entries are in
# state, and neither moved blocks nor state mv can change a resource's type. Removing them from state detaches nothing:
# the import blocks in workers.tf adopt the live domains on the next apply. See the module's MIGRATION.md.
#
#   apply    Removes the v4 entries from state. Run it right before applying the module, under the same concurrency
#            group. Once they're gone it does nothing, so a run whose apply failed recovers by running again.
#   preview  Plans the module against a local copy of the state without the v4 entries, which is what apply and the
#            deploy's apply will do, and fails if that plan destroys anything. The remote state isn't touched. Once
#            the entries are gone it does nothing and the module plans as usual. In GitHub Actions it sets the step
#            output pending to true when it planned the module, so the job's own plan can leave it out.
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

preview() {
  local state found deletes
  local -a old
  work=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/tiles-worker-v5.XXXXXX")
  trap 'rm -rf "$work"' EXIT
  # The state holds the worker's secrets. It stays in this private directory, which goes when the script exits.
  state="$work/terraform.tfstate"
  tg_init
  tg state pull > "$state"
  found=$(old_entries < "$state")
  mapfile -t old <<<"$found"
  if [ -z "$found" ]; then
    echo "$env: no $old_type entries in state, the module plans as usual"
    set_pending false
    return
  fi

  # A copy of the module whose backend is the local copy of the state, so nothing here can write the real one.
  mkdir "$work/module"
  tar -C "$module_dir" --exclude=./.terraform --exclude=./.terragrunt-cache -cf - . | tar -C "$work/module" -xf -
  jq -n --arg path "$state" '{terraform: {backend: {local: {path: $path}}}}' > "$work/module/backend_override.tf.json"
  cd "$work/module"
  # Keeps tofu from suggesting the saved plan be applied.
  export TF_IN_AUTOMATION=1
  group "$tofu init"
  "$tofu" init -input=false
  endgroup

  # The same removal apply makes, here on the copy.
  "$tofu" state rm "${old[@]}" > /dev/null
  echo "$env: the deploy removes these v4 custom domain entries from state before its apply:"
  printf '  %s\n' "${old[@]}"
  echo "$env: planning the module against a copy of the state without them (the remote state is unchanged)"
  # The same var file terragrunt.hcl passes.
  "$tofu" plan -input=false -lock=false -var-file="tiles.$env.tfvars.json" -out="$work/plan"

  deletes=$("$tofu" show -json "$work/plan" |
    jq -r '.resource_changes[]? | select(.change.actions | index("delete")) | .address')
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
