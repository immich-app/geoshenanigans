#!/usr/bin/env bash
# Clears what Cloudflare provider v4 left in an environment's tiles-worker state that provider v5 can't read: the
# cloudflare_workers_domain entries (v5 has no such type, and its successor's move support only takes
# cloudflare_worker_domain) and v4's cloudflare_workers_cron_trigger (v5's state upgrader rejects v4's list of cron
# strings). Neither can be moved in config, and a removed block still has to read them. Taking them out of state
# detaches nothing: the import blocks in the module's workers.tf adopt the live domains and cron trigger as they are.
#
#   forget  Removes those entries from state. The jobs that apply the module run it right before their apply, under
#           the same concurrency group.
#   plan    For a PR's plan, which must not write state: plans the module with those entries left out (-exclude),
#           which is the plan the deploy applies once forget has run. In GitHub Actions it sets the step output
#           planned=true when it did, so the job's Plan All can skip the module.
#
# Both do nothing once the entries are gone, so they're safe on every run, and a run whose apply failed after forget
# recovers by running again. Run it like the terragrunt jobs: ENVIRONMENT set, under op run --env-file=deployment/.env.
#
#   tiles-worker-v4-state.sh forget|plan
set -euo pipefail

usage="usage: $0 forget|plan"
if [ $# -ne 1 ]; then
  echo "$usage" >&2
  exit 2
fi
mode=$1
# What terragrunt.hcl picks the var file and state schema by; deployment/.env sets it from ENVIRONMENT.
env=${TF_VAR_env:?set ENVIRONMENT and run under op run --env-file=deployment/.env}
module_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../modules/cloudflare/tiles-worker" && pwd)

tg() {
  (cd "$module_dir" && terragrunt run --non-interactive -- "$@")
}

set_output() {
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "$1" >> "$GITHUB_OUTPUT"
  fi
}

# Addresses of the v4 entries, one per line. The state holds the worker's secrets, so it's only ever piped. A v4 cron
# trigger keeps its schedules as strings, where v5 keeps objects.
v4_entries() {
  tg state pull | jq -r '
    .resources[]?
    | select(.mode == "managed" and (.module // "") == "")
    | . as $r
    | .instances[]
    | select($r.type == "cloudflare_workers_domain"
        or ($r.type == "cloudflare_workers_cron_trigger" and (.attributes.schedules[0]? | type) == "string"))
    | "\($r.type).\($r.name)" + (if .index_key == null then "" else "[\(.index_key | tojson)]" end)'
}

# Terragrunt's auto-init doesn't notice the pg backend's schema changing with ENVIRONMENT, so a checkout that last ran
# the other environment would read and write that one's state.
tg init -input=false -reconfigure >&2

found=$(v4_entries)
entries=()
if [ -n "$found" ]; then
  mapfile -t entries <<<"$found"
fi

case "$mode" in
  forget)
    if [ ${#entries[@]} -eq 0 ]; then
      echo "$env: no provider v4 entries in tiles-worker state"
      exit 0
    fi
    echo "$env: removing ${#entries[@]} provider v4 entries from tiles-worker state; the apply imports the live objects"
    printf '  %s\n' "${entries[@]}"
    tg state rm -lock-timeout=1m "${entries[@]}"
    if [ -n "$(v4_entries)" ]; then
      echo "::error::$env tiles-worker state still has provider v4 entries after state rm" >&2
      exit 1
    fi
    ;;
  plan)
    if [ ${#entries[@]} -eq 0 ]; then
      echo "$env: no provider v4 entries in tiles-worker state; Plan All plans the module"
      set_output planned=false
      exit 0
    fi
    echo "$env: the deploy removes these provider v4 entries from tiles-worker state before its apply:"
    printf '  %s\n' "${entries[@]}"
    echo "$env: planning the module without them (the state is unchanged); this is the plan the deploy applies"
    exclude=()
    for entry in "${entries[@]}"; do
      exclude+=("-exclude=$entry")
    done
    tg plan -input=false "${exclude[@]}"
    set_output planned=true
    ;;
  *)
    echo "$usage" >&2
    exit 2
    ;;
esac
