# tiles-worker: Cloudflare provider v4 to v5

Prod's tiles-worker state was written by provider v4. Dev's already holds v5 entries (5.28.1, from the dev deploys of
#489). The live worker, its domains and its cron trigger stay as they are; only state changes.

## What the deploy does

- `cloudflare_workers_script.tiles` keeps its address, and v5's state upgrader rewrites prod's v4 entry on the first
  plan. Prod's script plans no change. Dev's apply replaces the build, bindings and cache settings #489 left there
  with main's.
- v5 can't read prod's `cloudflare_workers_domain.tiles` and `cloudflare_workers_domain.tiles_prod[0]` (v5 has no such
  type, and its successor's move support only takes `cloudflare_worker_domain`) or v4's
  `cloudflare_workers_cron_trigger.tiles` (v5's state upgrader rejects v4's schedules). A `moved` or `removed` block
  still has to read them, so neither works. `deployment/scripts/tiles-worker-v4-state.sh forget` removes those entries
  from state right before each apply of the module (Deploy Terragrunt and Deploy Dev Worker in `tiles-worker.yml`,
  Activate in `tiles-deploy.yml`), under the same `tiles-apply-<env>` concurrency group. The import blocks in
  `workers.tf` then adopt the live domains as `cloudflare_workers_custom_domain.tiles["<hostname>"]` and the cron
  trigger as `cloudflare_workers_cron_trigger.heartbeat`.
- v5 never reads a cron trigger's schedules back, so the imported trigger has none and the first apply sends the
  schedule list again: the same single `* * * * *` in prod. The `geoshenanigans-tiles-heartbeat` alert fires if the
  heartbeat stops after that.
- A PR's Plan Terragrunt jobs run the script's `plan`, which plans the module with those entries excluded and writes
  nothing. That is the plan the deploy applies once `forget` has run. Its "Missing resource schema" and "Failed to
  decode resource" warnings are about the excluded entries.

A Tiles Deploy dispatched before the merge runs its old workflow, without the `forget` step. If its Activate reaches
prod before main's deploy has migrated it, its apply fails before changing anything; re-run it afterwards.

## If an apply fails

Re-run the job. `forget` finds nothing left to remove, and the import blocks adopt whatever isn't in state yet. In
between, the worker, domains and cron trigger keep running unchanged, just unmanaged.

## Before merging: a copy of prod's state (optional)

The pg backend keeps only the current state. To be able to put prod's v4 state back exactly, stream it into 1Password
before merging, from `deployment/modules/cloudflare/tiles-worker` with no tiles deploy running. It holds the worker's
secrets, so it never touches disk, and the pull needs `--no-masking`, since `op run` would otherwise replace the
injected secrets in it:

```sh
set -euo pipefail
export ENVIRONMENT=prod
BACKUP="tiles-worker-prod state before provider v5"
op run --env-file=../../../.env -- terragrunt run -- init -reconfigure
op run --no-masking --env-file=../../../.env -- terragrunt run -- state pull |
  op document create - --vault tf_prod --title "$BACKUP" --file-name tiles-worker-prod.tfstate
if op document get "$BACKUP" --vault tf_prod | grep -q 'concealed by 1Password'; then
  echo "the copy is masked" >&2
  exit 1
fi
```

Dev needs none: nothing is removed from its state.

## Going back to v4

v4 can't read state v5 has written, and reverting the commit doesn't put the removed entries back. Revert the module
(`config.tf`, `workers.tf`, `.terraform.lock.hcl`) and the workflow steps. Then, for each environment and with no tiles
deploy running, work from `deployment/modules/cloudflare/tiles-worker` with `TF_VAR_tiles_build_dir` pointing at a
build of the worker (`npm run build` in `tiles`):

```sh
set -euo pipefail
export ENVIRONMENT=prod  # then dev
tg() { op run --env-file=../../../.env -- terragrunt run -- "$@"; }
tg init -reconfigure
```

and either push prod's copy back, if prod has applied nothing since it was taken:

```sh
op document get "tiles-worker-prod state before provider v5" --vault tf_prod | tg state push -force -
```

or adopt the live objects under the v4 types again, which needs no copy:

```sh
account=$(op read op://tf/cloudflare/account_id)
# Each custom domain's hostname and ID.
tg state pull | jq -r '.resources[] | select(.type == "cloudflare_workers_custom_domain") | .instances[]
  | "\(.index_key) \(.attributes.id)"'
tg state rm cloudflare_workers_script.tiles cloudflare_workers_custom_domain.tiles \
  cloudflare_workers_cron_trigger.heartbeat
tg import cloudflare_workers_script.tiles "$account/tiles-$ENVIRONMENT"
tg import cloudflare_workers_cron_trigger.tiles "$account/tiles-$ENVIRONMENT"
tg import cloudflare_workers_domain.tiles "$account/<ID of tiles.$ENVIRONMENT.immich.cloud>"
# prod only:
tg import 'cloudflare_workers_domain.tiles_prod[0]' "$account/<ID of tiles.immich.cloud>"
```

v4's import doesn't read secrets back, so the next v4 plan updates the script in place with them.

## Afterwards

Once prod and dev have applied, delete the import blocks and the `cloudflare_workers_custom_domains` data source in
`workers.tf`, the script, its test, the workflow steps that run it, and this file.
