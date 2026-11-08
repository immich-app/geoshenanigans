# tiles-worker: Cloudflare provider v4 to v5

The module moved to provider v5 for Workers Cache (`cache_options`, `exports`). The live Worker and its custom domains
stay as they are; only Terraform state changes, and CI does that by itself.

## What happens to state

- `cloudflare_workers_script.tiles` keeps its address. v5's state upgrader rewrites the v4 entry on the first plan, and
  the plan is an in-place update of the script (new code, bindings list, cache settings).
- The custom domains move from `cloudflare_workers_domain.tiles` and `cloudflare_workers_domain.tiles_prod[0]` to
  `cloudflare_workers_custom_domain.tiles["<hostname>"]`. This can't be done in config: v5 has no
  `cloudflare_workers_domain` type, so it can't plan while those entries are in state ("no schema available"). Its move
  handler only accepts the singular `cloudflare_worker_domain`, and `tofu state mv` refuses to change resource types. So
  the two entries are removed from state, and the `import` blocks in `workers.tf` adopt the live domains by ID. Nothing
  is detached or re-attached.

## How it runs

`deployment/scripts/tiles-worker-v5-state.sh` does the state change, in two modes:

- `apply` removes every `cloudflare_workers_domain` entry from the environment's state. The jobs that apply the module
  run it right before their apply, in the same job and so under the same `tiles-apply-<env>` concurrency group:
  Deploy Terragrunt and Deploy Dev Worker in `tiles-worker.yml`, and Activate in `tiles-deploy.yml`.
- `preview` is for the PR's Plan Terragrunt jobs and never writes the remote state. It copies the state into the
  runner's temp directory, removes the same entries from that copy, and plans a copy of the module against it. The
  state holds the Worker's secrets, so the copy is only ever written encrypted (OpenTofu state encryption, under a
  passphrase made for the run and kept in memory), and the plan isn't saved. Plan All then leaves tiles-worker out,
  since it can't plan against the real state yet.

Once an environment's entries are gone, both modes do nothing: `preview` lets Plan All plan the module as usual, and
`apply` makes no state write. That makes them safe to run on every deploy, and a run whose apply failed recovers by
running again: the entries are already gone, and an import of a domain that's already in state is a no-op.

On the PR, each environment's Preview tiles-worker state migration step lists the entries the deploy will remove,
then each resource's planned action and the totals of the plan the deploy will then apply (not the attribute diff):
its domains imported (1 in dev, 2 in prod), `cloudflare_workers_script.tiles` updated in place, and nothing added or
destroyed. A local run against a v4-shaped state and a stub API gave `2 to import, 5 to add, 1 to change, 0 to destroy`
for prod, the adds being the Grafana resources that test state didn't have. The step fails if the plan would destroy
or replace anything.

On merge, main's Deploy Terragrunt migrates and applies prod, then Deploy Dev Worker does dev. No tiles deploy needs to
be held, with one exception: a Tiles Deploy dispatched before the merge runs the workflow without the migration step,
so if its Activate gets to an environment first, the apply fails before changing anything. Re-run it once main's deploy
has migrated that environment.

## By hand

Only if CI can't run it. From `deployment`, for each environment:

```sh
ENVIRONMENT=dev op run --env-file=.env -- scripts/tiles-worker-v5-state.sh apply  # then prod
```

Without the script, from `deployment/modules/cloudflare/tiles-worker`:

```sh
export ENVIRONMENT=dev  # then prod
tg() { op run --env-file=../../../.env -- terragrunt run -- "$@"; }
# Terragrunt doesn't re-init when ENVIRONMENT changes, and would otherwise use the last environment's state.
tg init -reconfigure
tg state rm 'cloudflare_workers_domain.tiles'
# prod only:
tg state rm 'cloudflare_workers_domain.tiles_prod[0]'
```

An apply of the v4 config afterwards puts the entries back (re-attaching the same hostnames to the same worker); the
next v5 deploy removes them again.

## Afterwards

Once both environments have applied, the `cloudflare_workers_custom_domains` data source and the `import` block in
`workers.tf` can be deleted, along with the script and the workflow steps that run it.

## Going back to v4

v4 can't read state v5 has written, so a rollback adopts the live worker and domains again under the v4 types. No state
backup is needed, and nothing live changes. Revert the module to provider v4 (`config.tf`, `workers.tf`,
`.terraform.lock.hcl`), then from `deployment/modules/cloudflare/tiles-worker`, with `TF_VAR_tiles_build_dir` pointing
at a build of the worker (`npm run build` in `tiles`), for each environment:

```sh
set -euo pipefail
export ENVIRONMENT=dev  # then prod
tg() { op run --env-file=../../../.env -- terragrunt run -- "$@"; }
tg init -reconfigure
account=$(op read op://tf/cloudflare/account_id)
# Each custom domain's hostname and ID.
tg state pull | jq -r '.resources[] | select(.type == "cloudflare_workers_custom_domain") | .instances[]
  | "\(.index_key) \(.attributes.id)"'
tg state rm cloudflare_workers_script.tiles cloudflare_workers_custom_domain.tiles
tg import cloudflare_workers_script.tiles "$account/tiles-$ENVIRONMENT"
tg import cloudflare_workers_domain.tiles "$account/<ID of tiles.$ENVIRONMENT.immich.cloud>"
# prod only:
tg import 'cloudflare_workers_domain.tiles_prod[0]' "$account/<ID of tiles.immich.cloud>"
```

The next v4 plan then updates the script in place and leaves the domains alone.
