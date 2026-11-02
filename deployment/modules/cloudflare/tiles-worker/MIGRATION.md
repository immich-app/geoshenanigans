# tiles-worker: Cloudflare provider v4 to v5

The module moved to provider v5 for Workers Cache (`cache_options`, `exports`). The live Worker and its custom domains
stay as they are; only Terraform state changes, and one step of that is done by hand.

## What happens to state

- `cloudflare_workers_script.tiles` keeps its address. v5's state upgrader rewrites the v4 entry on the first plan, and
  the plan is an in-place update of the script (new code, bindings list, cache settings).
- The custom domains move from `cloudflare_workers_domain.tiles` and `cloudflare_workers_domain.tiles_prod[0]` to
  `cloudflare_workers_custom_domain.tiles["<hostname>"]`. This can't be done in config: v5 has no
  `cloudflare_workers_domain` type, so it can't plan while those entries are in state ("no schema available"). Its move
  handler only accepts the singular `cloudflare_worker_domain`, and `tofu state mv` refuses to change resource types. So
  the two entries are removed from state by hand, and the `import` blocks in `workers.tf` adopt the live domains by ID.
  Nothing is detached or re-attached.

A local plan with state in the v4 shape and a stub API gives `2 to import, 0 to add, 1 to change, 0 to destroy`
for prod.

## Steps

Do this right before merging, for dev and prod, while no tiles deploy is running (the `tiles-apply-<env>` concurrency
group is idle). Run it from `deployment/modules/cloudflare/tiles-worker` with this branch checked out:

```sh
set -euo pipefail
export ENVIRONMENT=dev  # then prod
OP_VAULT=tf_prod        # a 1Password vault only maintainers can read
BACKUP="tiles-worker-$ENVIRONMENT state before provider v5"
tg() { op run --env-file=../../../.env -- terragrunt run -- "$@"; }

# The state holds the worker's secrets (Tigris key, metrics token), so it streams straight into 1Password and never
# touches disk. With pipefail, a failed pull or upload stops the script before any state rm. The pull needs
# --no-masking: op run otherwise replaces the injected secrets in the output, and the state holds the same values.
op run --no-masking --env-file=../../../.env -- terragrunt run -- state pull |
  op document create - --vault "$OP_VAULT" --title "$BACKUP" --file-name "tiles-worker-$ENVIRONMENT.tfstate"
# A masked backup would only show itself on rollback, so check it now.
if op document get "$BACKUP" --vault "$OP_VAULT" | grep -q 'concealed by 1Password'; then
  echo "the backup is masked; fix it before removing anything from state" >&2
  exit 1
fi

tg state rm 'cloudflare_workers_domain.tiles'
# prod only:
tg state rm 'cloudflare_workers_domain.tiles_prod[0]'
```

Then re-run the PR's Plan Terragrunt jobs. Each environment should import its domains (1 in dev, 2 in prod), update
`cloudflare_workers_script.tiles` in place, and add or destroy nothing. Merge, and main's deploy jobs apply prod and
then dev.

An apply of main's v4 config in between would create the old domain resources again (re-attaching the same hostnames
to the same worker) and put them back into state, and the v5 plan fails until `state rm` is repeated. Hold tiles
deploys until the merge.

## Afterwards

Once both environments have applied, the `cloudflare_workers_custom_domains` data source and the `import` block in
`workers.tf` can be deleted. To go back to v4, revert the module and stream the backup back, again without a plaintext
copy (v4 can't read state written by v5):

```sh
op document get "$BACKUP" --vault "$OP_VAULT" | tg state push -force -
```

Delete the 1Password document once both environments have applied v5 and you no longer need the way back.
