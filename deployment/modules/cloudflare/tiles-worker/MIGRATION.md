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
export ENVIRONMENT=dev  # then prod
# The backup holds the worker's secrets: keep it out of the repo.
op run --env-file=../../../.env -- terragrunt run -- state pull > ~/"tiles-worker-$ENVIRONMENT.tfstate.backup"
op run --env-file=../../../.env -- terragrunt run -- state rm 'cloudflare_workers_domain.tiles'
# prod only:
op run --env-file=../../../.env -- terragrunt run -- state rm 'cloudflare_workers_domain.tiles_prod[0]'
```

Then re-run the PR's Plan Terragrunt jobs. Each environment should import its domains (1 in dev, 2 in prod), update
`cloudflare_workers_script.tiles` in place, and add or destroy nothing. Merge, and main's deploy jobs apply prod and
then dev.

An apply of main's v4 config in between would create the old domain resources again (re-attaching the same hostnames
to the same worker) and put them back into state, and the v5 plan fails until `state rm` is repeated. Hold tiles
deploys until the merge.

## Afterwards

Once both environments have applied, the `cloudflare_workers_custom_domains` data source and the `import` block in
`workers.tf` can be deleted. To go back to v4, revert the module and push the backup with
`terragrunt run -- state push -force <backup>`: v4 can't read state written by v5.
