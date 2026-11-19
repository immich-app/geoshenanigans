# Bunny tiles zone

OpenTofu for tiles.immich.cloud on Bunny, where every tile position is its own file in an Edge Storage zone behind a pull zone:

| Resource | What it is |
| --- | --- |
| `bunnynet_storage_zone.tiles` | `immich-tiles`: Edge SSD, main region DE, replicated to the 14 other SSD regions. Holds `/v1/{z}/{x}/{y}.mvt` (the gzip bytes of the Protomaps build), `/v1.json` and the styles. Imported. |
| `bunnynet_storage_zone.state` | `immich-tiles-state`: Standard tier, DE, no replicas, no pull zone. The sync's `state.json` and manifests. Imported. |
| `bunnynet_pullzone.tiles` | `immich-tiles`, with the tiles zone as its origin. Request logs on (Logging API v2), query strings ignored, origin shield and Perma-Cache off. |
| `bunnynet_pullzone_edgerule.*` | The headers prod's Worker sends (below). |
| `bunnynet_compute_script.gzip` | The gzip middleware (`script/tiles-gzip.ts`), in every `gzip_mode`; linked to the pull zone unless `gzip_mode = "edge_rule"`. |
| `bunnynet_pullzone_hostname.custom` | `test_hostname`, and `public_hostname` once `serve_public_hostname` is true. |

The Bunny account is production only and there is no dev zone, so there is one environment, `prod` (`tiles.prod.tfvars.json`, state schema `prod_bunny_tiles`). This unit has its own workflow, `.github/workflows/tiles-bunny-infra.yml`: it plans on pull requests, once a reviewer approves the run (The workflow, below), and applies on `main`. The `terragrunt run --all` over `deployment/` that the tiles workflows and `mise run tf` do leaves it out (the `exclude` block in `terragrunt.hcl`), so run it from this directory:

```bash
cd deployment/modules/bunny/tiles
export ENVIRONMENT=prod
op run --env-file=bunny.env -- terragrunt plan
op run --env-file=bunny.env -- terragrunt apply
```

`bunny.env` holds this unit's 1Password references, apart from `deployment/.env`, which every pull request's plan resolves.

| Variable | 1Password | Notes |
| --- | --- | --- |
| `TF_VAR_bunny_api_key` | `op://tf_bunny/BUNNY_API_KEY/password` | The Bunny account API key (Account settings, API). The purge and the log reader use the same key. |
| `TF_VAR_tf_state_postgres_conn_str` | `op://tf/tf_state/postgres_conn_str` | The state backend, as in `deployment/.env`. |
| `TF_VAR_state_passphrase` | `op://tf_bunny/BUNNY_TILES_STATE_PASSPHRASE/password` | The state's encryption passphrase (State encryption, below). |

The sync, the watchdog and the worker read more items: every one is in the go-live checklist below, and they must exist before the branch merges.

## Who gets the Bunny account key

The account key can do anything on the Bunny account, so it sits in a vault of its own, `tf_bunny`, which neither `OP_TF_PROD_ENV` nor `OP_TF_DEV_ENV` reads, with the Globalping token and the storage zones' read-write passwords (which can overwrite or delete every tile, and the sync's state). Its service account, `OP_TF_BUNNY_ENV` below, reads `tf_bunny`, `tf_prod` and `tf` (the jobs that use it resolve those items too), and its token is a secret of two GitHub environments only, never of the repository. A job gets an environment's secrets only by naming that environment and passing its protection rules, so a branch whose workflow drops or renames the environment gets no token:

- `tiles-bunny-plan`: this unit's pull request plans. Required reviewers approve each run.
- `tiles-bunny`: deployment branches limited to `main`, so a job on any other branch that names it is refused. This unit's apply, the Tiles Worker workflow's apply on `main` and Tiles Deploy's prod activation (which put the key and the token on the worker, from `deployment/tiles-worker-bunny.env`, beside `.env`), and Tiles Sync's sync job (its purge, and its writes to both zones; a dry run writes nothing and uses neither).

Every other job that reads the zones only reads, with their read-only passwords, which are in `tf_prod`: Tiles Sync's resolve job, its decision check and its dry runs, and both of the watchdog's jobs. Bunny answers GET, listings and DESCRIBE (`Checksum` and `ReplicatedZones` included) with a zone's read-only password and refuses PUT and DELETE (401; checked on a test zone). `OP_TF_PROD_ENV` is a repository secret, which pull requests' Tiles Worker plans get, so those two passwords are within a pull request's reach: they read the public tiles and the sync's state, nothing more.

The state backend's connection string is in `tf`, which every pull request's plan resolves, so neither of the worker's two values is in a state, and the state that holds Bunny's other credentials is encrypted:

- **The worker's two secrets** are Worker secrets, not `secret_text_binding`s (OpenTofu keeps a binding's value in the state). After the upload, `terraform_data.bunny_secrets` in tiles-worker's `workers.tf` puts them through Cloudflare's API (`put-worker-secrets.sh`, with the provider's token), from variables, which the state doesn't keep. The provider (4.52.10) drops every secret it doesn't manage on each upload, and its refresh lists them without their values, so every prod plan of tiles-worker shows the script updating (their bindings going) and `terraform_data.bunny_secrets` replaced: each apply uploads, then puts both back within seconds, the current values from 1Password included. Pull requests' Tiles Worker plans pass stand-ins, which reach no resource. A prod plan or apply of tiles-worker that resolved `.env` alone stops at a precondition instead of turning the log reader and the probe off; locally, add `--env-file=deployment/tiles-worker-bunny.env` for prod, with `bash`, `curl` and `jq` installed.
- **This unit's state** holds the storage zones' passwords and the middleware script's deployment key: it is encrypted (State encryption, below), and tiles-worker no longer reads it (it takes the pull zone id as the variable `bunny_pull_zone_id`).

### What this doesn't cover

Keeping the worker's two values out of state doesn't keep them from pull requests' plans. Those plans run the branch's Terragrunt code with the shared backend credential, which also reads `prod_cloudflare_api_keys`: the Cloudflare account token that tiles-worker's provider and `put-worker-secrets.sh` use, which can deploy Workers. With it, code in a pull request can upload a version of `tiles-prod` whose handler reads the Worker's secrets (`BUNNY_API_KEY` and `GLOBALPING_TOKEN`, like `O11Y_TOKEN` and the others) and sends them elsewhere; with the Bunny account key it then has every zone's passwords too. The same token can already change what `tiles-prod` serves and anything else on the Cloudflare account, so this is the exposure every unit of `deployment/` has today; what this module adds is that the Bunny account key is reachable through it from the moment the Worker holds it (go-live step 3), and the Globalping token from step 7. Closing it needs infrastructure outside this module, one of:

- a state backend credential for pull requests' plans that can't read `prod_cloudflare_api_keys`, with a read-only Cloudflare token for those plans instead, so that no pull request gets a token that deploys Workers (every unit's plan, not only tiles-worker's); or
- the log reader and the probe (`tiles/src/bunny-logs`) moved off the Worker into a scheduled job in the `tiles-bunny` environment, so that the Worker holds neither value.

### State encryption

`encryption.tf` encrypts this unit's state and plans (AES-GCM, with a key derived from the passphrase `tf_bunny/BUNNY_TILES_STATE_PASSPHRASE`) and refuses to write either unencrypted. The backend holds ciphertext that the shared backend credential can read but not decrypt, and a state written without the passphrase is refused. That credential can still delete the state; the apply job's import check then stops instead of creating the zones again. `terragrunt state pull` and `state list` decrypt with the passphrase, as the workflow's steps do.

Losing the passphrase loses the state (the zones would have to be imported again), so keep the 1Password item. A state written before this encryption (an import, say) is refused too: migrate it once, locally, by changing the `state` block to

```hcl
    method "unencrypted" "migrate" {}

    state {
      method = method.aes_gcm.state
      fallback {
        method = method.unencrypted.migrate
      }
    }
```

running `op run --env-file=bunny.env -- terragrunt apply` (an apply with no changes rewrites the state, now encrypted), and restoring the block. A new passphrase goes the same way, with a second `pbkdf2` key provider and method for the old one as the fallback.

## The workflow

- **Pull requests and branch dispatches** run the checks (formatting, `tofu validate`, and `tofu test` against a mocked provider) with no credentials. Their plan runs the branch's Terragrunt configuration with the Bunny token, so it runs in the `tiles-bunny-plan` environment, which holds that token (`OP_TF_BUNNY_ENV`) as its own secret: GitHub releases it once one of the environment's required reviewers approves the run, which runs that one commit. Create the environment once in the repository's settings (Environments): required reviewers (the maintainers who review this module, with self-review prevented when more than one can approve), deployment branches and tags left at "No restriction" so that every branch's plan can run, and the secret `OP_TF_BUNNY_ENV`. GitHub creates an environment a job names without any protection, so until it has a reviewer the plan job stops at its first step, before anything reads a secret.
- **`main`** applies after the merge, in the `tiles-bunny` environment (deployment branches: `main` only), which holds its own copy of `OP_TF_BUNNY_ENV`. A run whose commit no longer has main's configuration (this directory, `deployment/root.hcl`, the tool versions in `.mise/` or the workflow, as on main now) skips the apply with a warning: that is a re-run of an older run, or one queued behind a newer commit, and putting its configuration back would undo the newer one (a hostname it attached, say). The newer commit's own run applies. Applies never overlap (concurrency group `tiles-bunny-apply`).

## First run: import the existing zones

Both storage zones already exist, so import them before the first apply; until then the workflow's apply job stops at its import check instead of trying to create them. Their ids are the numbers in the Bunny dashboard URL of each zone, or:

```bash
cd deployment/modules/bunny/tiles
export ENVIRONMENT=prod
op run --env-file=bunny.env -- sh -c 'curl -sS -H "AccessKey: $TF_VAR_bunny_api_key" https://api.bunny.net/storagezone' |
  jq '.[] | select(.Name == "immich-tiles" or .Name == "immich-tiles-state") | {Id, Name}'
```

Then:

```bash
op run --env-file=bunny.env -- terragrunt import bunnynet_storage_zone.tiles <immich-tiles zone id>
op run --env-file=bunny.env -- terragrunt import bunnynet_storage_zone.state <immich-tiles-state zone id>
op run --env-file=bunny.env -- terragrunt plan
```

The plan must show no change to either zone: a different name, region or tier would replace a zone, and both have `prevent_destroy`. It then creates the pull zone, its edge rules, the middleware script and the test hostname. Bunny can't remove a replication region once it's added, so `replication_regions` only grows.

## Headers

Edge rules set what prod's Worker sends (`tiles/src/index.ts`, `cachePolicy`), except a tile's `Content-Type` and `Content-Encoding` while the gzip middleware sets them (`gzip_mode`, below). URL patterns match the full URL without its query string.

| Files | Response headers | Kept in the CDN |
| --- | --- | --- |
| `*.mvt` | `Cache-Control: public, max-age=86400, stale-while-revalidate=2592000` (the Worker's policy for tile URLs without `?v=`), `Content-Type: application/x-protobuf`, `Content-Encoding: gzip` | a year: tile URLs are stable and the daily sync purges the whole zone once its uploads have replicated |
| `*.json`, `*/style/*` (TileJSON, and the styles as `v1/style/{name}.json` and `v1/style/{name}`) | `Cache-Control: public, max-age=300`, `Content-Type: application/json` (storage types files by extension, and `v1/style/{name}` has none) | 300 s |
| everything | `Access-Control-Allow-Origin: *`, `Vary: Origin` | |

The `Cache-Control`, `Content-Type` and `Content-Encoding` rules only apply to a file that exists: status 200, 206 or 304, and not 304 for `Content-Type` and `Content-Encoding`, as with the Worker. A missing file gets Bunny's 404 page with Bunny's `Cache-Control: no-cache`, so neither Bunny nor browsers keep it, as with the Worker's 404; with the tile rules on it Bunny kept a missing tile until the next purge, and in `edge_rule` mode it was labelled gzip and failed to decode. A file no rule matches gets `max-age=300` in browsers (`cache_expiration_time_browser`) instead of the CDN's year.

Differences from the Worker: tiles revalidate with `Last-Modified` instead of an `ETag`; there is no `PMTiles-Deployment-Key` header, because the live build changes daily and the sync never changes configuration; HEAD answers 200 instead of 405.

## Gzip: `gzip_mode`

Storage serves the stored gzip tiles without `Content-Encoding`. With an edge rule adding `Content-Encoding: gzip`, the PoPs serving India, the Gulf, Thailand and a few others (MU1, IN1, AE1, FU1, RI1, BHR1, TH1, CO1) send corrupt or empty bodies; a ticket with Bunny is open. Hence the modes:

- `middleware` (default): `script/tiles-gzip.ts` is linked to the pull zone as a middleware script and sets `Content-Encoding: gzip` and `Content-Type` on the storage response of every `.mvt`. That works at every PoP. It runs only on cache misses (`onOriginResponse`), costing ~49 ms on a miss served by a cold isolate and ~0-2 ms warm; hits never reach it. The pull zone then routes to scripting PoPs only (`routing.filters = ["scripting"]`), as Bunny requires for scripts.
- `edge_rule`: two edge rules set `Content-Type` and `Content-Encoding` on `*.mvt`; the script stays but is unlinked, and every PoP serves the zone. Switch to it only once Bunny has fixed the PoPs above and tiles decode from each of them.
- `both`: the middleware and the edge rules, only as the step between the two (below).

A Force Compression rule on `*.mvt` stays in every mode. With the middleware it makes most PoPs pass the stored gzip through to clients; a few (MU1, IN1, AE1, TH1, CO1) send it decompressed, which is correct but larger. Without it a tile cached through the middleware is sent decompressed, and once the `Content-Encoding` rule labels that gzip it is corrupt for every client: that would be every cached tile from the moment a PoP has `edge_rule` until the next purge. In `edge_rule` mode it changes nothing. (Measured on a test zone at UK1, like the other mode behaviour below.)

The script is uploaded as written, `npm:` import included. On a throwaway zone it served decodable tiles at every PoP probed (all of the PoPs above among them), with misses no slower than the bundled build the ~49 ms was measured with. The provider saves a new script's code but doesn't publish it, so `terraform_data.publish_gzip` publishes the first release through Bunny's API (`curl`, with the key in its environment) before the pull zone links the script; later code changes are published by the provider. The script needs no variables or secrets.

### Switching modes

Each step is a configuration change (below): a PoP keeps the old one for up to ~48 minutes, and it can get the parts of one apply (script link, routing, rules) at different times. Going straight from `middleware` to `edge_rule` leaves PoPs with the middleware unlinked but no `Content-Encoding` rule yet, which serve tiles as gzip with no `Content-Encoding`. So switch in two steps, each confirmed from probes at every PoP (cached and uncached tiles decode) before the next, at a quiet hour. There is no tool for that check in the repo yet: use Globalping HTTP measurements from enough locations to reach each PoP above (the `Server` header names it), and check that both tiles gunzip.

1. `middleware` → `both`. In `both`, a tile cached through the middleware breaks for a client that sends no `Accept-Encoding: gzip` (measured; browsers and MapLibre always send it), so don't stay there longer than the PoPs need.
2. `both` → `edge_rule`, then one full purge once every PoP has it, so tiles cached through the middleware are fetched again.

Back to the middleware: `edge_rule` → `both`, a full purge once every PoP has it (a tile cached without the middleware would go out as gzip with no `Content-Encoding` once the rules are gone), then `both` → `middleware`.

In `edge_rule` and `both`, a `Range` request without `Accept-Encoding: gzip` fails (Bunny resets the stream); with it the range comes from the gzip bytes, labelled gzip.

### The script's deployment key

The provider doesn't mark the script's `deployment_key` sensitive, and anyone with it and the script id can publish code that runs on every tile miss. A plan that deletes or replaces the script prints it (`prevent_destroy` doesn't help: OpenTofu prints the plan before the error). So no mode deletes the script, and the workflow masks the key, read from the state, before it plans or applies. Changing the script's `type` or removing its block replaces or deletes it, which is fine through this workflow (masked) or locally, not from any other public CI. In-place changes (its code, its name) don't print it.

## Hostnames and the cutover

- `test_hostname` (`tiles-bunny.immich.cloud`) serves the zone before the DNS cutover. Point a DNS-only (not proxied) CNAME at the `cdn_domain` output, then add the name to `tls_hostnames`: Bunny obtains its certificate over HTTP, so the apply that lists it fails until the record resolves to Bunny.
- `public_hostname` (`tiles.immich.cloud`) is attached once `serve_public_hostname` is true. It can only get a managed certificate after its DNS points at Bunny, so with a managed certificate HTTPS fails from the DNS switch until the certificate is issued. Avoid that gap with a certificate obtained beforehand (DNS challenge) passed as the hostname's `certificate` and `certificate_key`, or accept it at a quiet hour. The provider doesn't mark `certificate_key` sensitive either: pass it through a `sensitive = true` variable (from 1Password, like the API key) so plans show `(sensitive value)`. Rolling back is pointing the DNS back at the Worker (Rolling back to the Worker, below).
- Configuration changes (pull zone, edge rules, hostnames, the script link) can take ~48 minutes to reach every PoP; a new zone answered "Domain suspended or not configured" at one PoP for 20-30 minutes. Make them well ahead of anything that depends on them, and confirm from probes at every PoP. The daily sync changes no configuration: it uploads, purges the pull zone (output `pull_zone_id`) and writes state.

## Go-live checklist

Work through it in order; the first item comes before the branch merges.

1. **Create the vault, its service account, the environments and the 1Password items** (Who gets the Bunny account key, above), and check that each item resolves (`op read <ref> > /dev/null`) with each service account that reads its vault. Without them the next scheduled Tiles Sync fails at `op run` and pages (geoshenanigans-tiles-sync-failed), the watchdog sends no heartbeat (geoshenanigans-tiles-watchdog-silent), and the Tiles Worker workflow's apply on `main` and Tiles Deploy's prod activation fail, as prod's tiles-worker needs the first two items.
   - The vault `tf_bunny`, and a 1Password service account that reads `tf_bunny`, `tf_prod` and `tf`, and nothing else is given access to `tf_bunny`.
   - The environments `tiles-bunny-plan` (required reviewers, The workflow above) and `tiles-bunny` (deployment branches and tags: selected, the branch `main` only), each with the secret `OP_TF_BUNNY_ENV`, that service account's token. No repository secret holds it.

   | Vault | Item (field `password`) | Value | Read by |
   | --- | --- | --- | --- |
   | `tf_bunny` | `BUNNY_API_KEY` | the Bunny account API key | this unit (`bunny.env`), the sync's purge, the worker's log reader (`deployment/tiles-worker-bunny.env`) |
   | `tf_bunny` | `GLOBALPING_TOKEN` | a Globalping API token | the worker's synthetic probe (`deployment/tiles-worker-bunny.env`) |
   | `tf_bunny` | `BUNNY_TILES_STATE_PASSPHRASE` | a random passphrase of at least 16 characters (`op item create --vault tf_bunny --category password --title BUNNY_TILES_STATE_PASSPHRASE --generate-password=64,letters,digits`) | this unit's state encryption (`bunny.env`, State encryption above) |
   | `tf_bunny` | `BUNNY_STORAGE_PASSWORD` | `immich-tiles`' password (read-write) | Tiles Sync's sync job (not a dry run) |
   | `tf_bunny` | `BUNNY_STATE_PASSWORD` | `immich-tiles-state`'s password (read-write) | Tiles Sync's sync job (not a dry run) |
   | `tf_prod` | `BUNNY_STORAGE_READONLY_PASSWORD` | `immich-tiles`' read-only password | the watchdog's sample check |
   | `tf_prod` | `BUNNY_STATE_READONLY_PASSWORD` | `immich-tiles-state`'s read-only password | Tiles Sync's resolve job, decision check and dry runs, the watchdog |
   | `tf_prod` | `BUNNY_PULL_ZONE_ID` | the `pull_zone_id` output (step 3) | Tiles Sync's purge |

   Each zone's password and read-only password are on its page in the Bunny dashboard (FTP & API Access). Keep the read-write ones out of `tf_prod`: every pull request's Tiles Worker plan resolves `tf_prod` (Who gets the Bunny account key, above). `BUNNY_PULL_ZONE_ID` is copied by hand rather than read from this unit's state: the sync job writes production and doesn't run OpenTofu or read its state backend, and only this unit's jobs can decrypt the state. `O11Y_VICTORIAMETRICS_VMAUTH_PASSWORD` (`tf_prod`) and `tf_state` (`tf`) exist already.
2. **Apply this unit**: import the zones (First run, above), plan, apply. That creates the pull zone, its edge rules, the middleware script and the test hostname.
3. **Copy `pull_zone_id`** into `tf_prod/BUNNY_PULL_ZONE_ID` (the sync's purge) and, as `"bunny_pull_zone_id": "<id>"`, into `deployment/modules/cloudflare/tiles-worker/tiles.prod.tfvars.json` (a pull request). Its Tiles Worker run on `main` starts the log reader. Until then the worker applies without it, with the log reader off.
   - First, a maintainer closes the exposure in What this doesn't cover (above), or accepts it on the record (in that pull request): from this apply on, the Bunny account key is a secret of `tiles-prod`, which anyone holding the Cloudflare account token can read, pull requests' plans included. Step 7 does the same for the Globalping token.
4. **Test hostname**: a DNS-only CNAME `tiles-bunny.immich.cloud` → the `cdn_domain` output, then add the name to `tls_hostnames` and apply again (Hostnames and the cutover, above).
5. **Hold scheduled syncs**: set the repository variable `TILES_SYNC_HOLD` (any text, e.g. "first load") before anything is seeded.
6. **Load and verify the tiles, then adopt them** (Loading the tiles, below).
7. **Turn the probe on**: set `"bunny_public_url": "https://tiles-bunny.immich.cloud"` in `deployment/modules/cloudflare/tiles-worker/tiles.prod.tfvars.json` and run the Tiles Worker workflow on `main`. Not before the hostname has its certificate and the seed covers zoom 15: the probe requests random z15 tiles, and geoshenanigans-tiles-bunny-probe-errors pages on 404s. If the zone serves requests while the probe is still off, geoshenanigans-tiles-bunny-probe-off warns.
8. **Let replication drain**, then clear `TILES_SYNC_HOLD` (Loading the tiles, step 5).
9. **A week of daily syncs** on the test hostname (plan phase 5), then the cutover.

### The cutover

One change, made at a quiet hour, after `serve_public_hostname` has been applied ~48 minutes ahead (Hostnames and the cutover, above):

- DNS: `tiles.immich.cloud` is today the Worker's custom domain (`cloudflare_workers_domain.tiles_prod` in `deployment/modules/cloudflare/tiles-worker/workers.tf`, a proxied record Cloudflare manages). It becomes a DNS-only CNAME at `cdn_domain` with a short TTL (60 s), so a rollback spreads fast.
- `BUNNY_PUBLIC_URL` in `.github/workflows/tiles-sync.yml` and `bunny_public_url` in tiles-worker's `tiles.prod.tfvars.json` become `https://tiles.immich.cloud`. The next sync that uploads a build rewrites `v1.json` and the styles and purges (a run of the live build alone is skipped); until then they point tiles at the test hostname, which keeps serving.
- The Worker's own alerts (geoshenanigans-tiles-silent, -5xx and -slow) watch the Worker's traffic, which stops: remove them or limit them to `tiles.prod.immich.cloud` in the same change, or geoshenanigans-tiles-silent pages within 20 minutes.

### Rolling back to the Worker

For a Bunny outage (the one on 2026-05-21 took CDN, storage and scripts down for 1.7 hours):

1. Check the Worker still serves current tiles: `curl -s https://tiles.prod.immich.cloud/v1.json` answers, and `pmtiles_deployment_key` in tiles-worker's `tiles.prod.tfvars.json` is recent. Tiles Deploy only runs when dispatched, so after weeks on Bunny that build is weeks old: dispatch Tiles Deploy for prod first when there is time, or accept older tiles for the outage. Tiles Deploy checks the Worker on its own hostname, `tiles.prod.immich.cloud` (`deployment/scripts/tiles-served-key.sh`), so it works while `tiles.immich.cloud` still points at Bunny, which sends no deployment key.
2. Delete the CNAME and put the Worker's custom domain back: revert the cutover change and apply tiles-worker, or, faster, in the Cloudflare dashboard (delete the `tiles.immich.cloud` record, then add it as a custom domain of the `tiles-prod` Worker). Clients follow within the CNAME's TTL.
3. Tiles Sync can keep running: it only writes the Bunny zones, which keeps them current for going back. During the outage itself every run fails; set `TILES_SYNC_HOLD` to keep scheduled runs quiet, and clear it after.
4. Restore the Worker's alerts if the cutover removed them. Cutting over again is the cutover above.

## Loading the tiles

The zone's files come from the tiles tooling (`tiles/src/bunny`, run from `tiles/`), not from this module. The commands need `BUNNY_STORAGE_ZONE`, `BUNNY_STORAGE_PASSWORD`, `BUNNY_STATE_ZONE` and `BUNNY_STATE_PASSWORD` (the read-write passwords, in `tf_bunny`; a command that only reads, such as `bunny:status` or `bunny:replication`, works with the `tf_prod` read-only ones); `bunny:sync` also the API key, pull zone id and public URL (its header lists them), and, unless `--dry-run`, the map-styles build, which it uploads beside the TileJSON. Build that once, from the repository root, before the first adopt (Tiles Sync builds its own):

```bash
(cd map-styles && npm ci && npm run build)
export STYLES_DIR="$PWD/map-styles/dist"  # absolute: the commands run from tiles/
cd tiles
```

1. **Seed** every position with `npm run bunny:seed -- --archive <build>.pmtiles --state-dir <dir>`. Run it as separate resumable jobs (plan section 4), each with its own `--state-dir`, so a failure repeats one range: zooms 0-13 (`--min-zoom 0 --max-zoom 13`), zoom 14 in two (`--from-id 89478485 --to-id 223696213`, `--from-id 223696213 --to-id 357913941`) and zoom 15 in eight, from `--from-id 357913941` in steps of 134,217,728 (492131669, 626349397, 760567125, 894784853, 1029002581, 1163220309, 1297438037, up to `--to-id 1431655765`). One job over everything works too. Each job must end `Seed complete` (exit 0); run a job's command again until it does.
2. **Verify** before adopting: `npm run bunny:replication -- --listing --prefix v1` lists every column of every zoom (4 listings at once, about 4 hours for the planet) and exits 1 if any position has no file; `npm run bunny:replication -- --archive <build>.pmtiles --sample 1000 --per-zoom` checks bytes. Both report each region's coverage.
3. **Adopt**: with `STYLES_DIR` set to the map-styles build (above), `npm run bunny:sync -- --archive <same build>.pmtiles --build <YYYYMMDD> --adopt --seed-state <dir> [--seed-state <dir> ...]`, one `--seed-state` per job. It refuses unless every state dir says its seed completed, for this archive, zone and prefix, and together they cover every position; then it checks a sample of positions, uploads the TileJSON and styles, waits until every region holds them (the replication gate; `--regions` limits it), purges the pull zone and writes `state.json` (with the archive's identity). It writes no tiles. The seed's backlog still fills the account's replication queue, so that gate would wait it out: on the first load, before the domain points at the zone, add `--regions none`, which only checks that storage holds them. "This archive" is the archive's identity: a SHA-256 hash of the whole file (version 2, `sha256-tree-v2:...` in `seed.json`), which bunny:seed records when it starts and bunny:sync computes again (about 15 s for a planet build read from NVMe; both hash on up to 16 threads).
   - A seed started by an older bunny:seed (its `seed.json` has `archive.fingerprint` and no `archive.identity`) recorded only a version 1 fingerprint: the archive's header, root directory and size, which an archive with other tile bytes can share. Adopt only accepts such a state dir with `--archive-md5 <md5sum>`, the build's `md5sum` from `https://build-metadata.protomaps.dev/builds.json` (base64, e.g. `InTVk6k9bWf+XFvDHsyQwQ==`): the fingerprint must match the one computed from the archive given, and the archive's MD5, computed over the whole file (4 to 8 minutes for a planet build: MD5 runs on one core), must equal it. Without the flag, or with another MD5, it refuses. Take the MD5 from builds.json, not from the downloaded file. The MD5 says nothing about the bytes the seed read, so `--archive` must also be the file the seed read, at the path in its `status.json` (`archive`, an absolute path), and that file must not have changed since the seed began: its ctime (and a symlink's at that path) no later than `seed.json`'s mtime and the last run's `startedAt`. A download still in progress when the seed started, or a file replaced, rewritten, moved or even chmodded since, is refused: seed again with this archive, a new `--state-dir` and `--verify-existing` (it writes only the files that differ) and adopt that state dir. A resume of such a seed must use the bunny:seed that started it: a newer one refuses its checkpoint.
4. **Catch up**: while scheduled syncs are held, dispatch Tiles Sync (newest build) to apply the days since the seeded build. The account's replication queue still holds the seed's backlog, so its gate cannot pass in time: dispatch with the `regions` input set to `none`, or to the regions that keep up.
5. **Wait for replication to drain**, then clear `TILES_SYNC_HOLD`. Replication runs from the first PUT at ~3.7k files/s in Europe, ~3k in the US and Singapore, ~2-2.5k in Tokyo, Hong Kong, Johannesburg and São Paulo, and ~1.3-1.5k in Sydney: for 1.43B files about 4.5 days for Europe and 11-13 days for Sydney. It has drained when `npm run bunny:replication -- --listing --min-zoom 15 --max-zoom 15` shows 100% in every region and exits 0, and the watchdog's daily sample check reports no region lacking (geoshenanigans-tiles-sample-unreplicated resolves). To sync before then, set the repository variable `TILES_SYNC_REGIONS` to the regions that have drained (e.g. `UK,SE,ES,CZ`) and remove it afterwards: a region the gate skips serves the day's changed tiles from Frankfurt until it catches up.

The zone is not live (no DNS cutover) until steps 2 and 5 pass.

## Repairs

- **Reverting a failed or cancelled sync**: dispatch Tiles Sync with the live build as `build`. A sync that failed after its first PUT leaves the build pending, and the zone serves its bytes on a cache miss wherever it changed a tile; syncing the live build again puts the live bytes back there (DESCRIBEing each first), gates, purges and clears `pending`. `npm run bunny:status` lists pending builds.
- **Missing or wrong files** (geoshenanigans-tiles-sample-wrong, or a listing): `npm run bunny:seed -- --archive <live build>.pmtiles --prefix <live prefix> --min-zoom Z --max-zoom Z --verify-existing --state-dir <new dir>` DESCRIBEs every file of those zooms and writes only the missing or wrong ones. `npm run bunny:status` prints the live prefix: `--prefix` defaults to `v1`, which a full rewrite leaves behind, and a state dir an earlier seed used skips the chunks it completed. After the first adopt, a seed over the live prefix refuses any archive but the live build's (`state.json` records its identity, the whole file's hash): an older build's bytes would undo daily syncs, and no later diff repairs a position that doesn't change again. A `state.json` that records only a version 1 fingerprint counts as recording none, so such a seed refuses until a sync makes a build live again. With builds pending, revert first (above).
- **A file that never replicates**: the same paths show up run after run in the sync's log line ("still not replicated: v1/15/x/y.mvt (not in SYD, ...)") while everything else replicates, and a DESCRIBE of one shows the right `Checksum` with regions missing from `ReplicatedZones`. Bunny drops a file's replica list for good when the same bytes are PUT again; the sync DESCRIBEs before every PUT, but a PUT that timed out after Bunny stored it, followed by a DESCRIBE answered before Bunny committed it, can still lead to a second identical PUT. Each such file blocks every later sync (the build stays pending and the gate waits for it again). Let syncs through by dispatching Tiles Sync with the `regions` input listing every region but those the file lacks (or set `TILES_SYNC_REGIONS`), and raise the path with Bunny. Deleting the file and uploading it again may make Bunny replicate it; that is untested, so try it first on a throwaway file in another zone (PUT it twice with the same bytes, then DELETE and PUT).

## Full rewrites

A Protomaps schema change (Tiles Sync stops with `schema_changed`) or a diff over the forced guard rail (2%) goes in under a new tile prefix while the old one keeps serving (plan section 6). Immich keeps loading `/v1/style/{name}.json` and `/v1.json`: those stay where they are, and only the TileJSON's tile template moves.

1. Pick a prefix beside the live one, not inside its zoom folders: live `v1`, new `v1/e2` (tiles at `/v1/e2/{z}/{x}/{y}.mvt`), then `v1/e3`.
2. Seed the new build under it (`--prefix v1/e2`), as separate jobs like the first load. Scheduled syncs keep stopping at the schema check meanwhile, so the live build ages and geoshenanigans-tiles-sync-stale and -osm-old fire; that is expected.
3. Wait until the new prefix has replicated: `npm run bunny:replication -- --listing --prefix v1/e2` exits 0.
4. Switch: with `STYLES_DIR` set to a fresh map-styles build (Loading the tiles, above), `npm run bunny:sync -- --archive <build>.pmtiles --build <YYYYMMDD> --adopt --replace --prefix v1/e2 --seed-state <dir> [...]`. Besides the adopt checks, the sampled positions must be in every region. It writes `v1.json` with the new tile template, waits until every region has it, purges, and makes the build live under `v1/e2`. Daily syncs follow the live prefix from then on; dispatch one to catch up.
5. A week later, delete the old tiles: `npm run bunny:delete-prefix -- --prefix v1` says what it would delete, then the same with `--yes` deletes the `v1/<z>/` folders (never `v1.json`, `v1/style/` or `v1/e2/`). It refuses the live prefix and any prefix holding it, and likewise the prefix the TileJSON (`v1.json`, `--static-prefix`) points tiles at, and checks both again before each delete. Storage is double until then.

## Outputs

`pull_zone_id` (`BUNNY_PULL_ZONE_ID` for the purge, and tiles-worker's `bunny_pull_zone_id` for the log reader), `cdn_domain` (the zone's own `{name}.b-cdn.net` hostname, the CNAME target), `hostnames`, `storage_zone_id`, `state_zone_id`, `gzip_script_id`. The storage zone passwords stay out of the outputs (the sync and the watchdog read them from 1Password: the read-write ones from `tf_bunny`), and the state that holds them is encrypted.
