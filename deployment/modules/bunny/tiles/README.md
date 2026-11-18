# Bunny tiles zone

OpenTofu for tiles.immich.cloud on Bunny, where every tile position is its own file in an Edge Storage zone behind a pull zone:

| Resource | What it is |
| --- | --- |
| `bunnynet_storage_zone.tiles` | `immich-tiles`: Edge SSD, main region DE, replicated to the 14 other SSD regions. Holds `/v1/{z}/{x}/{y}.mvt` (the gzip bytes of the Protomaps build), `/v1.json` and the styles. Imported. |
| `bunnynet_storage_zone.state` | `immich-tiles-state`: Standard tier, DE, no replicas, no pull zone. The sync's `state.json` and manifests. Imported. |
| `bunnynet_pullzone.tiles` | `immich-tiles`, with the tiles zone as its origin. Request logs on (Logging API v2), query strings ignored, origin shield and Perma-Cache off. |
| `bunnynet_pullzone_edgerule.*` | The headers prod's Worker sends (below). |
| `bunnynet_compute_script.gzip` | The gzip middleware (`script/tiles-gzip.ts`), while `gzip_mode = "middleware"`. |
| `bunnynet_pullzone_hostname.custom` | `test_hostname`, and `public_hostname` once `serve_public_hostname` is true. |

The Bunny account is production only and there is no dev zone, so there is one environment, `prod` (`tiles.prod.tfvars.json`, state schema `prod_bunny_tiles`). This unit has its own workflow, `.github/workflows/tiles-bunny-infra.yml`: it plans on pull requests and applies on `main`. The `terragrunt run --all` over `deployment/` that the tiles workflows and `mise run tf` do leaves it out (the `exclude` block in `terragrunt.hcl`), so run it from this directory:

```bash
cd deployment/modules/bunny/tiles
export ENVIRONMENT=prod
op run --env-file=bunny.env -- terragrunt plan
op run --env-file=bunny.env -- terragrunt apply
```

`bunny.env` holds this unit's 1Password references, apart from `deployment/.env`: its key lives in `tf_prod` only, and the tiles workflows also resolve `.env` for dev.

| Variable | 1Password | Notes |
| --- | --- | --- |
| `TF_VAR_bunny_api_key` | `op://tf_prod/BUNNY_API_KEY/password` | The Bunny account API key (Account settings, API). The purge and the log reader use the same key. |
| `TF_VAR_tf_state_postgres_conn_str` | `op://tf/tf_state/postgres_conn_str` | The state backend, as in `deployment/.env`. |

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

Edge rules set what prod's Worker sends (`tiles/src/index.ts`, `cachePolicy`), except a tile's `Content-Type` and `Content-Encoding`, which come from the gzip middleware while `gzip_mode = "middleware"`. URL patterns match the full URL without its query string.

| Files | Response headers | Kept in the CDN |
| --- | --- | --- |
| `*.mvt` | `Cache-Control: public, max-age=86400, stale-while-revalidate=2592000` (the Worker's policy for tile URLs without `?v=`), `Content-Type: application/x-protobuf`, `Content-Encoding: gzip` | a year: tile URLs are stable and the daily sync purges the whole zone once its uploads have replicated |
| `*.json` (TileJSON, styles) | `Cache-Control: public, max-age=300` | 300 s |
| everything | `Access-Control-Allow-Origin: *`, `Vary: Origin` | |

Differences from the Worker: tiles revalidate with `Last-Modified` instead of an `ETag`; there is no `PMTiles-Deployment-Key` header, because the live build changes daily and the sync never changes configuration; HEAD answers 200 instead of 405; the `*.mvt` `Cache-Control` also goes on a 404, which Bunny doesn't cache (`cache_errors = false`) but a browser may keep for a day.

## Gzip: `gzip_mode`

Storage serves the stored gzip tiles without `Content-Encoding`. With an edge rule adding `Content-Encoding: gzip`, the PoPs serving India, the Gulf, Thailand and a few others (MU1, IN1, AE1, FU1, RI1, BHR1, TH1, CO1) send corrupt or empty bodies; a ticket with Bunny is open. Hence two modes:

- `middleware` (default): `script/tiles-gzip.ts` is linked to the pull zone as a middleware script and sets `Content-Encoding: gzip` and `Content-Type` on the storage response of every `.mvt`. That works at every PoP. It runs only on cache misses (`onOriginResponse`), costing ~49 ms on a miss served by a cold isolate and ~0-2 ms warm; hits never reach it. A Force Compression rule on `*.mvt` makes most PoPs pass the stored gzip through to clients; a few (MU1, IN1, AE1, TH1, CO1) send it decompressed, which is correct but larger. The pull zone then routes to scripting PoPs only (`routing.filters = ["scripting"]`), as Bunny requires for scripts.
- `edge_rule`: two edge rules set `Content-Type` and `Content-Encoding` on `*.mvt`, no script, every PoP. Switch to it only once Bunny has fixed the PoPs above and tiles decode from each of them.

The provider saves a new script's code but doesn't publish it, so `terraform_data.publish_gzip` publishes the first release through Bunny's API (`curl`, with the key in its environment) before the pull zone links the script; later code changes are published by the provider. The script needs no variables or secrets.

Switching modes is a configuration change like any other (below), made in several API calls: switch at a quiet hour and check tiles from every PoP afterwards.

## Hostnames and the cutover

- `test_hostname` (`tiles-bunny.immich.cloud`) serves the zone before the DNS cutover. Point a DNS-only (not proxied) CNAME at the `cdn_domain` output, then add the name to `tls_hostnames`: Bunny obtains its certificate over HTTP, so the apply that lists it fails until the record resolves to Bunny.
- `public_hostname` (`tiles.immich.cloud`) is attached once `serve_public_hostname` is true. It can only get a managed certificate after its DNS points at Bunny, so with a managed certificate HTTPS fails from the DNS switch until the certificate is issued. Avoid that gap with a certificate obtained beforehand (DNS challenge) passed as the hostname's `certificate`, or accept it at a quiet hour. Rolling back is pointing the DNS back at the Worker.
- Configuration changes (pull zone, edge rules, hostnames, the script link) can take ~48 minutes to reach every PoP; a new zone answered "Domain suspended or not configured" at one PoP for 20-30 minutes. Make them well ahead of anything that depends on them, and confirm from probes at every PoP. The daily sync changes no configuration: it uploads, purges the pull zone (output `pull_zone_id`) and writes state.

## Outputs

`pull_zone_id` (`BUNNY_PULL_ZONE_ID` for the purge and the log reader), `cdn_domain`, `hostnames`, `storage_zone_id`, `state_zone_id`, `gzip_script_id`. The storage zone passwords stay out of the outputs: the sync reads them from 1Password.
