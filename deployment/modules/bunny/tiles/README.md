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

Each step is a configuration change (below): a PoP keeps the old one for up to ~48 minutes, and it can get the parts of one apply (script link, routing, rules) at different times. Going straight from `middleware` to `edge_rule` leaves PoPs with the middleware unlinked but no `Content-Encoding` rule yet, which serve tiles as gzip with no `Content-Encoding`. So switch in two steps, each confirmed from probes at every PoP (cached and uncached tiles decode) before the next, at a quiet hour:

1. `middleware` → `both`. In `both`, a tile cached through the middleware breaks for a client that sends no `Accept-Encoding: gzip` (measured; browsers and MapLibre always send it), so don't stay there longer than the PoPs need.
2. `both` → `edge_rule`, then one full purge once every PoP has it, so tiles cached through the middleware are fetched again.

Back to the middleware: `edge_rule` → `both`, a full purge once every PoP has it (a tile cached without the middleware would go out as gzip with no `Content-Encoding` once the rules are gone), then `both` → `middleware`.

In `edge_rule` and `both`, a `Range` request without `Accept-Encoding: gzip` fails (Bunny resets the stream); with it the range comes from the gzip bytes, labelled gzip.

### The script's deployment key

The provider doesn't mark the script's `deployment_key` sensitive, and anyone with it and the script id can publish code that runs on every tile miss. A plan that deletes or replaces the script prints it (`prevent_destroy` doesn't help: OpenTofu prints the plan before the error). So no mode deletes the script, and the workflow masks the key, read from the state, before it plans or applies. Changing the script's `type` or removing its block replaces or deletes it, which is fine through this workflow (masked) or locally, not from any other public CI. In-place changes (its code, its name) don't print it.

## Hostnames and the cutover

- `test_hostname` (`tiles-bunny.immich.cloud`) serves the zone before the DNS cutover. Point a DNS-only (not proxied) CNAME at the `cdn_domain` output, then add the name to `tls_hostnames`: Bunny obtains its certificate over HTTP, so the apply that lists it fails until the record resolves to Bunny.
- `public_hostname` (`tiles.immich.cloud`) is attached once `serve_public_hostname` is true. It can only get a managed certificate after its DNS points at Bunny, so with a managed certificate HTTPS fails from the DNS switch until the certificate is issued. Avoid that gap with a certificate obtained beforehand (DNS challenge) passed as the hostname's `certificate` and `certificate_key`, or accept it at a quiet hour. The provider doesn't mark `certificate_key` sensitive either: pass it through a `sensitive = true` variable (from 1Password, like the API key) so plans show `(sensitive value)`. Rolling back is pointing the DNS back at the Worker.
- Configuration changes (pull zone, edge rules, hostnames, the script link) can take ~48 minutes to reach every PoP; a new zone answered "Domain suspended or not configured" at one PoP for 20-30 minutes. Make them well ahead of anything that depends on them, and confirm from probes at every PoP. The daily sync changes no configuration: it uploads, purges the pull zone (output `pull_zone_id`) and writes state.

## Loading the tiles

The zone's files come from the tiles tooling (`tiles/src/bunny`, run from `tiles/`), not from this module:

1. `npm run bunny:seed -- --archive <build>.pmtiles` uploads every position of a build. It is resumable: run the same command again after a stop.
2. `npm run bunny:sync -- --archive <same build>.pmtiles --build <YYYYMMDD> --adopt` records it as the live build: it checks a sample of positions, uploads the TileJSON and styles, purges the pull zone and writes `state.json`. Nothing writes tiles over a seed: a later sync DESCRIBEs every file before its PUT.
3. From then on Tiles Sync (`.github/workflows/tiles-sync.yml`) applies each daily build's changed positions. While the seed's files are still replicating, the account-wide replication queue holds the daily changes up past the sync's replication gate: set the `TILES_SYNC_HOLD` repository variable until it drains.

## Outputs

`pull_zone_id` (`BUNNY_PULL_ZONE_ID` for the purge and the log reader), `cdn_domain` (the zone's own `{name}.b-cdn.net` hostname, the CNAME target), `hostnames`, `storage_zone_id`, `state_zone_id`, `gzip_script_id`. The storage zone passwords stay out of the outputs: the sync reads them from 1Password.
