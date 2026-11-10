# tiles.immich.cloud on Bunny, one file per tile, updated daily: implementation plan (draft for review)

Follows the [Bunny Storage + Edge Script PoC](2026-11-10-bunny-storage-edge-script-poc.md) and the follow-up measurements of 2026-11-10 (section 8). Nothing here is built yet. Decisions marked **D1-D7** need the maintainer's answer before Phase 1. Recommendations are given for each.

## 1. What we are building

- **Every tile is its own file** in one Bunny Edge SSD storage zone:
  - main region Frankfurt, replicated to the 14 other SSD regions, S3 compatibility off;
  - paths `/v1/{z}/{x}/{y}.mvt`, holding the gzip bytes exactly as they are in the Protomaps build;
  - the TileJSON (`/v1.json`) and styles (`/v1/style/{name}.json`) are files in the same zone.
- **A pull zone on `tiles.immich.cloud`** uses the storage zone as its origin. Edge rules set the headers prod sends today:
  - `Content-Encoding: gzip` and `Content-Type` on tiles;
  - `Access-Control-Allow-Origin: *` and `Vary: Origin`;
  - prod's `Cache-Control` per route.
- **No script in the request path.** A CDN hit is served from the PoP. A miss is one read from the nearest storage replica.
- **Daily in-place updates:**
  - compare each new Protomaps daily build with the live one, position by position;
  - upload only the tiles that changed and delete the ones that became ocean;
  - wait until every replica has them;
  - only then switch caches over (D3).
- **Same URLs as today:** Immich keeps loading `https://tiles.immich.cloud/v1/style/{light,dark}.json`.

Why, from the measurements:

| | Prod today | Bunny PoC with Edge Script | This plan (storage origin) |
|---|---|---|---|
| Never-requested tile, weighted first byte | 334 ms | 124 ms | ~46 ms expected¹ |
| Cached tile | 32 ms | 10 ms | ~10 ms |
| Lookups per miss | D1 + Tigris range read | directory block + chunk range read | one file read |

¹ A Bunny storage-backed object fetched uncached from 120 probes on 2026-11-09 took 46 ms weighted. Perma-Cache copies took 6-20 ms median from Sydney, Singapore and New York. Spike S3 measures it on our own tiles.

## 2. Decisions for review

| # | Decision | Options | Recommendation |
|---|---|---|---|
| D1 | Ocean tiles | (a) upload nothing for the ocean tile: the storage zone's `Custom404FilePath` + `Rewrite404To200` return it for any missing file; (b) upload every position | **(a)**, if spike S2 passes. 637M files, 185 GB and $55.60/month, against 1.43B, 261 GB and $78.30. The first load is about half as long. |
| D2 | Scope | (a) every zoom as files; (b) zooms 0-14 as files and zoom 15 in chunks behind the Edge Script | **(a)**. With daily diffs the first load is a one-time cost. (b) keeps a script, a directory and chunk repacking for zoom 15. |
| D3 | Invalidation after a daily update | (a) bump `?v=` in the TileJSON daily: every tile URL changes, so CDN and browser caches start cold each day; (b) keep URLs stable and purge only the changed tiles (~200-330k a day) from the CDN; browsers revalidate daily (`max-age` 1 day with ETag); (c) like (a), but weekly | **(b)**, if spike S6 shows Bunny can purge ~330k URLs within an hour. Otherwise (a). |
| D4 | Analytics | see section 3 | **CDN logs into o11y**, plus a synthetic latency probe. A script on every request only if S7 shows it costs nothing on hits. |
| D5 | Account and keys | production Bunny account owned by FUTO/Immich | The account API key is used only by OpenTofu and the purge/log jobs (1Password `tf_prod`). Uploads use the storage zone's own password, which only works on that zone. |
| D6 | Cutover | test hostname first, then move the DNS record | Seed and verify under a test hostname on the same pull zone, try it in Immich, then CNAME `tiles.immich.cloud` to Bunny (DNS-only in Cloudflare). The Cloudflare Worker stays deployable for rollback for 4 weeks. |
| D7 | Dev environment | (a) its own zone with a single region (no replicas); (b) full replicas; (c) none | **(a)**: about $3.70/month. It exercises the whole pipeline without paying for 14 replicas. |

## 3. Analytics: what we keep and how

Today the Worker sends these to o11y on every request:
- `tiles_handle_request` by colo and continent;
- `tiles_response` by status;
- `tiles_cdn_hit` by zoom and style;
- `tiles_not_modified`;
- tile, D1 and Tigris durations;
- a sample of request logs with the site and network;
- a heartbeat every minute.

With no script in the path, none of this would run. What Bunny offers:

- **Code after the response, the question raised:** yes.
  - `Bunny.v1.waitUntil(promise)` keeps the isolate alive until the promise settles. Bunny's runtime docs list "firing off telemetry once the response has gone back to the client" as a use.
  - With **Run script before cache** on the pull zone (`EdgeScriptExecutionPhase = 2`), a *middleware* script's `onClientResponse` hook runs on every response, cache hits included.
  - **The CDN cache still does the caching.** Only a standalone script takes over caching in that mode, so with middleware we would not be "in control of the cache ourselves".
  - Cost: $0.20 per million requests plus $0.02 per 1,000 s of CPU, about **$8-10/month** at ~36M requests.
  - Unknowns for S7:
    - extra latency on hits, especially cold isolates (misses on our standalone script spent ~35-40 ms before the script ran, network included);
    - whether the hook can see HIT/MISS;
    - whether a script error fails open.
- **CDN logs, with no request-path cost:**
  - the Logging API v2 returns per-request JSON, kept 3 days, near real-time;
  - fields: time, status, cache status, bytes, country, edge location, ASN and organisation, path, user agent, referer;
  - IPs are anonymised by default.
  - Log Forwarding (syslog, 10-30 s delay, delivery not guaranteed) and Permanent Log Storage (gzip parts in a storage zone) also exist.
  - The logs carry **no timings**.

Proposed mapping, with the logs read every minute by a small cron job (the existing Worker's cron and o11y code are the natural home) and written to o11y as per-minute sums:

| Today | New source |
|---|---|
| Requests by type, zoom, style, status | log path and status |
| CDN hit/miss | log `cacheStatus` |
| Per colo / continent | log `edgeLocation`, `countryCode` |
| Sampled request logs (site, network) | log `referer`, `asnOrganization`, `userAgent` (sampled) |
| 304s | log status 304 |
| Heartbeat | the cron job |
| Tile/D1/Tigris durations | gone: those steps no longer exist |
| Request latency | a synthetic probe (a few fixed tiles, hit and forced miss, from several regions, every few minutes). Optionally miss timing from an `onOriginResponse` hook, which runs on misses only. |

## 4. Uploading everything (first full load)

**Tooling** (TypeScript next to `tiles/src/deploy`, with unit tests and a local end-to-end test against a fake storage server):

1. **Manifest:** for a build, the content hash of every tile position. It walks the PMTiles directories and hashes each distinct tile in parallel.
   - Our prototype took ~6 min for the walk and ~2 min for hashing on 48 cores.
   - Stored compressed in a private location, for the next day's diff.
2. **Uploader:**
   - about 1,500 PUTs in flight over keep-alive connections to all storage endpoint addresses, which gave 11.7k files/s with no 429s;
   - Bunny's `Checksum` header (SHA-256) on each PUT, so Bunny verifies every file;
   - retries with backoff on 429, 5xx and network errors;
   - a checkpoint per tile-id range, so a restart resumes where it stopped;
   - lowest zooms first.
3. **Verifier:**
   - per-region content checks on a random sample of the written files, through each region's storage endpoint;
   - directory listings (`ReplicatedZones`) for a sample of directories;
   - full listings by zoom for the first load.
4. **Switch:** upload the TileJSON and styles, then purge or bump the version per D3.

**First load**, from the dedicated server or the self-hosted runner. It is limited by API calls, not bandwidth, so it needs 3-6 MB/s at most:

| Zooms | Files (D1a) | Upload at ~11.5k/s | Sydney has them (~1.3-1.5k files/s) |
|---|---|---|---|
| 0-12 | 12.0M | 17 min | ~2.2-2.6 h |
| 13 | 31.5M | 46 min | +6-7 h |
| 14 | 120.8M | 2.9 h | +22-26 h |
| 15 | 472.4M | 11.4 h | +3.6-4.2 days |
| **All** | **636.7M** | **~15.4 h** | **~5-6 days** |

With D1b, the totals are 1.43B files, ~35 h of uploading and ~11-13 days for Sydney.

The other regions finish sooner:
- London, Stockholm, Madrid and Prague replicate at ~3.7k files/s;
- the US regions and Singapore at ~3k/s;
- Tokyo, Hong Kong, Johannesburg and São Paulo at ~2-2.5k/s.

The load runs in separate resumable jobs (zooms 0-13, zoom 14, zoom 15 in 8 tile-id ranges). The zone is not live until the verifier passes.

While Sydney catches up, new daily builds keep arriving. After the first load, the pipeline applies the diffs from the loaded build to the latest one (~1-2M files, minutes) before the zone goes live.

**Ask Bunny support** (maintainer):
- the ~11.5k files/s upload cap, with 429s past ~3,000 in flight;
- per-region replication throughput, especially Sydney (~1.3k files/s) and New York stragglers taking 27 minutes;
- whether a one-off bulk load can go faster;
- whether account sub-users can hold narrower keys for the log reader.

## 5. Daily update

Protomaps publishes daily builds around 09:05 UTC. A scheduled workflow, "Tiles Sync", runs at about 10:00 UTC on the self-hosted runner, plus a manual dispatch:

1. Pick the newest build (`builds.json`). Skip it if it is already live, or if its schema version differs (see section 6).
2. Download it and check the MD5 (~11 min today).
3. Build the manifest (~8-10 min).
4. Diff it against the live manifest (a few minutes): measured 327,826 positions in 1 day, 1.67M in 8 days and 1.90M in 9 days.
   - **Guard rail:** if more than 5% of positions changed, stop and alert, and treat it as a full rewrite.
5. Upload the changed tiles and delete the ones that became ocean: ~30 s to a few minutes.
6. Wait for replication and verify: ~5-25 min in measurements; give up and alert after a set limit.
7. Purge the changed URLs or bump `?v=` (D3), then upload the TileJSON and styles.
8. Store the new manifest. Send the run's numbers to o11y: files changed, durations, replication wait.

That is about 45-60 min of runner time a day. The live zone is never in a half-updated state as far as cached content goes: the switch happens after replication.

## 6. Full rewrites (schema or basemap upgrades)

- A Protomaps schema bump (e.g. 4.15 → 4.16) may change most tiles. That is unmeasured: every build we diffed was 4.15.2.
- Then write the new build under a new prefix (e.g. `/v1/e2/{z}/{x}/{y}.mvt`) as a second first load, while the old one keeps serving.
- Switch the TileJSON template once the new prefix is verified, and delete the old prefix after a week.
- Storage doubles for that window: ~$56-78 for a month, prorated.
- The TileJSON already tells clients the tile template, so nothing changes for Immich.

## 7. Phases

| Phase | Work | Exit criteria |
|---|---|---|
| 0. Spikes (PoC account, ~1-2 days) | S1-S8 below | Every spike answered; D1-D7 confirmed |
| 1. Tooling | manifest, diff, uploader, verifier, switch, with tests | unit + local end-to-end tests pass; Kritika approves |
| 2. Infrastructure | OpenTofu with `bunnyway/bunnynet` (v0.20: `storage_zone`, `pullzone`, `pullzone_edgerule`, `pullzone_hostname`) for dev and prod; secrets in 1Password | plans clean in CI |
| 3. First load | dev, then prod under the test hostname | verifier passes; 2,000+ random tiles decode identical to prod for the same build; 48-probe latency vs prod; hands-on in Immich |
| 4. Analytics | log reader → o11y, synthetic probe, updated dashboard and alerts (`o11y/dashboards/geoshenanigans-tiles.json`, `o11y/alerts/geoshenanigans-tiles.yaml`) | dashboard shows Bunny traffic matching the CDN's own statistics |
| 5. Daily sync | Tiles Sync on dev, then prod on the test hostname for a week | 7 clean daily runs |
| 6. Cutover | DNS for `tiles.immich.cloud` to Bunny | 48-probe and session checks after the switch; rollback = DNS back to the Worker |
| 7. Retire | after 4 stable weeks: D1, Tigris layout, Worker, Tiles Deploy/Cleanup | resources removed in OpenTofu |

**Spikes:**
- **S1, storage-origin pull zone:** tiles decode identical to prod, with prod's headers via edge rules. Bunny neither unpacks nor re-compresses the stored gzip. ETag and 304 work.
- **S2, ocean fallback via `Custom404FilePath` + `Rewrite404To200`:** the ocean tile comes back with tile headers, 200, and is cached.
- **S3, miss latency:** never-requested tiles on the 48 pinned probes, storage-origin zone vs the PoC script vs prod.
- **S4, large directories:** a zoom-15 column holds up to 32,768 files. Check PUT rate, listing time and replication at that size.
- **S5, regional checks:** each region's storage endpoint returns its own copy, and 404s before the copy arrives. The replication gate depends on this.
- **S6, purge:** per-URL purge rate and limits for ~330k URLs. This decides D3.
- **S7, analytics:**
  - Logging API v2 completeness and lag against requests we send;
  - a before-cache middleware with `onClientResponse` + `waitUntil`: hit-latency cost on the probes, whether cache status is visible, and fail-open behaviour.
- **S8, upload robustness:** a 1-hour run at ~1,500 in flight with checksums; 429 handling.

## 8. Cost (35.8M tile requests and 1.39 TB a month)

| Item | $/month |
|---|---|
| Storage, 15 regions ($0.30/GB-month): D1a 185 GB / D1b 261 GB | 55.60 / 78.30 |
| Delivery, $0.0141/GB blended | 19.60 |
| Uploads, API calls, replication, logs | 0 |
| Dev zone, one region (D7a) | ~3.70 |
| Optional before-cache analytics script | ~8-10 |
| **Total** | **~79 (D1a, logs) to ~112 (D1b + script)** |

That compares with prod at about $26-29 today and [#489](https://github.com/immich-app/geoshenanigans/pull/489) at about $36. Small files are billed at their real size; 3.4M 1 KiB test files added exactly 3.5 GB.

Measurements behind this plan (2026-11-10):
- Uploads plateau at ~11.5k files/s. 429s start past ~3,000 in flight, and the client machine was 16% busy.
- Replication is shared across zones: four zones were not faster than one. It runs from ~3.7k files/s in Europe down to ~1.3k/s in Sydney.
- The planet is already deduplicated by bytes. Only 0.03% of distinct tiles repeat at another offset.
- The ocean tile (95 B) covers 795M of 1.43B positions.
- Tiles built into an Edge Script slow every cold isolate by ~22 ms per MB of script.

## 9. Risks

- **A missing file shows as ocean, silently (D1a):** a failed upload or a replica that hasn't caught up draws ocean instead of an error. Mitigations:
  - the replication gate before every switch;
  - a daily check of random positions against the manifest in every region.
- **Replication throughput** sets the first-load time (days) and the worst case of a large daily diff. Raise it with Bunny.
- **Outage of Bunny itself:** on 2026-05-21 Bunny's own domain was suspended for 1.7 h, taking CDN, storage and scripts with it. Keep the Cloudflare path deployable and keep a written DNS rollback.
- **Header differences seen in the PoC:**
  - an extra `Vary: Accept-Encoding`;
  - HEAD gets 200 where prod gives 405;
  - clients that refuse gzip get gzip bytes. Browsers and MapLibre accept gzip.
- **China:** Bunny has no mainland PoPs. A test from 37 mainland probes (32 complete pairs) was faster than prod at the median, but most requests went to Los Angeles or Frankfurt. Routing will be raised with Bunny later.
