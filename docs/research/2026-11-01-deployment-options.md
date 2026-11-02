# tiles.immich.cloud: deployment options (research, 2026-11-01; v2.3, 2026-11-02)

Synthesis of 8 research tracks (cf-baseline, cf-optimize, vm-providers, cdn-layer, tile-formats, latency-measure, multi-region-ops, hybrid), each checked by an adversarial verifier. **v2** applies a 35-item critic review, five fix-up tracks (cf-pricing, host-pricing, measure, deploy-cadence, competitor-cdn-latency) and one re-run latency and cost model (`synthesis/v2/model.py`, `cost.py`). **v2.2** reworks every VM figure after maintainer feedback that v2.1 over-sized the VMs and ignored disk-image seeding. It adds three tracks (fly-docs, planet-bench: the full planet served under emulated small Fly shapes, image-replication) and a small-VM latency and cost model (`small-vm/MODEL_SMALLVM.md`, `model_smallvm.py`, `cost_smallvm.py`). **v2.3** adds live tests on real Fly.io Machines and Tigris buckets (the maintainer approved a test budget; spend about $1.42, at most $6.43, everything destroyed afterwards), a per-region read test of the production bucket, and a Tigris-backed (volume-less) design track (§2.5, §2.6, §4.D). Measured Fly numbers replace modelled ones where they exist. Appendix B lists what changed against v1, the v2.1 consistency fixes (B.5), the v2.2 rework (B.6) and the v2.3 live measurements (B.7). Prices were checked 2026-11-01/02. Scratch data lives under the research scratch directory (not in the repo).

**Labels used throughout:**
- **measured**: measured in this research. *Measured live* (v2.3) means on real Fly.io Machines, Tigris buckets or the production bucket, not under emulation.
- **derived**: computed from measurements.
- **modelled lower bound**: the network model in §5. The same formulas apply to every option, and nothing is validated end to end.
- **calibrated**: measured anchors for today's stack plus modelled deltas.
- **unverified estimate**: no source.
- **assumed**: staff time (engineer-days, ops hours, the $100/h rate) is an estimate with a stated basis, never a measurement. Headline costs are **infrastructure only**; staff time and 12-month TCO are a separate, secondary line (§7.1).

**Option labels added in v2.2.** A "-s" suffix (B2-s, B3-s, B4-s, C1-s, C3-s, C6-s) is the v2.1 option rebuilt on small 160 GB VMs (Hetzner Cloud CX43 in the EU, Vultr vc2-4c-8gb elsewhere). **Fly-direct, Fly-cf, Fly-cf-stc and Fly-wkr** are small-Machine Fly.io fleets (§4.D); they replace v2.1's Fly options D1 and D2. The small-VM model files call these V-cf / V-direct and F-direct / F-cf / F-cf-stc / F-wkr. The hosted-API rows F1-F3 are unrelated.

FX is EUR 1 = USD 1.1172, the ECB reference rate for 30 Oct 2026 ([ECB](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/eurofxref-graph-usd.en.html), [daily XML](https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml); 90-day range 1.1130-1.1699). All EUR prices are ex-VAT.

**Pending production split.** The production breakdown (misses by continent, zoom mix, colo by continent, D1 `served_by_region`, peak 5-minute rate) was not available for v2. So:
- misses are weighted by request share, with a sensitivity case;
- the distant-colo share is still an extrapolation from probes;
- peak bandwidth assumes a 3-4× peak-to-average ratio.

Each of these is marked *pending production split*. Appendix C has the exact queries.

The live deployment key is `20250818` (tiles schema 4.11.9). Planet statistics come from the newest build, `20261101` (139.24 GB, v4.15.2), because a future deploy would ship that. During the v2.3 tests a production Tiles Deploy run (GitHub Actions run 41498473373) wrote a new `20261102/` prefix to `tiles-geo` (5,826 chunks, 5.83 GB, 18:27-18:44Z); whether the live key has moved was not checked.

---

## 1. TL;DR

1. **Traffic is small, and so is the infrastructure bill for every serious option.** Measured: 13.7M requests/month (5.3 req/s), a 73.8% per-colo tile hit ratio, and traffic 63% EU / 21% NA / 11% Asia. Today's stack costs about **$15/month**. One small EU origin is **$21-71/month** (B2-s / B2), and a small fleet of 3-4 regions **$98-136/month**. Migration is an *assumed* 6.5-13 engineer-days for these options (up to 17.5-18 with a Worker in front). Staff time and 12-month TCO are a secondary, assumed line (§7.1).
2. **v2.3 measured the main alternative live** (§2.5). A Fly.io fleet of four small Machines (VersaTiles on shared-cpu-1x 1 GB, one 150 GB volume each, ams / iad / sin / syd, clients straight to Fly's anycast, no CDN) was probed from the same 48 Globalping probes, at the same time as today's service:
   - **per tile, traffic-weighted: 51 ms warm and 55 ms never-requested**, against today's 54 ms hit and **459 ms miss** (**159 ms blended** at 73.8% hits);
   - **first map load 184-195 ms**, against today's 195 ms when everything hits and 590 ms when the tile misses (190 vs 379 expected);
   - about **$128-136/month** at today's traffic (from Fly's price list). The tests cost about $1.42 (at most $6.43), and every resource was destroyed.
3. **Today's misses are slow because of the path to Tigris** (§2.2). The Tigris endpoint test (X7) split a cold read from Helsinki: cold placement in the serving Tigris region is **60-80%** of it, a new connection about 52 ms, and the warm floor about 33 ms. Tigris's own service time is 0-8 ms. The Worker's Fly endpoint is slower than `t3.storage.dev` in Europe: 396 vs 251 ms cold and 185 vs 89 ms warm, on fresh connections.
4. **Is `tiles-geo` replicated to every region? On paper yes, in practice no** (§2.6). The bucket is configured for 12 regions and its objects list 11. Yet in every one of 6 regions tested, the first read of a chunk was a Tigris cache miss (160-670 ms), and Tokyo, Singapore and Sydney (Sydney on the Fly endpoint) were served from Johannesburg. So eager placement (A10, Dual-Region) is **dropped**. Next: ask Tigris support with these examples, and test a per-region pre-warm after each deploy (about $0.85 for 12 regions).
5. **#1, now: the cheap Worker fixes (A1 + A1b)**, 1.5-3.5 engineer-days, about $15/month. A1b moves the Tigris client to `t3.storage.dev` and builds it once per isolate. t3 wins in Europe (and on first reads in North America and Asia) but has no São Paulo or Sydney presence (it served them from iad and sjc), so **pick the endpoint per continent** until a Worker-side test (X12) settles it. Calibrated full miss: 202-456 ms.
6. **#2, next: measure Cloudflare in front of the Fly fleet** (X11, on a Cloudflare test hostname). It was not tested live, because no Cloudflare credentials were used. Modelled: 30-40 ms per tile with Tiered Cache off. The same model was 4-9 ms optimistic for direct Fly (a like-for-like 3-region figure derived from the measurements is 58-63 ms, against 54 modelled). Cloudflare in front adds an L7 shield, faster connection setup (36 vs 54 ms TCP + TLS, measured) and ~340 colos for hits, and cuts Fly egress to about 26%.
7. **#3, the main move: a Fly volume fleet**, direct or behind Cloudflare depending on #2 (§4.D):
   - **a new cluster per data update.** A per-region parallel pull from Tigris (t3) on a temporary performance-2x loader takes about **35 min**, capped by the volume write rate (derived from the measured ams seed and t3 rates; a full pull per region was not run). A fork is slower: a 35 min seed, then **43 / 86 / 159 min** of hydration for iad / sin / syd. Do not route traffic until the volume is `created`: during hydration a never-requested tile takes 98-315 ms, and afterwards **4.3-5.0 ms** in every region;
   - **fewer copies than regions works.** Fly's edges forward to the nearest Machine (measured): the Americas to iad, Europe and Africa to ams, Tokyo and Singapore to sin. A missing region costs its users: NA-West 104-118 ms and Latin America about 220 ms per tile;
   - **shared-cpu-1x 1 GB is enough.** It reached 279-285 req/s while bursting and 20-30 req/s on its baseline, at 2.7-3.1 ms of CPU per tile. The busiest region today averages 3.4 req/s.
8. **Suspend works but saves little** (§4.D). The first request after a suspend takes **+0.42-0.49 s** and caches survive. A stopped Machine takes **20.6 s** to serve again. The proxy suspends after about 6 min idle and wakes the nearest suspended Machine rather than using a running one farther away. Suspend saves only compute ($6.70-8.50 per region) while the volume ($22.50) keeps billing, and at today's traffic a region with a Machine is rarely idle for 6 minutes. Use it for second Machines and volume-less designs.
9. **Serverless alternative, if no new vendor is wanted: Workers Cache (A2)**, $14/month at R ($18 with the metrics gateway). Calibrated A1b + A2: mean per tile 49-85 ms, against the measured Fly fleet's 53 ms blended (different methods: the live method puts today at 159 ms, against its calibrated 96-154), and unverified until the Workers Cache spike (X1). Small VMs behind Cloudflare (B3-s $126, B2-s $21 EU-only) remain modelled. No code can run in Cloudflare's upper tier (§4.A9).
10. **Tigris-backed Machines without volumes are a later cost cut, not the first move** (§4.D). Measured: stock go-pmtiles reading one 139 GB object is slow (246 ms warm, 612 ms never-requested per tile), because Tigris did not cache the big object outside fra. TigrisFS is not recommended (19-33 tiles/s cap on cold tiles, 5 MiB read-ahead per open, unbounded disk cache). A server with every directory in RAM, today's 1 MB chunks and a local chunk cache (or the open-source TAG proxy) is modelled at about **$40-60/month for 4 regions instead of ~$130**, but needs code and a per-region pre-warm.
11. **Do not:**
    - route traffic to a Fly fork that is still hydrating, or pull from Tigris in Singapore through the Fly endpoint (it stalled);
    - serve tiles by reading one big Tigris object, or through TigrisFS;
    - put Fly behind Cloudflare with Smart Tiered Cache on (an anycast origin collapses it to one upper tier);
    - size VMs with performance CPUs, 64 GB RAM or two-copy disks;
    - put tile bytes in D1, KV or Durable Objects ($150-340/month at R), or use metered tile APIs ($250-2,300/month at R);
    - expect leaving PMTiles to fix speed. R2 is **unproven either way** (X8).
12. **Uncertainties:** Cloudflare in front of Fly (X11); the Worker-side effect of t3 (X12); how Tigris caches per region (X13, X14); Workers Cache topology and what the gateway can see (X1); the *pending production split* (miss weights, distant-colo share, zoom mix, peak rate; Appendix C); whether Fly bills fork hydration traffic; Fly's lack of an SLA below Enterprise. Live Fly throughput and suspend figures are one run per setting. Deploy cadence costs latency, not money (monthly deploys add about 12-20 ms to today's mean per tile; §7.2).

---

## 2. Current setup and measured baseline

### 2.1 Request path (from `tiles/src/*.ts`, `deployment/modules/cloudflare/tiles-worker`)

| Step | Hit | Miss | Notes |
|---|---|---|---|
| Worker invocation on the custom domain `tiles.immich.cloud`, billed per request | yes | yes | The Worker runs before any zone cache ([CF how the cache works](https://developers.cloudflare.com/workers/reference/how-the-cache-works/)). |
| `new CloudflareD1Repository()` fires an un-awaited `SELECT 1` through `withSession('first-unconstrained')` (`repository.ts:22`) | **yes** | yes | One D1 query per request, hits included. It runs outside `waitUntil`, so the runtime may cancel it. |
| `caches.default.match(url)` plus a check of the `PMTiles-Deployment-Key` header | yes | yes | Per data centre, not tiered, no request collapsing ([Cache API](https://developers.cloudflare.com/workers/runtime-apis/cache/), [limitations](https://developers.cloudflare.com/workers/cache/limitations/)). A key mismatch counts as a miss, so **every deploy already empties the edge cache**. |
| TileJSON metadata from the isolate `memCache`, or a D1 query when empty | no | yes | Measured at 7-816 ms when it runs. It ran on 21/40 and 6/10 synthetic cold misses, which is probably an upper bound for production. |
| D1 leaf-directory lookup (`startTileId <= ? ORDER BY … LIMIT 1`, 50 entries per row) | no | yes | About 3.5 GB of text per planet. D1 caps a database at 10 GB ([D1 limits](https://developers.cloudflare.com/d1/platform/limits/)). |
| Tigris `GetObject` with a Range on `<key>/chunk_<N>.pmtiles` (about 1 MB chunks, about 140k objects) | no | yes | A new `S3Client` and SigV4 signature per miss, against **`https://fly.storage.tigris.dev`** (`repository.ts:74`). The bundle is 762 KB. |
| `await response.arrayBuffer()`, then `cache.put` in `waitUntil` | no | yes | The whole body is buffered before the first byte goes out. |
| Influx line-protocol POST to the metrics endpoint (`waitUntil`) | yes | yes | Default tags are `continent`, `colo`, `asOrg`, `apexDomain` and `protocol` (`repository.ts:273-279`); there is no country tag. `apexDomain` (one per self-hosted origin) and `asOrg` are a cardinality risk. |
| Response headers | `Cache-Control: public, max-age=2678400`, `Vary: Origin` (live it shows as `Origin, accept-encoding`), `Access-Control-Allow-Origin: *` | | Tile URLs are unversioned (`/v1/{z}/{x}/{y}.mvt`), so browsers can mix old tiles with a new style for up to 31 days after a deploy. |

### 2.2 Measured latency

**Keep-alive clients, random city tiles, Worker `Server-Timing` (brief plus tracks):**

| Vantage (colo) | Hit TTFB p50 | Miss TTFB p50/p90/p99 | Worker on miss | D1 p50/p90/p99 | Tigris p50/p90/p99 |
|---|---|---|---|---|---|
| London (LHR) | 24-32 ms | 131-141 / 238-315 / 504-573 | 110/285/527 | 14-15 / 25-27 / 234-348 | 79-86 / 159-191 / 402-418 |
| Helsinki box (WAW) | 29-31 ms | 197-221 / 414-501 / 744-1074 | about 190 | 24 / 28-37 / 314-518 | 126-160 / 331-423 / 692-818 |

**Worldwide** (latency-measure track): 48 Globalping probes, 43 tagged as ISP networks and 5 in datacentres, 127 miss samples. Their median edge RTT is 6 ms, so these are best-case access lines (§5, caveat 5). "Wtd" means a traffic-weighted mean of per-region medians, not a true p50.
- Tigris GET over all misses: **p50 380 ms, p90 about 960 ms**.
- D1 lookup over all misses: **p50 39 ms, p90 about 340 ms**. Oceania was 276-662 ms.
- Johannesburg misses were 1,309-1,434 ms in all four samples.
- Per-probe miss medians: Tokyo 1,186 ms, Jakarta 1,138, Singapore 860, Sydney 791, Melbourne 834. Perth (1,741 ms, pathological ISP routing) and Auckland (1,352) pull Oceania up.
- Adjacent-tile repeats about an hour later help in Europe and North America (London 243 → 103 ms, Madrid 365 → 104, Ashburn 700 → 124) but not in APAC or Johannesburg (0.9-2.0 s).
- Traffic-weighted: single-shot misses **488 ms**, warm-isolate repeats (R5) **300 ms**, hits 24 ms warm and 35 ms on a new connection.

**Why misses are slow** (ordered by measured size):

1. **Tigris time dominates, and the cause is the path to Tigris, not Tigris's service time.**
   - *Method (fixups-measure, 2026-11-02).* The Worker's `caches.default` key is the full URL, but the router reads only the pathname. So `?nc=<random>` forces the real D1 + Tigris path on the same byte range every time.
   - *Metric.* `tigris_storage_get` in `Server-Timing` is wall time until Tigris returns headers. Workers timers advance only on I/O ([CF](https://developers.cloudflare.com/workers/runtime-apis/performance/)), so this is waiting, not CPU.
   - *Vantages.* A London FTTP line (colo LHR), the Hetzner Helsinki box (colo WAW), and Globalping from 6 colos. About 440 forced misses in total.

   | `tigris_storage_get`, ms (p50 / p90 / max) | LHR | WAW |
   |---|---|---|
   | Same tile, one kept-alive client connection, reads 2-30 (n=102) | 32 / 39 / 91 | 52 / 85 / 244 |
   | First read of a fixed tile (3 tiles) | 61, 292, 412 | 156, 216, 469 |
   | 30 adjacent tiles in 1-2 chunks: first 3 / the rest | 129-255 / 32 / 75 / 113 | 173-453 / 47 / 56 / 82 |
   | 30 tiles in far-apart chunks, kept alive | 144 / 318 / 403 | 250 / 419 / 556 |
   | Same warm tile, new client connection each, 0.5 s apart (n=30) | 36 / 226 / 301 | 57 / 154 / 383 |
   | Same warm tile, new connection after 5-300 s idle (n=14) | 177 / 274 / 298 | 187 / 379 / 746 |
   | Globalping FRA/IAD/SIN/SYD/GRU/NRT, new connection each: read 1 / reads 2-5 | 339-1,274 / 192 / 473 / 997 (4 of 24 under 70 ms) | |

   What this separates:
   - **Service time is small.** Tigris's own `Server-Timing` on a public test object was 0-8 ms per cached read. On cold reads it showed 7-14 ms while the client waited 0.7-0.8 s, so it excludes remote fetches.
   - **Regional cold placement is real.** The first read of a chunk costs 130-470 ms from LHR/WAW and 0.3-1.3 s elsewhere. Adjacent tiles turn fast after 2-3 reads, so the serving region caches whole chunks ("Data pulled on demand and cached locally", [Tigris bucket locations](https://www.tigrisdata.com/docs/buckets/locations/)).
   - **Per-instance state matters about as much.** A warm range settles at 32 ms (LHR) or 52 ms (WAW) on one kept-alive connection. Once requests spread across Worker instances, or after 5 s or more of idle time, 22 of 28 reads of the same warm range took 111-746 ms. Cold isolates explain little (`tiles_pmtiles_init` was 0 on most slow reads). Production has about 1.3 misses/s spread over hundreds of colos, so most misses arrive in this state. That also explains v1's "byte-identical re-read still slow in 7 of 10 cities".
   - **The Worker uses Fly's endpoint** (`fly.storage.tigris.dev`). On a public Tigris object it served London and Helsinki from **AMS**, while the canonical `t3.storage.dev` served them from LHR and FRA. Keep-alive reads were 51 vs 18 ms (London); first requests on a new connection were 120 vs 30 ms. On production chunks from Helsinki (v2.3), fresh-connection reads were **396 vs 251 ms cold and 185 vs 89 ms warm** (Fly endpoint vs t3). Details, the per-region picture and sources are in A1b and §2.6.
   - **Separated in v2.3 (the Tigris endpoint test, X7, from the Helsinki box with a read-only production key; §2.5).** On a fresh t3 connection a cold production chunk read is 251 ms p50: **cold placement in the serving Tigris region is 60-80% of it** (about +165-280 ms), connection setup about 52 ms, and the warm floor 33 ms. On the Fly endpoint, 55-60% of fresh connections also carry a server-side term of about 110 ms (3% on reused connections). Tigris's `Server-Timing` misses most of the cold cost: client TTFB minus `Server-Timing` is 100-250 ms. Idle connections close after 60-120 s on t3 and 120-300 s on the Fly endpoint, so at about 1.3 misses/s spread over hundreds of colos most Worker misses open a new connection.
   - **Net:** a 30-50 ms floor in the EU, plus 100-250 ms when the Worker instance has no warm connection (measured in the Worker; from the box the same step costs about 52 ms on t3 and 70-180 ms on the Fly endpoint), plus 100-400 ms (EU/NA; up to about 1.2 s elsewhere) when the chunk is cold in the serving region. The bucket already lists 11-12 regions, yet first reads miss in every region tested (§2.6), so eager multi-region placement (A10) is dropped.
2. **D1 tail and distance.**
   - p90 is about 340 ms.
   - Repeat lookups of the *same row* (2026-11-02) still had a tail: p50 14 ms in LHR and 26 ms in WAW, with 7-9% above 100 ms (max 649 / 833 ms). So the tail is replica or routing variance, not data placement.
   - Globalping repeats were flat by colo: SYD 324-326 ms on all 5 reads, GRU 135-149, SIN 42-74, IAD 38-41, FRA 6-10, NRT 7-10. Sydney and São Paulo are consistently far, despite Cloudflare listing an OC replica.
   - D1 routing for `first-unconstrained` is "non-deterministic" and can go to the primary ([D1 read replication](https://developers.cloudflare.com/d1/best-practices/read-replication/)). The `served_by_region` tag the Worker already emits would settle where those reads go (*pending production split*; Appendix C).
3. **Two serial remote hops.** On cold isolates there is an extra serial D1 query (7-816 ms), and the whole body is buffered (a single 400 ms observation in Mumbai).
4. **Per-colo, untiered cache.**
   - Each of [348 cities](https://www.cloudflare.com/network/) fills on its own.
   - Eviction is LRU and "not configurable" ([retention vs freshness](https://developers.cloudflare.com/cache/concepts/retention-vs-freshness/)), so a 31-day max-age does not keep long-tail tiles. The deploy-cadence model puts per-colo retention at about 1-24 days (§7).
5. **This zone is routed to distant colos for some users.**
   - For 12 of 48 probes, tiles.immich.cloud landed in a distant colo while www.cloudflare.com was served locally. Examples: Mumbai→MRS 99 ms against BOM 2 ms, Seoul→LAX 135 against 4, Taipei→SJC 126 against 1, Lagos→LHR 104 against 2. That adds 30-135 ms to every request, hits included.
   - Applying these 12 single-ISP probes to whole sub-regions gives about **8-11% of traffic**. That is an **extrapolation, not a measurement** (*pending production split*).
   - The Worker tags `continent`, `colo` and `asOrg`, not country. So production can measure the share of requests served from a colo **outside the client's continent** (Appendix C, Q2-Q6). Intra-continent detours (Mexico City→DFW, Lima→EZE) appear only by `asOrg`. Structural routing that a plan change would not fix (Cairo, Dubai → MRS, as for www.cloudflare.com) must be subtracted.
   - A zero-cost comparison (2026-11-02) suggests the zone sits in Cloudflare's Free-plan IP pool (A7).
   - Cloudflare says colo selection "cannot be configured per zone, per account, or by support request" ([CF geographic routing](https://developers.cloudflare.com/support/troubleshooting/general-troubleshooting/geographic-traffic-routing/)).

### 2.3 Traffic: measured, pending, scenarios

| Source | Value |
|---|---|
| VictoriaMetrics `tiles_*` metrics, 7 days to 2026-11-01T23:53Z | 3,201,740 Worker requests: 2.17M tile hits, 0.77M tile misses, 0.25M JSON/style. Projected per month: **13.7M requests, 12.6M tiles, 3.3M tile misses, 73.8% tile hit ratio, 5.3 req/s average**. May undercount if VictoriaMetrics dedup applies. |
| By continent (same data) | EU 62.9%, NA 20.8%, AS 11.0%, OC 3.0%, SA 1.7%, AF 0.6% |
| Immich blog, 2026-06-01 | "tens of millions of requests each month" ([immich.app](https://immich.app/blog/futo-two-years-later)), i.e. 10-99M |
| *Pending production split* (Appendix C) | Misses and hits by continent (miss weights), by zoom, colo by continent (distant-colo share), D1 `served_by_region`, miss latency quantiles by continent, peak 5-minute rate, Cloudflare dashboard request count |

Scenarios used throughout: **R = 13.7M (measured)**, S = 50M, M = 250M, L = 1B, XL = 3B requests/month. At an 18 KB mean that is 0.25 / 0.9 / 4.5 / 18 / 54 TB.

### 2.4 Cost baseline

**Canonical model.** All cost tables use these inputs (`synthesis/v2/cost.py`).
- Cloudflare Workers ([pricing](https://developers.cloudflare.com/workers/platform/pricing/), updated 2026-10-02):
  - $5/month minimum.
  - 10M requests included, then $0.30/M. Requests served from cache are billed too.
  - 30M CPU-ms included, then $0.02/M CPU-ms.
  - Subrequests and egress are not billed.
- Tigris ([pricing](https://www.tigrisdata.com/pricing.md)): $0.02/GB-month, Class B GETs $0.50/M, egress free.
- D1 ([pricing](https://developers.cloudflare.com/d1/platform/pricing/)): 25B rows read included, 5 GB included, then $0.75/GB-month.
- Hit ratio: 73.8% per colo, measured 14 months after the last deploy. This is a converged no-deploy steady state, not a "6-monthly" figure. URL-changing deploys lower the time-averaged ratio: 73% 6-monthly, 69% monthly, 57% weekly, 36% daily (modelled, §7).
- Worker CPU: 1.5 ms per hit and 10 ms per miss (**unverified estimate**); 8 ms after A1, 6 ms after A1b, 3 ms for a thin Worker, 1 ms per gateway run (assumptions).
- Storage: 2 retained deployments. Tigris 2 × 129.7 GiB = $5.19; D1 2 GB over the free 5 GB = $1.50.
- Workers Logs off. Check this: if it is on, Logs is billed per event until 2026-12-01 and per GB after ([Workers Logs](https://developers.cloudflare.com/workers/observability/logs/workers-logs/), [Observability pricing](https://developers.cloudflare.com/observability/pricing/)).

| Per-colo hit ratio | R 13.7M | S 50M | M 250M | L 1B | XL 3B |
|---|---|---|---|---|---|
| 60% (≈ weekly deploys with the key in the URL, modelled 57%) | $16 | $38 | $158 | $606 | $1,802 |
| **73.8% (measured; ≈ monthly or slower deploys)** | **$15** | **$33** | **$134** | **$514** | **$1,525** |
| 90% | $13 | $28 | $107 | $405 | $1,199 |

- **Breakdown at R:** requests including the $5 base $6.11, CPU $0.42, Tigris GETs $1.74, storage $6.69. Storage and the base fee are about 78% of the bill. The per-request fee dominates only from about S up. Hits are billed too, so raising the hit ratio from 60% to 90% cuts the bill by only 17-34% (17% at R, 27-34% at S-XL).
- **Hit ratio rising with volume (sensitivity).** Every cost table holds the per-colo hit ratio at 73.8% at every volume. The deploy-cadence model's traffic-scaled runs (`fixups-deploy-cadence/out/model_summary.txt`, "scale (T fixed)") give a steady state of 83.6% at S, 94.8% at L and 96.8% at XL, because more traffic keeps more tiles in each colo.
  - If that holds, today costs $30 at S, **$373 at L** (not $514) and $1,062 at XL.
  - Break-evens against today move out: B2 125M → about 153M, B2-HA 233M → about 298M, B1 326M → about 1,000M, B3 550M → about 750M (`consistency-check/scale.py`).
  - §7.2's L adders for deploy cadence (+$9 / +$33 / +$99 for monthly / weekly / daily) come from these traffic-scaled runs. At this table's constant hit ratio, the same cadences add about +$30 / +$110 / +$250 to A0 at L (`cost.py` CADENCE: $543 / $624 / $764 against $514). So the 60% row (+$92 at L, the constant-ratio counterpart of weekly deploys) and §7.2's weekly +$33 differ by this assumption, not by a cost error.
- **Plugging in a real number** (N = million requests/month; slopes are $ per million requests above 10M):

  | Option | $/month | Slope |
  |---|---|---|
  | Today (A0) | ≈ $13 + $0.51 × (N − 10) | 0.506 |
  | Workers Cache (A2) | ≈ $12 + $0.39 × (N − 10) | 0.386 |
  | Workers Cache + metrics gateway (A2-gw) | ≈ $15 + $0.71 × (N − 10) | 0.706 |
  | CDN-fronted single VM, no Worker (B2) | **$71 flat** (AX41) | 0 |
  | Same on a small Hetzner Cloud CX43 (B2-s, v2.2) | **$21 flat** | 0 |
  | Thin Worker + VM (B1) | ≈ $76 + $0.31 × (N − 10) | 0.308 |
  | Small 3-region VM fleet behind a CF LB (B3-s, v2.2) | **$126 flat** | 0 |
  | Cloudflare in front of 3 small Fly regions (Fly-cf N=3, v2.2) | $98 at R, $125 at M, $222 at L, $507 at XL | ≈ 0.12-0.14 (Fly egress on 26% of bytes, bigger shapes from M) |
  | Fly anycast, 3 regions, no CDN (Fly-direct N=3, v2.2) | $102 at R, $217 at M, $625 at L | ≈ 0.43 egress, plus bigger shapes from S |
  | Fly anycast, 4 regions, no CDN (Fly-direct N=4, the fleet measured live in v2.3) | $128-136 at R, ~$250 at M, ~$660 at L | ≈ 0.43 egress, plus bigger shapes from S |

  Break-even against today: **B2 at 125M** (**B2-s at about 26M**), B2-HA at 233M, B3-s at about 233M, Fly-cf N=3 at about 230M, B1 at 326M, B1-fetch at 384M. B1-gw, A2-gw and Fly-direct are never cheaper.
- **Account-level allowances** (critic #20).
  - The $5 minimum, the 10M requests and 30M CPU-ms, D1's 25B rows read, 50M rows written and 5 GB ("the sum of all databases in your account"), and the KV and Durable Object allowances are all **per account** ([Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/), [D1 pricing](https://developers.cloudflare.com/d1/platform/pricing/)).
  - tiles-prod already shares them with tiles-dev, tiles-d1-proxy and the `tiles_dev` database, and possibly with other Immich Workers. We cannot see account usage.
  - **Marginal view** (account already past every allowance, $5 base excluded): today ≈ $10.44 + $0.506 × N, i.e. $17 at R. B2 break-even is 120M, B1 307M, B2-HA 228M.
  - Every cell differs from the canonical view by $3 or less, except A4 (+$4 at R, +$6 at S-XL, because R2's free Class B allowance is also account-level). Today costs slightly *more* in the marginal view, so B2 saves slightly more, not less as the critic suggested. Each deploy also writes about 3.57M D1 rows: $0 within the 50M included, $3.57 at marginal rates.
- **Other platform risks:**
  - D1 is single-threaded per database: "If your average query takes 1 ms, you can run at most 1,000 queries per second" ([D1 limits](https://developers.cloudflare.com/d1/platform/limits/)). Read replicas spread reads.
  - The 10 GB per-database cap fits two 3.5 GB planet tables, about 8 years at +7 GB/year of planet growth. It does not fit three: #245's served + live + previous retention already needs 10.5 GB and a second database.

v1's four track models ($31-36 at S, $440-532 at L) are superseded (Appendix A).

### 2.5 Live measurements on Fly.io and Tigris (v2.3)

**Why.** v2.2 modelled every Fly figure and listed the Fly small-fleet test (X10) and the Tigris endpoint test (X7) as the main gaps. The maintainer approved a live-test budget (hard cap $100). On 2026-11-02, 13:28-19:52Z, the tests below ran on real Fly.io Machines and Tigris buckets, plus read-only GETs on the production bucket. **Fly behind Cloudflare was not tested**: no Cloudflare credentials were used, so it stays modelled (X11).

**Setup.**
- **Test bucket** `tiles-research-fs-20261102` (Tigris, data in fra). It held the 20261101 planet as one 139,235,644,790-byte object, uploaded from the Hetzner box through t3 in 21 min at 110 MB/s (the box's 1 Gbit uplink), and 1,500 chunk objects of production's 1 MB size (fixed byte ranges; production's chunks are tile-aligned). 30 of 30 integrity checks matched.
- **Volume app** (`tiles-research-vol-20261102`): VersaTiles 5.6.0 on shared-cpu-1x 1 GB Machines in ams, iad, sin and syd, each with one 150 GB volume (snapshots off), behind Fly's TLS proxy on the shared anycast IPv4. ams was seeded from the bucket; iad, sin and syd are forks of it.
- **S3 app** (`tiles-research-s3-20261102`): go-pmtiles 1.31.2 reading the single planet object from Tigris (400 MB directory cache, no volume) on shared-cpu-1x 1 GB in ams, iad and sin, plus one TigrisFS Machine in ams.
- **Box tests** (Helsinki): the Tigris endpoint test (X7) on the test bucket and on the production bucket (read-only key), and go-pmtiles and TigrisFS serving from the box.
- **Per-region production read test**: one curl Machine each in lhr, iad, gru, nrt, sin and syd, destroyed after 2 minutes (§2.6).
- **Worldwide latency**: the 48 v2 Globalping probes, pinned so every host and round hits the same physical probe; 740 tests, no account. Each round ran three hosts back to back: today's `tiles.immich.cloud` (T), the volume app (V) and the S3 app (S).
  - Never-requested tiles were random z14/z15 tiles within ±0.08° of each probe's city, excluding every tile any other test had requested. Warm means the same tile about a minute later.
  - TTFB is Globalping's `firstByte`, timed after TLS on a new HTTP/1.1 connection. Connection setup is TCP + TLS.
  - Aggregation: the median per probe, the median of probes per region, then traffic-weighted by continent (EU 62.9 / NA 20.8 / AS 11 / OC 3 / SA 1.7 / AF 0.6). Figures with §5's 11-region weights are in brackets.
  - First map load = TCP + TLS + 2 × TileJSON TTFB (the style is assumed to cost the same; on 10 probes it was 35 vs 48.5 ms) + one tile.

**Worldwide latency, traffic-weighted p50 (ms; measured live, 17:38-19:43Z):**

| Host | New connection TCP + TLS | Warm tile | Never-requested tile | Blended at 73.8% hits | TileJSON | First map load warm / cold | Expected (40% of views miss, §5.4) |
|---|---|---|---|---|---|---|---|
| Today, CF Worker + Tigris (T) | 36 | 54 (53) | **459** (482) | **159** | 44 | 195 / 590 (211 / 622) | 379 |
| Fly volume fleet, direct (V) | 54 | **51** (54) | **55** (59) | **53** | 44 | 195 / 184 (208 / 202) | **190** |
| Fly S3: go-pmtiles on one Tigris object (S) | 53 | 246 (251) | 612 (632) | 329 | 202 (metadata read from Tigris) | 711 / 1,070 | 828 |

- On V a never-requested tile costs only about 4 ms more than a warm one. The fleet has **no miss tail**.
- Today's never-requested tile (459-482 ms) matches v2's measured single-shot miss (488 ms). Three Singapore-colo misses timed out after 15 s at 19:40Z and count as 15 s; retries took 1.2-1.4 s and were still misses.
- Today's warm tile here (53-54 ms) is higher than §2.2's 24-35 ms. Globalping times new HTTP/1.1 connections from 48 mostly ISP probes, which differs from keep-alive and edge-timed hits. Within this run all three hosts were measured the same way.
- V served planet 20261101 and T serves 20250818, so tile sizes differ slightly.

**Per region (p50 ms):**

| Region (share) | Today (T) warm / never-requested | Fly fleet (V) warm / never-requested | V answered by | Fly-direct N=3 model (§5.7) | Blended, T → V (derived) |
|---|---|---|---|---|---|
| Europe (62.9%) | 43 / 342 | 41 / 48 | ams | 42 | 121 → 43 |
| NA-East (9.4%) | 37 / 600 | 33 / 29 | iad | 28 | 185 → 32 |
| NA-Central (5.2%) | 46 / 472 | 43 / 44 | iad | 42 | 158 → 43 |
| NA-West (6.2%) | 77 / 494 | 118 / 104 | iad (sjc, lax edges) | 88 | 186 → 114 |
| LatAm (1.7%) | 75 / 804 | 220 / 224 | iad (gru edge) | 168 | 266 → 221 |
| Africa (0.6%) | 158 / 1,157 | 160 / 195 | ams (jnb, lhr edges) | 154 | 420 → 169 |
| Middle East (1.2%) | 83 / 302 | 88 / 78 | ams; Dubai sin | 76 | 140 → 85 |
| South Asia (2.5%) | 169 / 879 | 144 / 182 | ams (Mumbai); sin or syd (Bengaluru) | 108 | 355 → 154 |
| East Asia (4.1%) | 112 / 604 | 94 / 121 | sin (nrt edge) | 100 | 241 → 101 |
| SE Asia (3.3%) | 94 / 1,424 | 42 / 42 | sin; Jakarta via lax to iad | 39 | 442 → 42 |
| Oceania (3.0%) | 25 / 1,267 | 28 / 35 | syd | 155 (no syd Machine) | 350 → 30 |
| **Weighted** | **53 / 482** | **54 / 59** | | **54** | **166 → 55** |

- **Blended per region** (0.738 × warm + 0.262 × never-requested), **the fleet is faster than today in every region.** On warm tiles alone it is slower in NA-West (+41 ms) and Latin America (+145 ms), where it has no Machine.
- **Model against measurement.** The Fly-direct N=3 model came within 6 ms in Europe, North America East and Central, Africa, East Asia and SE Asia. It was 12-52 ms optimistic in NA-West, Latin America, the Middle East and South Asia, where edges forward to a distant Machine or probes detour. With syd present the weighted figures match (54 measured, 54 modelled). Putting Oceania back at the model's no-syd 155 ms gives a like-for-like 3-region figure of about **58 / 63 ms** (derived): the model was 4-9 ms optimistic, inside v2's 5-20 ms calibration margin.
- **Fly's connection setup is slower than Cloudflare's**: 54 vs 36 ms weighted TCP + TLS. Anycast detours seen: Jakarta → lax edge → iad Machine (165 ms TCP), Mumbai → ams (140 ms), Dubai → sin (80-160 ms, where Cloudflare uses MCT at 10 ms), and Bucharest → ams at 135-140 ms TCP on the volume app's shared IPv4 but about 40 ms to lhr on the S3 app's.
- **Edge → Machine, measured with the `flyio-debug: doit` header:** the lhr, cdg, fra, arn, ams and jnb edges forwarded to ams; ewr, iad, yyz, ord, dfw, sjc, lax and gru to iad; nrt and sin to sin; syd to syd (to sin on the S3 app, which had no syd Machine). This is the fall-through to the nearest region that the maintainer described.

**Fly volume operations (measured live; one run each):**

| Step | Result |
|---|---|
| Seed the ams volume from the test bucket (performance-2x, s5cmd, 16 × 64 MiB parts, Fly endpoint) | **34.6 min** at 67.0 MB/s. Volume writes were pinned at the 64 MiB/s performance-2x cap; the network had headroom. 18 of 18 block hashes matched |
| Tigris read throughput, 64 MiB ranges to /dev/null | ams performance-2x, 16 streams: t3 548 MB/s, Fly endpoint 370. Shared-cpu-1x, 8 streams: iad t3 216 / Fly 137; sin t3 176 / Fly **7.8, with 4 of 32 ranges stalled for over 120 s**; syd t3 98.5 / Fly 47.7 MB/s |
| Fork ams → iad, sin and syd, issued together from one source | `hydrating` after 27-43 s; `created` after **43.4 / 85.6 / 158.8 min**, about 54 / 27 / 15 MB/s. Rate × RTT stays near 4.5 MB, so hydration is round-trip bound |
| Never-requested tile inside the Machine (VersaTiles on 127.0.0.1) | ams local volume 5.4 ms p50. **During hydration 98 / 167 / 315 ms** (iad / sin / syd). **After `created`, 4.3-5.0 ms in every region** (iad had 21 IO blips of 31-566 ms in its first 15 min) |
| VersaTiles start-up (it reads about 362 MB of directories before binding) | **20 s** on a local volume at the 16 MiB/s cap (first boot 20.7 s, 5 later cold boots 19.8-20.1 s); **50-90 s** on a hydrating fork |

**Throughput of one shared-cpu-1x 1 GB Machine** (ams, planet request mix over the private network, 60 s steps):
- With burst credit: 5, 30 and 50 req/s at p50 6 ms and p99 9-14 ms. Saturation **279-285 req/s** (p50 60 ms, about 75% CPU).
- The credit ran out after about 2 minutes at saturation. On the baseline alone **20 req/s was clean** (p99 9 ms), 30 marginal (p99 87 ms) and 50 collapsed (p99 1.7 s); saturation fell to 26-35 req/s.
- CPU per request: **2.7-3.1 ms**, as the box emulation found (about 3 ms). But sustainable baseline capacity is about **20-30 req/s, below the emulation's 45**. One run per setting.
- A new Machine started with about 50 CPU-seconds of burst credit (the docs say 5 s), refilled at the unused baseline (about 6% of a CPU) when idle.
- Today's busiest Fly region would average 3.4 req/s (14 at an assumed 4× peak), so a shared-cpu-1x has 1.5-6× headroom on its baseline alone (derived).

**Suspend and stop** (the ams VersaTiles Machine, from the Helsinki box, a new TLS connection per request, 3 repeats; S3-app Machines in brackets):
- Suspending takes 0.85 s (1.3-4.1 s). **The first request after a suspend takes 523-594 ms**, against 106 ms while running: **+0.42-0.49 s**. flyd itself resumes in 0.28-0.34 s.
- **Caches survive.** Warm and never-requested tiles right after a resume match a running Machine, and 12 of 12 S3-app repeats kept the same process and directory cache.
- **After a stop, the first request takes 20.6-20.9 s**, almost all of it VersaTiles reloading its directories at the 16 MiB/s volume cap. Warm tiles are then 9 ms slower (page cache lost). go-pmtiles Machines without a volume came back in 2.2-5.5 s.
- **The proxy suspends an idle Machine after about 6 min** (6.3 min; 5.4-7.5 on the S3 app). With two Machines of the app in one region, it kept one running. Proxy-triggered resumes on the S3 app were slower than API resumes (0.87-6.8 s against 0.43-2.4 s), for reasons not identified.
- **A request wakes the nearest suspended Machine** rather than going to a running Machine in another region. With ams suspended, an unpinned European request woke it (787 ms first byte) instead of using the running iad Machine; the S3 app did the same 3 times out of 3.

**Tigris-backed serving without volumes (measured live; design options in §4.D):**

| Variant | Where | Result |
|---|---|---|
| go-pmtiles reading the one 139 GB object (S3-direct) | Fly ams / iad / sin, in the Machine, 5 req/s | cold p50 163 / 543 / 864 ms, warm 129 / 466 / 746 ms; 1.5-1.7 GETs per tile. **Tigris did not cache the big object outside fra**: keep-alive range GETs took 32 / 113 / 182 ms while `Server-Timing` said 1-12 ms. Worldwide: 246 ms warm, 612 never-requested |
| 1 MB chunk objects, read from the same Machines | Fly ams / iad / sin | 1st-2nd GET 130-1,630 ms; **3rd and later 30-42 / 20-110 / 15-24 ms**. Chunks are cached in the reading region after about two reads |
| TigrisFS + go-pmtiles | Fly ams | cold 341, warm 130, repeat 7 ms p50; the disk cache is not reused after a TigrisFS restart |
| TigrisFS + VersaTiles | box; Fly ams | box: cold p50 121 ms at 5 req/s, but **only 19-25 tiles/s** when offered 50 (queueing p50 17-22 s). On a 1 GB Machine it left 40 MB of RAM free |
| go-pmtiles over S3 from the box (Helsinki, bucket in fra) | box | cold p50 122-171 ms; warm 54-58 ms with every directory cached (1 GB cache, 1.55 GB RSS) |

**The Tigris endpoint test (X7), from the Helsinki box** (production `tiles-geo`, read-only; 16 KiB ranges; n = 60-120 per cell over two rounds):

| Production chunk read | t3.storage.dev (served from fra) | fly.storage.tigris.dev (served from ams) |
|---|---|---|
| Cold, fresh connection, total p50 / p90 | **251** / 630 | 396 / 642 |
| Warm, fresh connection, total p50 | **89** | 185 |
| Warm, keep-alive TTFB p50 / p90 | **34** / 146 | 71 / 196 |
| Connection setup p50 (500 connections) | TCP 21 + TLS 31 | TCP 30 + TLS 40 |
| Idle keep-alive closed after | 60-120 s | 120-300 s |

- The rule "switch to t3 if it cuts the fresh-connection p50 by ≥ 50 ms in the EU" is met from Helsinki. Cold placement is 60-80% of a cold t3 read.
- Unexplained: on t3, cold first reads on a reused connection were slower than on fresh ones (TTFB p50 312 vs 198 ms, in both rounds).
- On the test bucket (fra only), the first read of a chunk cached the whole 1 MB object: the adjacent range then took 33-37 ms. Ranges of the single 139 GB object never reported `X-Tigris-Read-Source: cache`.

**Cost of the tests.** About 45 Machine-hours (43.4 on the eight shared-cpu-1x serving Machines), 21.3 volume-hours at 150 GB, a 140.7 GB test bucket for 6.3 hours and about 100k Tigris requests: **about $1.42**, or **at most $6.43** if Fly bills the 3 × 139 GB of fork hydration as cross-region transfer (undocumented). Both test apps, the probe app, all 4 volumes and the bucket were destroyed by 19:52Z and verified gone; `tiles-geo` and the other apps and buckets were untouched. Fly's tooling could not confirm that the test bucket's access key is revoked, so revoke it, and the org token and production read key used for the tests, in the consoles.

**Caveats.** Every live figure is one run per setting on one day. During the latency run, caches on V and S were partly warm from other tests: their tiles were excluded, but not their directories. Regional medians rest on 2-14 probes, and anycast routing changed between tests for some probes. Hydration overlapped load and suspend tests on the source Machine. Globalping truncates bodies, so every figure is a first-byte time. Raw data: `live/live-results.json` and `live/{fly-vol,fly-s3,box-tigris,latency,x7-regions}/`.

### 2.6 Is tiles-geo replicated to every region? (v2.3)

The maintainer asked: "the bucket has data replicated to all regions, is that not true?" **The configuration says yes. Reads show the data is not hot in each region.**

**What the bucket reports.** `GetBucketLocation` returns 12 regions: fra, gru, hkg, iad, jnb, lhr, ord, sin, syd, sjc, nrt and ams. The 20250818 chunk objects carry `X-Tigris-Regions: fra,gru,hkg,iad,jnb,lhr,nrt,ord,sin,syd,sjc` (11; no ams).

**What reads show** (2026-11-02, about 18:31Z; one curl Machine per region on Fly; 5-6 random production chunks per endpoint; 16 KiB signed range GETs on a new connection each; time = TTFB minus TLS, p50; `live/x7-regions/`):

| Reader region | Endpoint | First read: served from, p50, Tigris cache | Repeats p50 (2nd / 3rd), served from |
|---|---|---|---|
| lhr | Fly | lhr, 184 ms, miss | 114 / 120, lhr |
| lhr | t3 | lhr, 164 ms, miss | 39 / 19, lhr |
| iad | Fly | iad, 310 ms, miss | 182 / 91, iad |
| iad | t3 | iad, 273 ms, miss | 220 / 99, iad1 |
| gru | Fly | gru, 224 ms, miss | 20 / 18, gru |
| gru | t3 | **iad**, 337 ms, miss | 275 / 251, iad1 (t3 has no gru presence) |
| nrt | Fly | **jnb**, 596 ms, miss | 215 / 97, nrt |
| nrt | t3 | **jnb**, 396 ms, miss | 247 / 9, nrt |
| sin | Fly | **jnb**, 671 ms, miss | 293 / 326, sin |
| sin | t3 | **jnb**, 529 ms, miss | 292 / 304, sin |
| syd | Fly | **jnb**, 661 ms, miss | 132 / 98, syd |
| syd | t3 | **sjc**, 364 ms, miss | 287 / 264, sjc (t3 has no syd presence) |

**Findings.**
- **Every region's first read of a chunk was a Tigris cache miss (160-670 ms)**, including regions the objects list. In Tokyo and Singapore (both endpoints) and Sydney (Fly endpoint; t3 went to sjc) the first read was **served from Johannesburg**.
- Repeats were served from the reader's region where the endpoint has one (t3 in gru and syd stayed on iad and sjc), but were noisy: 9-330 ms, and Singapore stayed near 300 ms.
- Other tests agree. All 120 first reads of production chunks from Helsinki were misses in fra and in ams (§2.5). The v2 forced-miss runs saw first reads of 130-470 ms from LHR/WAW and 0.3-1.3 s elsewhere (§2.2).
- So a listed region does not mean a hot local copy of every chunk. Possible reasons, none verified: placement is on demand and the list names eligible regions; regional copies are evicted after some time; or the header describes metadata rather than data. Tigris documents neither the per-region cache size nor its eviction.
- **t3 against the Fly endpoint by region.** t3 was better on first reads in lhr, iad, nrt and sin, and much better on lhr repeats (nrt repeats were mixed: 247 / 9 against 215 / 97 ms). It was worse where it has no presence: gru (served from iad, 251-337 ms against 18-224 on the Fly endpoint) and syd repeats (served from sjc, 264-287 ms against 98-132).
- A production Tiles Deploy run wrote to `tiles-geo` from 18:27 to 18:44Z, overlapping this test. Whether that load affected it is unknown.

**What to do.**
1. **Ask Tigris support (X14)**, with these examples: bucket `tiles-geo`, keys `20250818/chunk_<N>.pmtiles`, 2026-11-02 about 18:31Z, reads from Fly Machines in nrt, sin and syd that returned `X-Tigris-Served-From: jnb` and a cache miss although `X-Tigris-Regions` lists the reader's region. Ask what `X-Tigris-Regions` guarantees, why first reads come from jnb, how long regional copies are kept and how they are evicted, and whether a placement setting or a prefetch request (TigrisFS source sends an undocumented `X-Tigris-Prefetch` header) makes a new deploy hot in every region.
2. **Test a per-region pre-warm (X13).** After a deploy, read every chunk once from a Machine in each region: about 140k chunks × 12 regions ≈ 1.7M GETs ≈ **$0.85 per deploy**. Then read random chunks 1 hour, 1 day and 7 days later to learn retention. This helps today's Worker as well as any Tigris-backed design.
3. **Choose the endpoint per region.** t3 has no presence in gru, syd or jnb. Until a Worker-side test (X12) says otherwise, use t3 for Europe, North America and Asia, and the Fly endpoint for South America, Oceania and Africa.
4. **Drop A10** (eager Dual-Region placement). The bucket already lists 11-12 regions and first reads still miss everywhere tested.
5. Local copies avoid the problem: a Fly volume served never-requested tiles in 4.3-5.0 ms in every region once hydrated (§2.5).

---

## 3. Master comparison

**Reading the table.**
- **Latency** is traffic-weighted, under today's Cloudflare colo routing, on a warm client connection.
  - Every figure is a **modelled lower bound** from the same formulas (§5), unless marked *m* (measured) or *c* (calibrated).
  - Weights are request shares; per-region miss weights are a *pending production split*.
  - Local-PoP routing (A7 fixed) lowers Cloudflare hits by about 8 ms (§5.2).
- **Miss columns:**
  - "tier" is a lower-tier miss answered by an upper tier.
  - "full" is a miss at every tier, given as warm–cold. Warm means every server-side connection is already open. Cold means every server-side leg opens TCP + TLS.
  - "(x)" is the cold figure when only connections that leave Cloudflare are cold.
  - "blended" = 0.5 × tier + 0.5 × full. u = 0.5 for every tiered option (modelled 0.43, range 0-0.7; §7).
- **Calibrated column** (A options only): measured anchors for today run from 300 ms (warm-isolate miss) to 488 ms (single-shot miss), and hits from 24 to 35 ms.
- **Costs** are monthly USD at R / M / L in the canonical view. Δ is against today's $15 / $134 / $514. Break-even is the volume above which the option is cheaper than today. S and XL are in §4.
- **Ops and migration** use the basis in §7, which is an assumption. 12-month total cost of ownership is also in §7.
- **v2.2 rows** (the "-s" rows and the Fly rows) use the small-VM model (§5.7). It differs from the v2.1 rows in two ways. Server time is the **measured planet p50 (5.1 ms, VersaTiles)** instead of an assumed 3 ms, so v2.1's B/C full misses are 2-7 ms optimistic by comparison. And Fly hops use **measured Fly anycast edges** plus a measured 7 ms proxy overhead. v2.1 rows on big nodes are kept so the two can be compared; their cost cells point to the small-node equivalent.
- **v2.3 rows** are marked *m (live)*: measured on real Fly.io Machines and Tigris buckets from the same 48 probes as today, at the same time (§2.5). Their latency cells are traffic-weighted TTFB on a new connection (warm / never-requested), which is a different metric from the model columns; compare them with today's figures from the same run (warm 54, miss 459, blended 159 ms). Fly behind Cloudflare was not tested live and stays modelled.
- **Hit ratio.** Every hit-dependent figure assumes the 73.8% no-deploy steady state. Monthly deploys with the key in the URL add about 12-20 ms to today's mean per tile and +2-4 ms warm (up to about 10 cold) to B's (§7).
- **(f)** Hit figures for B5-B7 are not like for like:
  - Each provider was measured on a different object and zone: our 76 KB tile on the Worker, OpenFreeMap's 337 KB tile on Cloudflare, OSM's 134 KB Shortbread tile on Fastly, a 22 KB font on Bunny, and a 17 KB AWS logo on CloudFront.
  - Per-probe noise is the same size as the differences. The same Bunny object an hour apart moved a median 1 ms, but 59-186 ms on 3 probes whose PoP changed.
  - Treat hit differences of 5-10 ms between CDNs as noise until X9 has run.

| ID | Option | Hit | Miss: tier · full warm–cold · blended | Mean per tile | Calibrated: full miss · mean | $/mo R · M · L | Δ R · L; break-even | Ops h/mo · migration days | Regions | Key risk |
|---|---|---|---|---|---|---|---|---|---|---|
| **Stay on Cloudflare (serverless)** | | | | | | | | | | |
| A0 | **Today**: Worker + Cache API + D1 + Tigris | 24 (*m* 24-35) | — · 86–206 · = full | 40–72 | *m* 300–488 · 96–154; *m (live)* 2026-11-02: miss 459, warm 54, blended 159 | 15 · 134 · 514 | 0 · 0 | 1.5 · 0 | 348 CF cities; D1 6 replica regions; Tigris 8 regions + Fly POPs | 0.3-1.4 s misses outside the EU from the Tigris path |
| A1 | Code quick wins (SELECT 1, metadata, streaming, dir LRU, Vary, metrics sampling) | 24 | — · 78–171 · = full | 38–63 | *c* 292–466 · 94–148 | 15 · 132 · 503 | −0 · −10; cheaper at all volumes | 1.5 · 1–3 | as A0 | dashboards change if metrics are sampled |
| A1b | A1 + Tigris client on `t3.storage.dev`, built once per isolate | 24 | as A1 | 38–63 | *c* 202–456 · 71–146. Tigris endpoint test (X7, box): t3 vs Fly endpoint 251 vs 396 ms cold, 89 vs 185 warm (fresh connection) | 15 · 129 · 493 | −0 · −21; cheaper at all volumes | 1.5 · 1.5–3.5 | as A0 | gain inside the Worker unmeasured (X12); t3 has no gru, syd or jnb presence, so choose the endpoint per continent (§2.6) |
| A10 | ~~A1b + Tigris Dual-Region `fra,iad`~~ **Dropped in v2.3** | 24 | as A1 | 38–63 | was *c* 202–359 · 71–120 | 20 · 134 · 498 | +5 · −16 | 1.5 · 2–4.5 | Tigris fra + iad eager | the bucket already lists 11-12 regions, yet first reads miss in every region tested (§2.6); ask Tigris support (X14) instead |
| A2 | **Workers Cache** + A1 | 18 (*c* 18-22) | 38 · 104–234 · 71–155 | 32–54 | *c* 318–492 (tier 42, blended 180–267) · 61–86 | 14 · 105 · 394 | −1 · −119; cheaper at all volumes | 1.75 · 3–8 | lower tier 348 cities; upper tier regional or global (sources conflict, X1) | new product; topology unconfirmed (X1); no documented Terraform attribute; per-hit metrics lost |
| A2-gw | A2 + metrics gateway (uncached entrypoint → cached `ctx.exports`) | 23 | 43 · 109–239 · 76–160 | 37–59 | ≈ A2 + 5 | 18 · 185 · 715 | +3 · +201; never | 1.75 · 4–9 | as A2 | 2× billed requests; `Cf-Cache-Status` on `ctx.exports` unverified (X1) |
| A3 | A2 + loopback chunk/dir entrypoints, directories off D1 | 18 | 38 · 78–159 · 58–117 | 29–44 | n/m | 14 · 114 · 434 | −1 · −80; cheaper at all volumes | 2 · 8–15 | as A2 | loopback range slicing unverified; bundled z0-z12 directories decode to 146 MiB, so never decode them at startup |
| A4 | R2 chunks instead of Tigris (or the stock single-file Worker) | as A0 | unmeasured for the design that would ship (X8). Public-domain reads of the 139 GB multipart object: ~370 ms from London (*m*) | — | — | 12 · 121 · 472 | −3 · −41 | 1.5 · 3–6 | 1 R2 region per bucket | abandoned once ([#170](https://github.com/immich-app/geoshenanigans/pull/170)); single region for cold reads |
| A5 | z0-z8 as free Workers static assets | low zooms ~19-22 (*m*) | unchanged for z9+ | — | — | 14 · 115 · 437 (v1 model; if z0-z8 = 15% of requests; zoom mix pending) | −1 · −77 | — · 2–4 | global | assets become billable once Workers Cache is on, which conflicts with A2 |
| A6 | Tile bytes in D1 shards / KV / Durable Object SQLite (+A2) | as A2 | D1 ~60-75, KV ~70-110, DO ~20-60 (derived or unverified) | — | — | D1 216 · 291 · 527; KV 150 · 235 · 521; DO 340 · 419 · 670 (v1 model; + ingest up to $36 per deploy) | +135…+325 · +7…+156 | — · 2-6 weeks | D1 6 regions; DO up to 9 hints | latency untested for 40 KB rows; 28+ D1 databases |
| A7 | Fix the distant-colo routing (plan change) | affected users −30…−135 (*m*); weighted hit 24 → 16 today, 18 → 10 with A2/B | B3 Asia misses 146 / 144 → 69 / 91 | — | — | Pro +$20-25 or Business +$200-250 for the whole zone | — | low | CF | plan-pool link unverified; prior odds Pro ~30%, Business ~55-65%; size it from production first |
| A8 | Cache Reserve / Smart Placement without Workers Cache / Containers / Argo on A paths | no gain | no gain | — | — | CR ~$5-40 (est.); placement $0; Containers infeasible | ~0 | — | n/a | none of these fixes the slow origin path |
| A9 | Upper-tier control: Durable Object regional shields (P4). P1-P3 are rows B1-P1, A2-gw and B1-gw | n/a | Worker→DO hop instead of D1 (unmeasured) | — | — | +0.2 · +5-17 · +20-45 | — | — · 1-2 weeks (est.) | DO in ~6 hint regions (~11% of PoPs) | worth it only if D1's tail survives A1/A3; tile bytes still come from Tigris or the origin |
| **CDN + VM origin** | | | | | | | | | | |
| B1 | **Thin Worker + Workers Cache + 1 EU NVMe origin**, D1 + Tigris fallback | 18 | 38 · 116–296 (260) · 77–186 | 33–62 | — | 77 · 149 · 380 (+7 while fallback data is kept) | +62 · −133; 326M | 3.5 · 10–17 | CF edge; origin FSN or HEL | full misses fetch the origin from the regional upper tier over the full distance; Worker→origin pooling undocumented |
| B1-P1 | B1 + placement hint next to the origin | 18 | 38 · 119–305 (125) · 78–190 | 34–63 | — | as B1 | as B1 | 3.5 · 10–17 | as B1 | extra hop on full misses; the docs say only that "Future updates may bring these choices closer together" |
| B1-z | B1 with the origin fetch through a proxied hostname (zone Smart Tiered Cache) | 18 | 38 · 123–309 (129) · 80–192 | 34–63 | — | as B1 | as B1 | 3.5 · 10–17 | as B1 | two cache layers; zone tiering on Worker subrequests documented, Argo not |
| B1-gw | B1 + metrics gateway (cold 130 with P3 placement) | 23 | 43 · 121–301 (265) · 82–191 | 38–67 | — | 81 · 229 · 700 | +66 · +187; never | 3.5 · 11–18 | as B1 | 2× billed requests; P3 needs the cached entrypoint as its own Worker |
| B1-fetch | Worker on every request, zone cache, no Workers Cache | 24 | 98 · 104–254 (110) · 101–248 | 44–82 | — | 77 · 157 · 410 | +62 · −103; 384M | 3.5 · 8–12 | as B1 | Worker stays on the hit path; tier hits go to the EU upper tier |
| B2 | **CF zone proxy + Smart Tiered Cache + 1 EU origin**, no Worker | 18 (10 local) | 92 · 98–248 (104) · 95–242 | 38–76 | — | 71 · 71 · 71 (AX41; B2-s on a small node) | +56 · −442; 125M | 2.75 · 8–12 | as B1 | no HA (a dead origin fails misses); loses Worker metrics; "large files" ToS clause (low risk) |
| B2-s | **B2 on a small node** (v2.2): Hetzner Cloud CX43, 8 vCPU / 16 GB / 160 GB, one planet copy, fresh node or pull per deploy | 18 (10 local) | 92 · 100–250 (106) · ≈ B2 | 38–76 | — | **21 · 21 · 21** | +6 · −493; **26M** | 2.75 · 8–12 | EU (NBG, FSN, HEL) | no RAID1 or guaranteed port; 160 GB holds one copy for ~1.5-2 years; Cloud stock unverified |
| B2-HA | B2 + a second origin at another vendor (Scaleway) + failover LB | as B2 | as B2 | 38–76 | — | 126 (121–152) flat; ~49 with 2× CX43 + $10 LB (one vendor) | +111 · −388; 233M | 4.75 · 10–16 | EU, 2 vendors | LB price list unpublished |
| B2a | B2 + Argo Smart Routing | as B2 | as B2 (gain on the short upper-tier→origin leg expected small) | 38–76 | — | 79 · 135 · 312 | +64 · −202; 252M | 2.75 · 8.5–12.5 | as B2 | per-GB rate last published in 2017 |
| B3 | CF + 3 origins (EU, US-East, Singapore), LB with one pool per region | 18 | 54 · 60–133 (66) · 57–127 | 28–46 | — | 286 flat (276–311) on big nodes; **126 as B3-s** | +271 · −227; 550M | 6.5 · 10–14 | EU, US-E, SG | Asia gain needs A7; LB price unpublished; SG 320 GB disk holds 2 copies (~3 years) |
| B3-s | **B3 on small nodes** (v2.2): CX43 (EU) + 2 × Vultr vc2-4c-8gb (US-East, SG), 160 GB each, LB pool per region | 18 | ≈ B3 · 62–135 (68) | 28–46 | — | **126 flat** | +111 · −388; 233M | 4.75 · 8–13 | EU, US-E, SG | as B3; two vendors; one copy per node |
| B3-w | B3 with Worker steering | 18 | 38 · 78–182 (145) · 58–128 | 28–46 | — | 267 · 339 · 570 | +252 · +57; 1,287M | 7 · 12–19 | as B3 | continent steering mis-routes detoured users |
| B4 | CF + 6 origins (EU, US-E, US-W, SAO, SG/TYO, SYD), LB | 18 | 44 · 50–103 (56) · 47–97 | 26–39 | — | 555 flat (545–580) on big nodes; **281 as B4-s** | +540 · +42; 1,082M | 11 · 12–16 | 6 regions | Vultr 320 GB disks hold 2 copies for ~1.3 years; only 9% of traffic is in LatAm/OC/US-W |
| B4-s | **B4 on small nodes** (v2.2): CX43 + 5 × Vultr 160 GB, LB | 18 | ≈ B4 · 51–102 (57) | 26–39 | — | **281 flat** | +266 · −233; ~540M | not re-estimated (B4: 11 · 12–16) | 6 regions | as B4 |
| B4-w | B4 with Worker steering | 18 | 38 · 68–151 (115) · 53–113 | 27–43 | — | 521 · 593 · 824 | +506 · +311; 2,572M | 11.5 · 14–21 | as B4 | as B4 |
| B5 | Bunny + EU origin shield + VM origin: Standard / Volume | Std 13, Vol 32 (f) | Std 90 · 96–248 · 93–243; Vol 94 · 100–229 · 99–227 | Std 30–64; Vol 42–62 | — | Std 75 · 134 · 324; Vol 72 · 94 · 161 | Std +60 · −189 (250M); Vol +57 · −352 (152M) | 2.5 · 10–15 | 119 PoPs (Std) / 10 (Vol) | DNS and Worker move off CF; Volume adds ~18 ms to EU hits and ~150 ms in Oceania |
| B6 | CloudFront + VM origin | London 14.6 (*m*, f) | ≈ B2 (one EU origin); edge hit ~68-80%, ~16% origin miss | — | — | PAYG ~76 · ~701 · ~2,880 (~3,040 at a 72% edge hit); flat-rate 271 · 1,071 · 2,321 | PAYG +61 · flat +1,807 | — · 2-3 weeks | 750+ PoPs | expensive from M up; overuse is "served from fewer or more distant edges" |
| B7 | Fastly + VM origin (ideally through Fast Forward) | London 12.0 (*m*, f) | ≈ B2; edge hit ~79%; 13% origin miss with shielding | — | — | ~106 · ~1,100 · ~4,209 (79% edge hit); $71 with Fast Forward | +91 · +3,695 | — · 2-3 weeks | ~114 PoPs | list price; Fast Forward acceptance unknown |
| B8 | Other CDNs (Gcore, KeyCDN, CDN77, Google Cloud CDN, Azure Front Door) | n/m | Gcore ≈ B2 (edge hit ~77%, 23% origin miss without paid shielding); others n/m | — | — | Gcore 71 · 112 · 268; KeyCDN ~82 · 293 · 960; CDN77 1,061 flat; GCP — · 631 · 2,257; Azure — · 761 · 2,724 | +56…+1,046 · −246…+2,210 | — · 2-3 weeks | varies | little edge logic; CDN fees at a 90% edge hit (cdn-layer track) |
| **VM-only, no CDN** | | | | | | | | | | |
| C1 | 1 EU node serving clients directly | 85 per request; first load 417 | no hit/miss split | 85 | — | 71 flat through XL (bandwidth checked); 26 as C1-s | +56 · −442; 125M | 3.75 · 5–10 | EU only | no DDoS shield; 90-330 ms RTT outside the EU; single point of failure |
| C3 | 3 nodes, GeoDNS | 42; first load 206 | — | 42 | — | 271 (N=1) to ~530 (N+1) flat on big nodes; 111 / 209 as C3-s | +256 · −242; 520M | 16 · 15–20 | EU, US-E, SG | L7 DDoS; needs N+1; DNS-01 TLS; 24/7 pages |
| C6 | 6-10 nodes, GeoDNS | 34 (6 nodes); first load 163 | — | 34 | — | 525 (6) / ~830 (10) flat on big nodes; 251 / 416 as C6-s | +510 · +12; 1,023M | 23 · 18–25 | up to 10 metros | ops cost; still sparser than CF PoPs |
| C1-s · C3-s · C6-s | **C1/C3/C6 on small nodes** (v2.2): CX43 (EU) + Vultr 160 GB, GeoDNS, TLS on the VM | 87 · 44 · 36 per request; first load 423 · 212 · 168 | — | 87 · 44 · 36 | — | 26 · 111 (209 with N+1) · 251 (416 for 10 nodes); flat to L, Vultr pooled overage at XL (C3-s 241) | C3-s N+1: +194 · −305; ~400M | C3-s N+1: 6.25 · 9–15 | as C1/C3/C6 | as C1/C3/C6: TLS, DDoS and N+1 are still ours |
| **Edge platforms** (v2.2: Fly rows re-sized with shared-cpu-1x 1 GB Machines and one 150 GB volume each; §4.D) | | | | | | | | | | |
| Fly-cf (was D1) | **CF zone proxy → small Fly Machines in 3 regions** (ams, iad, sin), Tiered Cache **off** | 18 (10 local) | — · 61–98 (98) · = full | **30–40** | — | **98 · 125 · 222** | +83 · −292; ~230M | 2 · 7–12.5 | CF edge; 3 of 17 Fly regions | Smart Tiered Cache must stay off (anycast origin); Fly egress on 26% of bytes; no SLA below Fly Enterprise; Asian misses need A7; not tested live (X11); the same model was 4-9 ms optimistic for Fly-direct |
| Fly-cf N=6 | same, + sjc, nrt, syd | 18 | — · 51–79 (79) · = full | 27–35 | — | 197 · 225 · 320 | +182 · −194 | 2 · 7–12.5 | 6 Fly regions | as Fly-cf |
| Fly-cf-stc | Fly-cf with Smart Tiered Cache **on**: one upper tier beside ams, so only ams is useful (2 Machines there) | 18 | ≈ B2 · 99–227 (105) | 38–72 | — | 64 · 77 · 124 | +49 · −390; ~120M | 2 · 7–12.5 | CF edge; ams | ≈ B2 in speed: extra Fly regions add nothing |
| Fly-direct (was D2) | **Clients → Fly anycast, no CDN**; N = 3 / 6 / 10 / 17 regions | 54 / 46 / 38 / 34 per request (cold +0–19); first load 205–265 / 182–213 / 154–174 / 148 (model; measured N=4 below) | no hit/miss split | 54 / 46 / 38 / 34 | — | N=3: 102 · 217 · 625; N=6: 202 · 315 · 703; N=17: 562 · 666 · 1,043 | N=3: +87 · +111; never | 1.75 · 6.5–11.5 | 3-17 Fly regions | egress grows linearly ($427 at L, $1,282 at XL); no shared cache or L7 shield; anycast detours (Mumbai→ams, Jakarta→lax); the model put its first load behind CF-fronted fleets, but measured (N=4) it equals today's all-hit first load |
| **Fly-direct N=4 (measured live, v2.3)** | **Clients → Fly anycast, no CDN; VersaTiles on shared-cpu-1x 1 GB + one 150 GB volume in ams, iad, sin, syd** | *m (live)* 51 warm (54 on 11-region weights) | *m (live)* never-requested 55 (59); no miss tail | *m (live)* 53 blended | — | ~128-136 · ~250 · ~660 (derived from the N=3 rows + one region) | +113-121 · +146; never | 1.75 · 6.5–11.5 | 4 of 17 Fly regions | no L7 shield (a flood is an egress bill); egress linear ($427 at L); TCP+TLS 54 vs CF 36 ms; NA-West 104-118 and LatAm ~220 ms per tile without a Machine there; first map load *m (live)* 184-195 (today 195-590) |
| Fly-wkr | Thin Worker + Workers Cache, misses steered with `fly-prefer-region`, N=6 | 18 | ~38 · 74–139 (102) | 28–42 | — | 203 · 288 · 563 | +188 · +49; never | 3 · 10–17.5 | CF edge; 6 Fly regions | Workers fees as B1; needs the X1 spike; keeps per-request metrics and a D1 + Tigris fallback |
| Fly-s3 (measured live, v2.3) | go-pmtiles on Fly Machines reading the planet as **one 139 GB Tigris object**, no volume (ams, iad, sin) | *m (live)* 246 warm | *m (live)* never-requested 612 | *m (live)* 329 blended | — | ~45-50 for 4 regions (derived) | — | — | Tigris (data in fra) | **not recommended**: Tigris did not cache the big object outside fra (iad / sin in-Machine p50 466-864 ms) |
| Fly-tigrisfs (measured live, v2.3) | TigrisFS mount + go-pmtiles or VersaTiles, no volume (ams) | in-Machine cold 341, warm 130, repeat 7 | box: 19-33 tiles/s cap on cold tiles | — | — | ~45-55 for 4 regions (derived) | — | — | Tigris | **not recommended**: 5 MiB read-ahead per open, page cache off, unbounded disk cache, read-only keys mount read-write (#31) |
| Fly-chunkcache (modelled, v2.3) | Server with **every directory in RAM**, today's 1 MB chunk objects, a local chunk LRU or the TAG sidecar; no planet volume | ≈ Fly-direct N=4 + 3-25 ms once pre-warmed (modelled from the simulation and live chunk reads) | a chunk cold in its Tigris region 130-670 ms until pre-warmed | — | — | **~40-60** for 4 regions | about −$70-90 vs Fly-direct N=4 | — · not sized (code to write, or TAG tuning) | Tigris + 4 Fly regions | no implementation; needs a per-region pre-warm (X13); Tigris regional caching undocumented (X14); later cost cut, not the first move |
| D3 | Bunny-only: Storage (Edge SSD ×5) + Edge Script lookup, no VMs: Standard / Volume | Std 13, Vol 32 | Std 75–167; Vol 72–125 (no tier; = full) | Std 27–49; Vol 38–47 | — | Std 33 · 115 · 376; Vol 30 · 62 · 166 | Std +18 · −138 (125M); Vol +15 · −348 (55M) | 2 · 10–20 | 119 PoPs; storage 15 regions | no implementation exists; 500 RPS per storage zone; one EU replica in the 5-replica set |
| **Tile format and origin format** | | | | | | | | | | |
| E1 | Origin software and format: single `.pmtiles` with VersaTiles/go-pmtiles (vs nginx tile tree, MBTiles + Martin) | **planet** server time p50 / p99 at 5 req/s: VersaTiles 5.1 / 9.1, go-pmtiles default 10.2 / 17.6 (*m*, emulated small Fly shapes); extract 0.1-0.5 | same | — | — | $0 extra (part of B/C/D) | 0 | part of B | any | VersaTiles needs ≥ 1 GB RAM (OOM-killed at 256 MB); go-pmtiles' default 64 MB directory cache misses on 82% of the synthetic mix (3.4-4 ms CPU each) |
| E2 | Rejected origin designs: per-tile objects, dynamic PostGIS | — | objects ≈ today −15-25; PostGIS tens to hundreds of ms per low-zoom tile (unverified) | — | — | objects $614-683 in PUTs per full deploy; PostGIS ≈ $185-300+ per region | — | high | — | per-tile objects still need a duplicate lookup; PostGIS gains nothing for read-only data |
| E3 | Client bytes: MLT, brotli/zstd, z14 max zoom (needs our own Planetiler build) | MLT −5…−17% bytes; br/zstd −4…−8%; z14 ~2.5× fewer z15+ requests (est.) | — | — | — | build $0 (existing box) to EUR 159-259/month plus a EUR 129 setup fee; dual tile sets double origin storage | — | weeks | n/a | old clients break unless served on a new `/v2` URL |
| **Hosted APIs** | | | | | | | | | | |
| F1 | OpenFreeMap public instance | 23-26 (*m*) | first pass 26 / 56 p50/p90 (59/60 already hits) | — | — | 0 · 0 · 0 | −15 · −514 | medium (restyle) | global (CF) | no SLA; OpenMapTiles schema |
| F2 | Protomaps API; VersaTiles public; OSMF | VersaTiles 41-48 (no CDN); others n/m | — | — | — | Protomaps: contract; others $0 | unknown | low-high | varies | pricing or capacity unknown |
| F3 | Metered APIs (Stadia, CARTO, MapTiler, Mapbox) | n/m | — | — | — | Stadia 250 · 3,625 · 14,875; CARTO 1,500 · contact · contact; MapTiler ~2,010 · 37,455 · 149,955; Mapbox ~2,305 · 37,750 · 150,250 | +235…+2,290 · +14k… | high | global | leaked keys mean unbounded cost; schema change |
| **Client-side** | | | | | | | | | | |
| G1 | Client and protocol wins (inline tiles in style, preconnect, mobile ambient cache) | −1 serial RTT on map open (~20-70 ms) | — | — | — | 0 | 0 | days + app releases | all | slow rollout to self-hosted installs |
| G2 | Client-side `pmtiles://` straight from storage | +2-3 serial RTTs on first view | — | — | — | storage + CDN | — | client releases | any | breaks old apps; the 139 GB file is not cacheable on CF |

---

## 4. Option details

### 4.A Stay on Cloudflare (serverless)

#### A0. Today

Covered in §2. Cost S/M/L/XL: $33 / $134 / $514 / $1,525 (R $15).
- Deploys are manual, about every 6 months. The live key `20250818` has been served since about 2025-09-04 ([#170](https://github.com/immich-app/geoshenanigans/pull/170)).
- Draft [#245](https://github.com/immich-app/geoshenanigans/pull/245) automates the deploy. It picks the newest 4.x build, publishes chunks to Tigris and rows to D1, verifies 20k sampled tiles byte for byte, holds a deploy lock, and cleans up while keeping the served, live and previous keys.

#### A1. Code quick wins, including A1b (do these whatever else is chosen)

**The changes:**
1. Remove the per-request D1 `SELECT 1` (`repository.ts:22`, constructed at `index.ts:89` before `cache.match`), or move it to the miss path.
2. Bake the TileJSON metadata into the bundle or an env var. This removes the serial D1 query that runs whenever the isolate's `memCache` is empty (7-816 ms measured).
3. Stream with `body.tee()` instead of `await response.arrayBuffer()` (`index.ts:94`).
4. **A1b: fix the Tigris client.**
   - Replace `new S3Client` per miss and the 762 KB SDK with a module-scope client (for example `aws4fetch`).
   - Point it at **`https://t3.storage.dev`** instead of `https://fly.storage.tigris.dev` (`repository.ts:74`). Fly calls t3 "the canonical Tigris S3 endpoint … We recommend using it for new code", and says both endpoints "reach the same Tigris service and the same buckets" ([Fly docs](https://docs.fly.io/tigris)). Tigris staff recommend t3 for anything hosted outside Fly ([Fly community](https://community.fly.io/t/tigris-series-a-implications/26591)), and Tigris's [AWS CLI guide](https://www.tigrisdata.com/docs/sdks/s3/aws-cli/) lists only t3.
   - Keep signed reads. v1 suggested a public-read bucket instead. That would expose unbounded Class B GET charges ($0.50/M, [Tigris pricing](https://www.tigrisdata.com/pricing.md)) to anyone who finds the bucket, and it bypasses Cloudflare.
   - Measured on a public Tigris object, 4 KiB ranges, 2026-11-02:

     | Client | Endpoint | Served from | Keep-alive p50 | First request on a new connection p50 (with TCP+TLS) |
     |---|---|---|---|---|
     | London FTTP | fly.storage.tigris.dev | AMS | 51 ms | 120 ms (165) |
     | London FTTP | t3.storage.dev | LHR | 18 ms | 30 ms (62) |
     | Helsinki box | fly.storage.tigris.dev | AMS | 61 ms | 92 ms (162) |
     | Helsinki box | t3.storage.dev | FRA | 50 ms | 59 ms (115) |

   - **v2.3, the Tigris endpoint test (X7) on production chunks.** From the Helsinki box, fresh-connection reads were **251 vs 396 ms cold and 89 vs 185 ms warm** (t3 vs the Fly endpoint), and keep-alive warm reads 34 vs 71 ms. That meets the "≥ 50 ms better in the EU" rule. From Fly Machines in six regions (§2.6), t3 was better or equal on first reads in lhr, iad, nrt and sin, but **worse where it has no presence**: São Paulo was served from iad (251-337 ms against 18-224 on the Fly endpoint) and Sydney repeats from sjc (264-287 ms against 98-132).
   - **So choose the endpoint per continent** with `request.cf.continent`: t3 for Europe, North America and Asia; the Fly endpoint for South America, Oceania and Africa (t3 has no gru, syd or jnb region). Keep one client per endpoint at module scope. t3 closes idle connections after 60-120 s, so most misses will still open a new connection.
   - The effect inside the Worker is still unmeasured; a Worker-side endpoint test (X12) settles it. Cloudflare subrequests resolve t3 through Cloudflare's resolver, so which Tigris region they reach is unverified. Every region's first read of a chunk is a Tigris miss today (§2.6), whichever endpoint is used.
5. Add an in-isolate LRU of decoded directory rows.
6. Drop `Vary: Origin` (`index.ts:184`; ACAO is `*`) and normalise or drop `accept-encoding`.
7. Sample hit metrics (1-5%), drop the `apexDomain`/`asOrg` tags, or move to Workers Analytics Engine (10M data points included, then $0.25/M, [pricing](https://developers.cloudflare.com/analytics/analytics-engine/pricing/)). **Run the Appendix C queries first**, because `asOrg` and per-hit points are what measure the distant-colo share and per-colo hit ratios. Consider adding a `country` tag.
8. Optional: cache 404s, add an `OPTIONS` handler, and send a backup Tigris GET after about 150 ms (hedging, about +10% GETs) to cut the p99.

**Effect (calibrated, traffic-weighted):**
- A1: full miss **292-466 ms**. That is today's 300-488 minus the cold-isolate D1 query (14 ms weighted) and an 8 ms lighter miss path.
- A1b: **202-456 ms**. That is A1 minus 10-90 ms per miss, taken from the public-object deltas; the Worker path is unmeasured (X12). Mean per tile 71-146 ms against 96-154 today. The v2.3 box test found bigger per-read gains in Europe (96-145 ms on fresh connections), but the calibrated range is kept until the Worker-side test.
- Hits unchanged. D1 latency unchanged.
- Cost ≈ today minus some CPU: L $503 (A1), $493 (A1b).

**Effort and risk:** 1-3 days through the existing Terraform deploy, plus about half a day for A1b. Sampling metrics changes the alerts (the per-apex RPS alert). Low risk.

#### A2. Workers Cache instead of `caches.default` (recommended after the X1 spike)

**How it works.**
- [Workers Cache](https://developers.cloudflare.com/workers/cache/), launched [2026-07-06](https://blog.cloudflare.com/workers-cache/) on all plans, puts a read-through cache *in front of* a Worker entrypoint.
- Enable it with `[cache] enabled = true`, which "Requires Wrangler 4.69.0 or above". `cross_version_cache = true` "requires Wrangler 4.107.0 or above" ([configuration](https://developers.cloudflare.com/workers/cache/configuration/)).
- It is driven by `Cache-Control`, `Cache-Tag` and `Vary`. `Vary` values are honoured verbatim, with no allowlist.
- Hits do not run the Worker and bill no CPU, but **are billed at the standard request rate**. There is no separate SKU or storage fee.
- Concurrent misses are collapsed "per cache key, per data center".
- The key is entrypoint + path + query + Worker version (by default) + `ctx.props`, **not host** and not ordinary headers ([cache keys](https://developers.cloudflare.com/workers/cache/cache-keys/)).
- Purges work by tag, prefix or everything, scoped to the entrypoint, "always" at Free-tier limits. There is no pre-warm API ([limitations](https://developers.cloudflare.com/workers/cache/limitations/)).
- "No zone configuration for caching applies to Workers Caching", so Cache Rules, Cache Reserve and Argo do not apply ([Workers Cache](https://developers.cloudflare.com/workers/cache/)).

**Topology: unresolved, regional pools or one global tier per key (X1 decides).**
- *Regional reading.* The [blog](https://blog.cloudflare.com/workers-cache/) says "Workers Cache is regionally tiered by default". The Workers Cache docs say it uses "a generic tiered cache topology" ([limitations](https://developers.cloudflare.com/workers/cache/limitations/)). Cloudflare's Tiered Cache docs (updated 2026-09-29) say "Generic Global Tiered Cache organizes Cloudflare's network into regional upper-tier pools", and that it "uses a consistent hash of the cache key to select an upper-tier data center from the relevant regional pool" ([tiered cache](https://developers.cloudflare.com/cache/how-to/tiered-cache/)). On this reading there is one upper tier per key **per region**, and a London lower-tier miss stays in Europe.
- *Global reading.* The same blog describes "An upper tier that aggregates fills across the whole network". The [Workers Cache](https://developers.cloudflare.com/workers/cache/) page says "The upper tier aggregates cache fills across the whole network", and: "the first request for a given cache key anywhere on Earth populates the upper tier. Every later request, from any Cloudflare data center, can be served from the upper tier without running your Worker". That points to one global upper tier per key.
- Sources conflict: the blog says "regionally tiered" and Tiered Cache defines Generic Global as regional pools, but the Workers Cache page says a fill anywhere serves "any Cloudflare data center". X1 decides.
- The topology is not configurable; custom topologies do not apply.
- Model: the colo→upper-tier hop is 15-50 ms by region (assumption, about 18 ms weighted). Doubling it adds about 18 ms warm and 55 ms cold to every A2/A3/B1-family tier hit and full miss. cf-optimize's derived 45-120 ms London tier hit would imply a 30-100 ms hop, i.e. a global tier.
- The model takes the favourable reading on both inputs: a short regional hop for latency, and u = 0.43-0.5 from the cadence model (§7.2), which assumes one global upper tier. The Speed section below gives both alternatives.
- A regional upper tier aggregates fills only from its own region. So its u is probably lower than that of Smart Tiered Cache's single upper tier, especially in small regions.
- Where the Worker runs on a full miss (the lower-tier colo or the upper-tier colo) is undocumented. The model places it at the upper tier; X1 logs `request.cf.colo`.

**Required changes:**
- Delete the Cache API code.
- **Keep the cache across code deploys and refresh it on data deploys.** Set `cross_version_cache = true` and put the deploy key in the cache key. Without it, "Each deployed version has its own isolated cache, so a new deployment starts from an empty cache" ([configuration](https://developers.cloudflare.com/workers/cache/configuration/)). There are two ways to get the key into the cache key:
  - Plain A2: put it in the public tile URL through a short-TTL TileJSON (`/v1/<key>/{z}/{x}/{y}.mvt`). This also stops browsers mixing old tiles with a new style.
  - A2-gw: the gateway rewrites to `/<DEPLOYMENT_KEY>/v1/{z}/{x}/{y}.mvt` before calling the cached entrypoint, so the public URL can stay stable.
  - Either way, version the public URL for schema changes (`/v2`). For data-only deploys, either deploy at most monthly with the key in the cache key, or keep keys stable and use `stale-while-revalidate` (§7).
- **Drop `Vary: Origin`** (ACAO is `*`) and normalise `accept-encoding`. Otherwise the cache splits into one variant per self-hosted Immich origin and per Accept-Encoding string.
- **Emit a canonical host in `/v1.json`.** The prod Worker also answers `tiles.prod.immich.cloud`, and host is not in the key, so a cached TileJSON would return whichever host filled it.
- **No `Server-Timing` in cached responses.** It would be frozen into the cache. In A2-gw the gateway adds its own.
- **Terraform.** The Worker is deployed with `cloudflare_workers_script` (provider 4.52.5, `deployment/modules/cloudflare/tiles-worker/workers.tf`), which has no documented cache attribute. The SDK has an undocumented `cache_options` field. Spike: provider v5, the API's script metadata, or deploying this Worker with wrangler.

**Speed:**
- *Calibrated:*
  - Hits 18-22 ms. In London that is 19-23 ms against 24-32 ms today, so 2-10 ms saved like for like.
  - Tier hit about 42 ms.
  - Full miss **318-492 ms**: A1's calibrated miss plus a modelled 26 ms detour (colo→regional upper tier + 2 ms lookup + 6 ms CDN hop − the Worker hit saving). This replaces v1's unsupported "today + 10-30 ms".
  - Blended **180-267 ms**. Mean per tile 61-86 ms against 96-154 today.
  - *Topology sensitivity* (calibrated, same anchors). A regional tier with a lower u = 0.3: blended 235-357 ms, mean per tile 75-110 ms. One global tier (hop ×2, +18 ms warm), u = 0.5: tier hit 60, full miss 336-510, blended 198-285, mean per tile 65-91 ms. A2 still beats today's 96-154 ms in both cases.
- *Modelled lower bound:* tier 38, full 104-234, blended 71-155 ms.

**Cost:**

| Variant | R | S | M | L | XL |
|---|---|---|---|---|---|
| A2, u = 0.5 | $14 | $27 | $105 | $394 | $1,167 |
| A2, u = 0.3 to 0.8 | — | $29-25 | $113-92 | $429-343 | $1,271-1,012 |
| A2-gw (gateway) | $18 | $43 | $185 | $715 | $2,127 |

- u = 0.5 fits the §7 model: 0.43 at steady state, 0.57 if the upper tier keeps tiles for 31 days, 0-0.7 across scenarios.
- Monthly deploys with the key in the cache key add about $0.3 (R) to $3 (XL) per month in origin fetches (modelled).
- Turning caching on also makes static-asset and Worker-to-Worker requests billable.

**Gateway variant (A2-gw): the "A cache between every Worker entrypoint" pattern** ([blog](https://blog.cloudflare.com/workers-cache/)). The maintainer suggested it as a way to keep metrics with tiering.
- **Design.**
  - The default entrypoint has caching disabled and runs at the user's colo on every request. It normalises the request, sets CORS, records metrics and writes its own `Server-Timing`.
  - It then calls `ctx.exports.Tiles.fetch(rewrittenRequest)`.
  - `Tiles` is the cached entrypoint. It emits no `Vary: Origin` and no per-request `Server-Timing`.
- **Both tiers apply to the internal call.**
  - "Caching works for any `fetch()` invocation of the Worker — eyeball requests …, service bindings, and loopback `fetch()` calls between entrypoints via `ctx.exports`" ([Workers Cache](https://developers.cloudflare.com/workers/cache/)).
  - "the cache evaluates that call the same way it would evaluate a request from a browser", and it "consults the lower and upper cache tiers" ([examples](https://developers.cloudflare.com/workers/cache/examples/)).
  - The key is the callee entrypoint + path + query + `ctx.props`, plus the version unless `cross_version_cache` is set.
- **What metrics survive.**
  - The gateway sees every request, with colo, continent, apex domain and latency.
  - The callee runs only on misses and emits miss metrics. Hits = gateway requests − callee runs.
  - The per-apex RPS alert and the colo dashboards keep working.
  - `Cf-Cache-Status` is on "every response" ([configuration](https://developers.cloudflare.com/workers/cache/configuration/)), but it is not confirmed to be readable on a `ctx.exports` response. X1 checks; callee-side miss counting is the robust fallback.
- **Billing.** Once caching is on, "worker-to-worker invocations through service bindings or `ctx.exports`" are billed at the standard request rate. CPU is billed only when the callee runs. That roughly doubles billed requests: **+$3 (R), +$10 (S), +$50 (M), +$201 (L), +$603 (XL)** against today, or +$0.32 per million requests on top of A2.
- **Smart Placement off for the gateway.** Placement is per Worker and would move the gateway away from users. For a placed miss path, make the cached entrypoint its own Worker behind a service binding (P3, §4.A9).
- Latency: the model adds 5 ms per request (assumed 1-5 ms).

**Regions:** a lower tier in 348 CF cities; an upper tier, regional or global (locations unpublished; X1).

**Ops:** low (+0.25 h/month).

**Migration:** 3-8 days including the Terraform/wrangler spike, plus 1 day for the gateway. [#245](https://github.com/immich-app/geoshenanigans/pull/245) is unaffected apart from key versioning.

**Risks:**
- Brand-new product; the sources conflict on its topology (X1).
- Full misses are still D1 + Tigris.
- Without the gateway, hit metrics, colo dashboards and the per-apex alert go away. Hits must then be counted from zone analytics.
- Run the Appendix C queries **before** enabling A2, because afterwards hits no longer run the Worker.

#### A3. Workers Cache plus loopback entrypoints, and directories off D1

**How it works:**
- A cached `Chunk` entrypoint returns whole 1 MB chunks. Workers Cache slices `Range` requests from a cached full 200 response, so neighbouring tiles become cache hits. A chunk holds about 1,300 z15 tiles on average, about 230 in cities.
- A cached `Dir` entrypoint returns directory blocks.
- Directories also move off D1:
  - An isolate LRU.
  - The 104 z0-z12 leaf directories bundled into the Worker. Measured on 20261101: 6.36M entries, 10.47 MB gzip, 40.3 MB of decompressed varints.
    - **Never decode them at startup.** Fully decoded they take **146 MiB as typed arrays (24 B/entry) or about 580 MiB as JS objects**, more than an isolate's 128 MB.
    - Ship the gzip blob as a binary `Data` module. That uses 10.5 MB of the 64 MiB uncompressed limit ("There is no compressed size limit"), and global scope must parse within 1 s ([limits](https://developers.cloudflare.com/workers/platform/limits/)).
    - Decode one leaf on first use (3-4 ms CPU and about 1.5 MB each, measured) into an LRU of 20-40 leaves (30-60 MB).
    - Avoid base64: a base64 string in `index.js` costs about 155 ms of compile time per cold isolate, plus about 50 ms to decode (measured in Node).
    - Today's Terraform deploy uploads a single `index.js`, so this needs wrangler (`rules = [{ type = "Data" }]`, [config](https://developers.cloudflare.com/workers/wrangler/configuration/)) or a multi-module upload.
    - A cheaper option: bundle only z0-z10 (11 leaves, 0.98 MB gzip, about 16 MB decoded), which can be decoded in full.
  - z13-z15 blocks served from D1 or static assets.
- KV as a directory store costs $0.50/M reads ([KV pricing](https://developers.cloudflare.com/kv/platform/pricing/)). Its cold-read p99 of about 250 ms ([changelog](https://developers.cloudflare.com/changelog/post/2025-08-22-kv-performance-improvements/)) is no better than D1.

**Speed:** modelled tier hit 38 ms, full miss 78-159 ms, blended 58-117 ms. Removes D1's 350-520 ms p99 tail. v1's derived London lower-tier miss was 40-80 ms p50 and 140-220 ms p90.

**Cost:** R $14, S $29, M $114, L $434, XL $1,285. This includes 2 extra billed requests per origin miss and assumes a 40% chunk-level miss share (unverified).

**Risks:** range slicing on loopback calls whose body is still streaming is unverified. Upper-tier retention for a 139 GB working set is unknown. Medium effort: 8-15 days including A2.

#### A4. R2 instead of Tigris; stock Protomaps single-file Worker

**Prices:**
- R2: $0.015/GB-month, Class B $0.36/M after 10M free, egress free ([R2 pricing](https://developers.cloudflare.com/r2/pricing/)).
- A bucket has one region; location hints are best effort ([data location](https://developers.cloudflare.com/r2/reference/data-location/)).

**Measured on public R2 objects only.** These are client TTFB through `build.protomaps.com` (`cf-cache-status: DYNAMIC`), not through a Worker binding, so they are not comparable with Worker-side Tigris timings:
- 16 KiB ranges deep into the 139 GB multipart object: 125-196 ms inside the bucket's region (inferred WNAM), 367-435 ms from Europe, 0.4-2.0 s from Asia, Oceania and Africa.
- Keep-alive from London: p50 371 ms (edge RTT 18 ms). Re-reads were no faster (p50 367 ms).
- A 20.9 MB single-part object: 80 ms in-region.
- [bdon/cloudflare-r2-latency](https://github.com/bdon/cloudflare-r2-latency) shows the same pattern (2023, public domain, mean of 30). From US West: 168 ms for a 1-byte range, 206 ms at the start of a ~160 GB PMTiles, 358 ms at its end. Deep offsets in a huge multipart object cost about 150 ms extra, which 1 MB single-part chunks would not pay.
- No published benchmark of Worker-binding range reads exists. Cloudflare's [R2 vs S3 post](https://blog.cloudflare.com/r2-is-faster-than-s3) gives only the p95 of whole 1 MB public downloads. Tigris's [R2 comparison](https://www.tigrisdata.com/docs/overview/benchmarks/cloudflare-r2/) gives an R2 read p50 of 606 ms, but it is a competitor benchmark with no stated method.
- Binding, S3 and REST requests all pass the R2 Gateway, which tries a tiered read cache before the storage region ([How R2 works](https://developers.cloudflare.com/r2/how-r2-works/)). Hot chunks may therefore be served near the Worker; cold reads pay the round trip to the single region.
- Protomaps warns of "higher latency (500ms or higher)" ([docs](https://docs.protomaps.com/deploy/cloudflare)). In 2022 the PMTiles maintainer wrote that range latency "can spike to 1-2 seconds" ([#98](https://github.com/protomaps/PMTiles/discussions/98)).

**Variants:**
- **R2 chunks in WEUR:** London misses about 135-185 ms. APAC, Oceania and South America get 150-300 ms worse, because Tigris serves near the reader. Both figures are modelled from the public-domain numbers and unverified until X8.
- **Stock Protomaps Worker:** needs 1-2 serial reads into the 139 GB object and caches only `ResolvedValueCache(25)` directories per isolate, against 2,917 leaves ([source](https://github.com/protomaps/PMTiles/blob/main/serverless/cloudflare/src/index.ts)). Expect misses of about 200-350 ms.

**Cost:** R2 chunks R $12, S $27, M $121, L $472, XL $1,410. The stock single-file Worker is within ±$15 of that.

**History:** Immich ran six regional R2 buckets until [#170](https://github.com/immich-app/geoshenanigans/pull/170) moved to Tigris.

**Verdict:** **unproven either way.** The measured slowness comes from public-domain, multipart, cross-region reads, not from the design that would ship. Run X8 after the Worker-side endpoint test (X12). Consider R2 only if binding reads of 1 MB chunks beat the fixed Tigris path where most misses are (63% EU).

#### A5. z0-z8 as free Workers static assets

**Facts:**
- z0-z8 is 87,381 addressed tiles (29.2k unique, 0.56 GB). That fits the 100,000-files-per-version limit and the 25 MiB per-file cap ([limits](https://developers.cloudflare.com/workers/platform/limits/)).
- The file count is fixed by zoom, so planet growth never reaches the cap. Size grows about 5% a year (0.56 → about 0.65 GB in 3 years), and no total-size limit is documented. z0-z10 (1.4M paths) does not fit.
- Static-asset requests are free ([billing](https://developers.cloudflare.com/workers/static-assets/billing-and-limitations/)) **unless Workers Cache is on for that Worker**.

**Cost** (v1 model, h = 0.74; not in `cost.py` v2, within about $3 of it at v2's 0.738): if z0-z8 is 15% of requests (unverified; the zoom mix is a *pending production split*, Appendix C): R $14, S $29, M $115, L $437, XL $1,294.

**Speed:** no latency gain; low zooms are already hot everywhere.

**Conflicts:** with A2, so it needs a separate route or Worker. **Untested:** whether pre-gzipped MVT can be served with `Content-Encoding: gzip`.

#### A6. Tile bytes in Cloudflare storage (D1 shards, KV, Durable Object SQLite)

Costs in this section are from the v1 model (h = 0.74); they are not in `cost.py` v2 and differ from it by $2 or less.

**D1 shards.**
- Layout: each row holds a 50-entry directory block plus its contiguous tile bytes (about 3.57M rows, about 39 KB each). That needs about 14 databases per copy, given 10 GB per database and the 100 KB SQL statement limit ([D1 limits](https://developers.cloudflare.com/d1/platform/limits/)).
- Speed: full miss about 60-75 ms p50 and 100-130 ms p90 from London, **derived** from small-row D1 timings. The p99 keeps D1's 350-520 ms tail. There are no replicas in South America, Africa or the Middle East.
- Cost: $0.75/GB-month gives about $205 for 2 copies. R $216, S $227, M $291, L $527, XL $1,158.

**KV.** Values of 32-64 KB, because KV has no range reads. Storage $0.50/GB-month is about $139 for 2 copies. Cold reads p95 about 50 ms and p99 about 250 ms, measured inside the gateway ([blog](https://blog.cloudflare.com/faster-workers-kv/)). Cost: R $150, S $161, M $235, L $521, XL $1,282.

**Durable Object SQLite regional copies.** 10 GB per object, $0.20/GB-month, $0.15/M requests ([DO limits](https://developers.cloudflare.com/durable-objects/platform/limits/), [DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)). Six regions × 2 copies come to about $334 of storage. Speed is plausibly 20-60 ms per miss, unmeasured. Cost: R $340, S $352, M $419, L $670, XL $1,341.

**Per-deploy ingest** (about 3.57M rows or keys; not in the totals above):
- **KV:** $17.85 in writes at $5/M ([KV pricing](https://developers.cloudflare.com/kv/platform/pricing/)), plus up to $17.85 to delete the previous deploy's keys. That is $18-36 per deploy, or $77-155/month at a weekly cadence.
- **D1 shards:** 3.57-7.14M rows written: $0 within the account's 50M/month, otherwise $1/M ([D1 pricing](https://developers.cloudflare.com/d1/platform/pricing/)).
- **DO SQLite × 6 regions:** 21.4M rows: $0 within 50M included, otherwise about $21. Requests and duration (128 MB × active seconds, 400k GB-s included) are about $0.

**Verdict:** all three cost 10-20× today at R, for gains that have not been measured. Keep D1 shards only as the "must stay serverless" fallback idea.

#### A7. Fix the distant-colo routing

**Problem:** 12 of 48 probes were served from distant colos for this zone (§2.2, item 5).

**Corrections to the track data:** Dallas (AT&T) uses the same DFW colo, just over a slower path. Lima's www.cloudflare.com reference is not local either. Two probes show the reverse (Kuala Lumpur, Ho Chi Minh City).

**Gain if fixed:**
- Edge RTT for affected users: Mumbai 99→2 ms, Seoul 135→4, Taipei 126→1, Lagos 104→2. Today's traffic-weighted hit drops from 24 to 16 ms, and a plain CDN hit from 18 to 10 ms (modelled).
- With three origins (B3), South and East Asia misses drop from 146 / 144 to 69 / 91 ms (§5.2).
- Size this from production before buying a test zone: gain ≈ Σ over each (client continent → distant colo) pair of its request share × that colo's extra RTT (Appendix C, Q2-Q6).

**What Cloudflare says.** Colo selection "cannot be configured per zone, per account, or by support request". The same page says "The set of data centers that can serve a given domain varies between Cloudflare customers" ([CF](https://developers.cloudflare.com/support/troubleshooting/general-troubleshooting/geographic-traffic-routing/)). "Network Prioritization" is listed for Contract plans only ([plans](https://www.cloudflare.com/plans/network-cdn.md)).

**Zero-cost test (2026-11-02).** 14 Globalping probes fetched `/cdn-cgi/trace` from tiles.immich.cloud and from zones in other anycast pools. Plan labels come from the [judge.sh list](https://cloudflare-test.judge.sh/) and are unverified.
- tiles (104.21.x / 172.67.x) matched a Free-listed zone on 13 of 14 probes, so immich.cloud is probably a Free zone (confirm in the dashboard, §9 Q8).
- A Pro-listed zone was local for 2 of the 6 affected probes (Delhi, Hyderabad).
- A Business-listed zone matched www.cloudflare.com on all 14 probes and was local for all 6 affected ones.
- A second set showed tiles non-local for Busan, Kolkata, Manila, Ho Chi Minh City, Lima, Bogotá, Mexico City, Cairo, Dubai and Nairobi. Most of those probes sit in cloud or hosting networks.
- Community reports tie the 104.21.x / 172.67.x pool to Free-plan routing ([centminmod](https://blog.centminmod.com/2022/05/19/2794/what-are-the-benefits-of-using-cloudflare-business-plan/)). That link is **unverified**.

**Prior odds** (subjective) that a plan change fixes most affected traffic: Pro about 30%, Business about 55-65%, Enterprise about 85-90%.

**Price.** A plan covers the whole immich.cloud zone: Pro +$20/month billed annually or +$25 monthly; Business +$200 or +$250 ([plans](https://www.cloudflare.com/plans/network-cdn.md)). Business is 13-17× today's tiles bill, for an extrapolated 8-11% of traffic. A test needs a throwaway domain (Cloudflare Registrar "Starting at $7.85", [plans](https://www.cloudflare.com/plans/network-cdn.md)) and one month of the plan (X5).

**Alternative:** Bunny, CloudFront and Fastly all had local PoPs in most affected cities (B5).

#### A8. Features that do not help

- **Cache Reserve:** $0.015/GB-month, $4.50/M writes, $0.36/M reads ([docs](https://developers.cloudflare.com/cache/advanced-configuration/cache-reserve/)). It is a zone-cache feature: it does not apply to `caches.default` or Workers Cache, and it is bypassed for R2 public buckets. Origin Range requests are not supported. Reads are R2-backed, so not faster. Consider it only for B2 with a slow origin.
- **Smart Placement:** no gain on today's stack, because D1 and Tigris are already distributed. With the Cache API it is harmful, because the cache would move away from users ([placement](https://developers.cloudflare.com/workers/configuration/placement/)). With Workers Cache the cache is consulted first, so placement no longer affects hits. It becomes a miss-path tool (§4.A9).
- **Pre-warming:** not practical with today's per-colo cache across 348 cities. With a tiered cache it is: requesting the top 10^5-10^6 tiles costs about $0.10-1 per upper-tier region per deploy and covers a modelled 58-88% of requests (§7). Workers Cache has no pre-warm API, so warming means real requests from each region.
- **Containers:** maximum 20 GB disk, ephemeral ([limits](https://developers.cloudflare.com/containers/platform/limits/)). Infeasible.
- **Argo Smart Routing:** not applicable to A options. Worker reads from Tigris and D1 are not zone origin fetches, and Workers Cache ignores zone settings. See B2a.

#### A9. Controlling the upper tier: gateway, placement hints, Durable Object shields, our own tier

**The maintainer's question: can we run or control code at the top tier?** No. No code runs *in* Cloudflare's upper tier, and Workers Cache does not let us choose where that tier is: it uses "a generic tiered cache topology", and custom topologies do not apply ([limitations](https://developers.cloudflare.com/workers/cache/limitations/)). What we can control:
- what runs near the user on every request (a gateway);
- where the miss-path code runs (placement hints);
- an optional stateful regional layer of our own (Durable Objects);
- an upper tier next to our own origin, through the zone's Smart Tiered Cache. No code runs there either.

| Variant | Where our code runs | Hit (wtd) | Full miss warm / cold external leg (wtd) | Extra $/month vs A2 or B1: R · M · L · XL | Per-request metrics | Status |
|---|---|---|---|---|---|---|
| Workers Cache only (A2 / B1) | misses only; location undocumented (modelled at the upper tier) | 18 | B1: 116 / 260 | 0 | no (zone analytics only) | documented |
| **P1** placement hint (B1-P1): `placement.region = "aws:eu-central-1"` (Frankfurt, next to FSN), or `hostname` = origin | full misses, next to the origin | 18 | 119 / **125** | 0 | no | documented: "The cache is always consulted before Smart Placement is considered"; a full miss "traverses three locations" ([Workers Cache](https://developers.cloudflare.com/workers/cache/#smart-placement-and-the-cache)) |
| **P2** gateway (A2-gw / B1-gw) | gateway at the user's colo on every request; cached entrypoint only on misses | 23 | 121 / 265 | +4 · +80 · +320 · +960 | **yes**: hits = gateway requests − callee runs | documented; both calls billed |
| **P3** gateway Worker A → service binding → cached Worker B with placement (B1-gw-P1) | A near the user; B next to the origin | 23 | 124 / **130** | as P2 | **yes** | placement "does not affect … named entrypoints" ([placement](https://developers.cloudflare.com/workers/configuration/placement/)), so B must be its own Worker |
| **P4** Durable Object regional shields (one object per `locationHint`; directories in SQLite + memory LRU) | DO in about 6 regions | n/a | Worker→DO hop instead of D1 (unmeasured) | +0.2 · +5-17 · +20-45 · +70-85 | n/a | unmeasured; tile bytes still come from Tigris or the origin |
| Own upper tier, B1-z: Worker `fetch()` to a proxied origin hostname | none in the tier; the zone tier sits next to our origin | 18 | 123 / **129** | 0 | misses only | documented: a Worker fetch to a proxied hostname "passes through Cloudflare's cache and Tiered Cache" ([docs](https://developers.cloudflare.com/cache/interaction-cloudflare-products/workers/)) |
| Own upper tier, B2: VM origin behind Smart Tiered Cache, no Worker | none | 18 | 98 / **104** | −$6 − $0.31/M vs B1 | no | documented |
| B1-fetch: Worker on every request, zone cache, no Workers Cache | every request at the user's colo | 24 | 104 / 110 | 0 at R; +$0.03/M | **yes** | documented; tier hits 98 ms (EU upper tier) |

- **Internal calls use both tiers.** A `ctx.exports` or service-binding call is evaluated "the same way it would evaluate a request from a browser" and "consults the lower and upper cache tiers" ([examples](https://developers.cloudflare.com/workers/cache/examples/)).
- **Billing.** With caching on, worker-to-worker calls are billed at the standard request rate; CPU only when the callee runs ([pricing](https://developers.cloudflare.com/workers/cache/#pricing)).
- **Placement hints.**
  - Hint types: AWS/GCP/Azure region ids, or `hostname` probes. Hostname probes are "experimental" and do not work for anycast targets such as Tigris.
  - Gain: every full miss worldwide runs in one colo, so the directory LRU and keep-alive connections to the origin stay warm even at 5 req/s. That addresses v1's main warm/cold uncertainty.
  - Price: one more inter-colo hop on full misses (about 5-20 ms inside the EU, unverified estimate). The docs say only "Future updates may bring these choices closer together" ([Workers Cache](https://developers.cloudflare.com/workers/cache/#smart-placement-and-the-cache)); the blog says "We're working to coordinate those choices" ([blog](https://blog.cloudflare.com/workers-cache/)).
  - Placement is free on all plans.
- **Durable Object shields.**
  - Hints are wnam, enam, weur, eeur, apac, apac-ne, apac-se and oc. sam, afr and me spawn elsewhere, and hints are best effort ([data location](https://developers.cloudflare.com/durable-objects/reference/data-location/)).
  - DOs exist in about 11% of PoPs ([where.durableobjects.live](https://where.durableobjects.live/), 2026-11-02), so most colos pay an extra hop.
  - An object has 128 MB of memory, so keep compressed leaves in SQLite (2,917 rows of about 121 KB, 353 MB per region) with a decoded LRU.
  - Behind Workers Cache only full misses (13% of requests at u = 0.5) reach a shield ([DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/)).
- **Zone-side tier.** Smart Tiered Cache "selects the single closest upper tier for each of your website's origins" and adds free connection reuse ([Smart Tiered Cache](https://developers.cloudflare.com/smart-shield/configuration/smart-tiered-cache/)). This is the only documented way to put an upper tier next to our origin.

**Recommendation:**
- With B1, use **P1**: it is free, one config line, and tested in X2.
- Use **P2**, or **P3** with an origin, if per-request metrics stay a requirement (§9 Q6). At R it costs $3-4/month; the price scales at $0.32 per million requests.
- Use **P4** only if the A path is kept and D1's tail remains after A1/A3.
- If no per-request logic is needed, B2's zone tier gives the best cold-external full miss (only the leg leaving Cloudflare is cold; 104 ms) for the least money.

#### A10. Tigris eager placement (Dual-Region) on top of A1b (dropped in v2.3)

**v2.3: dropped.** The production bucket already lists 11-12 regions, yet the first read of a chunk missed in every region tested, and Tokyo, Singapore and Sydney were served from Johannesburg (§2.6). Adding eager `fra,iad` placement on top of that is unlikely to make chunks hot, and it does nothing for the connection penalty. Ask Tigris support (X14) and test a per-region pre-warm (X13) instead. The v2.2 analysis is kept below for reference.

**Prices and semantics** ([pricing](https://www.tigrisdata.com/pricing.md); storage billed per binary GB; [locations](https://www.tigrisdata.com/docs/buckets/locations/)):

| Location type | Storage | PUT (Class A) | Placement |
|---|---|---|---|
| Global (today, assumed; confirm in the Tigris console) | $0.02/GB-month | $0.005/1k | stored near the uploader; other regions copy on read ("Data is not copied eagerly") |
| Multi-region `eur` (ams, fra) / `usa` (sjc, ord, iad) | $0.025 | $0.01/1k | two or more copies inside one geography, strongly consistent |
| Dual-region, any two regions (e.g. `fra,iad`) | $0.02 per region | $0.005/1k per region | eager replication between the two |

**Cost for 2 retained copies (259.3 GiB):**
- Global: $5.19 (today).
- Dual-region `fra,iad`: $10.37, i.e. **+$5.19/month**.
- Three regions: $15.56 (+$10.37). The pricing page shows a 3-region example, but the docs and [CLI](https://www.tigrisdata.com/docs/cli/buckets/set/) document only two (**unverified**). The critic's "about $17" assumed three.
- Multi-region `eur`: +$1.30, Europe only.
- Deploy PUTs: $0.70 → $1.40 per deploy. GET cost is unchanged.

**How:** `tigris buckets set <bucket> --locations fra,iad`, or create a new Dual-region bucket. Existing objects probably stay where they are: "The location of existing data doesn't change" ([Fly support, May 2026](https://community.fly.io/t/does-a-bucket-region-switch-trigger-a-data-migration-process/27766)). #245 writes each deploy under a new key, so switch before the next deploy. No copy is needed, and the change can be reverted at the following deploy.

**Speed (calibrated):** **202-359 ms** full miss (202-289 if the cold-chunk term is 200 ms), against A1b's 202-456. The derivation removes the EU/NA cold-chunk term (115-200 ms on the cold anchor, applied to 84% of traffic); the 359 uses the smaller 115 ms term. It does nothing for the per-instance penalty (§2.2). Readers outside fra/iad (Asia 11%, Oceania 3%) may lose Global's read-side copies (**unverified**).

**Effort / risk:** under a day; low. v2.2 said to adopt it only if X7 showed cold-chunk reads above about 150 ms p50 after A1b. X7 showed cold placement is 60-80% of a cold read, but also that the existing multi-region placement does not prevent it, so the gate no longer applies.

### 4.B CDN in front of VM origins

#### Where the VM figures came from (v2.2)

The maintainer asked where v2.1's VM figures came from, and pointed out that small machines should serve plenty of requests and that a disk image can seed a machine in another region. Both points hold. Most of v2.1's VM numbers were assumptions carried over from track write-ups, and they over-sized every node. The full audit, with file and line references, is in `small-vm/MODEL_SMALLVM.md` §1.

| v2.1 figure | Where it came from | Measured or assumed | v2.2 |
|---|---|---|---|
| **Fly "$93-137 per region"** (§4.D, §4.C catalogue) | $93 = performance-1x 2 GB ($33) + 400 GB volume ($60). It was quoted from a track write-up; no surviving script computes it. $137 = `vm-providers/costmodel.py:26`: performance-2x + 4 GB extra RAM + 300 GB volume + dedicated IPv4 | **Assumed**: performance CPUs; 300-400 GB so two planet copies sit side by side for blue/green; 4-8 GB RAM for a full directory cache; a dedicated IPv4 | **$31.45 (iad) to $35.57 (gru) per region**: shared-cpu-1x 1 GB + one 150 GB volume + a 3-day deploy overlap. A fresh volume per deploy replaces the second copy, and a shared IPv4 is free. The volume is 70-80% of the cost |
| **D1 "$233 / 241 / 362 / 419"** | `hybrid/scripts/cost.py`: 3 × (performance-1x $33 + 300 GB $45) = **$78/region, not the $93-137 printed next to it**; origin share 6% (assumed) | Assumed | **Fly-cf N=3: $98 / $125 / $222 / $507**, with an origin share of 26.2% (tiering off). v2.1 never modelled its latency; v2.2 does (30-40 ms per tile) |
| **D2 "$474 / 586 / 1,363 / 2,314"**, "~34 ms weighted" | Same script: 8 × (shared-cpu-2x 2 GB $13.39 + 300 GB $45). The latency was a text estimate | The CPU was already small. The two-copy volumes were $360 of the $467 | 8 regions on one 150 GB volume each: **$267 / $368 / $752** at R / M / L. Modelled with measured Fly edges: **46 / 38 / 34 ms** for 6 / 10 / 17 regions |
| **B2 / C1 "$71"** | `synthesis/v2/cost.py`: AX41 $65.91 + two Tigris staging copies $5.19 | Price measured. **The 64 GB RAM and 2×512 GB sizing was assumed** (a ≥ 4.5 GB directory cache, 2-3 copies) | The price holds. A **Hetzner Cloud CX43** (8 vCPU / 16 GB / 160 GB, €16.49 = $18.42) plus one staging copy runs B2 for **about $21** (B2-s), with a break-even of about 26M instead of 125M requests/month. The AX41 still buys RAID1, a guaranteed 1 Gbit port and room for several copies |
| **B3 $286, B4 $555, C3 $271-530, C6 $525-830** | `cost.py` VM3/VM6: AX41 + OVH RISE-1 VIN 2×960 + Akamai g6-standard-6 (+ Vultr 320 GB nodes); LB $25 / $40 (estimated). The ~$530 and ~$830 were text extrapolations | Big nodes **assumed**: ≥ 300 GB for 2-3 copies, 16-64 GB RAM | On 160 GB nodes (CX43 in the EU, Vultr vc2-4c-8gb elsewhere): **B3-s $126, B4-s $281, C3-s $111 ($209 with N+1), C6-s $251 ($416 for 10)** |
| **"19k req/s/core"** (Server, below; E1) | tile-formats track: VersaTiles on the 1.98 GB NL/BE extract | Measured, **on the wrong dataset** | On the full planet: **766 req/s per core** (VersaTiles), 288 (go-pmtiles default), and 45 on a shared-cpu-1x at its 6.25% baseline. That is still ≥ 3× the busiest region's assumed peak at R (14 req/s) |
| **Server time 0.1-0.5 ms** (E1); the model's 3 ms | Extract measurement; assumption | — | Planet p50 / p99 at 5 req/s: **5.1 / 9.1 ms** (VersaTiles), 10.2 / 17.6 (go-pmtiles default). v2.1's B/C full misses are 2-7 ms optimistic, which adds 0.3-1.8 ms to a CDN-fronted mean per tile |
| **"8-17 engineer-days"** | `cost.py` STAFF: the v1 multi-region-ops estimate (8-12) plus a Workers Cache spike (2-5) | **Assumed**; never sized task by task | Re-estimated with a stated basis (§7.1): Fly-direct 6.5-11.5, Fly-cf 7-12.5, Fly-wkr 10-17.5, B3-s 8-13, B2 8-12. These are still estimates |
| **Ops h/month** (B2 2.75 … C6 23; Fly "3-6") | §7.1 basis: 1.5 h per self-managed node, +2 h per region for direct serving. Fly's "3-6" had no breakdown | **Assumed** | Fly re-estimated at 1.75 h (direct), 2.0 (behind CF) and 3.0 (Worker): no OS, RAID or certificates to run, and the proxy fails over between regions. Unverified either way |
| **"+$10-16k over 12 months"** (TL;DR, §7.1, §8) | `cost.py` tco12: 12 × infrastructure + migration days × 8 h × **$100/h (assumed)** + 12 × ops h × $100 | **Mostly assumed staff time.** For B2: migration 79% of the delta, extra ops 15%, infrastructure 7% (+$673/year) | Headlines now quote infrastructure cost only. TCO is a secondary line (§7.1): 85-100% of every v2.2 option's TCO delta is assumed staff time (77-82% for v2.1's big-node B3 and B4) |
| **"Do not serve directly from VMs (first load 417 ms)"** | C1 model: one EU node, TCP + TLS at origin distance | Modelled | Holds for **one EU node**. Fly terminates TLS at an edge near the user: first load 205-265 ms with 3 regions and 148 ms with 17 (modelled). **v2.3, measured live:** 184-195 ms with 4 regions, the same as today's all-hit first load (195) measured the same way. The modelled 90-125 for Cloudflare-fronted fleets uses a different method and is unmeasured (X11) |
| Fly egress "$20-120/TB", D2 blend $0.026/GB; volume refill "~46 min on performance-1x" | fly.io pricing with an assumed 80/17/3 mix; 129.7 GiB at the 48 MiB/s volume cap | Price measured; arithmetic | Both hold. Egress is $0.0237/GB on the measured mix (priced by destination). The refill is 35 min on performance-2x and 138 min on shared-cpu-1x; volume fork is an alternative (§4.D) |

**What v2.1 did not get wrong:** it never copied the planet by hand. Every node pulled the single file from Tigris in 19-31 minutes ("Data", below). A volume fork or disk snapshot is a second way to seed a region (§4.C, §4.D), not a missing capability.

#### Shared facts (origin side)

**Origin node (EU).** There are two equal candidates and one fallback. Prices come from live pages and APIs on 2026-11-02, ex-VAT, converted at ECB 1.1172.

| Node | €/month | $/month | RAM / disk | Network | Terms | IaC |
|---|---|---|---|---|---|---|
| Hetzner AX41-NVMe (FSN1, HEL1) ([AX41](https://www.hetzner.com/dedicated-rootserver/ax41/), [price list](https://docs.hetzner.com/general/infrastructure-and-availability/price-adjustment/)) | 59.00 incl. €1.70 IPv4 | **65.91** (69.00 if billed in USD) | 64 GB / 2×512 GB NVMe | "Guaranteed bandwidth 1 GBit/s", unlimited traffic ([traffic](https://docs.hetzner.com/robot/general/traffic/)) | no setup fee, no minimum term; hourly with a monthly cap ([Robot](https://docs.hetzner.com/general/billing-and-account-management/cancellation/cancellations-robot/)); "-LTD" offer sold while supply lasts | community `silenium-dev/hetzner-robot` only ([registry](https://search.opentofu.org/provider/silenium-dev/hetzner-robot)) |
| Scaleway EM-A610R-NVMe (PAR, AMS, WAW) ([pricing](https://www.scaleway.com/en/pricing/elastic-metal/)) | 39.99 (49.98 with the 1 Gbps option) | **44.68** (55.84) | 32 GB ECC / 2×1.02 TB NVMe | 500 Mbps guaranteed, up to 1 Gbps, unlimited | no setup fee; monthly billing charges a one-month commitment fee, hourly (€0.11/h) does not; IPv4 included ([networking](https://www.scaleway.com/en/docs/elastic-metal/reference-content/elastic-metal-networking/)) | official `scaleway/scaleway` |
| OVH RISE-1 (FRA) ([catalogue](https://eu.api.ovh.com/v1/order/catalog/public/eco?ovhSubsidiary=IE)) | 56.99 | 63.67 | 32 GB / 2×512 GB NVMe | 1 Gbps unmetered (plan code `bandwidth-1000-unguaranteed`) | setup €56.99 unless committed 12 months upfront | official `ovh/ovh` |

- **Hetzner server auction** ([auction](https://www.hetzner.com/sb/), [feed](https://www.hetzner.com/_resources/app/data/app/live_data_sb.json); the critic's `live_data_sb_EUR.json` URL returns 404). 31 of 90 offers have ≥2×512 GB NVMe and ≥32 GB. The cheapest is €74.70 including IPv4 (i7-6700, 64 GB non-ECC, HEL1); the next are €75.70-80.70. **None beats the AX41.**
- **Choice:**
  - Pick the AX41 if 64 GB RAM and a guaranteed 1 Gbit matter most.
  - Pick the EM-A610R-NVMe if an official Terraform provider, €19/month less and twice the disk matter more. On it, 3 copies fit for decades, against about 3 years on the AX41.
  - Both are far above B-option origin load (about 21 Mbit/s average at XL).
  - **v2.2: neither size is needed.** The planet bench (Server, below) shows a tile node needs about 1 GB of RAM and one vCPU. With a fresh node per deploy, one copy of the planet on a 160 GB disk is enough. A **Hetzner Cloud CX43** (8 vCPU / 16 GB / 160 GB NVMe, 20 TB included, €16.49 = **$18.42**, [cost-optimized](https://www.hetzner.com/cloud/cost-optimized/)) runs B2 for about $21 with one staging copy (B2-s). The dedicated servers remain the choice for RAID1, a guaranteed 1 Gbit port, or several copies on disk.
- The next Hetzner tier is AX42-1-LTD at €79 including IPv4 plus €39 setup, or AX42 at €99 plus €49 setup.
- Hetzner dedicated servers are sold only in Germany and Finland: AX/EX in FSN1 and HEL1, DX in NBG1/HEL1, auction servers in NBG/FSN/HEL ([AX41](https://www.hetzner.com/dedicated-rootserver/ax41/), [price list](https://docs.hetzner.com/general/infrastructure-and-availability/price-adjustment/), [auction feed](https://www.hetzner.com/_resources/app/data/app/live_data_sb.json)). The AX41 is not offered in NBG.
- Hetzner raised prices twice in 2026:
  - on 1 April, for new and existing products ([statement](https://www.hetzner.com/pressroom/statement-price-adjustment/), [heise](https://heise.de/-11185981));
  - on 15 June, for new orders only ([notice](https://www.hetzner.com/pressroom/standardization-and-price-adjustment-of-our-server-products/)).
- **Tax.** Hetzner charges "an 8% sales tax for customers in Texas" unless an exemption certificate is filed ([Hetzner VAT](https://docs.hetzner.com/general/billing-and-account-management/billing-at-hetzner/value-added-tax/)). FUTO is in Austin, so a FUTO-billed AX41 would likely be $71.18 and B2 $76.37.

**Hetzner Cloud alternative** (EU, official Terraform; critic #18):

| Plan (NBG1/HEL1) | Server €/month | 400 GB volume | Primary IPv4 | Total € / $ | Note |
|---|---|---|---|---|---|
| CPX42: 8 shared vCPU, 16 GB, 320 GB NVMe | 69.99 | 17.60 | 0.50 | **88.09 / 98.41** | 720 GB in total: 3 copies with >10 years of headroom |
| CPX52: 12 vCPU, 24 GB, 480 GB NVMe | 100.99 | — | 0.50 | 101.49 / 113.38 | 3 copies on local disk for only ~1.7 years |
| CCX23: 4 dedicated vCPU, 16 GB, 160 GB | 86.49 | 17.60 | 0.50 | 104.59 / 116.85 | dedicated cores |
| CPX32: 4 vCPU, 8 GB, 160 GB | 35.99 | 17.60 | 0.50 | 54.09 / 60.43 | v2.1 called 8 GB tight next to a 4.5 GB directory cache; the planet bench shows ~1 GB is enough |
| **CX43: 8 vCPU, 16 GB, 160 GB** (v2.2) | **16.49** | — (one copy on local disk) | included | **16.49 / 18.42** | B2-s; one copy with ~1.5-2 years of headroom at +7.1 GB/year, then CX53 or larger. The static pages showed "currently unavailable" on create buttons; stock unverified |

- Sources: [regular performance](https://www.hetzner.com/cloud/regular-performance/), [general purpose](https://www.hetzner.com/cloud/general-purpose/), [Primary IPv4 €0.50](https://docs.hetzner.com/cloud/servers/primary-ips/overview), [volumes €0.044/GB-month, billed hourly](https://www.hetzner.com/cloud/block-storage/).
- Billing: hourly with a monthly cap and no minimum term ([billing FAQ](https://docs.hetzner.com/cloud/billing/faq/)).
- Traffic: 20 TB included, then €1.00/TB; ingress is free.
- Volumes: up to 5,000 IOPS and 200 MB/s, stored on three servers ([volumes](https://docs.hetzner.com/cloud/volumes/overview)). Volume read latency is unmeasured and irrelevant next to 5-300 ms network hops.
- IaC: official `hetznercloud/hcloud` ([registry](https://registry.terraform.io/v1/providers/hetznercloud/hcloud), v1.70.2).
- Trade-off against the AX41: about +€29/month buys API rebuilds, snapshots, an official provider and no "-LTD" stock risk. With CPX42, B2 would cost $103.60.
- Two corrections to the critic. Robot now also bills hourly with immediate cancellation, so billing granularity is not a differentiator. And "no stock limits" overstates it: Cloud plans show per-location availability.

**Planet growth and disk headroom** (critic #16; [builds.json](https://build-metadata.protomaps.dev/builds.json), 61 builds, 2023-09-18 → 2026-11-01, 114.68 → 139.24 GB):
- **Trend.** +7.1 GB/year (least squares, 5.7%/year log-linear). The last two years ran at +4.9 GB/year; the fastest window was +11.1 GB/year (2023-24, schema v0→v4). Schema releases cause steps (+2.1 GB at v3.6.0, −5.2 GB at v4.7.1).
- **Next deploy.** The live key 20250818 is not in builds.json. The same-schema 20251020 build is 130.40 GB, so the next deploy ships +8.8 GB (+6.8%).
- **Projection** (low / central / high): +1 year 144 / 146 / 150 GB; +2 years 149 / 153 / 161 GB; +3 years 154 / 160 / 172 GB.

| Store (usable after ~2% filesystem + 15-20 GiB OS, estimate) | Copies | Used today | Years of headroom (low / central / high) |
|---|---|---|---|
| AX41/EX44 2×512 GB RAID1 (476.9 GiB raw, ~447 GiB usable) | 3 (served + previous + staging) | 389 GiB, 87% | 4.2 / 2.9 / 1.9 |
| same, previous copy deleted before staging | 2 | 259 GiB, 58% | 20 / 14 / 9 |
| Akamai g6-standard-6, 320 GiB ([types API](https://api.linode.com/v4/linode/types)) | 2 | 87% | 4.3 / 3.0 / 1.9 |
| Vultr vc2-6c-16gb, 320 GB ([plans API](https://api.vultr.com/v2/plans)) | 2 | 94% | 1.9 / 1.3 / 0.9 |
| Akamai or Vultr 320 GB | 3 | 130-140% | does not fit today |
| Scaleway EM-A610R-NVMe, 2×1.02 TB | 3 | 43% | 38 / 27 / 17 |
| D1, 10 GB per database ([limits](https://developers.cloudflare.com/d1/platform/limits/)), 3.5 GB per planet table (assumed to scale with planet size) | 2 tables | 7.0 GB | 12 / 8.4 / 5.4 |
| D1 | 3 tables (#245 retention) | 10.5 GB | already over |

- On 320 GB nodes, blue/green must delete the previous copy before downloading the new one, which turns rollback into a 19-31 minute re-download. Otherwise the node needs block storage.
- **v2.2: replace per deploy instead.** Every provider here bills hourly or per second. Build a fresh node or volume for each deploy (fork, snapshot or pull), verify it, switch, and keep the old one for a few days as the rollback. Each node then needs room for **one** copy: 160 GB lasts about 1.5-2 years at the central growth rate. A second 160 GB Fly volume held for 3 hours costs about $0.10.
- A second tile set (E3 `/v2`) doubles every figure.

**Load and bandwidth.**
- About 3.3M per-colo misses/month is about 1.3 req/s. Behind an upper tier (B2's Smart Tiered Cache, u = 0.5) the origin sees about half of that: 1.65M/month, about 0.63 req/s.
- Behind a tiered CDN, origin egress is ≤7 TB/month even at XL, about 21 Mbit/s average.
- A fully cold cache after a deploy at XL is about 1,050 tile req/s, 150-164 Mbit/s average and about 650 Mbit/s at an assumed 4× peak (*pending production split* for the real peak). That fits 1 Gbit but not Scaleway's 500 Mbps guarantee.

**Server** (v2.2: **measured on the full planet**, planet-bench track).
- **Method.** The 20261101 planet (139,235,644,790 bytes, integrity verified) was served on the Hetzner box under cgroup v2 limits that emulate Fly shapes: CPU quota on an 80 ms period (6.25% = a shared-cpu-1x baseline), memory limits, and Fly's documented per-size volume caps (16 / 48 / 64 MiB/s). Load came from `oha` with latency correction on a synthetic request list: 70% population-weighted city tiles at z10-15 and 30% uniform at z0-15, with a 29.7 KB mean tile against production's 18 KB. These are emulated limits on an EPYC 7502P, not Fly hardware, and each configuration ran once.

  | Shape (emulated) | VersaTiles 5.6.0, max req/s | go-pmtiles 1.31.2 default, max req/s | Latency p50 / p99 at 5 req/s | Fly price (iad) |
  |---|---|---|---|---|
  | shared-cpu-1x, baseline 6.25% | **45** (512 MB, pinned at the limit) | 17 | VT 4.8 / 59.8 (pinned); pm 59.9 / 295.6 | $3.69 (512 MB) / $6.70 (1 GB) |
  | shared-cpu-1x while bursting (500 s balance) | — | 225 (bound by the 16 MiB/s volume cap) | pm 10.2 / 17.6 | as above |
  | 25% of a core, 1 GB (shared-cpu-4x budget) | **207** | 74 | VT 5.1 / 9.1 | $8.78 |
  | performance-1x 2 GB | **766** (500 req/s at p50 2.2 / p99 12.7) | 288 (524 with `--cache-size 768`) | VT 4.6 / 9.1; pm 10.2 / 17.8 | $33.00 |
  | performance-2x (2 GB profile) | **1,529** | 570 (1,017 with cache 768) | VT 5.1 / 8.9; pm 9.7 / 14.9 | $66.00 (4 GB minimum) |

- **Smallest viable node.** At today's load (≤ 3.4 req/s average in the busiest region, 14 at an assumed 4× peak), a shared-cpu-1x is enough.
  - VersaTiles uses about 3 ms of CPU per request at low rates. 14 req/s is 4.2% of a vCPU, under the 6.25% baseline, so the burst balance never drains.
  - VersaTiles needs **1 GB**. It was OOM-killed at 256 MB, because it keeps the whole 337 MiB compressed leaf section in RAM, and it sat pinned at 512 MB.
  - go-pmtiles runs in **512 MB** but needs about 8 ms of CPU per request at low rates, so a sustained 4× peak drains the burst balance in about 2.8 hours.
- **Why the planet is 25-33× slower per core than the extract.** It has 2,917 leaf directories averaging 121 KB gzipped, 4.28 GB once decoded. go-pmtiles' default 64 MB cache holds about 43 of them, so 82% of the synthetic mix needs a leaf that is not cached (the simulation and go-pmtiles' own log agree). Each miss costs 3.4-4 ms of CPU plus a 121 KB re-read from disk. With every directory cached in 8 GB of RAM, go-pmtiles reaches 3,279 req/s on one core and becomes bound by the 48 MiB/s volume cap. VersaTiles decodes from RAM at 1.2-2 ms per miss, with no extra disk read.
- **Startup.** go-pmtiles serves in 0.06-0.08 s. VersaTiles reads 362 MB of directories before it binds the port: 21 s at 16 MiB/s, 5.3 s at 64 MiB/s, 0.5 s uncapped. Give it a health-check grace period, and do not start it on a Fly fork that is still hydrating.
- **v2.3, on real Fly hardware** (§2.5): a shared-cpu-1x 1 GB Machine with VersaTiles used 2.7-3.1 ms of CPU per request, as emulated, but sustained only **about 20-30 req/s on its baseline** (clean at 20, p99 87 ms at 30), against the emulation's 45. With burst credit it reached 279-285 req/s. Start-up was 20 s on a 150 GB volume. Today's load still fits with 1.5-6× headroom; from S upward the shape table in §4.D steps up one size sooner than the emulation suggested (derived).
- **The earlier extract figures** (VersaTiles 19k req/s on 1 core, go-pmtiles 9.6k, p50 0.11-0.23 ms) apply only to the 1.98 GB NL/BE extract, which fits in page cache.
- Raw results: `small-vm/bench/raw/results/` (locally) and `/ssd/2026-11-01-flowly-47dc35b5-deploy-research/small-vm/results/` on the box.

**Data.**
- Stage each build once as a single file in Tigris. Protomaps discourages hotlinking and keeps daily builds for one week ([downloads](https://docs.protomaps.com/basemaps/downloads)).
- A node pulls a copy in about 19-31 minutes: Tigris single stream measured 769 Mbit/s, 898 Mbit/s with 8 streams, and about 600 Mbit/s on a re-run under load. The test object was a warm, 11-region geocoder file, so a cold tiles object may be slower.
- **v2.3, measured into Fly regions** (§2.5): the box uploaded the planet to a fresh bucket at 110 MB/s; a performance-2x in ams pulled it in 34.6 min (volume-write bound at 67 MB/s); and t3 delivered 98.5-216 MB/s into shared-cpu-1x Machines in iad, sin and syd (8 streams). The Fly endpoint was slower everywhere and stalled in sin (7.8 MB/s). Use t3 for pulls.
- Then verify the `b3sum` and run the [#245](https://github.com/immich-app/geoshenanigans/pull/245) 20k-tile check, swap a symlink, reload, and report the key on `/health`.
- **v2.2 alternatives to the per-node pull:** a Fly volume fork, or a disk snapshot that can be restored in another region (Hetzner Cloud, Vultr, DigitalOcean, AWS, Lightsail, OVH). Both seed a region without a download on each node. Timing and trade-offs are in §4.C and §4.D. The pull stays the portable default: one script works on every provider and in a mixed fleet, and the transfer is free (Tigris egress and provider ingress cost nothing).

**Origin protection.** Origin CA certificates and Authenticated Origin Pulls, available on all plans ([AOP](https://developers.cloudflare.com/ssl/origin-configuration/authenticated-origin-pull/), [Origin CA](https://developers.cloudflare.com/ssl/origin-configuration/origin-ca/)). Firewall the origins to Cloudflare IP ranges.

**Latency model and warm connections** (§5 has the formulas).
- Every B figure is a modelled lower bound with an explicit upper-tier term: tier hit = hit + colo→upper RTT + 2 ms lookup; full miss = that + upper→origin + 3 ms origin.
- Cloudflare keeps idle origin connections for 900 s ([connection limits](https://developers.cloudflare.com/fundamentals/reference/connection-limits/)). Modelled at R's 1.26 misses/s:
  - 97% of per-colo fetches find a warm connection if each colo has one pool, 80% with 10 pools and 47% with 50. Pool granularity is undocumented.
  - A single upper tier next to the origin (Smart Tiered Cache) stays about 100% warm.
  - Deploys raise the miss rate, so refills run warm.
- So the warm/cold question matters for B1 with a direct `fetch()`, not for B2. Expected full miss at a warm share of 0.97 / 0.8 / 0.47 (today's routing, modelled):

  | Option | 0.97 | 0.8 | 0.47 |
  |---|---|---|---|
  | B1 | 120 | 145 | 192 |
  | B1-P1 | 119 | 120 | 122 |
  | B1-z | 123 | 124 | 126 |
  | B2 | 98 | 99 | 101 |
  | B3 (LB) | 60 | 61 | 63 |
  | B3 (Worker steering) | 80 | 91 | 113 |

- Calibration (§5.1) says to expect real B figures 5-20 ms above the warm lower bound once the Tigris and D1 terms are gone. X2 validates.
- The v2.1 B rows assume 3 ms of origin time. The planet bench measured 5.1 ms p50 (VersaTiles), so their full misses are about 2 ms optimistic (7 ms with go-pmtiles' default cache). The v2.2 "-s" and Fly rows use the measured figure.

#### Hit-ratio assumptions for every CDN option (unverified estimate; critic #25)

Today's measured tile hit ratio is 73.8% per Cloudflare colo, with no tiering.
- A CDN's edge hit ratio rises when the same traffic is served by fewer PoPs. It cannot exceed the hit ratio of one global cache, 1 − m∞. u = 0.5 gives m∞ ≈ 13% (5-18% for u = 0.8 to 0.3).
- Model per region: edge miss = m∞ + (26.2% − m∞) × k^−0.5, where k = Cloudflare colos ÷ the CDN's PoPs serving that region. Ranges use exponents 0.3-0.7.
- Figures are weighted by the measured traffic mix (`fixups-competitor-cdn-latency/scripts/hitratio.py`).
- v1's CDN cost scripts silently assumed 90%.

| CDN (PoPs) | Edge hit (range) | Tier in front of the origin | Full origin miss |
|---|---|---|---|
| Cloudflare today, Cache API (342 colos, [status page](https://www.cloudflarestatus.com/api/v2/components.json)) | 74% measured | none | 26% |
| Cloudflare Workers Cache / Smart Tiered Cache (A2, B1-B4) | 74% | upper tier, u = 0.5 | 13% (5-18%) |
| Bunny Standard (119: EU 34, NA 22, Asia 27, OC 7, LATAM 18, MEA 13, [network](https://bunny.net/network/)) | 78% (75-82%) | Origin Shield, one EU or US site, free ([docs](https://bunny.net/docs/cdn/performance/origin-shield)) | 13% |
| Bunny Volume (10: FRA, PAR, CHI, DAL, LAX, MIA, HKG, SIN, TYO, SAO) | 84% (79-93%) | Origin Shield, if available on Volume (unverified) | 13% |
| Fastly (~114 locations, "fewer, more powerful POPs", [map](https://www.fastly.com/network-map)) | 79% (76-85%) | shield POP; shield misses billed twice | 13%; 21% without shielding |
| CloudFront (750+ POPs in 100+ cities, 15 free regional edge caches, [features](https://aws.amazon.com/cloudfront/features/)) | 68-80% | regional edge caches by default; Origin Shield paid | ~16%; 13% with Origin Shield |
| Gcore (210+, [CDN](https://gcore.com/cdn)) | 77% (75-80%) | none unless shielding is bought ([paid option](https://gcore.com/support/articles/214080309)) | 23%; 13% with shielding |

- Every design with one global tier ends at the same origin-miss floor; topology only moves misses between the edge and the tier.
- With one EU origin the tier sits beside the origin, so a tier hit costs almost a full miss (92 vs 98 ms weighted for B2).
- Per-tile latency is therefore set by edge hit ratio × edge RTT. Bunny's "95% Avg. Cache Hit Ratio" is a fleet-wide marketing figure and does not apply here.
- Calibrate the exponent with production per-colo hit ratios (Appendix C, Q7).

#### B1. Thin Worker with Workers Cache, misses fetched from one EU origin (and its variants)

**How it works:**
- Workers Cache (A2) serves hits.
- On a full miss, a thin Worker (no AWS SDK, D1 or Tigris on the hot path) calls `fetch()` on the origin. On a timeout or 5xx it falls back to today's D1 + Tigris path. That fallback is B1's HA story, and it costs $6.69/month of retained data.
- Variants:
  - **B1-P1:** add `placement.region = "aws:eu-central-1"` so full misses run next to the origin (A9 P1).
  - **B1-z:** send the origin fetch through a proxied origin hostname, so the zone's Smart Tiered Cache keeps a warm upper tier next to the origin. A Worker fetch to a proxied hostname "passes through Cloudflare's cache and Tiered Cache" ([docs](https://developers.cloudflare.com/cache/interaction-cloudflare-products/workers/)). That is two cache layers.
  - **B1-gw:** add the A2 gateway for per-request metrics. **B1-gw-P1** (A9 P3) makes the cached part its own placed Worker.
  - **B1-fetch:** a Worker on every request with no Workers Cache. Hits come from the zone cache through `fetch()` (`cacheEverything`).

**Speed** (modelled lower bound, weighted, today's routing; §5.3):

| Variant | Hit | Tier hit | Full miss warm–cold (cold external leg only) | Blended | Mean per tile |
|---|---|---|---|---|---|
| B1 | 18 | 38 | 116–296 (**260**) | 77–186 | 33–62 |
| B1-P1 | 18 | 38 | 119–305 (**125**) | 78–190 | 34–63 |
| B1-z | 18 | 38 | 123–309 (**129**) | 80–192 | 34–63 |
| B1-gw (B1-gw-P1) | 23 | 43 | 121–301 (265; P3 **130**) | 82–191 | 38–67 |
| B1-fetch | 24 | 98 | 104–254 (110) | 101–248 | 44–82 |
| B2, for comparison | 18 | 92 | 98–248 (**104**) | 95–242 | 38–76 |

- Plain B1 pays the cold external leg over the full colo→origin distance. P1, B1-z and B2 move it next to the origin.
- B1's regional tier hits are fast (38 vs 92 ms for B2), because a non-EU user's upper tier is modelled in their region rather than next to the EU origin (if the tier is global, see the A2 topology sensitivity). That gives the B1 family a slightly better blended miss and mean per tile. But the regional tier's u is probably lower than 0.5 in small regions, which narrows the gap.

**Cost:**

| Variant | R | S | M | L | XL | Slope $/M | Break-even vs today |
|---|---|---|---|---|---|---|---|
| B1, B1-P1, B1-z | $77 | $88 | $149 | $380 | $996 | 0.308 | 326M |
| B1-gw, B1-gw-P1 | $81 | $104 | $229 | $700 | $1,956 | 0.628 | never |
| B1-fetch | $77 | $89 | $157 | $410 | $1,086 | 0.338 | 384M |
| B2 (comparison) | $71 | $71 | $71 | $71 | $71 | 0 | 125M |

- Add $6.69/month while the D1 + Tigris fallback data is kept.
- B1 − B2 = $2 + $0.30 × N below about 76M requests/month and $1.40 + $0.308 × N above: $6 at R, $78 at M, $309 at L, $925 at XL. v1's "move to B2 at ~300M" was not a switching point. The real question is whether per-request logic is worth $0.31 per million requests.
- **HA comparison (derived):** B1 plus fallback data ($84 at R) is cheaper than B2-HA ($126) below about **150-170M requests/month**.

**Which metrics survive** (critic #3):

| Variant | Hit counts | Miss metrics | Per-apex RPS alert, colo dashboards | Fallback to D1 + Tigris |
|---|---|---|---|---|
| B1, B1-P1, B1-z | zone analytics / `Cf-Cache-Status` only | Worker | lost | yes |
| B1-gw, B1-gw-P1 | gateway sees every request | callee | kept | yes |
| B1-fetch | Worker sees every request | Worker | kept | yes |
| B2 | zone analytics only | origin logs | lost | no |

v1's "keeps metrics, canary and fallback for about $6/month" was wrong for plain B1: hits never run the Worker. Keeping per-request metrics needs B1-gw (+$4 at R, +$320 at L) or B1-fetch (+$0 at R, +$30 at L).

**Regions:** CF edge everywhere; origin in FSN or HEL (AX41), or PAR/AMS/WAW (Scaleway).

**Ops:** about 3.5 h/month: CF stack 1.75, node 1.5, extra vendor 0.25 (§7 basis).

**Migration:** 10-17 engineer-days.
- Node image and config: 2-4 days.
- Extend [#245](https://github.com/immich-app/geoshenanigans/pull/245) with a single-file Tigris publish and a node agent: 2-3 days.
- Worker origin selection, fallback and canary: 1-2 days.
- Observability (node_exporter + vmagent into the existing VictoriaMetrics; extend the `tiles-worker` Grafana module): 1-2 days.
- Workers Cache spike: 2-5 days. Then cutover. B1-gw adds 1 day; B1-fetch skips the spike (8-12 days).

**Risks:**
- A single node. Mitigation: fallback, RAID1, and a tested restore of about 25-60 minutes from Tigris.
- AX41/EX44 are "-LTD" offers sold while supply lasts.
- No official Terraform provider for Robot. If IaC is required, use Hetzner Cloud: a CX43 (€16.49, one copy on local disk; v2.2) or CPX42 + volume (€88). Scaleway (€40) also has an official provider.
- v2.2: on a CX43 instead of an AX41, every B1 cost above drops by about $50/month (origin $21 instead of $71).
- Outbound connection reuse from the Workers runtime is undocumented (X2, run at R-level traffic).

#### B2. Cloudflare zone proxy and Smart Tiered Cache in front of one EU origin, no Worker

**How it works:**
- `tiles.immich.cloud` becomes a proxied record to the origin. A Cache Rule makes `.mvt` and JSON cacheable; they are not cached by default ([default cache behaviour](https://developers.cloudflare.com/cache/concepts/default-cache-behavior/)).
- **Smart Tiered Cache is free on every plan, including Free.**
  - The availability table lists Free "Tiered Cache: Yes, Smart Topology: Yes" ([tiered cache](https://developers.cloudflare.com/cache/how-to/tiered-cache/)).
  - Since 2025-09 it is the free base tier of Smart Shield ("Get started for free", [Smart Shield](https://developers.cloudflare.com/smart-shield/get-started/)), together with connection reuse, which "reduces overall connections to your origin by 30% on average" ([connection reuse](https://developers.cloudflare.com/smart-shield/concepts/connection-reuse/)).
  - Only "Smart Shield + Argo", listed from $5/month, is paid (B2a). v1's "re-check Free availability" is resolved.
- Smart Tiered Cache "selects the single closest upper tier for each of your website's origins" ([Smart Tiered Cache](https://developers.cloudflare.com/smart-shield/configuration/smart-tiered-cache/)), so the upper tier sits next to the origin.
- Style and TileJSON become static files with short TTLs. Tile URLs either carry the deploy key (deploy at most monthly) or stay stable, with origin ETags and `stale-while-revalidate` (§7).
- Origin Rules that override DNS or Host are Enterprise-only ([origin rules](https://developers.cloudflare.com/rules/origin-rules/)).

**Speed (modelled lower bound, weighted):**
- Hit 18 ms under today's routing, 10 ms with local-PoP routing. A plain CDN hit measured 19.2-22.6 ms in London.
- Tier hit 92 ms. Full miss 98-248 ms; with only the upper-tier→origin leg cold, **104 ms**. Blended 95-242 ms. Mean per tile 38-76 ms.
- The upper-tier→origin connection is modelled as about 100% warm (shared facts), so real misses should sit near 98-104 ms plus the 5-20 ms calibration margin. That replaces v1's unsupported "should be nearer the warm figure".
- Europe 41 ms warm against today's modelled 79 and measured 172-365. North America East 135, East Asia 281, Oceania 342 (§5.2): outside Europe one origin is slower than today's *modelled* floor, though not than today's measured misses.

**Cost:** **$71/month flat** (node $65.91 + Tigris staging $5.19) at every scenario. Workers fees go away. Tiering needs no plan upgrade. Break-even against today is **125M** requests/month (120M marginal).

**B2-s: the same on a small node (v2.2).** A Hetzner Cloud CX43 ($18.42) plus one staging copy ($2.60) costs **about $21/month flat**, which breaks even with today at about **26M** requests/month.
- Modelled with the measured planet server time: full miss 100-250 ms (106 cold-external), mean per tile 38-76, first load 110-169 (§5.7).
- Behind Smart Tiered Cache the origin sees about 0.7 req/s at R and 150 req/s at XL. The CX43's 8 vCPUs and 16 GB cover that many times over, and origin egress (≤ 7 TB/month at XL) fits the 20 TB included.
- What the AX41 still buys: RAID1, a guaranteed 1 Gbit port (Hetzner Cloud publishes no bandwidth guarantee), and room for several copies. On the CX43 a failed node is rebuilt from a Tigris pull (19-31 minutes of download) or a snapshot (restore time undocumented).
- Staff time is the same as B2.

**Risks:**
- The per-hit Worker metrics, the per-request fallback and custom logic are lost. Hit metrics come only from zone analytics.
- The CDN terms allow limiting Free/Pro/Business zones that serve "a disproportionate percentage of pictures, audio files, or other large files" ([terms](https://www.cloudflare.com/service-specific-terms-application-services/)). 18 KB tiles are unlikely to qualify, but leaving the Worker gives up the Developer Platform cover. Low risk.
- **No HA.** `stale-if-error` triggers only on 500/502/503/504 ([cache-control](https://developers.cloudflare.com/cache/concepts/cache-control/)), so a dead origin fails misses; hits keep serving. Either accept that, or use B2-HA.

**B2-HA: second origin plus failover** (critic #5).
- AX41 + Scaleway EM-A610R-NVMe (vendor-diverse) + a 2-origin failover LB at $10 (range $5-15, see B3 steering): **$126/month flat** (range $121 to $152 with 2× AX41 and a $15 LB). Break-even against today is **233M**.
- The critic estimated about $156 and 294M with 2× AX41 and a $15 LB. The fix-up found a 2-origin failover LB costs $5-10 and a cheaper second vendor.
- Ops 4.75 h/month, migration 10-16 days.
- v2.2: two CX43s behind a $10 failover LB cost about **$49** (one vendor, two locations).

**B2a: Argo Smart Routing (Smart Shield + Argo).**
- Price: $5/month ([plans](https://www.cloudflare.com/plans/network-cdn.md)), plus a per-GB charge "transferred between Cloudflare and your origin", first 1 GB included ([how charges accrue](https://developers.cloudflare.com/billing/understand/how-charges-accrue/), [usage-based billing](https://developers.cloudflare.com/billing/understand/usage-based-billing/)). The last published rate is $0.10/GB ([blog, 2017](https://blog.cloudflare.com/argo/)); the current rate is shown only in the dashboard. Cache hits are not charged.
- It applies to zone cache misses ([CDN reference architecture](https://developers.cloudflare.com/reference-architecture/architectures/cdn/)). It is not documented for Worker `fetch()`, though it plausibly applies to a proxied-hostname fetch (B1-z). It never applies to Tigris/D1 reads or to Workers Cache.
- Cost on an origin-fetch basis: R $79, S $88, M $135, L $312, XL $783 in total. Upper bound if all egress were billed: +$30 / $95 / $455 / $1,805 / $5,405.
- Speed: Cloudflare claims "30% (on average) faster". With Smart Tiered Cache the leg Argo optimises is short, and without Argo it is "still intelligently routed". Expected gain: small (unverified).
- **Verdict:** not a default. Turn it on for one month during X2 (about $8) and keep it only if the non-EU miss p50 drops measurably.

**Planet-in-RAM variant:** not worth it. An AX102 with 128 GB costs EUR 159-259/month plus a EUR 129 setup fee ([AX matrix](https://www.hetzner.com/dedicated-rootserver/matrix-ax/)). It saves about 0.08 ms per origin read against NVMe (PM9A3, 80 µs QD1, per [datasheet](https://toko.nale.co.id/wp-content/uploads/2024/02/PM9A3-NVMe-U.2-Datasheet_for-07_v1.5.pdf)), while network hops are 5-300 ms.

#### B3. Cloudflare plus three origins (EU, US-East, Singapore)

**Node choices** (cheapest robust mix):

| Region | Node | $/month | Notes |
|---|---|---|---|
| EU | Hetzner AX41 | 65.91 | |
| US-East | OVH RISE-1 Vint Hill, 2×960 | 94 | The base 32 GB / 2×512 config was unavailable in VIN on 2026-11-02 01:44 UTC (HIL in stock), per the [OVHcloud US availability API](https://api.us.ovhcloud.com/v1/dedicated/server/datacenter/availabilities?planCode=24rise01-v2-us). List price $70 + $24 disk ([OVH US Rise](https://eco.us.ovhcloud.com/rise/)). The bandwidth add-on is coded "unguaranteed" in the EU and US alike. |
| Singapore | Akamai g6-standard-6 | 96 | 320 GiB disk: 2 copies for about 3 years at central growth; 3 never fit. 8 TB transfer included, then $5/TB ([types API](https://api.linode.com/v4/linode/types), [transfer](https://techdocs.akamai.com/cloud-computing/docs/network-transfer-usage-and-costs)) |
| **Total** | | **$255.91** + $5.19 staging | |

**US-East alternatives:**
- Vultr vc2-6c-16gb at $80. Its 320 GB disk holds 2 copies for about 1.3 years ([Vultr plans API](https://api.vultr.com/v2/plans)).
- Netcup Manassas, on a 24-month minimum (contract terms in §4.C).
- Hetzner Cloud CCX33 in Ashburn at EUR 141.49 ($158). Its 240 GB disk holds only one copy ([Hetzner GP](https://www.hetzner.com/cloud/general-purpose)).

**Single vendor does not avoid "three APIs and three bills" cheaply** (critic #17; live OVH catalogue and availability APIs, 2026-11-02):

| Region | OVH node | $/month | Stock |
|---|---|---|---|
| EU | RISE-1 FRA, 32 GB, 2×512 | 63.67 (€56.99) | 1H-high |
| US-East | RISE-1 VIN, 2×960 (2×512 unavailable) | 94 | 1H-low |
| Singapore | RISE-1, 2×960 (2×512 unavailable) | 111.71 (€99.99) | "unknown" |
| | or ADVANCE-1 64 GB, 2×960 | 158.63 (€141.99) | 1H-low |
| **Total** | | **269-316**, against 256 for the mixed fleet | |

- The critic's $247 used a Singapore 2×512 config that is unavailable.
- Setup fees equal one month per node, about $223 in total, unless each node is committed for 12 months.
- VIN and HIL are sold only by the separate OVHcloud US entity ([community](https://community.ovhcloud.com/t/non-us-customer-using-ovhcloud-us-portal-for-vps-locations-policy-clarification-needed/53206)), so all-OVH is still two accounts and two bills.
- Vultr-only ($240) or Akamai-only ($288) fleets exist, but their 320 GB disks hold only 2 copies.

**Steering:**
- **Worker**: $0 extra, but it follows `request.cf.continent`, which mis-routes users that today's routing sends to a distant colo.
- **CF Load Balancing** (proxied, geo steering, one pool per region; critic #7):
  - *Published price:* only "Starting at $5/mo" ([plans](https://www.cloudflare.com/plans/network-cdn.md)). The itemised list is in the dashboard ([LB docs](https://developers.cloudflare.com/load-balancing/)).
  - *What is metered:* DNS queries, first 500K included ([usage-based billing](https://developers.cloudflare.com/billing/understand/usage-based-billing/)). Proxied LB "reduces authoritative queries" ([proxy modes](https://developers.cloudflare.com/load-balancing/understand-basics/proxy-modes/)), so proxied HTTP requests are **not** metered and the critic's "about $1,000 at L" case does not arise. Proxied answers keep a fixed 300 s TTL ([TTL](https://developers.cloudflare.com/dns/manage-dns-records/reference/ttl/)).
  - *Last itemised list* (2018, dashboard-derived, **unverified for 2026**, [mattgadient](https://mattgadient.com/geo-dns-cloudflare-vs-route-53-a-look-and-short-test-results/)): 2 origins $5; geo steering +$10; faster or multi-region health checks +$10-15 each; $0.50 per extra 500K queries. A 2026 guide says non-Enterprise geo steering needs "Traffic steering" bought separately ([oneuptime](https://oneuptime.com/blog/post/2026-03-20-cloudflare-global-server-load-balancing-ipv4/markdown)).
  - *Limits:* no per-pool fee is documented. Non-Enterprise accounts get up to 20 pools and 20 endpoints and a 15 s minimum check interval ([limits](https://developers.cloudflare.com/load-balancing/reference/limitations/)).
  - *Estimate:* 3 endpoints **$25/month (range $15-50)**; 6 endpoints **$40 ($30-65)**; a 2-origin failover $5-10. Add ≤ $0.50 of DNS queries at R, at most tens of dollars at L.
  - *Steering scope:* geo steering uses CF regions; PoP steering is Enterprise-only ([geo steering](https://developers.cloudflare.com/load-balancing/understand-basics/traffic-steering/steering-policies/geo-steering/)).
- With LB, "Smart Tiered Cache will select the single best Upper Tier for the entire Load Balancing Pool" ([tiered cache, "Load Balancing interaction"](https://developers.cloudflare.com/cache/how-to/tiered-cache/)). Use **one pool per region**, or US and Asian misses may be funnelled through an EU upper tier.
- LB is cheaper than Worker steering above about **76M requests/month** (45-160M across the LB price range).

**Speed (modelled lower bound, weighted):**
- LB: tier 54, full 60-133 ms (66 with only external legs cold), blended 57-127, mean per tile 28-46. Worker steering: full 78-182 (145).
- By region, full miss warm under today's routing / local routing: NA-East 31, SE Asia 34, NA-West 91 / 88, LatAm 149 / 115, **South Asia 146 / 69, East Asia 144 / 91**, Oceania 206 (Sydney→SIN is 159 ms).
- Today's routing sends Mumbai, Seoul and Taipei misses through MRS, LAX and SJC, so **B3's Asian gain depends on A7** (critic #23). Choose Sydney over Singapore if Oceania matters (3% of traffic).

**Cost:**

| Steering | R | S | M | L | XL |
|---|---|---|---|---|---|
| LB (central $25) | $286 flat (range $276-311) + DNS-query meter | | | | |
| Worker | $267 | $278 | $339 | $570 | $1,186 |

Break-even against today: 550M (LB), 1,287M (Worker).

**Ops:** about 6.5 h/month (LB) or 7 (Worker). **Migration:** 10-14 days (LB) or 12-19 (Worker, including the Workers Cache spike).

**B3-s: the same on small nodes (v2.2).** CX43 in the EU ($18.42) + Vultr vc2-4c-8gb (4 vCPU / 8 GB / 160 GB, 4 TB pooled) in US-East and Singapore ($40 each) + LB $25 + staging $2.60 = **$126/month flat**, against $286 on big nodes.
- Modelled with the measured planet server time: full miss 62-135 ms (68 cold-external), mean per tile 28-46, first load 94-123 (§5.7). That is B3's latency.
- One copy per node; replace per deploy (shared facts).
- Staff time (assumed): 8-13 engineer-days (node image 2-4; snapshot or pull orchestrator 2-3, +1-2 for a second provider; LB pools, Origin CA and AOP 1-1.5; observability 1-2; validation and cutover 1-2) and 4.75 h/month.
- The Fly equivalent without an LB is **Fly-cf** ($98, §4.D).

#### B4. Cloudflare plus six origins

| Region | Node | $/month |
|---|---|---|
| EU | Hetzner AX41 | 65.91 |
| US-East | OVH VIN | 94 |
| US-West | OVH Hillsboro (2×512, in stock) | 70 |
| São Paulo | Vultr | 120 (the API's `location_cost`; the track's $80 was wrong) |
| Singapore or Tokyo | Vultr | 80 |
| Sydney | Vultr | 80 |
| **Total** | | **$509.91** + $5.19 staging |

- OVH has no South American data centre, so B4 cannot be all-OVH. OVH ×5 plus Vultr São Paulo comes to about $532. Sydney there would be RISE-GAME-1 (€64.99, 250 Mbps burst 1 Gbps, 25 TB/month), since there is no RISE-1 in SYD.
- The Vultr nodes' 320 GB disks hold 2 copies for about 1.3 years, never 3.

**Speed (modelled lower bound, weighted):** tier 44, full 50-103 (56), blended 47-97, mean 26-39. Oceania 37 ms, LatAm 76 (66 local).

**Cost:** LB (central $40) **$555 flat** (range $545-580). Worker steering $521 / $532 / $593 / $824 / $1,440. Break-even 1,082M (LB).

**Ops:** about 11 h/month; migration 12-16 days.

**Diminishing returns:** only 9% of measured traffic comes from LatAm, Oceania and US-West combined.

**B4-s (v2.2):** CX43 + 5 Vultr 160 GB nodes ($60 in São Paulo) + LB $40 + staging = **$281/month flat**, against $555. Modelled full miss 51-102 ms (57), mean per tile 26-39, first load 90-110.

#### B5. Bunny CDN in front of the VM origin

**Prices:**
- Standard network (119 PoPs): $0.01/GB EU/NA, $0.03 Asia/Oceania, $0.045 South America, $0.06 Middle East/Africa. No request fees, $1 minimum ([Bunny CDN pricing](https://bunny.net/pricing/cdn/)). That is **$0.0141/GB blended on the measured traffic mix**; v1 used the cdn-layer track's assumed mix.
- Volume network: $0.005/GB, tiered down to $0.002. Its 10 PoPs are Frankfurt, Paris, Chicago, Dallas, Los Angeles, Miami, Hong Kong, Singapore, Tokyo and São Paulo ([network](https://bunny.net/network/)). There are none in the UK, Benelux, the Nordics, New York/Ashburn, India, the Middle East, Africa or Oceania.

**Features:**
- Free Origin Shield, one EU or US site ([docs](https://bunny.net/docs/cdn/performance/origin-shield)).
- Optional request coalescing ([docs](https://bunny.net/docs/cdn/request-coalescing)).
- Perma-Cache back-fills geo-replicated storage, billed as storage only ([docs](https://bunny.net/docs/cdn/perma-cache)). It needs versioned URLs, and a miss is not guaranteed to be stored.

**Speed** (modelled lower bound, weighted, hit ratios from the table above; §5.5):
- **Standard, hits.** Edge RTT, measured from all 48 probes, is 7 ms weighted, against Cloudflare's 4 ms (local colo) and 12 ms (today's routing). Hit about 13 ms, against Cloudflare's 10 (local) and 18 (today). In Europe the gap is within object and zone noise (note f). It is a clear win in South Asia (8 vs 115 ms) and East Asia (12 vs 72 ms), where Cloudflare routes this zone far away.
- **Standard, misses.** Tier hit 90 ms, full miss 96-248, blended 93-243, about the same as B2. With one EU origin, every CDN's full miss is roughly the client→origin RTT plus overheads.
- **Volume.** Edge RTT 26 ms weighted (about 21 ms in Europe, about 150 ms in Oceania). Hit about 32 ms; full miss 100-229.
- **Mean per tile:** Standard 30-64 ms, Volume 42-62, B2 38-76. Volume's higher hit ratio does not pay for the extra RTT on every request.
- Volume does not fix the detoured cities: Mumbai 66 ms via Singapore, Istanbul 37 via Frankfurt, Lagos 110 via Paris, Seoul 35 via Tokyo.
- Bunny has detours of its own: Bogotá→Miami 45 ms, Lagos 56, Nairobi→Johannesburg 61. Dubai and Tel Aviv switched PoP between two runs an hour apart.

**Cost (including the $71 origin):**

| Network | R | S | M | L | XL | Break-even vs today |
|---|---|---|---|---|---|---|
| Standard | $75 | $84 | $134 | $324 | $830 | 250M |
| Volume | $72 | $76 | $94 | $161 | $341 | 152M |

**Risks:** moving `tiles.immich.cloud` off Cloudflare DNS and Workers; losing the Worker metrics; bandwidth fees that scale linearly with traffic.

**Verdict:** the best alternative **on the Standard network** if the Cloudflare routing problem (A7) cannot be fixed and Asian users matter. Volume is cheaper at L/XL, but about 12 ms slower per tile warm (about equal cold), and it does not help the affected cities.

#### B6. CloudFront in front of the VM origin

**Flat-rate plans** ([pricing](https://aws.amazon.com/cloudfront/pricing/), [docs](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/flat-rate-pricing-plan.html)), with no overage charges:

| Plan | $/month | Requests | Transfer |
|---|---|---|---|
| Pro | 15 | 10M | 50 TB |
| Business | 200 | 125M | 50 TB |
| Premium | 1,000 | 500M | 50 TB |
| Premium | 2,250 | 1.25B | 125 TB |
| Premium | 6,000 | 3.5B | 350 TB |

- Sustained overuse means "fewer or more distant edge locations".
- Origin Shield ("Origin load reduction", "High-speed origin routing") is Premium-only ([features by tier](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/flat-rate-pricing-plan.html#pricing-plan-features)).
- **PAYG** ([PAYG](https://aws.amazon.com/cloudfront/pricing/pay-as-you-go/)), CDN only at a 90% edge hit: about $5 at R, $52 at S, $630 at M, $2,809 at L, $8,499 at XL. At the modelled 72% edge hit, Origin Shield request fees add about $162 at L.
- **Speed:** hit 14.6 ms keep-alive from London, 2 ms from Helsinki (HEL POP), Globalping median 4 ms on an AWS logo object (note f). Misses ≈ B2 (one EU origin), with ~16% origin misses via regional edge caches.

| Plan (including the $71 origin) | R | S | M | L | XL |
|---|---|---|---|---|---|
| PAYG | ~$76 | ~$123 | ~$701 | ~$2,880 (~$3,040 at 72% edge hit) | ~$8,570 |
| Flat-rate | $271 | $271 | $1,071 | $2,321 | $6,071 |

**Verdict:** cheap only at R/S on PAYG, and not faster in any way that matters to Immich.

#### B7. Fastly in front of the VM origin

**Prices** ([pricing](https://www.fastly.com/pricing)): free tier 100 GB and 1M requests; NA/EU $0.12/GB, then $0.08; requests $0.01/10k. Shield misses are billed twice ([shielding](https://www.fastly.com/documentation/guides/getting-started/hosts/shielding/)).

**Speed:** hits measured 12 ms in London and 6 ms in Helsinki (Stockholm) on an OSM Shortbread tile (note f). The Johannesburg probe was routed to London. Misses ≈ B2.

**Cost (including the origin):** R ~$106, S $249, M $1,005, L $3,832, XL $10,267 at a 90% edge hit. At the modelled 79%, shield double-billing gives M $1,100, L $4,209, XL $11,207. **$71 if accepted into [Fast Forward](https://www.fastly.com/fast-forward)**; OpenStreetMap's tiles run there ([case study](https://www.fastly.com/customers/openstreetmap)).

**Verdict:** worth an application if FUTO wants a second CDN for free; otherwise too expensive.

#### B8. Other CDNs

| CDN | CDN-only price | Total with origin (S / M / L / XL) | Notes |
|---|---|---|---|
| Gcore | First 1 TB free, then EUR 0.01/GB ([PAYG](https://gcore.com/developers/pay-as-you-go-cdn)) | $71 / $112 / $268 / $686 | Regional surcharges unverified; edge hit ~77%, no shield unless paid |
| KeyCDN | $0.04/GB NA/EU ([pricing](https://www.keycdn.com/pricing)) | $115 / $293 / $960 / $2,507 | No edge compute |
| CDN77 | $990 including 250 TB ([pricing](https://www.cdn77.com/pricing)) | $1,061 flat | |
| Google Cloud CDN | Lookups $0.0075/10k plus a load balancer ([pricing](https://cloud.google.com/cdn/pricing)) | $197 / $631 / $2,257 / $6,100 | |
| Azure Front Door Standard | $35 base plus per GB and per request ([pricing](https://azure.microsoft.com/en-us/pricing/details/frontdoor/)) | $237 / $761 / $2,724 / $7,566 | |
| Akamai, CacheFly | Contract only; third-party $0.035-0.049/GB ([costbench](https://costbench.com/software/cdn-edge/akamai)) | — | Unverified |

None of these was measured. Fees are at the cdn-layer track's 90% edge hit; at the modelled 77-80% they rise modestly.

### 4.C VM-only, no CDN

**C1 (1 EU node serving clients directly).**
- Every request costs RTT + about 3 ms. London residential to Hetzner NBG/FSN averaged **29.4/31.3 ms ICMP**. That is about 33-35 ms per warm tile, roughly today's hit; comparing ICMP RTT with HTTP TTFB flatters direct serving.
- Outside Europe: 90-330 ms per request.
- Traffic-weighted warm per-tile TTFB is 85 ms. The **first map load is 417 ms**, because TCP + TLS and the style→TileJSON→tiles chain all run at origin distance. B2 is 109-168 ms (72-135 with local-PoP routing), and today is 207-311 ms (calibrated).
- Cost: **$71 flat through XL** on Hetzner (unlimited traffic, 1 Gbit/s guaranteed). XL peaks at about 500-660 Mbit/s at an assumed 3-4× peak-to-average.

**C3 / C6 (GeoDNS).** Traffic-weighted warm TTFB is 42 ms (3 nodes) and 34 ms (6 nodes); first map loads are 206 and 163 ms.

- **DNS options:**

  | Provider | Price | Notes |
  |---|---|---|
  | Route 53 | Latency queries $0.60/M; non-AWS HTTPS health checks about $2.75/month each ([Route 53](https://aws.amazon.com/route53/pricing/)) | |
  | Bunny DNS | Free ([pricing](https://bunny.net/pricing/dns/)) | Checks only on port 80, no alerts ([records](https://bunny.net/docs/dns/records)) |
  | Gcore | Free tier with 300 s checks ([docs](https://docs.gcore.com/dns/about-gcore-dns)) | |
  | NS1 | $99+ ([IBM](https://www.ibm.com/products/ns1-connect/pricing)) | |

- **Costs:** $271/month for 3 nodes with one node per region; about $530 with N+1 per region, which direct serving needs; $525-830 for 6-10 nodes.
- **Bandwidth check** (critic #28). The mean is 18 KB. Peak-to-average is an **assumed 3-4×** (*pending production split*: the max 5-minute rate ÷ the 7-day mean, Appendix C). Per-node TB/month follows the measured continent mix.

  | Node | Share | L TB | XL TB | XL avg / 4× peak (Mbit/s) | Included | Overage at XL |
  |---|---|---|---|---|---|---|
  | C1 single EU node | 100% | 18 | 54 | 164 / 657 | Hetzner unlimited, 1 Gbit guaranteed ([AX41](https://www.hetzner.com/dedicated-rootserver/ax41/)) | $0 |
  | C3 EU (EU + AF + ME) | 64.7% | 11.6 | 34.9 | 106 / 425 | Hetzner unlimited | $0 |
  | C3 US-East (NA + SA) | 22.5% | 4.0 | 12.2 | 37 / 148 | OVH VIN 1 Gbps unmetered (plan code "unguaranteed") | $0 |
  | C3 Singapore (Asia ex-ME + OC) | 12.8% | 2.3 | 6.9 | 21 / 84 | Akamai 8 TB, then $5/TB ([prices](https://api.linode.com/v4/network-transfer/prices)) | $0 (Vultr: ~$19) |
  | C6 Vultr SAO + SG/TYO + SYD | 14.6% | 2.6 | 7.9 | ≤16 / 65 per node | 3 × 5,120 GB pooled, then $0.01/GB ([Vultr](https://docs.vultr.com/support/platform/billing/what-is-the-bandwidth-overage-rate)) | $0 |

  - **"Flat" holds through XL.** A 1 Gbit port covers C1 at XL at a 3-4× peak and saturates near 6× average.
  - Scaleway's 500 Mbps guarantee needs the €9.99 1 Gbps option for C1 at XL.
  - Netcup throttles to 300 Mbit/s after 3 TB in 24 h ([netcup](https://www.netcup.com/en/server/root-server/rs-4000-g12.5-iv-24m-eu)). XL averages 1.8 TB/day, so Netcup is unsuitable for C1 at XL.
  - APAC OVH Rise is capped at 25 TB/month, then throttled ([OVH](https://support.us.ovhcloud.com/hc/en-us/articles/4404330636819-All-About-APAC-Region-Servers)).
- **TLS:** HTTP-01 breaks under GeoDNS ([Let's Encrypt](https://letsencrypt.org/docs/challenge-types/)), so use DNS-01. Certificate lifetimes drop to 64 days from 2027-02-10 ([LE](https://letsencrypt.org/2026/02/24/rate-limits-45-day-certs)).
- **DDoS:** Hetzner's free filtering publishes no capacity or L7 figures ([Hetzner DDoS](https://www.hetzner.com/unternehmen/ddos-schutz/)).
- **Ops:** 16-23 h/month with 24/7 urgency (§7 basis).
- **Bring-your-own anycast** is 20-30 engineer-days and not justified. That is an own ASN plus a leased /24 (about $65-130/month, [i.lease](https://i.lease/blog/how-much-does-a-24-ipv4-block-cost-in-2026), unverified), announced over BGP on Vultr ([Vultr BYOIP](https://docs.vultr.com/support/products/network/can-i-use-my-own-ip-space-for-vultr-instances)).
- **Probe population.** The critic expected residential last miles to hurt direct options most. The model says otherwise: direct and CDN paths have the same number of serial round trips, so a real user's access penalty adds equally to both (§5, caveat 5).

**Verdict:** worse first loads than any CDN option, plus DDoS and egress exposure. Not recommended. v2.2 narrows this: it is clearest for **one EU node** (C1, first load 417 ms). Multi-node direct serving reaches 168-212 ms first loads (C3-s/C6-s, modelled). Fly's managed anycast (Fly-direct, §4.D) removes the TLS, GeoDNS and N+1 work, and v2.3 **measured** its 4-region first load at 184-195 ms, equal to today's all-hit first load measured the same way. Self-run GeoDNS fleets (C3-s, C6-s) stay not recommended: they keep the TLS, DDoS and N+1 work that Fly takes over.

#### Small VMs and image/snapshot replication (v2.2)

**Small-node fleets.** From the planet bench (§4.B shared facts), a node needs about 1 GB of RAM, one vCPU and 160 GB of local disk for one copy. Cheapest per metro (image-replication track, `small-vm/img-repl/calc-output.txt`; prices 2026-11-02):

| Provider / plan (≥ 160 GB local disk) | $/month | Included traffic |
|---|---|---|
| **Hetzner Cloud CX43**, EU only (8 vCPU / 16 GB / 160 GB) | **18.42** (€16.49) | 20 TB, then €1/TB |
| **Vultr vc2-4c-8gb** (4 vCPU / 8 GB / 160 GB, 31 locations) | **40** (60 in São Paulo) | 4 TB pooled, then $0.01/GB |
| Lightsail 8 GB / 2 vCPU / 160 GB | 44 | 5 TB (half in Mumbai, Sydney, Jakarta, Malaysia, Hong Kong, São Paulo) |
| DigitalOcean Basic 8 GiB / 4 vCPU / 160 GiB | 48 | 5,000 GiB pooled |
| Akamai Linode 8 GB / 160 GiB | 48 (57.60 Jakarta, 67.20 São Paulo) | 5 TB pooled, then $5/TB |
| Hetzner Cloud CPX32 Singapore / CPX31 Ashburn, Hillsboro | 55.29 / 70.37 | 2 TB / 3 TB |
| AWS EC2 t4g.small + 160 GB gp3 + IPv4 | about 29 | plus $0.09/GB egress |
| Fly.io shared-cpu-1x 1 GB + 150 GB volume (§4.D) | 31.45-35.57 | $0.02-0.12/GB, by destination |

**Small-node C options** (CX43 in the EU, Vultr elsewhere; GeoDNS $5-15; staging $2.60): **C1-s $26**, **C3-s $111** ($209 with N+1), **C6-s $251** ($416 for 10 nodes). All are flat to L. At XL, bandwidth overage takes C1-s to $64, C3-s to $241 and C6-s to $267. Modelled per request with the measured server time: 87 / 44 / 36 ms; first load 423 / 212 / 168 ms (§5.7).

**Seeding another region from a disk image** (docs fetched 2026-11-02; only the Fly fork was run, in v2.3: 43 / 86 / 159 min to `created` for iad / sin / syd, §2.5; no other time below is measured):

| Provider | ~150 GB disk to another region? | How | Image cost |
|---|---|---|---|
| Fly.io | yes | `fly volumes fork <vol> --region <r>`, then `fly machine clone --attach-volume` (§4.D) | a fork is a full volume, billed at provisioned size |
| Hetzner Cloud | yes, server disk only | snapshot; "not location bound… create a new server in any location" ([FAQ](https://docs.hetzner.com/cloud/servers/backups-snapshots/faq/)) | one copy for all locations, about $2.32/month |
| Vultr | yes | "Vultr Snapshots span across all regions" ([FAQ](https://docs.vultr.com/products/storage/snapshots/faq)); restore adds 10-15 min | $7.25/month, one copy |
| DigitalOcean | yes, Droplet disk only | snapshot + `image-action transfer`, free for any number of regions ([docs](https://docs.digitalocean.com/products/snapshots/how-to/change-regions/)) | $8.70/month |
| AWS EC2, Lightsail | yes | EBS/AMI or Lightsail snapshot copy; EBS volumes from snapshots load lazily | $7.25 per region + $2.90-13.05 per copy (EC2) |
| OVH Public Cloud | yes | "distant backup" | about $1.78 per region |
| Akamai | only by cloning a whole Linode | images capped at 6 GB | — |
| Scaleway | no (within one region only) | export to Object Storage, import in the same region | — |

- **Only server disks travel.** Network block volumes cannot be copied to another region on Hetzner, Vultr, DigitalOcean or Akamai. So the image route puts the planet on the server's own disk, which means at least a 160 GB plan. Fly is the exception, because its volumes are local NVMe.
- **The planet does not compress** (measured on 5 × 256 MiB samples: zstd-3 0.974-1.000, gzip-6 0.975-1.000), so snapshots are billed at about the full 145 GB.
- **Pull or image?** On Fly, v2.3's measurements make the pull faster for a new build: about 35 min per region in parallel on performance-2x loaders (volume-write bound; derived from the measured ams seed and t3 rates) against a measured 35 min seed plus 43-159 min of hydration (§4.D). Elsewhere, for a *new* build, neither is clearly faster: the image route starts with a pull on a seed node and adds snapshot, copy and restore steps whose times no provider documents. Images win when the build's image already exists (disaster recovery, adding a region) and on slow disks or ports (Fly shared-cpu volumes at 16 MiB/s; OVH d2-2's 100 Mbit/s port, 3.1 h per pull). Images are provider-specific code and cannot cross providers; one pull script covers a mixed fleet.
- **Lazy hydration** (Fly forks, AWS snapshot volumes): a node can start before its data is local, so cold reads cross regions. Add it to the pool only once the Fly volume reaches `created`, or AWS initialisation has finished. Measured on Fly (v2.3): never-requested tiles took 98 / 167 / 315 ms in iad / sin / syd while hydrating, and 4.3-5.0 ms once `created`.
- **IP churn.** Replacing nodes changes their IPs. Update the LB pool or GeoDNS by API, or use reserved IPs (Hetzner Primary IP €0.50/month).
- **Recommendation:** keep the Tigris pull as the default because it works on every provider. Use snapshots or forks as a single-provider speed-up, mainly for disaster recovery and for adding a region from an existing build.

**Provider catalogue for any origin role** (v2.1: cheapest suitable plan with ≥ 300 GB and 8-64 GB RAM, sized for 2-3 copies; v2.2 shows that a node needs only ≥ 160 GB and about 1 GB RAM, as in the small-node table above; ECB FX; ex-VAT):

| Provider | Plan | $/mo | Traffic | Regions |
|---|---|---|---|---|
| Hetzner dedicated | AX41/EX44 | 65.91 (€59.00) | unlimited @ 1 Gbit | FSN, HEL |
| Hetzner auction | cheapest with ≥2×512 NVMe and ≥32 GB | 83.45 (€74.70 incl. IPv4) | unlimited @ 1 Gbit | FSN, NBG, HEL |
| Hetzner Cloud | CPX42 + 400 GB volume | 98.41 (€88.09) | 20 TB, then €1/TB | NBG, HEL |
| OVH Rise | RISE-1 | 63.67 EU (€56.99) / 70 HIL / 94 VIN (2×960) / 111.71 SGP (2×960, stock unknown); SYD only RISE-GAME-1 72.61 ([Rise](https://eco.ovhcloud.com/en/rise/)) | 1 Gbps unmetered EU/US/CA (coded "unguaranteed"); APAC 25 TB, then throttled | FR, DE, UK, PL, CA, VIN, HIL, SGP, SYD, BOM |
| Scaleway Elastic Metal | EM-A610R-NVMe | 44.68 (€39.99) | unlimited, 500 Mbps guaranteed (1 Gbps option €9.99) | PAR, AMS, WAW |
| Netcup | RS 4000 G12.5 | 62 on a 24-month term (€55.60); about 84 on a 1-month term; Manassas +€8.34 | throttled to 300 Mbit/s after 3 TB in 24 h | NUE, VIE, Manassas, SIN |
| Contabo | Cloud VPS 16 | about 34-39 ([Contabo](https://contabo.com/en/vps/)) | fair use | EU, US, SG, JP, IN, AU |
| Vultr | vc2-6c-16gb | 80 (120 in SAO) | 5 TB pooled, then $0.01/GB ([docs](https://docs.vultr.com/support/platform/billing/what-is-the-bandwidth-overage-rate)) | 33 regions ([API](https://api.vultr.com/v2/regions)) |
| Akamai | g6-standard-6 | 96 (134.40 GRU) | 8 TB, then $5/TB ($7 GRU, $15 Jakarta) ([prices API](https://api.linode.com/v4/network-transfer/prices)) | ~26 cities |
| DigitalOcean | 8 vCPU / 16 GB | 96 ([pricing](https://www.digitalocean.com/pricing/droplets)) | 6 TB, then $0.01/GiB | 13 cities |
| Oracle | A1.Flex 4/24 + 400 GB | 72.48 (first node about 36 with Always Free, now 2 OCPU / 12 GB ([OCI](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm))) | 10 TB free, then $8.50/TB NA/EU | about 50 regions (unverified) |
| AWS | i4g.large | about 113 + $3.65 IPv4 ([vantage](https://instances.vantage.sh/aws/ec2/i4g.large), [VPC](https://aws.amazon.com/vpc/pricing/)) | $90-50/TB | 30+ |
| Fly.io | v2.1: performance-2x 8 GB + 300 GB volume. **v2.2: shared-cpu-1x 1 GB + 150 GB volume** (§4.D) | v2.1 about 137; **v2.2 31.45-35.57** ([pricing](https://docs.fly.io/about/pricing)) | $20/TB NA/EU, $40 APAC/SA, $120 Africa/India, priced by destination | 17 ([regions](https://docs.fly.io/reference/regions)) |

UpCloud throttles to 100 Mbit/s past its fair limit ([docs](https://upcloud.com/docs/products/networking/network-transfer/)). OVH APAC VPSs drop to 10 Mbps after 3 TB. Avoid both as high-volume origins.

**Contract terms** (critic #19; 2026-11-02, ex-VAT):

| Node | Monthly | Setup | Minimum term / billing | IPv4 |
|---|---|---|---|---|
| Hetzner AX41-1-LTD | €59.00 ($65.91; $69.00 if billed in USD) | €0 | none; hourly with a monthly cap, cancel immediately ([Robot](https://docs.hetzner.com/general/billing-and-account-management/cancellation/cancellations-robot/)) | €1.70 included ([IP pricing](https://docs.hetzner.com/general/infrastructure-and-availability/ipv4-pricing/)) |
| Hetzner AX42 / AX42-1-LTD | €99 / €79 | €49 / €39 ([price list](https://docs.hetzner.com/general/infrastructure-and-availability/price-adjustment/)) | as above | included |
| Hetzner Cloud CPX42 + 400 GB volume | €88.09 | €0 | hourly with a monthly cap ([FAQ](https://docs.hetzner.com/cloud/billing/faq/)) | €0.50, included in the total |
| OVH RISE-1 EU (FRA) | €56.99 | €56.99 (one month) without commitment; €0 with 12 months upfront ([catalogue](https://eu.api.ovh.com/v1/order/catalog/public/eco?ovhSubsidiary=IE)) | monthly, or 12/24 months upfront | included |
| OVH RISE-1 US VIN, 2×960 | $94 ($70 + $24 disk) | $70 without commitment; $0 with 12 months upfront ([US catalogue](https://api.us.ovhcloud.com/v1/order/catalog/public/eco?ovhSubsidiary=US)) | as above; separate OVHcloud US account | included |
| OVH RISE-1 SGP, 2×960 | €99.99 / $111.20 | €79.99 / $89 | as above | included |
| Akamai g6-standard-6 | $96 | $0 | hourly ($0.144) with a monthly cap ([types API](https://api.linode.com/v4/linode/types)) | included |
| Vultr vc2-6c-16gb | $80 ($120 SAO) | $0 | hourly with a monthly cap ([plans API](https://api.vultr.com/v2/plans)) | included |
| Netcup RS 4000 G12.5 | €55.60 on a 24-month term (€66.16 incl. 19% VAT); about €75.21 on a 1-month term | €0 | **24-month minimum** at the base price ([netcup](https://www.netcup.com/en/server/root-server/rs-4000-g12.5-iv-24m-eu), fetched 2026-11-01) | included |
| Scaleway EM-A610R-NVMe | €39.99 or €0.11/h | "No setup fee", but monthly billing has a one-month commitment fee; hourly has none ([pricing](https://www.scaleway.com/en/pricing/elastic-metal/)) | hourly or monthly | included; extra €0.005/h |
| AWS (any EC2 row) | — | $0 | per second | **$0.005/h = $3.65/month** per public IPv4 ([VPC pricing](https://aws.amazon.com/vpc/pricing/)) |

- **VAT:** all prices are ex-VAT.
  - Hetzner invoices EU business customers with a VAT ID under reverse charge. It **charges 8% sales tax to Texas customers** unless an exemption certificate is filed, and invoices other unlisted non-EU countries without VAT ([Hetzner VAT](https://docs.hetzner.com/general/billing-and-account-management/billing-at-hetzner/value-added-tax/)).
  - FUTO is in Austin, Texas, so a FUTO-billed Hetzner account would likely pay +8%.
  - VAT and sales-tax treatment by OVH, Scaleway, Akamai and Vultr for a US business is unverified.

**Candidates from the regional-storage handoff, ruled out** (critic #29; each with one number refreshed on 2026-11-02):

| Candidate | Why not |
|---|---|
| AWS Lambda MicroVMs | Max disk 32 GB ([images](https://docs.aws.amazon.com/lambda/latest/dg/microvms-images.html)); max lifetime 8 h including suspended time; endpoints require a JWE token: "There is no unauthenticated access option" ([launching](https://docs.aws.amazon.com/lambda/latest/dg/microvms-launching.html)). |
| DigitalOcean MicroVMs | Public preview, available only in MKC1 ([availability](https://docs.digitalocean.com/products/microvms/details/availability/)). |
| EC2 + EBS gp3 | m7g.large $59.57 ([vantage](https://instances.vantage.sh/aws/ec2/m7g.large)) + 400 GB gp3 $32 (AWS's $0.08/GB-month example rate, [EBS](https://aws.amazon.com/ebs/pricing/)) + IPv4 $3.65 ≈ **$95/month** before $0.09/GB egress ([DT price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/us-east-1/index.json)). 1.4× the AX41, and EBS is single-AZ. |
| Modal sandboxes | Lifetime "up to 24 hours" ([sandboxes](https://modal.com/docs/guide/sandboxes)); not a 24/7 origin. |
| Daytona | 2 vCPU / 8 GiB / 300 GiB running 24/7 ≈ $191/month ([pricing](https://www.daytona.io/pricing)). The 256 GiB disk cap in earlier screening (unconfirmed) is below 2 copies (259 GiB). |
| E2B | Pro: 20 GiB storage and 24 h sessions, plus a $150/month plan fee ([pricing](https://e2b.dev/pricing)). |
| Blaxel | Scales to zero after about 5 s idle. 8 GB running 24/7 ≈ $242/month of compute plus $36 for a 300 GB volume ([pricing](https://blaxel.ai/pricing)). |

### 4.D Edge platforms

#### Fly.io small-Machine fleets (v2.2; replace v2.1's D1 and D2)

v2.1's D1 (Fly behind Cloudflare, $233 at R) used performance CPUs, and both it and D2 (Fly direct, 8 regions, $474; shared-cpu-2x at R/M) paid for two-copy 300-400 GB volumes. v2.2 re-sized both to small Machines and added two variants. Sources: the fly-docs track (Fly documentation, plus the pricing-page calculator constants, which reproduce Fly's published prices to the cent), the planet bench (§4.B shared facts) and the latency model (§5.7). **v2.3 ran a 4-region fleet live** (ams, iad, sin, syd; §2.5) and folds in the maintainer's design inputs: a new cluster per data update, fewer copies than regions, shared-cpu-1x/2x Machines, and suspend. Fly behind Cloudflare was not tested live and stays modelled.

**Node.** One per region.
- **shared-cpu-1x, 1 GB:** $6.70 (iad/ewr), $6.95 (ams), $7.73 (fra), $8.50 (sin/syd), $8.76 (nrt), $10.82 (gru). Regional markups (1.000-1.615) apply to CPU and RAM only ([pricing](https://docs.fly.io/about/pricing), [fly.io/pricing.md](https://fly.io/pricing.md)). VersaTiles needs the 1 GB (it was OOM-killed at 256 MB on the box). shared-cpu-2x, the maintainer's other suggested size, doubles the CPU baseline; use it where a region nears the limits below.
- **One 150 GB volume:** $22.50 at $0.15 per provisioned GB-month, with no regional markup. It holds only the planet: the OS lives in the image's root filesystem. Fly's "GB" is GiB (inferred from the docs' `df` example), so the 129.7 GiB planet fits with about 20 GiB spare, about 1.5-2 years at the central growth rate (§4.B).
- **Deploy overlap.** With a new cluster per data update (below), old and new volumes overlap for hours, not days: about $0.03 per volume-hour, so $0.50-1 per 4-region deploy including the temporary loaders. v2.2's tables assume a 3-day overlap (+$2.25 per region per month at monthly deploys) and are kept as the upper end.
- **Snapshots off.** Daily snapshots are on by default and would add about $10.40 per volume per month; the data can be rebuilt from Tigris.
- Shared anycast IPv4 and IPv6 and the first 10 certificates are free.
- **Total: $29.20 (iad) to $33.32 (gru) per region**, or $31.45-35.57 with v2.2's 3-day overlap. The volume is 70-80% of it.

**Why a shared-cpu-1x is enough (measured live, §2.5).**
- **CPU.** A shared vCPU gets 5 ms per 80 ms (6.25%) as a baseline, plus a burst balance ([CPU performance](https://docs.fly.io/machines/cpu-performance)). Fly's proxy terminates TLS ([services](https://docs.fly.io/networking/services)), so the Machine serves plain HTTP.
- **Measured on Fly:** 2.7-3.1 ms of CPU per request; **20 req/s clean and 30 marginal on the baseline alone**; 279-285 req/s while bursting. The box emulation had predicted 45 req/s at the baseline.
- **Load.** The busiest region averages 3.4 req/s at R with Fly-direct (14 at an assumed 4× peak), and 0.9 behind Cloudflare. That is 1.5-6× headroom on the baseline, and burst credit covers spikes.
- **Volume IO.** Shared CPUs get 4,000 IOPS and 16 MiB/s ([volumes](https://docs.fly.io/volumes/overview)). That is fine for serving up to M (VersaTiles reads about 1.1× the bytes it serves). It binds VersaTiles' start-up (20 s to read 362 MB of directories) and bulk loading, which is why seeding uses a temporary performance-2x loader (64 MiB/s).
- **Smallest shape per region** (busiest region; assumed 4× peak at ≤ 80% of saturation; VersaTiles; v2.2, from the emulation):

  | Variant | R | S | M | L | XL |
  |---|---|---|---|---|---|
  | Fly-direct N=3 (ams 65% of requests) | shared-1x | shared-2x | shared-8x | perf-2x | 2× perf-4x |
  | Fly-direct N=10 (lhr 29%) | shared-1x | shared-1x | shared-4x | perf-1x | perf-4x |
  | Fly-direct N=17 (ams 15%) | shared-1x | shared-1x | shared-2x | shared-8x | perf-2x |
  | Fly-cf N=3 (ams 67% of misses) | shared-1x | shared-1x | shared-2x | shared-8x | perf-2x |
  | Fly-wkr N=6 | shared-1x | shared-1x | shared-1x | shared-4x | perf-1x |

  shared-cpu-2x and 8x are extrapolated from the emulated 6.25% and 25% profiles. Fly's measured baseline saturation is about 60-80% of the emulation's (26-35 against 45 req/s; 20-30 sustained), so from S upward expect one size larger in the busiest region (derived; one run per setting). R is unaffected.

**Variants.**
- **Fly-direct** (was D2): clients go straight to Fly's anycast address; no CDN. Regions: N=3 ams, iad, sin; **N=4 adds syd (the fleet measured in v2.3)**; N=6 adds sjc, nrt, syd; N=10 is ams, fra, lhr, iad, ord, sjc, gru, sin, nrt, syd; N=17 is every region.
- **Fly-cf** (was D1): a Cloudflare zone proxy (Cache Rules for `.mvt` and JSON, `_fly-ownership` TXT record, Full (strict)) in front of the same fleet, **with Tiered Cache off**. The origin then sees the 26.2% per-colo miss stream. Not tested live (X11).
- **Fly-cf-stc:** Fly-cf with Smart Tiered Cache on. Fly's anycast origin address "significantly reduce[s] the effectiveness" of Smart Tiered Cache, and cloud region hints cover only AWS, GCP, Azure and Oracle ([CF Tiered Cache](https://developers.cloudflare.com/cache/how-to/tiered-cache/)). On Free, Pro and Business, Smart is the only tiered topology. So it collapses to one upper tier beside ams, and extra Fly regions add nothing. Priced with 2 Machines in ams.
- **Fly-wkr:** a thin Worker with Workers Cache (as B1) that fetches misses with `fly-prefer-region` set to the region nearest the colo. The origin sees 13% of requests. It keeps per-request metrics (with the gateway) and a D1 + Tigris fallback.

**Cost** (infrastructure $/month; machines + volumes + v2.2's 3-day deploy overlap + egress; fork rollout, so no Tigris staging; 30-day months; `small-vm/model/out/cost_smallvm_tables.md`):

| Variant | N | Machines per region | R | S | M | L | XL |
|---|---|---|---|---|---|---|---|
| Fly-direct | 1 | 1 / 2 | 38 / 69 | 55 / 86 | 166 / 193 | 589 / 614 | 1,581 / 1,606 |
| | 3 | 1 / 2 | **102** / 199 | 118 / 214 | 217 / 305 | 625 / 702 | 1,738 / 1,815 |
| | **4 (measured fleet, v2.3; derived)** | 1 | **128-136** | ~150 | ~250 | ~660 | ~1,775 |
| | 6 | 1 / 2 | **202** / 398 | 218 / 413 | 315 / 503 | 703 / 881 | 1,836 / 2,004 |
| | 10 | 1 | 335 | 351 | 442 | 838 | 1,976 |
| | 17 | 1 / 2 | 562 / 1,119 | 578 / 1,134 | 666 / 1,220 | 1,043 / 1,563 | 2,171 / 2,667 |
| Fly-cf | 1 | 1 / 2 | 33 / 65 | 37 / 69 | 62 / 93 | 171 / 198 | 429 / 454 |
| | 3 | 1 / 2 | **98** / 194 | 102 / 198 | **125** / 221 | **222** / 310 | 507 / 589 |
| | 4 (derived) | 1 | ~123-131 | ~135 | ~158 | ~255 | ~540 |
| | 6 | 1 | 197 | 201 | 225 | 320 | 608 |
| | 17 | 1 | 558 | 562 | 584 | 672 | 944 |
| Fly-cf-stc | 1 (ams) | 2 | 64 | 66 | 77 | 124 | 254 |
| Fly-wkr | 3 | 1 | 103 | 116 | 189 | 464 | 1,220 |
| | 6 | 1 | 203 | 216 | 288 | 563 | 1,319 |
| *Today (A0)* | | | *15* | *33* | *134* | *514* | *1,525* |

- **N=4 rows** add syd (shared-cpu-1x $8.50 + volume $22.50 + overlap) to the N=3 rows. For Fly-direct the R range is about $128 with hours of overlap (including about $1 per deploy) up to $136 with v2.2's 3-day overlap; Fly-cf N=4 is $123-131.
- **At R the cost is almost all volumes.** Fly-direct N=4 is $31 of machines, $90 of volumes and $6 of egress.
- **Egress is what grows.** Fly prices it by destination ([fly.io/pricing.md](https://fly.io/pricing.md)): $0.02/GB to North America and Europe, $0.04 to Asia-Pacific, Oceania and South America, $0.12 to Africa and India. That is **$0.0237/GB** blended on the measured traffic mix ($0.0267 if a third of Asia is India), and the same for any number of regions. Fly-direct pays $6 / $21 / $107 / $427 / $1,282 from R to XL; Fly-cf pays 26% of that and Fly-wkr 13%. Traffic to Cloudflare is billed like any other: the docs make no CDN exception.
- **Fly-wkr adds Workers fees** ($6 at R, $302 at L, $902 at XL), as B1 does.
- **Against today:** Fly-direct is never cheaper up to XL, because its egress slope ($0.43 per million requests) is close to today's $0.51. Fly-cf N=3 is cheaper from about 230M requests/month, Fly-cf-stc from about 120M.
- **Two Machines per region** roughly doubles each figure.
- **v2.1's D1 and D2 re-priced** with small nodes, Fly side only (fly-docs track): D1 (fra, iad, sin) $99 / $125 / $232 at R / M / L, against $233 / $241 / $362; D2 (8 regions) $267 / $368 / $752, against $474 / $586 / $1,363.
- go-pmtiles instead of VersaTiles needs bigger shapes from S upward: Fly-direct N=3 becomes $121 at S, $278 at M and $747 at L.

**Speed.** Fly-direct N=4 is measured live; every other row is a modelled lower bound under today's Cloudflare routing (§5.7). The measured row uses Globalping TTFB on new connections, so compare it with today's figures from the same run.

| Variant | Hit | Full miss warm–cold (cold ext) | Mean per tile | First map load |
|---|---|---|---|---|
| **Fly-direct N=4, measured live** | = per tile | never-requested **55** | warm **51**, blended 53 | **184-195** |
| *Today, measured in the same run* | *54* | *459* | *blended 159* | *195-590 (379 expected)* |
| Fly-direct N=3 / 6 / 10 / 17 (model) | = per tile | 54–73 / 46–55 / 38–41 / 34 | 54 / 46 / 38 / 34 | 205–265 / 182–213 / 154–174 / 148 |
| Fly-cf N=3 / N=6 (model) | 18 | 61–98 / 51–79 | **30–40** / 27–35 | **95–110** / 92–103 |
| Fly-cf-stc (any N; model) | 18 | 99–227 (105) | 38–72 | 108–159 |
| Fly-wkr N=3 / N=6 (model) | 18 | 84–158 (121) / 74–139 (102) | 30–44 / 28–42 | 97–121 / 95–116 |
| *Today (v2: measured / calibrated)* | *24–35* | *300–488* | *96–154* | *207–311* |
| *B2 / B3 (v2.1, modelled)* | *18* | *98–248 / 60–133* | *38–76 / 28–46* | *109–168 / 94–122* |

- **Fly-direct has no miss tail, measured.** A never-requested tile costs about 4 ms more than a warm one, against today's 459 ms misses. Blended per tile, the fleet is faster than today in every region (§2.5): Europe 43 vs 121 ms, North America East 32 vs 185, SE Asia 42 vs 442, Oceania 30 vs 350. On warm tiles alone it is slower where it has no Machine: NA-West 118 vs 77 and Latin America 220 vs 75.
- **The model was close.** Like for like, the 3-region model was 4-9 ms optimistic (measured about 58 / 63 ms, derived, against 54), inside v2's 5-20 ms margin. The Fly-cf figures come from the same model and are probably similarly close, but they are unmeasured (X11).
- **First map load.** Measured with one method, Fly-direct's first load (184-195 ms) equals today's all-hit first load (195) and beats today's expected (379) and cold (590) loads. v2.2's claim that Fly-direct's first load is slower than any Cloudflare-fronted fleet rested on modelled figures from a different method (§5.4). Cloudflare still sets up connections faster (36 vs 54 ms TCP + TLS, measured), so Fly-cf should gain something on first loads; X11 measures how much.
- **Anycast detours** (measured): Jakarta → lax edge → iad Machine, Mumbai → ams, Dubai → sin, Bucharest → ams at 135-140 ms TCP on one shared IPv4 but 40 ms to lhr on another.

**A new cluster per data update** (the maintainer's design; times measured live in v2.3 unless marked).
1. **Create a new app per data key** (for example `tiles-<key>`) with the same image. The OS is in the image's root filesystem; each region gets one 150 GB volume for the planet.
2. **Fill one volume per region.** Two routes:
   - **Parallel per-region pull (recommended for new builds).** In every region at once, create the volume and a temporary performance-2x loader, pull the staged planet from Tigris **through t3** with an in-stream hash and the 20k-tile check, then attach a shared-cpu-1x serving Machine to the volume. About **35 min in every region** (derived), bound by the 64 MiB/s volume write cap: 34.6 min measured in ams (through the Fly endpoint), and t3 delivered 98.5-216 MB/s into iad, sin and syd, above that cap. A full pull outside ams was not run. Do not pull through the Fly endpoint: it stalled in sin (7.8 MB/s).
   - **Seed and fork.** Seed ams (34.6 min), then `fly volumes fork <seed> --region <r>`. Forks reached `hydrating` in 27-43 s and **`created` after 43 / 86 / 159 min** (iad / sin / syd, all three at once from one source; round-trip bound at about 15-54 MB/s). End to end that is 1.3-3.3 hours. Forks suit disaster recovery and adding a region from an existing build, and need no download code on each node.
3. **Do not route traffic until the volume is `created` and the health check reports the new key.** A hydrating fork can be mounted, but never-requested tiles then take 98-315 ms (cross-region reads to the source host), and VersaTiles takes 50-90 s to start instead of 20 s. Fly's proxy routes to any started, healthy Machine, so the rollout script must gate on volume state, not on health. Pre-reading the file is unnecessary: hydration finishes on its own, and afterwards reads are 4.3-5.0 ms in every region.
4. **Cut over.** Behind Cloudflare, switch the origin hostname. Direct, move the public hostname's DNS to the new app after issuing its certificate there (Fly's DNS-challenge route; not tested here). Rollback is switching back while the old app exists.
5. **Delete the old app after a few hours.** Overlap and loaders cost about $0.50-1 per 4-region deploy, plus up to $5 per deploy on the fork route if Fly bills hydration as cross-region transfer (undocumented).
- Name each deploy's volumes uniquely so Fly's unique-zone check does not exhaust hardware zones in small regions. Restoring a snapshot into a different region is not documented; treat it as untested.

**Fewer copies than regions** (maintainer input; routing measured). Fly's edges forward to the nearest region that has a Machine, so a region needs no copy of its own:
- Measured mapping with ams, iad, sin and syd: the lhr, cdg, fra, arn, ams and jnb edges went to ams; ewr, iad, yyz, ord, dfw, sjc, lax and gru to iad; nrt and sin to sin; syd to syd.
- The price is paid by the regions without a Machine. Measured per tile: NA-West 104-118 ms (via iad), Latin America 220-224 (via iad), Africa 160-195 (via ams), South Asia 144-182 (Mumbai via ams).
- Adding a region costs one Machine and one volume: sjc about $29/month (NA-West, 6.2% of traffic; modelled 31-45 ms with a Machine there, about 4 ms off the weighted mean) or gru about $33 (Latin America, 1.7%; modelled 58-67 ms, about 3 ms off the weighted mean). Decide from production per-region numbers (Appendix C).
- Start with ams, iad, sin and syd, as measured. Choose syd over sin only if Oceania matters more than Asia.

**Suspend** (maintainer input; measured live, §2.5). `auto_stop_machines = "suspend"` with `auto_start_machines = true` snapshots the Machine, memory included ([suspend-resume](https://fly.io/docs/reference/suspend-resume/); ≤ 2 GB RAM, no swap).
- **Measured:** the first request after a suspend takes **+0.42-0.49 s**, and page and directory caches survive (warm-tile latency unchanged). The proxy suspends a Machine after **about 6 min** idle. A request wakes the **nearest suspended Machine** rather than going to a running Machine in another region. A stopped Machine takes **20.6 s** to serve again, because VersaTiles reloads its directories at the volume cap.
- **What it saves:** compute only, $6.70-8.50 per region per month. The volume ($22.50) is billed either way, so suspend saves at most 23-27% of a region.
- **When it saves anything:** at today's traffic a region with a Machine is rarely idle for 6 minutes. Even Africa (0.6% of requests) would see a request about every 32 s with direct serving, or a miss every ~2 min behind Cloudflare (derived). Only nightly troughs would suspend.
- **Where it makes sense:** a second Machine per region for peaks or HA (with two Machines in a region the proxy kept one running), the volume-less designs below (compute is most of their cost), and staging clusters.
- **Cautions:** never use `stop` with VersaTiles on a volume. Fly invalidates the suspend snapshot on deploys, host migrations and maintenance, which turns the next wake into a 20 s cold boot, so keep a running Machine in busy regions. On the S3 app, proxy-triggered resumes were slower than API resumes (0.87-6.8 s against 0.43-2.4 s), for unknown reasons. Network connections are stale after a resume.

**Networking and reliability.**
- Fly's proxy routes to the "least loaded, closest Machine". Cross-region routing happens only when every Machine in the local region is unhealthy or at its hard limit ([load balancing](https://docs.fly.io/reference/load-balancing)), or, as measured, when a region has no Machine at all.
- Fly's connection setup is slower than Cloudflare's: 54 vs 36 ms traffic-weighted TCP + TLS (measured).
- A volume is one NVMe slice on one host, not replicated ([volumes](https://docs.fly.io/volumes/overview)). The data is immutable and rebuildable, so one Machine per region with N ≥ 3 already fails over to the next region (Europe to iad is about +80 ms at N=3, modelled). Use 2 Machines per region at N=1, or in the EU if a cross-region hop is unacceptable.
- **No SLA below Enterprise** (99.9%, support from $2,500/month; [SLA](https://fly.io/legal/sla-uptime/)).
- Region capacity for a 150 GB volume is not guaranteed. Fly flags sjc and gru as "in high demand", and a March 2026 forum thread reports zero capacity in ORD plus a stuck snapshot restore. Capacity and control-plane incidents block creating and forking, not serving. The v2.3 tests had no capacity problems in ams, iad, sin or syd.
- Fly-direct has no shared cache or L7 shield in front of it, so a scraper or flood becomes an egress bill: 1 Gbit/s for a day is about 10.8 TB, roughly $256.

**Staff time (assumed; basis in §7.1).** Fly-direct **6.5-11.5 engineer-days**, 1.75 h/month: container image 1-2; `fly.toml`, checks, certificates, DNS and CI 1-1.5; per-update cluster rollout script 2-3; observability 1-2; validation 1-2; cutover 0.5-1. The v2.3 test scripts (seed, fork polling, serve, probes) cover part of the image, rollout and validation work. Fly-cf adds 0.5-1 day for the Cloudflare zone (2.0 h/month). Fly-wkr adds 3-7 days for the Worker and the Workers Cache spike (3.0 h/month). Fly's real saving is in ops: no OS to patch, automatic TLS, automatic failover.

**Verdict (v2.3).** The **Fly volume fleet is the only alternative measured end to end**: 51-55 ms per tile worldwide with no miss tail, a first map load of 184-195 ms, and about $128-136/month at R for ams, iad, sin and syd. Build it as a new cluster per data update with parallel t3 pulls. Serve it **direct** unless the Cloudflare-in-front test (X11) shows a worthwhile gain, or the shield and the egress slope matter more than a second vendor in the path: Fly-cf costs about the same at R and cuts egress to about 26% at L. **Do not** size Fly with performance CPUs or two-copy volumes, do not route to hydrating forks, and do not expect Smart Tiered Cache to use more than one Fly region.

#### Tigris-backed Fly Machines without volumes (v2.3)

The maintainer asked whether the planet could stay in Tigris (cheaper, in the same regions as Fly) with Fly compute next to the bucket, and pointed to Tigris's S3-as-filesystem work. v2.3 checked the documentation and source code (`tigris-backed-results.json`, docs part), simulated a server reading from remote storage on the box (sim part), and ran two variants live on Fly (§2.5).

**Why it is tempting: cost.**
- A Global Tigris bucket is billed once however many regions cache it: **$2.59/month per planet copy** ($5.19 with two copies during a deploy), against $22.50 per region for a 150 GB volume ([Tigris pricing](https://www.tigrisdata.com/pricing.md)).
- GETs are $0.50 per million: $1.65-2.15/month at today's 3.3M misses, or about $6.30 if a direct fleet fetched all 12.6M tiles with no local cache. One full-copy volume pays for 45M GETs a month in its region, about 17 misses/s sustained; today's worldwide miss rate is about 1.3/s.
- Tigris egress into Fly is free.

**What the docs and code show.**
- **TigrisFS** (v1.2.4, Apache-2.0, a GeeseFS fork) is a poor fit for random 4-60 KB reads from a 139 GB file on a small VM. It turns off the kernel page cache across opens (`KeepPageCache = false`), uses a 5 MiB read-ahead on every new file handle, and has a disk cache with no size limit or eviction (a `FIXME` in the source). Several cache flags are defined but never read. Read-only keys still mount read-write (issue #31), and listing deadlocks are still open.
- **"S3 as a filesystem for runners"** is most likely Fly Sprites: data in Tigris with a sparse 100 GB NVMe read-through cache per Sprite. Fly replaced its JuiceFS base with SBD (ext4 on an NBD device), which is not available outside Sprites. Sprites themselves do not fit: 100 GB of disk against a 139 GB planet, and hot storage at $0.50/GB-month.
- **Tigris Cloud Disk** is beta and closed source (NBD; local disk cache off by default; read-only mounts on several machines undocumented).
- **TAG, the Tigris Acceleration Gateway** (Apache-2.0, v1.24.0), is an S3-compatible caching proxy that keeps 1 MiB blocks in an LRU on local disk and coalesces concurrent misses. Its `size_threshold` (1 GB by default) means a 139 GB object is never cached unless raised; today's 1 MB chunks are fine. A first range miss costs two upstream GETs, and its defaults are sized for 4-8 GiB pods.
- **go-pmtiles** caches directories only (4.28 GB decoded, so they do not fit in 2 GB) and fetches every tile from the bucket.

**Simulation** (box; a delay proxy in front of the planet emulating a storage round trip *d*). VersaTiles with an HTTP source reads the 353 MB leaf section once at boot (445-592 MiB peak, so it fits a 1 GB Machine that can be suspended) and then needs exactly **one GET per tile**: server time is about *d* + 5 ms (5.8-6.8 ms at *d* = 1, 10.5-10.9 at 5, 20-21 at 15). go-pmtiles in 2 GB re-fetched a leaf for 33-83% of tiles. VersaTiles cannot sign S3 requests (it needs presigned URLs, which last at most 7 days, or a local signing proxy), and after an emulated resume its range splitting turned 8 tiles into 454 GETs.

**The three options:**

| Option | Status | Per tile | $/month, 4 regions at R | Verdict |
|---|---|---|---|---|
| (i) **Fly-s3**: stock go-pmtiles reading the planet as one big object | measured live | worldwide 246 warm / 612 never-requested; in the Machine iad 466-543, sin 746-864 | ~45-50 (derived) | **Not recommended.** Tigris did not cache the 139 GB object outside fra |
| (ii) **Fly-tigrisfs**: TigrisFS mount + go-pmtiles or VersaTiles | measured live (ams) and on the box | in the Machine cold 341, warm 130, repeat 7; cold capacity 19-33 tiles/s | ~45-55 (derived) | **Not recommended** (above) |
| (iii) **Fly-chunkcache**: every directory in RAM + today's 1 MB chunk objects + a local chunk cache or a TAG sidecar | modelled from the simulation and live chunk reads | about 55-80 once pre-warmed; 130-670 ms on a chunk cold in its Tigris region | **~40-60** | **A later cost cut** |

Option (iii) in detail:
- **How.** Keep today's chunk layout and export the directories once per deploy. At boot, load all 2,917 leaf directories compressed into RAM (about 353 MB) and decode on demand. Serve tiles from an LRU of whole chunks on the root filesystem (survives suspend, about 8 GB) or a 10-20 GB volume ($1.50-3). On a miss, range-GET the tile and fill the whole chunk in the background. Keep a warm connection pool to Tigris: t3 where it has a region, the Fly endpoint in gru, syd and jnb. The no-code variant runs TAG on localhost (tuned for ≤ 2 GB, long TTL) with VersaTiles or a small server in front.
- **Latency (modelled).** A local chunk hit is like the volume case (4-5 ms at the server). A chunk cached in the Tigris region adds about 15-35 ms (live: third and later chunk GETs took 15-42 ms in ams and sin, 20-110 ms in iad). A chunk cold in its Tigris region costs 130-670 ms (live first reads, §2.6), so every region must be pre-warmed after each deploy (about 140k chunks × 4 regions ≈ 560k GETs ≈ $0.28) and retention checked (X13, X14). At an assumed 30-80% local hit ratio that adds about 3-25 ms to the measured 51-55 ms: **about 55-80 ms per tile** worldwide.
- **Cost.** Machines $30.65 (1 GB, 4 regions) + storage $2.59 ($5.19 during an overlap) + GETs $1.30-6.30 + optional cache volumes $6-12 + egress $6: **about $40-60/month**, against $128-136 with full volumes. Compute becomes most of the bill, so suspend now saves a real share.
- **Risks.** Code to write and maintain, or TAG to tune for 2 GB. Every deploy starts cold in every region, and §2.6 shows that the regions a bucket lists are not hot. Tigris's per-region cache size and eviction are undocumented, and its tail is real (Prisma measured about 1% of reads over 300 ms in Frankfurt). A Tigris incident would then stop serving everywhere, not just deploys. VersaTiles needs request signing, and its range splitting after connection resets must be fixed before suspend is used.
- **When.** After a volume fleet runs, if saving about $70-90/month matters, and once the pre-warm test (X13) and Tigris support (X14) show that regional copies stay hot for days. Not the first move.

**D3. Bunny-only (no VMs).**
- **How it works:** chunk objects in Bunny Storage Edge SSD, replicated to about 5 regions at $0.02/GB per region ([storage](https://bunny.net/pricing/storage/)). A standalone Edge Script acts as the pull zone's origin and does the directory lookup and range read. Scripts run after the cache by default ([middleware](https://bunny.net/docs/scripting/middleware)), so hits are bandwidth-only (an inference from the docs).
- **Script pricing:** $0.20/M requests plus $0.02 per 1,000 CPU-seconds ([scripting pricing](https://bunny.net/docs/scripting/pricing)).
- **Limits:** 128 MB memory, 50 subrequests ([limits](https://bunny.net/docs/scripting/limits.md)); 500 RPS sustained per storage zone ([storage limits](https://bunny.net/docs/storage/limits)).
- **Misses (modelled lower bound, §5.5).**
  - Assumptions: five Edge SSD replicas (Frankfurt, New York, Seattle, Singapore, Sydney); the script runs at the serving PoP (unverified; the [docs](https://bunny.net/docs/scripting/) say only "close to your users"); 5 ms storage service time (unverified); two serial reads per miss, because 128 MB of script memory cannot hold 4.3 GB of decoded directories.
  - Results: Standard 75-167 ms weighted, Volume 72-125.
  - Europe gets 69 ms on Standard, worse than B5's 41, because Frankfurt is the only EU replica. More EU replicas cost $0.02/GB-month each.
  - With the leaf directory cached, one read: 46 ms (Standard) or 53 ms (Volume).
  - There is no shield, so every edge miss (22% Standard, 16% Volume) runs the script.
- **Cost** (Edge Script requests at the modelled 22% / 16% edge-miss ratio; v1 assumed 10%):

  | Network | R | S | M | L | XL |
  |---|---|---|---|---|---|
  | Standard | ~$33 | $45 | $115 | $376 | $1,071 |
  | Volume | ~$30 | $35 | $62 | $166 | $442 |

- **Effort:** 2-4 weeks to port the Worker. No PMTiles-on-Bunny implementation exists.
- **Magic Containers is ruled out:** 100 GB volume maximum and 10 MB/s volume throughput ([volumes](https://bunny.net/docs/magic-containers/persistent-volumes)).

---

## 5. Latency model

**Method** (latency-measure track, re-run as `synthesis/v2/model.py`):
- **Measurements.**
  - 48 Globalping probes: 43 tagged `eyeball-network`, 5 `datacenter-network`; edge RTT median 6 ms, Europe 3 ms (caveat 5). Queried with no account, inside the 250-tests/hour free limit ([spec](https://api.globalping.io/v1/spec.yaml)).
  - Miss and hit rounds read the colo from `cf-ray` and the breakdown from `Server-Timing`.
  - ICMP min-RTT was taken from each probe to candidate origin metros: Hetzner fsn1/ash/hil/sin and Vultr tyo/syd/sao/jnb/bom/sgp. Examples: [fsn1](https://api.globalping.io/v1/measurements/2M56r2JKrsZdggNWX00021QWK), [ash](https://api.globalping.io/v1/measurements/26sMrTtlcIPsDyhst00021QWM), [sin](https://api.globalping.io/v1/measurements/2ysMR8JHRrpSnnpXo00021QXI), [syd](https://api.globalping.io/v1/measurements/2juyBsKv0NOYs8kaw00021QXI).
- **Hops.** Edge = probe→serving-colo TCP RTT. Colo→X = ICMP min probe→X minus the probe's edge RTT.
- **Routing variants.**
  - *Today:* the colo Cloudflare uses for this zone now. Colo→X comes from a proxy probe whose local colo is that colo (for example MRS→Paris); four probe/target pairs fall back to a triangle bound.
  - *Local:* the probe's own colo, i.e. A7 fixed.
  - Multi-origin options with LB pick the origin nearest the serving colo. Worker steering picks by continent.
- **Warmth variants, identical for every option.**
  - *Warm:* every server-side connection on the miss path is open, the isolate is warm, storage caches are warm.
  - *Cold:* every server-side leg after the edge colo opens TCP + TLS 1.3, counted as 3× the leg. That includes Cloudflare-internal tier hops, plus today's serial D1 metadata query on a cold isolate (A0 only).
  - *Cold external* (sensitivity): only legs that leave Cloudflare are cold.
  - The client connection is warm in every per-tile figure. A first map load adds 2 edge RTTs.
- **Tier structure** (critic #10, #27):
  - hit = edge + overhead;
  - tier hit = hit + colo→upper-tier RTT + 2 ms lookup;
  - full miss = tier hit + the path beyond the upper tier;
  - blended = u × tier + (1 − u) × full, with u = 0.5 everywhere. For Bunny, u is set so the origin-miss floor is 13%.
  - Smart Tiered Cache and Bunny's shield put the upper tier next to the origin: colo→upper ≈ colo→origin, and upper→origin is 3 ms.
  - Workers Cache is modelled with it in the colo's region (15-50 ms away). The sources conflict on whether the tier is regional or global; §4.A2 gives the global-tier sensitivity. The Worker is modelled as running at the upper tier, and B1 then fetches the origin over the full distance. B1-P1 runs the miss-path Worker next to the origin; in B1-z the zone tier sits next to the origin.
- **Today's architecture (A0/A1) in the same model:** edge + Worker overhead (20 ms measured beyond D1 + Tigris) + RTT to the nearest D1 replica + 2 + RTT to the nearest Tigris region + 5.
- **Direct (C):** RTT to the nearest origin + 3. First load = 2 RTT + style + TileJSON + tile, all at origin distance.
- **First map load** = 2 × edge + style hit + TileJSON hit + slowest tile. Clustered view-miss probabilities apply (0.40 per-colo; 0.24 for an origin fetch with tiering; §5.4).

**Parameters** (ms unless stated):

| Parameter | Value | Source / status |
|---|---|---|
| CDN hit overhead | 6 | measured: London keep-alive 19.2 ms at a 12-14 ms edge; Globalping 10-city server time median 10 ms (the model is 4 ms low there) |
| Worker hit overhead today | 12 | calibrated: matches keep-alive London; 13 ms low against first-request Globalping hits |
| Worker miss overhead today, beyond D1 + Tigris | 20 | measured 17-24 ms (LHR pinned forced misses) |
| Same after A1 / thin miss Worker / gateway run | 12 / 3 / 5 | assumptions |
| Upper-tier lookup | 2 | assumption |
| Upper tier → origin, when the tier is beside the origin | 3 | assumption (FRA↔FSN ≈ 4 ms) |
| Origin service time | 3 (v2.1 rows); **5.1** (v2.2 rows) | v2.1: assumption from the extract (0.1-0.8 ms). v2.2: **measured on the planet** under emulated small Fly shapes, p50 / p99 at 5 req/s 5.1 / 9.1 ms (VersaTiles) and 10.2 / 17.6 (go-pmtiles default). v2.1 rows are therefore 2-7 ms optimistic on full misses |
| Fly proxy overhead (v2.2 rows) | 7 | measured: median `firstByte − tcp` on the public app `debug.fly.dev` from the 20 probes with a Fly edge ≤ 20 ms |
| D1 / Tigris service time | 2 / 5 | Tigris's own `Server-Timing` 0-8 ms per cached read (measured); D1 assumed |
| Workers Cache colo → regional upper tier | 15-50 | assumption by region: EU 15, NA-E 15, NA-C/W 20, LatAm 40, AF 50, ME 30, S-Asia 40, E-Asia 40, SE-Asia 30, OC 20 (X1 measures) |
| u | 0.5 | assumption; modelled 0.43 (0.57 at 31-day retention), range 0-0.7 |

**Weights.**
- Measured continent split: EU 62.9, NA 20.8, AS 11.0, OC 3.0, SA 1.7, AF 0.6%.
- The NA split (13.5:7.5:9 East:Central:West) and the Asia split (1.5:3:5:4 Middle East:South:East:SE) are **unsourced v1 assumptions** (*pending production split*; Appendix C derives them from colo tags).
- Region figures are medians of per-probe values, with 2-9 probes per region.
- **Misses are weighted by request share** (critic #8, *pending production split*). Smaller colos probably miss more, so non-EU/NA regions likely carry a larger share of misses than of requests. Sensitivity: with an EU/NA hit ratio of 76.5% and 60% elsewhere (still 73.8% overall), the non-EU/NA share of misses rises from 16.3% to 24.9%. Full miss warm, today's routing:

  | Option | Request-share weights | Miss-share weights | Δ |
  |---|---|---|---|
  | A0 today (model) | 86 | 89 | +3 |
  | A2 Workers Cache | 104 | 109 | +5 |
  | B1 | 116 | 131 | +15 |
  | B2 / B2-HA / B2a | 98 | 112 | +14 |
  | B3 (LB) | 60 | 67 | +7 |
  | B4 (LB) | 50 | 53 | +3 |
  | B5 Bunny Standard | 96 | 109 | +14 |

  Single-EU-origin options lose about 14-15 ms and multi-origin options 3-7 ms. The miss-share weighting therefore strengthens the case for B3 slightly. Run Appendix C Q7 to replace this.

**Caveats:**
1. "Wtd" is a weighted mean of regional medians of per-probe medians (2-3 samples each). It is not a p50.
2. The model is a **lower bound**. Colo→X is ICMP minimum minus edge RTT, which underestimates the real path. The CDN overhead was measured on hits. Origin time and the upper-tier hop are assumed. Doubling the regional upper-tier hop adds about 18 ms (warm) or 55 ms (cold) to every A2/A3/B1-family tier hit and full miss.
3. **Not modelled:** Tigris-internal state, D1 replica routing tails, browser caches and mobile access. Together these are the calibration residual (§5.1).
4. Samples are small: one probe per city.
5. **Probe population: read the absolute figures as best case** (critic #26).
   - 43 of 48 probes carry Globalping's network-level `eyeball-network` tag ("part of an eyeball network (your regular ISP)", [Globalping](https://blog.globalping.io/new-globalping-feature-eyeball-and-data-center-network-tags/)). 5 are datacentre probes: Ashburn AWS, Miami Vultr, Bogotá EdgeUno, Cairo LightNode and Dubai M247.
   - Several "eyeball" probes sit in hosting, IX or personal ASNs: Frankfurt AS200461; "Los Angeles" Flexential, actually served by ATL and counted as NA-East; Mumbai Extreme IX; Lagos Workonline.
   - Edge RTT (TCP connect, per-probe minimum): p25 / p50 / p75 / p90 = 3 / 6 / 33 / 104 ms. All 9 European probes are at 1-5 ms. Values above 30 ms come from distant-colo routing, not access lines.
   - For comparison: a London FTTP line measured 12-14 ms to LHR. The FCC reports median idle latency to the nearest server of 8-14 ms for fibre, 13-22 for cable and 20-61 for DSL ([FCC MBA report](https://data.fcc.gov/download/measuring-broadband-america/2023/2023-Fixed-Measuring-Broadband-America-Report.pdf), Chart 16). Mobile is likely higher (unverified).
   - **Effect:** a typical user adds Δ ≈ 5-20 ms per round trip to *every* option alike: about +Δ per tile and +4-5Δ per first load. Both the CDN model (probe→edge) and the direct model (probe→origin) already include the probe's own access line.
   - At Δ = 10 ms, B2's hit goes from 18 to 28 ms and today's warm hit from 24 to 34. Ratios shrink and the ranking holds. This partly contradicts the critic: residential access does not hurt direct options more, because they have the same number of serial round trips.

### 5.1 Calibration on today's architecture (critic #2)

| Region | n | Hit measured / model | A0 miss measured single-shot / R5 | A0 model warm / cold / cold ext | D1 measured / model | Tigris measured / model | Cold-isolate init measured |
|---|---|---|---|---|---|---|---|
| Europe | 9 | 20 / 15 | 365 / 172 | 79 / 204 / 155 | 16 / 27 | 312 / 30 | 7 |
| NA-East | 5 | 29 / 13 | 607 / 188 | 59 / 140 / 108 | 39 / 18 | 388 / 21 | 38 |
| NA-Central | 2 | 70 / 48 | 613 / 430 | 116 / 252 / 199 | 59 / 29 | 450 / 32 | 14 |
| NA-West | 4 | 56 / 26 | 598 / 302 | 90 / 199 / 156 | 33 / 23 | 399 / 26 | 12 |
| LatAm | 6 | 96 / 52 | 720 / 526 | 167 / 499 / 280 | 158 / 107 | 435 / 33 | 76 |
| Africa | 4 | 42 / 32 | 824 / 1,338 | 185 / 480 / 304 | 86 / 75 | 524 / 27 | 56 |
| Middle East | 3 | 88 / 67 | 450 / — | 93 / 134 / 119 | 14 / 6 | 240 / 9 | 0 |
| South Asia | 2 | 132 / 121 | 469 / 494 | 183 / 301 / 255 | 21 / 25 | 281 / 28 | 5 |
| East Asia | 4 | 115 / 78 | 912 / 1,448 | 144 / 277 / 225 | 56 / 23 | 657 / 26 | 28 |
| SE Asia | 5 | 49 / 24 | 860 / 756 | 48 / 55 / 53 | 62 / 3 | 695 / 6 | 54 |
| Oceania | 4 | 20 / 15 | 1,093 / 569 | 71 / 176 / 135 | 390 / 23 | 670 / 26 | 0 |
| **Weighted** | 48 | 35 / 24 | 488 / 300 | 86 / 206 / 157 | 39 / 26 | 370 / 27 | 14 |

| Per-probe comparison (model − measured) | n | Median error | Median ratio model/measured |
|---|---|---|---|
| Worker hit (model edge + 12) vs measured first-request hit | 47 | −13 | 0.65 |
| A0 warm model vs single-shot miss | 47 | −526 | 0.17 |
| A0 cold model vs single-shot miss | 47 | −392 | 0.40 |
| A0 warm model vs warm-isolate (R5) miss | 20 | −315 | 0.25 |
| A0 cold model vs R5 miss | 20 | −189 | 0.58 |
| D1 component (RTT to nearest replica + 2) | 47 | −27 | 0.65 |
| Tigris component (RTT to nearest region + 5) | 47 | −396 | 0.06 |

**What the calibration says:**
- The model reproduces the **fully warm** path where it can be measured: London pinned keep-alive forced misses were 73-91 ms p50, against 80 ms modelled.
- It reproduces plain-CDN hits to within 0-4 ms, and Worker hits 13 ms low on first requests.
- It explains only 17-25% of today's measured miss time with warm connections and 40-58% with cold ones.
  - The D1 component is 13 ms low overall, but Oceania is 390 ms measured against 23 modelled; that is replica routing.
  - **The Tigris component is the failure:** 27 ms modelled against 370 ms measured. The residual is Tigris-internal state (§2.2: the Fly-endpoint floor, per-instance connection state, cold chunks) plus D1 routing tails.
- **Like for like (critic #1).** Modelled lower bound against modelled lower bound, today's architecture is *not* slower than one EU origin worldwide: A0's full miss is 86-206 ms against B2's 98-248 (today's routing).
  - The single-origin advantage is in Europe (79 vs 41 ms) and in regions today's routing already sends to European colos (Middle East, Africa, South Asia). It comes from replacing two remote hops plus a 20 ms Worker overhead with one hop.
  - In North America, LatAm, East/SE Asia and Oceania, the distributed D1 + Tigris floor is lower than one EU origin.
  - B's worldwide gain therefore rests on an NVMe origin not reproducing the Tigris/D1 residual. That is expected (local disk, no remote cache) but unvalidated (X2).
- **Pairing like with like:**
  - The R5 warm-isolate miss (300 ms) is roughly a cold connection with warm data. That pairs with B2 from cold-external (104 ms) to fully cold (248 ms): 1.2-2.9×.
  - The single-shot miss (488 ms) is roughly a cold connection with cold data. That pairs with B2 cold (248 ms): 2.0×.
  - The fully warm London path (73-91 ms) pairs with B2 warm on the same line (47 ms): 1.6-1.9×.
  - So one EU origin is **1.2-3× faster on misses** (measured today vs modelled lower bound for B2, matched by warmth; the +5-20 ms calibration margin is not included; X2 validates). v1's "1.2-5×" paired today's cold with B's warm.
- **Mean per tile, like for like:** today 96-154 ms against B2 38-76, B1 33-62 and B3 28-46 (today's routing, warm to cold). That is **1.3-4×**, or about 2-2.5× pairing warm with warm and cold with cold. Model against model, today's mean is 40-72 ms, about the same as B2's.
- **What transfers to the B estimates:** the hit-overhead error (+0-4 ms CDN, +13 ms Worker hit on first requests) and connection warmth (bracketed by warm and cold). The Tigris and D1 terms do not transfer. Expect B figures 5-20 ms above the warm lower bound in a warm state.

**A family, calibrated** (weighted, today's routing; ranges run from the warm-isolate anchor to the single-shot anchor):

| Option | Hit | Tier hit | Full miss | Blended miss | Mean per tile | Basis |
|---|---|---|---|---|---|---|
| A0 | 24-35 | — | 300-488 | 300-488 | 96-154 | measured |
| A1 | 24-35 | — | 292-466 | 292-466 | 94-148 | measured minus the cold-isolate D1 query (14 ms) and an 8 ms lighter miss path |
| A1b | 24-35 | — | 202-456 | 202-456 | 71-146 | A1 minus 10-90 ms per miss for t3 + module-scope client (public-object deltas; the v2.3 box test found 96-145 ms per fresh-connection read in Europe; Worker path unmeasured, X12) |
| A10 (dropped in v2.3, §2.6) | 24-35 | — | 202-359 (202-289) | 202-359 (202-289) | 71-120 (71-101) | A1b minus the EU/NA cold-chunk term on the cold anchor (115 ms × 84% of traffic; in brackets, 200 ms); APAC may regress |
| A2 | 18-22 | 42 | 318-492 | 180-267 | 61-86 | tier hit modelled (+4 ms CDN offset); full = A1 + 26 ms regional upper-tier detour (modelled) |
| A2, regional tier at u = 0.3 | 18-22 | 42 | 318-492 | 235-357 | 75-110 | topology sensitivity: a regional tier aggregates fewer fills (§4.A2) |
| A2, one global tier (hop ×2) | 18-22 | 60 | 336-510 | 198-285 | 65-91 | topology sensitivity: +18 ms warm hop, u = 0.5 (§4.A2) |
| A1b + A2 (recommended package) | 18-22 | 42 | 228-482 | 135-262 | 49-85 | A1b + the same 26 ms detour |

A2 still beats today's 96-154 ms mean per tile under either topology reading.

### 5.2 Region × architecture (hit · full miss warm/cold, ms; modelled lower bound)

**Today's Cloudflare routing:**

| Region (share) | Edge | A0 measured miss single-shot / R5 | A0 | A2 | B1 | B2 | B3 | B4 | B5 Std | C1 | C3 | C6 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Europe (62.9%) | 3 | 365 / 172 | 15 · 79/204 | 9 · 94/222 | 9 · 56/136 | 9 · 41/97 | 9 · 41/97 | 9 · 41/97 | 12 · 41/93 | 30 | 30 | 30 |
| NA-East (9.4%) | 1 | 607 / 188 | 13 · 59/140 | 7 · 74/167 | 7 · 150/421 | 7 · 135/382 | 7 · 31/68 | 7 · 31/68 | 8 · 135/362 | 124 | 20 | 20 |
| NA-Central (5.2%) | 36 | 613 / 430 | 48 · 116/252 | 42 · 136/283 | 42 · 194/481 | 42 · 174/427 | 42 · 77/136 | 42 · 77/136 | 15 · 147/400 | 136 | 39 | 39 |
| NA-West (6.2%) | 14 | 598 / 302 | 26 · 90/199 | 20 · 110/236 | 20 · 213/581 | 20 · 193/527 | 20 · 91/226 | 20 · 56/105 | 22 · 191/517 | 180 | 77 | 34 |
| LatAm (1.7%) | 40 | 720 / 526 | 52 · 167/499 | 46 · 207/508 | 46 · 298/849 | 46 · 258/735 | 46 · 149/378 | 46 · 76/133 | 14 · 217/597 | 206 | 104 | 55 |
| Africa (0.6%) | 20 | 824 / 1,338 | 32 · 185/480 | 26 · 218/523 | 26 · 189/450 | 26 · 157/336 | 26 · 141/288 | 26 · 141/288 | 56 · 155/326 | 144 | 128 | 128 |
| Middle East (1.2%) | 55 | 450 / — | 67 · 93/134 | 61 · 108/169 | 61 · 88/133 | 61 · 73/94 | 61 · 73/94 | 61 · 73/94 | 46 · 82/154 | 71 | 71 | 71 |
| South Asia (2.5%) | 109 | 469 / 494 | 121 · 183/301 | 115 · 233/426 | 115 · 196/343 | 115 · 146/199 | 115 · 146/199 | 115 · 146/199 | 8 · 149/421 | 138 | 58 | 58 |
| East Asia (4.1%) | 66 | 912 / 1,448 | 78 · 144/277 | 72 · 174/339 | 72 · 311/750 | 72 · 281/666 | 72 · 144/279 | 72 · 118/200 | 12 · 268/749 | 257 | 80 | 80 |
| SE Asia (3.3%) | 12 | 860 / 756 | 24 · 48/55 | 18 · 78/142 | 18 · 234/646 | 18 · 204/562 | 18 · 34/42 | 18 · 34/42 | 10 · 214/583 | 203 | 22 | 22 |
| Oceania (3.0%) | 3 | 1,093 / 569 | 15 · 71/176 | 9 · 91/214 | 9 · 362/1050 | 9 · 342/996 | 9 · 206/590 | 9 · 37/85 | 24 · 342/968 | 331 | 195 | 26 |
| **Weighted** | 12 | 488 / 300 | **24 · 86/206** | **18 · 104/234** | **18 · 116/296** | **18 · 98/248** | **18 · 60/133** | **18 · 50/103** | **13 · 96/248** | **85** | **42** | **34** |

**Local-PoP routing (A7 fixed)**, showing only the regions that change. Europe, NA-East and Oceania are as above.

| Region (share) | Edge | A0 | A2 | B1 | B2 | B3 | B4 |
|---|---|---|---|---|---|---|---|
| NA-Central (5.2%) | 9 | 21 · 89/225 | 15 · 109/256 | 15 · 167/454 | 15 · 147/400 | 15 · 50/109 | 15 · 50/109 |
| NA-West (6.2%) | 10 | 22 · 80/189 | 16 · 100/225 | 16 · 211/584 | 16 · 191/530 | 16 · 88/223 | 16 · 45/94 |
| LatAm (1.7%) | 6 | 18 · 159/474 | 12 · 199/512 | 12 · 257/708 | 12 · 217/594 | 12 · 115/290 | 12 · 66/164 |
| Africa (0.6%) | 5 | 17 · 206/704 | 11 · 256/705 | 11 · 205/577 | 11 · 155/433 | 11 · 139/385 | 11 · 139/385 |
| Middle East (1.2%) | 9 | 21 · 135/337 | 15 · 165/382 | 15 · 112/259 | 15 · 82/175 | 15 · 82/154 | 15 · 82/154 |
| South Asia (2.5%) | 8 | 20 · 130/369 | 14 · 170/440 | 14 · 189/523 | 14 · 149/409 | 14 · **69**/170 | 14 · 69/170 |
| East Asia (4.1%) | 3 | 15 · 101/282 | 9 · 141/364 | 9 · 308/888 | 9 · 268/774 | 9 · **91**/242 | 9 · 91/242 |
| SE Asia (3.3%) | 12 | 24 · 48/55 | 18 · 78/142 | 18 · 244/669 | 18 · 214/585 | 18 · 34/42 | 18 · 34/42 |
| **Weighted** | 4 | **16 · 82/209** | **10 · 100/237** | **10 · 114/306** | **10 · 96/256** | **10 · 53/129** | **10 · 45/104** |

C columns are per-request direct TTFB and do not depend on Cloudflare routing. A local colo can raise a miss when that colo is farther from the origin than today's distant one (Middle East, Africa, South Asia for single-EU-origin options).

### 5.3 Weighted summary per option (ms; modelled lower bound; request-share weights)

| Option | Hit: today / local | Tier hit (warm), today | Full miss warm–cold, today (cold external) | Full miss warm–cold, local | Blended warm–cold, today | Mean per tile warm–cold, today / local | First load warm–cold, today / local |
|---|---|---|---|---|---|---|---|
| A0 today (model) | 24 / 16 | — | 86–206 (157) | 82–209 | = full | 40–72 / 33–67 | 121–169 / 83–134 |
| A1, A1b, A10 (model) | 24 / 16 | — | 78–171 (123) | 74–173 | = full | 38–63 / 31–58 | 118–155 / 80–119 |
| A2 Workers Cache | 18 / 10 | 38 | 104–234 (149) | 100–237 | 71–155 | 32–54 / 25–48 | 102–139 / 64–102 |
| A2-gw gateway | 23 / 15 | 43 | 109–239 (154) | 105–242 | 76–160 | 37–59 / 30–53 | 117–154 / 79–117 |
| A3 | 18 / 10 | 38 | 78–159 (124) | 73–159 | 58–117 | 29–44 / 21–37 | 96–121 / 57–84 |
| B1 | 18 / 10 | 38 | 116–296 (260) | 114–306 | 77–186 | 33–62 / 27–57 | 105–154 / 67–119 |
| B1-P1 | 18 / 10 | 38 | 119–305 (125) | 117–315 | 78–190 | 34–63 / 27–58 | 106–156 / 68–121 |
| B1-z | 18 / 10 | 38 | 123–309 (129) | 121–319 | 80–192 | 34–63 / 27–58 | 107–157 / 69–122 |
| B1-gw | 23 / 15 | 43 | 121–301 (265) | 119–311 | 82–191 | 38–67 / 32–62 | 120–169 / 82–134 |
| B1-gw-P1 (P3) | 23 / 15 | 43 | 124–310 (130) | 122–320 | 83–195 | 39–68 / 32–63 | 121–171 / 83–136 |
| B1-fetch | 24 / 16 | 98 | 104–254 (110) | 102–262 | 101–248 | 44–82 / 38–79 | 127–186 / 90–153 |
| B2 / B2-HA / B2a | 18 / 10 | 92 | 98–248 (104) | 96–256 | 95–242 | 38–76 / 32–73 | 109–168 / 72–135 |
| B3 (LB) | 18 / 10 | 54 | 60–133 (66) | 53–129 | 57–127 | 28–46 / 21–40 | 94–122 / 55–84 |
| B3 (Worker steering) | 18 / 10 | 38 | 78–182 (145) | 72–179 | 58–128 | 28–46 / 21–40 | 96–127 / 57–88 |
| B4 (LB) | 18 / 10 | 44 | 50–103 (56) | 45–104 | 47–97 | 26–39 / 19–33 | 90–110 / 52–74 |
| B4 (Worker steering) | 18 / 10 | 38 | 68–151 (115) | 64–154 | 53–113 | 27–43 / 20–37 | 94–119 / 55–82 |
| B5 Bunny Standard | 13 / 13 | 90 | 96–248 (102) | 96–248 | 93–243 | 30–64 / 30–64 | 87–147 / 87–147 |
| B5 Bunny Volume | 32 / 32 | 94 | 100–229 (106) | 100–229 | 99–227 | 42–62 / 42–62 | 172–223 / 172–223 |
| D3 Bunny-only Standard | 13 / 13 | — | 75–167 (167) | 75–167 | = full | 27–49 / 27–49 | 79–116 / 79–116 |
| D3 Bunny-only Volume | 32 / 32 | — | 72–125 (125) | 72–125 | = full | 38–47 / 38–47 | 162–183 / 162–183 |
| C1 / C3 / C6 direct | 85 / 42 / 34 per request | — | — | — | — | 85 / 42 / 34 | 417 / 206 / 163 |
| **v2.2 rows** (§5.7; measured server time and Fly edges; local first load and local full miss not computed) | | | | | | | |
| B2-s (B2 on a CX43) | 18 / 10 | ≈ 92 | 100–250 (106) | — | ≈ B2 | 38–76 / 32–73 | 110–169 / — |
| B3-s / B4-s (small nodes, LB) | 18 / 10 | ≈ B3 / B4 | 62–135 (68) / 51–102 (57) | — | ≈ B3 / B4 | 28–46 / 21–40; 26–39 / 19–33 | 94–123 / 90–110 |
| Fly-cf N=3 / N=6 (tiering off) | 18 / 10 | — | 61–98 (98) / 51–79 (79) | — | = full | 30–40 / 23–34; 27–35 / 20–29 | 95–110 / 92–103 |
| Fly-cf-stc | 18 / 10 | ≈ B2 | 99–227 (105) | — | ≈ B2 | 38–72 / 31–67 | 108–159 |
| Fly-wkr N=6 | 18 / 10 | ≈ 38 | 74–139 (102) | — | — | 28–42 / 21–36 | 95–116 |
| Fly-direct N=3 / 6 / 10 / 17 | 54 / 46 / 38 / 34 per request (cold 73 / 55 / 41 / 34) | — | — | — | — | 54 / 46 / 38 / 34 | 205–265 / 182–213 / 154–174 / 148 |
| C1-s / C3-s / C6-s (ideal GeoDNS) | 87 / 44 / 36 per request | — | — | — | — | 87 / 44 / 36 | 423 / 212 / 168 |
| **v2.3, measured live** (§2.5; Globalping TTFB on new connections, a different metric from the rows above) | | | | | | | |
| Today, same run | 54 warm | — | never-requested 459 | — | — | blended 159 | 195–590 (379 expected) |
| Fly-direct N=4 (ams, iad, sin, syd) | 51 warm per request | — | never-requested 55 | — | — | blended 53 | 184–195 |

### 5.4 Map views and first map load (critic #21)

- A 1920×1080 viewport with 512 px tiles shows about 15 tiles (13.7 on average, because low zooms show fewer).
- At h = 0.74, independence would give 1 − 0.74^15 ≈ 99% of views with at least one miss. That is an upper bound: misses cluster, because a view usually repeats an area recently seen at the same colo, or none of it.
- A view-level simulation (modelled, not measured) draws views from OSM-derived popularity (§7) and calibrates a per-colo LRU to 73.8%. It gives:
  - **37-41% of views wait on at least one miss** (40-44% for full-screen views), with 3.6 misses per view on average;
  - by zoom: z0-z5 3-6%, z6-z10 15-20%, z11-z13 44-51%, z14-z15 60-69%;
  - with a tiered cache, 21-27% of views wait on an origin fetch;
  - after a deploy that changes tile URLs: 94-97% of views in the first hour, 74-78% over the first day, 41-49% on day 7.
- **First map load** (style, TileJSON and the slowest tile on a new connection; clustered probabilities):
  - today 207-311 ms (calibrated on measured anchors; v1 said 578 under independence);
  - B2 109-168 ms (72-135 local);
  - B3 94-122;
  - C1 417, C3 206, C6 163 (direct);
  - v2.2: Fly-cf N=3 95-110, B3-s 94-123, Fly-direct 205-265 / 182-213 / 154-174 / 148 for 3 / 6 / 10 / 17 regions (§5.7).
  - **v2.3, measured live with a simpler method** (TCP + TLS + 2 × TileJSON + one tile, traffic-weighted Globalping medians; §2.5): today 195 ms when everything hits, 590 when the tile misses, **379 expected** at the 40% view-miss share above; the Fly-direct 4-region fleet **184-195** (190 expected). The measured figures are higher in absolute terms than the model's, so compare measured with measured: the fleet matches today's all-hit load and halves the expected one.
- Using the p50 miss as the "slowest tile" understates every option, and today's most of all, given its heavy tail (Tigris p90 about 960 ms).
- **Production can measure this** while the Worker still runs on every request. Sample 1% of clients by a salted hash of IP + User-Agent, log (hash, ms timestamp, hit/miss) to Analytics Engine for a week, and group each client's requests into bursts separated by more than 1 s idle.

### 5.5 Bunny Standard vs Volume, and D3 (modelled; competitor-cdn-latency fix-up)

- **Bunny Standard edge RTT** was measured from all 48 probes, all cache HITs ([run](https://api.globalping.io/v1/measurements/2VaXmlrlQnzbeR0UO00021QYC)): 7 ms weighted, against Cloudflare's 4 (local) and 12 (today's routing). It is 2 ms in South Asia and 6 in East Asia, where Cloudflare routes this zone far away.
- **Bunny Volume edge RTT** is the RTT to the nearest of its 10 metros: 26 ms weighted, 21 in Europe, 152 in Oceania.
  - Measured by Globalping ICMP to Vultr hosts in [Paris](https://api.globalping.io/v1/measurements/2uZHjPqoYMyFjimhp00021QYC), [Frankfurt](https://api.globalping.io/v1/measurements/2YA0VMyv2GUViJcED00021QYC), [Chicago](https://api.globalping.io/v1/measurements/27R8VxDcz3a75FXN900021QYC), [Miami](https://api.globalping.io/v1/measurements/2RFnisqCyQjq7POQo00021QYC) and [Dallas](https://api.globalping.io/v1/measurements/2kZYYvG1LF4mBoP4w00021QYC), plus the existing SIN/TYO/SAO pings.
  - LAX and HKG use [WonderNetwork](https://wondernetwork.com/pings) city-pair minimum + edge RTT, checked against 80 measured pairs (median error −0.2 ms, p90 19 ms).
  - This assumes Bunny's DNS steers users to the nearest Volume PoP (unverified).
- **D3 miss** = edge + 6 + 3 (script) + 2 × (PoP→replica RTT + 5 ms storage), with replicas in FRA, NY, SEA, SIN and SYD.
  - Weighted 75 ms warm (Standard) and 72 ms (Volume). Europe is 69 ms on Standard, because FRA is the only EU replica, and 50 ms on Volume.
  - Cold figures: 122 / 99 ms with only per-leg setup, or 167 / 125 ms under the v2 all-legs-cold definition used in §5.3.
- B5 hit and miss figures by region are in §5.2. Scripts: `fixups-competitor-cdn-latency/scripts/bunny_model.py`, `summary.py`.

### 5.6 Other worldwide data (corroborating)

**cf-baseline: 10 cities, 4 random rural tiles each, cold.** Medians in ms. Each median is n=4, and the tiles were tiny (75-6,504 B).

| City | Worker | D1 | Tigris |
|---|---|---|---|
| FRA | 196 | 8 | 180 |
| IAD | 296 | 38 | 200 |
| LAX | 310 | 100 | 226 |
| JNB | 352 | 152 | 198 |
| NRT | 478 | 8 | 458 |
| GRU | 512 | 140 | 366 |
| DFW | 624 | 46 | 498 |
| SIN | 768 | 73 | 648 |
| SYD | 1,056 | 349 | 546 |
| BOM | 1,132 | bimodal 118-1,146 | 441 |

Measurements: [2RZBqA0q…](https://api.globalping.io/v1/measurements/2RZBqA0qfJerPZAgx00021QWQ), [2LDvUi5D…](https://api.globalping.io/v1/measurements/2LDvUi5DwQH0kNCjC00021QWR).

**hybrid: 2 fixed z14 tiles, datacentre probes.**
- Miss first byte ranges, in ms: London 166-274, NY 374-448, Dallas 601-751, São Paulo 502-974, Tokyo 414-637, Singapore 680-1,289, Sydney 1,188-1,252, Mumbai 1,332-2,155.
- Tigris 308-1,171 ms outside Europe. D1 6-716 ms; only São Paulo, Mumbai and Sydney were consistently ≥136 ms.
- Hits were 18-71 ms in 9 of 10 cities ([2WM51xEi…](https://api.globalping.io/v1/measurements/2WM51xEiLuqL3wB4D00021QWS)).

**cdn-layer: cold run, n=1 each** ([2SQlu7cR…](https://api.globalping.io/v1/measurements/2SQlu7cRFBhy6mbh500021QWP)).
- First byte: LA 299, NY 568, Tokyo 750, Singapore 884, São Paulo 955, Sydney 2,762, Mumbai 3,972 ms.
- Tigris GET: LA 201, NY 429, Tokyo 717, Sydney 1,387, Mumbai 1,627 ms.

**Hit comparison: Globalping first byte, 10 cities, median (different objects per provider; note f in §3).**

| | CF Worker + Cache API | CF plain CDN (OpenFreeMap) | Fastly | Bunny | CloudFront |
|---|---|---|---|---|---|
| Object (size) | our z14 tile (76 KB gzip) | OpenFreeMap z14 (337 KB gzip) | OSM Shortbread z14 (134 KB br) | font woff2 (22 KB) | logo PNG (17 KB) |
| Globalping median | 26 ms | 12 ms | 3 ms | 3 ms | 4 ms |
| First byte − TCP, 10-city median | 24 ms | 10 ms | 2 ms | 1.5 ms | 3 ms |
| London keep-alive | 24-38 ms (varies by run) | 19.2 ms | 12.0 ms | 12.5 ms | 14.6 ms |

Measurements: [Worker](https://api.globalping.io/v1/measurements/2kepW8F262e6fGrUL00021QWP), [CF CDN](https://api.globalping.io/v1/measurements/2ygsYT7KDqHSJ3cv800021QWQ), [Fastly](https://api.globalping.io/v1/measurements/2HbOOvkebgUG2sMBY00021QWQ), [Bunny](https://api.globalping.io/v1/measurements/2YtJgUWw2Uz27GVs600021QWQ), [CloudFront](https://api.globalping.io/v1/measurements/2Mx3QXi4K0eEMWd1d00021QWQ). The same Bunny object from the same 13 probes an hour later ([run 2](https://api.globalping.io/v1/measurements/2VaXmlrlQnzbeR0UO00021QYC)) moved by a median 1 ms, but by 59-186 ms on three probes whose PoP changed (Dubai, Tel Aviv, Seoul).

**Connection setup from London.**
- TLS completes at 32.9 ms on HTTP/1.1, 33.4 ms on HTTP/2 and 17.2 ms on HTTP/3.
- TTFB from connection start: 67.6 / 59.9 / 51.9 ms.
- `alt-svc: h3` is already advertised. QUIC saves one RTT per new connection ([CF 0-RTT](https://blog.cloudflare.com/even-faster-connection-establishment-with-quic-0-rtt-resumption/); [RFC 8446](https://www.rfc-editor.org/rfc/rfc8446#section-2.3)).

### 5.7 Small-VM fleets and Fly (v2.2; modelled lower bound with measured inputs)

**Method** (`small-vm/model/scripts/model_smallvm.py`; same conventions as `synthesis/v2/model.py`: the same 48 probes, regional medians weighted by request share, warm and cold variants, u = 0.5, h = 0.738). v2.2 tested no Fly or small-VM origin end to end; **v2.3 measured a 4-region Fly-direct fleet live and compares it with this model at the end of this section.** New measurements for the model (2026-11-02, 189 anonymous Globalping tests, no account):
1. **The Fly anycast edge for each probe**: an HTTPS GET to the public Fly app `debug.fly.dev`, which sits on Fly's shared anycast IPv4 pool ([measurement](https://api.globalping.io/v1/measurements/2Cdt0Iz24EnxNfhrA00021Qi9)). `timings.tcp` gives the client → Fly-edge RTT, and the `fly-request-id` suffix names the edge region. All 48 returned 200.
2. **Fly proxy overhead: 7 ms.** This is the median of `firstByte − tcp` over the 20 probes whose Fly edge is ≤ 20 ms away. It includes the in-region hop and the echo app's own time. Cloudflare's equivalent Globalping figure was 10 ms.
3. **ICMP to Vultr hosts in the Fly metros the v2 dataset lacked:** [ams](https://api.globalping.io/v1/measurements/2l1WD9TAppjU4E1tS00021Qi9) (all 48 probes), [lhr](https://api.globalping.io/v1/measurements/2oKfxC8ApMrWvRneY00021QiA), [arn](https://api.globalping.io/v1/measurements/2T8S5wjiqj5GXYEqV00021QiA), [yyz](https://api.globalping.io/v1/measurements/2UYFSsRJQOESSKdee00021QiA), [ewr](https://api.globalping.io/v1/measurements/2yDV494NZFfW20eLU00021QiA), [sjc](https://api.globalping.io/v1/measurements/2oKmN5SHFPZmffzdz00021QiA) and [lax](https://api.globalping.io/v1/measurements/251iI4dvQbhaluv1p00021QiA). The existing v2 pings stand in for the other metros.
4. **Server time** from the planet bench: VersaTiles p50 5.1 ms (p99 9.1), go-pmtiles default p50 10.2 ms (p99 17.6).

**Formulas.**
- **Fly-direct**, per tile (warm) = Fly edge RTT + edge→Machine RTT + 7 + server time. The edge→Machine RTT is to the nearest region in the set, taken from an inter-region matrix built from proxy probes (for example ams→iad 83 ms, ams→sin 160, sin→nrt 64, syd→sin 121).
  - "Cold" adds one edge→Machine RTT for a new TCP connection on Fly's internal WireGuard leg (no TLS).
  - A new client connection adds 2 × the edge RTT, because TLS terminates at the Fly edge.
- **Fly-cf** (tiering off): hit = CF edge + 6. Full miss = hit + colo→nearest Fly Machine + 7 + server time. Cold adds 2 × colo→nearest Fly edge (TCP + TLS at the Fly edge next to the colo) + 1 × edge→Machine.
- **Fly-cf-stc** follows the B2 structure, with the upper tier beside ams plus the Fly overhead.
- **Fly-wkr** follows the B3/B4 Worker-steering structure (regional Workers Cache tier, then a thin Worker fetch with `fly-prefer-region`).
- **C-s** is ideal GeoDNS: RTT + server time, with TLS at the VM.
- **B-s** is the v2 B2/B3/B4 structure with the measured server time in place of 3 ms.

**Weighted results** (ms; today's Cloudflare routing; server = VersaTiles p50):

| Option | Hit | Full miss warm–cold (cold ext) | Mean per tile warm–cold | With server p99 | New client connection, per tile | First map load warm–cold |
|---|---|---|---|---|---|---|
| *Today, A0* | *24-35 (m)* | *300-488 (m)* | ***96-154 (m/c)*** | | | *207-311 (c)* |
| *A1b + A2 (calibrated)* | *18-22* | *228-482* | ***49-85*** | | | |
| *B2, v2.1* | *18* | *98-248 (104)* | ***38-76*** | | | *109-168* |
| Fly-direct N=1 (ams) | = per tile | 93-151 | 93 | 97 | 136 | 323-500 |
| **Fly-direct N=3** | = per tile | 54-73 | **54** | 58 | 98 | 205-265 |
| **Fly-direct N=6** | = per tile | 46-55 | **46** | 50 | 91 | 182-213 |
| Fly-direct N=10 | = per tile | 38-41 | 38 | 42 | 81 | 154-174 |
| Fly-direct N=17 | = per tile | 34 | 34 | 38 | 79 | 148 |
| Fly-direct N=6 / N=17 with ideal anycast (sensitivity) | = per tile | 40-52 / 28 | 40 / 28 | 44 / 32 | 72 / 61 | 150-191 / 118 |
| Fly-cf N=1 (tiering off) | 18 | 94-162 | 38-57 | 39 | — | 109-136 |
| **Fly-cf N=3** (tiering off) | 18 | 61-98 | **30-40** | 31 | — | **95-110** |
| Fly-cf N=6 / N=17 | 18 | 51-79 / 40-55 | 27-35 / 25-29 | 28 / 26 | — | 92-103 / 87-93 |
| Fly-cf-stc (any N) | 18 | 99-227 (105) | 38-72 | 38 | — | 108-159 |
| Fly-wkr N=3 / N=6 | 18 | 84-158 (121) / 74-139 (102) | 30-44 / 28-42 | 30 / 29 | — | 97-121 / 95-116 |
| C3-s / C6-s (ideal GeoDNS) | = per tile | 44 / 36 | 44 / 36 | 48 / 40 | 123 / 97 | 212 / 168 |
| B2-s / B3-s / B4-s | 18 | 100-250 / 62-135 / 51-102 | 38-76 / **28-46** / 26-39 | 39 / 29 / 26 | — | 110-169 / 94-123 / 90-110 |

- With go-pmtiles' default cache instead of VersaTiles, add about 5 ms per tile to Fly-direct (N=3: 59) and 1-2 ms to the CDN-fronted means.
- **Like for like:** these are lower bounds. v2's calibration says to expect real figures for an origin with no Tigris path **5-20 ms above** the warm lower bound. Today's 96-154 is measured; model against model, today is 40-72.

**Per region** (mean per tile, warm, ms; VersaTiles; today's Cloudflare routing for the CF-fronted variants):

| Region (share) | Today, measured | Fly-direct N=3 | N=6 | N=10 | N=17 | Fly-cf N=3 | Fly-wkr N=6 | C3-s | C6-s | B2 (v2) |
|---|---|---|---|---|---|---|---|---|---|---|
| Europe (62.9%) | 60-110 | 42 | 42 | 33 | 31 | 19 | 19 | 32 | 32 | 17 |
| NA-East (9.4%) | 71-181 | 28 | 28 | 28 | 27 | 15 | 17 | 22 | 22 | 40 |
| NA-Central (5.2%) | 164-212 | 42 | 42 | 27 | 17 | 52 | 53 | 41 | 41 | 76 |
| NA-West (6.2%) | 120-198 | 88 | 45 | 45 | 37 | 39 | 31 | 79 | 33 | 63 |
| LatAm (1.7%) | 209-259 | 168 | 168 | 67 | 58 | 61 | 58 | 106 | 57 | 85 |
| Africa (0.6%) | 247-382 | 154 | 154 | 145 | 64 | 51 | 51 | 130 | 130 | 54 |
| Middle East (1.2%) | 183 | 76 | 76 | 70 | 70 | 66 | 68 | 73 | 73 | 63 |
| South Asia (2.5%) | 220-227 | 108 | 108 | 108 | 108 | 123 | 133 | 59 | 59 | 122 |
| East Asia (4.1%) | 324-464 | 100 | 49 | 49 | 49 | 92 | 83 | 82 | 82 | 126 |
| SE Asia (3.3%) | 234-261 | 39 | 39 | 39 | 39 | 21 | 28 | 24 | 24 | 64 |
| Oceania (3.0%) | 164-301 | 155 | 34 | 34 | 34 | 61 | 19 | 197 | 28 | 96 |
| **Weighted** | **105-154** | **54** | **46** | **38** | **34** | **30** | **28** | **44** | **36** | **38** |

"Today, measured" per region = 0.738 × the measured first-request hit + 0.262 × the measured miss (R5 to single-shot). Weighted this way it is 105-154; v2.1's 96-154 uses the 24 ms keep-alive hit at the low end.

**What the measured Fly edges show.**
- **Anycast detours** (one sample per probe; routes vary by ISP and over time):

  | Probe | Fly edge | RTT | Nearest Fly region by ICMP |
  |---|---|---|---|
  | Mumbai | ams | 138 ms | sin, 66 ms |
  | Jakarta | lax | 183 ms | sin, 19 ms |
  | Lagos | lhr | 104 ms | jnb, 74 ms (Cloudflare also sends it to LHR today) |
  | Warsaw | ams | 37 ms | fra, 21 ms |
  | Bucharest | lhr | 41 ms | fra, 33 ms |
  | Bogotá | gru | 116 ms | iad, 65 ms |

  Seoul and Taipei landed on nrt (37 / 52 ms), where Cloudflare sends them to LAX/SJC today. Detours cost Fly-direct about 6 ms on the weighted mean; South Asia is hit hardest (108 against 66 ms with ideal anycast).
- **Fly's edge is farther than Cloudflare's.** Fly has 17 edge regions against Cloudflare's ~340 colos. Europe's median Fly edge is 19 ms away, against 3 ms to Cloudflare, so Fly-direct's European per-tile figure (31-42 ms) is about twice a Cloudflare hit (17). That is also why every Cloudflare-fronted variant wins the first map load.
- **Smart Tiered Cache with an anycast origin** collapses to one upper tier. Cloudflare's region hints cover only AWS, GCP, Azure and Oracle, and on Free, Pro and Business Smart is the only tiered topology. So Fly-cf-stc is no better than B2 (38-72 ms), and Fly-cf must run with tiering off to use more than one region.

**Caveats.**
1. Every anycast edge is a single sample from Fly's shared IPv4 pool. Our app would share that pool, but a dedicated IPv4 or IPv6 can route differently.
2. The 7 ms proxy overhead comes from `debug.fly.dev`, whose Machine placement is unknown.
3. Inter-region RTTs are probe-ping lower bounds, not measurements of Fly's backhaul.
4. Server time and capacity come from emulation on the box, not Fly hardware.
5. The 4× peak is assumed, pending the production split. Tables: `small-vm/model/out/latency_smallvm_tables.md`.

**Model against the live fleet (v2.3).** The live run (§2.5) measured Fly-direct with Machines in ams, iad, sin and syd from the same 48 probes. Warm per tile, ms:

| Region | Model, Fly-direct N=3 | Measured, N=4 warm / never-requested | Measured − model (warm) |
|---|---|---|---|
| Europe | 42 | 41 / 48 | −1 |
| NA-East | 28 | 33 / 29 | +5 |
| NA-Central | 42 | 43 / 44 | +1 |
| NA-West | 88 | 118 / 104 | +30 |
| LatAm | 168 | 220 / 224 | +52 |
| Africa | 154 | 160 / 195 | +6 |
| Middle East | 76 | 88 / 78 | +12 |
| South Asia | 108 | 144 / 182 | +36 |
| East Asia | 100 | 94 / 121 | −6 |
| SE Asia | 39 | 42 / 42 | +3 |
| Oceania | 155 (no syd Machine) | 28 / 35 (syd Machine) | not comparable |
| **Weighted** | **54** | **54 / 59** | like for like about +4-9 (derived) |

- The model holds where a Machine is near (Europe, North America East and Central, East and SE Asia: within 6 ms).
- It is optimistic where edges forward to a distant Machine: NA-West (the sjc and lax edges forward to iad), Latin America (gru to iad), South Asia (Mumbai detours to ams) and the Middle East. The probe-ping inter-region RTTs (caveat 3) understate Fly's backhaul there.
- Like for like (Oceania put back at the model's 155 ms), the measured 3-region figure is about 58 / 63 ms against 54 modelled, inside v2's 5-20 ms calibration margin. Treat the other Fly rows in this section, including Fly-cf, as lower bounds that are probably 5-10 ms low.

---

## 6. Moving away from PMTiles

**On its own it buys almost nothing.**
- **Server time is small next to the network, whatever the format.** On the NL/BE extract:
  - nginx static tree: 0.06-0.18 ms (134k req/s on 4 cores);
  - VersaTiles `.pmtiles`: 0.11-0.23 ms;
  - go-pmtiles: 0.20-0.32 ms;
  - Martin MBTiles: 0.23-0.40 ms;
  - tileserver-gl: 0.28-0.47 ms.

  **On the full planet (v2.2, measured under emulated small Fly shapes; §4.B shared facts)** a single `.pmtiles` is slower, because most requests need a leaf directory that is not cached. p50 / p99 at 5 req/s is 5.1 / 9.1 ms (VersaTiles) and 10.2 / 17.6 ms (go-pmtiles default). Saturation is 766 and 288 req/s per core. That is 25-33× below the extract figures (19k and 9.6k), but still 5-10 ms against network RTTs of 5-300 ms, and ≥ 3× the busiest region's assumed peak at R even on a shared-cpu-1x baseline. A pre-extracted tree would avoid the directory cost, but it was only measured on the extract.

  Network RTT is 5-300 ms. The current design is slow because the PMTiles lookup is split across **two remote services** (D1 for directories, Tigris for bytes), and §2.2 shows the Tigris path is the larger part. PMTiles is not the cause.
- **Keep the single `.pmtiles` file on the origin.** It is Protomaps' native output. Built-in dedup handles 1.43B addressed versus 136.5M unique tiles, and a deploy is one download and a swap.
  - **Directory memory (v2.2).** Decoded, the planet's directories are 4.28 GB. At our load a node does not need them all in RAM. VersaTiles holds the 337 MiB compressed leaf section plus a fixed 100 MB decoded cache, and needs about 1 GB in total. go-pmtiles runs in 512 MB with its 64 MB default; `--cache-size 768` (about 1.4 GB) roughly halves its CPU per request.
  - A 4.5 GB cache matters only above several hundred req/s per node. With every directory cached in 8 GB, go-pmtiles reaches 3,279 req/s on one core.
- **The alternatives lose:**
  - **Pre-extracted tree (OpenFreeMap-style):** 136.5M unique files plus 1.29B hard links. It needs Btrfs inlining; on ext4 the 93M z15 tiles alone would take about 380 GB of 4 KiB blocks. Image builds take hours. It is fastest at the server, which is irrelevant end to end ([OpenFreeMap](https://github.com/hyperknot/openfreemap)).
  - **MBTiles:** 6.6% larger and slower under Martin.
  - **Per-tile objects:** $614 (R2) to $683 (Tigris) in PUTs per full re-upload of the unique tiles ([R2](https://developers.cloudflare.com/r2/pricing/), [Tigris](https://www.tigrisdata.com/pricing/)). That is the worst case; a content-addressed diff upload would be cheaper. Duplicates still need a lookup, which is today's D1 problem again.
  - **Dynamic PostGIS:** a 590-730 GB database plus a 100 GB flat-node file and 6-10 h imports ([osm2pgsql](https://osm2pgsql.org/doc/manual.html)). Slower on low zooms, and the Protomaps schema exists only as a Planetiler Java profile.

**Format changes that reduce client bytes** (independent of hosting; they need our own Planetiler build):

| Change | Measured or claimed effect | Caveats |
|---|---|---|
| **MLT** (MapLibre Tile) | Martin's raw MLT on the wire was **−8%** against gzip MVT (NL extract). −13…−17% only if the MLT is gzipped as well, which Martin and probably Cloudflare do not do. Planetiler's native MLT gives −5…−10% archive size, and `--mlt-shared-dict` cuts the largest tiles by 20-30% ([Planetiler 0.10.0](https://github.com/onthegomap/planetiler/releases/tag/v0.10.0)) | MLT v1 is "for the adventurous" ([MapLibre](https://maplibre.org/news/2026-01-23-mlt-release/)). It needs GL JS ≥ 5.12 ([release](https://github.com/maplibre/maplibre-gl-js/releases/tag/v5.12.0)), Android ≥ 12.1 ([release](https://github.com/maplibre/maplibre-native/releases/tag/android-v12.1.0)) and iOS ≥ 6.20 ([release](https://github.com/maplibre/maplibre-native/releases/tag/ios-v6.20.0)). Current Immich web (maplibre-gl ^6.9) and mobile (maplibre_gl ^0.27) support it, but old installs do not, so it must go on a new `/v2` style and URL. |
| **Brotli / zstd** precompression | −6.2% (whole extract, brotli), −8.4% (8 large city tiles, brotli-11), zstd-19 −4.1…−4.9%. Cloudflare's own re-encoding of gzip at the edge gave about ±1%. | Android (OkHttp) gets gzip only, so a gzip copy is needed. |
| **Max zoom 14, z15 folded into z14** | z15 is 70.05 GB (50.4%) of bytes and 93.1M (68%) of unique tiles. About 2.5× fewer requests at z15+ views (estimate); 4× fewer cache keys at the deepest zoom, so a better hit ratio. | Truncation drops most POIs and some roads and buildings, which start at z15 ([Pois.java](https://github.com/protomaps/basemaps/blob/main/tiles/src/main/java/com/protomaps/basemap/layers/Pois.java)). Folding makes z14 tiles bigger, so **bytes may not fall at all**; the −50% storage figure applies only to truncation. Needs a visual check. |
| **Own Planetiler build** | About 1-2 h on 64 threads (unverified estimate). Needs ≥ 0.5× PBF in RAM (mmap) and 10× PBF in disk ([PLANET.md](https://github.com/onthegomap/planetiler/blob/main/PLANET.md)). | We would own build failures and schema drift. A dual tile set doubles origin disk (§4.B growth table). |

**Verdict:** keep PMTiles. Consider MLT or z14 only after the origin decision, as a client-bytes project on a new `/v2` URL.

---

## 7. Ops, staff time and deploy cadence

### 7.1 Ops and reliability

| Aspect | Today (CF Worker + D1 + Tigris) | CDN-fronted origins, 1 node per region (B1/B2/B3, B-s) | Direct GeoDNS fleet (C3/C6, C-s) | Fly small fleet (Fly-direct; Fly-cf behind CF) (v2.2) |
|---|---|---|---|---|
| Steady ops (basis below; assumption) | 1.5 h/month (+0.25 with Workers Cache) | B2 2.75 h, B1 3.5, B2-HA 4.75, B3 6.5-7, B3-s 4.75 (v2.2 basis), B4 11-11.5 | C3 16 h, C6 23; C3-s with N+1 6.25 (v2.2 basis) | Fly-direct 1.75 h, Fly-cf 2.0, Fly-wkr 3.0 (v2.1 had "3-6", with no breakdown) |
| Urgency of a node failure | n/a | low: cache hits keep serving; B1 misses fall back to D1 + Tigris; B2 misses fail unless B2-HA | high (24/7 pages) | low to medium: with N ≥ 3 the proxy sends a failed region's users to the next region |
| One-time migration | — | B2 8-12, B1 10-17, B3 10-14, B3-s 8-13 engineer-days | 15-25; C3-s with N+1 9-15 | Fly-direct 6.5-11.5, Fly-cf 7-12.5, Fly-wkr 10-17.5 (v2.1: 6-10) |
| TLS | CF | CF edge + Origin CA / AOP | DNS-01 on every node; LE 64-day certs from 2027-02 | automatic at the Fly edge; behind CF, Full (strict) + `_fly-ownership` TXT |
| DDoS / L7 | CF | CF | Hetzner basic filtering (no published capacity); Vultr add-on $10/instance ([Vultr](https://docs.vultr.com/support/platform/billing/how-much-does-ddos-protection-cost)) | Fly-direct: no shared cache or L7 shield, so floods become egress; Fly-cf: CF |
| Patching | none | unattended-upgrades, staggered reboots, cloud-init + Ansible/NixOS (or replace the node per deploy) | same | none (image rebuilds only) |
| Observability | Worker Influx metrics + Grafana module | B1-gw / B1-fetch keep per-request Worker metrics; plain B1 keeps miss metrics; B2 has zone analytics + origin logs. Add node_exporter, vmagent, blackbox probes, alerts for disk/key-mismatch/cert/5xx | log-derived metrics | Fly metrics federated into VictoriaMetrics + logs; Fly-direct sees every request at the origin |
| Data deploy | chunk PUTs (~140k) + D1 rows via #245 | one file per node: 19-31 min at 600-900 Mbit/s from Tigris; daily delta is 0.6-1.7% of bytes, so about 1.5-6 GB/day with block sync; or a snapshot within one provider (§4.C) | same | new cluster per data update (v2.3; fork measured, pull time derived): parallel per-region t3 pull on a temporary performance-2x, ~35 min; or a 35 min seed + fork, `created` after 43-159 min (§4.D) |
| Vendors on the miss path | CF Worker + D1 + Tigris | CF + one host provider | DNS + host | Fly (+ CF for Fly-cf) |

**Ops-hour basis** (critic #15; assumptions):
- The CF Worker stack is 1.5 h/month: alerts, dashboards, Terraform/Wrangler upkeep, automated #245 deploys.
- Workers Cache adds 0.25 h. Dropping the Worker saves 0.5 h.
- Each self-managed node is 1.5 h: patch and reboot review 0.5, monitoring and RAID 0.5, incident reserve 0.5 (about one 6-hour incident per node-year).
- Each extra vendor is 0.25 h, and LB or GeoDNS 0.25 h.
- Direct serving adds 2 h per region for 24/7 urgency, L7 DDoS and DNS-01 TLS.
- **v2.2, Fly:** the per-node basis does not apply. There is no OS to patch, no RAID and no certificate to renew, and the proxy fails over between regions. Fly-direct is 1.75 h: monitoring 0.5, image rebuilds 0.25, a Fly platform incident reserve 0.5, deploy oversight 0.25, and watching egress and abuse 0.25. Fly-cf adds 0.25 for the Cloudflare zone; Fly-wkr adds 1.25 for the Worker and Workers Cache.
- **v2.2, small VMs** (B3-s, C3-s): 1.0 h per replace-per-deploy node (counted per region: C3-s's N+1 second nodes are not counted; counting them would give 9.25 h), 0.25 per VM vendor (two here), 0.25 for the LB or GeoDNS, and 1.0 for the Cloudflare stack (B3-s only). C3-s adds 2.5 for direct-serving urgency (v2.1's basis would give +6).
- **None of these hours is measured.** They are estimates to compare options consistently.

**Staff time and 12-month total cost of ownership** at an **assumed $100/h**. TCO = infrastructure + migration + ops. Latency gains are not monetised. **This is a secondary figure.** In every v2.2 row and most v2.1 rows, 85-100% of the TCO delta against today at R is assumed staff time (B3 and B4 on big nodes: 77-82%). For B2 it is 79% migration, 15% extra ops and 7% infrastructure. Compare options on infrastructure cost and engineer-days, and treat these totals as an illustration at one assumed rate.

| Option | Migration eng-days | Ops h/month | 12-month TCO at R | Δ vs today at R | Δ vs today at L | Payback of migration (months) at L · XL |
|---|---|---|---|---|---|---|
| A0 today | 0 | 1.5 | $1,980 | — | — | — |
| A1 | 1-3 | 1.5 | $3,578 | +$1,598 | +$1,474 | 153 · 51 |
| A1b | 1.5-3.5 | 1.5 | $3,976 | +$1,997 | +$1,748 | 95 · 32 |
| A10 (dropped in v2.3) | 2-4.5 | 1.5 | $4,638 | +$2,659 | +$2,411 | 165 · 45 |
| A2 | 3-8 | 1.75 | $6,664 | +$4,684 | +$3,271 | 47 · 13 |
| A2-gw | 4-9 | 1.75 | $7,513 | +$5,533 | +$7,911 | never |
| A3 | 8-15 | 2 | $11,770 | +$9,791 | +$8,843 | 309 · 49 |
| B1 (P1, z) | 10-17 | 3.5 | $15,927 | +$13,947 | +$11,601 | never · 33 |
| B1-gw | 11-18 | 3.5 | $16,776 | +$14,796 | +$16,241 | never |
| B1-fetch | 8-12 | 3.5 | $13,127 | +$11,147 | +$9,161 | never · 34 |
| **B2** | 8-12 | 2.75 | $12,153 | **+$10,174** | **+$4,190** | **25 · 6** |
| B2-HA | 10-16 | 4.75 | $17,609 | +$15,630 | +$9,646 | 166 · 10 |
| B3 (LB) | 10-14 | 6.5 | $20,903 | +$18,924 | +$12,940 | never · 13 |
| B4 (LB) | 12-16 | 11 | $31,201 | +$29,222 | +$23,238 | never · 580 |
| B5 Standard | 10-15 | 2.5 | $13,895 | +$11,915 | +$8,926 | 112 · 17 |
| C1 | 5-10 | 3.75 | $11,353 | +$9,374 | +$3,390 | 28 · 5 |
| C3 | 15-20 | 16 | $36,453 | +$34,474 | +$28,490 | never |
| D3 Standard | 10-20 | 2 | $14,796 | +$12,816 | +$10,949 | 137 · 30 |
| **v2.2 rows** | | | | | | |
| B2-s (CX43) | 8-12 | 2.75 | $11,552 | +$9,572 | +$3,584 | 22 · 6 |
| Fly-direct N=3 | 6.5-11.5 | 1.75 | $10,528 | +$8,548 | +$8,832 | never |
| Fly-direct N=4 (measured fleet, v2.3; $128 at R) | 6.5-11.5 | 1.75 | $10,836 | +$8,856 | ≈ +$9,250 | never |
| Fly-cf N=3 | 7-12.5 | 2.0 | $11,375 | +$9,395 | +$4,896 | 32 · 8 |
| Fly-wkr N=6 | 10-17.5 | 3.0 | $17,033 | +$15,053 | +$13,388 | never · 196 |
| B3-s (small VMs, LB) | 8-13 | 4.75 | $15,612 | +$13,632 | +$7,644 | 133 · 8 |
| C3-s with N+1 | 9-15 | 6.25 | $19,613 | +$17,633 | +$11,645 | never · 12 |

Infrastructure alone, 12 months against today at R: B2 +$673, B2-s +$72, Fly-cf N=3 +$995, Fly-direct N=3 +$1,047, Fly-direct N=4 +$1,356-1,452 (v2.3, derived), B3-s +$1,332, Fly-direct N=6 +$2,242, Fly-wkr N=6 +$2,253 (`small-vm/model/out/cost_smallvm_tables.md`; `small-vm/editor/tco.py`).

What this says:
- At today's traffic every option costs more than it saves in money. The decision is about latency per engineer-day, and the engineer-days are estimates.
- B2 pays back its migration in about 25 months at L and 6 at XL (B2-s 22 and 6). B1 never pays back at L: its Worker fees plus ops exceed the saving. Fly-direct never pays back, because its egress grows with traffic.
- At R, a small 3-region fleet (Fly-cf $98, B3-s $126) costs $77-105/month more than B2-s, and $27-55 more than B2 on an AX41.

**Cloudflare's own failures:**
- 2025-11-18: core proxy 5xx from 11:20 UTC, all resolved 17:06 ([postmortem](https://blog.cloudflare.com/18-november-2025-outage/)).
- 2025-06-12: Workers KV 90.22% failing and D1 errors peaking at 22% for about 2.5 h ([postmortem](https://blog.cloudflare.com/cloudflare-service-outage-june-12-2025/)).
- CDN-fronted origins remove D1 and Tigris from the path but keep the CDN dependency.

**Fleet failure modes:**
- Host or disk failure: use RAID1 on dedicated servers; Fly volumes are not replicated.
- Provider incidents and patch reboots.
- Hetzner dedicated servers have no documented fair-use throttle at 1 Gbit. Hetzner Cloud AP locations include 0.5-5 TB, then EUR 7.40/TB ([Hetzner Cloud](https://www.hetzner.com/cloud/regular-performance/)).

**Redundancy:**
- Behind a CDN, one node per region is acceptable if misses have somewhere else to go: the Worker's D1 + Tigris fallback (B1 family), a second origin behind an LB (B2-HA), or a cross-region pool (B3). `stale-if-error` alone does not cover connection failures.
- Direct serving with GeoDNS needs N+1 per region.
- Fly (v2.2): with N ≥ 3 and one Machine per region, the proxy sends a failed region's users to the next healthy region, about +80 ms for Europe at N=3. That is acceptable for immutable, rebuildable data. N=1 needs 2 Machines.

### 7.2 Deploy cadence and caching (critic #6; modelled unless marked)

**What is known:**
- Every deploy already empties the edge cache today: `index.ts` treats a cached tile whose `PMTiles-Deployment-Key` differs from the live key as a miss.
- So the measured 73.8% is a **converged steady state**. Key `20250818` had been live for 14 months when it was measured. It is not a "6-monthly" figure, as the critic assumed.
- A deploy key in the URL or cache key (A2, B2) has the same edge effect, and in the URL it also invalidates browser caches.
- Workers Cache without `cross_version_cache` also empties on every *code* deploy ([configuration](https://developers.cloudflare.com/workers/cache/configuration/)).
- Cloudflare evicts by LRU; retention "is determined by the object's relative popularity and the size of the cache, and is not configurable" ([retention vs freshness](https://developers.cloudflare.com/cache/concepts/retention-vs-freshness/)). Our 31-day `max-age` caps it.

**Model** (`fixups-deploy-cadence/`):
- **Popularity.** One OSM tile-log day ([2026-10-31](https://planet.openstreetmap.org/tile_logs/), 3.15B requests). The log lists tiles with ≥10 requests from ≥3 IPs ([format](https://pypi.org/project/tilelog/1.7.1a1/)), which covers 85% of requests.
  - Raster z0-z19 requests were mapped to their 512 px vector ancestor, then reweighted to the measured continent mix.
  - The result is Zipf-like, α ≈ 0.85. The top 10^5 vector tiles get 58% of requests and the top 10^6 get 88%. Published tile logs show the same skew ([Wang et al. 2010](https://ch.whu.edu.cn/en/article/id/873), [InTechOpen](https://intechopen.com/chapters/38302)).
- **Colos and cache.** One colo per z5 area (357 with traffic). A share f of each tile's requests comes from its own area's colo. Each colo is an LRU under Che's approximation, with retention T fitted so the steady state is 73.8%.
- **Constraint.** Weak locality (f ≤ 0.5) would need T = 60-900 days, which the 31-day max-age rules out. The 11 consistent scenarios give **T ≈ 1-24 days**; the central case (f = 0.8) gives **13 days**.

| Cadence (key in URL or cache key) | Per-colo hit (range) | Tiered combined hit (u) | Today mean per tile (measured anchors) | A2 mean (calibrated) | B1 / B2 / B3 mean warm (model) | Extra $/month at R · L (L from the traffic-scaled model): A0 / A2 / B |
|---|---|---|---|---|---|---|
| steady state (measured, no deploy for 14 months) | 73.8% | 85% (0.43) | 96–154 | 66–94 | 35 / 38 / 28 | — |
| 6-monthly | 73.1% (72–74%) | 84.4% (0.42) | 98–157 | 68–98 | 36 / 39 / 29 | ≈0 / ≈0 / 0 |
| monthly | 69.4% (64–73%) | 81.8% (0.41) | 108–174 | 76–110 | 38 / 42 / 30 | 0.37 · 9 / 0.26 · 2 / 0 |
| weekly | 57.3% (47–72%) | 70.4% (0.31) | 142–229 | 110–164 | 50 / 51 / 35 | 1.4 · 33 / 1.24 · 11 / 0 |
| daily | 36.4% (24–61%) | 47.6% (0.18) | 200–323 | 178–270 | 72 / 68 / 44 | 3.18 · 99 / 3.16 · 54 / 0 |

- The A2 and B1 columns use the cadence model's own u (0.43 at steady state), so they read slightly higher than the u = 0.5 figures elsewhere (A2 61-86, B1 33-62).
- **The L adders are from the traffic-scaled model**, in which the steady-state hit ratio rises with traffic (94.8% per colo at L; §2.4). At the constant 73.8% used by every other cost table, the same cadences add about **+$30 / +$110 / +$250** to A0 at L for monthly / weekly / daily (`cost.py` CADENCE), and about +$21 / +$96 / +$247 to A2. At R the two methods agree within about $0.25.
- **Refill after a deploy** (central): per-colo hit ratio 16% after 1 h, 47% after 1 day, 68% after 7 days. Tiered: 24% / 59% / 81%.
- **Extra misses per deploy at R:** 0.55M per-colo (range 0.06-1.2M), or 0.39M origin fetches with tiering. That is about $0.67 per million on D1 + Tigris (Tigris GET + about 8.5 ms CPU) and $0 on a VM.
- **Latency is the real cost.** Each lost hit-ratio point adds 2.8-4.6 ms to today's mean per tile and 0.7-2.3 ms to B's.
- **Origin load after a deploy at XL:** about 1,050 tile req/s average, about 150 Mbit/s, nearly all in the first minutes. At an assumed 3-4× peak that is 450-650 Mbit/s: it fits 1 Gbit, but not Scaleway's 500 Mbps guarantee. Modelled decay with tiering: about 170 req/s after 1 h, 45 after 6 h, 8 after 1 day. Deploy off-peak.
  - For A options, D1 is single-threaded ("at most 1,000 queries per second" at 1 ms per query, [D1 limits](https://developers.cloudflare.com/d1/platform/limits/)). A cold cache at XL sits at that ceiling.
- **Warm connections.** Cadence makes connections warmer, not colder. In the first hour after a deploy, per-colo warm shares rise from 97 / 80 / 47% to 99 / 93 / 75% (1 / 10 / 50 pools per colo). The open question is steady-state B1 per-colo fetches at R traffic.

**How much data changes (measured, request-weighted).**
- Method: 1,500 tiles drawn by the OSM-derived popularity, with their PMTiles entries compared across builds. A length difference counts as changed, so this is a lower bound.
- Results: **1 day: 15% of requests** (z0-z8 31%, z9-z12 16%, z13-z15 6%); **5 days: 37%**; **82 days: 80%**.
- In a spot check, 14 of 80 popular tiles changed in one day, all by small real edits.
- v1's 0.1-0.2%/day counted unique tiles over random leaves, which rural z15 tiles dominate. Popular tiles change far more often, so **most revalidations would not be 304s**.

**Stable URLs plus serve-stale refresh (the alternative to a key in the URL).**
- How it works: data-only deploys keep the URL; schema changes still get a new path (`/v2`). The edge serves the cached tile and refreshes it in the background.
  - Workers Cache: `cloudflare-cdn-cache-control: max-age=…, stale-while-revalidate=…`, with a Worker `304` recorded as `REVALIDATED` ([configuration](https://developers.cloudflare.com/workers/cache/configuration/)).
  - B2 zone cache: revalidates with `If-None-Match`/`If-Modified-Since` ([cache statuses](https://developers.cloudflare.com/cache/concepts/cache-responses/), [revalidation](https://developers.cloudflare.com/cache/concepts/revalidation/)).
  - Today's Cache API supports neither ([Cache API](https://developers.cloudflare.com/workers/runtime-apis/cache/)). The same effect is one change in `index.ts`: on a deployment-key mismatch, return the cached tile and refresh it in `waitUntil`.
- Effect: the hit ratio stays at its steady state whatever the cadence.
- Cost: about one background fetch per (colo, tile) requested after a deploy, roughly as many as the refill misses. **This saves latency, not origin requests.** It is free on a VM origin, which can answer 304 from a content hash. On today's origin each refresh costs about as much as a miss ($0.67/M) because PMTiles entries carry no hash; that is under $3/month at R.
- Staleness: one stale view per (colo, tile), mixed builds across tile seams, and browsers keep tiles for their own `max-age` (31 days today).
- Targeted purges are no substitute. Purge-by-URL is 800 URLs/s on Free ([purge limits](https://developers.cloudflare.com/cache/how-to/purge-cache/)), and Workers Cache offers only tag, prefix or everything ([limitations](https://developers.cloudflare.com/workers/cache/limitations/)).

**Model limits:**
- OSM raster demand stands in for Immich photo-map demand.
- The catalogue is 4.8M observed tiles, not the 136.5M in the planet.
- z5 areas are coarser than real colos, so real refills are slower.
- Browser-cache invalidation is not modelled.

**Recommendation:**
- With the deploy key in the URL or cache key, deploy **at most monthly**. That costs about 4 hit-ratio points (0.5-9), about +12-20 ms on today's mean per tile, and under $1/month at R. Weekly costs 2-27 points (central 17), and daily 13-50.
- More frequent data deploys need stable URLs with serve-stale refresh.
- With a tiered cache, warm the upper tier before flipping TileJSON: request the top 10^5-10^6 tiles from last month's logs through each upper-tier region. That costs about $0.10-1 per region per deploy and covers a modelled 58-88% of requests.
- Pin T before choosing:
  - log the age of cached tiles on hits (a cached-at header written at `cache.put`);
  - after the next deploy, chart `tiles_cdn_hit / tiles_tile_request` per hour for the new `deployment_key`. That chart *is* the refill curve.

---

## 8. Recommendation

### 8.1 Ranked shortlist

How the order got here. v1 recommended B1 on a modelled 1.2-5× miss gain. v2 found that today's slowness is mostly the Tigris path. v2.2 found that v2.1 had over-sized every VM, so a small fleet of 3-4 regions costs about $100-130/month rather than $233-286. **v2.3 measured the main alternative live** (§2.5):
- **The Fly volume fleet is the only alternative measured end to end.** Direct anycast with four regions gives 51-55 ms per tile worldwide with no miss tail and a first map load of 184-195 ms, for about $128-136/month at R. Today, measured in the same run: 159 ms blended per tile (459 ms misses), first load 195-590 ms (379 expected).
- **The model behind the other alternatives was close** for Fly-direct (4-9 ms optimistic like for like), so the modelled Cloudflare-fronted figures are plausible. None of them is measured.
- **Tigris is not a hot multi-region store** for this bucket (§2.6). A10 is dropped, A1b picks its endpoint per continent, and any Tigris-backed design needs a pre-warm.

1. **Now: the cheap Worker fixes, A1 + A1b** (1.5-3.5 engineer-days, about $15/month at R).
   - A1b uses a module-scope Tigris client: t3 for Europe, North America and Asia, and the Fly endpoint for South America, Oceania and Africa, until the Worker-side endpoint test (X12) settles it. From Helsinki, t3 cut a fresh-connection cold read from 396 to 251 ms and a warm one from 185 to 89 ms.
   - Calibrated full miss: 202-456 ms (kept until X12).
   - Read the production numbers (Appendix C, X4) before changing metric tags or enabling Workers Cache.
2. **Next, within days and for a few dollars: four small tests.**
   - **Cloudflare in front of the Fly fleet (X11).** Rebuild the 4-region fleet for a day (about $1-5), put a proxied Cloudflare test hostname with Tiered Cache off in front of it, and repeat the §2.5 protocol from the same 48 probes. This needs a Cloudflare test hostname; v2.3 used no Cloudflare credentials.
   - **The Worker-side endpoint test (X12):** a dev Worker reading the same chunks through t3 and the Fly endpoint, from 8 or more colos.
   - **The per-region pre-warm test (X13):** about $0.85 for 12 regions, plus retention checks over a week.
   - **The Tigris support question (X14)**, with the §2.6 examples.
3. **Then the main move (2-4 weeks): a Fly volume fleet.** VersaTiles on shared-cpu-1x 1 GB with one 150 GB volume per region in ams, iad, sin and syd, built as a new cluster per data update with parallel t3 pulls (§4.D). 6.5-11.5 engineer-days and 1.75-2 h/month (assumed).
   - **Direct anycast (Fly-direct N=4), measured:** 51-55 ms per tile and a first load of 184-195 ms. $128-136 at R, about $250 at M and about $660 at L, because egress grows linearly ($427 at L). Against it: no L7 shield (a flood becomes an egress bill), slower connection setup than Cloudflare (54 vs 36 ms), anycast detours, no SLA below Fly Enterprise, and request logs from Fly instead of the Worker metrics.
   - **Behind Cloudflare (Fly-cf N=4), modelled:** the same fleet behind a proxied zone with Tiered Cache off, $123-131 at R and about $255 at L. The model gives 30-40 ms per tile for three regions (expect 5-10 ms more). It adds the shield and Cloudflare's dense hit path, and only the 26% per-colo misses reach Fly.
   - **How to choose:** take Fly-cf if X11 shows it at least as fast as direct worldwide (per tile and first load), or if the shield or the egress slope matter more than a second vendor in the path. Otherwise direct is the simpler, measured choice. Either way the per-apex Worker metrics go unless a Worker stays in front (Fly-wkr, $203 at R for 6 regions, modelled).
   - Add regions from production per-region numbers: sjc (NA-West measured at 104-118 ms per tile without it) for about $29/month, gru (Latin America, about 220 ms) for about $33.
4. **Alternatives if Fly is not acceptable (§9 Q12):**
   - **A2, Workers Cache** (stay serverless): $14/month at R, $18 with the gateway, 3-8 days. Calibrated A1b + A2: blended miss 135-262 ms and mean per tile 49-85 ms, against the measured fleet's 53 ms blended (different methods: the live method puts today at 159, against its calibrated 96-154). Unverified until the Workers Cache spike (X1). Use the gateway variant (A2-gw) if per-request metrics are a hard requirement (§9 Q6).
   - **B3-s** (small VMs behind a Cloudflare LB, $126 flat) or **B2-s** (one small EU origin, $21 flat, fast only in Europe). Both are modelled; X2 validates.
5. **Later, a cost cut: Tigris-backed Machines without volumes (Fly-chunkcache).** About $40-60/month for four regions instead of about $130, modelled at 55-80 ms per tile once pre-warmed. Do it after the volume fleet runs, once X13 and X14 show that regional copies stay hot, and once a server with every directory in RAM and a local chunk cache (or a tuned TAG sidecar) exists.
6. **Dropped in v2.3:** A10 (Dual-Region placement), stock go-pmtiles reading one big Tigris object (Fly-s3), and TigrisFS (Fly-tigrisfs).

**Bigger fleets** (Fly N=6 or more, B4-s $281) only once production per-region numbers are in. Behind Cloudflare, Asian misses depend on A7 (B3-s South and East Asia full misses are 146 / 144 ms under today's routing, 69 / 91 if fixed). Direct Fly avoids Cloudflare's routing but has its own anycast detours (Mumbai → ams).

**Staff time (secondary; assumed).** Migration estimates are 6.5-13 engineer-days for the shortlisted fleet options (10-18 with a Worker in front), and ops 1.75-4.75 h/month (§7.1). At an assumed $100/h the 12-month TCO deltas at R are Fly-direct N=4 +$8.9k, Fly-direct N=3 +$8.5k, Fly-cf N=3 +$9.4k, B2-s +$9.6k, B3-s +$13.6k and Fly-wkr N=6 +$15.1k; 85-100% of each is staff time.

**Runner-up CDN: B5 on Bunny's Standard network, not Volume.** Choose it if A7 cannot be fixed and the production distant-colo share is material. The 8-11% figure is an extrapolation from 12 probes; replace it with Appendix C Q2-Q6 before acting.

### 8.2 Phased plan

**This week (no new vendors)**
- Ship A1 + A1b:
  - drop or move `SELECT 1`;
  - embed TileJSON metadata;
  - `tee()` streaming;
  - module-scope Tigris clients with signed reads: t3 for Europe, North America and Asia, the Fly endpoint for South America, Oceania and Africa (by `request.cf.continent`);
  - a directory LRU;
  - drop `Vary: Origin` and normalise `accept-encoding`;
  - a canonical host in `/v1.json`.
- **Read the numbers that decide everything** (Appendix C, X4), before changing metrics tags or enabling Workers Cache:
  - the Workers request count in the CF dashboard (Q8);
  - hits and misses per continent, which give the miss weights;
  - the zoom mix;
  - the out-of-continent colo share and the top detours (Q2-Q6);
  - per-colo hit ratio against volume (Q7);
  - D1 `served_by_region` for SYD, GRU and BOM;
  - miss latency quantiles by continent;
  - the peak 5-minute rate;
  - whether Workers Logs is on for `tiles-prod`;
  - actual Worker CPU per hit and per miss;
  - the per-client burst miss share from a 1% hashed sample (§5.4);
  - hit age, which needs a cached-at header (§7.2).
- Send the Tigris support question (X14) and run the Worker-side endpoint test (X12).
- Revoke the credentials used for the v2.3 tests (§2.5).

**Next 1-2 weeks (a few dollars)**
- **Cloudflare in front of the Fly fleet (X11).** Reuse the v2.3 scripts (`live/fly-vol/scripts/`, `live/latency/scripts/`).
- **Per-region pre-warm (X13)** on the next deploy or a test prefix.
- The Workers Cache spike (X1), only if A2 stays an option.

**Medium term (2-4 weeks)**
- **Choose:** Fly-direct or Fly-cf from X11; A2 or B3-s if Fly is rejected (§9 Q12).
- **Build the fleet** (§4.D):
  - one Fly app per data key; ams, iad, sin and syd; shared-cpu-1x 1 GB; VersaTiles 5.6 or later with a start-up grace of at least 30 s; one 150 GB volume each, snapshots off; autostop off (suspend only for second Machines);
  - fill each volume with a parallel t3 pull on a temporary performance-2x loader (about 35 min), hash in the stream, and run the 20k-tile check;
  - route nothing until the volume is `created` and `/health` reports the new key; then switch DNS (direct) or the origin hostname (Fly-cf); delete the old app after a few hours;
  - for Fly-cf, a proxied record with Cache Rules, Full (strict) and **Tiered Cache off**.
- **Extend [#245](https://github.com/immich-app/geoshenanigans/pull/245):** stage the single `.pmtiles` in Tigris next to the chunks, drive the new-cluster flow, and wait until every region reports the new key before switching.
- Keep today's Worker + D1 + Tigris path deployable for a few weeks as the fallback.
- Ship Fly metrics and logs into VictoriaMetrics; direct serving sees every request at the Machine.

**Long term (quarter)**
- Add regions (sjc, gru) only where production per-region numbers justify them. For the B-s alternatives, one LB pool per region, after sizing A7 (X5).
- Consider the volume-less cost cut (Fly-chunkcache) once X13 and X14 are in.
- **Deploy cadence.** With Fly-direct there is no CDN cache to refill: every Machine serves from its local volume, so a deploy costs only the new cluster (about $0.50-1). Behind a CDN, move to monthly data deploys with the key in the URL, or to any cadence with stable URLs plus serve-stale refresh; with a tiered cache, warm the upper tier before flipping TileJSON (§7.2).
- Optional client-bytes project: our own Planetiler build with MLT and/or z14 max on a `/v2` URL. Client wins:
  - inline `tiles` in the style to save one serial RTT;
  - preconnect to static.immich.cloud;
  - raise the MapLibre Native ambient cache from its 50 MB default ([docs](https://maplibre.org/maplibre-native/android/api/-map-libre%20-native%20-android/org.maplibre.android.offline/-offline-manager/set-maximum-ambient-cache-size.html)).
- Keep an OpenFreeMap-based emergency fallback style, after contacting OpenFreeMap; consider sponsoring them.

### 8.3 Experiments to validate

Done in v2.3: the planet service-time test (X3), the Tigris endpoint test (X7, from Helsinki and from six regions) and the Fly small-fleet test (X10). New: X11-X14.

| # | Status | What | How | Success criterion, or result |
|---|---|---|---|---|
| X1 | to do, if A2 stays an option | Workers Cache spike: topology, gain, and what the gateway can see | A dev Worker with Workers Cache, in plain and gateway (`ctx.exports`) variants. Request a fresh tile from probe A, then from probes in the same and other regions (48 probes). Log `cf-cache-status`, the `cf-ray` colo, `cf-placement` and `request.cf.colo` of the cached entrypoint on full misses. Check whether the gateway can read `Cf-Cache-Status`. Redeploy code only and check that hits survive with `cross_version_cache`. Check for `Vary` fragmentation. Free tier | Tier hits **≤ 80 ms p50 in EU/NA**. Topology settled (global or regional). The gateway can count hits, with the header and callee-count methods within 1%. A code-only deploy keeps the hits |
| X2 | to do, only for the B options | Miss latency with a VM origin, warm and cold | A temporary small origin (CX43, hourly) behind test hostnames in the B1, B1-P1, B1-z and B2 variants; 48 probes × cold tiles, repeated after 10+ minutes idle; drive B1 at about 1.3 misses/s | Traffic-weighted full miss **p50 ≤ 150 ms, p99 ≤ 500 ms**; EU p50 ≤ 80 ms |
| X3 | **done** | Planet-scale origin service time | v2.2: the full planet under emulated Fly shapes on the box. v2.3: on Fly hardware (§2.5) | VersaTiles 2.7-3.1 ms CPU per request; p50 6 ms and p99 9-14 ms at 5-50 req/s; 279-285 req/s with burst; **20 req/s clean on the shared-cpu-1x baseline** (emulation: 45). Enough at our load |
| X4 | to do, free | Production split: volume, hit ratio, zoom, continent and miss mix, peak rate | The Appendix C queries over 7 and 30 days, against the CF dashboard Workers request count. Run before changing metric tags | Sources agree within ±20%. Re-run `synthesis/v2/model.py` and `cost.py` with production weights; choose extra Fly regions from per-region numbers |
| X5 | step 0 done; step 1 optional | Whether a plan change fixes distant-colo routing | Step 1: one month of a Business test zone ($250 + a domain from $7.85) with a dev tiles Worker; the affected Mumbai/Delhi/Hyderabad, Seoul, Taipei, Lagos and Istanbul probes. Matters only for Cloudflare-fronted options | At least 5 of 6 affected probes land on their local colo. If not, consider B5 |
| X6 | partly done | Deploy dry run | v2.3 measured the parts: seed 34.6 min, t3 pulls at 98.5-216 MB/s per region, forks `created` after 43-159 min, VersaTiles start 20 s. Remaining: the full new-cluster flow (§4.D) with cutover and rollback, under load | **< 45 min end to end** (pull route), zero 5xx during the switch, rollback in under 2 min |
| X7 | **done** (Helsinki + six regions) | Why Tigris dominates miss time; t3 vs the Fly endpoint | Signed 16 KiB range GETs on production chunks and a test bucket, fresh vs kept-alive connections, first read vs repeats (§2.5); one Fly Machine each in lhr, iad, gru, nrt, sin and syd (§2.6) | **t3 251 vs 396 ms cold, 89 vs 185 warm** (fresh connections, Helsinki): the ≥ 50 ms rule is met. Cold placement is 60-80% of a cold read, connection setup about 52 ms, floor 33 ms. Every region's first read misses (160-670 ms); nrt, sin and syd served from jnb. t3 is worse in gru and syd. The Worker-side part moves to X12; the Dual-Region bucket test is dropped with A10 |
| X8 | to do, low priority | R2 Worker-binding range reads vs Tigris | Two R2 buckets (WEUR, ENAM) with about 100 copied 1 MB chunks; a dev Worker times `env.BUCKET.get(key, {range})` against Tigris in the same request, from the X12 colos | R2 p50 ≤ Tigris (t3) p50 in EU and NA on first and repeat reads; otherwise drop A4 |
| X9 | to do, low priority | Same-object CDN hit and miss comparison | Three real tiles as identical static files on Cloudflare, Bunny Standard and Volume, Fastly and CloudFront (about $1-2); 48 probes, 3 rounds an hour apart | Per CDN: median over probes and a paired bootstrap 90% CI against Cloudflare; a difference is real only above 5 ms and the CI half-width |
| X10 | **done** (except part e) | Fly small-fleet test | 4-region VersaTiles fleet (ams seed, forks to iad, sin, syd), 48-probe latency, throughput, Tigris pulls, suspend (§2.5). Cost about $1.42 (at most $6.43) | (a) Forks `created` after 43 / 86 / 159 min: iad met the 45-min bar, sin and syd **missed** it, so pull beats fork for new builds. (b) **Met:** 51-55 ms per tile (≤ 70) and 184-195 ms first load (≤ 270). (c) **Missed:** 30 req/s on the baseline had p99 87 ms (20 req/s clean). (d) **Met with t3** (98.5-216 MB/s per region); the Fly endpoint stalled in sin. (e) Fly-cf not run → X11 |
| X11 | **new**, about $1-5 | Cloudflare in front of the Fly fleet | Rebuild the 4-region volume app (t3 pulls, about 35 min). Put a proxied Cloudflare test hostname in front (Cache Rules for `.mvt` and JSON, Full (strict), Tiered Cache off). Run the §2.5 protocol from the same 48 probes against the proxied hostname, direct Fly and today, in the same rounds; add per-colo cold misses | Weighted per tile and first load against measured direct Fly (51-55; 184-195). Choose Fly-cf if it is no slower worldwide, or within about 10 ms when the shield or egress matter. Model expectation: 30-40 ms per tile |
| X12 | **new**, free tier | Worker-side Tigris endpoint test | A dev Worker (or a header-gated prod path) with module-scope clients for t3 and the Fly endpoint that copies `X-Tigris-Served-From`, `X-Tigris-Regions` and `Server-Timing` into its own `Server-Timing`. Cache-busted `?nc=` requests from Globalping in 8+ colos including GRU, SYD, JNB, NRT and SIN; fresh vs warm; 0.5 s and ≥ 30 s spacing | Per continent, the endpoint with the lower Worker-side p50; adopt t3 where it is ≥ 50 ms better on fresh connections |
| X13 | **new**, about $1 per run | Per-region pre-warm | After a deploy (or on a test prefix), read every chunk once from a Fly Machine in each of 12 regions (about 1.7M GETs ≈ $0.85, plus a few Machine-hours). Then read random chunks at 1 hour, 1 day and 7 days, recording `X-Tigris-Served-From` and the cache state | First-read p50 ≤ 60 ms in region after 1 day and retention ≥ 7 days: then pre-warm on every deploy (for today's Worker and for Fly-chunkcache) |
| X14 | **new**, free | Tigris support question | Send the §2.6 examples (keys, times, regions, headers; no credentials) | Answers: what `X-Tigris-Regions` guarantees, why first reads come from jnb, regional retention and eviction, and whether a placement or prefetch setting makes a deploy hot everywhere |

---

## 9. Open questions for the maintainer

1. **Traffic volume and growth.** Is 13.7M requests/month (5.3 req/s average) right? Check the CF dashboard Workers request count. Is L or XL realistic within a year? That decides whether B2's flat price matters.
2. **Budget and staff time.** Is about +$113-121/month of infrastructure for the measured 4-region Fly fleet acceptable (about +$108-116 behind Cloudflare; +$6-56 for one EU origin, B2-s or B2; +$111 for B3-s)? Are 6.5-13 engineer-days (assumed; 10-18 with a Worker in front) plus 0.25-3.25 extra hours a month acceptable? What hourly rate should replace the assumed $100/h?
3. **Existing hardware.** Does FUTO or Immich already run servers with ≥ 160 GB of disk and 1-2 GB of RAM to spare (v2.1 asked for ≥ 400 GB) that could host an origin? That would make B roughly free in infrastructure.
4. **Required regions.** Measured traffic is 63% EU, 21% NA, 11% Asia and 3% Oceania. Are APAC and Oceania latencies a product priority, or is EU + NA enough? For the Fly fleet: is ams, iad, sin and syd the right starting set, and are NA-West (104-118 ms per tile without an sjc Machine) and Latin America (about 220 ms without gru) acceptable (§4.D)?
5. **Update cadence.** Stay at about 6-monthly, or move to monthly or weekly? The modelled cost with the key in the URL is about −4 hit-ratio points for monthly, −17 for weekly and −37 for daily (§7.2). Should we track the newest 4.x schema, given that styles are pinned to @protomaps/basemaps 5.5.1?
6. **Per-hit metrics.** Are the per-apex-domain RPS alert and the colo dashboards a hard requirement? If yes, use the gateway (A2-gw / B1-gw), +$0.32 per million requests. If no, B2 is acceptable.
7. **Availability.** With one origin, should a dead origin fail misses (B2, hits keep serving), fall back to D1 + Tigris (B1 family), or fail over to a second vendor (B2-HA, $126)? Is next-business-day repair acceptable?
8. **Cloudflare plan, Terraform and a test hostname.** The routing test suggests immich.cloud is on the Free plan; please confirm. Can we get a Cloudflare test hostname (and API access) to measure Cloudflare in front of the Fly fleet (X11)? Is moving the tiles Worker deploy to wrangler or provider v5 acceptable if Workers Cache needs it? Is a one-month Business test zone ($250) acceptable for X5?
9. **Other Workers on the account.** Do other Immich Workers share the account's Workers Paid allowances? It changes cost by ≤ $3/month (§2.4), so this is for completeness.
10. **Sponsorship.** Should we apply to Fastly Fast Forward or ask Cloudflare about OSS programmes?
11. **Schema changes.** Are MLT or a z14-max build acceptable for future `/v2` styles?
12. **Fly or not (v2.3).** Measured live, a 4-region Fly volume fleet serves 51-55 ms per tile worldwide with no miss tail, for about $128-136/month. Is Fly.io acceptable as a vendor for the tile path: no SLA below Enterprise, single-host volumes, region capacity not guaranteed? If yes, should Cloudflare sit in front (Fly-cf: shield, faster connection setup, about 26% of the egress) or not (Fly-direct: one vendor, measured, linear egress)? X11 measures the latency side.
13. **Tigris (v2.3).** May we send the §2.6 examples to Tigris support (X14) and run a per-region pre-warm test (X13, about $1)? Is a later move to Tigris-backed Machines without volumes (about $40-60/month instead of about $130, needs code; §4.D) of interest, or are volumes preferred for simplicity?
14. **Test credentials (v2.3).** Please revoke the Fly org token, the Tigris test-bucket key and the production read key used for the v2.3 tests, or confirm they are scoped and short-lived.

---

## 10. Sources

**Measured data (this research):**
- `traffic-7d-2026-11-01.json`: VictoriaMetrics `tiles_*` metrics, 7 days.
- v2 fix-up tracks: `fixups-cf-pricing/`, `fixups-host-pricing/` (growth.py, headroom.py, bw.py, auction.py, ovh.py), `fixups-measure/` (forced-miss Tigris runs, probe population), `fixups-deploy-cadence/` (osm_fit.py, cache_model.py, view_sim.py, change_weighted.py, warm.py), `fixups-competitor-cdn-latency/` (hitratio.py, bunny_model.py, queries.metricsql, colo_continent.csv).
- Track scratch directories under the research scratch directory (not in the repo): cf-baseline, cf-optimize, vm-providers, cdn-layer, tile-formats, latency-measure, multi-region-ops, hybrid, and the `verify-*` directories.
- `synthesis/model.py` and `synthesis/cost.py` (v1); `synthesis/v2/model.py`, `cost.py`, `MODEL_V2.md` (v2 latency, calibration and cost model used by this report).
- v2.2 small-VM rework, under `small-vm/`: `MODEL_SMALLVM.md`; `model/scripts/model_smallvm.py`, `cost_smallvm.py` and `gp_fly.py` with outputs in `model/out/` and raw Globalping results and Cloudflare tiered-cache docs in `model/raw/`; the fly-docs track (`scripts/fly_cost.py`, page copies in `fly-docs/`); the planet bench (`scripts/`, results in `bench/raw/results/`, and on the box under `/ssd/2026-11-01-flowly-47dc35b5-deploy-research/small-vm/`); the image-replication track (`img-repl/`, including `calc-output.txt` and `results/compressibility.txt`); `editor/tco.py` (v2.2 TCO rows).
- v2.3 live tests, under `live/`: `live-results.json` (setup, the three test tracks, worldwide latency, teardown and cost); `fly-vol/` (seed, fork polling, serving, throughput and suspend scripts and data), `fly-s3/` (S3-direct and TigrisFS Machines), `box-tigris/` (the Tigris endpoint test and box serving), `latency/` (48-probe Globalping runs: `summary.json`, `raw/`, `scripts/run.py`, `analyze.py`), `x7-regions/` (per-region production read test: `SUMMARY.md`, `probe.sh`, `raw_all.txt`), `teardown/` and `resources.jsonl` (every resource created and destroyed). Tigris-backed design track: `tigris-backed-results.json` and `tigris-backed/` (docs and source reading, `s3sim/` box simulation, cost script). Maintainer inputs: `workers-cache-notes.md` (bottom section).

**Globalping measurements:**
- [2RZBqA0qfJerPZAgx00021QWQ](https://api.globalping.io/v1/measurements/2RZBqA0qfJerPZAgx00021QWQ)
- [2Lvi8S0k3GL2IzqDn00021QWQ](https://api.globalping.io/v1/measurements/2Lvi8S0k3GL2IzqDn00021QWQ)
- [26gmL3fjBtWROFGI300021QWQ](https://api.globalping.io/v1/measurements/26gmL3fjBtWROFGI300021QWQ)
- [2erYmJ9llI9Hp2iDg00021QWR](https://api.globalping.io/v1/measurements/2erYmJ9llI9Hp2iDg00021QWR)
- [2LDvUi5DwQH0kNCjC00021QWR](https://api.globalping.io/v1/measurements/2LDvUi5DwQH0kNCjC00021QWR)
- [2i7vKLeS7oHW8BwQ000021QWS](https://api.globalping.io/v1/measurements/2i7vKLeS7oHW8BwQ000021QWS)
- [2FSMeL46rlAzW3l3400021QWP](https://api.globalping.io/v1/measurements/2FSMeL46rlAzW3l3400021QWP)
- [2RGjpI62ZXCKFX4KZ00021QWP](https://api.globalping.io/v1/measurements/2RGjpI62ZXCKFX4KZ00021QWP)
- [2kepW8F262e6fGrUL00021QWP](https://api.globalping.io/v1/measurements/2kepW8F262e6fGrUL00021QWP)
- [2ygsYT7KDqHSJ3cv800021QWQ](https://api.globalping.io/v1/measurements/2ygsYT7KDqHSJ3cv800021QWQ)
- [2HbOOvkebgUG2sMBY00021QWQ](https://api.globalping.io/v1/measurements/2HbOOvkebgUG2sMBY00021QWQ)
- [2YtJgUWw2Uz27GVs600021QWQ](https://api.globalping.io/v1/measurements/2YtJgUWw2Uz27GVs600021QWQ)
- [2Mx3QXi4K0eEMWd1d00021QWQ](https://api.globalping.io/v1/measurements/2Mx3QXi4K0eEMWd1d00021QWQ)
- [2SQlu7cRFBhy6mbh500021QWP](https://api.globalping.io/v1/measurements/2SQlu7cRFBhy6mbh500021QWP)
- [2KVdVOq8qUPH0PddH00021QWQ](https://api.globalping.io/v1/measurements/2KVdVOq8qUPH0PddH00021QWQ)
- [27Hr84sgTdHz85ghd00021QXI](https://api.globalping.io/v1/measurements/27Hr84sgTdHz85ghd00021QXI)
- [2ZlO4rrdQ9Hv0P0bV00021QXM](https://api.globalping.io/v1/measurements/2ZlO4rrdQ9Hv0P0bV00021QXM)
- [2nFYCPFLVki5yZfsg00021QXM](https://api.globalping.io/v1/measurements/2nFYCPFLVki5yZfsg00021QXM)
- [2pijh02dE4ZUdCbH000021QXM](https://api.globalping.io/v1/measurements/2pijh02dE4ZUdCbH000021QXM)
- [2AH2K9Mrdpg1iJiE500021QWL](https://api.globalping.io/v1/measurements/2AH2K9Mrdpg1iJiE500021QWL)
- [2l44YEzej8EMggVZA00021QWK](https://api.globalping.io/v1/measurements/2l44YEzej8EMggVZA00021QWK)
- [2jY1MT0slvCNE5qSR00021QWK](https://api.globalping.io/v1/measurements/2jY1MT0slvCNE5qSR00021QWK)
- [2AZak4UGG6a7q8Mfr00021QXJ](https://api.globalping.io/v1/measurements/2AZak4UGG6a7q8Mfr00021QXJ)
- [2x60vNyIUntKZOJYk00021QXJ](https://api.globalping.io/v1/measurements/2x60vNyIUntKZOJYk00021QXJ)
- [2M56r2JKrsZdggNWX00021QWK](https://api.globalping.io/v1/measurements/2M56r2JKrsZdggNWX00021QWK)
- [26sMrTtlcIPsDyhst00021QWM](https://api.globalping.io/v1/measurements/26sMrTtlcIPsDyhst00021QWM)
- [2ysMR8JHRrpSnnpXo00021QXI](https://api.globalping.io/v1/measurements/2ysMR8JHRrpSnnpXo00021QXI)
- [2N9S3NGEZNsGUt3LP00021QXI](https://api.globalping.io/v1/measurements/2N9S3NGEZNsGUt3LP00021QXI)
- [2rwpQvB302iSfiKX100021QXI](https://api.globalping.io/v1/measurements/2rwpQvB302iSfiKX100021QXI)
- [2juyBsKv0NOYs8kaw00021QXI](https://api.globalping.io/v1/measurements/2juyBsKv0NOYs8kaw00021QXI)
- [2plNoTDkTdc4fN4WQ00021QXJ](https://api.globalping.io/v1/measurements/2plNoTDkTdc4fN4WQ00021QXJ)
- [2XVVyVswAeMNYhNbd00021QXJ](https://api.globalping.io/v1/measurements/2XVVyVswAeMNYhNbd00021QXJ)
- [2RSTB7JNmIPCiuwi300021QXJ](https://api.globalping.io/v1/measurements/2RSTB7JNmIPCiuwi300021QXJ)
- [22CCx4dtw1gCSJ5pC00021QXJ](https://api.globalping.io/v1/measurements/22CCx4dtw1gCSJ5pC00021QXJ)
- [2WM51xEiLuqL3wB4D00021QWS](https://api.globalping.io/v1/measurements/2WM51xEiLuqL3wB4D00021QWS)
- v2 forced-miss Tigris runs: [FRA](https://api.globalping.io/v1/measurements/25GOyHLNhMfuV1hnV00021QYI), [IAD](https://api.globalping.io/v1/measurements/2MHzrQoe3cOafpEs300021QYI), [SIN](https://api.globalping.io/v1/measurements/26DWFSC2xPRRLqZSS00021QYI), [SYD](https://api.globalping.io/v1/measurements/25PymW0u6fPo5EynY00021QYI), [GRU](https://api.globalping.io/v1/measurements/2Gzmu3F3mBI0pwCqA00021QYI), [NRT](https://api.globalping.io/v1/measurements/2l6kZqgp2KWabQPpi00021QYI)
- v2 plan-pool routing test: [tiles](https://api.globalping.io/v1/measurements/2KiIgSPGAbaZCk6Go00021QYC), [Free-listed](https://api.globalping.io/v1/measurements/2F4zJK9aHbgAbIJCQ00021QYC), [Pro-listed](https://api.globalping.io/v1/measurements/2jLErE5UtsmX1V51A00021QYC), [Business-listed](https://api.globalping.io/v1/measurements/2sGRzv9fIrgNgOeyj00021QYC), [cloudflare.com](https://api.globalping.io/v1/measurements/2BDI1VQmIi2fz7g5o00021QYC), [immich.app](https://api.globalping.io/v1/measurements/2zUIlDyOphyrlX9in00021QYC), [set 2 tiles](https://api.globalping.io/v1/measurements/2tR7vCjn9V4u2mqsG00021QYD), [set 2 Free-listed](https://api.globalping.io/v1/measurements/2LOeN3SCKR43LLR4h00021QYD)
- v2 Bunny and Volume-metro runs: [Bunny Standard 48 probes](https://api.globalping.io/v1/measurements/2VaXmlrlQnzbeR0UO00021QYC), [Vultr Paris](https://api.globalping.io/v1/measurements/2uZHjPqoYMyFjimhp00021QYC), [Frankfurt](https://api.globalping.io/v1/measurements/2YA0VMyv2GUViJcED00021QYC), [Chicago](https://api.globalping.io/v1/measurements/27R8VxDcz3a75FXN900021QYC), [Miami](https://api.globalping.io/v1/measurements/2RFnisqCyQjq7POQo00021QYC), [Dallas](https://api.globalping.io/v1/measurements/2kZYYvG1LF4mBoP4w00021QYC)
- v2.2 Fly and metro runs: [Fly anycast edge, 48 probes (`debug.fly.dev`)](https://api.globalping.io/v1/measurements/2Cdt0Iz24EnxNfhrA00021Qi9), Vultr [ams](https://api.globalping.io/v1/measurements/2l1WD9TAppjU4E1tS00021Qi9), [lhr](https://api.globalping.io/v1/measurements/2oKfxC8ApMrWvRneY00021QiA), [arn](https://api.globalping.io/v1/measurements/2T8S5wjiqj5GXYEqV00021QiA), [yyz](https://api.globalping.io/v1/measurements/2UYFSsRJQOESSKdee00021QiA), [ewr](https://api.globalping.io/v1/measurements/2yDV494NZFfW20eLU00021QiA), [sjc](https://api.globalping.io/v1/measurements/2oKmN5SHFPZmffzdz00021QiA), [lax](https://api.globalping.io/v1/measurements/251iI4dvQbhaluv1p00021QiA)
- API spec: [api.globalping.io/v1/spec.yaml](https://api.globalping.io/v1/spec.yaml); probe tags: [Globalping blog](https://blog.globalping.io/new-globalping-feature-eyeball-and-data-center-network-tags/); access-line comparison: [FCC MBA 2023](https://data.fcc.gov/download/measuring-broadband-america/2023/2023-Fixed-Measuring-Broadband-America-Report.pdf)
- City-pair RTTs: [WonderNetwork](https://wondernetwork.com/pings), [WonderNetwork Frankfurt](https://wondernetwork.com/pings/Frankfurt), [Washington](https://wondernetwork.com/pings/Washington), [Los Angeles](https://wondernetwork.com/pings/Los%20Angeles), [Singapore](https://wondernetwork.com/pings/Singapore)

**Cloudflare:**
- [Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)
- [Workers limits](https://developers.cloudflare.com/workers/platform/limits/), [Workers performance timers](https://developers.cloudflare.com/workers/runtime-apis/performance/), [Wrangler configuration](https://developers.cloudflare.com/workers/wrangler/configuration/)
- [Cache API](https://developers.cloudflare.com/workers/runtime-apis/cache/)
- [How the cache works](https://developers.cloudflare.com/workers/reference/how-the-cache-works/)
- [Workers Cache](https://developers.cloudflare.com/workers/cache/), [configuration](https://developers.cloudflare.com/workers/cache/configuration/), [cache keys](https://developers.cloudflare.com/workers/cache/cache-keys/), [limitations](https://developers.cloudflare.com/workers/cache/limitations/), [examples](https://developers.cloudflare.com/workers/cache/examples/), [blog 2026-07-06](https://blog.cloudflare.com/workers-cache/)
- [Retention vs freshness](https://developers.cloudflare.com/cache/concepts/retention-vs-freshness/)
- [Cache-Control](https://developers.cloudflare.com/cache/concepts/cache-control/)
- [Cache responses](https://developers.cloudflare.com/cache/concepts/cache-responses/)
- [Default cache behaviour](https://developers.cloudflare.com/cache/concepts/default-cache-behavior/)
- [Vary](https://developers.cloudflare.com/cache/concepts/vary/)
- [Range requests](https://developers.cloudflare.com/cache/reference/range-requests/)
- [Tiered Cache](https://developers.cloudflare.com/cache/how-to/tiered-cache/), [Smart Tiered Cache](https://developers.cloudflare.com/smart-shield/configuration/smart-tiered-cache/), [cache and Workers interaction](https://developers.cloudflare.com/cache/interaction-cloudflare-products/workers/), [revalidation](https://developers.cloudflare.com/cache/concepts/revalidation/), [connection limits](https://developers.cloudflare.com/fundamentals/reference/connection-limits/)
- [Smart Shield](https://developers.cloudflare.com/smart-shield/get-started/), [connection reuse](https://developers.cloudflare.com/smart-shield/concepts/connection-reuse/), [Smart Shield launch blog](https://blog.cloudflare.com/introducing-observatory-and-smart-shield/)
- Argo: [launch blog 2017](https://blog.cloudflare.com/argo/), [get started](https://developers.cloudflare.com/argo-smart-routing/get-started/), [product page](https://www.cloudflare.com/application-services/products/argo-smart-routing/), [CDN reference architecture](https://developers.cloudflare.com/reference-architecture/architectures/cdn/)
- [Cache Reserve](https://developers.cloudflare.com/cache/advanced-configuration/cache-reserve/)
- [Purge](https://developers.cloudflare.com/cache/how-to/purge-cache/)
- [Compression](https://developers.cloudflare.com/speed/optimization/content/compression/)
- [D1 pricing](https://developers.cloudflare.com/d1/platform/pricing/), [D1 limits](https://developers.cloudflare.com/d1/platform/limits/), [D1 read replication](https://developers.cloudflare.com/d1/best-practices/read-replication/), [D1 replication blog](https://blog.cloudflare.com/d1-read-replication-beta/)
- [R2 pricing](https://developers.cloudflare.com/r2/pricing/), [R2 data location](https://developers.cloudflare.com/r2/reference/data-location/), [How R2 works](https://developers.cloudflare.com/r2/how-r2-works/), [R2 is faster than S3 (2023)](https://blog.cloudflare.com/r2-is-faster-than-s3)
- [KV pricing](https://developers.cloudflare.com/kv/platform/pricing/), [KV limits](https://developers.cloudflare.com/kv/platform/limits/), [KV changelog 2025-08-22](https://developers.cloudflare.com/changelog/post/2025-08-22-kv-performance-improvements/), [Faster KV blog](https://blog.cloudflare.com/faster-workers-kv/)
- [Durable Objects limits](https://developers.cloudflare.com/durable-objects/platform/limits/), [DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/), [DO data location](https://developers.cloudflare.com/durable-objects/reference/data-location/), [where.durableobjects.live](https://where.durableobjects.live/)
- [Containers limits](https://developers.cloudflare.com/containers/platform/limits/), [Containers pricing](https://developers.cloudflare.com/containers/pricing/)
- [Static assets billing](https://developers.cloudflare.com/workers/static-assets/billing-and-limitations/), [static assets headers](https://developers.cloudflare.com/workers/static-assets/headers/)
- [Placement](https://developers.cloudflare.com/workers/configuration/placement/)
- [Custom domains](https://developers.cloudflare.com/workers/configuration/routing/custom-domains/)
- [Workers Logs](https://developers.cloudflare.com/workers/observability/logs/workers-logs/), [Observability pricing](https://developers.cloudflare.com/observability/pricing/), [Analytics Engine pricing](https://developers.cloudflare.com/analytics/analytics-engine/pricing/)
- [Origin rules](https://developers.cloudflare.com/rules/origin-rules/)
- [Origin CA](https://developers.cloudflare.com/ssl/origin-configuration/origin-ca/), [Authenticated Origin Pulls](https://developers.cloudflare.com/ssl/origin-configuration/authenticated-origin-pull/)
- [Rate limiting](https://developers.cloudflare.com/waf/rate-limiting-rules/)
- Load Balancing: [docs](https://developers.cloudflare.com/load-balancing/), [geo steering](https://developers.cloudflare.com/load-balancing/understand-basics/traffic-steering/steering-policies/geo-steering/), [proxy modes](https://developers.cloudflare.com/load-balancing/understand-basics/proxy-modes/), [limitations](https://developers.cloudflare.com/load-balancing/reference/limitations/), [API](https://developers.cloudflare.com/api/resources/load_balancers/methods/create/), [DNS TTL](https://developers.cloudflare.com/dns/manage-dns-records/reference/ttl/), [how charges accrue](https://developers.cloudflare.com/billing/understand/how-charges-accrue/), [usage-based billing](https://developers.cloudflare.com/billing/understand/usage-based-billing/); third party: [mattgadient 2018 (unverified)](https://mattgadient.com/geo-dns-cloudflare-vs-route-53-a-look-and-short-test-results/), [oneuptime 2026](https://oneuptime.com/blog/post/2026-03-20-cloudflare-global-server-load-balancing-ipv4/markdown)
- [Geographic routing](https://developers.cloudflare.com/support/troubleshooting/general-troubleshooting/geographic-traffic-routing/), [judge.sh plan-pool list (unverified labels)](https://cloudflare-test.judge.sh/), [status page components (colo list)](https://www.cloudflarestatus.com/api/v2/components.json), [zone analytics by colo (GraphQL)](https://developers.cloudflare.com/analytics/graphql-api/migration-guides/zone-analytics-colos/)
- [Plans](https://www.cloudflare.com/plans/network-cdn.md), [plans (network/cdn)](https://www.cloudflare.com/plans/network-cdn/), [plans](https://www.cloudflare.com/plans/)
- [Network](https://www.cloudflare.com/network/)
- [Service-specific terms](https://www.cloudflare.com/service-specific-terms-application-services/), [ToS update 2023](https://blog.cloudflare.com/updated-tos/)
- [Outage 2025-11-18](https://blog.cloudflare.com/18-november-2025-outage/), [outage 2025-06-12](https://blog.cloudflare.com/cloudflare-service-outage-june-12-2025/)
- [QUIC 0-RTT](https://blog.cloudflare.com/even-faster-connection-establishment-with-quic-0-rtt-resumption/)

**Tigris:** [pricing.md](https://www.tigrisdata.com/pricing.md), [pricing](https://www.tigrisdata.com/pricing/), [bucket locations](https://www.tigrisdata.com/docs/buckets/locations/), [regions](https://www.tigrisdata.com/docs/concepts/regions/), [CLI buckets set](https://www.tigrisdata.com/docs/cli/buckets/set/), [bucket settings](https://www.tigrisdata.com/docs/buckets/settings/), [AWS CLI guide](https://www.tigrisdata.com/docs/sdks/s3/aws-cli/), [R2 benchmark (competitor)](https://www.tigrisdata.com/docs/overview/benchmarks/cloudflare-r2/), [Fly Tigris docs](https://docs.fly.io/tigris), [Fly community: t3 endpoint](https://community.fly.io/t/tigris-series-a-implications/26591), [Fly community: region switch](https://community.fly.io/t/does-a-bucket-region-switch-trigger-a-data-migration-process/27766), [Prisma evaluation](https://www.prisma.io/blog/evaluating-object-storage-providers-for-prisma-compute), and for the v2.3 Tigris-backed track: [TigrisFS](https://github.com/tigrisdata/tigrisfs), [TigrisFS docs](https://www.tigrisdata.com/docs/training/tigrisfs/), [TAG architecture](https://www.tigrisdata.com/docs/acceleration-gateway/architecture), [TAG repo](https://github.com/tigrisdata/tag), [Cloud Disk](https://www.tigrisdata.com/docs/cloud-disk/), [Goldsky case study](https://www.tigrisdata.com/blog/case-study-goldsky/), [Fly Sprites design](https://fly.io/blog/design-and-implementation.md), [Sprites block device](https://fly.io/blog/sprites-block-device.md), [Fly suspend-resume](https://fly.io/docs/reference/suspend-resume/), [go-pmtiles server docs](https://docs.protomaps.com/deploy/server)

**VM and dedicated providers:**
- FX: [ECB USD reference rate](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/eurofxref-graph-usd.en.html), [daily XML](https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml), [90-day XML](https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml)
- Hetzner: [price adjustment](https://docs.hetzner.com/general/infrastructure-and-availability/price-adjustment/), [AX41](https://www.hetzner.com/dedicated-rootserver/ax41/), [Robot cancellation and hourly billing](https://docs.hetzner.com/general/billing-and-account-management/cancellation/cancellations-robot/), [auction feed](https://www.hetzner.com/_resources/app/data/app/live_data_sb.json), [IPv4 pricing](https://docs.hetzner.com/general/infrastructure-and-availability/ipv4-pricing/), [VAT](https://docs.hetzner.com/general/billing-and-account-management/billing-at-hetzner/value-added-tax/), [statement 2026-02](https://www.hetzner.com/pressroom/statement-price-adjustment/), [notice 2026-05](https://www.hetzner.com/pressroom/standardization-and-price-adjustment-of-our-server-products/), [cloud block storage](https://www.hetzner.com/cloud/block-storage/), [volumes](https://docs.hetzner.com/cloud/volumes/overview), [primary IPs](https://docs.hetzner.com/cloud/servers/primary-ips/overview), [cloud billing FAQ](https://docs.hetzner.com/cloud/billing/faq/), [traffic](https://docs.hetzner.com/robot/general/traffic/), [AX matrix](https://www.hetzner.com/dedicated-rootserver/matrix-ax/), [EX matrix](https://www.hetzner.com/dedicated-rootserver/matrix-ex/), [server auction](https://www.hetzner.com/sb/), [cloud regular](https://www.hetzner.com/cloud/regular-performance), [cloud general purpose](https://www.hetzner.com/cloud/general-purpose), [cloud cost-optimized](https://www.hetzner.com/cloud/cost-optimized), [cloud locations](https://docs.hetzner.com/cloud/general/locations/), [cloud servers](https://docs.hetzner.com/cloud/servers/overview/), [DDoS](https://www.hetzner.com/unternehmen/ddos-schutz/), [heise](https://heise.de/-11185981)
- OVHcloud: [EU catalogue API](https://eu.api.ovh.com/v1/order/catalog/public/eco?ovhSubsidiary=IE), [US catalogue API](https://api.us.ovhcloud.com/v1/order/catalog/public/eco?ovhSubsidiary=US), [ASIA catalogue API](https://ca.api.ovh.com/v1/order/catalog/public/eco?ovhSubsidiary=ASIA), [EU availability](https://eu.api.ovh.com/v1/dedicated/server/datacenter/availabilities), [US availability](https://api.us.ovhcloud.com/v1/dedicated/server/datacenter/availabilities?planCode=24rise01-v2-us), [SGP availability](https://ca.api.ovh.com/v1/dedicated/server/datacenter/availabilities?planCode=24rise01-v2-sgp), [EU/US entities](https://community.ovhcloud.com/t/non-us-customer-using-ovhcloud-us-portal-for-vps-locations-policy-clarification-needed/53206), [APAC servers](https://support.us.ovhcloud.com/hc/en-us/articles/4404330636819-All-About-APAC-Region-Servers), [Rise](https://eco.ovhcloud.com/en/rise/), [Rise US](https://eco.us.ovhcloud.com/rise/), [Advance](https://www.ovhcloud.com/en/bare-metal/advance/), [VPS](https://www.ovhcloud.com/en/vps/), [VPS Singapore](https://www.ovhcloud.com/en/vps/vps-singapore/), [privatedevops (unverified)](https://privatedevops.com/news/ovhcloud-price-increase-october-2026-renewal)
- Netcup: [root servers](https://www.netcup.com/en/server/root-server), [RS 4000 G12.5](https://www.netcup.com/en/server/root-server/rs-4000-g12.5-iv-24m-eu), [Singapore](https://www.netcup.com/en/about-netcup/server-locations/singapore)
- Contabo: [VPS](https://contabo.com/en/vps/), [HostAdvice location fees](https://hostadvice.com/hosting-company/contabo-reviews/data-center-locations/)
- Scaleway: [Elastic Metal](https://www.scaleway.com/en/pricing/elastic-metal/), [networking](https://www.scaleway.com/en/docs/elastic-metal/reference-content/elastic-metal-networking/)
- Vultr: [plans API](https://api.vultr.com/v2/plans), [regions API](https://api.vultr.com/v2/regions), [overage](https://docs.vultr.com/support/platform/billing/what-is-the-bandwidth-overage-rate), [block storage](https://www.vultr.com/products/block-storage/), [block performance](https://docs.vultr.com/products/storage/block-storage/storage-performance-block-storage), [BYOIP](https://docs.vultr.com/support/products/network/can-i-use-my-own-ip-space-for-vultr-instances), [DDoS](https://docs.vultr.com/support/platform/billing/how-much-does-ddos-protection-cost)
- Akamai/Linode: [types API](https://api.linode.com/v4/linode/types), [regions API](https://api.linode.com/v4/regions), [transfer prices API](https://api.linode.com/v4/network-transfer/prices), [transfer docs](https://techdocs.akamai.com/cloud-computing/docs/network-transfer-usage-and-costs), [block storage](https://www.akamai.com/cloud/pricing/block-storage)
- DigitalOcean: [droplets](https://www.digitalocean.com/pricing/droplets), [bandwidth](https://docs.digitalocean.com/platform/billing/bandwidth/), [volumes](https://docs.digitalocean.com/products/volumes/details/pricing/), [regions](https://docs.digitalocean.com/platform/regional-availability/)
- UpCloud: [network transfer](https://upcloud.com/docs/products/networking/network-transfer/), [data centres](https://upcloud.com/data-centres/)
- Latitude.sh: [pricing](https://www.latitude.sh/pricing), [bandwidth](https://www.latitude.sh/docs/usage/bandwidth), [locations](https://www.latitude.sh/locations)
- Leaseweb: [getdeploying (third party)](https://getdeploying.com/leaseweb)
- IONOS: [VPS](https://www.ionos.com/servers/vps)
- Oracle: [Always Free](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm), [price API](https://apexapps.oracle.com/pls/apex/cetools/api/v1/products/?currencyCode=USD), [InfoQ](https://infoq.com/news/2026/07/oracle-cloud-free-tier-limits/)
- AWS: [i4g.large](https://instances.vantage.sh/aws/ec2/i4g.large), [m7g.large](https://instances.vantage.sh/aws/ec2/m7g.large), [Lambda MicroVMs images](https://docs.aws.amazon.com/lambda/latest/dg/microvms-images.html), [Lambda MicroVMs launching](https://docs.aws.amazon.com/lambda/latest/dg/microvms-launching.html), [data transfer price list](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AWSDataTransfer/current/us-east-1/index.json), [EBS](https://aws.amazon.com/ebs/pricing/), [VPC](https://aws.amazon.com/vpc/pricing/), [Route 53](https://aws.amazon.com/route53/pricing/)
- GCP: [network pricing](https://cloud.google.com/vpc/network-pricing). Azure: [bandwidth](https://azure.microsoft.com/en-us/pricing/details/bandwidth/)
- Fly.io: [pricing](https://docs.fly.io/about/pricing), [pricing.md](https://fly.io/pricing.md), [billing](https://docs.fly.io/about/billing), [regions](https://docs.fly.io/reference/regions), [machine placement](https://docs.fly.io/machines/guides-examples/machine-placement), [machine sizing](https://docs.fly.io/machines/guides-examples/machine-sizing), [CPU performance](https://docs.fly.io/machines/cpu-performance), [volumes](https://docs.fly.io/volumes/overview), [volume management](https://docs.fly.io/volumes/volume-manage), [volume states](https://docs.fly.io/volumes/volume-states), [snapshots](https://docs.fly.io/volumes/snapshots), [create volume API](https://docs.fly.io/api/machines/volumes/create-volume), [volume forking](https://docs.fly.io/blueprints/volume-forking), [flyctl volumes fork](https://docs.fly.io/flyctl/volumes-fork), [flyctl machine clone](https://docs.fly.io/flyctl/machine-clone), [Making Machines Move (blog)](https://fly.io/blog/machine-migrations/), [services](https://docs.fly.io/networking/services), [load balancing](https://docs.fly.io/reference/load-balancing), [fly-proxy](https://docs.fly.io/reference/fly-proxy), [dynamic request routing](https://docs.fly.io/networking/dynamic-request-routing), [autostop/autostart](https://docs.fly.io/launch/autostop-autostart), [understanding Cloudflare](https://docs.fly.io/networking/understanding-cloudflare), [SLA](https://fly.io/legal/sla-uptime/), community: [slow volume forking](https://community.fly.io/t/slow-18s-volume-forking/26735), [ORD capacity](https://community.fly.io/t/ord-region-has-zero-machine-capacity-volume-corruption-snapshot-restore-stuck/27440); [buildmvpfast (unverified)](https://www.buildmvpfast.com/blog/fly-io-pricing-2026-ram-increase-vs-hetzner)
- Image and snapshot replication (v2.2): Hetzner [snapshot FAQ](https://docs.hetzner.com/cloud/servers/backups-snapshots/faq/), [API spec](https://docs.hetzner.cloud/cloud.spec.json); Vultr [snapshot FAQ](https://docs.vultr.com/products/storage/snapshots/faq), [cross-region](https://docs.vultr.com/support/products/orchestration/can-i-use-a-snapshot-in-a-different-vultr-region), [restore time](https://docs.vultr.com/support/products/orchestration/how-long-will-my-snapshot-take-to-complete), [block storage FAQ](https://docs.vultr.com/products/storage/block-storage/faq); DigitalOcean [change regions](https://docs.digitalocean.com/products/snapshots/how-to/change-regions/), [snapshot pricing](https://docs.digitalocean.com/products/snapshots/details/pricing/), [limits](https://docs.digitalocean.com/products/snapshots/details/limits/); Akamai [capture an image](https://techdocs.akamai.com/cloud-computing/docs/capture-an-image), [clone](https://techdocs.akamai.com/cloud-computing/docs/clone-a-compute-instance); AWS [EBS snapshot copy](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-copy-snapshot.html), [EBS initialization](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-initialize.html), [Lightsail snapshot copy](https://docs.aws.amazon.com/lightsail/latest/userguide/amazon-lightsail-copying-snapshots-from-one-region-to-another.html), [Lightsail pricing](https://aws.amazon.com/lightsail/pricing/), [t4g.small](https://instances.vantage.sh/aws/ec2/t4g.small); Scaleway [snapshot import/export](https://www.scaleway.com/en/docs/instances/how-to/snapshot-import-export-feature/); OVH [instance backup](https://docs.ovhcloud.com/en/guides/public-cloud/compute/save-an-instance), [migration between regions](https://docs.ovhcloud.com/en/guides/public-cloud/compute/migration-between-regions), [public cloud catalogue](https://eu.api.ovh.com/v1/order/catalog/public/cloud?ovhSubsidiary=IE)
- Handoff candidates: [DO MicroVMs availability](https://docs.digitalocean.com/products/microvms/details/availability/), [Modal sandboxes](https://modal.com/docs/guide/sandboxes), [Daytona](https://www.daytona.io/pricing), [E2B](https://e2b.dev/pricing), [Blaxel](https://blaxel.ai/pricing)
- NVMe: [Samsung PM9A3 datasheet](https://toko.nale.co.id/wp-content/uploads/2024/02/PM9A3-NVMe-U.2-Datasheet_for-07_v1.5.pdf)
- IaC: [hcloud (Terraform registry)](https://registry.terraform.io/v1/providers/hetznercloud/hcloud), [scaleway](https://registry.terraform.io/v1/providers/scaleway/scaleway), [ovh](https://registry.terraform.io/v1/providers/ovh/ovh), [ovh_dedicated_server](https://github.com/ovh/terraform-provider-ovh/blob/master/docs/resources/dedicated_server.md); OpenTofu registry: [hcloud](https://search.opentofu.org/provider/hetznercloud/hcloud), [hetzner-robot (community)](https://search.opentofu.org/provider/silenium-dev/hetzner-robot), [vultr](https://search.opentofu.org/provider/vultr/vultr), [ovh](https://search.opentofu.org/provider/ovh/ovh), [bunnynet](https://search.opentofu.org/provider/bunnyway/bunnynet)

**CDNs:**
- Bunny: [network (PoP list)](https://bunny.net/network/), [scripting](https://bunny.net/docs/scripting/), [CDN pricing](https://bunny.net/pricing/cdn/), [pricing](https://bunny.net/pricing/), [storage](https://bunny.net/pricing/storage/), [DNS](https://bunny.net/pricing/dns/), [DNS records](https://bunny.net/docs/dns/records), [origin shield](https://bunny.net/docs/cdn/performance/origin-shield), [request coalescing](https://bunny.net/docs/cdn/request-coalescing), [Perma-Cache](https://bunny.net/docs/cdn/perma-cache), [middleware](https://bunny.net/docs/scripting/middleware), [middleware overview](https://bunny.net/docs/scripting/middleware/overview), [scripting pricing](https://bunny.net/docs/scripting/pricing), [scripting limits](https://bunny.net/docs/scripting/limits.md), [storage limits](https://bunny.net/docs/storage/limits), [storage zone limits](https://support.bunny.net/hc/en-us/articles/360020141672-Are-there-any-limitations-on-the-storage-zones), [Magic Containers pricing](https://bunny.net/docs/magic-containers/pricing), [persistent volumes](https://bunny.net/docs/magic-containers/persistent-volumes), [range requests](https://docs.bunny.net/cdn/frequently-asked-questions/range-requests), [video delivery](https://support.bunny.net/hc/en-us/articles/360012297779), [edge rules](https://docs.bunny.net/cdn/edge-rules/variable-expansion), [stale cache](https://bunny.net/blog/introducing-stale-cache-more-efficient-cache-handling/), [Bunny Database](https://bunny.net/blog/meet-bunny-database-the-sql-service-that-just-works)
- CloudFront: [features](https://aws.amazon.com/cloudfront/features/), [flat-rate features by tier](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/flat-rate-pricing-plan.html#pricing-plan-features), [pricing](https://aws.amazon.com/cloudfront/pricing/), [PAYG](https://aws.amazon.com/cloudfront/pricing/pay-as-you-go/), [flat-rate docs](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/flat-rate-pricing-plan.html), [flat-rate blog](https://aws.amazon.com/blogs/networking-and-content-delivery/amazon-cloudfront-flat-rate-pricing-plans-new-features-and-expanded-capabilities), [Lambda@Edge triggers](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/lambda-cloudfront-trigger-events.html), [how CloudFront works](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/HowCloudFrontWorks.html), [range GETs](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/RangeGETs.html), [origin selection functions](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/origin-mtls-cloudfront-functions.html)
- Fastly: [pricing](https://www.fastly.com/pricing), [shielding](https://www.fastly.com/documentation/guides/getting-started/hosts/shielding/), [request collapsing](https://www.fastly.com/documentation/guides/concepts/edge-state/cache/request-collapsing/), [Fast Forward](https://www.fastly.com/fast-forward), [OSM case study](https://www.fastly.com/customers/openstreetmap), [network map](https://www.fastly.com/network-map)
- Others: [Gcore CDN](https://gcore.com/cdn), [Gcore shielding](https://gcore.com/support/articles/214080309), [Gcore PAYG](https://gcore.com/developers/pay-as-you-go-cdn), [Gcore FastEdge (third party)](https://www.srvrlss.io/provider/gcore-fastedge/), [KeyCDN](https://www.keycdn.com/pricing), [CDN77](https://www.cdn77.com/pricing), [Google Cloud CDN](https://cloud.google.com/cdn/pricing), [costbench GCP (third party)](https://www.costbench.com/software/cdn/google-cloud-cdn/hidden-costs/), [Azure Front Door](https://azure.microsoft.com/en-us/pricing/details/frontdoor/), [Akamai](https://www.akamai.com/pricing), [costbench Akamai (third party)](https://costbench.com/software/cdn-edge/akamai), [dupple CacheFly (third party)](https://dupple.com/learn/cheapest-cdn-providers), [Troy Hunt on Cache Reserve](https://www.troyhunt.com/to-infinity-and-beyond-with-cloudflare-cache-reserve/), [centminmod](https://blog.centminmod.com/2022/05/19/2794/what-are-the-benefits-of-using-cloudflare-business-plan/), [Ghost forum](https://forum.ghost.org/t/integrate-bunnycdn-with-ghost-a-brief-guide/52929)

**DNS, TLS, steering:** [Gcore DNS](https://docs.gcore.com/dns/about-gcore-dns), [NS1](https://www.ibm.com/products/ns1-connect/pricing), [costbench CF LB (third party)](https://costbench.com/software/load-balancers/cloudflare-load-balancing/), [i.lease /24 (third party)](https://i.lease/blog/how-much-does-a-24-ipv4-block-cost-in-2026), [LE challenge types](https://letsencrypt.org/docs/challenge-types/), [LE 45-day certs](https://letsencrypt.org/2026/02/24/rate-limits-45-day-certs), [SecurityWeek](https://www.securityweek.com/lets-encrypt-cuts-default-certificate-validity-to-64-days-ahead-of-45-day-switch/), [RFC 8446](https://www.rfc-editor.org/rfc/rfc8446#section-2.3)

**Tiles, formats, data:**
- Protomaps: [builds.json](https://build-metadata.protomaps.dev/builds.json), [20261101.pmtiles](https://build.protomaps.com/20261101.pmtiles), [downloads](https://docs.protomaps.com/basemaps/downloads), [CF deploy](https://docs.protomaps.com/deploy/cloudflare), [server deploy](https://docs.protomaps.com/deploy/server), [cost](https://docs.protomaps.com/deploy/cost), [CLI](https://docs.protomaps.com/pmtiles/cli), [PMTiles CF Worker source](https://github.com/protomaps/PMTiles/blob/main/serverless/cloudflare/src/index.ts), [PMTiles discussion #98](https://github.com/protomaps/PMTiles/discussions/98), [Pois.java](https://github.com/protomaps/basemaps/blob/main/tiles/src/main/java/com/protomaps/basemap/layers/Pois.java), [basemaps changelog](https://github.com/protomaps/basemaps/blob/main/CHANGELOG.md), [Protomaps API](https://protomaps.com/api), [sponsors](https://github.com/sponsors/protomaps)
- Planetiler: [benchmarks](https://github.com/onthegomap/planetiler#benchmarks), [PLANET.md](https://github.com/onthegomap/planetiler/blob/main/PLANET.md), [v0.10.0](https://github.com/onthegomap/planetiler/releases/tag/v0.10.0)
- MapLibre: [MLT release](https://maplibre.org/news/2026-01-23-mlt-release/), [MLT paper](https://arxiv.org/pdf/2508.10791), [GL JS v5.12.0](https://github.com/maplibre/maplibre-gl-js/releases/tag/v5.12.0), [GL JS v6.16.0](https://github.com/maplibre/maplibre-gl-js/releases/tag/v6.16.0), [Android 12.1.0](https://github.com/maplibre/maplibre-native/releases/tag/android-v12.1.0), [iOS 6.20.0](https://github.com/maplibre/maplibre-native/releases/tag/ios-v6.20.0), [flutter plugin changelog](https://github.com/maplibre/flutter-maplibre-gl/blob/main/maplibre_gl/CHANGELOG.md), [MapOptions](https://maplibre.org/maplibre-gl-js/docs/API/type-aliases/MapOptions/), [setMaxParallelImageRequests](https://maplibre.org/maplibre-gl-js/docs/API/functions/setMaxParallelImageRequests/), [ambient cache](https://maplibre.org/maplibre-native/android/api/-map-libre%20-native%20-android/org.maplibre.android.offline/-offline-manager/set-maximum-ambient-cache-size.html), [Native PMTiles](https://maplibre.org/maplibre-native/android/examples/data/PMTiles/)
- Martin: [config](https://github.com/maplibre/martin/blob/main/docs/content/files/generated_config.md), [MLT conversion](https://github.com/maplibre/martin/blob/main/docs/content/postprocessing/mlt.md)
- [zstd support (secondary)](https://www.youngju.dev/blog/2026-07-17-content-encoding-zstd-dictionary-transport.en)
- OpenFreeMap: [site](https://openfreemap.org/), [ToS](https://openfreemap.org/tos/), [repo](https://github.com/hyperknot/openfreemap), [self-hosting](https://github.com/hyperknot/openfreemap/blob/main/docs/self_hosting.md), [benchmark](https://github.com/hyperknot/openfreemap/blob/main/docs/benchmark/results.md), [Wplace post](https://hyperknot.com/blog/openfreemap-survived-100000-requests/)
- [VersaTiles](https://versatiles.org/), [OSMF vector tile policy](https://operations.osmfoundation.org/policies/vector/)
- Hosted APIs: [Stadia](https://stadiamaps.com/pricing/), [CARTO](https://carto.com/basemaps), [MapTiler](https://www.maptiler.com/cloud/pricing/), [MapTiler sessions](https://docs.maptiler.com/guides/account/sessions-vs-requests/), [Mapbox](https://www.mapbox.com/pricing)
- PostGIS: [osm2pgsql manual](https://osm2pgsql.org/doc/manual.html), [Crunchy Data](https://www.crunchydata.com/blog/dynamic-vector-tiles-from-postgis)
- [bdon/cloudflare-r2-latency](https://github.com/bdon/cloudflare-r2-latency)
- Tile demand (deploy-cadence model): [OSM tile logs](https://planet.openstreetmap.org/tile_logs/), [tilelog format](https://pypi.org/project/tilelog/1.7.1a1/), [Wang et al. 2010](https://ch.whu.edu.cn/en/article/id/873), [InTechOpen](https://intechopen.com/chapters/38302)

**Immich and repo:** [FUTO two years later](https://immich.app/blog/futo-two-years-later), [Immich blog archive (FUTO location)](https://v1.110.0.archive.immich.app/blog), [2025 year in review](https://immich.app/blog/2025-year-in-review), [2024 year in review](https://immich.app/blog/2024-year-in-review), [#170](https://github.com/immich-app/geoshenanigans/pull/170), [#245](https://github.com/immich-app/geoshenanigans/pull/245), [repository.ts at 4a210795](https://github.com/immich-app/geoshenanigans/blob/4a210795d23484a7e9b48f1b4cf90e5d6cc1bfa7/tiles/src/repository.ts#L22)

---

## Appendix A: how conflicting track numbers were reconciled

This is v1's reconciliation of the 8 research tracks. Items that v2 superseded say so.

1. **Traffic.** Every track treated volume as unknown. A measured 7-day file was already in the workspace (13.7M requests/month, 73.8% hit ratio), so it is used as scenario R and as the hit-ratio input.
2. **Baseline cost.** Four models disagreed:

   | Track | S / M / L / XL |
   |---|---|
   | cf-baseline (80% hit) | $31 / $124 / $472 / $1,400 |
   | cf-optimize | $36 / $130 / $440 / $1,220 |
   | hybrid | $34 / $139 / $532 / $1,580 |
   | cdn-layer (requests only, no CPU) | $17 / $77 / $302 / $902 |

   All were replaced by one model at the measured hit ratio. "Request fee dominates" and "storage is noise" are false at R.
3. **Hetzner Cloud "unavailable"** was a hidden-tooltip scraping artefact. Only CX/CAX rows were actually unavailable.
4. **Fleet prices:**
   - OVH RISE-1 VIN base config is unavailable, so about $94 instead of $70.
   - Vultr São Paulo is $120, not $80.
   - Akamai GRU egress is $7/TB and Jakarta $15/TB.
   - Result: 3-node mix $258, not $234; 6-node $512, not $448. *v2: $255.91 and $509.91 at ECB FX.*
5. **RTT table.** vm-providers reported per-target maximum pings as averages. The corrected London averages are CF 13.5, NBG 29.4, FSN 31.3 and ASH 86.9 ms.
6. **Workers Cache topology.** Some tracks wrote "regional pools"; the docs say the upper tier "aggregates fills across the whole network". *v2.1: not resolved. The blog says "regionally tiered" and the Tiered Cache docs define Generic Global as regional pools, but the Workers Cache page says a fill anywhere serves "any Cloudflare data center". X1 decides; §4.A2 gives the sensitivity.*
7. **Hit overhead of the Worker.** Same-object keep-alive tests gave 2-10 ms; cross-object runs gave 10-20 ms. The report uses 2-10 ms like for like.
8. **Tigris "lazy regional placement" as the cause of slow misses.** Not established in v1: neighbour samples were cherry-picked, and a byte-identical re-read was still slow. Tigris lists syd/gru/jnb as Fly regions, so "no presence in Oceania/Africa" was dropped. *v2: measured. Cold placement and per-instance connection state each explain about half; service time is 0-8 ms (§2.2).*
9. **D1 in Sydney.** "Replica missing" was dropped, because routing is documented as non-deterministic. Mumbai's "411 ms" was a midpoint of bimodal samples. *v2: same-row repeats confirm a routing tail; SYD reads are consistently about 325 ms.*
10. **MLT savings.** −8% raw on the wire (Martin), −13…−17% only with an extra gzip, −5…−10% with Planetiler native. All three are reported.
11. **z15 savings.** −50% storage and −68% tiles apply only to truncation, which drops features. Folding z15 into z14 may not reduce bytes.
12. **Directory memory.** 353 MB is the gzipped size; decoded it is about 4.3 GB (go-pmtiles accounting). *v2.2: a node does not need it all in RAM at our load (VersaTiles about 1 GB in total, go-pmtiles 512 MB; §4.B shared facts).*
13. **Download and delta sizes.** Tigris single stream is 600-900 Mbit/s, so 19-31 min per copy. 1 Gbit line rate is 18.6 min, not 21. The daily delta is about 1.5-6 GB, not 1-3 GB (sampling seed and block-alignment corrections).
14. **Latency "p50" values** are weighted means of medians. The 27% distant-colo figure is a share of probes, about 8-11% of measured traffic. Singapore misses are 860 ms, not 1.0-1.4 s. *v2: the 8-11% is still an extrapolation; the production share is pending (Appendix C).*
15. **hybrid's C-option costs** ($92-818, $260-460, $464) and AX42 disk size (2×1.92 TB, unverified) were replaced by the corrected node prices.
16. **CloudFront PAYG at S** is about $52, not $99, because the free 1 TB was misapplied. Cache Reserve applies only to zone or proxy setups, not to today's Worker. DO SQLite storage on a two-copy basis is about $334, not $202.
17. **D1 cap.** Three retained deployments (3 × 3.5 GB) exceed the 10 GB per-database cap and need a second database.
18. **Fly per-node cost** differed by machine size ($93 vs $137). Both are shown. *v2.2: both were over-sized (performance CPUs, two-copy volumes). A small node is $31.45-35.57 per region (§4.D).*

---

## Appendix B: changes in v2, v2.1, v2.2 and v2.3

### B.1 What changed in the conclusions

- **Ranking.**
  - v1: A1 + A2 now, then B1 (moving to B2 near 300M), then B3.
  - v2: A1 + **A1b** now, with X7 and X1 run alongside; A2 after X1 and the Appendix C reads (2-6 weeks). Then **A10** if cold chunks still dominate. One EU origin follows only if misses stay high after that: **B2 by default**, B1-gw if per-request metrics are required, B1-P1 if the D1 + Tigris fallback is wanted as HA. B3 comes only after A7 is sized and production miss weights are in.
- **Headline speed-up.** v1 claimed 1.2-5× on misses; v2 has 1.2-3× (measured today vs modelled lower bound for B2, matched by warmth; +5-20 ms calibration margin not included; X2 validates), and 1.3-4× (about 2-2.5× warm-to-warm and cold-to-cold) on mean per tile. Model against model, today's architecture is not slower than one EU origin outside Europe and the regions today's routing sends to EU colos (Middle East, Africa, South Asia; with Europe about 67% of traffic); the measured gap is Tigris-internal.
- **Withdrawn:**
  - "Move to B2 at about 300M". B1 − B2 = $2 + $0.30 × N below about 76M and $1.40 + $0.308 × N above, positive at every volume.
  - "Measured R2 reads are not faster". Now unproven either way (X8).
  - "Most map views wait on at least one miss". Now 37-41%, modelled.
  - "B1 keeps metrics for about $6/month". Hits never run the Worker; that needs B1-gw or B1-fetch.
- **Staff time** dominates every option's cost at today's traffic: 12-month TCO +$10-16k for one EU origin (B2 $10.2k to B2-HA $15.6k) at an assumed $100/h. *v2.2: that figure is 85-100% assumed staff time and is now a secondary line; headlines use infrastructure cost (B.6).*

### B.2 Critic items

| # | Change in v2 | Where |
|---|---|---|
| 1 | Every option computed under both routings and both warmth states. Today 96-154 vs B2 38-76 ms per tile (B1 33-62, B3 28-46). v1's 31 ms is now the local-routing warm figure. | §3, §5.2-5.3, TL;DR 7 |
| 2 | Model run on today's architecture and calibrated: fully warm London 80 vs 73-91 ms measured, hits within 0-13 ms. Only 17-58% of today's misses explained; Tigris 27 vs 370 ms. Every B/C/D figure labelled modelled lower bound. One warmth definition for all options. | §5.1, labels |
| 3 | B1-gw and B1-fetch rows added, with costs and a "which metrics survive" table. The §8 rationale corrected. | §3, §4.B B1 |
| 4 | Break-even corrected: B1 326M (339M at v1 FX), B1-fetch 384M. B1→B2 framed as "$0.31 per million requests for per-request logic". | §2.4, §4.B B1 |
| 5 | B2-HA priced at $126 flat (121-152), break-even 233M. *Differs from the critic's $156/294M: a 2-origin failover LB is $5-10, and the second node is a cheaper second vendor.* | §4.B B2 |
| 6 | Hit ratio modelled by deploy cadence (73 / 69 / 57 / 36%; tiered 84 / 82 / 70 / 48%), with refill curves, costs, post-deploy origin load, warm connections, and stable URLs + serve-stale. *Against the critic: 73.8% is a 14-month no-deploy steady state, not a 6-monthly figure, and today's stack already flushes on every deploy.* | §7.2, §2.4 |
| 7 | LB re-priced: B3 $25 (15-50), B4 $40 (30-65). Proxied HTTP requests are not metered; LB meters DNS queries only. *The critic's "$1,000 at L" case does not arise.* | §4.B B3 |
| 8 | Per-region miss weights not available: request-share weights kept, a miss-share sensitivity added (+14-15 ms for single-EU-origin options), exact query given. Sub-region splits marked as unsourced. **Pending production split.** | §5, Appendix C |
| 9 | Distant-colo share relabelled as an extrapolation; production queries written. *Against the critic: the Worker has no country tag, so the measurable quantity is the out-of-continent share, plus `asOrg`.* **Pending production split.** | §2.2, A7, Appendix C |
| 10 | Every tiered option shows tier hit, full miss and blended, all at the same u. A2's full miss = A1 + 26 ms modelled detour, replacing v1's unsupported "+10-30". | §3, §5.3, A2 |
| 11 | X7 added. The Tigris path was decomposed by forced-miss measurements. | §2.2, §8.3 |
| 12 | A10 added: Dual-Region `fra,iad` is +$5.19/month. *Against the critic's ~$17: only two regions are documented, and placement explains about half of the penalty, so A10 is gated on X7.* | §4.A A10 |
| 13 | R2 claim downgraded to "unmeasured for the design that would ship"; X8 added. | A4, TL;DR 11 |
| 14 | Argo added as B2a. Smart Tiered Cache confirmed free on all plans, so no Smart Shield charge in B2. | §4.B B2 |
| 15 | Staff time and 12-month TCO added, with a stated ops-hour basis and an assumed $100/h. | §7.1 |
| 16 | Planet growth projection (+7.1 GB/year) and years of headroom per node and for D1. | §4.B shared facts |
| 17 | Hetzner auction (all offers dearer than the AX41), Scaleway EM-A610R as an equal EU candidate, all-OVH B3 priced. *Against the critic's $247: that config is unorderable; the real figure is $269-316, with two OVH accounts.* | §4.B, §4.C |
| 18 | Hetzner Cloud CPX42 + 400 GB volume priced at €88. *Against the critic: Robot also bills hourly, and Cloud does have per-location availability.* | §4.B shared facts |
| 19 | Contract terms table (setup, minimum term, IPv4), VAT notes (Hetzner 8% Texas sales tax), ECB FX 1.1172, AWS IPv4 $3.65. | §4.C, header |
| 20 | Account-level allowances stated; marginal view added. *Against the critic: today costs slightly more in the marginal view, so B2 saves slightly more.* | §2.4 |
| 21 | "Most views" softened to a modelled 37-41%, with a production measurement method. | TL;DR 3, §5.4 |
| 22 | First load now 417 ms (direct) vs 109-168 (B2, today's routing) / 72-135 (local). | §4.C, §5.4 |
| 23 | B/C/D misses computed under today's routing. South/East Asia 3-origin misses are 146 / 144 ms, against 69 / 91 local. | §5.2, B3 |
| 24 | Footnote (f) on hit comparisons, object rows added, X9 same-object test proposed. | §3, §5.6, §8.3 |
| 25 | Per-CDN hit-ratio assumptions stated. Bunny Volume latency modelled (hit 32 vs 13 ms Standard). D3 re-costed at 22% / 16% edge miss. | §4.B, §5.5 |
| 26 | Probe population published (43 eyeball / 5 datacentre, median edge RTT 6 ms). *Against the critic: the access penalty adds equally to every option, so it does not hurt direct serving more.* | §5, caveat 5 |
| 27 | Explicit upper-tier term (hop + 2 ms lookup) in every tiered option. B2's "nearer warm" now rests on the modelled warm upper-tier connection. | §5, B2 |
| 28 | Bandwidth checked per node through XL; "flat" holds with ≤ $19 overage. Peak factor assumed at 3-4×. **Peak pending production split.** | §4.C |
| 29 | Handoff candidates (Lambda MicroVMs, DO MicroVMs, EC2+EBS, Modal, Daytona, E2B, Blaxel) dismissed with one sourced number each. | §4.C |
| 30 | Plan routing test: free step 0 done (tiles behaves like a Free-plan zone; a Business-listed zone was local for 6 of 6 affected probes). Business test priced at $250 + domain; prior odds stated. | A7, X5 |
| 31 | Deltas and slopes given from R to XL (A2-gw +3 / +10 / +50 / +201 / +603). | §2.4, §3, A2 |
| 32 | The public-read bucket suggestion was replaced by signed reads with a module-scope client, and the abuse exposure noted. | A1 |
| 33 | All nine sourcing gaps linked inline. B1 origin locations corrected to FSN or HEL (the AX41 is not sold in NBG). | inline |
| 34 | Bundled z0-z12 directories measured: 10.47 MB gzip, 146 MiB decoded as typed arrays. Ship them as a `Data` module and decode lazily. | A3 |
| 35 | Per-deploy ingest costs added to A6 (KV $18-36 per deploy). | A6 |

### B.3 New in v2 beyond the critic

- **Maintainer feedback:**
  - Workers Cache topology checked against the docs. *v2.1: the sources conflict (regional pools vs one global tier per key); X1 decides, and §4.A2 gives the sensitivity.*
  - The gateway + cached entrypoint pattern made first-class (A2-gw, B1-gw, P3), with its cost, the metrics it keeps and the unverified `Cf-Cache-Status` visibility.
  - A9 answers "can we control the top tier?".
  - The Workers Cache required changes listed: `cross_version_cache` + deploy key in the cache key, drop `Vary: Origin`, canonical host, no `Server-Timing` in cached responses, Smart Placement off for the gateway, and the Terraform spike.
- **A1b** (`t3.storage.dev` + module-scope client), from the measured Fly-endpoint penalty.
- **Same-row D1 repeats:** the D1 tail is replica or routing variance.
- **B1-P1 / B1-z / B1-gw-P1** variants, and the B1-vs-B2-HA crossover (about 150-170M).
- **Appendix C**, the production queries.

### B.4 Numbers that changed (selection; full list in `synthesis/v2/MODEL_V2.md` §7)

| Number | v1 | v2 |
|---|---|---|
| FX | EUR 1 = USD 1.16 (unverified) | 1.1172 (ECB, 30 Oct 2026) |
| AX41 / B2 / C1 $/month | 68 / 73 / 73 | 65.91 / 71 / 71 |
| Today $ R/S/M/L/XL | 15 / 33 / 134 / 512 / 1,521 | 15 / 33 / 134 / 514 / 1,525 |
| B1 $ | 79 / 90 / 152 / 382 / 998 | 77 / 88 / 149 / 380 / 996 |
| B3 LB / B4 LB $ | 268 / 527 flat | 286 (276-311) / 555 (545-580) flat |
| B5 Standard $ | 77 / 87 / 142 / 349 / 902 | 75 / 84 / 134 / 324 / 830 |
| D3 Standard $ | 32 / 44 / 106 / 340 / 963 | 33 / 45 / 115 / 376 / 1,071 |
| Break-even today vs B2 / B1 | 130-135M / "about 300M" | 125M / 326M |
| One-origin miss, weighted | 91 warm / 245 cold (local routing, no tier term) | B2 full 98-248 (today's routing; 104 cold external), tier 92, blended 95-242 |
| B1 miss, weighted | ~95 / ~250 | full 116-296 (cold external 260; P1 125) |
| 3-origin / 6-origin miss | 48 / 118; 40 / 93 | 60-133; 50-103 (today's routing) |
| Today mean per tile | 153 / 145 | 96-154 (warm hit + R5 to first-request hit + single-shot) |
| B / C / D mean per tile | 31 / 20 / 18 (local, warm) | B2 38-76, B3 28-46, B4 26-39 (today's routing) |
| A2 miss | "tier 45-120; full = today +10-30; ~250-320 blended" | tier 42, full 318-492, blended 180-267 (calibrated) |
| First map load today / B2 / C1 | 578 / 118 / 417 | 207-311 / 109-168 / 417 |
| P(view ≥1 miss) | ≈99% | 37-41% (21-27% origin fetch with tiering) |
| Hit ratio | 74% for every cadence | 73 / 69 / 57 / 36% by cadence |
| Bunny Volume latency | not modelled | hit 32 vs 13 ms (Standard) |

### B.5 v2.1 consistency fixes

From a consistency check of v2 (`consistency-check/`; figures re-run with `cost.py` v2 and `scale.py`):
- **Workers Cache topology** is no longer called resolved. The blog and Tiered Cache docs point to regional pools; the Workers Cache page says a fill anywhere serves "any Cloudflare data center". Both quoted with links (§4.A2). Sensitivity added: a regional tier at u = 0.3 gives a mean per tile of 75-110 ms, a global tier 65-91 ms; both beat today (§4.A2, §5.1).
- **Hit ratio rising with volume:** sensitivity added to §2.4 (A0 $373 at L; break-evens B2 about 153M, B2-HA 298M, B1 1,000M, B3 750M). §7.2's L adders are labelled as traffic-scaled; the constant-ratio figures are +$30 / +$110 / +$250. TL;DR 8 states the assumption.
- **Recommended package figures:** A1b + A2 calibrated (full 228-482, blended 135-262, mean 49-85 ms) added; 180-267 / 61-86 labelled as A2 on A1.
- **Timing:** A1 + A1b now; A2 after X1 and the Appendix C reads (2-6 weeks), matching §8.2.
- **Wording:** the 1.2-3× miss gain is labelled measured vs modelled; "model against model only in Europe" now includes the regions routed to EU colos (about 67% of traffic); "cold" → "cold-external" where it meant that; A1b's claim narrowed to the Fly endpoint and per-miss client (per-instance penalty is X7); A10's "best case" dropped (202-359, or 202-289 with a 200 ms term); TL;DR 7 floor 78-171 and A10 made conditional on X7.
- **Numbers:** gateway cost given against both baselines (+$4 / +$320 over A2, +$3 / +$201 against today); TCO range +$10-16k; B1 − B2 formula ($2 + $0.30 × N below about 76M, $1.40 + $0.308 × N above) and intercepts (A2-gw $15, B1 $76); A2 u-range at L/XL; A5/A6 marked as v1 model, A6 L delta +7…+156 (A5 −77); 60→90% saving 17-34%; marginal view exception for A4; Tigris GETs at R $1.74; B4 payback at XL 580 months; origin rate behind a tier 0.63 req/s; Bunny Volume 12 ms slower warm, about equal cold; cadence note +2-4 ms warm for B.
- **Tables and sources:** SE Asia row added to the local-routing table; Cloudflare Registrar price linked; the Smart Placement co-location claim re-quoted from the docs and blog.

### B.6 v2.2: small-VM rework

Triggered by maintainer feedback: the VM figures looked unsourced, small VMs should serve plenty of requests, and a disk image can seed a machine in another region. Three new tracks: **fly-docs** (Fly documentation and pricing constants), **planet-bench** (the full 139 GB planet served on the Hetzner box under emulated Fly CPU quotas, memory limits and volume caps) and **image-replication** (9 providers' snapshot and image documentation, plus a compressibility test of the planet). There is also a small-VM latency and cost model (`small-vm/MODEL_SMALLVM.md`). The rules were: no cloud accounts, resources or spend (documentation, public endpoints and the box only), and 189 anonymous Globalping tests.

**What changed in the conclusions**
- The maintainer was right on both points. v2.1 over-sized every VM, which inflated VM costs about 2-4× (about 3× per Fly region). Small machines serve the full planet comfortably, and Fly volume fork (or a snapshot at Hetzner Cloud, Vultr, DigitalOcean or AWS) seeds regions without manual copying. v2.1 never copied by hand either: every node pulled from Tigris.
- **Headlines are now infrastructure cost.** The "+$10-16k over 12 months" was 85-100% assumed staff time. It moves to a secondary, explicitly assumed line (§7.1).
- **Ranking.** A1 + A1b first is unchanged. After that, A2 (stay serverless) and a small fleet behind Cloudflare are alternatives, not sequential steps.
  - The fleet is **Fly-cf N=3** ($98 at R; modelled 30-40 ms per tile, first load 95-110) or **B3-s** ($126).
  - **B2-s** ($21) is the cheap EU-only origin. **Fly-direct N=3-6** ($102-202) is the simple single-vendor option up to M.
  - B3 and B4 on big nodes are superseded by B3-s and B4-s.
- **"Do not serve directly from VMs"** is narrowed to a single EU node. Fly terminates TLS near the user, but its first load is still slower than any Cloudflare-fronted fleet.
- **New "do not":** do not put Fly behind Cloudflare with Smart Tiered Cache on (an anycast origin collapses it to one upper tier), and do not size VMs with performance CPUs, 64 GB RAM or two-copy disks.

**New sections and rows**
- "Where the VM figures came from" (§4.B): an audit table of every v2.1 VM figure, its source, and whether it was measured or assumed.
- §4.B shared facts: planet-bench server results replace the extract-only claims; the CX43 is added as a small origin; replace-per-deploy replaces two-copy disks; fork and snapshot are added as alternatives to the pull.
- §4.B: B2-s, B3-s and B4-s.
- §4.C: small-node C options (C1-s, C3-s, C6-s), the small-node provider table, and the cross-region image/snapshot table.
- §4.D: D1 and D2 are replaced by Fly-cf, Fly-direct, Fly-cf-stc and Fly-wkr, with sizing, cost, speed, fork-vs-pull deploys, reliability and staff time.
- §5.7: the small-VM latency model, with the measured Fly anycast edges, a measured 7 ms Fly proxy overhead and per-region tables.
- §3 and §5.3: rows for every new option. §7.1: Fly ops basis and v2.2 TCO rows. §8.3: X10, the Fly small-fleet test ($10-30); X3 marked largely done. §9: Q12 (Fly or not).

**Numbers that changed**

| Number | v2.1 | v2.2 |
|---|---|---|
| Fly node per region | $93-137 (performance CPUs, 300-400 GB) | $31.45-35.57 (shared-cpu-1x 1 GB, 150 GB, snapshots off) |
| Fly behind CF, 3 regions (D1 → Fly-cf) R / M / L | $233 / $241 / $362, latency not modelled | $98 / $125 / $222; mean per tile 30-40 ms (modelled) |
| Fly direct (D2 → Fly-direct) R / M / L | 8 regions $474 / $586 / $1,363; "~34 ms" (text estimate) | N=3 $102 / $217 / $625, N=6 $202 / $315 / $703; 54 / 46 / 38 / 34 ms for 3 / 6 / 10 / 17 regions |
| One EU origin (B2) | $71 (AX41); break-even 125M | $71 on an AX41, or **$21 on a CX43 (B2-s)**; break-even about 26M |
| B3 / B4 (LB) | $286 / $555 | B3-s $126, B4-s $281 |
| C1 / C3 / C6 | $71 / $271-530 / $525-830 | C1-s $26, C3-s $111-209, C6-s $251-416 |
| Origin server time | 0.1-0.5 ms (extract); 3 ms in the model | Planet p50 / p99 5.1 / 9.1 ms (VersaTiles), 10.2 / 17.6 (go-pmtiles default) |
| Throughput per core | 19k req/s (extract) | 766 (VersaTiles), 288 (go-pmtiles default) on the planet; 45 on a shared-cpu-1x baseline |
| Node RAM | 64 GB, ≥ 4.5 GB directory cache | 1 GB (VersaTiles) or 512 MB (go-pmtiles) |
| Migration days | 8-17 (assumed) | 6.5-17.5 by option, with a stated basis (still assumed) |
| Fly ops h/month | "3-6" (no basis) | 1.75 / 2.0 / 3.0 (direct / behind CF / Worker; assumed) |
| First load, direct serving | 417 ms (C1) | 417 for one EU node; Fly-direct 205-265 (3 regions) to 148 (17) |

**Caveats carried forward:** no Fly or small VM was tested end to end. Fly latencies are modelled lower bounds built on single-sample anycast edges. Fork hydration speed and fork-transfer billing are undocumented. Capacity figures come from cgroup emulation, not Fly hardware, and shared-cpu-2x/8x are extrapolated. The 4× peak is assumed. Cloudflare LB prices are still estimates. X10 and X2 close these gaps. *v2.3: X10 ran live (B.7); X2 remains.*

### B.7 v2.3: live measurements

Triggered by maintainer inputs on 2026-11-02 (`workers-cache-notes.md`, bottom section): a new cluster per data update, fewer copies than regions, shared-cpu-1x/2x Machines, Fly suspend, and Tigris as shared storage with compute next to it. The maintainer approved a live-test budget with a $100 hard cap. Spend was about $1.42 (at most $6.43 if Fly bills fork hydration), and every resource was destroyed and verified gone.

**What was measured live** (§2.5, §2.6):
- a 4-region Fly volume fleet (VersaTiles, shared-cpu-1x 1 GB, 150 GB volumes; ams seed, forks to iad, sin and syd): seeding, fork hydration, Tigris pulls into each region, throughput, suspend and stop, and 48-probe worldwide latency next to today's service;
- two volume-less variants: go-pmtiles reading one 139 GB Tigris object (ams, iad, sin) and TigrisFS (ams), plus TigrisFS and go-pmtiles over S3 on the box;
- the Tigris endpoint test (X7) from Helsinki on production chunks, and a per-region read test of the production bucket from Fly Machines in lhr, iad, gru, nrt, sin and syd.
- Not tested: Fly behind Cloudflare (no Cloudflare credentials were used).

**What changed in the conclusions**
- **The Fly volume fleet is now measured, not modelled:** 51-55 ms per tile worldwide with no miss tail, first map load 184-195 ms, against today's 159 ms blended (459 ms misses) and 195-590 ms in the same run. About $128-136/month at R for 4 regions.
- **Ranking.** A1 + A1b stays first. Next come four small tests (Cloudflare in front of Fly, the Worker-side endpoint test, a per-region pre-warm, the Tigris support question). Then the Fly volume fleet is the main move, direct or behind Cloudflare depending on X11. A2 and B3-s / B2-s become the alternatives if Fly is not acceptable. Tigris-backed Machines without volumes are a later cost cut.
- **First load.** v2.2's "Fly-direct's first load is slower than any Cloudflare-fronted fleet" rested on modelled figures. Measured, Fly-direct's first load equals today's all-hit first load measured the same way; the Cloudflare-fronted figures remain unmeasured.
- **A10 dropped.** The production bucket lists 11-12 regions, yet every region's first read missed and three Asian and Oceanian regions were served from Johannesburg.
- **A1b refined.** t3 wins in Europe (251 vs 396 ms cold, 89 vs 185 warm, from Helsinki) but has no gru, syd or jnb presence, so the endpoint is chosen per continent until X12.
- **Deploys.** A parallel per-region t3 pull on performance-2x loaders (about 35 min, volume-write bound; derived) beats seed + fork (35 min + 43-159 min of hydration). Gate routing on the volume being `created`. Overlap is hours, not days.
- **Suspend** works (+0.42-0.49 s on the first request, caches survive), but saves only compute while volumes bill, and at today's traffic regions with a Machine are rarely idle for 6 minutes.
- **Sizing.** Fly's shared-cpu-1x baseline sustains about 20-30 req/s, not the emulated 45. Still ample at R; from S upward the busiest region needs one size more.

**New sections and rows**
- §2.5 "Live measurements on Fly.io and Tigris (v2.3)": method, latency tables, Fly volume operations, throughput, suspend, volume-less variants, the Tigris endpoint test, cost and caveats.
- §2.6 "Is tiles-geo replicated to every region? (v2.3)": the per-region table, the jnb finding and actions.
- §3: rows Fly-direct N=4 (measured), Fly-s3 and Fly-tigrisfs (measured, not recommended) and Fly-chunkcache (modelled); A10 marked dropped; A0 and A1b annotated with live results.
- §4.D: the Fly section rewritten around measured numbers, with the per-update cluster flow, region subsets, suspend and a new subsection "Tigris-backed Fly Machines without volumes".
- §5.3, §5.4, §5.7: measured rows and a model-against-measurement table.
- §7.1: Fly deploy row and a Fly-direct N=4 TCO row. §8 re-ranked; §8.3 gains a status column, results for X3, X7 and X10, and X11-X14. §9: Q2, Q4, Q8 and Q12 updated; Q13 (Tigris) and Q14 (test credentials) added.

**Numbers that changed**

| Number | v2.2 | v2.3 |
|---|---|---|
| Fly-direct per tile, weighted | 54 (N=3, modelled) | **51 warm / 55 never-requested (N=4, measured)**; like-for-like N=3 about 58 / 63 (derived) |
| Fly-direct first map load | 205-265 (N=3, modelled) | **184-195 (N=4, measured)** |
| Today, same-run measurement | miss 300-488, mean per tile 96-154 | miss 459 (482 on 11-region weights), warm 54, blended 159; first load 195-590 (379 expected) |
| Fork hydration | undocumented; 6-93+ min assumed | **43 / 86 / 159 min** (iad / sin / syd, concurrent, one source) |
| Per-region pull | 35-45 min if Tigris delivers ≥ 540 Mbit/s | about 35 min on a performance-2x loader (derived; volume-write bound, measured in ams); t3 delivered 98.5-216 MB/s; Fly endpoint stalled in sin |
| Reads on a hydrating fork / after `created` | cross-region (unmeasured) | 98-315 ms / 4.3-5.0 ms |
| shared-cpu-1x baseline capacity | 45 req/s (emulated) | 20 clean, 30 marginal (Fly); 279-285 with burst |
| Fly deploy overlap | 3 days (+$2.25 per region per month) | hours (about $0.50-1 per 4-region deploy) |
| Fly suspend | not measured | +0.42-0.49 s first request; idle timer about 6 min; stop → 20.6 s |
| Tigris endpoint (Helsinki, production) | public-object deltas only | t3 251 vs Fly 396 ms cold, 89 vs 185 warm (fresh connections) |
| A10 | +$5/month if cold chunks dominate | dropped |
| Fly connection setup | 7 ms proxy overhead (modelled edges) | TCP + TLS 54 ms weighted vs Cloudflare 36 |

**Caveats carried forward:** every live Fly figure is one run per setting on one day. Fly behind Cloudflare, B2-s, B3-s, A2 and every Cloudflare-fronted figure are still modelled. Fork-transfer billing is undocumented. Tigris's regional caching is unexplained until support answers. The production split (Appendix C) is still pending.

---

## Appendix C: production queries (pending production split)

The breakdown below was not available for v2, so the affected figures use request-share weights or assumptions. Run these and re-run `synthesis/v2/model.py` and `cost.py`.

**Inputs.**
- Every Influx point the Worker pushes carries `continent` (`request.cf.continent`), `colo`, `asOrg`, `apexDomain` and `protocol` (`repository.ts:273-279`). There is no country tag.
- Each request writes one point with value 1, so `sum_over_time(…[7d])` counts requests.
- VictoriaMetrics names Influx fields `<measurement>_<field>`:
  - `tiles_handle_request_invocation`: every request;
  - `tiles_cdn_hit_count{request_type, z}`: hits;
  - `tiles_tile_request_invocation` / `_duration{z}`: tile misses;
  - `tiles_get_tile_duration`: Tigris;
  - `tiles_d1_lookup_duration`;
  - `d1_query_duration{served_by_region, served_by_primary}`.
- The hit, miss and request metric names and Q1-Q8 were checked on a throwaway VictoriaMetrics fed with synthetic Worker lines. The duration and D1 metric names follow the same naming rule but were not checked.
- If production sets `-dedup.minScrapeInterval`, high-rate series lose samples. That undercounts volume and overstates the out-of-continent share. Check Q8 against the Cloudflare dashboard.
- Run as instant queries at `time=2026-11-01T23:53:00Z`, on data from **before** Workers Cache. After A2, hits no longer run the Worker.

```promql
# Q1 requests by client continent x serving colo (join offline with colo_share.py and table C.1)
sort_desc(sum by (continent, colo) (sum_over_time(tiles_handle_request_invocation[7d])))

# Q2 share of all requests served from a colo outside the client's continent (critic #9)
#    <EU>, <NA>, ... = the colo lists in table C.1 joined with "|" (starred colos go in both EU and AS)
sum(
     sum_over_time(tiles_handle_request_invocation{continent="EU", colo!~"<EU>"}[7d])
  or sum_over_time(tiles_handle_request_invocation{continent="NA", colo!~"<NA>"}[7d])
  or sum_over_time(tiles_handle_request_invocation{continent="AS", colo!~"<AS>"}[7d])
  or sum_over_time(tiles_handle_request_invocation{continent="OC", colo!~"<OC>"}[7d])
  or sum_over_time(tiles_handle_request_invocation{continent="SA", colo!~"<SA>"}[7d])
  or sum_over_time(tiles_handle_request_invocation{continent="AF", colo!~"<AF>"}[7d])
) / sum(sum_over_time(tiles_handle_request_invocation[7d]))

# Q3 per client continent: the Q2 numerator wrapped in sum by (continent) (...); Q4 is the denominator
sum by (continent) (sum_over_time(tiles_handle_request_invocation[7d]))

# Q5 who is detoured: the Q2 numerator grouped by network
topk(40, sum by (continent, colo, asOrg) ( <Q2 numerator> ))

# Q6 intra-continent detours the continent test cannot see (Mexico->DFW, Peru->EZE): compare asOrg with colo by eye
topk(60, sum by (continent, colo, asOrg) (sum_over_time(tiles_handle_request_invocation{continent=~"NA|SA|AS"}[7d])))

# Q7 tile hits and misses per colo: per-region miss weights (critic #8), the §4.B hit-ratio exponent, sub-region splits
sum by (continent, colo) (sum_over_time(tiles_cdn_hit_count{request_type="tile"}[7d]))
sum by (continent, colo) (sum_over_time(tiles_tile_request_invocation[7d]))

# Q8 total, to compare with the Cloudflare dashboard Workers request count
sum(sum_over_time(tiles_handle_request_invocation[7d]))

# Q9 zoom mix (A5 cost, §5.4): hits and misses by zoom
sum by (z) (sum_over_time(tiles_cdn_hit_count{request_type="tile"}[7d]))
sum by (z) (sum_over_time(tiles_tile_request_invocation[7d]))

# Q10 D1 replica routing (§2.2 item 2): where each colo's D1 reads are served, and their latency
sum by (continent, colo, served_by_region) (count_over_time(d1_query_duration[7d]))
histogram_quantile(0.9, sum by (served_by_region, vmrange) (histogram_over_time(d1_query_duration[7d])))

# Q11 Worker-side miss latency by continent (p50; repeat with 0.9 and 0.99), plus the Tigris part
histogram_quantile(0.5, sum by (continent, vmrange) (histogram_over_time(tiles_tile_request_duration[7d])))
histogram_quantile(0.5, sum by (continent, vmrange) (histogram_over_time(tiles_get_tile_duration[7d])))

# Q12 peak 5-minute request rate (req/s) and the 7-day mean, for the peak/average factor (critic #28)
max_over_time((sum(sum_over_time(tiles_handle_request_invocation[5m])) / 300)[7d:5m])
sum(sum_over_time(tiles_handle_request_invocation[7d])) / (7 * 86400)
```

**Using the results.**
- **Miss weights (#8):** per-continent misses from Q7, summed by continent, replace the request shares in `model.py`. Sub-region splits come from Q7 colos mapped to NA-East/Central/West and Middle East/South/East/SE Asia.
- **Distant-colo share (#9):** Q2 gives it directly. Or save Q1 and Q7 as `/api/v1/query` JSON and run `python3 -I scripts/colo_share.py colo_continent.csv q1.json [q7_hits.json q7_misses.json]` in `fixups-competitor-cdn-latency/`. To compare with the 8-11% extrapolation, add Q6 rows where a country with its own colo is served elsewhere, and subtract structural Middle East/Africa routing that also applies to www.cloudflare.com.
- **Country level:** zone analytics (GraphQL `httpRequestsAdaptiveGroups` by `coloCode`, [guide](https://developers.cloudflare.com/analytics/graphql-api/migration-guides/zone-analytics-colos/); the country dimension name is unverified) survive Workers Cache. Alternatively, add `country: request.cf?.country ?? ''` to the Worker's default tags.
- **Per-client burst miss share** (§5.4) and **hit age** (§7.2) need small Worker changes: a 1% hashed client sample to Analytics Engine, and a cached-at header.

**Table C.1: Cloudflare colo → `request.cf.continent`.** All 342 colos on the [status page component list](https://www.cloudflarestatus.com/api/v2/components.json), fetched 2026-11-02, with countries mapped to continents by the GeoNames convention. \* marks colos in TR, RU, CY, GE, AM, AZ or KZ, which count as in-continent for both EU and AS. CSV: `fixups-competitor-cdn-latency/colo_continent.csv`.

| Continent | Colos |
|---|---|
| EU (56) | AMS ARN ATH BCN BEG BOD BRU BTS BUD CDG CPH DME* DUB DUS FCO FRA GOT GVA HAM HEL KBP KEF KIV LCA* LED* LHR LIS LJU LUX LYS MAD MAN MLA MRS MSQ MUC MXP OSL OTP PMO PRG PRN RIX SKG SKP SOF STR TIA TLL TXL VIE VNO WAW WRO ZAG ZRH |
| NA (69) | ABQ ANC ATL AUS BGI BGR BNA BOS CLE CLT CMH CVG DEN DFW DTW EWR FSD GDL GND GUA HNL IAD IAH IND JAX KIN LAS LAX MCI MEM MEX MIA MSP OKC OMA ORD ORF PDX PHL PHX PIT POS PTY QRO RDU RIC SAN SAP SAT SDQ SEA SFO SJC SJO SJU SLC SMF STI STL TGU TLH TPA YHZ YUL YVR YWG YXE YYC YYZ |
| AS (124) | ACX ADB* AGR AIP AKX* ALA* AMD AMM AVA BAH BBI BDQ BEY BGW BKK BLR BOM BSR BWN CAN CCU CEB CGD CGK CGO CGP CGY CJB CKG CMB CNN CNX COK CRK CSX CTU CZX DAC DAD DEL DLC DMM DOH DPS DXB DYU EBL EVN* FOC FRU FUK FUO GYD* HAK HAN HFA HGH HKG HYD HYN ICN ISB IST* ISU IXC JAI JED JHB JOG JRG JXG KCH KHH KHI KHN KIX KMG KNU KTM KUL KWE KWI LHE LHW LLK* LUH LYA MAA MCT MFM MLE MLG MNL NAG NJF NQZ* NRT OKA PAT PBH PKX PNH PNQ RUH SGN SHA SIN SJW SZX TAO TBS* TEN TLV TNA TPE TYN UDR ULN URT VTE XFN XIY XNH ZDM |
| OC (14) | ADL AKL BNE CBR CHC GUM HBA MEL NOU PER PPT SUV SYD WLG |
| SA (46) | ARI ARU ASU BAQ BEL BOG BSB CAW CFC CGB CLO CNF COR CWB EZE FLN FOR GEO GIG GRU GYE GYN JDO JOI LIM LPB MAO MDE NQN NVT PBM PMW POA QWJ RAO REC SCL SJK SJP SOD SSA UDI UIO VCP VIX XAP |
| AF (33) | AAE ABJ ACC ADD ALG ASK CAI CPT CZL DAR DKR DLA DUR EBB GBE HRE JIB JNB KGL LAD LLW LOS LUN MBA MPM MRU NBO ORN OUA RUN TNR TUN WDH |
