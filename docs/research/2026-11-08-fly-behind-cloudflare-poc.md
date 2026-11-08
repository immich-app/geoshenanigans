# Fly.io volume fleet behind Cloudflare: live PoC (2026-11-08)

Follow-up to [the deployment options report](2026-11-01-deployment-options.md). Question from the maintainer: what does the Fly.io plan (small Machines serving the planet PMTiles from a local volume) look like with Cloudflare in front of it in a tiered cache setup, measured for real?

Everything was built, measured on Sunday 2026-11-08 between 13:51 and 16:53 UTC, independently re-derived from the raw data, and torn down. Spend was about $2.60.

## What was built

| Host | What it is |
|---|---|
| prod | tiles.immich.cloud as it runs today (Worker + per-colo cache + D1 + Tigris), with the every-request D1 warm-up restored by #503 |
| g | PR #489 (Workers Cache gateway + cached entrypoint + D1 + Tigris, race at 120 ms) on prod data, read-only, workers.dev |
| ft | The #489 gateway + cached-entrypoint structure, but the cached entrypoint fetches the tile from the Fly fleet instead of D1 + Tigris |
| ftr | ft with a 60 ms gateway race (the client colo fetches from Fly itself if the tier is slow) |
| cdn | A thin Worker on a temporary immich.cloud hostname that fetches Fly through the zone cache (`cacheEverything`), so the zone's own cache and tiering sit in front of Fly |
| fly | The Fly fleet directly, no Cloudflare |

**Fleet:** VersaTiles 5.6.0 on shared-cpu-1x 1 GB Machines in ams, iad, sjc, sin and syd, each with a 150 GB volume holding the 139 GB planet (build 20261101, the same data prod serves). All five regions were seeded in 56 minutes: one upload to a temporary Tigris bucket (117 MB/s), then parallel pulls by temporary performance-8x Machines, capped by the 128 MiB/s volume write limit. Decoded tiles matched prod on every host (56/56 in the smoke test, 195/195 against the fleet). VersaTiles needs `--minimal-recompression`, otherwise it recompresses to brotli for any client that accepts it (18-166 ms of CPU per tile).

## Results

Globalping firstByte (excludes DNS/TCP/TLS), 48 pinned probes, about 3 samples per probe per host, per-probe median → continent median → traffic-weighted (EU 62.9, NA 20.8, AS 11.0, OC 3.0, SA 1.7, AF 0.6). Paired differences are per probe with bootstrap 95% CIs.

| ms, weighted | prod | g (#489) | ft | ftr | cdn | fly direct |
|---|---|---|---|---|---|---|
| Never-requested tile | 371 | 507 | 167 | 166 | 155 | 48 |
| Pool-mate tile (filled ~1 h earlier from another colo) | 260 | 99 | 86 | 75 | 120 | 53 |
| Warm tile (cached at the colo) | 41 | 60 | 46 | 44 | 55 | 50 |

- Never-requested, paired vs prod: ft −173 [−234, −122], ftr −167 [−257, −138], cdn −208 [−269, −128]. In Europe alone the gain is smaller: ft −97, ftr −56, cdn −122 (9 probes). cdn vs ft −23 [−39, −3] is not robust (−26 [−60, 0] without the first round): read it as cdn ≈ ft ≈ ftr.
- Warm tiles: ft +6 [−4, 15] and ftr +5 [−4, 10] vs prod, i.e. no measurable cost on hits. g is +20 [7, 28].
- The zone cache does tier: 38 of 46 cdn pool-mate answers carried another colo's fill (within regional groups). ft showed the same (38/47).

Map sessions (London and Helsinki only, 320 sessions, 1,959 paired views per host, 18,528 tiles, 0 errors), view completion ms:

| | prod | g | ft | ftr | cdn | fly direct |
|---|---|---|---|---|---|---|
| p50 | 277 | 256 | 186 | 176 | 193 | 154 |
| p90 | 623 | 547 | 417 | 364 | 383 | 453 |
| p99 | 1117 | 1020 | 857 | 690 | 795 | 847 |
| mean | 333 | 304 | 233 | 207 | 228 | 212 |

Paired vs prod (mean / p90): ft −100 / −206, ftr −127 / −259, cdn −105 / −240. The sessions are a best case for the Fly hosts: every fetch went to ams over hot connections and a page cache warmed by near-simultaneous fetches from the other Fly-backed hosts.

## Where the time goes on a never-requested tile (ft, 167 ms weighted)

| Part | Weighted median | What it is |
|---|---|---|
| Client → Cloudflare and before the gateway handler | 42 (of which client RTT 11) | Paid by every request; a warm hit totals ~46 |
| Gateway work | 0 | It only forwards to the cached entrypoint |
| Workers Cache tier hop | 51 | Lower-tier miss → upper tier → cached entrypoint start → fill and stream back. Even when the upper tier is the client's own colo it costs ~25 ms, so about half is Workers Cache/entrypoint overhead, not distance. An upper-tier hit costs 31 ms |
| Upper tier → Fly | 58 | TLS to Fly's anycast edge, Fly's proxy, Fly's edge → Machine hop |
| VersaTiles read | 4 | Local volume |

The upper tier → Fly part is mostly connection setup on idle connections, not distance. The same paths during the sessions (hot connections, 22-67 origin req/s) were 2-3× faster: FRA → Fly cdg edge → ams 27 ms vs 93 ms in the Globalping rounds, AMS → ams 11 vs 37, CDG 18 vs 42. Real production traffic sends roughly 1-5 origin requests/s, so it sits between the two. Fly's anycast also lands some Cloudflare colos on a far edge (HKG → nrt → sin 373 ms, DFW → ord → iad 214 ms, GRU → iad 200 ms: no South American region), which a nrt/hkg and a gru region would fix.

The #489 baseline (g) was 166 ms slower than prod on never-requested tiles in this run (it was 54-63 ms on 2026-11-04). It has no real traffic, so its tier → Tigris connections and Tigris regional cache were cold (EU Tigris 253 ms at the tier vs 186 for prod); the cause was not settled.

## Cost (infrastructure only, $/month, marginal on the existing Cloudflare account)

Cloudflare billed 8.35M tiles requests in the last 7 days, i.e. **35.8M/month**. The 13.7M figure used earlier came from the Worker's own metrics, which undercount 2.25×. Average response 38.9 KB; average miss 40 KB.

| Option | 13.7M, 3 regions | 13.7M, 5 regions | 35.8M, 3 regions | 35.8M, 5 regions |
|---|---|---|---|---|
| Today (Worker + D1 + Tigris) | 14 | 14 | 26 | 26 |
| #489 | 18 | 18 | 36 | 36 |
| Fly + Workers Cache (ft) | 101 | 162 | 118 | 180 |
| ft + race (ftr) | 102 | 164 | 122 | 183 |
| Fly + CDN through a thin Worker | 98 | 160 | 112 | 173 |
| Fly + CDN, plain proxied DNS (not measured) | 94 | 155 | 100 | 162 |
| Fly direct | 102 | 164 | 122 | 184 |

- The fleet is 88-98% of every Fly bill: about $30 per region per month on shared-cpu-1x, of which $22.50 is the 150 GB volume. 3 regions = ams/iad/sin, 5 adds sjc/syd, 7 (gru/nrt) is about $216.
- A new cluster per data update costs about $1-2 per update with a 6 h overlap; weekly updates with a 72 h overlap would add about $67/month at 5 regions.
- Capacity: behind Cloudflare the busiest region (ams) sees 1.5-5 origin req/s at the measured peak, well inside a shared-cpu-1x (20 req/s clean on baseline CPU). The first hour after a cut-over is the stress point (9-24 req/s at ams at the evening peak), so cut over in the 03-06 UTC trough. Fly direct at 35.8M would need ams on shared-cpu-2x.
- The 60 ms race fired on 59-89% of misses (not the assumed 35%), up to +59% origin fetches: about +$1-3/month.

## Reading

- Behind Cloudflare, a Fly volume fleet roughly halves the never-requested tile time (≈ 155-167 vs 371 ms weighted) and cuts map-session view time by ~100-130 ms on average in Europe, with no measurable cost on cached tiles.
- The three front-ends are within noise of each other on never-requested tiles. ftr is best in the sessions (p90 364). cdn is the simplest but its upper-tier hits are slower (120 vs 86 ms) and it gives up the Worker's metrics and control.
- It costs roughly $75-160/month more than today, depending on region count and real traffic volume. The latency numbers are probably pessimistic for the Fly options (idle connections), and the race variant's origin load is higher than modelled.

## Not measured

The zone's tiered-cache setting (the token could not read it), Fly's actual invoice, sustained Fly capacity under production hit ratios, ft on a custom domain instead of workers.dev, IPv6, plain proxied DNS (option 4b), and the cause of g's larger gap in this run.
