# tiles.immich.cloud on Bunny Storage + Edge Script: live PoC (2026-11-09/10)

Follow-up to [other providers worth investigating](2026-11-09-other-providers.md). The whole planet (build 20261101) was served from Bunny Edge SSD storage in all 15 regions through a standalone Bunny Edge Script, with no servers, on a dedicated bunny.net account. It was measured against prod in the same window with the same 48 pinned Globalping probes as the [Fly PoC](2026-11-08-fly-behind-cloudflare-poc.md). The PoC stays up for hands-on testing in Immich and in a side-by-side comparison page.

Latency is Globalping firstByte, per-probe median, then continent median, then traffic-weighted (EU 62.9, NA 20.8, AS 11.0, OC 3.0, SA 1.7, AF 0.6). All differences are paired per probe, with bootstrap 95% CIs.

## 1. The answer

- **Never-requested tiles:** Bunny 146 ms against prod 357 ms weighted, a saving of 139 ms [92-240]. Bunny wins on every continent except Europe, where the saving (54 ms [-15, 220]) is not significant. The savings are largest far from Europe: Oceania -780 ms, Africa -624, South America -473, Asia -301, North America -176.
- **Tiles another visitor just requested elsewhere:** Bunny 127 ms against prod 308 ms, a saving of 153 ms [73-226].
- **Cached tiles:** Bunny 10 ms against prod 32 ms, a saving of 23 ms [14-30].
- **Map sessions** (London and Helsinki, 2 batches, about 430 views per host per batch): views finish 112-129 ms sooner at the per-view median. Bunny is faster on 93-96% of views. Its first views take 108-136 ms at the median against prod's 263-337 ms.
- **Storage reads inside the script:** median 8 ms, 99th percentile 135 ms over 6,395 reads from 21 cities. Only 2 of 8,400 reads took over 900 ms; one took 10 s.
- **Cost at today's traffic:** about $72-78 a month with all 15 storage regions (section 7), against about $26-29 for prod today.
- **Against the decision rule from the plan:**
  - Never-requested tiles ≤ 60 ms weighted: **not met** (146 ms as measured; 120 ms with a warm isolate). Section 3 explains why the 32-50 ms model was optimistic.
  - Storage 99th percentile ≤ 150 ms: **met** (135 ms).
  - Cost ≤ ~$80 a month: **met** (estimate).
- **Compared with the other options:** never-requested tiles are on par with or slightly better than Fly behind Cloudflare (155-167 ms, measured in a different window). Cached tiles are 4-5 times faster than anything behind our Worker (34-55 ms). No server needs running, and the 139 GB upload takes 22 minutes.

## 2. What was built

| Part | Detail |
|---|---|
| Storage | One Edge SSD storage zone. Main region Frankfurt, replicated to the 14 other SSD regions (London, Stockholm, Madrid, Prague, New York, Miami, Los Angeles, Seattle, Tokyo, Hong Kong, Singapore, Sydney, São Paulo, Johannesburg). S3 compatibility off. |
| Data | Re-cut from the planet with the repo's own deploy code (`tiles/src/deploy`: ChunkPlanner, toDirectoryRows). There are 138,883 chunk files of about 1 MB, every MD5 identical to prod's chunks. The 3,570,045 D1 directory rows became 55,782 block files of 64 rows, plus a 223 KB `index.u32` with each block's first tile id. |
| Internal pull zone | Storage origin. The script reads chunks (Range GET) and blocks through it, so reads go to the nearest replica. |
| Edge Script | A standalone script (Bunny's Deno runtime, 26 KB) that ports prod's lookup: z/x/y to tile id, binary search in the index, then the block (isolate LRU, then Cache API, then internal zone), then prod's `DirectoryString.findTile`, then one Range GET. It serves `/v1/{z}/{x}/{y}.mvt`, `/v1.json` and `/v1/style/{light,dark}.json` with prod's headers. |
| Public pull zone | Caches the script's answers by `Cache-Control`. Three edge rules: treat `.mvt` as compressible so the stored gzip passes through, restore `Vary: Origin`, and send no `Cache-Control` on bare errors (as prod does). |

**Correctness:**
- 2,409 tiles were compared with prod, every one decoded byte-identical, and Bunny's gzip bytes equal the stored chunk bytes.
- Headers match prod, with three exceptions that come from Bunny's CDN layer:
  - an extra `Vary: Accept-Encoding` line;
  - HEAD gets 200 where prod gives 405;
  - 304s keep `Content-Type`.

## 3. Latency against prod (Globalping, 48 pinned probes, 4 rounds, 2026-11-10 00:55-02:00 UTC)

| ms, weighted | Bunny | Prod | Bunny - prod [95% CI] |
|---|---|---|---|
| Never-requested | 146 | 357 | -139 [-240, -92] |
| Pool-mate (another probe requested it seconds earlier) | 127 | 308 | -153 [-226, -73] |
| Warm (cache hit) | 10 | 32 | -23 [-30, -14] |

Never-requested tiles per continent (probes in brackets):

| | EU (9) | NA (11) | SA (6) | AF (4) | AS (14) | OC (4) |
|---|---|---|---|---|---|---|
| Bunny | 127 | 184 | 354 | 131 | 148 | 158 |
| Prod | 276 | 395 | 666 | 727 | 534 | 902 |
| Bunny - prod | -54 [-220, 15] | -176 [-297, -145] | -473 [-528, -101] | -624 [-1431, -111] | -301 [-682, -226] | -780 [-910, -369] |

**Where a Bunny miss spends its time** (medians, from the script's Server-Timing, all never-requested misses):

| Step | ms |
|---|---|
| Client to PoP, CDN and isolate start (`pre`) | 41 |
| Index load from the internal zone | 28 |
| Block load from the internal zone (cache miss) | 34 |
| Chunk Range GET | 26 |
| Script total | 114 |

**Why this is slower than the 32-50 ms model:**
- At test traffic almost every miss landed on a cold isolate. A cold isolate makes four sequential subrequests: the index, the TileJSON, the block and the chunk. In 185 of 189 misses the index was loaded fresh.
- The internal zone had not cached the blocks yet.
- Production traffic keeps isolates and the internal zone warm, which removes most of the index and block cost; the warm-isolate estimate is 120 ms.
- **Fixes:**
  - bundle the 223 KB index into the script itself (the size limit is 10 MB), which removes the index subrequest;
  - keep hot blocks in the Cache API, which the script already does.

**Bunny PoP and replica routing:**
- **Oceania:** Australian and New Zealand probes are served from Sydney storage (27 of 28 cities).
- **Mumbai:** the script runs in Singapore and reads the Singapore copy (there is no Indian script location).
- **Seoul:** reads Tokyo, about 39 ms.
- **Mexico City:** reads Los Angeles, about 62 ms.
- **Madrid and Lagos:** some directory-block reads went through Italy to Frankfurt, about 80 ms.
- **East Asia:** 15 of 53 cities, probably mainland China networks, were served by the Los Angeles PoP.

**Reliability:**
- No Bunny failures in the 4 rounds; prod had 1.
- In the extra sweep of one tile per city across Asia, Oceania, South America and Africa (369 tests), 5 requests got Bunny's own 500 page, all from the Rio de Janeiro PoP. The same tiles served fine minutes later. This looks like a transient platform fault at one PoP.

## 4. Map sessions (paired, Bunny vs prod)

| Batch | Views per host | Prod median / mean | Bunny median / mean | Per-view median difference [CI] | Bunny faster |
|---|---|---|---|---|---|
| 1 (00:40 UTC) | 430 | 198 / 265 ms | 61 / 91 ms | -129 [-140, -117] | 96% |
| 2 (01:41 UTC) | 427 | 168 / 227 ms | 62 / 95 ms | -112 [-126, -95] | 93% |

- First views: Bunny's median was 136 and 108 ms against prod's 337 and 263 ms.
- Tile misses: Bunny's time to first byte was 34 ms at the median against prod's 128-134 ms.
- Both vantages are in Europe.

## 5. Storage read latency inside the script (2 rounds an hour apart, 21 cities)

- **Overall:** 6,395 reads, median 8 ms, 90th percentile 63 ms, 99th percentile 135 ms, 99.9th percentile 437 ms.
- **Slow reads:** 7-15 of 8,400 over 500 ms, 2 over 900 ms, and one 10 s outlier.
- **Near a replica:** usually 5-15 ms at the median.
- The ~4% of reads at about 1 s seen earlier on Bunny's own storage zones did not show up on Edge SSD.

## 6. Uploads and replication

- **139 GB dataset:** 194,668 files in 22.2 minutes over the HTTP API, with zero errors or retries, at about 111 MB/s (the box's 1 Gbps link).
- **Replication:** every object was on all 14 replicas about 12.7 minutes after the last upload. New York lagged by a few hundred chunks for about 12 of those minutes, so a deploy must wait for a full listing before switching over.
- **One million small files** (real z14/z15 tiles, 833 B mean):
  - 359 uploads/s at 32 parallel, 755 at 64 and about 1,110 at 96, with zero throttling.
  - 95% of a sample was on all replicas 1 s after the upload ended.
  - Deleting the whole folder took 220 s.
  - At about 1,100 files/s, a one-file-per-tile layout (637M files even with an ocean fallback) would take about 6.7 days per upload; z0-z12 alone (12M files) about 3 hours.

## 7. Side tests and cost

**Perma-Cache with a script origin:**
- Turned on directly, it breaks the zone: every miss returns a 502 and the script never runs.
- It works behind a second pull zone, and far PoPs then serve from the nearest storage copy in 6-20 ms.
- But its stored copies lose gzip (1.46 times larger) and the CORS header, and it caches `no-store` answers. **Not usable as is.**

**Subrequests are billed:**
- What the script pulls from the internal zone shows up as bandwidth on that zone, on top of the bytes delivered to users.
- Account-level charges still read $0 during the test (trial credit or billing lag), so actual billed amounts were not observable.

**Monthly cost at 35.8M tiles and 1.39 TB a month:**

| Item | $/month |
|---|---|
| Edge SSD storage, 141.6 GB × 15 regions × $0.02 | 42.5 |
| Delivery to users, $0.0141/GB blended | 19.6 |
| Subrequest bandwidth, at 22-35% of requests missing at the edge, each pulling the tile range plus often a block (~60-90 KB) | 8-13 |
| Edge Script requests and CPU, on misses only | 2-3 |
| **Total** | **~72-78** |

Prod today is about $26-29 at this traffic, and [#489](https://github.com/immich-app/geoshenanigans/pull/489) about $36. The Fly fleet behind Cloudflare was $173-183.

## 8. Caveats

- The PoC ran at test traffic, so isolates and the internal zone were mostly cold. That makes Bunny's miss numbers pessimistic, and the edge hit ratio is unknown.
- Bunny's caches are per server, so real traffic will hit less often than a per-PoP model suggests.
- Sessions ran from Europe only.
- The Fly numbers come from a different window (2026-11-08).
- A cache miss is billed twice (subrequest plus delivery), and abuse traffic costs bandwidth.
- HTTP/3 is not available on Bunny.
- On 2026-05-21 Bunny's own domain was suspended for 1.7 h, taking out CDN, storage and scripts together. Keeping today's Cloudflare path deployable behind a DNS switch is the cheap mitigation.
