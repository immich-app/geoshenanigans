# tiles.immich.cloud deployment options: summary (2026-11-02, v2.3)

Today `tiles.immich.cloud` is a Cloudflare Worker. It checks the per-colo Cache API (`caches.default`, not tiered), looks up PMTiles directories in D1 on a miss, and range-reads tile bytes from about 140k 1 MB chunk objects in Tigris. The live deploy key is `20250818`, and deploys are manual, about every 6 months ([#245](https://github.com/immich-app/geoshenanigans/pull/245) automates them). Measured traffic is 13.7M requests/month. This page summarises the [full report](2026-11-01-deployment-options.md) (v2.3); every number comes from it, and the links go to the sources and method. Labels: **m** = measured, **m-live** = measured on real Fly.io Machines and Tigris buckets (v2.3), **c** = calibrated (measured anchors for today plus modelled deltas), **mod** = modelled lower bound (the same network model for every option, not validated end to end). Costs are **infrastructure only**; staff time (engineer-days, ops hours) is an assumed estimate, shown separately.

**v2.3** ran the main alternative live: a 4-region Fly.io fleet, two Fly designs without volumes, a test of the two Tigris endpoints, and a per-region read test of the production bucket. The tests cost about $1.42 (at most $6.43), and everything was destroyed afterwards. Fly behind Cloudflare was **not** tested, because no Cloudflare credentials were used. Option labels: "-s" options (B2-s, B3-s, B4-s, C-s) run on small 160 GB VMs. Fly-direct, Fly-cf, Fly-cf-stc and Fly-wkr are small-Machine Fly.io fleets ([§4.D](2026-11-01-deployment-options.md#flyio-small-machine-fleets-v22-replace-v21s-d1-and-d2)); Fly-s3, Fly-tigrisfs and Fly-chunkcache are Fly Machines without volumes, reading Tigris. F1-F3 are hosted APIs. Where v2.1's over-sized VM figures came from is in [§4.B](2026-11-01-deployment-options.md#where-the-vm-figures-came-from-v22).

## What we measured

- **Traffic** ([§2.3](2026-11-01-deployment-options.md#23-traffic-measured-pending-scenarios)): 13.7M requests/month (5.3 req/s), 3.3M tile misses, a **73.8% per-colo tile hit ratio**; EU 63%, NA 21%, Asia 11%, Oceania 3%. 7 days of VictoriaMetrics data; check it against the Cloudflare dashboard.
- **Cost today** ([§2.4](2026-11-01-deployment-options.md#24-cost-baseline)): about **$15/month** at R, 78% of it storage and the $5 base fee. Cache hits are billed as Worker requests: $134 at 250M requests/month, $514 at 1B.
- **Misses are slow** (m): the traffic-weighted full miss is 300 ms with a warm isolate and 488 ms single-shot. The v2.3 re-measure from 48 probes gave **459 ms** for a never-requested tile and **159 ms blended** per tile.
- **Why: the path to Tigris, not Tigris itself** ([§2.2](2026-11-01-deployment-options.md#22-measured-latency)). Tigris service time is 0-8 ms. The Tigris endpoint test (X7) split a cold read from Helsinki: **cold placement in the serving Tigris region is 60-80%** of it, a new connection about 52 ms, the warm floor 33 ms. The Worker's Fly endpoint is slower than `t3.storage.dev` in Europe: **396 vs 251 ms cold, 185 vs 89 ms warm** on fresh connections.
- **The full planet on small machines** ([§4.B](2026-11-01-deployment-options.md#shared-facts-origin-side)): VersaTiles needs 1 GB of RAM and about 3 ms of CPU per tile. On a real Fly shared-cpu-1x it sustained **20-30 req/s on its CPU baseline** (the box emulation said 45) and 279-285 req/s while bursting. The busiest region today averages 0.8-3.4 req/s.
- **Other measured factors:** D1's tail comes from replica routing (Sydney about 325 ms every time). 12 of 48 probes reached this zone through a distant colo (about 8-11% of traffic, **extrapolated from probes**); the zone looks like it is in Cloudflare's Free-plan IP pool.

## Live measurements on Fly.io and Tigris (v2.3)

Details: [§2.5](2026-11-01-deployment-options.md#25-live-measurements-on-flyio-and-tigris-v23). Same 48 Globalping probes, same time, traffic-weighted p50 (ms), new HTTP/1.1 connection per test:

| Host | TCP + TLS | Warm tile | Never-requested tile | Blended at 73.8% hits | First map load (warm / cold; expected) |
|---|---|---|---|---|---|
| Today: Worker + Tigris | 36 | 54 | **459** | **159** | 195 / 590; 379 |
| **Fly volume fleet, direct**: VersaTiles on shared-cpu-1x 1 GB + 150 GB volume in ams, iad, sin, syd | 54 | **51** | **55** | **53** | 195 / 184; **190** |
| Fly, go-pmtiles reading one 139 GB Tigris object, no volume | 53 | 246 | 612 | 329 | 711 / 1,070; 828 |

- **The fleet has no miss tail and is faster than today in every region** on a blended basis: Europe 43 vs 121 ms, NA-East 32 vs 185, SE Asia 42 vs 442, Oceania 30 vs 350. Without a Machine nearby it pays the backhaul: NA-West 104-118 ms and Latin America about 220 ms per tile (Fly's edges forwarded the Americas to iad).
- **The model was close.** Like for like, the 3-region Fly-direct model was 4-9 ms optimistic (54 modelled; about 58-63 derived from the measurements). It was within 6 ms near a Machine and 12-52 ms optimistic where edges forward far away.
- **Fly sets up connections more slowly than Cloudflare** (54 vs 36 ms) and has anycast detours (Jakarta via lax to iad, Mumbai to ams, Dubai to sin).
- **Deploys:** a parallel per-region pull through t3 on a temporary performance-2x loader takes about **35 min** (derived: the ams seed was volume-write bound at 34.6 min, and t3 delivered 98.5-216 MB/s into iad, sin and syd; the Fly endpoint stalled in sin). Seed + fork took a 35 min seed, then **43 / 86 / 159 min** to `created` in iad / sin / syd. During hydration a never-requested tile took 98-315 ms; afterwards **4.3-5.0 ms** in every region.
- **Suspend:** **+0.42-0.49 s** on the first request after a suspend; caches survive. The proxy suspends after about 6 min idle and wakes the nearest suspended Machine rather than using a running one farther away. A stopped Machine takes **20.6 s** to serve again (VersaTiles reloads its directories).
- **Without volumes:** go-pmtiles on one big object is slow, because **Tigris did not cache the 139 GB object outside fra**; 1 MB chunk objects were cached in the reading region after about two reads (then 15-42 ms in ams and sin, 20-110 ms in iad). TigrisFS capped at 19-33 cold tiles/s and is not recommended.

## Is tiles-geo replicated to every region?

Details: [§2.6](2026-11-01-deployment-options.md#26-is-tiles-geo-replicated-to-every-region-v23). **On paper yes; in practice the data is not hot in each region.** The bucket is configured for 12 regions and the chunks list 11 in `X-Tigris-Regions`. One Fly Machine per region read random production chunks (16 KiB, new connection each, p50):

| Region | Fly endpoint: first read (served from) → repeats | t3: first read (served from) → repeats |
|---|---|---|
| lhr | 184 (lhr) → 114 / 120 | 164 (lhr) → 39 / 19 |
| iad | 310 (iad) → 182 / 91 | 273 (iad) → 220 / 99 |
| gru | 224 (gru) → 20 / 18 | 337 (**iad**; no t3 region) → 275 / 251 |
| nrt | 596 (**jnb**) → 215 / 97 | 396 (**jnb**) → 247 / 9 |
| sin | 671 (**jnb**) → 293 / 326 | 529 (**jnb**) → 292 / 304 |
| syd | 661 (**jnb**) → 132 / 98 | 364 (**sjc**; no t3 region) → 287 / 264 |

- **Every first read was a Tigris cache miss (160-670 ms)**, and Tokyo, Singapore and Sydney (Sydney on the Fly endpoint) were served from **Johannesburg**. Why is undocumented.
- **Actions:** ask Tigris support with these examples (X14); test a per-region pre-warm after each deploy (X13; about $0.85 for 12 regions); choose the Tigris endpoint per continent (t3 for Europe, North America and Asia; the Fly endpoint for South America, Oceania and Africa) until a Worker-side test (X12). **A10 (Dual-Region placement) is dropped.**

## All options at a glance

Today's baseline: full miss **300-488 ms (m)**, mean per tile **96-154 ms (m)**; v2.3 same-run: miss 459, blended 159 (m-live). Modelled figures are lower bounds (expect real ones 5-20 ms higher; the Fly-direct model was 4-9 ms low). "Full" is warm–cold, with the cold-external figure in brackets. Costs are $/month at **R = 13.7M** and **L = 1B** requests/month, with Δ against today's $15 / $514. Effort is engineer-days (assumed), plus ops hours a month where they differ from today's 1.5. Full rows: [§3](2026-11-01-deployment-options.md#3-master-comparison).

| Option | What changes | Speed | $/mo R · L (Δ R · L) | Effort | Main risk |
|---|---|---|---|---|---|
| **Stay on Cloudflare** | | | | | |
| A0 | Today: Worker + Cache API + D1 + Tigris | m: full 300-488, mean 96-154; m-live: miss 459, blended 159 | 15 · 514 (0 · 0) | 0; 1.5 h/mo | 0.3-1.4 s misses outside the EU |
| A1 | Code quick wins: drop `SELECT 1`, bake in metadata, stream, directory LRU, drop `Vary: Origin`, sample metrics | c: full 292-466, mean 94-148 | 15 · 503 (≈0 · −10) | 1-3 d | dashboards change if metrics are sampled |
| A1b | A1 + module-scope Tigris client; t3 where it has a region, Fly endpoint elsewhere | c: full 202-456, mean 71-146; X7: t3 251 vs 396 cold | 15 · 493 (≈0 · −21) | 1.5-3.5 d | Worker-side gain unmeasured (X12); t3 lacks gru, syd, jnb |
| ~~A10~~ | ~~A1b + Tigris Dual-Region `fra,iad`~~ | **dropped in v2.3**: first reads miss even in listed regions | — | — | — |
| A2 | Workers Cache instead of `caches.default` | c: blended 180-267, mean 61-86; **with A1b: blended 135-262, mean 49-85** | 14 · 394 (−1 · −119) | 3-8 d; 1.75 h/mo | topology unconfirmed (X1); no Terraform attribute; per-hit metrics lost |
| A2-gw | A2 + uncached gateway → cached `ctx.exports` entrypoint | c: ≈ A2 + 5 ms | 18 · 715 (+3 · +201) | 4-9 d; 1.75 h/mo | 2× billed requests; `Cf-Cache-Status` visibility unverified |
| A3 | A2 + cached chunk/directory entrypoints, directories off D1 | mod: full 78-159, mean 29-44 | 14 · 434 (−1 · −80) | 8-15 d; 2 h/mo | range slicing on loopback calls unverified |
| A4 | R2 instead of Tigris | unmeasured for the shipping design (X8) | 12 · 472 (−3 · −41) | 3-6 d | one region per bucket |
| A5 / A6 | z0-z8 as static assets / tile bytes in D1, KV or DO | A5 no gain; A6 untested | A5 14 · 437; A6 150-340 at R | 2 d - 6 weeks | A6 is 10-20× today's bill |
| A7 | Plan change to fix distant-colo routing | affected users −30…−135 ms per request (m) | Pro +20-25 or Business +200-250 | low | plan-pool link unverified |
| A8 / A9 | Cache Reserve, Smart Placement alone, Argo / Durable Object shields | no gain / unmeasured | CR ~5-40 / +0.2 · +20-45 | — / 1-2 weeks | none fixes the origin path |
| **CDN + VM origin** | | | | | |
| B1 family | Thin Worker + Workers Cache + 1 EU origin (P1 placement, zone tier, gateway or fetch variants) | mod: full 104-309, mean 33-82 | 77-81 · 380-700 | 8-18 d; 3.5 h/mo | cold leg to the origin; gateway doubles requests |
| B2 / **B2-s** | CF zone proxy + Smart Tiered Cache + 1 EU origin (AX41 / small CX43) | mod: full 98-250, mean 38-76 | 71 · 71 / **21 · 21** | 8-12 d; 2.75 h/mo | no HA; fast only in Europe |
| B2-HA / B2a | B2 + a second-vendor origin and failover LB / B2 + Argo Smart Routing | mod: as B2 (mean 38-76) | 126 · 126 (~49 with 2× CX43) / 79 · 312 | 10-16 d; 4.75 h/mo / 8.5-12.5 d | LB price list unpublished / Argo per-GB rate last published 2017 |
| **B3-s** / B4-s | CF + 3 / 6 small origins behind an LB | mod: full 62-135 / 51-102, mean 28-46 / 26-39 | **126 · 126** / 281 · 281 | 8-16 d; 4.75-11 h/mo | Asia gain needs A7; two vendors |
| B3 / B4 (big nodes), B3-w / B4-w | v2.1 big-node versions (superseded by B3-s / B4-s) / Worker steering instead of an LB | mod: as B3-s / B4-s; -w full 78-182 / 68-151 | 286 / 555 flat; -w 267 · 570 / 521 · 824 | 10-21 d | -w: continent steering mis-routes detoured users |
| B5-B8 | Bunny, CloudFront, Fastly, Gcore and others + VM origin | mod: ≈ B2 (Bunny Std mean 30-64) | 71-106 at R; 161-4,209 at L | 10 d - 3 weeks | DNS and the Worker leave Cloudflare; metered CDNs dear from 250M |
| **VM-only, no CDN** | | | | | |
| C1-s / C3-s / C6-s | 1 / 3 / 6 self-run nodes, GeoDNS, TLS on the VM | mod: 87 / 44 / 36 per tile; first load 423 / 212 / 168 | 26 / 111-209 / 251-416 flat | 5-25 d; up to 23 h/mo | no DDoS shield; 24/7 pages |
| **Edge platforms** | | | | | |
| **Fly-direct N=4** | **Clients → Fly anycast; VersaTiles on shared-cpu-1x 1 GB + 150 GB volume in ams, iad, sin, syd** | **m-live: 51 warm, 55 never-requested, 53 blended; first load 184-195** | **128-136 · ~660 (+113-121 · +146)** | 6.5-11.5 d; 1.75 h/mo | no L7 shield; egress linear ($427 at L); no SLA below Enterprise |
| Fly-direct N=3 / 6 | Same, 3 or 6 regions | mod: 54 / 46 per tile | 102 · 625 / 202 · 703 | 6.5-11.5 d | as above |
| **Fly-cf** N=3 / 4 | CF zone proxy → the same fleet, Tiered Cache **off** | mod: full 61-98, **mean 30-40** (N=3); not tested live (X11) | **98 / 123-131 · 222 / ~255** | 7-12.5 d; 2 h/mo | tiering must stay off; Asian misses need A7 |
| Fly-cf-stc / Fly-wkr | Fly-cf with Smart Tiered Cache / thin Worker steering to 6 regions | mod: mean 38-72 / 28-42 | 64 · 124 / 203 · 563 | 7-17.5 d | stc uses one region only; wkr pays Workers fees |
| Fly-s3 | go-pmtiles reading one big Tigris object, no volume | m-live: 246 warm, 612 never-requested | ~45-50 for 4 regions | — | **not recommended** |
| Fly-tigrisfs | TigrisFS mount, no volume | m-live: in-Machine cold 341, warm 130; 19-33 cold tiles/s | ~45-55 | — | **not recommended** |
| Fly-chunkcache | Directories in RAM + today's 1 MB chunks + local chunk cache or TAG sidecar | mod: ~55-80 per tile once pre-warmed; 130-670 ms on region-cold chunks | **~40-60 for 4 regions** | not sized | needs code and a per-region pre-warm; later cost cut |
| D3 | Bunny-only: Edge Storage + Edge Script | mod: Std mean 27-49 | Std 33 · 376 | 10-20 d | no implementation exists |
| **Format, hosted, client** | | | | | |
| E1-E3 | Single `.pmtiles` origin (keep) / per-tile objects or PostGIS (rejected) / MLT, brotli, z14 | planet server time 5 ms p50 (VersaTiles) | part of B/C/D | — | old clients need a `/v2` URL for MLT |
| F1-F3 | OpenFreeMap, Protomaps API, metered APIs | F1 hit 23-26 (m) | 0 to 250-2,305 at R | medium-high | no SLA / leaked keys mean unbounded cost |
| G1-G2 | Inline tiles, preconnect, bigger mobile cache / client-side `pmtiles://` | G1 −20-70 ms on map open | 0 | app releases | slow rollout; G2 breaks old apps |

Break-evens against today: B2-s about 26M, B2 125M, Fly-cf N=3 about 230M, B3-s about 233M requests/month; Fly-direct is never cheaper (its egress slope, $0.43/M, is close to today's $0.51/M). At today's traffic every move costs more than it saves: +$6 (B2-s) to +$121 (Fly-direct N=4) a month in infrastructure. Most of the cost is assumed staff time: at an assumed $100/h the 12-month TCO deltas run from +$8.5k to +$13.6k, 85-100% of it staff time ([§7.1](2026-11-01-deployment-options.md#71-ops-and-reliability)).

## Workers Cache and the gateway pattern

Details: [§4.A2](2026-11-01-deployment-options.md#a2-workers-cache-instead-of-cachesdefault-recommended-after-the-x1-spike) and [§4.A9](2026-11-01-deployment-options.md#a9-controlling-the-upper-tier-gateway-placement-hints-durable-object-shields-our-own-tier).
- **Tiering.** Each colo is a lower tier with an upper tier above it. Whether that is regional pools or one global tier per key is unresolved (Cloudflare's sources conflict); the Workers Cache spike (X1) decides. Both readings beat today: mean per tile 75-110 ms (regional, u = 0.3) or 65-91 ms (global), calibrated.
- **Cost.** Hits skip the Worker and bill no CPU but **are still billed as requests**: $14 at R, $394 at L.
- **Gotchas:** set `cross_version_cache = true` (Wrangler 4.107.0+) and put the deploy key in the cache key; host is not in the key (emit a canonical host in `/v1.json`); drop `Vary: Origin`; keep `Server-Timing` out of cached responses; zone settings do not apply; no pre-warm API; static assets become billable; no documented Terraform attribute. **Run the Appendix C queries first**, because hits no longer run the Worker afterwards.
- **No code runs in Cloudflare's upper tier**, and we cannot choose where it is. We can control a gateway at the user's colo, a free placement hint for the miss path, Durable Object shields, or a zone tier next to our own origin.
- **The gateway** (an uncached entrypoint that records metrics and calls a cached `ctx.exports` entrypoint) keeps per-request metrics (hits = gateway requests − callee runs) for **+$4 at R and +$320 at L over A2**. Whether it can read `Cf-Cache-Status` is unverified (X1).

## Recommendation

Ranked plan, from [§8](2026-11-01-deployment-options.md#81-ranked-shortlist). v2.3 changes the order: the Fly volume fleet is now the only alternative **measured end to end**, at about 3× today's speed per tile (53 vs 159 ms blended) for about $113-121/month more.

1. **Now: the cheap Worker fixes (A1 + A1b)**, 1.5-3.5 engineer-days, about $15/month. Module-scope Tigris clients, t3 for Europe, North America and Asia and the Fly endpoint for South America, Oceania and Africa. Calibrated full miss 202-456 ms. Read the production numbers (Appendix C) before changing metrics.
2. **Next, within days and for a few dollars: four small tests.**
   - **Cloudflare in front of the Fly fleet (X11)**: rebuild the fleet for a day (about $1-5) behind a proxied Cloudflare test hostname with Tiered Cache off, and repeat the 48-probe run. **Needs a Cloudflare test hostname.**
   - **Worker-side endpoint test (X12)**, **per-region pre-warm test (X13)** and the **Tigris support question (X14)**.
3. **Then the main move (2-4 weeks): a Fly volume fleet.** VersaTiles on shared-cpu-1x 1 GB, one 150 GB volume per region in ams, iad, sin and syd ([§4.D](2026-11-01-deployment-options.md#flyio-small-machine-fleets-v22-replace-v21s-d1-and-d2)). 6.5-11.5 engineer-days (assumed).
   - **A new cluster per data update** (the maintainer's design): one app per data key, filled by parallel t3 pulls (about 35 min); route nothing until each volume is `created` and `/health` reports the new key; switch DNS or the Cloudflare origin; delete the old app after a few hours. Forks (43-159 min) are for disaster recovery and adding a region.
   - **Direct anycast (measured):** 51-55 ms per tile, first load 184-195 ms, $128-136/month at R, about $660 at L (egress). No shield, no SLA below Enterprise.
   - **Behind Cloudflare (modelled):** $123-131 at R, about $255 at L, 30-40 ms per tile modelled for 3 regions. Adds the shield and Cloudflare's hit path and cuts Fly egress to about 26%. **Choose it if X11 shows it at least as fast as direct**, or if the shield or the egress slope matter.
   - **Fewer copies than regions works:** Fly's edges forward to the nearest Machine. Add sjc (about $29/month) or gru (about $33) only if production numbers show NA-West or Latin America matter.
   - **Suspend** saves only compute ($6.70-8.50 per region) while volumes bill, and costs +0.45 s on the first request after about 6 min idle. Use it for second Machines and volume-less designs, never `stop`.
4. **If Fly is not acceptable:** **A2** (Workers Cache, $14/month; calibrated mean per tile 49-85 ms with A1b; gateway variant if per-request metrics are required), or **B3-s** ($126 flat) / **B2-s** ($21 flat, Europe only), all modelled.
5. **Later, a cost cut: Fly-chunkcache** (Machines without volumes reading Tigris, directories in RAM, a local chunk cache): about $40-60/month for 4 regions instead of about $130, once the pre-warm test and Tigris support show that regional copies stay hot, and the server exists.

**Do not:**
- route traffic to a hydrating Fly fork, or pull from Tigris in Singapore through the Fly endpoint;
- serve tiles by reading one big Tigris object, or through TigrisFS;
- put Fly behind Cloudflare with Smart Tiered Cache on (it collapses to one upper tier);
- size VMs with performance CPUs, 64 GB RAM or two-copy disks;
- put tile bytes in D1, KV or Durable Objects ($150-340/month at R), or use metered tile APIs ($250-2,300/month at R);
- expect leaving PMTiles to fix speed.

**Runner-up CDN:** Bunny Standard (not Volume), if A7 cannot be fixed and the production distant-colo share is material. **Deploy cadence:** with Fly-direct there is no CDN cache to refill; behind a CDN, deploy at most monthly with the key in the URL (+12-20 ms on today's mean per tile) or use stable URLs with serve-stale refresh ([§7.2](2026-11-01-deployment-options.md#72-deploy-cadence-and-caching-critic-6-modelled-unless-marked)).

## Experiments

Full protocols and results: [§8.3](2026-11-01-deployment-options.md#83-experiments-to-validate).

| # | Status | Question | Result or decides |
|---|---|---|---|
| X3 | **done** | Planet service time on small machines | 2.7-3.1 ms CPU per tile; 20 req/s clean on a shared-cpu-1x baseline (Fly), 279-285 bursting |
| X7 | **done** | Tigris endpoint test: where cold-read time goes; t3 vs the Fly endpoint (Helsinki + six regions) | cold placement 60-80%; t3 251 vs 396 ms cold in Europe; every region's first read misses; t3 worse in gru and syd |
| X10 | **done** (not part e) | Fly small-fleet test | latency met (51-55 ms per tile, 184-195 first load); fork slower than a pull (43-159 min); 30 req/s on the baseline missed (p99 87 ms); t3 pulls 98.5-216 MB/s |
| X11 | **new**, about $1-5 | Cloudflare in front of the Fly fleet | Fly-cf vs Fly-direct; needs a Cloudflare test hostname |
| X12 | **new**, free tier | Worker-side Tigris endpoint test from 8+ colos | which endpoint per continent in A1b |
| X13 | **new**, about $1 | Per-region pre-warm and retention | whether to pre-warm on every deploy; gates Fly-chunkcache |
| X14 | **new**, free | Tigris support question with the §2.6 examples | what `X-Tigris-Regions` guarantees; why reads come from jnb |
| X4 | to do, free | Production split queries (Appendix C) | miss weights, distant-colo share, zoom mix, peak rate; which extra Fly regions |
| X1 | if A2 stays an option | Workers Cache topology and the gateway's view | A2 go/no-go (tier hits ≤ 80 ms p50 in EU/NA) |
| X6 | partly done | Full new-cluster deploy dry run with cutover and rollback | < 45 min end to end, zero 5xx, rollback < 2 min |
| X2, X5, X8, X9 | only for the B, CF-plan, R2 and CDN options | VM-origin misses; plan-change routing; R2 binding reads; same-object CDN hits | as in §8.3 |

## Open questions

The ones that change the decision; the full list is in [§9](2026-11-01-deployment-options.md#9-open-questions-for-the-maintainer).

1. **Fly or not.** Is Fly.io acceptable for the tile path (no SLA below Enterprise, single-host volumes, no guaranteed region capacity)? If yes, Cloudflare in front or not: X11 measures the latency side.
2. **A Cloudflare test hostname** (and API access) for X11.
3. **Budget.** Is about +$113-121/month of infrastructure for the 4-region fleet acceptable, plus 6.5-11.5 engineer-days (assumed)? What rate should replace the assumed $100/h?
4. **Regions.** Is ams, iad, sin and syd the right start? Are NA-West (104-118 ms per tile without sjc) and Latin America (about 220 ms without gru) acceptable?
5. **Tigris.** May we send the §2.6 examples to Tigris support and run the pre-warm test? Is the later volume-less design of interest?
6. **The production split is pending** (Appendix C, Q1-Q12): run it before changing metric tags or enabling Workers Cache. Are per-request metrics (the per-apex RPS alert, colo dashboards) a hard requirement?
7. **Test credentials.** Please revoke the Fly org token, the Tigris test-bucket key and the production read key used for the v2.3 tests.
