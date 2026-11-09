# tiles.immich.cloud: other providers worth investigating (desk research, 2026-11-09)

Follow-up to the [deployment options report](2026-11-01-deployment-options.md) (sections 3, 4.B, 4.C, 4.D) and the [Fly-behind-Cloudflare PoC](2026-11-08-fly-behind-cloudflare-poc.md).

The maintainer asked: "Are there any other providers that might be worth investigating? What about bunny.net Magic Containers or something like that?" and "Bunny has their own CDN that we could possibly put in front of their compute, which might prove better than using Cloudflare with some other compute." This report combines four desk-research tracks run on 2026-11-09: Bunny Magic Containers, Bunny Storage + Edge Scripting, the rest of Bunny's CDN catalogue, and other providers. No accounts were opened and nothing was bought.

**Labels used below.**
- **m**: measured with Globalping (traffic-weighted firstByte, per-probe median → continent median → weights EU 62.9, NA 20.8, AS 11.0, OC 3.0, SA 1.7, AF 0.6).
- **s**: a stand-in measurement on a different object or path (for example a Bunny-owned font file instead of a tile). Indicative, not like for like.
- **r**: reasoned or modelled, not measured.
- **unverified**: not from a primary source.

All prices are list prices, ex-tax, checked on 2026-11-09, for 35.8M requests/month and 1.39 TB/month of edge egress.

## 1. The answer

1. **Worth a PoC: Bunny only.** Test Bunny CDN → Magic Containers (for latency) and Bunny CDN + Perma-Cache in front of one VM (for cost) on one account. The other vendors change cost, not latency.
2. **Bunny CDN → Magic Containers should beat Cloudflare + Fly on never-requested tiles:** ~45-60 ms weighted (r) against 155-167 ms (m). It has no Workers Cache tier hop (51 ms) and no cold cross-vendor upper tier → Fly leg (58 ms). No public Magic Containers endpoint exists, so this is modelled, not measured.
3. **Edge hits:** Bunny 9-14 ms (s) against 34-55 ms (m) for tiles through Cloudflare Workers. Most of the gap is the Worker: a plain Cloudflare zone hit was 12 ms in a 10-city median where Bunny was 3 ms. Bunny also serves Asia from local PoPs.
4. **Magic Containers holds the planet only split by zoom** across the 2 volumes an app may have (≤ 100 GB each, 10 MB/s each, blank for every new pod): z0-z14 68.8 GB + z15 70.1 GB, and a ~2 h fill per pod per update, against 35-56 min on Fly.
5. **Cost:** $118-127/month for 5 regions against $173-183 for Fly + Cloudflare, if billed RAM stays at 1-1.5 GB per pod. gVisor's docs say all sandbox memory, including its own file cache, is charged to the container's cgroup, and Magic Containers has no memory cap. If Bunny bills that figure, random reads over 139 GB could push billed RAM towards the 32 GiB limit (~$117 per region per month). Measure this first.
6. **Bunny takes away** Worker logic and metrics on hits (unless Edge Scripting runs before the cache, ~$8/month), HTTP/3, and free egress (~$19.5/month today, linear with traffic). Neither Magic Containers nor Fly has an Indian region.
7. **Perma-Cache** makes a single-origin design cheap ($42-45/month; repeat tiles ~46 ms, s; origin requests 3-7%), but never-requested tiles far from the origin stay slow (90-225 ms weighted, r).
8. **Others:** Vultr and Oracle cost ~$14-18 per region (Fly ~$30) with free egress: a cost cut behind the existing Worker, no latency gain. DigitalOcean's Global Load Balancer has no Japan, South America or Africa region. **Cloudflare Containers cannot hold the planet** (20 GB disk); R2-backed designs cost $59-86/month for 3-5 instances and would take ~200-330 ms per never-requested tile (r). No PoC.
9. **Order:** the staged Bunny PoC (section 4.1, starting with a ~$2 routing test that needs no planet); Vultr behind the Worker (section 4.2) only if tiles stay on Cloudflare; Bunny Storage + Edge Script ($43-57/month, 1.5-3 weeks of code) if Magic Containers fails on volumes or RAM billing.

## 2. Comparison table

Latency in ms, traffic-weighted Globalping firstByte. Cost in $/month at 35.8M requests; "3 / 5" is the number of compute or replica regions. Baseline rows are measured on 2026-11-08/09 with the same 48 pinned probes.

| Provider / architecture | Regions (OC / AS / SA / AF) | 139 GB copy per region? | Never-requested | Warm / cached | $/month, 3 / 5 regions | Ops burden | Maturity | Blockers |
|---|---|---|---|---|---|---|---|---|
| **Prod today**: Worker + per-colo cache + D1 + Tigris | Cloudflare edge; D1 replicas never answer from OC; Tigris regions | n/a (chunks in Tigris) | 371 (m) | ~41 (m) | 26-29 (not per region) | low (running) | mature | 0.3-1.4 s misses outside Europe; OC pays ~290 ms for D1 |
| **[#489](https://github.com/immich-app/geoshenanigans/pull/489) on Tigris**: Workers Cache gateway | as prod | n/a | 431 (m) | 34 (m) | 36-41 | low | new code | Tigris connection setup on misses |
| **[#489](https://github.com/immich-app/geoshenanigans/pull/489) on R2, bucket per colo** | 6 R2 location hints, none in SA, AF, India | n/a (6 full copies) | 330 (m) | 35 (m) | 60 | low | mature | R2 cold reads 263-458 ms in APAC, 615-734 ms at BOM |
| **Fly direct**, 5 regions (ams, iad, sjc, sin, syd) | 17 Fly regions: syd; sin, nrt; gru; jnb (no India) | yes, 150 GB volume | 48 (m) | 50 (m) | 122 / 184 | medium | mature; no SLA below Enterprise | egress $0.0237/GB grows with traffic; no shield; 120-450 ms where the fleet has no region (SA, AF, India) |
| **Fly behind Cloudflare Workers Cache** (ft, ftr, cdn front-ends) | as Fly, behind Cloudflare | yes | 155-167 (m) | 44-55 (m) | 112-122 / 173-183 (100 / 162 plain proxied, not measured) | medium | mature | 51 ms tier hop + 58 ms upper tier → Fly on idle connections |
| **Bunny CDN → Magic Containers**, 1 pod per region | MC 41 regions: OC Sydney only; AS 9 (Tokyo, HK, SG, Bangkok, Jakarta, KL, Manila, Istanbul, Tel Aviv; no India, Korea, Taiwan); SA São Paulo, Bogotá, Mexico City; AF Johannesburg, Lagos | only split by zoom across the app's 2 volumes (≤100 GB each), 10 MB/s each, blank per pod; ~2 h fill per pod per update | ~45-60 (r; range 36-62); 3 regions ~48 with OC ~200 (r) | 9-14 (s) | 79-84 / 118-127, if billed RAM is 1-1.5 GB per pod (page cache could push it far higher) | medium: new app per update, 2 h fills, logs 5 days + syslog | MC since 2025-03, volumes since 2026-03; 8 status incidents involving MC in 2026 | volume caps; billed RAM under gVisor; PoP → region routing undocumented; no India region; nothing MC-specific measured |
| **Bunny CDN + Perma-Cache + 1 EU VM** (Hetzner CX43, VersaTiles) | 121 PoPs (OC 6, Asia 27, SA 17, AF/ME 14); Perma-Cache store on Edge SSD up to 15 regions (SYD; SG, JP, HK; BR; JH) | one copy on the VM; Perma-Cache keeps the requested subset (7-21 GB per monthly cycle, r) | ~90-225 (r): EU 45-60, OC 300+; pool-mate tiles ~46 (s) | 9-14 (s) | 42-45 (one origin) | low-medium: one VM, purge + folder delete per deploy | CDN mature; Perma-Cache since 2020 | slow first requests far from the EU origin; a miss is not guaranteed to be stored |
| **Bunny CDN (+ Load Balancer, + Perma-Cache) → Fly fleet** | as Fly | yes | ~45-90 (r) | 9-14 (s) | 112-130 / 172-190 | medium, two vendors | LB launched 2026-09-30 | cold TLS from ~120 PoPs to Fly's anycast edge (the leg that cost 58 ms behind Cloudflare) |
| **Bunny CDN + Load Balancer → Vultr VMs** (derived here, r) | as Vultr | yes (block volume) | ≈ the Fly row above (r) | 9-14 (s) | ~70-73 / ~109-112 | medium-high: own OS and TLS | both mature; LB new | as the Fly row; HDD block 500 IOPS |
| **Bunny Storage (Edge SSD) + Edge Script**, no servers | SSD regions up to 15 (SYD; JP, HK, SG; BR; JH), Frankfurt main; scripts on 72 PoPs (none in India; Johannesburg only in Africa) | yes: 139 GB per SSD region, ~$2.8 per region-month | ~32-50 central (r; range 19-114), +35-68 if the ~4% ~1 s storage tail seen on Bunny-owned zones applies to Edge SSD | 6-14 (s) | ~35-41 / 43-48 (8 regions 52-57) | low to run; 1.5-3 weeks of code to write | storage mature; before-cache scripts GA 2026-09-10 | no implementation; storage tail unmeasured on SSD; script → pull zone path undocumented |
| **Cloudflare Containers (A)**: `basic` instance per region + R2 bucket per region, behind the existing Worker + Workers Cache | location list unpublished; ME, OC, AFR cannot be used alone; no pinning under the recommended policy | **no**: 20 GB maximum disk and image, ephemeral | ~250-330 (r) | 34-41 (as [#489](https://github.com/immich-app/geoshenanigans/pull/489), m) | **59 / 86** (containers + Durable Objects + R2 $36 / $63, Workers ~$23) | medium | GA 2026-04-13 | cannot hold the planet; R2 cold reads; 1-3 s cold starts |
| **Cloudflare Containers (B)**: 20 GB custom instance with z0-z12 local (18.1 GB), z13-z15 from R2 | as (A) | no (subset only) | ~200-290 (r) | 34-41 (m) | **107 / 167** | medium | as (A) | as (A); the 18 GB is baked into the image or re-fetched on every start |
| **Vultr VM + 150 GB block volume behind the existing Worker + Workers Cache** | 33 regions: SYD, MEL; SGP, NRT, ITM, ICN, BOM, DEL, BLR, TLV; SAO, SCL, MEX; JNB | yes (block volume, Tigris pull) | ~140-170 (r, ≈ Fly behind Cloudflare) | 41-55 (as ft, m) | ~64 / ~103 (VMs $41 / $80 + Workers ~$23) | medium-high: OS, Origin CA or Tunnel, colo → origin map | mature | no latency gain over Fly behind Cloudflare; HDD block 500 IOPS; no provider HA |
| **Vultr VMs direct** with GeoDNS | as above | yes | ~40-55 (r) | ~40-55 (r) | 41-56 / 80-95 | high: TLS (DNS-01) and DDoS ours | mature | first map load at origin distance; GeoDNS mis-mapping |
| **Oracle OCI A1 + block volume** (behind the Worker, or direct) | ~50 regions: Sydney, Melbourne; Singapore, Tokyo, Osaka, Seoul, Mumbai, Hyderabad, Dubai; São Paulo, Santiago, Bogotá, Querétaro; Johannesburg, Casablanca | yes | as the two Vultr rows (r) | as the Vultr rows | 59-77 / 94-112 behind the Worker (VMs 36-54 / 71-89) | high: ARM, capacity, account set-up | mature | A1 "out of host capacity"; first 10 TB/month egress free |
| **DigitalOcean Droplets + Volumes + Global Load Balancer** | 16 datacentres: SYD1; SGP1, BLR1; no Japan, SA or AF | yes | unknown (edges not published) | unknown | 112 / 166 | medium | GLB GA 2024-09 | region gaps; GLB edge locations and routing unknown |

**How the less obvious figures were derived.**
- **Bunny → MC never-requested.** The bunny-mc model (max(hit, RTT to nearest pod) + 8 ms service + 3 ms overhead) gives 42.4 ms for 5 regions. The same model was 5 ms optimistic on the measured Fly fleet, and the bunny-cdn track inferred 50-60 ms from the measured CDN → storage-replica miss. The table therefore shows 45-60.
- **Bunny warm hit.** Two runs of the same font object (fonts.bunny.net) about 5 minutes apart (19:12-19:14 and 19:17 UTC), both selecting probes with the Fly PoC's 48-city filters (not necessarily the same physical probes), gave 8.7 and 13.7 ms weighted (all HIT). The difference is per-probe noise (Europe's median moved from 15 to 7 ms as London, Madrid and Warsaw changed). A hot font is the best case; long-tail tiles will hit less often (section 3.1).
- **Bunny intra-network miss (stand-in).** A unique query string on the same storage-backed zone forces a CDN miss answered by the nearest of 7 storage replicas: 45.9 ms weighted (m on the stand-in, 71 tests). That is the closest public proxy for "Bunny edge → Bunny origin in the next room".
- **Cloudflare-fronted VM and container rows** add the Fly PoC cost model's Worker + Workers Cache charge for the ft design at 35.8M requests ($22.67/month) to each provider's infrastructure cost.
- **Bunny CDN egress** is $0.0140-0.0141/GB blended on our traffic mix, about $19.5/month. It is in every Bunny row.

## 3. Per-provider notes

### 3.1 Bunny, product by product

| Product | Facts that matter for tiles | Verdict |
|---|---|---|
| CDN, Standard network | 119 PoPs (docs; the public region API lists 121: EU 35, Asia 27, NA 22, SA 17, AF/ME 14, OC 6). $0.01/GB EU/NA, $0.03 Asia/OC, $0.045 SA, $0.06 ME/Africa; no request fees; $1 monthly minimum. Hit 9-14 ms weighted (s). Caches are per server, not per PoP: 8 of 36 immediate repeats landed on another server in the same PoP, and all 8 missed. Files not requested for "typically 5 to 7 days" may be evicted | **Use**: the base of every Bunny design. Expect a lower edge hit ratio than the earlier report's 78% |
| CDN, Volume network | 10 PoPs, $0.005/GB; none in Oceania, India or Africa | **No**: saves ~$12/month, costs ~150 ms in Oceania |
| **Perma-Cache** | A storage-zone layer between CDN and origin. A miss first asks the storage replica nearest the PoP; if absent, the origin answers and the file is copied in the background. Billed as storage only (Edge SSD $0.02/GB per region). Our working set: 7-21 GB per monthly deploy cycle (r), ~$1-4/month in 10-15 SSD regions. Origin requests fall to ~3-4% (monthly deploys) or 5-7% (weekly), against 26% per colo today (r). Stand-in hit: 46 ms weighted (s). **Cannot be combined with Origin Shield**; unavailable when the origin is itself a storage zone; wildcard and tag purges stop working; a full purge switches to a new folder that must be deleted by hand; "a cache MISS on the CDN does not 100% guarantee" storage | **Yes for a single-origin design** (it acts as a regional upper tier with no eviction). In front of a 5-region MC or Fly fleet it moves the weighted mean little, because a pod answers a miss in about the same time; it still helps South America and Africa (BR and JH replicas), cuts Fly egress, and shields MC's 10 MB/s volumes after a deploy, for $1-4/month. Whether it works with an MC or Edge Script origin, or together with the Load Balancer, is undocumented |
| Origin Shield | Free; two locations only, Paris or Chicago; mutually exclusive with Perma-Cache | **No** with multi-region origins (every miss would funnel through one site); possible with one origin and no Perma-Cache |
| Request coalescing | Merges identical concurrent misses, per PoP | **Yes**, mainly after deploys |
| **CDN Load Balancer** (2026-09-30) | $9.50/month per LB, 50M origin requests included, then $0.65/M; only misses count. Up to 10 origin groups and 50 origins. Group routing by geographic distance from the edge server or lowest average latency; active health checks and passive failover. Origins: HTTP, Storage, Edge Script, Magic Containers CDN endpoint (anycast endpoints only as plain HTTP origins) | **Yes** for any multi-origin design. It is also the documented way to control MC region choice: one MC app per region, one LB group each |
| Edge Rules | 50 per zone: cache time, headers, origin change, LB, rate limit, coalescing; Lua patterns since 2026-03-18 | **Yes** (CORS, Content-Type/Encoding, cache times) |
| Smart Cache | Caches only listed extensions (not `.mvt`) | **Keep off** |
| Burrow / Optimizer | $9.50 per zone; single external origin only; not with Origin Shield, LB, Perma-Cache or a storage origin | **No** |
| Edge Scripting | Bunny's own Deno runtime; 72 PoPs (none in India, Johannesburg only in Africa; others forward to the nearest scripting PoP); $0.20/M requests + $0.02 per 1,000 CPU-seconds; runs on misses only by default; before-cache mode and a regional Cache API GA on 2026-09-10; 128 MB memory, 50 subrequests | **Only** for the no-server design (D3) or per-request metrics (~$8/month in before-cache mode) |
| Edge Storage, Standard (HDD) | Up to 9 regions; vendor latency "30-300 ms" | **No** |
| Edge Storage, Edge SSD | Up to 15 regions, Frankfurt main, replication regions cannot be removed; $0.02/GB per region; storage → CDN traffic free; the storage HTTP API is one Frankfurt site (TCP RTT 251 ms from Sydney, m), so scripts must read through a storage-backed pull zone | **Yes** as the Perma-Cache store or for D3 |
| **Magic Containers** | Section 3.2 | **PoC** |
| Bunny Database | libSQL, public preview (free during preview), 1 GB per database, no Terraform resource | **No**: VersaTiles needs no directory store; D3 can keep directory blocks as storage files |
| Bunny DNS | Free since 2026-06-24, smart (geo/latency) records included; health checks HTTP on port 80 or ICMP, every 30 s, no alerts | **Not needed**: a DNS-only CNAME on Cloudflare DNS is enough for a Bunny pull zone |
| Bunny Shield (WAF, DDoS) | Basic free, Advanced $9.50/month, Business $99/month; the page lists 25M / 50M / 250M included requests with $0.70 / $0.65 / $0.60 per million overage, without saying which tier each belongs to | **Optional**; check the request allowance before enabling (at 35.8M, overage could reach ~$7.6/month) |
| Logging | Logging API with 3-day retention; syslog or Datadog forwarding (10-30 s delay, delivery not guaranteed); permanent storage as gzip files in a storage zone; statistics API | **Needed**: replaces the Worker's metrics |
| HTTP/3 | Not documented; not observed (no `alt-svc`, HTTP/3-only connections timed out from one London vantage) | **Gap** against Cloudflare's measured 1-RTT saving on new connections |
| Billing | Prepaid; an account negative for 4 days is disabled and services go offline; volumes and storage keep billing while negative | Turn on auto-recharge |
| Terraform / CLI | Official `BunnyWay/bunnynet` provider v0.20.1 (2026-11-06): pull zones (Perma-Cache, Origin Shield), load balancers, `compute_container_app` (regions, autoscaling, probes, volumes), `compute_script`, storage zones | Automation is covered |
| Stream, Fonts, image Optimizer, Smart Preloader | — | Not relevant |

### 3.2 Bunny CDN + Magic Containers against Cloudflare + Fly, part by part

| Part of the path | Cloudflare Workers Cache → Fly (measured 2026-11-08) | Bunny CDN → Magic Containers (expected) |
|---|---|---|
| Edge hit | 44-55 ms weighted with a Worker on the hit path (34-41 for prod and [#489](https://github.com/immich-app/geoshenanigans/pull/489)); a plain Cloudflare zone hit 12 ms (10-city median, OpenFreeMap's tile, where Bunny was 3 ms) | 9-14 ms weighted (s); Asia served locally (Mumbai 1 ms, Hong Kong 4, Tokyo 8) |
| New connection (TCP + TLS) | Cloudflare 36 ms, Fly direct 54 (earlier runs) | Bunny 26.7 ms (m, 2026-11-09); no HTTP/3 |
| Tier or shield on a miss | Workers Cache tier hop 51 ms, about half of it overhead even when the tier is the client's own colo | None needed with pods in 5 regions. Origin Shield (Paris or Chicago) stays off; Perma-Cache optional |
| Edge → compute | Upper tier → Fly 58 ms: TLS to Fly's anycast edge on idle connections, Fly's proxy, edge → Machine | Inside Bunny's network. Bunny: traffic enters at its edge and is forwarded "only to the regions where your application is actually running" through tunnels. Edge → pod is plain HTTP unless "SSL for origin" is set. 50 of the 74 /24 prefixes holding MC nodes also hold CDN edge servers, so pods probably share facilities with PoPs in the 41 MC metros (inference) |
| Choosing the region | Fly anycast from the Cloudflare upper tier; detours measured (HKG → nrt → sin 373 ms, GRU → iad 200 ms) | Undocumented for an app's own CDN endpoint. Documented alternative: one app per region behind the Load Balancer's geo or latency routing (+$9.50/month) |
| Server | VersaTiles on a Fly volume, 4 ms | VersaTiles on a 10 MB/s volume: ~4 ms for a 40 KB tile; a burst of 20 cold z15 tiles can queue 60-90 ms (r) |
| Never-requested tile | 155-167 ms (m) | ~45-60 ms (r) |
| Control and metrics | A Worker on every request: metrics, the race, a D1 + Tigris fallback | Edge Rules only on hits (headers, cache time, origin, rate limit); our code runs only in the pod, on misses. Logic on hits needs Edge Scripting before-cache (~$8/month at 35.8M, 72 PoPs). Metrics from CDN logs (3 days, syslog forwarding) and the statistics API |
| Egress | Free on Cloudflare; Fly egress on misses | CDN $0.01-0.06/GB, ~$19.5/month today, linear with traffic; no separate MC egress for traffic through the CDN |
| Monthly cost, 5 regions | $173-183 (m front-ends) | $118-127 (if RAM ≈ 1-1.5 GB per pod) |

**Why the conclusion survives a large model error.** Even if the edge → pod hop cost as much as Cloudflare's upper tier → Fly leg (58 ms), a never-requested tile would be about 100 ms, still well under 155-167 ms. Even if Bunny's per-server caches cut the edge hit ratio from 78% to 55%, the blended tile would be about 0.55 × 12 + 0.45 × 50 ≈ 29 ms, against roughly 65-70 ms for Fly behind Cloudflare (r, from the PoC's warm, pool-mate and never-requested figures). The risks that could sink the design are storage mechanics, RAM billing, routing surprises and maturity, not the latency model.

**Magic Containers facts** (Bunny docs and the public MC API schema, checked 2026-11-09):
- **Runtime:** Docker images (linux/amd64) in a gVisor sandbox. Up to 8 CPUs and 32 GiB RAM per pod, billed on use; 10 GB ephemeral storage (the pod is evicted above it); 1 Gbps in and out by default.
- **Pricing:** CPU $0.02 per CPU-hour of CPU time used; RAM $0.005 per GB-hour in 64 MB blocks, "including additional system overhead that is variable with the workload"; volumes $0.10 per provisioned GB-month (detached volumes keep billing); anycast IPv4 $2/month. Egress through the CDN is billed by the CDN only. The FAQ says ingress is free "if there are no endpoints", which leaves ingress on apps with endpoints unclear.
- **RAM risk.** The API exposes no per-app memory limit. gVisor's resource-model docs say application memory is backed by a memfd charged to the sandbox's cgroup (as shmem or file), and the Sentry keeps its own page cache for some files. If Bunny bills that cgroup usage, random reads over 139 GB would push billed RAM up until something reclaims it. Bunny does not document which figure it bills.
- **Volumes:** 2 per application, 100 GB maximum each (the API schema caps `size` at 100), 10 MB/s read and 10 MB/s write per volume (cgroup limit). Each pod gets its own blank volume; no backups, replication or snapshots; a volume is bound to its node, and a disk replacement recreates it empty.
- **Headroom:** z15 (70.05 GB) on an 80 GB volume leaves roughly 8 GB after filesystem overhead, about 2 years at the planet's central growth rate if z15 grows with it; the 100 GB cap lasts about 8 years. Each fresh app can size its volumes, so this is a cost note (+$2 per region at 100 GB), not a blocker.
- **Scaling:** min/max replicas per region (max 10 on standard accounts), no scale to zero. "Allowed" regions are provisioned by an ML provisioner from traffic; set allowed = required regions and min = max = 1 so no blank-volume pod appears.
- **Rolling updates:** with volumes, old pods stop before new ones start, so a region with one pod has a short outage on every image update. A fresh app per data update avoids that but costs a refill.
- **Fresh cluster per update:** create app `tiles-<key>` with two volumes and a loader sidecar, fill both volumes in parallel (~1.9 h each at 10 MB/s), report the key on readiness, switch the pull zone origin, delete the old app and explicitly delete its volumes. About 2-2.5 h end to end, ~$0.2 of overlapping volumes per region per update.
- **Serving two files as one tileset:** `go-pmtiles extract` has `--minzoom` and `--maxzoom` (checked in the source), and VersaTiles documents a `from_stacked` pipeline operation that takes each tile from the first source that has it; serving that pipeline was not tested. A small router in front of two VersaTiles processes also works.
- **Ops:** Terraform resource, REST API, CLI and a GitHub Action (image tag only). App and system logs kept 5 days, syslog forwarding; no Prometheus or OTLP export found. SLA coverage for MC is not stated.
- **Limits that shape a deployment:** 20 apps per account (enough for one app per region and blue/green at 5 regions), up to 10 pods per region, `Volumes per application: 2`.
- **Maturity:** announced 2025-03-06; volumes 2026-03-04 (a third party called them public preview; Bunny's docs carry no Preview label today). MC appears in 8 status incidents in 2026, including "Isolated 502s for Magic Containers" (2026-04-30, major, about four weeks, stale CDN endpoint data) and a major issue on 2026-06-01 (about 1 h, a hosting provider's network).
- **One failure domain.** On 2026-05-21 a registrar suspended the bunny.net domain for about 1.7 h, and Bunny's status page lists CDN, Edge Storage, Edge Scripting, Magic Containers and DNS among the affected components. With CDN and compute at one vendor, such an incident takes out hits and misses together. Keeping today's Cloudflare path deployable behind a DNS-only CNAME switch is the cheap mitigation.

**Cloudflare in front of Magic Containers** brings back the tier hop and the cold cross-vendor connection, so it should land near the Fly-behind-Cloudflare result (r). Not worth testing.

### 3.3 Cloudflare Containers (asked for explicitly)

| Item | Fact (Cloudflare docs, 2026-11-09) |
|---|---|
| Largest instance | `standard-4`: 4 vCPU, 12 GiB, 20 GB disk; custom types up to 4 vCPU, 12 GiB, 20 GB, at least 3 GiB per vCPU |
| Image | Image size equals instance disk (so ≤ 20 GB); 50 GB of images per account |
| Disk | Ephemeral, reset from the image on every start; snapshots capped at 20 GB |
| Limits can be raised? | "All instance size maximums and account limits on this page are defaults and can be increased" through the account team. Whether ~160 GB is grantable is unknown |
| Price | Memory $0.0000025 per GiB-second, disk $0.00000007 per GB-second (both as provisioned while running), CPU $0.00002 per vCPU-second of active use; egress $0.025/GB NA/EU (1 TB included), $0.05 OC/Korea/Taiwan, $0.04 elsewhere (500 GB included). Plus Workers and one Durable Object per container (128 MB billed per object) |
| Placement | Starts "in the location nearest to the incoming request with a pre-fetched image". Region constraints exist only in the deprecated `default` policy; ME, OC and AFR "cannot be used exclusively"; the `durable_object` policy (beta) takes no constraints. No location list is published |

**Worked monthly price** at 35.8M requests, with today's Worker + Workers Cache in front so the containers see ~15% of requests (5.4M):

| Design | Per always-on instance | Durable Objects | R2 (139 GB per regional bucket, $0.015/GB) | Workers + Workers Cache | **Total, 3 / 5 regions** |
|---|---|---|---|---|---|
| (A) `basic` (1 GiB, 4 GB), tiles range-read from R2 | $7.31 | $8.0 / $16.2 | $2.09 per region | ~$22.7 | **$59 / $86** |
| (B) custom 1 vCPU / 3 GiB / 20 GB, z0-z12 local | $23.39 | as (A) | $2.09 per region | ~$22.7 | **$107 / $167** |
| (C) hypothetical 160 GB disk, full planet local | $49.14 | as (A) | — | ~$22.7 | $178 / $285 (only if Cloudflare raised the limit) |

R2 Class B reads stay inside the free 10M per month; CPU is under $1. Expected never-requested latency: (A) ~250-330 ms and (B) ~200-290 ms (r), because the tier hop remains and R2 cold reads measured 150-460 ms in region. **Verdict: no**, unless Cloudflare grants ~160 GB of disk; even then each instance costs more than a Fly region.

### 3.4 Other compute providers (from the others track)

| Provider | Per region (smallest node + ~150 GB) | Egress at our volume | Notes |
|---|---|---|---|
| **Vultr** | $13.75 (vc2-1c-2gb $10 + 150 GB HDD block $3.75); $25 on NVMe block; São Paulo compute ×1.5 | $0 (2 TB pooled per vc2-1c-2gb, then $0.01/GB) | HDD block 500 IOPS / 100 MB/s, NVMe 10,000 IOPS / 400 MB/s (4K random read 2.7-3.2 ms); SYD offers only NVMe block; 33 regions. Official Terraform |
| **Oracle OCI** | $17.86 (A1 1 OCPU + 2 GB, 197 GB block); the free A1 allowance applies in the home region | $0 (first 10 TB/month free) | Widest coverage in LatAm, Africa and India; ARM (VersaTiles ships aarch64 builds); A1 capacity shortages; account friction (anecdotal, unverified) |
| Akamai / Linode | $20-27 (Nanode 1 GB or Linode 2 GB + 150 GB volume at $0.10/GB); São Paulo and Jakarta cost more | $0 (pooled) | 33 core regions in 26 cities, all with block storage, incl. Sydney, Melbourne, Tokyo, Osaka, Singapore, Jakarta, Mumbai, Chennai, São Paulo; distributed regions (Auckland, Johannesburg, Santiago, Bogotá, Querétaro …) have no block storage and unpublished prices |
| DigitalOcean | $27 (2 GiB + 150 GiB volume) | $0 (pooled) | Global LB $15/month incl. 25M requests and 1 TB, then $0.70/M and $0.02/GB, "route users to the nearest available backend", optional CDN caching; 16 datacentres, none in Japan, LatAm or Africa |
| Gcore Edge Cloud | unknown (calculator needs JavaScript) | $0 (VM egress free and unlimited) | 33 VM locations incl. Sydney, Tokyo, Mumbai, Dubai, Johannesburg, São Paulo; own CDN. Worth one more desk step for prices |
| Lightsail | $27 (2 GB + 150 GB block) | $0 inside bundles, but inbound transfer also counts | No Africa |
| Hetzner Cloud | EU CX43 €16.49 (160 GB local); Singapore ~$36 | EU 20 TB included | Singapore costs more than Fly sin |

**Ruled out on documented limits:** Koyeb (volumes 1-10 GB, only in Washington and Frankfurt, preview), Railway ("replicas cannot be used with volumes", 4 regions), Render (5 regions, disk $0.25/GB, one instance per disk), Northflank (about Fly's price plus $0.06/GB egress), Fastly Compute (no disk; bandwidth $0.12-0.28/GB), Deno Deploy ($0.20/GiB egress), Unikraft (5 metros, small volume quotas), Sliplane (Hetzner reseller), OVH APAC VPS (throttled to 10 Mbps after 1 TB), Akamai CDN (contract only).

**What the others track measured** (68 Globalping tests from the Fly PoC probes, against Vultr and Oracle public endpoints): a VM in the user's own region answers in 2-10 ms (São Paulo 4, Johannesburg 2, Mumbai 2, Seoul 5, Santiago 5), against 120-450 ms from the 5-region Fly fleet in South America, Africa and South Asia. The gain comes from having a region there, not from the vendor. Fly has gru, jnb and nrt too, at about $30 each, but no Indian region (its regions page lists 17: ams, arn, cdg, dfw, ewr, fra, gru, iad, jnb, lax, lhr, nrt, ord, sin, sjc, syd, yyz). The others track's "Fly has bom" and its 8-region Fly costs that include a bom region are wrong.

### 3.5 Corrections and updates to the 2026-11-01 report

| Section | The report said | Now (2026-11-09) |
|---|---|---|
| 4.D D3 | "Magic Containers is ruled out: 100 GB volume maximum and 10 MB/s" | Not ruled out: an app may have 2 volumes, so a zoom-split planet fits. 10 MB/s per volume and blank volumes per pod remain |
| 4.B B5 | Origin Shield "one EU or US site" | Paris or Chicago only; mutually exclusive with Perma-Cache; Burrow unavailable with either |
| 4.B B5 | Risk: "DNS and Worker move off Cloudflare" | DNS can stay on Cloudflare as a DNS-only CNAME; the Worker (and its metrics) still goes |
| 4.B hit-ratio table | Bunny Standard 78% edge hit, per PoP | Bunny caches per server: 8 of 8 repeats on another server in the same PoP missed. The real edge hit ratio is lower; how much is unknown |
| 4.B B5 costs | at 13.7M requests | At 35.8M requests and 1.39 TB, Bunny Standard egress is ~$19.5/month |
| 4.D D3 | "500 RPS per storage zone" | That is the S3 API limit. The HTTP API allows 250 concurrent connections per zone and 100 per IP, per storage server |
| 4.D D3 | Edge SSD "about 5 regions"; script runs at the serving PoP (unverified) | Up to 15 SSD regions, Frankfurt main, regions cannot be removed. Scripts run on 72 PoPs; others forward to the nearest one. Scripts run only on misses (now documented); before-cache mode GA 2026-09-10 |
| 4.D D3 | storage reads | The storage HTTP API and SDK go to one Frankfurt site; only a storage-backed pull zone reaches the nearest replica |
| 4.C small VMs | "Only server disks travel", so at least a 160 GB plan | True for the image route only. The recommended Tigris pull works onto a block volume, so the smallest VM + 150 GB volume is enough: Vultr $13.75-25, DigitalOcean $21-27, Linode $20-27, Oracle ~$18 per region |
| 4.C catalogue | Akamai ~26 cities; DigitalOcean 13 cities | Unchanged as city counts (33 Akamai core regions in 26 cities, 16 DigitalOcean datacentres in 13 cities). New: every Akamai core region has block storage, and DigitalOcean now sells a Global Load Balancer |
| 4.C DNS | Bunny DNS free, checks only on port 80 | Free now includes smart and scriptable records; checks are still port 80 (or ICMP) with no alerts |
| A8 | Containers infeasible | Still true after Cloudflare Containers GA (2026-04-13): 20 GB disk |
| new | — | Bunny CDN Load Balancer (2026-09-30) makes multi-origin Bunny designs concrete |

**Conflicts between the tracks, and how they were resolved.**
- The others track said Magic Containers "cannot hold the 139 GB planet". It can, split across the two volumes an app may have (bunny-mc track; docs checked).
- Bunny hit: 13.7 ms (bunny-mc) and 8.7 ms (bunny-cdn) on the same object and city filters, about 5 minutes apart; 5.8 ms (storage-edge) on 36 other probes. Reported as 9-14 ms.
- Bunny → MC never-requested: 42-48 ms (bunny-mc model) and 50-60 ms (bunny-cdn inference). Reported as ~45-60 ms.
- go-pmtiles `extract --minzoom/--maxzoom` was "unverified" in the bunny-mc track; both flags are in the go-pmtiles source.
- The others track listed Mumbai (bom) as a Fly region. Fly's regions page lists 17 regions and no Indian one.

## 4. PoC plan

Same Globalping method as the Fly PoC for every round: the 48 pinned probes, about 3 samples per probe, never-requested / pool-mate / warm tiles, per-probe median → continent median → traffic weights, paired differences with bootstrap CIs. Test hostnames stay on `*.b-cdn.net` (Bunny) or `workers.dev` (Cloudflare), so no DNS change is needed.

### 4.1 PoC 1: Bunny, staged (spend cap $40 on Bunny, ~$3 on Tigris; ~500-650 Globalping tests)

| Stage | Setup | Measures | Answers | Cost, time |
|---|---|---|---|---|
| **A. Routing and the edge → pod hop, no planet** | One MC app with pods in DE, NY or Ashburn, San Jose, SG and SYD running a tiny HTTP server (or VersaTiles on a small extract) that returns `X-MC-Region` and `Server-Timing`. Its CDN endpoint pull zone: Origin Shield off, coalescing on. Variant: one app per region behind a Load Balancer with geo groups | ~200 tests: never-requested (cache-busted) × 3 rounds, warm repeats; the `Server` and region headers | which region each PoP's misses reach; edge → pod overhead; miss firstByte against Fly direct (48) and Fly behind Cloudflare (155-167); app routing against LB geo routing | ~$2, half a day |
| **B. Perma-Cache with a single origin** | Edge SSD storage zone (FRA, UK, SE, NY, LA, SG, JP, SYD, BR, JH) connected as Perma-Cache; origin = VersaTiles on the build box serving the 20261101 planet with `--minimal-recompression`; Edge Rules for cache time, CORS and Content-Type/Encoding | ~200 tests: N, pool-mate tiles filled 10 and 60 min earlier from another continent, W; the `CDN-Cache`, `Perma-Cache` and `CDN-StorageServer` headers from all 48 cities | Perma-Cache hit time and replication delay; whether gzip MVT headers survive; origin requests per cold fill (box log); purge folder behaviour; side check: does Perma-Cache accept an MC origin | ~$3-5, one day |
| **C. The planet on MC volumes, 2 regions** | Split the planet on the box (`pmtiles extract --maxzoom 14` / `--minzoom 15`), b3sum, stage in Tigris (t3). New MC app in DE and SYD, two 80 GB volumes, loader sidecar with an in-stream hash, VersaTiles (`from_stacked` or a router). Fill with no endpoint attached, then add the CDN endpoint | fill time against 10 MB/s; VersaTiles start; in-pod random-read latency, single and in bursts of 20; **billed RAM over 24-48 h under a replayed request mix**; ~100 tests on never-requested tiles near each region; blue/green origin switch; app and volume deletion; zero run rate after teardown | whether the design is buildable and what it really costs | ~$10-25, 2 days |

**Decision rule.** Go ahead with Bunny CDN → MC only if all of these hold: never-requested ≤ 60 ms weighted in stage A; billed RAM ≤ 2 GB per pod in stage C; fill ≤ 3 h per pod; no unexplained 5xx. If billed RAM grows with reads, two fallbacks avoid the page cache: MC pods without volumes reading ranges from an Edge SSD storage zone through a pull zone (bunny-mc's option B, ~$55-70/month for 5 regions, r; large-object range caching unverified), or Bunny Storage + Edge Script (2-3 days of porting for a PoC, under $10). If stage A shows misses above ~100 ms, Fly direct stays the best measured option.

**The maintainer provides:**
- A decision to keep today's Cloudflare path deployable as a fallback if Bunny is adopted later (idle, it costs only its Tigris and D1 storage).
- A FUTO-owned bunny.net **paid** account with a card, ~$50 prepaid and auto-recharge on. The trial (1 CPU / 4 GiB, 3 regions, one volume ≤ 30 GB) can run only a 3-region stage A.
- An account-level API key in 1Password (Bunny's GitHub Action does not accept sub-user keys).
- Box time to split and serve the planet, and a temporary Tigris bucket.
- Approval for ~500-650 Globalping tests.
- Questions to Bunny support, ideally before stage C: can the 100 GB volume cap or 10 MB/s be raised; how a pull zone chooses the MC region and whether edge → pod connections stay warm; whether gVisor or page-cache memory is billed as RAM; whether ingress is billed on apps with endpoints; whether MC is GA and covered by the 99.99% SLA; capacity for 160 GB of volumes per region.

### 4.2 PoC 2, only if tiles stay behind Cloudflare: Vultr VMs behind the existing Worker (~$2, ~300-500 tests)

- **Build:** 3 regions (Amsterdam on HDD block, Singapore on NVMe block, Johannesburg on HDD block), each a vc2-1c-2gb with a 150 GB volume filled from Tigris, VersaTiles 5.8.0; a variant of the ft Worker with a colo → origin map on workers.dev; one Cloudflare Tunnel per VM for an A/B test.
- **Answers:** whether a per-region origin hostname or a Tunnel shrinks the 58 ms upper tier → origin leg; HDD vs NVMe block read latency for never-requested tiles; pull time into block storage; the cost cut against Fly (~$64 / ~$103 for 3 / 5 regions instead of $118 / $180).
- **The maintainer provides:** a Vultr account and API key, permission for a workers.dev test Worker, Tigris read keys, and a Zero Trust organisation for the Tunnel variant.

## 5. What was not checked

- **Nothing on Magic Containers was measured.** There is no public MC endpoint. The edge → pod hop, warm-connection behaviour, PoP → region routing, cold starts, image-pull times and the maximum image size are all unknown.
- **Billing unknowns that could change the answer:** which memory figure Bunny bills, and so whether file and page cache under gVisor counts (gVisor's docs make it plausible; worst case a pod at its 32 GiB limit bills ~$117 per region per month); whether ingress is billed on apps with endpoints (if it were, weekly fills of 139 GB per region would add tens of dollars a month); whether deleting an app deletes its volumes; minimum deposit; sales tax on a Texas billing address.
- **Volume behaviour:** IOPS and latency under gVisor, whether "GB" and "MB/s" mean 10^9 / 10^6, and whether the caps can be raised (the API schema hard-codes 100).
- **Bunny's real edge hit ratio for tiles** (per-server caches, 5-7 day eviction); only a canary on real traffic can show it.
- **Perma-Cache:** replication delay, fill guarantee, header preservation, the undocumented `PermaCacheType = Manual`, and whether it combines with a Load Balancer, an MC origin or an Edge Script origin.
- **Bunny Storage + Edge Script:** Edge SSD read tail (the ~1 s outliers were on zones of unknown tier), script overhead per miss, whether a script's fetch to a pull zone stays in the PoP and is billed, and replication time for 139 GB.
- **HTTP/3 on Bunny** was tested from one London vantage only.
- **Other vendors:** DigitalOcean GLB edge locations and routing; Gcore VM and volume prices; whether Cloudflare would raise Containers disk to ~160 GB; Oracle A1 capacity; block-volume read latency on every provider except Vultr's published fio figures; Akamai distributed-region prices.
- **Not researched at all:** Azion, Zenlayer edge compute, Leaseweb, CDN77/DataPacket compute, and support quality or account approval times at any vendor.
- **No new Globalping tests were run in this synthesis stage** (0 of 150). All latency inputs come from the four tracks (363 tests) and the earlier PoCs.

## 6. Sources

All checked on 2026-11-09. Saved copies are kept with the research data (not published).

**Bunny (docs, API and site):**
- Magic Containers: [index](https://bunny.net/docs/magic-containers/index.md), [regions](https://bunny.net/docs/magic-containers/regions.md), [limits](https://bunny.net/docs/magic-containers/limits.md), [persistent volumes](https://bunny.net/docs/magic-containers/persistent-volumes.md), [pricing](https://bunny.net/docs/magic-containers/pricing.md), [endpoints](https://bunny.net/docs/magic-containers/endpoints.md), [autoscaling](https://bunny.net/docs/magic-containers/autoscaling.md), [rolling updates](https://bunny.net/docs/magic-containers/rolling-updates.md), [FAQs](https://bunny.net/docs/magic-containers/faqs.md), [public API schema v1.11.1.0](https://api-mc.opsbunny.net/docs/public/swagger.json), [MC node list](https://api.bunny.net/mc/nodes/plain), [CDN edge list](https://api.bunny.net/system/edgeserverlist/plain)
- CDN: [pricing](https://bunny.net/docs/cdn/pricing.md), [Perma-Cache](https://bunny.net/docs/cdn/perma-cache.md), [Origin Shield](https://bunny.net/docs/cdn/performance/origin-shield.md), [connectivity](https://bunny.net/docs/cdn/connectivity.md), [request coalescing](https://bunny.net/docs/cdn/request-coalescing.md), [Load Balancer pricing](https://bunny.net/docs/cdn/load-balancer/pricing.md), [routing](https://bunny.net/docs/cdn/load-balancer/routing.md), [origins](https://bunny.net/docs/cdn/load-balancer/origins.md), [smart cache](https://bunny.net/docs/cdn/smart-cache.md), [logging](https://bunny.net/docs/cdn/logging/index.md), [region API](https://api.bunny.net/region)
- Storage: [pricing](https://bunny.net/docs/storage/pricing.md), [tiers](https://bunny.net/docs/storage/storage-tiers.md), [replication](https://bunny.net/docs/storage/replication.md), [limits](https://bunny.net/docs/storage/limits.md), [storage region API](https://api.bunny.net/storagezone/regions)
- Edge Scripting: [pricing](https://bunny.net/docs/scripting/pricing.md), [regions](https://bunny.net/docs/scripting/regions.md), [limits](https://bunny.net/docs/scripting/limits.md), [before-cache](https://bunny.net/docs/scripting/before-cache.md)
- Other: [Database limits](https://bunny.net/docs/database/limits.md), [DNS pricing](https://bunny.net/pricing/dns/), [Shield](https://bunny.net/shield/), [Optimizer pricing](https://bunny.net/pricing/optimizer/), [negative balance](https://bunny.net/docs/billing/negative-balance.md), [sales tax](https://bunny.net/docs/billing/sales-tax.md), [changelog](https://bunny.net/docs/changelog.md), [status incidents](https://status.bunny.net/api/v2/incidents.json), [Terraform provider](https://registry.terraform.io/v1/providers/BunnyWay/bunnynet)
- Blog: [MC launch, 2025-03-06](https://bunny.net/blog/introducing-magic-containers-what-edge-computing-was-meant-to-be/), [anycast and tunnels, 2025-03-13](https://bunny.net/blog/global-anycast-in-a-click-how-magic-containers-simplifies-low-level-networking/), [persistent volumes, 2026-03-04](https://bunny.net/blog/magic-containers-now-supports-persistent-volumes-storage-that-survives-restarts-and-redeployments/); third party (unverified): [ahoi.dev on volumes](https://ahoi.dev/bunny-net-persistent-volumes/)

**Cloudflare:** [Containers limits](https://developers.cloudflare.com/containers/platform/limits/), [pricing](https://developers.cloudflare.com/containers/platform/pricing/), [placement](https://developers.cloudflare.com/containers/concepts/placement/), [scheduling policy](https://developers.cloudflare.com/containers/configuration/scheduling-policy/), [GA changelog](https://developers.cloudflare.com/changelog/post/2026-04-13-containers-sandbox-ga/), [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/), [R2 pricing](https://developers.cloudflare.com/r2/pricing/), [Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)

**Other providers:** [Vultr plans API](https://api.vultr.com/v2/plans?per_page=500), [Vultr regions API](https://api.vultr.com/v2/regions), [Vultr block storage datasheet](https://discover.vultr.com/block-storage-datasheet), [Vultr block performance](https://docs.vultr.com/products/storage/block-storage/storage-performance-block-storage.md), [Vultr overage](https://docs.vultr.com/support/platform/billing/what-is-the-bandwidth-overage-rate.md), [Oracle price API](https://apexapps.oracle.com/pls/apex/cetools/api/v1/products/?currencyCode=USD), [Oracle regions](https://www.oracle.com/cloud/public-cloud-regions/), [DigitalOcean LB pricing](https://docs.digitalocean.com/products/networking/load-balancers/details/pricing/index.html.md), [DigitalOcean GLB](https://docs.digitalocean.com/products/networking/load-balancers/how-to/create-global-load-balancer/index.html.md), [DigitalOcean availability](https://docs.digitalocean.com/platform/regional-availability/index.html.md), [Linode regions](https://api.linode.com/v4/regions), [Akamai distributed regions](https://techdocs.akamai.com/cloud-computing/docs/distributed-compute-regions.md), [Gcore bandwidth](https://docs.gcore.com/cloud/bandwidth-and-traffic.md), [Koyeb volumes](https://www.koyeb.com/docs/reference/volumes), [Railway volumes](https://docs.railway.com/volumes/reference.md), [Render pricing](https://render.com/pricing), [Northflank pricing](https://northflank.com/pricing), [Fastly pricing](https://www.fastly.com/pricing), [Deno Deploy pricing](https://deno.com/deploy/pricing)

**Fly and gVisor:** [Fly regions](https://fly.io/docs/reference/regions.md), [gVisor resource model (memory accounting)](https://gvisor.dev/docs/architecture_guide/resources/)

**Tools:** [go-pmtiles main.go (extract flags)](https://raw.githubusercontent.com/protomaps/go-pmtiles/main/main.go), [VersaTiles pipeline docs](https://raw.githubusercontent.com/versatiles-org/versatiles-rs/main/versatiles_pipeline/README.md)

**Measurements (Globalping, 2026-11-09):** Bunny hit runs [2oUk4lQ3P24qo2F4x00121TRl](https://api.globalping.io/v1/measurements/2oUk4lQ3P24qo2F4x00121TRl) and [2D7rA1UsvYq5PKykG00121TRg](https://api.globalping.io/v1/measurements/2D7rA1UsvYq5PKykG00121TRg) (London, first of 47); Bunny forced miss [29kICOeoFdwGMFbAE00121TRh](https://api.globalping.io/v1/measurements/29kICOeoFdwGMFbAE00121TRh) (London, first of 71); storage reads [2J8z0ocHqb7Z5yLtn00121TRc](https://api.globalping.io/v1/measurements/2J8z0ocHqb7Z5yLtn00121TRc), [24iZ5aNVJCh7tJiPh00121TRd](https://api.globalping.io/v1/measurements/24iZ5aNVJCh7tJiPh00121TRd), [2RXnwL3peEva3EQFO00121TRd](https://api.globalping.io/v1/measurements/2RXnwL3peEva3EQFO00121TRd); storage API [2ZFmrbyPrua6SYq1y00121TRd](https://api.globalping.io/v1/measurements/2ZFmrbyPrua6SYq1y00121TRd).

## 7. Verification log (2026-11-09)

Every price and limit in sections 1 and 2 was re-checked against the saved copy of its source (Bunny docs and API schema, Cloudflare docs, the Vultr, Oracle, DigitalOcean and Linode APIs and docs, the Fly PoC and R2-vs-Tigris write-ups, and the planet's per-zoom statistics). The unedited draft is kept as the unedited draft (kept with the raw data).

### Corrections made to the draft

| # | Where | Draft said | Corrected to | Evidence |
|---|---|---|---|---|
| 1 | §1, §3.2 | Tiles through Cloudflare Workers: 41-55 ms warm | 34-55 ms (34-35 with [#489](https://github.com/immich-app/geoshenanigans/pull/489)'s Workers Cache) | R2-vs-Tigris write-up, cached class |
| 2 | §1, §3.2 | Plain Cloudflare zone hit "about 10-12 ms" | 12 ms 10-city median firstByte (Bunny 3 ms in the same comparison) | Options report §5.6 |
| 3 | §2, §3.4 | Fly "also has … bom" | Fly lists 17 regions and none in India; India is a gap for Fly and Magic Containers alike | Fly regions page |
| 4 | §2 | Fly direct: 150-450 ms where no region | 120-450 ms (Lagos 122-130) | Others track, regional table |
| 5 | §1, §3.2, §5 | Billed RAM under gVisor "unknown" | Plausible mechanism documented: gVisor charges application memory (a memfd) to the sandbox's cgroup and keeps a Sentry page cache; the MC API has no memory limit | gVisor resource model; MC API schema; MC pricing |
| 6 | §1, §3.2 | Answer did not cover control or egress | Added what Bunny takes away (Worker logic and metrics on hits, HTTP/3, free egress) and two rows in the path table | Bunny scripting, logging and CDN pricing docs |
| 7 | §3.2 | Fly + Cloudflare blended "roughly 65 ms" | 65-70 ms (recomputed from the PoC's warm, pool-mate and never-requested figures) | Fly PoC results |
| 8 | §3.2 | No volume headroom, ingress or app-count notes | Added: ~2 years on an 80 GB z15 volume, ~8 years at the 100 GB cap; ingress unclear with endpoints; 20 apps per account | Planet stats; MC FAQs and limits |
| 9 | §3.2, §4.1 | No single-vendor failure-domain risk | Added the 2026-05-21 registrar suspension (~1.7 h; CDN, storage, scripting, MC and DNS affected) and keeping the Cloudflare path as a fallback | Bunny status incidents |
| 10 | §3.1 | Perma-Cache: "little gain" in front of a fleet | Moves the weighted mean little, but helps South America and Africa, cuts Fly egress and shields MC volumes after deploys | Perma-Cache docs; SSD region list |
| 11 | §3.1 | "8 of 36 repeats that landed on another server missed" (ambiguous) | 8 of 36 repeats landed on another server, and all 8 missed | Storage-edge track data |
| 12 | §3.4 | Linode "$20-27 + volume" | $20-27 already includes the 150 GB volume; 33 regions in 26 cities, all with block storage | Linode types, volume and region APIs |
| 13 | §3.5 | "Correction": Akamai ~26 cities, DigitalOcean 13 | Not a correction: those are city counts and are right (33 regions in 26 cities; 16 datacentres in 13 cities) | Linode region API; DigitalOcean availability page |
| 14 | §4.1 | One fallback (Storage + Edge Script) | Added MC pods without volumes reading Edge SSD through a pull zone, which avoids the page cache | bunny-mc track option B |
| 15 | header | Relative links into a local research folder | Links to sibling files, so the report holds no local paths | — |
| 16 | §1 | 9 long points | Shortened to 9 one-to-three-sentence lines, details moved to §2-3 | — |
| 17 | §2 notes, §3.5 | The two Bunny hit runs were "an hour apart" on "the same probe set" | About 5 minutes apart (19:12-19:14 and 19:17 UTC), same city filters, not necessarily the same probes | Measurement timestamps |
| 18 | §2 | Oracle Africa: Johannesburg | Johannesburg and Casablanca (Kenya listed as coming soon) | Oracle regions page |
| 19 | §4.1 | Trial account "can run stage A" | Only a 3-region stage A (trial limit: 3 regions) | MC limits |
| 20 | §2 notes | Workers charge "measured" | It is the Fly PoC cost model's figure for the ft design, not a measured invoice | Fly PoC cost tables |
| 21 | §2, §3.2 | Bare PR numbers | Linked to the PR | — |
| 22 | §6 | — | Added the Fly regions page and the gVisor resource model as sources | — |

### Trying to refute the top recommendation (Bunny CDN → Magic Containers PoC)

| Line of attack | Finding | Effect |
|---|---|---|
| Data size limits | Confirmed: 2 volumes per app, 100 GB each (docs and the API schema's `maximum: 100`), 10 MB/s each, blank per pod. The zoom split is 68.83 + 70.05 GB (planet statistics). `go-pmtiles extract` has `--minzoom/--maxzoom`; VersaTiles documents `from_stacked` | Holds, with ~2 h fills per update |
| Egress and billing traps | Bunny CDN egress is linear (~$19.5/month now). MC ingress on apps with endpoints is unclear: if billed at egress rates, weekly fills of 5 regions would cost ~$54/month (fill before attaching the endpoint). Detached volumes, old Perma-Cache folders and storage keep billing, also while the prepaid balance is negative. A Shield request allowance (25M-250M) may bill overage at 35.8M. Billed RAM is the largest unknown | Holds as a PoC; stage C must measure RAM and check ingress on the invoice |
| Region gaps | No India (Fly has none either), Oceania only Sydney, Africa only Johannesburg and Lagos, no Korea or Taiwan. Edge Scripting has no Indian PoP. Bunny CDN itself has Indian, Oceanian and African PoPs, so hits stay local | Holds for our traffic mix (AS 11%, OC 3%, AF 0.6%); India's share is unknown |
| Maturity | Volumes are ~8 months old; 8 MC incidents in 2026, one four-week NA 502 issue, one platform-wide domain incident; SLA coverage for MC unstated | The main reason to keep it at PoC stage and keep the Cloudflare path as a fallback |
| Latency model | Survives a 2× error on the edge → pod hop and a 55% edge hit ratio (§3.2) | Holds |

**Result:** the recommendation stands as a PoC, not as an adoption decision. The question most likely to sink Magic Containers is billed RAM under random reads; the routing test (stage A) is the cheapest way to confirm or kill the latency case.
