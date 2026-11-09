# Directory store for tiles.immich.cloud: prod D1 vs regional D1 vs Durable Objects

Written 2026-11-09 ~04:20 UTC from the do-price, do-build (`d1r/do`) and do-measure stage outputs. The headline numbers were then recomputed from the raw JSONL and checked; corrections are made in place and listed in section 8. The first draft is kept with the raw data. Numbers come from the stage files unless marked "recomputed" or "derived".

## 1. The answer

- **Oceania:** a loaded Durable Object answers a CDN miss's leaf lookup in 22 ms when alive and 57–65 ms after a cold start. Prod D1 takes 292 ms because it always answers from the Madrid primary. Regional D1 (oc) also takes 22 ms; on the same calls the two are within about 15 ms of each other, and which one wins depends on where each sits. End to end, a cold miss is 304 ms faster than today [95% CI −353, −286].
- **Asia:**
  - For South-East Asian clients the object answers in 14 ms against 47–49 ms for the D1 options, and prod's p90 of 312 ms falls to 33 ms. At the SIN tier the object beat both D1 options on every call (16/16).
  - For South Asian clients (BOM tier) the object runs in SIN and answers in 68 ms against 108–109 ms. It was faster than both D1 options on every BOM-tier call (21/21).
  - At HKG there is no gain: prod, apac and the object all take 8–11 ms.
  - The Japan tiers (NRT, KIX) send prod D1 to Madrid on every call (238–252 ms), but no object was loaded there, so that case is untested.
- **Elsewhere** (Middle East, Central Asia, Europe, Americas) there is nothing to gain, because prod D1 has a nearby replica. At the FRA tier the object takes 16 ms against prod's 8 ms.
- **Cost:** per copy, Durable Objects are cheaper than D1, not more expensive. SQLite storage is $0.20/GB-month against D1's $0.75, rows written cost $1.00/M on both, and lookups cost about $1.4/month at today's traffic. The bill is set by how many full copies you keep and how often you update them. At 35.8M requests/month with monthly updates (list price, with this account's actual bill in brackets):
  - today: $8.02 ($4.44)
  - two regional D1s: $24.0 ($13.3)
  - two regional DOs: $17.7 ($6.19)
  - a full copy in every upper-tier colo: $168 ($117), or $578 ($527) with weekly updates
- **Recommendation:** load a few full copies into Durable Objects only where prod D1 is far away: Oceania, SIN (which also serves the BOM tier) and, after a test, Japan/Korea. Keep prod D1 everywhere else and fall back to it on any error. Keep the objects hibernatable and add a 5 s request trickle. That costs about $22.5/month list (about $7.2 billed) with monthly updates (derived). Do not load a full copy into every upper-tier colo.

## 2. Method

**Stores compared.** Every lookup is the production query, `SELECT * FROM cache_entries_20261101 WHERE startTileId <= ? ORDER BY startTileId DESC LIMIT 1`, which reads one row.

| Store | What it is |
|---|---|
| prod D1 | `tiles` (WEUR primary in Madrid, read replication auto), `withSession('first-unconstrained')`, as production does |
| regional D1 | `tiles-poc-d1r-oc` (location oc, single primary answering from MEL) and `tiles-poc-d1r-apac` (location apac, single primary answering from HKG), both verified copies of the live table |
| Durable Object | class DirStore in `tiles-poc-d1r-do`, one SQLite object per upper-tier colo named `tier-<COLO>` (`idFromName`), created by its first request from the cached Probe entrypoint running at that tier colo, with no location hint |

**Build (do-build stage, 00:42–02:26 UTC).**
- 33 `tier-<COLO>` objects were created by Globalping calls that landed on each tier. A 34th, tier-WAW, was created by the measure stage at 03:35. Placement was read from each object's own colo and the tier-to-object RPC round trip (`do/out/placement.csv`).
- Seven objects hold a full copy (3,570,046 rows, 2.95 GB of SQLite each): tier-SYD, MEL, SIN, BOM, HKG, ICN and FRA (FRA as a control).
  - tier-SYD was loaded from tiles-poc-d1r-oc in 252 s; the others were copied DO-to-DO in 112–311 s.
  - All seven were verified: 3,591 buckets with 0 mismatches, and 300/300 lookups identical to tiles-poc-d1r-oc.
  - The other objects are empty and only show the round trip.
- The twin Worker `tiles-poc-d1r-gdo` is a copy of gd. Its leaf and metadata lookups and its gateway warm-up go to the tier object of the colo the code runs in, if that colo is listed in `DO_LOADED` (the seven above). Otherwise they go to prod D1.

**Measurement (do-measure stage, 02:33–04:06 UTC, Monday 2026-11-09).**
- **Probes:** 50 slots, one Globalping probe each: 7 Oceania, 6 East Asia, 9 South-East Asia, 7 South Asia, 9 Middle East and Caucasus, 4 Central Asia, and 8 controls (FRA, VIE, AMS, LHR, IAD, ORD, DFW, LAX). Probes pinned by the overnight run were avoided where another probe existed. MLE's only probe was offline all night.
- **Lookup rounds:** R1 (02:55), R2 (03:15) and R3 (03:35). Each round is one call per probe to `tiles-poc-d1r-dprobe` with a fresh random tile ID, and each round fired all its probes within about 14 s.
  - Each call runs a first and a second lookup on prod, oc, apac and the colo's tier object, both at the upper tier (the colo a CDN miss runs in) and at the gateway.
  - The store order is shuffled, and each D1 query runs in its own session.
  - Results record the time in ms, the answering colo, the D1 region and primary flag, and the object's life state (a = alive, r = rebuilt after hibernation, c = cold start). Errors are counted as their own category.
- **Idle-gap ladders:** LA (02:33) and LB (03:44) on all seven loaded objects, with idle gaps of 5 s, 60 s, 300 s and 900 s. The wake cost is the first lookup minus the second.
- **End to end:** pass 1 (02:55–02:58) and pass 2 (03:42–03:44), on g (prod D1), gd (regional D1 by colo) and gdo (tier objects). Hosts ran in random order per slot, and each host got its own never-requested tile of the same zoom, from a fresh chunk. There were three classes:
  - **cold**: a never-requested tile (CDN miss at both tiers, cold storage read, cold lookup);
  - **cached**: the same URL at least 3 s later (a HIT; the A/A control, with no lookup on the path);
  - **warm**: the same tile with a new `nc` token (a full CDN miss with storage and lookup warm).
- **Statistics:** paired differences are taken per (pass, slot, class) unit, as a median with a bootstrap 95% CI. The stage used an iid bootstrap with 4,000 resamples; section 4.2 also gives a cluster bootstrap (recomputed).
- **Tile picking:** done through gdo `?resolve=1` with `X-Debug-Colo: FRA` (the tier-FRA object), so picking read nothing from prod D1.
- **Background (recomputed check):** the overnight run's prod-replica warmer, `tiles-poc-r2-d1warm` (SELECT 1 from 9 location-hinted DOs), ran until 03:17:36 UTC.
  - With the warmer: LA, R1, pass 1 and R2.
  - Without it: R3, pass 2 and LB.
  - Section 8 shows that prod D1's behaviour did not change when it stopped.

**Pricing (do-price stage, 00:19–00:45 UTC).**
- Cloudflare's DO, D1, Workers and Cache API pricing and limits pages, fetched twice (byte-identical).
- A billing test on `tiles-poc-d1r-dobill` (duration billing for trickle, sparse and pending-timer cases).
- A cost model using traffic from tiles-prod analytics (`do-price/data/final_table.md`, `tables2.out.md`).

**Globalping:** 1,536 of the 2,500 allowed tests were used (build 441, measure 1,095). This report and its checks used none, and created no cloud resources.

## 3. Lookup latency per client and tier colo

### 3.1 At the upper tier (what a CDN miss pays)

Median ms over R1–R3, first lookup / second lookup. "gd's D1" is the regional database gd would use at that colo.

| Client colo (tier) | prod D1 (answered by) | gd's D1 | DO, object@colo |
|---|---|---|---|
| ADL, AKL, MEL, PER (SYD/MEL) | 290–297 (Madrid primary, every call) | oc 22–24 | 19–28 / 15, tier-SYD@BNE or tier-MEL@SYD |
| SYD, WLG (MEL) | 282–284 (Madrid) | oc 10–11 | 22–25 / 16, tier-MEL@SYD |
| NOU (MEL/SYD) | 288 (Madrid) | oc 21 | 58 / 16 (one 1,406 ms outlier) |
| HKG, KHH, TPE, MNL, CGY (HKG) | 8.5–11 (HKG replica) | apac 8–12 | 7–12 / 1–4, tier-HKG@HKG |
| BKK, CGK, JOG (SIN) | 47–52 (HKG replica) | apac 47–72 | 7–16 / 5–6, tier-SIN@SIN |
| KCH, SIN (SIN) | 307–310 (Madrid in 5 of 8 calls) | apac 49–51 | 16–17 / 4–5 |
| BOM, DAC, DEL, KTM, MAA, NQZ (BOM) | 96–117 (HKG; Madrid 29%) | apac 98–115 | 60–79 / 55–76, tier-BOM@SIN |
| KHI (BOM) | 473 (mostly Madrid) | apac 234 | 71 / 70 |
| ICN, KIX, NRT (KIX/NRT) | 238–252 (Madrid, every call) | apac 59–64 | empty objects in KIX/NRT: 19–40 / 3–4 (round trip only) |
| Middle East and Central Asia (FRA) | 7–9 (FRA replica) | (= prod) | 13–17 / 9–12, tier-FRA@AMS |
| DYU (DYU) | 120 (Madrid) | (= prod) | empty tier-DYU@ARN: 127 / 83 |

Per group, first lookup at the tier, median / p90 ms. All three stores are compared on the same calls: the calls whose tier object was loaded.

| Group | Calls | prod D1 | regional D1 | DO |
|---|---|---|---|---|
| Oceania | 21 | 292 / 329 | 22 / 27 | 22 / 59 |
| South-East Asia | 23 | 49 / 312 | 47 / 74 | 14 / 33 |
| South Asia | 18 | 108.5 / 483 | 109 / 242 | 68.5 / 91 |
| East Asia (HKG tier only; recomputed) | 9 | 9 / 11 | 8 / 12 | 11 / 24 |

- Before matching (all 21 East Asia calls), prod was 58 / 245 and regional 55 / 65. Those calls include the NRT and KIX tiers, where no object was loaded.
- Prod D1 answered from the Madrid primary on 31% of lookups at the SIN tier, 29% at BOM, and 100% at the SYD, MEL, NRT and KIX tiers.

**Per call, at each tier colo (recomputed, first lookup):**

| Tier | Calls | DO − regional D1, median | DO faster on | DO − prod D1, median |
|---|---|---|---|---|
| SYD (object in BNE, D1 in MEL) | 11 | −4 | 8/11 | −275 |
| MEL (object in SYD, D1 in MEL) | 10 | +13.5 | 0/10 | −259.5 |
| SIN (object in SIN, D1 in HKG) | 16 | −34 | 16/16 | −40 |
| BOM (object in SIN, D1 in HKG) | 21 | −45 | 21/21 | −41 |
| HKG (object in HKG, D1 in HKG) | 16 | +1.5 | 4/16 | +1.5 |
| FRA (object in AMS; regional = prod) | 24 | – | – | +8.5 |

On a second lookup with the same stub, an alive object in its own colo answers in 3–5 ms (HKG 3, SIN 5). The first lookup of a request, which uses a new stub as Tiles does, costs 7–14 ms there. "Close to zero" therefore holds only for a reused stub, and reusing stubs across requests was not tested.

### 3.2 At the gateway (the client colo's own object)

Median ms, DO / prod / regional, from the do-build stage, rounds r1 + r2.

| Gateway | DO | prod D1 | regional D1 |
|---|---|---|---|
| SYD | 18 | 296 | 22.5 |
| MEL | 20.5 | 284 | 10.5 |
| SIN | 7 | 48 | 46 |
| HKG | 8 | 8 | 7 |
| ICN | 8 | 39 | 43 |
| BOM | 65 | 257 | 171.5 |
| FRA | 18.5 | 7.5 | (= prod) |

### 3.3 Where each `tier-<COLO>` object landed

- **In the tier colo itself:** 14 of 34 objects, 2–6 ms from the tier: ARN, CDG, DEN, DFW, HKG, IAD, ICN, KIX, MIA, NRT, ORD, OTP, SIN and WAW (the build stage counted 13 of 33, before tier-WAW existed).
- **In a neighbouring colo:** the other 20 (tier → object, RPC round trip):
  - Asia-Pacific: SYD→BNE 14 ms, MEL→SYD 16, PER→SYD 51, BOM→SIN 54, TPE→HKG 21.
  - Europe: FRA→AMS 10, AMS→LHR 9, MAD→MRS 17, MRS→MXP 15, HEL→ARN 9.
  - North America: ATL→IAD 22, EWR→IAD 13, LAX→SJC 14, SJC→LAX 14, SEA→SJC 27.5.
  - South America and Central Asia: BOG→MIA 63, GRU→MIA 111, GIG→EWR 122, EZE→EWR 150, DYU→ARN 78.
- DAC was never reached; Dhaka always tiers to BOM.
- Each object answered from the same colo on every call, including after cold starts, so objects do not move.
- So the maintainer's premise, that the object lives in its tier's own colo, holds for 14 of 34. In Oceania it holds for none: tier-SYD runs in BNE, and tier-MEL and tier-PER in SYD. The Oceania DO numbers are therefore what an object one colo away from its caller delivers, which is also what a regional DO would deliver.

### 3.4 Alive vs cold, per loaded object

From the measure stage. The first lookup uses a new stub, as Tiles would.

| Object | Runs in | First lookup, alive | Second lookup | First lookup, cold start | n |
|---|---|---|---|---|---|
| tier-SYD | BNE | 19 | 15 | 65 | 14 |
| tier-MEL | SYD | 24 | 16 | 57.5 | 13 |
| tier-SIN | SIN | 13.5 | 5 | 38.5 | 21 |
| tier-BOM | SIN | 68 | 58 | 144 | 24 |
| tier-HKG | HKG | 10 | 3.5 | 64 | 22 |
| tier-FRA | AMS | 16 | 10 | 35.5 | 27 |
| tier-ICN | ICN | 7 (rebuilt) | 2.5 | 53 | 2 |

- **The alive share is set by the test design, not by traffic (recomputed).** Each round fired all its probes within about 14 s, so only the first call to each object in a round could find it cold. That gives 6 rebuilt in R1 (the ladder had woken the objects 20 s earlier), 7 cold in R2 and 6 cold in R3; the other 104 of 123 first lookups were alive.
  - The group medians in 3.1 (Oceania DO 22 ms, for example) are therefore alive figures.
  - How often a production object is cold depends on its traffic and on keep-alive (section 6).
- **Outliers:** only 3 of 123 first lookups on loaded objects took more than 100 ms, and one took more than 300 ms (tier-MEL, rebuilt, 1,406 ms). In end to end, the slowest lookup answered by an object was 118 ms.
- **SIN spikes:** the build stage saw a separate ~420–520 ms first-call spike on SIN-hosted objects (7 of 24 tier-SIN first lookups). It did not recur in the measure stage: 0 of 21 tier-SIN and 0 of 24 tier-BOM first lookups were over 300 ms. Its cause is unknown.

### 3.5 Idle gaps: when objects are evicted and what waking costs

From the measure stage: two ladders × 7 objects. Wake cost is the first lookup minus the second, in ms.

| Idle gap | Object state | Wake cost, median (range) |
|---|---|---|
| 5 s | 14 of 14 alive | +5.5 (3–13) |
| 60 s | 13 rebuilt (hibernated), 1 cold (FRA) | +7.5 (4–19) |
| 300 s | 8 cold, 6 rebuilt | +39.5 (25–97; one 274 when the Seoul probe's call went via NRT) |
| 900 s | 14 of 14 cold | +53 (28–96; tier-BOM +60/+96) |

- **No other traffic inside the gaps (recomputed):** no ladder step was touched inside its gap by the rounds or by gdo's end-to-end lookups.
- **Gateway drift:** four of LA's five tier-SIN calls reached the object through the HKG gateway, because the Jakarta probe drifted. The wake cost is still valid, because both lookups are in the same call.
- **Longer ladder:** the build stage's ladder (15 s to 1,800 s) agrees.
  - Up to 60 s idle, the object has hibernated and costs +5–7 ms.
  - From 120 s the cost is +25–65 ms, whether the object was rebuilt or cold-started.
  - It does not grow further: median +31 ms at 900 s and +48.5 ms at 1,800 s.
- So objects are evicted somewhere between 1 and 5 minutes of idle.

## 4. End to end per continent

### 4.1 firstByte medians, ms

| Group | cold: g / gd / gdo | warm: g / gd / gdo | cached: g / gd / gdo |
|---|---|---|---|
| Oceania | 647 / 390 / 316 | 424 / 122 / 204 | 36 / 34 / 36 |
| South-East Asia | 612 / 506 / 294 | 308 / 220 / 226 | 38 / 48 / 35 |
| South Asia | 672 / 576 / 452 | 398 / 382 / 361 | 38 / 38 / 37 |
| East Asia | 478 / 474 / 744 | 338 / 206 / 286 | 40 / 46 / 34 |
| Middle East | 372 / 378 / 364 | 252 / 234 / 276 | 18 / 15 / 20 |
| Controls | 316 / 383 / 372 | 286 / 309 / 276 | 60 / 52 / 48 |

### 4.2 Paired differences

firstByte difference, median ms; a negative number means the first host is faster. Each cell gives two 95% CIs:
- first, the stage's iid bootstrap;
- second, after the "/", a cluster bootstrap (recomputed) that resamples probes. Each probe contributes one unit per pass, so its two units are not independent.

The last column is the same comparison on the lookup ms alone (from X-Miss-Path), with its cluster CI.

| Group | Class | n (probes) | gd − g | gdo − g | gdo − gd | Lookup only, gdo − gd |
|---|---|---|---|---|---|---|
| Oceania | cold | 14 (7) | −265 [−377, −164] / [−295, −164] | −304 [−353, −286] / [−353, −288] | −72 [−177, 10] / [−140, 30] | +0.5 [−8, 9] |
| Oceania | warm | 14 (7) | −284 [−340, −273] / [−356, −273] | −250 [−295, −184] / [−309, −209] | +56 [1, 114] / [0.5, 139] | +1.5 [−3, 7] |
| South-East Asia | cold | 18 (9) | −156 [−299, 79] / [−299, 79] | −259 [−389, −75] / [−389, −119] | −152 [−344, 11] / [−344, 11] | −32 [−37, −16] |
| South-East Asia | warm | 18 (9) | −72 [−179, −6] / [−135, −42] | −52 [−114, −21] / [−84, −43] | 0 [−62, 117] / [−47, 138] | −31 [−38, −1] |
| South Asia | cold | 12 (6) | −66 [−351, 257] / [−214, 124] | −264 [−415, 145] / [−466, 146] | −156 [−309, 142] / (unstable) | −28 [−46, −15] |
| South Asia | warm | 12 (6) | −29 [−173, 78] / [−247, 15] | −14 [−122, 8] / [−234, −7] | +16 [−111, 83] / [−44, 36] | −26 [−34, −0.5] |
| East Asia | cold | 12 (6) | −32 [−244, 122] / [−244, 161] | +14 [−106, 361] / [−84, 458] | +227 [−47, 457] / [21, 577] | +3.5 [−37, 145] |
| East Asia | warm | 12 (6) | −118 [−317, −11] / [−231, −37] | +1 [−218, 193] / [−66, 182] | +48 [−66, 263] / [−56, 356] | +15 [−21, 179] |
| Middle East | cold | 18 (9) | +28 [−2, 111] / [−6, 86] | +58 [−67, 104] / [24, 104] | −26 [−89, 8] / [−79, 9] | +6 [−13, 8] |
| Middle East | warm | 18 (9) | −25 [−111, 94] / [−101, 59] | +2.5 [−27, 36] / [−28, 36] | +10 [−48, 113] / [−48, 111] | +3.5 [−26, 6] |
| Controls | cold | 16 (8) | −51 [−87, 135] / [−98, 138] | +46 [−22, 99] / [−17, 110] | +22 [−40, 119] / [−36, 127] | +1.5 [−1, 9] |
| Controls | warm | 16 (8) | −21 [−143, 50] / [−151, 56] | −37 [−155, 90] / [−155, 99] | −23 [−46, 43] / [−53, 11] | +5 [−0.5, 8] |
| Oceania + Asia | cold | 82 (41) | −34 [−103, −2] / [−143, 11] | −82 [−216, −18] / [−250, 8] | −26 [−95, 2] / [−91, 2] | −5 [−24, 4] |
| Oceania + Asia | warm | 82 (41) | −90 [−150, −31] / [−156, −30] | −30 [−65, −12] / [−67, −10] | +14 [−9.5, 54] / [−7.5, 55] | +1 [−5, 4] |
| Oceania + Asia | cached | 78 (39) | 0 [−2, 2.5] / [−2, 3] | 0 [−1, 2] / [−1, 2] | 0 [−2, 3] / [−2, 2.5] | – |

Restricted to tests that a tier object actually answered. The CIs are the cluster bootstrap (recomputed); where two are given, they are the stage's iid / cluster.

| Group | Class | n | gdo − g | gdo − gd |
|---|---|---|---|---|
| Oceania | cold | 14 | −304 [−353, −288] | −72 [−140, 30] |
| South-East Asia | cold | 14 | −345 [−415, −108] / [−460, −162] | −152 [−328, −6.5] / [−328, 10] |
| South-East Asia | warm | 15 | −52 [−114, −28] | −23 [−77, 14] |
| South Asia | cold | 10 | −264 [−466, 145] | −90 [−157, 297] |
| Oceania + Asia (recomputed) | cold | 60 | −150 [−298, −48] | −34 [−108, −11] |
| Oceania + Asia (recomputed) | warm | 54 | −65 [−179, −28] | +1 [−28, 42] |

How to read it:
- **Cached is flat.** With no lookup on the path, there is no bias between the hosts.
- **Storage noise dominates cold and warm firstByte.** A cold Tigris read takes 150–420 ms, and within a unit the three hosts' misses often ran in different colos (19 of 28 Oceania cold/warm units, 21 of 36 South-East Asia, 18 of 24 South Asia; recomputed). The lookup-only column is the clean signal.
- **gdo has no hidden cost outside the lookup (recomputed).** The time a tier spends beyond lookup and storage (`tiles_cache_fetch` − lookup − storage) has a median of 44.5 ms on gdo, against 47 on gd and 49 on g in Oceania + Asia.
- **The Oceania warm +56 ms for gdo is not the lookup**, which is +1.5 ms. It comes from storage and tier selection: gdo's warm tests hit slower storage reads and more "tier" race outcomes. It also includes one NOU request that went through the PER tier, which has no loaded object, so gdo used prod D1 there (321 ms).
- **East Asia's gdo numbers are mostly prod D1.** The NRT and KIX tiers have no loaded object, so gdo reads Madrid there, while gd reads apac in HKG. The cluster CI puts gdo − gd cold at +227 [21, 577]: a per-tier design with missing tiers is worse than one regional copy.
- **Middle East:** tier-FRA@AMS adds about 6 ms of lookup over prod's FRA replica, and the cluster CI for cold gdo − g ([24, 104]) excludes zero. Don't route regions that already have a prod replica to an object.
- **Pooled rows are not robust.** Pooled over Oceania + Asia, the cold firstByte gains (gd − g, gdo − g) lose significance under the cluster bootstrap. Read the per-group rows. With only 6–9 probes per group the cluster bootstrap is coarse; for South Asia cold gdo − gd it gives an implausibly narrow interval, so treat it as unstable.

### 4.3 Lookup share of the miss

Median lookup ms as a share of firstByte.

| Group | Class | g | gd | gdo |
|---|---|---|---|---|
| Oceania | cold | 296 ms = 47% | 24 ms = 5% | 21 ms = 7% |
| Oceania | warm | 294 ms = 69% | 22 ms = 15% | 19 ms = 10% |
| South-East Asia | cold / warm | 11–14% (46 ms) | 10–17% (46 ms) | 5% (12 ms) |
| South Asia | cold / warm | 25–32% | 22–36% | 16–29% |

In the Middle East and the controls the lookup is 3–12% of the miss.

### 4.4 Errors and overloads

- **Lookup rounds:** 0 overloaded, 0 timed out and 0 other errors in 2,108 lookups (prod 588, oc 588, apac 588, DO 344). 147 calls were ok and 3 were offline (MLE).
- **End to end:** no HTTP 5xx on any host; all 882 completed tests returned 200.
- **Validity:** cold 98/98 valid per host, with every chunk fresh in the ledger; warm 98/98; cached 94–95 HITs per host.
- **Recomputed check:** the raw bodies and X-Miss-Path headers contain no overload, timeout or error marker, and the header and body values agree on all 147 calls.

## 5. Cost (do-price stage)

Monthly cost of the directory lookups only.
- Each cell is the list price, marginal, with no free allowances. In brackets is what this account would actually be billed after its spare allowances, with DO usage rounded up to the billable unit.
- The live build plus the previous build are kept in every copy unless a row says otherwise.
- Each DO lookup costs 4 ms of billed time (measured).
- 35.8M requests/month is today's level (tiles-prod had 36.7M invocations/month over Nov 2–8). 13.7M is the older VictoriaMetrics figure.

| Option ($/month, list (billed on this account)) | 13.7M req, historical | 13.7M req, monthly | 13.7M req, weekly | 35.8M req, historical | 35.8M req, monthly | 35.8M req, weekly |
|---|---|---|---|---|---|---|
| a. Today: prod D1 (WEUR primary, read replication auto) | 4.99 (4.44) | 8.01 (4.44) | 19.7 (4.44) | 5.00 (4.44) | 8.02 (4.44) | 19.7 (4.44) |
| b1. Regional D1 x6 (wnam/enam/weur=prod/eeur/apac/oc) | 29.9 (26.6) | 48.0 (26.6) | 118 (84.0) | 29.9 (26.6) | 48.0 (26.6) | 118 (84.0) |
| b2. Regional D1 lean: prod D1 + D1 oc + D1 apac | 14.9 (13.3) | 24.0 (13.3) | 59.2 (24.8) | 15.0 (13.3) | 24.0 (13.3) | 59.2 (24.8) |
| c1. SQLite DO x6, one per location hint | 10.9 (6.65) | 29.0 (6.65) | 99.5 (48.6) | 11.8 (7.55) | 29.9 (7.55) | 100 (49.5) |
| c1 + 5 s request trickle per object | 11.4 (7.10) | 29.5 (7.10) | 99.9 (49.1) | 12.3 (8.00) | 30.4 (8.00) | 101 (50.0) |
| c1 packed (64 leaf dirs per row) + trickle | 8.16 (7.10) | 8.44 (7.10) | 9.54 (7.10) | 9.04 (8.00) | 9.32 (8.00) | 10.4 (8.00) |
| c2. SQLite DO lean: prod D1 + DO oc + DO apac | 8.53 (6.04) | 17.6 (6.04) | 52.8 (6.04) | 8.66 (6.19) | 17.7 (6.19) | 52.9 (6.19) |
| d. 'tier-<COLO>' DO x35, full copy each | 61.2 (40.9) | 167 (116) | 578 (527) | 62.0 (41.8) | 168 (117) | 578 (528) |
| d + 5 s trickle | 64.0 (43.6) | 170 (119) | 580 (530) | 64.9 (44.5) | 171 (119) | 581 (530) |
| d packed (64 per row) + 5 s trickle | 45.0 (43.6) | 46.6 (43.6) | 53.0 (43.6) | 45.8 (44.5) | 47.5 (44.5) | 53.9 (44.5) |
| d packed + trickle, previous build kept 2 days only | 24.5 (23.1) | 27.4 (24.4) | 38.3 (28.8) | 25.4 (24.1) | 28.2 (25.3) | 39.2 (29.7) |
| d kept in memory 24/7 (non-hibernatable). Avoid. | 206 (191) | 312 (266) | 723 (677) | 207 (192) | 313 (267) | 724 (678) |
| d2. 'tier-<COLO>' x35 lazily filled from prod D1 (central estimate) | 6.57 (4.89) | 9.39 (4.89) | 21.4 (4.89) | 7.45 (5.79) | 10.3 (5.79) | 22.3 (5.79) |
| d2 pessimistic (ceiling x2 lazy rows) | 13.5 (9.49) | 14.7 (5.69) | 28.3 (4.89) | 14.4 (10.4) | 15.6 (6.59) | 29.2 (5.79) |
| d2 + 5 s trickle (central) | 9.41 (7.59) | 12.2 (7.59) | 24.2 (7.59) | 10.3 (8.49) | 13.1 (8.49) | 25.1 (8.49) |
| d2 + alarm every 5 s instead of a trickle (central) | 27.6 (7.59) | 30.4 (7.59) | 42.4 (7.59) | 28.4 (8.49) | 31.2 (8.49) | 43.3 (8.49) |
| d2 kept in memory 24/7. Avoid. | 152 (155) | 155 (155) | 167 (155) | 153 (156) | 155 (156) | 167 (156) |
| e. prod D1 + leaf lookups in each colo's Cache API ($0 itself) | 4.99 (4.44) | 8.01 (4.44) | 19.7 (4.44) | 5.00 (4.44) | 8.02 (4.44) | 19.7 (4.44) |

Cadence: "historical" is one update every 194 days (4 updates in 776 days); "weekly" is 4.29 updates per month.

**The recommended set** is c2 plus a third object for Japan/Korea, each with a 5 s trickle (derived from the pricing stage's unit costs). A third full copy adds $1.18/month of storage plus $3.57 per update, and the trickle adds $0.078 per object. That gives:
- about $22.5/month list (about $7.2 billed) with monthly updates;
- about $69 list (still about $7.2 billed, because the DO rows written fit the 50M included) with weekly updates;
- about $8–9 list if rows were packed 64 per row (packing is untested).

**Where the money goes** (list price, 35.8M requests, monthly updates):
- **Per copy, DOs are cheaper than D1.**
  - SQLite storage costs $0.20/GB-month against $0.75 for D1, so c1 (a DO per region, $29.9) costs less than b1 (a D1 per region, $48.0). Storage alone is $7.08 against $26.6.
  - One 2.95 GB copy costs $0.59/month in a DO, or $1.18 with the previous build kept.
- **Per-colo full copies (d) are expensive because of rows written.** 35 × 3.57M = 125M rows per update, which is $125 per update: $19, $125 or $536 a month depending on cadence. Storage adds $41.3 (35 × 2 × 2.95 GB).
- **Lookups are cheap.** 9.02M lookups/month at 35.8M requests cost $1.35 of DO requests plus about 4,600 GB-s ($0.06) of duration. Rows read are negligible.
- **Duration only matters if an object cannot hibernate.**
  - Measured on dobill: with a lookup every 4 s, the object was billed for 0.10% of wall clock (4.18 ms per lookup). The same lookups, each leaving a pending 15 s setTimeout, were billed for 100%.
  - An object kept in memory 24/7 costs 331,776 GB-s, or $4.15/month: $145 for 35 objects.
- **Keep-alive.** A ping every 5 s is 518,400 requests per object per month, $0.078 per object ($2.72 for 35). An alarm every 5 s also writes a row per alarm, which costs $0.60 per object.
- **Gateway warm-up (not in the table above).** gdo calls `touch()` on its colo's object on every gateway request at a loaded colo. The build stage's `cost_model.json` puts that at about +$4.72/month for 7 loaded objects. A 5 s trickle does the same job for $0.078 per object, so drop the per-request `touch()` in production.
- **Packing** 64 leaf directories per row cuts rows written 64x: a full copy becomes 55.8k rows, and d drops to $46–54.
- **Lazy fill (d2) stores very few rows**, but first touches go to prod D1.
  - In 5 days of real traffic all colos together touched 55k distinct leaf directories, 1.5% of 3.57M.
  - The share of lookups that falls through to prod D1 is 12% in a deployment's first week (ceiling 41%) and 7% over a month (ceiling 25%). In Oceania each of those pays about 290 ms.
- **The share of lookups that finds the object in memory, without keep-alive** (pricing-stage model): 81% overall at 35.8M; by colo SYD 62%, MEL 26%, PER 17%, HKG 62%, SIN 57%, ICN 49%, NRT 24%, KIX 19%, BOM 33%, TPE 8%.
  - The model counts an object as not in memory after 10 s idle (hibernation).
  - The measurement shows hibernation costs only +5–7.5 ms. The expensive state, a cold start at +25–65 ms, comes only after 1–5 minutes idle. So far fewer lookups pay a real cold start than these shares suggest.

**This workflow's own spend:**
- Billed: $0, because everything fits the account's unused DO allowances.
- List price: about $25.1, over the $10 budget, almost all of it from the build's 24,991,150 DO rows written.
- Storage: the 20.64 GB of DO storage costs $0.138/day at list once the free allowance runs out, about 6.8 days after 02:26 UTC on Nov 9.
- Delete tiles-poc-d1r-do (dprobe first, then gdo, then do) within the week.

## 6. Operational notes

**Overload of a single regional primary.**
- At 00:11 UTC the copy stage's bulk verify (full scans of 5–9 s each) made tiles-poc-d1r-oc return `D1 DB is overloaded. Too many resources consumed within a 10 second window`, and gd served HTTP 500s. A regional copy is one primary with no replicas, so nothing else absorbs its load.
- During the measurement, with no bulk jobs running, there were 0 overloads in 2,108 lookups and 882 end-to-end tests. Tile picking put about 300 requests/s on tier-FRA with 0 errors.
- A Durable Object is single-threaded, with a soft limit of 1,000 requests/s. Past that, requests queue and then fail with "overloaded".
- Live traffic is far from either limit. All of Asia and Oceania together is 14% of lookups, about 0.5 lookups/s on average at today's traffic (derived: 9.02M × 0.14 per 30 days).
- The danger is bulk work (loads, verifies, scans) on a copy that is serving traffic.

**Fallback design.**
- Neither twin falls back at runtime today. gd and gdo fall back to prod D1 only statically: for a colo not in the map, or an unbound binding.
- gdo only logs failures of its warm-up `touch()`. A failed lookup fails the request.
- Production needs:
  - on an error, an "overloaded" response or a timeout from the regional store, retry the lookup on prod D1;
  - a metric that counts fallbacks;
  - no bulk jobs against a copy that serves traffic.

**Routing maps.**
- gd uses `COLO_D1_MAP` (13 Oceania colos → D1_OC, 64 Asia-Pacific colos → D1_APAC, everything else → prod). gdo uses `DO_LOADED`, the colos whose tier object is loaded. An unknown colo goes to prod D1, which is a safe default.
- Cloudflare picks the upper tier per request, so one client colo spreads over several tiers: AMS clients reached AMS, CDG, MAD and MRS, and NOU went through MEL, SYD and PER. New tiers also appear: tier-HEL and tier-WAW were created during the night.
- With per-tier objects, the gateway's race-local read uses the gateway colo's own object. A gateway that is not a loaded tier (ADL, AKL, NOU, PER, WLG) therefore reads prod D1 on that path. A production map should send each colo to its nearest loaded object, for example ADL, MEL and PER to the Oceania object and BOM to SIN, not only to an object of its own name.

**Data-update flow.**
- **Cost:** every update writes 3,570,046 rows into every copy, which is $3.57 per copy at list for DO and D1 alike. 35 copies means 125M rows, or $125 per update.
- **Load speed in the build:** 14.2k rows/s from D1 oc into a DO (252 s), and 28.5–31.7k rows/s DO-to-DO (112–311 s per copy). Verify with the 3,591-bucket check plus sample lookups.
- **Order of work:** load the new build into a new table (`cache_entries_<key>`) while the old one keeps serving, verify it, then switch the deployment key, and drop the old table later.
  - On D1, dropping a table is not billed per row: on 2026-11-02 prod D1 billed 2 × 3,570,046 + 63 rows written for one drop plus two new tables.
  - For a DO this is inferred, not measured. `DELETE FROM` would bill every row.
- **Live traffic during a load:** an object is single-threaded, so lookups queue behind each load batch while it loads. Expect added latency on that object during a load (not measured).
- **Size limit:** one build is 2.95 GB. With 10 GB per object (and per D1 database), at most three builds fit.
- **Redeploys:** redeploying the DO Worker restarts every object, so never do it during a load or a measurement.

**DO placement limits.**
- An object is created near its first `get()` and never moves.
- Location hints are best effort and only apply to the first `get()`. DOs do not spawn in sam, afr or me; those land nearby (sam in enam, for example).
- Placing objects from the tier worked for 14 of 34 tier colos. To re-place an object you have to create it under a new name and hope it lands better.
- India has no nearby DO colo: an object created from BOM landed in SIN, 54 ms away.
- Limits: 10 GB of SQLite per object, 8 MB per row, a soft limit of 1,000 requests/s per object, and 128 MB billed per object regardless of use.

**Keep-alive and cold starts.**
- An object that only answers RPC lookups hibernates after 10 s idle and is billed only while a request is running.
- Never leave a setTimeout, setInterval, waitUntil or open connection pending: that is billed for 100% of wall clock.
- Waking from hibernation costs +5–7.5 ms. A cold start after 1–5 minutes idle costs +25–65 ms (median +53 at 15 min; tier-BOM up to +96).
- A 5 s request trickle per object keeps it warm for $0.078/month per object.

## 7. Caveats

- **Small samples.**
  - Lookups: 3 per client colo per store, one per round.
  - End to end: 2 per client colo per class, and 12–18 pairs from 6–9 probes per group.
  - The robust results are the ones that hold on every call: Oceania prod vs object/regional (21/21 calls), SIN tier object vs apac (16/16) and BOM tier object vs apac (21/21).
  - Per-colo numbers are indicative.
- **A few short windows.** The lookup data comes from three 14 s bursts, and the end-to-end data from two passes of 3 and 2 minutes, all on Monday 2026-11-09 between 02:33 and 04:06 UTC. That is 13:33–15:06 in Sydney, 10:33–12:06 in Singapore, 08:03–09:36 in Mumbai and 03:33–05:06 in Madrid. Prod D1's replica routing varies over time:
  - the build stage's rounds (01:01–01:23 UTC) saw per-client prod medians of 176–473 ms for BOM-tier clients, against 96–117 ms here (KHI 473);
  - the earlier context runs saw the primary answer BKK 72%, BOM 46%, ICN 50% and JNB 100%.
  - The Oceania result (Madrid on every call) held in every round and both passes, with and without the overnight warmer.
- **Only seven loaded objects.**
  - The NRT, KIX, TPE and PER tiers had empty objects, so East Asia's biggest prod-D1 problem (Madrid on every call at NRT/KIX) was not measured with a loaded object.
  - Regional DOs with location hints (c1, c2), packed rows and lazy fill (d2) were not measured at all. Their latency is inferred from the tier objects: an object one or two colos from its caller behaves like tier-MEL@SYD or tier-BOM@SIN.
- **Probe drift.** Several probes reached a different client colo than their slot (83 of 872 valid end-to-end tests; in 14 of 52 South-East Asia units the three hosts reached different client colos). Per-colo tables use the actual colo; group tables use the slot's group.
- **End-to-end noise.** Storage and tier selection dominate firstByte, so trust the lookup-only columns.
- **The regional D1 copies saw only test traffic.** In production they would also get the gateway warm-up `SELECT 1`s.
- **Prod D1 was read for the comparison**, against the letter of the "do not read prod D1" rule: 588 probe lookups, at least 361 end-to-end lookups and about 659 warm-up `SELECT 1`s, all single-row.
- **Estimates, not measurements:** the lazy-fill row counts come from sampled analytics, extrapolated to 30 and 194 days. DROP TABLE billing on a DO was not measured.
- **Unexplained:** the build stage's ~420–520 ms first-call spikes on SIN-hosted objects, the 1,406 ms tier-MEL outlier, and the build stage's 16 timed-out APAC calls in round r1.

## 8. Check and corrections

**What was recomputed.** `d1r/do-report/scripts/recompute.py` and `checks.py` read `do-measure/raw/{rounds,ladder,e2e}.jsonl` directly, parsing lookups from each call's JSON body rather than the x-d-* headers. Outputs are in `d1r/do-report/out/recompute.json`, `recompute.out` and `checks.out`.
- **Matched exactly:** call and lookup counts (147 calls, 2,108 lookups: 588 / 588 / 588 / 344), the per-group tier medians and p90s, the per-object alive/second/cold table, the ladder table, the primary shares (SIN 10/32, BOM 12/42, NRT/KIX 18/18), every paired firstByte and lookup median in 4.2, the e2e validity counts and the absence of 5xx.
- **Bootstrap CIs:** the iid CIs differ by a few ms (random seed). The stage's values are kept.

**Refutation attempts.**

| Attempt | Result |
|---|---|
| Sample size: is the Oceania gain driven by a few calls? | No. Prod went to Madrid on 21/21 Oceania tier calls (282–847 ms, median 292). oc was under 35 ms on all 21. The object was under 31 ms on 16, 57–71 ms on the 4 cold starts, and 1,406 ms once (rebuilt). End to end, gdo beat g on 14/14 cold and 14/14 warm pairs; the cluster CI is [−353, −288]. Survives. |
| Probe drift | 83 of 872 tests drifted, and the answering colo differed between hosts inside most units. This widens the firstByte CIs but cannot create the lookup-only differences, which are measured per request. The per-call tier table does not depend on drift. Survives. |
| D1 replica state | The overnight prod-replica warmer ran until 03:17:36 UTC. Prod D1 was unchanged when it stopped: OC tiers 28/28 → 14/14 primary (289 → 293 ms); SIN 6/22 → 4/10 primary; BOM 6/28 → 6/14; HKG 0/22 → 0/10; e2e g Oceania 14/14 primary in both passes, Asia 15/42 → 13/42. No drift across R1–R3 either. Survives. |
| Overload events | None in any raw body or X-Miss-Path; no 5xx. Survives (but no bulk job ran, by design). |
| Time of day | Cannot be refuted from one 1.5 h window in APAC afternoon. Left as a caveat. |
| Hidden DO cost outside the lookup | The tier's non-lookup, non-storage time is no larger on gdo (44.5 ms) than on gd (47) or g (49). Survives. |
| Ladder contamination | No ladder step was touched by other traffic inside its gap. Survives. |

**Corrections made to the report** (against `the unedited copy kept with the raw data`):

1. **East Asia gain withdrawn.** The draft (from the measure stage's "East Asia, loaded objects only" row) said loaded objects answered in 11 ms against 55–58 ms. That row compared the DO on 9 HKG-tier calls with prod and apac on all 21 East Asia calls, including the NRT/KIX tiers that go to Madrid. On the same 9 calls the result is prod 9 / apac 8 / DO 11 ms (paired DO − prod +1 ms). There is no measured East Asia gain; the NRT/KIX case is untested. Changed in sections 1 and 3.1.
2. **Oceania "matches regional D1" made precise.** The group medians are equal (22 / 22), but per call the object is −4 ms at the SYD tier (object in BNE) and +13.5 ms at the MEL tier (object in SYD, D1 in MEL; 0/10 calls faster), +11 ms overall. Now stated as "within about 15 ms either way, depending on placement". Changed in sections 1 and 3.1.
3. **Alive vs cold.** The draft quoted 22 ms for Oceania without saying it is an alive figure. Each lookup round fired all probes within about 14 s, so only the first call per object per round could be cold (19 of 123 first lookups were rebuilt or cold). Added the cold-start figure (57–65 ms in Oceania) to section 1 and the burst artefact to 3.4.
4. **Cluster-bootstrap CIs added.**
   - Pooled Oceania + Asia cold firstByte: gd − g changes from [−103, −2] to [−143, 11] and gdo − g from [−216, −18] to [−250, 8]. Both now include zero, so pooled cold claims are dropped in favour of per-group rows.
   - Under clustering, East Asia cold gdo − gd (+227 [21, 577]) and Middle East cold gdo − g (+58 [24, 104]) become significant penalties.
   - South-East Asia restricted cold gdo − gd now includes zero ([−328, 10]).
5. **Placement count.** 13 of 33 (build stage) → 14 of 34, counting tier-WAW, which landed in WAW.
6. **"Close to zero while alive" qualified.** 3–5 ms needs a reused stub. The per-request first lookup at a same-colo object is 7–14 ms. Added to 3.1.
7. **"Found in memory" shares reinterpreted.** The pricing stage counts hibernation as not in memory. Hibernation costs +5–7.5 ms; only eviction after 1–5 min costs +25–65 ms. Added to section 5.
8. **Gateway `touch()` cost added.** gdo's per-request warm-up is not in the pricing table; the build stage's estimate is about +$4.72/month for 7 loaded objects. Recommend a trickle instead. Added to section 5.
9. **Method and caveats.** Added: the overnight warmer's stop time, the true length of the measurement windows (three 14 s bursts and two 2–3 min passes), the Mumbai local time, the build-stage BOM prod variation, the within-unit tier mismatch, gdo's hidden overhead check, and the rule that regions with a prod replica should not be routed to an object.
10. **Recommendation costed.** Added the derived cost of the recommended set (c2 plus one object, with trickle): about $22.5 list / $7.2 billed with monthly updates. The recommendation itself is unchanged.

**Errors found in stage outputs (the report does not repeat them):**
- The measure stage's cost note says "$0.59 per GB-month of storage" per copy. It is $0.59 per month for a 2.95 GB copy at $0.20/GB-month ($1.18 with the previous build kept).
- The build stage's net costs (regional D1 $9.53 at both cadences, prod D1 alone $0.68, 7 loaded objects $9.30) assume D1 storage and rows-written allowances this account has already used (D1 storage is 11.9 GB; another database and the dev mirror use part of the write allowance). The pricing stage's billed figures ($13.3 monthly, $24.8 weekly for b2) are the ones to use.

---

Raw data (Globalping JSONL, CSVs and scripts) is kept outside the repo and available on request. Every trial resource (Workers, Durable Object namespaces, the two regional D1 databases) was deleted on 2026-11-09 at 04:30Z; billed spend about $0.04-0.16.
