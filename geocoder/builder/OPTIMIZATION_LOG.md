# Build Optimization Log

## October 2026: 1h 20m → about 5 min, 188 → 118 GB peak (80 GB fits)

Build: `--continents --multi-output --multi-quality` with TIGER, wikidata
sitelinks and external postcodes, chained on the previous day
(`--prev-output`, so strategy-2 runs). Machine: 64-core / 251 GB box, run on
62 cores. Every change was gated on byte-identical output: oceania at 64 and
8 reported CPUs, then a full planet day compared file by file with the
reference build, plus all 126 patches compared byte for byte.

| Phase | Before | Round 2 | Round 3 | What changed |
|---|---|---|---|---|
| Pass 1 | 129 s @1 core | 5 s | 5 s | parallel blob-header scan |
| Pass 2 (nodes) | 173 s | 71 s | 50 s | sparse node index, parallel merge and interning; blobs inflated outside the read limit |
| Pass 2b (ways) | 222 s | 35 s | 31 s | streams only way blobs, parallel merge, packed way geometries |
| Admin assembly | 465 s | 12 s | 7 s | ranked candidates, bbox reject, parallel TIGER/GeoNames loading |
| S2 cell computation | 925 s | 91 s | 42 s | per-cell linking, parallel interpolation endpoints; S2 and abseil without debug checks |
| Deterministic ordering | 699 s @1 core | 51 s | 31 s | parallel sorts with a serial fallback when ties reach output; steps side by side |
| All index writing | 2143 s @2 cores | 114 s | 112 s | parallel postcode centroids and strategy-2, memory-budgeted stages |
| **Total** | **1:19:42** | **7:06** | **5:03** | |

Round 3's runs took 4:47 to 5:25, depending mostly on how much of the
planet PBF the page cache still held from the run before (Pass 2).

Round 3's S2 gain is mostly the libraries: the distro S2 and abseil are built
without NDEBUG, so every S2 call ran its validity checks and every
absl::Mutex its deadlock detection under one global lock. The nightly links
static copies of the same package sources built with `-DNDEBUG`
(`tools/build_s2.sh`); with the distro libraries, deadlock detection is
switched off at startup.

The PBF's blobs inflate through libdeflate: 2.46x faster than zlib over the
planet's 1.52M blobs (99 GB in, 235 GB out), with the same bytes. That takes
about 15 s off Pass 1, 2 and 2b, where inflate was 14.5% of the build's CPU.
pbf-apply also deflates the blobs it re-encodes with libdeflate, 2.2x faster
and 0.6% smaller. Every block still decodes to what zlib's run gave.

Memory: the peak was 188 GiB in Pass 2b (a dense 111 GiB node index plus
merge copies). Only nodes that a kept way references get coordinates now,
thread data is freed as it merges, and the write phase schedules its regions
and stages within the memory limit (min of MemTotal and the cgroup limit,
capped by `GC_MEMORY_LIMIT_MIB`). Unconstrained it peaks at 118 GB, running
more at once where memory allows. With identical output it finishes in 5:31
at 114 GB inside a 128 GiB cgroup, in 5:48 at 93 GB inside 96 GiB and in
6:31 at 76 GB inside 80 GiB.

Pipeline around the builder (same box):
- Applying the daily diff: `osmium apply-changes` 52 min → `pbf-apply` about
  2 min (copies untouched blobs, re-encodes the rest in parallel; object
  content identical to osmium's). `--direct-io` cuts that to 48 s on the
  NVMe, but reading the output back then takes 54 s instead of 11 s from the
  page cache. On the nightly runner (238 MB/s direct writes, 530 MB/s reads)
  apply plus read-back took 620 s direct against 400-450 s through the page
  cache, writeback included, so the nightly leaves it off.
- Patch generation, 126 variants: 23 min → 6 min (geocoder-diff memory and
  parallelism; a diff's memory budget is released when its verify starts).
- Compression: zstd 18 instead of 19 (+0.3% size, −21% CPU), identical files
  compressed once (18% of a build), one worker per core sized by file, run
  alongside patch generation.
- Upload: each file goes to Tigris as soon as it is compressed, checksums on
  every core, retention deletes batched. The previous day's index downloads
  while the planet builds.

On the nightly runner (80 CPUs) the whole run went from 3h 42m to 1h 08m, a
re-run of a day whose PBF was cached, so without the diff apply. It was then
bound by its network and disk. Both turned out to be infrastructure: the
node's link had negotiated 1 Gbit/s (forced to 10G: 9 Gbit/s to Ceph and
Tigris since), and its local disk did 240-530 MB/s. Its scratch now lives on
a Ceph block volume, which does 650 MB/s of writes and 1.5 GB/s of reads,
and both at once. One of the two Ceph RGW daemons is still on a 1 Gbit/s
link.

A real nightly (2026-10-12, diff applied) now takes 38 min:

| Step | Time | |
|---|---|---|
| First download starts | 18 s in | tools compile, or come from a cache keyed on the builder source, and run their tests beside the downloads |
| Downloads | 4.2 min | the PBF at ~590 MB/s; wikidata, GeoNames and TIGER beside it |
| Apply the day's diff | 2.2 min | pbf-apply with libdeflate |
| Build | 8.6 min | the write phase shares the scratch volume with yesterday's index as it decompresses |
| Patches + compression | 20 min | zstd 1.5.7 at -18, ~100k core-seconds on 80 cores; patch generation beside it |
| Finalize | under 1 min | Tigris upload at ~400-500 MB/s |

zstd 1.5.7 at the same level, -18, compressed every planet file sampled
smaller and in less CPU than Ubuntu's 1.5.5 (geo_cells 15% smaller in 17%
less CPU), and the published build 2.4% smaller overall; any zstd
decompresses it.

Tried on the runner, and reverted:
- Compressing each file while the build still wrote the rest, on the write
  phase's idle cores. On the old disk and link it took 71.6-74.4 min against
  73.5; on Ceph and 10G, 37.6 against 34.9. Compression is CPU-bound, and
  overlapping it only moved the contention.
- Fetching yesterday's index during the downloads instead of the build. Its
  113 GB of writes pushed the downloaded PBF out of the page cache, and the
  build's Pass 2 read it back from the volume: 157-277 s instead of 31 s.
- Read-ahead advice in the patcher's file reader. Verifying planet/full
  takes ~585 s in the nightly, but alone on the runner it applies in
  212-217 s, mostly on CPU (~195 s user + sys); with the advice it took
  234-236 s. The nightly's 585 s comes from sharing the cores with
  compression, which ends after patch generation anyway.
- Fetching yesterday's index once the build was done with the PBF (after
  Pass 2b) instead of with it. On 128 GiB the PBF then stayed cached and
  the build took 466 s against 541; but the fetch's writes landed on the
  write phase instead, which is bound by the volume's write rate: 463
  against 394 s on giant, 652 against 581 s on 96 GiB.

Smaller runners, same day re-run (2026-10-12, no diff to apply):

| Runner | Run | Build | Patches + compression |
|---|---|---|---|
| giant, 80 CPUs, 200 GiB | 30.8 min | 6.6 min | 18.9 min |
| massive, 64 CPUs, 128 GiB | 37.9 min | 9.1 min | 22.1 min |
| massive-96, 64 CPUs, 96 GiB | 38.5 min | 9.8 min | 22.4 min |

Patches and compression scale with the cores. The build loses its page
cache: below ~200 GiB the PBF and yesterday's index don't both fit, Pass 2
and 2b read the PBF back from the volume, and the PBF prefetcher (8 MiB
reads, 16 in flight) holds that to 119 + 88 s against 337 + 67 s without
it. At 96 GiB the build also needs `GC_MEMORY_RESERVE_MIB` (12 GiB in the
nightly): it sizes its write phase to the job's limit, and the fetch
beside it (~4.6 GiB, plus dirty pages) shares that limit; without the
reserve the pod was OOM-killed. Telling the build it had the CPU quota's
46 cores instead of the host's 64 changed nothing (4:57-5:20 against
5:09-5:12 on the box).

## Earlier result: 54m → 12m 22s (77% faster)

Build: `--multi-output --continents --multi-quality`
Output: 379 files, 91 GiB (planet + 8 continents × 3 modes × 7 quality levels)
Machine: Node 3 — 32-core / 174 GiB RAM
Validated: planet output identical to pre-optimization baseline

## Phase Breakdown (Node 3)

| Phase | Baseline (54m) | Final (12m 22s) | Speedup |
|---|---|---|---|
| Pass 1: relations | 46.6s | **3.9-7s** | 7-12x |
| Pass 2: nodes | 184.4s | **80-106s** | 1.7-2.3x |
| Pass 2b: ways | 132.3s | **88-115s** | 1.1-1.5x |
| Admin S2 drain | 134.4s | 109-132s | ~same |
| S2 street ways | 136.9s | 129-155s | ~same |
| Rebuild cell maps | 224.2s | **0s** | eliminated |
| Dedup | (in S2) | 38-55s | — |
| Continent processing | 2087.5s | **60-131s** | 16-35x |
| **Total** | **54m 1.7s** | **12m 22s** | **4.4x** |

## All Optimizations (30+)

### Custom PBF Reader (replaced osmium)
1. Parallel PBF reader with per-thread fds and pread
2. Streaming node decode — no PbfNode objects, inline callback
3. Streaming way decode — no PbfWay objects, inline callback
4. Zero-copy varint decode for dense nodes
5. Binary search blob classification (32 decompressions vs 50K)
6. Reusable decompression buffer + z_stream per thread
7. Flat way_refs array (eliminates per-way vector allocation)
8. PbfBlock reuse across iterations (keeps vector capacity)
9. Zero-copy tag storage (string table indices, not copies)
10. posix_fadvise WILLNEED for page cache warming
11. Single-pass tag extraction + early exit for irrelevant ways
12. Bitset admin way lookup (replaces hash set)
13. First-character switch for highway/tag comparisons

### Data Processing
14. Parallel continents (4 concurrent, largest-first by bbox area)
15. Eliminated planet cell map rebuild (filter from sorted pairs)
16. Parallel remap in filter_by_bbox
17. Parallel mode writes (full/no-addresses/admin simultaneously)
18. Parallel quality variants (3 concurrent)
19. Sorted pair filtering (cache-friendly linear scan)
20. Parallel chunked scanning per continent
21. Parallel per-cell dedup
22. Admin polygon sorting (largest-first for work-stealing)
23. Worker threads = N-1
24. Deterministic interp resolution (lexicographic tiebreaker)
25. Sorted pair remap for continents (fixes street_entries bug)
26. Precomputed continent bitmasks (1 S2 lookup per cell instead of 8)
27. Bitset ID collection for continent filtering (replaces hash set)
28. Per-thread bitsets to avoid race conditions
29. MADV_HUGEPAGE on dense node index

### Continent Boundaries
30. Geofabrik .poly files for continent boundaries
31. Coordinate-level polygon filtering (point-in-polygon per item)
32. Any-node-inside way matching (matches Geofabrik behavior)

### Quality Variants
33. Never drop polygons — binary search for minimum epsilon preserving ≥4 vertices

### Profiling
34. Per-phase CPU/memory profiling (getrusage + /proc/self/statm)
35. Per-thread read/decode/callback timing in streaming readers

## Bug Fixes
- Continent street_entries.bin was empty (fixed by sorted pair remap)
- Deterministic interp resolution (old code had nondeterministic last-wins)
- PBF way callback used return instead of continue (skipped 50% of ways)
- Race condition in continent bitset (per-thread bitsets fix)
- Polygons dropped during quality simplification (min-vertex enforcement)

## Multi-Machine Results

| Machine | Cores | RAM | Build Time |
|---|---|---|---|
| Node 3 (Newer CPU) | 32 | 174 GiB | **12m 22s** |
| Node 2b (E5-2699A v4) | 80 (cgroup) | 192 GiB | ~22-27m (I/O bound) |

Node 2b is slower despite more cores due to older CPU, slower storage, and
NUMA effects on the 111 GiB dense index. The fadvise optimization helps
significantly (72% faster reads) but the machine's I/O subsystem limits
parallel pread throughput.
