# Build Optimization Log

## October 2026: 1h 20m → 7m 06s, 188 → 117 GB peak

Build: `--continents --multi-output --multi-quality` with TIGER, wikidata
sitelinks and external postcodes, chained on the previous day
(`--prev-output`, so strategy-2 runs). Machine: 64-core / 251 GB box, run on
62 cores. Every change was gated on byte-identical output: oceania at 64 and
8 reported CPUs, then a full planet day compared file by file with the
reference build, plus all 126 patches compared byte for byte.

| Phase | Before | After | What changed |
|---|---|---|---|
| Pass 1 | 129 s @1 core | 5 s | parallel blob-header scan |
| Pass 2 (nodes) | 173 s | 71 s | sparse node index, parallel merge and interning |
| Pass 2b (ways) | 222 s | 35 s | streams only way blobs, parallel merge, packed way geometries |
| Admin assembly | 465 s | 12 s | ranked candidates, bbox reject, parallel TIGER/GeoNames loading |
| S2 cell computation | 925 s | 91 s | per-cell linking, parallel interpolation endpoints |
| Deterministic ordering | 699 s @1 core | 51 s | parallel sorts with a serial fallback when ties reach output |
| All index writing | 2143 s @2 cores | 114 s | parallel postcode centroids and strategy-2, memory-budgeted stages |
| **Total** | **1:19:42** | **7:06** | |

Memory: the peak was 188 GiB in Pass 2b (a dense 111 GiB node index plus
merge copies). Only nodes that a kept way references get coordinates now,
thread data is freed as it merges, and the write phase schedules its regions
and stages within the memory limit (min of MemTotal and the cgroup limit,
capped by `GC_MEMORY_LIMIT_MIB`). Unconstrained peak 117 GB; inside a 128 GiB
cgroup the planet build finishes in 7:17 at 112 GB with identical output.

Pipeline around the builder (same box):
- Applying the daily diff: `osmium apply-changes` 52 min → `pbf-apply` about
  2 min (copies untouched blobs, re-encodes the rest in parallel; object
  content identical to osmium's).
- Patch generation, 126 variants: 23 min → 6 min (geocoder-diff memory and
  parallelism; a diff's memory budget is released when its verify starts).
- Compression: zstd 18 instead of 19 (+0.3% size, −21% CPU), identical files
  compressed once (18% of a build), one worker per core sized by file, run
  alongside patch generation.
- Upload: cache copy alongside the Tigris upload, checksums on every core,
  retention deletes batched.

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
