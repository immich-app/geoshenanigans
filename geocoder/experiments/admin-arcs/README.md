# Admin arcs: each shared border stored once

Does storing every shared border once, as arcs that neighbouring polygons
and the levels above them reference, remove the size-against-accuracy
trade-off that `../boundary-tiers` and `../admin-tolerances` work around?
This study builds arcs from the OSM member ways of every admin polygon,
simplifies each arc once, writes an arc file format plus a prototype reader,
and compares it with per-polygon schemes on size, accuracy, overlaps, read
cost and day-to-day change. Run 2026-10-19 on the box (`/ssd/arcs`) on
planet-261006 (`fix/admin-nesting` bc2e758 plus dump hooks), re-checked the
same day by an independent verifier (`/ssd/verify5`, `verify/`) that
regenerated the files from the committed tool, rebuilt the readers and
scored its own fresh pools.

Where the study and the verifier differ, the verifier's number is given.
Tags: **M** = measured on the box, **C** = computed from measured values,
**E** = estimated. MB = 10⁶ bytes, zstd 1.5.7 `-18 -T1`. Truth is the
uncapped geometry with holes, read by today's reader.

## Results (planet-261006, verified 2026-10-19)

The configuration taken forward is **flat 37.5 m at every level, ring cap
0.1×√area, per-arc and chunk bboxes** (`arcs` scheme
`uni=37.5;cap=0.1;chunk=8`, variant `af37cb8`).

- **Size (M):** admin-minimal 189.09 MB raw / 89.07 MB zstd; quality dir
  206.77 / 125.33 MB. Regenerated from the committed tool, 26/26 files are
  byte-identical to the study's.
- **Against per-polygon schemes on the same planet (C):** admin-minimal
  versus q2.5+cap −21.8% raw / −31.6% zstd (per-polygon zstd in the planned
  layout Bg); versus r1 (`../admin-tolerances`) −21.5% / −31.1%; quality dir
  versus r1 −27.4% / −36.9%.
- **Whole admin-minimal set (M):** 377.2 → 326.4 MB raw, 232.2 → 152.2 MB
  zstd (−50.8 / −80.0 MB). The study's totals (379.1 → 328.3, 233.1 →
  153.1) have the same differences but both include one extra 1.87 MB file.
- **Accuracy (M):** more accurate than every per-polygon scheme, including
  per-polygon flat 37.5 m (which is 25% bigger than q2.5). Against flat
  37.5 m per-polygon: equal on L2, L4 and photo-like; better on L8 city (19
  against 38 points, p = 0.016) and on border points overall (p = 3e-4).
  Against r1: better on L2, L4, L7 and photo-like city (p ≤ 2e-4); L8 not
  significant (p = 0.22). Table below.
- **Overlaps (M):** points two same-level polygons both claim where the
  truth has at most one, per 10k of the verifier's 70k border points
  (L2 / L4 / L8): arcs 0 / 0 / 0 (6 in total over L2–L8); q2.5 54.6 /
  67.9 / 36.6; r1 3.9 / 23.7 / 33.4; 75/50 + cap 7.2 / 21.4 / 50.7 (C).
  What arcs leave is gaps along coasts and maritime limits, which no
  neighbour shares: at q2.5 tolerances arcs still get 10.7% of countries
  wrong near L2 borders (study).
- **Round trip (M):** the arc reader on the arc format (with
  `admin_vertices.bin` replaced by 16-byte dummies, so any read of it would
  show) gives the same answers as today's reader on the arcs decoded into
  today's files: 0 differences on 160k points × 2 sets. On per-polygon
  files the arc reader also matches today's reader: 0 differences.
- **Read cost (M):** about 2× faster than today's reader on admin-minimal
  (94.6 → 47.0 µs photo-like, 83.5 → 29.4 µs at country borders, cold 800 →
  591 µs) and not slower on recommended (220 → 147 / 134 → 57 µs). But the
  arc reader on per-polygon q2.5 already gets 45.9 / 36.4 µs: most of the
  speed-up comes from the reader commits, not the arc format.
- **Builder cost (M):** the standalone tool takes 81.7 s / 19.4 GB peak
  (admin-minimal) and 123.6 s / 26.3 GB (quality dir) on 32 threads. The
  study measured 98–156 s and 136–184 s; per-polygon `tiervar` takes
  4.6–6.3 s and 3.4 GB. Inside the builder, node ids and a parallel repair
  should cut this a lot (E).
- **Daily patch, about 2× smaller (E), not 3–7×:** the study's per-polygon
  baseline was an idealised changed-block payload. On the real q2.5+cap
  261017 → 261018b quality dir that measure gives 1,081,437 B zstd while the
  real `geocoder-diff` patch is 274,664 B, overstated 3.9× (M). Against real
  patches the arc payload (120–151 KB zstd, M) is about 1.7–2.3× smaller
  (E: different day pairs; no arc patcher exists).
- **Stable arc ids (M, study):** from planet-261005 to 261006, 99.93% of
  arcs are byte-identical (1,063,653 same, 697 new, 615 removed); 507 of the
  697 new arcs run along the same set of ways as an arc of the day before,
  so a key on that way set would keep their id. A stable-slot allocator for
  arcs is required (E: without one every reference list shifts).
- **Live Nominatim (M):** verifier, arcs against r1 on 24 disagreement
  points (zoom 18): Nominatim gives arcs' answer 14 times, r1's 7, neither
  3. Study, arcs against per-polygon 75/50 + cap on 32 points (zoom 10):
  arcs 22, 75/50 6, truth 25 (zoom 18: 23 / 6 / 21).

### Accuracy, planet-261006 (M, verifier, % wrong)

bb70k (10k points per level, ±200 m of borders), bp50k photo-like, ba30k
address points; fresh-10k figures in brackets.

| scheme | country B2 | state B4 | city B7 | city B8 | photo city | photo any | address any |
|---|---|---|---|---|---|---|---|
| q2.5 (today) | 35.32 (34.79) | 15.79 | 3.01 | 2.44 (3.75) | 0.266 | 0.508 | 0.277 |
| q2.5+cap | 35.28 (34.65) | 15.74 | 3.02 | 2.41 (3.75) | 0.254 | 0.488 | 0.277 |
| r1 | 3.82 (4.09) | 4.61 | 2.28 | 1.98 (3.17) | 0.242 | 0.372 | 0.150 |
| 75/50 + cap | 4.05 (3.82) | 4.07 | 3.34 | 2.73 (4.47) | 0.290 | 0.316 | 0.190 |
| flat 37.5 m, per-polygon | 1.79 (2.05) | 1.86 | 1.82 | 2.04 (3.31) | 0.192 | 0.202 | 0.123 |
| **arcs, flat 37.5 m + cap** | **1.77 (2.18)** | **1.84** | **1.76** | **1.84 (3.46)** | **0.180** | **0.188** | **0.110** |

Enclave grid points right out of 3439: arcs 3408, flat 37.5 m 3407, 75/50
3380, q2.5 2738.

### Query time, planet-261006 (M, verifier, µs per query)

Single thread under the timing lock; warm best of 3 minus process start;
cold = page cache dropped, 10k points.

| set | reader | photo-like | L2 border | cold |
|---|---|---|---|---|
| admin-minimal q2.5 | nest (today) | 94.6 | 83.5 | 800 |
| admin-minimal q2.5 | arc reader | 45.9 | 36.4 | 739 |
| admin-minimal r1 | nest3 | 64.7 | 89.4 | 839 |
| admin-minimal arcs | arc reader | 47.0 | 29.4 | 591 |
| recommended q2.5 | nest | 220.0 | 133.9 | 2178 |
| recommended r1 | nest3 | 170.0 | 133.3 | 2105 |
| recommended arcs | arc reader | 147.3 | 57.3 | 1842 |

nest = `fix/admin-nesting` bc2e758; nest3 = nest + 20e4f4f, f2f6d52,
8dbb58d; arc reader = `arcs-reader` e566648 (r5 below).

### Study tables (M, the study's pools; not re-measured except as above)

Arc structure:

| | admin-minimal (L2–8) | quality dir (all levels) |
|---|---|---|
| rings | 509,699 | 1,143,215 |
| vertex occurrences → distinct vertices | 296.0M → 111.4M (2.66×) | 421.6M → 151.1M |
| arcs (shared by ≥2 rings / used by several levels) | 1,064,353 (922,075 / 333,659) | 2,498,319 (2,008,905 shared) |
| shared arcs whose users run along different OSM ways | 9,655 (1.0%) | 74,950 |
| new crossings made by simplification → left after repair | 17,473 → 798 | 74,727 → 4,954 |

Stored vertices 17.69M (q2.5) and 13.92M (flat 75 m) against
`../boundary-tiers`' estimates of 18.42M and 14.34M; the arc offset table is
4.3 MB raw / 1.9 MB zstd (estimated ~6 / ~3). Every arc round-trips from its
encoded bytes with 0 mismatches.

Admin-minimal candidates (arc formats: arc_vertices, arc_index, poly_arcs,
admin_polygons plus the unchanged cells and entries; "planned" = per-polygon
vertices in layout Bg, the arc format is already in that style):

| scheme | format | raw MB (Δ vs q2.5) | zstd MB (Δ vs q2.5 planned 129.0) | photo-like any % | L2 country % | L4 state % | L8 city % (fresh L8) | enclaves /3439 | overlaps per 10k (L2 / L4 / L8) |
|---|---|---|---|---|---|---|---|---|---|
| q2.5 | per-polygon | 239.9 | 169.1 / planned 129.0 | 0.51 | 34.98 | 15.36 | 2.42 (2.71) | 2738 | 75.6 / 89.4 / 32.4 |
| q2.5 + cap | per-polygon | 241.8 (+0.8%) | planned 130.2 (+0.9%) | 0.49 | 34.97 | 15.29 | 2.42 (2.71) | 3301 | 76.0 / 89.2 / 32.4 |
| 75 m, L8 50 m + cap | per-polygon | 241.8 (+0.8%) | planned 127.3 (−1.3%) | 0.34 | 4.26 | 4.39 | 3.15 (3.96) | 3380 | 9.5 / 28.1 / 45.5 |
| 75 m + cap | per-polygon | 226.2 (−5.7%) | planned 116.6 (−9.6%) | 0.41 | 4.26 | 4.39 | 4.76 (5.49) | 3380 | 9.5 / 28.1 / 76.6 |
| flat 37.5 m | per-polygon | 300.1 (+25.1%) | planned 157.9 (+22.4%) | 0.20 | 2.06 | 2.10 | 2.21 (2.64) | 3407 | 6.9 / 16.5 / 32.4 |
| q2.5 tolerances + cap | arcs | 176.6 (−26.4%) | 80.7 (−37.5%) | 0.21 | 10.68 | 4.68 | 2.24 (2.16) | 3408 | 0.5 / 0.3 / 0.6 |
| **flat 37.5 m + cap** | **arcs** | **187.0 (−22.1%)** | **87.0 (−32.6%)** | **0.18** | **2.02** | **2.13** | **2.24 (2.16)** | **3408** | **0.0 / 0.2 / 0.6** |
| same + chunk bboxes (timed) | arcs | 189.1 (−21.2%) | 89.1 (−30.9%) | same geometry | | | | | |
| flat 50 m + cap | arcs | 174.3 (−27.3%) | 79.3 (−38.5%) | 0.24 | 2.76 | 2.83 | 2.96 (3.06) | 3390 | 0.0 / 0.3 / 1.0 |
| 75 m, L8 50 m + cap | arcs | 168.5 (−29.8%) | 75.7 (−41.3%) | 0.28 | 3.73 | 3.68 | 2.96 (3.06) | 3390 | 0.2 / 0.5 / 1.0 |
| 75 m + cap | arcs | 159.1 (−33.7%) | 69.5 (−46.1%) | 0.38 | 4.22 | 4.25 | 4.45 (4.45) | 3387 | 0.2 / 0.7 / 1.0 |
| 75/50 + 11 m grid | arcs | 155.9 (−35.0%) | 59.9 (−53.6%) | 0.29 | 3.87 | 3.79 | 3.22 | 3391 | 0.2 / 0.7 / 1.0 |

Recommended quality dir: arcs flat 37.5 m + cap 201.7 MB raw / 120.4 MB
zstd (with chunk bboxes 206.8 / 125.3), against per-polygon q2.5 283.2 /
planned 197.2; arcs need no postal_vertices, since postal polygons point at
the same arcs. Loc-to-city: 296–297 of 421 for every candidate.

Read cost by reader step (study, admin-minimal, µs photo-like / L2 border):
today's reader q2.5 83.3 / 83.1; r3 (reader commits + integer straddle
test) q2.5 37.0 / 33.8; arcs flat 37.5 without chunk bboxes 63.3 / 47.2
(without per-arc bboxes 101 / 110); with chunk bboxes (r5) 40.5 / 27.9.
The integer straddle test alone speeds up the per-polygon reader by 10–40%.
Decoded vertices per query: q2.5 r3 9.6k / 17.5k, arcs r5 1.7k.

Daily payload by identity, planet-261005 → 261006 (study; payload M, the
patcher framing around it E):

| scheme | part | arcs raw / zstd | per-polygon raw / zstd (idealised) | polygons changed, arcs / per-polygon |
|---|---|---|---|---|
| q2.5 + cap | admin-minimal | 252 KB / 120 KB | 344 KB / 316 KB | 860 / 346 |
| 75/50 + cap | admin-minimal | 237 KB / 110 KB | 828 KB / 764 KB | 860 / 354 |
| 75/50 + cap | quality dir | 373 KB / 147 KB | 902 KB / 823 KB | 1676 / 642 |

## Decision status

Not shipping. The verifier's recommendation: ship r1 now
(`../admin-tolerances`, branch `feat/admin-tier-tolerances`) and keep arcs
as the next step. Arcs make per-level tolerances moot, so r1 costs nothing
to throw away later. Arcs need:
- a builder integration with stable arc ids and continent subsetting;
- a patcher for the three new files (none exists);
- the reader, from the prototype in `patches/`;
- a measurement on planet-261018b (which has China; arcs were only
  measured on planet-261006) and a real warm build plus patcher run.

Do not ship the 11 m grid with arcs as built: it drops 4,400 tiny polygons
in admin-minimal (5,344 in the quality dir) and leaves 5–6k crossings.

The reader commits and the integer straddle test (626a568) are independent
of arcs; the straddle test went into `../reader-cell-scan` as 4e8518b.

### Not verified / open

- Whether admin-minimal and the recommended set should share one arc file
  (today each has its own).
- After repair 600–800 crossings remain in admin-minimal and ~4.7k in the
  quality dir, at grid-snap duplicates; their effect is inside the accuracy
  numbers.
- Size savings that need reader support: varint references (−8 MB raw in
  admin-minimal, M) and delta-coded chunk bboxes (E).
- The study found that 66a5ad0 ("find_admin reads only the query cell")
  changes 5–6 of 419,221 answers per set; the cause is diagnosed in
  `../reader-cell-scan`.

## Method

- **Dump hooks** (`patches/arcs-dump.patch`, d3ad25b on bc2e758):
  `GC_TIER_DUMP` (the final admin arrays, as tiers2) and `GC_ARC_DUMP`
  (every admin polygon's rings exactly as assembled from its OSM member
  ways, exact 1e-7 coordinates plus the member way of each edge; nothing
  simplified). One cold planet build each of planet-261006 (d1006) and
  planet-261005 (d1005) with both hooks (`study/run_planet.sh`; the 261006
  build took 364 s and wrote a 6.4 GB arcdump).
- **Arcs** (`study/arcs.cpp`, format documented in its header): a junction
  is a vertex where the combined rings meet more than two neighbours; each
  junction-to-junction run is one arc, stored once in the direction any ring
  uses it; rings point to arcs with a direction bit, holes included. Each arc
  is simplified once (the builder's Douglas–Peucker) at the finest tolerance
  of all rings that use it, endpoints fixed, cap 0.1×√(ring area). Vertices
  snap to a global 1.1 m grid so a junction is bit-identical in every arc;
  every ring keeps ≥3 vertices and a non-zero area; crossings that
  simplification created get vertices back (crossings in the raw data stay).
  Writes the arc format (`arc/`) and the arcs decoded back into today's
  files (`dec/`, raw f32 per vertex) for today's reader.
- **Per-polygon baselines:** `../boundary-tiers/study/tiervar.cpp` (same
  source, the study's own build `/ssd/arcs/tiervar`), on the same dump. The study's
  baselines reproduce tiers2's (enclaves 2738 / 3301 / 3380, L8 city 2.42 /
  3.15 / 4.76).
- **Candidates:** `mkvar.sh` (one candidate: `pp` per-polygon or `arc`;
  sets `<name>` read the decoded files, `<name>a` the arc format), `cand.sh`
  (mkvar + answers + `score.py` + loc-to-city), `batch1.sh` (truth,
  baselines and every arc candidate, then `sizes.py`), `master.sh` (gaps and
  overlaps `batch3.sh` with `cover.cpp`, patch study `batch2.sh` with
  `patchcmp.py`, timing `batch4.sh`), `batch5.sh` / `batch6.sh` (timing of
  r3 and r5), `report.py` (tables).
- **Readers** (`patches/`, `timeq.sh` labels): r0 = bc2e758 with the dump
  hooks; r1 = r0 + 20e4f4f, f2f6d52, 8dbb58d and the arc reader prototype
  (372a27b: when a set holds `arc_vertices.bin` / `arc_index.bin` /
  `poly_arcs.bin` / `arc_meta.json`, admin containment runs on the arcs,
  each arc decoded at most once per point, skipped by its bbox;
  `GC_ARC_STATS=1` prints counters); r3 = + integer straddle test (626a568:
  edges that do not straddle the point's longitude are rejected with an
  integer compare, answers bit-identical); r5 = + chunk bboxes in the ring
  references (7c7cecf). Answer equivalence: `eqr1.sh`, `eqarc.sh`,
  `eqr3.sh` (r1, r3, r5 give byte-identical answers to today's reader on
  419,221 points per set, M).
- **Work and time:** `workq.sh` (decoded geometry per query), `timeq.sh`
  (timing lock, pinned core, warm best of 3 minus process start, cold 10k).
- **Accuracy:** `score.py` (Immich fields as `mapGeocoderAddress`, enclave
  grids), `l2c.sh` (421 loc-to-city checks), `nomcheck.py` (public
  Nominatim where two candidates disagree), `wholeset.py`.
- **Debug:** `dbg_arc.py`; `bboxshift.py` measures how far today's packing
  shifts decoded vertices (f32 bbox minimum against offsets relative to the
  f64 minimum); size of the effect only.
- **Verifier** (`verify/`): `arcs_regen.sh` regenerates af37cb8 with its
  own build of the committed `arcs.cpp` and compares 26/26 files;
  `stage_arcs.sh` assembles d6 sets (today's files, truth, 75/50 + cap,
  flat 37.5 m, af37cb8 decoded and in the arc format with dummy vertex
  files) and answers them with nest, nest3 and the arc reader;
  `stage3.sh` adds per-polygon q2.5+cap and r1 on the same dump (with
  `../admin-tolerances/study/tiervar3.cpp`), sizes, planned sizes and the
  `cover` overlap counts on its own 70k border points; `wholeset.sh`
  whole-set totals; `ppreal.py` the idealised per-polygon patch measure on
  real q2.5+cap builds (it imports a copy of `study/patchcmp.py` whose temp
  dir points at `/ssd/verify5/tmp`). Builds, samplers, scorer, timing and
  live Nominatim are shared with the tiers3 check, in
  `../admin-tolerances/verify/` (`build_bins.sh`, `v5border.cpp`,
  `v5photo.py`, `v5score.py`, `v5q.sh`, `sizes.sh`, `planned.sh`,
  `timing.sh`, `stage2.sh`, `stage2t.sh`, `nompick.py`, `napi3.py`,
  `nomscore.py`).

## Layout

- `patches/` (`git format-patch` from branch `arcs-reader` in
  `/ssd/arcs/repo`):
  - `arcs-dump.patch` (d3ad25b): builder dump hooks, on bc2e758.
  - `arcs-reader.patch` (372a27b): the arc reader prototype, on top of
    cherry-picks of 20e4f4f, f2f6d52, 8dbb58d and 66a5ad0.
  - `arcs-straddle.patch` (626a568): integer straddle test, after the
    revert of 66a5ad0.
  - `arcs-chunk-bboxes.patch` (7c7cecf): chunk bboxes in the arc refs.
- `study/`: `geocoder/builder/tools/arcs_study/` of `arcs-reader` (e566648)
  plus `bboxshift.py`.
- `verify/`: the arcs part of the verifier's `verify5/tools` (743e6ef).

Left out: the edit scripts that made the commits (`arc_dump_patch.py`,
`reader_patch.py`, `reader_patch2.py`, `reader_patch4.py` = the patches
here) and `reader_patch3.py` (an arc parity cache keyed by the last four
points, never committed or reported).

The scripts hard-code the box paths (`/ssd/arcs`, `/ssd/verify5`, and
`/ssd/tiers2`, `/ssd/tiers3`, `/ssd/bugs`, `/ssd/bugs2`, `/ssd/shrink`,
`/ssd/transport-verify`, `/ssd/loc2city` for shared tools, pools, truth and
checks) and run Python from `/ssd/shrink/venv` (numpy).

## Rebuild and run

```sh
X=$PWD/geocoder/experiments/admin-arcs
R=/path/to/geoshenanigans            # a second checkout
A=/ssd/arcs; V=/ssd/verify5
mkdir -p $A && cp $X/study/* $A/ && cp $X/verify/* $V/

# builder with the dump hooks -> $A/bin/build-index; readers r0, r1, r3, r5
git -C $R checkout -b arcs-reader bc2e758
git -C $R am $X/patches/arcs-dump.patch             # r0: build-index and grid_query here
git -C $R cherry-pick 20e4f4f f2f6d52 8dbb58d 66a5ad0
git -C $R am $X/patches/arcs-reader.patch
git -C $R revert --no-edit HEAD~1                    # 66a5ad0 changes answers
git -C $R am $X/patches/arcs-straddle.patch          # r3
git -C $R am $X/patches/arcs-chunk-bboxes.patch      # r5
# (checked when packaging: this gives 7c7cecf's lib.rs, grid_query.rs and build_index.cpp blob for blob)
# grid_query per step: CARGO_TARGET_DIR=$A/target{,-r,-r3,-r5} cargo build --release --example grid_query

# tools (compile lines were not scripted, except cover's in verify/stage3.sh)
I=$R/geocoder/builder/src
g++ -O3 -march=native -std=c++20 -fopenmp -I$I $X/study/arcs.cpp -o $A/arcs
g++ -O3 -march=native -std=c++17 -fopenmp -I$I $X/../boundary-tiers/study/tiervar.cpp -o $A/tiervar
g++ -O3 -march=native -std=c++20 -fopenmp $X/study/cover.cpp -o $A/cover

cd $A
./run_planet.sh d1006 /path/to/planet-261006.osm.pbf     # takes the timing lock
./run_planet.sh d1005 /path/to/planet-261005.osm.pbf
./mkbase.sh d1006
./batch1.sh && ./master.sh                                # candidates, overlaps, patches, timing
./batch5.sh && ./batch6.sh                                # r3 and r5 timing
/ssd/shrink/venv/bin/python -I report.py q25 q25c r50c r75c f37 aq25c af37c ar50c > cmp/report.md

# verifier (after ../admin-tolerances/verify/build_bins.sh; bin/arcs built from study/arcs.cpp)
cd $V
./arcs_regen.sh && ./stage_arcs.sh && ./stage3.sh
./wholeset.sh d6base d6qcap d6r1 d6af d6afa
```

The study's point pools in `$A/pts` are tiers2's (p200k, places, L2, L4,
L8, uniform100k, random5k, grids, l2c) plus the tiers2 verifier's fresh20k.

## Box paths

- Study: `/ssd/arcs` (89 GB): dumps `build/{d1005,d1006}`, build
  `build/out-d1006`, candidates `var/`, sets `sets/`, answers `q/`, results
  `cmp/` (`*.sizes.json`, `*.score.json`, `*.cover.json`, `patch_*.json`,
  `time*_*.tsv`, `work_*.tsv`, `nom_r50c_af37c_smallest.json`), `l2c/`.
- Study repo `/ssd/arcs/repo`, branch `arcs-reader` (d3ad25b, cherry-picks
  761a776 / 6c8249c / 7af530c / e5b869d, 372a27b, revert 91f8a33, tools
  d2afa22, 626a568, 7c7cecf, scripts e566648) and `arcs-dump`.
- Verifier: `/ssd/verify5/arcs/af37cb8` (regenerated files), `sets/d6*`,
  `var/d6*`, `cmp/` (`arcs_regen_cmp.tsv`, `arcs_regen_size.tsv`,
  `cover_*.json`, `time_d6.tsv`), `pts/d6/`.

## Data and tool versions

- OSM planets 2026-10-05 and 2026-10-06 with the nightly's inputs (TIGER,
  Wikidata sitelinks, GB postcodes); builder `fix/admin-nesting` bc2e758 +
  d3ad25b; 96 GiB scope without swap.
- Readers: bc2e758 (nest), the `arcs-reader` steps above, nest3 (bc2e758 +
  20e4f4f, f2f6d52, 8dbb58d).
- Layout Bg: `../boundary-precision/grid/bgcodec.cpp`. Loc-to-city:
  `../loc2city-eval/verify/l2c_eval.py`. Public Nominatim `/reverse`
  (jsonv2, addressdetails).
