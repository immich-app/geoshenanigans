# Boundary tiers: one tolerance across levels instead of the q ladder

Which simplification tolerances should the admin boundaries of the
admin-minimal set and the recommended set's quality dir use, at the size of
q2.5 plus the ring cap (9b55d28) or smaller? q2.5 grows the tolerance with
each coarser level (L8 37.5 m … L2 1250 m); `../boundary-precision` found
that one uniform tolerance beats that ladder at the same size. This study
re-simplifies the builder's own final planet admin arrays (with holes, from
`fix/admin-holes`) into 35 candidate schemes and scores each with the real
reader against the uncapped truth. Run 2026-10-18 on the box
(`/ssd/tiers2`), re-checked the same day by an independent verifier
(`/ssd/tiers2-verify`, `verify/`) with its own build of the tool, its own
scorer and fresh points.

Every number below is the verifier's. The study's labels for the border
error rates were wrong (they were "any Immich field changed", not the
country or state field), and its claim that city borders improve did not
hold: city answers near L7/L8 borders get worse.

## Results (planet-261006, verified 2026-10-18)

**Recommended scheme: 75 m at L2–L7, 50 m at L8, 37.5 m at L9 and above,
cap 0.1×√area** (`tiervar` scheme
`uni=75;t8=50;t9=37.5;t10=37.5;t11=37.5;t12=37.5;t13=37.5;t14=37.5;t15=37.5;cap=0.1`).
It is the same size as q2.5+cap, on disk and compressed.

- **Size:** admin files +0.8% raw / +0.4% zstd versus q2.5 (the cap alone:
  +0.8% / +0.9%); whole admin-minimal set +0.5% / +0.3%.
- **Country near country borders:** L2 country error 34.0% → 4.0%
  (q2.5+cap → scheme, fresh points within ±200 m of L2 borders).
- **State near state borders:** L4 state error 14.7% → 4.0%.
- **Photo-like points:** any Immich field changed 0.43% → 0.30% (paired
  sign test p = 0.02). The gain is all state and country; city stays at
  0.23–0.24%.
- **City near city borders gets worse:** L8 city 2.71% → 3.96% (21 points
  wrong only under the scheme against 3 only under the cap, p = 0.0003); L7
  city 25 against 8 (p = 0.005). The any-field rate at L8 (4.38 → 4.03%)
  does not change significantly (p = 0.55).
- **Enclaves:** 3301 → 3380 of 3439 enclave-grid points right (q2.5 2738,
  truth 3438), equal or better on every grid. Answers leaking the
  surrounding place: 734 / 153 / 66 / 6 (q2.5 / cap / scheme / truth).
- **Live Nominatim:** on 10 fresh points where q2.5+cap and the scheme give
  a different country or state, public Nominatim (zoom 10) agrees with the
  scheme on 9, with the cap on 1 and with the truth on 10.

**Smaller option: 75 m at L2–L8 plus the cap** (`uni=75;cap=0.1`, L9+ kept
at 37.5 m in the quality dir): −5.7% raw / −8.6% zstd on the admin files
(−3.6% / −6.2% of the whole admin-minimal set); quality dir −4.3% / −4.8%
(−1.1% / −2.1% of the whole recommended set). The price is about double the
city error near L7/L8 borders: L8 city 5.49%, L7 city 2.60 → 4.73%
(p < 1e-5); photo-like city 0.24 → 0.31% (not significant).

**11 m grid** (snap the simplified rings to a 1e-4° grid): 6–9% smaller
compressed (zstd of the admin files −6.1% admin-minimal, −9.2% quality dir;
planned Bg −27.2% / −29.5%), raw unchanged because today's format keeps 2-
or 4-byte offsets. No detectable accuracy cost on the fresh sample (33
against 28 points, p = 0.61). Worth adding as its own step if download size
matters.

**Query cost:** decoded admin vertices per query rise about 3× (counting
reader, fresh points): admin-minimal photo-like 37.4k → 115.8k, L2 border
34.3k → 131.9k; recommended photo-like 51.4k → 129.9k. The reader decodes
every candidate polygon (about 118 per query in admin-minimal, 212 in
recommended), and with flat tolerances those include large country and
state rings: Russia's L2 ring goes from 3,740 to 26,293 vertices. The µs
timing is unverified: the study measured 75 → 167 µs (admin-minimal) and
289 → 357 µs (recommended) per query, but the verifier could not re-run it
because the timing lock was busy. Reader fixes (decode a polygon only when
it can still win, a bbox max in the header) are untested.

**Rejected or left for later:**
- **Chain reader rejected** (candidate g: country and state from the finest
  polygon's parent chain, the way a full-mode query resolves them). It only
  matches Nominatim where polygons nest, so it needs a build-time nesting
  flag (`nestcheck`: 97.6% of L8 chains nest), reader and builder changes
  and an `admin_parents.bin` (+1.88 MB raw, +0.87 MB zstd), and still
  changes 16.2% of L2 border answers. Flat tolerance beats it with no format
  change. Study numbers, not re-measured.
- **Shared arcs, estimated −26–33% raw** (`study/topo/`, each shared border
  stored once): 18.42M stored vertices at q2.5 tolerances and 14.34M at
  flat 75 m, against 38.7M and 33.9M stored per polygon. The coordinate and
  structure streams were measured, the arc offset table estimated; accuracy
  unverified. Needs a new format, reader, patcher and builder.

**Where the vertices go:** q2.5 spends 53.2% of admin-minimal's vertices on
L8 at 37.5 m while country and state borders sit at 250–1250 m. The
recommended scheme moves vertices from L8 (20.60M → 17.48M) to L2–L5
(3.59M → 7.14M).

### Admin-minimal (4 admin files: admin_vertices, admin_polygons, admin_cells, admin_entries)

MB = 10⁶ bytes, zstd 1.5.7 `-18 -T1`, Δ versus q2.5. "Planned" is
admin_vertices in layout Bg (`../boundary-precision`), every Bg file decoded
back byte-identical. Accuracy is % of the fresh 20k sample changed versus
the truth: 10k border points (level uniform over L2–L8, about 1,440 per
level) and 10k photo-like points.

| scheme | raw MB (Δ) | zstd MB (Δ) | planned zstd MB (Δ) | whole set raw / zstd Δ | photo-like any (city/state/country) | L2 country | L4 state | L8 any (city) | enclave pts right /3439 |
|---|---|---|---|---|---|---|---|---|---|
| q2.5 | 240.1 | 169.2 | 129.1 | 377.4 / 232.3 MB | 0.46 (0.24/0.19/0.10) | 34.11 | 14.84 | 4.38 (2.71) | 2738 |
| q2.5+cap | 241.9 (+0.8%) | 170.7 (+0.9%) | 130.3 (+0.9%) | +0.5% / +0.7% | 0.43 (0.23/0.16/0.10) | 34.04 | 14.70 | 4.38 (2.71) | 3301 |
| **75 m, L8 50 m + cap** | **241.9 (+0.8%)** | **169.8 (+0.4%)** | 127.4 (−1.3%) | +0.5% / +0.3% | 0.30 (0.24/0.10/0.02) | 3.97 | 4.04 | 4.03 (**3.96**) | 3380 |
| 75 m + cap | 226.4 (−5.7%) | 154.7 (−8.6%) | 116.7 (−9.6%) | −3.6% / −6.2% | 0.36 (0.31/0.10/0.02) | 3.97 | 4.04 | 5.56 (**5.49**) | 3380 |
| 75 m, L8 50 m + cap + 11 m grid | 241.7 (+0.7%) | 158.9 (−6.1%) | 94.0 (−27.2%) | +0.4% / −4.4% | 0.31 (0.26/0.11/0.02) | 3.90 | 3.83 | 3.89 (3.82) | 3382 |
| 75 m, L8 50 m, no cap | not measured | | | | 0.33 (0.27/0.11/0.02) | 3.97 | 4.18 | 4.03 (3.96) | 3363 |
| truth (uncapped, holes) | 1241.5 | 996.4 | | | 0 | 0 | 0 | 0 | 3438 |

### Recommended set's quality dir (admin_vertices, admin_polygons, postal_vertices, postal_polygons; L9+ at 37.5 m)

| scheme | raw MB (Δ) | zstd MB (Δ) | planned zstd MB (Δ) | whole set raw / zstd Δ | photo-like any (city) | border any | L8 any (city) | enclave pts right |
|---|---|---|---|---|---|---|---|---|
| q2.5 | 283.5 | 249.8 | 201.1 | 1112.9 / 578.7 MB | 0.53 (0.28) | 16.56 | 4.45 (2.71) | 2737 |
| q2.5+cap | 286.9 (+1.2%) | 252.5 (+1.1%) | 203.3 (+1.1%) | +0.3% / +0.5% | 0.50 (0.27) | 16.48 | 4.45 (2.71) | 3299 |
| **75 m, L8 50 m + cap** | **286.8 (+1.2%)** | **252.4 (+1.1%)** | 201.8 (+0.3%) | +0.3% / +0.5% | 0.34 (0.28) | 4.12 | 4.03 (3.96) | 3378 |
| 75 m + cap | 271.3 (−4.3%) | 237.9 (−4.8%) | 191.5 (−4.8%) | −1.1% / −2.1% | 0.39 (0.34) | 4.60 | 5.49 (5.42) | 3378 |
| 75 m, L8 50 m + cap + 11 m grid | 286.6 (+1.1%) | 226.8 (−9.2%) | 141.8 (−29.5%) | +0.3% / −4.0% | 0.34 (0.29) | 4.16 | 3.89 (3.82) | 3380 |

The study kept L9 and above at 37.5 m because at 75 m the sub-city fields
(suburb, neighbourhood and similar) changed on 0.46% of photo-like points
instead of 0.32% (study number, not re-measured).

### Enclave points right per grid

Identical in the study and the verifier.

| grid | q2.5 | q2.5+cap | 75 m, L8 50 m + cap | truth |
|---|---|---|---|---|
| Vatican | 209 | 340 | 340 | 350 |
| Campione | 161 | 357 | 357 | 362 |
| Büsingen | 288 | 401 | 428 | 436 |
| Baarle | 88 | 140 | 143 | 159 |
| Llívia | 442 | 465 | 490 | 496 |
| Jungholz | 281 | 340 | 348 | 357 |
| San Marino | 426 | 415 | 430 | 434 |
| Lesotho | 843 | 843 | 844 | 844 |

The cap is still needed: 75 m with L8 at 50 m and no cap gets 329 of 350
Vatican points right, 3363 in total.

### Not verified

- µs per query (see above), builder time and daily patch size under the new
  scheme.
- Any planet newer than 261006, whose China, Poland and Ukraine L2 polygons
  are broken (this affects every candidate equally).
- The chain reader and shared-arc numbers, and the other 30 candidates.
- Untested idea: keeping L7 at 50 m might win back part of the L7 city loss.

Open decisions: L8 at 50 m or 75 m (size against city accuracy); whether
about 3× the decoded vertices per query is acceptable or a reader fix comes
first; whether the arc format is worth doing later.

## Method

- **Dump:** `patches/tiers2-dump.patch` adds `GC_TIER_DUMP=<dir>` to
  `build_index.cpp`: the planet run writes its final admin arrays (polygons,
  vertices, OSM ids, parent ids). One cold planet build
  (`study/run_planet.sh`, 329 s).
- **Variants:** `study/tiervar.cpp` re-simplifies those arrays with the
  builder's own code paths (`split_rings` / `simplify_rings`,
  `simplify_polygon_epsilon`, `plan_polygon` / `pack_polygon`, the
  `write_cell_index` layout) and writes admin-minimal and quality-dir files
  in today's format; its header documents the scheme language. Controls are
  byte-identical: q2.5 8 of 8 files against the builder's own admin-minimal
  and quality/q2.5, q2.5+cap 8 of 8 against the 9b55d28 cap build, uncapped
  4 of 4. admin_cells and admin_entries are identical in every candidate (no
  polygon is ever dropped).
- **Truth:** uncapped geometry with holes (scheme `none`), read with the
  same reader.
- **Immich fields** (the mapper in `geocoding.repository.ts`): city = city
  ?? town ?? village ?? hamlet ?? municipality, state = state ?? province,
  country = country_code.
- **Study pools:** photo-like p200k (200k) and places (20k); 20k points
  within ±200 m of each of the L2, L4 and L8 borders; uniform 100k;
  random 5k; 13.8k enclave grid points scored against an osmium-area truth
  (8 grids summed); the 421 loc-to-city checks.
- **Verifier:** recompiled `tiervar` from source and rebuilt the four key
  schemes (32 of 32 files identical to the study's), queried them with a
  plain reader (`fix/admin-holes` 97c732c without the chain patch), and
  scored a fresh 20k sample with its own scorer: 10k border points from
  `verify/borders.cpp` (admin level uniform over L2–L8, an edge picked by
  length, ±200 m perpendicular, keyhole connector edges never sampled) and
  10k photo-like points from `verify/photo.py` (new seed). Paired
  comparisons use an exact two-sided sign test on discordant points
  (`verify/vpaired.py`). Sizes were re-measured from scratch
  (`verify/sizes.sh`, `vsizes.py`, `bg.sh`). Nominatim's `get_country_code`
  and `getNearFeatures` were read at 87eb582: both test full geometry with
  `ST_Covers`.

## Layout

- `study/` (the study's `/ssd/tiers2`):
  - tools: `tiervar.cpp`; `nestcheck.cpp` (per-polygon "nests in its
    parent" flags for the chain reader); `topo/topo_uncapped.cpp` (the arc
    estimator of `../boundary-precision/vertices` plus `TOPO_UNI=<m>`, one
    tolerance at every level) with `topo/run_topo.sh`.
  - build and candidates: `run_planet.sh` (cold planet build with the dump
    hook), `mkvar.sh` (one candidate's files and flat sets), `cand.sh`
    (mkvar + queries + compare + enclave grids), `batch1.sh`–`batch4.sh`
    (every candidate with its scheme string), `cleanup.sh`.
  - queries: `runq.sh` (`CHAIN=1` for the chain reader), `timeq.sh`
    (single-thread latency under the box's timing lock), `work.sh` (decodes
    and decoded vertices per query, counting reader).
  - scoring: `compare.py`, `score.py`, `grids.py`, `l2c.sh`, `sizes.py`
    (raw, zstd, Bg with round trip), `report.py` (Pareto tables),
    `nomcheck.py` (public Nominatim where two candidates disagree),
    `fields.py`, `diffs.py`, `ringsizes.py`, `polystats.py`.
- `verify/` (the verifier's `/ssd/tiers2-verify`): `mkv.sh`, `runq.sh`,
  `borders.cpp`, `photo.py`, `vscore.py`, `vdetail.py`, `vpaired.py`,
  `sizes.sh`, `vsizes.py`, `bg.sh`, `nomv.py`, `timeq.sh` (never got the
  lock).
- `patches/` (`git format-patch`, one commit each, base `fix/admin-holes`
  97c732c):
  - `tiers2-dump.patch` (77c010a): the builder's `GC_TIER_DUMP` hook.
  - `tiers2-chain-reader.patch` (e9d2928): the reader's `GC_CHAIN_ADMIN=1`
    chain mode (candidate g); without the variable the reader is unchanged.
  - `tiers2-count-reader.patch` (3fc18c9, on top of the chain reader):
    admin decode counters that `grid_query` prints to stderr at exit.

The scripts hard-code the box paths (`/ssd/tiers2`, `/ssd/tiers2-verify`,
and `/ssd/shrink`, `/ssd/bugs`, `/ssd/transport-verify` for shared tools,
point pools and the enclave truth) and run Python from `/ssd/shrink/venv`
(numpy).

## Rebuild and run

```sh
X=$PWD/geocoder/experiments/boundary-tiers   # this directory
R=/path/to/geoshenanigans                    # a second checkout for builder and reader
T=/ssd/tiers2; V=/ssd/tiers2-verify
mkdir -p $T/topo $V/bin
cp $X/study/*.sh $X/study/*.py $T/ && cp $X/study/topo/run_topo.sh $T/topo/
cp $X/verify/*.sh $X/verify/*.py $V/

# builder with the dump hook, then one cold planet build that writes $T/dump
git -C $R checkout -b tiers2-dump 97c732c && git -C $R am $X/patches/tiers2-dump.patch
# build build-index with geocoder/builder/tools/ci_build_tools.sh
bash $T/run_planet.sh dump /path/to/build-index $T/dump

# tools, against the builder headers at 97c732c
I=$R/geocoder/builder/src
g++ -O3 -march=native -std=c++17 -fopenmp -I$I $X/study/tiervar.cpp -o $T/tiervar
g++ -O3 -march=native -std=c++17 -fopenmp -I$I $X/study/nestcheck.cpp -o $T/nestcheck
g++ -O3 -march=native -std=c++17 -fopenmp -I$I $X/study/topo/topo_uncapped.cpp -o $T/topo/topo_uncapped
g++ -O3 -march=native -std=c++17 -I$I $X/verify/borders.cpp -o $V/bin/borders
cp $T/tiervar $V/bin/tiervar

# readers: chain reader -> $T/target, counting reader -> $T/target-c, plain reader -> $V/target
git -C $R checkout -b tiers2-chain-reader 97c732c && git -C $R am $X/patches/tiers2-chain-reader.patch
CARGO_TARGET_DIR=$T/target cargo build --release --manifest-path $R/geocoder/server/Cargo.toml --example grid_query
git -C $R am $X/patches/tiers2-count-reader.patch
CARGO_TARGET_DIR=$T/target-c cargo build --release --manifest-path $R/geocoder/server/Cargo.toml --example grid_query
git -C $R checkout --detach 97c732c
CARGO_TARGET_DIR=$V/target cargo build --release --manifest-path $R/geocoder/server/Cargo.toml --example grid_query
```

The study expects `$T/sets/base/{smallest,recommended}` (symlinks to the
dump build's admin-minimal and recommended outputs) and the point pools in
`$T/pts` (copied from `/ssd/bugs/pts`). Truth first, then any candidate
(about 10 s per variant; scheme strings in `batch1.sh`–`batch4.sh`):

```sh
cd $T
./mkvar.sh unc none
for s in smallest recommended; do ./runq.sh unc_$s sets/unc/$s; done
R9='t9=37.5;t10=37.5;t11=37.5;t12=37.5;t13=37.5;t14=37.5;t15=37.5'
./cand.sh q25 'scale=2.5'
./cand.sh cap10 'scale=2.5;cap=0.1'
./cand.sh uni75t8rc10 "uni=75;t8=50;$R9;cap=0.1"
./cand.sh uni75rc10 "uni=75;$R9;cap=0.1"
PY=/ssd/shrink/venv/bin/python
$PY -I sizes.py q25 cap10 uni75t8rc10 uni75rc10
for s in smallest recommended; do $PY -I score.py $s q25 cap10 uni75t8rc10 uni75rc10; done
./l2c.sh q25 cap10 uni75t8rc10 uni75rc10
$PY -I report.py q25 cap10 uni75t8rc10 uni75rc10 > cmp/report.md
./work.sh smallest q25 uni75t8rc10
./timeq.sh smallest q25 uni75t8rc10          # takes the box's timing lock
```

The verifier (fresh 20k sample = the 10k border points, then the 10k
photo-like points):

```sh
cd $V; mkdir -p pts cmp tmp
bin/borders $T/dump 10000 <seed> > pts/border10k.meta
$PY -I photo.py 10000 <seed> > pts/photo10k.meta
cat <(cut -d' ' -f1,2 pts/border10k.meta) <(cut -d' ' -f1,2 pts/photo10k.meta) > pts/fresh20k.txt
cp $T/pts/grids.txt pts/
./mkv.sh unc none none
./mkv.sh q25 'scale=2.5' 'scale=2.5'
./mkv.sh cap10 'scale=2.5;cap=0.1' 'scale=2.5;cap=0.1'
./mkv.sh rec 'uni=75;t8=50;cap=0.1' "uni=75;t8=50;$R9;cap=0.1"
./mkv.sh small 'uni=75;cap=0.1' "uni=75;$R9;cap=0.1"
./runq.sh
$PY -I vscore.py && $PY -I vdetail.py
$PY -I vpaired.py smallest cap10 rec
./sizes.sh && $PY -I vsizes.py && ./bg.sh
$PY -I nomv.py smallest cap10 rec 10
```

## Data and tool versions

- OSM planet PBF of 2026-10-06 (`planet-261006.osm.pbf`) with the
  nightly's inputs (TIGER, Wikidata sitelinks, GB postcodes); cold build of
  `fix/admin-holes` 97c732c plus the dump hook (77c010a), 96 GiB scope
  without swap like the nightly runner. q2.5+cap control: the 9b55d28 cap
  build (`/ssd/bugs/build/out-cap`).
- Point pools from `/ssd/bugs/pts`: p200k from
  `../boundary-precision/places-strings-index/gen_points2.py`, L2/L4/L8
  from `../boundary-precision/grid/gen_borders.py`, places from
  `../boundary-precision/vertices/gen_places.py`, uniform100k and random5k.
  Enclave grids and their osmium-area truth: `/ssd/bugs/grids`
  (`score_grids_lib.py` in `/ssd/bugs/probe`). Loc-to-city: the 421 checks
  scored by `../loc2city-eval/verify/l2c_eval.py` (mapper B) with Natural
  Earth `ne_10m_admin_0_countries`.
- Layout Bg: `../boundary-precision/grid/bgcodec.cpp`
  (`/ssd/shrink/grid/bgcodec`). zstd 1.5.7 `-18 -T1` for every size.
- `grid_query` from 97c732c, with the chain and counter patches for the
  study's chain and work runs.
- Public Nominatim `/reverse` (jsonv2, addressdetails, zoom 10); Nominatim
  source at 87eb582 for the containment rules.
