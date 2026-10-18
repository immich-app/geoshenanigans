# Precision tiers: Original and Maximum, streets, search names

"Maximum quality" has not been the original OSM data since April: building
outlines are capped at 20 points (closed ways only), outlines spanning
73-730 km sit on an 11 m grid, and the builder rounds all geometry to f32 at
parse. This study measures what a true-precision "Original" tier and an
uncapped "Maximum" tier cost, which street / address precision to ship, and
how big a place-search name file is. Run 2026-10-14 on the box
(`/ssd/precision`) from an instrumented cold planet build, re-verified the
same day (`verify/`). The builder instrumentation (exact-coordinate dumps,
cap counters, name and link dumps) is on `chore/precision-instrumentation`.

## Results (planet-261006, verified 2026-10-14)

**Build with the cap off:** +20.1M building vertices (2.8%), +161.8 MB of
output (42 files differ, all address-related), deterministic, no measurable memory
change. Highest max RSS 80.52 GiB: 3.5 GiB under the builder's 84 GiB budget,
15.5 GiB under the runner's 96 GiB. 19,343 relation building outlines were
never capped (1,638 of them over 20 vertices today). Precision is first lost
at the `NodeCoord` f32 casts (PBF → int32 is exact): f32 street error p50
0.16 m, max 0.85 m. The exact dumps equal OSM's 1e-7 integers (17,183 sampled
coordinates checked with osmium).

**Tier sizes** (zstd 1.5.7 `-18 -T16`; every encoding round-trips, checked by
independent decoders):

| file | today | Maximum | Original |
|---|---:|---:|---:|
| boundaries | 1,388.9 MB (`admin_vertices`), 757.9 in layout B | 677.9 MB (0.1 m DP, 1e-5 grid, PB7) | 1,393.7 MB exact, no simplification (1,306.3 with 0.1 m DP) |
| buildings | 1,118.4 MB (abs2) | 1,429.7 MB (uncapped, 1e-6 grid, AB7) | 2,023.0 MB (uncapped, exact) |

Planet max totals in the new formats (computed): Maximum ≈6,628 MiB with
today's f32 streets; Original ≈8,978 MiB (no simplification, on a 1.1 m base
plus refinement), ≈8,895 with 0.1 m DP.

**Streets and addresses** (download MB, answers differing from exact, cold
read per street way):

| option | MB | answers differ | cold read |
|---|---:|---:|---:|
| today's f32 | 2,461.3 | 1.70% | 169 µs |
| 1.1 m base | 2,026.7 | 2.91% | 315 µs |
| **11 cm base** | 2,913.7 | **0.24%** | — |
| exact: 1.1 m base + refinement | 3,601.7 | 0 | 466 µs |
| exact: separate file | 3,617.8 | 0 | 316 µs |

A refinement file costs the same as a separate exact file but reads slower
cold (+1.8 page faults per way), so the separate file wins.

**Search names** (zstd `-18 -T1`): places base 20.13 MB, all languages
26.50 MB; with major POIs 26.00 / 33.98 MB. 20.5% of places carry
`name:en`, 23% two or more languages; 1.32 distinct strings per place (base),
1.67 (all).

**Decisions taken (2026-10-13 and 10-18):** the top tier ships as an entirely separate
exact file with no simplification at all; the shared street / address base
for every preset uses the 11 cm grid; Maximum keeps the 11 cm building grid
but drops the 20-point cap. The lower boundary tiers are a separate study.

## Layout

- `build/`: `run_planet.sh` (cold planet build in a 96 GiB no-swap scope
  like the nightly runner, memory sampled three ways), `chain.sh`,
  `post.sh`, `repeat.sh`, `oc_check.sh`, `summarise.py`, `f32_error.py`,
  `outline_grids.py`; `DUMPS.md` and `NAME_DUMPS.md` describe the
  instrumented builder's dump formats.
- `tiers/`: `tiers.cpp` (snap, PB7 boundary and AB7 building encoders and
  decoders; its header documents every command and format), the reader
  tests (`gen_borders.py`, `gen_bldg_pts.py`, `near_mask.py`, `runq.sh`,
  `compare.py`, `bldg_dist.py`), `admin_run.sh`, `bldg_run.sh`,
  `bldg_sizes.sh`, `zs.sh`, `check_today_f32.py`, `level_counts.py`;
  `NOTES.md` is the step's own guide to its configs and pools.
- `streets/`: `prec.cpp` (exact values in sorted order, e5 / e7 packed
  ways, refinements, address points), `cells.cpp` (level-17 cell effects
  of precision; links S2), `bench.cpp` (warm / cold read benchmark),
  `gen_q.py`, `cmp_q.py`, `runq.sh`, `lib_f64_sidecars.patch` (server patch
  for `grid_query_f64`, which answers from exact sidecars).
- `names/`: `prep_osm.py`, `places.py`, `poi_strings.py`, `extra.py`,
  `geonames.py` (cities500 vs our names), `poi_unnamed_check.py`,
  `compress.sh`, `compress_extra.sh`, `run_links.sh`.
- `verify/`: independent decoders `vtiers.cpp` / `vstreets.cpp`, rebuild
  runs `vrun.sh` / `vchain.sh`, `cap_recount.py`, `cap_kind.py`,
  `pbf_sample.py`, `names_check.py`, `names_big.py`, `pn_count.py`,
  `f32err.py`, `vbench.sh`, `cmp_base_nocap.sh`, `gc_run.sh`.

The scripts hard-code the box paths (`/ssd/precision/...`).

## Rebuild and run

```sh
I=geocoder/builder/src
g++ -O2 -march=native -std=c++17 -I$I tiers/tiers.cpp -o tiers
g++ -O2 -march=native -std=c++17 -fopenmp streets/prec.cpp -o prec
g++ -O2 -march=native -std=c++17 streets/bench.cpp -o bench
g++ -O2 -std=c++17 -fopenmp verify/vtiers.cpp -o vtiers
g++ -O2 -std=c++17 -fopenmp verify/vstreets.cpp -o vstreets
bash build/run_planet.sh nocap /path/to/build-index --building-outline-cap 0 --exact-dumps dumps/raw --name-dumps names
```

`cells.cpp` links the builder's S2 (`builder/tools/build_s2.sh` prefix).

## Data and tool versions

- OSM planet PBF of 2026-10-06 (`planet-261006.osm.pbf`) with the
  nightly's inputs (TIGER, Wikidata sitelinks, GB postcodes), cold build
  without `--prev-output` so the dumps align record for record.
- `build-index` from `chore/precision-instrumentation` (3633970; the link
  dumps for the names step are 427c0c2), built with
  `builder/tools/ci_build_tools.sh`. The 2026-10-13 published build is the
  baseline for the new-format totals (`../format-codecs`).
- `grid_query` from the same branch (`geocoder/server/examples`), and
  `grid_query_f64` with `streets/lib_f64_sidecars.patch` applied.
- GeoNames `cities500` and `alternateNamesV2` for the names comparison.
