# Reader cell scan: exact admin lookups, faster

Can the reader's admin lookup be made fast without changing a single answer?
`feat/place-search-prod`'s 66a5ad0 ("find_admin reads only the query cell")
made `find_admin` much faster by dropping the scan of the 8 neighbour cells,
but it loses polygons: the answer is empty or wrong at points near long
borders (Egypt's 22° N line, Algeria). This track diagnoses why, restores
the neighbour scan and makes the exact scan cheap, then proves the answers
are identical to main's on a million lines and times every step. Run
2026-10-19 on the box (`/ssd/reader`, two agents: the second resumed after a
cutoff), re-checked the same day by an independent verifier
(`/ssd/verify6`, `verify/`) with its own builds and fresh points.

The code is branch `perf/reader-cell-scan` (8 commits on
`feat/place-search-prod` 74a4a88, box repo `/ssd/reader/repo`, not yet
pushed). It is not in this research branch.

Where the study and the verifier differ, the verifier's number is given.
Tags: **M** = measured on the box (by this track's agents or the verifier),
**C** = computed from measured values, **E** = estimated.

## Why 66a5ad0 loses polygons (M)

The builder lists each ring in the cells of an S2 covering, and S2 treats
every ring edge as a **great-circle arc** (`cover_polygon` in
`s2_helpers.cpp`, run on the builder's own ring). The reader, like
Nominatim's PostGIS geometry, tests **straight lat/lng edges**.

A long edge that does not run along a meridian bows towards the pole as a
great circle. A point inside that bow is inside the polygon for the reader
but outside the S2 polygon. When the point's own level-10 cell does not
touch the S2 polygon, the cell does not list the polygon. Main still finds
it through a neighbour cell; 66a5ad0 does not.

| case (planet-261018b) | ring | own level-10 cell | edge responsible | straight edge vs great circle at the point | result |
|---|---|---|---|---|---|
| Egypt, (22.0019170, 27.5734270) | Egypt 284843 (L2) and New Valley 598834 (L4) | 0x14193f…, not in the covering (13,313 cells); listed by neighbours 1, 4, 5, 7, 8 | (22.0, 31.39888)–(22.0, 25.0), 659.7 km along 22° N | 22.0000° N vs 22.0299° N; the point is 3.1 km south of the arc, the bow 3.3 km deep at most | 66a5ad0 returns an empty address; main and Nominatim say New Valley, Egypt |
| Algeria, (24.102539, −3.445324) | Algeria 60251 and 3 Bordj Badji Mokhtar rings | 0x0de3f5…, not in the covering (31,586 cells) | (24.87, −4.64)–(21.91, −0.06), 571.6 km, diagonal | 24.1016° N vs 24.1273° N; bow 2.9 km deep | country lost |

- Checked with `scan_diag` on the actual cell index and `lune_diag`, which
  reruns the builder's S2 covering on the ring as stored
  (`classify_lost.sh` runs it over every lost ring).
- On the fresh pools, 66a5ad0 differs from main on 332 of 50k lines in each
  of 8 sets, all in the 10k points beside parallel-following borders.
- Bows can be deeper than one cell: the deepest among edges of 50 km or
  more that follow a parallel is 34.7 km, and a cell is about 7–10 km wide.
  So the point's position inside its cell cannot rule a neighbour out; only
  per-ring data could.
- **Main misses some of these too:** on planet-261018b recommended, main's
  3×3 scan misses Kitikmeot Region (L5, 2 cells away) on 57 of 10k
  parallel points, and the "Polynesian Triangle" place area on 2 of 30k
  general points. Nominatim does return Kitikmeot at (67.0220238,
  −98.5481604). Matching main exactly keeps both misses.

The general lesson: any index built from an S2 covering of geometry that a
reader tests with planar edges needs either a neighbour margin as deep as
the deepest bow, or a covering of the planar ring (for example long edges
split before covering).

## Results (verified 2026-10-19)

- **Answers identical to main (M):** 0 of 1,000,000 lines differ: 10 sets
  (o-* planet-261006 minimal / recommended / max; b-* planet-261018b
  smallest / recommended / max at q2.5; u-* `../boundary-tiers`' 75/50 +
  cap; r1-* `../admin-tolerances`' r1) × 50k fresh points × `query_json`
  and `find_admin`. The straddle commit was also checked on its own: 0
  differences on all 10 sets. The verifier: 0 of 180,000 lines (3 sets × 2
  calls × 30k fresh points, including 9k beside parallel-following borders
  and 9k within ±200 m of borders); as a check the pool can catch the bug,
  66a5ad0 differs on 310–312 lines per set.
- **Speed (M):** `query_json`, the call the server answers with, is faster
  than 66a5ad0 warm and the same cold. The verifier's tip / 66a5ad0 ratios:
  `query_json` warm 0.66 / 0.91 / 0.78 / 0.93, cold 1.00–1.01;
  `find_admin` warm 1.20 / 1.45 / 1.03 / 0.71; **`find_admin` cold
  1.25–1.46×** (the track reported 1.14–1.42×).
- **Why `find_admin` alone stays slower (M):** per lookup the tip reads
  about 190 polygon records against about 56 for 66a5ad0. The extra ones
  are the neighbour cells' entries, which exactness requires. Prefetching
  hides most of that when the files are in memory; cold, the pages still
  have to be read.
- **Where the speed came from (M, b-recommended photo-like, `find_admin`
  warm):** 25.4 µs at 1f98a23, 23.3 with the straddle test, 19.7 with
  prefetch; 66a5ad0 takes 13.8, main 108.2.
- **Tests (M):** 57 server lib tests pass, 14 of them new (5 vertex
  encodings in `point_in_encoded_polygon`, the header-bounds early-out,
  `grid_threshold`, the Egypt/Sudan neighbour-cell case, interior flags,
  equal-area visiting order, current and municipality boundaries, the
  shared scan, the gallop lookup, the PIP memo, own-cell ordering,
  prefetch); the verifier ran 57 + 27. Mutation checks: an off-by-one in
  `grid_threshold` is caught; flipping the straddle compare from `>` to
  `>=` is not (it only matters when a vertex lies exactly on the ray, where
  the ray cast gives the same answer either way); the million-line
  comparison covers that path.

### Timing (M, track; mean / p99 µs per lookup)

Single thread on core 2, nice −20, under the timing lock; warm = fastest of
3 passes per point, cold = the set's files evicted first. Full table on the
box: `/ssd/reader/timing3/TABLE.md`.

| set, pool | call | cache | main | 8dbb58d | 66a5ad0 | tip | tip / 66a5ad0 |
|---|---|---|---|---|---|---|---|
| b-smallest photo | query_json | warm | 87.5 / 228 | 38.0 / 111 | 30.1 / 88 | 20.1 / 49 | 0.67 |
| | query_json | cold | 754 / 7631 | 682 / 7268 | 665 / 7152 | 664 / 7175 | 1.00 |
| | find_admin | warm | 69.2 / 205 | 19.1 / 57 | 9.0 / 25 | 10.7 / 27 | 1.20 |
| | find_admin | cold | 545 / 4799 | 475 / 4284 | 399 / 2638 | 466 / 4217 | 1.17 |
| b-recommended photo | query_json | warm | 218.3 / 1521 | 156.3 / 1133 | 130.3 / 867 | 117.4 / 853 | 0.90 |
| | query_json | cold | 2039 / 27286 | 1946 / 25554 | 1901 / 25042 | 1914 / 25180 | 1.01 |
| | find_admin | warm | 108.2 / 930 | 45.2 / 615 | 13.8 / 102 | 19.7 / 101 | 1.43 |
| | find_admin | cold | 795 / 9325 | 725 / 8326 | 594 / 4803 | 704 / 8312 | 1.19 |
| b-recommended border | query_json | warm | 153.0 / 1340 | 88.8 / 870 | 56.9 / 337 | 44.1 / 309 | 0.78 |
| | find_admin | warm | 112.9 / 1222 | 47.1 / 808 | 13.3 / 119 | 13.7 / 80 | 1.03 |
| b-max photo5k | query_json | warm | 3752 / 21574 | 2372 / 21020 | 2154 / 19503 | 1938 / 18271 | 0.90 |
| | find_admin | warm | 1765 / 6597 | 332 / 2870 | 187 / 630 | 133 / 434 | 0.71 |
| | find_admin | cold | 5776 / 35346 | 3688 / 26015 | 2314 / 14172 | 3219 / 23503 | 1.39 |
| r1-smallest photo | query_json | warm | 259.6 / 817 | 56.7 / 152 | 46.8 / 110 | 30.3 / 65 | 0.65 |
| | find_admin | warm | 235.8 / 778 | 37.7 / 127 | 25.3 / 70 | 21.1 / 50 | 0.83 |
| r1-recommended photo | query_json | warm | 365.1 / 1611 | 174.9 / 1155 | 148.1 / 894 | 128.4 / 878 | 0.87 |
| | find_admin | warm | 254.6 / 1102 | 63.8 / 635 | 30.2 / 123 | 30.3 / 116 | 1.00 |
| u-recommended photo | query_json | warm | 337.7 / 1653 | 173.7 / 1183 | 142.9 / 883 | 126.3 / 874 | 0.88 |
| | find_admin | warm | 230.8 / 1127 | 65.0 / 692 | 27.6 / 127 | 28.9 / 119 | 1.05 |

### The commits (all answer-identical to main)

| commit | change |
|---|---|
| 3e44536 | test: `point_in_encoded_polygon` against the decoded ray cast |
| 83dfed3 | fix: `find_admin` scans the neighbour cells again (reverts 66a5ad0) |
| 49ad63e | perf: skip rings whose 10-byte header bounds already exclude the point |
| 188fd0a | perf: one shared 3×3 scan per reverse lookup, plus a memo of point-in-polygon results |
| 374d778 | perf: find neighbour cells by galloping from the first lookup (about 1–3%) |
| 1f98a23 | perf: read each cell entry and polygon record once per lookup |
| 4e8518b | perf: integer straddle test (the point-in-polygon part of `../admin-arcs`' 626a568, applied by hand) |
| 9b16065 | perf: prefetch the scan's lists, records and ring headers; x86_64 only, a no-op elsewhere |

## Decision status

The verifier found all 8 commits safe to push as they are (answers
identical to main, tests pass). Open:
1. **Builder fix, needs a decision:** if the builder covered each tier's
   stored straight-edge ring (for example by splitting long edges before
   covering), the reader could scan only its own cell and drop the
   neighbour scan, and main's misses beyond 3×3 such as Kitikmeot would be
   fixed (Nominatim agrees). But it changes answers compared with main and
   changes the cell files. Not done (E).
2. Prefetch is x86_64 only; the aarch64 path gets no hint and is untested.
3. Keep 374d778 (gallop, about 1–3%) or drop it for a smaller diff?
4. Is `find_admin` cold at 1.25–1.46× 66a5ad0 acceptable, given
   `query_json` is the same cold and faster warm?

## Method

- **Pools** (`gen_pts.py`, seed 20261020, none from earlier tracks):
  general30k (10k photo-like from GeoNames cities500, 10k uniform on land,
  10k uniform on the sphere), parallel10k (beside borders that follow a
  parallel, sampled across the lune between the straight and the
  great-circle edge), border10k (±200 m of L2 / L4 / L8 borders).
- **Builds:** `build.sh` builds `grid_query`, `admin_dump` (`find_admin`
  answers, one line per point) and `rg_bench` (latency) from a `git
  archive` of each revision, each in its own target dir; `build_count.sh`
  builds `rg_count` on a copy patched by `instrument.py` (cell lookups,
  records read, rings and vertices decoded per lookup).
- **Equivalence:** `eq_run.sh`, `eq_run_low.sh`, `eq_run3.sh` (all 10
  sets, both calls), `eq_cmp.py` (differing lines per set, call and pool),
  `diff_fields.py` (which fields differ).
- **Diagnosis:** `scan_diag.rs` (polygons listed only by neighbour cells
  that contain the point; `--lost` lists them), `dump_ring.py` (a ring as
  the reader decodes it), `lune_diag.cpp` (the builder's S2 covering at
  level 10 on that ring, and the lune of each edge at the point; links
  against s2geometry), `classify_lost.sh`.
- **Timing:** `timing.sh`, `timing2.sh`, `timing3.sh` (three windows under
  the timing lock, readers interleaved per set, pool and call) with
  `timing_table.py`, `timing_table5.py`, `timing_table3.py`.
- **Verifier** (`verify/`): `rd_eq.sh` (main, 66a5ad0 and the tip on the
  verifier's rd30k: 6k photo-like, 3k land, 3k sphere, 9k beside
  parallel-following borders, 9k within ±200 m of L2/L4/L8 borders) and
  `rd_time.sh` (`rg_bench` warm and cold). Its binaries come from
  `../nominatim-parity/verify/build_all.sh`, its pool from
  `../nominatim-parity/verify/gen_pts6.py`, its tests from
  `../nominatim-parity/verify/tests6.sh`.

## Layout

- `study/`: the track's `/ssd/reader/scripts`: the server examples
  `admin_dump.rs` and `rg_bench.rs` (copied into `geocoder/server/examples/`
  by `build.sh`), `rg_count.rs` (by `build_count.sh`) and `scan_diag.rs`
  (built the same way by hand), `lune_diag.cpp`, and the scripts above.
- `verify/`: the reader part of the verifier's `/ssd/verify6/scripts`.

Left out: `patch_prefetch.py`, `patch_rings.py` and `splice_consts.py`
(edit scripts whose results are commits 9b16065 and 1f98a23 on the
branch), `build.sh.bak` (identical to `build.sh`), and the local
`repo/lib.rs` / `lib.rs.orig` source snapshots (269 KB and 237 KB, over the
200 KB limit, and copies of the branch).

The scripts hard-code the box paths (`/ssd/reader`, `/ssd/verify6`, and the
sets in `/ssd/search-prod`, `/ssd/bugs2`, `/ssd/tiers2`, `/ssd/tiers3`) and
run Python from `/ssd/shrink/venv` (numpy).

## Rebuild and run

```sh
X=$PWD/geocoder/experiments/reader-cell-scan
R=/ssd/reader
mkdir -p $R/scripts $R/bin $R/pts && cp $X/study/* $R/scripts/
# $R/repo: a clone with feat/place-search-prod and perf/reader-cell-scan
/ssd/shrink/venv/bin/python -I $R/scripts/gen_pts.py $R/pts
$R/scripts/build.sh main:f48ddc4 pip2:8dbb58d head:66a5ad0 tip:perf/reader-cell-scan
$R/scripts/build_count.sh head:66a5ad0 tip:perf/reader-cell-scan
$R/scripts/eq_run3.sh main tip && /ssd/shrink/venv/bin/python -I $R/scripts/eq_cmp.py main tip
$R/scripts/timing3.sh && /ssd/shrink/venv/bin/python -I $R/scripts/timing_table3.py $R/timing3

# diagnosis of a lost polygon
<scan_diag> <set dir> --lost < $R/pts/parallel10k.txt > $R/diag/lost.txt
$R/scripts/classify_lost.sh $R/diag/lost.txt <set dir>     # needs $R/bin/lune_diag
```

`scan_diag` is a server example built by hand; `lune_diag` is compiled
against s2geometry as the builder links it. Neither build line was kept.

## Box paths

- `/ssd/reader`: binaries `bin/{grid_query,admin_dump,rg_bench}.<name>`
  (`bin/REVS` maps names to commits), pools `pts/`, answers `out/`,
  diagnosis `diag/` (`lost.*`, `wide.*`, `ringB.*`), counters `count/`,
  timing `timing/`, `timing2/`, `timing3/`, logs `logs/`.
- Repo `/ssd/reader/repo`, branch `perf/reader-cell-scan` (tip 9b16065).
- Verifier: `/ssd/verify6/rd/` (answers and timing), `pts/rd30k`.

## Data and tool versions

- Sets: planet-261006 (`/ssd/search-prod/rg/sets-orig`), planet-261018b
  base build (`/ssd/bugs2/sets/base`), `../boundary-tiers` uni75t8rc10,
  `../admin-tolerances` r1 (`/ssd/tiers3/setsD1/r1`).
- Readers: main f48ddc4, 8dbb58d (pip2), 66a5ad0 (head), the branch's
  commits; GeoNames cities500 and Natural Earth 10m countries for the pools.
