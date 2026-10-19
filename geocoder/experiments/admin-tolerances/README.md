# Admin tolerances: per-level metre tolerances (r1 / sr1)

Which simplification tolerance should each admin level get in admin-minimal
and in the recommended set's quality dir (quality/q2.5), at the size of
q2.5 plus the ring cap or smaller? `../boundary-tiers` found that one flat
tolerance (75 m, L8 50 m) fixes country and state answers near borders but
makes city answers worse near L7/L8 borders. This study sweeps per-level
tolerances with a fast evaluator, then scores the best candidates with the
real reader on two planets. Run 2026-10-18/19 on the box (`/ssd/tiers3`),
re-checked 2026-10-19 by an independent verifier (`/ssd/verify5`,
`verify/`) with its own binaries built from the committed sources, its own
samplers and scorer, and a planet build of the committed builder.

Where the study and the verifier differ, the verifier's number is given.
Tags: **M** = measured on the box, **C** = computed from measured values,
**E** = estimated by a model. MB = 10⁶ bytes, zstd 1.5.7 `-18 -T1`. "Wrong"
means the Immich field (city, state or country) differs from the truth,
which is uncapped geometry with holes read by the same reader. B*n* = points
within ±200 m of level-*n* borders.

## Schemes (metres; every scheme except q2.5 has the ring cap 0.1×√area)

| scheme | measured in | L2 | L3 | L4 | L5 | L6 | L7 | L8 | named city/town/village | L9 | L10 | L11+ |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| q2.5 / q2.5+cap (today) | degrees | 1250 | 500 | 250 | 125 | 75 | 50 | 37.5 | – | 37.5 | 37.5 | 37.5 |
| flat, L8 50 m (f50, `../boundary-tiers`) | degrees | 75 | 75 | 75 | 75 | 75 | 75 | 50 | – | 37.5 | 37.5 | 37.5 |
| flat 75 m (f75) | degrees | 75 | 75 | 75 | 75 | 75 | 75 | 75 | – | 37.5 | 37.5 | 37.5 |
| **r1 (default)** | metres | 82 | 400 | 90 | 175 | 175 | 54 | 45 | ≤50 | 42 | 46 | 47 |
| **sr1 (size-first)** | metres | 90 | 400 | 100 | 200 | 200 | 82 | 66 | ≤66 | 42 | 46 | 47 |

r1 has three parts:
1. **A tolerance per level.** Country and state get about the city
   tolerance; L3, L5 and L6 get coarser, because a pick change there changes
   an Immich field only 10–18% of the time on border points, against
   89–100% at L2, L4 and L8 (M, table under Method).
2. **Metres in every direction.** The builder converts metres to degrees as
   a longitude distance and compares in raw degrees, so north–south error
   can grow by 1/cos(latitude), 1.6× at 50° N. r1 simplifies in a locally
   isotropic frame. Worth 5–8% less error at the same bytes (M).
3. **A city rule.** A polygon the reader names as a city, town or village
   keeps ≤50 m whatever its level. China needs it: its Immich city is the
   level-6 county-level city. Without the rule China's city errors double
   (photo 0.08 → 0.17%, B8 1.88 → 3.05%, M).

`tiervar3` scheme strings (quality dir: append `;t9=42;t10=46;t11=47;t15=47`):
- r1 `t2=82;t3=400;t4=90;t5=175;t6=175;t7=54;t8=45;cap=0.1;iso=1;cityovr=50`
- sr1 `t2=90;t3=400;t4=100;t5=200;t6=200;t7=82;t8=66;cap=0.1;iso=1;cityovr=66`

## Results (planet-261018b, verified 2026-10-19)

Admin-minimal unless noted. Base q2.5+cap: admin-minimal 243.2 / 171.1 MB
raw / zstd, quality dir 287.4 / 253.2 MB (M).

- **Size (M):** r1 −0.31% raw / −0.55% zstd (planned layout Bg −0.7%);
  quality dir −0.54% / −0.56%. sr1 −11.10% / −15.25%. f75 −6.30% / −9.23%.
- **Country, B2 (M):** 35.69% → 4.22% wrong (fresh 20k: 36.65 → 3.55).
  China, Poland and Ukraine (broken in planet-261006) now have truth points:
  4.1% (n = 559), 4.6% (n = 65) and 5.6% (n = 160), against 42.4, 36.9 and
  42.5% for q2.5+cap.
- **State, B4 (M):** 15.2% → 4.7% against q2.5+cap (study). r1 is worse
  than f50: 4.80% against 4.33% (p = 1e-8). Most of r1's extra state errors
  are island coastlines: French Polynesia 300 and Greece 156 of 1,212. On
  Greek islands live Nominatim gives r1's answer (14 of 16 points).
- **City, r1 vs q2.5+cap (M, exact McNemar):**
  - B7: 3.09 → 2.66% (p = 1e-9).
  - B8: 2.37 → 2.14% (p = 0.001). The study had called this no difference
    (p = 0.27). The fresh 1.4k-point slice is not significant (2.81 → 2.53,
    p = 0.39).
  - B6: 2.14 → 1.93% (p = 0.023). The study's "slightly worse, p = 0.08"
    did not reproduce.
  - Photo-like: 0.273 → 0.233% (p = 0.001). Address points: 0.220 →
    0.190% (p = 0.07).
- **City, r1 vs f50 (M):** better at B7 (p = 6e-49), B8 (p = 4e-29), photo
  (p = 2e-6) and address points (p = 0.004); B6 not significant (p = 0.07).
- **Enclaves (M):** 3386 of 3439 grid points right (q2.5+cap 3301, f50
  3380). Cieszyn 231 → 346, Dandong 880 → 900, San Marino 859 → 895,
  Słubice 521 of 522. **Regression:** r1 gets the state wrong on 120 Baarle
  grid points (97 of them Flanders → Antwerp), against 33 for q2.5+cap.
- **Planet build (M):** the committed builder (bdac8cd) builds
  planet-261018b with rc 0 and writes the same 8 admin files as `tiervar3`
  r1, byte for byte; nothing outside admin-minimal polygons/vertices and
  quality/q2.5 changes. Wall 379.1 s against 373.7 s for q2.5+cap, back to
  back (+1.4%, C); max RSS 84.34 against 84.61 GB. 457/457 tests pass.
  Determinism: the verifier's own q2.5+cap build matches the study's on
  33/33 files.
- **Daily patch (M):** real `geocoder-diff` patches planet-261017 →
  261018b, both days built with each builder, each applied and verified:
  r1 250,699 B (admin-minimal) / 248,700 B (quality/q2.5) against
  q2.5+cap 274,805 / 274,664 B, −8.8% / −9.5% (C). Matches the study to the
  byte. One day pair only.
- **Query cost (M):** with today's reader (main f48ddc4) r1 costs 2.7× /
  2.9× q2.5+cap per query, not the study's 2.15×. With the three
  answer-identical reader commits 20e4f4f, f2f6d52 and 8dbb58d, r1 costs
  61 µs on photo-like points against today's 94 µs for q2.5+cap (−35%, C);
  at country borders 85 against 81 µs, so not cheaper. Table below.
- **Live Nominatim on r1 (M, zoom 18, ≥2.1 s apart):** points where
  Nominatim gives r1's answer : the other scheme's (: neither): country B2
  vs q2.5+cap 10:2; state B4 vs q2.5+cap 16:2:2; city B8 vs f50 13:2:5;
  state B4 vs f50 9:6:1; photo-like state vs f50 14:2.

### Query time (M, verifier, µs per query, photo-like 50k / L2 border 20k)

Single thread, pinned core, under the box's timing lock, but load average
was 6–16 from another track's `zstd -T16`: treat absolute values as ±15%.
The ratios held across two runs, and an interleaved re-check gave 253 / 234
against 94 / 82 for main.

| reader | admin-minimal q2.5+cap | admin-minimal r1 | recommended q2.5+cap | recommended r1 |
|---|---|---|---|---|
| main f48ddc4 | 93.7 / 80.9 | 250.5 / 234.2 | 221.5 / 134.4 | 363.5 / 283.7 |
| psp 74a4a88 | 35.8 / 24.6 | 51.0 / 40.0 | 132.7 / 54.6 | 138.6 / 67.0 |
| pspr (psp without 66a5ad0) | 43.8 / 42.8 | 61.3 / 85.1 | 160.0 / 86.2 | 181.0 / 131.3 |
| nest bc2e758 | 100.4 / 86.2 | 258.6 / 241.3 | 229.5 / 139.6 | 370.4 / 295.3 |
| nest3 (nest + the 3 commits) | 50.8 / 49.3 | 67.8 / 93.9 | 158.9 / 88.2 | 182.9 / 138.4 |

Cold (page cache dropped, 2k points): 2.3–2.8 ms admin-minimal, 6.5–7.3 ms
recommended, for every scheme. Answers: main vs pspr 0 of 310k differ; main
vs psp 9 (q2.5+cap) and 8 (r1), all at Egypt's 22° N line, caused by 66a5ad0
(see `../reader-cell-scan`).

### Study tables (M, the study's own pools; not re-measured except as above)

planet-261018b (fix/admin-nesting + cap, warm build; border sets 20k per
level, photo-like 100k):

| scheme | admin-minimal raw / zstd | quality dir raw / zstd | country B2 | state B4 | city B7 | city B8 | photo city | photo any (excl. Greece) | China photo / B8 city | enclaves /3439 | l2c /421 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| q2.5 | −0.8% / −0.9% | −1.2% / −1.1% | 36.42 | 15.29 | 3.13 | 2.48 | 0.288 | 0.548 | 0.09 / 1.73 | 2738 | 303 |
| q2.5+cap | 243.2 / 171.1 MB | 287.4 / 253.2 MB | 36.37 | 15.21 | 3.10 | 2.46 | 0.278 | 0.525 (0.522) | 0.09 / 1.73 | 3301 | 304 |
| f50 | +0.1% / −0.4% | −0.6% / −0.7% | 4.38 | 4.22 | 4.05 | 3.35 | 0.315 | 0.363 (0.361) | 0.07 / 2.30 | 3380 | 304 |
| f75 | −6.3% / −9.2% | −6.0% / −6.5% | 4.38 | 4.22 | 4.78 | 5.03 | 0.394 | 0.426 (0.422) | 0.09 / 3.16 | 3380 | 303 |
| r1 without the city rule (d2) | +0.05% / −0.1% | −0.2% / −0.2% | 4.26 | 4.31 | 2.85 | 2.62 | 0.286 | 0.412 (0.338) | 0.17 / 3.05 | 3386 | 304 |
| **r1** | **−0.3% / −0.5%** | **−0.5% / −0.6%** | 4.26 | 4.73 | 2.70 | 2.39 | 0.245 | 0.383 (0.308) | 0.08 / 1.88 | 3386 | 304 |
| **sr1** | **−11.1% / −15.2%** | −9.7% / −10.2% | 4.78 | 5.28 | 4.22 | 3.64 | 0.326 | 0.460 (0.384) | 0.07 / 2.86 | 3388 | 303 |

planet-261006 (comparable with `../boundary-tiers`; border sets 30k per
level, photo-like 300k; vL8 = the tiers2 verifier's ~1.4k L8 points):

| scheme | admin-minimal raw / zstd / planned | country B2 | state B4 | city B7 | city B8 | vL8 city | photo city | photo any (excl. GR) | enclaves | l2c |
|---|---|---|---|---|---|---|---|---|---|---|
| q2.5 | 240.1 / 169.2 / 129.1 MB | 35.56 | 15.27 | 2.81 | 2.49 | 2.71 | 0.281 | 0.543 | 2738 | 297 |
| q2.5+cap | 241.9 / 170.7 / 130.3 MB | 35.56 | 15.23 | 2.79 | 2.49 | 2.71 | 0.268 | 0.514 (0.515) | 3301 | 298 |
| f50 | −0.0% / −0.5% / −2.2% | 4.43 | 4.36 | 3.71 | 3.29 | 3.96 | 0.301 | 0.338 (0.337) | 3380 | 298 |
| f75 | −6.4% / −9.4% / −10.5% | 4.43 | 4.36 | 4.48 | 4.78 | 5.49 | 0.387 | 0.413 (0.412) | 3380 | 297 |
| **r1** | **−0.4% / −0.7% / −0.8%** | 4.20 | 4.90 | 2.46 | 2.32 | 2.92 | 0.225 | 0.360 (0.285) | 3386 | 298 |
| **sr1** | **−11.2% / −15.4% / −13.1%** | 4.64 | 5.49 | 3.65 | 3.54 | 4.03 | 0.309 | 0.443 (0.367) | 3388 | 297 |

On planet-261006 the verifier's own pools give r1 3.82% country B2, 4.61%
state B4, 2.28% city B7, 1.98% city B8 and 0.242% photo city (M, table in
`../admin-arcs`).

- **Greece flips by a tie-break, not geometry (M).** On Greek islands the
  state answer flips between the coincident "Aegean" (L4) and "South/North
  Aegean" (L5) island rings, decided by their stored areas: about 225 of
  300k photo-like points, hence the "excl. Greece" column. Nominatim gives
  South/North Aegean, r1's answer, not the truth's.
- **Ring-size-relative tolerances only move error around (E, tierscan):**
  at L8 they trade border error against photo-like error at the same bytes.
- **Builder time (M, study):** the phases a scheme touches take 0.7–1.0 s
  (quality/q2.5 simplify) and 2.4 s (admin-minimal) on a planet build.

## Decision status

- **Shipping: r1**, on branch `feat/admin-tier-tolerances` (bdac8cd,
  pushed). It is not in this research branch. It adds `admin_tolerance.h`,
  `simplify_ring_isotropic`, `test_admin_tolerance.cpp` and new tests in
  `test_geometry.cpp`, and sits on 4f87ab6 (the ring cap 9b55d28
  cherry-picked onto `fix/admin-nesting` bc2e758), so merge order matters.
  It changes what "q2.5" contains; the directory name stays.
- **sr1** is the same code with other table values, the size-first option.
- **Reader:** the verifier recommends shipping r1 together with 20e4f4f,
  f2f6d52 and 8dbb58d from `feat/place-search-prod`, and without 66a5ad0.
  The reader work continued in `../reader-cell-scan`.
- `../admin-arcs` beats r1 on size and accuracy but needs a new format;
  r1 now, arcs later.

Open: accept the Baarle state and French Polynesia coast regressions or
give the city rule a small-polygon variant; fix the Greek area tie-break in
the reader; re-run the main-reader timing on a quiet box (the 251 against
170 µs gap may partly be load); offer sr1 for the recommended set too.

### Not verified

- sr1 at planet scale (only r1 was built by the verifier), and patch sizes
  beyond one day pair.
- Hamlets (place type 13) are not in the city rule.
- r1 is only 0.3–0.6% under budget; re-check on the nightly as the planet
  grows.
- Untested idea: tolerances by address role for every level, from the
  reader's `admin_levels.json`.

## Method

- **Builder patches** (`patches/`, on 4f87ab6): `tiers3-dump.patch`
  (ba63f93, tiers2's `GC_TIER_DUMP` hook re-applied), `tiers3-scheme.patch`
  (81f5767: `GC_TIER_SCHEME` replaces the q2.5 tolerances; the dump also
  writes admin-minimal's slot remap so `tiervar3` can reproduce a warm
  build), `tiers3-iso.patch` (fafc695: `iso=1`, metres in every direction).
  No behaviour change without the variables.
- **Planets:** planet-261006 with the tiers2 dump (`fix/admin-holes`
  97c732c), and a consecutive pair built with dumps: D = planet-261017
  (planet-261006 plus daily diffs 5130–5148 through `pbf-apply`, cold) and
  D1 = planet-261018b (plus 5149, warm with `--prev-output D`)
  (`pipeline_all.sh`, `run_build.sh`: nightly arguments and inputs, 96 GiB
  scope without swap).
- **Variants:** `tiervar3.cpp` re-simplifies a dump with the builder's own
  code paths (scheme language in its header: `iso`, `cityovr`, `rest`,
  `rk` are new). It reproduces the real builder on 8/8 files for D, D1,
  D1_d2 and D1_s5i, warm slots (M, `post1.sh`). The branch's builder
  matches `tiervar3` r1 on an Oceania extract, 8/8 files (M, `post4.sh`).
- **Fast evaluator:** `tierscan.cpp` replays the reader's per-level pick
  (smallest-area polygon containing the point) against the truth on 590k
  points for any per-level spec. Calibrated at q2.5+cap: 2.50% at L8 and
  35.56% at L2, against the real reader's 2.49% city and 35.56% country (M).
  It swept every level from 20 to 1250 m, in degrees and metres, plus
  ring-size-relative variants (`curves.py`).
- **How much each level matters (M):** one level simplified at its
  q2.5+cap tolerance, all others truth, real reader (`batch_sl.sh`,
  `rel.py`): Immich-field changes per pick change on border points.

  | level | 2 | 3 | 4 | 5 | 6 | 7 | 8 |
  |---|---|---|---|---|---|---|---|
  | field changes per pick change | 1.00 | 0.10 | 0.96 | 0.16 | 0.18 | 0.44 | 0.89 |

- **Frontier (E):** `frontier.py` and `bf.py` (exact search over the
  levels with that relevance model) produced the candidates. 31 candidates
  then ran through the real reader on planet-261006 (`cand6.sh`,
  `batch_c*.sh`) and 12 on planet-261018b (`evalD1.sh`, `batch_d34.sh`,
  `batch_r.sh`, `batch_sr.sh`). `pareto.py`: measured Pareto set.
- **Scoring:** `score3.py` maps fields as Immich's `mapGeocoderAddress`
  (city = city ?? town ?? village ?? hamlet ?? municipality, state = state
  ?? province, country = country_code); `paired.py` exact sign test;
  `exgr.py`, `slice.py`, `bycountry.py` per-country views; enclave grids
  through `score_grids_lib` (`/ssd/bugs/probe`); `l2c3.sh` the 421
  loc-to-city checks (`../loc2city-eval`); `nom3.py` live Nominatim.
- **Cost:** `timeq3.sh` (timing lock, warm best of 3 minus process start,
  cold first 2,000 queries), `readereq.sh` and `build_count.sh` +
  `count_patch3.py` (admin vertices touched per query), `patch3.sh` (real
  daily patches D → D1 per scheme), `bg3.sh` / `planned.py` (layout Bg),
  `buildtime.py`.
- **Verifier** (`verify/`): own binaries from committed sources
  (`build_bins.sh`: builders 4f87ab6 and bdac8cd; readers main, psp, pspr,
  nest, nest3, arcs), four planet builds (`planets.sh`: q2.5+cap and r1 on
  261018b, r1 on both patch days), own border sampler `v5border.cpp` (level
  uniform over L2–L8, edge picked by length, ±200 m), own photo-like and
  address samplers `v5photo.py`, own scorer `v5score.py` (exact McNemar),
  fresh 20k plus bb140k / bp100k / ba50k on 261018b, re-timing
  (`timing.sh`, interleaved `recheck.sh`), real patches (`patch.sh`), live
  Nominatim (`nompick.py`, `napi3.py`, `nomscore.py`).

## Layout

- `patches/`: the three study-only builder commits of branch `tiers3-study`
  (`git format-patch`, base 4f87ab6, apply in the order dump, scheme, iso).
- `study/` (the study's `/ssd/tiers3`):
  - tools: `tiervar3.cpp`, `tierscan.cpp`, `borders2.cpp` (border pools,
    exactly n per level); `count_patch3.py` (reader counters, applied by
    `build_count.sh`).
  - builds and pipeline: `pipeline_all.sh` / `pipeline1.sh` (lock window),
    `run_build.sh`, `post1.sh`–`post4.sh`, `wait_post1.sh`,
    `build_readers.sh`, `build_count.sh`; `cands_build.txt` and
    `cands_time.txt` (the candidate builds and timing sets they read).
  - candidates: `mkset6.sh`, `cand6.sh` (planet-261006), `mksetD1.sh`,
    `evalD1.sh` (planet-261018b), `batch_*.sh`, `wait_sl.sh`.
  - queries and pools: `runq.sh`, `pools.txt`, `photo.py`, `photo2.py`.
  - scoring and tables: `score3.py`, `paired.py`, `table.py`, `pareto.py`,
    `exgr.py`, `slice.py`, `bycountry.py`, `rel.py`, `curves.py`,
    `frontier.py`, `bf.py`, `l2c3.sh`, `nom3.py`.
  - cost: `timeq3.sh`, `time_table.py`, `readereq.sh`, `patch3.sh`,
    `patch_table.py`, `bg3.sh`, `planned.py`, `buildtime.py`.
- `verify/` (the tiers3 part of the verifier's `/ssd/verify5`, repo branch
  `verify5/tools` 743e6ef): `build_bins.sh`, `planets.sh`, `stage1.sh`,
  `stage2.sh`, `stage2t.sh`, `recheck.sh`, `mkset.sh`, `v5q.sh`,
  `v5border.cpp`, `v5photo.py`, `v5score.py`, `table.py`, `bycc.py`,
  `cc_rates.py`, `grids.py`, `gridfields.py`, `eqcmp.sh`, `sizes.sh`,
  `planned.sh`, `patch.sh`, `timing.sh`, `time_table.py`, `nompick.py`,
  `napi3.py`, `nomscore.py`. The arcs part is in `../admin-arcs/verify`;
  `stage2.sh` / `stage2t.sh` also time the arcs sets.

Left out: the edit scripts that made the study commits and the final
change (`scheme_patch.py`, `iso_patch.py` = the patches here;
`final_patch.py`, `final_patch2.py` = bdac8cd).

The scripts hard-code the box paths (`/ssd/tiers3`, `/ssd/verify5`, and
`/ssd/tiers2`, `/ssd/bugs`, `/ssd/bugs2`, `/ssd/shrink`,
`/ssd/transport-verify` for shared tools, pools, truth and sets) and run
Python from `/ssd/shrink/venv` (numpy).

## Rebuild and run

```sh
X=$PWD/geocoder/experiments/admin-tolerances
R=/path/to/geoshenanigans            # a second checkout
T=/ssd/tiers3; V=/ssd/verify5
mkdir -p $T/bin $V/bin && cp $X/study/* $T/ && cp $X/verify/* $V/

# study builder: 4f87ab6 + the three patches -> $T/bin-builder2 (bin-builder: without iso)
git -C $R checkout -b tiers3-study 4f87ab6
git -C $R am $X/patches/tiers3-dump.patch $X/patches/tiers3-scheme.patch $X/patches/tiers3-iso.patch
# build build-index with cmake (geocoder/builder) into $T/bin-builder2

# tools (compile lines were not scripted; flags as in ../boundary-tiers)
g++ -O3 -march=native -std=c++17 -fopenmp -I<builder src at 97c732c> $X/study/tiervar3.cpp -o $T/bin/tiervar3-h
g++ -O3 -march=native -std=c++17 -fopenmp -I$R/geocoder/builder/src $X/study/tiervar3.cpp -o $T/bin/tiervar3-n
g++ -O3 -march=native -std=c++17 -fopenmp -I<builder src at 97c732c> $X/study/tierscan.cpp -o $T/bin/tierscan-h
g++ -O3 -march=native -std=c++17 -I$R/geocoder/builder/src $X/study/borders2.cpp -o $T/bin/borders2

cd $T
./build_readers.sh && ./build_count.sh   # readers main, psp, nest and counting builds, from worktrees of
                                         # $T/repo (refs main-f48 = f48ddc4, psp = 74a4a88, tiers3-study);
                                         # pspr (bf056c9 = psp with 66a5ad0 reverted) was built by hand
./pipeline_all.sh     # lock: planet-261017, builds D and D1 with dumps, candidate builds, timing
./post1.sh            # tiervar3-n vs the real builds, daily patches, d2 on planet-261006
./post2.sh            # baselines and d2 / s5i on planet-261018b, reader equivalence
./batch_sl.sh         # per-level relevance (needs the unc truth answers: mkset6.sh unc none + runq.sh)
./batch_r.sh          # r1 and r2 on both planets
./batch_sr.sh         # sr1 on both planets, its patch and planned size
PY=/ssd/shrink/venv/bin/python
$PY -I table.py --ref cap10 q25 cap10 f50 f75 r1 sr1
$PY -I paired.py cap10 r1 B8:city photo:city

# verifier
cd $V
./build_bins.sh && ./planets.sh
./stage1.sh && ./stage2.sh            # stage2 waits for ../admin-arcs/verify/stage_arcs.sh before timing
$PY -I v5score.py pts/D1/big D1_unc_smallest D1_qcapB_smallest D1_r1B_smallest --pair D1_qcapB_smallest:D1_r1B_smallest
./recheck.sh
```

The verifier's pools (`pts/D1/fresh`, `pts/D1/big`, `pts/d6/*`) were made
with `v5border` and `v5photo.py` on new seeds; those calls were not
scripted (`logs/v5b_*.log` on the box).

## Box paths

- Study: `/ssd/tiers3` (276 GB). Results `cmp/` (`score_*.json`,
  `size_*.tsv`, `bg_*.tsv`, `patch_*.tsv`, `time*.tsv`, `work.tsv`,
  `readereq.tsv`, `check_builds.txt`, `relevance.json`, `bf_*.txt`,
  `nom3_*.json`), sweeps `scan/`, variants `var6/`, `varD1/`, sets
  `sets6/`, `setsD1/`, answers `q/`, pools `pts/`, `rpts/`, `rptsD1/`,
  builds `build/out-D`, `build/out-D1`, dumps `dumpD`, `dumpD1`, patches
  `pv/`, `planet/planet-261017.osm.pbf` (93 GB).
- Study repo `/ssd/tiers3/repo`: `tiers3-study` (the patches here),
  `feat/admin-tier-tolerances` (worktree `wt-final`, build `bin-final`),
  `wt-pspr` at bf056c9 (psp with 66a5ad0 reverted).
- Verifier: `/ssd/verify5` (248 GB, mostly `build/out-{qD1,r1D1,r1D,r1D1w}`):
  `cmp/` (`score*_*.json`, `size_*.tsv`, `bg_*.tsv`,
  `patch_{base,qB,r1}.tsv`, `bytecheck.txt`, `time_D1.tsv`,
  `time_recheck.tsv`), `nom/`, `q/`, `sets/`, `var/`, `pts/`, `logs/`.
  Repo `/ssd/verify5/repo`: `verify5/tools` 743e6ef, `verify5/pspr`
  c869a27, `verify5/nest3` 5c7e8de.

## Data and tool versions

- OSM planets 2026-10-06, 2026-10-17 (`planet-261006` + replication diffs
  5130–5148) and 2026-10-18 (`planet-261018b`, + 5149), with the nightly's
  inputs (TIGER, Wikidata sitelinks, GB postcodes).
- Builders: 4f87ab6 + the three patches (study), bdac8cd (shipping
  branch); tiers2 dump from 97c732c + 77c010a.
- Readers (`grid_query`): main f48ddc4, `feat/place-search-prod` 74a4a88
  (psp), pspr (psp − 66a5ad0), `fix/admin-nesting` bc2e758 (nest), nest3
  (nest + 20e4f4f, f2f6d52, 8dbb58d).
- Layout Bg: `../boundary-precision/grid/bgcodec.cpp`. Loc-to-city:
  `../loc2city-eval/verify/l2c_eval.py`. Public Nominatim `/reverse`
  (jsonv2, addressdetails, zoom 18).
