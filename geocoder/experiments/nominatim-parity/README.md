# Nominatim parity: validation harness for the parity fixes

How to change the builder and server so their reverse answers match
Nominatim's, and prove that every changed answer is explained and is
better? The parity track fixed four reported differences (relations without
an area type, quarter and neighbourhood sharing one slot, Singapore without
a city in the smallest set, Leukerbad's state), plus the place-node rules
they exposed, each traced to a file and line of the Nominatim source
(87eb582). This directory holds the validation harness: cold planet builds
of base and tip, a one-step-at-a-time walk that attributes every changed
answer to one commit and checks its mechanism, sampled checks against the
public Nominatim API, loc-to-city, enclave grids, query time and download
size. Run 2026-10-19 on the box (`/ssd/parity`), re-checked the same day by
an independent verifier (`/ssd/verify6`, `verify/`) that rebuilt every
binary and both planet outputs from committed source, on fresh points.

The code is branch `fix/nominatim-parity` (11 commits on `fix/admin-nesting`
bc2e758, box repo `/ssd/parity/repo`, not yet pushed). It is not in this
research branch.

Where the study and the verifier differ, the verifier's number is given.
Tags: **M** = measured on the box, **C** = computed from measured values,
**E** = estimated. API = the public Nominatim instance, `/reverse` jsonv2
with addressdetails at zoom 18, calls ≥2 s apart. Better / worse / same
compares the number of mismatched place fields (country … neighbourhood)
against Nominatim's answer. Presets: smallest (admin-minimal),
recommended, max.

## The commits and the Nominatim source behind each

| commit | change | Nominatim source (87eb582) |
|---|---|---|
| fff988c | only relations of type multipolygon/boundary are imported; `place=*` on such a relation makes a place area | init.lua:140-144, 680-684; presets.lua:116-124; ranking.sql:233, 271 |
| 984e668 | quarter (rank 22) and neighbourhood (rank 24) each get their own field | address-levels.json:24-25; placex_triggers.sql:612, 646-654 |
| 7e8e9bb | a place node's fuzzy-area square has half side = radius (was radius/√2) | utils.sql:328-357 |
| c99516b | suburb/hamlet/quarter/neighbourhood nodes only count within their own fuzzy area | utils.sql:382-386; partition-functions.sql:37-44 |
| 9171a8e | city/town/village picked by fuzzy area and distance weighted 0.25 / 0.5 / 1 | placex_triggers.sql:597-604 |
| 253eb9a | smallest set keeps place areas up to rank 16 (Singapore R17140517) | API: R17140517 is the city at rank 16 |
| 0094397 | postcode areas only from relations that have a postcode tag | init.lua:787-797; presets.lua:345-347 |
| 509845b | state/region/province taken at the street point nearest the query when chain and query disagree | reverse.py:44-47; results.py:593-625 |
| 64183ef | city/town/village nodes must lie inside the current boundary (fixes a Dhaka regression from 9171a8e) | placex_triggers.sql:606-609, 650-653 |
| 3726c0e | regions without admin polygons still get the admin-minimal files and `admin_parents.bin` | configurations.template.json |
| f7096b0 | no place areas for state/province/region, address rank 0 (fixes a query-time regression from fff988c) | placex_triggers.sql:1254, 285; address-levels.json:9-11 |

The verifier checked every file:line above against the source (M).

## Results (planet-261018b, base bc2e758 → tip f7096b0, verified 2026-10-19)

- **Rebuilds exactly (M):** the verifier's planet builds match the track's
  byte for byte: tip 1177/1177 files, base 1184/1184 (also against the
  enclaves track's `out-new`). Planet build base 332.9 s / 79.98 GiB, tip
  336.6 s / 79.51 GiB.
- **Tests (M):** builder 448/448 at base, 456/456 at the tip; server
  45+27 at base, 52+27 at the tip.
- **Better against Nominatim (M):** on 83 sampled changed points of the
  verifier's fresh 30k, tip against base is 54 better / 11 worse / 18 same;
  exact matches 8 → 35. On the 24 fixed API points and 9 trace points the
  verifier gets exact matches 3 → 8 / 9 → 16 / 10 → 19 (smallest /
  recommended / max), one point higher than the track in recommended and
  max, probably because Nominatim's own answers changed since.
- **Every change explained (M):** on 30k fresh points, the walk base → tip
  one commit at a time changes only fields inside each commit's scope:

  | step | smallest | recommended | max | changes touching a field outside the commit's scope |
  |---|---:|---:|---:|---|
  | builder (5 builder commits) | 761 | 327 | 357 | – |
  | 984e668 | 0 | 650 | 649 | 0 (only quarter/neighbourhood) |
  | 7e8e9bb | 201 | 956 | 951 | 0 (place-node fields only) |
  | c99516b | 1,900 | 4,405 | 4,403 | 0 |
  | 9171a8e | 2,562 | 2,481 | 2,482 | 0 |
  | 509845b | 0 | 0 | 20 | 0 (only state/region) |
  | 64183ef | 601 | 597 | 594 | 0 |
  | **base → tip** | 5,430 | 7,256 | 7,280 | |

  For 755/761, 313/327 and 341/357 of the builder step's changes a polygon
  at the point was added or dropped (`admin_probe`). All 574 objects behind
  them were checked against the planet's tags: every dropped one matches a
  rule (323 relations without a type tag; 1,135 place=region areas and 1
  place=state; postal relations without a postcode tag; 13 postal closed
  ways); every added one is a type=boundary or multipolygon place area.
- **Loc-to-city (M), corrected:** checks passing 303 → 302 / 316 → 309 /
  317 → 309. The checks that change are 50 checks at 21 points across the
  three presets; there the tip's city/town/village equals Nominatim's on
  47/50, the base's on 0/50. The only misses are Castries ×3 (Nominatim:
  village Goodlands, tip: Conway). The track's "16 points, 15/15/15" was
  wrong. The checks expect capital-city names Nominatim does not return.
- **Enclave grids (M):** country_code identical on all 20,100 grid points
  in every preset.
- **Singapore (M):** smallest-set points with city=Singapore 7/48 → 48/48
  on the verifier's pool (track: 5 → 83 of 83 SG points).
- **Changed answers on 200k photo-like points (M):** 48,420 / 56,988 /
  57,083; the verifier's base and tip answers on recommended are
  byte-identical to the track's. 64183ef alone changes 2,048 / 2,013 /
  2,004 (confirmed exactly).
- **Query time, one pinned core (M):** smallest 19.73 → 18.22 s (−7.7%),
  recommended 42.94 → 36.70 s (−14.5%), 3 runs each on p200k; max on 30k
  points 102.6 → 97.0 s (−5.5%; the track measured −8.0% on p200k, one
  run). Most of recommended's gain is f7096b0 (−22.8%), which undoes
  fff988c's data cost (+14.3%).
- **Download size, zstd -19 (M):** +2,896,939 / −4,306,884 / −19,067,085
  bytes. The smallest set grows because it now carries 27,167 place areas of
  rank ≤16 (23,311 villages), about 0.8% raw.
- **Antarctica, corrected (M):** with 3726c0e all presets assemble at the
  tip, but only max answers; smallest and recommended return an empty
  address at all 5 test points (McMurdo, Palmer, Concordia and two others),
  at the tip and at bc2e758 alike. Not a regression of this branch.

### Per commit on the 200k points (M, track; better / worse / same is an API sample of that commit's own changes)

| commit | changed (smallest / rec / max) | explained | better / worse / same |
|---|---|---|---|
| fff988c | 364 / 748 / 755 | all classified; 2 UNVERIFIED (Taipei postcode, Peterborough road) | 20 / 4 / 6 of 30 |
| 253eb9a | 1,457 / – / – | point in a kept place area 1,452, place node 5 | 22 / 2 / 3 of 27 |
| 0094397 | 0 / 14 / 69 | postal area dropped or changed | – |
| 984e668 | 0 / 4,434 / 4,429 | 100% by mechanism | 13 / 11 / 6 of 30 |
| 7e8e9bb | 5,111 / 9,085 / 9,072 | 100% | 15 / 11 / 4 of 30; **verifier: 8 / 11 / 1 of 20** |
| c99516b | 18,213 / 27,869 / 27,861 | all but 1 field | 24 / 7 / 5 of 36 |
| 9171a8e | 28,233 / 27,808 / 27,795 | all but 1 field | 25 / 8 / 7 of 40 |
| 509845b | 0 / 0 / 96 | only state/region/province | 17 / 0 / 7 of 24 |
| 64183ef | 2,048 / 2,013 / 2,004 | 100% by mechanism | 22 / 3 / 11 of 36 |
| 3726c0e + f7096b0 | 0 / 0 / 0 | 1 of 30k extra points (Mecca, matches the API) | – |

7e8e9bb as a net gain on its own is UNVERIFIED: the verifier's sample of
its own changes went 8 better / 11 worse / 1 same. The commit does match
Nominatim's source.

Relation census (M, track): fff988c drops 1,574 relations (1,565 without a
type tag) and adds 2,239 place areas (all type=boundary, 667 place=region);
0094397 drops 4,221 postal polygons (2,092 relations without a postcode
tag, 2,129 closed ways); f7096b0 drops 1,136 rank-0 place relations.

Rejected candidates (track, re-scored against the API, M): "nearest node
that passes the gate" 13 better / 15 worse of 30; "ranks held by boundaries
plus fuzzy-area reset" 11 better / 26 worse of 40. Their code is on the box
(`/ssd/parity/tmp/lib.rs.s4_rejected`, `lib.rs.s5_rejected`).

### Gaps the verifier found against the source

1. **Distance metric:** Nominatim orders place nodes by `ST_Distance` in
   plain degrees (partition-functions.sql:32); the server scales longitude
   by cos(lat) (`lib.rs:2095`). An uncommitted experiment in degrees
   (`verify/deg6.sh`) changes 511/30k recommended answers, 5 better / 6
   worse / 4 same against the API (M): no evidence it helps.
2. **f7096b0 ignores country rank overrides** (place=province ranks 10 in
   Spain, 12 in Saudi Arabia, address-levels.json:236-254). Nothing on
   planet-261018b is affected (M: no place=province areas; the Saudi areas
   dropped are desert place=region; kept place=county areas are GB 141 and
   PK 1).
3. A boundary=postal_code relation that also has place=* gets no place
   area here; `tiger:zip_left` / `tiger:zip_right` are not read as
   postcode keys.

## Decision status

The verifier found every commit rebuilds exactly from source, passes its
tests and is net better against Nominatim, so the branch is ready for a PR
once these are decided:
1. Update the loc-to-city expectations to Nominatim's answers (as was done
   for the Vatican)? The tip loses 1 / 7 / 8 capital-city checks that
   Nominatim itself fails.
2. Keep 7e8e9bb, given the verifier's negative sample?
3. Commit titles run past AGENTS.md's 2–5 words; reword before the PR.

Open follow-ups: fuzzy areas are tested against the query point, Nominatim
tests them against the result street's geometry (explains most remaining
"worse" rows: Ydby, Hurden, Taguig, Catbalogan); capitals weigh 0.2 in
Nominatim but our place nodes carry no capital flag (a format change);
smallest set could keep only the 488 city place areas instead of all rank
≤16 areas (the API sample favours all, 22 better / 2 worse); closed ways
with place=* and a non-place boundary tag are not imported as place areas;
Antarctica is empty in smallest and recommended (predates the branch).

## Method

- **Builds:** each revision from a `git archive` export: builder
  (`build_revs.sh`, `builder_build.sh`, unit tests run) and server
  (`cargo_build.sh`; `server_revs.sh` re-runs `cargo test` per server
  commit and checks the archive `grid_query` is md5-identical to the binary
  used for the measurements). Cold planet builds with the nightly's
  arguments and inputs, 96 GiB scope without swap (`run_planet.sh`), under
  the timing lock: `pipeline.sh` (base bc2e758, r1 fff988c, r4 0094397),
  `pipeline2.sh` (r5 8896cf5, the rank-0 change before 3726c0e was put in
  front of it and it became f7096b0), `pipeline3.sh`
  (r6 = tip f7096b0, plus the Antarctica sets via
  `probe/assemble_region.py`). Base checked file by file against the
  enclaves track's output. Client presets via `probe/assemble_sets.py`
  (copy of `/ssd/bugs2/probe/assemble_sets.py`).
- **Attribution walk:** answer sets labelled data × server (`queries.sh`,
  `runq.sh`): B0 base × base, D1 r1 data × base server, D4 tip data × base
  server, then T2a…T6 / T7 / T9 tip data × each server commit, so every
  changed answer belongs to one step. Builder steps: `probe/explain_builder.py`
  (point falls in a dropped or added polygon, place node shown or hidden,
  label moved, POI relation dropped), `probe/explain_label.py`,
  `probe/explain_poi.py`, `probe/dropped_ids.py` + osmium + `probe/tag_census.py`
  (OSM tags of every object the builds disagree on), `probe/l15_cost.py`.
  Server steps: `probe/explain_server.py` (fields a step may touch),
  `probe/mech_server.py`, `probe/mech2.py`, `probe/mech_s7.py` (mechanism
  from the place-node debug of `probe/parity_probe.rs` and the polygons of
  `probe/admin_probe.rs`). `probe/adiff.py` diffs two answer sets.
- **Against Nominatim:** `probe/sample.py` / `probe/usample.py` pick
  changed points, `probe/napi.py` queries the API (2.1 s apart, resumable),
  `probe/score.py` scores servers against it, `probe/ndetails.py`
  (`/details`), `probe/nodedist.py`, `probe/waymid.py`, `probe/r16_vs_api.py`.
- **Other checks:** `l2c.sh` and `probe/l2c_diff.py` (421 loc-to-city
  checks with `../loc2city-eval/verify/l2c_eval.py`), enclave grids with
  the enclaves track's `score2.py`, `probe/sg_city.py`, `sizes.sh` +
  `probe/sizesum.py` (zstd -19 per changed file), `probe/polycount.py`,
  `probe/geo.py`, `probe/showpts.py`. Query time: `bench.sh`,
  `after*.sh`, `final*.sh` (one pinned core, warm, runs alternated, under
  the timing lock). `analyze.sh` / `analyze2.sh` run the whole analysis.
- **Verifier** (`verify/`): `build_all.sh` builds every binary of the
  parity, reader and searchbuild checks from `git archive` of committed
  revisions (also used by `../reader-cell-scan` and `../search-build-memory`);
  `planet.sh` two cold planet builds compared file by file with the track's;
  `gen_pts6.py` fresh pools (par30k seed 61906: 22,500 photo-like from
  GeoNames cities500 plus 7,500 on land; rd30k seed 61907 for the reader
  check); `pq.sh` the walk B → D → S1…S5 → T with its own labels,
  `pq_an.py` attribution and out-of-scope check; `mech6.py`, `osm6.sh`,
  `osm6.py` mechanism and OSM tags; `sample6.py`, `napi6.py`, `score6.py`,
  `l2c6.py` API scoring; `deg6.sh`, `deg_sample.py` the degree-distance
  experiment; `rank_overrides.py`, `region_cc.py`, `county_cc.py` country
  rank overrides; `pt_time.sh` query time; `sizes6.sh`, `sizes_max.sh`
  download size; `tests6.sh` cargo tests (parity and reader); `chain.sh`
  runs the whole verifier after the planet builds.

## Layout

- `study/`: the track's `/ssd/parity` scripts (`*.sh`) and `study/probe/`
  (its `probe/` directory: analysis scripts and the two server examples
  `admin_probe.rs`, `parity_probe.rs`). Both the first run and the resume
  are here; the resume's scripts are `after2.sh`, `after3.sh`,
  `analyze2.sh`, `final*.sh`, `pipeline2.sh`, `pipeline3.sh` and their
  `probe/` helpers.
- `verify/`: the parity part of the verifier's `/ssd/verify6/scripts`, plus
  the shared `build_all.sh`, `chain.sh`, `tests6.sh` and `gen_pts6.py`.

Left out: the edit scripts and commit messages that built the branch
(`edit_s7.py`, `edit_s9.py`, `edit_empty_admin.py`, `msg_*.txt`: the
branch holds the result), the rejected-candidate edits (`edit_s4.py`,
`edit_s4_tests.py`, which patched a local source copy; the rejected code
is on the box), the local source snapshots (`src/`, `bsrc/`, `diffs/`:
copies of the branch, some files over 200 KB), the local copy of the
Nominatim source (`ref/Nominatim`, third-party), and `tt.py` (a transcript
reader, not part of the harness).

The scripts hard-code the box paths (`/ssd/parity`, `/ssd/verify6`, and
`/ssd/bugs2`, `/ssd/shrink`, `/ssd/transport-verify`, `/ssd/loc2city` for
the planet, sets, grids and shared tools) and run Python from
`/ssd/shrink/venv` (numpy).

## Rebuild and run

```sh
X=$PWD/geocoder/experiments/nominatim-parity
P=/ssd/parity; V=/ssd/verify6
mkdir -p $P/probe $P/bin $V/scripts
cp $X/study/*.sh $P/ && cp $X/study/probe/* $P/probe/ && cp $X/verify/* $V/scripts/
# $P/repo: a clone with branch fix/nominatim-parity; $P/pts: p200k, l2c, grids, v4 pools
cd $P
./build_revs.sh                      # build-index.{base,r1,r4} from git archive, tests run
./cargo_build.sh base <repo at bc2e758>; ./cargo_build.sh s6 <repo at 509845b>   # etc. per server commit
./pipeline.sh                        # lock: three cold planet builds, base check, sets
./queries.sh && ./analyze.sh         # every answer set, then the attribution and API scoring
./final2.sh                          # resume: analyze2.sh, then timing under the lock
./pipeline2.sh && ./pipeline3.sh && ./final3.sh   # r5 and r6 (tip) builds, T9 answers, API sets, timing
./server_revs.sh                     # archive builds of each server commit: tests + md5

# verifier
cd $V
scripts/build_all.sh && scripts/planet.sh && scripts/chain.sh
/ssd/shrink/venv/bin/python -I scripts/pq_an.py
```

## Box paths

- Track: `/ssd/parity` (599 GB): builds `build/{base,r1,r4,r5,r6}` and
  `out-*` (about 125 GB each; out-r1 and out-r5 deletable), sets
  `sets/{base,r1,r4,r5,ant-r4,ant-r6}`, answers `q/<label>_<preset>/`,
  analysis `cmp/`, census `census/`, API responses `nominatim/*.jsonl`,
  loc-to-city `l2c/`, logs `logs/`, rejected code `tmp/lib.rs.s*_rejected`.
- Repo `/ssd/parity/repo`, branch `fix/nominatim-parity` (tip f7096b0).
- Verifier: `/ssd/verify6`: `bin/`, `build/out-{base,tip}` (about 250 GB,
  deletable), `sets/`, `pts/` (`par30k`, `rd30k`), `q/`, `cmp/`, `api/`,
  `osm/`, `logs/`.

## Data and tool versions

- OSM planet 2026-10-18 (`planet-261018b`) with the nightly's inputs;
  base `fix/admin-nesting` bc2e758, tip f7096b0.
- Nominatim source 87eb582; public Nominatim API, 2026-10-19.
- Loc-to-city: the 421 checks of `../loc2city-eval` with Natural Earth
  `ne_10m_admin_0_countries`.
