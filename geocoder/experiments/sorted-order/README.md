# Sorted order: content-ordered days and identity-matched patches

Can the street, interp and address files be kept in a canonical,
content-derived (spatial) order every day, instead of strategy-2 stable
slots, without changing any answer, and what do patches between such days
cost? Study run 2026-10-13/14 on the box (`/ssd/shrink/sortpatch/`) on the
published planet maximum set (`planet/full`, `planet/quality/uncapped`,
`planet/poi/all`) of 2026-10-11, 10-12 and 10-13.

The patch format this produced (GCPATCH v8, identity-matched merges) is on
the `feat/gcpatch-v8` branch; the tools here measure it.

## Results (all verified by an independent re-run, 2026-10-14)

**Identity.** A slot live on two consecutive published days holds the same
OSM entity: checked against the published `*.osm_ids` sidecars, 100% for
street (55.5M), addr (179.7M) and interp (34.0M) slots on both day pairs; no
dead slot names an entity. So the slot is an exact identity proxy.

**Canonical order** (`canon_sort`): streets by Hilbert-24 of the first node,
name, exact first-node lat/lng, then slot; interps the same plus a slot-order
constraint for lines that share a vertex and differ in street, postcode or
numbering; addresses by the cell listing the point, Hilbert key, lat, lng,
house number, street, then slot. Tombstones are dropped and every reference
remapped (cell entries, node / vertex offsets, `parent_way_id`, the four
per-record side files).
- `grid_query` answers, full JSON, sorted vs published: 0 diffs on 100k mixed
  points per day, 0 of 2,224 / 3,356 targeted tie queries, 0 on 12k shared
  interp vertices (re-verified on fresh seeds: 0 diffs).
- The tie-class rule is required: the reader breaks exact street ties by
  list order, so ways with the same name and first node must keep slot order
  (1.87M groups / 3.76M ways, 6.8%). With a node-count key, 18 of 3,356
  targeted queries changed house-number refinement; for interps 1 of 100k.
  A production sort therefore still needs the previous order inside reader
  tie classes, or must accept a few exact-tie answer changes.
- Sorting a planet day: 93.0 s wall, 282 s CPU, 20.0 GiB anon on 8 threads
  (verifier: 89.3 s / 267.5 s), deterministic, idempotent.

**Order churn** between sorted days (kept / moved records): streets 55.55M /
1,693-2,209 moved, addrs 179.7M / 1,004-1,042 moved, interps 1 moved.

**Patches, planet/full** (compressed bytes; every apply byte-identical):

| pair | published (v7, stable slots) | v7 tools on sorted days | v8 on sorted days | v8 on stable-slot days |
|---|---|---|---|---|
| 10-12 → 13 | 3,916,360 | 356,133,137 | **3,319,141** (−15.2%) | 3,453,290 (−11.8%) |
| 10-11 → 12 | 2,920,005 | 332,055,399 | **2,538,023** (−13.1%) | 2,580,869 (−11.6%) |

- Today's tools on sorted days send 91-114x more: position-diffed side files
  (1.42 GB raw) and 55.4M explicit street id pairs.
- All three variants per day: 5.45 / 4.00 MiB with v8 vs 6.02 / 4.37 MiB.
- Diff (cold, 8 cores): v8 56.7 s / 226 s CPU / 24.4 GiB vs 70.1 s / 388 s /
  39.5 GiB published. Apply (1 core, cold): 173.4 s vs 212.3 s; small device
  unchanged (~1,609 s, read-IOPS bound); anon peak 7-9 MiB.
- Most of the gain is the v8 format, not the order: v8 on today's stable-slot
  days already gives −12% size and −21% apply CPU. Sorting adds −2 to −4%
  and stops compressed formats eroding (the gxc geo index 2,329 → 518 MiB
  needs spatial order), at about +282 s CPU per night and 1.08 GB/day of
  server-side identity sidecars.

## Layout

- `src/canon_sort.cpp`: rewrites a published `planet/full` dir in canonical
  order, today's file format; writes `<family>.slot.u32` (new id → slot) and
  `stats.txt` to a meta dir. `--way-key spec` / `--interp-order key`
  reproduce the first (rejected) key.
- `src/order_churn.cpp`: kept / deleted / inserted / moved / copy runs
  between two days' slot maps.
- `src/identity.py`: slot identity across two days against the `*.osm_ids`
  sidecars. `src/gen_points.py`, `src/gen_interp_ties.py`: seeded query
  points (100k mix, tie classes, shared interp vertices).
- `scripts/`: `fetch_day.py` (download a day's planet max set with its
  patches, sha256-checked), `fetch_sidecars.sh` (osm_ids), `link_day.sh`
  (complete a sorted day with hardlinks), `runq.sh` (published vs sorted
  `grid_query` answers), `timed.sh` (cold timing under the shared lock),
  `smoke.sh` (today's diff / patch on sorted days).
- `harness/baseline/`: today's patch costs on the published days (diff
  cold on cores 56-63; apply in a systemd scope on a private byte copy, p8 /
  p1 / small-device profiles, then `verify_patch.sh`).
- `harness/patch/`: the same harness for v7 / v8 / v8c (no sidecars) / v8s
  (v8 on stable-slot days), plus `walk_patch.py` / `section_sizes2.py`
  (bytes per patch section), `collect.py` (all numbers in one table),
  `check_packed.py`, and `gen_pub_identity.py` (the numpy original of
  `tools/gen_identity_sidecar.py` on `feat/gcpatch-v8`, byte-identical
  output).
- `verify/`: the independent verifier: `vsort.cpp` (own Hilbert and key
  code, checks a sorted day against the published one), `gen_pts.py`,
  `runq.sh`, `vtime.sh`, `vpost.sh`, `vident.py`.

The scripts hard-code the box paths of the study (`/ssd/shrink/...`).

## Rebuild and run

```sh
g++ -O3 -march=native -std=c++17 -fopenmp src/canon_sort.cpp -o canon_sort
g++ -O2 -std=c++17 src/order_churn.cpp -o order_churn
g++ -O3 -march=native -std=c++17 -fopenmp verify/vsort.cpp -o vsort
python3 scripts/fetch_day.py 2026-10-13 /ssd/shrink/days/2026-10-13
./canon_sort /ssd/shrink/days/2026-10-13/planet/full OUT/planet/full META/2026-10-13
bash scripts/link_day.sh 2026-10-13
```

Patch measurements: build the tools from the commits below with CI's
method (`cmake -DCMAKE_BUILD_TYPE=Release`, `make geocoder-diff
geocoder-patch`), put zstd 1.5.7 first on PATH as CI does (the compressed
patch bytes depend on it), build the mirror tree with
`harness/baseline/build_tree.sh`, then `harness/patch/run_v8.sh`.

## Data and tool versions

- Days: published builds 2026-10-11/12/13 from
  `https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/<date>/`
  (planet max set plus `planet/quality/q2.5`, which `geocoder-diff` borrows
  admin polygons from, as in CI), and the `planet/full/*.osm_ids` sidecars.
- v7 tools (`geocoder-diff`, `geocoder-patch`) and `grid_query`
  (`geocoder/server/examples/grid_query.rs`): main at 5caed45, g++ 13.3.
- v8 tools: 5caed45 plus the v8 changes, now commit `feat(geocoder): gcpatch
  v8 identity merge` on `feat/gcpatch-v8` (its v8s patch for 10-12 → 13 was
  re-made byte-identical from that branch on 2026-10-18).
