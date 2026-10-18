# Minimal set: bundling, no postcodes, contents

Three questions about the planet `admin-minimal` set (220 MiB zstd): what
shipping it inside immich-server in place of GeoNames would cost and change,
whether the "Immich recommended" set (admin, q2.5, POI major) can drop its
postcode files, and what the minimal set is made of. Run and verified
2026-10-13 on the box (`/ssd/shrink/{bundling,no-postcodes,contents}`)
against the 2026-10-13 planet build, immich-server v3.3.1 and repo main
`5caed45`.

## Results (planet build 2026-10-13, verified 2026-10-13)

**Bundling into immich-server (`bundling/`).**

- **Image size: about +210 MiB compressed per architecture.** Swapping
  the GeoNames layer (18.6 MiB) for the minimal set (228.7 MiB as a gzip -6
  layer) takes amd64 from 583.1 to 793.2 MiB (+36%) and arm64 from 346.3 to
  556.4 MiB (+61%). A zstd layer saves only about 8 MB, because
  `admin_vertices.bin` is about 95% incompressible.
- **Forward search depends on `geodata_places`.** `GET /search/places`
  (the "Search places" box in the Change location modal) is a trigram
  query over that table, and the geocoder has no name → coordinates search.
  Dropping GeoNames means keeping a places table for search, building a
  name index, or losing the feature.
- **Today's cost.** The GeoNames import takes 7.9 s on the box (a lower
  bound; [immich#23912](https://github.com/immich-app/immich/issues/23912)
  logs 905 s on a 2 vCPU host) and 127.5 MB of Postgres per install. The
  geodata layer digest changed in 6 of 9 stable releases from v3.0.2 to
  v3.3.1.
- **Answers change vs today's GeoNames path.** On population-weighted,
  photo-like points (n = 300, accents and case normalised) country matches
  99.3%, state 69.0% (84.0% counting suffix variants such as "Saitama
  Prefecture") and city 46.7% (58.7%).
  - Better: near borders (56 of the 84 Manhattan grid points GeoNames puts
    in New Jersey move to New York; Windsor ON is no longer Detroit) and in
    rural areas (GeoNames is country-only for 51% of random land points;
    the geocoder gives a state for all of them).
  - Worse: inner London (90.5% "Greater London"), Tokyo (no prefecture at
    225 of 225 grid points), 21 of 61 capitals and big cities without a
    state, and territories reported as their parent country (HK/MO → China,
    PR → United States, ...). A few minimal-only regressions (Washington DC
    → Arlington, Moscow without a city) are fixed by the recommended set.
- **Coupling.** The reader never checks `build_version`, and Tigris keeps
  full indexes 3 days and patches 14 days, so bundled data is locked to
  the image's native module.
- **Startup** with `grid_query`: process start, mmap and one query take
  28.9 ms cold and 3.4 ms warm.

**Recommended set without postcodes (`no-postcodes/`).**

- **550.4 → 512.2 MiB zstd** (1061.0 → 969.8 MiB raw) by dropping the 6
  postcode files.
- **0 Immich-field diffs on 22,007 points**: city, state, country, landmark
  and places[] are unchanged; only the postcode field changes (200
  postcodes remain, because POI parent postcodes are boundary names in
  `strings_core`).
- **Loading and patching.** The reader loads with whole postcode groups
  missing and refuses partial groups. Today's published patches refuse to
  apply to dirs that skipped the files. A no-postcode variant's own patch
  is about the same size (admin 39 KB, q2.5 198 KB for 10-12 → 10-13).
- **Postal boundaries are stored twice.** admin_level 11 rings sit in
  `admin_polygons` / `admin_vertices` / the admin cell index and are copied
  into `postal_*`. Removing either copy gives byte-identical output on all
  22,007 points and saves 13.7 MiB (admin copy) or 13.1 MiB (`postal_*`)
  for every admin-mode client.
- **admin-minimal + POI major** returns the same landmarks as recommended,
  but only when both come from the same build date. Mixing dates corrupts
  names silently (all 3,402 landmarks wrong).

**Contents of the minimal set (`contents/`).** Bill of materials of the
220.06 MiB zstd (358.8 MiB raw) set. Every published `.zst` size is
reproduced with zstd 1.5.7 `-18 -T1`. Per-content zstd is the slice
compressed alone, scaled to the file's real size.

| Content | zstd MiB | Share |
|---|---:|---:|
| Admin geometry L8 / L6 / L7 (`admin_vertices`) | 75.99 / 36.36 / 17.82 | 59.1% |
| Admin geometry L2-L5 | 14.76 | 6.9% |
| Place nodes: hamlet 18.40, village 16.68, town 1.16, city 0.13 | 36.37 | 16.5% |
| Indexes: admin cells 5.77, admin entries 4.36, place cells 1.70, place entries 9.52 | 21.35 | 9.7% |
| `strings_core`: names used here / names nothing here uses | 7.71 / 5.04 | 5.8% |
| Admin ring records (`admin_polygons`) | 4.65 | 2.1% |

- **Dead weight: 17.0 MiB (7.7%)**, with no output change by
  construction: 5.04 MiB of unreferenced strings, 3.37 MiB of hidden and
  tombstoned place records with their index entries, and 8.62 MiB of
  `PlaceNode.parent_poly_id`, which the reader never reads. Pruned set:
  203.04 MiB.
- **Beyond that, output changes.** Dropping rings whose label Immich
  doesn't read saves 43.5 MiB more (→ 159.50) and changes Immich's
  (country, state, city) at 0.6-3.2% of points. Dropping hamlet nodes saves
  21.7 MiB more (→ 137.80), but city changes at 16.6% of hamlet-stratum
  points. Dropping L3 saves nothing.
- **What feeds Immich.** L4 is the state source, L2 the only source of the
  country code, and place nodes the main city source near settlements.
- All 469,726 minimal rings are byte-identical to the same rings in admin
  mode's `quality/q2.5`.

## Layout

- `bundling/`: today's GeoNames footprint (`fetch_geodata.sh`,
  `pg_import/{make_sql.py,schema.sql,run.sh}` for a throwaway Postgres in
  Immich's image), registry and layer sizes (`registry_sizes.py`,
  `fetch_build_layer.py`, `geodata_layer_history.py`, `layer_size.sh`),
  the regression check against Immich's GeoNames lookup re-implemented in
  Python (`compare.py`, then `analyze.py`, `classify.py`, `variants.py`,
  `country_arbiter.py`, `random_land.py`, `land_arbiter.py`, `grids.py`,
  `capitals.py`), startup cost (`time_load.py`) and the arm64 cross-build of
  the Node module (`arm64/build.sh`).
- `no-postcodes/`: sizes from the manifest (`sizes.py`), hardlinked variant
  dirs (`setup_dirs.sh`, `l11_strip.py`, `minimal_poi_remap.py`), query
  points and runs (`gen_points.py`, `run_queries.sh`, `compare.py`,
  `named_table.py`), cross-reference checks (`poi_parent_ids.py`,
  `core_postal_strings.py`, `edge_checks.sh`) and the patching test
  (`fetch_layout.py`, `patch_test.sh`).
- `contents/`: `inventory.py` (record-level inventory, writes the string
  class mask), `zstd_shares.py`, `bom.py`, `extra.py`, `strings_attr2.py`
  with `extra/fetch_extra.sh` (its POI-all and street-way inputs),
  `ablate3.py` (hide one level or place type, re-query), `diff_examples.py`,
  `prune2.py` (rewrite pruned files in today's formats and recompress),
  `groups.py`, `q25_check.py`. Each script's docstring gives its usage and
  the formats it reads.

The scripts hard-code the box paths (`/ssd/shrink/...`) and write their
outputs next to themselves; copy each directory to `/ssd/shrink/<name>` and
run it from there.

## Setup

```sh
W=/ssd/shrink
X=/path/to/repo/geocoder/experiments
git clone --depth 1 https://github.com/immich-app/geoshenanigans.git $W/repo
docker run --rm -v $W/repo:/repo -v $W/cargo:/usr/local/cargo/registry \
  -v $W/target:/target -e CARGO_TARGET_DIR=/target -w /repo/geocoder/server \
  rust:bookworm cargo build -q --release --example grid_query
python3 -m venv $W/venv && $W/venv/bin/pip install numpy
P=$W/venv/bin/python
mkdir -p $W/data && curl -so $W/data/configurations-2026-10-13.json \
  https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/2026-10-13/configurations.json
cd $W
$P $X/loc2city-eval/fetch_preset.py planet admin-minimal q2.5 none 2026-10-13 idx/smallest
$P $X/loc2city-eval/fetch_preset.py planet admin q2.5 major 2026-10-13 idx/recommended
```

`contents/` also needs zstd 1.5.7 (`$W/zstd157/bin/zstd`, built from
source): the box's 1.5.5 overstates `place_entries` by 0.7 MiB. Tigris
keeps full indexes for 3 days, so a re-run needs a current build date
(change it in the commands above and in `sizes.py`, `fetch_extra.sh`,
`patch_test.sh`).

## How to run

`bundling/` (needs Docker and network access to ghcr.io):

```sh
mkdir -p geodata && ./fetch_geodata.sh > fetch_geodata.out
$P pg_import/make_sql.py > pg_import/counts.out && pg_import/run.sh > pg_import/import.out
$P registry_sizes.py > registry_sizes.out
$P fetch_build_layer.py > build_layer.out
$P geodata_layer_history.py > geodata_layer_history.out
./layer_size.sh                                   # layer_size.out
$P compare.py                                     # comparison.jsonl (minimal)
DATASET=$W/idx/recommended OUT=comparison_recommended.jsonl $P compare.py
$P analyze.py --examples > analyze.out
$P classify.py > classify.out
$P variants.py > variants.out                     # also minimal vs recommended
$P country_arbiter.py > country_arbiter.out
$P random_land.py > random_land.out && $P land_arbiter.py > land_arbiter.out
$P grids.py > grids.out
$P capitals.py > capitals.out
DATASET=$W/idx/recommended $P capitals.py > capitals_recommended.out
$P time_load.py > time_load.out
bash arm64/build.sh                               # links only; no qemu on the box
```

`no-postcodes/`:

```sh
mkdir -p out
$P sizes.py
./setup_dirs.sh
$P gen_points.py 10000 2000 > points22k.txt       # 22,007 points
$P gen_points.py 100 0 > points207.txt            # 207 points
$P l11_strip.py                                   # dirs/rec_nopc_nol11
./run_queries.sh points22k.txt p22k && ./run_queries.sh points207.txt p207
$P compare.py out/recommended.p22k.jsonl out/rec_nopc.p22k.jsonl
$P compare.py out/rec_nopc.p22k.jsonl out/rec_nopc_nol11.p22k.jsonl
$P poi_parent_ids.py
$P core_postal_strings.py
$P minimal_poi_remap.py && $W/target/release/examples/grid_query dirs/minimal_poi_remap \
  < points22k.txt > out/minimal_poi_remap.p22k.jsonl
./edge_checks.sh
$P named_table.py
```

`patch_test.sh` needs `geocoder-diff` and `geocoder-patch` built from the
same commit in `bin/` (`builder/tools/ci_build_tools.sh`), and
`patch/<date>/planet/{admin,quality/q2.5,full}` for 2026-10-12 and
2026-10-13 laid out by `fetch_layout.py <date> patch <prefix>...` (with
`--patches` for 2026-10-13, `--link-from $W/idx/recommended` to reuse
files). geocoder-diff borrows the `strings_*.bin` tiers and
`street_ways.bin` from the sibling `full/` (`geocoder_diff.cpp:548-575,
888-925`); the rest of `planet/full` (about 30 GB) is not needed.

`contents/` (`M=$W/idx/smallest`, `R=$W/idx/recommended`,
`GQ=$W/target/release/examples/grid_query`, `ZSTD=$W/zstd157/bin/zstd`):

```sh
$P inventory.py $M $R inventory.json > inventory.out
ZSTD=$ZSTD $P zstd_shares.py $M inventory.json.strcls.npy $W/tmp/zs zstd_shares.json > zstd_shares.out
$P bom.py inventory.json zstd_shares.json $W/data/configurations-2026-10-13.json > bom.txt
$P extra.py $M
bash extra/fetch_extra.sh
$P strings_attr2.py $M inventory.json.strcls.npy extra/poi_all_poi_records.bin \
  extra/no-addresses_street_ways.bin strings_attr2.json
$P ablate3.py $M $R $GQ $W/tmp/ablate3 ablate3.json > ablate3.out
$P diff_examples.py $GQ $M $R $W/tmp/ablate3/coords.txt [CC] [N]
ZSTD=$ZSTD $P prune2.py $M $W/tmp/prune2 prune2.json > prune2.out
$P groups.py $M
$P q25_check.py $M $R
```

## Data and tool versions

- Geocoder data: planet build 2026-10-13 (manifest `version` 17,
  `patch_version` 7) from
  `geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/`,
  as the client presets `admin-minimal q2.5 none` (`idx/smallest`) and
  `admin q2.5 major` (`idx/recommended`); 2026-10-12 → 2026-10-13 for the
  patch test; planet `poi/all/poi_records.bin` and
  `no-addresses/street_ways.bin` of 2026-10-13 for the string attribution.
- `grid_query`, `geocoder-diff`, `geocoder-patch`: repo main `5caed45`.
- GeoNames `cities500.zip`, `admin1CodesASCII.txt`, `admin2Codes.txt`,
  `countryInfo.txt` (downloaded 2026-10-13) and Natural Earth
  `ne_10m_admin_0_countries.geojson` v5.1.2: the files immich-app/base-images
  `server/Dockerfile` adds.
- immich-server `release` (v3.3.1) and tags v3.0.2-v3.3.1, read
  anonymously from ghcr.io; Postgres image
  `ghcr.io/immich-app/postgres:14-vectorchord0.4.3-pgvectors0.2.0`.
- Immich's lookup re-implemented from `server/src/repositories/map.repository.ts`
  (GeoNames path) and the immich-geocoder prototype mapper
  (`geocoding.repository.ts:66-80`).
- zstd 1.5.7 at the publish level (`-18 -T1`); Python 3.12 with numpy;
  `rust:bookworm` with `gcc-aarch64-linux-gnu` for the arm64 build.
