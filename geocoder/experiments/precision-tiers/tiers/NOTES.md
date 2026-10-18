# Precision tiers: boundaries and buildings (planet-261006)

Built from the exact dumps in `/ssd/precision/build/dumps`, which are aligned record for record with `out-nocap/planet`. Tool: `bin/tiers` (source `src/tiers.cpp`; its header comment documents every command and both formats). Compile with `g++ -O2 -march=native -std=c++17 -I /ssd/precision/repo/geocoder/builder/src`.

## Kept encodings (`work/keep/`)

All of these round-trip byte for byte against their input.

| file | content |
|---|---|
| `boundaries_maximum.pb7` + `.polys` | 0.1 m DP rings snapped to the global 1e-5 grid, with consecutive duplicates dropped (`tiers snap ... 100`); PB7 with `--order2` |
| `boundaries_original_dp.pb7` + `.polys` | exact 0.1 m DP rings (`dumps/admin_dp.e7`); PB7 with `--order2` |
| `boundaries_original.pb7` + `.polys` | exact rings with no simplification (`dumps/admin_raw.e7`); PB7 with `--order2` |
| `buildings_maximum.ab7` + `.i2` | uncapped outlines snapped to the global 1e-6 grid; AB7 with `--planes` and the 64-point group index |
| `buildings_original.ab7` + `.i2` | uncapped exact outlines; AB7 with `--planes` |

The `.polys` files are today's 24-byte `AdminPolygon` records. In each one, `vertex_offset` is the PB7 block offset, `vertex_count` includes the closing vertex, and `area` is recomputed with the builder's `polygon_area` from the tier's doubles.

## Regenerating

- `admin_run.sh`: snap, the today's-format encoding-3 dirs, the PB7 variants with `pb-dec` + `cmp`, and gc layout B of today with a round trip.
- `bldg_run.sh`: the AB7 variants with `ab-verify`, today's-format Maximum, and gc abs2 of base and nocap with round trips.
- `bldg_sizes.sh` and `zs.sh`: zstd 1.5.7 `-18 -T16` whole-file sizes.

## Reader tests

The reader is `target/release/examples/grid_query`, built from `/ssd/precision/repo`.

- **Flat dirs:** `flat/base` and `flat/nocap` hardlink full + poi/all + quality/uncapped.
- **Variants:** each one copies `flat/nocap` and swaps in `admin_*` (from `today-admin`) or `addr_*` (from `today-bldg`). The variant dirs were deleted afterwards.
- **Point pools:** `pts/`.
  - `p200k`, `places` and `random` come from `/ssd/shrink/grid/pts`.
  - `L{2,4,6,8,10}` come from `gen_borders.py`: within ±50 m of the exact DP borders.
  - `bT`, `bA` and `bG` come from `gen_bldg_pts.py`: within ±15 m of building edges.
  - `near_mask.py` limits the exact building variant to cells near the pools, because of the 4 GiB u32 offsets.
- **Answers:** `q/<config>.<pool>.jsonl` from `runq.sh`.
- **Comparisons:** `out/*.md` from `compare.py`.
- **Building distance check:** `out/bldg_dist.md` from `bldg_dist.py`, in f64 against the exact outline.

Config names:

| config | what it is |
|---|---|
| `a` | today's uncapped tier |
| `b` | Maximum, as encoding 3 |
| `c` | exact DP rings, as encoding 3 |
| `d` | exact raw rings, as encoding 3 |
| `k20` | out-base (cap 20) |
| `unc` | out-nocap |
| `mx` | Maximum buildings in today's format |
| `cx` (`cxTG`, `cxA`) | exact buildings near the pools |
