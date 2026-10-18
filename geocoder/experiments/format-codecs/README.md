# Format codecs: lossless re-encodings per file family

Lossless (bit-exact, no grids) re-encodings of every large file family of the
maximum-quality and with-addresses presets, keeping random access where the
reader needs it, plus download-only variants. Four tracks ran in parallel on
2026-10-13 (`/ssd/shrink/max/{streets,addresses,index-poi-strings,four-changes}`)
and a verifier combined and re-measured them the same night
(`/ssd/shrink/max/verify`); on 2026-10-14 a single compiled codec (`gc`)
encoded and decoded whole days (`/ssd/shrink/sortpatch/codecs`).

## Results (build 2026-10-13, zstd 1.5.7 `-18 -T16`, verified)

`combined_tables.md` is the verified per-file table for both presets.

| preset | today | four changes (lossless trims) | best lossless on disk | download-only |
|---|---:|---:|---:|---:|
| maximum quality | 11,689.5 MiB | 10,961.9 | **6,286.7** (−46.2%) | 5,717.5 (−51.1%) |
| with street addresses | 10,500.0 MiB | 10,401.1 | **5,726.0** | 5,185.8 |
| with addresses + 11 m grid on q2.5 outlines (lossy) | | 10,388.9 | 5,672.3 | 5,144.1 |

Raw on disk, max: 35.0 → 15.7 GB. The biggest wins:
- **Geo index** (`geo_cells` + street / interp / addr entries) 2,328.8 →
  517.8 MiB: 128-row blocks, spatial way order with delta-coded lists, and
  cell-ordered addresses so address lists become an inline count. Lookups
  2.03 µs vs 2.11 µs today (2M warm lookups).
- **Street pair** 2,424.7 → 1,508.6 (download-only 1,411.4): ways in
  spatial order, groups of 16, per-way second-order node deltas, every node
  the same f32 bit pattern. Interp pair 824.5 → 576.9.
- **addr_vertices** 1,731.7 → 1,014.9 (abs2), **addr_points** 1,528.5 →
  874.9 (byte-planed columns; slower per record), **admin_vertices**
  uncapped 1,330.7 → 725.8 (layout B, same blocks), **poi_records** 387.6
  → 315.7 and **poi_vertices** 523.8 → 433.1.
- The four changes (`four-changes/`): (1) dead weight (unread fields,
  hidden place nodes, unused strings), (2) the place index rebuilt at load,
  (3) layout B for `admin_vertices`, plus dropping the postal boundaries
  every admin-mode set stores twice (`postal_*`, never read): 0 full-JSON
  answer differences over 402,007 points. (4) the 11 m grid (lossy, with
  addresses only) changes 0.378% of answers (0.135% on the verifier's fresh
  sample).
- Answers: every reorder and trim gave 0 full-JSON differences on a fresh
  20k sample; every coordinate encoding round-trips byte for byte over the
  whole file, each through an independent C decoder (`verify/`).
- The tracks' own savings double-count about 400 MiB (mostly the geo index,
  which only the verifier built combined, `verify/gxc.py`); the totals
  above are the measured combination.

Findings along the way: "maximum quality" is not the original data (outlines
spanning 73-730 km on an 11 m grid, buildings on 11 cm and capped at 20
vertices; see `../precision-tiers`); 4.48M duplicate address ids in cell
lists; about 300k orphan street strings; the reader drops unnamed POIs.

**Whole day with `gc`** (`gc-day/`, sorted 2026-10-13 day, verified
2026-10-14): 35.04 GB → 15.86 GB on disk, 6,499.6 MiB after zstd -18;
encode 78 s / decode 64 s warm on one core (small device 1,735 s / 775 s,
read-bound); every decoded file byte-identical, repeat encodes identical.
`geocoder/experiments/stream-lowmem/gc` on `feat/lowmem-streaming-update`
is a later version of this tool (libzstd, threads, streaming patcher);
`gc-day/src` is the version these numbers were measured with.

## Layout

- `streets/`: `waypack.cpp` (packed ways, groups of 16), `nodeenc.cpp` /
  `nodedl.cpp` (node encoders, download-only), `entenc.cpp` (entry lists),
  `wstats.cpp`, `reorder.py` (spatial way order), `sidecars.py`,
  `stats.py`, column experiments, `eval_*.sh` / `runq.sh` (answer checks).
- `addresses/`: `codecs_addr.py` (abs2, columns, pax blocks), `tierb.py`,
  `final_build.py`, `build_sorted.py`, `purge_street.py`, experiments
  (`exp_*.py`, `geo_variants.py`, `pax.py`), `strings_check*.py`, stats.
- `index-poi-strings/`: block geo index (`geo_gx.py`, `gx_check.cpp`),
  `geo_rebuild.cpp` (recomputes the cell lists from geometry with the
  builder's S2 code; needs S2 / abseil from `builder/tools/build_s2.sh`),
  list dedup, POI records / vertices (`poi_rec.py`, `poi_vert*.py`),
  strings (purge, front coding), `layouts.py`.
- `four-changes/`: `build1.py`, `layoutB4.py`, `layouts_stack.py`,
  `postal.py`, `census.py`, `check1.py`, `compare4.py`, `sizes4.py`,
  `bench_b.c`.
- `verify/`: independent C decoders (`wdec.c` streets, `abs2dec.c`
  addresses, `bopendec.c` POI vertices, `entdec.c`, `gxdec.c`, `gxcdec.c`,
  `layoutb.c`, `gxcbench.c`), the combined geo index `gxc.py`, `combine.py`
  (→ `combined_tables.md`), and the job scripts `j_*.sh`.
- `gc-day/`: `src/` (the `gc` codec), `enc_day.sh` / `dec_day.sh` /
  `cmp_day.sh` (whole day), `parity.sh` (byte parity with the tracks'
  encoders), `sizes.sh` / `sizes_table.py`, timing (`timed.sh`,
  `run_timings*.sh`, `timing_table.py`), order experiments.

The scripts hard-code the box paths (`/ssd/shrink/...`).

## Rebuild and run

```sh
g++ -O2 -o waypack streets/waypack.cpp && g++ -O2 -o entenc streets/entenc.cpp
g++ -O2 -o nodeenc streets/nodeenc.cpp && g++ -O2 -o nodedl streets/nodedl.cpp
for c in verify/*.c; do gcc -O2 -o $(basename $c .c) $c -lm; done
g++ -O2 -std=c++17 -o gx_check index-poi-strings/gx_check.cpp
g++ -O2 -std=c++17 -ffp-contract=off -o gc gc-day/src/gc.cpp
ln -s gc gc-encode && ln -s gc gc-decode
./gc-encode DAY/planet/full ENC/planet/full && ./gc-decode ENC/planet/full DEC/planet/full
```

`-ffp-contract=off` matters for `gc`: fused float operations change how the
address bbox prediction rounds.

## Data and tool versions

- Build 2026-10-13 from
  `https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/2026-10-13/`:
  `idx/max` (planet full, uncapped, POI all) and `idx/withaddr` (planet
  full, q2.5, POI all); `gc-day` ran on the canonical sorted 2026-10-13 day
  (`../sorted-order`).
- Reader checks with `grid_query` and builder code (`segment_rect.h`,
  `s2_helpers.cpp`) from main at 5caed45.
