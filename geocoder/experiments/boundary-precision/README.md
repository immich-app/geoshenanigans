# Boundary precision: size vs accuracy of admin boundaries

How small can the admin boundaries (and the rest of the minimal set) get, and
what does each step cost in answers, measured with the real reader
(`grid_query`) against the uncapped boundaries as the truth? Run 2026-10-13
on the box (`/ssd/shrink/{vertices,places-strings-index,stack,grid}`), build
2026-10-13; each track was re-checked by a verifier the same day.

## Results

**Lossless re-layout of `admin_vertices.bin`** (`vertices/`, `stack/`):
today each polygon stores absolute u16 offsets from its bbox corner, which
zstd barely compresses (3.98 B/vertex). Layout B keeps every polygon block at
the same offset and size (random access kept, `admin_polygons.bin`
unchanged) but stores wrapping zigzag deltas as byte planes; Bg adds a
per-polygon divisor in the header pad byte. Minimal set (q2.5, L2-8):
144.9 MiB → about 107 MiB with B (−38 MiB); every layout decodes back to
today's file byte for byte (`stack/roundtrip.sh`).

**Minimal-set stack, measured** (`stack/`, verified): dead-weight package +
place index rebuilt at load + 11 m grid + layout B = 144.76 MiB download
(156.97 MiB gzip image layer, 293 MiB raw), not the ~118 MiB first
estimated, because B stores the snapped deltas as multiples of 10 / 100 and
misses the snap; B + per-polygon divisor = **120.72 MiB** (129.46 layer);
download-only layout A = 114.61 MiB. Only the 11 m snap changes answers
(0.035-0.056% of photo-like points, about 1% near borders); 7,675 tiny
admin rings (median 21 m²) collapse to zero area. Queries are not slower.
- Dead-weight package (`stack/build_v11.py`, builder-only, reader
  unchanged, 0 of 200k answers differ): unread `PlaceNode.parent_poly_id`,
  hidden place nodes, strings nobody references, duplicate admin cell lists;
  about −20 MiB. The place cell index can be rebuilt at load in 0.46 s on
  one core, byte-identical (`stack/rbench`), −10.5 MiB.

**Accuracy against the truth** (`grid/`, verified with fresh points,
`grid/report_table.md` has every configuration). Flip rate of the
Immich fields (city / state / country) for points within ±200 m of
country (L2), state (L4) and city (L8) borders; L2-8 outlines in layout Bg,
zstd -18:

| configuration | L2-8 Bg MB | L2 / L4 / L8 border flips |
|---|---:|---|
| q2.5 + 11 m grid (today's minimal tier + snap) | 80 | 39.2 / 21.5 / 4.5% |
| q1 (today) | 173 | 26.9 / 11.0 / 1.7% |
| uniform 37.5 m + 11 m grid | 101 | 2.9 / 2.6 / 2.6% |
| uniform 22 m + 11 m grid | 127 | 1.7 / 1.6 / 1.5% |
| uniform 11 m + 11 m grid | 165 | 1.0 / 0.9 / 0.9% |
| uncapped + 11 m grid | 282 | 0.5 / 0.4 / 0.6% |
| uncapped (truth) | 504 | 0 |

- The per-level tolerances of the q tiers (q2.5: L2 1250 m … L8 37.5 m) are
  the dominant error, not the grid: one uniform tolerance at every level
  beats the whole q ladder at the same size. So don't scale grid or
  tolerance by level.
- Away from borders the differences are small: on the place, random-land
  and 200k photo-like pools every configuration changes at most 0.54% of
  the Immich answers (q2.5).
- The 11 m snap alone barely simplifies the original data (4.7% of
  vertices merge). Collapse tests must use the integer shoelace on grid
  coordinates.
- Topology-aware storage (each shared border once) saves about 30% more on
  top of either layout, but needs new builder, reader and patch code.

## Layout

- `vertices/`: the minimal set's `admin_vertices.bin`: `decode.py`,
  `stats.py`, `encode*.py` (lossless layouts, local deltas, variants),
  `share.py` / `topo.py` / `edges.py` (shared borders, TopoJSON-style arcs),
  `topo_uncapped.cpp` (the same on the uncapped set), `variant.cpp` (lossy
  variants: snap, floors, scale), `gen_points.py` / `gen_places.py` (border
  and place query points), `runq.sh`, `compare.py`, `compress.sh`,
  `mkvariants.sh`, `stack.sh`.
- `places-strings-index/`: the minimal set's place nodes, strings and cell
  indexes (contents, dead data, packages A / B / C, place index rebuild
  benchmarks; `cover_bench.cpp` and `place_cell_bench.cpp` need S2 and
  abseil, see their build scripts). `common.py` is imported by
  `stack/build_v11.py`.
- `stack/`: the measured stack: `build_v11.py` (dead-weight package as real
  files), `layouts.py` (transfer layouts A / An / B / Bg with decoders),
  `roundtrip.sh`, `check_compose.py`, `restore_check.sh`, `sizes.py`,
  `runq.sh`, `compare.py`, `rbench/` (load-time place index rebuild with the
  server's s2 crate); `stack/verify/` is the verifier's checks.
- `grid/`: accuracy vs the truth: `gridvar.cpp` (boundary variants in
  today's format: uniform re-simplification and grid snap, using the
  builder's own geometry headers), `bgcodec.cpp` (layout Bg in C++ for the
  uncapped-size files, byte-identical to `layouts.py`), `gen_borders.py`
  (±200 m border points from the truth rings), `build_vars.sh`, `runq.sh`,
  `compare.py`, `sizes.py`, `levels.py`, `enc_hist.py`, `report.py`
  (→ `report_table.md`), `download.sh`; `grid/verify/` is the verifier's
  re-run (fresh points, ring degeneracy, area flips, duplicates).

The scripts hard-code the box paths (`/ssd/shrink/...`).

## Rebuild and run

The C++ tools include the builder's headers (`geometry.h`, `vertex_pack.h`,
`types.h`) from main at 5caed45:

```sh
I=geocoder/builder/src
g++ -O3 -march=native -std=c++17 -fopenmp -I$I grid/gridvar.cpp -o gridvar
g++ -O3 -std=c++17 grid/bgcodec.cpp -o bgcodec
g++ -O3 -std=c++17 -fopenmp -I$I vertices/variant.cpp -o variant
g++ -O3 -march=native -std=c++17 -fopenmp -I$I vertices/topo_uncapped.cpp -o topo_uncapped
cargo build --release --manifest-path stack/rbench/Cargo.toml
bash grid/download.sh && bash grid/build_vars.sh
bash grid/runq.sh T=/ssd/shrink/idx/max q25_g4=sets/q25_g4 unc_g4=sets/unc_g4
python3 grid/compare.py T q25_g4 unc_g4
```

## Data and tool versions

- Build 2026-10-13 from
  `https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/2026-10-13/`:
  planet admin-minimal q2.5 (`idx/smallest`), admin q2.5 POI major
  (`idx/recommended`), the max set with uncapped boundaries (`idx/max`, the
  truth), the with-addresses set (q2.5 boundaries, `idx/withaddr`) and the
  `planet/quality/{q0.2,q0.5,q1,q1.5,q2}` dirs.
- `grid_query` and the builder headers: main at 5caed45. zstd 1.5.7
  `-18 -T1` (the publish setting) for every size.
