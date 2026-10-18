# Transport codec: columns + byte-stream-split before zstd

A transport-only transform for the published index files: each fixed-stride
struct file is split into columns, and each column is stored raw,
byte-stream-split, zigzag-delta (on the raw bit patterns) or step-from-the-
previous-valid-value, chosen per column, in independently decodable blocks of
about 4 MiB, then zstd -18 as today. The client undoes it right after
download, so it gets today's `.bin` files byte for byte: reader, on-disk
format, patcher and the manifest's raw sha256 stay as they are. Run
2026-10-17 on the box (`/ssd/transport`), re-verified the same night
(`/ssd/transport-verify`).

## Results

Verified (independent driver, rebuilt `tt`, 2026-10-17):
- **Planet maximum set (build 2026-10-13, 38 files): 11,689.5 → 7,747.0 MiB
  (−33.7%)** with the best encoding per column at 4 MiB blocks. Every
  `.tt.zst` re-encoded to the same byte size; all 38 files decode to the
  manifest sha256, also through a separate numpy decoder written only from
  the format description (`verify/npdec.py`, 3 largest files).
- Oceania, the 7 files of the original description: 220.44 → 136.18 MiB
  (−38.2%) with the described encodings, 129.63 MiB (−41.2%) with the
  column search. All 32 Oceania files: 241.44 → 151.11 / 144.15 MiB.
- Blocks cost 0.7-1.2 points on planet (0.8-1.6 on Oceania) against whole
  files. A spec table chosen on planet works on Oceania within +0.1%, so a
  fixed table can ship.
- Client decode, 1 core warm, idle box: 97.3 s with the one-pass record +
  vertex decoder (`tt dec2`) against 112.6 s for plain `zstd -d`; `dec2`
  holds at most ~270k pending vertex counts (about 1 MiB).

Measured once, not re-verified:
- With the canonical sorted order (`../sorted-order`) and a column search
  on it: 6,620.4 MiB (−43.4%).
- Small device decode 495 s against 644 s plain; server zstd CPU about 57%
  lower on the transformed files (`scripts/server_cost.py`).

Against our own on-disk formats (`../format-codecs`, 6,286.7 MiB on disk,
5,717.5 download-only): those win on the geo index, streets and building
outlines through structure; the transform wins on `poi_cells` / entries,
postcode centroids and `admin_polygons` (hybrid about 5,680 MiB, computed).

Suggested staging (from the report): ship `.tt.zst` next to `.zst` now
(old clients keep the plain files), then the canonical sort with v8
identity patches, then the on-disk formats for the disk saving.

## Layout

- `scripts/tt.cpp`: the transform. `tt enc KIND SPEC IN OUT [--block BYTES]
  [--side PATH:STRIDE:CNTOFF]`, `tt dec IN OUT` (streams a `.zst`),
  `tt dec2` (a records file and its vertex file in one pass), `tt cols`
  (every column under every candidate encoding, for the search). The format
  is described at the top of the file.
- `scripts/specs.py`: per-file layout (`KIND`), side records for vertex
  files, the described ("theirs") encodings, the CI zstd thread rule.
- `scripts/colsearch.py`: per-column search (whole file, zstd -18) →
  spec table. `specs/planet_best.json` and `specs/planet_best_sorted.json`
  are the tables it chose on the 10-13 planet set (published and sorted
  order); `run_cfg.py` takes them directly.
- `scripts/run_cfg.py`: transform + zstd -18 + round-trip check for a dir;
  `compare.py`, `groups.py`, `merge_results.py`, `count_cost.py`: tables;
  `compress_plain.py`: the plain baseline; `server_cost.py`: server CPU;
  `enc_day.sh`, `dec_day.sh`, `timed.sh`, `timing_series.sh`, `scope_run.sh`:
  timing; `chain_planet.sh`: the planet run; `fetch.py`: download a region
  selection; `check_layout.py`: checks the struct layouts in `specs.py`.
- `verify/`: the re-check: `enc_set.py` / `plain_set.py` (own drivers),
  `npdec.py` (independent decoder), `dec_set.sh`, `rt_check.py`,
  `phase1.sh`, `phase2.sh`.

The scripts hard-code the box paths (`/ssd/transport`, `/ssd/shrink`).

## Rebuild and run

`tt` links a single-threaded static libzstd 1.5.7 (CI publishes with the
1.5.7 CLI); this command reproduces the measured binary byte for byte:

```sh
curl -sSL https://github.com/facebook/zstd/releases/download/v1.5.7/zstd-1.5.7.tar.gz | tar xz
make -C zstd-1.5.7/lib libzstd.a
g++ -O2 -std=c++17 -Izstd-1.5.7/lib scripts/tt.cpp zstd-1.5.7/lib/libzstd.a -o tt
python3 -I scripts/run_cfg.py SRC_FLAT_DIR OUT specs/planet_best.json 4194304 --jobs 16
```

`specs.py` points `TT` and `ZSTD` at the box binaries; adjust them first.

## Data

Build 2026-10-13, planet / full / uncapped / poi all, flat dir
(`/ssd/shrink/idx/max`), from
`https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/2026-10-13/`
(`configurations.json` gives each file's published `size_zst` and raw
sha256). Oceania: the 2026-10-17 download with the 2026-10-14 manifest.
