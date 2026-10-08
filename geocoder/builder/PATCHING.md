# Geocoder Incremental Patch System

## Goal

Produce small patch files nightly so users can update their geocoder index without
re-downloading the full dataset. A user starting from any build can apply patches
in sequence and arrive at output **byte-identical** to a fresh build.

## Current Status

### What Works
- **Deterministic builds**: Same PBF always produces byte-identical output (verified on Germany, Europe, and Planet)
- **Single-patch application**: Verified on both Europe and Planet — all 14 files byte-identical
- **Sequential patching**: Verified on planet with 3 distinct weekly snapshots (Mar 9 → Mar 16 → Mar 23) — **PASS**; with the v5 tools on three daily planets (Oct 4 → 5 → 6), every variant applied stacked in a client-shaped tree — **PASS** (126/126)
- **Custom patch format**: Merge sequences, string-level diffs, parent-aware coordinate merges, secondary ID matching, cell corrections — fully custom diff/apply logic (only zstd for transport compression)

### What Needs Work
- **Code cleanup**: Legacy dead code block in geocoder_patch.cpp, prototype files

## Tested Patch Sizes

### One day on planet (2026-10-05 → 10-06)

Compressed `.gcpatch` bytes a client downloads for one day of OSM edits, per
selection (sum of its variant dirs). Every patch rebuilds the new day
byte-identically, alone and stacked on the previous day's patch. "Before"
is the v4 tools on the same data; v6 is GCPATCH v6 with the builder of that
time (admin-minimal keeps stable slots, unshipped POI names stay out of the
pool); v7 is GCPATCH v7 (every variant dir patches alone) with POI
tombstones kept to the tiers that held the record.

| Selection | Before | v5 | v6 | v7 |
|-----------|--------|----|----|----|
| planet full + q2.5 + poi/all | 173.5 MiB | 4.76 MiB | 4.52 MiB | **4.49 MiB** |
| planet full + uncapped + poi/all | 253.8 MiB | 5.56 MiB | 5.32 MiB | **5.28 MiB** |
| planet admin + q2.5 + poi/major | 65.6 MiB | 0.45 MiB | 0.45 MiB | **0.35 MiB** |
| planet admin-minimal | 48.2 MiB | 0.61 MiB | 0.14 MiB | **0.12 MiB** |
| europe full + q2.5 + poi/all | 74.5 MiB | 2.16 MiB | 2.05 MiB | **2.04 MiB** |
| europe admin + q2.5 + poi/major | 28.8 MiB | 0.15 MiB | 0.15 MiB | **0.13 MiB** |
| north-america no-addresses + q2.5 | 9.6 MiB | 0.47 MiB | 0.43 MiB | **0.43 MiB** |
| all 126 variant dirs (what CI uploads) | 1094 MiB | 20.9 MiB | 18.8 MiB | **18.3 MiB** |

Most of the old bytes were bookkeeping the patcher can derive:
- offset fixups travel as runs of one shift (one edit shifts every later
  node_offset / vertex_offset by the same amount)
- place nodes and postal polygons merge instead of being re-sent whole
- the postcode centroid cell index, the admin / POI / place corrections and
  (v6) the street / addr / interp entry corrections travel as per-cell lost /
  gained ids
- a replaced record keeps its unchanged geometry, an edited one keeps its
  unchanged head and tail
- far merge jumps need a run of matches behind them (duplicates no longer
  re-send thousands of records); POI entries remap their interior flag

The tables below predate these changes.

### Europe (7 GiB dataset)

| Gap | Patch Size | Per Day | All Match? |
|-----|-----------|---------|------------|
| 6 days (Mar 21→27) | **32 MiB** | **~5.3 MiB** | YES — all optimizations + secondary matching, all 14 MATCH |
| 6 days (Mar 21→27) | 34 MiB | ~5.7 MiB | YES — before secondary matching |

### Planet (17 GiB dataset)

| Gap | Patch Size | Per Day | All Match? |
|-----|-----------|---------|------------|
| 7 days (Mar 16→23) | **73 MiB** | **~10.4 MiB** | YES — all optimizations + secondary matching, all 14 MATCH |
| 7 days (Mar 16→23) | 77 MiB | ~11 MiB | YES — delta fixups, before secondary matching |
| 7 days (Mar 9→16) | 160 MiB | ~23 MiB | YES — without delta fixups |
| Sequential (Mar 9→16→23) | **74+73 MiB** | N/A | **PASS** — both steps byte-identical, all optimizations |
| Sequential (Mar 9→16→23) | 160+159 MiB | N/A | PASS — both steps byte-identical (old diff, without delta fixups) |

### Per-File Breakdown (Planet 7-day gap, Mar 16→23, with secondary matching)

| Component | Raw Size | Notes |
|-----------|---------|-------|
| **street_nodes merge** | **264 MiB** | **Parent-aware, 6.65% of file** |
| Way fixups (47.7M, delta-encoded) | 39 MiB | node_offset fixes, varint delta |
| street_entries corrections (1.59M cells) | 25.7 MiB | Down from 2.03M (22% fewer with secondary matching) |
| admin_vertices merge | 16 MiB | Parent-aware, 2.34% of file |
| admin_polygons merge + fixups | 7.4 MiB | 0.93% + vertex_offset fixes |
| addr_points merge | 5.9 MiB | 0.25% of file |
| street_ways merge | 5.1 MiB | 0.92% of file |
| addr_entries corrections (95K cells) | 2.5 MiB | Down from 116K (18% fewer) |
| Flag corrections (224K cells) | 1.9 MiB | |
| Cell changes (184K added, 46K removed) | 1.8 MiB | |
| Secondary remap (119K pairs) | 949 KB | 55K ways + 59K addrs + 4.6K admin |
| admin_entries corrections (4.6K cells) | 245 KB | Down from 193K (**97% fewer** — country_code hash fix) |
| String diff (+10.7K, -4.5K) | ~200 KB | |
| interp + other | ~8 KB | |
| **Total uncompressed** | **404 MiB** | |
| **Compressed (zstd transport)** | **73 MiB** | All 14 files verified MATCH |

### Per-File Breakdown (Europe 6-day gap, Mar 21→27, latest approach)

| Component | Raw Size | Notes |
|-----------|---------|-------|
| street_nodes.bin merge | 108 MiB | Parent-aware, 7.49% of file |
| admin_vertices.bin merge | 7.1 MiB | Parent-aware, 2.44% of file |
| Way fixups (19.4M, delta-encoded) | 19 MiB | node_offset fixes |
| street_entries corrections (549K cells) | 9.1 MiB | After secondary matching (was ~700K+ before) |
| addr_points.bin merge | 4.4 MiB | 0.32% of file |
| street_ways.bin merge | 2.7 MiB | 1.16% of file |
| addr_entries corrections (64K cells) | 1.7 MiB | After secondary matching |
| Cell flag corrections (83K cells) | 749 KB | has_street/has_addr/has_interp |
| Secondary remap (87K pairs) | 696 KB | Recovers IDs for geometry-changed records |
| Cell changes (59K added, 13K removed) | 571 KB | |
| admin_polygons.bin merge + fixups | 544 KB | 0.88% + vertex_offset fixes |
| String diff (+4152, -2396) | ~60 KB | |
| admin_entries corrections (1.8K cells) | 55 KB | |
| interp merge + fixups | ~14 KB | |
| **Total uncompressed** | **~167 MiB** | |
| **Compressed (zstd transport)** | **32 MiB** | |

## Architecture

```
build-index (deterministic) → geocoder-diff old/ new/ → patch.gcpatch
geocoder-patch old/ patch.gcpatch → new/  (must be byte-identical to fresh build)
```

### Tools
- **build-index** (`src/build_index.cpp`): Builder with deterministic ordering
- **geocoder-diff** (`tools/geocoder_diff.cpp`): Compares two builds, produces `.gcpatch`
- **geocoder-patch** (`tools/geocoder_patch.cpp`): Applies patch, produces new build
- **patch_format.h** (`tools/patch_format.h`): Shared format definitions + rebuild functions

### How Patching Works

**Diff tool:**
1. Build string-level diff (walk both sorted pools, emit added/deleted strings)
2. Build string remap (old string pool offsets → new, derived from string diff)
3. For each data file: apply string remap to old, match records by content hash (for ways/admin: also fix node_offset/vertex_offset fields), build merge sequence (MATCH/INSERT/DELETE at record stride)
4. For coordinate files (nodes/vertices): derive merge from parent way/polygon merge — verify node blocks match before COPY, INSERT if nodes changed
5. Derive ID remaps from merge sequences (MATCH ops define old→new record correspondence)
6. **Secondary matching**: For DELETE/INSERT records in merge sequences, match by relaxed key (name + node_count for ways, street + housenumber for addrs, etc.) to recover ID mappings for modified-but-same-entity records
7. Rebuild entry files from enhanced ID remaps + cell changes
8. Compare derived entries with new entries cell by cell — include differing cells as corrections
9. Compare cell flags (has_street/has_addr/has_interp) between old and new — include differences
10. Package everything, compress whole patch with zstd for transport

**Patch tool:**
1. Decompress zstd
2. Read the strings section → rebuild each shipped tier from the dir's own old tier + additions/deletions and derive its remap runs; take the sent runs of unshipped tiers and the cross-tier pairs as they come
3. For each data file: apply string remap + offset fixups to old, replay merge sequence → output file, track ID mapping from MATCH ops
4. Read secondary ID remaps → merge into derived ID mappings
5. Read cell changes (added/removed cell IDs) and flag corrections
6. Rebuild entry files from enhanced ID mappings + cell changes
7. Apply cell-level entry corrections (replace specific cells' data)
8. Rebuild geo_cells/admin_cells from corrected entries + flag corrections

## Known Issues

### 1. Cell correction overhead (PARTIALLY FIXED)

**Impact**: ~29 MiB for planet (down from ~42 MiB), ~11 MiB for Europe (down from ~15 MiB).

**Status**: Two improvements applied:
- **Secondary ID matching** recovers 119K additional ID mappings on planet (55K ways, 59K addrs, 4.6K admin). Reduced street corrections 2.03M → 1.59M (22%), addr corrections 116K → 95K (18%).
- **Admin hash bug fix** (was reading padding byte instead of country_code). Admin corrections 193K → 4.6K cells (**97% reduction**).

**Remaining cause**: 1.59M street cells (25.7 MiB) still need corrections. These are from records that genuinely differ (geometry changes where name + node_count also changed, truly new/removed entities). Further reduction would require OSM-level entity tracking.

### 2. Way fixup table size (FIXED)

**Impact**: 19-39 MiB of delta-encoded fixup data per patch.

**Status**: Delta encoding with varint compression reduces raw fixup size 3-4x (e.g., 148 MiB → 39 MiB for Europe).

### 3. street_nodes.bin still 6-7%

**Impact**: 108-264 MiB of merge data.

**Problem**: Parent-aware merge reduced this from 33% to 7%, but it's still the largest single component. The remaining 7% is from ways whose header matches (byte-identical) but whose actual node coordinates changed (geometry edits in OSM).

**Potential fix**: None needed — this represents actual data changes, not algorithm inefficiency.

### 4. Struct padding non-determinism (FIXED)

**Status**: Fixed by adding explicit padding fields to `AdminPolygon` and `InterpWay` structs.

### Each variant dir patches alone

A client holds some variant dirs (a mode dir, maybe a quality and a poi dir)
and patches each one from its own files only: the patcher never reads
outside the dir it is given, and CI applies every patch to an isolated copy
of the old dir (`tools/link_client_files.sh`) to prove it. Every dir of a
build shares one global string layout, so each tier's offsets are the sum of
the tier sizes before it, which the patch carries.

- **Shipped tier**: a `strings_<tier>.bin` the old and new dir both hold
  (empty files count). The patch sends its string diff and size and hash
  stamps; the patcher rebuilds it and derives its remap runs by walking the
  old and new pool, checking each looked-up offset is a string start. A tier
  only the new dir holds is newly shipped: it counts as unshipped in the
  strings section, and its file comes whole in a full-replacement section.
- **Referenced tier**: a tier that some string field of the dir's own old
  files points into (the fields the patcher remaps, `string_field_offsets`
  and the sparse files of remap kind 2 and 3).
- **Sent runs**: for a referenced tier the dir doesn't ship, the diff walks
  the tier itself and sends the shift runs. The patcher looks them up without
  a string-start check: every stored reference is a string start or NO_DATA
  (`partition_strings_into_tiers`), and the daily byte-identity check proves
  it.
- A tier neither shipped nor referenced sends nothing.

Dirs patched on different days don't fit together: the server combines a
mode dir with a quality or poi dir by global string offsets, so the selected
dirs must all be patched to the same date and swapped in together.

## Patch Format (.gcpatch, version 7)

Whole file is zstd-compressed for transport. Internal structure (all integers
little-endian; the header, client files, old file sizes and strings sections
are read by position, everything after them by marker):

```
Header: "GCPATCH\0" (8) + version=7 (u32) + flags=0 (u32)

Client Files: marker 0xFFFFFFF2 (u32) + n (u32)
  + n × {name_len:u16, name, size:u64, inline:u8, [bytes] if inline}
  Every file the new variant dir holds except *.osm_ids, *.gcpatch, *.zst and
  dotfiles, sorted by name. JSON files (strings_layout.json, poi_meta.json)
  are inline and written verbatim. The patcher builds anything not listed in
  its scratch dir, and every listed file must exist at its listed size.

Old file sizes: n (u32) + n × {file_id:u32, old_size:u64}
  (the old files no section names, UNSECTIONED_OLD_FILES: the entry
  pipeline's cell indexes and the entries file beside a cell list delta;
  0 = absent. The patcher checks them first.)

Strings: marker 0xFFFFFFF6 (u32)
  + 5 × {old_size:u32, new_size:u32} (core, street, addr, postcode, poi: the
  build's tiers, which place every global offset)
  + shipped (u32, bit per tier the dir rebuilds; each must be a listed
  client file, and a listed tier without its bit comes in a per-file section)
  + sent (u32, bit per unshipped tier the dir's old files reference)
  + per tier in order:
    shipped: old_hash:u64, new_hash:u64, n_added:u32, n_deleted:u32,
      [string\0] × n_added, [index:u32] × n_deleted (the patcher refuses an
      old tier of another size or hash and checks the one it rebuilds)
    sent: n_runs:u32, runs_size:u32, runs of varint (start - previous end,
      end - start, zigzag change of the shift), from the tier's old base
  + n_pairs (u32) + [(old_off:u32, new_off:u32)] × n_pairs (strings that
  changed tier, out of a shipped or referenced tier)

Parent-id remap: marker 0xFFFFFFF3 (u32)
  + n_admin (u32) + [(old:u32, new:u32)] × n_admin
  + n_street (u32) + [(old:u32, new:u32)] × n_street + 0 (u32, reserved)

Per-file section: file_id (u32) + stride (u32) + old_size (u64) + new_size (u64) + ...
  (the patcher refuses an old file of another size, 0 meaning absent)
  stride=0: full replacement (n_fixups=0 u32, size u64, data)
  stride=0xFD: unchanged, copy from cur_dir
  stride=0xFC: sparse delta (value_stride, remap_kind, n, [(pos, value)] × n)
  stride=0xFB: cell list delta for <name>_cells.bin + <name>_entries.bin
    (payload_size u64, new entries size u64, n_removed u32 + [cell u64],
    n_set u32 + [cell u64, n_lost u32, n_gained u32, lost ids, gained ids])
  otherwise: n_runs (u32) + n_values (u32) + runs_size (u32) + values_size (u32)
    + runs + values + seq_size (u64) + merge ops
  Ops: MATCH(count:u32) | INSERT(count:u32, data) | DELETE(count:u32)
  Offset fixups (node_offset / vertex_offset of MATCH records, see
  OffsetFixups in tools/patch_format.h): runs of varint (records skipped since
  the previous run, run length, zigzag change of the shift); the patcher adds
  the run's shift to every old offset in it except NO_DATA ones. values lists
  the NO_DATA records given an offset as varint (index delta, offset).

Cell Changes: marker 0xFFFFFFFB/FA/F5/F4 (geo/admin/poi/place) + n_added (u32)
  + n_removed (u32) + [cell_id:u64] × n_added + [cell_id:u64] × n_removed

Secondary ID Remap: marker 0xFFFFFFF6 (u32) + n_files (u32)
  per file: file_id (u32) + n_pairs (u32) + [(old_id:u32, new_id:u32)] × n_pairs
  Recovers old→new mappings for modified records (matched by relaxed key)

Geo Entry Deltas (street / addr / interp entries): marker 0xFFFFFFF0 (u32)
  + file_id (u32) + count (u32), then per cell in cell order:
  cell_id (u64) + n_lost (u16) + n_gained (u16) + [id:u32] × n_lost
  + [id:u32] × n_gained, turning the list the patcher derives (old list,
  ids remapped) into the new one. n_lost 0xFFFF: the gained ids are the
  whole new list.

Entry Corrections: marker 0xFFFFFFF8 (u32) + file_id (u32) + count (u32)
  + [(cell_id:u64, entry_count:u16, [id:u32] × entry_count)] × count

Cell Index Delta: marker 0xFFFFFFF7 (u32) + file_id (u32, the entries file)
  + payload_size (u64) + the cell list delta from the rebuilt admin / POI /
  place index to the new one (same layout as stride=0xFB); replaces Entry
  Corrections for that file

End marker: 0xFFFFFFFF (u32)
```

## Performance

| Operation | Europe | Planet |
|-----------|--------|--------|
| Build (deterministic) | ~12 min | ~14 min |
| Diff generation | **3m40s** | **15m22s** |
| Patch application, 1 core, cold cache (`full`, one day) | **81s** | **241s** |
| Patch peak anonymous memory | **5 MiB** | **12 MiB** |
| Patch memory limit tested (page cache included) | 1 GiB | 1 GiB |

## TODO (Priority Order)

### Completed
1. ~~Sequential planet test~~ — **PASS** (3 distinct weekly snapshots, old diff tool)
2. ~~Parent-aware coordinate merge~~ — **DONE** (street_nodes 33%→7%, admin_vertices 210%→2.4%)
3. ~~String-level merge~~ — **DONE** (83 MiB → ~100 KB)
4. ~~Planet test with optimizations~~ — **DONE** (852 MiB → 186 MiB)
5. ~~Build determinism~~ — **PASS** (Germany, Europe, Planet — all verified)
6. ~~Eliminate explicit string remap~~ — **DONE** (derived from string diff)
7. ~~Struct padding fix~~ — **DONE** (explicit padding in AdminPolygon/InterpWay)
8. ~~Addr_points dedup~~ — **DONE** (4.4M duplicates on planet, fixed non-determinism)
9. ~~Delta-encode fixup tables~~ — **DONE** (148 MiB → 39 MiB for Europe, Europe patch 68→34 MiB)
10. ~~Run planet with delta fixups~~ — **DONE** (160 MiB → 77 MiB, all 14 MATCH)
11. ~~Secondary ID matching~~ — **DONE** (recovers ~87K additional ID mappings on Europe, reduces corrections)
12. ~~Fix admin polygon country_code hash~~ — **DONE** (was reading padding byte instead of country_code)

### Must Do
- ~~Sequential test with optimized diff on planet~~ — **PASS** (160 MiB + 159 MiB, both steps byte-identical)
- ~~Sequential test with all optimizations on planet~~ — **PASS** (74 + 73 MiB, both steps all 14 MATCH)
- **Verify patched planet still serves correct data** — Spot-check geocoding results against fresh build

### Should Do (Patch Size)
- ~~Run planet with secondary matching~~ — **DONE** (77→73 MiB, street corrections 2.03M→1.59M, admin corrections 193K→4.6K)
- **Further reduce cell corrections** — Remaining 1.59M street corrections on planet (25.7 MiB). Would need OSM-level entity tracking for further improvement.

### Nice to Have (Performance)
- **Parallel merge building** — Currently sequential across files
- ~~Streaming patch application~~ — **DONE** (the decompressed patch is mmapped from scratch; records, cell indexes, remaps and deltas stream)
- **Code cleanup** — Remove legacy dead code block in geocoder_patch.cpp, prototype files

### Future Consideration (Format Changes)
- **Inline node storage** — Store way nodes inline instead of separate flat array. Eliminates coordinate cascade entirely. Requires server format change.
- **Content-addressed record IDs** — Hash-based IDs instead of sequential indices. Makes entry files stable across builds. Requires server format change.
- **Fixed-size cell slots** — Pre-allocate per cell. Analysis showed 2-6x space waste due to skewed distribution.

## File Inventory

### Core files (committed)
- `src/build_index.cpp` — Deterministic ordering
- `src/types.h` — Explicit struct padding
- `src/s2_helpers.cpp` — Zero padding in AdminPolygon creation
- `tools/geocoder_diff.cpp` — Diff tool
- `tools/geocoder_patch.cpp` — Patch tool
- `tools/patch_format.h` — Shared format definitions + rebuild functions
- `tools/geocoder_canonicalize.cpp` — Standalone canonicalize (testing tool)
- `CMakeLists.txt` — Build targets

### Test data on Node 3 (/home/michtest/)
- `planet-A/` — Built from planet-260309 (Mar 9), 17 GiB
- `planet-B/` — Built from planet-260316 (Mar 16), 17 GiB
- `planet-C/` — Built from planet-260323 (Mar 23), 17 GiB
- `det-A/` — Europe built from Mar 21 PBF, 7 GiB
- `det-today/` — Europe built from Mar 27 PBF, 7 GiB

### Validated patches (saved for regression testing)
- `validated-patches/europe-mar21-to-mar27-v2.gcpatch` — 32 MiB, all 14 MATCH
- `validated-patches/planet-mar09-to-mar16-v2.gcpatch` — 74 MiB, all 14 MATCH
- `validated-patches/planet-mar16-to-mar23-v2.gcpatch` — 73 MiB, all 14 MATCH
- Sequential test validated: A + patch_AB → B (MATCH), patched-B + patch_BC → C (MATCH)
