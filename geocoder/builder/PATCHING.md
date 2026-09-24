# Geocoder Incremental Patch System

## Goal

Produce small patch files nightly so users can update their geocoder index without
re-downloading the full dataset. A user starting from any build can apply patches
in sequence and arrive at output **byte-identical** to a fresh build.

## Current Status

### What Works
- **Deterministic builds**: Same PBF always produces byte-identical output (verified on Germany, Europe, and Planet)
- **Single-patch application**: Verified on both Europe and Planet — all 14 files byte-identical
- **Sequential patching**: Verified on planet with 3 distinct weekly snapshots (Mar 9 → Mar 16 → Mar 23) — **PASS**
- **Custom patch format**: Merge sequences, string-level diffs, parent-aware coordinate merges, secondary ID matching, cell corrections — fully custom diff/apply logic (only zstd for transport compression)

### What Needs Work
- **Sequential test with optimized diff**: The sequential test used the old diff tool; need to re-run with all optimizations (delta fixups + secondary matching)
- **Code cleanup**: Legacy dead code block in geocoder_patch.cpp, prototype files

## Tested Patch Sizes

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
2. Read string-level diff → reconstruct new string pool from old + additions/deletions, derive remap
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

## Patch Format (.gcpatch, version 2)

Whole file is zstd-compressed for transport. Internal structure:

```
Header: "GCPATCH\0" (8) + version=2 (u32) + flags=0 (u32)

String Remap: marker 0xFFFFFFFE (u32) + count=0 (u32)
  (remap derived from string diff, explicit table no longer needed)

Per-file merge: file_id (u32) + stride (u32) + old_size (u64) + new_size (u64)
  + n_fixups (u32) + [(record_idx, new_offset_value)] × n_fixups
  + seq_size (u64) + merge_ops
  Ops: MATCH(count:u32) | INSERT(count:u32, data) | DELETE(count:u32)
  stride=0: full replacement (no fixups, data follows directly)

String Diff: marker 0xFFFFFFF7 (u32) + n_added (u32) + n_deleted (u32)
  + [string_data\0] × n_added + [deleted_index:u32] × n_deleted

Cell Changes: marker 0xFFFFFFFB/0xFFFFFFFA (u32) + n_added (u32) + n_removed (u32)
  + [cell_id:u64] × n_added + [cell_id:u64] × n_removed

Secondary ID Remap: marker 0xFFFFFFF6 (u32) + n_files (u32)
  per file: file_id (u32) + n_pairs (u32) + [(old_id:u32, new_id:u32)] × n_pairs
  Recovers old→new mappings for modified records (matched by relaxed key)

Cell Flag Corrections: marker 0xFFFFFFF9 (u32) + count (u32)
  + [(cell_id:u64, flags:u8)] × count
  flags: bit0=has_street, bit1=has_addr, bit2=has_interp

Entry Corrections: marker 0xFFFFFFF8 (u32) + file_id (u32) + count (u32)
  + [(cell_id:u64, entry_count:u16, [id:u32] × entry_count)] × count

End marker: 0xFFFFFFFF (u32)
```

## Performance

| Operation | Europe | Planet |
|-----------|--------|--------|
| Build (deterministic) | ~12 min | ~14 min |
| Diff generation | **3m40s** | **15m22s** |
| Patch application | **33s** | **1m29s** |
| Patch peak memory | **104 MiB** | **243 MiB** |
| Patch min cgroup | ~200 MiB | **500 MiB** |

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
- **Streaming patch application** — Currently loads entire decompressed patch into memory
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
