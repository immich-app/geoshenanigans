# Geocoder Incremental Patch System

## Goal

Produce small patch files nightly so users can update their geocoder index without
re-downloading the full dataset. A user starting from any build can apply patches
in sequence and arrive at output **byte-identical** to a fresh build.

## Current Status

### What Works
- **Deterministic builds**: Same PBF always produces byte-identical output (verified on Germany, Europe, and Planet)
- **Single-patch application**: Verified on both Europe and Planet — every file byte-identical
- **Sequential patching**: Verified on planet with 3 distinct weekly snapshots (Mar 9 → Mar 16 → Mar 23) — **PASS**; with the v5 tools on three daily planets (Oct 4 → 5 → 6), every variant applied stacked in a client-shaped tree — **PASS** (126/126)
- **Custom patch format**: Merge sequences, string-level diffs, parent-aware coordinate merges, secondary ID matching, cell corrections — fully custom diff/apply logic (only zstd for transport compression)

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

### GCPATCH v8 on planet (2026-10-11 → 12 and 10-12 → 13)

v8 matches street / interp / addr records across days by a server-side
identity sidecar instead of by content (`geocoder-diff --identity`; on the
published stable-slot days the key is the slot, see
`tools/gen_identity_sidecar.py`). Measured 2026-10-14 on planet/full of the
published days against the nightly's v7 patches (cold cache, `geocoder-patch`
on 1 core, every apply byte-identical to the new day):

| planet/full | v7 patch | v8 patch | v7 apply CPU | v8 apply CPU | v7 diff CPU / RSS | v8 diff CPU / RSS |
|-------------|----------|----------|--------------|--------------|-------------------|-------------------|
| 10-12 → 13 | 3.73 MiB | **3.29 MiB** (−11.8%) | 212 s | **167 s** (−21%) | 388 s / 39.5 GiB | 221 s / 24.9 GiB |
| 10-11 → 12 | 2.78 MiB | **2.46 MiB** (−11.6%) | 209 s | **165 s** (−21%) | 390 s / 36.7 GiB | 211 s / 24.9 GiB |

planet/quality/uncapped and planet/poi/all patch to within a few bytes of v7
(no street / interp / addr records). Without `--identity` the families merge
by content as in v7, with the rest of the v8 format.

The tables below predate all of these changes.

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
1. Build string-level diff (per tier, keep the longest run of common strings in the same order, every one of them in a sorted tier; emit the other old strings as deleted and the other new ones as added, with their new index unless the new tier is sorted)
2. Build string remap (old string pool offsets → new, derived from string diff)
3. For each data file: apply string remap to old, match records by content hash (for ways/admin: also fix node_offset/vertex_offset fields), build merge sequence (MATCH/INSERT/DELETE at record stride)
4. For coordinate files (nodes/vertices): derive merge from parent way/polygon merge — verify node blocks match before COPY, INSERT if nodes changed
5. Derive ID remaps from merge sequences (MATCH ops define old→new record correspondence)
6. **Secondary matching**: For DELETE/INSERT records in merge sequences, match by relaxed key (name + node_count for ways, street + housenumber for addrs, etc.) to recover ID mappings for modified-but-same-entity records
7. Derive each cell's entry list from the old one (IDs remapped) and the cell changes, and compare it with the new one: street / addr / interp cells that differ go out as per-cell lost / gained ids (Geo Entry Deltas), the admin / POI / place cell indexes as a Cell Index Delta (whole-cell Entry Corrections where the new layout can't be rebuilt)
8. Package everything, compress whole patch with zstd for transport

**Patch tool:**
1. Decompress zstd
2. Read the strings section → rebuild each shipped tier from the dir's own old tier + additions/deletions and derive its remap runs; take the sent runs of unshipped tiers and the moved-string pairs as they come
3. For each data file: apply string remap + offset fixups to old, replay merge sequence → output file, track ID mapping from MATCH ops
4. Read secondary ID remaps → merge into derived ID mappings
5. Read cell changes (added/removed cell IDs) and the per-cell entry deltas
6. Rebuild geo_cells and the street / addr / interp entry files in one pass over the old geo index: each cell's lists with their IDs remapped, corrected by the cell's delta
7. Rebuild the admin / POI / place cell indexes from their ID remaps + cell changes, corrected by the Cell Index Delta (or Entry Corrections)

## Known Issues

### 1. Cell correction overhead (FIXED)

**Impact**: ~29 MiB for planet (down from ~42 MiB), ~11 MiB for Europe (down from ~15 MiB).

**Status**: Two improvements applied:
- **Secondary ID matching** recovers 119K additional ID mappings on planet (55K ways, 59K addrs, 4.6K admin). Reduced street corrections 2.03M → 1.59M (22%), addr corrections 116K → 95K (18%).
- **Admin hash bug fix** (was reading padding byte instead of country_code). Admin corrections 193K → 4.6K cells (**97% reduction**).

**Remaining cause**: 1.59M street cells (25.7 MiB) still needed corrections. These are from records that genuinely differ (geometry changes where name + node_count also changed, truly new/removed entities). Further reduction needed OSM-level entity tracking, which strategy-2 stable IDs now provide: a record keeps its id from one build to the next (`<file>.osm_ids` sidecars, read through `build-index --prev-output`), and v6 sends what is left as per-cell lost / gained ids.

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
  stamps; the patcher rebuilds it and derives its remap runs from the same
  walk, checking each looked-up offset is a string start. The tiers are
  sorted alphabetically except the POI tier, which is in POI record order
  (the names of POI tier 0, 1, 2, 3 in turn, each at its first record of
  that tier, so a reader walking the records reads the names front to
  back). A string a tier's order moves past others is deleted and added,
  and its offset travels as a pair. A tier
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

## Place search index

The place search index (`geocoder/search`) points into a selection's own
files by record index and string offset, so it answers right only beside the
exact files it was built from. Its `search_meta.json` records the sha256 of
each of them (`dataset`: admin_polygons, admin_vertices, place_nodes,
poi_records, strings_core, strings_layout.json, strings_poi), and the binding
(`openPlaceSearch`) uses an index only while the files beside it hash the
same. Any other index, or none, is built there from the files first, on one
thread, within 32 MiB.

- **Shipped**: the selections in `search_index.shipped` of
  `configurations.template.json` publish their index in every region under
  `{region}/search/{mode}/{quality}/{poi_tier}/`, with size and sha256 like
  every other file; a client downloads it into `<dataset>/search/`.
- **Built on the device**: every other selection, the first time place search
  opens.
- **Never patched**: the index sits outside the variant dirs (the patcher
  never lists a dir, and `generate_patches.sh` only diffs dirs holding `.bin`
  files), and a patched dataset no longer matches the hashes, so the next
  open rebuilds it beside the patched files. The patcher writes into an
  empty dir, so `search/` stays with the old files: moved beside the new
  ones or not, the next open builds the index.

A selection ships the index when it adds at most 5% (zstd) to the selection's
planet pack. That holds for the two full-mode selections only. Measured on
planet 2026-10-18 (stack/10), pack sizes from the published 2026-10-26 build.
Device build is the binding's open building it on one thread, in 32 MiB the
same build in a 32 MiB cgroup (2026-10-19), both on a busy box:

| Selection | Pack | Index | Adds | Ships | Device build | In 32 MiB |
|-----------|------|-------|------|-------|--------------|-----------|
| smallest (admin-minimal) | 217.4 MiB | 36.6 MiB | +16.8% | no | 10 s | 12 s |
| recommended (admin + q2.5 + poi/major) | 547.5 MiB | 61.0 MiB | +11.1% | no | 17 s | 20 s |
| with-addresses (full + q2.5 + poi/all) | 10,510 MiB | 412.0 MiB | +3.9% | yes | 135 s | 205 s |
| max-quality (full + uncapped + poi/all) | 11,686 MiB | 411.9 MiB | +3.5% | yes | 131 s | 219 s |

A shipped selection ships it in every region: from +1.4% (north-america,
54.1 MiB) to +11.2% (africa, 21.8 MiB) of the regional pack.

Why the index is rebuilt rather than patched, measured from 2026-10-18 to
10-19 on planet with-addresses: 17.7% of the place keys and 98% of the POI
keys change postings (POI record indexes shift every day), so every index
file changes. A zstd -18 `--patch-from` delta of the files is 382.4 MiB,
against 412.0 MiB to download the new index and 4.5 MiB for the selection's
v8 data patch that day; applying the delta also needs memory for the whole
old file (1.0 GB for poi.fst). Rebuilding costs no download and two to three
and a half minutes of one core.

## Patch Format (.gcpatch, version 9)

Whole file is zstd-compressed for transport. Internal structure (all integers
little-endian; the header, client files, old file sizes and strings sections
are read by position, everything after them by marker):

```
Header: "GCPATCH\0" (8) + version=9 (u32) + flags=0 (u32)
  (a v9 patcher accepts only v9 patches; the v8 and v9 changes are marked
  below)

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
      order:u32 (v9; 0 sorted: the new tier sorts and an added string
      goes where it sorts; 1 indexed: the new indices follow the strings),
      [string\0] × n_added, indexed only [new index:u32] × n_added,
      [old index:u32] × n_deleted (indices and a sorted tier's added
      strings ascending; the patcher refuses an old tier of another size or
      hash and checks the one it rebuilds)
    sent: n_runs:u32, runs_size:u32, runs of varint (start - previous end,
      end - start, zigzag change of the shift), from the tier's old base
  + n_pairs (u32) + [(old_off:u32, new_off:u32)] × n_pairs (surviving
  strings no tier's edit keeps: they changed tier, or (v9) the POI tier's
  order moved them; out of a shipped or referenced tier)

Parent-id remap: marker 0xFFFFFFF3 (u32)
  + n_admin (u32) + [(old:u32, new:u32)] × n_admin
  + n_street (u32) + [(old:u32, new:u32)] × n_street + 0 (u32, reserved)
  (v8: the street pairs are only the ways no MATCH / REPLACE run maps, the
  moved and secondary-matched ones; the patcher derives the rest of the
  street remap from the street_ways replay)

Per-file section: file_id (u32) + stride (u32) + old_size (u64) + new_size (u64) + ...
  (the patcher refuses an old file of another size, 0 meaning absent)
  stride=0: full replacement (n_fixups=0 u32, size u64, data)
  stride=0xFD: unchanged, copy from cur_dir
  stride=0xFC: sparse delta (value_stride, remap_kind, n, [(pos, value)] × n)
  stride=0xFA (v8): parallel delta for a per-record side file (way_parents,
    way_postcodes, addr_postcodes, interp_postcodes), rebuilt along its
    parent's merge: parent_file_id u32, value_stride u32 = 4, remap_kind u32,
    n_inserted u32 + [value u32], n_changes u32 + [(pos u32, value u32)].
    MATCH / REPLACE copy the old value (remapped), INSERT takes the next
    inserted value, DELETE skips one; then the changes overwrite positions
    of the new file. A parent sent unchanged counts as one MATCH.
  stride=0xFB: cell list delta for <name>_cells.bin + <name>_entries.bin
    (payload_size u64, new entries size u64, n_removed u32 + [cell u64],
    n_set u32 + [cell u64, n_lost u32, n_gained u32, lost ids, gained ids])
  otherwise: n_runs (u32) + n_values (u32) + runs_size (u32) + values_size (u32)
    + runs + values + seq_size (u64) + merge ops
  Ops: MATCH(count:u32) | INSERT(count:u32, data) | DELETE(count:u32)
    | REPLACE(count:u32, data) (v8: the same entities with new bytes, written
    like an INSERT, mapped old → new like a MATCH)
  Order (v8): street ways and nodes, interp ways and nodes, then addr
  points, whose parent_way_id the patcher moves through the street replay's
  remap.
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
  (v8: the street_ways list is empty when its pairs went in the parent-id
  remap)

Geo Entry Deltas (street / addr / interp entries): marker 0xFFFFFFF0 (u32)
  + file_id (u32) + count (u32), then per cell in cell order:
  cell_id (u64) + n_lost (u16) + n_gained (u16) + [id:u32] × n_lost
  + [id:u32] × n_gained, turning the list the patcher derives (old list,
  ids remapped) into the new one. n_lost 0xFFFF: the gained ids are the
  whole new list. v8: an old id the remap doesn't map is a deleted record
  and drops out of the derived list.

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
| Patch application, 1 core, cold cache (`full`, one day) | **81s** | **241s** |
| Patch peak anonymous memory | **5 MiB** | **12 MiB** |
| Patch memory limit tested (page cache included) | 1 GiB | 1 GiB |

One deterministic `build-index` run writes the planet and every continent in
about 5 minutes on 64 cores, and `geocoder-diff` makes the patches of all 126
variant dirs in about 6 minutes on the same box (see `OPTIMIZATION_LOG.md`).

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
- ~~Further reduce cell corrections~~ — **DONE** (strategy-2 stable IDs; per-cell lost / gained ids since v6)

### Nice to Have (Performance)
- ~~Parallel merge building~~ — **DONE** (geocoder-diff builds each file family on its own thread)
- ~~Streaming patch application~~ — **DONE** (the decompressed patch is mmapped from scratch; records, cell indexes, remaps and deltas stream)
- ~~Code cleanup~~ — **DONE** (legacy patch paths and prototype tools removed)

### Future Consideration (Format Changes)
- **Inline node storage** — Store way nodes inline instead of separate flat array. Eliminates coordinate cascade entirely. Requires server format change.
- ~~Content-addressed record IDs~~ — superseded by strategy-2 stable IDs, which keep record IDs, and so entry files, stable across builds with no server format change
- **Fixed-size cell slots** — Pre-allocate per cell. Analysis showed 2-6x space waste due to skewed distribution.

## File Inventory

### Core files (committed)
- `src/build_index.cpp` — Deterministic ordering
- `src/types.h` — Explicit struct padding
- `src/s2_helpers.cpp` — Zero padding in AdminPolygon creation
- `tools/geocoder_diff.cpp` — Diff tool
- `tools/geocoder_patch.cpp` — Patch tool
- `tools/patch_format.h` — Shared format definitions + rebuild functions
- `tools/merge_sequence.h` — Positional merge of record files
- `tools/sequential_file_reader.h` — The patcher's forward reads of old files (pread)
- `tools/generate_patches.sh` — CI: diff, isolated apply and verify for every variant
- `tools/link_client_files.sh` — An isolated copy of a variant dir (its client files only)
- `tools/verify_patch.sh` — Whole client file set comparison
- `CMakeLists.txt` — Build targets
