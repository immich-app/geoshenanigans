# Geocoder Index Updates — Client Guide

## S3 Structure

```
builds/
  latest.json                              # entry point for all clients
  2026-04-07/
    configurations.json                    # build metadata + axes/components/files + sha256s
    checksums.sha256                       # on-wire hashes (*.zst, *.gcpatch, *.json)
    planet/
      full/            *.bin.zst + strings_layout.json(.zst) + patch.gcpatch
      no-addresses/    ...
      admin/           ...
      admin-minimal/   ...
      admin-minimal-poi/ ...
      quality/
        simplified/    admin_polygons.bin.zst + admin_rings.bin.zst + arc_index.bin.zst
                       + arc_vertices.bin.zst + patch.gcpatch
        exact/         ...
      poi/
        major/         poi_*.bin.zst + strings_poi.bin.zst + strings_layout.json(.zst)
                       + poi_meta.json(.zst) + patch.gcpatch
        notable/       ...
        all/           ...
    europe/
      full/            ...
      no-addresses/    ...
      admin/           ...
      quality/         ...
      poi/             ...
    africa/            ...
```

## Files Overview

A complete geocoder index for a given region and configuration consists of files from **up to three directories**:

### Mode directory (pick one)

| Mode | Files | Description |
|------|-------|-------------|
| `full/` | 22 files (+4 optional) | Streets + addresses + interpolation + admin + postcodes |
| `no-addresses/` | 14 files (+3 optional) | Streets + admin + postcodes (no address points) |
| `admin/` | 6 files (+3 optional) | Admin cell indexes + place nodes + postcodes |
| `admin-minimal/` | 9 files | Admin levels 2-8 + place nodes, with its own polygons over the minimal arc set, simplified to within 37.5 m on an 11 m grid (a ring that grid would collapse keeps the 1.1 m one; takes the place of the quality dir, so pick simplified), and only the core strings its records name. For POI tier `none` |
| `admin-minimal-poi/` | 9 files | The same, on the shared polygon slots and core strings that a POI tier's parent polygons and names point into (a dropped polygon's slot holds a tombstone), with the same arc files. The admin-minimal mode's dir beside a POI tier |

Optional files are written only when the data has them; see `optional_files`
under configurations.json below. The admin-minimal mode picks its dir by the
POI tier (its component's `variants`, below): `admin-minimal/`'s strings and
polygon ids are its own, so no other dir's offsets or ids can be read
against it.

No dir ships the place nodes' cell index: the first load of an index dir
derives it from `place_nodes.bin` into `<dir>/derived/` (see Fresh Install).

### Quality directory (pick one)

| Quality | Files | Description |
|---------|-------|-------------|
| `quality/simplified/` | 4 files | Admin boundaries simplified to within 37.5 m (less for small areas), on a 1.1 m grid |
| `quality/exact/` | 4 files | Every OSM point of every admin boundary, with nothing simplified, snapped or capped |

Both hold the admin polygons (postcode areas among them, at level 11) as
`admin_polygons.bin` and their rings as `admin_rings.bin`: the same bytes in
both. Each ring is a list of refs to the arcs of `arc_index.bin` and
`arc_vertices.bin`, the borders the polygons on either side of them share, each
stored once; those two files are what the tiers differ in.

### POI directory (optional, pick one)

| Tier | Files | Description |
|------|-------|-------------|
| `poi/major/` | 7 files | Major POIs only (airports, national parks, cathedrals, volcanoes, etc.) |
| `poi/notable/` | 7 files | Major + notable POIs (museums, castles, stadiums, beaches, etc.) |
| `poi/all/` | 7 files | All POIs including minor ones (picnic sites, small galleries, etc.) |

POI files: `poi_records.bin`, `poi_vertices.bin`, `poi_cells.bin`, `poi_entries.bin`, `strings_poi.bin`, `strings_layout.json`, `poi_meta.json`. POIs with Wikipedia/Wikidata tags are promoted one tier.

## latest.json

The entry point for all clients:

```json
{
  "build_version": 18,
  "patch_version": 14,
  "latest": "2026-04-10",
  "previous": "2026-04-09",
  "oldest_indexes": "2026-04-08",
  "oldest_patches": "2026-03-27",
  "updated_at": "2026-04-10T02:30:00Z"
}
```

| Field | Description |
|-------|-------------|
| `build_version` | Incremented when file structure changes. If this doesn't match your local version, download fresh. |
| `patch_version` | Incremented when patch format changes. If this doesn't match your local version, download fresh. |
| `latest` | Most recent build date. |
| `previous` | The date `latest` patches from, or `null` when `latest` has no patches (a fresh build). |
| `oldest_indexes` | Oldest date that still has full `.bin` index files of this `build_version` available for download. `latest` when no older one does. |
| `oldest_patches` | Oldest local date that can patch to `latest`: every build after it on the `build.previous` chain to `latest` still has its `patch.gcpatch` files. `latest` when `latest` has no patches. |

## configurations.json (per-build)

Each build date has a `configurations.json` covering both the per-build
metadata (formerly in `manifest.json`) and the schema that drives the
client UI — axes (region/mode/quality/poi_tier), components (which
files belong to each axis value), constraints, presets, and the flat
`files` map keyed by relative path with `size_zst`/`size_raw`/`sha256`:

```json
{
  "schema_version": 1,
  "build": {
    "version": 18,
    "patch_version": 14,
    "date": "2026-04-10",
    "previous": "2026-04-09",
    "built_at": "2026-04-10T09:00:14Z",
    "wikidata_date": "2026-04-08"
  },
  "axes":        { "region": { ... }, "mode": { ... }, ... },
  "components":  { "mode.full": { "files": ["{region}/full/...", ... ] }, ... },
  "constraints": [ { "when": { "mode": "admin-minimal" }, "requires": { "quality": "simplified" } } ],
  "presets":     [ { "id": "smallest", "selections": { ... } }, ... ],
  "files": {
    "planet/full/admin_cells.bin": { "size_zst": 6393328, "size_raw": 24807168, "sha256": "..." },
    "...": "..."
  }
}
```

The static portions (axes/components/constraints/presets) come from
`geocoder/builder/configurations.template.json` and are stable across
builds. CI splices in `build.*` and the `files` table during the
build's compress step.

A component's `optional_files` are written only when the data has them
(`interp_postcodes.bin` in TIGER regions, the postcode centroids where a
region has postcodes); a client skips any that are absent from `files`.

A component's `variants` add one more component, picked by the whole
selection: the first variant whose `when` names the selection's value on
every axis it lists (a variant without `when` takes any selection). Its
`component` is resolved like an axis component, `files`, `optional_files`,
`replaces` and all.
`strings_layout.json` and `poi_meta.json` are listed and compressed like
the `.bin` files, because the server can't load a region without them.

## Client Decision Tree

### Fresh Install

```
1. GET builds/latest.json
2. Choose a date >= oldest_indexes (typically use latest)
3. GET builds/{date}/configurations.json
4. Choose region (e.g. europe), mode (e.g. full), quality (e.g. simplified), and POI tier (e.g. notable)
5. Download every file the selected components and the variants they pick
   list (their `files`, the `optional_files` that appear in `files`, minus
   any `replaces`), each as
   <path>.zst, decompress, and check it against its sha256:
   - builds/{date}/europe/full/…                (22 files + up to 4 optional)
   - builds/{date}/europe/quality/simplified/…  (4 files)
   - builds/{date}/europe/poi/notable/…         (7 files, optional)
   The JSON files (strings_layout.json, poi_meta.json) are part of the set:
   the server can't load a region without them.
6. Store build_version, patch_version, and date locally
```

The first load of the dir (the server, the node binding, `grid_query`)
derives the place index from `place_nodes.bin` into `<dir>/derived/`:
`place_cells.idx`, `place_entries.idx` and `place_index.json`, which records
what they were derived from (place_nodes.bin's size, mtime, inode and
sha256, and the cell level). Planet admin (4.1M place nodes) takes about a
second on one core, with a heap peak of 5.4 MiB. Later loads map them while
`place_nodes.bin` keeps its stat, or its sha256 when the files were copied.
Where `<dir>` can't be written, the index goes to a private dir under
`$TMPDIR` instead, one per user and dataset dir
(`geocoder-place-index-<uid>-<sha256 of the dir's path>`, mode 0700; a load
refuses one another user owns or can open), checked the same way.
A pack or an image that bundles a dataset only has to load it once when it
is built: the `derived/` that load writes comes along, and loads from the
copy use it as is.

### Daily Update (patching)

```
1. GET builds/latest.json
2. Compare build_version and patch_version with local values
   - If either changed → go to "Version Mismatch" below
3. If local_date == latest → already up to date, done
4. If local_date < oldest_patches → patches too old, go to "Fresh Install"
5. Walk the patch chain:
   a. GET builds/{latest}/configurations.json → build.previous = date_n-1
   b. GET builds/{date_n-1}/configurations.json → build.previous = date_n-2
   c. Continue until previous == local_date
6. Apply patches in order (oldest first):
   For each date in the chain:
   - Download builds/{date}/europe/full/patch.gcpatch → apply to mode files
   - Download builds/{date}/europe/quality/simplified/patch.gcpatch → apply to quality files
   - Download builds/{date}/europe/poi/notable/patch.gcpatch → apply to POI files (if using POIs)
7. Update local date to latest
```

Apply every patch out of place (`geocoder-patch <old-dir> <patch> -o <new-dir>`;
the tool refuses `-o` equal to the current dir or a non-empty one) and keep
the build's directory layout. The new dir ends up holding exactly the
variant's files, JSON included. Each variant dir patches from its own files
alone, in any order: the tool reads nothing outside the dir it is given. The
patch records the size of every old file it reads and the hash of every
string tier it rebuilds, and the tool fails when one differs. A size check
only catches a file from another build when its size changed; the string
tiers' hashes are the content check. The server combines a mode dir with a
quality or poi dir by global string offsets, so patch every selected dir to
the same date and swap the whole set in together. The first load of the
patched set derives its place index again.

### Version Mismatch

```
1. build_version changed → file structure is different, must fresh install
2. patch_version changed → patch format is different, must fresh install
3. Follow "Fresh Install" steps above
```

### Switching Quality

Quality changes don't require patching — just download the new quality files:

```
1. Download the files the quality.{new_quality} component lists for
   {current_date} (its `files` and the `optional_files` that appear in
   `files`), each as <path>.zst, decompress, and check it against its sha256
2. Replace the local quality dir with them: admin_polygons.bin,
   admin_rings.bin, arc_index.bin and arc_vertices.bin
```

### Switching Region

Regions are independent. Download the new region's mode + quality directories.

## Retention Policy

| Data | Retention | Notes |
|------|-----------|-------|
| Index files (`.bin`) | 3 days | Full index available for fresh downloads |
| Patch files (`.gcpatch`) and `configurations.json` | 14 days | Small files, allow long catch-up windows; `configurations.json` stays so the chain walk works |
| Cached PBFs | 2 days | Build server cache only, not client-facing: today's and yesterday's, the only ones a run reads |
| Incomplete multipart uploads | 1 day | Both buckets; left by a killed upload, invisible to listings |

## Patch Application

Each `patch.gcpatch` file transforms the files in its directory from the previous day's version to the current day's version. Apply with `geocoder-patch`:

```bash
geocoder-patch <current-dir> <patch-file> -o <output-dir>
```

The patch tool:
- Reads old files front to back through `pread` (sequential readahead) and
  writes each new file in one streaming pass; only the patch and its
  decompressed copy, each old string tier while it rebuilds it, and the remap
  runs it spills to its scratch dir are mmapped. Long unchanged runs and
  unchanged files are copied in the kernel (`copy_file_range`), or through
  `pread` where the kernel, the filesystem or a seccomp profile refuses that
- Peak anonymous memory: about 1 MiB, or 2 MiB in low-memory mode (an
  encoded installed set, see `builder/PATCHING.md`), for every variant and
  whatever the patch; everything else is reclaimable page cache. That is the
  default build, which decompresses the patch with libzstd; one built with
  `-DGEOCODER_PATCH_LIBZSTD=OFF` also runs a `zstd` child that allocates
  about 11.3 MiB beside it.
  Its scratch dir, which holds the decompressed patch, is under `$TMPDIR`:
  keep that on disk, since a RAM-backed one counts against memory
- Runs on one core inside a 32 MiB memory limit (page cache included): one
  day of `planet/full` takes about 4 minutes, `europe/full` about 1.5 minutes
  on an SSD. On a slow disk it is disk-bound: it reads the old variant and
  writes the new one, two to three times the variant's size in IO
- Needs free space for the new copy of the variant (it never patches in place)
- Produces byte-identical output to a fresh build
