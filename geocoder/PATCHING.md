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
      quality/
        uncapped/      admin_polygons.bin.zst + admin_vertices.bin.zst (+ postal_*.bin.zst) + patch.gcpatch
        q0.2/          ...
        q0.5/ … q2.5/  ...
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
| `full/` | 24 files (+4 optional) | Streets + addresses + interpolation + admin + postcodes |
| `no-addresses/` | 16 files (+3 optional) | Streets + admin + postcodes (no address points) |
| `admin/` | 8 files (+3 optional) | Admin cell indexes + place nodes + postcodes |
| `admin-minimal/` | 9 files | Admin levels 2-8 + place nodes, with its own q2.5 polygons (takes the place of the quality dir, so pick q2.5) |

Optional files are written only when the data has them; see `optional_files`
under configurations.json below.

### Quality directory (pick one)

| Quality | Files | Description |
|---------|-------|-------------|
| `quality/uncapped/` | 2 files (+2 optional) | Full-resolution admin boundaries |
| `quality/q0.2/` | 2 files (+2 optional) | Admin boundaries simplified at 0.2x |
| `quality/q0.5/` | 2 files (+2 optional) | Admin boundaries simplified at 0.5x |
| `quality/q1/` | 2 files (+2 optional) | Admin boundaries simplified at 1x |
| `quality/q1.5/` | 2 files (+2 optional) | Admin boundaries simplified at 1.5x |
| `quality/q2/` | 2 files (+2 optional) | Admin boundaries simplified at 2x |
| `quality/q2.5/` | 2 files (+2 optional) | Admin boundaries simplified at 2.5x |

Higher quality numbers = more simplification = smaller files = less accurate boundaries.

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
  "build_version": 2,
  "patch_version": 7,
  "latest": "2026-04-10",
  "oldest_indexes": "2026-04-08",
  "oldest_patches": "2026-03-25",
  "updated_at": "2026-04-10T02:30:00Z"
}
```

| Field | Description |
|-------|-------------|
| `build_version` | Incremented when file structure changes. If this doesn't match your local version, download fresh. |
| `patch_version` | Incremented when patch format changes. If this doesn't match your local version, download fresh. |
| `latest` | Most recent build date. |
| `oldest_indexes` | Oldest date that still has full `.bin` index files available for download. |
| `oldest_patches` | Oldest date that still has `patch.gcpatch` files available. |

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
    "version": 15,
    "patch_version": 7,
    "date": "2026-04-10",
    "previous": "2026-04-09",
    "built_at": "2026-04-10T09:00:14Z",
    "wikidata_date": "2026-04-08"
  },
  "axes":        { "region": { ... }, "mode": { ... }, ... },
  "components":  { "mode.full": { "files": ["{region}/full/...", ... ] }, ... },
  "constraints": [ { "when": { "mode": "admin-minimal" }, "requires": { "quality": "q2.5" } } ],
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
(`interp_postcodes.bin` in TIGER regions, `postal_*` where postal
boundaries exist); a client skips any that are absent from `files`.
`strings_layout.json` and `poi_meta.json` are listed and compressed like
the `.bin` files, because the server can't load a region without them.

## Client Decision Tree

### Fresh Install

```
1. GET builds/latest.json
2. Choose a date >= oldest_indexes (typically use latest)
3. GET builds/{date}/configurations.json
4. Choose region (e.g. europe), mode (e.g. full), quality (e.g. q1), and POI tier (e.g. notable)
5. Download every file the selected components list (their `files`, the
   `optional_files` that appear in `files`, minus any `replaces`), each as
   <path>.zst, decompress, and check it against its sha256:
   - builds/{date}/europe/full/…          (24 files + up to 4 optional)
   - builds/{date}/europe/quality/q1/…    (2 files + up to 2 optional)
   - builds/{date}/europe/poi/notable/…   (7 files, optional)
   The JSON files (strings_layout.json, poi_meta.json) are part of the set:
   the server can't load a region without them.
6. Store build_version, patch_version, and date locally
```

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
   - Download builds/{date}/europe/quality/q1/patch.gcpatch → apply to quality files
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
the same date and swap the whole set in together.

### Version Mismatch

```
1. build_version changed → file structure is different, must fresh install
2. patch_version changed → patch format is different, must fresh install
3. Follow "Fresh Install" steps above
```

### Switching Quality

Quality changes don't require patching — just download the new quality files:

```
1. Download builds/{current_date}/{region}/quality/{new_quality}/*.bin
2. Replace local admin_polygons.bin and admin_vertices.bin
```

### Switching Region

Regions are independent. Download the new region's mode + quality directories.

## Retention Policy

| Data | Retention | Notes |
|------|-----------|-------|
| Index files (`.bin`) | 3 days | Full index available for fresh downloads |
| Patch files (`.gcpatch`) | Weeks | Small files, allow long catch-up windows |
| Cached PBFs | 5 days | Build server cache only, not client-facing |

## Patch Application

Each `patch.gcpatch` file transforms the files in its directory from the previous day's version to the current day's version. Apply with `geocoder-patch`:

```bash
geocoder-patch <current-dir> <patch-file> -o <output-dir>
```

The patch tool:
- Reads old files front to back through `pread` (sequential readahead) and
  writes each new file in one streaming pass; only the old string tiers,
  probed at random, and the decompressed patch are mmapped. Long unchanged
  runs and unchanged files are copied in the kernel (`copy_file_range`), or
  through `pread` where the kernel, the filesystem or a seccomp profile
  refuses that
- Peak anonymous memory: about 10 MiB for `planet/full`; everything else is
  reclaimable page cache
- Runs on one core inside a 1 GiB memory limit (page cache included): one
  day of `planet/full` takes about 4 minutes, `europe/full` about 1.5 minutes
  on an SSD. On a slow disk it is disk-bound: it reads the old variant and
  writes the new one, about twice the variant's size in IO
- Needs free space for the new copy of the variant (it never patches in place)
- Produces byte-identical output to a fresh build
