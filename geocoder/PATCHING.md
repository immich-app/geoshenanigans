# Geocoder Index Updates — Client Guide

## S3 Structure

```
builds/
  latest.json                              # entry point for all clients
  2026-04-07/
    manifest.json                          # build metadata + region/quality info
    planet/
      full/            *.bin + patch.gcpatch
      no-addresses/    *.bin + patch.gcpatch
      admin/           *.bin + patch.gcpatch
      quality/
        uncapped/      admin_polygons.bin + admin_vertices.bin + patch.gcpatch
        q0.5/          admin_polygons.bin + admin_vertices.bin + patch.gcpatch
        q1/            ...
      poi/
        major/         poi_records.bin + poi_vertices.bin + poi_cells.bin + poi_entries.bin + patch.gcpatch
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
| `full/` | 12 files | Streets + addresses + admin cell indexes |
| `no-addresses/` | 7 files | Streets + admin cell indexes (no address points) |
| `admin/` | 3 files | Admin cell indexes + strings only |

### Quality directory (pick one)

| Quality | Files | Description |
|---------|-------|-------------|
| `quality/uncapped/` | 2 files | Full-resolution admin boundaries |
| `quality/q0.5/` | 2 files | Admin boundaries simplified at 0.5x |
| `quality/q1/` | 2 files | Admin boundaries simplified at 1x |
| `quality/q1.5/` | 2 files | Admin boundaries simplified at 1.5x |
| `quality/q2/` | 2 files | Admin boundaries simplified at 2x |
| `quality/q2.5/` | 2 files | Admin boundaries simplified at 2.5x |

Higher quality numbers = more simplification = smaller files = less accurate boundaries.

### POI directory (optional, pick one)

| Tier | Files | Description |
|------|-------|-------------|
| `poi/major/` | 4 files | Major POIs only (airports, national parks, cathedrals, volcanoes, etc.) |
| `poi/notable/` | 4 files | Major + notable POIs (museums, castles, stadiums, beaches, etc.) |
| `poi/all/` | 4 files | All POIs including minor ones (picnic sites, small galleries, etc.) |

POI files: `poi_records.bin`, `poi_vertices.bin`, `poi_cells.bin`, `poi_entries.bin`. POIs with Wikipedia/Wikidata tags are promoted one tier.

## latest.json

The entry point for all clients:

```json
{
  "build_version": 2,
  "patch_version": 2,
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

## manifest.json (per-build)

Each build date has a manifest with region details and sizes:

```json
{
  "build_version": 2,
  "patch_version": 1,
  "date": "2026-04-10",
  "previous": "2026-04-09",
  "indexes_available": true,
  "regions": {
    "planet": {
      "modes": {
        "full": { "size": 19734832441 },
        "no-addresses": { "size": 16139154204 },
        "admin": { "size": 81058844 }
      },
      "qualities": {
        "uncapped": { "scale": 0, "size": 2951027360 },
        "q0.5": { "scale": 0.5, "size": 1037646024 },
        "q1": { "scale": 1, "size": 737764592 }
      }
    },
    "europe": { ... },
    "africa": { ... }
  }
}
```

After cleanup (indexes older than 3 days are removed), `indexes_available` is set to `false`. Patches remain available for weeks.

## Client Decision Tree

### Fresh Install

```
1. GET builds/latest.json
2. Choose a date >= oldest_indexes (typically use latest)
3. GET builds/{date}/manifest.json
4. Choose region (e.g. europe), mode (e.g. full), quality (e.g. q1), and POI tier (e.g. notable)
5. Download:
   - builds/{date}/europe/full/*.bin             (12 files)
   - builds/{date}/europe/quality/q1/*.bin       (2 files)
   - builds/{date}/europe/poi/notable/*.bin      (4 files, optional)
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
   a. GET builds/{latest}/manifest.json → previous = date_n-1
   b. GET builds/{date_n-1}/manifest.json → previous = date_n-2
   c. Continue until previous == local_date
6. Apply patches in order (oldest first):
   For each date in the chain:
   - Download builds/{date}/europe/full/patch.gcpatch → apply to mode files
   - Download builds/{date}/europe/quality/q1/patch.gcpatch → apply to quality files
   - Download builds/{date}/europe/poi/notable/patch.gcpatch → apply to POI files (if using POIs)
7. Update local date to latest
```

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
- Reads old files via mmap (low memory)
- Streams output to disk
- Peak memory: ~250 MiB for planet-scale data
- Produces byte-identical output to a fresh build
