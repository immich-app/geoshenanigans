# Geocoder Index Updates — Client Guide

## S3 Structure

```
builds/
  latest.json                              # entry point for all clients
  2026-04-07/
    configurations.json                    # build metadata + axes/components/files/units + sha256s
    checksums.sha256                       # on-wire hashes (*.zst, *.gcpatch, *.json)
    planet/
      full/            *.bin.zst + places.tt.zst + street_ways.tt.zst
                       + interp_ways.tt.zst + addr_index.tt.zst
                       + strings_layout.json(.zst) + attribution.json(.zst)
                       + patch.gcpatch
      no-addresses/    ... (places, street_ways units)
      admin/           ... (places unit)
      admin-minimal/   ... (places, arcs, admin_polygons units)
      admin-minimal-poi/ ...
      quality/
        simplified/    admin_rings.bin.zst + arcs.tt.zst + admin_polygons.tt.zst
                       + patch.gcpatch
        exact/         ...
      poi/
        major/         poi_cells.bin.zst + poi_entries.bin.zst + strings_poi.bin.zst
                       + pois.tt.zst + strings_layout.json(.zst) + poi_meta.json(.zst)
                       + poi_attribution.json(.zst) + patch.gcpatch
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

A file is published either as its own `<path>.zst` or inside a transport unit
(`*.tt.zst`, below), never both.

## Files Overview

A complete geocoder index for a given region and configuration consists of files from **up to three directories**:

### Mode directory (pick one)

| Mode | Files | Description |
|------|-------|-------------|
| `full/` | 24 files (+5 optional) | Streets + addresses + interpolation + admin + postcodes |
| `no-addresses/` | 17 files (+3 optional) | Streets + admin + postcodes (no address points) |
| `admin/` | 9 files (+3 optional) | Admin cell indexes + place nodes + postcodes |
| `admin-minimal/` | 12 files (+1 optional) | Admin levels 2-8 + place nodes, with its own polygons over the minimal arc set, simplified to within 37.5 m on an 11 m grid (a ring that grid would collapse keeps the 1.1 m one; takes the place of the quality dir, so pick simplified), and only the core strings its records name. For POI tier `none` |
| `admin-minimal-poi/` | 12 files (+1 optional) | The same, on the shared polygon slots and core strings that a POI tier's parent polygons and names point into (a dropped polygon's slot holds a tombstone), with the same arc files. The admin-minimal mode's dir beside a POI tier |

Optional files are written only when the data has them; see `optional_files`
under configurations.json below. The admin-minimal mode picks its dir by the
POI tier (its component's `variants`, below): `admin-minimal/`'s strings and
polygon ids are its own, so no other dir's offsets or ids can be read
against it.

The place nodes come as three files: `place_cells.bin` (each admin cell's
first row), `place_nodes.bin` (the visible nodes in cell order, bit-packed)
and `place_hidden.bin` (the ones a boundary hides), which the server reads
as they are.

Every mode dir holds `attribution.json`, the credit of the data it holds
(Attribution, below). A selection takes one mode dir, so every dataset holds
its credit at `<dataset>/attribution.json`, and a dataset with a POI tier
the tier's `poi_attribution.json` beside it where the build has one.

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

POI files: `poi_records.bin`, `poi_vertices.bin`, `poi_cells.bin`, `poi_entries.bin`, `strings_poi.bin`, `strings_layout.json`, `poi_meta.json`, and `poi_attribution.json` (optional: Wikidata's credit, where its sitelinks ranked a POI of the tier; Attribution, below). POIs with Wikipedia/Wikidata tags are promoted one tier.

## Transport units

Some files download as a transport unit: a download-only form of them, their
values in byte-aligned columns, which compress better than the packed files
do (`server/src/transport.rs`; [builder/PATCHING.md](builder/PATCHING.md#transport-units)
has the format). Every dir holding all of a unit's files publishes the unit as
`<dir>/<unit>`, and that is the only form those files are published in:

| Unit | Files | Dirs |
|------|-------|------|
| `places.tt.zst` | `place_cells.bin`, `place_nodes.bin`, `place_hidden.bin` | `full/`, `no-addresses/`, `admin/`, `admin-minimal/`, `admin-minimal-poi/` |
| `pois.tt.zst` | `poi_records.bin`, `poi_vertices.bin` | `poi/*/` |
| `arcs.tt.zst` | `arc_index.bin`, `arc_vertices.bin` | `quality/*/`, `admin-minimal/`, `admin-minimal-poi/` |
| `admin_polygons.tt.zst` | `admin_polygons.bin` | `quality/*/`, `admin-minimal/`, `admin-minimal-poi/` |
| `street_ways.tt.zst` | `street_ways.bin` | `full/`, `no-addresses/` |
| `interp_ways.tt.zst` | `interp_ways.bin` | `full/` |
| `addr_index.tt.zst` | `addr_index.bin` | `full/` |

A unit decodes into exactly the files it holds, byte for byte, each checked
against its `sha256` in `files`; the client then holds what a plain download
gives, so patches apply to it as to any other copy. Only the geocoder's code
decodes a unit: the query-server library (`query_server::transport::decode_into_dir`,
which the server's downloader runs), the node binding
(`createTransportDecoder(dir, files)`: `write(chunk)` each chunk of the unit
as it downloads, then `finish()`) or the CLI
(`geocoder-transport decode <unit.tt.zst | -> <dir>`, a bin of
`geocoder/search`). zstd alone does not give the files.

Each decoder writes a unit's files in a hidden dir of its own in the
dataset dir (`.transport.<pid>.<n>`) and moves them into place once the whole
unit has decoded and, but for the CLI's `decode`, which checks no hash, every
file's sha256 is the one `files` gives; the files they replace go into that
dir first and are removed once all are in. On any error the dataset dir is
as it was once the decode finished what killed decodes left there (below):
the files moved in so far are taken out again and the ones they replaced put
back. Two decodes of one unit into a dir at once on one machine each put
complete files in place, the later install's standing. So a download needs
the largest unit's files' size free beside the dataset (planet `poi/all`:
1.16 GB), and a unit that fails is fetched again whole. Decoding holds the
zstd window (8 MiB) and one chunk of the unit's columns (1 MiB, more only for
a single polygon or arc larger than that).

A decode that stops before it can clean up (its process killed, say) leaves
its dir behind, perhaps part way through moving files into place. What follows
holds for a process that stops, not for a machine that does: a decode syncs
nothing to disk, so after a power cut a file it moved in may hold bytes that
never reached the disk, and the file it replaced be gone. Check the dataset's
files against their `sha256` then.

On unix decodes take two locks, both exclusive flock(2) locks: the install
lock, on the dataset dir, and each decode's own lock, on its own dir. A
decode:

1. takes the install lock, waiting while another decode holds it;
2. finishes the leftovers: it tries the lock of each `.transport.<pid>.<n>`
   dir there; one held is a running decode's, decoding into it, and stays;
   one it takes is a dead decode's, and is finished (below), unless it is
   gone by then: its decode removed it, empty, before it let go of the lock;
3. makes its own dir and takes its lock, held until after the dir is
   removed, then lets go of the install lock;
4. decodes the unit's files, as other decodes do theirs at the same time;
5. takes the install lock again and finishes the leftovers again (decodes
   that died meanwhile; its own dir is locked, so it stays): a lock that
   fails here fails the decode before it moves a file in, whatever the
   error, ENOLCK too (which flock(2) gives also when the kernel is out of
   lock records), as only the lock of step 1 tells a file system without
   locks;
6. moves its files into place, removes its dir and only then lets go of the
   install lock (on an error, once it put back what it replaced).

So installs go one at a time, and an install killed part way is finished
before any later install goes in, never after it.

A decode puts an empty `.installed` in its dir once all its files are in
place, before it removes the ones they replaced: a leftover holding it is
removed, with those files, and the decode's files stay. A leftover without it
is undone: each file the decode replaced (`.earlier.<name>` in the dir) goes
back to `<name>` in the dataset dir, over the decode's file or where there is
none, then the dir is removed. A file the decode moved in that replaced none
stays, complete and checked as any decoded file is. A dir that can't be read,
or holding a file that can't go back (a directory in its place, say), stays
with what is still in it, for a later decode to try again; so does the dir of
a decode that failed and couldn't put back a file it replaced (its error names
the file). While such a dir would put a file back over one a decode is to put
in place, or can't be read, opened or locked, so that no decode can tell, that
decode fails before it moves a file in, naming the dir: finished after the
install, the dir would undo it. A decode it holds no file for goes ahead. The
server's downloader finishes the leftovers in a dir, as step 2 does, before it
reads the dir, and fails while one stays that would put a file back: else a
later decode would put back a file once the download had listed the files to
remove. On a
file system without locks, or on platforms other than unix, decodes take no
locks at all: none finishes leftovers, which stay until removed by hand, and
installs are not serialised (the last to move a file in wins, and one that
fails while another installs may undo some of that one's files).

The locks are flock(2) locks, and on a network file system (NFS, SMB) a lock
on a dir is seen only by the machine that takes it. A decode on one machine,
into a dataset dir another machine shares, takes the install lock while a
decode on the other machine holds it, and finds that decode's dir unlocked:
it may finish that dir while the decode runs, leaving older files in place of
its files or failing it. Decode into such a dir from one machine at a time.

## latest.json

The entry point for all clients:

```json
{
  "build_version": 19,
  "patch_version": 18,
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
client UI — the credit of the data (`attribution`, below), axes
(region/mode/quality/poi_tier), components (which files belong to each
axis value), constraints, presets, the flat `files` map keyed by relative
path with `size_raw`/`sha256` and either `size_zst` (the file's own `.zst`)
or `unit` (the transport unit that holds it), and the `units` map of the
build's transport units:

```json
{
  "schema_version": 1,
  "build": {
    "version": 19,
    "patch_version": 18,
    "date": "2026-04-10",
    "previous": "2026-04-09",
    "built_at": "2026-04-10T09:00:14Z",
    "wikidata_date": "2026-04-08"
  },
  "attribution": [ { "id": "osm", "text": "© OpenStreetMap contributors", "licence": "ODbL 1.0", ... }, ... ],
  "axes":        { "region": { ... }, "mode": { ... }, ... },
  "components":  { "mode.full": { "files": ["{region}/full/...", ... ] }, ... },
  "constraints": [ { "when": { "mode": "admin-minimal" }, "requires": { "quality": "simplified" } } ],
  "presets":     [ { "id": "smallest", "selections": { ... } }, ... ],
  "transport":   { "version": 1, "units": [ { "unit": "places.tt.zst", "kind": "places", "files": [ ... ] }, ... ] },
  "files": {
    "planet/full/admin_cells.bin": { "size_zst": 6393328, "size_raw": 24807168, "sha256": "..." },
    "planet/full/place_nodes.bin": { "size_raw": 30573231, "sha256": "...", "unit": "planet/full/places.tt.zst" },
    "...": "..."
  },
  "units": {
    "planet/full/places.tt.zst": {
      "kind": "places", "transport_version": 1, "size": 30427178,
      "files": ["planet/full/place_cells.bin", "planet/full/place_nodes.bin", "planet/full/place_hidden.bin"]
    },
    "...": "..."
  }
}
```

The static portions (attribution/axes/components/constraints/presets/
transport) come from `geocoder/builder/configurations.template.json` and are
stable across builds. CI splices in `build.*` and the `files` and `units`
tables during the build's compress step. `transport` lists the units a dir publishes when
it holds all of their files; `units` the ones this build has, each with its
kind, the transport version it was written in, its bytes on the wire and its
files. A client refuses a unit of a `transport_version` it doesn't read.

A component's `optional_files` are written only when the data has them
(`addr_cities.bin` where a region has address points, `interp_postcodes.bin`
in TIGER regions, the postcode centroids where a region has postcodes,
`postcode_grid.bin` in the mode dirs of a region holding the United States); a
client skips any that are absent from `files`.

A component's `variants` add one more component, picked by the whole
selection: the first variant whose `when` names the selection's value on
every axis it lists (a variant without `when` takes any selection). Its
`component` is resolved like an axis component, `files`, `optional_files`,
`replaces` and all.
`strings_layout.json`, `poi_meta.json`, `attribution.json` and
`poi_attribution.json` are listed and compressed like the `.bin` files: the
server can't load a region without the first two, and the last two are the
credit the data is owed.

## Attribution

The data is owed credit, and the dataset carries it in two places:

- **`attribution`** in configurations.json: a list of every source, each with
  an `id`, the `text` to show, the source's `url`, its `licence` (and
  `licence_url`, but for public domain), the `notices` it must travel with,
  verbatim, and the components whose files can hold its data
  (`applies_to`, patterns over the component names):

  | `id` | Source | Licence | In |
  |---|---|---|---|
  | `osm` | OpenStreetMap: the dataset is a derivative database of it | ODbL 1.0 | every dir |
  | `geonames` | GeoNames: postcode centroids where OSM has none (`--external-postcodes`; GB_full, which the nightly loads), with the notices of its readme (the Royal Mail one GB_full's, where a GB centroid is there) and the changes the dataset made (CC BY 4.0 section 3(a)(1)(B)) | CC BY 4.0 | the dirs of `full`, `no-addresses` and `admin`, the ones holding postcode centroids |
  | `codepoint-open` | Ordnance Survey Code-Point Open, which GB_full is derived from, with the Ordnance Survey, Royal Mail and National Statistics statements of its licence | OGL v3.0 | the same, beside `geonames` where a GB centroid is there |
  | `tiger` | US Census Bureau TIGER/Line: US address ranges and ZIPs, postcode centroids where OSM has none, and the postcode grid's votes of the ranges' ZIPs | Public domain, credited as a courtesy | the same, and the admin-minimal dirs, which hold the postcode grid |
  | `wikidata` | Wikidata: the sitelink counts of the POIs' items, which their importance takes (`--wikidata-sitelinks`, which the nightly passes) | CC0 1.0 | the POI tier dirs holding a POI whose importance took one |

- **`attribution.json`** in every mode dir (`full/`, `no-addresses/`,
  `admin/`, `admin-minimal/`, `admin-minimal-poi/`): a JSON array of the
  entries above, without `applies_to`, of the sources of the data that dir
  holds, in the list's order. OSM's is always there; GeoNames's where the
  dir holds a postcode centroid that came from GeoNames, Code-Point Open's
  and GeoNames's notice of GB_full where one of them is a GB postcode's,
  TIGER's where it holds an interpolation or a centroid that did, or a
  postcode grid of a build whose TIGER ranges voted in it
  (so `europe/admin/` credits GeoNames and Code-Point Open,
  `north-america/full/` TIGER, and the admin-minimal dirs, which hold no
  postcode centroids, OSM alone, and TIGER beside it where they hold the
  grid). The quality dirs hold OpenStreetMap data only, which the mode dir's
  OSM entry covers.
- **`poi_attribution.json`** in each POI tier dir (`poi/major/`,
  `poi/notable/`, `poi/all/`) holding a POI whose importance took the
  sitelink count of its Wikidata item: Wikidata's entry alone, in the same
  form. Their other data is
  OpenStreetMap's, which the mode dir's OSM entry covers, so a dataset with a
  POI tier credits the union of the two files. It has a name of its own as a
  dataset's files land in one dir by name. The template lists it among the
  tier's `optional_files`.

A client shows the entries of the `attribution.json` and
`poi_attribution.json` it holds wherever it credits map data; the test
portal's page shows the build's `attribution` list, which query-portal's
`/datasets` passes on, and query-server's `/test/credits` gives those of the
datasets it loaded. Every reverse answer also carries Nominatim's `licence`
string (README.md).

## Client Decision Tree

### Fresh Install

```
1. GET builds/latest.json
2. Choose a date >= oldest_indexes (typically use latest)
3. GET builds/{date}/configurations.json
4. Choose region (e.g. europe), mode (e.g. full), quality (e.g. simplified), and POI tier (e.g. notable)
5. Download every file the selected components and the variants they pick
   list (their `files`, the `optional_files` that appear in `files`, minus
   any `replaces`):
   - a file with a `unit`: through that unit, once for all the files it
     holds (builds/{date}/{unit}), decoded into them (Transport units,
     above) and each checked against its sha256; a selection takes every
     file of each unit it needs
   - any other: as <path>.zst, decompressed and checked against its sha256
   - builds/{date}/europe/full/…                (24 files + up to 4 optional)
   - builds/{date}/europe/quality/simplified/…  (4 files)
   - builds/{date}/europe/poi/notable/…         (7 files + 1 optional, with a POI tier)
   The JSON files (strings_layout.json, poi_meta.json, attribution.json,
   poi_attribution.json) are part of the set: the server can't load a region
   without the first two, and the last two are the dataset's credit
   (Attribution, above).
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
string tier it rebuilds (of its FCC2 file), and the tool fails when one differs. A size check
only catches a file from another build when its size changed; the string
tiers' hashes are the content check. The server combines a mode dir with a
quality or poi dir by global string offsets, so patch every selected dir to
the same date and swap the whole set in together.

### Version Mismatch

```
1. build_version changed → file structure is different, must fresh install
2. patch_version changed → patch format is different, must fresh install
3. Follow "Fresh Install" steps above; a unit of a transport_version the
   client doesn't read takes a client that does
```

### Switching Quality

Quality changes don't require patching — just download the new quality files:

```
1. Download the files the quality.{new_quality} component lists for
   {current_date} (its `files` and the `optional_files` that appear in
   `files`), each through its unit (arcs.tt.zst, admin_polygons.tt.zst) or
   as <path>.zst, as a fresh install does, and check it against its sha256
2. Replace the local quality dir with them: admin_polygons.bin,
   admin_rings.bin, arc_index.bin and arc_vertices.bin
```

### Switching Region

Regions are independent. Download the new region's mode + quality directories.

## Retention Policy

| Data | Retention | Notes |
|------|-----------|-------|
| Index files (`.bin.zst`, `.tt.zst`) | 3 days | Full index available for fresh downloads |
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
  decompressed copy, each old string tier while it decodes it, and the remap
  runs it spills to its scratch dir are mmapped. Long unchanged runs and
  unchanged files are copied in the kernel (`copy_file_range`), or through
  `pread` where the kernel, the filesystem or a seccomp profile refuses that
- Peak anonymous memory: about 2 MiB for every variant and whatever the
  patch; everything else is reclaimable page cache. That is the
  default build, which decompresses the patch with libzstd; one built with
  `-DGEOCODER_PATCH_LIBZSTD=OFF` also runs a `zstd` child that allocates
  about 11.3 MiB beside it.
  Its scratch dir, which holds the decompressed patch, is under `$TMPDIR`:
  keep that on disk, since a RAM-backed one counts against memory
- Runs on one core inside a 32 MiB memory limit (page cache included): one
  day of `planet/full` takes a little over 2 minutes on an SSD. On a slow
  disk it is disk-bound: it reads the old variant and writes the new one,
  two to three times the variant's size in IO
- Needs free space for the new copy of the variant (it never patches in place)
- Produces byte-identical output to a fresh build
