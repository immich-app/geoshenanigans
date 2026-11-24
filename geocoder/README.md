# Reverse Geocoder

A fast reverse geocoder built from OpenStreetMap data. Given latitude and longitude coordinates, it returns the nearest street address including house number, street name, city, state, county, postcode, and country, resolved the way Nominatim resolves them (see [NOMINATIM_REFERENCE.md](NOMINATIM_REFERENCE.md)), and the notable places nearby.

Based on the open source [Traccar Geocoder](https://github.com/traccar/traccar-geocoder).

## Features

- Street-level reverse geocoding from OSM data
- Address point, street name, and address interpolation lookup (TIGER ranges in the US)
- Administrative boundary resolution (country, state, county, city, postcode)
- Points of interest in three tiers ([POI_OVERVIEW.md](POI_OVERVIEW.md))
- Sub-millisecond query latency with memory-mapped index files
- Nightly indexes with daily patches ([PATCHING.md](PATCHING.md))
- Automatic HTTPS with Let's Encrypt

## Published Indexes

The nightly build (`.github/workflows/geocoder-build.yml`) builds the planet
and eight continents from the day's OSM data and publishes every index
variant, with a patch from the previous day, to Tigris:
`https://geoshenanigans-reverse-geocoding.t3.tigrisfiles.io/geocoder/builds/latest.json`.
[PATCHING.md](PATCHING.md) describes the layout, the variants (mode, boundary
quality, POI tier) and how a client downloads and patches them.

## Monitoring

The nightly reports every run on `main` to FUTO o11y (project
`geoshenanigans`), whatever its outcome; test dispatches from other branches
don't report. Its last step, `builder/tools/o11y_report.py nightly`, pushes
what only its runner has: each build phase's duration, peak memory, the patch
verify counts, each patch's size, the index size, the data's age and the
bytes of planet/full's address footprint stream beside the limit they may not
pass, and logs build.log, the memwatch samples and each variant's verify
result. Every 2 hours the watchdog's `report` job (`o11y_report.py watchdog`,
on a GitHub-hosted runner) pushes the dates in the published `latest.json`
with a heartbeat (none when it can't read `latest.json`), and the latest
finished nightly's outcome, duration and step durations from the GitHub API,
logging that run's steps once. A run whose runner died or that timed out never
reaches its own last step, so its outcome comes from the watchdog.

Both push straight to o11y's public gateway (`O11Y_URL` in the workflows,
`https://vmauth.futostatus.com`), metrics to VictoriaMetrics and logs to
VictoriaLogs, with every sample and log line labelled
`project=geoshenanigans`, `cluster=geocoder`, `env=prod`, `provider=github`,
`region=global`. The gateway's bearer token is read from 1Password
(`op://tf_prod/O11Y_VICTORIAMETRICS_VMAUTH_PASSWORD/password`, with
the `OP_TF_PROD_ENV` service account) inside the reporting step, which hands
it to `o11y_report.py` as `$O11Y_TOKEN`; without one, nothing is sent.

- Dashboard: `geoshenanigans-geocoder-nightly` in the Grafana folder
  `geoshenanigans`.
- Alerts, in the same folder, go to the Rootly project `geoshenanigans`:
  critical when a run fails, a patch fails verify or nothing has been
  published for 30 hours; warning when a run takes over an hour, the
  planet/full patch passes 64 MiB, the data is over two days old or the
  watchdog goes silent.
- Logs, in VictoriaLogs (add `run_id:="<run id>"` for one run, or use
  `app:="geocoder-watchdog"` for the watchdog's):
  `project:="geoshenanigans" app:="geocoder-build"`

`o11y_report.py nightly --dry-run` (or `watchdog --dry-run`) prints the
metrics and log lines instead of sending them.

## Quick Start

Serve the published indexes:

```bash
cargo build --release --manifest-path server/Cargo.toml
./server/target/release/query-server data
```

The server downloads a continent from the latest build the first time a query
lands in it: `admin` mode, `simplified` boundaries and no POIs unless an admin sets
another selection with `PUT /admin/configuration` (a JSON body of `base_url`,
`mode`, `quality`, `poi_tier` and the regions to `preload`). A value the build
doesn't offer fails each download, naming those it does, and the PUT is refused
with them (400) when the server has the build's manifest at hand. A continent
dir keeps a ledger of what each of its files holds, `.file_sha256` (a line
`<name> <sha256>` each, `-` while unknown), and a download skips a file only
where the ledger names the build's sha256 for it. It removes the files its
selection doesn't take (another selection's or build's) that the ledger names,
or whose names files of the build have, the ledger naming them or not; files of
other names stay. The dir names its build in `.build_date` once a whole
selection of it is in and those are gone; a download that fetches or removes
anything removes it first, once it has made the ledger where the dir has none
(a dir holding neither was laid out by hand, and a server loads it). Before it
reads the dir a download finishes what a transport decode killed part way left
there (PATCHING.md), which would otherwise put back the files the decode
replaced once the download had listed the files to remove; one it can't finish
fails the download.
Downloads into a dir go one at a time, those of other servers on the data dir
too: each holds a lock on the dir's `.download.lock` to its end, and runs to
its end though the query that started it gave up. A server checks and
loads a dir, when it starts and once its own download into the dir is in,
under a shared lock on that file, which it makes where it isn't there, so
that it waits for a download into the dir (another server's) to end and loads
what it left, and the first download into a dir waits for it; after its own
download it waits on a thread of its own, and answers queries meanwhile. A
dir it can't make the file in (read-only, or not writable to it) loads
without a lock. A download changes the dir's `.build_date` or `.file_sha256`
before any other file, and writes them by appending or by renaming a new file
into place: a server notes both (device, inode, size and modification time)
before it checks a dir and again once it loaded it, and where they differ, a
download ran meanwhile, it drops what it loaded and says so, the continent
left unloaded. A server that finds a ledger but no stamp in a dir leaves that
continent unloaded until a query or a preload finishes its download. Queries
need an API key from the dashboard (see Authentication below).

Servers can share a data dir. Those that run as one user are safe side by
side: their downloads into a dir go one at a time and their loads wait for
them, by the lock on `.download.lock`. A server that can't write the data dir
(a read-only mount, or a dir of another user) is safe beside one that
downloads into it: it takes the lock where `.download.lock` is there, else
loads the dir without it and drops what it loaded where a download ran
meanwhile, by the check of `.build_date` and `.file_sha256` above. Servers of
several users can each download into the data dir only where each can write
every file and dir the others' downloads make in it: the ledger, the temp
files of a fetch, of the ledger and of the stamp, and the dir of a decode
killed part way, which the next download finishes. Give the servers a group of
their own and, before their first download, make the data dir that group's
with group write and the setgid bit (`chgrp`, then `chmod g+ws`), so that what
is made in it is that group's too; and run each server with a umask of 002.
Under a umask of 022 a server's download into a dir whose ledger another
server made fails, naming the file. A download that can't write
`.download.lock` (one a server of umask 022 made) takes its lock on it open to
read, which a local file system allows; on NFS an exclusive lock needs the
file open to write, so there it fails, saying so.

The test portal is a map page for checking answers, served by every query
server at `/test`. Its compose file runs two servers on the latest published
index, a full one and a minimal one, side by side:

```bash
cd test-portal && docker compose up --build
# open http://localhost:3002/test
```

## API

### GET /reverse

Query parameters:
- `lat` - latitude (required)
- `lon` - longitude (required)
- `key` - API key (required)

Example request:

```
GET /reverse?lat=43.7384&lon=7.4246&key=YOUR_API_KEY
```

Response follows [Nominatim](https://nominatim.org/release-docs/latest/api/Reverse/) format:

```json
{
  "display_name": "Avenue de la Costa 42, 98000 Monaco, Monaco",
  "address": {
    "house_number": "42",
    "road": "Avenue de la Costa",
    "city": "Monaco",
    "state": "Monaco",
    "county": "Monaco",
    "postcode": "98000",
    "country": "Monaco",
    "country_code": "MC"
  }
}
```

Fields are omitted when not available.

Status codes:
- `200` - success
- `401` - missing or invalid API key
- `429` - rate limit exceeded

### Authentication

The server includes a web dashboard for managing API keys. On first launch, navigate to the server URL in a browser to create an admin account. Once logged in, you can generate API keys and create additional users with configurable rate limits.

## Architecture

The project consists of four components:

- **Builder** (`builder/`, C++) - `build-index` parses OSM PBF files and creates a compact binary index using S2 geometry cells for spatial lookup. `geocoder-diff` and `geocoder-patch` make and apply the daily patches ([builder/PATCHING.md](builder/PATCHING.md)), and `pbf-apply` applies OSM replication diffs to a PBF.
- **Server** (`server/`, Rust) - Memory-maps the index files and serves queries via HTTP/HTTPS with sub-millisecond latency. It is also the `query_server` library the binding wraps.
- **Node binding** (`node/`) - The native module Immich uses to geocode in-process: `Geocoder.load(dir)`, then `reverseGeocode(lat, lng)` resolves to the response JSON as a string. `openPlaceSearch()` opens the place search index of the dataset (shipped with the bigger packs, else built on the device; see [builder/PATCHING.md](builder/PATCHING.md#place-search-index)), then `searchPlaces(query)` resolves to the results as a JSON string. `createTransportDecoder(dir, files)` decodes a downloading transport unit into the dataset dir ([PATCHING.md](PATCHING.md#transport-units)).
- **Test portal** (`test-portal/`) - The map page served at `/test`, and a compose file that runs it on the published index.

### Index Structure

A full index holds the files below. The published variants split them
between a mode dir, a quality dir and a POI dir; the files of each are listed
in [PATCHING.md](PATCHING.md) and `builder/configurations.template.json`.
Some download as transport units, a download-only column form that decodes
to the same files ([builder/PATCHING.md](builder/PATCHING.md#transport-units)).

| Files | Description |
|------|-------------|
| `geo_cells.bin`, `geo_entries.bin` | S2 cell index for streets, addresses and interpolations: each cell's three lists, in blocks of 64 cells, each list against the one before (GXT2, `builder/src/geo_index.h`); the server reads a query's cell and its neighbours in one walk per block |
| `street_ways.bin`, `street_nodes.bin` | Street ways: a row per way (its name, where its nodes start) in groups of 16 ways, and the ways' nodes as deltas in a block per way (`builder/src/way_files.h`). A fresh build writes the ways in cell order: by the least street cell that lists a way, then by name and nodes, so the ways of one place sit together (a continent's files keep the planet's order) |
| `way_parents.bin`, `way_postcodes.bin` | Per street: smallest containing admin polygon, postcode from its postal boundary; a value per street in blocks of 64 (`builder/src/side_blocks.h`) |
| `addr_points.bin`, `addr_vertices.bin`, `addr_index.bin` | Address points (coordinates, house number, street) in blocks of 16 records, building outlines as a blob per building, and where each block and its blobs start (`builder/src/addr_files.h`); in cell order as the ways are, then by street and house number |
| `addr_postcodes.bin` | Per address point: its own `addr:postcode` where its country's pattern takes it, in blocks as the streets' side files |
| `interp_ways.bin`, `interp_nodes.bin` | Interpolation ways and their nodes, in the street ways' format; in cell order as the ways are, then by street and range |
| `interp_postcodes.bin` | Per interpolation way: its TIGER ZIP (TIGER regions only), in blocks as the other side files |
| `admin_cells.bin`, `admin_entries.bin` | S2 cell index for admin boundaries: lists in blocks of 8 cells, each against the one before (the cell pair layout, `builder/src/cell_pairs.h`) |
| `admin_polygons.bin`, `admin_rings.bin` | Admin polygon records (postcode areas among them, at level 11; a place area linked to a place node of another type keeps its own place tag in the last byte, whose top bit marks a closed way's polygon) and their rings, as refs to arcs |
| `arc_index.bin`, `arc_vertices.bin` | The boundary arcs those rings share, each border once (per boundary tier: `quality/simplified` or `quality/exact`) |
| `admin_parents.bin` | Per admin polygon: the next larger polygon containing it |
| `place_cells.bin`, `place_nodes.bin`, `place_hidden.bin` | Place nodes (city, town, village, suburb, ...): the visible ones in admin cell order, bit-packed in blocks of 64, with each cell's first row; the ones a boundary hides (its label for place search) beside them (`builder/src/place_files.h`) |
| `postcode_centroids.bin`, `postcode_centroid_cells.bin` | Postcode centroids in admin cell order, Rice-coded steps in blocks of 32 with a box per block (PCC1, `builder/src/postcode_files.h`), and each admin cell's rows of them |
| `poi_records.bin`, `poi_vertices.bin`, `poi_cells.bin`, `poi_entries.bin` | Points of interest and their cell index (a list per cell) |
| `strings_core.bin`, `strings_street.bin`, `strings_addr.bin`, `strings_postcode.bin`, `strings_poi.bin` | Deduplicated string pool, in five tiers so each variant ships only the strings it uses; each tier front-coded in buckets cut by content (FCC2, `builder/src/string_files.h`), a string's id still its offset in the tier's pool |
| `strings_layout.json` | Start and end of each string tier, which place every string offset |
| `poi_meta.json` | POI category names and scoring parameters |

## Building from Source

### Prerequisites

**Builder (C++):**
- CMake 3.16+
- C++17 compiler
- protozero, s2geometry and abseil, libdeflate (`build-index`); zlib and expat (`pbf-apply`)
- the static libzstd, `libzstd.a` (`geocoder-patch`, which decompresses patches in process;
  Debian/Ubuntu `libzstd-dev`, Fedora `libzstd-static`)
- the `zstd` command, which `geocoder-diff` compresses patches with

**Server (Rust):**
- Rust toolchain

### Build

```bash
# Build the indexer
mkdir build && cd build && cmake ../builder && make

# Without libzstd: geocoder-patch runs the zstd command on PATH instead
cmake ../builder -DGEOCODER_PATCH_LIBZSTD=OFF && make

# Build the server
cargo build --release --manifest-path server/Cargo.toml
```

### Run

```bash
# Create index from PBF file, into a new or empty output-dir (it refuses any other:
# an earlier build's files left there would be published as this one's). The index
# is output-dir/planet; with --continents, each continent's dir beside it is one too
./build/build-index output-dir input.osm.pbf [input2.osm.pbf ...]

# Start the server (on output-dir instead, a --continents build serves continent by continent)
./server/target/release/query-server output-dir/planet [bind-address]

# Start with automatic HTTPS (certificates cached in --cache, default acme-cache)
./server/target/release/query-server output-dir/planet --domain geocoder.example.com
```

### Planet builds

A full planet build (`--continents --multi-output --multi-quality`, the
nightly's configuration; `--multi-quality` writes the two boundary tiers,
`quality/simplified` and `quality/exact`, which `--multi-output` needs: its
mode dirs hold no admin geometry and are read beside a tier) takes about 5 minutes on 64 cores and fits in
80 GB of RAM (6.5 minutes there): the write phase sizes how much it runs at
once to the memory limit, the smaller of MemTotal and the process's cgroup
limit. Set
`GC_MEMORY_LIMIT_MIB` to cap it further when other work shares the host,
or `GC_MEMORY_RESERVE_MIB` to keep that much of the limit for other work
under it (the nightly's fetch shares the job's cgroup).
Disk: the planet PBF (~95 GB) plus the output (~120 GB), and the previous
day's output for `--prev-output`.

The distro S2 and abseil packages keep their debug checks on. The nightly
links static copies built from the same package sources without them
(`tools/build_s2.sh <prefix>`, then `cmake -DGEOCODER_S2_PREFIX=<prefix>`).
The indexes are identical either way; the release libraries are faster.

`pbf-apply in.osm.pbf change.osc.gz... -o out.osm.pbf` applies replication
diffs with the same result as `osmium apply-changes`, in a few minutes on a
planet. `--direct-io` writes the output around the page cache. That is faster
on a fast NVMe, but the next reader of the output then finds none of it cached.

## Memory Footprint

The server is designed to run inside memory-constrained deployments
(e.g. as an Immich sidecar on small NAS/VPS hosts):

- All index files are memory-mapped with `MADV_RANDOM`; nothing is
  loaded into the heap at startup. Process anonymous memory stays
  under ~10 MiB regardless of dataset size and query volume.
- Resident file pages are clean page cache: the kernel reclaims them
  under memory pressure, and a cgroup/container limit simply bounds
  the cache working set. Measured on a full planet index: stable
  operation and ~5–15 ms warm-query latency inside a hard 32 MiB
  container limit; a 64–256 MiB limit buys better cache hit rates on
  busy instances.
- Dataset downloads stream (HTTP → zstd decode → hash → disk) with
  RAM bounded by a few network chunks, so multi-GiB index files can
  be fetched under the same limits.
- The async worker pool defaults to `min(cores, 8)` threads; override
  with `GEOCODER_WORKER_THREADS`.
- `GET /health` returns `200 {"status":"ok","regions":N}` once the
  index is serving — wire it to container health checks.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GEOCODER_WORKER_THREADS` | Async worker pool size | `min(cores, 8)` |
| `GEOCODER_ALLOW_ANONYMOUS` | `true` accepts requests without an API key, configuration changes included, with no rate limit: only for a server on a private container network | (disabled) |

## License

    Apache License, Version 2.0

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
