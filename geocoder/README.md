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

## Quick Start

Serve the published indexes:

```bash
cargo build --release --manifest-path server/Cargo.toml
./server/target/release/query-server data
```

The server downloads a continent from the latest build the first time a query
lands in it: `admin` mode, `q2.5` boundaries and no POIs unless an admin sets
another selection with `PUT /admin/configuration` (a JSON body of `base_url`,
`mode`, `quality`, `poi_tier` and the regions to `preload`). Queries need an
API key from the dashboard (see Authentication below).

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
- **Node binding** (`node/`) - The native module Immich uses to geocode in-process: `Geocoder.load(dir)`, then `reverseGeocode(lat, lng)` resolves to the response JSON as a string.
- **Test portal** (`test-portal/`) - The map page served at `/test`, and a compose file that runs it on the published index.

### Index Structure

A full index holds the files below. The published variants split them
between a mode dir, a quality dir and a POI dir; the files of each are listed
in [PATCHING.md](PATCHING.md) and `builder/configurations.template.json`.

| Files | Description |
|------|-------------|
| `geo_cells.bin` | Merged S2 cell index for streets, addresses, and interpolations |
| `street_entries.bin`, `street_ways.bin`, `street_nodes.bin` | Street way IDs per cell, way headers (node offset, name), node coordinates |
| `way_parents.bin`, `way_postcodes.bin` | Per street: smallest containing admin polygon, postcode from its postal boundary |
| `addr_entries.bin`, `addr_points.bin`, `addr_vertices.bin` | Address point IDs per cell, point data (coordinates, house number, street), building outlines |
| `addr_postcodes.bin` | Per address point: its own `addr:postcode` |
| `interp_entries.bin`, `interp_ways.bin`, `interp_nodes.bin` | Interpolation way IDs per cell, way headers, node coordinates |
| `interp_postcodes.bin` | Per interpolation way: its TIGER ZIP (TIGER regions only) |
| `admin_cells.bin`, `admin_entries.bin` | S2 cell index for admin boundaries |
| `admin_polygons.bin`, `admin_vertices.bin` | Admin polygon metadata and vertices (per quality variant) |
| `admin_parents.bin` | Per admin polygon: the next larger polygon containing it |
| `postal_polygons.bin`, `postal_vertices.bin` | Postal code boundaries (per quality variant, where they exist) |
| `place_cells.bin`, `place_entries.bin`, `place_nodes.bin` | Place nodes (city, town, village, suburb, ...) and their cell index |
| `postcode_centroids.bin`, `postcode_centroid_cells.bin`, `postcode_centroid_entries.bin` | Postcode centroids and their cell index |
| `poi_records.bin`, `poi_vertices.bin`, `poi_cells.bin`, `poi_entries.bin` | Points of interest and their cell index |
| `strings_core.bin`, `strings_street.bin`, `strings_addr.bin`, `strings_postcode.bin`, `strings_poi.bin` | Deduplicated string pool, in five tiers so each variant ships only the strings it uses |
| `strings_layout.json` | Start and end of each string tier, which place every string offset |
| `poi_meta.json` | POI category names and scoring parameters |

## Building from Source

### Prerequisites

**Builder (C++):**
- CMake 3.16+
- C++17 compiler
- protozero, s2geometry and abseil, libdeflate (`build-index`); zlib and expat (`pbf-apply`)
- the `zstd` command, which `geocoder-diff` and `geocoder-patch` run on patches

**Server (Rust):**
- Rust toolchain

### Build

```bash
# Build the indexer
mkdir build && cd build && cmake ../builder && make

# Build the server
cargo build --release --manifest-path server/Cargo.toml
```

### Run

```bash
# Create index from PBF file
./build/build-index output-dir input.osm.pbf [input2.osm.pbf ...]

# Start the server
./server/target/release/query-server output-dir [bind-address]

# Start with automatic HTTPS (certificates cached in --cache, default acme-cache)
./server/target/release/query-server output-dir --domain geocoder.example.com
```

### Planet builds

A full planet build (`--continents --multi-output --multi-quality`, the
nightly's configuration) takes about 5 minutes on 64 cores and fits in
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
