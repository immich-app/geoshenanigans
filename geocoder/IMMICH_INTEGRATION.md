# Immich Integration Plan

Plan for replacing Immich's built-in reverse geocoding with this geocoder,
embedded in-process in `immich-server` (no additional container). Based on
reading the Immich source at the paths referenced below (server @ NestJS
monorepo layout, 2026-07).

## 1. How Immich reverse-geocodes today

- **Implementation**: `server/src/repositories/map.repository.ts:148`
  (`reverseGeocode`). Nearest GeoNames `cities500` row within 25 km
  (`reverseGeocodeMaxDistance`, Postgres earthdistance query), falling back
  to Natural Earth country polygons, falling back to nulls. Returns
  `{ country, state, city }` where `state` is the GeoNames admin1 name.
- **Data**: baked into the server base image at build time
  (`base-images/server/Dockerfile:85` downloads `cities500.zip` +
  `admin1CodesASCII.txt` / `admin2Codes.txt` + Natural Earth GeoJSON),
  imported into the `geodata_places` Postgres table on startup whenever
  `geodata-date.txt` changes (`map.repository.ts:55`).
- **Consumer**: `server/src/services/metadata.service.ts:263` — during EXIF
  extraction, GPS coordinates are reverse-geocoded and
  `{country, state, city}` written to `asset_exif`. Gated by
  `SystemConfig.reverseGeocoding.enabled` (`server/src/config.ts:111`).
- **Downstream**: the `asset_exif.city/state/country` columns power the
  places/search features (`server/src/repositories/search.repository.ts:385`).

Limitations of the current approach this replaces: nearest-populated-place
matching is wrong near borders and in rural areas (assigns the nearest big
town's name and its admin1, not the containing polygon), tops out at
city granularity, and the ~50 MB geodata import lives inside Postgres.

## 2. What the geocoder provides

- Polygon-exact containment (Nominatim-parity rules) instead of
  nearest-place-within-25 km: correct city/municipality/state at borders,
  suburbs/neighbourhoods, localized names, postcodes, optional
  street/house-number and POI tiers.
- Verified constrained-memory operation: ~10 MiB anonymous heap, correct
  serving inside a 32 MiB container limit, 5–15 ms warm queries
  (see README "Memory Footprint").
- `GET /health` for orchestration; `GEOCODER_ALLOW_ANONYMOUS=true` for
  keyless use on private container networks; streaming dataset downloader
  (RAM-bounded, resumable per file, sha256-verified).
- **Dataset sizes** (planet, per mode — measured on the 2026-07 build):

  | Mode | Size | Contents |
  |---|---|---|
  | `admin` (+ simplified polygons) | **1.3 GiB** | country/state/county/city/municipality/suburb/quarter + approximate postcode — everything Immich consumes today, strictly better accuracy |
  | `no-addresses` | 16 GiB | + streets, POIs |
  | `full` | 32 GiB | + house numbers, address interpolation |

  Continent subsets exist for all modes. The **`admin` mode is the
  recommended Immich default**: it maps 1:1 to Immich's stored fields at
  1/25th the size of full. (Postcode quality is reduced in this mode —
  centroid-only — but Immich does not consume postcodes.)

## 3. Integration architecture — in-process, no new container

Immich keeps its container count minimal, so the geocoder embeds inside
`immich-server` itself. Two viable shapes, both with precedent in the
Immich codebase; A is the recommendation.

### Option A (recommended): napi-rs native module

The query engine (`server/src/lib.rs`) is a dependency-clean Rust
library — no tokio/axum/http anywhere in the query path (those live only
in the standalone binary). Wrap it as a native Node addon the same way
`sharp` wraps libvips (already a direct dependency of immich-server, so
native prebuilds are an established part of their toolchain):

- **New crate** `geocoder/node/` in this repo using napi-rs, exposing:

  ```ts
  class Geocoder {
    static load(dir: string): Geocoder;          // mmaps the index, ~3 MiB RSS
    reverseGeocode(lat: number, lon: number): AddressResult;  // sub-ms warm
    close(): void;
  }
  downloadDataset(name: string, destDir: string,
                  onProgress?: (done: number, total: number) => void): Promise<void>;
  datasetInfo(dir: string): { buildDate: string; mode: string } | null;
  ```

  `reverseGeocode` runs as a napi AsyncTask (libuv worker pool) so cold
  page faults on slow disks never block the event loop; warm lookups are
  effectively instant (no HTTP hop, no serialization beyond the result
  object). `downloadDataset` reuses the streaming (RAM-bounded,
  sha256-verified) downloader.

- **Prebuilds**: `linux-x64-gnu` + `linux-arm64-gnu` npm packages from
  this repo's CI (the immich-server image is Debian-based; same targets
  sharp ships).

- **Immich changes** (small, all in existing files):
  - `map.repository.ts::reverseGeocode` calls the native module when a
    dataset is present; the existing Postgres/GeoNames path stays as the
    fallback and remains the out-of-the-box behavior.
  - Field mapping: `city ← city ?? town ?? village ?? hamlet ??
    municipality`, `state ← state ?? province ?? region`,
    `country ← country`.
  - Config: keep `reverseGeocoding.enabled`; add a dataset selection
    (e.g. `reverseGeocoding.dataset: 'geonames' | 'planet-admin' | ...`)
    surfaced in the admin UI via the zod DTO pattern
    (`system-config.dto.ts`).
  - Dataset lifecycle: on first enable, `downloadDataset` fetches
    `planet-admin` (1.3 GiB) into a folder next to the existing model
    cache / upload location; version-keyed like their current
    `geodata-date.txt` mechanism. Long-term Immich can drop the GeoNames
    import from the base image entirely (startup Postgres import and
    ~50 MB of image weight go away).

- **Memory**: the ~10 MiB anonymous heap and mmap page cache now live
  inside the immich-server cgroup. Page cache is reclaimable, so the
  effective added cost under memory pressure is ~10-25 MiB — measured
  behavior identical to the 32-64 MiB container tests, just without the
  container.

### Option B (fallback): vendored binary child process

Exactly the `exiftool-vendored` pattern Immich already relies on (an npm
package shipping a platform binary that the server supervises as a child
process): ship `query-server` per-platform, spawn it bound to
`127.0.0.1:<ephemeral>` or a unix socket with
`GEOCODER_ALLOW_ANONYMOUS=true`, health-check via `/health`, restart on
exit. Zero binding work — everything (server, downloader, health) is
reused as-is; the cost is process supervision plus a localhost HTTP hop.
Worth keeping as the escape hatch if napi prebuilds ever become a
maintenance burden, or as a very cheap proof-of-concept stage.

### Rollout phases

1. **PoC (option B shape, days)**: spawn the existing binary from a
   NestJS provider behind `reverseGeocoding.urls`-style config, validate
   end-to-end metadata flow + places search against the `planet-admin`
   dataset.
2. **Productize (option A)**: napi crate + prebuild CI + npm publish;
   swap `map.repository.ts` to the native call; admin-UI dataset picker;
   download/update job; keep GeoNames fallback.
3. **Enrichment (later, additive)**: `display_name` for the asset detail
   panel, suburb/neighbourhood columns for places browsing, POI landmark
   attribution ("taken at the Louvre") from the `poi` tier, postcode
   column — each is one nullable column + a bigger dataset tier.

## 4. Performance / capacity notes

- In-process lookups remove the HTTP hop entirely; the standalone-server
  measurements (5-15 ms warm under a hard 32 MiB limit, 4,000 mixed
  queries/s sustained) are an upper bound on cost — warm in-process calls
  are sub-millisecond.
- Metadata extraction concurrency in Immich is a handful of jobs; even
  cold-cache spinning-disk lookups (tens of ms) cannot back up the queue.
- The library is `Send + Sync` over immutable mmaps — one shared
  `Geocoder` instance serves all worker calls without locking.

## 5. Open questions for the Immich team

1. Dataset hosting/bandwidth (1.3 GiB initial download per install;
   weekly deltas are a couple hundred MB via the patch pipeline — or
   simple monthly full re-downloads to start).
2. Where the dataset lives on disk (model-cache volume vs upload
   location vs dedicated path) and whether download happens in a job vs
   on-boot.
3. Appetite for phase-3 schema additions (display_name / suburb /
   postcode columns).
4. e2e fixtures: CI needs a tiny regional dataset for deterministic
   geocoding assertions (a small extract builds in minutes and is a few
   MB).
5. napi toolchain: happy to maintain prebuild CI on our side and publish
   the npm package, or vendor the crate into their monorepo — their
   call.
