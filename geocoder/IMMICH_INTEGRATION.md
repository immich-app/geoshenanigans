# Immich Integration Plan

Plan for replacing Immich's built-in reverse geocoding with this geocoder,
run as a companion container. Based on reading the Immich source at the
paths referenced below (server @ NestJS monorepo layout, 2026-07).

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

## 3. Integration architecture (mirrors the ML sidecar)

Immich already has the exact pattern to copy — the machine-learning
service:

- env/config: `server/src/config.ts:291` (`machineLearning.urls`,
  default `http://immich-machine-learning:3003`, enabled flag)
- HTTP client with health polling + multi-URL failover:
  `server/src/repositories/machine-learning.repository.ts` (polls `/ping`
  every 30 s, tries healthy URLs first, marks failures unhealthy, throws
  only when all fail)
- zod DTO drives the admin settings UI:
  `server/src/dtos/system-config.dto.ts:175`
- compose service with named cache volume, image healthcheck, no
  `depends_on` from the server (graceful degradation):
  `docker/docker-compose.yml:34`
- remote/optional service documentation pattern:
  `docs/docs/guides/remote-machine-learning.md`

### Phase 1 — sidecar behind config (no schema changes, zero default-behavior change)

1. **Compose service** (`docker/docker-compose.yml`):

   ```yaml
   immich-geocoder:
     container_name: immich_geocoder
     image: ghcr.io/immich-app/immich-geocoder:${IMMICH_VERSION:-release}
     environment:
       - GEOCODER_ALLOW_ANONYMOUS=true
       - GEOCODER_DATASET=planet-admin     # downloaded on first boot (1.3 GiB)
       - GEOCODER_WORKER_THREADS=4
     volumes:
       - geocoder-data:/data
     restart: always
     mem_limit: 128m                       # 64m works; headroom for cache
     healthcheck:
       test: ["CMD", "curl", "-fsS", "http://localhost:3000/health"]
   ```

   No published ports — the server reaches it on the internal network.

2. **SystemConfig extension** (mirror the ML shape, keep back-compat):

   ```ts
   reverseGeocoding: {
     enabled: boolean;          // existing flag, unchanged
     urls: string[];            // NEW; [] = built-in Postgres geodata path
     availabilityChecks: { enabled, timeout, interval };  // NEW
   }
   ```

   Env default: `IMMICH_GEOCODER_URL` → `urls`. Empty `urls` keeps the
   current behavior exactly, so existing installs are unaffected and the
   compose addition is opt-in until Immich flips the default.

3. **New `geocoding.repository.ts`** modeled line-for-line on
   `machine-learning.repository.ts`: health polling against `/health`,
   ordered failover across `urls`, single method
   `reverseGeocode(point) → GET /reverse?lat=&lon=`.

4. **Repository switch** in `map.repository.ts::reverseGeocode`: if
   configured and healthy, call the sidecar and map the response;
   on any failure fall back to the existing Postgres path (which stays
   intact — it is the fallback and the default). Field mapping:

   | Immich | Geocoder response (`address`) |
   |---|---|
   | `city` | `city ?? town ?? village ?? hamlet ?? municipality` |
   | `state` | `state ?? province ?? region` |
   | `country` | `country` |

   The cascade mirrors how Nominatim consumers derive a settlement name;
   all keys are already Nominatim-parity so behavior is predictable.

5. **Dataset provisioning**: extend `entrypoint.sh` with a
   `GEOCODER_DATASET=<name>` mode that invokes the built-in manifest
   downloader (streaming, sha256-verified, resumable per file) into
   `/data` on first boot, then starts the server. The existing
   `PBF_URLS`/build mode stays for self-builders.

6. **Backfill**: no new job needed — the existing admin "Extract
   Metadata" re-run repopulates `asset_exif` through the new path.
   Optionally a targeted variant that only re-processes assets with GPS
   coordinates (cheap query on `asset_exif.latitude is not null`).

### Phase 2 — richer metadata (options for later)

Kept out of phase 1 to avoid schema churn; candidates once the sidecar
is established:

- store the full `display_name` (asset detail panel: street-level "where
  was this taken"),
- suburb/neighbourhood columns for finer-grained places browsing,
- POI landmark attribution from the `poi` tier ("taken at the Louvre"),
  which also unlocks landmark search terms,
- postcode column (requires `no-addresses`+ dataset for quality).

Each is additive: larger dataset tier + one nullable column + mapping.

### Phase 3 — distribution & rollout

- **Image**: publish `immich-geocoder` from this repo's CI (Dockerfile
  already has a HEALTHCHECK; multi-arch build needed — amd64/arm64).
- **Datasets**: host the prebuilt `planet-admin` (1.3 GiB) + continent
  subsets behind the existing manifest format. The day-to-day patch
  pipeline (couple-hundred-MB weekly deltas after the d109013 differ
  fix) keeps update bandwidth low; the server's `.build_date`-keyed
  downloader already handles updates.
- **Docs**: a `docs/docs/guides/reverse-geocoding.md` modeled on
  `remote-machine-learning.md` (enable in admin settings, remote
  instance option, dataset size table, memory guidance).
- **Rollout**: ship default-off (empty `urls`); flip the compose default
  after a release cycle of soak; the Postgres geodata path remains as
  fallback indefinitely (it is also what keyless quick-start installs
  without the sidecar keep using).

## 4. Performance / capacity notes

- Measured 4,000 mixed real-coordinate queries in ~1 s through HTTP
  (8-way parallel) inside a 64 MiB container — far above metadata-job
  ingest rates; no queueing concerns.
- Cold-cache worst case (nothing resident, NVMe): 4,000 queries in
  0.9 s. On spinning disks expect tens of ms per cold query — still
  fine for background metadata jobs.
- The sidecar is stateless over a read-only dataset volume; horizontal
  scaling or remote placement works exactly like remote ML (`urls`
  array).

## 5. Open questions for the Immich team

1. Hosting/bandwidth for dataset downloads (1.3 GiB initial per install;
   who pays — mirror on their CDN vs ours).
2. Appetite for phase-2 schema additions (display_name/suburb/postcode).
3. e2e fixtures: Immich's e2e asserts on geodata results; CI needs a tiny
   fixture dataset (a small regional extract serves).
4. Whether `reverseGeocoding.urls` should instead be a new top-level
   `geocoder` config section (cleaner separation from the legacy flag).
