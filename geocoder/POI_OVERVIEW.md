# Geocoder POI (Points of Interest) System

## Overview

The geocoder now includes a POI layer that tells you what notable places are near a given coordinate. When you reverse geocode a location, the response includes not just the street address and admin boundaries, but also nearby landmarks, parks, museums, airports, natural features, and more.

```json
{
  "display_name": "Avenue Anatole France 5, 75007 Paris, France",
  "address": {
    "house_number": "5",
    "road": "Avenue Anatole France",
    "city": "Paris",
    "country": "France"
  },
  "places": [
    {"name": "Champ de Mars", "category": "attraction", "distance_m": 0.0},
    {"name": "Tour Eiffel", "category": "attraction", "distance_m": 0.0},
    {"name": "Paris, rives de la Seine", "category": "protected_area", "distance_m": 0.0},
    {"name": "Pavillon Eiffel", "category": "attraction", "distance_m": 7.6},
    {"name": "Bureau de Gustave Eiffel", "category": "attraction", "distance_m": 15.8}
  ]
}
```

## Data Stats

Sizes and counts here are from the planet build of 2026-10-12.

- **24.5 million** POI records in the `all` tier, from OSM nodes, closed ways and multipolygon relations
- **205 categories** across tourism, historic, amenity, shop, leisure, natural, transport, and more

## Categories

POIs are extracted from these OSM tag families:

| Group | Categories | Examples |
|---|---|---|
| Tourism | museum, attraction, viewpoint, theme_park, zoo, gallery, artwork, aquarium, camp_site, resort | Musée du Louvre, Stonehenge, Grand Canyon viewpoint |
| Historic | castle, monument, ruins, archaeological_site, memorial, battlefield, fort, ship | Edinburgh Castle, Colosseum, Machu Picchu |
| Amenity | place_of_worship, university, hospital, theatre, cinema, library, marketplace, embassy, casino | Notre-Dame, Harvard, British Museum |
| Leisure | park, nature_reserve, stadium, garden, water_park, golf_course, marina | Central Park, Serengeti, Wembley |
| Natural | peak, volcano, beach, cave_entrance, waterfall, glacier, geyser, bay, cape, island | Mount Fuji, Niagara Falls, Iceland glaciers |
| Transport | aerodrome, station, ferry_terminal | Heathrow, Gare du Nord |
| Man-made | tower, lighthouse, windmill, bridge, pier, dam, observatory | Eiffel Tower, Golden Gate Bridge |
| Building | cathedral, palace | St. Peter's Basilica, Versailles |
| Boundary | national_park, protected_area | Yellowstone, Great Barrier Reef |
| Other | winery, brewery, power_plant | Napa Valley wineries |

## Three Tier Levels

Users choose how much POI data they want:

| Tier | Records | Download Size | What's included |
|---|---|---|---|
| **Major** | 0.9M | 204 MiB | Airports, national parks, volcanoes, glaciers, islands, cathedrals, palaces, train stations, UNESCO sites |
| **Notable** | 3.8M | 373 MiB | + museums, castles, stadiums, beaches, waterfalls, universities, viewpoints, lighthouses |
| **All** | 24.5M | 1.04 GiB | + every named POI: places of worship, peaks, memorials, artwork, libraries, gardens, springs |

Tiers are cumulative — Notable includes all Major POIs, All includes everything.

## Importance Scoring

Each POI gets an importance score (0-255) computed at build time from:

1. **Category base weight** — volcanoes (40) > airports (30) > museums (20) > parks (15) > peaks (10) > fountains (2)
2. **Wikipedia/Wikidata multiplier** — `1 + log2(1 + sitelinks) / 3` from the Wikidata item's sitelinks count; a POI without one falls back to its tags: both get 3x, Wikipedia only 2.5x, Wikidata only 1.5x
3. **Elevation scaling** (peaks/volcanoes) — Mount Everest ~130, random 200m hill ~5
4. **Polygon area scaling** — Central Park > neighborhood garden

The server scores each candidate POI as:

```
score = importance × 1 / (1 + (distance / reference_distance)²)
```

This means:
- **Close things win** — a fountain 10m away beats a national park 500m away
- **Famous things reach further** — Mount Fuji shows from 3km, a minor hillock from 300m
- **Hard max distance** — peaks can't show up in cities 10km away regardless of importance

## Download Sizes

A client picks: **region** + **mode** + **quality** + **POI tier** (all independent choices).

### By Mode (planet)

| Mode | Size | Description |
|---|---|---|
| Full | 9.0 GiB | Streets + addresses + interpolation + admin cells + place nodes + postcodes |
| No-addresses | 3.8 GiB | Streets + admin cells + place nodes + postcodes (no address points or interpolation) |
| Admin | 110 MiB | Admin boundary cell index + place nodes + postcodes |
| Admin-minimal | 220 MiB | Admin levels 2-8 + place nodes, with its own q2.5 polygons (no quality dir needed) |

Sizes are zstd downloads. Except for admin-minimal, a mode dir holds no
boundary polygons: those come from the quality dir.

### By Quality (planet, admin boundary resolution)

The vertex file (`admin_vertices.bin`) is most of what varies between
quality variants. The admin mode dir (place_nodes, postcode_centroids,
string tiers, cell index, etc.) adds **305 MiB raw / 110 MiB zstd**
regardless of quality; the quality dir also holds `admin_polygons.bin` and
the postal boundaries.

| Quality | `admin_vertices.bin` | Admin mode + quality dir (raw) | (zstd download) | Boundary precision (L8) |
|---|---|---|---|---|
| Uncapped | 1.51 GiB | 1.93 GiB | 1.50 GiB | <1 m |
| q0.2 | 789 MiB | 1.14 GiB | 893 MiB | ~3 m |
| q0.5 | 534 MiB | 899 MiB | 649 MiB | ~7 m |
| q1 | 379 MiB | 734 MiB | 497 MiB | ~15 m |
| q1.5 | 305 MiB | 655 MiB | 424 MiB | ~22 m |
| q2 | 260 MiB | 607 MiB | 378 MiB | ~30 m |
| **q2.5** | **229 MiB** | **573 MiB** | **347 MiB** | **~37 m — default** |

### By POI Tier (planet)

| Tier | Size | Records |
|---|---|---|
| Major | 204 MiB | 0.9M |
| Notable | 373 MiB | 3.8M |
| All | 1.04 GiB | 24.5M |

### Total Download Examples

| Configuration | Total Size | Use case |
|---|---|---|
| Full + uncapped + all POIs | 11.4 GiB | Maximum quality, all data |
| Full + q1 + notable POIs | 9.7 GiB | Good balance for photo apps |
| No-addresses + q1 + major POIs | 4.4 GiB | Streets + major landmarks |
| Admin + q1 | 497 MiB | Minimal — just boundaries |
| Admin + q2.5 | 347 MiB | Smallest useful configuration |

### Regional Extracts

Available for: planet, europe, asia, north-america, south-america, africa, oceania, central-america, antarctica.

Regional downloads are smaller — Europe full + uncapped is 4.4 GiB vs planet's 10.4 GiB.

## Incremental Updates

The system supports daily incremental patching:

1. Each day a new build is produced from the latest OpenStreetMap data
2. Patches (`.gcpatch` files) are generated against the previous day's build
3. Clients download and apply patches instead of re-downloading the full index
4. Patches are verified byte-identical to a fresh build before upload
5. Patches are well under 0.1% of the index: one day of planet full + q2.5 + all POIs is about 4.5 MiB (see `builder/PATCHING.md`)

POI data is patched independently — each tier directory gets its own patch file.

## Architecture

### Builder (C++)
- Parses OpenStreetMap PBF files in parallel
- Extracts POIs from nodes (points), closed ways (polygons), and relations (multipolygons)
- Indexes using S2 geometry cells at level 10 (~10km resolution)
- Computes importance scores from category, Wikidata sitelinks (or wiki tags), elevation, area
- Outputs deterministic binary files for efficient diff/patch

### Server (Rust)
- Memory-maps all index files for zero-copy access
- Sub-millisecond query latency
- Loads category names and scoring parameters from `poi_meta.json`; the OSM class a landmark is reported under (tourism, historic, ...) is mapped from the category number in code
- Scoring-based POI lookup with distance decay and max distance cutoffs

### Files (7 per POI tier)

| File | Record Size | Description |
|---|---|---|
| `poi_records.bin` | 36 bytes | Name, category, tier, importance, coordinates, polygon reference, parent street, postcode and admin polygon |
| `poi_vertices.bin` | 10-byte header + 4 or 8 bytes per vertex | Polygon outlines: encoding and bounding box, then the packed vertices |
| `poi_cells.bin` | 12 bytes | S2 cell spatial index |
| `poi_entries.bin` | variable | Per-cell POI ID lists |
| `strings_poi.bin` | variable | POI names |
| `strings_layout.json` | — | String tier layout |
| `poi_meta.json` | — | Category names, scoring parameters |

## Test Portal

A web-based test tool is included for validating results:

- Click anywhere on the map to reverse geocode
- Upload photos (EXIF GPS extracted in browser)
- Paste coordinates (single or batch)
- See full JSON response with address + POI data
- Dark theme with Immich vector tile map

Run with Docker:
```bash
cd geocoder/test-portal
docker compose up --build
# Open http://localhost:3002/test (the minimal-dataset server for comparison is on :3001)
```
