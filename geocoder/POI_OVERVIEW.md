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
| **Major** | 0.9M | 124 MiB | Airports, national parks, volcanoes, glaciers, islands, cathedrals, palaces, train stations, UNESCO sites |
| **Notable** | 3.8M | 304 MiB | + museums, castles, stadiums, beaches, waterfalls, universities, viewpoints, lighthouses |
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

Measured on planet-261018b; sizes are zstd downloads (`compress_index.py`).

| Mode | Size | Description |
|---|---|---|
| Full | 9.2 GiB | Streets + addresses + interpolation + admin cells + place nodes + postcodes |
| No-addresses | 4.0 GiB | Streets + admin cells + place nodes + postcodes (no address points or interpolation) |
| Admin | 84 MiB | Admin boundary cell index + place nodes + postcodes |
| Admin-minimal | 124 MiB | Admin levels 2-8 + place nodes, with its own simplified boundaries (no quality dir needed); with a POI tier its `admin-minimal-poi/` dir (128 MiB), on the shared polygon ids and strings |

Except for admin-minimal, a mode dir holds no boundary polygons: those come
from the quality dir.

### By Quality (planet, admin boundary resolution)

A quality dir holds the admin polygons (`admin_polygons.bin`, postcode areas
among them) and their rings (`admin_rings.bin`), the same in both tiers, and
the arcs the rings are made of (`arc_index.bin`, `arc_vertices.bin`): each
border once, shared by the boundaries on either side of it. The arcs are what
the tiers differ in. The admin mode dir (place_nodes, postcode_centroids,
string tiers, cell index, etc.) adds **264 MiB raw / 84 MiB zstd** whatever
the tier.

| Quality | Arcs (raw) | Quality dir (raw / zstd) | Admin mode + quality dir (zstd download) | Boundary precision |
|---|---|---|---|---|
| **Simplified** | **124 MiB** | **166 / 118 MiB** | **202 MiB** | **within 37.5 m (less for small areas), on a 1.1 m grid — default** |
| Exact | 650 MiB | 691 / 611 MiB | 695 MiB | every OSM point |

### By POI Tier (planet)

| Tier | Size | Records |
|---|---|---|
| Major | 124 MiB | 0.9M |
| Notable | 304 MiB | 3.8M |
| All | 1.04 GiB | 24.5M |

### Total Download Examples

The presets of `builder/configurations.template.json`:

| Configuration | Total Size | Use case |
|---|---|---|
| Admin-minimal (smallest) | 124 MiB | Smallest useful configuration |
| Admin + simplified + major POIs (recommended) | 326 MiB | Best balance for photo apps |
| Full + simplified + all POIs (with addresses) | 10.4 GiB | House-number resolution where tagged |
| Full + exact + all POIs (max) | 10.9 GiB | Maximum quality, all data |

### Regional Extracts

Available for: planet, europe, asia, north-america, south-america, africa, oceania, central-america, antarctica.

Regional downloads are smaller: each region's dirs hold only its own data.

## Incremental Updates

The system supports daily incremental patching:

1. Each day a new build is produced from the latest OpenStreetMap data
2. Patches (`.gcpatch` files) are generated against the previous day's build
3. Clients download and apply patches instead of re-downloading the full index
4. Patches are verified byte-identical to a fresh build before upload
5. Patches are well under 0.1% of the index: one day of planet full + q2.5 + all POIs (the boundary tier before simplified) was about 4.5 MiB with the GCPATCH v7 tools (see `builder/PATCHING.md`)

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
| `strings_poi.bin` | variable | POI names: the names of the dir's POI tiers, the head of the build's POI string tier |
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
