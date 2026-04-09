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

- **5.85 million** POIs extracted from OpenStreetMap
- **55 categories** across tourism, historic, amenity, leisure, natural, transport, and more
- **2.8 million** polygon POIs (parks, airports, islands) + **3.0 million** point POIs (peaks, viewpoints, monuments)
- **~970K** POIs enriched with Wikipedia/Wikidata metadata

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
| **Major** | 902K | 249 MiB | Airports, national parks, volcanoes, glaciers, islands, cathedrals, palaces, train stations, UNESCO sites |
| **Notable** | 3.0M | 476 MiB | + museums, castles, stadiums, beaches, waterfalls, universities, viewpoints, lighthouses |
| **All** | 5.9M | 643 MiB | + every named POI: places of worship, peaks, memorials, artwork, libraries, gardens, springs |

Tiers are cumulative — Notable includes all Major POIs, All includes everything.

## Importance Scoring

Each POI gets an importance score (0-255) computed at build time from:

1. **Category base weight** — volcanoes (40) > airports (30) > museums (20) > parks (15) > peaks (10) > fountains (2)
2. **Wikipedia/Wikidata multiplier** — POIs with both get 3x, Wikipedia only 2.5x, Wikidata only 1.5x
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
| Full | 15.7 GiB | Streets + addresses + interpolation + admin cells |
| No-addresses | 12.3 GiB | Streets + interpolation + admin cells (no address points) |
| Admin-only | 77 MiB | Admin boundary cell index + strings only |

### By Quality (planet, admin boundary resolution)

| Quality | Size | Simplification |
|---|---|---|
| Uncapped | 2.7 GiB | Full resolution boundaries |
| q0.2 | 1.5 GiB | Minimal simplification |
| q0.5 | 1.0 GiB | Light simplification |
| q1 | 703 MiB | Moderate (default) |
| q1.5 | 594 MiB | Noticeable simplification |
| q2 | 506 MiB | Heavy simplification |
| q2.5 | 445 MiB | Maximum simplification |

### By POI Tier (planet)

| Tier | Size | Records |
|---|---|---|
| Major | 249 MiB | 902K |
| Notable | 476 MiB | 3.0M |
| All | 643 MiB | 5.9M |

### Total Download Examples

| Configuration | Total Size | Use case |
|---|---|---|
| Full + uncapped + all POIs | 19.1 GiB | Maximum quality, all data |
| Full + q1 + notable POIs | 16.9 GiB | Good balance for photo apps |
| No-addresses + q1 + major POIs | 13.2 GiB | Streets + major landmarks |
| Admin-only + q1 | 780 MiB | Minimal — just boundaries |
| Admin-only + q2.5 | 522 MiB | Smallest useful configuration |

### Regional Extracts

Available for: planet, europe, asia, north-america, south-america, africa, oceania, central-america, antarctica.

Regional downloads are much smaller — Europe full + uncapped is ~1.3 GiB vs planet's 18.4 GiB.

## Incremental Updates

The system supports daily incremental patching:

1. Each day a new build is produced from the latest OpenStreetMap data
2. Patches (`.gcpatch` files) are generated against the previous day's build
3. Clients download and apply patches instead of re-downloading the full index
4. Patches are verified byte-identical to a fresh build before upload
5. Patches are typically 0.1-1% of the full index size

POI data is patched independently — each tier directory gets its own patch file.

## Architecture

### Builder (C++)
- Parses OpenStreetMap PBF files in parallel
- Extracts POIs from nodes (points), closed ways (polygons), and relations (multipolygons)
- Indexes using S2 geometry cells at level 10 (~10km resolution)
- Computes importance scores from category, wiki flags, elevation, area
- Outputs deterministic binary files for efficient diff/patch

### Server (Rust)
- Memory-maps all index files for zero-copy access
- Sub-millisecond query latency
- Loads category metadata from `poi_meta.json` (no hardcoded categories)
- Scoring-based POI lookup with distance decay and max distance cutoffs

### Binary Files (4 per POI tier)

| File | Record Size | Description |
|---|---|---|
| `poi_records.bin` | 24 bytes | Name, category, tier, importance, coordinates, polygon reference |
| `poi_vertices.bin` | 8 bytes | Polygon boundary vertices (lat/lng pairs) |
| `poi_cells.bin` | 12 bytes | S2 cell spatial index |
| `poi_entries.bin` | variable | Per-cell POI ID lists |
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
# Open http://localhost:3000/test
```
