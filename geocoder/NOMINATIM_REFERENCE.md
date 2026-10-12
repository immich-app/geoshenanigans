# Nominatim Reverse Geocoding — Reference

This document traces how Nominatim resolves admin boundaries to address fields, based on analysis of the source code at https://github.com/osm-search/Nominatim.

## Source files

| File | Purpose |
|---|---|
| `settings/address-levels.json` | Country-specific admin_level → address_rank mapping |
| `lib-sql/functions/ranking.sql` | `compute_place_rank()` — looks up rank from address_levels table |
| `lib-sql/functions/placex_triggers.sql` | `find_linked_place()` — links boundaries to place nodes. `placex_update()` — rank adjustments for nested boundaries |
| `src/nominatim_api/v1/classtypes.py` | `get_label_tag()` — determines the output field name (city, state, county, etc.) |
| `src/nominatim_api/v1/format_json.py` | `_write_typed_address()` — writes the address JSON using labels from get_label_tag |

## Step 1: admin_level → address_rank

`compute_place_rank()` in `ranking.sql` constructs a key like `"administrative5"` and queries the `address_levels` table (loaded from `address-levels.json`).

Default mapping: `admin_level * 2 = rank` (level 2→4, level 4→8, level 8→16).

Country overrides change these. Format in JSON: `[search_rank, address_rank]` where `address_rank=0` means non-addressable (boundary exists for search but not in address output). Single integer means both ranks are the same.

### Verified country overrides

Our `admin_levels.json` matches Nominatim's `address-levels.json` exactly (verified programmatically). Countries with overrides: au, be, br, cz, de, es, id, jp, nl, no, ru, se, sk, us.

## Step 2: Label determination — get_label_tag()

`get_label_tag(category, extratags, rank, country)` in `classtypes.py` determines the output field name. Priority order:

```python
1. If postcode category → "postcode"
2. If rank < 26 and extratags has 'place' key → extratags['place']  # e.g., "city"
3. If rank < 26 and extratags has 'linked_place' key → extratags['linked_place']
4. If boundary=administrative → ADMIN_LABELS[(country, rank//2)]
5. If rank < 26 → category[1]  # e.g., place type
6. If rank < 28 → "road"
7. Else → category[0]
```

### ADMIN_LABELS mapping (rank//2 → label)

```
(_, 1): 'Continent'
(_, 2): 'Country'
(_, 3): 'Region'
(_, 4): 'State'
(_, 5): 'State District'
(_, 6): 'County'
(_, 7): 'Municipality'
(_, 8): 'City'
(_, 9): 'City District'
(_, 10): 'Suburb'
(_, 11): 'Neighbourhood'
(_, 12): 'City Block'
```

Country-specific overrides: Norway/Sweden have `(no/se, 3): State` and `(no/se, 4): County`.

### Key insight: extratags override

The `place` and `linked_place` tags from OSM data end up in the `extratags` column. When present, they OVERRIDE the rank-based label. This is how:
- NYC (admin_level=5, `linked_place=city`) → label "city" instead of "State District" (rank 10)
- SF (admin_level=6, also tagged `border_type=city`) → label "city" instead of "County" (rank 12)
- Manhattan (admin_level=7, also tagged `border_type=borough`) → label "borough", an address field of its own (not suburb); we output it as `borough`

Neither label comes from `border_type`, which Nominatim never reads (see below).

**VERIFIED**: Despite line 841 of placex_triggers.sql appearing to remove `linked_place` from extratags, empirical testing shows NYC's extratags still contain `linked_place=city` at query time. Both `place` and `linked_place` checks are active.

**CRITICAL FINDING about NYC's "city" label**: NYC in Nominatim's database has category `('place', 'city')`, NOT `('boundary', 'administrative')`. This is because NYC's boundary relation is linked to a place=city node — when linked, the boundary ADOPTS the place node's category. So `get_label_tag()` hits check 5 (line 33-34: `category[1]` = "city") rather than check 4 (ADMIN_LABELS). The `linked_place=city` extratag (check 3) would also produce "city" as a backup.

**`border_type` tag**: Not referenced anywhere in Nominatim's source code. It exists on OSM objects as metadata but Nominatim doesn't use it. Only `linked_place` and `place` extratag keys matter for label determination.

## Step 3: Linked place nodes

`find_linked_place()` in `placex_triggers.sql` links admin boundaries to `place=*` nodes. Search order:

1. **Label role member** — relation has a member node with role "label" and class "place"
2. **Wikidata match** — boundary and node share the same `wikidata` extratag
3. **Place type match** — boundary has `extratags->'place'` and a node with matching type + name exists inside
4. **Name match** — a place node with same name and appropriate rank inside the boundary

When linked: the boundary adopts the place node's names. The place node itself gets `linked_place_id` set (effectively hidden from results, as it's represented by the boundary).

**NOTE**: Linking primarily affects names and search, NOT the label/rank determination. The label comes from `get_label_tag()` via extratags (step 2), not from the linked node's category.

## Step 4: Rank adjustments for nested boundaries

`placex_triggers.sql` lines 909-942: after computing initial rank, admin boundaries get adjusted:

```sql
-- Find parent boundary (higher admin_level, containing this one)
-- If parent.rank_address >= our rank_address:
--   our rank_address = parent.rank_address + 2
```

This ensures nested boundaries don't share the same address rank. Example:
- If a level 5 boundary (rank 10) contains a level 6 boundary (also rank 12), no adjustment needed (12 > 10)
- If a level 5 boundary (rank 12 via override) contains a level 6 boundary (rank 12), the level 6 gets bumped to 14

**We implement this at query time** (`server/src/lib.rs`, before field assignment), on the polygons that contain the point: the nested-admin, linked-place and place-area promotions, repeated so a bump can cascade. Containment between those polygons is judged by admin level and area. Smallest-area-wins then settles polygons that still map to the same output field (if two polygons both map to "county", the smaller one wins).

## Step 5: Competing polygons

Nominatim uses `isaddress` flag in the database to determine which boundaries appear in the address. When multiple boundaries exist at the same rank, only one gets `isaddress=true`.

We use smallest-area-wins with PIP verification. This is a different mechanism but achieves a similar result for most cases.

## boundary=place handling

Entities like Washington DC have `boundary=place` + `place=city` (not `boundary=administrative`). Nominatim imports these with `class='place', type='city'`. The `compute_place_rank()` lookup queries `address_levels` for `class='place', type='city'` which gives rank 16 (city). These are NOT admin boundaries — they're pure place entities with polygon geometry.

In `get_label_tag()`, DC has `category=('place', 'city')` so it hits check 5 (line 33-34): `category[1]` = "city". Same path as NYC.

**For our implementation**: We extract `boundary=place` relations in the builder and store them as admin polygons with `admin_level=15` (special marker). The `place_type_override` field captures the place type (city/town/village). The server uses `place_type_override` to determine the field name, bypassing the admin_level→rank mapping entirely.

## Competing polygons and rank adjustments

### Nominatim's approach

**Pre-computed at index time** (placex_triggers.sql lines 909-942):
- For each admin boundary, find parent boundary (lower admin_level, containing geometry)
- If child's rank_address ≤ parent's rank_address → bump child to parent + 2
- This prevents two nested boundaries from occupying the same address rank
- Additional check: if a boundary is fully contained in a `place=*` polygon with equal or higher rank → bump

**At query time** (results.py line 625):
- Address rows are sorted by rank_address DESC
- For each rank level, only the FIRST entry gets `isaddress=true`
- Subsequent entries at the same rank are `isaddress=false` (excluded from output)

### Our approach

- At query time: apply Nominatim's rank adjustment to the polygons containing the point (Nominatim runs it at index time)
- Then for each output field (city, county, state, etc.), keep the smallest-area polygon, admin boundaries before place areas — when two polygons map to the same field, the more specific (smaller) one wins

### Difference impact

Where Nominatim's rank adjustment matters:
- Two admin levels that both map to the same rank range (e.g., level 5 and 6 both → county in some countries)
- Nominatim bumps the inner one, and so do we. The difference left is that we only see the polygons containing the query point and judge containment between them by admin level and area, where Nominatim tests the geometry.

## How our implementation maps to Nominatim

| Nominatim concept | Our implementation | Status |
|---|---|---|
| address-levels.json | admin_levels.json (exact match, verified programmatically) | ✅ Verified |
| get_label_tag() check 2-3 (place/linked_place extratags) | place_type_override field on AdminPolygon (from the linked place node, else the boundary's linked_place / place tags) | ✅ Implemented |
| get_label_tag() check 4 (ADMIN_LABELS rank//2) | rank_to_field() function | ✅ Implemented |
| get_label_tag() check 5 (category[1] for place=* entities) | place_type_override covers this — boundary=place entities get place_type set | ✅ Implemented |
| find_linked_place() node linking | Builder: label role member, wikidata match, the boundary's own place tag, then name match (`link_places_by_name`), in Nominatim's order; a linked place node is hidden from place results | ✅ Implemented |
| Rank adjustment for nested boundaries | At query time on the polygons containing the point, then smallest-area-wins per field | ✅ Implemented — containment judged by admin level and area |
| boundary=place entities (e.g., Washington DC) | Extracted in builder with admin_level=15 + place_type_override | ✅ Implemented |
| Place node fallback (nearest city/town/village) | place_nodes.bin + find_places() with wide search | ✅ Implemented — place nodes are indexed at the admin cell level (L10); the query cell and its 8 neighbours cover ~30 km |
| place_addressline pre-computed relationships | Not applicable — we do spatial lookup at query time | N/A — different architecture |
| Postcodes from addr:postcode | addr_postcodes.bin (an address point's own addr:postcode) and postcode_centroids.bin (centroids of every addr:postcode, plus external ones) | ✅ Implemented — see "Postcode handling" section below |
| border_type tag | Not used: Nominatim never reads this tag | ✅ Ignored, as in Nominatim |
| Wikidata-based place linking | Part of find_linked_place() above: a boundary's wikidata matched against place node wikidata | ✅ Implemented |
| Display name formatting | Ours is a clean formatted address; Nominatim dumps all address rows comma-separated | ℹ️ Ours is arguably better for end users. Nominatim's is more of a debug breadcrumb. Could add suburb fallback when city is missing. |
| Street/road resolution | Minor differences in closest street selection | ℹ️ Our point-to-segment distance vs Nominatim's pre-computed parent streets. Causes occasional one-off differences at intersections. |
| Suburb/neighbourhood rank adjustment | Covered by the query-time rank adjustment above | ✅ Implemented |
| TIGER address interpolation | `--tiger-data` loads Nominatim's TIGER export (address ranges, one CSV per county) as interpolation lines, with each line's ZIP in interp_postcodes.bin | ✅ Implemented — a US street that wins without an address point takes its house number from the nearest interpolation line of that street, as Nominatim's `_find_tiger_number_for_street` does |
| Postal city names | Not implemented | ❌ Deferred — in the US, USPS assigns city names to ZIP codes that may differ from the actual municipal jurisdiction (e.g., "Cumming, GA" for unincorporated Forsyth County). Neither we nor Nominatim handle this — would require external USPS data or addr:city extraction. |
| Name localisation | Not implemented — we store one name per feature, `name:en` where present, else `name` | ❌ Deferred — Nominatim supports `accept-language` header to return locale-specific names (e.g., `name:en=Borough of Luton` instead of `name=Luton`). Builder would need to store `name:en`, `name:fr`, etc. in the string pool (or a separate name table), and server would need to select based on client language preference. |

## Postcode handling in Nominatim (traced from source)

Nominatim has THREE sources for postcodes, combined into the `location_postcodes` table:

### Source 1: Postcode boundary polygons (`place_postcode` table)
- OSM `boundary=postal_code` relations with admin_level=11
- These have actual polygon geometry
- Stored with `osm_id IS NOT NULL` in location_postcodes
- **We use these at build time**: a street (way_postcodes.bin) and a POI (`parent_postcode_id`) inherit the postcode of the postal boundary containing them. Like Nominatim, which reads the stored `placex.postcode`, the server does no point-in-polygon on postal boundaries

### Source 2: addr:postcode tags on places (`placex` table)
- Any OSM object with an `address->'postcode'` hstore entry
- Nominatim collects `(country_code, postcode, centroid)` from ALL objects in placex that have addr:postcode
- These create **artificial postcode centroids** — computed as the average position of all objects sharing that postcode
- Stored with `osm_id IS NULL` in location_postcodes
- Only used for areas NOT already covered by postcode boundary polygons (line 342: `NOT EXISTS(SELECT * FROM _global_postcode_area ...)`)
- **We implement this**: the builder keeps each address point's addr:postcode (addr_postcodes.bin) and averages the address points of each (country, postcode) into postcode_centroids.bin

### Source 3: External postcode CSV files
- Per-country CSV files (`{country}_postcodes.csv`) placed in the project directory
- Format: `postcode,lat,lon`
- Added to fill gaps where neither boundary polygons nor addr:postcode data exists
- **We implement this**: `--external-postcodes` loads GeoNames postcode centroids, and `--tiger-data` adds the US ones. They fill only the (country, postcode) pairs OSM has none for, as Nominatim's `_update_from_external` does

### How postcodes are resolved at query time

`get_nearest_postcode()` in `utils.sql`:
1. For polygon results: find postcode whose centroid is inside the polygon (must be exactly 1)
2. For point results: find postcode whose coverage area contains the point, ordered by distance to centroid

The postcode is also populated from the `place_addressline` chain — if a street/building has addr:postcode, it appears in the address rows directly.

### What we implement

`resolve_postcode()` in `server/src/lib.rs` follows Nominatim's stored `placex.postcode` chain for the feature that won the query:

0. An address point's own addr:postcode (addr_postcodes.bin)
1. A street's or POI's postcode inherited from its postal boundary at build time (way_postcodes.bin, `parent_postcode_id`); for a US street, the ZIP of a TIGER line of that street within ~100 m (interp_postcodes.bin)
2. The nearest postcode centroid of the same country within 0.05° (postcode_centroids.bin, as `get_nearest_postcode()`)
