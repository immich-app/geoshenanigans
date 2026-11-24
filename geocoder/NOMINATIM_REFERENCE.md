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

Our `admin_levels.json` matches the `boundary` entries of Nominatim's `address-levels.json` as the public server runs it (master: Slovakia's levels were removed on 2026-05-02, and Brazil's administrative5 to 7 rank 0 since 5.3.0). Countries with overrides: au, be, br, cz, de, es, id, jp, nl, no, ru, se, us. The builder's `admin_rank_config.h` mirrors it for place linking.

A country's `place` entries (ca, de, es, id, ru, sa: CA and DE county 0, DE region 0, ES province 10, ID and RU municipality 18, SA province 12 and municipality 18) are in `place_type_to_rank`, which ranks place areas and the place types of boundaries, and in the builder's `place_area_rank_address`, which keeps place=province areas for Spain and Saudi Arabia (every other place type of rank 0 everywhere makes no area; admin-minimal keeps every province area too, as the countries are known only after its arcs) and links no node to a place area of rank 0 in its country.

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
2. **Wikidata match** — boundary and node share the same `wikidata` extratag, the boundary covers the node, and no boundary indexed before it linked the node (`linked_place_id is null`); of several, the first `ORDER BY lower(name->'name') = bnd_name desc` finds (a node of the boundary's name before others; a null comparison, either name missing, first of all). Nominatim indexes the boundaries by rank, then the place areas by address rank: the builder gives each boundary its turn in that order (`link_wikidata_places`: lowest admin level first, place areas after every boundary by the address rank of their own place tag in their country, equals by id; a boundary or place area of rank 0 in its country, found at a point inside it before the arcs, takes no turn and reserves no node), the nodes of a name match by their address rank (the order leaves them unordered: Espanola's town before its hamlet), then node id, and a boundary or place area that links none keeps its own place tag for the steps after (Lent's place=quarter area, whose village node of the same wikidata its wijk links first, stays a quarter)
3. **Place type match** — boundary has `extratags->'place'` and a node with matching type + name exists inside
4. **Name match** — a place node with same name and appropriate rank inside the boundary, where either has no wikidata or both the same (Louvain-la-Neuve's section, Q665868, links no town node of its name tagged Q329642)

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

**We implement this at query time** (`promote_nested_ranks` in `server/src/lib.rs`, before field assignment), on the polygons that contain the point, coarsest admin level first:

1. **Admin nested** (placex_triggers.sql:876-910), for boundaries of rank > 0: the parent is the boundary of the highest admin_level below the child's and above 3 that holds the point (the smallest of that level). A parent of rank >= the child's lifts it to the parent's + 2, and to 25 under a parent of 24 or more.
2. **Place area** (:912-929), for boundaries of rank 10 or more without a place type: the place area of the highest rank from the boundary's up to 23 that is larger and contains the boundary (`ST_Contains`, below) lifts it to that rank + 2.
3. **Linked place** (:1109-1170): a boundary's place type (from a linked node or its own `place` tag) replaces its rank where that is higher than both its own and its parent's.

A boundary=administrative relation of rank > 0 whose closest boundary relation round its centroid, of a lower admin level above 3 and of rank 1 to 25, has its wikidata and an equal geometry (`is_same`, :909-931) ranks 0 and is in no address; one that links a place node takes the node's rank back (:1161). The builder decides it (`take_out_repeated_boundaries`): the parent is the first such relation up the parent chain from the boundary's largest ring, the levels below settled first, and the rings are equal where they have the same vertices from any one on, either way round. Every ring of one taken out carries place type 15 (`BOUNDARY_REPEATS_PARENT`), which the server ranks in no address: Warsaw's county and gmina along its city's ways, and Arnhem's woonplaats along its gemeente's, which links the city node.

A place area (class='place': `boundary=place`, a place multipolygon or closed way, a `boundary=historic` or `ceremonial` with a place tag, such as County Antrim or Essex) takes none of the three steps: one of rank 16 to 23 is lifted to an area's rank + 2 where a boundary or a place area whose own rank is the place area's or more, up to 23, contains it (:930-950), judged as in step 2 by the area being larger and containing the place area; a place=village area inside a city of rank 16 ranks 18. A place area that links a place node of another type (its label member or its wikidata's) is labelled by the node's type, and ranks by its own `place` tag where that ranks 16 to 23 (the place-area branch leaves `parent_address_level` unset, so the link's rank never applies, :930-950 and :1129), else by the node's where that is above 3 and below 26: Weesp (R15419242, `boundary=place`, `place=suburb`, the town node as its label) is a town of rank 20. The builder keeps such an area's own place tag in its record's last byte (`own_place_type`) beside the node's type (`place_type_override`), and `place_area_ranks` ranks it; admin-minimal keeps it by the type that labels it. A boundary whose level ranks 0 in its country stays out of the address whatever its place type (Cairns Regional, Regierungsbezirk Köln). Areas of rank 25 are in no street's or house's address (getNearFeatures takes ranks below 25), so they fill no field and take no rank from place nodes.

What the polygons at a point cannot tell, and Nominatim decides on the whole geometry at index time:

- The admin-nested parent is the boundary covering the child's centroid, which need not hold the query point (Bugisa sub-region round Mbale City ranks 12 in Nominatim, 18 here).
- A place area is lifted where another area contains it whole (`ST_Contains`). Here the other is larger and contains it (`polygon_within`): every vertex of the polygon is inside the other or on its edges, none of its edges crosses one of the other's, no point of the other's edges lies inside it off its edges (of a hole of the other inside it, or a notch of the other that one of its edges spans; each edge of the other is cut at the polygon's vertices on it and tested at the middle of each piece), and its point on surface is inside the other off its edges (an enclave filling a hole of the other is not contained), the point tested at twice the coordinates, where it stays inside a polygon a unit or two of the e7 grid wide. Areas nested along shared ways meet on their edges alone.
- A linked place or the boundary's own place tag can lower the rank; the builder keeps one place type per polygon, from either.
- Every place type ranks a boundary (place=locality makes Northern Ireland's townlands rank 25); the builder keeps 14 types.
- Levels 11 and 12 (level 11 holds the postal areas here), and administrative ways only at levels 8 to 12.

Two rules wait for those. Nominatim ranks every boundary holding the point; here only the smallest of each admin_level is ranked, as a larger one round a replicated boundary would rank too high (Brussels' level 10 boundary under Bruxelles - Brussel would take the suburb from the Pentagon). And a boundary labelled city, town or village holds rank 16 from place nodes whatever its own rank, standing in for the linked place's rank that Nominatim gives it even below its own (the parish of Winsford ranks 16, its admin_level 20): holding only its own rank lets villages such as Argir in Tórshavn into the address.

Smallest-area-wins then settles polygons that still map to the same output field (if two polygons both map to "county", the smaller one wins). The address names the winner, as Nominatim's keeps the first row of a label, and display_name lists the other at its rank as well, where no field's polygon has that rank (Nominatim's display_name joins every row): York County (rank 10, labelled county) beside the City of Fredericton (12). A polygon of the outline of the one that took its field is not listed, as Nominatim takes out a boundary repeating its parent's wikidata and geometry (Bruxelles - Brussel under Brussels). The outline is taken as the same where the two have the same area and the same ring block, or every edge of each lies along the other's edges (`same_outline`); the records carry no wikidata, so it is not compared.

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
- Nominatim bumps the inner one, and so do we. The difference left is that we only see the polygons containing the query point and judge containment between them by admin level, area and the rings' containment test (`polygon_within`, Step 4), where Nominatim tests the geometry with `ST_Contains`.

## How our implementation maps to Nominatim

| Nominatim concept | Our implementation | Status |
|---|---|---|
| address-levels.json | admin_levels.json (exact match, verified programmatically) | ✅ Verified |
| get_label_tag() check 2-3 (place/linked_place extratags) | place_type_override field on AdminPolygon (from the linked place node, else the boundary's linked_place / place tags) | ✅ Implemented |
| get_label_tag() check 4 (ADMIN_LABELS rank//2) | rank_to_field() function | ✅ Implemented |
| get_label_tag() check 5 (category[1] for place=* entities) | place_type_override covers this — boundary=place entities get place_type set | ✅ Implemented |
| find_linked_place() node linking | Builder: label role member, wikidata match, the boundary's own place tag, then name match (`link_places_by_name`), in Nominatim's order; a boundary or place area of rank 0 in its country links nothing (find_linked_place returns at once); a linked place node is hidden from place results, except in admin-minimal when that set leaves its boundary out (L9-L11, place areas below rank 16): nothing there stands for the node | ✅ Implemented |
| Rank adjustment for nested boundaries | At query time on the polygons containing the point, then smallest-area-wins per field | ✅ Implemented — containment judged by admin level, area and the rings' containment test; what that leaves out is listed under Step 4 |
| boundary=place entities (e.g., Washington DC) | Extracted in builder with admin_level=15 + place_type_override | ✅ Implemented |
| Place node fallback (nearest city/town/village) | place_nodes.bin + find_places() with wide search | ✅ Implemented — place nodes are indexed at the admin cell level (L10); the query cell and its 8 neighbours cover ~30 km |
| place_addressline pre-computed relationships | Not applicable — we do spatial lookup at query time | N/A — different architecture |
| Postcodes from addr:postcode | addr_postcodes.bin (an address point's own addr:postcode) and postcode_centroids.bin (centroids of every addr:postcode, plus external ones) | ✅ Implemented — see "Postcode handling" section below |
| border_type tag | Not used: Nominatim never reads this tag | ✅ Ignored, as in Nominatim |
| Wikidata-based place linking | Part of find_linked_place() above: a boundary's wikidata matched against the wikidata of the place nodes it holds (`_st_covers`) | ✅ Implemented |
| Display name formatting (`display_name` in results.py) | `format_address`: the landmark, the house number with the road, the place parts finest first by the rank_address of the polygon or place node behind each field, with the polygons that lost their field to one of another rank, then the postcode (rank 5) and the country (4); a part is left out only where it repeats the part just before it, and every answer with a part has one | ✅ Implemented — the house number joined to the road as the country writes it |
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
- **We use these at build time**: a street (way_postcodes.bin) takes the postcode of the postal area covering its middle, its edges included (`ST_Covers`), as `get_nearest_postcode()` below does, and a POI (`parent_postcode_id`) that of the smallest postal boundary of its country containing it. They are admin polygons of level 11, so the server also finds the ones holding the query point, for `get_nearest_postcode()` below. Each has the country its centroid lies in (`get_country_code`, as `update_postcodes` gives `place_postcode` its country), one for all the rings of a relation, at the whole multipolygon's centroid. A US one of a ZIP+4 is its ZIP's (the US pattern's output is `\1`), and one of a postcode its country's pattern rejects carries place type 15 (`POSTAL_AREA_INVALID`): Nominatim makes no postcode area of it (`_update_postcode_areas`), and it names nothing. Past that a postcode of any length names it (Saudi Arabia's 12345-6789, Iran's 12345-67890), one value and no placeholder

### Source 2: addr:postcode tags on places (`placex` table)
- Any OSM object with an `address->'postcode'` hstore entry
- Nominatim collects `(country_code, postcode, centroid)` from ALL objects in placex that have addr:postcode
- These create **artificial postcode centroids** — computed as the average position of all objects sharing that postcode
- Each one covers a box around its centroid (`expand_by_meters(centroid, extent)`, postcodes.py), the `extent` of its country's postcode pattern in `country_settings.yaml`: US 25,000 m, GB and IM 700 m, NL 800 m, CH, EE and JP 3,000 m, GG, LU and PT 1,000 m, LI 4,000 m, RO 2,500 m, IE 50 m, Nigeria's lettered pattern 1 m, and 5,000 m elsewhere
- Stored with `osm_id IS NULL` in location_postcodes
- Only used for areas NOT already covered by postcode boundary polygons (line 342: `NOT EXISTS(SELECT * FROM _global_postcode_area ...)`)
- **We implement this**: the builder keeps each address point's addr:postcode (addr_postcodes.bin) and averages the address points of each (country, postcode) into postcode_centroids.bin

### Source 3: External postcode CSV files
- Per-country CSV files (`{country}_postcodes.csv`) placed in the project directory
- Format: `postcode,lat,lon`
- Added to fill gaps where neither boundary polygons nor addr:postcode data exists
- **We implement this**: `--external-postcodes` loads GeoNames postcode centroids, and `--tiger-data` adds the US ones. They fill only the (country, postcode) pairs OSM has none for, as Nominatim's `_update_from_external` does

### How postcodes are resolved at query time

`get_nearest_postcode()` in `utils.sql` (5.3 and master), which `placex_update` calls at index time for a feature that has no postcode of its own, from its parent street or from its address areas:
1. For polygon results: find postcode whose centroid is inside the polygon (must be exactly 1)
2. For point results: the postcodes of the country whose geometry covers the point, `ORDER BY osm_id is null, ST_Distance(p.centroid, geom)`: a postal area first, then the artificial postcodes' boxes, each by the distance to its centroid in plain degrees (geometry 4326)

The postcode is also populated from the `place_addressline` chain — if a street/building has addr:postcode, it appears in the address rows directly.

### What we implement

`resolve_postcode()` in `server/src/lib.rs` follows Nominatim's stored `placex.postcode` chain for the feature that won the query. A house (rank 30) takes its own addr:postcode, then its parent street's postcode, then `get_nearest_postcode()` at the house (`placex_update`); a street takes its postcode from `get_nearest_postcode()` at its centroid. The parent street is the address point's (its parent way, else the nearest street, which its road names); where the street that won takes its house number from an address within 0.001° (`_find_housenumber_for_street`), Nominatim answers with that house, of that street:

0. A house's own addr:postcode (addr_postcodes.bin), where the pattern of the house's own country takes it: the clean-postcodes sanitizer matches it there (`CountryPostcodeMatcher`, after an optional country code; a US ZIP+4 is its ZIP) and makes one that fails an address term, so the builder stores none and the house takes its street's
1. The street's postcode inherited from its postal boundary at build time (way_postcodes.bin), or a POI's (`parent_postcode_id`); for a US street that answers alone, the ZIP of a TIGER line of that street within ~100 m (interp_postcodes.bin)
2. `get_nearest_postcode()` for a point: of the postal areas holding it of its country (or of none known) whose postcode the country has, the one whose centroid is nearest it in plain degrees; else the postcode centroid of the same country nearest it in plain degrees among those whose box holds it (postcode_centroids.bin). A centroid of no country stands for one of the point's, and where the point has no country every centroid counts, each with its own country's box. The box is `expand_by_meters`' (`geography_box.rs`): the envelope of the centroid's buffer on the geography, a diamond of the extent in the point's UTM zone or, beyond 70°, the polar Lambert azimuthal equal-area, as PostGIS's `_ST_BestSRID` picks; a degree of latitude there is a meridian degree, 110,574 m at the equator. A box across the antimeridian spans the longitudes between its corners the long way round, as `ST_Envelope` makes it, and holds no point on the antimeridian. The server looks for the boxes holding a point among the centroids within its country's extent of it (`BOX_REACH_FACTOR`), and among the centroids within that extent of the antimeridian at the point's latitude, at any distance, unless none of those can be as near as the best so far: those are the boxes that reach across it, which can hold a point however far west or east of the centroid. The point is the street's centroid, the point halfway along it in plain degrees (`get_center_point`: `ST_LineInterpolatePoint(geometry, 0.5)`), for a street and a house of it, and the country the one there (`get_country_code` on the street's centroid; a house takes its street's), the smallest country polygon holding it, else the answer's; a street no postcode reaches has none, and neither has an answer that is that street. For a house whose street has none, the point is the house; for a POI, and where no street or house answers (admin mode), the query point

Nominatim keeps only postal areas whose postcode matches the country's pattern (`_update_postcode_areas`); the builder marks the others (place type 15) and neither it nor the server takes a postcode from them. A postal area's centroid is that of the ring holding the point, where Nominatim's is the whole relation's. Nominatim's street also takes the postcode of the finest of its address areas that has one (`insert_addresslines`: an area's own postcode tag, or the one postcode whose centroid lies inside it) before `get_nearest_postcode()`; the polygons carry no postcode, so the server leaves that step out.
