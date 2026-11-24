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

The builder's `admin_rank_config.h` holds the `boundary` entries of Nominatim's `address-levels.json` as the public server runs it (master: Slovakia's levels were removed on 2026-05-02, and Brazil's administrative5 to 7 rank 0 since 5.3.0), each a (rank_search, rank_address) pair (`admin_ranks`): rank_search orders the indexer's runners. Countries with overrides: au, be, br, cz, de, es, id, jp, nl, no, ru, se, us. Nothing else ranks: the server reads the rank the builder stores.

A country's `place` entries (ca, de, es, id, ru, sa: CA and DE county 0, DE region 0, ES province 10, ID and RU municipality 18, SA province 12 and municipality 18) are in `place_ranks` there too, which ranks place areas, the own place tags of boundaries and the nodes they link, and keeps place=province areas for Spain and Saudi Arabia (every other place type of rank 0 everywhere makes no area).

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

**The builder links** (`builder/src/place_links.h`) as Nominatim's indexer does on a fresh import, between the full arc set and admin-minimal's (which keeps place areas by the type that labels them):

- **Order:** the countries (rank_address 1 to 4), then one runner per rank_search for the administrative boundaries (relations and closed ways), then one per rank_address for the place areas. Every polygon of a runner looks for its node against the links as they stood when the runner started; then each links its node in the runner's order (boundaries by admin level, place areas the larger first, then by osm id), and the last to link a node keeps it. So two boundaries of one runner can link one node, and each takes its rank: Lublin's level 6 links the city node, and its levels 7 and 8, later runners, find it linked.
- **The steps** (placex_triggers.sql:271-391), for a polygon of rank_search below 26 and a rank_address other than 0: a relation's first label member that is a place node of any value, linked or not, named or not; a place node of its wikidata that it covers, unlinked, of rank_search below 26, one named as it is first (an unnamed one before both, as Nominatim's `ORDER BY ... desc` puts a null first); with a name and a place value of its own, an unlinked covered node of that value whose name holds its name or lies in it; with a name, an unlinked covered node of its name whose rank in its own country is the polygon's. Names are the `name` tag, lower-cased; ties go to the lower osm id.
- **Label members:** a node that a relation links as its label member is linked by no other polygon's steps 2 to 4, whatever runner it is in. On a fresh import a polygon of an earlier runner would take it first and the label relation take it over; the public server indexes each polygon again after its import and finds it linked then (Den Haag's level 8 boundary keeps its 14: its wikidata node is the label of its level 10 one).
- **Candidates:** every place node of any value, named, of a wikidata or a relation's label member.
- **Label:** the linked node's type where AdminPlaceType holds it, else the polygon's own place tag. AdminPlaceType holds 15 types, a locality's among them.

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

**The builder computes the ranks** (`builder/src/rank_address.h`) as `placex_update` does at index time, on the polygons' exact rings, and stores each polygon's rank_address in its record's last byte (`admin_polygons.bin`) and each place node's promotion in its row (`place_nodes.bin`, PNL4). It takes the objects in the indexer's order on a fresh import: the boundaries by rank_search, then admin_level (one BoundaryRunner per rank_search), the place areas by rank, then the place nodes, each step reading the others' ranks as they stand then. A boundary (a relation or a closed way, its rings together):

1. **Admin nested** (placex_triggers.sql:876-910), for relations of rank > 0: the parent is the relation of the highest admin_level below the child's and above 3, of rank 1 to 25, that covers the child's point on surface (its widest ring's, as GEOS's `ST_PointOnSurface`): Bugisa sub-region (12) has its point outside Mbale City, which lies in it. A parent with the child's wikidata and its rings (`ST_Equals`) makes the child rank 0 (Lublin's levels 7 and 8, Bruxelles - Brussel at level 9). Else a parent of rank >= the child's lifts it to the parent's + 2, and to 25 under a parent of 24 or more.
2. **Place area** (:912-929), from rank 10: of the place polygons (an own place tag) of rank 1 to 23 whose place rank is the boundary's or more and that contain it whole (`ST_Contains`, not `ST_Equals`, tested on the rings in integers, `polygon_relations.h`), the one of the highest place rank lifts it to its rank + 2.
3. **Linked place, else the own place tag** (:1109-1172): the rank of the place node it links, in the node's country, replaces its rank where that is above the parent's rank (3 where it has none) and below 26, so a link can lower it (the parish of Winsford, linked to a town, ranks 16, its admin_level 20). Where nothing links, its own `place` tag's rank replaces a rank of 4 to 25 above the parent's, up or down: Baralla (`place=municipality`) ranks 14, Town Parks (`place=locality`) 25. The place values are address-levels.json's whole `place` table with its country entries (`PlaceValue`, admin_rank_config.h).

Administrative relations are boundaries at levels 2 to 12 (22 and 24 at 11 and 12; JP 24 at 11), postal areas are polygons of level 13 here. A closed administrative way is a boundary at levels 8 to 12 only, as Nominatim's import makes it (presets.lua `admin_boundary`); at 2 to 7 its place tag alone makes a row, a place area, and one without a place tag none, except that a level 2 way with a country code stays for the country, of rank 0. A closed way takes no admin-nested step; one with a place tag of rank 16 to 23 takes the place-area step below.

A place area (class='place': `boundary=place`, a place multipolygon or closed way, a `boundary=historic` or `ceremonial` with a place tag, such as County Antrim or Essex) takes none of the three steps: one of rank 16 to 23 is lifted to an area's rank + 2 where a boundary or a place area whose place rank is the place area's or more, up to 23, contains it (:930-950); a place=village area inside a city of rank 16 ranks 18. Its branch leaves `parent_address_level` unset, so a link labels it but never ranks it: Weesp (R15419242, `boundary=place`, `place=suburb`, the town node as its label) is a town of rank 20. Any other place area takes the rank of the node it links where that is above 3 and below 26. A boundary whose level ranks 0 in its country stays out of the address whatever its place type (Cairns Regional, Regierungsbezirk Köln). Areas of rank 25 are in no street's or house's address (getNearFeatures takes ranks below 25), so they fill no field and take no rank from place nodes. Where no street, house or POI is near, Nominatim answers with an area itself (reverse.py `lookup_area`, `_lookup_area_address`): of the named areas round the point of rank_address 4 to 25 but 5 and 11, the one of the highest rank_search, unless a place node of a higher rank_search inside it is within its `reverse_place_diameter` (in degrees: 0.04 for a village, 0.02 for a hamlet), which answers instead. The server does the same for an area of rank 25, which no other address shows: it leads the answer, labelled by its place type ("locality" for an Australian locality boundary or an Irish townland of `place=locality`), else as a city block ("Telfer, Western Australia, 6762, Australia"). A dir without streets can't tell that no street is near and answers as for a street at the point. Unnamed and ref-only roads are no streets of ours, so where only such a road is near (Ireland's L roads) Nominatim answers with the road and we with the area.

A place node of rank 16 to 23 ranks 2 more where a relation covering it has its rank (:951-971): a boundary by its rank, a place-tagged relation by its place rank (Carrick Hill, a suburb node, is 22 in Town Parks). Ways promote nothing.

The server ranks every boundary and place area holding the point at its stored rank, however many of a level hold it (Brussels' level 10 boundary round Quays Quarter), and the ranks they hold take no place node.

What stays different:

- A place value outside address-levels.json's (one PlaceValue, OTHER) links no node by the type step, and a relation's label members past its first are not read.
- `ST_Equals` holds here only for the same rings (an extra vertex on a straight side makes another ring), and the order within one of Nominatim's runners is its threads'.

Each rank then shows one polygon, as Nominatim's `complete_address_details` orders a rank's rows (results.py:611-632). The street's index-time walk (`insert_addresslines`) takes the areas of a rank by getNearFeatures' distance, the street middle's distance to the area plus a hundred-thousandth of the street's to its centroid (partition-functions.sql:32-35), weighted at rank 16 by `rank_search` as place nodes are (0.25 for 16, 0.5 for 18, placex_triggers.sql:597-605), and accepts the first whose centroid lies in the area accepted before it; that row scores 0, and another area holding the result's centroid scores 1, the farthest by plain distance first, then the one of the higher `rank_search`. A boundary's centroid is the place node it links where that lies inside it (:1121-1125), else here its point on surface; a boundary's `rank_search` is its level's in its country (address-levels.json), a place area's its label's. getNearFeatures takes an area for a street only where the two share a stretch, so of two or more polygons of a rank holding the query point, one the street does not run through is left out where another is one it runs through. Here every polygon of a rank holds the query point, so a rank of two or more shows the farthest the walk does not accept: at 40 Mespil Road, County Dublin over the postal district Dublin 4 (a place area of rank 12 whose centre is the nearer the street); at Donegall Square North, Belfast, the County Borough over Belfast City District, whose centroid is the city node at the City Hall beside the street; and on Malone Road the district, as the walk takes the borough, a place=city area of `rank_search` 16, first. The others are no part. The same walk, over the boundaries and the place areas holding the point, gives the place nodes their `current_boundary`. A boundary repeating its parent's wikidata and geometry ranks 0 in the builder, as Nominatim takes it out of addresses (Bruxelles - Brussel under Brussels), and is no row.

The answer is then a list of rows, finest first, as Nominatim's `complete_address_details` gives it: these polygons, and at each rank no polygon holds a place node. The street's walk takes the place nodes whose fuzzy area it crosses by rank, then by distance (weighted at rank 16 by `rank_search`), leaves out a node outside the area accepted before its rank (`current_boundary`, no row), and accepts the first of a rank that lies in the last accepted node's fuzzy area; one outside it is turned down but is a row all the same (placex_triggers.sql:606-669). The accepted node is its rank's row. A rank whose nodes the walk all turns down still shows one: their rows all score -1 at query time and none is an area, so `distance desc` puts the farthest first, by plain distance, then `rank_search desc` (results.py:611-632). Of Nislevvej's four hamlets of rank 20, outside the village Otterup's fuzzy area, that is Stormose; at Ngaio Road, the quarter Woodlands Park over the nearer Waimā. The node so shown sets no fuzzy area. A node whose fuzzy area misses the street, or which lies outside its rank's boundary, is no row (the White House's West End), nor is any node of a rank an area holds (Hayes Valley beside the South of Market area at 24). Only an area the walk accepts marks its rank as had and clears the last accepted node's fuzzy area (:645-652): at a rank whose areas the walk turns down a node can be accepted, which is no row shown, as an area holding the result's centroid shows there, but whose fuzzy area holds the next nodes. Each row is a part of display_name, and the address keeps the first row of each label, finest first (`_write_typed_address`, format_json.py:29-34), a place node's or a polygon's alike: Brentwood's town node, promoted to 18 inside the Town of Islip (a boundary of rank 16 labelled town), is the town, and the Town of Islip is a part of display_name only; the parish of Vernhams Dean (rank 20, a linked village) is the village over the village node Vernham Street (18), which display_name lists after it. The one exception is a region of the country's own name (the Netherlands' constituent country), which the address leaves out and display_name keeps.

## Step 5: Competing polygons

Nominatim's `insert_addresslines` sets `isaddress` on one area a rank, and `complete_address_details` puts an area holding the result's centroid before it at query time, so one row a rank is shown. We take the polygons holding the query point, walk them as `insert_addresslines` does and show the one of Step 4.

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

- At build time, as Nominatim does at index time: the builder's rank pass (Step 4), whose ranks the server reads
- Then one polygon a rank (Step 4), and each label to the finest row that has it

### Difference impact

Where Nominatim's rank adjustment matters:
- Two admin levels that both map to the same rank range (e.g., level 5 and 6 both → county in some countries)
- Nominatim bumps the inner one, and so does the builder, on the whole geometry (Step 4).

## How our implementation maps to Nominatim

| Nominatim concept | Our implementation | Status |
|---|---|---|
| address-levels.json | builder/src/admin_rank_config.h (the boundary and place entries) | ✅ Verified |
| get_label_tag() check 2-3 (place/linked_place extratags) | place_type_override field on AdminPolygon (from the linked place node, else the boundary's place tag; an OSM `linked_place` tag is dropped, as placex_update drops it) | ✅ Implemented |
| get_label_tag() check 4 (ADMIN_LABELS rank//2) | rank_to_field() function | ✅ Implemented |
| get_label_tag() check 5 (category[1] for place=* entities) | place_type_override covers this — boundary=place entities get place_type set | ✅ Implemented |
| find_linked_place() node linking | Builder (`place_links.h`): label role member, wikidata match, place type match, then name match, runner by runner as the indexer runs them (Step 3); a boundary or place area of rank 0 in its country links nothing (find_linked_place returns at once); a linked place node is hidden from place results, except in admin-minimal when that set leaves its boundary out (L9-L12, place areas below rank 16): nothing there stands for the node, and no boundary linking it promotes it | ✅ Implemented |
| Rank adjustment for nested boundaries | Builder (rank_address.h) on the exact geometry, in the indexer's order; the server reads the ranks, then one polygon a rank by the address walk | ✅ Implemented — what stays different is listed under Step 4 |
| boundary=place entities (e.g., Washington DC) | Extracted in builder with admin_level=15 + place_type_override | ✅ Implemented |
| Place nodes (`insert_addresslines`' walk over them, `complete_address_details`' row of each rank) | place_nodes.bin + find_places(): the street's walk over the nodes whose fuzzy area it crosses, one row a rank, the accepted node's, else the farthest the walk turned down inside its boundary | ✅ Implemented — place nodes are indexed at the admin cell level (L10); the cells under the street (or round the point without one), grown by the largest fuzzy half side, hold every node that can cover it |
| place_addressline pre-computed relationships | Not applicable — we do spatial lookup at query time | N/A — different architecture |
| Postcodes from addr:postcode | addr_postcodes.bin (an address point's own addr:postcode) and postcode_centroids.bin (centroids of every addr:postcode, plus external ones) | ✅ Implemented — see "Postcode handling" section below |
| The result's address values first in their rank (`CrosscheckNames`, results.py:610-632) | addr_cities.bin holds an address point's `addr:city` where a place or a boundary has that name (a core string, else none). When the house answers (it won, or a winning street takes its number from it), a row of the address named so takes its rank's place, unless a part of its rank, name and field is shown: a polygon by its own name or a place node's it links on any ring of its boundary (Nominatim's `_place_*` names), among the polygons ranked at the point, the others holding it and those the street runs through (getNearFeatures), the farthest from the street's middle first; else a place node that the street's walk writes a row for (every node of a rank inside its boundary until one is accepted, then those within twice the rank's nearest, as `insert_addresslines`); a node a polygon links is hidden and no row. The address then keeps each label's first row, finest first, so a label the replaced row leaves goes to the next row of that label | ✅ Implemented for `addr:city` — Drexel Hill on Whalens Court. Not: the other `addr:*` keys; the index-time row a value adds for a place outside the street's rows (`create_poi_search_terms`); a place's other names (Nominatim matches every name variant, we keep one name, so a borough whose `name:cs` is the value loses its rank to a node of that name); a boundary's linked names are keyed by its record (name, level, label, country, rank), which another boundary of that record shares |
| border_type tag | Not used: Nominatim never reads this tag | ✅ Ignored, as in Nominatim |
| Wikidata-based place linking | Part of find_linked_place() above: one place node of the boundary's wikidata that it covers (`_st_covers`), its namesake first | ✅ Implemented |
| Display name formatting (`display_name` in results.py) | `format_address`: the landmark, the house number with the road, the address's rows finest first, each at the rank_address of its polygon or place node, then the postcode (rank 5) and the country (4); a part is left out only where it repeats the part just before it, and every answer with a part has one | ✅ Implemented — the house number joined to the road as the country writes it |
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
- **We use these at build time**: a street (way_postcodes.bin) and a POI (`parent_postcode_id`) inherit the postcode of the postal boundary containing them. They are admin polygons of level 13, so the server also finds the ones holding the query point, for `get_nearest_postcode()` below. Each has the country its centroid lies in (`get_country_code`, as `update_postcodes` gives `place_postcode` its country), and one of a postcode its country's pattern rejects carries place type 15 (`POSTAL_AREA_INVALID`): Nominatim makes no postcode area of it (`_update_postcode_areas`), and it names nothing

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

0. A house's own addr:postcode (addr_postcodes.bin)
1. The street's postcode inherited from its postal boundary at build time (way_postcodes.bin), or a POI's (`parent_postcode_id`); for a US street that answers alone, the ZIP of a TIGER line of that street within ~100 m (interp_postcodes.bin)
2. `get_nearest_postcode()` for a point: of the postal areas holding it of its country (or of none known) whose postcode the country has, the one whose centroid is nearest it in plain degrees; else the postcode centroid of the same country nearest it in plain degrees among those whose box holds it (postcode_centroids.bin). A centroid of no country stands for one of the point's, and where the point has no country every centroid counts, each with its own country's box. The box is `expand_by_meters`' (`geography_box.rs`): the envelope of the centroid's buffer on the geography, a diamond of the extent in the point's UTM zone or, beyond 70°, the polar Lambert azimuthal equal-area, as PostGIS's `_ST_BestSRID` picks; a degree of latitude there is a meridian degree, 110,574 m at the equator. A box across the antimeridian spans the longitudes between its corners the long way round, as `ST_Envelope` makes it, and holds no point on the antimeridian. The point is the street's centroid, the point halfway along it in plain degrees (`get_center_point`: `ST_LineInterpolatePoint(geometry, 0.5)`), for a street and a house of it; a street no postcode reaches has none, and neither has an answer that is that street. For a house whose street has none, the point is the house; for a POI, and where no street or house answers (admin mode), the query point

Nominatim keeps only postal areas whose postcode matches the country's pattern (`_update_postcode_areas`); the builder marks the others (place type 15) and neither it nor the server takes a postcode from them. A postal area's centroid is that of the ring holding the point, where Nominatim's is the whole relation's. Nominatim's street also takes the postcode of the finest of its address areas that has one (`insert_addresslines`: an area's own postcode tag, or the one postcode whose centroid lies inside it) before `get_nearest_postcode()`; the polygons carry no postcode, so the server leaves that step out.
