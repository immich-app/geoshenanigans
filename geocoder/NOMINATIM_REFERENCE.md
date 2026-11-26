# Nominatim Reverse Geocoding — Reference

This document traces how Nominatim resolves admin boundaries to address fields, based on analysis of the source code at https://github.com/osm-search/Nominatim.

## Source files

| File | Purpose |
|---|---|
| `settings/address-levels.json` | Country-specific admin_level → address_rank mapping |
| `lib-sql/functions/ranking.sql` | `compute_place_rank()` — looks up rank from address_levels table |
| `lib-sql/functions/placex_triggers.sql` | `find_linked_place()` — links boundaries to place nodes. `placex_update()` — rank adjustments for nested boundaries |
| `src/nominatim_api/v1/classtypes.py` | `get_label_tag()` — determines the output field name (city, state, county, etc.); `OSM_ATTRIBUTION`, the `licence` string |
| `src/nominatim_api/v1/format_json.py` | `_write_typed_address()` — writes the address JSON using labels from get_label_tag; `format_base_json()` puts `licence` in every result |
| `src/nominatim_api/reverse.py` | `ReverseGeocoder.lookup()` — the reverse steps; `_find_closest_street_or_pois()` — the nearby lookup, tried first: the closest street, address point or rank-30 POI; `_lookup_area_others()` — the area fallback, which returns the bodies of water of `layer=natural` |
| `lib-lua/themes/nominatim/presets.lua`, `init.lua` | The import's main tags (`MAIN_TAGS_POIS`, `MAIN_TAGS.natural`), which tags import as which class/type (`natural`, `water`, `waterway`, `place`), and how an object's main tags give its class and categories (`process_tags`) |

## Step 1: admin_level → address_rank

`compute_place_rank()` in `ranking.sql` constructs a key like `"administrative5"` and queries the `address_levels` table (loaded from `address-levels.json`).

Default mapping: `admin_level * 2 = rank` (level 2→4, level 4→8, level 8→16).

Country overrides change these. Format in JSON: `[search_rank, address_rank]` where `address_rank=0` means non-addressable (boundary exists for search but not in address output). Single integer means both ranks are the same.

### Verified country overrides

The builder's `admin_rank_config.h` holds the `boundary` entries of Nominatim's `address-levels.json` as the public server runs it (master: Slovakia's levels were removed on 2026-05-02, and Brazil's administrative5 to 7 rank 0 since 5.3.0), each a (rank_search, rank_address) pair (`admin_ranks`): rank_search orders the indexer's runners. Countries with overrides: au, be, br, cz, de, es, id, jp, nl, no, ru, se; the US takes the default table. Nothing else ranks an address: the server reads the rank_address the builder stores, and derives rank_search from the same tables where it orders areas (`admin_search_rank`, `place_type_search_rank`: the walk's weights, a rank's rows, the area that answers).

A country's `place` entries (ca, de, es, id, ru, sa: CA and DE county 0, DE region 0, ES province 10 and civil_parish 18, ID and RU municipality 18, SA province 12 and municipality 18) are in `place_value_ranks` there too, the ranks of every place value of address-levels.json (`PlaceValue`), which rank place areas, the own place tags of boundaries and the nodes they link. The importer makes a place area only of a place value a polygon's label holds (`AdminPlaceType`: city, town, village, suburb, neighbourhood, quarter, state, province, region, county, district, borough, hamlet, municipality and locality; a closed way only of a neighbourhood, quarter, suburb, borough or locality), keeping place=province before the country is known, for Spain and Saudi Arabia (`is_address_place_type`; every other place type of rank 0 everywhere makes no area). The other values (square, city_block, farm, isolated_dwelling, croft, subdivision, allotments, civil_parish and the rest) make no area.

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

### The answer itself and the rows of its address

A row of another place's address goes through `_result_row_to_address_row` (results.py:463-481), which puts `extratags['linked_place']` in `extratags['place']` before `get_label_tag` reads it: there a linked boundary is labelled by its node's type first, its own place tag second, which is the label the builder gives (`place_type_override`). The answer itself, and its own first row (`_setup_address_details`), keep their extratags, so a boundary that is the answer, where no street is near, is labelled by an own place tag in its extratags first. Bandar Seri Begawan (R8748136, `place=municipality`, `linked_place=city`) is the city of the addresses round it.

The public server's rows mostly hold no own place tag in their extratags, though. A row keeps what the server's import made of the object when it last imported it, and the rows it imported before about October 2026 hold none: Arraias and Paranã (R336746, R336839, `place=municipality` in OSM since 2023, `linked_place=town`, no `place` in their extratags) answer "town" as the answer itself, as do Nossa Senhora do Livramento (R333615, last edited 2026-02) and Babinda (R5813924, `place=hamlet`, edited 2026-09, "suburb"). Rows imported since hold it: Curvelândia (R333654, edited 2026-10-17) and Barão de Melgaço (R333718) answer "municipality", Baralla (R342114) ranks by its tag. So the builder keeps the linked node's label for the answer itself too: labelling the 23,332 planet boundaries whose own tag and linked node differ by their own tag changes 9 of the saved answers, 7 of them for the worse. Which rows hold the tag is the server's history, not the data, and the same goes for the own-tag rank and type steps, which the builder applies to every boundary with a place tag.

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

1. **Label role member** — relation has a member node with role "label" and class "place", of any place type, the first such of its label members in relation order (one that is no place node is passed over): the steps after never run for it, so a boundary labelled by a place=locality node links no node of its name
2. **Wikidata match** — boundary and node share the same `wikidata` extratag, the boundary covers the node, and no boundary indexed before it linked the node (`linked_place_id is null`); of several, the first `ORDER BY lower(name->'name') = bnd_name desc` finds (a node of the boundary's name before others; a null comparison, either name missing, first of all), of those the one of the lowest address rank in its country (the order leaves them unordered: Espanola's town before its hamlet), then the lower node id. A boundary or place area of rank 0 in its country links nothing (find_linked_place returns at once) and reserves no node, nor its label member; one that links none keeps its own place tag for the steps after (Lent's place=quarter area, whose village node of the same wikidata its wijk links first, stays a quarter)
3. **Place type match** — boundary has `extratags->'place'` and a node with matching type + name exists inside
4. **Name match** — a place node with same name and appropriate rank inside the boundary, where either has no wikidata or both the same (Louvain-la-Neuve's section, Q665868, links no town node of its name tagged Q329642)

When linked: the boundary takes the place node's names it lacks, or holds with another value, as `_place_*` names (`placex_indexing_prepare`, placex_triggers.sql:76-108); the node's `name` is also its `name:xx` for the default language of its country, where the country has one language (:86-90). An English answer reads `name:en`, then `_place_name:en`, then `name` (localization.py `Locales`, `_mingle_name_tags` showing a `_place_` name under its own key where the polygon lacks it), and `add_default_place_name` gives a polygon's `name` its `name:en` too in a country whose one language is English (:1271, utils.sql:121-139). So a linked boundary without a `name:en` of its own goes by its node's `name:en`, outside the English-only countries: Sosnovka (Tambov Oblast) and Kampala Capital City (R10546821, whose city node's `name:en` is Kampala). The place node itself gets `linked_place_id` set (effectively hidden from results, as it's represented by the boundary).

**NOTE**: Linking affects names and search, and can also change the label and the address rank: the builder labels a polygon by the linked node's type where AdminPlaceType has that type (Nominatim's `linked_place` extratag: `get_label_tag()` reads a polygon's own `place` first, but in another result's address `_result_row_to_address_row` first copies `linked_place` into `place`, Step 2, so there the linked type wins), and Step 4 says where the node's address rank replaces the polygon's.

**The builder links** (`builder/src/place_links.h`) as Nominatim's indexer does on a fresh import, between the full arc set and admin-minimal's (which keeps place areas by the type that labels them):

- **Order:** the countries (rank_address 1 to 4), then one runner per rank_search for the administrative boundaries (relations and closed ways), then one per rank_address for the place areas. Every polygon of a runner looks for its node against the links as they stood when the runner started; then each links its node in the runner's order (boundaries by admin level, place areas the larger first, then by osm id), and the last to link a node keeps it. So two boundaries of one runner can link one node, and each takes its rank: Lublin's level 6 links the city node, and its levels 7 and 8, later runners, find it linked.
- **The steps** (placex_triggers.sql:271-391), for a polygon of rank_search below 26 and a rank_address other than 0: a relation's first label member that is a place node of any value, linked or not, named or not; a place node of its wikidata that it covers, unlinked, of rank_search below 26, one named as it is first (an unnamed one before both, as Nominatim's `ORDER BY ... desc` puts a null first), of those the one of the lowest address rank in its country, one of rank 0 last; with a name and a place value of its own, an unlinked covered node of that value whose name holds its name or lies in it; with a name, an unlinked covered node of its name whose rank in its own country is the polygon's. Names are the `name` tag, lower-cased; ties go to the lower osm id.
- **Label members:** a node that a relation links as its label member is linked by no other polygon's steps 2 to 4, whatever runner it is in. On a fresh import a polygon of an earlier runner would take it first and the label relation take it over; the public server indexes each polygon again after its import and finds it linked then (Den Haag's level 8 boundary keeps its 14: its wikidata node is the label of its level 10 one). A polygon the public server has not indexed again since keeps its older link and the rank it gave: Ferraz de Vasconcelos' municipality (R298265, `place=municipality`) ranks 16 there and its city (R5683099, whose label is the city node) 18, Queluz (R298199) and Estiva Gerbi (R297955) 16, their extratags still naming the node's type as `linked_place`; the builder gives 14, 16, 14 and 14, as a re-index does.
- **Candidates:** every place node of any value, named, of a wikidata or a relation's label member.
- **Label:** the linked node's type where AdminPlaceType holds it, else the polygon's own place tag. AdminPlaceType holds 15 types, a locality's among them.
- **Name:** a linking polygon without a `name:en` of its own, outside a country whose one language is English (country_settings.yaml), goes by its node's `name:en`, or by its node's `name` where the node's own country's one language is English (`linked_english_name`); about 26,000 polygons on the planet. Its own name stays one of its names, as Nominatim keeps it in the polygon's `name` beside the `_place_*` ones: `admin_own_names.bin` holds it beside the admin polygons, and the server's addr:city crosscheck (`CrosscheckNames` compares every name, results.py:606) and place search read it there.

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

1. **Admin nested** (placex_triggers.sql:876-910), for relations of rank > 0: the parent is the relation of the highest admin_level below the child's and above 3, of rank 1 to 25, that covers the child's point on surface (its widest ring's, as GEOS's `ST_PointOnSurface`): Bugisa sub-region (12) has its point outside Mbale City, which lies in it. A parent with the child's wikidata and an equal geometry (`ST_Equals`: the same outlines, from any vertex on, either way round, whatever vertices either adds along its straight edges, `polygons_equal`) makes the child rank 0 (Lublin's levels 7 and 8, Bruxelles - Brussel at level 9); step 3 then reads its parent's rank as 3. Else a parent of rank >= the child's lifts it to the parent's + 2, and to 25 under a parent of 24 or more.
2. **Place area** (:912-929), from rank 10: of the place polygons (an own place tag) of rank 1 to 23 whose place rank is the boundary's or more and that contain it whole (`ST_Contains`, not `ST_Equals`, tested on the rings in integers, `polygon_relations.h`), the one of the highest place rank lifts it to its rank + 2.
3. **Linked place, else the own place tag** (:1109-1172): the rank of the place node it links, in the node's country, replaces its rank where that is above the parent's rank (3 where it has none) and below 26, so a link can lower it (the parish of Winsford, admin_level 10, linked to a town, ranks 16 instead of its level's 20). Where nothing links, its own `place` tag's rank replaces a rank of 4 to 25 above the parent's, up or down: Baralla (`place=municipality`) ranks 14, Town Parks (`place=locality`) 25. The place values are address-levels.json's whole `place` table with its country entries (`PlaceValue`, admin_rank_config.h).

Administrative relations are boundaries at levels 2 to 12 (22 and 24 at 11 and 12; JP 24 at 11), postal areas are polygons of level 13 here. The level is the import's Lua `tonumber` of the tag where that is a whole number of 1 to 15, else 15 (init.lua:306-311), which address-levels.json gives no administrative rank, so a relation or way of `admin_level=5;6`, `6.5` or `261` is in no address and links no place node (`nominatim_admin_level`). A named closed administrative way is a boundary of its level, its place tag in its extratags (Nominatim 5.3.0's presets.lua:116-125, `boundary = {'named', ...}`, and init.lua:740-745, which keeps a boundary's place tag as an extratag but for an island's), and `placex_insert` drops one of level 4 or less (placex_triggers.sql:687-690): a boundary from level 5. The public server's rows of these ways were made so: Iferouane (W1333677402, level 6) is a county there. Master has since made a way a boundary at levels 8 to 12 only (presets.lua `admin_boundary`, 2026-11-15), which the public server takes only for a way edited after it. A level 2 way with a country code stays for the country here, of rank 0. A closed way takes no admin-nested step; one with a place tag whose own rank_address (its level's, before any step) is 16 to 23 takes the place-area step below, as master's triggers, which the public server runs (database 5.3.99), take every polygon with a place category there, the categories of its older rows filled from their class, type and extratags' place (migration.py `backfill_categories`).

A place area (class='place': `boundary=place`, a place multipolygon or closed way, a `boundary=historic` or `ceremonial` with a place tag, such as County Antrim or Essex) takes none of the three steps: one of rank 16 to 23 is lifted to an area's rank + 2 where a boundary or a place area whose place rank is the place area's or more, up to 23, contains it (:930-950); a place=village area inside a city of rank 16 ranks 18. Its branch leaves `parent_address_level` unset, so a link labels it but never ranks it: Weesp (R15419242, `boundary=place`, `place=suburb`, the town node as its label) is a town of rank 20. Any other place area takes the rank of the node it links where that is above 3 and below 26. A boundary whose level ranks 0 in its country stays out of the address whatever its place type (Cairns Regional, Regierungsbezirk Köln). Areas of rank 25 are in no street's or house's address (getNearFeatures takes ranks below 25), so they fill no field and take no rank from place nodes. Where no street, house or POI is within 0.007 degrees, planar (reverse.py `lookup_street_poi`), Nominatim answers with an area itself (`lookup_area`, `_lookup_area_address`): of the named areas round the point of rank_address 4 to 25 but 5 and 11, the one of the highest rank_search, unless a place node of a higher rank_search inside it is within its `reverse_place_diameter` (in degrees: 0.04 for a village, 0.02 for a hamlet): the one of the highest rank_search, then the nearest, answers instead, its own address of the ranks below its own. The server does the same where that area is one of rank 25, which no other address shows: the node leads the answer, or else the area, labelled by its place type or the type of the node it links ("locality" for Telfer, an Australian locality boundary linking its `place=locality` node, "Telfer, Western Australia, 6762, Australia", or an Irish townland of `place=locality`), else as a city block (an Indonesian desa its parent lifts to 25). The server reads the streets, address points and POIs within that reach, farther than its own street search, a POI whatever its street, before the area answers. A dir without streets can't tell that no street is near and answers as for a street at the point. Unnamed and ref-only roads are no streets of ours, nor are locality, farm and isolated dwelling nodes place nodes of ours, so where only such a road is near (Ireland's L and R roads) Nominatim answers with the road, and where such a node is (Llano Grande in Arboledas, Colombia) with the node, and we with the area.

A place node of rank 16 to 23 ranks 2 more where a relation covering it has its rank (:951-971): a boundary by its rank, a place-tagged relation by its place rank (Carrick Hill, a suburb node, is 22 in Town Parks). Ways promote nothing.

The server ranks every boundary and place area holding the point at its stored rank, however many of a level hold it (Brussels' level 10 boundary round Quays Quarter), but of levels 3 to 12 a larger one beside the smallest of its level there, not round it (`polygon_within`), as the simplified presets' neighbours overlap along their shared borders; the ranks they hold take no place node.

`build-index --rank-dump <path>` writes how each boundary and place area came to its rank, a TSV row an object by osm id (r for a relation, w for a closed way): its level, country, rank_search and initial rank, its admin-nested parent and the parent's rank, whether it repeats its parent, the place area that lifted it and that one's rank, its rank before the link step, the step that ranked it (L for the link, O for its own place tag), its rank, and the link step and node. The path must not be there yet: the build makes the file at its start, and fails before it reads anything where the path names a file.

What stays different:

- A place value outside address-levels.json's (one PlaceValue, OTHER) links no node by the type step.
- The order within one of Nominatim's runners is its threads'.

Each rank then shows one polygon, as Nominatim's `complete_address_details` orders a rank's rows (results.py:611-632). The street's index-time walk (`insert_addresslines`) takes the areas of a rank by getNearFeatures' distance, the street middle's distance to the area plus a hundred-thousandth of the street's to its centroid (partition-functions.sql:32-35), weighted at rank 16 by `rank_search` as place nodes are (0.25 for 16, 0.5 for 18, placex_triggers.sql:597-605), and accepts the first whose centroid lies in the area accepted before it (here an area smaller than that one is taken as lying in it and its centroid is not tested, a larger or equal one's is: `CurrentBoundaries::accepted`); that row scores 0, another area holding the result's centroid scores 1 and one that does not -1, the farthest by plain distance first, then the one of the higher `rank_search`. The result's centroid is the answering house's or POI's, else the street's point nearest the query (`ST_ClosestPoint`, reverse.py:46). A boundary's centroid is the place node it links where that lies inside it (:1121-1125), else here its point on surface; a boundary's `rank_search` is its level's in its country (address-levels.json), a place area's its label's. getNearFeatures takes an area for a street only where the two share a stretch, so of two or more polygons of a rank holding the query point, one the street does not run through is left out where another is one it runs through. Here every polygon of a rank holds the query point, so a rank of two or more shows the farthest the walk does not accept where that holds the result's centroid, else the one it accepts: at 40 Mespil Road, County Dublin over the postal district Dublin 4 (a place area of rank 12 whose centre is the nearer the street); at Donegall Square North, Belfast, the County Borough over Belfast City District, whose centroid is the city node at the City Hall beside the street; and on Malone Road the district, as the walk takes the borough, a place=city area of `rank_search` 16, first. The others are no part. The same walk, over the boundaries and the place areas holding the point, gives the place nodes their `current_boundary`. A boundary repeating its parent's wikidata and geometry ranks 0 in the builder, as Nominatim takes it out of addresses (Bruxelles - Brussel under Brussels), and is no row.

The answer is then a list of rows, finest first, as Nominatim's `complete_address_details` gives it: these polygons, and at each rank no polygon holds a place node. The street's walk takes the place nodes whose fuzzy area it crosses by rank, then by distance (weighted at rank 16 by `rank_search`), leaves out a node outside the area accepted before its rank (`current_boundary`, no row), and accepts the first of a rank that lies in the last accepted node's fuzzy area; one outside it is turned down but is a row all the same (placex_triggers.sql:606-669). The accepted node is its rank's row. A rank whose nodes the walk all turns down still shows one: their rows all score -1 at query time and none is an area, so `distance desc` puts the farthest first, by plain distance, then `rank_search desc` (results.py:611-632). Of Nislevvej's four hamlets of rank 20, outside the village Otterup's fuzzy area, that is Stormose; at Ngaio Road, the quarter Woodlands Park over the nearer Waimā. The node so shown sets no fuzzy area. A node whose fuzzy area misses the street, or which lies outside its rank's boundary, is no row (the White House's West End). A node of a rank an area holds is a row the answer does not show, the area's coming first (Hayes Valley inside the South of Market area at 24), but a house's `addr:city` naming it can still make it the rank's row (the crosscheck below). Only an area the walk accepts marks its rank as had and clears the last accepted node's fuzzy area (:645-652): at a rank whose areas the walk turns down a node can be accepted, which is no row shown, as an area holding the result's centroid shows there, but whose fuzzy area holds the next nodes. Each row is a part of display_name, and the address keeps the first row of each label, finest first (`_write_typed_address`, format_json.py:29-34), a place node's or a polygon's alike: Brentwood's town node, promoted to 18 inside the Town of Islip (a boundary of rank 16 labelled town), is the town, and the Town of Islip is a part of display_name only; the parish of Vernhams Dean (rank 20, a linked village) is the village over the village node Vernham Street (18), which display_name lists after it. The one exception is a region of the country's own name (the Netherlands' constituent country), which the address leaves out and display_name keeps.

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

## The primary POI: Nominatim's rank-30 lane

`_find_closest_street_or_pois()` (`reverse.py:160-240`) takes, besides streets
and address points, POIs of `rank_search = 30` whose class is not `place` or
`building` and whose geometry is not a line (`reverse.py:192-195`). The
closest wins, and an answer whose primary is a POI takes its `road` and
`postcode` from it (its parent street, its own postcode). The server's
`find_nearest_poi_with_parent()` is that lane.

**Which objects rank 30.** An object gets one row, of all its main tags'
categories (`init.lua` `process_tags`, lines 734-878): its class and type are
its alphabetically first main tag that is no fallback tag, else its first
fallback tag. Its ranks are those of the category with the lowest positive
address rank, else the lowest search rank (`ranking.sql`
`compute_place_rank`, lines 132-233). `address-levels.json` ranks (lines as
of master 5348a803, 2026-11-21):

| Main tag | rank_search, rank_address | Lines |
|---|---|---|
| `place=*` of an address rank: `county` 12, ..., `subdistrict` 14, ..., `locality` and `plot` 25, `square` 26 | as listed | 3-38 |
| `natural=*` other than peak, volcano and mountain_range (18, 0), sea (4, 0) and tree, spring, cave_entrance, geyser, hot_spring, rock and stone (30, 30) | 22, 0 | 69-82 |
| `water=*` (with `natural=water`) | 20 to 24, 0 | 83-90 |
| `waterway=*` other than boatyard, lock_gate and access_point (30, 30) | 19 to 22, 0 | 91-100 |
| `landuse=*` on an area | 24, 0 (residential and the other admin ones 24, 24) | 53-62 |
| `landuse=*` on a node or line | 30, 30 (`ranking.sql:178-180`) | |
| a key it doesn't list: `amenity`, `shop`, `tourism`, `emergency`, most `leisure`, ... | 30, 30 (`ranking.sql:195-198`) | |

So no water kind ever reaches the lane: `natural=water`, `wetland`, `bay` and
`glacier` rank 22, and `landuse=reservoir` and `basin` 24 on an area. The
server never lets those categories (`NATURAL_WATER`, `WETLAND`, `BAY`,
`GLACIER`, and `RESERVOIR` as a polygon) take the primary slot, nor set `road`
or `postcode` from it (`Index::is_below_poi_rank`); a `RESERVOIR` point, a
`landuse=reservoir|basin` node, is a rank-30 POI there and stays, but for one
a main tag of a lower rank ranks, or whose class the lane takes none of
(below), which the builder links no parent street. A reservoir or basin
beside a `natural=*` value of rank 30 alone (`spring`, `hot_spring`, ...) is a
rank-30 POI of it, class `natural`.

**Water streets.** A water area a `highway=*` kind of rank 26 or 27 ranks
(`area=yes`, `highway=pedestrian`, `natural=water`, ...), where no place or
landuse kind of a rank below 26 ranks it lower (`place=square` ranks 26), is
one placex row of class `highway` and `rank_address` 26 or 27 in Nominatim: a
street, whose street lane takes address ranks 26 to 29 (`reverse.py:187-189`)
by the distance to its geometry, 0 inside a polygon, and answers its own name
(`lookup_street_poi`). A street has no parent street (`placex_update` parents
`rank_search > 27` alone) and is no address point, whatever `addr:*` tags it
carries (`IsAddressPoint` takes `rank_address = 30` alone). A multipolygon's
geometry is its polygon; a closed way's is its polygon where `area=yes`
alone, and its outline as a line else (`init.lua` `process_way`). The builder
names one by its tags (`poi_classify.h` `poi_water_street`): its POI polygon
takes the highway flag, but for a closed way of no `area=yes`, which is the
line the street index holds, and it writes no address point of it. A
reservoir or basin way of `area=yes` that is a street is a POI polygon of its
own, with no POI main tag beside it (`way_tags.h` `has_poi_tags`). The
server lets a water area of that flag compete as streets do, linked to a
parent street or not, its own name the `road` wherever it wins
(`Index::is_water_street`): no address node inside it takes its place, as a
rank-30 one does in an area of `rank_search` above 27 alone
(`lookup_street_poi`), nor does a tier-1 one take a landmark's head start. It
reaches as far as a POI does (100 m), about as far as the street scan, of the
query's street cell and its neighbours. Three differences stay: Nominatim's
lane takes an area only where its centroid is within 0.007 degrees of the
point too; its answer for a street of `addr:housenumber` carries that house
number (`results.py` `_setup_address_details`), which a POI record holds none
of, and its postcode is the street's own `addr:postcode` first
(`placex_update`, `token_get_postcode`), where a street or POI here takes its
postal boundary's, as below.
Such a street ships in the POI tier of its water kind or in that of the
category its tags give, whichever is lower: a lake that is an attraction is in
`poi/notable`, as an attraction is.

**Water with a POI main tag.** A water object that also carries a main tag of
the import's POI list (`presets.lua` `MAIN_TAGS_POIS`, lines 170-262) with a
category ranked 30 has a positive address rank, so it ranks 30 and is in the
lane: `amenity=fountain` and `leisure=swimming_area` (class `amenity`,
`leisure`, alphabetically before `natural`), `emergency=fire_water_pond`,
`shop=*`, `man_made=pier`, and so on. A main tag of a lower positive address
rank beside it ranks the object instead, out of the lane: `place=*` of an
address rank (`locality` and `plot` 25, `suburb` 20, `subdistrict` 14, ...),
and on an area `landuse=residential` and the other admin landuse kinds (24)
and the `highway=*` kinds (26, 27); a landuse or highway node ranks 30. Such
an object stays water (`poi_classify.h` `has_lower_rank_main_tag`), whatever
its other tags. The builder classifies a rank-30 one by its rank-30 tags
(`has_poi_main_tag`): `FOUNTAIN`, `SWIMMING_AREA`, `FIRE_WATER_POND`, `PIER`,
..., or `GENERIC_RANK30` where they have no category of their own; a tag of a
lower rank or of no main tag (`boundary=*`, `landuse=*` on an area,
`leisure=park`, `natural=wood`, `place=island`, `waterway=waterfall`,
`man_made=dam`, `power=*`) gives it none, though the classifier reads some of
them before the water kind. Both rules run before any category. A generic
rank-30 feature ships only with a wiki tag, as every one does; without one, the
object stays water, so its name stays in `places` and as the `landmark`, out
of the primary slot. Where its water kind ships in a lower POI tier than its
category, the tiers between ship it as the water kind (`types.h`
`poi_record_in_tier`): a bay (notable) that is a pier (all) is a `BAY` in
`poi/notable` and a `PIER` in `poi/all`. The way pass resolves a named
`emergency=fire_water_pond` way, and a named `landuse=reservoir|basin` way
with a POI main tag, though the other `emergency=*` values and `shop=*` make
no POI way alone (`way_tags.h` `has_poi_tags`). Values the import drops
(`amenity=parking_space`, `emergency=fire_hydrant`, `tourism=yes`, ...) and
tags of a rank below 30 and no address rank (`leisure=park`,
`historic=neighbourhood`, `place=island`, `waterway=*`) leave the water
category; beside a tag the object yields to, `shop=no` makes no shop.
`building=*`, a fallback tag, ranks 30 beside `natural=*` (the class stays
`natural`) but is the class beside `landuse=*`, also a fallback tag, and
the lane takes no class `building`. So a reservoir or basin whose class the
lane leaves out stays one, whatever tag ranks it 30
(`reservoir_leaves_poi_lane`): `building` where it has fallback tags alone
(`building`, `historic`, `tourism=attraction`), building sorting first
among them, and `place` where `place=*` sorts before every other main tag of
no fallback (`place=island` with `tourism=attraction` or `shop=bakery`, not
with `amenity=fountain`).

**Not matched.**
- The builder reads no `club`, `military`, `healthcare` or `aerialway` key,
  nor a node's `highway`: a water object whose only other main tag is one of
  those keeps its water category, though it ranks 30 in Nominatim. So does
  one beside `waterway=boatyard`, `lock_gate` or `access_point`, which rank
  30 (`address-levels.json:96-98`) where the builder takes no `waterway=*`
  for a POI main tag.
- Multipolygons: Nominatim imports one as it does a closed way, and the
  builder classifies a water multipolygon that is a rank-30 POI of another
  main tag as it does a closed way (a bay or a lake that is a swimming area
  is a `SWIMMING_AREA`). Otherwise its relation scan takes only some keys,
  and of the water kinds bays, glaciers and streets alone: a multipolygon bay
  or glacier the classifier keeps water (a lower rank, or rank-30 tags no
  tier ships) is the `BAY` or `GLACIER` it is, and a water multipolygon a
  highway kind ranks a street the water it is, of the highway flag, as the
  closed way is; a lake, wetland or reservoir of no street is no POI as its
  water kind, nor in the tiers that don't ship the POI it is, and one the
  scan's own keys take (`leisure=nature_reserve`, `tourism=attraction`,
  `place=island`, ...) keeps that category in `places` but never takes the
  primary slot (the builder links it no parent street). A POI polygon of a
  multipolygon is its outer rings, without its holes, so a water
  multipolygon with an `inner` member the classifier makes one that could
  take the slot (a rank-30 POI, or a street) takes the scan's own categories
  instead, and never the slot: one the scan takes none of (a lake that is a
  swimming area) is no POI, so a point on its island finds neither its name
  nor its road, nor the address its `addr:*` tags give; the one it takes is
  no street either. No water multipolygon of holes is an address point,
  whatever its `addr:*` tags. A plain bay or glacier of holes keeps its
  parent street.
- Where no boundary and no street anchors the address (a coast no boundary
  covers), Nominatim finds nothing; a water POI that would have won the slot
  but for the lane still gives the answer its `landmark` and `places`,
  without a `road` or `postcode` (`Index::query`). A bay or glacier would
  have won it as an area label (below), only where no other POI is near.
- A water object of a generic rank-30 tag without a wiki tag stays water,
  out of the primary slot, where Nominatim's lane holds it as a POI.
- The lower rank leaves out `boundary=administrative`, which ranks by an
  admin_level the classifier doesn't read, and the country tables of
  `address-levels.json` (`place=county` in ca and de, ...).
- The area-label lane: peaks, volcanoes, beaches, capes, islands, parks,
  nature reserves, stadiums, gardens, water parks, golf courses, national
  parks, protected areas, campuses (university, college, hospital),
  marketplaces, cemeteries, prisons, theme parks, zoos, camp sites, resorts,
  battlefields, aerodromes and power plants (`Index::is_area_label_category`)
  take the primary slot only where no other POI is near. Nominatim ranks many
  of them below 30 (peak and volcano 18, beach and cape 22, island 17 or 22,
  park and nature_reserve 24, garden 25, protected areas 25) and holds the
  others in the lane like any POI. Bays and glaciers are not in it: as water
  kinds, they never take the slot.
- River and canal areas (`natural=water` with `water=river`, `canal`, ...)
  are POIs here, though Nominatim imports none (`presets.lua:157-163` gives
  them no category). As water they never take the primary slot; they can be
  the `landmark` and are in `places`.

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
| Place nodes (`insert_addresslines`' walk over them, `complete_address_details`' row of each rank) | place_nodes.bin + find_places(): the street's walk over the nodes whose fuzzy area it crosses, one row a rank, the accepted node's, else the farthest the walk turned down inside its boundary. A place node is an address row only inside the walk's current_boundary, the last row insert_addresslines took that is no place node, a place area as much as a boundary (:583-586, :622-632): the node Al Khalidiyah outside the place=suburb way Al Bateen round the point is in no address there. It ranks two up inside a relation of its rank alone, a boundary of that rank_address or a place area of that place rank, not inside a closed way's area (osm_type = 'R', :984-1004): the place=suburb node Ramat HaNasi in the place=suburb way בת ים stays at 20, where the way's row comes first | ✅ Implemented — place nodes are indexed at the admin cell level (L10); the cells under the street (or round the point without one), grown by the largest fuzzy half side, hold every node that can cover it |
| place_addressline pre-computed relationships | Not applicable — we do spatial lookup at query time | N/A — different architecture |
| Postcodes from addr:postcode | addr_postcodes.bin (an address point's own addr:postcode) and postcode_centroids.bin (centroids of every addr:postcode, plus external ones) | ✅ Implemented — see "Postcode handling" section below |
| The result's address values first in their rank (`CrosscheckNames`, results.py:610-632) | addr_cities.bin holds an address point's `addr:city` where a place or a boundary has that name (a core string, else none). When the house answers (it won, or a winning street takes its number from it), a row of the address named so takes its rank's place, unless a part of its rank, name and field is shown: a polygon by its own name or a place node's it links on any ring of its boundary (Nominatim's `_place_*` names), among the house's street's rows, the polygons the street meets in more than a point anywhere along it, the polygon taken at the street's f32 precision, on which a node the boundary shares lies on its vertex (a node inside, a segment crossing or running along an edge, or two touches; getNearFeatures takes the street whole: the admin cells under it are read, and a polygon ranked at the point is one only where the street meets it), or without a street those holding the point, each at the rank the builder gave it (a boundary under the boundaries round its own point on surface, as `placex_update` ranks it), the farthest from the street's middle first; else a place node that the street's walk writes a row for (every node of a rank inside its boundary until one is accepted, then those within twice the rank's nearest, as `insert_addresslines`), the farthest first, of equals the one of the higher rank_search; a node a polygon links is hidden and no row. The address then keeps each label's first row, finest first, so a label the replaced row leaves goes to the next row of that label | ✅ Implemented for `addr:city` — Drexel Hill on Whalens Court. Not: the other `addr:*` keys; the index-time row a value adds for a place outside the street's rows (`create_poi_search_terms`); a place's other names (Nominatim matches every name variant, we keep one name, so a borough whose `name:cs` is the value loses its rank to a node of that name); a boundary's linked names are keyed by its record (name, level, label, country, rank), which another boundary of that record shares |
| border_type tag | Not used: Nominatim never reads this tag | ✅ Ignored, as in Nominatim |
| Wikidata-based place linking | Part of find_linked_place() above: one place node of the boundary's wikidata that it covers (`_st_covers`), its namesake first | ✅ Implemented |
| `licence` in every result (`format_base_json`) | `licence` first in every answer, `OSM_ATTRIBUTION`'s words (`Address::licence`); `{}` where no loaded region covers the point, as Nominatim's error carries none | ✅ Implemented |
| The rank-30 POI lane of `_find_closest_street_or_pois` | `find_nearest_poi_with_parent()`: water kinds never in it, water with a POI main tag classified by that tag (The primary POI, above) | ✅ Implemented — the area-label lane is a divergence |
| Display name formatting (`display_name` in results.py) | `format_address`: the landmark, the house number with the road, the address's rows finest first, each at the rank_address of its polygon or place node, then the postcode (rank 5) and the country (4); a part is left out only where it repeats the part just before it, and every answer with a part has one | ✅ Implemented — the house number joined to the road as the country writes it |
| Street/road resolution | Minor differences in closest street selection | ℹ️ Our point-to-segment distance vs Nominatim's pre-computed parent streets. Causes occasional one-off differences at intersections. |
| How far the closest street or address is looked for (`lookup_street_poi` runs `_find_closest_street_or_poi` within 0.006°) | `DEFAULT_SEARCH_DISTANCE`, 130 m: how far the street and address lookups reach, wherever the point lies in its street cell, and none farther: the nearest street way (measured to its segments) and address point that compete to be the answer's feature, and the interpolation lines that give a street that wins its house number | ℹ️ Divergence — Nominatim reaches 0.006° (about 660 m), but a road that far off is no longer the road the point is on: no street way past 130 m names the road. An address or POI that wins still gives the road it names, and a street that wins still takes its house number from within 100 m, as `_find_housenumber_for_street` does within 0.001° |
| Suburb/neighbourhood rank adjustment | Builder rank pass (Step 4); the server reads the stored address ranks | ✅ Implemented |
| TIGER address interpolation | `--tiger-data` loads Nominatim's TIGER export (address ranges, one CSV per county) as interpolation lines, with each line's ZIP in interp_postcodes.bin | ✅ Implemented — a US street that wins without an address point takes its house number from the nearest interpolation line of that street, as Nominatim's `_find_tiger_number_for_street` does |
| Postal city names | `address.postal_city` (US): the ZIP's postal city, from postal_cities.bin: from five votes, the `addr:city` most of its addresses carry, GeoNames' USPS place name and yesterday's name counting max(2, 10 %) more; below five, GeoNames' name; spelt as the ZIP's or its state's addresses spell it (`builder/src/postal_cities_build.h`) | ➕ An addition, not a Nominatim field — USPS assigns city names to ZIP codes that may differ from the municipal jurisdiction (e.g., "Cumming, GA" for unincorporated Forsyth County). Nominatim's own address rows reach some of these names (Pittsburgh, Miami) as place parts, from the houses' `addr:city`; `postal_city` changes no field Nominatim has, nor `display_name`. |
| `layer=natural` water bodies | The `water` field beside the address, from an optional water dir: the smallest area holding the point, then rank; river and canal areas named by their waterway; seas and oceans from Marine Regions; `water.landmark` where a location line names the water (see "Water" above) | ℹ️ Extension — Nominatim's default answer has no body of water, and its `layer=natural` area order is rank first |
| Name localisation | Not implemented — we store one name per feature, `name:en` where present, else `name` | ❌ Deferred — Nominatim supports `accept-language` header to return locale-specific names (e.g., `name:en=Borough of Luton` instead of `name=Luton`). Builder would need to store `name:en`, `name:fr`, etc. in the string pool (or a separate name table), and server would need to select based on client language preference. |

## Water (an extension)

### Nominatim's approach

- **No body of water in a default answer.** Bodies of water rank 19 to 24
  (`rank_search`; `waterway=river` 19, `water=lake|reservoir` 20, `water=pond`
  24, address-levels.json:83-99) with `rank_address = 0`, and a default reverse
  (`layer` unset: address and POI) takes streets and addresses (`rank_address`
  26 and up) and POIs of `rank_search` 30 only (reverse.py:187-195), so it never
  returns one nor puts one in the address. Water features of rank 30,
  `natural=spring` and `waterway=lock_gate|boatyard|access_point`
  (address-levels.json:75, 96-98), are POIs there like any other. A point in
  water gets the nearest street, address or POI within 0.007°, else the admin
  polygon reaching into the water, else the country of the static country grid
  (~12 nm offshore), else `Unable to geocode`.
- **`layer=natural`** first tries the nearby lookup (`lookup()` calls
  `lookup_street_poi()` at a max rank of 26 or more, reverse.py:618), which takes
  the layer's features of `rank_search` 26 up to that rank with
  `rank_address = 0` (`_find_closest_street_or_pois`, reverse.py:196-199): water
  features of rank 30, never a body of water. Those come from its area fallback
  (`_lookup_area_others`, reverse.py:461):
  named features of class `natural`, `water` or `waterway` whose bounding
  box overlaps the point ± 0.007°, the top 50 by `rank_search` (highest first),
  then distance; an area must contain the point, a line need not. So the higher
  rank wins, not the area holding the point: the culverted Tyburn stream
  relation (21) over The Serpentine (a lake, 20) that holds the point, and a
  wetland (22) over a lake inside it; a tie of rank (in the Venice lagoon the
  Laguna Sud wetland and the Laguna di Venezia lagoon, both 22, both holding the
  point) goes to database order.
- **Seas and oceans** never: `place=sea` ranks 2 and reverse takes
  `rank_search >= 5`; `place=ocean` exists only as nodes.
- **River and canal areas** are not imported: `water=river`, `canal`, `stream`,
  `ditch`, `drain` and `fish_pass` give no category (presets.lua classes them
  `never`), and `waterway=riverbank` is dropped. A point in a wide river gets the
  river only through its `waterway` line or relation under `layer=natural`,
  matched by bounding box, so a long river's relation can answer kilometres from
  its channel.
- **Unnamed water** is never imported.

### Our approach

- **A field of its own.** Where the dataset has a water dir, the answer carries
  `water: {name, class, type}` last; `display_name`, `address` and `places` are
  this geocoder's answer without water, unchanged, and the field is absent
  where no water holds the point. `class` and `type` are this dataset's
  feature labels. They generally follow Nominatim's categories, with extensions
  for features such as river areas and Marine Regions seas and oceans.
- **The rule.** Of the water areas whose rings hold the point (holes honoured: a
  point on an island mapped as a hole of its lake is not in the lake), the
  smallest OSM object answers, its area that of all its outer rings with their
  holes taken off (the water it holds), in square degrees (not the km² of the
  `major` tier's rule), then the higher `rank_search`, the lower type code and
  the smaller name. The order is the object's, so every polygon of a
  multipolygon, and every piece of one the builder cut, answers alike. Where
  continents overlap (Sinai, the Urals) and two loaded continents hold the
  point, the order runs over the water of both, the address's continent's
  winning a whole tie; the address is picked as without water.
- **Where it differs from `layer=natural`.** When a larger area of higher
  rank holds a smaller one of lower rank: a lake or reservoir (20) inside a
  wetland, glacier, bay or strait (22) answers the lake, where Nominatim gives
  the larger area. Of areas of one rank holding the point the smaller answers,
  where Nominatim's order (rank, then distance, 0 for each area holding the
  point, reverse.py:476-479) leaves them to database order. An area always beats
  no area: lines are never answers, and no bounding box stands in for a shape.
  Ties of one size and rank go to the lower type code, then the smaller name,
  not to database order.
- **Seas and oceans** are Marine Regions' IHO sea areas (EEZ∩IHO v5, CC BY 4.0),
  cut to OpenStreetMap's land, as `place/sea` and `place/ocean`. `major` and
  `all` keep that shore only within 37.5 m (Precision, below), so a point that
  close to it can fall on either side. A point at sea that no address reaches
  answers `{"address": {}, "water": {...}}`, where Nominatim answers
  `Unable to geocode`. Only a dataset whose region covers the open sea answers
  there: the planet's; a continent's dirs hold the coastal seas its region
  takes in. An area answers by the name its sea goes by where the IHO's reads
  awkwardly in a line (`marine::kShownNames`: "Mediterranean Sea" for either
  basin, "Barents Sea" for "Barentsz Sea", "Andaman Sea" for "Andaman or Burma
  Sea"); a name in dispute ("Japan Sea", "Persian Gulf") stays as the source
  has it.
- **River naming.** River and canal areas are answers, as `waterway/river` and
  `waterway/canal`, named by themselves or by a waterway; an unnamed one that no
  waterway names is left out. An unnamed one takes the waterway with the
  greatest total length inside its rings, counting only the portions of each
  segment inside the area, excluding holes, in whole millimetres. Ways belonging
  to a `type=waterway` relation count for that relation; other ways count for
  their local `name`, so one river under several `name:en` stays one. Its name
  follows the dataset's name rule, `name:en` else `name`: the relation's own
  `name:en`; else, where its river ways that carry a `name:en`
  hold more than half of its river length (whole millimetres, none counted
  across a missing node), the `name:en` of the most length among them, a tie
  going to the smaller name; else the relation's `name`. So neither one way's
  `name:en` nor a reach's local name renames the river. A river mapped only as
  a line has no area and gets no `water`. A name so taken is weak where the
  river's lines run under half the side of a square of the area's size inside
  it and the river itself, its relation or the lines of its name joined up, is
  under 64 such sides long, as a creek's line ends in the river it flows into
  (the East River's unnamed area takes "Newtown Creek"): the record says so,
  and the location line (below) doesn't name it. A river's own line through a
  piece of its delta or estuary, a long river beside the piece, is not weak.
- **Kinds out of scope**, as in Nominatim: unnamed water (but river and canal
  areas a waterway names, above), `water=stream|ditch|drain|fish_pass`, `water=*` without
  `natural=water`, `natural=coastline`. Left out for the Marine Regions areas:
  OSM's `place=sea` areas, and OSM bays and straits whose water is fully
  covered by Marine Regions areas of the same name.
- **Precision.** `water=all-exact` holds every OSM point of every shore. `major`
  and `all` are simplified within 37.5 m, so a point within that of a shore can
  fall either side of it.
- **The location line (`water.landmark`).** Nominatim has nothing like it. The
  answer's `water` carries `"landmark": true`, last and only when true, where a
  photo's location line would show the water in its landmark's place
  (`landmark = water.landmark ? water.name : address.landmark`): a kind people
  name (no basin, fishpond, lock or `water=*` value of its own), a name that is
  no catalogue number nor a weak river name, still water and wetlands of 1 ha
  or more, a name the line's city, state, country or landmark doesn't hold
  already, a landmark that gives way (none; a nature reserve, national park or
  protected area to a notable lake, glacier, coastal water or river smaller than
  it; a natural feature to smaller water; never a sea, ocean or wetland over a
  park), and no shore of the water within 40 m of the point, a cut between
  pieces of one water and a marine tile line with sea across it aside. It is
  decided in the multi-continent answer once the water and the address are
  final, the same on every tier; `display_name`, `address` and `places` don't
  change. README.md gives each rule.

The server's lookup is `server/src/water.rs`; the builder's kinds, ranks and
naming are `builder/src/water_classify.h`, `water_types.h` and `water_names.h`.

## Postcode handling in Nominatim (traced from source)

Nominatim has THREE sources for postcodes, combined into the `location_postcodes` table:

### Source 1: Postcode boundary polygons (`place_postcode` table)
- OSM `boundary=postal_code` relations with admin_level=11
- These have actual polygon geometry
- Stored with `osm_id IS NOT NULL` in location_postcodes
- **We use these at build time**: a street (way_postcodes.bin) takes the postcode of the postal area covering its middle, its edges included (`ST_Covers`), as `get_nearest_postcode()` below does, and a POI (`parent_postcode_id`) that of the smallest postal boundary of its country containing it. They are admin polygons of level 13, so the server also finds the ones holding the query point, for `get_nearest_postcode()` below. Each has the country its centroid lies in (`get_country_code`, as `update_postcodes` gives `place_postcode` its country), one for all the rings of a relation, at the whole multipolygon's centroid. Each takes the postcode its country's pattern makes of its tag (`m.norm`, the pattern's `output`): a US ZIP+4 its ZIP (`\1`), Latvia's 1050 LV-1050, the Netherlands' 1234AB 1234 AB. One of a postcode its country's pattern rejects carries place type 15 (`POSTAL_AREA_INVALID`): Nominatim makes no postcode area of it (`_update_postcode_areas`), and it names nothing. The patterns are `country_settings.yaml`'s, matched as `PostcodeFormatter.match` does (`match_postcode`, `postcode_validation.h`): the tag in upper case matches `\s*(?:CC[ -]?)?(pattern)\s*` whole, so a separator stands only where the pattern has one, and a match of zeros, hyphens and spaces is none; a country without a pattern takes `.*`. Saudi Arabia's `ddddd(?:-dddd)?` takes 12345-6789, not 12345-ABCD, Germany's `ddddd` DE 10115, not 10-115 nor D-E-10115. Past that a postcode of any length names it (Saudi Arabia's 12345-6789, Iran's 12345-67890), one value and no placeholder. One whose centroid lies in no country known names only a point of no country known: Nominatim makes no postcode area of a postal area of no country (`get_matcher(None)` is `None`), and takes one only of the point's country

### Source 2: addr:postcode tags on places (`placex` table)
- Any OSM object with an `address->'postcode'` hstore entry
- Nominatim collects `(country_code, postcode, centroid)` from ALL objects in placex that have addr:postcode
- These create **artificial postcode centroids** — computed as the average position of all objects sharing that postcode
- Each one covers a box around its centroid (`expand_by_meters(centroid, extent)`, postcodes.py), the `extent` of its country's postcode pattern in `country_settings.yaml`: US 25,000 m, GB and IM 700 m, NL 800 m, CH, EE and JP 3,000 m, GG, LU and PT 1,000 m, LI 4,000 m, RO 2,500 m, IE 50 m, Nigeria's lettered pattern 1 m, and 5,000 m elsewhere
- Stored with `osm_id IS NULL` in location_postcodes
- Only used for areas NOT already covered by postcode boundary polygons (line 342: `NOT EXISTS(SELECT * FROM _global_postcode_area ...)`)
- **We implement this**: the builder keeps each address point's addr:postcode (addr_postcodes.bin) as the postcode its country's pattern makes of it, and averages the address points of each (country, postcode) into postcode_centroids.bin, so that DE 10115 and 10115, or 1234AB and 1234 AB in the Netherlands, are one centroid, as `_PostcodeCollector` keys them by `m.norm`. A house sums by the postcode `update_postcodes` makes of its tag (`placex.address`), whatever the sanitizer keeps for the house: in a country without a pattern that is `PostcodeFormatter`'s default `.*`, not the sanitizer's `[A-Z0-9- ]{3,12}`, so Montserrat's 12.A makes a centroid, though the house tagged it keeps no postcode of its own

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

0. A house's own addr:postcode (addr_postcodes.bin), where the pattern of the house's own country takes it, as the postcode the pattern makes of it: the clean-postcodes sanitizer matches it there (`PostcodeFormatter.match`, after an optional country code; a US ZIP+4 is its ZIP, DE-10115 10115; a country without a pattern takes `[A-Z0-9- ]{3,12}`) and makes one that fails an address term, so the builder stores none and the house takes its street's
1. The street's postcode inherited from its postal boundary at build time (way_postcodes.bin), or a POI's (`parent_postcode_id`); for a US street that answers alone, the ZIP of a TIGER line of that street within ~100 m (interp_postcodes.bin)
2. `get_nearest_postcode()` for a point. The point is the street's centroid, the point halfway along it in plain degrees (`get_center_point`: `ST_LineInterpolatePoint(geometry, 0.5)`), for a street and a house of it, and the country the one there (`get_country_code` on the street's centroid; a house takes its street's), the smallest country polygon holding it, else the answer's; a street no postcode reaches has none, and neither has an answer that is that street. For a house whose street has none, the point is the house; for a POI, and where no street or house answers (admin mode), the query point. At that point:
   1. Of the postal areas holding it of its country (any where the point has none known) whose postcode the country has, the one whose centroid is nearest it in plain degrees
   2. Else, in the United States, the postcode grid's (postcode_grid.bin, `builder/src/postcode_grid_build.h`): the postcode most of the address points and the points halfway along the TIGER lines in the point's level-16 cell carry, as Nominatim's pattern takes their tags (`(ddddd)(?:-dddd)?`, the five digits), or, in a cell holding none, the nearer such cell's along the curve within the country's extent (25 km over the longest cell edge, 150 cells); none past that, and the centroids name the point. Nominatim has no such table: its `get_nearest_postcode()` takes the nearest centroid, whose ZIP area may end nearer the point than its own, where the houses round the point carry the postcode Nominatim answers with at zoom 18 (the house's or the TIGER line's it lands on). The grid stands in for it at the same point, after the postal areas, which `ORDER BY osm_id is null` puts before any centroid, and before the centroids. Only the US's: its postcodes are areas with next to no postal boundaries in OSM, where for other countries the centroids or the postal areas answer better
   3. Else the postcode centroid of the same country nearest it in plain degrees among those whose box holds it (postcode_centroids.bin). A centroid of no country stands for one of the point's, and where the point has no country every centroid counts, each with its own country's box. The box is `expand_by_meters`' (`geography_box.rs`): the envelope of the centroid's buffer on the geography, a diamond of the extent in the point's UTM zone or, beyond 70°, the polar Lambert azimuthal equal-area, as PostGIS's `_ST_BestSRID` picks; a degree of latitude there is a meridian degree, 110,574 m at the equator. A box across the antimeridian spans the longitudes between its corners the long way round, as `ST_Envelope` makes it, and holds no point on the antimeridian. The server looks for the boxes holding a point among the centroids within its country's extent of it (`BOX_REACH_FACTOR`), and among the centroids within that extent of the antimeridian at the point's latitude, at any distance, unless none of those can be as near as the best so far: those are the boxes that reach across it, which can hold a point however far west or east of the centroid

`address.postal_city`, which is ours, names the postcode of any of these where the dir's table has a row of it, but the nearest centroid's (2.3): a centroid's ZIP is the one whose centre is near the point, not the one it lies in.

Nominatim keeps only postal areas whose postcode matches the country's pattern (`_update_postcode_areas`); the builder marks the others (place type 15) and neither it nor the server takes a postcode from them. A postal area's centroid is that of the ring holding the point, where Nominatim's is the whole relation's. Nominatim's street also takes the postcode of the finest of its address areas that has one (`insert_addresslines`: an area's own postcode tag, or the one postcode whose centroid lies inside it) before `get_nearest_postcode()`; the polygons carry no postcode, so the server leaves that step out.
