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
| `src/nominatim_api/reverse.py` | `_find_closest_street_or_pois()` — the closest street, address point or rank-30 POI |
| `lib-lua/themes/nominatim/presets.lua`, `init.lua` | The import's main tags (`MAIN_TAGS_POIS`, `MAIN_TAGS.natural`) and how an object's main tags give its class and categories (`process_tags`) |

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

1. **Label role member** — relation has a member node with role "label" and class "place", of any place type: the steps after never run for it, so a boundary labelled by a place=locality node links no node of its name (`link_places_by_name` passes over every boundary the label or wikidata step linked)
2. **Wikidata match** — boundary and node share the same `wikidata` extratag, the boundary covers the node, and no boundary indexed before it linked the node (`linked_place_id is null`); of several, the first `ORDER BY lower(name->'name') = bnd_name desc` finds (a node of the boundary's name before others; a null comparison, either name missing, first of all). Nominatim indexes the boundaries by rank, then the place areas by address rank: the builder gives each boundary its turn in that order (`link_wikidata_places`: lowest admin level first, place areas after every boundary by the address rank of their own place tag in their country, equals by id; a boundary or place area of rank 0 in its country, found at a point inside it before the arcs, takes no turn and reserves no node, nor its label member, which the label claims of the others keep out of the step; off every country's outline the point takes the country whose boundary cells hold its cell, as the country cells give it later), the nodes of a name match by their address rank (the order leaves them unordered: Espanola's town before its hamlet), then node id, and a boundary or place area that links none keeps its own place tag for the steps after (Lent's place=quarter area, whose village node of the same wikidata its wijk links first, stays a quarter)
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

A boundary=administrative relation of rank > 0 whose closest boundary relation round its centroid, of a lower admin level above 3 and of rank 1 to 25, has its wikidata and an equal geometry (`is_same`, :909-931) ranks 0 and is in no address; one that links a place node takes the node's rank back where that is above 3 and below 26 (:1161), `parent_address_level` staying 3, whatever its parent's rank. The builder decides it (`take_out_repeated_boundaries`): the parent is the relation of the highest level, then the smallest, of those covering a point inside the boundary's largest ring, the levels below settled first, and the rings are equal where their outlines are, from any vertex on, either way round, whatever vertices either adds along its straight edges. Every ring of one taken out carries place type 15 (`BOUNDARY_REPEATS_PARENT`), which the server ranks in no address: Warsaw's county and gmina along its city's ways, and Arnhem's woonplaats along its gemeente's, which links the city node. Every ring of one that links a node keeps the node's type and has `own_place_type` 15, which `promote_nested_ranks` ranks as the node, past the three steps: a town boundary along a gmina tagged `place=neighbourhood` (rank 24) that links a city node is a city of rank 16, where the admin-nested step would give it 25.

A place area (class='place': `boundary=place`, a place multipolygon or closed way, a `boundary=historic` or `ceremonial` with a place tag, such as County Antrim or Essex) takes none of the three steps: one of rank 16 to 23 is lifted to an area's rank + 2 where a boundary or a place area whose own rank is the place area's or more, up to 23, contains it (:930-950), judged as in step 2 by the area being larger and containing the place area; a place=village area inside a city of rank 16 ranks 18. A place area that links a place node of another type (its label member or its wikidata's) is labelled by the node's type, and ranks by its own `place` tag where that ranks 16 to 23 (the place-area branch leaves `parent_address_level` unset, so the link's rank never applies, :930-950 and :1129), else by the node's where that is above 3 and below 26: Weesp (R15419242, `boundary=place`, `place=suburb`, the town node as its label) is a town of rank 20. The builder keeps such an area's own place tag in its record's last byte (`own_place_type`) beside the node's type (`place_type_override`), and `place_area_ranks` ranks it; admin-minimal keeps it by the type that labels it. A boundary whose level ranks 0 in its country stays out of the address whatever its place type (Cairns Regional, Regierungsbezirk Köln). Areas of rank 25 are in no street's or house's address (getNearFeatures takes ranks below 25), so they fill no field and take no rank from place nodes.

What the polygons at a point cannot tell, and Nominatim decides on the whole geometry at index time:

- The admin-nested parent is the boundary covering the child's centroid, which need not hold the query point (Bugisa sub-region round Mbale City ranks 12 in Nominatim, 18 here).
- A place area is lifted where another area contains it whole (`ST_Contains`). Here the other is larger and contains it (`polygon_within`): every vertex of the polygon is inside the other or on its edges, none of its edges crosses one of the other's, no point of the other's edges lies inside it off its edges (of a hole of the other inside it, or a notch of the other that one of its edges spans; each edge of the other is cut at the polygon's vertices on it and tested at the middle of each piece), and its point on surface is inside the other off its edges (an enclave filling a hole of the other is not contained), the point tested exactly, on its scan line at the middle of its run, a fraction of the e7 grid, so that it stays inside however narrow the polygon. Areas nested along shared ways meet on their edges alone.
- A linked place or the boundary's own place tag can lower the rank; the builder keeps one place type per polygon, from either.
- Every place type ranks a boundary (place=locality makes Northern Ireland's townlands rank 25); the builder keeps 14 types.
- Levels 11 and 12 (level 11 holds the postal areas here), and administrative ways only at levels 8 to 12.

Nominatim ranks every boundary of a rank above 0 holding the point (getNearFeatures, :1273). Here the smallest of each admin_level is ranked, and of levels 3 to 10 every larger one that the smallest lies within (`polygon_within`): Bruxelles - Brussel (level 10, 18 in Belgium) round Quays Quarter (level 10, a neighbourhood) is the city district, its parent the city, as the level 9 boundary repeating the city is in no address. The address has a row a rank (`address_havelevel`): of the boundaries of one rank the smallest alone fills a field or joins display_name (`one_boundary_a_rank`), so a level 8 boundary tagged place=town round a level 8 city, both of rank 16, names nothing. One beside the smallest is left out, as the simplified presets' neighbours overlap along their shared borders, and of level 2 the smallest alone names the country. One rule waits for those: a boundary labelled city, town or village holds rank 16 from place nodes whatever its own rank, standing in for the linked place's rank that Nominatim gives it even below its own (the parish of Winsford ranks 16, its admin_level 20): holding only its own rank lets villages such as Argir in Tórshavn into the address.

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
| address-levels.json | admin_levels.json (exact match, verified programmatically) | ✅ Verified |
| get_label_tag() check 2-3 (place/linked_place extratags) | place_type_override field on AdminPolygon (from the linked place node, else the boundary's linked_place / place tags) | ✅ Implemented |
| get_label_tag() check 4 (ADMIN_LABELS rank//2) | rank_to_field() function | ✅ Implemented |
| get_label_tag() check 5 (category[1] for place=* entities) | place_type_override covers this — boundary=place entities get place_type set | ✅ Implemented |
| find_linked_place() node linking | Builder: label role member, wikidata match, a node of the boundary's own place type whose name holds the boundary's or is held in it, of any rank and at any distance, then name match (`link_places_by_name`, of a node the boundary covers, `ST_Covers`), in Nominatim's order, the boundary's own place tag where none links; one node a boundary, the best of its parts' (of the boundary's own name first, then the nearest to a part's vertex average), which labels every part; a boundary or place area of rank 0 in its country links nothing (find_linked_place returns at once); a linked place node is hidden from place results, except in admin-minimal when that set leaves its boundary out (L9-L11, place areas below rank 16): nothing there stands for the node | ✅ Implemented |
| Rank adjustment for nested boundaries | At query time on the polygons containing the point, then smallest-area-wins per field | ✅ Implemented — containment judged by admin level, area and the rings' containment test; what that leaves out is listed under Step 4 |
| boundary=place entities (e.g., Washington DC) | Extracted in builder with admin_level=15 + place_type_override | ✅ Implemented |
| Place node fallback (nearest city/town/village) | place_nodes.bin + find_places() with wide search; a field a boundary and a place node both fill names the one of the finer rank, the boundary at an equal rank (`label_field`: the address keeps the first row of a label, its rows the finest first, and at one rank the boundary's, isguess asc), and display_name lists the boundary that lost it: the place=suburb node St Crispin's (22) inside the parish of Upton (20) is the suburb. A place node is an address row only inside the walk's current_boundary, the last row insert_addresslines took that is no place node, a place area as much as a boundary (:583-586, :622-632): the node Al Khalidiyah outside the place=suburb way Al Bateen round the point is in no address there. It ranks two up inside a relation of its rank alone, a boundary of that rank_address or a place area of that place rank, not inside a closed way's area (osm_type = 'R', :984-1004): the place=suburb node Ramat HaNasi in the place=suburb way בת ים stays at 20, where the way's row comes first | ✅ Implemented — place nodes are indexed at the admin cell level (L10); the query cell and its 8 neighbours cover ~30 km |
| place_addressline pre-computed relationships | Not applicable — we do spatial lookup at query time | N/A — different architecture |
| Postcodes from addr:postcode | addr_postcodes.bin (an address point's own addr:postcode) and postcode_centroids.bin (centroids of every addr:postcode, plus external ones) | ✅ Implemented — see "Postcode handling" section below |
| border_type tag | Not used: Nominatim never reads this tag | ✅ Ignored, as in Nominatim |
| Wikidata-based place linking | Part of find_linked_place() above: a boundary's wikidata matched against the wikidata of the place nodes it holds (`_st_covers`) | ✅ Implemented |
| `licence` in every result (`format_base_json`) | `licence` first in every answer, `OSM_ATTRIBUTION`'s words (`Address::licence`); `{}` where no loaded region covers the point, as Nominatim's error carries none | ✅ Implemented |
| The rank-30 POI lane of `_find_closest_street_or_pois` | `find_nearest_poi_with_parent()`: water kinds never in it, water with a POI main tag classified by that tag (The primary POI, above) | ✅ Implemented — the area-label lane is a divergence |
| Display name formatting (`display_name` in results.py) | `format_address`: the landmark, the house number with the road, the place parts finest first by the rank_address of the polygon or place node behind each field, with the polygons that lost their field to one of another rank or to a place node, then the postcode (rank 5) and the country (4); a part is left out only where it repeats the part just before it, and every answer with a part has one | ✅ Implemented — the house number joined to the road as the country writes it |
| Street/road resolution | Minor differences in closest street selection | ℹ️ Our point-to-segment distance vs Nominatim's pre-computed parent streets. Causes occasional one-off differences at intersections. |
| How far the closest street or address is looked for (`lookup_street_poi` runs `_find_closest_street_or_poi` within 0.006°) | `DEFAULT_SEARCH_DISTANCE`, 130 m: how far the street and address lookups reach, wherever the point lies in its street cell, and none farther: the nearest street way (measured to its segments) and address point that compete to be the answer's feature, and the interpolation lines that give a street that wins its house number | ℹ️ Divergence — Nominatim reaches 0.006° (about 660 m), but a road that far off is no longer the road the point is on: no street way past 130 m names the road. An address or POI that wins still gives the road it names, and a street that wins still takes its house number from within 100 m, as `_find_housenumber_for_street` does within 0.001° |
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
- **We use these at build time**: a street (way_postcodes.bin) takes the postcode of the postal area covering its middle, its edges included (`ST_Covers`), as `get_nearest_postcode()` below does, and a POI (`parent_postcode_id`) that of the smallest postal boundary of its country containing it. They are admin polygons of level 11, so the server also finds the ones holding the query point, for `get_nearest_postcode()` below. Each has the country its centroid lies in (`get_country_code`, as `update_postcodes` gives `place_postcode` its country), one for all the rings of a relation, at the whole multipolygon's centroid. Each takes the postcode its country's pattern makes of its tag (`m.norm`, the pattern's `output`): a US ZIP+4 its ZIP (`\1`), Latvia's 1050 LV-1050, the Netherlands' 1234AB 1234 AB. One of a postcode its country's pattern rejects carries place type 15 (`POSTAL_AREA_INVALID`): Nominatim makes no postcode area of it (`_update_postcode_areas`), and it names nothing. The patterns are `country_settings.yaml`'s, matched as `PostcodeFormatter.match` does (`match_postcode`, `postcode_validation.h`): the tag in upper case matches `\s*(?:CC[ -]?)?(pattern)\s*` whole, so a separator stands only where the pattern has one, and a match of zeros, hyphens and spaces is none; a country without a pattern takes `.*`. Saudi Arabia's `ddddd(?:-dddd)?` takes 12345-6789, not 12345-ABCD, Germany's `ddddd` DE 10115, not 10-115 nor D-E-10115. Past that a postcode of any length names it (Saudi Arabia's 12345-6789, Iran's 12345-67890), one value and no placeholder. One whose centroid lies in no country known names only a point of no country known: Nominatim makes no postcode area of a postal area of no country (`get_matcher(None)` is `None`), and takes one only of the point's country

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

Nominatim keeps only postal areas whose postcode matches the country's pattern (`_update_postcode_areas`); the builder marks the others (place type 15) and neither it nor the server takes a postcode from them. A postal area's centroid is that of the ring holding the point, where Nominatim's is the whole relation's. Nominatim's street also takes the postcode of the finest of its address areas that has one (`insert_addresslines`: an area's own postcode tag, or the one postcode whose centroid lies inside it) before `get_nearest_postcode()`; the polygons carry no postcode, so the server leaves that step out.
