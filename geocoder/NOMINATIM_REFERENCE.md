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

Our `admin_levels.json` matches Nominatim's `address-levels.json` exactly (verified programmatically). Countries with overrides: au, be, br, cz, de, es, id, jp, nl, no, ru, se, sk.

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
- SF (admin_level=6, `border_type=city`) → label "city" instead of "County" (rank 12)
- Manhattan (admin_level=7, `border_type=borough`) → label "borough" (TODO: verify this maps to suburb)

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

**We do NOT implement this.** Our smallest-area-wins logic handles the output field collision instead (if two polygons both map to "county", the smaller one wins). This might produce slightly different results than Nominatim in edge cases.

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

- At query time: for each output field (city, county, state, etc.), keep smallest-area polygon
- This is simpler but functionally similar — when two polygons map to the same field, the more specific (smaller) one wins
- We don't do rank adjustment at index time — edge cases where two levels map to the same rank range could produce different results than Nominatim

### Difference impact

For most cases, our smallest-area-wins approach produces the same result. Edge cases where Nominatim's rank adjustment matters:
- Two admin levels that both map to the same rank range (e.g., level 5 and 6 both → county in some countries)
- Nominatim bumps the inner one; we pick the smaller one. Result is usually the same entity.

## How our implementation maps to Nominatim

| Nominatim concept | Our implementation | Status |
|---|---|---|
| address-levels.json | admin_levels.json (exact match, verified programmatically) | ✅ Verified |
| get_label_tag() check 2-3 (place/linked_place extratags) | place_type_override field on AdminPolygon (from linked_place, border_type, place tags) | ✅ Implemented |
| get_label_tag() check 4 (ADMIN_LABELS rank//2) | rank_to_field() function | ✅ Implemented |
| get_label_tag() check 5 (category[1] for place=* entities) | place_type_override covers this — boundary=place entities get place_type set | ✅ Implemented |
| find_linked_place() node linking | Not implemented — we use tags on the boundary directly | ⚠️ Different mechanism, same result for tagged boundaries. Untagged boundaries that link to place nodes by name/wikidata won't get overrides. **Future fix**: at build time, match admin boundary wikidata tags against place node wikidata tags to set place_type_override. Name-based fallback is expensive (geometric containment test). |
| Rank adjustment for nested boundaries | Not implemented — we use smallest-area-wins per field | ⚠️ Different mechanism, similar result for most cases |
| boundary=place entities (e.g., Washington DC) | Extracted in builder with admin_level=15 + place_type_override | ✅ Implemented |
| Place node fallback (nearest city/town/village) | place_nodes.bin + find_places() at street cell level | ✅ Implemented |
| place_addressline pre-computed relationships | Not applicable — we do spatial lookup at query time | N/A — different architecture |
| Postcodes from addr:postcode | Not implemented — only boundary-based postcodes | ❌ Deferred — builder needs to extract addr:postcode from street/address nodes |
| border_type tag | Extracted but Nominatim doesn't actually use this tag | ⚠️ We use it as a fallback after linked_place; harmless but unnecessary |
| Wikidata-based place linking | Not implemented | ❌ Deferred — builder needs to match boundary wikidata tags against place node wikidata tags to set place_type_override. Name-based fallback requires geometric containment test. |
| Display name formatting | Ours is a clean formatted address; Nominatim dumps all address rows comma-separated | ℹ️ Ours is arguably better for end users. Nominatim's is more of a debug breadcrumb. Could add suburb fallback when city is missing. |
| Street/road resolution | Minor differences in closest street selection | ℹ️ Our point-to-segment distance vs Nominatim's pre-computed parent streets. Causes occasional one-off differences at intersections. |
| Suburb/neighbourhood rank adjustment | Not implemented — we use smallest-area-wins per field | ⚠️ Nominatim bumps child ranks to be > parent at index time. Could produce different suburb/neighbourhood assignments in edge cases with nested same-rank boundaries. |
| TIGER address interpolation | Not implemented | ❌ Deferred — US TIGER data encodes address ranges in `tiger:*` tags on road ways (e.g., `tiger:zip_left`, `tiger:zip_right`). Nominatim interpolates house numbers from these. Affects rural/suburban US where addr:housenumber tags are sparse. Builder would need to extract TIGER ranges and store as interpolation data. |
| Postal city names | Not implemented | ❌ Deferred — in the US, USPS assigns city names to ZIP codes that may differ from the actual municipal jurisdiction (e.g., "Cumming, GA" for unincorporated Forsyth County). Neither we nor Nominatim handle this — would require external USPS data or addr:city extraction. |
