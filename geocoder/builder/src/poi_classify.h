#pragma once
// POI classification: maps raw OSM tag values to (category, tier, flags).
// Extracted verbatim from build_index.cpp so the mapping — which encodes
// hundreds of Nominatim-parity decisions and evolves frequently — can be
// unit-tested (tests/test_poi_classify.cpp) without a full build+diff cycle.
#include <algorithm>
#include <cstring>
#include <initializer_list>
#include <optional>
#include "geometry.h"
#include "types.h"

// The name a POI node goes by: name:en, else name. An empty value names
// nothing, and a node without a name makes no POI: an unnamed POI could only
// surface its parent street as the road, which drifts the answer off the
// nearest street (San Francisco 37.7749,-122.4194: a waste basket at 0 m
// turned Market Street into South Van Ness).
inline const char* poi_name(const char* name_en, const char* name) {
    if (name_en && name_en[0]) return name_en;

    return name && name[0] ? name : nullptr;
}

// Whether `v` is one of `values`.
inline bool poi_tag_is(const char* v, std::initializer_list<const char*> values) {
    for (const char* x : values)
        if (std::strcmp(v, x) == 0) return true;
    return false;
}

// Whether leisure=`v` is a POI main tag of rank 30 in Nominatim
// (presets.lua:221-226): park, nature_reserve, garden and common rank 24 and
// 25 (address-levels.json:63-68), and the import drops leisure=no.
inline bool poi_leisure_ranks_30(const char* v) {
    return !poi_tag_is(v, {"no", "park", "nature_reserve", "garden", "common"});
}

// Whether natural=`v` ranks 30 in Nominatim (address-levels.json:69-82):
// tree, spring, cave_entrance, geyser, hot_spring, rock and stone; its other
// values rank 22 or lower.
inline bool poi_natural_ranks_30(const char* v) {
    return v && poi_tag_is(v, {"tree", "spring", "cave_entrance", "geyser", "hot_spring", "rock", "stone"});
}

// Whether man_made=`v` is a POI main tag in Nominatim (presets.lua:228-237);
// the other values are none.
inline bool poi_man_made_is_main(const char* v) {
    return poi_tag_is(v, {"pier", "tower", "bridge", "works", "water_tower", "dyke", "adit", "lighthouse", "watermill",
                          "tunnel"});
}

// Whether an object carries a main tag that makes it a rank-30 POI in
// Nominatim: a key of its import's POI main tags (lib-lua/themes/nominatim/
// presets.lua MAIN_TAGS_POIS, which the full style takes) with a value the
// import keeps, and a category settings/address-levels.json ranks 30, as it
// does every key it doesn't list. Nominatim ranks an object by the lowest
// positive address rank of its main tags' categories (lib-sql/functions/
// ranking.sql compute_place_rank), and a water kind's is 0, so beside such a
// tag the object is a rank-30 POI unless a main tag of a lower positive rank
// is there too (has_lower_rank_main_tag). Of those keys this reads the ones
// the classifier takes; building=*, a fallback tag, counts only beside
// natural=* (classify_poi).
inline bool has_poi_main_tag(const char* t_amenity, const char* t_leisure, const char* t_emergency,
                             const char* t_tourism, const char* t_historic, const char* t_shop,
                             const char* t_craft, const char* t_office, const char* t_aeroway,
                             const char* t_railway, const char* t_man_made) {
    // presets.lua:178-183; the values the import drops.
    if (t_amenity && !poi_tag_is(t_amenity, {"no", "parking_space", "parking_entrance", "waste_disposal", "hunting_stand"}))
        return true;
    if (t_leisure && poi_leisure_ranks_30(t_leisure)) return true;
    // presets.lua:192-195.
    if (t_emergency && !poi_tag_is(t_emergency, {"no", "yes", "fire_hydrant"})) return true;
    // presets.lua:255-259 (tourism=attraction, a fallback tag, ranks 30 too).
    if (t_tourism && !poi_tag_is(t_tourism, {"no", "yes"})) return true;
    // presets.lua:211-213, a fallback tag; historic=neighbourhood ranks 22
    // (address-levels.json:130-132).
    if (t_historic && !poi_tag_is(t_historic, {"no", "yes", "neighbourhood"})) return true;
    // presets.lua:253-254, 190-191, 241-242, 176-177.
    for (const char* v : {t_shop, t_craft, t_office, t_aeroway})
        if (v && std::strcmp(v, "no") != 0) return true;
    // presets.lua:243-252, of a named object (every POI is).
    if (t_railway && !poi_tag_is(t_railway, {"rail", "no", "abandoned", "disused", "razed", "level_crossing", "switch",
                                             "signal", "buffer_stop"}))
        return true;
    return t_man_made && poi_man_made_is_main(t_man_made);
}

// Whether highway=`v` is a kind of address rank 26 or 27, a street's
// (settings/address-levels.json:101-126), on an area: the street lane takes
// address ranks 26 to 29 (reverse.py _find_closest_street_or_pois).
inline bool poi_highway_ranks_street(const char* v) {
    return v && poi_tag_is(v, {"service", "cycleway", "path", "footway", "steps", "bridleway", "motorway_link",
                               "primary_link", "trunk_link", "secondary_link", "tertiary_link", "residential", "track",
                               "unclassified", "tertiary", "secondary", "primary", "living_street", "trunk", "motorway",
                               "pedestrian", "road", "construction"});
}

// Whether an object carries a main tag of a positive address rank below 30
// (settings/address-levels.json, as of Nominatim's 5348a803), which then
// ranks it whatever POI main tag it carries too (ranking.sql
// compute_place_rank takes the lowest positive address rank): place=* of an
// address rank (:3-38, place=subdistrict 14 and place=plot 25 among them;
// every place value is a main tag, plot a named one, presets.lua
// MAIN_TAGS.all_boundaries, and square a street's, MAIN_TAGS_STREETS) and, on
// an `area` (a polygon, not a node), landuse=residential and the other landuse
// kinds of an address rank (:53-62; a landuse node ranks 30,
// ranking.sql:178-180) and the highway kinds of rank 26 and 27
// (poi_highway_ranks_street; a highway node ranks 30, ranking.sql:175-177).
// boundary=administrative ranks by an admin_level the classifier doesn't
// read, and the country tables (place=county in ca and de, ...) by a
// country: both are left out.
inline bool has_lower_rank_main_tag(const char* t_place, const char* t_landuse, const char* t_highway, bool area) {
    if (t_place && poi_tag_is(t_place, {"county", "district", "municipality", "city", "town", "borough", "village",
                                        "suburb", "hamlet", "croft", "subdivision", "subdistrict", "allotments",
                                        "neighbourhood", "quarter", "isolated_dwelling", "farm", "city_block",
                                        "mountain_pass", "locality", "plot", "square"}))
        return true;
    if (!area) return false;
    if (t_landuse && poi_tag_is(t_landuse, {"residential", "farm", "farmyard", "industrial", "commercial", "allotments",
                                            "retail"}))
        return true;
    return poi_highway_ranks_street(t_highway);
}

// Whether Nominatim's POI lane leaves out, by its class, a reservoir or basin
// of these tags that ranks 30. landuse=* is a fallback main tag (presets.lua
// MAIN_TAGS.all_boundaries), so the object's class is that of its
// alphabetically first main tag that is no fallback tag, else that of its
// first fallback tag in key order (init.lua process_tags:775-835), and the
// lane takes no class building or place (reverse.py:192-195): place=* is the
// class where no main tag of a key before it is there, and building=* where
// the object has fallback tags alone and no boundary=protected_area, the one
// fallback key before it. Of the main tags this reads the keys the
// classifier takes, without the values the import drops.
inline bool reservoir_leaves_poi_lane(const char* t_aeroway, const char* t_amenity, const char* t_boundary,
                                      const char* t_building, const char* t_craft, const char* t_emergency,
                                      const char* t_highway, const char* t_leisure, const char* t_man_made,
                                      const char* t_natural, const char* t_office, const char* t_place,
                                      const char* t_railway, const char* t_shop, const char* t_tourism,
                                      const char* t_waterway, bool area) {
    auto main = [](const char* v, std::initializer_list<const char*> dropped) { return v && !poi_tag_is(v, dropped); };
    // presets.lua MAIN_TAGS_POIS, MAIN_TAGS.natural and all_boundaries
    // (boundary=* of a name is one on an area alone, named_area).
    const bool before_place =
        main(t_aeroway, {"no"}) ||
        main(t_amenity, {"no", "parking_space", "parking_entrance", "waste_disposal", "hunting_stand"}) ||
        (area && main(t_boundary, {"protected_area", "place", "land_area", "postal_code"})) || main(t_craft, {"no"}) ||
        main(t_emergency, {"no", "yes", "fire_hydrant"}) ||
        main(t_highway, {"no", "turning_circle", "mini_roundabout", "noexit", "crossing", "give_way", "stop",
                         "turning_loop", "passing_place"}) ||
        main(t_leisure, {"no"}) || (t_man_made && poi_man_made_is_main(t_man_made)) ||
        main(t_natural, {"yes", "no", "coastline", "saddle"}) || main(t_office, {"no"});
    if (before_place) return false;
    if (t_place) return true;
    const bool after_place = main(t_railway, {"rail", "no", "abandoned", "disused", "razed", "level_crossing", "switch",
                                              "signal", "buffer_stop"}) ||
                             main(t_shop, {"no"}) || main(t_tourism, {"attraction", "no", "yes"}) ||
                             main(t_waterway, {"riverbank"});
    if (after_place || (t_boundary && std::strcmp(t_boundary, "protected_area") == 0)) return false;
    return t_building && std::strcmp(t_building, "no") != 0;
}

// The water kind of an object, the category its tags give it as water, or
// UNKNOWN for none: landuse=reservoir or basin, which the classifier reads
// before natural=*, else natural=water, wetland, bay or glacier.
inline PoiCategory poi_water_kind(const char* t_natural, const char* t_landuse) {
    if (t_landuse && poi_tag_is(t_landuse, {"reservoir", "basin"})) return PoiCategory::RESERVOIR;
    if (!t_natural) return PoiCategory::UNKNOWN;
    if (std::strcmp(t_natural, "water") == 0) return PoiCategory::NATURAL_WATER;
    if (std::strcmp(t_natural, "wetland") == 0) return PoiCategory::WETLAND;
    if (std::strcmp(t_natural, "bay") == 0) return PoiCategory::BAY;
    if (std::strcmp(t_natural, "glacier") == 0) return PoiCategory::GLACIER;
    return PoiCategory::UNKNOWN;
}

// Whether an area of these tags is a water street: water (poi_water_kind) a
// highway kind of rank 26 or 27 ranks (poi_highway_ranks_street; a footway
// of footway=sidewalk, crossing, link or traffic_aisle is no main tag,
// is_excluded_footway), which no place or landuse kind of a rank below 26
// ranks lower (has_lower_rank_main_tag; place=square ranks 26). Nominatim
// imports a named one as one placex row of class highway, of rank_address
// 26 or 27 (init.lua process_tags, ranking.sql compute_place_rank): a
// street, which its street lane finds by the distance to its geometry and
// answers by its own name (reverse.py lookup_street_poi), which has no
// parent street (placex_update parents rank_search > 27 alone), and which is
// no address point, whatever addr:* tags it carries (IsAddressPoint takes
// rank_address 30 alone). classify_poi gives it the highway flag, by which
// the server answers it as a street, and the build writes no address point
// of it.
inline bool poi_water_street(const char* t_natural, const char* t_landuse, const char* t_highway,
                             const char* t_footway, const char* t_place) {
    if (poi_water_kind(t_natural, t_landuse) == PoiCategory::UNKNOWN || !poi_highway_ranks_street(t_highway))
        return false;
    if (std::strcmp(t_highway, "footway") == 0 && is_excluded_footway(t_footway)) return false;
    const char* place_below_streets = t_place && std::strcmp(t_place, "square") == 0 ? nullptr : t_place;
    return !has_lower_rank_main_tag(place_below_streets, t_landuse, nullptr, /*area=*/true);
}

// POI classification: maps a node/way's raw OSM tag values to a
// (category, tier, flags) triple (PoiClassification, defined in types.h),
// or nullopt when nothing matches or no POI dir ships the tier (a generic
// rank-30 feature without a wikipedia or wikidata tag). Pure function of its
// tag arguments (no shared state) — lifted out of main()'s node/way passes,
// which call it identically.
//
// The water kinds (poi_water_kind) rank below 30 in Nominatim
// (address-levels.json:69-82 and 53-62), whose POI lane takes rank 30 alone;
// the server keeps them out of the primary slot. An object that also carries
// a POI main tag (has_poi_main_tag) is a rank-30 POI there, unless a main tag
// of a lower positive rank ranks it (has_lower_rank_main_tag): it takes no
// water category, and is classified by its rank-30 tags, or as a generic
// rank-30 feature where they have no category of their own. Where no POI dir
// ships that, it stays water, and where its water kind ships in a lower tier
// than its category, its flags carry the kind for the tiers in between
// (POI_FLAG_WATER_MASK). A reservoir or basin beside a natural tag of rank
// 30 alone (poi_natural_ranks_30) is a rank-30 POI of it too. An object a
// main tag of a lower positive rank ranks is the water it is, whatever its
// other tags, and so is a reservoir whose class the POI lane takes none of
// (reservoir_leaves_poi_lane); such a reservoir point, which the server
// would let take the primary slot as it does any RESERVOIR point, never
// does (POI_FLAG_PLACES_ONLY).
inline std::optional<PoiClassification> classify_poi(
    const char* t_tourism, const char* t_historic, const char* t_boundary,
    const char* t_amenity, const char* t_leisure, const char* t_natural,
    const char* t_railway, const char* t_aeroway, const char* t_man_made,
    const char* t_building, const char* t_craft, const char* t_power,
    const char* t_place, const char* t_waterway, const char* t_office,
    const char* t_wikipedia, const char* t_wikidata, const char* t_highway,
    const char* t_shop, const char* t_landuse, const char* t_emergency, const char* t_footway, bool area) {
    PoiCategory cat = PoiCategory::UNKNOWN;

    // The water rule runs before any category. A water kind is the object's
    // category where a main tag of a lower positive rank ranks it, whatever
    // its other tags, and yields where a POI main tag makes it a rank-30 POI
    // (building=* is a fallback main tag, presets.lua:184-185: beside
    // natural=* the object keeps class natural and ranks 30; beside
    // landuse=*, also a fallback tag, building is its class, the
    // alphabetically first, init.lua process_tags, and Nominatim's POI lane
    // takes no class building, reverse.py:192-195). A kind that yields leaves
    // the category to the object's rank-30 tags, so that the tags of a lower
    // rank or of no main tag in Nominatim give none: boundary=* (25, 0),
    // landuse=* on an area (24, 0; a landuse node ranks 30), leisure=park,
    // natural=wood, place=island, waterway=waterfall, man_made=dam and
    // power=*, nor shop=no, which the import drops, and which the shop branch
    // would take for a shop. `yielded` is the kind that yielded. A reservoir
    // beside a natural tag of rank 30 alone yields too: natural=* is no
    // fallback tag, so it is the class, and 30 its rank, the lowest positive
    // one. A reservoir of the class building or place, which the lane leaves
    // out, stays one, whether a POI main tag is beside it or not.
    // A water object a highway kind of rank 26 or 27 ranks is a street there
    // (the street lane takes address ranks 26 to 29, reverse.py:187-189): on
    // an area it takes the highway flag below (poi_water_street), unless a
    // place or landuse kind of a rank below 26 ranks it, every one but
    // place=square (26), which makes it no street either. Such a street
    // ships in its kind's tier, or in that of the category the classifier's
    // branches give its tags where that is lower: a lake that is an
    // attraction, as an attraction does, in poi/notable, where the server
    // answers its own name inside it.
    const PoiCategory kind = poi_water_kind(t_natural, t_landuse);
    const bool water_street = area && poi_water_street(t_natural, t_landuse, t_highway, t_footway, t_place);
    // highway=footway of footway=sidewalk, crossing, link or traffic_aisle is
    // no main tag in Nominatim (is_excluded_footway): beside water it neither
    // ranks the object nor makes it a street.
    if (kind != PoiCategory::UNKNOWN && t_highway && std::strcmp(t_highway, "footway") == 0 &&
        is_excluded_footway(t_footway))
        t_highway = nullptr;
    const bool ranks_30 = !has_lower_rank_main_tag(t_place, t_landuse, t_highway, area);
    const char* t_place_below_streets = t_place && std::strcmp(t_place, "square") == 0 ? nullptr : t_place;
    const bool no_street =
        kind != PoiCategory::UNKNOWN && has_lower_rank_main_tag(t_place_below_streets, t_landuse, nullptr, area);
    const bool poi_main_tag = has_poi_main_tag(t_amenity, t_leisure, t_emergency, t_tourism, t_historic, t_shop, t_craft,
                                               t_office, t_aeroway, t_railway, t_man_made);
    PoiCategory yielded = PoiCategory::UNKNOWN;
    // The kind that stays the object's category, once the branches below
    // gave its tags theirs.
    PoiCategory kept = PoiCategory::UNKNOWN;
    if (kind != PoiCategory::UNKNOWN &&
        (!ranks_30 || (kind == PoiCategory::RESERVOIR &&
                       reservoir_leaves_poi_lane(t_aeroway, t_amenity, t_boundary, t_building, t_craft, t_emergency,
                                                 t_highway, t_leisure, t_man_made, t_natural, t_office, t_place,
                                                 t_railway, t_shop, t_tourism, t_waterway, area)))) {
        kept = kind;
    } else if (kind != PoiCategory::UNKNOWN &&
               (poi_main_tag || poi_natural_ranks_30(t_natural) ||
                (kind != PoiCategory::RESERVOIR && t_building && std::strcmp(t_building, "no") != 0))) {
        yielded = kind;
        t_boundary = nullptr;
        if (area || kind == PoiCategory::RESERVOIR) t_landuse = nullptr;
        if (t_leisure && !poi_leisure_ranks_30(t_leisure)) t_leisure = nullptr;
        if (t_natural && !poi_natural_ranks_30(t_natural)) t_natural = nullptr;
        if (t_man_made && !poi_man_made_is_main(t_man_made)) t_man_made = nullptr;
        if (t_shop && std::strcmp(t_shop, "no") == 0) t_shop = nullptr;
        t_power = t_place = t_waterway = nullptr;
    }

    // tourism (highest priority)
    if (cat == PoiCategory::UNKNOWN && t_tourism) {
        if (std::strcmp(t_tourism, "museum") == 0) cat = PoiCategory::MUSEUM;
        else if (std::strcmp(t_tourism, "attraction") == 0) cat = PoiCategory::ATTRACTION;
        else if (std::strcmp(t_tourism, "viewpoint") == 0) cat = PoiCategory::VIEWPOINT;
        else if (std::strcmp(t_tourism, "theme_park") == 0) cat = PoiCategory::THEME_PARK;
        else if (std::strcmp(t_tourism, "zoo") == 0) cat = PoiCategory::ZOO;
        else if (std::strcmp(t_tourism, "gallery") == 0) cat = PoiCategory::GALLERY;
        else if (std::strcmp(t_tourism, "artwork") == 0) cat = PoiCategory::ARTWORK;
        else if (std::strcmp(t_tourism, "alpine_hut") == 0) cat = PoiCategory::ALPINE_HUT;
        else if (std::strcmp(t_tourism, "aquarium") == 0) cat = PoiCategory::AQUARIUM;
        else if (std::strcmp(t_tourism, "camp_site") == 0) cat = PoiCategory::CAMP_SITE;
        else if (std::strcmp(t_tourism, "picnic_site") == 0) cat = PoiCategory::PICNIC_SITE;
        else if (std::strcmp(t_tourism, "resort") == 0) cat = PoiCategory::RESORT;
        // Lodging — all variants fold into HOTEL so a hostel / apartment
        // / guesthouse shows as "hotel" class rather than "feature".
        else if (std::strcmp(t_tourism, "hotel") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "motel") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "guest_house") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "hostel") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "apartment") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "chalet") == 0) cat = PoiCategory::HOTEL;
        else if (std::strcmp(t_tourism, "caravan_site") == 0) cat = PoiCategory::CAMP_SITE;
        else if (std::strcmp(t_tourism, "wilderness_hut") == 0) cat = PoiCategory::ALPINE_HUT;
        // Information / visitor-services
        else if (std::strcmp(t_tourism, "information") == 0) cat = PoiCategory::INFORMATION;
    }
    // historic
    if (cat == PoiCategory::UNKNOWN && t_historic) {
        if (std::strcmp(t_historic, "castle") == 0) cat = PoiCategory::CASTLE;
        else if (std::strcmp(t_historic, "monument") == 0) cat = PoiCategory::MONUMENT;
        else if (std::strcmp(t_historic, "ruins") == 0) cat = PoiCategory::RUINS;
        else if (std::strcmp(t_historic, "archaeological_site") == 0) cat = PoiCategory::ARCHAEOLOGICAL_SITE;
        else if (std::strcmp(t_historic, "memorial") == 0) cat = PoiCategory::MEMORIAL;
        else if (std::strcmp(t_historic, "battlefield") == 0) cat = PoiCategory::BATTLEFIELD;
        else if (std::strcmp(t_historic, "fort") == 0) cat = PoiCategory::FORT;
        else if (std::strcmp(t_historic, "ship") == 0) cat = PoiCategory::SHIP;
        else if (std::strcmp(t_historic, "wayside_cross") == 0) cat = PoiCategory::WAYSIDE_CROSS;
        else if (std::strcmp(t_historic, "wayside_shrine") == 0) cat = PoiCategory::WAYSIDE_SHRINE;
        else if (std::strcmp(t_historic, "city_gate") == 0) cat = PoiCategory::CITY_GATE;
        else if (std::strcmp(t_historic, "citywalls") == 0) cat = PoiCategory::CITYWALLS;
        else if (std::strcmp(t_historic, "boundary_stone") == 0) cat = PoiCategory::BOUNDARY_STONE;
        else if (std::strcmp(t_historic, "milestone") == 0) cat = PoiCategory::MILESTONE;
        else if (std::strcmp(t_historic, "mine") == 0) cat = PoiCategory::HISTORIC_MINE;
        else if (std::strcmp(t_historic, "aircraft") == 0) cat = PoiCategory::HISTORIC_AIRCRAFT;
        else if (std::strcmp(t_historic, "locomotive") == 0) cat = PoiCategory::LOCOMOTIVE;
        else if (std::strcmp(t_historic, "cannon") == 0) cat = PoiCategory::CANNON;
        else if (std::strcmp(t_historic, "tomb") == 0) cat = PoiCategory::TOMB;
        else if (std::strcmp(t_historic, "manor") == 0) cat = PoiCategory::MANOR;
    }
    // boundary
    if (cat == PoiCategory::UNKNOWN && t_boundary) {
        if (std::strcmp(t_boundary, "national_park") == 0) cat = PoiCategory::NATIONAL_PARK;
        else if (std::strcmp(t_boundary, "protected_area") == 0) cat = PoiCategory::PROTECTED_AREA;
        // `boundary=forest` (managed-forest admin) folds
        // into WOOD since both surface as "named forested
        // area" in the landmark line.
        else if (std::strcmp(t_boundary, "forest") == 0) cat = PoiCategory::WOOD;
    }
    // landuse — only named landuse polygons get indexed.
    // Note: landuse is deliberately NOT in the trigger list
    // of the GENERIC_RANK30 fallback below, so residential/
    // industrial zones don't flood the index.
    if (cat == PoiCategory::UNKNOWN && t_landuse) {
        if (std::strcmp(t_landuse, "forest") == 0) cat = PoiCategory::WOOD;
        else if (std::strcmp(t_landuse, "meadow") == 0) cat = PoiCategory::MEADOW;
        else if (std::strcmp(t_landuse, "orchard") == 0) cat = PoiCategory::ORCHARD;
        else if (std::strcmp(t_landuse, "vineyard") == 0) cat = PoiCategory::VINEYARD;
        else if (std::strcmp(t_landuse, "farmland") == 0) cat = PoiCategory::FARMLAND;
        else if (std::strcmp(t_landuse, "allotments") == 0) cat = PoiCategory::ALLOTMENTS;
        else if (std::strcmp(t_landuse, "quarry") == 0) cat = PoiCategory::QUARRY;
        else if (std::strcmp(t_landuse, "reservoir") == 0) cat = PoiCategory::RESERVOIR;
        else if (std::strcmp(t_landuse, "basin") == 0) cat = PoiCategory::RESERVOIR;
        else if (std::strcmp(t_landuse, "recreation_ground") == 0) cat = PoiCategory::RECREATION_GROUND;
        else if (std::strcmp(t_landuse, "military") == 0) cat = PoiCategory::MILITARY;
        else if (std::strcmp(t_landuse, "religious") == 0) cat = PoiCategory::RELIGIOUS_LANDUSE;
        else if (std::strcmp(t_landuse, "cemetery") == 0) cat = PoiCategory::CEMETERY;
        // residential / industrial / commercial / retail
        // landuse zones intentionally skipped — they're
        // admin-hierarchy concepts already covered by the
        // address chain (suburb / borough / city_district).
    }
    // amenity
    if (cat == PoiCategory::UNKNOWN && t_amenity) {
        if (std::strcmp(t_amenity, "place_of_worship") == 0) cat = PoiCategory::PLACE_OF_WORSHIP;
        else if (std::strcmp(t_amenity, "university") == 0) cat = PoiCategory::UNIVERSITY;
        else if (std::strcmp(t_amenity, "college") == 0) cat = PoiCategory::COLLEGE;
        else if (std::strcmp(t_amenity, "hospital") == 0) cat = PoiCategory::HOSPITAL;
        else if (std::strcmp(t_amenity, "theatre") == 0) cat = PoiCategory::THEATRE;
        else if (std::strcmp(t_amenity, "cinema") == 0) cat = PoiCategory::CINEMA;
        else if (std::strcmp(t_amenity, "library") == 0) cat = PoiCategory::LIBRARY;
        else if (std::strcmp(t_amenity, "marketplace") == 0) cat = PoiCategory::MARKETPLACE;
        else if (std::strcmp(t_amenity, "embassy") == 0) cat = PoiCategory::EMBASSY;
        else if (std::strcmp(t_amenity, "fountain") == 0) cat = PoiCategory::FOUNTAIN;
        else if (std::strcmp(t_amenity, "casino") == 0) cat = PoiCategory::CASINO;
        else if (std::strcmp(t_amenity, "cemetery") == 0) cat = PoiCategory::CEMETERY;
        else if (std::strcmp(t_amenity, "ferry_terminal") == 0) cat = PoiCategory::FERRY_TERMINAL;
        else if (std::strcmp(t_amenity, "planetarium") == 0) cat = PoiCategory::PLANETARIUM;
        else if (std::strcmp(t_amenity, "prison") == 0) cat = PoiCategory::PRISON;
        // Food & drink (each OSM value → distinct category)
        else if (std::strcmp(t_amenity, "restaurant") == 0) cat = PoiCategory::RESTAURANT;
        else if (std::strcmp(t_amenity, "cafe") == 0) cat = PoiCategory::CAFE;
        else if (std::strcmp(t_amenity, "bar") == 0) cat = PoiCategory::BAR;
        else if (std::strcmp(t_amenity, "pub") == 0) cat = PoiCategory::PUB;
        else if (std::strcmp(t_amenity, "fast_food") == 0) cat = PoiCategory::FAST_FOOD;
        else if (std::strcmp(t_amenity, "biergarten") == 0) cat = PoiCategory::BIERGARTEN;
        else if (std::strcmp(t_amenity, "food_court") == 0) cat = PoiCategory::FOOD_COURT;
        else if (std::strcmp(t_amenity, "ice_cream") == 0) cat = PoiCategory::ICE_CREAM;
        else if (std::strcmp(t_amenity, "nightclub") == 0) cat = PoiCategory::NIGHTCLUB;
        // Education
        else if (std::strcmp(t_amenity, "school") == 0) cat = PoiCategory::SCHOOL;
        else if (std::strcmp(t_amenity, "kindergarten") == 0) cat = PoiCategory::KINDERGARTEN;
        else if (std::strcmp(t_amenity, "driving_school") == 0) cat = PoiCategory::DRIVING_SCHOOL;
        else if (std::strcmp(t_amenity, "music_school") == 0) cat = PoiCategory::MUSIC_SCHOOL;
        // Health / care
        else if (std::strcmp(t_amenity, "pharmacy") == 0) cat = PoiCategory::PHARMACY;
        else if (std::strcmp(t_amenity, "doctors") == 0) cat = PoiCategory::DOCTORS;
        else if (std::strcmp(t_amenity, "dentist") == 0) cat = PoiCategory::DENTIST;
        else if (std::strcmp(t_amenity, "clinic") == 0) cat = PoiCategory::CLINIC;
        else if (std::strcmp(t_amenity, "veterinary") == 0) cat = PoiCategory::VETERINARY;
        // Finance
        else if (std::strcmp(t_amenity, "bank") == 0) cat = PoiCategory::BANK;
        else if (std::strcmp(t_amenity, "atm") == 0) cat = PoiCategory::ATM;
        else if (std::strcmp(t_amenity, "bureau_de_change") == 0) cat = PoiCategory::BUREAU_DE_CHANGE;
        // Mail / parcel
        else if (std::strcmp(t_amenity, "post_office") == 0) cat = PoiCategory::POST_OFFICE;
        else if (std::strcmp(t_amenity, "post_box") == 0) cat = PoiCategory::POST_BOX;
        else if (std::strcmp(t_amenity, "parcel_locker") == 0) cat = PoiCategory::PARCEL_LOCKER;
        // Emergency services
        else if (std::strcmp(t_amenity, "police") == 0) cat = PoiCategory::POLICE;
        else if (std::strcmp(t_amenity, "fire_station") == 0) cat = PoiCategory::FIRE_STATION;
        // Civic
        else if (std::strcmp(t_amenity, "townhall") == 0) cat = PoiCategory::TOWNHALL;
        else if (std::strcmp(t_amenity, "courthouse") == 0) cat = PoiCategory::COURTHOUSE;
        else if (std::strcmp(t_amenity, "community_centre") == 0) cat = PoiCategory::COMMUNITY_CENTRE;
        else if (std::strcmp(t_amenity, "social_centre") == 0) cat = PoiCategory::SOCIAL_CENTRE;
        // Transport
        else if (std::strcmp(t_amenity, "bus_station") == 0) cat = PoiCategory::BUS_STATION;
        else if (std::strcmp(t_amenity, "taxi") == 0) cat = PoiCategory::TAXI;
        else if (std::strcmp(t_amenity, "parking") == 0) cat = PoiCategory::PARKING;
        else if (std::strcmp(t_amenity, "bicycle_parking") == 0) cat = PoiCategory::BICYCLE_PARKING;
        else if (std::strcmp(t_amenity, "motorcycle_parking") == 0) cat = PoiCategory::MOTORCYCLE_PARKING;
        else if (std::strcmp(t_amenity, "fuel") == 0) cat = PoiCategory::FUEL;
        else if (std::strcmp(t_amenity, "charging_station") == 0) cat = PoiCategory::CHARGING_STATION;
        // Shelter-class
        else if (std::strcmp(t_amenity, "shelter") == 0) cat = PoiCategory::SHELTER;
        // Small named amenities
        else if (std::strcmp(t_amenity, "toilets") == 0) cat = PoiCategory::TOILETS;
        else if (std::strcmp(t_amenity, "drinking_water") == 0) cat = PoiCategory::DRINKING_WATER;
        else if (std::strcmp(t_amenity, "bench") == 0) cat = PoiCategory::BENCH;
        else if (std::strcmp(t_amenity, "vending_machine") == 0) cat = PoiCategory::VENDING_MACHINE;
        else if (std::strcmp(t_amenity, "waste_basket") == 0) cat = PoiCategory::WASTE_BASKET;
        else if (std::strcmp(t_amenity, "recycling") == 0) cat = PoiCategory::RECYCLING;
        else if (std::strcmp(t_amenity, "clock") == 0) cat = PoiCategory::CLOCK;
        else if (std::strcmp(t_amenity, "telephone") == 0) cat = PoiCategory::TELEPHONE;
        else if (std::strcmp(t_amenity, "bbq") == 0) cat = PoiCategory::BBQ;
        // Arts / culture / studio
        else if (std::strcmp(t_amenity, "arts_centre") == 0) cat = PoiCategory::ARTS_CENTRE;
        else if (std::strcmp(t_amenity, "studio") == 0) cat = PoiCategory::STUDIO;
        // Education extras
        else if (std::strcmp(t_amenity, "language_school") == 0) cat = PoiCategory::LANGUAGE_SCHOOL;
        else if (std::strcmp(t_amenity, "training") == 0) cat = PoiCategory::TRAINING;
        // Health extras
        else if (std::strcmp(t_amenity, "nursing_home") == 0) cat = PoiCategory::NURSING_HOME;
        else if (std::strcmp(t_amenity, "ambulance_station") == 0) cat = PoiCategory::AMBULANCE_STATION;
        else if (std::strcmp(t_amenity, "hospice") == 0) cat = PoiCategory::HOSPICE;
        // Religious extra / grave
        else if (std::strcmp(t_amenity, "monastery") == 0) cat = PoiCategory::MONASTERY;
        else if (std::strcmp(t_amenity, "grave_yard") == 0) cat = PoiCategory::GRAVE_YARD;
        // Vehicle services
        else if (std::strcmp(t_amenity, "car_wash") == 0) cat = PoiCategory::CAR_WASH;
        else if (std::strcmp(t_amenity, "car_rental") == 0) cat = PoiCategory::CAR_RENTAL;
        else if (std::strcmp(t_amenity, "bicycle_rental") == 0) cat = PoiCategory::BICYCLE_RENTAL;
        else if (std::strcmp(t_amenity, "bicycle_repair_station") == 0) cat = PoiCategory::BICYCLE_REPAIR_STATION;
        else if (std::strcmp(t_amenity, "vehicle_inspection") == 0) cat = PoiCategory::VEHICLE_INSPECTION;
    }
    // leisure
    if (cat == PoiCategory::UNKNOWN && t_leisure) {
        if (std::strcmp(t_leisure, "park") == 0) cat = PoiCategory::PARK;
        else if (std::strcmp(t_leisure, "swimming_area") == 0) cat = PoiCategory::SWIMMING_AREA;
        else if (std::strcmp(t_leisure, "nature_reserve") == 0) cat = PoiCategory::NATURE_RESERVE;
        else if (std::strcmp(t_leisure, "stadium") == 0) cat = PoiCategory::STADIUM;
        // Each OSM leisure value → distinct category.
        else if (std::strcmp(t_leisure, "sports_centre") == 0) cat = PoiCategory::SPORTS_CENTRE;
        else if (std::strcmp(t_leisure, "garden") == 0) cat = PoiCategory::GARDEN;
        else if (std::strcmp(t_leisure, "water_park") == 0) cat = PoiCategory::WATER_PARK;
        else if (std::strcmp(t_leisure, "golf_course") == 0) cat = PoiCategory::GOLF_COURSE;
        else if (std::strcmp(t_leisure, "marina") == 0) cat = PoiCategory::MARINA;
        else if (std::strcmp(t_leisure, "playground") == 0) cat = PoiCategory::PLAYGROUND;
        else if (std::strcmp(t_leisure, "fitness_station") == 0) cat = PoiCategory::FITNESS_STATION;
        else if (std::strcmp(t_leisure, "fitness_centre") == 0) cat = PoiCategory::FITNESS_CENTRE;
        else if (std::strcmp(t_leisure, "pitch") == 0) cat = PoiCategory::PITCH;
        else if (std::strcmp(t_leisure, "track") == 0) cat = PoiCategory::TRACK;
        else if (std::strcmp(t_leisure, "sports_hall") == 0) cat = PoiCategory::SPORTS_HALL;
        else if (std::strcmp(t_leisure, "sauna") == 0) cat = PoiCategory::SAUNA;
        else if (std::strcmp(t_leisure, "horse_riding") == 0) cat = PoiCategory::HORSE_RIDING;
        else if (std::strcmp(t_leisure, "bird_hide") == 0) cat = PoiCategory::BIRD_HIDE;
        else if (std::strcmp(t_leisure, "miniature_golf") == 0) cat = PoiCategory::MINIATURE_GOLF;
    }
    // emergency
    if (cat == PoiCategory::UNKNOWN && t_emergency) {
        if (std::strcmp(t_emergency, "fire_water_pond") == 0) cat = PoiCategory::FIRE_WATER_POND;
    }
    // natural
    if (cat == PoiCategory::UNKNOWN && t_natural) {
        if (std::strcmp(t_natural, "peak") == 0) cat = PoiCategory::PEAK;
        else if (std::strcmp(t_natural, "volcano") == 0) cat = PoiCategory::VOLCANO;
        else if (std::strcmp(t_natural, "beach") == 0) cat = PoiCategory::BEACH;
        else if (std::strcmp(t_natural, "cave_entrance") == 0) cat = PoiCategory::CAVE_ENTRANCE;
        else if (std::strcmp(t_natural, "spring") == 0) cat = PoiCategory::SPRING;
        else if (std::strcmp(t_natural, "wood") == 0) cat = PoiCategory::WOOD;
        else if (std::strcmp(t_natural, "heath") == 0) cat = PoiCategory::HEATH;
        else if (std::strcmp(t_natural, "scrub") == 0) cat = PoiCategory::SCRUB;
        else if (std::strcmp(t_natural, "wetland") == 0) cat = PoiCategory::WETLAND;
        else if (std::strcmp(t_natural, "grassland") == 0) cat = PoiCategory::GRASSLAND;
        else if (std::strcmp(t_natural, "water") == 0) cat = PoiCategory::NATURAL_WATER;
        else if (std::strcmp(t_natural, "valley") == 0) cat = PoiCategory::VALLEY;
        else if (std::strcmp(t_natural, "ridge") == 0) cat = PoiCategory::RIDGE;
        else if (std::strcmp(t_natural, "saddle") == 0) cat = PoiCategory::SADDLE;
        else if (std::strcmp(t_natural, "gorge") == 0) cat = PoiCategory::GORGE;
        else if (std::strcmp(t_natural, "bare_rock") == 0) cat = PoiCategory::BARE_ROCK;
        else if (std::strcmp(t_natural, "cliff") == 0) cat = PoiCategory::CLIFF;
        else if (std::strcmp(t_natural, "arch") == 0) cat = PoiCategory::ARCH;
        else if (std::strcmp(t_natural, "hot_spring") == 0) cat = PoiCategory::HOT_SPRING;
        else if (std::strcmp(t_natural, "geyser") == 0) cat = PoiCategory::GEYSER;
        else if (std::strcmp(t_natural, "bay") == 0) cat = PoiCategory::BAY;
        else if (std::strcmp(t_natural, "cape") == 0) cat = PoiCategory::CAPE;
        else if (std::strcmp(t_natural, "island") == 0) cat = PoiCategory::ISLAND;
        else if (std::strcmp(t_natural, "glacier") == 0) cat = PoiCategory::GLACIER;
    }
    // railway
    if (cat == PoiCategory::UNKNOWN && t_railway) {
        if (std::strcmp(t_railway, "station") == 0) cat = PoiCategory::STATION;
    }
    // aeroway
    if (cat == PoiCategory::UNKNOWN && t_aeroway) {
        if (std::strcmp(t_aeroway, "aerodrome") == 0) cat = PoiCategory::AERODROME;
    }
    // man_made
    if (cat == PoiCategory::UNKNOWN && t_man_made) {
        if (std::strcmp(t_man_made, "tower") == 0) cat = PoiCategory::TOWER;
        else if (std::strcmp(t_man_made, "lighthouse") == 0) cat = PoiCategory::LIGHTHOUSE;
        else if (std::strcmp(t_man_made, "windmill") == 0) cat = PoiCategory::WINDMILL;
        else if (std::strcmp(t_man_made, "bridge") == 0) cat = PoiCategory::BRIDGE;
        else if (std::strcmp(t_man_made, "pier") == 0) cat = PoiCategory::PIER;
        else if (std::strcmp(t_man_made, "dam") == 0) cat = PoiCategory::DAM;
        else if (std::strcmp(t_man_made, "observatory") == 0) cat = PoiCategory::OBSERVATORY;
        else if (std::strcmp(t_man_made, "silo") == 0) cat = PoiCategory::SILO;
        else if (std::strcmp(t_man_made, "chimney") == 0) cat = PoiCategory::CHIMNEY;
        else if (std::strcmp(t_man_made, "watermill") == 0) cat = PoiCategory::WATERMILL;
    }
    // building
    if (cat == PoiCategory::UNKNOWN && t_building) {
        if (std::strcmp(t_building, "cathedral") == 0) cat = PoiCategory::CATHEDRAL;
        else if (std::strcmp(t_building, "palace") == 0) cat = PoiCategory::PALACE;
    }
    // craft
    if (cat == PoiCategory::UNKNOWN && t_craft) {
        if (std::strcmp(t_craft, "winery") == 0) cat = PoiCategory::WINERY;
        else if (std::strcmp(t_craft, "brewery") == 0) cat = PoiCategory::BREWERY;
    }
    // power
    if (cat == PoiCategory::UNKNOWN && t_power) {
        if (std::strcmp(t_power, "plant") == 0) cat = PoiCategory::POWER_PLANT;
    }
    // place
    if (cat == PoiCategory::UNKNOWN && t_place) {
        if (std::strcmp(t_place, "island") == 0 || std::strcmp(t_place, "islet") == 0)
            cat = PoiCategory::ISLAND;
    }
    // waterway
    if (cat == PoiCategory::UNKNOWN && t_waterway) {
        if (std::strcmp(t_waterway, "waterfall") == 0) cat = PoiCategory::WATERFALL;
    }
    // office
    if (cat == PoiCategory::UNKNOWN && t_office) {
        if (std::strcmp(t_office, "government") == 0) cat = PoiCategory::GOVERNMENT;
    }
    // shop — split common values, fall back to generic SHOP.
    if (cat == PoiCategory::UNKNOWN && t_shop) {
        if (std::strcmp(t_shop, "supermarket") == 0) cat = PoiCategory::SHOP_SUPERMARKET;
        else if (std::strcmp(t_shop, "convenience") == 0) cat = PoiCategory::SHOP_CONVENIENCE;
        else if (std::strcmp(t_shop, "clothes") == 0) cat = PoiCategory::SHOP_CLOTHES;
        else if (std::strcmp(t_shop, "mall") == 0) cat = PoiCategory::SHOP_MALL;
        else if (std::strcmp(t_shop, "department_store") == 0) cat = PoiCategory::SHOP_DEPARTMENT_STORE;
        else if (std::strcmp(t_shop, "bakery") == 0) cat = PoiCategory::SHOP_BAKERY;
        else if (std::strcmp(t_shop, "butcher") == 0) cat = PoiCategory::SHOP_BUTCHER;
        else if (std::strcmp(t_shop, "hardware") == 0) cat = PoiCategory::SHOP_HARDWARE;
        else if (std::strcmp(t_shop, "doityourself") == 0) cat = PoiCategory::SHOP_HARDWARE;
        else if (std::strcmp(t_shop, "electronics") == 0) cat = PoiCategory::SHOP_ELECTRONICS;
        else if (std::strcmp(t_shop, "furniture") == 0) cat = PoiCategory::SHOP_FURNITURE;
        else if (std::strcmp(t_shop, "jewelry") == 0) cat = PoiCategory::SHOP_JEWELRY;
        else if (std::strcmp(t_shop, "books") == 0) cat = PoiCategory::SHOP_BOOKS;
        else if (std::strcmp(t_shop, "pet") == 0) cat = PoiCategory::SHOP_PET;
        else cat = PoiCategory::SHOP; // generic fallback
    }

    // Fallback: any rank-30 addressable feature that didn't
    // hit a specific PoiCategory above (Nominatim's reverse.py
    // DataLayer.POI filter: class_ NOT IN ('place', 'building'),
    // rank_search==30). Only a wikipedia or wikidata tag lifts
    // one into a POI dir (below).
    // A water object a POI main tag of no category of its own makes a
    // rank-30 POI is one too.
    if (cat == PoiCategory::UNKNOWN) {
        if (yielded != PoiCategory::UNKNOWN || t_amenity || t_tourism || t_historic || t_leisure ||
            t_man_made || t_craft || t_office || t_waterway ||
            t_natural || t_aeroway || t_railway || t_power ||
            t_shop) {
            cat = PoiCategory::GENERIC_RANK30;
        }
    }

    const PoiCategory tagged = cat;
    if (kept != PoiCategory::UNKNOWN) cat = kept;
    if (cat == PoiCategory::UNKNOWN) return std::optional<PoiClassification>{};

    uint8_t flags = 0;
    if (t_wikipedia) flags |= POI_FLAG_WIKIPEDIA;
    if (t_wikidata)  flags |= POI_FLAG_WIKIDATA;
    // Nominatim treats highway=pedestrian/footway/living_street/
    // path/service areas as rank_search=26 (road rank) rather
    // than rank_search=30. Mark so the server can surface the
    // feature's own name as `road` rather than its parent street. A water
    // area is a street of every highway kind that ranks it (poi_water_street),
    // and water a place or landuse kind of a lower rank ranks is no street
    // (no_street).
    if (water_street || (t_highway && !no_street && (
            std::strcmp(t_highway, "pedestrian") == 0 ||
            std::strcmp(t_highway, "footway") == 0 ||
            std::strcmp(t_highway, "living_street") == 0 ||
            std::strcmp(t_highway, "path") == 0 ||
            std::strcmp(t_highway, "cycleway") == 0 ||
            std::strcmp(t_highway, "service") == 0))) {
        flags |= POI_FLAG_HIGHWAY;
    }
    // A reservoir point a main tag of a lower rank ranks, or of a class the
    // POI lane takes none of, is in neither lane in Nominatim.
    if (kept == PoiCategory::RESERVOIR && !area) flags |= POI_FLAG_PLACES_ONLY;
    uint8_t tier = poi_tier_of(cat, flags);
    if (kept != PoiCategory::UNKNOWN && (flags & POI_FLAG_HIGHWAY)) tier = std::min(tier, poi_tier_of(tagged, flags));
    // A water object that yields ships as the water it is in the tiers whose
    // dirs ship its water kind but not its category: all of them where its
    // rank-30 tags give a category no POI dir ships (a generic rank-30
    // feature without a wiki tag), which keeps its name in the POI tiers and
    // out of the primary slot, and those between the two tiers where its
    // kind's is the lower (a bay, of tier 2, that is a pier, of tier 3). A
    // reservoir point in place of its POI, which the server would let take
    // the primary slot as it does any RESERVOIR point, never does
    // (POI_FLAG_PLACES_ONLY, poi_record_in_tier).
    if (yielded != PoiCategory::UNKNOWN) {
        const uint8_t water_tier = poi_tier_of(yielded, flags);
        if (tier > POI_MAX_SHIPPED_TIER) {
            cat = yielded;
            tier = water_tier;
            if (yielded == PoiCategory::RESERVOIR && !area) flags |= POI_FLAG_PLACES_ONLY;
        } else if (water_tier < tier) {
            flags = poi_flags_with_water(flags, yielded);
        }
    }
    if (tier > POI_MAX_SHIPPED_TIER) return std::optional<PoiClassification>{};

    return PoiClassification{cat, tier, flags, yielded};
}
