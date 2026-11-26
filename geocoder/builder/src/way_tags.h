#pragma once
// The tags the way pass reads, picked out of a way's tag list in one scan,
// and the rule for which ways get their node coordinates resolved. Lifted
// out of main()'s way pass so a node pre-scan can apply the same rule.
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "admin_rank_config.h"
#include "geometry.h"
#include "poi_classify.h"

struct WayTags {
    const char* interpolation = nullptr;
    const char* housenumber = nullptr;
    const char* street = nullptr;
    const char* postcode = nullptr;
    const char* city = nullptr;
    const char* highway = nullptr;
    const char* footway = nullptr;
    const char* name = nullptr;
    const char* name_en = nullptr;
    const char* boundary = nullptr;
    const char* admin_level = nullptr;
    const char* postal_code = nullptr;
    const char* iso = nullptr;
    const char* linked_place = nullptr;
    // POI tags
    const char* tourism = nullptr;
    const char* historic = nullptr;
    const char* amenity = nullptr;
    const char* leisure = nullptr;
    const char* natural = nullptr;
    const char* aeroway = nullptr;
    const char* railway = nullptr;
    const char* man_made = nullptr;
    const char* building = nullptr;
    const char* craft = nullptr;
    const char* power = nullptr;
    const char* place = nullptr;
    const char* waterway = nullptr;
    const char* water = nullptr;
    const char* office = nullptr;
    const char* wikipedia = nullptr;
    const char* wikidata = nullptr;
    const char* ele = nullptr;
    const char* shop = nullptr;
    const char* landuse = nullptr;
    // area=*: a closed way of a highway tag is a polygon in Nominatim where
    // it is yes alone (init.lua process_way), a line else.
    const char* area = nullptr;
    // Classifies a POI way (poi_classify.h), and makes one on its own where
    // it is fire_water_pond, a category of its own, as a node of it is.
    const char* emergency = nullptr;

    // Prefer name:en where present (English-only mode)
    const char* best_name() const { return (name_en && name_en[0]) ? name_en : name; }

    bool has_poi_tags() const {
        return tourism || historic || amenity || leisure ||
            natural || aeroway || railway || man_made || building ||
            craft || power || place || waterway || office ||
            (boundary && (std::strcmp(boundary, "national_park") == 0 ||
                          std::strcmp(boundary, "protected_area") == 0)) ||
            (emergency && std::strcmp(emergency, "fire_water_pond") == 0) ||
            // A reservoir or basin beside a POI main tag is a POI of that tag
            // (poi_classify.h), whose key (emergency, shop) may make no POI
            // way alone.
            (landuse && (std::strcmp(landuse, "reservoir") == 0 || std::strcmp(landuse, "basin") == 0) &&
             has_poi_main_tag(amenity, leisure, emergency, tourism, historic, shop, craft, office, aeroway, railway,
                              man_made)) ||
            // So is one that is a street (poi_water_street) of area=yes, which
            // alone makes a closed way of a highway tag a polygon (init.lua
            // process_way): its own name answers inside it.
            (area && std::strcmp(area, "yes") == 0 && poi_water_street(natural, landuse, highway, footway, place));
    }
};

// Single-pass tag extraction — avoid repeated linear scans. Keys and values
// index the block's string table; out-of-range indices are ignored.
inline WayTags parse_way_tags(const uint32_t* tag_keys, const uint32_t* tag_vals, size_t ntags,
                              const std::vector<std::string>& st) {
    WayTags t;
    for (size_t i = 0; i < ntags; i++) {
        if (tag_keys[i] >= st.size()) continue;
        const auto& k = st[tag_keys[i]];
        const char* v = tag_vals[i] < st.size() ? st[tag_vals[i]].c_str() : nullptr;
        // Fast dispatch by first character
        switch (k.size() > 0 ? k[0] : 0) {
            case 'a':
                if (k == "addr:interpolation") t.interpolation = v;
                else if (k == "addr:housenumber") t.housenumber = v;
                else if (k == "addr:street") t.street = v;
                else if (k == "addr:postcode") t.postcode = v;
                else if (k == "addr:city") t.city = v;
                else if (k == "admin_level") t.admin_level = v;
                else if (k == "area") t.area = v;
                else if (k == "amenity") t.amenity = v;
                else if (k == "aeroway") t.aeroway = v;
                break;
            case 'b':
                if (k == "boundary") t.boundary = v;
                else if (k == "building") t.building = v;
                break;
            case 'c': if (k == "craft") t.craft = v; break;
            case 'e':
                if (k == "ele") t.ele = v;
                else if (k == "emergency") t.emergency = v;
                break;
            case 'f': if (k == "footway") t.footway = v; break;
            case 'h':
                if (k == "highway") t.highway = v;
                else if (k == "historic") t.historic = v;
                break;
            case 'l':
                if (k == "leisure") t.leisure = v;
                else if (k == "linked_place") t.linked_place = v;
                else if (k == "landuse") t.landuse = v;
                break;
            case 'm': if (k == "man_made") t.man_made = v; break;
            case 'n':
                if (k == "name") t.name = v;
                else if (k == "name:en") t.name_en = v;
                else if (k == "natural") t.natural = v;
                break;
            case 'o':
                if (k == "office") t.office = v;
                break;
            case 'p':
                if (k == "postal_code") t.postal_code = v;
                else if (k == "place") t.place = v;
                else if (k == "power") t.power = v;
                break;
            case 'r': if (k == "railway") t.railway = v; break;
            case 's': if (k == "shop") t.shop = v; break;
            case 't': if (k == "tourism") t.tourism = v; break;
            case 'I': if (k == "ISO3166-1:alpha2") t.iso = v; break;
            case 'w':
                if (k == "waterway") t.waterway = v;
                else if (k == "water") t.water = v;
                else if (k == "wikipedia") t.wikipedia = v;
                else if (k == "wikidata") t.wikidata = v;
                break;
        }
    }
    return t;
}

// Whether a closed way of this place tag is a place area of its own: the
// rank-20+ place types (neighbourhood, quarter, suburb, borough) and a
// locality, of rank 25 and in no address. Higher-level place types on closed
// ways are rare, and taking them risks promoting a landuse polygon into the
// city or state chain.
inline bool is_place_area_way(const char* place) {
    switch (place_label_of(place_value_of(place))) {
        case AdminPlaceType::NEIGHBOURHOOD:
        case AdminPlaceType::QUARTER:
        case AdminPlaceType::SUBURB:
        case AdminPlaceType::BOROUGH:
        case AdminPlaceType::LOCALITY: return true;
        default: return false;
    }
}

// Whether the way pass resolves this way's node coordinates; every other
// way is skipped before any lookup. `relation_member`: the way has refs and
// belongs to a pass-1 admin/postal/place or POI relation. Highways and POI
// ways qualify on `name`, not best_name(), and ways tagged only
// landuse/shop never do; a place area's way (is_place_area_way) on
// best_name(), as the way pass names the area.
inline bool way_needs_nodes(const WayTags& t, bool relation_member) {
    return t.interpolation || t.housenumber ||
        (t.highway && is_included_highway_full(t.highway, t.footway) && t.name) ||
        t.boundary ||
        relation_member ||
        (t.has_poi_tags() && t.name) ||
        (t.place && t.best_name() && is_place_area_way(t.place));
}
