#pragma once
// The tags the way pass reads, picked out of a way's tag list in one scan,
// and the rule for which ways get their node coordinates resolved. Lifted
// out of main()'s way pass so a node pre-scan can apply the same rule.
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "geometry.h"

struct WayTags {
    const char* interpolation = nullptr;
    const char* housenumber = nullptr;
    const char* street = nullptr;
    const char* postcode = nullptr;
    const char* highway = nullptr;
    const char* footway = nullptr;
    const char* access = nullptr;
    const char* tunnel = nullptr;
    const char* name = nullptr;
    const char* name_en = nullptr;
    const char* boundary = nullptr;
    const char* admin_level = nullptr;
    const char* postal_code = nullptr;
    const char* iso = nullptr;
    const char* linked_place = nullptr;
    const char* border_type = nullptr;
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
    const char* office = nullptr;
    const char* wikipedia = nullptr;
    const char* wikidata = nullptr;
    const char* ele = nullptr;
    const char* shop = nullptr;
    const char* landuse = nullptr;

    // Prefer name:en where present (English-only mode)
    const char* best_name() const { return (name_en && name_en[0]) ? name_en : name; }

    bool has_poi_tags() const {
        return tourism || historic || amenity || leisure ||
            natural || aeroway || railway || man_made || building ||
            craft || power || place || waterway || office ||
            (boundary && (std::strcmp(boundary, "national_park") == 0 ||
                          std::strcmp(boundary, "protected_area") == 0));
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
                else if (k == "admin_level") t.admin_level = v;
                else if (k == "amenity") t.amenity = v;
                else if (k == "aeroway") t.aeroway = v;
                else if (k == "access") t.access = v;
                break;
            case 'b':
                if (k == "boundary") t.boundary = v;
                else if (k == "building") t.building = v;
                else if (k == "border_type") t.border_type = v;
                break;
            case 'c': if (k == "craft") t.craft = v; break;
            case 'e': if (k == "ele") t.ele = v; break;
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
            case 't':
                if (k == "tourism") t.tourism = v;
                else if (k == "tunnel") t.tunnel = v;
                break;
            case 'I': if (k == "ISO3166-1:alpha2") t.iso = v; break;
            case 'w':
                if (k == "waterway") t.waterway = v;
                else if (k == "wikipedia") t.wikipedia = v;
                else if (k == "wikidata") t.wikidata = v;
                break;
        }
    }
    return t;
}

// Whether the way pass resolves this way's node coordinates; every other
// way is skipped before any lookup. `relation_member`: the way has refs and
// belongs to a pass-1 admin/postal/place or POI relation. Highways and POI
// ways qualify on `name`, not best_name(), and ways tagged only
// landuse/shop never do.
inline bool way_needs_nodes(const WayTags& t, bool relation_member) {
    return t.interpolation || t.housenumber ||
        (t.highway && is_included_highway_full(t.highway, t.footway, t.access, t.tunnel) && t.name) ||
        t.boundary ||
        relation_member ||
        (t.has_poi_tags() && t.name);
}
