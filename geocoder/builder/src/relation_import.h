#pragma once

#include <cstdint>
#include <cstring>
#include <initializer_list>

// Which OSM relations Nominatim turns into places, and so which ones become
// admin, postal or place areas here.
//
// Nominatim's flex style hands a relation to the tag rules only when its type
// has a geometry function (lib-lua/themes/nominatim/init.lua, process_relation
// and RELATION_TYPES): multipolygon and boundary build areas, waterway builds
// lines. Every other relation is dropped whatever its tags, so a
// boundary=administrative relation without type=boundary never exists there
// (R19573110 is not in the public API).
//
// Among area relations the full style's main tags (presets.lua,
// MAIN_TAGS.all_boundaries) pick the row:
//   boundary=administrative  'named': an admin boundary. A place=* tag on it
//                            only labels it (one row, place in extratags).
//   boundary=postal_code     'postcode_area': a postcode area.
//   boundary=place           'delete': the tag is dropped.
//   place=*                  'always': a place area, whatever other boundary
//                            value the relation carries.
enum class RelationKind : uint8_t { NONE, ADMIN, POSTAL, PLACE };

inline bool is_area_relation_type(const char* type) {
    return type && (std::strcmp(type, "multipolygon") == 0 || std::strcmp(type, "boundary") == 0);
}

// The kind an area relation's tags give it; `has_place` is a place=* value
// the builder ranks (classify_place_override).
inline RelationKind area_relation_kind(const char* boundary, bool has_place) {
    if (boundary && std::strcmp(boundary, "administrative") == 0) return RelationKind::ADMIN;
    if (boundary && std::strcmp(boundary, "postal_code") == 0) return RelationKind::POSTAL;
    if (has_place) return RelationKind::PLACE;

    return RelationKind::NONE;
}

// The postcode of a boundary=postal_code relation, from the address tags
// Nominatim reads it from (presets.lua, ADDRESS_TAGS.core postcode keys).
// Null when it has none: Nominatim then makes no postcode area, and the name
// never stands in.
template <typename GetTag>
inline const char* postal_area_code(GetTag tag) {
    for (const char* key : {"postal_code", "postcode", "addr:postcode"})
        if (const char* value = tag(key)) return value;
    return nullptr;
}
