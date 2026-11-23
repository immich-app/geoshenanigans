#pragma once
// Per-country admin_level → Nominatim rank_address mapping.
//
// MUST STAY IN SYNC with geocoder/server/admin_levels.json (the server's
// AdminLevelConfig), which itself mirrors Nominatim's
// settings/address-levels.json boundary.administrativeN entries.
//
// Used by find_linked_place step 4 (name-based place linking): Nominatim
// links a boundary to a same-named place node only when the node's
// address rank EQUALS the boundary's per-country rank_address. Using the
// default mapping here linked e.g. Dutch gemeente boundaries (NL AL8 →
// rank 14, municipality) to their town nodes (rank 16), relabelling
// municipalities as towns — Nominatim keeps them separate.
#include <cstdint>

#include "country_code.h"
#include "types.h"

inline uint8_t admin_rank_address(uint16_t cc, uint8_t admin_level) {
    // Country overrides (packed uppercase 'XY').
    constexpr auto pack = pack_country_code;
    switch (cc) {
        case pack('A','U'): if (admin_level == 6) return 0; break;
        case pack('B','E'): switch (admin_level) {
            case 3: return 0; case 4: return 6; case 5: return 0;
            case 6: return 8; case 7: return 12; case 8: return 14;
            case 9: return 16; case 10: return 18; default: break; } break;
        case pack('B','R'): if (admin_level >= 5 && admin_level <= 7) return 0; break;
        case pack('C','Z'): switch (admin_level) {
            case 5: return 12; case 6: return 0; case 7: return 0;
            case 8: return 14; case 9: return 15; case 10: return 16; default: break; } break;
        case pack('D','E'): if (admin_level == 5) return 0; break;
        case pack('E','S'): switch (admin_level) {
            case 5: return 0; case 6: return 10; case 7: return 12;
            case 10: return 22; default: break; } break;
        case pack('I','D'): switch (admin_level) {
            case 5: return 12; case 6: return 14; case 7: return 16;
            case 8: return 20; case 9: return 22; case 10: return 24; default: break; } break;
        case pack('J','P'): switch (admin_level) {
            case 7: return 16; case 8: return 18; case 9: return 20;
            case 10: return 22; case 11: return 24; default: break; } break;
        case pack('N','L'): switch (admin_level) {
            case 7: return 0; case 8: return 14; case 9: return 0;
            case 10: return 16; default: break; } break;
        case pack('N','O'): switch (admin_level) {
            case 3: return 8; case 4: return 12; default: break; } break;
        case pack('R','U'): switch (admin_level) {
            case 5: return 0; case 7: return 0; case 8: return 14; default: break; } break;
        case pack('S','E'): switch (admin_level) {
            case 3: return 8; case 4: return 12; default: break; } break;
        default: break;
    }
    // Default: rank_address = admin_level * 2 for levels 2..12.
    if (admin_level >= 2 && admin_level <= 12)
        return static_cast<uint8_t>(admin_level * 2);
    return 0;
}

// rank_address of a place area by its place type: the default "place" ranks
// of Nominatim's settings/address-levels.json (state, province and region
// rank 0, outside addresses). Mirrors the server's place_type_to_rank.
inline uint8_t place_area_rank_address(uint8_t place_type) {
    switch (static_cast<AdminPlaceType>(place_type)) {
        case AdminPlaceType::CITY:
        case AdminPlaceType::TOWN:
        case AdminPlaceType::VILLAGE: return 16;
        case AdminPlaceType::SUBURB:
        case AdminPlaceType::HAMLET: return 20;
        case AdminPlaceType::QUARTER: return 22;
        case AdminPlaceType::NEIGHBOURHOOD: return 24;
        case AdminPlaceType::COUNTY:
        case AdminPlaceType::DISTRICT: return 12;
        case AdminPlaceType::BOROUGH: return 18;
        case AdminPlaceType::MUNICIPALITY: return 14;
        default: return 0;
    }
}

// rank_address of a place area by its place type in the country `cc`
// (packed upper case, 0 where unknown): the country's own "place" ranks of
// address-levels.json (ca, de, es, id, ru, sa; civil_parish is no type of
// ours), then the default ones. Mirrors the server's place_type_to_rank.
inline uint8_t place_area_rank_address(uint16_t cc, uint8_t place_type) {
    constexpr auto pack = pack_country_code;
    switch (static_cast<AdminPlaceType>(place_type)) {
        case AdminPlaceType::COUNTY:
            if (cc == pack('C', 'A') || cc == pack('D', 'E')) return 0;
            break;
        case AdminPlaceType::REGION:
            if (cc == pack('D', 'E')) return 0;
            break;
        case AdminPlaceType::PROVINCE:
            if (cc == pack('E', 'S')) return 10;
            if (cc == pack('S', 'A')) return 12;
            break;
        case AdminPlaceType::MUNICIPALITY:
            if (cc == pack('I', 'D') || cc == pack('R', 'U') || cc == pack('S', 'A')) return 18;
            break;
        default: break;
    }
    return place_area_rank_address(place_type);
}

// Whether a place area of this type is part of addresses in some country:
// Nominatim makes an address area only of a place with rank_address > 0
// (placex_update, placex_triggers.sql:1254) and links no place node to one
// of rank 0 (find_linked_place, :285). place=state and region rank 0
// everywhere, place=province only outside Spain and Saudi Arabia.
inline bool is_address_place_type(AdminPlaceType place_type) {
    return place_area_rank_address(static_cast<uint8_t>(place_type)) > 0 || place_type == AdminPlaceType::PROVINCE;
}

// rank_address of an admin polygon in the country `cc` before any nested
// promotion: its place type's for a place area (admin_level 15), its admin
// level's for a boundary. A polygon of rank 0 is in no address and links no
// place node (find_linked_place, placex_triggers.sql:285).
inline uint8_t polygon_rank_address(uint16_t cc, uint8_t admin_level, uint8_t place_type) {
    return admin_level == 15 ? place_area_rank_address(cc, place_type) : admin_rank_address(cc, admin_level);
}

// rank_address of a place area (admin_level 15) of its own place tag `own`
// that links a place node of type `linked` (both AdminPlaceType), in the
// country `cc`, before an area containing it lifts it: placex_update keeps a
// place area of rank 16 to 23 at its own rank whether it links a node or not
// (that branch leaves parent_address_level unset, placex_triggers.sql:930-950,
// :1129), and gives any other the linked node's rank where that is above 3
// and below 26 (:1109-1133). One of rank 0 links nothing (find_linked_place,
// :285). Mirrors the server's place_area_ranks.
inline uint8_t linked_place_area_rank(uint16_t cc, uint8_t own, uint8_t linked) {
    const uint8_t own_rank = place_area_rank_address(cc, own);
    if (own_rank == 0 || (own_rank >= 16 && own_rank <= 23)) return own_rank;
    const uint8_t linked_rank = place_area_rank_address(cc, linked);
    return linked_rank > 3 && linked_rank < 26 ? linked_rank : own_rank;
}

// Admin-minimal carries addresses down to rank 16, the cities, towns and
// villages its place nodes also reach: boundaries of admin_level 2..8 and the
// place areas (admin_level 15) of those ranks, a place area linked to a place
// node of another type at linked_place_area_rank's (`own_place_type` its own
// place tag, 0 where that is `place_type`). Singapore's city is its place=city
// area (R17140517), not a boundary.
inline bool admin_minimal_keeps(uint8_t admin_level, uint8_t place_type, uint8_t own_place_type = 0) {
    if (admin_level >= 2 && admin_level <= 8) return true;
    const uint8_t rank = own_place_type ? linked_place_area_rank(0, own_place_type, place_type)
                                        : place_area_rank_address(place_type);

    return admin_level == 15 && rank > 0 && rank <= 16;
}

// Admin-minimal's place nodes: cities, towns, villages and hamlets; no
// suburbs, neighbourhoods or quarters.
inline bool admin_minimal_keeps_place(uint8_t place_type) {
    switch (static_cast<PlaceType>(place_type)) {
    case PlaceType::CITY:
    case PlaceType::TOWN:
    case PlaceType::VILLAGE:
    case PlaceType::HAMLET:
        return true;
    default:
        return false;
    }
}
