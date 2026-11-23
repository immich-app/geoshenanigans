#pragma once
// Nominatim's ranks of the boundaries and places the builder keeps:
// settings/address-levels.json as the public server loads it (master), each
// entry a (rank_search, rank_address) pair, a country's entries before the
// default ones. rank_address.h gives every polygon and place node its final
// rank_address from these, and the dirs carry it; nothing else ranks.
#include <cstdint>

#include "country_code.h"
#include "types.h"

// A tag's rank_search, which orders Nominatim's indexing, and rank_address.
struct PlaceRanks {
    uint8_t search = 0, address = 0;
};
inline bool operator==(const PlaceRanks& a, const PlaceRanks& b) { return a.search == b.search && a.address == b.address; }

// The ranks of boundary=administrative at admin_level in the country `cc`
// (packed upper case, 0 where unknown): address-levels.json's
// administrativeN, the country's first. A level the file has no entry for
// ranks 0.
inline PlaceRanks admin_ranks(uint16_t cc, uint8_t admin_level) {
    constexpr auto pack = pack_country_code;
    auto both = [](uint8_t rank) { return PlaceRanks{rank, rank}; };
    auto out = [](uint8_t search) { return PlaceRanks{search, 0}; };
    switch (cc) {
        case pack('A','U'): if (admin_level == 6) return out(12); break;
        case pack('B','E'): switch (admin_level) {
            case 3: return out(5); case 4: return both(6); case 5: return out(7);
            case 6: return both(8); case 7: return both(12); case 8: return both(14);
            case 9: return both(16); case 10: return both(18); default: break; } break;
        case pack('B','R'): switch (admin_level) {
            case 5: return out(10); case 6: return out(12); case 7: return out(14); default: break; } break;
        case pack('C','Z'): switch (admin_level) {
            case 5: return both(12); case 6: return out(13); case 7: return out(14);
            case 8: return both(14); case 9: return both(15); case 10: return both(16); default: break; } break;
        case pack('D','E'): if (admin_level == 5) return out(10); break;
        case pack('E','S'): switch (admin_level) {
            case 5: return out(10); case 6: return both(10); case 7: return both(12);
            case 10: return both(22); default: break; } break;
        case pack('I','D'): switch (admin_level) {
            case 5: return both(12); case 6: return both(14); case 7: return both(16);
            case 8: return both(20); case 9: return both(22); case 10: return both(24); default: break; } break;
        case pack('J','P'): switch (admin_level) {
            case 7: return both(16); case 8: return both(18); case 9: return both(20);
            case 10: return both(22); case 11: return both(24); default: break; } break;
        case pack('N','L'): switch (admin_level) {
            case 7: return out(13); case 8: return both(14); case 9: return out(15);
            case 10: return both(16); default: break; } break;
        case pack('N','O'):
        case pack('S','E'): switch (admin_level) {
            case 3: return both(8); case 4: return both(12); default: break; } break;
        case pack('R','U'): switch (admin_level) {
            case 5: return out(10); case 7: return out(13); case 8: return both(14); default: break; } break;
        default: break;
    }
    // Default: rank 2 * admin_level for levels 2..12.
    if (admin_level >= 2 && admin_level <= 12) return both(static_cast<uint8_t>(admin_level * 2));
    return {};
}

inline uint8_t admin_rank_address(uint16_t cc, uint8_t admin_level) { return admin_ranks(cc, admin_level).address; }

// The ranks of a place=* tag of AdminPlaceType `place_type` in the country
// `cc`: address-levels.json's `place` entries, the country's first (ca, de,
// es, id, ru, sa; civil_parish is no type of ours). state, province and
// region are outside addresses by default.
inline PlaceRanks place_ranks(uint16_t cc, uint8_t place_type) {
    constexpr auto pack = pack_country_code;
    auto both = [](uint8_t rank) { return PlaceRanks{rank, rank}; };
    switch (static_cast<AdminPlaceType>(place_type)) {
        case AdminPlaceType::COUNTY:
            if (cc == pack('C', 'A') || cc == pack('D', 'E')) return {12, 0};
            return both(12);
        case AdminPlaceType::REGION:
            if (cc == pack('D', 'E')) return {10, 0};
            return {18, 0};
        case AdminPlaceType::PROVINCE:
            if (cc == pack('E', 'S')) return both(10);
            if (cc == pack('S', 'A')) return both(12);
            return {8, 0};
        case AdminPlaceType::MUNICIPALITY:
            if (cc == pack('I', 'D') || cc == pack('R', 'U') || cc == pack('S', 'A')) return both(18);
            return both(14);
        case AdminPlaceType::STATE: return {8, 0};
        case AdminPlaceType::DISTRICT: return both(12);
        case AdminPlaceType::CITY: return both(16);
        case AdminPlaceType::TOWN: return {18, 16};
        case AdminPlaceType::BOROUGH: return both(18);
        case AdminPlaceType::VILLAGE: return {19, 16};
        case AdminPlaceType::SUBURB: return {19, 20};
        case AdminPlaceType::HAMLET: return both(20);
        case AdminPlaceType::QUARTER: return {20, 22};
        case AdminPlaceType::NEIGHBOURHOOD: return both(24);
        default: return {};
    }
}

// rank_address of a place area by its place type, outside any country's own
// ranks.
inline uint8_t place_area_rank_address(uint8_t place_type) { return place_ranks(0, place_type).address; }

// rank_address of a place area by its place type in the country `cc`.
inline uint8_t place_area_rank_address(uint16_t cc, uint8_t place_type) { return place_ranks(cc, place_type).address; }

// rank_address of a place node by its PlaceType, the same in every country:
// city, town and village 16, suburb and hamlet 20, quarter 22, neighbourhood
// 24; 0 for a type no place node has. A node of rank 16 to 23 inside a
// relation of its rank ranks PLACE_NODE_PROMOTION more (placex_triggers.sql:
// 951-971).
inline uint8_t place_node_rank(uint8_t place_type) {
    switch (static_cast<PlaceType>(place_type)) {
        case PlaceType::CITY:
        case PlaceType::TOWN:
        case PlaceType::VILLAGE: return 16;
        case PlaceType::SUBURB:
        case PlaceType::HAMLET: return 20;
        case PlaceType::QUARTER: return 22;
        case PlaceType::NEIGHBOURHOOD: return 24;
        default: return 0;
    }
}

// What a promotion adds to a place node's rank, and whether a node of its
// type's rank can be promoted.
inline constexpr uint8_t PLACE_NODE_PROMOTION = 2;
inline bool place_node_promotable(uint8_t rank) { return rank >= 16 && rank <= 23; }

// Whether a place area of this type is part of addresses in some country:
// Nominatim makes an address area only of a place with rank_address > 0
// (placex_update, placex_triggers.sql:1254) and links no place node to one
// of rank 0 (find_linked_place, :285). place=state and region rank 0
// everywhere, place=province only outside Spain and Saudi Arabia.
inline bool is_address_place_type(AdminPlaceType place_type) {
    return place_area_rank_address(static_cast<uint8_t>(place_type)) > 0 || place_type == AdminPlaceType::PROVINCE;
}

// The ranks of an admin polygon in the country `cc` before any nested
// promotion, which Nominatim's compute_place_rank gives it: its place type's
// for a place area (admin_level 15), its admin level's for a boundary (a
// boundary's own place tag doesn't rank it there).
inline PlaceRanks polygon_ranks(uint16_t cc, uint8_t admin_level, uint8_t place_type) {
    return admin_level == 15 ? place_ranks(cc, place_type) : admin_ranks(cc, admin_level);
}

// rank_address of an admin polygon in the country `cc` before any nested
// promotion: its place type's for a place area (admin_level 15), its admin
// level's for a boundary. A polygon of rank 0 is in no address and links no
// place node (find_linked_place, placex_triggers.sql:285).
inline uint8_t polygon_rank_address(uint16_t cc, uint8_t admin_level, uint8_t place_type) {
    return polygon_ranks(cc, admin_level, place_type).address;
}

// Admin-minimal carries addresses down to rank 16, the cities, towns and
// villages its place nodes also reach: boundaries of admin_level 2..8 and the
// place areas (admin_level 15) of those ranks, by the type that labels them (a
// place area linked to a town node is a town there, whatever its own place
// tag). Singapore's city is its place=city area (R17140517), not a boundary.
inline bool admin_minimal_keeps(uint8_t admin_level, uint8_t place_type) {
    if (admin_level >= 2 && admin_level <= 8) return true;
    const uint8_t rank = place_area_rank_address(place_type);

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
