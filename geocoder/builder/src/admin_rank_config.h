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

inline uint8_t admin_rank_address(uint16_t cc, uint8_t admin_level) {
    // Country overrides (packed uppercase 'XY').
    constexpr auto pack = [](char a, char b) -> uint16_t {
        return static_cast<uint16_t>((a << 8) | b);
    };
    switch (cc) {
        case pack('A','U'): if (admin_level == 6) return 0; break;
        case pack('B','E'): switch (admin_level) {
            case 3: return 0; case 4: return 6; case 5: return 0;
            case 6: return 8; case 7: return 12; case 8: return 14;
            case 9: return 16; case 10: return 18; default: break; } break;
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
        case pack('S','K'): switch (admin_level) {
            case 5: return 0; case 6: return 11; case 7: return 0;
            case 8: return 12; case 9: return 16; case 10: return 18;
            case 11: return 20; default: break; } break;
        default: break;
    }
    // Default: rank_address = admin_level * 2 for levels 2..12.
    if (admin_level >= 2 && admin_level <= 12)
        return static_cast<uint8_t>(admin_level * 2);
    return 0;
}
