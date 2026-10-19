// The tolerance a quality variant simplifies an admin polygon's rings with
// (cell_index.cpp pack_admin_polygon).
#pragma once

#include <algorithm>
#include <cstdint>

#include "geometry.h"
#include "types.h"

// The recommended tier: quality/q2.5, which admin-minimal also uses. 2.5 x
// kAdminEpsilonMeters put country and state borders at 1250 and 250 m, where
// the answer changed for 35% and 15% of points within 200 m of them, while
// municipalities got 37.5 m. The tier gives country and state about the
// municipal tolerance instead, measured in metres in every direction, and
// pays with levels 3, 5 and 6, which seldom name an Immich field. Planet
// 2026-10-18 (tiers3 study): 0.3% smaller than q2.5 with the ring cap,
// country 36% -> 4.3% and state 15% -> 4.7% changed within 200 m of their
// borders, city near municipal borders no worse.
inline constexpr double kTierEpsilonScale = 2.5;
inline constexpr double kTierAdminToleranceMeters[12] = {
    37.5, 37.5, 82, 400, 90, 175,
    175, 54, 45, 42, 46, 47,
};
// A polygon the reader names as a city, town or village (a linked place,
// e.g. China's county-level cities at level 6) keeps at most this, whatever
// its level.
inline constexpr double kTierCityToleranceMeters = 50;

struct AdminTolerance {
    double meters;
    bool isotropic;  // simplify_ring_isotropic; else simplify_polygon_epsilon in raw degrees
};

inline AdminTolerance admin_tolerance(const AdminPolygon& ap, double epsilon_scale) {
    if (epsilon_scale != kTierEpsilonScale) return {admin_epsilon_meters(ap.admin_level) * epsilon_scale, false};
    double meters = kTierAdminToleranceMeters[std::min<int>(ap.admin_level, 11)];
    const auto place = static_cast<AdminPlaceType>(ap.place_type_override);
    if (place == AdminPlaceType::CITY || place == AdminPlaceType::TOWN || place == AdminPlaceType::VILLAGE)
        meters = std::min(meters, kTierCityToleranceMeters);
    return {meters, true};
}
