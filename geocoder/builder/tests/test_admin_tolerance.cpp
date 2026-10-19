// Unit tests for admin_tolerance.h: which tolerance each quality variant
// simplifies an admin polygon with.
#include "admin_tolerance.h"

#include "test_framework.h"

static AdminPolygon admin_polygon(uint8_t level, AdminPlaceType place = AdminPlaceType::NONE) {
    AdminPolygon ap{};
    ap.admin_level = level;
    ap.place_type_override = static_cast<uint8_t>(place);
    return ap;
}

TEST(admin_tolerance_tier_uses_its_table_isotropically) {
    const AdminTolerance country = admin_tolerance(admin_polygon(2), kTierEpsilonScale);
    CHECK_NEAR(country.meters, 82, 1e-9);
    CHECK(country.isotropic);
    CHECK_NEAR(admin_tolerance(admin_polygon(6), kTierEpsilonScale).meters, 175, 1e-9);
    CHECK_NEAR(admin_tolerance(admin_polygon(8), kTierEpsilonScale).meters, 45, 1e-9);
}

TEST(admin_tolerance_tier_levels_past_the_table_take_its_last_entry) {
    // Place areas (level 15) share the postal codes' entry.
    CHECK_NEAR(admin_tolerance(admin_polygon(15), kTierEpsilonScale).meters, kTierAdminToleranceMeters[11], 1e-9);
}

TEST(admin_tolerance_tier_keeps_a_city_tolerance_for_a_named_city) {
    // China's county-level cities are level 6 with a linked place=city.
    CHECK_NEAR(admin_tolerance(admin_polygon(6, AdminPlaceType::CITY), kTierEpsilonScale).meters, kTierCityToleranceMeters, 1e-9);
    CHECK_NEAR(admin_tolerance(admin_polygon(5, AdminPlaceType::TOWN), kTierEpsilonScale).meters, kTierCityToleranceMeters, 1e-9);
    // Never coarser than the level's own: a level-8 village stays at 45 m.
    CHECK_NEAR(admin_tolerance(admin_polygon(8, AdminPlaceType::VILLAGE), kTierEpsilonScale).meters, 45, 1e-9);
    // Other place types keep the level's tolerance.
    CHECK_NEAR(admin_tolerance(admin_polygon(6, AdminPlaceType::COUNTY), kTierEpsilonScale).meters, 175, 1e-9);
}

TEST(admin_tolerance_other_scales_keep_the_level_epsilon_in_degrees) {
    const AdminTolerance q1 = admin_tolerance(admin_polygon(4, AdminPlaceType::CITY), 1.0);
    CHECK_NEAR(q1.meters, admin_epsilon_meters(4), 1e-9);
    CHECK(!q1.isotropic);
}
