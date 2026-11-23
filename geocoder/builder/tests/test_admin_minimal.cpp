// Unit tests for admin_minimal_keeps (admin_rank_config.h): which admin
// polygons the smallest set carries.
#include "admin_rank_config.h"

#include "test_framework.h"

namespace {
uint8_t pt(AdminPlaceType t) { return static_cast<uint8_t>(t); }
}

TEST(admin_minimal_keeps_boundaries_down_to_level_8) {
    for (uint8_t level = 2; level <= 8; level++) CHECK(admin_minimal_keeps(level, 0));
    CHECK(!admin_minimal_keeps(9, 0));
    CHECK(!admin_minimal_keeps(10, 0));
    CHECK(!admin_minimal_keeps(11, 0));  // postal
}

TEST(admin_minimal_keeps_place_areas_down_to_rank_16) {
    CHECK(admin_minimal_keeps(15, pt(AdminPlaceType::CITY)));  // Singapore R17140517
    CHECK(admin_minimal_keeps(15, pt(AdminPlaceType::TOWN)));
    CHECK(admin_minimal_keeps(15, pt(AdminPlaceType::VILLAGE)));
    CHECK(admin_minimal_keeps(15, pt(AdminPlaceType::MUNICIPALITY)));
    CHECK(admin_minimal_keeps(15, pt(AdminPlaceType::COUNTY)));
    CHECK(!admin_minimal_keeps(15, pt(AdminPlaceType::BOROUGH)));
    CHECK(!admin_minimal_keeps(15, pt(AdminPlaceType::SUBURB)));
    CHECK(!admin_minimal_keeps(15, pt(AdminPlaceType::HAMLET)));
    CHECK(!admin_minimal_keeps(15, pt(AdminPlaceType::NEIGHBOURHOOD)));
    CHECK(!admin_minimal_keeps(15, pt(AdminPlaceType::REGION)));  // rank 0
    CHECK(!admin_minimal_keeps(15, pt(AdminPlaceType::NONE)));
    // place=province ranks 10 in Spain and 12 in Saudi Arabia, 0 elsewhere,
    // where the server leaves it out of addresses.
    CHECK(admin_minimal_keeps(15, pt(AdminPlaceType::PROVINCE)));
    CHECK(!admin_minimal_keeps(15, pt(AdminPlaceType::STATE)));
}

TEST(place_areas_of_rank_0_everywhere_are_no_address_areas) {
    CHECK(is_address_place_type(AdminPlaceType::CITY));
    CHECK(is_address_place_type(AdminPlaceType::COUNTY));
    CHECK(is_address_place_type(AdminPlaceType::NEIGHBOURHOOD));
    CHECK(!is_address_place_type(AdminPlaceType::STATE));
    CHECK(!is_address_place_type(AdminPlaceType::REGION));  // e.g. Mashriq
    CHECK(!is_address_place_type(AdminPlaceType::NONE));
    // place=province ranks 10 in Spain and 12 in Saudi Arabia.
    CHECK(is_address_place_type(AdminPlaceType::PROVINCE));
}

TEST(place_area_ranks_follow_the_country_levels) {
    const uint16_t ca = pack_country_code('C', 'A'), de = pack_country_code('D', 'E'), es = pack_country_code('E', 'S');
    const uint16_t sa = pack_country_code('S', 'A'), ru = pack_country_code('R', 'U'), gb = pack_country_code('G', 'B');
    CHECK_EQ(place_area_rank_address(ca, pt(AdminPlaceType::COUNTY)), uint8_t(0));
    CHECK_EQ(place_area_rank_address(de, pt(AdminPlaceType::COUNTY)), uint8_t(0));
    CHECK_EQ(place_area_rank_address(de, pt(AdminPlaceType::REGION)), uint8_t(0));
    CHECK_EQ(place_area_rank_address(es, pt(AdminPlaceType::PROVINCE)), uint8_t(10));
    CHECK_EQ(place_area_rank_address(sa, pt(AdminPlaceType::PROVINCE)), uint8_t(12));
    CHECK_EQ(place_area_rank_address(sa, pt(AdminPlaceType::MUNICIPALITY)), uint8_t(18));
    CHECK_EQ(place_area_rank_address(ru, pt(AdminPlaceType::MUNICIPALITY)), uint8_t(18));
    CHECK_EQ(place_area_rank_address(gb, pt(AdminPlaceType::COUNTY)), uint8_t(12));
    CHECK_EQ(place_area_rank_address(gb, pt(AdminPlaceType::PROVINCE)), uint8_t(0));
    CHECK_EQ(place_area_rank_address(0, pt(AdminPlaceType::CITY)), uint8_t(16));
    // A boundary's is its level's.
    CHECK_EQ(polygon_rank_address(pack_country_code('A', 'U'), 6, pt(AdminPlaceType::MUNICIPALITY)), uint8_t(0));
    CHECK_EQ(polygon_rank_address(gb, 6, 0), uint8_t(12));
    CHECK_EQ(polygon_rank_address(ca, 15, pt(AdminPlaceType::COUNTY)), uint8_t(0));
}

TEST(place_area_ranks_follow_address_levels) {
    CHECK_EQ(place_area_rank_address(pt(AdminPlaceType::CITY)), uint8_t(16));
    CHECK_EQ(place_area_rank_address(pt(AdminPlaceType::QUARTER)), uint8_t(22));
    CHECK_EQ(place_area_rank_address(pt(AdminPlaceType::NEIGHBOURHOOD)), uint8_t(24));
    CHECK_EQ(place_area_rank_address(pt(AdminPlaceType::STATE)), uint8_t(0));
}

TEST(boundary_ranks_follow_the_country_levels) {
    // Nominatim's address-levels.json: Slovakia has no levels of its own (since 2026-05-02), and
    // Brazil's administrative5 to 7 rank 0 (since 5.3.0); Australia's administrative6 too.
    const uint16_t sk = pack_country_code('S', 'K'), br = pack_country_code('B', 'R');
    for (uint8_t level = 2; level <= 10; level++)
        CHECK_EQ(admin_rank_address(sk, level), uint8_t(level * 2));
    CHECK_EQ(admin_rank_address(br, 4), uint8_t(8));
    for (uint8_t level = 5; level <= 7; level++) CHECK_EQ(admin_rank_address(br, level), uint8_t(0));
    CHECK_EQ(admin_rank_address(br, 8), uint8_t(16));
    CHECK_EQ(admin_rank_address(pack_country_code('A', 'U'), 6), uint8_t(0));
}
