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
}

TEST(place_area_ranks_follow_address_levels) {
    CHECK_EQ(place_area_rank_address(pt(AdminPlaceType::CITY)), uint8_t(16));
    CHECK_EQ(place_area_rank_address(pt(AdminPlaceType::QUARTER)), uint8_t(22));
    CHECK_EQ(place_area_rank_address(pt(AdminPlaceType::NEIGHBOURHOOD)), uint8_t(24));
    CHECK_EQ(place_area_rank_address(pt(AdminPlaceType::STATE)), uint8_t(0));
}
