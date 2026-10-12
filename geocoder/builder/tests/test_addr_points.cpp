// grow_addr_points / put_addr_point: the address arrays grow together.
#include "parsed_data.h"

#include <stdexcept>

#include "test_framework.h"

TEST(grow_addr_points_grows_every_parallel_array_and_returns_the_first_slot) {
    ParsedData d;
    CHECK_EQ(grow_addr_points(d, 2), size_t(0));
    put_addr_point(d, 1, 1.5f, 2.5f, 7, NO_DATA, 9, 42, 1001);
    CHECK_EQ(grow_addr_points(d, 3), size_t(2));
    CHECK_EQ(d.addr_points.size(), size_t(5));
    CHECK_EQ(d.addr_osm_ids.size(), size_t(5));
    CHECK_EQ(d.addr_postcode_ids.size(), size_t(5));
    CHECK_EQ(d.addr_cells.size(), size_t(5));
    CHECK_EQ(d.addr_points[1].housenumber_id, 7u);
    CHECK_EQ(d.addr_points[1].parent_way_id, NO_DATA);
    CHECK_EQ(d.addr_osm_ids[1], uint64_t(1001));
    CHECK_EQ(d.addr_postcode_ids[1], 9u);
    CHECK_EQ(d.addr_cells[1], uint64_t(42));
}

TEST(grow_addr_points_refuses_parallel_arrays_of_different_lengths) {
    ParsedData d;
    grow_addr_points(d, 2);
    d.addr_cells.pop_back();
    bool thrown = false;
    try {
        grow_addr_points(d, 1);
    } catch (const std::logic_error&) {
        thrown = true;
    }
    CHECK(thrown);
}
