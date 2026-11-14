// Unit tests for the record factories in types.h.
#include "types.h"

#include <cstring>
#include <initializer_list>
#include <string>

#include "test_framework.h"

namespace {

// The record's bytes as the file holds them.
template <class Record>
std::string bytes_of(const Record& r) {
    return std::string(reinterpret_cast<const char*>(&r), sizeof(r));
}

// Little-endian u32 words, as a record file holds them.
std::string words(std::initializer_list<uint32_t> ws) {
    std::string out;
    for (uint32_t w : ws) out.append(reinterpret_cast<const char*>(&w), sizeof(w));
    return out;
}

}  // namespace

// --- admin_polygon_tombstone ---

TEST(admin_polygon_tombstone_has_no_ring_block) {
    AdminPolygon t = admin_polygon_tombstone();
    CHECK_EQ(t.ring_offset, NO_DATA);
    CHECK_EQ(t.name_id, NO_DATA);
}

// --- street_way_tombstone, interp_way_tombstone, addr_point_tombstone ---

TEST(each_record_tombstone_is_the_fill_strategy_2_writes) {
    // (node_offset, node_count | pad, name_id)
    CHECK(bytes_of(street_way_tombstone()) == words({0, 0, NO_DATA}));
    // (node_offset, node_count | pad, street_id, start_number, end_number, interpolation | pad)
    CHECK(bytes_of(interp_way_tombstone()) == words({0, 0, NO_DATA, 0, 0, 0}));
    // (lat, lng, housenumber_id, street_id, parent_way_id, vertex_offset, vertex_count)
    CHECK(bytes_of(addr_point_tombstone()) == words({0, 0, 0, 0, NO_DATA, NO_DATA, 0}));
}
