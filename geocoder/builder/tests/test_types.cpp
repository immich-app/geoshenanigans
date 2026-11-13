// Unit tests for the record factories in types.h.
#include "types.h"

#include "test_framework.h"

// --- admin_polygon_tombstone ---

TEST(admin_polygon_tombstone_has_no_ring_block) {
    AdminPolygon t = admin_polygon_tombstone();
    CHECK_EQ(t.ring_offset, NO_DATA);
    CHECK_EQ(t.name_id, NO_DATA);
}
