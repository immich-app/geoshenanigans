// Unit tests for the record factories in types.h.
#include "types.h"

#include "test_framework.h"

// --- admin_polygon_tombstone ---

TEST(admin_polygon_tombstone_has_no_vertex_block) {
    // geocoder-diff sizes a polygon's vertex block as the gap to the next
    // non-NO_DATA vertex_offset; a tombstone at offset 0 claimed every byte
    // before the next polygon (oceania q1 patch: 24.3 MB out vs 13.3 MB).
    AdminPolygon t = admin_polygon_tombstone();
    CHECK_EQ(t.vertex_offset, NO_DATA);
    CHECK_EQ(t.vertex_count, 0u);
    CHECK_EQ(t.name_id, NO_DATA);
}
