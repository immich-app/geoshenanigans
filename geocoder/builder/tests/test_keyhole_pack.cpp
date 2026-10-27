// keyhole_pack.h: an admin polygon's keyhole range packed so that the rings
// the reader decodes still nest.
#include "keyhole_pack.h"

#include <vector>

#include "ring_contains.h"
#include "test_framework.h"

namespace {

using Ring = std::vector<LatLng>;

Ring square(double lat0, double lng0, double lat1, double lng1) {
    return {{lat0, lng0}, {lat0, lng1}, {lat1, lng1}, {lat1, lng0}, {lat0, lng0}};
}

// The reader's answer for (lat, lng) over the vertices it decodes.
bool decoded_contains(const std::vector<NodeCoord>& v, double lat, double lng) {
    return ring_contains(v.data(), static_cast<uint32_t>(v.size()), static_cast<float>(lat), static_cast<float>(lng));
}

// plan_polygon's bytes for the range.
std::vector<uint8_t> planned_bytes(const Ring& range) {
    std::vector<uint8_t> bytes(plan_polygon(range.data(), range.size()).bytes);
    pack_polygon_at(bytes.data(), range.data(), range.size());
    return bytes;
}

}  // namespace

TEST(pack_keyhole_keeps_a_hole_beside_a_slanted_outer_edge_inside_once_decoded) {
    // The outer triangle's diagonal runs along lat + lng = 1.00004 and the
    // hole's tip lies 1e-5 inside it. Its 1.00004 span packs on the 1e-4
    // grid, which rounds the diagonal to lat + lng = 1 and the tip to
    // (0.5001, 0.5), outside it: the crossing test would count the sliver
    // of the hole beyond the diagonal as inside.
    const Ring outer = {{0, 0}, {0, 1.00004}, {1.00004, 0}, {0, 0}};
    const Ring hole = {{0.50006, 0.49997}, {0.49, 0.49}, {0.48, 0.49}, {0.50006, 0.49997}};
    const Ring range = encode_rings(outer, {hole});
    REQUIRE(rings_nest(outer, {hole}));
    REQUIRE(plan_polygon(range.data(), range.size()).enc == VertexEncoding::U16_11M);
    const std::vector<NodeCoord> rounded = unpack_polygon(planned_bytes(range).data(), range.size());
    REQUIRE(!range_nests(rounded.data(), static_cast<uint32_t>(rounded.size()), 1));
    REQUIRE(decoded_contains(rounded, 0.50007, 0.49998));  // the sliver, outside the given outer ring

    const PackedKeyhole packed = pack_keyhole(range, 1);
    CHECK(packed.grid == KeyholeGrid::Finest);
    CHECK_EQ(packed.bytes[0], static_cast<uint8_t>(VertexEncoding::U32_1CM));
    const std::vector<NodeCoord> decoded = unpack_polygon(packed.bytes.data(), range.size());
    CHECK(range_nests(decoded.data(), static_cast<uint32_t>(decoded.size()), 1));
    CHECK(!decoded_contains(decoded, 0.50007, 0.49998));
    CHECK(!decoded_contains(decoded, 0.49002, 0.49332));  // in the hole
    CHECK(decoded_contains(decoded, 0.2, 0.2));
    CHECK(!decoded_contains(decoded, 0.6, 0.6));
}

TEST(pack_keyhole_keeps_plan_polygons_grid_while_the_decoded_rings_nest) {
    const Ring outer = square(10, 20, 10.5, 20.5);
    const Ring range = encode_rings(outer, {square(10.1, 20.1, 10.2, 20.2)});
    const PackedKeyhole packed = pack_keyhole(range, 1);
    CHECK(packed.grid == KeyholeGrid::Planned);
    CHECK(packed.bytes == planned_bytes(range));
    const PackedKeyhole no_holes = pack_keyhole(outer, 0);
    CHECK(no_holes.grid == KeyholeGrid::Planned);
    CHECK(no_holes.bytes == planned_bytes(outer));
}

TEST(pack_keyhole_leaves_rings_that_do_not_nest_unrounded_on_plan_polygons_grid) {
    // The hole crosses the outer ring's top edge as given.
    const Ring range = encode_rings(square(10, 20, 10.5, 20.5), {square(10.4, 20.1, 10.6, 20.2)});
    const PackedKeyhole packed = pack_keyhole(range, 1);
    CHECK(packed.grid == KeyholeGrid::Unnested);
    CHECK(packed.bytes == planned_bytes(range));
}

TEST(pack_keyhole_tells_rings_the_finest_grid_cannot_keep_nesting) {
    // A 20 degree triangle packs on the 1 cm grid already; its decode rounds
    // the hole's tip, 1e-8 inside the diagonal from (0, 20) to (17, 0), to
    // the f32 (3.40051007, 15.99940014), just outside it.
    const Ring outer = {{0, 0}, {0, 20}, {17, 0}, {0, 0}};
    const Ring hole = {{3.40050999, 15.99939999}, {2.4, 15.5}, {2.4, 15}, {3.40050999, 15.99939999}};
    const Ring range = encode_rings(outer, {hole});
    REQUIRE(rings_nest(outer, {hole}));
    REQUIRE(plan_polygon(range.data(), range.size()).enc == VertexEncoding::U32_1CM);
    const PackedKeyhole packed = pack_keyhole(range, 1);
    CHECK(packed.grid == KeyholeGrid::Broken);
    CHECK(packed.bytes == planned_bytes(range));
}

TEST(keyhole_grid_counts_tell_the_grids_apart) {
    KeyholeGridCounts counts;
    for (KeyholeGrid grid : {KeyholeGrid::Planned, KeyholeGrid::Finest, KeyholeGrid::Finest, KeyholeGrid::Broken,
                             KeyholeGrid::Unnested, KeyholeGrid::Unnested, KeyholeGrid::Unnested})
        counts.add(grid);
    CHECK_EQ(counts.summary(),
             std::string("2 packed on the 1 cm grid to keep nesting, 1 not nesting there either, 3 not nesting unrounded"));
}
