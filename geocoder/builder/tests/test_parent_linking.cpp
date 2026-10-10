// parent_linking.h: helpers the parent-linking passes share.
#include "parent_linking.h"

#include <vector>

#include "test_framework.h"

namespace {

// Closed unit square in (lat, lng).
const std::vector<NodeCoord> kSquare = {{0, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 0}};

bool square_contains(float lat, float lng) {
    return ring_contains(kSquare.data(), static_cast<uint32_t>(kSquare.size()), lat, lng);
}

}  // namespace

TEST(ring_contains_interior_and_exterior) {
    CHECK(square_contains(0.5f, 0.5f));
    CHECK(!square_contains(1.5f, 0.5f));
    CHECK(!square_contains(0.5f, -0.5f));
}

TEST(ring_contains_keeps_south_and_west_edges_only) {
    CHECK(square_contains(0.0f, 0.5f));
    CHECK(square_contains(0.5f, 0.0f));
    CHECK(!square_contains(1.0f, 0.5f));
    CHECK(!square_contains(0.5f, 1.0f));
}

TEST(ring_contains_empty_ring_contains_nothing) {
    CHECK(!ring_contains(kSquare.data(), 0, 0.5f, 0.5f));
}
