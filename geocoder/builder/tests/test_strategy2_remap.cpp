// Unit tests for the strategy-2 remap helpers (strategy2_remap.h).
#include "strategy2_remap.h"

#include "test_framework.h"

// --- remap_cell_map / remap_cell_pairs ---

TEST(remap_cell_map_leaves_each_list_sorted_by_raw_value) {
    // Stable slots don't follow the canonical record order, so a remapped
    // list comes out unsorted unless the helper restores the order.
    const std::vector<uint32_t> remap = {5, 1, 3};
    std::unordered_map<uint64_t, std::vector<uint32_t>> cells = {{7, {0, 1, 2}}};
    remap_cell_map(cells, [&](uint32_t& v) { remap_index(v, remap); });
    CHECK(cells[7] == std::vector<uint32_t>({1, 3, 5}));

    std::unordered_map<uint64_t, std::vector<uint32_t>> flagged = {{7, {0 | INTERIOR_FLAG, 1, 2}}};
    remap_cell_map(flagged, [&](uint32_t& v) { remap_index_flagged(v, remap); });
    CHECK(flagged[7] == std::vector<uint32_t>({1, 3, 5 | INTERIOR_FLAG}));
}

TEST(remap_cell_pairs_restores_cell_then_item_order) {
    const std::vector<uint32_t> remap = {5, 1, 3};
    std::vector<CellItemPair> pairs = {{2, 0}, {2, 1}, {4, 2}, {4, 0}};
    remap_cell_pairs(pairs, [&](uint32_t& v) { remap_index(v, remap); });
    const std::vector<std::pair<uint64_t, uint32_t>> want = {{2, 1}, {2, 5}, {4, 3}, {4, 5}};
    REQUIRE(pairs.size() == want.size());
    for (size_t i = 0; i < want.size(); i++) {
        CHECK_EQ(pairs[i].cell_id, want[i].first);
        CHECK_EQ(pairs[i].item_id, want[i].second);
    }
}
