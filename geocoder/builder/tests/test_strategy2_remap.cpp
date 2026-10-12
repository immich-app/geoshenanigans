// Unit tests for the strategy-2 remap helpers (strategy2_remap.h).
#include "strategy2_remap.h"

#include <algorithm>
#include <numeric>
#include <random>

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

namespace {

// remap_cell_pairs as it was: remap every item, then one global sort.
std::vector<CellItemPair> remap_and_sort(std::vector<CellItemPair> pairs, const std::vector<uint32_t>& remap) {
    for (auto& p : pairs) remap_index_flagged(p.item_id, remap);
    std::sort(pairs.begin(), pairs.end(), cell_item_less);
    return pairs;
}

bool same_pairs(const std::vector<CellItemPair>& a, const std::vector<CellItemPair>& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); i++)
        if (a[i].cell_id != b[i].cell_id || a[i].item_id != b[i].item_id) return false;
    return true;
}

}  // namespace

TEST(remap_cell_pairs_matches_a_global_sort_on_large_tables) {
    // Enough pairs that every worker gets cells, some flagged items, and one
    // cell longer than a worker's share.
    std::mt19937 rng(5);
    const uint32_t n_items = 50000;
    std::vector<uint32_t> remap(n_items);
    std::iota(remap.begin(), remap.end(), 0u);
    std::shuffle(remap.begin(), remap.end(), rng);
    std::vector<CellItemPair> pairs;
    for (uint64_t cell = 1; pairs.size() < 400000; cell++) {
        size_t k = cell == 77 ? 120000 : 1 + rng() % 6;
        for (size_t j = 0; j < k; j++) {
            uint32_t item = static_cast<uint32_t>(rng() % n_items);
            pairs.push_back({cell * 16, rng() % 9 == 0 ? (item | INTERIOR_FLAG) : item});
        }
    }
    std::sort(pairs.begin(), pairs.end(), cell_item_less);
    auto want = remap_and_sort(pairs, remap);
    remap_cell_pairs(pairs, [&](uint32_t& v) { remap_index_flagged(v, remap); });
    CHECK(same_pairs(pairs, want));
}

TEST(reorder_by_remap_moves_values_to_their_slots_and_fills_the_rest) {
    std::vector<uint32_t> values = {10, 11, 12};
    reorder_by_remap(values, {4, 0, 2}, 6, NO_DATA);
    CHECK(values == std::vector<uint32_t>({11, NO_DATA, 12, NO_DATA, 10, NO_DATA}));

    std::vector<uint32_t> shorter = {7};  // values past the end keep the fill
    reorder_by_remap(shorter, {1, 0}, 2, 0u);
    CHECK(shorter == std::vector<uint32_t>({0, 7}));
}

TEST(repack_nodes_lays_nodes_out_in_record_order) {
    // Record order 2, 0, 1 after a reorder; record 3 has no nodes and keeps
    // its offset; record 4 runs past the array and comes out empty.
    std::vector<NodeCoord> nodes = {{0, 0}, {0, 1}, {1, 0}, {1, 1}, {1, 2}, {2, 0}};
    std::vector<WayHeader> ways(5);
    ways[0] = {5, 1, 0, 0};
    ways[1] = {0, 2, 0, 0};
    ways[2] = {2, 3, 0, 0};
    ways[3] = {9, 0, 0, 0};
    ways[4] = {4, 3, 0, 0};
    repack_nodes(ways, nodes);
    CHECK_EQ(nodes.size(), size_t(6));
    CHECK_EQ(ways[0].node_offset, 0u);
    CHECK_EQ(ways[1].node_offset, 1u);
    CHECK_EQ(ways[2].node_offset, 3u);
    CHECK_EQ(ways[3].node_offset, 9u);
    CHECK_EQ(ways[4].node_offset, 0u);
    CHECK_EQ(ways[4].node_count, 0);
    CHECK_EQ(nodes[0].lat, 2.0f);
    CHECK_EQ(nodes[1].lat, 0.0f);
    CHECK_EQ(nodes[2].lng, 1.0f);
    CHECK_EQ(nodes[5].lng, 2.0f);
}

TEST(remap_cell_pairs_sorts_a_table_not_grouped_by_cell) {
    const std::vector<uint32_t> remap = {2, 0, 1};
    std::vector<CellItemPair> pairs = {{9, 0}, {3, 1}, {9, 2}, {3, 0}};
    auto want = remap_and_sort(pairs, remap);
    remap_cell_pairs(pairs, [&](uint32_t& v) { remap_index_flagged(v, remap); });
    CHECK(same_pairs(pairs, want));
}
