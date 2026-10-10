// Cell index writers (cell_index_io.h): the sorted-table writer must emit the
// same bytes as the map writer it stands in for.
#include "cell_index_io.h"

#include <fstream>
#include <iterator>
#include <random>
#include <string>

#include "scratch_dir.h"
#include "test_framework.h"

namespace {

std::string read_file(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

}  // namespace

TEST(write_cell_index_sorted_matches_the_map_writer) {
    ScratchDir dir("gctest-cellio");
    std::mt19937_64 rng(4);
    for (int round = 0; round < 20; round++) {
        // Cells with one or many items, repeated items and flagged ones.
        std::vector<CellItemPair> pairs;
        size_t cells = round == 0 ? 0 : 1 + rng() % 300;
        for (size_t c = 0; c < cells; c++) {
            uint64_t cell_id = (rng() % 100000) << 20;
            size_t k = 1 + (rng() % 10 == 0 ? rng() % 400 : rng() % 4);
            for (size_t j = 0; j < k; j++) {
                uint32_t item = static_cast<uint32_t>(rng() % 50);
                pairs.push_back({cell_id, rng() % 5 == 0 ? (item | INTERIOR_FLAG) : item});
            }
        }
        std::sort(pairs.begin(), pairs.end(), cell_item_less);
        std::unordered_map<uint64_t, std::vector<uint32_t>> map;
        for (const auto& p : pairs) map[p.cell_id].push_back(p.item_id);

        CHECK_EQ(count_cells(pairs), map.size());
        write_cell_index(dir.path() + "/map_cells.bin", dir.path() + "/map_entries.bin", map);
        write_cell_index_sorted(dir.path() + "/cells.bin", dir.path() + "/entries.bin", pairs);
        CHECK(read_file(dir.path() + "/cells.bin") == read_file(dir.path() + "/map_cells.bin"));
        CHECK(read_file(dir.path() + "/entries.bin") == read_file(dir.path() + "/map_entries.bin"));
    }
}

TEST(write_cell_index_sorted_rejects_a_table_out_of_order) {
    ScratchDir dir("gctest-cellio");
    bool thrown = false;
    try {
        write_cell_index_sorted(dir.path() + "/cells.bin", dir.path() + "/entries.bin", {{9, 1}, {3, 1}});
    } catch (const std::logic_error&) {
        thrown = true;
    }
    CHECK(thrown);
}

TEST(write_cell_index_sorted_rejects_a_cell_past_the_count_limit) {
    ScratchDir dir("gctest-cellio");
    std::vector<CellItemPair> pairs(70000, CellItemPair{7, 1});
    bool thrown = false;
    try {
        write_cell_index_sorted(dir.path() + "/cells.bin", dir.path() + "/entries.bin", pairs);
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
}
