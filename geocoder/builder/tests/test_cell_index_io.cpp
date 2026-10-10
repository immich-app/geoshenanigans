// Cell index writers (cell_index_io.h): the sorted-table writer must emit the
// same bytes as the map writer it stands in for, and the streamed geo index
// the bytes the whole-array writer laid out.
#include "cell_index_io.h"

#include <array>
#include <cstdio>
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

template <class T>
void append(std::string& out, const T& v) {
    out.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

// The geo index as the whole-array writer built it: the union of the tables'
// cells, then each table's entries and each cell's offsets into them.
struct GeoFiles {
    std::string geo;
    std::array<std::string, GEO_TABLE_COUNT> entries;
};
GeoFiles reference_geo_index(const GeoTables& tables) {
    std::vector<uint64_t> cells;
    for (const auto* table : tables)
        if (table)
            for (const auto& p : *table) cells.push_back(p.cell_id);
    std::sort(cells.begin(), cells.end());
    cells.erase(std::unique(cells.begin(), cells.end()), cells.end());
    GeoFiles out;
    std::array<size_t, GEO_TABLE_COUNT> next{};
    for (uint64_t cell : cells) {
        uint32_t offsets[GEO_TABLE_COUNT] = {NO_DATA, NO_DATA, NO_DATA};
        for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
            if (!tables[t]) continue;
            const auto& table = *tables[t];
            if (next[t] >= table.size() || table[next[t]].cell_id != cell) continue;
            offsets[t] = static_cast<uint32_t>(out.entries[t].size());
            size_t run = next[t];
            while (run < table.size() && table[run].cell_id == cell) run++;
            append(out.entries[t], static_cast<uint16_t>(run - next[t]));
            for (; next[t] < run; next[t]++) append(out.entries[t], table[next[t]].item_id);
        }
        append(out.geo, cell);
        for (uint32_t off : offsets) append(out.geo, off);
    }
    return out;
}

// A cell-grouped table of `cells` random cells, ids in no particular order.
std::vector<CellItemPair> random_table(std::mt19937_64& rng, size_t cells) {
    std::vector<CellItemPair> pairs;
    for (size_t c = 0; c < cells; c++) {
        uint64_t cell_id = (rng() % 2000) << 20;
        size_t k = 1 + (rng() % 10 == 0 ? rng() % 200 : rng() % 3);
        for (size_t j = 0; j < k; j++) pairs.push_back({cell_id, static_cast<uint32_t>(rng() % 5000)});
    }
    std::stable_sort(pairs.begin(), pairs.end(),
                     [](const CellItemPair& a, const CellItemPair& b) { return a.cell_id < b.cell_id; });
    return pairs;
}

}  // namespace

TEST(write_geo_index_matches_the_whole_array_layout) {
    ScratchDir dir("gctest-cellio");
    std::mt19937_64 rng(11);
    for (int round = 0; round < 40; round++) {
        std::array<std::vector<CellItemPair>, GEO_TABLE_COUNT> data;
        for (auto& table : data) table = random_table(rng, rng() % 4 == 0 ? 0 : rng() % 400);
        GeoTables tables{&data[0], &data[1], &data[2]};
        if (round % 3 == 0) tables[1] = tables[2] = nullptr;  // no-addresses: streets only
        const size_t per_slice = round % 5 == 0 ? 1 : 1 + rng() % 300;

        const GeoFiles want = reference_geo_index(tables);
        size_t rows = write_geo_index(dir.path(), tables, per_slice);
        CHECK_EQ(rows, want.geo.size() / 20);
        CHECK(read_file(dir.path() + "/geo_cells.bin") == want.geo);
        for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
            const std::string path = dir.path() + "/" + GEO_ENTRY_FILENAMES[t];
            if (tables[t]) CHECK(read_file(path) == want.entries[t]);
            std::remove(path.c_str());
        }
    }
}

TEST(write_geo_index_rejects_a_cell_past_the_count_limit_before_writing) {
    ScratchDir dir("gctest-cellio");
    std::vector<CellItemPair> ways{{3, 1}}, addrs(70000, CellItemPair{7, 1});
    bool thrown = false;
    try {
        write_geo_index(dir.path(), {&ways, &addrs, nullptr});
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
    CHECK(!std::ifstream(dir.path() + "/geo_cells.bin").good());
}

TEST(pairs_in_list_order_keeps_each_lists_order) {
    std::unordered_map<uint64_t, std::vector<uint32_t>> map{{9, {5, 1}}, {2, {7}}, {4, {3, 3, 0}}};
    std::vector<CellItemPair> pairs = pairs_in_list_order(map);
    std::vector<std::pair<uint64_t, uint32_t>> got;
    for (const auto& p : pairs) got.push_back({p.cell_id, p.item_id});
    CHECK((got == std::vector<std::pair<uint64_t, uint32_t>>{{2, 7}, {4, 3}, {4, 3}, {4, 0}, {9, 5}, {9, 1}}));
}

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
