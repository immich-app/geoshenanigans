// Cell index writers (cell_index_io.h): the sorted-table writer must emit the
// same bytes as the map writer it stands in for, both the cell pair files
// cell_pairs::Writer gives for the same lists (the patcher's), and the geo
// index the cores encode in parallel the bytes geo_index::Writer gives for
// its rows (the patcher's).
#include "cell_index_io.h"

#include <array>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <random>
#include <string>

#include "geo_index.h"
#include "patch_format.h"
#include "scratch_dir.h"
#include "test_framework.h"

using gctest::refusal_of;

namespace {

std::string file_text(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

// The geo index of the tables (geo_index.h) as the streaming Writer writes
// it, the patcher's: the union of the tables' cells, each with every table's
// items there.
std::pair<std::string, std::string> reference_geo_index(const GeoTables& tables) {
    std::vector<uint64_t> cells;
    for (const auto* table : tables)
        if (table)
            for (const auto& p : *table) cells.push_back(p.cell_id);
    std::sort(cells.begin(), cells.end());
    cells.erase(std::unique(cells.begin(), cells.end()), cells.end());
    std::pair<std::string, std::string> out;
    geo_index::Writer writer([&](const char* p, size_t n) { out.first.append(p, n); },
                             [&](const char* p, size_t n) { out.second.append(p, n); });
    std::array<size_t, GEO_TABLE_COUNT> next{};
    for (uint64_t cell : cells) {
        geo_index::RowLists lists;
        for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
            if (!tables[t]) continue;
            const auto& table = *tables[t];
            for (; next[t] < table.size() && table[next[t]].cell_id == cell; next[t]++) lists[t].push_back(table[next[t]].item_id);
        }
        writer.add(cell, geo_index::ids_of(lists));
    }
    writer.finish();
    return out;
}

// A cell-grouped table of `cells` random level-17 cells in canonical order
// (by cell, then item), a few with many items, some repeated.
std::vector<CellItemPair> random_table(std::mt19937_64& rng, size_t cells) {
    std::vector<CellItemPair> pairs;
    for (size_t c = 0; c < cells; c++) {
        uint64_t cell_id = geo_index::cell_of(rng() % 20000, 17);
        size_t k = 1 + (rng() % 10 == 0 ? rng() % 200 : rng() % 3);
        for (size_t j = 0; j < k; j++) pairs.push_back({cell_id, static_cast<uint32_t>(rng() % 5000)});
    }
    std::sort(pairs.begin(), pairs.end(), cell_item_less);
    return pairs;
}

}  // namespace

// The builder's cores encode blocks across slices of every size (down to a
// pair a slice, slices of no rows and blocks over many slices): the bytes
// are the streaming Writer's for the same rows.
TEST(write_geo_index_writes_the_streaming_writers_bytes) {
    ScratchDir dir("gctest-cellio");
    std::mt19937_64 rng(11);
    for (int round = 0; round < 40; round++) {
        std::array<std::vector<CellItemPair>, GEO_TABLE_COUNT> data;
        for (auto& table : data) table = random_table(rng, rng() % 4 == 0 ? 0 : rng() % 3000);
        GeoTables tables{&data[0], &data[1], &data[2]};
        if (round % 3 == 0) tables[1] = tables[2] = nullptr;  // no-addresses: streets only
        const size_t per_slice = round % 5 == 0 ? 1 : 1 + rng() % 300;

        const auto want = reference_geo_index(tables);
        size_t rows = write_geo_index(dir.path(), tables, per_slice);
        const geo_index::View view(want.first.data(), want.first.size(), want.second.data(), want.second.size());
        CHECK_EQ(uint64_t(rows), view.rows());
        CHECK(file_text(dir.path() + "/geo_cells.bin") == want.first);
        CHECK(file_text(dir.path() + "/geo_entries.bin") == want.second);
    }
}

TEST(write_geo_index_rejects_a_cell_past_the_count_limit_before_writing) {
    ScratchDir dir("gctest-cellio");
    const uint64_t c = geo_index::cell_of(3, 17), d = geo_index::cell_of(7, 17);
    std::vector<CellItemPair> ways{{c, 1}}, addrs(70000, CellItemPair{d, 1});
    CHECK_EQ(refusal_of([&] { write_geo_index(dir.path(), {&ways, &addrs, nullptr}); }),
             "cell entry count 70000 exceeds uint16 limit (65535) in " + dir.path() + "/geo_entries.bin (address lists)");
    CHECK(!std::ifstream(dir.path() + "/geo_cells.bin").good());
}

TEST(write_geo_index_rejects_a_table_out_of_order) {
    ScratchDir dir("gctest-cellio");
    const uint64_t c = geo_index::cell_of(3, 17);
    std::vector<CellItemPair> ways{{c, 5}, {c, 2}};
    CHECK_EQ(refusal_of<std::logic_error>([&] { write_geo_index(dir.path(), {&ways, nullptr, nullptr}); }),
             std::string("write_geo_index: the street table is out of order"));
}

// The level-10 cell of number n: a cell pair index holds cells of one level.
static uint64_t cell10(uint64_t n) { return cell_pairs::cell_of(n, 10); }

TEST(write_cell_index_sorted_matches_the_map_writer) {
    ScratchDir dir("gctest-cellio");
    std::mt19937_64 rng(4);
    for (int round = 0; round < 20; round++) {
        // Cells with one or many items, repeated items and flagged ones.
        std::vector<CellItemPair> pairs;
        size_t cells = round == 0 ? 0 : 1 + rng() % 300;
        for (size_t c = 0; c < cells; c++) {
            uint64_t cell_id = cell10(rng() % 100000);
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
        for (cell_pairs::Format format : {cell_pairs::ADMIN, cell_pairs::POI, cell_pairs::POSTCODE_CENTROID}) {
            write_cell_index(dir.path() + "/map_cells.bin", dir.path() + "/map_entries.bin", format, map);
            write_cell_index_sorted(dir.path() + "/cells.bin", dir.path() + "/entries.bin", format, pairs);
            CHECK(file_text(dir.path() + "/cells.bin") == file_text(dir.path() + "/map_cells.bin"));
            CHECK(file_text(dir.path() + "/entries.bin") == file_text(dir.path() + "/map_entries.bin"));
        }
    }
}

TEST(write_cell_index_writers_write_the_cell_pair_files_of_their_lists) {
    // Past the blocks the writers split their checks into: the files a
    // cell_pairs::Writer writes for the same lists, ids sorted within each
    // cell, which the patcher's rebuild writes too.
    ScratchDir dir("gctest-cellio");
    std::mt19937_64 rng(6);
    std::vector<CellItemPair> pairs;
    for (uint64_t cell = 1; pairs.size() < 400000; cell += 1 + rng() % 3) {
        size_t k = 1 + (rng() % 50 == 0 ? rng() % 300 : rng() % 4);
        for (size_t j = 0; j < k; j++) pairs.push_back({cell10(cell), static_cast<uint32_t>(rng() % 100000)});
    }
    std::sort(pairs.begin(), pairs.end(), cell_item_less);
    CellLists lists;
    for (const auto& p : pairs) lists[p.cell_id].push_back(p.item_id);
    // The map holds each cell's ids in no particular order.
    std::unordered_map<uint64_t, std::vector<uint32_t>> map;
    for (const auto& p : pairs) map[p.cell_id].push_back(p.item_id);
    for (auto& [cell, ids] : map) std::shuffle(ids.begin(), ids.end(), rng);

    for (cell_pairs::Format format : {cell_pairs::ADMIN, cell_pairs::POI, cell_pairs::POSTCODE_CENTROID}) {
        const auto [cells, entries] = write_cell_lists(lists, format);
        write_cell_index_sorted(dir.path() + "/cells.bin", dir.path() + "/entries.bin", format, pairs);
        CHECK(file_text(dir.path() + "/cells.bin") == std::string(cells.begin(), cells.end()));
        CHECK(file_text(dir.path() + "/entries.bin") == std::string(entries.begin(), entries.end()));
        write_cell_index(dir.path() + "/map_cells.bin", dir.path() + "/map_entries.bin", format, map);
        CHECK(file_text(dir.path() + "/map_cells.bin") == std::string(cells.begin(), cells.end()));
        CHECK(file_text(dir.path() + "/map_entries.bin") == std::string(entries.begin(), entries.end()));
        CHECK(parse_cell_lists(cells, entries, format) == lists);
    }
}

TEST(write_cell_index_writes_cells_without_ids) {
    // No ids at all: the sorted copy is empty and has no storage to copy from.
    ScratchDir dir("gctest-cellio");
    std::unordered_map<uint64_t, std::vector<uint32_t>> map = {{cell10(3), {}}};
    write_cell_index(dir.path() + "/cells.bin", dir.path() + "/entries.bin", cell_pairs::ADMIN, map);
    const std::string cells = file_text(dir.path() + "/cells.bin"), entries = file_text(dir.path() + "/entries.bin");
    CHECK(parse_cell_lists(std::vector<char>(cells.begin(), cells.end()), std::vector<char>(entries.begin(), entries.end()),
                           cell_pairs::ADMIN) == CellLists({{cell10(3), {}}}));
    // An index of no cells: two empty files.
    write_cell_index(dir.path() + "/cells.bin", dir.path() + "/entries.bin", cell_pairs::ADMIN, {});
    CHECK(file_text(dir.path() + "/cells.bin").empty() && file_text(dir.path() + "/entries.bin").empty());
}

TEST(write_cell_index_sorted_rejects_a_table_out_of_order) {
    ScratchDir dir("gctest-cellio");
    CHECK_EQ(refusal_of<std::logic_error>([&] {
                 write_cell_index_sorted(dir.path() + "/cells.bin", dir.path() + "/entries.bin", cell_pairs::ADMIN,
                                         {{cell10(9), 1}, {cell10(3), 1}});
             }),
             "write_cell_index_sorted: table out of order for " + dir.path() + "/entries.bin");
}

TEST(write_cell_index_sorted_rejects_a_cell_past_the_count_limit) {
    ScratchDir dir("gctest-cellio");
    std::vector<CellItemPair> pairs(70000, CellItemPair{cell10(7), 1});
    CHECK_EQ(refusal_of([&] {
                 write_cell_index_sorted(dir.path() + "/cells.bin", dir.path() + "/entries.bin", cell_pairs::ADMIN, pairs);
             }),
             "cell entry count 70000 exceeds uint16 limit (65535) in " + dir.path() + "/entries.bin");
    CHECK(!std::ifstream(dir.path() + "/cells.bin").good());
}
