// Binary file writers for the cell → item index files (cells + entries).
// No S2 here, so the unit tests can check the bytes.
#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cell_pairs.h"
#include "geo_index.h"
#include "parallel.h"
#include "types.h"

// Checked binary write: emits `buf` to `path` and throws on any stream
// failure. The success path writes exactly the same bytes std::ofstream
// would have; this only adds a failure-path check so a truncated/failed
// write surfaces as an error instead of a silently-corrupt index.
inline void write_binary_file(const std::string& path, const char* data, size_t size) {
    std::ofstream f(path, std::ios::binary);
    f.write(data, size);
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + path);
}

// A cell's list holds at most 0xFFFF entries (the geo index's entry lists
// count them in a uint16; a cell pair index's cap is the same,
// cell_pairs::LIST_MAX), so more is a hard build failure instead.
// Widening it is a routine build_version bump (one no-patch day) if growth
// ever approaches the limit — planet max as of 2026-06 is 29,975 entries
// (poi/all), 46% of the limit. The per-file "max entries/cell" line logged by
// each writer is the early-warning canary to watch.
inline uint16_t checked_entry_count(size_t n, const std::string& path) {
    if (n > 0xFFFFu)
        throw std::runtime_error("cell entry count " + std::to_string(n) +
                                 " exceeds uint16 limit (65535) in " + path);
    return static_cast<uint16_t>(n);
}

// Writes a cell pair index of `format` (cell_pairs.h) into its two files:
// cell c of [0, cells), cell_id(c) ascending, lists size(c) ids, which
// items(c, out) copies to `out`, ascending. The longest list is checked on
// every core before anything is written.
template <class CellId, class Size, class Items>
void write_cell_entries(const std::string& cells_path, const std::string& entries_path, cell_pairs::Format format,
                        size_t cells, CellId cell_id, Size size, Items items) {
    std::vector<size_t> block_longest(parallel_threads(), 0);
    const size_t blocks = parallel_blocks(cells, [&](size_t b, size_t begin, size_t end) {
        for (size_t c = begin; c < end; c++) block_longest[b] = std::max(block_longest[b], size_t(size(c)));
    });
    const size_t longest = blocks ? *std::max_element(block_longest.begin(), block_longest.begin() + blocks) : 0;
    checked_entry_count(longest, entries_path);

    std::ofstream cells_file(cells_path, std::ios::binary), entries_file(entries_path, std::ios::binary);
    cell_pairs::Writer writer(
        format, [&](const char* p, size_t n) { cells_file.write(p, static_cast<std::streamsize>(n)); },
        [&](const char* p, size_t n) { entries_file.write(p, static_cast<std::streamsize>(n)); });
    std::vector<uint32_t> ids;
    for (size_t c = 0; c < cells; c++) {
        ids.resize(size(c));
        items(c, ids.data());
        writer.add(cell_id(c), ids.data(), ids.size());
    }
    writer.finish();
    cells_file.flush();
    entries_file.flush();
    if (!cells_file) throw std::runtime_error("failed to write " + cells_path);
    if (!entries_file) throw std::runtime_error("failed to write " + entries_path);
    std::cerr << "  " << entries_path << ": max entries/cell = " << longest << std::endl;
}

inline void write_cell_index(
    const std::string& cells_path,
    const std::string& entries_path,
    cell_pairs::Format format,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map
) {
    using Cell = std::pair<uint64_t, const std::vector<uint32_t>*>;
    std::vector<Cell> sorted = parallel_collect<Cell>(cell_map.bucket_count(), [&](size_t b, std::vector<Cell>& out) {
        for (auto it = cell_map.begin(b); it != cell_map.end(b); ++it) out.push_back({it->first, &it->second});
    });
    parallel_sort(sorted.begin(), sorted.end(), [](const Cell& a, const Cell& b) { return a.first < b.first; });

    // Determinism safety net: sort each cell's inner entry vector by
    // value. Callers that build the map from parallel workers can push
    // entries in thread-scheduling order, which leaks non-determinism
    // into the on-disk layout and breaks incremental patching. Sorted
    // copies, back to back: the map stays as it is.
    const std::vector<size_t> from = parallel_offsets<size_t>(sorted.size(), [&](size_t c) {
        return sorted[c].second->size();
    });
    std::vector<uint32_t> ids;
    parallel_resize(ids, from.back());
    parallel_for(sorted.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t c = b; c < e; c++) {
            std::copy(sorted[c].second->begin(), sorted[c].second->end(), ids.begin() + from[c]);
            std::sort(ids.begin() + from[c], ids.begin() + from[c + 1]);
        }
    });
    write_cell_entries(cells_path, entries_path, format, sorted.size(),
        [&](size_t c) { return sorted[c].first; },
        [&](size_t c) { return from[c + 1] - from[c]; },
        [&](size_t c, uint32_t* out) { std::copy(ids.begin() + from[c], ids.begin() + from[c + 1], out); });
}

// Distinct cells of a cell-grouped table.
inline size_t count_cells(const std::vector<CellItemPair>& pairs) {
    return parallel_count(pairs.size(), [&](size_t i) { return i == 0 || pairs[i].cell_id != pairs[i - 1].cell_id; });
}

// write_cell_index for a table already in canonical order (by cell, then
// item, as cell_item_less): the same two files the map version writes for
// {cell: its items}, without building the map.
inline void write_cell_index_sorted(
    const std::string& cells_path,
    const std::string& entries_path,
    cell_pairs::Format format,
    const std::vector<CellItemPair>& pairs
) {
    if (parallel_any(pairs.empty() ? 0 : pairs.size() - 1, [&](size_t i) { return cell_item_less(pairs[i + 1], pairs[i]); }))
        throw std::logic_error("write_cell_index_sorted: table out of order for " + entries_path);

    const std::vector<size_t> starts = parallel_filter<size_t>(pairs.size(), [&](size_t i) {
        return i == 0 || pairs[i].cell_id != pairs[i - 1].cell_id;
    });
    auto end_of = [&](size_t c) { return c + 1 < starts.size() ? starts[c + 1] : pairs.size(); };
    write_cell_entries(cells_path, entries_path, format, starts.size(),
        [&](size_t c) { return pairs[starts[c]].cell_id; },
        [&](size_t c) { return end_of(c) - starts[c]; },
        [&](size_t c, uint32_t* out) {
            for (size_t i = starts[c], end = end_of(c); i < end; i++) *out++ = pairs[i].item_id;
        });
}

// The cell-grouped tables a mode's geo index draws on, in its table order
// (geo_index.h): streets, addrs, interps, each in canonical order (by cell,
// then item, as cell_item_less). A null table has no lists.
constexpr size_t GEO_TABLE_COUNT = geo_index::TABLES;
constexpr const char* GEO_CELLS_FILE = "geo_cells.bin";
constexpr const char* GEO_ENTRIES_FILE = "geo_entries.bin";
constexpr const char* GEO_TABLE_NAMES[GEO_TABLE_COUNT] = {"street", "address", "interpolation"};
using GeoTables = std::array<const std::vector<CellItemPair>*, GEO_TABLE_COUNT>;

// The rows of a geo index's tables in cell order, from given positions in
// them: each cell some table has items at, with every table's items there.
class GeoRowCursor {
public:
    GeoRowCursor(const GeoTables& tables, const std::array<size_t, GEO_TABLE_COUNT>& at) : tables_(tables), next_(at) {}

    // The next row's cell and lists (an empty one is none), false past the
    // tables' ends.
    bool row(uint64_t& cell, geo_index::RowLists& lists) {
        bool any = false;
        for (size_t t = 0; t < GEO_TABLE_COUNT; t++)
            if (tables_[t] && next_[t] < tables_[t]->size() && (!any || (*tables_[t])[next_[t]].cell_id < cell)) {
                cell = (*tables_[t])[next_[t]].cell_id;
                any = true;
            }
        if (!any) return false;
        for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
            lists[t].clear();
            if (!tables_[t]) continue;
            const auto& table = *tables_[t];
            for (size_t& i = next_[t]; i < table.size() && table[i].cell_id == cell; i++) lists[t].push_back(table[i].item_id);
        }
        return true;
    }

private:
    const GeoTables& tables_;
    std::array<size_t, GEO_TABLE_COUNT> next_;
};

// Writes a mode's geo index into dir (geo_index.h): its rows are the cells
// any table has items in. The tables split into slices of cells (none holds
// more than pairs_per_slice pairs of a table); every core counts a slice's
// rows, then encodes the blocks that start in it (reading on into the next
// slices for their rows), and the blocks are written in order, so only a few
// slices' blocks are held at once. The bytes are a geo_index::Writer's for
// the same rows. Returns the row count.
inline size_t write_geo_index(const std::string& dir, const GeoTables& tables,
                              size_t pairs_per_slice = size_t(1) << 17) {
    using geo_index::BLOCK_ROWS;
    const std::string top_path = dir + "/" + GEO_CELLS_FILE, blocks_path = dir + "/" + GEO_ENTRIES_FILE;
    for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
        const auto* table = tables[t];
        if (table && parallel_any(table->empty() ? 0 : table->size() - 1,
                                  [&](size_t i) { return cell_item_less((*table)[i + 1], (*table)[i]); }))
            throw std::logic_error(std::string("write_geo_index: the ") + GEO_TABLE_NAMES[t] + " table is out of order");
    }

    // Slices start at every pairs_per_slice-th cell of each table.
    std::vector<uint64_t> starts;
    for (const auto* table : tables)
        if (table)
            for (size_t i = pairs_per_slice; i < table->size(); i += pairs_per_slice)
                starts.push_back((*table)[i].cell_id);
    std::sort(starts.begin(), starts.end());
    starts.erase(std::unique(starts.begin(), starts.end()), starts.end());
    const size_t slices = starts.size() + 1;

    // Per slice: where each table's pairs begin, its rows, and per table its
    // longest cell.
    std::vector<std::array<size_t, GEO_TABLE_COUNT>> at(slices + 1);
    for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
        const size_t size = tables[t] ? tables[t]->size() : 0;
        at[0][t] = 0;
        at[slices][t] = size;
        for (size_t k = 1; k < slices; k++)
            at[k][t] = !tables[t] ? 0 : static_cast<size_t>(std::lower_bound(tables[t]->begin(), tables[t]->end(), starts[k - 1],
                [](const CellItemPair& p, uint64_t cell) { return p.cell_id < cell; }) - tables[t]->begin());
    }
    std::vector<uint64_t> slice_rows(slices, 0);
    std::vector<std::array<size_t, GEO_TABLE_COUNT>> longest(slices);
    parallel_for(slices, [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++) {
            longest[k] = {};
            std::array<size_t, GEO_TABLE_COUNT> next = at[k];
            for (;;) {
                bool any = false;
                uint64_t cell = 0;
                for (size_t t = 0; t < GEO_TABLE_COUNT; t++)
                    if (next[t] < at[k + 1][t] && (!any || (*tables[t])[next[t]].cell_id < cell)) {
                        cell = (*tables[t])[next[t]].cell_id;
                        any = true;
                    }
                if (!any) break;
                for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
                    const size_t from = next[t];
                    while (next[t] < at[k + 1][t] && (*tables[t])[next[t]].cell_id == cell) next[t]++;
                    longest[k][t] = std::max(longest[k][t], next[t] - from);
                }
                slice_rows[k]++;
            }
        }
    });
    for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
        if (!tables[t]) continue;
        size_t most = 0;
        for (const auto& l : longest) most = std::max(most, l[t]);
        const std::string what = blocks_path + " (" + GEO_TABLE_NAMES[t] + " lists)";
        checked_entry_count(most, what);
        std::cerr << "  " << what << ": max entries/cell = " << most << std::endl;
    }
    // first_row[k]: the index of slice k's first row.
    std::vector<uint64_t> first_row(slices + 1, 0);
    for (size_t k = 0; k < slices; k++) first_row[k + 1] = first_row[k] + slice_rows[k];
    const uint64_t rows = first_row[slices];

    // Slice k encodes the blocks whose first row is one of its own.
    auto encode = [&](size_t k) {
        std::vector<geo_index::EncodedBlock> out;
        const uint64_t first_block = (first_row[k] + BLOCK_ROWS - 1) / BLOCK_ROWS;
        const uint64_t end_block = (first_row[k + 1] + BLOCK_ROWS - 1) / BLOCK_ROWS;
        if (first_block >= end_block) return out;
        GeoRowCursor rows_at(tables, at[k]);
        geo_index::RowLists lists;
        uint64_t cell = 0;
        for (uint64_t skip = first_block * BLOCK_ROWS - first_row[k]; skip > 0; skip--) rows_at.row(cell, lists);
        geo_index::BlockEncoder encoder;
        int level = -1;
        for (uint64_t b = first_block; b < end_block; b++) {
            geo_index::EncodedBlock& block = out.emplace_back();
            encoder.start();
            const uint64_t n = std::min<uint64_t>(BLOCK_ROWS, rows - b * BLOCK_ROWS);
            for (uint64_t r = 0; r < n; r++) {
                if (!rows_at.row(cell, lists)) throw std::logic_error("write_geo_index: rows past the tables");
                if (level < 0) level = geo_index::level_of(cell);
                if (geo_index::level_of(cell) != level) throw std::runtime_error("write_geo_index: cells of two levels");
                if (r == 0) block.first_cell = cell;
                encoder.row(block.bytes, geo_index::number_of(cell, level), geo_index::ids_of(lists));
            }
            block.last_cell = cell;
            block.rows = static_cast<size_t>(n);
        }
        return out;
    };

    std::ofstream top(top_path, std::ios::binary), blocks(blocks_path, std::ios::binary);
    geo_index::Writer writer([&](const char* p, size_t n) { top.write(p, static_cast<std::streamsize>(n)); },
                             [&](const char* p, size_t n) { blocks.write(p, static_cast<std::streamsize>(n)); });
    parallel_ordered(slices, encode, [&](size_t, std::vector<geo_index::EncodedBlock> encoded) {
        for (const auto& block : encoded) writer.add_block(block);
    }, 0, parallel_threads());
    writer.finish();
    if (writer.rows() != rows) throw std::logic_error("write_geo_index: rows lost between the slices");
    top.flush();
    blocks.flush();
    if (!top) throw std::runtime_error("failed to write " + top_path);
    if (!blocks) throw std::runtime_error("failed to write " + blocks_path);
    return static_cast<size_t>(rows);
}
