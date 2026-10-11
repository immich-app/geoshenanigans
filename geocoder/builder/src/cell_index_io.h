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

// A cell's entry list is prefixed by a uint16 count on disk. More entries than
// fit would silently wrap the count and make the server under-read the cell,
// so overflow is a hard build failure instead. Widening the field is a routine
// build_version bump (one no-patch day) if growth ever approaches the limit —
// planet max as of 2026-06 is 29,975 entries (poi/all), 46% of the limit. The
// per-file "max entries/cell" line logged by each writer is the early-warning
// canary to watch.
inline uint16_t checked_entry_count(size_t n, const std::string& path) {
    if (n > 0xFFFFu)
        throw std::runtime_error("cell entry count " + std::to_string(n) +
                                 " exceeds uint16 limit (65535) in " + path);
    return static_cast<uint16_t>(n);
}

inline void write_cell_index(
    const std::string& cells_path,
    const std::string& entries_path,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map
) {
    std::vector<std::pair<uint64_t, std::vector<uint32_t>>> sorted(cell_map.begin(), cell_map.end());
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // Determinism safety net: sort each cell's inner entry vector by
    // value. Callers that build the map from parallel workers can push
    // entries in thread-scheduling order, which leaks non-determinism
    // into the on-disk layout and breaks incremental patching. Sorting
    // here is cheap compared to the rest of the index write.
    for (auto& [cell_id, ids] : sorted) {
        std::sort(ids.begin(), ids.end());
    }

    { std::ofstream f(cells_path, std::ios::binary);
      uint32_t current_offset = 0;
      for (const auto& [cell_id, ids] : sorted) {
          f.write(reinterpret_cast<const char*>(&cell_id), sizeof(cell_id));
          f.write(reinterpret_cast<const char*>(&current_offset), sizeof(current_offset));
          current_offset += sizeof(uint16_t) + ids.size() * sizeof(uint32_t);
      }
      f.flush();
      if (!f) throw std::runtime_error("failed to write " + cells_path); }

    { std::ofstream f(entries_path, std::ios::binary);
      size_t max_count = 0;
      for (const auto& [cell_id, ids] : sorted) {
          uint16_t count = checked_entry_count(ids.size(), entries_path);
          if (ids.size() > max_count) max_count = ids.size();
          f.write(reinterpret_cast<const char*>(&count), sizeof(count));
          f.write(reinterpret_cast<const char*>(ids.data()), ids.size() * sizeof(uint32_t));
      }
      f.flush();
      if (!f) throw std::runtime_error("failed to write " + entries_path);
      std::cerr << "  " << entries_path << ": max entries/cell = " << max_count << std::endl; }
}

// Distinct cells of a cell-grouped table.
inline size_t count_cells(const std::vector<CellItemPair>& pairs) {
    size_t cells = 0;
    for (size_t i = 0; i < pairs.size(); i++)
        if (i == 0 || pairs[i].cell_id != pairs[i - 1].cell_id) cells++;
    return cells;
}

// write_cell_index for a table already in canonical order (by cell, then
// item, as cell_item_less): the same two files the map version writes for
// {cell: its items}, without building the map.
inline void write_cell_index_sorted(
    const std::string& cells_path,
    const std::string& entries_path,
    const std::vector<CellItemPair>& pairs
) {
    for (size_t i = 1; i < pairs.size(); i++)
        if (cell_item_less(pairs[i], pairs[i - 1]))
            throw std::logic_error("write_cell_index_sorted: table out of order for " + entries_path);

    std::vector<char> cells, entries;
    entries.reserve(pairs.size() * sizeof(uint32_t));
    size_t max_count = 0;
    for (size_t run = 0; run < pairs.size(); ) {
        size_t run_end = run + 1;
        while (run_end < pairs.size() && pairs[run_end].cell_id == pairs[run].cell_id) run_end++;
        uint64_t cell_id = pairs[run].cell_id;
        uint32_t offset = static_cast<uint32_t>(entries.size());
        cells.insert(cells.end(), reinterpret_cast<const char*>(&cell_id),
                     reinterpret_cast<const char*>(&cell_id) + sizeof(cell_id));
        cells.insert(cells.end(), reinterpret_cast<const char*>(&offset),
                     reinterpret_cast<const char*>(&offset) + sizeof(offset));
        uint16_t count = checked_entry_count(run_end - run, entries_path);
        max_count = std::max(max_count, run_end - run);
        entries.insert(entries.end(), reinterpret_cast<const char*>(&count),
                       reinterpret_cast<const char*>(&count) + sizeof(count));
        for (size_t i = run; i < run_end; i++)
            entries.insert(entries.end(), reinterpret_cast<const char*>(&pairs[i].item_id),
                           reinterpret_cast<const char*>(&pairs[i].item_id) + sizeof(uint32_t));
        run = run_end;
    }
    write_binary_file(cells_path, cells.data(), cells.size());
    write_binary_file(entries_path, entries.data(), entries.size());
    std::cerr << "  " << entries_path << ": max entries/cell = " << max_count << std::endl;
}

// The cell-grouped tables a mode's geo index draws on, in geo_cells.bin
// column order: streets, addrs, interps. A null table has no entries file and
// leaves its column NO_DATA.
constexpr size_t GEO_TABLE_COUNT = 3;
constexpr const char* GEO_ENTRY_FILENAMES[GEO_TABLE_COUNT] = {
    "street_entries.bin", "addr_entries.bin", "interp_entries.bin"};
using GeoTables = std::array<const std::vector<CellItemPair>*, GEO_TABLE_COUNT>;

// A cell map as a cell-grouped table: cells ascending, each cell's items in
// its list's order (the order its entry lists them in).
inline std::vector<CellItemPair> pairs_in_list_order(
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map) {
    std::vector<uint64_t> cells;
    cells.reserve(cell_map.size());
    size_t total = 0;
    for (const auto& [cell_id, ids] : cell_map) {
        cells.push_back(cell_id);
        total += ids.size();
    }
    std::sort(cells.begin(), cells.end());
    std::vector<CellItemPair> pairs;
    pairs.reserve(total);
    for (uint64_t cell_id : cells)
        for (uint32_t id : cell_map.at(cell_id)) pairs.push_back({cell_id, id});
    return pairs;
}

// Writes a mode's geo index into dir: geo_cells.bin has one row per cell any
// table has items in, ascending: the cell id, then per table the byte offset
// of the cell's entry (uint16 count, then its items in table order) in that
// table's entries file, or NO_DATA. Every core builds slices of cells (none
// holds more than pairs_per_slice pairs of a table) that are appended in
// order, so only a few slices are held at once. Returns the row count.
inline size_t write_geo_index(const std::string& dir, const GeoTables& tables,
                              size_t pairs_per_slice = size_t(1) << 17) {
    constexpr size_t kRowBytes = sizeof(uint64_t) + GEO_TABLE_COUNT * sizeof(uint32_t);
    auto path_of = [&](size_t t) { return dir + "/" + GEO_ENTRY_FILENAMES[t]; };

    // Slices start at every pairs_per_slice-th cell of each table.
    std::vector<uint64_t> starts;
    for (const auto* table : tables)
        if (table)
            for (size_t i = pairs_per_slice; i < table->size(); i += pairs_per_slice)
                starts.push_back((*table)[i].cell_id);
    std::sort(starts.begin(), starts.end());
    starts.erase(std::unique(starts.begin(), starts.end()), starts.end());
    const size_t slices = starts.size() + 1;

    // Per table and slice: where its pairs begin, its cells, its longest
    // cell and the byte offset its entries start at.
    struct Layout {
        std::vector<size_t> at, cells, longest;
        std::vector<uint64_t> base;
    };
    std::array<Layout, GEO_TABLE_COUNT> layout;
    for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
        if (!tables[t]) continue;
        const auto& table = *tables[t];
        auto& l = layout[t];
        l.at.resize(slices + 1);
        l.at[0] = 0;
        l.at[slices] = table.size();
        for (size_t k = 1; k < slices; k++)
            l.at[k] = static_cast<size_t>(std::lower_bound(table.begin(), table.end(), starts[k - 1],
                [](const CellItemPair& p, uint64_t cell) { return p.cell_id < cell; }) - table.begin());
        l.cells.assign(slices, 0);
        l.longest.assign(slices, 0);
    }
    parallel_for(slices, [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++)
            for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
                if (!tables[t]) continue;
                const auto& table = *tables[t];
                auto& l = layout[t];
                for (size_t i = l.at[k], end = l.at[k + 1]; i < end;) {
                    size_t run = i + 1;
                    while (run < end && table[run].cell_id == table[i].cell_id) run++;
                    l.cells[k]++;
                    l.longest[k] = std::max(l.longest[k], run - i);
                    i = run;
                }
            }
    });
    auto slice_bytes = [&](size_t t, size_t k) {
        const auto& l = layout[t];
        return l.cells[k] * sizeof(uint16_t) + (l.at[k + 1] - l.at[k]) * sizeof(uint32_t);
    };
    for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
        if (!tables[t]) continue;
        auto& l = layout[t];
        l.base.resize(slices);
        uint64_t bytes = 0;
        for (size_t k = 0; k < slices; k++) {
            l.base[k] = bytes;
            bytes += slice_bytes(t, k);
        }
        size_t longest = *std::max_element(l.longest.begin(), l.longest.end());
        checked_entry_count(longest, path_of(t));
        std::cerr << "  " << path_of(t) << ": max entries/cell = " << longest << std::endl;
    }

    struct Slice {
        std::unique_ptr<char[]> rows;
        size_t row_count = 0;
        std::array<std::unique_ptr<char[]>, GEO_TABLE_COUNT> entries;
    };
    auto build = [&](size_t k) {
        Slice out;
        std::array<size_t, GEO_TABLE_COUNT> next{}, end{};
        std::array<char*, GEO_TABLE_COUNT> put{};
        size_t max_rows = 0;
        for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
            if (!tables[t]) continue;
            next[t] = layout[t].at[k];
            end[t] = layout[t].at[k + 1];
            out.entries[t].reset(new char[slice_bytes(t, k)]);
            put[t] = out.entries[t].get();
            max_rows += layout[t].cells[k];
        }
        out.rows.reset(new char[max_rows * kRowBytes]);
        char* row = out.rows.get();
        for (;;) {
            bool any = false;
            uint64_t cell = 0;
            for (size_t t = 0; t < GEO_TABLE_COUNT; t++)
                if (next[t] < end[t] && (!any || (*tables[t])[next[t]].cell_id < cell)) {
                    cell = (*tables[t])[next[t]].cell_id;
                    any = true;
                }
            if (!any) break;

            uint32_t offsets[GEO_TABLE_COUNT] = {NO_DATA, NO_DATA, NO_DATA};
            for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
                if (next[t] >= end[t] || (*tables[t])[next[t]].cell_id != cell) continue;
                const auto& table = *tables[t];
                offsets[t] = static_cast<uint32_t>(layout[t].base[k] + (put[t] - out.entries[t].get()));
                size_t run = next[t];
                while (run < end[t] && table[run].cell_id == cell) run++;
                uint16_t count = static_cast<uint16_t>(run - next[t]);
                std::memcpy(put[t], &count, sizeof(count));
                put[t] += sizeof(count);
                for (; next[t] < run; next[t]++, put[t] += sizeof(uint32_t))
                    std::memcpy(put[t], &table[next[t]].item_id, sizeof(uint32_t));
            }
            std::memcpy(row, &cell, sizeof(cell));
            std::memcpy(row + sizeof(cell), offsets, sizeof(offsets));
            row += kRowBytes;
            out.row_count++;
        }
        return out;
    };

    std::array<std::ofstream, GEO_TABLE_COUNT> entry_files;
    for (size_t t = 0; t < GEO_TABLE_COUNT; t++)
        if (tables[t]) entry_files[t].open(path_of(t), std::ios::binary);
    const std::string geo_path = dir + "/geo_cells.bin";
    std::ofstream geo(geo_path, std::ios::binary);
    size_t rows = 0;
    parallel_ordered(slices, build, [&](size_t k, Slice slice) {
        for (size_t t = 0; t < GEO_TABLE_COUNT; t++)
            if (tables[t])
                entry_files[t].write(slice.entries[t].get(), static_cast<std::streamsize>(slice_bytes(t, k)));
        geo.write(slice.rows.get(), static_cast<std::streamsize>(slice.row_count * kRowBytes));
        rows += slice.row_count;
    }, 0, parallel_threads());
    for (size_t t = 0; t < GEO_TABLE_COUNT; t++) {
        if (!tables[t]) continue;
        entry_files[t].flush();
        if (!entry_files[t]) throw std::runtime_error("failed to write " + path_of(t));
    }
    geo.flush();
    if (!geo) throw std::runtime_error("failed to write " + geo_path);
    return rows;
}
