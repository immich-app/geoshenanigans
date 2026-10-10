// Binary file writers for the cell → item index files (cells + entries).
// No S2 here, so the unit tests can check the bytes.
#pragma once

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

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
