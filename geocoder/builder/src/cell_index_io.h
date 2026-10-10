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
