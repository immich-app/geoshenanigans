#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "types.h"
#include "parsed_data.h"

std::vector<uint32_t> write_entries(
    const std::string& path,
    const std::vector<uint64_t>& sorted_cells,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map);

std::vector<uint32_t> write_entries_from_sorted(
    const std::string& path,
    const std::vector<uint64_t>& sorted_cells,
    const std::vector<CellItemPair>& sorted_pairs);

void write_cell_index(
    const std::string& cells_path,
    const std::string& entries_path,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map);

void write_index(const ParsedData& data, const std::string& output_dir, IndexMode mode);

// Write admin polygon/vertex files with on-the-fly simplification at a given epsilon scale.
// Scale 0 = uncapped (no simplification). Other files are symlinked/copied from source_dir.
void write_quality_variant(const ParsedData& data, const std::string& source_dir,
                           const std::string& output_dir, double epsilon_scale);
