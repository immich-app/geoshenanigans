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

// Strategy-2 persistent dense IDs. Loads the previous build's
// <prev_dir>/full/<file>.osm_ids sidecars (if present), allocates
// stable indices for each record by osm_id matching, reorders the
// in-memory record arrays, applies remap to every reference site,
// and stores the per-record sidecar slot vector on ParsedData so
// write_index can emit the new sidecar.
//
// Idempotent on the same input + same prev_dir. No-op if prev_dir is
// empty or its sidecars don't exist (first build / fresh start).
void apply_strategy2_remaps(ParsedData& data, const std::string& prev_dir);

// Write admin polygon/vertex files with on-the-fly simplification at a given epsilon scale.
// Scale 0 = uncapped (no simplification). Other files are symlinked/copied from source_dir.
void write_quality_variant(const ParsedData& data, const std::string& source_dir,
                           const std::string& output_dir, double epsilon_scale);

// Write a minimal admin polygon/vertex pair containing only polygons whose
// admin_level falls in [2, 8] (drops L9/L10/L11/L15 sub-municipal levels).
// Same delta-encoded vertex format as write_quality_variant. Fills
// id_remap so the caller can rewrite the admin cell index against the
// new dense ID space. Always emitted at q2.5 — admin-minimal isn't
// tiered by quality.
void write_admin_minimal_polygons(const ParsedData& data,
                                  const std::string& output_dir,
                                  double epsilon_scale,
                                  std::vector<uint32_t>& id_remap);
