#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "types.h"
#include "parsed_data.h"
#include "cell_index_io.h"

void write_index(const ParsedData& data, const std::string& output_dir, IndexMode mode);

// strings_layout.json: each tier's [start, end) in the global string-offset
// space, identical in every dir that resolves strings.
void write_strings_layout(const std::string& dir, const ParsedData& data);

// Country of the point via its admin cell's level-2 entries; border cells
// (multiple candidate countries) resolve by point-in-polygon. 0 if unknown.
uint16_t country_code_at_point(const ParsedData& data, double lat, double lng);

// Fold the OSM addr:postcode points into data.postcode_accum per
// (country, postcode), keeping external centroids only for the pairs OSM
// lacks. Run once on the full data before the continent split, so every
// region writes the same centroids (Nominatim's location_postcode is one
// table) instead of recomputing them from its own subset of addr points.
void collect_postcode_centroids(ParsedData& data);

// Strategy-2 persistent dense IDs. Loads the previous build's
// <prev_dir>/full/<file>.osm_ids sidecars (if present), allocates
// stable indices for each record by osm_id matching, reorders the
// in-memory record arrays, applies remap to every reference site,
// and stores the per-record sidecar slot vector on ParsedData so
// write_index can emit the new sidecar.
//
// Idempotent on the same input + same prev_dir. No-op if prev_dir is
// empty or its sidecars don't exist (first build / fresh start). The record
// kinds' passes run at once unless `order` is Serial.
void apply_strategy2_remaps(ParsedData& data, const std::string& prev_dir,
                            RunOrder order = RunOrder::Concurrent);

// Emit a strategy-2 *.osm_ids sidecar to disk. If `blob` is non-empty
// (apply_strategy2_remaps already ran), uses that pre-built table.
// Otherwise falls back to deriving slots from `osm_ids_fallback` so a
// first build (no prev sidecar) still emits a sidecar that subsequent
// builds can stabilize against.
void emit_strategy2_sidecar(const std::string& path,
                            const std::vector<gc::id_alloc::SidecarSlot>& blob,
                            const std::vector<uint64_t>& osm_ids_fallback);

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
                                  const std::string& prev_dir,
                                  double epsilon_scale,
                                  std::vector<uint32_t>& id_remap);
