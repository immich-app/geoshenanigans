#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "admin_links.h"
#include "types.h"
#include "parsed_data.h"
#include "cell_index_io.h"
#include "memory_limit.h"

void write_index(const ParsedData& data, const std::string& output_dir, IndexMode mode);

// Fills the admin cell indexes the reader reads (data.admin_index_cells,
// data.admin_minimal_index_cells) from the arc stage's tiers (`arcs` of
// data.admin_exact_rings, before any admin permute): the full set's gets
// each polygon's cells over every tier of the set, the exact tier's being
// `exact_cells` (admin_candidate_cells), interior only where interior in
// every one; the minimal set's the cells of its tier's rings. Each tier's
// interior is its exact ray cast at the cell centre. The admin permutes,
// strategy-2 and the continent split carry the tables on from there.
// data.admin_country_cells gets the country polygons' share of
// `exact_cells`.
void compute_admin_index_cells(ParsedData& data, const AdminArcs& arcs, std::vector<CellItemPair> exact_cells,
                               unsigned threads = 0);

// strings_layout.json: each tier's [start, end) in the dir's string-offset
// space, tier t at [bases[t], bases[t + 1]): the build's
// (data.strings_tier_bases) in every dir that resolves the shared strings.
void write_strings_layout(const std::string& dir, const std::array<uint32_t, STR_TIER_COUNT + 1>& bases);

// The rings of the country polygons for country_code_at_point: those that
// border cells (cells listing two countries) test, edge-indexed.
AdminRings border_country_rings(const ParsedData& data, unsigned int threads = 0);

// Country of the point via its own cell's entries of the country polygons'
// exact rings (data.admin_country_cells); border cells (multiple candidate
// countries) resolve by the exact point-in-polygon test of `countries`
// (border_country_rings). 0 if unknown.
uint16_t country_code_at_point(const ParsedData& data, const AdminRings& countries, double lat, double lng);

// Fold the OSM addr:postcode points into data.postcode_accum per
// (country, postcode), keeping external centroids only for the pairs OSM
// lacks. Run once on the full data before the continent split, so every
// region writes the same centroids (Nominatim's location_postcode is one
// table) instead of recomputing them from its own subset of addr points.
// The last reader of data.admin_exact_rings.
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
// kinds' passes run at once unless `order` is Serial. With a `budget`, each
// pass holds its estimated cost of it while it runs (strategy2_pass_costs),
// so a pass that doesn't fit beside the running ones waits for them.
void apply_strategy2_remaps(ParsedData& data, const std::string& prev_dir,
                            RunOrder order = RunOrder::Concurrent, MemoryBudget* budget = nullptr);

// Emit a strategy-2 *.osm_ids sidecar to disk. If `blob` is non-empty
// (apply_strategy2_remaps already ran), uses that pre-built table.
// Otherwise falls back to deriving slots from `osm_ids_fallback` so a
// first build (no prev sidecar) still emits a sidecar that subsequent
// builds can stabilize against.
void emit_strategy2_sidecar(const std::string& path,
                            const std::vector<gc::id_alloc::SidecarSlot>& blob,
                            const std::vector<uint64_t>& osm_ids_fallback);

// Where a region's admin_polygons.osm_ids sidecar, the full set's slots,
// sits under its dir: with the exact tier.
std::string admin_sidecar_path(const std::string& region_dir);

// Writes a region's boundary tiers (kArcTiers of the full set) under
// region_dir: quality/simplified/ and quality/exact/ each hold
// admin_polygons.bin and admin_rings.bin, the same bytes in both (every
// polygon at its own slot with its ring block in the full set, a tombstone
// at a strategy-2 one), and the tier's arc_index.bin and arc_vertices.bin.
// quality/exact/ holds the full set's sidecars too: admin_polygons.osm_ids
// (admin_sidecar_path) and arc_index.osm_ids (arc_sidecar_path).
void write_admin_tiers(const ParsedData& data, const std::string& region_dir);

// Writes the full set's admin geometry at the exact tier into dir: the bytes
// of quality/exact/'s admin_polygons.bin, admin_rings.bin, arc_index.bin and
// arc_vertices.bin (write_admin_tiers), no sidecar. A region's dir of a
// single-output build holds them beside its mode's files, which makes it a
// whole index: the admin cell index (compute_admin_index_cells) lists every
// polygon wherever any tier of the full set has it, so it serves the exact
// tier's rings.
void write_exact_admin_geometry(const ParsedData& data, const std::string& dir);

// Writes admin-minimal, the smallest useful set, in two dirs under
// region_dir. Both hold the minimal arc set (admin_minimal_keeps' polygons:
// its ring blocks, arc_index.bin and arc_vertices.bin, the same arc files in
// both) and drop:
//   - place_nodes of other types than admin_minimal_keeps_place's (suburbs,
//     neighbourhoods, quarters)
//   - the admin polygons admin_minimal_keeps leaves out: L9 borough, L10
//     quarter-area, L11 admin-postal, and L15 place areas ranked below 16.
//   - those boundaries' claims on place nodes: a node one of them
//     claims answers here as a place, since nothing here stands
//     for it (an L9 or L10 boundary claims Banja Luka, Mildura and
//     many Dutch villages).
// so neither needs a boundary tier. They differ in what other dirs they can
// be read beside:
//   - admin-minimal/, for a set without POIs: its polygons in its own stable
//     slots (region_prev's admin-minimal sidecar), and a core of its own,
//     only the strings its records name, in the shared core's order, with
//     a layout of that core alone. It holds the minimal set's arc sidecar.
//   - admin-minimal-poi/, for a set with a POI tier: the shared polygon
//     slots, which a POI's parent_poly_id names (a tombstone at each slot
//     it drops), and the shared core strings and layout, which the POI
//     tier's names extend.
void write_admin_minimal(const ParsedData& data, const std::string& region_dir, const std::string& region_prev);
