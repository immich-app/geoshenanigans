// The passes that link records to the admin polygons containing them, on the
// polygons' exact rings (ParsedData::admin_exact_rings): each admin polygon's
// parent, each street way's parent and postcode, each POI's parent and
// postcode, and the country of a point. A point is rounded to e7 (to_e7) and
// looked up in its own cell of the admin cell index the reader reads too
// (ParsedData::admin_index_cells, over every tier of the full set): an entry
// flagged interior is so in the exact tier and needs no ring test, the
// others take the exact ray cast.
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include <s2/s2cell_id.h>

#include "exact_pip.h"
#include "parsed_data.h"
#include "types.h"

// The admin cell index's cell of the e7 point p, the one cell a link reads.
S2CellId admin_link_cell(PackedLocation p);

// The entries of `cell` in a canonical cell index (cell_item_less order).
std::pair<const CellItemPair*, const CellItemPair*> admin_cell_entries(const std::vector<CellItemPair>& index,
                                                                       uint64_t cell);

// Containment in the admin polygons' exact rings, even-odd over all of a
// polygon's rings: each ring behind its box, and the rings of 64 vertices or
// more behind an edge index, of every polygon or only of those `indexed`
// takes (the rest are scanned).
class AdminRings {
public:
    // The rings must outlive this, and polygon ids stay put meanwhile.
    AdminRings(const AdminExactRings& rings, unsigned int threads,
               const std::function<bool(uint32_t)>& indexed = nullptr);

    bool contains(uint32_t pid, PackedLocation p) const;

    // The polygon of the first of `ranked` (admin cell index entries, see
    // rank_candidates) containing p, or NO_DATA: an entry flagged
    // INTERIOR_FLAG contains every point of its cell untested.
    uint32_t first_containing(const std::vector<uint32_t>& ranked, PackedLocation p) const;

private:
    // Rings this long cast their ray through an ExactRingEdgeIndex: the
    // passes test millions of points against the same big rings, each a
    // scan of thousands of edges.
    static constexpr uint32_t kMinIndexedRingVertices = 64;
    static constexpr size_t kPolygonsPerChunk = 16;

    const AdminExactRings& rings_;
    std::vector<ExactRingBox> boxes_;                        // by ring
    std::vector<std::unique_ptr<ExactRingEdgeIndex>> edges_;  // by ring, null when not indexed
};

void compute_admin_parent_chain(ParsedData& data, unsigned int num_threads);

void compute_way_parent_polygons(ParsedData& data, unsigned int num_threads);

// `order` holds the POIs, grouped by cell (items_by_cell), the POIs of one
// admin cell index cell together but for a few: each is walked in the cell of
// its own e7 point.
void compute_poi_parent_polygons(ParsedData& data, unsigned int num_threads, std::vector<CellItemPair> order,
                std::chrono::steady_clock::time_point& _s2t, CpuTicks& _s2cpu);
