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

    // Whether polygon pid covers p, as ST_Covers: p is inside it or on an
    // edge of one of its rings, a hole's too, which contains leaves to one
    // side or the other.
    bool covers(uint32_t pid, PackedLocation p) const;

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

// The country polygon i lies in, as Nominatim takes a place's at its centroid
// (get_country_code), ST_PointOnSurface for an area (get_center_point): its
// own code where it has one (a country, a postal area), else the country at a
// point inside its exact rings (interior_point), which a concave polygon's
// vertex average needn't be; 0 where none is known. `countries` is
// border_country_rings'.
uint16_t polygon_country(const ParsedData& data, const AdminRings& countries, uint32_t i);

void compute_admin_parent_chain(ParsedData& data, unsigned int num_threads);

// Takes out of addresses the boundaries that repeat their parent, as
// placex_update does (is_same, placex_triggers.sql:909-931): a boundary
// relation of a rank above 0 in its country whose closest boundary relation
// round it, of a lower admin level above 3 and a rank of 1 to 25, has its
// wikidata (data.boundary_wikidata) and its rings (the same outline, from
// any vertex on, either way round, whatever vertices either adds along its
// straight edges: ST_Equals) ranks 0. The closest is the one of the highest
// level, then the smallest, of those covering a point inside the
// boundary's largest ring (its admin cell entries, data.admin_index_cells),
// the levels below settled first. Every ring of one that links no place
// node (data.linking_boundaries), or a label member of a place type of no
// rank between 4 and 25 (data.unranked_label_boundaries), is marked
// BOUNDARY_REPEATS_PARENT, in no address; every ring of one that links
// another node keeps the node's type and has own_place_type
// BOUNDARY_REPEATS_PARENT: parent_address_level staying 3, it takes the
// node's rank where that is above 3 and below 26 (:1161), whatever its
// parent's, and is a parent of that rank. Returns how many boundaries
// repeat their parent.
size_t take_out_repeated_boundaries(ParsedData& data, unsigned int num_threads);

void compute_way_parent_polygons(ParsedData& data, unsigned int num_threads);

// `order` holds the POIs, grouped by cell (items_by_cell), the POIs of one
// admin cell index cell together but for a few: each is walked in the cell of
// its own e7 point.
void compute_poi_parent_polygons(ParsedData& data, unsigned int num_threads, std::vector<CellItemPair> order,
                std::chrono::steady_clock::time_point& _s2t, CpuTicks& _s2cpu);
