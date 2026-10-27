// Place nodes that boundaries claim. Nominatim's find_linked_place links an
// admin boundary to a place node (placex.linked_place_id): the node leaves
// the results and the boundary stands for it. A claimed node keeps its name
// and carries PLACE_FLAG_HIDDEN, which every reverse lookup skips like a
// nameless node, and, when its boundary has a polygon, PLACE_FLAG_LABEL_OF
// with parent_poly_id naming that polygon: place search uses the node as the
// boundary's label point and name.
#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <utility>
#include <vector>

#include "id_allocator.h"
#include "parallel.h"
#include "types.h"

// A boundary's claim on a place node: the node is its label member or shares
// its wikidata id (find_linked_place's first two steps).
struct PlaceClaim {
    uint64_t node;      // pack_osm_id(OSM_NODE, id), as place_osm_ids holds it
    uint64_t boundary;  // relation_boundary_id or way_boundary_id of the claimant
    bool label;         // a label member, else a wikidata match
};

// The stable id admin_osm_ids holds for ring `ring` of relation `relation`.
inline uint64_t admin_ring_id(int64_t relation, uint16_t ring) {
    return pack_osm_id(gc::id_alloc::ObjectType::OSM_RELATION,
                       static_cast<int64_t>((static_cast<uint64_t>(relation) << 16) | ring));
}

// The boundary an admin_osm_ids entry is a ring of: the rings of a relation
// share one (the ring index cleared), a closed way is its own.
inline uint64_t admin_boundary_id(uint64_t admin_osm_id) {
    constexpr uint64_t kRelation = static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_RELATION);
    return admin_osm_id >> 56 == kRelation ? admin_osm_id & ~uint64_t(0xFFFF) : admin_osm_id;
}

inline uint64_t relation_boundary_id(int64_t relation) { return admin_ring_id(relation, 0); }

inline uint64_t way_boundary_id(int64_t way) { return pack_osm_id(gc::id_alloc::ObjectType::OSM_WAY, way); }

// Whether reverse geocoding skips the node.
inline bool place_node_hidden(const PlaceNode& pn) {
    return pn.name_id == NO_DATA || (pn._pad1 & PLACE_FLAG_HIDDEN);
}

// The polygon standing for each boundary, as (boundary id, polygon) sorted by
// boundary id: its largest ring, equal areas to the lower ring id.
inline std::vector<std::pair<uint64_t, uint32_t>> boundary_polygons(const std::vector<AdminPolygon>& polygons,
                                                                    const std::vector<uint64_t>& polygon_ids) {
    std::vector<std::pair<uint64_t, uint32_t>> out;
    if (polygon_ids.size() != polygons.size()) return out;
    for (uint32_t p = 0; p < polygons.size(); p++)
        if (polygon_ids[p] != 0) out.push_back({admin_boundary_id(polygon_ids[p]), p});
    std::sort(out.begin(), out.end(), [&](const auto& a, const auto& b) {
        if (a.first != b.first) return a.first < b.first;
        if (polygons[a.second].area != polygons[b.second].area)
            return polygons[a.second].area > polygons[b.second].area;
        return polygon_ids[a.second] < polygon_ids[b.second];
    });
    out.erase(std::unique(out.begin(), out.end(), [](const auto& a, const auto& b) { return a.first == b.first; }),
              out.end());
    return out;
}

// One boundary's claim on a node, for choosing between claimants.
struct ClaimRank {
    bool label;
    uint8_t admin_level;
    uint64_t boundary;
};

// Whether claim a takes the node over claim b, as Nominatim's last write of
// linked_place_id: it indexes boundaries by ascending rank, a label link sets
// the node's link whatever it was (the highest-level label claim keeps it),
// a wikidata or name link only an unset one (the lowest-level claim keeps
// it, and never over a label). Admin level stands in for rank; equal levels,
// which Nominatim indexes in no set order, go to the lower boundary id.
inline bool claim_wins(const ClaimRank& a, const ClaimRank& b) {
    if (a.label != b.label) return a.label;
    if (a.admin_level != b.admin_level) return a.label ? a.admin_level > b.admin_level : a.admin_level < b.admin_level;
    return a.boundary < b.boundary;
}

// Hides a claimed node; names `polygon` (unless NO_DATA) as its claimant.
inline void flag_claimed_place_node(PlaceNode& pn, uint32_t polygon) {
    pn._pad1 |= PLACE_FLAG_HIDDEN;
    if (polygon == NO_DATA) return;
    pn._pad1 |= PLACE_FLAG_LABEL_OF;
    pn.parent_poly_id = polygon;
}

// Flags the place nodes (node_ids parallel to nodes) the label and wikidata
// claims hit, each naming the polygon of the claimant claim_wins picks among
// those with a polygon. Returns how many nodes it hid.
inline size_t hide_claimed_place_nodes(std::vector<PlaceNode>& nodes, const std::vector<uint64_t>& node_ids,
                                       std::vector<PlaceClaim> claims, const std::vector<AdminPolygon>& polygons,
                                       const std::vector<uint64_t>& polygon_ids, unsigned threads = 0) {
    if (claims.empty()) return 0;
    const auto boundaries = boundary_polygons(polygons, polygon_ids);
    auto polygon_of = [&](uint64_t boundary) {
        auto it = std::lower_bound(boundaries.begin(), boundaries.end(), std::make_pair(boundary, uint32_t(0)));
        return it != boundaries.end() && it->first == boundary ? it->second : NO_DATA;
    };
    std::sort(claims.begin(), claims.end(), [](const PlaceClaim& a, const PlaceClaim& b) { return a.node < b.node; });
    std::atomic<size_t> hidden{0};
    parallel_for(std::min(nodes.size(), node_ids.size()), [&](size_t begin, size_t end, unsigned) {
        size_t n = 0;
        for (size_t i = begin; i < end; i++) {
            auto it = std::lower_bound(claims.begin(), claims.end(), node_ids[i],
                                       [](const PlaceClaim& c, uint64_t node) { return c.node < node; });
            if (it == claims.end() || it->node != node_ids[i]) continue;
            uint32_t best = NO_DATA;
            ClaimRank best_rank{};
            for (; it != claims.end() && it->node == node_ids[i]; ++it) {
                const uint32_t p = polygon_of(it->boundary);
                if (p == NO_DATA) continue;
                const ClaimRank rank{it->label, polygons[p].admin_level, it->boundary};
                if (best == NO_DATA || claim_wins(rank, best_rank)) {
                    best = p;
                    best_rank = rank;
                }
            }
            flag_claimed_place_node(nodes[i], best);
            n++;
        }
        hidden += n;
    }, threads);
    return hidden;
}

// Flags the place nodes the name match links (find_linked_place's last
// step): `links` holds (node, polygon) for each boundary ring and its best
// node; a node several rings pick names the one claim_wins picks.
inline size_t hide_name_linked_place_nodes(std::vector<PlaceNode>& nodes, std::vector<std::pair<uint32_t, uint32_t>> links,
                                           const std::vector<AdminPolygon>& polygons,
                                           const std::vector<uint64_t>& polygon_ids) {
    auto rank = [&](uint32_t p) {
        return ClaimRank{false, polygons[p].admin_level, polygon_ids.size() == polygons.size() ? polygon_ids[p] : p};
    };
    std::sort(links.begin(), links.end(), [&](const auto& a, const auto& b) {
        if (a.first != b.first) return a.first < b.first;
        if (claim_wins(rank(a.second), rank(b.second))) return true;
        if (claim_wins(rank(b.second), rank(a.second))) return false;
        return a.second < b.second;
    });
    size_t hidden = 0;
    for (size_t k = 0; k < links.size(); k++) {
        if (k > 0 && links[k].first == links[k - 1].first) continue;
        flag_claimed_place_node(nodes[links[k].first], links[k].second);
        hidden++;
    }
    return hidden;
}

// Moves a place node's parent_poly_id into another polygon id space
// (remap[old] = new, NO_DATA for a polygon left out). A node whose claimant
// is left out stays hidden but names no boundary.
inline void remap_place_parent(PlaceNode& pn, const std::vector<uint32_t>& remap) {
    pn.parent_poly_id = pn.parent_poly_id < remap.size() ? remap[pn.parent_poly_id] : NO_DATA;
    if (pn.parent_poly_id == NO_DATA) pn._pad1 &= ~PLACE_FLAG_LABEL_OF;
}
