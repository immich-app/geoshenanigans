// Place nodes that boundaries claim. Nominatim's find_linked_place links an
// admin boundary to a place node (placex.linked_place_id): the node leaves
// the results and the boundary stands for it. A claimed node keeps its name
// and carries PLACE_FLAG_HIDDEN, which every reverse lookup skips like a
// nameless node, and, when its boundary has a polygon, PLACE_FLAG_LABEL_OF
// with parent_poly_id naming that polygon: place search uses the node as the
// boundary's label point and name. admin-minimal, which keeps boundaries by
// level, shows a node whose claimant it leaves out: nothing there stands for
// it.
#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "admin_rank_config.h"
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

// Whether an admin_osm_ids entry is a ring of a relation, not a closed way.
inline bool admin_relation_ring(uint64_t admin_osm_id) {
    return admin_osm_id >> 56 == static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_RELATION);
}

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

// The polygon of `boundary` in boundary_polygons' list; NO_DATA when it has none.
inline uint32_t boundary_polygon(const std::vector<std::pair<uint64_t, uint32_t>>& boundaries, uint64_t boundary) {
    auto it = std::lower_bound(boundaries.begin(), boundaries.end(), std::make_pair(boundary, uint32_t(0)));
    return it != boundaries.end() && it->first == boundary ? it->second : NO_DATA;
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

// PostgreSQL's lower() of a name, which orders the nodes of a boundary's
// wikidata (wikidata_order): the capital letters of the Latin-1, Latin
// Extended-A, Greek, Cyrillic and Armenian blocks as Unicode's simple case
// mapping lowers them, every other character as it is. Empty for none.
inline std::string lower_name(const char* name) {
    std::string out;
    if (!name) return out;
    auto put = [&](uint32_t cp) {  // below 0x800
        if (cp < 0x80) {
            out += static_cast<char>(cp);
        } else {
            out += static_cast<char>(0xC0 | cp >> 6);
            out += static_cast<char>(0x80 | (cp & 0x3F));
        }
    };
    auto lower = [](uint32_t cp) -> uint32_t {
        auto pair = [&](uint32_t first, uint32_t last, uint32_t upper_parity) {
            return cp >= first && cp <= last && (cp & 1) == upper_parity;
        };
        if (cp >= 'A' && cp <= 'Z') return cp + 32;
        if (cp >= 0xC0 && cp <= 0xDE && cp != 0xD7) return cp + 32;
        if (cp == 0x130) return 'i';
        if (pair(0x100, 0x137, 0) || pair(0x139, 0x148, 1) || pair(0x14A, 0x177, 0) || pair(0x179, 0x17E, 1))
            return cp + 1;
        if (cp == 0x178) return 0xFF;
        if (cp == 0x386) return 0x3AC;
        if (cp >= 0x388 && cp <= 0x38A) return cp + 37;
        if (cp == 0x38C) return 0x3CC;
        if (cp == 0x38E || cp == 0x38F) return cp + 63;
        if (cp >= 0x391 && cp <= 0x3AB && cp != 0x3A2) return cp + 32;
        if (cp >= 0x400 && cp <= 0x40F) return cp + 80;
        if (cp >= 0x410 && cp <= 0x42F) return cp + 32;
        if (pair(0x460, 0x481, 0) || pair(0x48A, 0x4BF, 0) || pair(0x4C1, 0x4CE, 1) || pair(0x4D0, 0x52F, 0))
            return cp + 1;
        if (cp == 0x4C0) return 0x4CF;
        if (cp >= 0x531 && cp <= 0x556) return cp + 48;
        return cp;
    };
    const unsigned char* p = reinterpret_cast<const unsigned char*>(name);
    while (*p) {
        // A character of two bytes, and ASCII, is lowered; every other byte
        // is copied as it is.
        if (p[0] >= 0xC2 && p[0] < 0xE0 && (p[1] & 0xC0) == 0x80) {
            put(lower(static_cast<uint32_t>(p[0] & 0x1F) << 6 | (p[1] & 0x3F)));
            p += 2;
        } else {
            out += static_cast<char>(*p < 0x80 ? lower(*p) : *p);
            p++;
        }
    }
    return out;
}

// A place node of a boundary's wikidata that the boundary covers, one that
// find_linked_place's wikidata step can link (placex_triggers.sql).
struct WikidataCandidate {
    uint64_t boundary;   // relation_boundary_id or way_boundary_id
    uint64_t node;       // pack_osm_id(OSM_NODE, id), as place_osm_ids holds it
    uint8_t order;       // its place in the step's order (wikidata_order)
    uint8_t place_type;  // the AdminPlaceType it labels its boundary with
};

// Where the step's ORDER BY lower(name->'name') = bnd_name desc puts a node
// (lower_name of its name tag and its boundary's, empty for none): first
// where either has none, the comparison null and nulls first descending,
// then a node of the boundary's name, then any other.
inline uint8_t wikidata_order(const std::string& node_name, const std::string& boundary_name) {
    if (node_name.empty() || boundary_name.empty()) return 0;
    return node_name == boundary_name ? 1 : 2;
}

// The rank_address of polygon `poly` (slot p) in no country known
// (polygon_rank_address of country 0): link_wikidata_places' default rank.
struct RankWithoutCountry {
    uint8_t operator()(uint32_t, const AdminPolygon& poly) const {
        return polygon_rank_address(0, poly.admin_level, own_place_tag(poly));
    }
};

// Where a node of the place type `place_type` (AdminPlaceType) comes among
// the nodes of a boundary's wikidata that wikidata_order leaves equal: by its
// address rank, the default one of address-levels.json, one of rank 0 (in no
// address) last.
inline uint8_t candidate_rank_order(uint8_t place_type) {
    const uint8_t rank = place_area_rank_address(place_type);
    return rank == 0 ? UINT8_MAX : rank;
}

// find_linked_place's wikidata step for every boundary of a candidate (a
// node of its wikidata it covers). The boundaries take their turns as
// Nominatim indexes them: by admin level (rank_search), the lowest first,
// then the place areas (level 15), after every boundary, by their address
// ranks (index_by_rank), equals by boundary id. `rank(p, polygons[p])` is the
// rank_address of a boundary's polygon p in its country (its own place
// tag's for a place area); one of rank 0 links nothing (find_linked_place
// returns at once) and takes no turn. Each links the first of its nodes in
// wikidata_order, then candidate_rank_order, then node id, that no boundary
// before it linked (placex.linked_place_id is null). Every polygon of a
// boundary that links a node takes the node's place type, a place area
// keeping its own place tag beside it (own_place_type); one that links none
// keeps its own type for the steps after. The links go to `claims` for
// hide_claimed_place_nodes. Returns how many boundaries link a node.
template <class Rank = RankWithoutCountry>
inline size_t link_wikidata_places(std::vector<WikidataCandidate> candidates, std::vector<AdminPolygon>& polygons,
                                   const std::vector<uint64_t>& polygon_ids, std::vector<PlaceClaim>& claims,
                                   Rank rank = {}) {
    const auto boundaries = boundary_polygons(polygons, polygon_ids);
    if (candidates.empty() || boundaries.empty()) return 0;
    std::sort(candidates.begin(), candidates.end(), [](const WikidataCandidate& a, const WikidataCandidate& b) {
        return std::make_tuple(a.boundary, a.order, candidate_rank_order(a.place_type), a.node) <
               std::make_tuple(b.boundary, b.order, candidate_rank_order(b.place_type), b.node);
    });
    struct Turn {
        uint8_t admin_level;
        uint8_t place_rank;  // a place area's rank_address, 0 for a boundary
        uint64_t boundary;
        size_t begin, end;  // the boundary's candidates
    };
    std::vector<Turn> turns;
    for (size_t k = 0; k < candidates.size();) {
        size_t end = k + 1;
        while (end < candidates.size() && candidates[end].boundary == candidates[k].boundary) end++;
        const uint32_t p = boundary_polygon(boundaries, candidates[k].boundary);
        if (p != NO_DATA) {
            const uint8_t r = rank(p, polygons[p]);
            const uint8_t level = polygons[p].admin_level;
            if (r != 0) turns.push_back({level, level == 15 ? r : uint8_t(0), candidates[k].boundary, k, end});
        }
        k = end;
    }
    std::sort(turns.begin(), turns.end(), [](const Turn& a, const Turn& b) {
        return std::tie(a.admin_level, a.place_rank, a.boundary) < std::tie(b.admin_level, b.place_rank, b.boundary);
    });
    std::unordered_set<uint64_t> linked;
    std::vector<std::pair<uint64_t, uint8_t>> links;  // (boundary, place type), a boundary's one link
    for (const Turn& turn : turns) {
        for (size_t k = turn.begin; k < turn.end; k++) {
            const WikidataCandidate& c = candidates[k];
            if (!linked.insert(c.node).second) continue;
            links.push_back({c.boundary, c.place_type});
            claims.push_back({c.node, c.boundary, false});
            break;
        }
    }
    std::sort(links.begin(), links.end());
    for (uint32_t p = 0; p < polygons.size(); p++) {
        if (polygon_ids[p] == 0) continue;
        const uint64_t boundary = admin_boundary_id(polygon_ids[p]);
        const auto it = std::lower_bound(links.begin(), links.end(), std::make_pair(boundary, uint8_t(0)));
        if (it == links.end() || it->first != boundary) continue;
        AdminPolygon& poly = polygons[p];
        const uint8_t own = poly.place_type_override;
        poly.place_type_override = it->second;
        poly.own_place_type = poly.admin_level == 15 && it->second != own ? own : 0;
    }
    return links.size();
}

// Hides a claimed node; names `polygon` (unless NO_DATA) as its claimant.
inline void flag_claimed_place_node(PlaceNode& pn, uint32_t polygon) {
    pn._pad1 |= PLACE_FLAG_HIDDEN;
    if (polygon == NO_DATA) return;
    pn._pad1 |= PLACE_FLAG_LABEL_OF;
    pn.parent_poly_id = polygon;
}

// Every claimant's polygon links (hide_claimed_place_nodes).
inline bool every_polygon_links(uint32_t) { return true; }

// Flags the place nodes (node_ids parallel to nodes) the label and wikidata
// claims hit, each naming the polygon of the claimant claim_wins picks among
// those with a polygon. A claim of a claimant whose polygon p does not link,
// `links(p)` false (one of rank 0 in its country), is no claim. Returns how
// many nodes it hid.
template <class Links = bool (*)(uint32_t)>
inline size_t hide_claimed_place_nodes(std::vector<PlaceNode>& nodes, const std::vector<uint64_t>& node_ids,
                                       std::vector<PlaceClaim> claims, const std::vector<AdminPolygon>& polygons,
                                       const std::vector<uint64_t>& polygon_ids, unsigned threads = 0,
                                       Links links = every_polygon_links) {
    if (claims.empty()) return 0;
    const auto boundaries = boundary_polygons(polygons, polygon_ids);
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
            bool claimed = false;
            for (; it != claims.end() && it->node == node_ids[i]; ++it) {
                const uint32_t p = boundary_polygon(boundaries, it->boundary);
                if (p != NO_DATA && !links(p)) continue;
                claimed = true;
                if (p == NO_DATA) continue;
                const ClaimRank rank{it->label, polygons[p].admin_level, it->boundary};
                if (best == NO_DATA || claim_wins(rank, best_rank)) {
                    best = p;
                    best_rank = rank;
                }
            }
            if (!claimed) continue;
            flag_claimed_place_node(nodes[i], best);
            n++;
        }
        hidden += n;
    }, threads);
    return hidden;
}

// Flags the place nodes the name match links (find_linked_place's last
// step): `links` holds (node, polygon) for each boundary ring and its best
// node. A node names its boundary's largest ring whichever ring matched, as
// a label or wikidata claim does; a node several boundaries pick names the
// one claim_wins picks.
inline size_t hide_name_linked_place_nodes(std::vector<PlaceNode>& nodes, std::vector<std::pair<uint32_t, uint32_t>> links,
                                           const std::vector<AdminPolygon>& polygons,
                                           const std::vector<uint64_t>& polygon_ids) {
    const auto boundaries = boundary_polygons(polygons, polygon_ids);
    if (!boundaries.empty()) {
        for (auto& link : links) {
            if (polygon_ids[link.second] != 0)
                link.second = boundary_polygon(boundaries, admin_boundary_id(polygon_ids[link.second]));
        }
    }
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

// remap_place_parent for admin-minimal, which keeps boundaries by level
// (admin_minimal_keeps: L2-L8 and the place areas down to rank 16): a node
// whose claimant it leaves out (an orphan) is a place again, as if
// unclaimed, so a city, town or village that an L9 or L10 boundary claims
// still answers there. A node hidden for a claimant without a polygon (no
// PLACE_FLAG_LABEL_OF) stays hidden, as in every set.
inline void remap_place_parent_unhiding_orphans(PlaceNode& pn, const std::vector<uint32_t>& remap) {
    const bool claimed = (pn._pad1 & PLACE_FLAG_LABEL_OF) != 0;
    remap_place_parent(pn, remap);
    if (claimed && pn.parent_poly_id == NO_DATA) pn._pad1 &= ~PLACE_FLAG_HIDDEN;
}
