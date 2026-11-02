#include "admin_links.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

#include "cell_walk.h"
#include "parallel.h"
#include "parent_linking.h"
#include "polygon_rings.h"
#include "ring_contains.h"
#include "s2_helpers.h"

namespace {

// Admin polygon containment with every ring's RingBox precomputed, so the ray
// cast only runs where the exact box reject can't answer. Polygons whose
// vertex range is out of bounds get the empty box; callers skip them anyway.
class AdminRings {
public:
    AdminRings(const ParsedData& data, unsigned int threads)
        : data_(data), boxes_(data.admin_polygons.size()), edges_(data.admin_polygons.size()) {
        // The polygons come largest first: hand them out a few at a time.
        parallel_for_dynamic(boxes_.size(), kRingsPerChunk, [&](size_t begin, size_t end, unsigned) {
            for (size_t i = begin; i < end; i++) {
                const auto& p = data.admin_polygons[i];
                if (p.vertex_offset + p.vertex_count > data.admin_vertices.size()) {
                    boxes_[i] = ring_box(nullptr, 0);
                    continue;
                }
                const NodeCoord* verts = &data.admin_vertices[p.vertex_offset];
                boxes_[i] = ring_box(verts, p.vertex_count);
                if (p.vertex_count >= kMinIndexedRingVertices)
                    edges_[i] = std::make_unique<RingEdgeIndex>(verts, p.vertex_count);
            }
        }, threads);
    }

    bool contains(uint32_t pid, float lat, float lng) const {
        if (ring_box_excludes(boxes_[pid], lat, lng)) return false;
        if (edges_[pid]) return edges_[pid]->contains(lat, lng);
        const auto& p = data_.admin_polygons[pid];
        return ring_contains(&data_.admin_vertices[p.vertex_offset], p.vertex_count, lat, lng);
    }

    // The first of `ranked` (see rank_candidates) containing the point, or NO_DATA.
    uint32_t first_containing(const std::vector<uint32_t>& ranked, float lat, float lng) const {
        for (uint32_t pid : ranked)
            if (contains(pid, lat, lng)) return pid;
        return NO_DATA;
    }

private:
    // Rings this long cast their ray through a RingEdgeIndex, which answers
    // as ring_contains does: the passes test millions of points against the
    // same big rings, each a scan of thousands of edges.
    static constexpr uint32_t kMinIndexedRingVertices = 64;
    static constexpr size_t kRingsPerChunk = 16;

    const ParsedData& data_;
    std::vector<RingBox> boxes_;
    std::vector<std::unique_ptr<RingEdgeIndex>> edges_;  // null below kMinIndexedRingVertices
};

// Calls visit(pid, polygon) for each in-bounds admin polygon listed in `cell`
// or its neighbours, in the order the parent passes have always scanned them:
// the cell, then AppendAllNeighbors order, each cell's list in order. An id
// listed in several cells is visited once per listing.
template <typename Visit>
void for_each_admin_candidate(const ParsedData& data, S2CellId cell,
                std::vector<S2CellId>& nbrs, Visit visit) {
    auto scan = [&](S2CellId c) {
        auto it = data.cell_to_admin.find(c.id());
        if (it == data.cell_to_admin.end()) return;
        for (uint32_t raw_id : it->second) {
            uint32_t pid = raw_id & ID_MASK;
            if (pid >= data.admin_polygons.size()) continue;
            const auto& p = data.admin_polygons[pid];
            if (p.vertex_offset + p.vertex_count > data.admin_vertices.size()) continue;
            visit(pid, p);
        }
    };
    nbrs.clear();
    cell.AppendAllNeighbors(kAdminCellLevel, &nbrs);
    scan(cell);
    for (S2CellId n : nbrs) scan(n);
}

// rank_candidates orders admin polygons by area, which needs a strict weak
// order. The shoelace area of finite coordinates is never NaN.
void require_ordered_admin_areas(const ParsedData& data) {
    for (const auto& p : data.admin_polygons)
        if (std::isnan(p.area)) throw std::runtime_error("Admin polygon area is NaN");
}

// rank_candidates order of the admin parent-chain picks: highest admin_level
// (the closest parent), then smallest area.
auto closest_parent_first(const ParsedData& data) {
    return [&data](uint32_t a, uint32_t b) {
        const auto& pa = data.admin_polygons[a];
        const auto& pb = data.admin_polygons[b];
        if (pa.admin_level != pb.admin_level) return pa.admin_level > pb.admin_level;
        return pa.area < pb.area;
    };
}

// Runs fn(ids) on every cell's list in cell_to_admin, lists spread over the
// cores; fn must touch only the list it is given.
template <typename Fn>
void for_each_admin_cell_list(ParsedData& data, unsigned int threads, Fn fn) {
    const auto cells = cell_lists(data.cell_to_admin, threads);
    parallel_for(cells.size(), [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end; i++) fn(*cells[i].second);
    }, threads);
}

}  // namespace

void remap_admin_candidates(ParsedData& data, const std::vector<uint32_t>& old_to_new, unsigned int threads) {
    for_each_admin_cell_list(data, threads, [&](std::vector<uint32_t>& ids) {
        for (auto& id : ids) {
            uint32_t flags = id & INTERIOR_FLAG;
            uint32_t old_id = id & ID_MASK;
            id = old_to_new[old_id] | flags;
        }
    });
}

// Deterministic tie-break for the smallest-containing-admin parent
// computations (way_parent / admin_parent / poi parent). Those
// pick the min-area containing polygon with a strict `area < best`,
// leaving exact-area ties to whichever candidate is iterated first.
// AdminPolygon.area is float32, so distinct polygons routinely share
// an area value, and cell_to_admin's in-memory order is
// non-deterministic (S2 cover drain + Sort #1 remap, never re-sorted),
// so the tie winner flipped between same-PBF builds (~33k way parents
// of churn). Sort each cell's candidate list by
// admin osm_id (build-invariant, unique, kept aligned by Sort #1) so
// the first-of-equal-area winner is stable. write_cell_index re-sorts
// by raw poly_id at write time, so the on-disk layout is unaffected;
// this order only governs the parent tie-breaks.
void order_admin_candidates(ParsedData& data, unsigned int threads) {
    if (data.admin_osm_ids.size() == data.admin_polygons.size()) {
        for_each_admin_cell_list(data, threads, [&](std::vector<uint32_t>& ids) {
            std::sort(ids.begin(), ids.end(), [&](uint32_t x, uint32_t y) {
                return data.admin_osm_ids[x & ID_MASK] <
                       data.admin_osm_ids[y & ID_MASK];
            });
        });
    }
}

// Admin polygon parent chain: for each admin polygon, find the closest containing
// polygon with a lower admin_level so the server walks the chain without PIP.
// "Containing" is judged at a point inside the polygon, as Nominatim computes a
// boundary's address from its get_center_point (ST_PointOnSurface): the vertex
// average lies outside concave rings (West Bengal's fell in Bangladesh,
// Karakalpakstan's in Turkmenistan), which handed them a foreign parent.
void compute_admin_parent_chain(ParsedData& data, unsigned int num_threads) {
    {
        auto _ap_t = std::chrono::steady_clock::now();
        auto _ap_cpu = CpuTicks::now();
        data.admin_parent_ids.assign(data.admin_polygons.size(), NO_DATA);
        require_ordered_admin_areas(data);
        const AdminRings rings(data, num_threads);
        auto closer = closest_parent_first(data);
        struct Candidates {
            std::vector<S2CellId> nbrs;
            std::vector<uint32_t> parents;
            std::vector<std::pair<uint32_t, uint32_t>> scratch;
        };
        std::vector<Candidates> scratch(std::max(1u, num_threads));
        parallel_for_dynamic(data.admin_polygons.size(), 64, [&](size_t begin, size_t end, unsigned worker) {
            Candidates& c = scratch[worker];
            for (size_t i = begin; i < end; i++) {
                const auto& self = data.admin_polygons[i];
                if (self.vertex_count == 0 || self.admin_level <= 2) continue;
                const auto [ilat, ilng] = polygon_interior_point(
                    &data.admin_vertices[self.vertex_offset], self.vertex_count, self.hole_count);
                const float clat = static_cast<float>(ilat);
                const float clng = static_cast<float>(ilng);
                S2CellId cell = S2CellId(S2LatLng::FromDegrees(clat, clng)).parent(kAdminCellLevel);

                // Pick the closest parent: highest admin_level
                // (most immediate), with smallest area as tiebreaker.
                // This matches Nominatim's parent_place_id = closest
                // containing by rank_address. A level-0 polygon must
                // beat the old scan's starting best area.
                c.parents.clear();
                for_each_admin_candidate(data, cell, c.nbrs, [&](uint32_t pid, const AdminPolygon& p) {
                    if (pid == i || p.admin_level >= self.admin_level) return;
                    if (p.admin_level > 0 || p.area < 1e18f) c.parents.push_back(pid);
                });
                rank_candidates(c.parents, closer, c.scratch);
                data.admin_parent_ids[i] = rings.first_containing(c.parents, clat, clng);
            }
        }, num_threads);
        double el = std::chrono::duration<double>(std::chrono::steady_clock::now() - _ap_t).count();
        uint32_t linked = 0;
        for (auto id : data.admin_parent_ids) if (id != NO_DATA) linked++;
        std::cerr << "Admin parent chain: " << linked << "/" << data.admin_polygons.size()
                  << " polygons linked in " << el << "s" << std::endl;
        log_phase("    Admin: parent chain", _ap_t, _ap_cpu);
    }
}

// Way parent polygon: for each street way, find the smallest admin polygon containing
// its centroid (server walks the admin chain from a road).
void compute_way_parent_polygons(ParsedData& data, unsigned int num_threads) {
    {
        auto _wp_t = std::chrono::steady_clock::now();
        auto _wp_cpu = CpuTicks::now();
        data.way_parent_ids.assign(data.ways.size(), NO_DATA);
        data.way_postcode_ids.assign(data.ways.size(), NO_DATA);
        require_ordered_admin_areas(data);
        const AdminRings rings(data, num_threads);
        auto more_specific = closest_parent_first(data);

        // Note: per-way postcode is set from postal boundary PIP only.
        // The centroid fallback (get_nearest_postcode) is handled at
        // query time by the server using the per-country-validated
        // centroid index. Previously the way sweep also did centroid
        // lookup, but that used the raw (non-per-country) accum which
        // produced wrong results from cross-country contamination.

        auto centroid = [&](const WayHeader& w) {
            float sum_lat = 0, sum_lng = 0;
            uint16_t cnt = w.node_count;
            for (uint16_t k = 0; k < cnt; k++) {
                sum_lat += data.street_nodes[w.node_offset + k].lat;
                sum_lng += data.street_nodes[w.node_offset + k].lng;
            }
            return NodeCoord{sum_lat / cnt, sum_lng / cnt};
        };
        // Ways are walked per centroid admin cell, so each cell's
        // candidates are gathered and ranked once.
        auto order = items_by_cell(data.ways.size(), num_threads, [&](size_t i) -> uint64_t {
            const auto& w = data.ways[i];
            if (w.node_count < 1) return 0;
            NodeCoord c = centroid(w);
            return S2CellId(S2LatLng::FromDegrees(c.lat, c.lng)).parent(kAdminCellLevel).id();
        });

        struct PostalVote { uint32_t pid; uint32_t votes; float area; };
        struct Candidates {
            std::vector<S2CellId> nbrs;
            std::vector<uint32_t> admins, postals;
            std::vector<std::pair<uint32_t, uint32_t>> scratch;
            std::vector<uint32_t> seen;
            std::vector<PostalVote> postal_votes;
        };
        walk_items_by_cell<Candidates>(order, num_threads,
            [&](S2CellId cell, Candidates& c) {
                // Admin levels <= 10 compete for the parent chain, postal
                // boundaries (11) for the postcode. A level-0 polygon must
                // beat the old per-way scan's starting best area.
                c.admins.clear();
                c.postals.clear();
                for_each_admin_candidate(data, cell, c.nbrs, [&](uint32_t pid, const AdminPolygon& p) {
                    if (p.admin_level <= 10) {
                        if (p.admin_level > 0 || p.area < 1e18f) c.admins.push_back(pid);
                    } else if (p.admin_level == 11) {
                        c.postals.push_back(pid);
                    }
                });
                rank_candidates(c.admins, more_specific, c.scratch);
                keep_first_occurrences(c.postals, c.seen);
            },
            [&](uint32_t i, Candidates& c) {
                const auto& w = data.ways[i];
                uint16_t cnt = w.node_count;
                NodeCoord center = centroid(w);

                // Pick the most specific containing polygon: highest
                // admin_level, then smallest area. Parent-chain pick uses
                // centroid PIP — admin hierarchy is fine-grained enough
                // that centroid containment gives a stable result.
                data.way_parent_ids[i] = rings.first_containing(c.admins, center.lat, center.lng);

                // Nominatim's `getNearFeatures`
                // (lib-sql/functions/partition-functions.sql:42)
                // returns every postal polygon that intersects the
                // street's LineString, ordered by
                //   `min(ST_Distance(way_centroid, poly_geom))
                //    + 0.00001 * ST_Distance(way, poly_centroid)`.
                // The dominant term picks polygons containing the way's
                // centroid; when multiple polygons overlap the way but
                // only one contains its centroid, that one wins. For a
                // long street crossing several postal polygons (Delhi's
                // Kartavya Path traverses 110001 / 110004 / 110098),
                // which polygon owns the centroid depends on the
                // street's shape — centroid PIP alone can pick a polygon
                // that covers only a small fraction of the way. We tally
                // a per-polygon vertex-containment vote and pick the
                // polygon that covers the most way vertices; ties break
                // by smallest area. That matches the spirit of
                // `ST_Distance(way, poly_centroid)` closer than pure
                // centroid containment.
                // Votes count sampled vertices (every step-th, to bound
                // work on huge ways). Polygons holding at least one
                // sample vote in first-listed order: std::sort breaks
                // (votes, area) ties by input order.
                auto& postal_votes = c.postal_votes;
                postal_votes.clear();
                uint8_t samples = cnt < 16 ? cnt : 16;
                uint32_t step = cnt > samples ? cnt / samples : 1;
                for (uint32_t pid : c.postals) {
                    uint32_t votes = 0;
                    for (uint32_t k = 0; k < cnt; k += step) {
                        const auto& nd = data.street_nodes[w.node_offset + k];
                        if (rings.contains(pid, nd.lat, nd.lng)) votes++;
                    }
                    if (votes > 0) postal_votes.push_back({pid, votes, data.admin_polygons[pid].area});
                }
                // Sort: highest votes first, tiebreak by smaller area
                // (more specific).
                std::sort(postal_votes.begin(), postal_votes.end(),
                    [](const PostalVote& a, const PostalVote& b) {
                        if (a.votes != b.votes) return a.votes > b.votes;
                        return a.area < b.area;
                    });
                if (!postal_votes.empty()) {
                    data.way_postcode_ids[i] = data.admin_polygons[postal_votes[0].pid].name_id;
                }
            });
        // Counted afterwards: a shared per-way counter would bounce one
        // cache line between every core.
        size_t way_linked = parallel_count(data.ways.size(),
            [&](size_t i) { return data.way_parent_ids[i] != NO_DATA; }, num_threads);
        double el = std::chrono::duration<double>(std::chrono::steady_clock::now() - _wp_t).count();
        std::cerr << "Way parent polygon: " << way_linked << "/" << data.ways.size()
                  << " ways linked in " << el << "s" << std::endl;
        log_phase("    Admin: way parent polygons", _wp_t, _wp_cpu);
    }
}

// POI parent polygons, the smallest containing the POI centroid (area as the
// specificity proxy, lower = tighter; ties to the first one scanned):
// - parent_postcode_id: the postal boundary's (11) postcode, Nominatim's
//   placex.postcode chain approximation.
// - parent_poly_id: the admin polygon (levels 2..10; place-area markers 15
//   skipped) the server's chain-containment check walks up from.
// POIs are walked per admin cell, so each cell's candidates are gathered and
// ranked once for both. `order` holds the POIs by street cell (items_by_cell):
// a POI's admin cell is an ancestor of its street cell, so the same order
// keeps each admin cell's POIs together.
void compute_poi_parent_polygons(ParsedData& data, unsigned int num_threads, std::vector<CellItemPair> order,
                std::chrono::steady_clock::time_point& _s2t, CpuTicks& _s2cpu) {
    if (!data.poi_records.empty() && !data.admin_polygons.empty()) {
        auto _pp_t = std::chrono::steady_clock::now();
        std::cerr << "Computing POI parent-postcode and parent-admin links ("
                  << data.poi_records.size() << " POIs)..."
                  << std::endl;

        require_ordered_admin_areas(data);
        const AdminRings rings(data, num_threads);
        auto smaller = [&](uint32_t a, uint32_t b) {
            return data.admin_polygons[a].area < data.admin_polygons[b].area;
        };
        // order holds each POI's street cell, whose parent is its admin cell
        // unless --admin-level is the finer one; then the point decides.
        const bool admin_from_street = kAdminCellLevel <= kStreetCellLevel;
        parallel_for(order.size(), [&](size_t b, size_t e, unsigned) {
            for (size_t k = b; k < e; k++) {
                const auto& poi = data.poi_records[order[k].item_id];
                S2CellId cell = admin_from_street ? S2CellId(order[k].cell_id)
                                                  : S2CellId(S2LatLng::FromDegrees(poi.lat, poi.lng));
                order[k].cell_id = cell.parent(kAdminCellLevel).id();
            }
        }, num_threads);

        struct Candidates {
            std::vector<S2CellId> nbrs;
            std::vector<uint32_t> postal, admin;
            std::vector<std::pair<uint32_t, uint32_t>> scratch;
        };
        walk_items_by_cell<Candidates>(order, num_threads,
            [&](S2CellId cell, Candidates& c) {
                c.postal.clear();
                c.admin.clear();
                // 1e18 is the old per-POI scan's starting best area.
                for_each_admin_candidate(data, cell, c.nbrs, [&](uint32_t pid, const AdminPolygon& p) {
                    if (!(p.area < 1e18f)) return;
                    if (p.admin_level == 11) c.postal.push_back(pid);
                    else if (p.admin_level >= 2 && p.admin_level <= 10) c.admin.push_back(pid);
                });
                rank_candidates(c.postal, smaller, c.scratch);
                rank_candidates(c.admin, smaller, c.scratch);
            },
            [&](uint32_t i, Candidates& c) {
                auto& pr = data.poi_records[i];
                uint32_t postal = rings.first_containing(c.postal, pr.lat, pr.lng);
                pr.parent_postcode_id = postal == NO_DATA ? NO_DATA : data.admin_polygons[postal].name_id;
                pr.parent_poly_id = rings.first_containing(c.admin, pr.lat, pr.lng);
            });
        // Counted afterwards: shared per-POI counters would bounce one cache
        // line between every core. Postal boundaries are named, so a POI
        // has a postcode exactly when it found one.
        size_t postcode_linked = parallel_count(data.poi_records.size(),
            [&](size_t i) { return data.poi_records[i].parent_postcode_id != NO_DATA; }, num_threads);
        size_t admin_linked = parallel_count(data.poi_records.size(),
            [&](size_t i) { return data.poi_records[i].parent_poly_id != NO_DATA; }, num_threads);

        double _pp_el = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - _pp_t).count();
        std::cerr << "POI parent-postcode linking: "
                  << postcode_linked << "/"
                  << data.poi_records.size()
                  << " POIs linked to a postal boundary" << std::endl;
        std::cerr << "POI parent-admin linking: "
                  << admin_linked << "/"
                  << data.poi_records.size()
                  << " POIs linked to an admin polygon in "
                  << _pp_el << "s" << std::endl;
        log_phase("  POI: parent-postcode + parent-admin linking", _s2t, _s2cpu);
    }
}
