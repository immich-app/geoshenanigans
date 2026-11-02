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

#include "admin_cover.h"
#include "cell_index_io.h"
#include "cell_walk.h"
#include "parallel.h"
#include "parent_linking.h"
#include "polygon_rings.h"
#include "s2_helpers.h"

namespace {

// Calls visit(id, polygon) for each admin polygon listed in `cell`, id its
// entry there (INTERIOR_FLAG kept), in the list's order.
template <typename Visit>
void for_each_admin_candidate(const ParsedData& data, S2CellId cell, Visit visit) {
    auto it = data.cell_to_admin.find(cell.id());
    if (it == data.cell_to_admin.end()) return;
    for (uint32_t id : it->second) {
        const uint32_t pid = id & ID_MASK;
        if (pid >= data.admin_polygons.size()) continue;
        visit(id, data.admin_polygons[pid]);
    }
}

// rank_candidates orders admin polygons by area, which needs a strict weak
// order. The shoelace area of finite coordinates is never NaN.
void require_ordered_admin_areas(const ParsedData& data) {
    for (const auto& p : data.admin_polygons)
        if (std::isnan(p.area)) throw std::runtime_error("Admin polygon area is NaN");
}

// rank_candidates order of the admin parent-chain picks over candidate
// entries: highest admin_level (the closest parent), then smallest area.
auto closest_parent_first(const ParsedData& data) {
    return [&data](uint32_t a, uint32_t b) {
        const auto& pa = data.admin_polygons[a & ID_MASK];
        const auto& pb = data.admin_polygons[b & ID_MASK];
        if (pa.admin_level != pb.admin_level) return pa.admin_level > pb.admin_level;
        return pa.area < pb.area;
    };
}

// rank_candidates order of the smallest containing polygon over candidate
// entries.
auto smallest_first(const ParsedData& data) {
    return [&data](uint32_t a, uint32_t b) {
        return data.admin_polygons[a & ID_MASK].area < data.admin_polygons[b & ID_MASK].area;
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

S2CellId admin_link_cell(PackedLocation p) {
    return S2CellId(S2LatLng::FromDegrees(p.lat(), p.lon())).parent(kAdminCellLevel);
}

std::vector<CellItemPair> admin_candidate_cells(const AdminExactRings& rings, uint32_t begin, uint32_t end,
                                                unsigned int threads) {
    if (threads == 0) threads = parallel_threads();
    auto vertices_of = [&](size_t k) {
        const uint32_t pid = begin + static_cast<uint32_t>(k);
        const RingSpan* r = rings.rings_of(pid);
        size_t count = 0;
        for (uint32_t q = 0; q < rings.polygons[pid].ring_count; q++) count += r[q].count;
        return count;
    };
    // Largest first, one at a time: a country takes seconds, and started last
    // it would leave one core working alone.
    const std::vector<uint32_t> largest_first = costliest_first(end - begin, vertices_of, threads);
    std::vector<std::vector<CellItemPair>> found(threads);
    parallel_for_dynamic(largest_first.size(), 1, [&](size_t from, size_t to, unsigned worker) {
        for (size_t k = from; k < to; k++) {
            const uint32_t pid = begin + largest_first[k];
            const AdminRingCells cells = admin_ring_cells(rings.vertices.data(), rings.rings_of(pid),
                                                          rings.polygons[pid].ring_count, kAdminCellLevel,
                                                          ADMIN_LINK_REACH_RAD);
            for (const auto& [cell, interior] : cells)
                found[worker].push_back({cell, interior ? pid | INTERIOR_FLAG : pid});
        }
    }, threads);
    std::vector<size_t> at(found.size() + 1, 0);
    for (size_t w = 0; w < found.size(); w++) at[w + 1] = at[w] + found[w].size();
    std::vector<CellItemPair> out;
    parallel_resize(out, at.back());
    parallel_for_each(found.size(), [&](size_t w, unsigned) {
        std::copy(found[w].begin(), found[w].end(), out.begin() + at[w]);
        std::vector<CellItemPair>().swap(found[w]);
    }, threads);
    return out;
}

std::unordered_map<uint64_t, std::vector<uint32_t>> admin_candidate_map(std::vector<CellItemPair> cells,
                                                                         unsigned int threads) {
    // A polygon gives a cell once, so (cell, id) pairs never repeat.
    parallel_sort(cells.begin(), cells.end(), cell_item_less, threads);
    std::unordered_map<uint64_t, std::vector<uint32_t>> map;
    map.reserve(count_cells(cells));
    for (size_t k = 0; k < cells.size();) {
        size_t run_end = k + 1;
        while (run_end < cells.size() && cells[run_end].cell_id == cells[k].cell_id) run_end++;
        std::vector<uint32_t>& ids = map[cells[k].cell_id];
        ids.reserve(run_end - k);
        for (; k < run_end; k++) ids.push_back(cells[k].item_id);
    }
    return map;
}

AdminRings::AdminRings(const AdminExactRings& rings, unsigned int threads,
                       const std::function<bool(uint32_t)>& indexed)
    : rings_(rings), boxes_(rings.rings.size()), edges_(rings.rings.size()) {
    parallel_for_dynamic(rings.polygons.size(), kPolygonsPerChunk, [&](size_t begin, size_t end, unsigned) {
        for (size_t pid = begin; pid < end; pid++) {
            const auto& polygon = rings.polygons[pid];
            const bool index = !indexed || indexed(static_cast<uint32_t>(pid));
            for (uint32_t r = polygon.first_ring; r < polygon.first_ring + polygon.ring_count; r++) {
                const PackedLocation* v = rings.vertices.data() + rings.rings[r].begin;
                const uint32_t n = rings.rings[r].count;
                boxes_[r] = exact_ring_box(v, n);
                if (index && n >= kMinIndexedRingVertices) edges_[r] = std::make_unique<ExactRingEdgeIndex>(v, n);
            }
        }
    }, threads);
}

bool AdminRings::contains(uint32_t pid, PackedLocation p) const {
    const auto& polygon = rings_.polygons[pid];
    bool inside = false;
    for (uint32_t r = polygon.first_ring; r < polygon.first_ring + polygon.ring_count; r++) {
        if (exact_ring_box_excludes(boxes_[r], p)) continue;
        if (edges_[r]) {
            inside ^= edges_[r]->contains(p);
        } else {
            const RingSpan& span = rings_.rings[r];
            inside ^= exact_ring_contains(rings_.vertices.data() + span.begin, span.count, p);
        }
    }
    return inside;
}

uint32_t AdminRings::first_containing(const std::vector<uint32_t>& ranked, PackedLocation p) const {
    for (uint32_t id : ranked)
        if ((id & INTERIOR_FLAG) || contains(id & ID_MASK, p)) return id & ID_MASK;
    return NO_DATA;
}

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
// AdminPolygon.area is float32, so distinct polygons can share an area
// value. Sort each cell's candidate list by admin osm_id
// (build-invariant, unique, kept aligned by the largest-first sort) so
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
        const AdminExactRings& exact = data.admin_exact_rings;
        const AdminRings rings(exact, num_threads);
        auto closer = closest_parent_first(data);
        struct Candidates {
            std::vector<uint32_t> parents;
            std::vector<std::pair<uint32_t, uint32_t>> scratch;
        };
        std::vector<Candidates> scratch(std::max(1u, num_threads));
        parallel_for_dynamic(data.admin_polygons.size(), 64, [&](size_t begin, size_t end, unsigned worker) {
            Candidates& c = scratch[worker];
            for (size_t i = begin; i < end; i++) {
                const auto& self = data.admin_polygons[i];
                const uint32_t ring_count = exact.polygons[i].ring_count;
                if (ring_count == 0 || self.admin_level <= 2) continue;
                const auto [ilat, ilng] = interior_point(exact.vertices.data(), exact.rings_of(i), ring_count);
                const PackedLocation inside = point_e7(ilat, ilng);

                // Pick the closest parent: highest admin_level
                // (most immediate), with smallest area as tiebreaker.
                // This matches Nominatim's parent_place_id = closest
                // containing by rank_address.
                c.parents.clear();
                for_each_admin_candidate(data, admin_link_cell(inside), [&](uint32_t id, const AdminPolygon& p) {
                    if ((id & ID_MASK) == i || p.admin_level >= self.admin_level) return;
                    c.parents.push_back(id);
                });
                rank_candidates(c.parents, closer, c.scratch);
                data.admin_parent_ids[i] = rings.first_containing(c.parents, inside);
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
        const AdminRings rings(data.admin_exact_rings, num_threads);
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
            return point_e7(sum_lat / cnt, sum_lng / cnt);
        };
        // Ways are walked per centroid admin cell, so each cell's
        // candidates are gathered and ranked once.
        auto order = items_by_cell(data.ways.size(), num_threads, [&](size_t i) -> uint64_t {
            const auto& w = data.ways[i];
            if (w.node_count < 1) return 0;
            return admin_link_cell(centroid(w)).id();
        });

        struct PostalVote { uint32_t pid; uint32_t votes; float area; };
        struct Candidates {
            uint64_t cell = 0;
            std::vector<uint32_t> admins, postals;  // the centroid cell's
            std::vector<PackedLocation> samples;
            std::vector<uint64_t> other_cells;      // samples' cells past the centroid's
            std::vector<uint32_t> sample_postals;
            std::vector<std::pair<uint32_t, uint32_t>> scratch;
            std::vector<PostalVote> postal_votes;
        };
        auto add_postals = [&](S2CellId cell, std::vector<uint32_t>& postals) {
            for_each_admin_candidate(data, cell, [&](uint32_t id, const AdminPolygon& p) {
                if (p.admin_level == 11) postals.push_back(id & ID_MASK);
            });
        };
        walk_items_by_cell<Candidates>(order, num_threads,
            [&](S2CellId cell, Candidates& c) {
                // Admin levels <= 10 compete for the parent chain, postal
                // boundaries (11) for the postcode.
                c.cell = cell.id();
                c.admins.clear();
                c.postals.clear();
                for_each_admin_candidate(data, cell, [&](uint32_t id, const AdminPolygon& p) {
                    if (p.admin_level <= 10) c.admins.push_back(id);
                });
                add_postals(cell, c.postals);
                rank_candidates(c.admins, more_specific, c.scratch);
            },
            [&](uint32_t i, Candidates& c) {
                const auto& w = data.ways[i];
                uint16_t cnt = w.node_count;

                // Pick the most specific containing polygon: highest
                // admin_level, then smallest area. Parent-chain pick uses
                // centroid PIP — admin hierarchy is fine-grained enough
                // that centroid containment gives a stable result.
                data.way_parent_ids[i] = rings.first_containing(c.admins, centroid(w));

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
                // work on huge ways). A postal polygon holding a sample is
                // listed in that sample's own cell, which needn't be the
                // centroid's.
                uint8_t samples = cnt < 16 ? cnt : 16;
                uint32_t step = cnt > samples ? cnt / samples : 1;
                c.samples.clear();
                c.other_cells.clear();
                for (uint32_t k = 0; k < cnt; k += step) {
                    const auto& nd = data.street_nodes[w.node_offset + k];
                    c.samples.push_back(point_e7(nd.lat, nd.lng));
                    const uint64_t cell = admin_link_cell(c.samples.back()).id();
                    if (cell != c.cell) c.other_cells.push_back(cell);
                }
                const std::vector<uint32_t>* postals = &c.postals;
                if (!c.other_cells.empty()) {
                    std::sort(c.other_cells.begin(), c.other_cells.end());
                    c.other_cells.erase(std::unique(c.other_cells.begin(), c.other_cells.end()), c.other_cells.end());
                    c.sample_postals = c.postals;
                    for (uint64_t cell : c.other_cells) add_postals(S2CellId(cell), c.sample_postals);
                    std::sort(c.sample_postals.begin(), c.sample_postals.end());
                    c.sample_postals.erase(std::unique(c.sample_postals.begin(), c.sample_postals.end()),
                                           c.sample_postals.end());
                    postals = &c.sample_postals;
                }
                auto& postal_votes = c.postal_votes;
                postal_votes.clear();
                for (uint32_t pid : *postals) {
                    uint32_t votes = 0;
                    for (const PackedLocation& p : c.samples)
                        if (rings.contains(pid, p)) votes++;
                    if (votes > 0) postal_votes.push_back({pid, votes, data.admin_polygons[pid].area});
                }
                // Highest votes first, then the smaller area (more
                // specific), then the lower osm id, whichever cells listed
                // the polygons.
                std::sort(postal_votes.begin(), postal_votes.end(),
                    [&](const PostalVote& a, const PostalVote& b) {
                        if (a.votes != b.votes) return a.votes > b.votes;
                        if (a.area != b.area) return a.area < b.area;
                        return data.admin_osm_ids[a.pid] < data.admin_osm_ids[b.pid];
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

// POI parent polygons, the smallest containing the POI (area as the
// specificity proxy, lower = tighter; ties to the first one listed):
// - parent_postcode_id: the postal boundary's (11) postcode, Nominatim's
//   placex.postcode chain approximation.
// - parent_poly_id: the admin polygon (levels 2..10; place-area markers 15
//   skipped) the server's chain-containment check walks up from.
// POIs are walked per admin cell, so each cell's candidates are gathered and
// ranked once for both.
void compute_poi_parent_polygons(ParsedData& data, unsigned int num_threads, std::vector<CellItemPair> order,
                std::chrono::steady_clock::time_point& _s2t, CpuTicks& _s2cpu) {
    if (!data.poi_records.empty() && !data.admin_polygons.empty()) {
        auto _pp_t = std::chrono::steady_clock::now();
        std::cerr << "Computing POI parent-postcode and parent-admin links ("
                  << data.poi_records.size() << " POIs)..."
                  << std::endl;

        require_ordered_admin_areas(data);
        const AdminRings rings(data.admin_exact_rings, num_threads);
        auto smaller = smallest_first(data);
        auto point_of = [&](uint32_t i) { return point_e7(data.poi_records[i].lat, data.poi_records[i].lng); };
        parallel_for(order.size(), [&](size_t b, size_t e, unsigned) {
            for (size_t k = b; k < e; k++) order[k].cell_id = admin_link_cell(point_of(order[k].item_id)).id();
        }, num_threads);

        struct Candidates {
            std::vector<uint32_t> postal, admin;
            std::vector<std::pair<uint32_t, uint32_t>> scratch;
        };
        walk_items_by_cell<Candidates>(order, num_threads,
            [&](S2CellId cell, Candidates& c) {
                c.postal.clear();
                c.admin.clear();
                for_each_admin_candidate(data, cell, [&](uint32_t id, const AdminPolygon& p) {
                    if (p.admin_level == 11) c.postal.push_back(id);
                    else if (p.admin_level >= 2 && p.admin_level <= 10) c.admin.push_back(id);
                });
                rank_candidates(c.postal, smaller, c.scratch);
                rank_candidates(c.admin, smaller, c.scratch);
            },
            [&](uint32_t i, Candidates& c) {
                auto& pr = data.poi_records[i];
                const PackedLocation p = point_of(i);
                uint32_t postal = rings.first_containing(c.postal, p);
                pr.parent_postcode_id = postal == NO_DATA ? NO_DATA : data.admin_polygons[postal].name_id;
                pr.parent_poly_id = rings.first_containing(c.admin, p);
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
