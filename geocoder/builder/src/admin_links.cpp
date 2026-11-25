#include "admin_links.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

#include "admin_rank_config.h"
#include "cell_index.h"
#include "cell_index_io.h"
#include "cell_walk.h"
#include "parallel.h"
#include "parent_linking.h"
#include "polygon_rings.h"
#include "s2_helpers.h"

namespace {

// Calls visit(id, polygon) for each admin polygon listed in `cell`, id its
// entry there (INTERIOR_FLAG kept), in the index's order.
template <typename Visit>
void for_each_admin_candidate(const ParsedData& data, S2CellId cell, Visit visit) {
    const auto [first, last] = admin_cell_entries(data.admin_index_cells, cell.id());
    for (const CellItemPair* e = first; e != last; ++e) {
        const uint32_t pid = e->item_id & ID_MASK;
        if (pid >= data.admin_polygons.size()) continue;
        visit(e->item_id, data.admin_polygons[pid]);
    }
}

// The tie-break of the passes' rankings: the lower admin osm id, which
// doesn't depend on slots.
bool lower_osm_id(const ParsedData& data, uint32_t a, uint32_t b) {
    return data.admin_osm_ids.size() == data.admin_polygons.size() &&
           data.admin_osm_ids[a & ID_MASK] < data.admin_osm_ids[b & ID_MASK];
}

// rank_candidates orders admin polygons by area, which needs a strict weak
// order. The shoelace area of finite coordinates is never NaN.
void require_ordered_admin_areas(const ParsedData& data) {
    for (const auto& p : data.admin_polygons)
        if (std::isnan(p.area)) throw std::runtime_error("Admin polygon area is NaN");
}

// rank_candidates order of the admin parent-chain picks over candidate
// entries: highest admin_level (the closest parent), then smallest area,
// then the lower osm id.
auto closest_parent_first(const ParsedData& data) {
    return [&data](uint32_t a, uint32_t b) {
        const auto& pa = data.admin_polygons[a & ID_MASK];
        const auto& pb = data.admin_polygons[b & ID_MASK];
        if (pa.admin_level != pb.admin_level) return pa.admin_level > pb.admin_level;
        if (pa.area != pb.area) return pa.area < pb.area;
        return lower_osm_id(data, a, b);
    };
}

// rank_candidates order of the smallest containing polygon over candidate
// entries, then the lower osm id.
auto smallest_first(const ParsedData& data) {
    return [&data](uint32_t a, uint32_t b) {
        const float area_a = data.admin_polygons[a & ID_MASK].area, area_b = data.admin_polygons[b & ID_MASK].area;
        if (area_a != area_b) return area_a < area_b;
        return lower_osm_id(data, a, b);
    };
}

// Whether the server's postal_area_postcode takes a postal area's postcode
// (postcode_looks_valid), whatever its length, assign_postal_area_countries
// having checked it against its country's pattern: one value (no ';'), and
// past its white space and hyphens neither empty nor one character repeated,
// as a placeholder such as 00000 is.
bool postcode_taken(const char* postcode) {
    if (std::strchr(postcode, ';')) return false;
    auto white = [](uint32_t c) {  // Rust's char::is_whitespace
        return (c >= 0x09 && c <= 0x0D) || c == 0x20 || c == 0x85 || c == 0xA0 || c == 0x1680 ||
               (c >= 0x2000 && c <= 0x200A) || c == 0x2028 || c == 0x2029 || c == 0x202F || c == 0x205F ||
               c == 0x3000;
    };
    uint32_t first = 0;
    bool any = false, repeated = true;
    for (const unsigned char* p = reinterpret_cast<const unsigned char*>(postcode); *p;) {
        // One UTF-8 character; a byte of none is a character of its own.
        const int n = *p >= 0xF0 ? 4 : *p >= 0xE0 ? 3 : *p >= 0xC0 ? 2 : 1;
        uint32_t c = n == 1 ? *p : *p & (0x7F >> n);
        int k = 1;
        for (; k < n && (p[k] & 0xC0) == 0x80; k++) c = c << 6 | (p[k] & 0x3F);
        if (k < n) c = *p, k = 1;
        p += k;
        if (white(c) || c == '-') continue;
        if (!any) first = c;
        else if (c != first) repeated = false;
        any = true;
    }
    return any && !repeated;
}

// Whether the postal area `area`, of the postcode `postcode`, names a point of
// the country `cc` (0 where unknown), as the server's postal_area_postcode
// takes it: get_nearest_postcode takes an area of the point's country only
// (utils.sql: p.country_code = country), of a postcode that country's pattern
// takes (not POSTAL_AREA_INVALID) and the server takes (postcode_taken). One
// of no country known names no point of a country, Nominatim making no
// postcode area of it (get_matcher(None) is None); any names a point of no
// country known.
bool postal_area_names(const AdminPolygon& area, uint16_t cc, const char* postcode) {
    if (area.place_type_override == POSTAL_AREA_INVALID || !postcode_taken(postcode)) return false;
    return cc == 0 || area.country_code == cc;
}

// The point halfway along a street's nodes, Nominatim's centroid of a line
// (get_center_point: ST_LineInterpolatePoint(geom, 0.5)), as the server's
// line_middle finds it on the same f32 nodes: along their lengths in plain
// degrees, rounded to 1e-7.
PackedLocation street_middle(const NodeCoord* nodes, uint16_t count) {
    auto length = [&](uint16_t k) {
        return std::hypot(double(nodes[k].lat) - nodes[k - 1].lat, double(nodes[k].lng) - nodes[k - 1].lng);
    };
    double total = 0;
    for (uint16_t k = 1; k < count; k++) total += length(k);
    double left = total / 2, lat = nodes[0].lat, lng = nodes[0].lng;
    for (uint16_t k = 1; k < count; k++) {
        const double step = length(k);
        if (step > 0 && left <= step) {
            const double t = left / step;
            lat = nodes[k - 1].lat + t * (double(nodes[k].lat) - nodes[k - 1].lat);
            lng = nodes[k - 1].lng + t * (double(nodes[k].lng) - nodes[k - 1].lng);
            break;
        }
        left -= step;
        lat = nodes[k].lat;
        lng = nodes[k].lng;
    }
    auto reduce = [](double v) { return std::round(v * 1e7) / 1e7; };
    return point_e7(reduce(lat), reduce(lng));
}

// get_nearest_postcode's distance from a point to a postal area's centroid,
// as the server takes it: squared, in radians, unscaled by latitude.
double centroid_distance(PackedLocation p, const LatLng& centroid) {
    const double dlat = (p.lat() - centroid.first) * (M_PI / 180.0);
    const double dlng = (p.lon() - centroid.second) * (M_PI / 180.0);
    return dlat * dlat + dlng * dlng;
}

// Whether the cycles a and b of m vertices each are one: the same vertices
// from the first of their smallest on, either way round.
bool same_cycle(const PackedLocation* a, const PackedLocation* b, uint32_t m) {
    if (m == 0) return false;
    auto smallest = [m](const PackedLocation* v) {
        uint32_t s = 0;
        for (uint32_t k = 1; k < m; k++)
            if (v[k] < v[s]) s = k;
        return s;
    };
    const uint32_t sa = smallest(a), sb = smallest(b);
    bool forward = true, backward = true;
    for (uint32_t k = 0; k < m && (forward || backward); k++) {
        const PackedLocation& v = a[(sa + k) % m];
        forward = forward && v == b[(sb + k) % m];
        backward = backward && v == b[(sb + m - k) % m];
    }
    return forward || backward;
}

// Whether b lies on the segment from a to c, past a and before c: a vertex
// there adds no point to an outline (exact, in e7 integers).
bool straight_between(const PackedLocation& a, const PackedLocation& b, const PackedLocation& c) {
    const __int128 ux = int64_t(b.lon_e7) - a.lon_e7, uy = int64_t(b.lat_e7) - a.lat_e7;
    const __int128 vx = int64_t(c.lon_e7) - b.lon_e7, vy = int64_t(c.lat_e7) - b.lat_e7;
    return ux * vy == uy * vx && ux * vx + uy * vy > 0;
}

// The outline of the closed ring v (n vertices) as a cycle, into `out`: its
// vertices without the closing one, one of each run of equal ones, and none
// that lies straight between the vertices either side of it.
void ring_outline(const PackedLocation* v, uint32_t n, std::vector<PackedLocation>& out) {
    out.clear();
    const uint32_t m = n > 1 && v[0] == v[n - 1] ? n - 1 : n;
    for (uint32_t k = 0; k < m; k++) {
        if (!out.empty() && out.back() == v[k]) continue;
        while (out.size() >= 2 && straight_between(out[out.size() - 2], out.back(), v[k])) out.pop_back();
        out.push_back(v[k]);
    }
    // Round the end, where the cycle closes.
    for (bool changed = true; changed;) {
        changed = false;
        if (out.size() >= 2 && out.back() == out.front()) {
            out.pop_back();
            changed = true;
        } else if (out.size() >= 3 && straight_between(out[out.size() - 2], out.back(), out.front())) {
            out.pop_back();
            changed = true;
        } else if (out.size() >= 3 && straight_between(out.back(), out.front(), out[1])) {
            out.erase(out.begin());
            changed = true;
        }
    }
}

// Whether the closed rings a (an vertices) and b (bn) are one ring, as
// ST_Equals takes them: the same outline, from any vertex on, either way
// round, whatever vertices either adds along its straight edges.
bool same_ring(const PackedLocation* a, uint32_t an, const PackedLocation* b, uint32_t bn) {
    if (an == 0 || bn == 0) return false;
    if (an == bn && same_cycle(a, b, an > 1 && a[0] == a[an - 1] ? an - 1 : an)) return true;
    thread_local std::vector<PackedLocation> outline_a, outline_b;
    ring_outline(a, an, outline_a);
    ring_outline(b, bn, outline_b);
    return outline_a.size() == outline_b.size() &&
           same_cycle(outline_a.data(), outline_b.data(), static_cast<uint32_t>(outline_a.size()));
}

// Whether polygons p and q of the exact rings are one: their outer rings
// one ring and their holes one each, in any order.
bool same_polygon(const AdminExactRings& rings, uint32_t p, uint32_t q) {
    const uint32_t count = rings.polygons[p].ring_count;
    if (count == 0 || count != rings.polygons[q].ring_count) return false;
    const RingSpan* a = rings.rings_of(p);
    const RingSpan* b = rings.rings_of(q);
    const PackedLocation* v = rings.vertices.data();
    auto same = [&](const RingSpan& x, const RingSpan& y) { return same_ring(v + x.begin, x.count, v + y.begin, y.count); };
    if (!same(a[0], b[0])) return false;
    std::vector<bool> used(count);
    for (uint32_t i = 1; i < count; i++) {
        bool found = false;
        for (uint32_t j = 1; j < count && !found; j++)
            if (!used[j] && same(a[i], b[j])) used[j] = found = true;
        if (!found) return false;
    }
    return true;
}

}  // namespace

S2CellId admin_link_cell(PackedLocation p) {
    return S2CellId(S2LatLng::FromDegrees(p.lat(), p.lon())).parent(kAdminCellLevel);
}

std::pair<const CellItemPair*, const CellItemPair*> admin_cell_entries(const std::vector<CellItemPair>& index,
                                                                       uint64_t cell) {
    const auto first = std::lower_bound(index.begin(), index.end(), cell,
                                        [](const CellItemPair& e, uint64_t c) { return e.cell_id < c; });
    auto last = first;
    while (last != index.end() && last->cell_id == cell) ++last;
    return {index.data() + (first - index.begin()), index.data() + (last - index.begin())};
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

bool AdminRings::covers(uint32_t pid, PackedLocation p) const {
    const auto& polygon = rings_.polygons[pid];
    bool inside = false;
    for (uint32_t r = polygon.first_ring; r < polygon.first_ring + polygon.ring_count; r++) {
        const ExactRingBox& box = boxes_[r];
        // Outside the ring's closed box, which holds its edges, the ring adds nothing.
        if (p.lat_e7 < box.lat_lo || p.lat_e7 > box.lat_hi || p.lon_e7 < box.lng_lo || p.lon_e7 > box.lng_hi) continue;
        const RingSpan& span = rings_.rings[r];
        const PackedLocation* v = rings_.vertices.data() + span.begin;
        if (edges_[r] ? edges_[r]->on_edge(p) : exact_ring_on_edge(v, span.count, p)) return true;
        inside ^= edges_[r] ? edges_[r]->contains(p) : exact_ring_contains(v, span.count, p);
    }
    return inside;
}

uint32_t AdminRings::first_containing(const std::vector<uint32_t>& ranked, PackedLocation p) const {
    for (uint32_t id : ranked)
        if ((id & INTERIOR_FLAG) || contains(id & ID_MASK, p)) return id & ID_MASK;
    return NO_DATA;
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
// its centroid (server walks the admin chain from a road), and the postcode
// of the postal area at its middle.
void compute_way_parent_polygons(ParsedData& data, unsigned int num_threads) {
    {
        auto _wp_t = std::chrono::steady_clock::now();
        auto _wp_cpu = CpuTicks::now();
        data.way_parent_ids.assign(data.ways.size(), NO_DATA);
        data.way_postcode_ids.assign(data.ways.size(), NO_DATA);
        require_ordered_admin_areas(data);
        const AdminRings rings(data.admin_exact_rings, num_threads);
        auto more_specific = closest_parent_first(data);

        // A street's postcode is get_nearest_postcode's at its centroid, the
        // point halfway along it (placex_update, placex_triggers.sql), from
        // the postal areas only: the server takes the postcode centroids
        // there at query time where none holds it. Of the areas covering the
        // middle, their edges included (ST_Covers(p.geometry, geom)), that
        // name it (postal_area_names), the one whose centroid is nearest
        // (ORDER BY ST_Distance(p.centroid, geom)), then the smaller, then
        // the lower postcode, as the server's postal_area_postcode.
        const auto& exact = data.admin_exact_rings;
        std::vector<uint32_t> postal_slots;  // ascending, with postal_centroids
        for (uint32_t pid = 0; pid < data.admin_polygons.size(); pid++)
            if (data.admin_polygons[pid].admin_level == POSTAL_AREA_LEVEL) postal_slots.push_back(pid);
        std::vector<LatLng> postal_centroids(postal_slots.size());
        parallel_for(postal_slots.size(), [&](size_t begin, size_t end, unsigned) {
            for (size_t k = begin; k < end; k++) {
                const uint32_t pid = postal_slots[k];
                postal_centroids[k] = exact_polygon_centroid(exact.vertices.data(), exact.rings_of(pid),
                                                             exact.polygons[pid].ring_count);
            }
        }, num_threads);
        auto centroid_of = [&](uint32_t pid) {
            return postal_centroids[std::lower_bound(postal_slots.begin(), postal_slots.end(), pid) - postal_slots.begin()];
        };

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

        struct Candidates {
            uint64_t cell = 0;
            std::vector<uint32_t> admins, postals;  // the centroid cell's
            std::vector<uint32_t> middle_postals;   // a middle's cell's past the centroid's
            std::vector<std::pair<uint32_t, uint32_t>> scratch;
        };
        auto add_postals = [&](S2CellId cell, std::vector<uint32_t>& postals) {
            postals.clear();
            for_each_admin_candidate(data, cell, [&](uint32_t id, const AdminPolygon& p) {
                if (p.admin_level == POSTAL_AREA_LEVEL) postals.push_back(id);
            });
        };
        walk_items_by_cell<Candidates>(order, num_threads,
            [&](S2CellId cell, Candidates& c) {
                // The administrative levels compete for the parent chain,
                // postal boundaries (POSTAL_AREA_LEVEL) for the postcode.
                c.cell = cell.id();
                c.admins.clear();
                for_each_admin_candidate(data, cell, [&](uint32_t id, const AdminPolygon& p) {
                    if (p.admin_level <= kMaxBoundaryLevel) c.admins.push_back(id);
                });
                add_postals(cell, c.postals);
                rank_candidates(c.admins, more_specific, c.scratch);
            },
            [&](uint32_t i, Candidates& c) {
                const auto& w = data.ways[i];

                // Pick the most specific containing polygon: highest
                // admin_level, then smallest area. Parent-chain pick uses
                // centroid PIP — admin hierarchy is fine-grained enough
                // that centroid containment gives a stable result.
                data.way_parent_ids[i] = rings.first_containing(c.admins, centroid(w));
                if (w.node_count == 0) return;

                const PackedLocation middle = street_middle(data.street_nodes.data() + w.node_offset, w.node_count);
                const uint64_t middle_cell = admin_link_cell(middle).id();
                const std::vector<uint32_t>* postals = &c.postals;
                if (middle_cell != c.cell) {
                    add_postals(S2CellId(middle_cell), c.middle_postals);
                    postals = &c.middle_postals;
                }
                const char* pool = data.string_pool.data().data();
                uint16_t cc = 0;
                bool cc_found = false;
                uint32_t best = NO_DATA;
                double best_distance = 0;
                for (uint32_t id : *postals) {
                    const uint32_t pid = id & ID_MASK;
                    const AdminPolygon& area = data.admin_polygons[pid];
                    if (area.name_id == NO_DATA || !(area.area > 0)) continue;
                    if (!(id & INTERIOR_FLAG) && !rings.covers(pid, middle)) continue;
                    if (!cc_found) {
                        cc = country_code_at_point(data, rings, middle.lat(), middle.lon());
                        cc_found = true;
                    }
                    if (!postal_area_names(area, cc, pool + area.name_id)) continue;
                    const double distance = centroid_distance(middle, centroid_of(pid));
                    if (best != NO_DATA) {
                        const AdminPolygon& held = data.admin_polygons[best];
                        if (distance != best_distance) {
                            if (distance > best_distance) continue;
                        } else if (area.area != held.area) {
                            if (area.area > held.area) continue;
                        } else if (std::strcmp(pool + area.name_id, pool + held.name_id) >= 0) {
                            continue;
                        }
                    }
                    best = pid;
                    best_distance = distance;
                }
                if (best != NO_DATA) data.way_postcode_ids[i] = data.admin_polygons[best].name_id;
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
// - parent_postcode_id: the postal boundary's (POSTAL_AREA_LEVEL) postcode,
//   Nominatim's placex.postcode chain approximation, of one that names a
//   point of the POI's country (postal_area_names).
// - parent_poly_id: the admin polygon of levels 2 to kMaxPoiParentLevel (10;
//   the blocks and neighbourhoods of 11 and 12 and the place-area markers 15
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
                    if (p.admin_level == POSTAL_AREA_LEVEL) {
                        if (p.place_type_override != POSTAL_AREA_INVALID && p.name_id != NO_DATA) c.postal.push_back(id);
                    }
                    else if (p.admin_level >= 2 && p.admin_level <= kMaxPoiParentLevel) c.admin.push_back(id);
                });
                rank_candidates(c.postal, smaller, c.scratch);
                rank_candidates(c.admin, smaller, c.scratch);
            },
            [&](uint32_t i, Candidates& c) {
                auto& pr = data.poi_records[i];
                const PackedLocation p = point_of(i);
                const char* pool = data.string_pool.data().data();
                uint16_t cc = 0;
                bool cc_found = false;
                pr.parent_postcode_id = NO_DATA;
                for (uint32_t id : c.postal) {
                    const AdminPolygon& area = data.admin_polygons[id & ID_MASK];
                    if (!(id & INTERIOR_FLAG) && !rings.contains(id & ID_MASK, p)) continue;
                    if (!cc_found) {
                        cc = country_code_at_point(data, rings, p.lat(), p.lon());
                        cc_found = true;
                    }
                    if (!postal_area_names(area, cc, pool + area.name_id)) continue;
                    pr.parent_postcode_id = area.name_id;
                    break;
                }
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
