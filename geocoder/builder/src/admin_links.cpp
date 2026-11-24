#include "admin_links.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
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
#include "postcode_validation.h"
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
// of no country known names a point of any country but one without
// postcodes, the server having no pattern to test it by; any names a point
// of no country known.
bool postal_area_names(const AdminPolygon& area, uint16_t cc, const char* postcode) {
    if (area.place_type_override == POSTAL_AREA_INVALID || !postcode_taken(postcode)) return false;
    if (cc == 0 || area.country_code == cc) return true;
    if (area.country_code != 0) return false;
    const char lower[3] = {static_cast<char>(std::tolower(cc >> 8)), static_cast<char>(std::tolower(cc & 0xFF)), 0};
    return !country_without_postcodes(lower);
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
    if (contains(pid, p)) return true;
    const auto& polygon = rings_.polygons[pid];
    for (uint32_t r = polygon.first_ring; r < polygon.first_ring + polygon.ring_count; r++) {
        const ExactRingBox& box = boxes_[r];
        if (p.lat_e7 < box.lat_lo || p.lat_e7 > box.lat_hi || p.lon_e7 < box.lng_lo || p.lon_e7 > box.lng_hi) continue;
        const RingSpan& span = rings_.rings[r];
        if (edges_[r] ? edges_[r]->touches(p) : exact_ring_touches(rings_.vertices.data() + span.begin, span.count, p))
            return true;
    }
    return false;
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

uint16_t polygon_country(const ParsedData& data, const AdminRings& countries, uint32_t i) {
    const AdminPolygon& poly = data.admin_polygons[i];
    if (poly.country_code != 0) return poly.country_code;
    const auto& rings = data.admin_exact_rings;
    const uint32_t ring_count = rings.polygons[i].ring_count;
    if (ring_count == 0) return 0;
    const LatLng inside = interior_point(rings.vertices.data(), rings.rings_of(i), ring_count);
    return country_code_at_point(data, countries, inside.first, inside.second);
}

size_t take_out_repeated_boundaries(ParsedData& data, unsigned int num_threads) {
    const size_t n = data.admin_polygons.size();
    if (n == 0 || data.admin_osm_ids.size() != n) return 0;
    auto& qids = data.boundary_wikidata;
    std::sort(qids.begin(), qids.end());
    qids.erase(std::unique(qids.begin(), qids.end()), qids.end());
    auto& linking = data.linking_boundaries;
    std::sort(linking.begin(), linking.end());
    linking.erase(std::unique(linking.begin(), linking.end()), linking.end());
    if (qids.empty()) return 0;
    auto qid_of = [&](uint64_t boundary) {
        const auto it = std::lower_bound(qids.begin(), qids.end(), std::make_pair(boundary, uint64_t(0)));
        return it != qids.end() && it->first == boundary ? it->second : uint64_t(0);
    };
    // The rings of each boundary relation of levels 4 to 10, a boundary's
    // together, and the polygon standing for it.
    std::vector<std::pair<uint64_t, uint32_t>> slots;
    for (uint32_t p = 0; p < n; p++) {
        const uint8_t level = data.admin_polygons[p].admin_level;
        if (level >= 4 && level <= 10 && admin_relation_ring(data.admin_osm_ids[p]))
            slots.push_back({admin_boundary_id(data.admin_osm_ids[p]), p});
    }
    std::sort(slots.begin(), slots.end());
    auto slots_of = [&](uint64_t boundary) {
        return std::equal_range(slots.begin(), slots.end(), std::make_pair(boundary, uint32_t(0)),
                                [](const auto& a, const auto& b) { return a.first < b.first; });
    };
    const auto boundaries = boundary_polygons(data.admin_polygons, data.admin_osm_ids);
    const auto& rings = data.admin_exact_rings;
    auto same_boundary = [&](uint64_t a, uint64_t b) {
        const auto [a_first, a_last] = slots_of(a);
        const auto [b_first, b_last] = slots_of(b);
        if (a_last - a_first != b_last - b_first) return false;
        std::vector<bool> used(static_cast<size_t>(b_last - b_first));
        for (auto x = a_first; x != a_last; ++x) {
            bool found = false;
            for (auto y = b_first; y != b_last && !found; ++y) {
                const size_t k = static_cast<size_t>(y - b_first);
                if (!used[k] && same_polygon(rings, x->second, y->second)) used[k] = found = true;
            }
            if (!found) return false;
        }
        return true;
    };

    const AdminRings countries = border_country_rings(data, num_threads);
    // Each polygon's rank_address in its country, found the first time it
    // is asked for, on any thread: 0 not yet, else kKnown and the rank.
    constexpr uint32_t kKnown = 1u << 8;
    std::vector<std::atomic<uint32_t>> rank_memo(n);
    auto rank_of = [&](uint32_t p) {
        const uint32_t memo = rank_memo[p].load(std::memory_order_relaxed);
        if (memo != 0) return static_cast<uint8_t>(memo);
        const uint8_t rank = admin_rank_address(polygon_country(data, countries, p), data.admin_polygons[p].admin_level);
        rank_memo[p].store(kKnown | rank, std::memory_order_relaxed);
        return rank;
    };
    // The rings a boundary can repeat, of the boundary relations of levels
    // 4 to 9, edge-indexed for the tests at the boundaries' points.
    auto may_repeat = [&](uint32_t p) {
        const uint8_t level = data.admin_polygons[p].admin_level;
        return level >= 4 && level <= 9 && admin_relation_ring(data.admin_osm_ids[p]);
    };
    const AdminRings parents(rings, num_threads, may_repeat);
    const auto closer = closest_parent_first(data);
    // By slot, 1 for the rings of a boundary in no address (rank 0): a
    // level's are marked after it, so the levels below a boundary are
    // settled when it looks for its parent. One that repeats its parent and
    // links a place node of a rank above 3 and below 26 takes that rank and
    // stays a parent.
    std::vector<uint8_t> out(n);
    size_t repeating = 0;
    for (uint8_t level = 5; level <= 10; level++) {
        std::vector<uint64_t> repeated = parallel_collect<uint64_t>(boundaries.size(),
            [&](size_t k, std::vector<uint64_t>& found) {
                const auto [boundary, p] = boundaries[k];
                if (data.admin_polygons[p].admin_level != level || !admin_relation_ring(data.admin_osm_ids[p])) return;
                const uint64_t qid = qid_of(boundary);
                if (qid == 0 || rank_of(p) == 0) return;
                const uint32_t ring_count = rings.polygons[p].ring_count;
                if (ring_count == 0) return;
                // The closest relation round the boundary's point, of a lower
                // level above 3 and a rank of 1 to 25, Nominatim's ORDER BY
                // admin_level desc of those covering its centroid.
                const LatLng at = interior_point(rings.vertices.data(), rings.rings_of(p), ring_count);
                const PackedLocation inside = point_e7(at.first, at.second);
                thread_local std::vector<uint32_t> ids;
                thread_local std::vector<std::pair<uint32_t, uint32_t>> ranked;
                ids.clear();
                for_each_admin_candidate(data, admin_link_cell(inside), [&](uint32_t id, const AdminPolygon& poly) {
                    if (poly.admin_level < level && may_repeat(id & ID_MASK)) ids.push_back(id);
                });
                rank_candidates(ids, closer, ranked);
                uint32_t parent = NO_DATA;
                for (uint32_t id : ids) {
                    const uint32_t q = id & ID_MASK;
                    if (out[q] || rank_of(q) == 0) continue;
                    if ((id & INTERIOR_FLAG) || parents.covers(q, inside)) {
                        parent = q;
                        break;
                    }
                }
                if (parent == NO_DATA) return;
                const uint64_t parent_boundary = admin_boundary_id(data.admin_osm_ids[parent]);
                if (qid_of(parent_boundary) != qid) return;
                if (same_boundary(boundary, parent_boundary)) found.push_back(boundary);
            }, num_threads);
        for (uint64_t boundary : repeated) {
            const auto [first, last] = slots_of(boundary);
            const bool links = std::binary_search(linking.begin(), linking.end(), boundary);
            for (auto it = first; it != last; ++it) {
                AdminPolygon& poly = data.admin_polygons[it->second];
                if (!links) {
                    poly.place_type_override = BOUNDARY_REPEATS_PARENT;
                    out[it->second] = 1;
                    continue;
                }
                // placex_update's parent_address_level stays 3 (:1161).
                poly.own_place_type = BOUNDARY_REPEATS_PARENT;
                const uint8_t linked = place_area_rank_address(polygon_country(data, countries, it->second),
                                                               poly.place_type_override);
                out[it->second] = linked < 4 || linked > 25;
            }
        }
        repeating += repeated.size();
    }
    return repeating;
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
            if (data.admin_polygons[pid].admin_level == 11) postal_slots.push_back(pid);
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
                if (p.admin_level == 11) postals.push_back(id);
            });
        };
        walk_items_by_cell<Candidates>(order, num_threads,
            [&](S2CellId cell, Candidates& c) {
                // Admin levels <= 10 compete for the parent chain, postal
                // boundaries (11) for the postcode.
                c.cell = cell.id();
                c.admins.clear();
                for_each_admin_candidate(data, cell, [&](uint32_t id, const AdminPolygon& p) {
                    if (p.admin_level <= 10) c.admins.push_back(id);
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
// - parent_postcode_id: the postal boundary's (11) postcode, Nominatim's
//   placex.postcode chain approximation, of one that names a point of the
//   POI's country (postal_area_names).
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
                    if (p.admin_level == 11) {
                        if (p.place_type_override != POSTAL_AREA_INVALID && p.name_id != NO_DATA) c.postal.push_back(id);
                    }
                    else if (p.admin_level >= 2 && p.admin_level <= 10) c.admin.push_back(id);
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
