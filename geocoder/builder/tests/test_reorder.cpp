// The deterministic-ordering steps (reorder.h, partition_strings_into_tiers)
// against the serial reference (reorder_reference.h): every output byte must
// match on inputs dense with ties, duplicates and edge values.
#include "reorder.h"

#include <cstring>
#include <future>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <string>
#include <utility>

#include "reorder_reference.h"
#include "test_framework.h"

namespace {

const unsigned kThreadCounts[] = {1, 3, 8, 64};

struct Shape {
    size_t n = 1 << 18;  // past parallel_sort's serial cutoff for every family
    bool osm_ids = true;      // osm id arrays parallel to their records
    bool side_arrays = true;  // postcode / parent / elevation / qid arrays
    bool odd_vertices = false;  // addr polygons with vertex_count > 0 but no vertices
    bool nan = false;           // NaN coordinates
    bool uniform_interp_postcodes = false;  // tied interps carry one postcode
    bool unique_osm_ids = false;            // no record ties another
};

struct Rng {
    std::mt19937_64 g;
    explicit Rng(uint64_t seed) : g(seed) {}
    uint32_t below(uint32_t n) { return static_cast<uint32_t>(g() % std::max(n, 1u)); }
    bool one_in(uint32_t n) { return below(n) == 0; }
};

float coord(Rng& r, const Shape& s) {
    static const float kCoords[] = {0.0f, -0.0f, 1.5f, -2.25f, 3.0f, 47.125f};
    if (s.nan && r.one_in(50)) return std::numeric_limits<float>::quiet_NaN();
    return kCoords[r.below(6)];
}

uint32_t small_id(Rng& r, uint32_t range) {
    return r.one_in(10) ? NO_DATA : r.below(range);
}

NodeCoord node(Rng& r, const Shape& s) { return {coord(r, s), coord(r, s)}; }

// An e7 point from a handful of values, so that rings tie often.
PackedLocation e7_point(Rng& r) {
    static const int32_t kE7[] = {0, 15000000, -22500000, 30000000, 471250000};
    return {kE7[r.below(5)], kE7[r.below(5)]};
}

std::vector<CellItemPair> new_cell_pairs(Rng& r, size_t n, uint32_t items, bool flags) {
    std::vector<CellItemPair> pairs(n);
    for (auto& p : pairs) {
        p.cell_id = r.below(97) * 0x1000000001ull;
        p.item_id = r.below(items) | (flags && r.one_in(3) ? INTERIOR_FLAG : 0);
    }
    std::sort(pairs.begin(), pairs.end(), cell_item_less);
    return pairs;
}

// Records after the string partition: name ids are plain small integers so
// that every sort key collides often.
void fill_records(ParsedData& d, std::vector<float>& elevations, std::vector<uint32_t>& qids,
                  uint64_t seed, const Shape& s) {
    Rng r(seed);
    const size_t n = s.n;
    const uint32_t n_ways = static_cast<uint32_t>(n), n_admin = static_cast<uint32_t>(n / 2);

    for (size_t i = 0; i < n; i++) {
        AddrPoint a{};
        a.lat = coord(r, s);
        a.lng = coord(r, s);
        a.housenumber_id = small_id(r, 6);
        a.street_id = small_id(r, 6);
        a.parent_way_id = r.one_in(20) ? n_ways + r.below(5) : small_id(r, n_ways);
        a.vertex_offset = NO_DATA;
        if (r.one_in(3)) {
            a.vertex_count = 1 + r.below(3);
            if (!(s.odd_vertices && r.one_in(5))) {
                a.vertex_offset = static_cast<uint32_t>(d.addr_vertices.size());
                for (uint32_t j = 0; j < a.vertex_count; j++) d.addr_vertices.push_back(node(r, s));
            }
        }
        d.addr_points.push_back(a);
        if (s.osm_ids) d.addr_osm_ids.push_back(s.unique_osm_ids ? i + 1 : r.one_in(5) ? 0 : r.below(static_cast<uint32_t>(n / 3)));
        if (s.side_arrays) d.addr_postcode_ids.push_back(small_id(r, 5));
    }
    d.sorted_addr_cells = new_cell_pairs(r, n, static_cast<uint32_t>(n), false);

    for (uint32_t i = 0; i < n_ways; i++) {
        WayHeader w{};
        w.node_offset = static_cast<uint32_t>(d.street_nodes.size());
        w.node_count = static_cast<uint16_t>(1 + r.below(2));
        w.name_id = small_id(r, 5);
        for (uint32_t j = 0; j < w.node_count; j++) d.street_nodes.push_back(node(r, s));
        d.ways.push_back(w);
        if (s.osm_ids) d.way_osm_ids.push_back(s.unique_osm_ids ? i + 1 : r.one_in(5) ? 0 : static_cast<int64_t>(r.below(n_ways / 4)) - 7);
        if (s.side_arrays) {
            d.way_parent_ids.push_back(r.one_in(20) ? n_admin + 3 : small_id(r, n_admin));
            d.way_postcode_ids.push_back(small_id(r, 4));
        }
    }
    d.sorted_way_cells = new_cell_pairs(r, n, n_ways, false);

    for (uint32_t i = 0; i < n_ways; i++) {
        InterpWay iw{};
        iw.node_offset = static_cast<uint32_t>(d.interp_nodes.size());
        iw.node_count = static_cast<uint16_t>(1 + r.below(2));
        iw.street_id = small_id(r, 4);
        iw.start_number = r.below(3);
        iw.end_number = r.below(3);
        iw.interpolation = static_cast<uint8_t>(r.below(2));
        for (uint32_t j = 0; j < iw.node_count; j++) d.interp_nodes.push_back(node(r, s));
        d.interp_ways.push_back(iw);
        // Like TIGER's synthetic ids: a hash of the content (type aside),
        // so identical segments share an id.
        uint64_t osm = iw.street_id * 31ull + iw.start_number * 7 + iw.end_number;
        for (uint32_t j = 0; j < iw.node_count; j++) {
            uint64_t bits;
            std::memcpy(&bits, &d.interp_nodes[iw.node_offset + j], sizeof(bits));
            osm = osm * 1000003 ^ bits;
        }
        if (s.unique_osm_ids) osm = i + 1;
        else if (r.one_in(10)) osm = r.below(n_ways / 8);
        if (s.osm_ids) d.interp_osm_ids.push_back(osm);
        if (s.side_arrays)
            d.interp_postcode_ids.push_back(s.uniform_interp_postcodes ? static_cast<uint32_t>(osm % 7)
                                                                     : small_id(r, 4));
    }
    d.sorted_interp_cells = new_cell_pairs(r, n, n_ways, false);

    for (uint32_t i = 0; i < n_admin; i++) {
        AdminPolygon p{};
        p.name_id = small_id(r, 4);
        p.admin_level = static_cast<uint8_t>(2 + r.below(2));
        p.country_code = static_cast<uint16_t>(r.below(2));
        p.area = static_cast<float>(r.below(3));
        std::vector<PackedLocation> outer;
        for (uint32_t j = r.one_in(4) ? 21 + r.below(3) : 3 + r.below(2); j > 0; j--)
            outer.push_back(outer.size() < 3 ? e7_point(r) : PackedLocation{10000000, 10000000});
        outer.push_back(outer.front());
        std::vector<std::vector<PackedLocation>> holes;
        if (r.one_in(8)) holes.push_back({e7_point(r), e7_point(r), e7_point(r), outer.front()});
        d.admin_exact_rings.append(outer, holes);
        d.admin_polygons.push_back(p);
        if (s.osm_ids) d.admin_osm_ids.push_back(r.one_in(50) && !s.unique_osm_ids ? 0 : i + 1);
        if (s.side_arrays) d.admin_parent_ids.push_back(r.one_in(20) ? n_admin + 1 : small_id(r, n_admin));
    }
    d.admin_index_cells = new_cell_pairs(r, 20000, n_admin + 3, true);
    d.admin_minimal_index_cells = new_cell_pairs(r, 10000, n_admin + 3, true);
    d.admin_country_cells = new_cell_pairs(r, 5000, n_admin + 3, true);
    // The arc stage's encoding, which comes before the ordering: each
    // polygon's ring numbers and its ring block in each set (none in the
    // minimal set for some), each tier's arc blocks, and each set's arcs
    // (boxes, keys, slots, sidecar) and every ring's refs to them, which the
    // ordering leaves where they are (it moves polygons, not rings). Its own
    // draws, so the records after it are as they were.
    {
        Rng a(seed ^ 0xa3c5u);
        for (const AdminExactRings::Polygon& exact : d.admin_exact_rings.polygons) {
            AdminArcPolygon p{exact.first_ring, exact.ring_count, {}};
            for (size_t set = 0; set < 2; set++) {
                std::vector<uint8_t>& blocks = d.admin_arcs.sets[set].ring_blocks;
                const uint32_t size = set == 0 || !a.one_in(3) ? 1 + a.below(6) : 0;
                if (size) p.blocks[set] = {static_cast<uint32_t>(blocks.size()), size};
                for (uint32_t k = 0; k < size; k++) blocks.push_back(static_cast<uint8_t>(a.below(256)));
            }
            d.admin_arcs.polygons.push_back(p);
        }
        for (ArcBlocks& tier : d.admin_arcs.tiers) {
            tier.at = {0};
            for (uint32_t arc = 0; arc < 300; arc++) {
                for (uint32_t k = 1 + a.below(9); k > 0; k--) tier.bytes.push_back(static_cast<uint8_t>(a.below(256)));
                tier.at.push_back(static_cast<uint32_t>(tier.bytes.size()));
            }
        }
        for (ArcSetBlocks& set : d.admin_arcs.sets) {
            const uint32_t arcs = 50 + a.below(50);
            for (uint32_t k = 0; k < arcs; k++) {
                const auto e7 = [&] { return static_cast<int32_t>(a.below(2000000000u)) - 1000000000; };
                set.boxes.push_back({e7(), e7(), e7()});
                const uint64_t high = a.below(1u << 31);
                set.keys.push_back(high << 32 | a.below(UINT32_MAX));
                set.planet_slots.push_back(a.below(2 * arcs));
                set.slots.push_back(a.below(2 * arcs));
                set.sidecar.push_back({static_cast<uint8_t>(a.below(4)), static_cast<uint8_t>(a.below(2)),
                                       static_cast<uint8_t>(a.below(3)), 0, set.keys.back()});
            }
            set.slot_counts = {a.below(9), a.below(9), a.below(9), a.below(9), a.below(2)};
            set.ring_refs = {0};
            for (size_t ring = 0; ring < d.admin_exact_rings.rings.size(); ring++) {
                for (uint32_t k = a.below(4); k > 0; k--) set.refs.push_back({a.below(arcs), a.one_in(2)});
                set.ring_refs.push_back(static_cast<uint32_t>(set.refs.size()));
            }
        }
    }

    for (size_t i = 0; i < n; i++) {
        PoiRecord p{};
        p.lat = coord(r, s);
        p.lng = coord(r, s);
        p.vertex_offset = NO_DATA;
        if (r.one_in(4)) {
            p.vertex_count = 3 + r.below(2);
            p.vertex_offset = static_cast<uint32_t>(d.poi_vertices.size());
            for (uint32_t j = 0; j < p.vertex_count; j++) d.poi_vertices.push_back(node(r, s));
        }
        p.name_id = small_id(r, 4);
        p.category = static_cast<uint8_t>(r.below(3) == 0 ? static_cast<uint8_t>(PoiCategory::PEAK) : r.below(4));
        p.tier = static_cast<uint8_t>(1 + r.below(4));
        p.flags = static_cast<uint8_t>(r.below(4));
        p.parent_street_id = small_id(r, 4);
        p.parent_postcode_id = small_id(r, 4);
        p.parent_poly_id = r.one_in(20) ? n_admin + 4 : small_id(r, n_admin);
        if (s.osm_ids) d.poi_osm_ids.push_back(s.unique_osm_ids ? i + 1 : r.one_in(5) ? 0 : r.below(static_cast<uint32_t>(n / 2)));
        if (s.side_arrays) {
            elevations.push_back(static_cast<float>(r.below(4000)) - 100.0f);
            qids.push_back(r.below(50));
        }
        if (i > 0 && r.one_in(10)) {
            // An exact copy (vertices at their own offset): ties that can't
            // show in the output.
            size_t j = r.below(static_cast<uint32_t>(i));
            p = d.poi_records[j];
            if (p.vertex_offset != NO_DATA) {
                p.vertex_offset = static_cast<uint32_t>(d.poi_vertices.size());
                for (uint32_t v = 0; v < p.vertex_count; v++)
                    d.poi_vertices.push_back(d.poi_vertices[d.poi_records[j].vertex_offset + v]);
            }
            if (s.osm_ids) d.poi_osm_ids.back() = d.poi_osm_ids[j];
            if (s.side_arrays) {
                elevations.back() = elevations[j];
                qids.back() = qids[j];
            }
        }
        d.poi_records.push_back(p);
    }
    d.sorted_poi_cells = new_cell_pairs(r, n, static_cast<uint32_t>(n), true);

    for (size_t i = 0; i < n; i++) {
        PlaceNode p{};
        p.lat = coord(r, s);
        p.lng = coord(r, s);
        p.name_id = small_id(r, 8);
        p.place_type = static_cast<uint8_t>(r.below(3));
        p.parent_poly_id = r.one_in(20) ? n_admin + 5 : small_id(r, n_admin);
        d.place_nodes.push_back(p);
        if (s.osm_ids) d.place_osm_ids.push_back(i * 3 + 1);
    }
    d.sorted_place_cells = new_cell_pairs(r, n, static_cast<uint32_t>(n), false);
}

std::vector<QidSitelinks> sitelinks() {
    std::vector<QidSitelinks> v;
    for (uint32_t q = 1; q < 50; q += 3) v.push_back({q, static_cast<uint16_t>(q * 2 % 7)});
    return v;
}

template <class T>
bool same_bytes(const std::vector<T>& a, const std::vector<T>& b) {
    return a.size() == b.size() && (a.empty() || std::memcmp(a.data(), b.data(), a.size() * sizeof(T)) == 0);
}

bool same_pairs(const std::vector<CellItemPair>& a, const std::vector<CellItemPair>& b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); i++)
        if (a[i].cell_id != b[i].cell_id || a[i].item_id != b[i].item_id) return false;
    return true;
}

// Two copies of one record: cell 7 lists both, cells 5 and 9 one each.
const std::vector<CellItemPair> kTwoCopiesCells = {{5, 0}, {7, 0}, {7, 1}, {9, 1}};
// The copies folded into record 0, listed once in each of their cells.
const std::vector<CellItemPair> kFoldedCells = {{5, 0}, {7, 0}, {9, 0}};

void check_same_records(const ParsedData& a, const ParsedData& b) {
    CHECK(same_bytes(a.addr_points, b.addr_points));
    CHECK(same_bytes(a.addr_osm_ids, b.addr_osm_ids));
    CHECK(same_bytes(a.addr_postcode_ids, b.addr_postcode_ids));
    CHECK(same_bytes(a.addr_vertices, b.addr_vertices));
    CHECK(same_pairs(a.sorted_addr_cells, b.sorted_addr_cells));
    CHECK(same_bytes(a.ways, b.ways));
    CHECK(same_bytes(a.way_osm_ids, b.way_osm_ids));
    CHECK(same_bytes(a.street_nodes, b.street_nodes));
    CHECK(same_bytes(a.way_parent_ids, b.way_parent_ids));
    CHECK(same_bytes(a.way_postcode_ids, b.way_postcode_ids));
    CHECK(same_pairs(a.sorted_way_cells, b.sorted_way_cells));
    CHECK(same_bytes(a.interp_ways, b.interp_ways));
    CHECK(same_bytes(a.interp_osm_ids, b.interp_osm_ids));
    CHECK(same_bytes(a.interp_postcode_ids, b.interp_postcode_ids));
    CHECK(same_bytes(a.interp_nodes, b.interp_nodes));
    CHECK(same_pairs(a.sorted_interp_cells, b.sorted_interp_cells));
    CHECK(same_bytes(a.admin_polygons, b.admin_polygons));
    CHECK(same_bytes(a.admin_osm_ids, b.admin_osm_ids));
    CHECK(same_bytes(a.admin_exact_rings.polygons, b.admin_exact_rings.polygons));
    CHECK(same_bytes(a.admin_exact_rings.rings, b.admin_exact_rings.rings));
    CHECK(same_bytes(a.admin_exact_rings.vertices, b.admin_exact_rings.vertices));
    CHECK(same_bytes(a.admin_arcs.polygons, b.admin_arcs.polygons));
    for (size_t set = 0; set < std::size(a.admin_arcs.sets); set++) {
        const ArcSetBlocks &x = a.admin_arcs.sets[set], &y = b.admin_arcs.sets[set];
        CHECK(same_bytes(x.boxes, y.boxes));
        CHECK(same_bytes(x.keys, y.keys));
        CHECK(same_bytes(x.planet_slots, y.planet_slots));
        CHECK(same_bytes(x.slots, y.slots));
        CHECK(same_bytes(x.sidecar, y.sidecar));
        CHECK(std::memcmp(&x.slot_counts, &y.slot_counts, sizeof(AdminArcSlotCounts)) == 0);
        CHECK(same_bytes(x.ring_refs, y.ring_refs));
        CHECK(x.refs == y.refs);  // ArcRef has padding: by value
        CHECK(same_bytes(x.ring_blocks, y.ring_blocks));
    }
    for (size_t t = 0; t < kArcTierCount; t++) {
        CHECK(same_bytes(a.admin_arcs.tiers[t].bytes, b.admin_arcs.tiers[t].bytes));
        CHECK(same_bytes(a.admin_arcs.tiers[t].at, b.admin_arcs.tiers[t].at));
    }
    CHECK(same_bytes(a.admin_parent_ids, b.admin_parent_ids));
    CHECK(same_pairs(a.admin_index_cells, b.admin_index_cells));
    CHECK(same_pairs(a.admin_minimal_index_cells, b.admin_minimal_index_cells));
    CHECK(same_pairs(a.admin_country_cells, b.admin_country_cells));
    CHECK(same_bytes(a.poi_records, b.poi_records));
    CHECK(same_bytes(a.poi_osm_ids, b.poi_osm_ids));
    CHECK(same_bytes(a.poi_vertices, b.poi_vertices));
    CHECK(same_pairs(a.sorted_poi_cells, b.sorted_poi_cells));
    CHECK(same_bytes(a.place_nodes, b.place_nodes));
    CHECK(same_bytes(a.place_osm_ids, b.place_osm_ids));
    CHECK(same_pairs(a.sorted_place_cells, b.sorted_place_cells));
}

void check_steps_match_reference(uint64_t seed, const Shape& s) {
    const auto links = sitelinks();
    ParsedData want;
    std::vector<float> want_ele;
    std::vector<uint32_t> want_qids;
    fill_records(want, want_ele, want_qids, seed, s);
    reorder_ref::reorder_addr_points(want);
    reorder_ref::reorder_ways(want);
    reorder_ref::reorder_interps(want);
    reorder_ref::reorder_admin_polygons(want);
    reorder_ref::reorder_pois(want, want_ele, want_qids, links);
    reorder_ref::reorder_place_nodes(want);

    // No headroom: each step makes its output arrays after its sort instead
    // of beside it.
    for (uint64_t headroom : {UINT64_MAX, uint64_t(0)})
        for (unsigned threads : kThreadCounts) {
            ParsedData got;
            std::vector<float> got_ele;
            std::vector<uint32_t> got_qids;
            fill_records(got, got_ele, got_qids, seed, s);
            reorder_addr_points(got, threads, headroom);
            reorder_ways(got, threads, headroom);
            reorder_interps(got, threads);
            reorder_admin_polygons(got, threads);
            reorder_pois(got, got_ele, got_qids, links, threads, headroom);
            reorder_place_nodes(got, threads);

            check_same_records(want, got);
            CHECK(same_bytes(want_ele, got_ele));
            CHECK(same_bytes(want_qids, got_qids));
        }
}

// The order reorder_deterministically runs the steps in when they fit side
// by side: addrs, ways, POIs and places at once, then the addr points'
// parent ways and the admins.
void check_side_by_side_steps_match_reference(uint64_t seed, const Shape& s) {
    const auto links = sitelinks();
    ParsedData want;
    std::vector<float> want_ele;
    std::vector<uint32_t> want_qids;
    fill_records(want, want_ele, want_qids, seed, s);
    reorder_ref::reorder_addr_points(want);
    reorder_ref::reorder_ways(want);
    reorder_ref::reorder_interps(want);
    reorder_ref::reorder_admin_polygons(want);
    reorder_ref::reorder_pois(want, want_ele, want_qids, links);
    reorder_ref::reorder_place_nodes(want);

    ParsedData got;
    std::vector<float> got_ele;
    std::vector<uint32_t> got_qids;
    fill_records(got, got_ele, got_qids, seed, s);
    std::vector<uint32_t> way_old_to_new;
    auto ways = std::async(std::launch::async, [&] { way_old_to_new = sort_ways(got, 4); });
    auto pois = std::async(std::launch::async, [&] { reorder_pois(got, got_ele, got_qids, links, 4); });
    auto places = std::async(std::launch::async, [&] { reorder_place_nodes(got, 4); });
    auto interps = std::async(std::launch::async, [&] { reorder_interps(got, 4); });
    reorder_addr_points(got, 4);
    ways.get();
    pois.get();
    places.get();
    interps.get();
    remap_parent_ways(got, way_old_to_new, 4);
    reorder_admin_polygons(got, 4);

    check_same_records(want, got);
    CHECK(same_bytes(want_ele, got_ele));
    CHECK(same_bytes(want_qids, got_qids));
}

// --- String pool partition ---

std::string random_string(Rng& r) {
    // Some share a head longer than the 8 bytes the sort keys hold.
    std::string s = r.one_in(3) ? "Avenue d" : "";
    for (uint32_t len = r.below(9); len > 0; len--) s += "abcdQ\xc3\xa9 "[r.below(8)];
    return s;
}

// A string pool with every kind of reference: valid starts, NO_DATA, offsets
// inside a string and past the pool.
void fill_strings(ParsedData& d, uint64_t seed, size_t n_strings, size_t n_refs) {
    Rng r(seed);
    std::vector<uint32_t> offs;
    for (size_t i = 0; i < n_strings; i++) offs.push_back(d.string_pool.intern(random_string(r)));
    const uint32_t pool_size = static_cast<uint32_t>(d.string_pool.data().size());
    auto ref = [&]() -> uint32_t {
        uint32_t k = r.below(100);
        if (k == 0) return NO_DATA;
        if (k == 1) return pool_size + r.below(3);
        if (k == 2) return offs[r.below(static_cast<uint32_t>(offs.size()))] + 1;
        return offs[r.below(static_cast<uint32_t>(offs.size()))];
    };
    for (size_t i = 0; i < n_refs; i++) {
        AddrPoint a{};
        a.housenumber_id = ref();
        a.street_id = ref();
        d.addr_points.push_back(a);
        d.addr_postcode_ids.push_back(ref());
        WayHeader w{};
        w.name_id = ref();
        d.ways.push_back(w);
        d.way_orig_name_ids.push_back(ref());
        d.way_postcode_ids.push_back(ref());
        PoiRecord p{};
        p.name_id = ref();
        p.parent_street_id = ref();
        p.parent_postcode_id = ref();
        p.tier = static_cast<uint8_t>(1 + r.below(POI_MAX_SHIPPED_TIER));
        d.poi_records.push_back(p);
        if (i % 4 == 0) {
            InterpWay iw{};
            iw.street_id = ref();
            d.interp_ways.push_back(iw);
            d.interp_postcode_ids.push_back(ref());
            AdminPolygon ap{};
            ap.name_id = ref();
            d.admin_polygons.push_back(ap);
            PlaceNode pn{};
            pn.name_id = ref();
            d.place_nodes.push_back(pn);
            d.postcode_accum[postcode_key(static_cast<uint16_t>(r.below(3)), ref())].add(1.0, 2.0);
        }
    }
}

void check_same_strings(const ParsedData& a, const ParsedData& b) {
    CHECK(a.strings_tiers == b.strings_tiers);
    CHECK(a.strings_tier_bases == b.strings_tier_bases);
    CHECK(a.string_pool.data() == b.string_pool.data());
    CHECK(same_bytes(a.addr_points, b.addr_points));
    CHECK(same_bytes(a.addr_postcode_ids, b.addr_postcode_ids));
    CHECK(same_bytes(a.ways, b.ways));
    CHECK(same_bytes(a.way_orig_name_ids, b.way_orig_name_ids));
    CHECK(same_bytes(a.way_postcode_ids, b.way_postcode_ids));
    CHECK(same_bytes(a.poi_records, b.poi_records));
    CHECK(same_bytes(a.interp_ways, b.interp_ways));
    CHECK(same_bytes(a.interp_postcode_ids, b.interp_postcode_ids));
    CHECK(same_bytes(a.admin_polygons, b.admin_polygons));
    CHECK(same_bytes(a.place_nodes, b.place_nodes));
    // Same insertion sequence, so the same iteration order too.
    std::vector<std::pair<uint64_t, int64_t>> pa, pb;
    for (const auto& [k, acc] : a.postcode_accum) pa.push_back({k, acc.sum_lat_e7 + int64_t(acc.count)});
    for (const auto& [k, acc] : b.postcode_accum) pb.push_back({k, acc.sum_lat_e7 + int64_t(acc.count)});
    CHECK(pa == pb);
}

template <class Mutate>
void check_partition_matches_reference(uint64_t seed, size_t n_strings, size_t n_refs, Mutate mutate) {
    ParsedData want;
    fill_strings(want, seed, n_strings, n_refs);
    mutate(want);
    reorder_ref::partition_strings_into_tiers(want);
    for (unsigned threads : kThreadCounts) {
        ParsedData got;
        fill_strings(got, seed, n_strings, n_refs);
        mutate(got);
        partition_strings_into_tiers(got, threads);
        check_same_strings(want, got);
    }
}

}  // namespace

TEST(beside_sort_needs_room_for_the_arrays_and_the_sort) {
    struct Key { uint32_t a, b; };
    // 10 keys: 2 * 8 B of keys and merge copy plus a 4 B order entry each.
    CHECK(beside_sort<Key>(10, 100, 100 + 200));
    CHECK(!beside_sort<Key>(10, 100, 100 + 199));
    CHECK(!beside_sort<Key>(10, 100, 50));
    CHECK(beside_sort<Key>(10, 0, UINT64_MAX));
}

TEST(reorder_steps_match_the_serial_reference) {
    check_steps_match_reference(1, Shape{});
}

TEST(reorder_steps_match_without_parallel_arrays) {
    Shape s;
    s.osm_ids = false;
    s.side_arrays = false;
    check_steps_match_reference(2, s);
}

TEST(reorder_steps_match_with_odd_vertices_and_nan) {
    Shape s;
    s.odd_vertices = true;
    s.nan = true;
    check_steps_match_reference(3, s);
}

TEST(reorder_steps_match_when_tied_interps_share_a_postcode) {
    Shape s;
    s.uniform_interp_postcodes = true;
    check_steps_match_reference(4, s);
}

TEST(reorder_steps_match_when_no_records_tie) {
    Shape s;
    s.unique_osm_ids = true;
    check_steps_match_reference(6, s);
}

TEST(reorder_steps_side_by_side_match_the_serial_reference) {
    check_side_by_side_steps_match_reference(7, Shape{});
    Shape s;
    s.odd_vertices = true;
    s.nan = true;
    check_side_by_side_steps_match_reference(8, s);
    Shape bare;
    bare.osm_ids = false;
    bare.side_arrays = false;
    check_side_by_side_steps_match_reference(9, bare);
}

TEST(reorder_steps_match_on_tiny_inputs) {
    for (size_t n : {size_t(8), size_t(100), size_t(5000)}) {
        Shape s;
        s.n = n;
        check_steps_match_reference(5 + n, s);
    }
}

TEST(reorder_addr_points_lists_folded_copies_once_per_cell) {
    for (unsigned threads : kThreadCounts) {
        ParsedData d;
        AddrPoint a{};
        a.lat = 1.5f;
        a.lng = -2.25f;
        a.housenumber_id = 3;
        a.street_id = 4;
        a.parent_way_id = NO_DATA;
        a.vertex_offset = NO_DATA;
        d.addr_points = {a, a};
        d.sorted_addr_cells = kTwoCopiesCells;
        reorder_addr_points(d, threads);
        CHECK_EQ(d.addr_points.size(), size_t(1));
        CHECK(same_pairs(d.sorted_addr_cells, kFoldedCells));
    }
}

TEST(reorder_ways_lists_folded_copies_once_per_cell) {
    for (unsigned threads : kThreadCounts) {
        ParsedData d;
        d.street_nodes = {{1.5f, -2.25f}, {3.0f, 0.0f}, {1.5f, -2.25f}, {3.0f, 0.0f}};
        WayHeader w{};
        w.node_count = 2;
        w.name_id = 4;
        d.ways = {w, w};
        d.ways[1].node_offset = 2;
        d.sorted_way_cells = kTwoCopiesCells;
        reorder_ways(d, threads);
        CHECK_EQ(d.ways.size(), size_t(1));
        CHECK(same_pairs(d.sorted_way_cells, kFoldedCells));
    }
}

TEST(least_cells_gives_each_item_its_smallest_cell_and_none_to_an_item_no_cell_lists) {
    // Not in cell order: the smallest wins wherever it stands.
    const std::vector<CellItemPair> pairs = {{9, 2}, {5, 0}, {7, 2}, {8, 0}, {7, 3}, {6, 2}};
    const std::vector<uint64_t> want = {5, UINT64_MAX, 6, 7, UINT64_MAX};
    for (unsigned threads : kThreadCounts) CHECK(least_cells(pairs, 5, threads) == want);
    CHECK(least_cells({}, 2, 1) == std::vector<uint64_t>(2, UINT64_MAX));

    Rng r(31);
    const size_t n = 200000;
    const auto many = new_cell_pairs(r, 3 * n, static_cast<uint32_t>(n), false);
    std::vector<uint64_t> serial(n, UINT64_MAX);
    for (const auto& p : many) serial[p.item_id] = std::min(serial[p.item_id], p.cell_id);
    for (unsigned threads : kThreadCounts) CHECK(least_cells(many, n, threads) == serial);
}

TEST(regroup_by_cell_moves_whole_runs_by_their_least_cell_and_keeps_ties_in_place) {
    // Runs {4}, {2, 0}, {3}, {1}, {5}: records 0 and 3 are listed in cell 6,
    // record 2 in cell 8, record 4 in cell 9, records 1 and 5 nowhere.
    const std::vector<CellItemPair> pairs = {{6, 0}, {6, 3}, {8, 2}, {9, 4}};
    for (unsigned threads : kThreadCounts) {
        std::vector<uint32_t> order = {4, 2, 0, 3, 1, 5};
        std::vector<uint32_t> starts = {0, 1, 3, 4, 5};
        regroup_by_cell(order, starts, pairs, threads);
        // Run {2, 0} takes record 0's cell and stays ahead of {3}, which
        // ties it; {4} follows; {1} and {5} stay last, in their order.
        CHECK(order == std::vector<uint32_t>({2, 0, 3, 4, 1, 5}));
        CHECK(starts == std::vector<uint32_t>({0, 2, 3, 4, 5}));
    }
    // Nothing listed: nothing moves.
    std::vector<uint32_t> order = {1, 0, 2}, starts = {0, 2};
    regroup_by_cell(order, starts, {}, 2);
    CHECK(order == std::vector<uint32_t>({1, 0, 2}));
    CHECK(starts == std::vector<uint32_t>({0, 2}));
}

TEST(reorder_ways_puts_them_in_cell_order_and_ways_of_one_cell_in_name_order) {
    for (unsigned threads : kThreadCounts) {
        ParsedData d;
        // Way i has i + 1 nodes at latitude 10 + i and name 3 - i, so the
        // names order them 3, 2, 1, 0.
        for (uint32_t i = 0; i < 4; i++) {
            WayHeader w{};
            w.node_offset = static_cast<uint32_t>(d.street_nodes.size());
            w.node_count = static_cast<uint16_t>(i + 1);
            w.name_id = 3 - i;
            for (uint32_t j = 0; j <= i; j++) d.street_nodes.push_back({10.0f + i, 1.0f * j});
            d.ways.push_back(w);
        }
        d.way_osm_ids = {100, 101, 102, 103};
        d.way_parent_ids = {200, 201, 202, 203};
        d.way_postcode_ids = {300, 301, 302, 303};
        // Way 0 lies in cells 10 and 40, ways 1 and 2 in cell 20, way 3 in
        // none.
        d.sorted_way_cells = {{10, 0}, {20, 1}, {20, 2}, {40, 0}};
        d.addr_points.resize(5);
        for (uint32_t i = 0; i < 5; i++) d.addr_points[i].parent_way_id = i < 4 ? i : NO_DATA;
        reorder_ways(d, threads);

        // Way 0 first (cell 10), then cell 20's by name (way 2, way 1), then
        // the way no cell lists.
        REQUIRE(d.ways.size() == size_t(4));
        const uint32_t want[] = {0, 2, 1, 3};
        uint32_t at = 0;
        for (uint32_t k = 0; k < 4; k++) {
            const uint32_t i = want[k];
            CHECK_EQ(d.ways[k].name_id, 3 - i);
            CHECK_EQ(d.ways[k].node_count, uint16_t(i + 1));
            CHECK_EQ(d.ways[k].node_offset, at);
            for (uint32_t j = 0; j <= i; j++) {
                CHECK_EQ(d.street_nodes[at + j].lat, 10.0f + i);
                CHECK_EQ(d.street_nodes[at + j].lng, 1.0f * j);
            }
            at += i + 1;
            CHECK_EQ(d.way_osm_ids[k], int64_t(100 + i));
            CHECK_EQ(d.way_parent_ids[k], 200 + i);
            CHECK_EQ(d.way_postcode_ids[k], 300 + i);
        }
        CHECK_EQ(d.street_nodes.size(), size_t(10));
        CHECK(same_pairs(d.sorted_way_cells, {{10, 0}, {20, 1}, {20, 2}, {40, 0}}));
        CHECK_EQ(d.addr_points[0].parent_way_id, 0u);
        CHECK_EQ(d.addr_points[1].parent_way_id, 2u);
        CHECK_EQ(d.addr_points[2].parent_way_id, 1u);
        CHECK_EQ(d.addr_points[3].parent_way_id, 3u);
        CHECK_EQ(d.addr_points[4].parent_way_id, NO_DATA);
    }
}

TEST(reorder_addr_points_puts_folded_copies_under_the_least_cell_of_either) {
    for (unsigned threads : kThreadCounts) {
        ParsedData d;
        AddrPoint a{};
        a.lat = 1.5f;
        a.lng = -2.25f;
        a.housenumber_id = 3;
        a.street_id = 4;
        a.parent_way_id = NO_DATA;
        a.vertex_count = 2;
        AddrPoint b = a;
        b.street_id = 2;  // ahead of a by street
        b.vertex_count = 1;
        // b, then two copies of a, each with its own copy of the footprint.
        d.addr_points = {b, a, a};
        d.addr_points[0].vertex_offset = 0;
        d.addr_points[1].vertex_offset = 1;
        d.addr_points[2].vertex_offset = 3;
        d.addr_vertices = {{7.0f, 7.5f}, {8.0f, 8.5f}, {9.0f, 9.5f}, {8.0f, 8.5f}, {9.0f, 9.5f}};
        d.addr_osm_ids = {50, 61, 60};
        d.addr_postcode_ids = {70, NO_DATA, 81};
        // The copies' exact points lie in two cells, one each side of b's.
        d.sorted_addr_cells = {{5, 2}, {7, 0}, {9, 1}};
        reorder_addr_points(d, threads);

        // The copies fold into one record, which cell 5 puts ahead of b; it
        // keeps the first copy's osm id (60) and the first postcode set.
        REQUIRE(d.addr_points.size() == size_t(2));
        CHECK_EQ(d.addr_points[0].street_id, 4u);
        CHECK_EQ(d.addr_points[0].vertex_offset, 0u);
        CHECK_EQ(d.addr_points[1].street_id, 2u);
        CHECK_EQ(d.addr_points[1].vertex_offset, 4u);
        CHECK(same_bytes(d.addr_vertices, std::vector<NodeCoord>({{8.0f, 8.5f}, {9.0f, 9.5f}, {8.0f, 8.5f},
                                                                    {9.0f, 9.5f}, {7.0f, 7.5f}})));
        CHECK(d.addr_osm_ids == std::vector<uint64_t>({60, 50}));
        CHECK(d.addr_postcode_ids == std::vector<uint32_t>({81, 70}));
        CHECK(same_pairs(d.sorted_addr_cells, {{5, 0}, {7, 1}, {9, 0}}));
    }
}

TEST(reorder_interps_puts_them_in_cell_order_and_interps_of_one_cell_in_street_order) {
    for (unsigned threads : kThreadCounts) {
        ParsedData d;
        // Interp i has street 3 - i and one node at latitude 20 + i.
        for (uint32_t i = 0; i < 4; i++) {
            InterpWay iw{};
            iw.node_offset = i;
            iw.node_count = 1;
            iw.street_id = 3 - i;
            iw.start_number = 1;
            iw.end_number = 9;
            d.interp_nodes.push_back({20.0f + i, 0.5f});
            d.interp_ways.push_back(iw);
        }
        d.interp_osm_ids = {100, 101, 102, 103};
        d.interp_postcode_ids = {300, 301, 302, 303};
        // Interps 1 and 3 share cell 6, interp 0 lies in cell 8, interp 2 in
        // none.
        d.sorted_interp_cells = {{6, 1}, {6, 3}, {8, 0}};
        reorder_interps(d, threads);

        REQUIRE(d.interp_ways.size() == size_t(4));
        const uint32_t want[] = {3, 1, 0, 2};
        for (uint32_t k = 0; k < 4; k++) {
            const uint32_t i = want[k];
            CHECK_EQ(d.interp_ways[k].street_id, 3 - i);
            CHECK_EQ(d.interp_ways[k].node_offset, k);
            CHECK_EQ(d.interp_nodes[k].lat, 20.0f + i);
            CHECK_EQ(d.interp_osm_ids[k], uint64_t(100 + i));
            CHECK_EQ(d.interp_postcode_ids[k], 300 + i);
        }
        CHECK(same_pairs(d.sorted_interp_cells, {{6, 0}, {6, 1}, {8, 2}}));
    }
}

TEST(partition_strings_matches_the_serial_reference) {
    check_partition_matches_reference(11, 400000, 300000, [](ParsedData&) {});
    check_partition_matches_reference(12, 300, 2000, [](ParsedData&) {});
}

TEST(partition_strings_matches_with_duplicate_strings_in_the_pool) {
    // Interning keeps the pool unique, but nothing else enforces it: a
    // duplicate makes the sort's tie order part of the layout.
    check_partition_matches_reference(13, 400000, 300000, [](ParsedData& d) {
        auto& pool = d.string_pool.mutable_data();
        const char* dup = "abc";
        pool.insert(pool.end(), dup, dup + 4);
        d.string_pool.intern(std::string("ab\0abc", 6));
        d.place_nodes[0].name_id = static_cast<uint32_t>(pool.size() - 4);
    });
}

TEST(partition_strings_handles_an_empty_pool) {
    ParsedData d;
    partition_strings_into_tiers(d);
    CHECK_EQ(d.strings_tier_bases[STR_TIER_COUNT], 0u);
}

// --- Cell maps ---

TEST(permute_admin_polygons_moves_osm_ids_and_cell_entries_with_each_polygon) {
    for (unsigned threads : kThreadCounts) {
        ParsedData d;
        for (uint32_t i = 0; i < 4; i++) {
            AdminPolygon p{};
            p.name_id = i;
            d.admin_polygons.push_back(p);
            d.admin_osm_ids.push_back(100 + i);
        }
        d.admin_index_cells = {{5, 0}, {5, 3 | INTERIOR_FLAG}, {7, 1}, {7, 2}, {9, 9}};
        d.admin_minimal_index_cells = {{5, 2 | INTERIOR_FLAG}, {6, 0}, {6, 3}};
        d.admin_country_cells = {{8, 1}, {8, 2}};
        auto old_to_new = permute_admin_polygons(d, {2, 0, 3, 1}, threads);
        CHECK(old_to_new == std::vector<uint32_t>({1, 3, 0, 2}));
        CHECK(d.admin_osm_ids == std::vector<uint64_t>({102, 100, 103, 101}));
        for (uint32_t i = 0; i < 4; i++) CHECK_EQ(d.admin_polygons[old_to_new[i]].name_id, i);
        // Each cell's entries in canonical order again; an id past the
        // polygons stays.
        CHECK(same_pairs(d.admin_index_cells, {{5, 1}, {5, 2 | INTERIOR_FLAG}, {7, 0}, {7, 3}, {9, 9}}));
        CHECK(same_pairs(d.admin_minimal_index_cells, {{5, 0 | INTERIOR_FLAG}, {6, 1}, {6, 2}}));
        CHECK(same_pairs(d.admin_country_cells, {{8, 0}, {8, 3}}));
    }
}

TEST(permute_admin_polygons_moves_each_polygons_exact_rings_with_it) {
    for (unsigned threads : kThreadCounts) {
        ParsedData d;
        for (int32_t i = 0; i < 4; i++) {
            AdminPolygon p{};
            p.name_id = static_cast<uint32_t>(i);
            d.admin_polygons.push_back(p);
            std::vector<std::vector<PackedLocation>> holes(static_cast<size_t>(i % 2), {{i, 1}, {i, 2}, {i, 1}});
            d.admin_exact_rings.append({{i, 0}, {i, 5}, {i + 5, 5}, {i, 0}}, holes);
        }
        const auto vertices = d.admin_exact_rings.vertices;
        const auto rings = d.admin_exact_rings.rings;
        permute_admin_polygons(d, {2, 0, 3, 1}, threads);
        // Only the polygons' entries move.
        CHECK(d.admin_exact_rings.vertices == vertices);
        CHECK(d.admin_exact_rings.rings.size() == rings.size());
        for (uint32_t k = 0; k < 4; k++) {
            const auto& polygon = d.admin_exact_rings.polygons[k];
            const int32_t was = static_cast<int32_t>(d.admin_polygons[k].name_id);
            CHECK_EQ(polygon.ring_count, uint32_t(1 + was % 2));
            const RingSpan& outer = d.admin_exact_rings.rings_of(k)[0];
            CHECK_EQ(d.admin_exact_rings.vertices[outer.begin].lat_e7, was);
        }
    }
}

TEST(the_admin_order_follows_the_exact_rings_whatever_their_rotation_or_direction) {
    // Four polygons of one name, level and country: a five-vertex ring, then
    // two six-vertex ones whose smallest vertex is (0, 0) by osm id, then a
    // six-vertex one whose smallest is higher.
    const std::vector<std::vector<PackedLocation>> rings = {
        {{0, 1000}, {50, 60}, {0, 3000}, {40, 2000}, {30, 1000}, {20, 500}},
        {{1000, 1000}, {1000, 5000}, {5000, 5000}, {5000, 1000}, {3000, 400}},
        {{0, 0}, {0, 5000}, {5000, 5000}, {6000, 2000}, {5000, 0}, {2000, -100}},
        {{0, 0}, {0, 5000}, {5000, 5000}, {6000, 2000}, {5000, 0}, {2000, -100}},
    };
    const uint64_t osm_ids[] = {4, 3, 2, 1};
    for (int variant = 0; variant < 12; variant++) {
        ParsedData d;
        for (size_t k = 0; k < rings.size(); k++) {
            std::vector<PackedLocation> ring = rings[k];
            std::rotate(ring.begin(), ring.begin() + (variant + int(k)) % int(ring.size()), ring.end());
            if (variant % 2) std::reverse(ring.begin(), ring.end());
            ring.push_back(ring.front());
            AdminPolygon p{};
            p.name_id = 7;
            p.admin_level = 8;
            d.admin_polygons.push_back(p);
            d.admin_osm_ids.push_back(osm_ids[k]);
            d.admin_exact_rings.append(ring, {});
        }
        reorder_admin_polygons(d, variant % 3 + 1);
        CHECK(d.admin_osm_ids == std::vector<uint64_t>({3, 1, 2, 4}));
    }
}

TEST(repack_and_first_set_per_run_fill_the_buffers_they_are_given) {
    const std::vector<uint32_t> src = {10, 11, 12, 13, 14, 15};
    auto count = [](size_t i) { return size_t(i % 3); };
    auto from = [](size_t i) { return size_t(5 - i); };
    auto fresh = repack(3, src, count, from, 2);
    CHECK(fresh.items == std::vector<uint32_t>({14, 13, 14}));
    CHECK(fresh.at == std::vector<size_t>({0, 0, 1, 3}));
    auto given = repack(3, src, count, from, 2, std::vector<size_t>(4, 7), std::vector<uint32_t>(10, 9));
    CHECK(given.items == fresh.items);
    CHECK(given.at == fresh.at);

    const std::vector<uint32_t> values = {0, 5, 0, 6, 7};
    const std::vector<uint32_t> order = {4, 2, 0, 3, 1};
    const std::vector<uint32_t> starts = {0, 1, 3};
    const std::vector<uint32_t> want = {7, 0, 6};
    CHECK(first_set_per_run(values, order, starts, 0u, 2) == want);
    CHECK(first_set_per_run(values, order, starts, 0u, 2, std::vector<uint32_t>(3, 8)) == want);
}

TEST(cell_lists_lists_every_cell_once_with_its_own_list) {
    std::unordered_map<uint64_t, std::vector<uint32_t>> m;
    Rng r(5);
    for (uint32_t c = 0; c < 70000; c++) m[r.g()].push_back(c);
    for (unsigned threads : kThreadCounts) {
        auto cells = cell_lists(m, threads);
        REQUIRE(cells.size() == m.size());
        std::set<uint64_t> seen;
        bool own = true;
        for (const auto& [cell, ids] : cells) {
            seen.insert(cell);
            own = own && ids == &m.at(cell);
        }
        CHECK_EQ(seen.size(), m.size());
        CHECK(own);
    }
}

TEST(sorted_item_cells_orders_by_cell_then_item) {
    Rng r(21);
    std::vector<uint64_t> cells(200000);
    for (auto& cell : cells) cell = r.below(30000) * 0x9E3779B97F4A7C15ull;
    std::map<uint64_t, std::set<uint32_t>> want;
    for (uint32_t i = 0; i < cells.size(); i++) want[cells[i]].insert(i);
    std::vector<std::pair<uint64_t, uint32_t>> want_pairs;
    for (const auto& [cell, ids] : want)
        for (uint32_t id : ids) want_pairs.push_back({cell, id});

    for (unsigned threads : kThreadCounts) {
        auto pairs = sorted_item_cells(cells, threads);
        REQUIRE(pairs.size() == want_pairs.size());
        bool same = true;
        for (size_t i = 0; i < pairs.size(); i++)
            same = same && pairs[i].cell_id == want_pairs[i].first && pairs[i].item_id == want_pairs[i].second;
        CHECK(same);
    }
}
