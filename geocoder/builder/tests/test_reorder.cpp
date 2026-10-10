// The deterministic-ordering steps (reorder.h, partition_strings_into_tiers)
// against the serial reference (reorder_reference.h): every output byte must
// match on inputs dense with ties, duplicates and edge values.
#include "reorder.h"

#include <cstring>
#include <limits>
#include <random>
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
    d.cell_to_addrs[1] = {1, 2, 3};

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
        p.vertex_offset = static_cast<uint32_t>(d.admin_vertices.size());
        p.vertex_count = r.one_in(4) ? 21 + r.below(3) : 1 + r.below(2);
        p.name_id = small_id(r, 4);
        p.admin_level = static_cast<uint8_t>(2 + r.below(2));
        p.country_code = static_cast<uint16_t>(r.below(2));
        p.area = static_cast<float>(r.below(3));
        for (uint32_t j = 0; j < p.vertex_count; j++)
            d.admin_vertices.push_back(j < 3 ? node(r, s) : NodeCoord{1.0f, 1.0f});
        d.admin_polygons.push_back(p);
        if (s.osm_ids) d.admin_osm_ids.push_back(r.one_in(50) && !s.unique_osm_ids ? 0 : i + 1);
        if (s.side_arrays) d.admin_parent_ids.push_back(r.one_in(20) ? n_admin + 1 : small_id(r, n_admin));
    }
    for (uint32_t c = 0; c < 500; c++) {
        auto& ids = d.cell_to_admin[c * 7919ull];
        for (uint32_t k = r.below(40); k > 0; k--)
            ids.push_back((r.one_in(20) ? n_admin + 2 : r.below(n_admin)) | (r.one_in(2) ? INTERIOR_FLAG : 0));
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

void check_same_records(const ParsedData& a, const ParsedData& b) {
    CHECK(same_bytes(a.addr_points, b.addr_points));
    CHECK(same_bytes(a.addr_osm_ids, b.addr_osm_ids));
    CHECK(same_bytes(a.addr_postcode_ids, b.addr_postcode_ids));
    CHECK(same_bytes(a.addr_vertices, b.addr_vertices));
    CHECK(same_pairs(a.sorted_addr_cells, b.sorted_addr_cells));
    CHECK_EQ(a.cell_to_addrs.size(), b.cell_to_addrs.size());
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
    CHECK(same_bytes(a.admin_vertices, b.admin_vertices));
    CHECK(same_bytes(a.admin_parent_ids, b.admin_parent_ids));
    CHECK(a.cell_to_admin == b.cell_to_admin);
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

    for (unsigned threads : kThreadCounts) {
        ParsedData got;
        std::vector<float> got_ele;
        std::vector<uint32_t> got_qids;
        fill_records(got, got_ele, got_qids, seed, s);
        reorder_addr_points(got, threads);
        reorder_ways(got, threads);
        reorder_interps(got, threads);
        reorder_admin_polygons(got, threads);
        reorder_pois(got, got_ele, got_qids, links, threads);
        reorder_place_nodes(got);

        check_same_records(want, got);
        CHECK(same_bytes(want_ele, got_ele));
        CHECK(same_bytes(want_qids, got_qids));
    }
}

// --- String pool partition ---

std::string random_string(Rng& r) {
    std::string s(r.below(9), 'a');
    for (auto& c : s) c = static_cast<char>("abcdQ\xc3\xa9 "[r.below(8)]);
    return s;
}

// A string pool with every kind of reference: valid starts, NO_DATA, offsets
// inside a string, past the pool, and strings only unshipped POIs use.
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
        p.tier = static_cast<uint8_t>(1 + r.below(5));
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

TEST(reorder_steps_match_on_tiny_inputs) {
    for (size_t n : {size_t(8), size_t(100), size_t(5000)}) {
        Shape s;
        s.n = n;
        check_steps_match_reference(5 + n, s);
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
