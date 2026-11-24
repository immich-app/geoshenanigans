// The builder's admin links (admin_links.h) on the polygons' exact rings:
// a ring S2 won't take as a loop still gets its candidates, a point one e7
// step inside one of two neighbours links to that one, a street takes the
// postcode at its middle and a POI none of another country, and the
// candidates don't depend on how a ring is written down. A street's postal
// area covers its middle, its edges included, and has a postcode the server
// takes.
#include "admin_links.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "admin_arcs.h"
#include "cell_index.h"
#include "cell_walk.h"
#include "country_code.h"
#include "s2_helpers.h"
#include "test_framework.h"

namespace {

using Ring = std::vector<PackedLocation>;

PackedLocation at(double lat, double lng) { return point_e7(lat, lng); }

// The closed ring of these vertices.
Ring closed(Ring vertices) {
    vertices.push_back(vertices.front());
    return vertices;
}

// Admin polygons as the build leaves them: records, exact rings, osm ids,
// and the admin cell index over their arcs' tiers.
struct Admins {
    ParsedData data;
    AdminArcs arcs;

    // A polygon named `name`, "area <slot>" without one, of the country `cc`.
    uint32_t add(uint8_t level, const Ring& outer, const std::vector<Ring>& holes = {}, const std::string& name = "",
                 uint16_t cc = 0) {
        const uint32_t pid = static_cast<uint32_t>(data.admin_polygons.size());
        data.admin_exact_rings.append(outer, holes);
        AdminPolygon p{};
        p.admin_level = level;
        p.name_id = data.string_pool.intern(name.empty() ? "area " + std::to_string(pid) : name);
        p.country_code = cc;
        p.area = data.admin_exact_rings.area(pid);
        data.admin_polygons.push_back(p);
        data.admin_osm_ids.push_back(1000 + pid);
        return pid;
    }

    void map() {
        const uint32_t n = static_cast<uint32_t>(data.admin_polygons.size());
        arcs = build_admin_arcs(data.admin_exact_rings, std::vector<bool>(n, false), "", 1);
        compute_admin_index_cells(data, arcs, 1);
    }

    void add_poi(float lat, float lng) {
        PoiRecord pr{};
        pr.lat = lat;
        pr.lng = lng;
        pr.vertex_offset = NO_DATA;
        data.poi_records.push_back(pr);
    }

    void link_pois() {
        auto order = items_by_cell(data.poi_records.size(), 1, [](size_t) { return uint64_t(1); });
        auto t = std::chrono::steady_clock::now();
        CpuTicks cpu = CpuTicks::now();
        compute_poi_parent_polygons(data, 1, std::move(order), t, cpu);
    }

    // A street way through these vertices, at f32 as the build holds them.
    void add_way(const Ring& vertices) {
        WayHeader w{};
        w.node_offset = static_cast<uint32_t>(data.street_nodes.size());
        w.node_count = static_cast<uint16_t>(vertices.size());
        w.name_id = NO_DATA;
        for (const PackedLocation& v : vertices)
            data.street_nodes.push_back({static_cast<float>(v.lat()), static_cast<float>(v.lon())});
        data.ways.push_back(w);
    }

    bool listed(PackedLocation p, uint32_t pid) const {
        const auto [first, last] = admin_cell_entries(data.admin_index_cells, admin_link_cell(p).id());
        return std::any_of(first, last, [&](const CellItemPair& e) { return (e.item_id & ID_MASK) == pid; });
    }
};

// The closed ring round the box (south, west, north, east).
Ring box(double s, double w, double n, double e) { return closed({at(s, w), at(s, e), at(n, e), at(n, w)}); }

// The ring from its k-th vertex on, the same ring written from another start.
Ring rotated(const Ring& ring, size_t k) {
    Ring open(ring.begin(), ring.end() - 1);
    std::rotate(open.begin(), open.begin() + static_cast<long>(k % open.size()), open.end());
    return closed(open);
}

Ring reversed(const Ring& ring) { return Ring(ring.rbegin(), ring.rend()); }

}  // namespace

TEST(an_admin_inside_a_ring_s2_rejects_gets_its_parent) {
    // Canton-sized rings near Bern that S2 rejects as loops (a vertex
    // repeated): one with a spike out and back along the same line, one
    // touching itself at a vertex between two lobes.
    const Ring spike = closed({at(46.5, 7.0), at(46.5, 7.4), at(46.2, 7.4), at(46.5, 7.4), at(46.5, 8.0),
                               at(47.0, 8.0), at(47.0, 7.0)});
    const Ring lobes = closed({at(46.5, 7.0), at(47.0, 7.0), at(46.75, 7.5), at(47.0, 8.0), at(46.5, 8.0),
                               at(46.75, 7.5)});
    for (const Ring& canton : {spike, lobes}) {
        Admins a;
        const uint32_t country = a.add(2, closed({at(45, 5), at(45, 11), at(48, 11), at(48, 5)}));
        const uint32_t l4 = a.add(4, canton);
        const uint32_t l8 = a.add(8, closed({at(46.7, 7.1), at(46.7, 7.2), at(46.8, 7.2), at(46.8, 7.1)}));
        a.map();
        compute_admin_parent_chain(a.data, 1);
        CHECK_EQ(a.data.admin_parent_ids[l8], l4);
        CHECK_EQ(a.data.admin_parent_ids[l4], country);
    }
}

TEST(a_poi_one_step_inside_one_of_two_neighbours_links_to_that_one) {
    std::mt19937_64 rng(1102);
    std::uniform_real_distribution<double> lat(-60, 70), lng(-179, 179);
    Admins a;
    std::vector<uint32_t> want;
    for (int k = 0; k < 200; k++) {
        // The POI's f32 point, then an edge 1:3 across the parallels two
        // neighbours share, one e7 step west of it (k even) or east of it.
        const float plat = static_cast<float>(lat(rng)), plng = static_cast<float>(lng(rng));
        const PackedLocation p = point_e7(plat, plng);
        const int32_t east = k % 2 == 0 ? 1 : -1;
        const PackedLocation q{p.lat_e7, p.lon_e7 - east};
        const int32_t m = 100000;
        const PackedLocation s{q.lat_e7 - 3 * m, q.lon_e7 - m}, n{q.lat_e7 + 3 * m, q.lon_e7 + m};
        const uint32_t west_id = a.add(8, closed({s, n, {n.lat_e7, n.lon_e7 - 5 * m}, {s.lat_e7, s.lon_e7 - 5 * m}}));
        const uint32_t east_id = a.add(8, closed({s, {s.lat_e7, s.lon_e7 + 5 * m}, {n.lat_e7, n.lon_e7 + 5 * m}, n}));
        a.add_poi(plat, plng);
        want.push_back(east > 0 ? east_id : west_id);
    }
    a.map();
    a.link_pois();
    for (size_t i = 0; i < want.size(); i++) {
        CHECK_EQ(a.data.poi_records[i].parent_poly_id, want[i]);
        CHECK_EQ(a.data.poi_records[i].parent_postcode_id, NO_DATA);
    }
}

TEST(a_streets_postcode_is_the_postal_area_at_its_middle_wherever_most_of_its_vertices_lie) {
    // Nominatim takes a street's postcode at its centroid, the point halfway
    // along it (get_center_point). Ten of the street's eleven vertices lie
    // close together in a postal area south of the equator, where their
    // average lies too; its long first stretch reaches north of it, into
    // another postal area, which holds its middle. Level-10 cells meet along
    // the equator here: that one is listed in the middle's cell, not in the
    // vertex average's.
    Ring street{at(0.050, 0.03)};
    for (int k = 0; k < 10; k++) street.push_back(at(-0.005 - 0.001 * k, 0.03));
    const PackedLocation average = at((0.050 - 0.095) / 11, 0.03), middle = at(0.018, 0.03);
    Admins a;
    const uint32_t north = a.add(11, box(0.005, 0.02, 0.06, 0.04), {}, "1000");
    a.add(11, box(-0.07, 0.02, -0.002, 0.04), {}, "2000");
    a.add_way(street);
    a.map();
    REQUIRE(a.listed(middle, north));
    REQUIRE(!a.listed(average, north));
    compute_way_parent_polygons(a.data, 1);
    CHECK_EQ(a.data.way_postcode_ids[0], a.data.admin_polygons[north].name_id);
}

TEST(of_the_postal_areas_at_a_streets_middle_the_one_of_its_country_whose_centroid_is_nearest_names_it) {
    // get_nearest_postcode takes, of the postal areas of the street's country
    // covering its centroid, the one whose own centroid is nearest it
    // (ORDER BY ST_Distance(p.centroid, geom)), whatever their size. A
    // Swiss area and one of a postcode Germany can't have, each centred on
    // the middle, name no German street.
    const uint16_t de = pack_country_code('D', 'E'), ch = pack_country_code('C', 'H');
    Admins a;
    a.add(2, box(0, 0, 1, 1), {}, "Deutschland", de);
    a.add(2, box(1, 0, 2, 1), {}, "Schweiz", ch);
    a.add(11, box(0.499, 0.509, 0.501, 0.511), {}, "8000", ch);
    const uint32_t invalid = a.add(11, box(0.498, 0.508, 0.502, 0.512), {}, "123", de);
    a.data.admin_polygons[invalid].place_type_override = POSTAL_AREA_INVALID;
    a.add(11, box(0.49, 0.505, 0.502, 0.53), {}, "10115", de);
    const uint32_t nearest = a.add(11, box(0.45, 0.46, 0.55, 0.56), {}, "10117", de);
    a.add_way({at(0.5, 0.5098), at(0.5, 0.5102)});
    a.map();
    compute_way_parent_polygons(a.data, 1);
    CHECK_EQ(a.data.way_postcode_ids[0], a.data.admin_polygons[nearest].name_id);
}

TEST(a_streets_postal_area_is_none_of_a_placeholder_or_a_list_of_postcodes) {
    // The server's postal_area_postcode passes over an area whose postcode is longer than 10
    // characters, lists several or, past its spaces and hyphens, repeats one character
    // (centroid_postcode_ok, postcode_looks_valid). A US area of 00000, which the US pattern
    // takes, centred on a street's middle leaves the street to the larger area of 10001 round it;
    // outside every country, areas of 12345;12346 and of 123456789012 leave it to 12345.
    const uint16_t us = pack_country_code('U', 'S');
    Admins a;
    a.add(2, box(0, 0, 1, 1), {}, "United States", us);
    a.add(11, box(0.499, 0.499, 0.501, 0.502), {}, "00000", us);
    const uint32_t zip = a.add(11, box(0.45, 0.46, 0.55, 0.56), {}, "10001", us);
    a.add_way({at(0.5, 0.5), at(0.5, 0.501)});
    a.add(11, box(-0.501, 0.499, -0.499, 0.502), {}, "12345;12346");
    a.add(11, box(-0.502, 0.498, -0.498, 0.503), {}, "123456789012");
    const uint32_t plain = a.add(11, box(-0.55, 0.46, -0.45, 0.56), {}, "12345");
    a.add_way({at(-0.5, 0.5), at(-0.5, 0.501)});
    a.map();
    compute_way_parent_polygons(a.data, 1);
    CHECK_EQ(a.data.way_postcode_ids[0], a.data.admin_polygons[zip].name_id);
    CHECK_EQ(a.data.way_postcode_ids[1], a.data.admin_polygons[plain].name_id);
}

TEST(a_streets_postal_area_holds_its_middle_on_its_edges_too) {
    // get_nearest_postcode takes the postal areas covering the street's centroid (ST_Covers,
    // utils.sql:172), their edges included: streets whose middles lie on a postal area's north
    // edge, on a hole's south edge and on the north edge of a ring of 120 vertices, which an edge
    // index tests, take their areas' postcodes.
    Admins a;
    const uint32_t north = a.add(11, box(0.49, 0.5, 0.5, 0.51), {}, "1000");
    a.add_way({at(0.5, 0.502), at(0.5, 0.504)});
    const uint32_t holed = a.add(11, box(0.6, 0.5, 0.7, 0.6), {reversed(box(0.64, 0.54, 0.66, 0.56))}, "2000");
    a.add_way({at(0.64, 0.545), at(0.64, 0.555)});
    Ring dense;
    for (int k = 0; k < 30; k++) dense.push_back(at(0.3, 0.5 + 0.01 * k / 30));
    for (int k = 0; k < 30; k++) dense.push_back(at(0.3 + 0.1 * k / 30, 0.51));
    for (int k = 0; k < 30; k++) dense.push_back(at(0.4, 0.51 - 0.01 * k / 30));
    for (int k = 0; k < 30; k++) dense.push_back(at(0.4 - 0.1 * k / 30, 0.5));
    const uint32_t indexed = a.add(11, closed(dense), {}, "3000");
    a.add_way({at(0.4, 0.502), at(0.4, 0.504)});
    a.map();
    compute_way_parent_polygons(a.data, 1);
    CHECK_EQ(a.data.way_postcode_ids[0], a.data.admin_polygons[north].name_id);
    CHECK_EQ(a.data.way_postcode_ids[1], a.data.admin_polygons[holed].name_id);
    CHECK_EQ(a.data.way_postcode_ids[2], a.data.admin_polygons[indexed].name_id);
}

TEST(a_poi_takes_no_postcode_of_a_postal_area_of_another_country) {
    // get_nearest_postcode takes a postcode only of the POI's own country
    // (p.country_code = country): a Swiss area reaching over the border, its
    // centroid in Switzerland, names the Swiss POI and not the German one,
    // which takes the larger German area round it.
    const uint16_t de = pack_country_code('D', 'E'), ch = pack_country_code('C', 'H');
    Admins a;
    a.add(2, box(0, 0, 1, 1), {}, "Deutschland", de);
    a.add(2, box(1, 0, 2, 1), {}, "Schweiz", ch);
    const uint32_t swiss = a.add(11, box(0.99, 0.4, 1.2, 0.6), {}, "8000", ch);
    const uint32_t german = a.add(11, box(0.8, 0.3, 1.0, 0.7), {}, "79539", de);
    a.add_poi(0.995f, 0.5f);
    a.add_poi(1.01f, 0.5f);
    a.map();
    a.link_pois();
    CHECK_EQ(a.data.poi_records[0].parent_postcode_id, a.data.admin_polygons[german].name_id);
    CHECK_EQ(a.data.poi_records[1].parent_postcode_id, a.data.admin_polygons[swiss].name_id);
}

TEST(a_house_keeps_no_postcode_of_its_own_that_its_countrys_pattern_rejects) {
    // The clean-postcodes sanitizer matches a house's addr:postcode in the house's own country
    // (CountryPostcodeMatcher: upper-cased, after an optional country code and a space or a
    // hyphen) and makes one that fails an address term, so the house has no postcode of its own
    // and takes its street's (placex_update). A house outside every country known keeps its tag.
    // Each tag still counts towards the centroids, which keep only the pairs the pattern takes.
    Admins a;
    a.add(2, box(0, 0, 1, 1), {}, "Deutschland", pack_country_code('D', 'E'));
    a.add(2, box(1, 0, 2, 1), {}, "United States", pack_country_code('U', 'S'));
    a.map();
    struct House {
        double lat, lng;
        const char* tag;
        bool kept;
    };
    const std::vector<House> houses = {
        {0.5, 0.5, "78462", true},      {0.5, 0.5, "DE-78462", true},    {0.5, 0.5, "de 78462", true},
        {0.5, 0.5, " DE78462 ", true},  {0.5, 0.5, "12ABC", false},      {0.5, 0.5, "7846", false},
        {0.5, 0.5, "DE-7846", false},   {0.5, 0.5, "US-78462", false},   {1.5, 0.5, "10001", true},
        {1.5, 0.5, "10001-2062", true}, {1.5, 0.5, "US 10001-2062", true}, {1.5, 0.5, "100012062", false},
        {1.5, 0.5, "1000", false},      {5.0, 5.0, "12ABC", true},
    };
    for (const House& h : houses) {
        AddrPoint ap{};
        ap.lat = static_cast<float>(h.lat);
        ap.lng = static_cast<float>(h.lng);
        ap.vertex_offset = NO_DATA;
        a.data.addr_points.push_back(ap);
        a.data.addr_postcode_ids.push_back(a.data.string_pool.intern(h.tag));
    }
    const std::vector<uint32_t> tags = a.data.addr_postcode_ids;
    collect_postcode_centroids(a.data);
    for (size_t i = 0; i < houses.size(); i++) {
        CHECK_EQ(a.data.addr_postcode_ids[i], houses[i].kept ? tags[i] : NO_DATA);
        if (a.data.addr_postcode_ids[i] != (houses[i].kept ? tags[i] : NO_DATA))
            std::printf("    \"%s\" at (%g, %g)\n", houses[i].tag, houses[i].lat, houses[i].lng);
    }
    CHECK(a.data.postcode_accum.count(postcode_key(pack_country_code('D', 'E'), tags[4])));
}

TEST(a_point_finds_no_country_where_only_its_simplified_rings_reach_its_cell) {
    // A country round the corner (0, 0), where the cells south of the
    // equator and east of Greenwich begin at every level: its exact rings
    // keep 6.7 m from that cell, round a notch north-west of the corner
    // that the simplified tier cuts across the cell. A point in the cell,
    // outside every country, has none, though a list of only that country
    // would hand it the country.
    Admins a;
    const uint32_t country =
        a.add(2, closed({at(-0.002, -0.01), at(-0.002, -0.00006), at(0.0002, -0.0002), at(0.00006, 0.00009),
                         at(0.00006, 0.01), at(0.01, 0.01), at(0.01, -0.01)}));
    a.data.admin_polygons[country].country_code = pack_country_code('C', 'H');
    a.map();
    const PackedLocation p = at(-0.005, 0.005);
    REQUIRE(a.listed(p, country));  // by the simplified tier
    REQUIRE(!kArcTiers[1].simplifies());
    const auto exact = admin_tier_cells(a.arcs, a.data.admin_exact_rings, 1, 1).cells;
    REQUIRE(std::none_of(exact.begin(), exact.end(), [&](const CellItemPair& e) {
        return e.cell_id == admin_link_cell(p).id();
    }));
    const AdminRings countries = border_country_rings(a.data, 1);
    CHECK_EQ(country_code_at_point(a.data, countries, p.lat(), p.lon()), uint16_t(0));
    CHECK_EQ(country_code_at_point(a.data, countries, 0.005, -0.005), pack_country_code('C', 'H'));
}

TEST(admin_candidates_are_the_same_for_any_rotation_or_direction_of_the_rings) {
    std::mt19937_64 rng(27);
    std::uniform_real_distribution<double> unit(0, 1);
    size_t cells = 0;
    for (int round = 0; round < 16; round++) {
        // A star with a hole, near the poles and the antimeridian too.
        const double clat = round % 4 == 0 ? 88.0 : -70 + 140 * unit(rng);
        const double clng = round % 4 == 1 ? 179.7 : -170 + 340 * unit(rng);
        const double radius = round % 2 ? 0.05 : 1.0;
        const size_t n = 300 + rng() % 1500;
        Ring outer, hole;
        for (size_t k = 0; k < n; k++) {
            const double t = 2 * M_PI * (k + 0.5 * unit(rng)) / n, r = radius * (0.6 + 0.4 * unit(rng));
            outer.push_back(at(std::min(89.999, clat + r * std::sin(t)), std::min(179.999, clng + r * std::cos(t))));
            if (k % 10 == 0)
                hole.push_back(at(std::min(89.999, clat + 0.2 * r * std::sin(t)),
                                  std::min(179.999, clng + 0.2 * r * std::cos(t))));
        }
        outer = closed(outer);
        hole = closed(reversed(hole));
        auto candidates = [](const Ring& o, const Ring& h) {
            Admins a;
            a.add(4, o, {h});
            a.map();
            std::vector<std::pair<uint64_t, uint32_t>> entries;
            for (const CellItemPair& e : a.data.admin_index_cells) entries.push_back({e.cell_id, e.item_id});
            return entries;
        };
        const auto base = candidates(outer, hole);
        cells += base.size();
        const size_t k = 1 + rng() % (n - 1);
        CHECK(candidates(rotated(outer, k), hole) == base);
        CHECK(candidates(reversed(outer), hole) == base);
        CHECK(candidates(rotated(reversed(outer), k), rotated(reversed(hole), k / 10 + 1)) == base);
    }
    CHECK(cells > 1000);
}
