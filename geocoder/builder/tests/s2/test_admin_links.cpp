// The builder's admin links (admin_links.h) on the polygons' exact rings:
// a ring S2 won't take as a loop still gets its candidates, a point one e7
// step inside one of two neighbours links to that one, a street takes the
// postcode at its middle and a POI none of another country, and the
// candidates don't depend on how a ring is written down. A street's postal
// area covers its middle, its edges included, is of the street's country, or
// of any where the street has none, and has a postcode the server takes. A
// house keeps the postcode its country's pattern makes of its tag, and the
// centroids are summed by the one it makes of the tag for them, whatever the
// house keeps. A boundary that repeats the closest boundary relation round
// its point, the same outline whatever vertices lie along its edges, and
// links no place node is in no address; one that links one takes the node's
// rank.
#include "admin_links.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "admin_arcs.h"
#include "cell_index.h"
#include "cell_walk.h"
#include "country_code.h"
#include "place_claims.h"
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
    const uint32_t north = a.add(POSTAL_AREA_LEVEL, box(0.005, 0.02, 0.06, 0.04), {}, "1000");
    a.add(POSTAL_AREA_LEVEL, box(-0.07, 0.02, -0.002, 0.04), {}, "2000");
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
    a.add(POSTAL_AREA_LEVEL, box(0.499, 0.509, 0.501, 0.511), {}, "8000", ch);
    const uint32_t invalid = a.add(POSTAL_AREA_LEVEL, box(0.498, 0.508, 0.502, 0.512), {}, "123", de);
    a.data.admin_polygons[invalid].place_type_override = POSTAL_AREA_INVALID;
    a.add(POSTAL_AREA_LEVEL, box(0.49, 0.505, 0.502, 0.53), {}, "10115", de);
    const uint32_t nearest = a.add(POSTAL_AREA_LEVEL, box(0.45, 0.46, 0.55, 0.56), {}, "10117", de);
    a.add_way({at(0.5, 0.5098), at(0.5, 0.5102)});
    a.map();
    compute_way_parent_polygons(a.data, 1);
    CHECK_EQ(a.data.way_postcode_ids[0], a.data.admin_polygons[nearest].name_id);
}

TEST(a_postal_area_of_no_country_names_no_street_of_a_country) {
    // Nominatim makes no postcode area of a postal area of no country (get_matcher(None) is None,
    // postcodes.py _update_postcode_areas), and get_nearest_postcode takes an area only of the
    // street's own country (p.country_code = country). An area of no country centred on a German
    // street's middle leaves the street to the German area round it; outside every country, a
    // street takes the area of no country centred on it.
    const uint16_t de = pack_country_code('D', 'E');
    Admins a;
    a.add(2, box(0, 0, 1, 1), {}, "Deutschland", de);
    a.add(POSTAL_AREA_LEVEL, box(0.499, 0.499, 0.501, 0.502), {}, "AB123");
    const uint32_t german = a.add(POSTAL_AREA_LEVEL, box(0.45, 0.46, 0.55, 0.56), {}, "10115", de);
    a.add_way({at(0.5, 0.5), at(0.5, 0.501)});
    const uint32_t countryless = a.add(POSTAL_AREA_LEVEL, box(-0.501, 0.499, -0.499, 0.502), {}, "AB124");
    a.add(POSTAL_AREA_LEVEL, box(-0.55, 0.46, -0.45, 0.56), {}, "10117", de);
    a.add_way({at(-0.5, 0.5), at(-0.5, 0.501)});
    a.map();
    compute_way_parent_polygons(a.data, 1);
    CHECK_EQ(a.data.way_postcode_ids[0], a.data.admin_polygons[german].name_id);
    CHECK_EQ(a.data.way_postcode_ids[1], a.data.admin_polygons[countryless].name_id);
}

TEST(a_streets_postal_area_is_none_of_a_placeholder_or_a_list_of_postcodes) {
    // The server's postal_area_postcode passes over an area whose postcode lists several or, past
    // its spaces and hyphens, repeats one character (postcode_looks_valid). A US area of 00000,
    // which the US pattern takes, centred on a street's middle leaves the street to the larger
    // area of 10001 round it; outside every country, an area of 12345;12346 leaves it to 12345.
    const uint16_t us = pack_country_code('U', 'S');
    Admins a;
    a.add(2, box(0, 0, 1, 1), {}, "United States", us);
    a.add(POSTAL_AREA_LEVEL, box(0.499, 0.499, 0.501, 0.502), {}, "00000", us);
    const uint32_t zip = a.add(POSTAL_AREA_LEVEL, box(0.45, 0.46, 0.55, 0.56), {}, "10001", us);
    a.add_way({at(0.5, 0.5), at(0.5, 0.501)});
    a.add(POSTAL_AREA_LEVEL, box(-0.501, 0.499, -0.499, 0.502), {}, "12345;12346");
    const uint32_t plain = a.add(POSTAL_AREA_LEVEL, box(-0.55, 0.46, -0.45, 0.56), {}, "12345");
    a.add_way({at(-0.5, 0.5), at(-0.5, 0.501)});
    a.map();
    compute_way_parent_polygons(a.data, 1);
    CHECK_EQ(a.data.way_postcode_ids[0], a.data.admin_polygons[zip].name_id);
    CHECK_EQ(a.data.way_postcode_ids[1], a.data.admin_polygons[plain].name_id);
}

TEST(a_streets_postal_area_has_a_postcode_its_country_takes_whatever_its_length) {
    // Nominatim makes a postcode area of every postcode its country's pattern takes
    // (postcodes.py _update_postcode_areas): Saudi Arabia's ddddd(?:-dddd)? takes 12345-6789,
    // kept whole, and Iran's (ddddd)[-_ ]?(ddddd) 12345-67890, of 11 characters. Each, centred on
    // a street's middle, names the street over the larger area round it.
    const uint16_t sa = pack_country_code('S', 'A'), ir = pack_country_code('I', 'R');
    Admins a;
    a.add(2, box(0, 0, 1, 1), {}, "Saudi Arabia", sa);
    a.add(2, box(1, 0, 2, 1), {}, "Iran", ir);
    const uint32_t extended = a.add(POSTAL_AREA_LEVEL, box(0.499, 0.499, 0.501, 0.502), {}, "12345-6789", sa);
    a.add(POSTAL_AREA_LEVEL, box(0.45, 0.46, 0.55, 0.56), {}, "12345", sa);
    a.add_way({at(0.5, 0.5), at(0.5, 0.501)});
    const uint32_t iranian = a.add(POSTAL_AREA_LEVEL, box(1.499, 0.499, 1.501, 0.502), {}, "12345-67890", ir);
    a.add(POSTAL_AREA_LEVEL, box(1.45, 0.46, 1.55, 0.56), {}, "1234567891", ir);
    a.add_way({at(1.5, 0.5), at(1.5, 0.501)});
    a.map();
    compute_way_parent_polygons(a.data, 1);
    CHECK_EQ(a.data.way_postcode_ids[0], a.data.admin_polygons[extended].name_id);
    CHECK_EQ(a.data.way_postcode_ids[1], a.data.admin_polygons[iranian].name_id);
}

TEST(a_streets_postal_area_holds_its_middle_on_its_edges_too) {
    // get_nearest_postcode takes the postal areas covering the street's centroid (ST_Covers,
    // utils.sql:172), their edges included: streets whose middles lie on a postal area's north
    // edge, on a hole's south edge and on the north edge of a ring of 120 vertices, which an edge
    // index tests, take their areas' postcodes.
    Admins a;
    const uint32_t north = a.add(POSTAL_AREA_LEVEL, box(0.49, 0.5, 0.5, 0.51), {}, "1000");
    a.add_way({at(0.5, 0.502), at(0.5, 0.504)});
    const uint32_t holed = a.add(POSTAL_AREA_LEVEL, box(0.6, 0.5, 0.7, 0.6), {reversed(box(0.64, 0.54, 0.66, 0.56))}, "2000");
    a.add_way({at(0.64, 0.545), at(0.64, 0.555)});
    Ring dense;
    for (int k = 0; k < 30; k++) dense.push_back(at(0.3, 0.5 + 0.01 * k / 30));
    for (int k = 0; k < 30; k++) dense.push_back(at(0.3 + 0.1 * k / 30, 0.51));
    for (int k = 0; k < 30; k++) dense.push_back(at(0.4, 0.51 - 0.01 * k / 30));
    for (int k = 0; k < 30; k++) dense.push_back(at(0.4 - 0.1 * k / 30, 0.5));
    const uint32_t indexed = a.add(POSTAL_AREA_LEVEL, closed(dense), {}, "3000");
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
    const uint32_t swiss = a.add(POSTAL_AREA_LEVEL, box(0.99, 0.4, 1.2, 0.6), {}, "8000", ch);
    const uint32_t german = a.add(POSTAL_AREA_LEVEL, box(0.8, 0.3, 1.0, 0.7), {}, "79539", de);
    a.add_poi(0.995f, 0.5f);
    a.add_poi(1.01f, 0.5f);
    a.map();
    a.link_pois();
    CHECK_EQ(a.data.poi_records[0].parent_postcode_id, a.data.admin_polygons[german].name_id);
    CHECK_EQ(a.data.poi_records[1].parent_postcode_id, a.data.admin_polygons[swiss].name_id);
}

TEST(a_house_keeps_the_postcode_its_countrys_pattern_makes_of_its_tag) {
    // The clean-postcodes sanitizer matches a house's addr:postcode in the house's own country
    // (PostcodeFormatter.match: upper-cased, after an optional country code and a space or a
    // hyphen, the pattern's output) and makes one that fails an address term, so the house has no
    // postcode of its own and takes its street's (placex_update). A house outside every country
    // known keeps its tag. The centroids are summed by the postcodes the houses keep: Germany's
    // 78462 of four tags, the US 10001 of three.
    const uint16_t de = pack_country_code('D', 'E'), us = pack_country_code('U', 'S');
    Admins a;
    a.add(2, box(0, 0, 1, 1), {}, "Deutschland", de);
    a.add(2, box(1, 0, 2, 1), {}, "United States", us);
    a.map();
    struct House {
        double lat, lng;
        const char* tag;
        const char* kept;
    };
    const std::vector<House> houses = {
        {0.5, 0.5, "78462", "78462"},       {0.5, 0.5, "DE-78462", "78462"},    {0.5, 0.5, "de 78462", "78462"},
        {0.5, 0.5, " DE78462 ", "78462"},   {0.5, 0.5, "12ABC", nullptr},       {0.5, 0.5, "7846", nullptr},
        {0.5, 0.5, "DE-7846", nullptr},     {0.5, 0.5, "US-78462", nullptr},    {0.5, 0.5, "78-462", nullptr},
        {0.5, 0.5, "D-E-78462", nullptr},   {1.5, 0.5, "10001", "10001"},       {1.5, 0.5, "10001-2062", "10001"},
        {1.5, 0.5, "US 10001-2062", "10001"}, {1.5, 0.5, "100012062", nullptr}, {1.5, 0.5, "1000", nullptr},
        {1.5, 0.5, "00000", nullptr},       {5.0, 5.0, "12ABC", "12ABC"},
    };
    for (const House& h : houses) {
        AddrPoint ap{};
        ap.lat = static_cast<float>(h.lat);
        ap.lng = static_cast<float>(h.lng);
        ap.vertex_offset = NO_DATA;
        a.data.addr_points.push_back(ap);
        a.data.addr_postcode_ids.push_back(a.data.string_pool.intern(h.tag));
    }
    normalise_addr_postcodes(a.data, 1);
    std::vector<uint32_t> kept;
    for (const House& h : houses) kept.push_back(h.kept ? a.data.string_pool.intern(h.kept) : NO_DATA);
    collect_postcode_centroids(a.data);
    for (size_t i = 0; i < houses.size(); i++) {
        CHECK_EQ(a.data.addr_postcode_ids[i], kept[i]);
        if (a.data.addr_postcode_ids[i] != kept[i])
            std::printf("    \"%s\" at (%g, %g)\n", houses[i].tag, houses[i].lat, houses[i].lng);
    }
    const auto& accum = a.data.postcode_accum;
    REQUIRE(accum.count(postcode_key(de, kept[0])) && accum.count(postcode_key(us, kept[10])));
    CHECK_EQ(accum.at(postcode_key(de, kept[0])).count, uint64_t(4));
    CHECK_EQ(accum.at(postcode_key(us, kept[10])).count, uint64_t(3));
    CHECK_EQ(accum.size(), size_t(2));
}

TEST(a_centroid_takes_the_postcode_the_countrys_pattern_makes_of_the_tag_whatever_the_house_keeps) {
    // update_postcodes sums the houses' addr:postcode tags as it reads them (placex.address) by the
    // postcode their country's pattern makes of them, PostcodeFormatter's default .* where the
    // country has none (_PostcodeCollector.add); the clean-postcodes sanitizer's default for a
    // house's own is [A-Z0-9- ]{3,12}. Montserrat has no pattern: two houses of 12.A and 12.a keep
    // no postcode of their own, and the centroid of 12.A is theirs, halfway between them; MSR1110
    // is the house's and the centroid's alike. A copy of a house of 12.A, deduplicated before the
    // centroids are summed (reorder_addr_points), is summed once.
    const uint16_t ms = pack_country_code('M', 'S');
    Admins a;
    a.add(2, box(0, 0, 1, 1), {}, "Montserrat", ms);
    a.map();
    struct House {
        double lat, lng;
        const char* tag;
        uint64_t osm_id;
    };
    const std::vector<House> houses = {
        {0.2, 0.2, "12.A", 1}, {0.4, 0.4, "12.a", 2}, {0.6, 0.6, "MSR1110", 3}, {0.2, 0.2, "12.A", 1},
    };
    for (const House& h : houses) {
        AddrPoint ap{};
        ap.lat = static_cast<float>(h.lat);
        ap.lng = static_cast<float>(h.lng);
        ap.vertex_offset = NO_DATA;
        a.data.addr_points.push_back(ap);
        a.data.addr_postcode_ids.push_back(a.data.string_pool.intern(h.tag));
        a.data.addr_osm_ids.push_back(pack_osm_id(gc::id_alloc::ObjectType::OSM_NODE, h.osm_id));
    }
    normalise_addr_postcodes(a.data, 1);
    const uint32_t twelve = a.data.string_pool.intern("12.A"), msr = a.data.string_pool.intern("MSR1110");
    CHECK_EQ(a.data.addr_postcode_ids[0], NO_DATA);
    CHECK_EQ(a.data.addr_postcode_ids[1], NO_DATA);
    CHECK_EQ(a.data.addr_postcode_ids[2], msr);
    // The copy goes as reorder_addr_points takes it out.
    a.data.addr_points.pop_back();
    a.data.addr_postcode_ids.pop_back();
    a.data.addr_osm_ids.pop_back();
    collect_postcode_centroids(a.data);
    const auto& accum = a.data.postcode_accum;
    REQUIRE(accum.count(postcode_key(ms, twelve)) && accum.count(postcode_key(ms, msr)));
    CHECK_EQ(accum.at(postcode_key(ms, twelve)).count, uint64_t(2));
    CHECK(std::abs(accum.at(postcode_key(ms, twelve)).lat() - 0.3) < 1e-6);
    CHECK_EQ(accum.at(postcode_key(ms, msr)).count, uint64_t(1));
    CHECK_EQ(accum.size(), size_t(2));
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

TEST(a_street_links_to_a_level_11_or_12_polygon_round_it_and_a_poi_to_one_of_level_10_or_less) {
    // Nominatim's administrative levels run to 12: a street inside a level 11 polygon has it as
    // its parent, the first of its address chain, and one inside a level 12 polygon that one. A
    // POI's parent is the jurisdiction a query must lie in for the POI to answer it
    // (poi_chain_contains_query), of levels 2 to 10: one inside a level 12 polygon has the level
    // 10 one round it. A postal area round them is none's.
    Admins a;
    a.add(2, closed({at(45, 5), at(45, 11), at(48, 11), at(48, 5)}));
    const uint32_t l10 = a.add(10, closed({at(46.0, 7.0), at(46.0, 7.4), at(46.4, 7.4), at(46.4, 7.0)}));
    const uint32_t l11 = a.add(11, closed({at(46.1, 7.1), at(46.1, 7.3), at(46.3, 7.3), at(46.3, 7.1)}));
    const uint32_t l12 = a.add(12, closed({at(46.15, 7.15), at(46.15, 7.2), at(46.2, 7.2), at(46.2, 7.15)}));
    const uint32_t postal = a.add(POSTAL_AREA_LEVEL, closed({at(46.05, 7.05), at(46.05, 7.35), at(46.35, 7.35), at(46.35, 7.05)}));
    a.add_poi(46.17f, 7.17f);
    a.add_way({at(46.25, 7.22), at(46.26, 7.24)});
    a.add_way({at(46.16, 7.16), at(46.17, 7.18)});
    a.map();
    a.link_pois();
    compute_way_parent_polygons(a.data, 1);
    CHECK_EQ(a.data.poi_records[0].parent_poly_id, l10);
    CHECK_EQ(a.data.poi_records[0].parent_postcode_id, a.data.admin_polygons[postal].name_id);
    CHECK_EQ(a.data.way_parent_ids[0], l11);
    CHECK_EQ(a.data.way_parent_ids[1], l12);
}

TEST(admin_rings_cover_their_edges_and_contain_a_shared_edge_on_one_side) {
    // Two squares sharing an edge, the west one with a hole and its outer ring of 200 vertices
    // (behind an edge index), the east one of 4 (scanned): a point on the shared edge, on a
    // vertex, on the hole's edge and on each outer edge is covered, as _ST_Covers has it; of
    // the two squares only one contains the middle of the edge they share; a point a step
    // outside either, or inside the hole, is not covered.
    Admins a;
    Ring west;
    for (int k = 0; k < 50; k++) west.push_back({0, 20000 * k});
    for (int k = 0; k < 50; k++) west.push_back({20000 * k, 1000000});
    for (int k = 0; k < 50; k++) west.push_back({1000000, 1000000 - 20000 * k});
    for (int k = 0; k < 50; k++) west.push_back({1000000 - 20000 * k, 0});
    const Ring hole = closed({{400000, 400000}, {400000, 600000}, {600000, 600000}, {600000, 400000}});
    const uint32_t w = a.add(8, closed(west), {hole});
    const uint32_t e = a.add(8, closed({{0, 1000000}, {0, 2000000}, {1000000, 2000000}, {1000000, 1000000}}));
    const AdminRings rings(a.data.admin_exact_rings, 1);
    for (const PackedLocation p : {PackedLocation{500000, 1000000}, PackedLocation{0, 1000000}, PackedLocation{1000000, 1000000}}) {
        CHECK(rings.covers(w, p));
        CHECK(rings.covers(e, p));
    }
    CHECK(rings.contains(w, PackedLocation{500000, 1000000}) != rings.contains(e, PackedLocation{500000, 1000000}));
    for (const PackedLocation p : {PackedLocation{0, 500000}, PackedLocation{1000000, 500000}, PackedLocation{500000, 0},
                                   PackedLocation{0, 0}, PackedLocation{500000, 400000}, PackedLocation{600000, 500000}})
        CHECK(rings.covers(w, p));
    for (const PackedLocation p : {PackedLocation{1000000, 1500000}, PackedLocation{500000, 2000000}, PackedLocation{0, 1500000}})
        CHECK(rings.covers(e, p));
    for (const PackedLocation p : {PackedLocation{-1, 500000}, PackedLocation{500000, -1}, PackedLocation{1000001, 500000},
                                   PackedLocation{500000, 500000}, PackedLocation{450000, 550000}})
        CHECK(!rings.covers(w, p));
    CHECK(!rings.covers(e, PackedLocation{500000, 2000001}));
    CHECK(rings.covers(w, PackedLocation{300000, 300000}));
}
