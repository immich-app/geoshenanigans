// The builder's admin links (admin_links.h) on the polygons' exact rings:
// a ring S2 won't take as a loop still gets its candidates, a point one e7
// step inside one of two neighbours links to that one, a street's postal
// votes reach the cells of its vertices, and the candidates don't depend on
// how a ring is written down.
#include "admin_links.h"

#include <algorithm>
#include <chrono>
#include <random>
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

    uint32_t add(uint8_t level, const Ring& outer, const std::vector<Ring>& holes = {}) {
        const uint32_t pid = static_cast<uint32_t>(data.admin_polygons.size());
        data.admin_exact_rings.append(outer, holes);
        AdminPolygon p{};
        p.admin_level = level;
        p.name_id = pid;
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

TEST(a_streets_postcode_is_the_postal_polygon_holding_most_of_its_vertices_wherever_its_centroid_lies) {
    // Level-10 cells meet along the equator here. Two of the street's three
    // vertices lie in a postal polygon wholly north of it; the third vertex
    // and the vertex average lie south of it, in another postal polygon or
    // in none.
    const Ring street{at(0.010, 0.03), at(0.012, 0.03), at(-0.060, 0.03)};
    const PackedLocation centroid = at((0.010 + 0.012 - 0.060) / 3, 0.03);
    for (const bool south_polygon : {false, true}) {
        Admins a;
        const uint32_t north = a.add(11, closed({at(0.005, 0.02), at(0.005, 0.04), at(0.015, 0.04), at(0.015, 0.02)}));
        if (south_polygon) a.add(11, closed({at(-0.07, 0.02), at(-0.07, 0.04), at(-0.005, 0.04), at(-0.005, 0.02)}));
        a.add_way(street);
        a.map();
        CHECK(a.listed(street[0], north));
        CHECK(!a.listed(centroid, north));
        compute_way_parent_polygons(a.data, 1);
        CHECK_EQ(a.data.way_postcode_ids[0], a.data.admin_polygons[north].name_id);
    }
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
