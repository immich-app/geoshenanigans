// rank_address.h: the rank_address placex_update gives each boundary, place
// area and place node, in the order Nominatim's indexer runs it. Each case is
// one of its rules, laid out as the rings of the objects it was found on.
#include "rank_address.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#include "admin_rank_config.h"
#include "country_code.h"
#include "exact_pip.h"
#include "place_claims.h"
#include "test_framework.h"

namespace {

using gc::id_alloc::ObjectType;

// A box as (south, west, north, east) in degrees.
struct Box {
    double s, w, n, e;
};

using Ring = std::vector<PackedLocation>;

// A closed ring through (lat, lng) points in degrees.
Ring ring_of(std::initializer_list<std::pair<double, double>> points) {
    Ring ring;
    for (const auto& [lat, lng] : points) ring.push_back(pack_location(lat, lng));
    ring.push_back(ring.front());
    return ring;
}

Ring square(Box b) { return ring_of({{b.s, b.w}, {b.s, b.e}, {b.n, b.e}, {b.n, b.w}}); }

uint8_t pt(AdminPlaceType t) { return static_cast<uint8_t>(t); }

// What a polygon carries beyond its rings: its own place tag, the type of the
// place node it links and its wikidata; a place value AdminPlaceType holds
// no type of where own_value is set.
struct Tags {
    AdminPlaceType own = AdminPlaceType::NONE;
    AdminPlaceType linked = AdminPlaceType::NONE;
    std::string wikidata;
    bool boundary_category = false;
    PlaceValue own_value = PlaceValue::NONE;
};

// Admin polygons and place nodes, every point in `country`, and the pass over
// them with every polygon a candidate at every point.
struct World {
    ParsedData data;
    uint16_t country = 0;

    explicit World(const char* cc) : country(pack_country_code(cc[0], cc[1])) {}

    uint32_t add(uint64_t osm_id, uint8_t level, const Ring& outer, const std::vector<Ring>& holes, const Tags& tags) {
        const uint32_t pid = static_cast<uint32_t>(data.admin_polygons.size());
        data.admin_exact_rings.append(outer, holes);
        AdminPolygon p{};
        p.name_id = pid;
        p.admin_level = level;
        const PlaceValue own = tags.own_value != PlaceValue::NONE ? tags.own_value : place_value_of(tags.own);
        p.own_place_value = static_cast<uint8_t>(own);
        p.place_type_override = pt(tags.linked != AdminPlaceType::NONE ? tags.linked : tags.own);
        if (tags.boundary_category) p.flags |= ADMIN_FLAG_BOUNDARY;
        p.area = data.admin_exact_rings.area(pid);
        data.admin_polygons.push_back(p);
        data.admin_osm_ids.push_back(osm_id);
        const uint64_t boundary = admin_boundary_id(osm_id);
        if (!tags.wikidata.empty()) {
            AdminTags t;
            t.boundary = boundary;
            t.wikidata = tags.wikidata;
            data.admin_tags.push_back(t);
        }
        // A link, as the link pass gives it: the node's rank in the country.
        if (tags.linked != AdminPlaceType::NONE) {
            const PlaceValue linked = place_value_of(tags.linked);
            data.admin_links.push_back({boundary, 0, static_cast<uint8_t>(linked),
                                        place_value_ranks(country, linked).address, LinkStep::LABEL});
        }
        return pid;
    }

    // Ring `ring` of an administrative relation.
    uint32_t relation(int64_t id, uint8_t level, const Ring& outer, const Tags& tags = {}, uint16_t ring = 0,
                      const std::vector<Ring>& holes = {}) {
        // A relation's wikidata is listed once.
        Tags once = tags;
        if (ring > 0) once.wikidata.clear();
        return add(admin_ring_id(id, ring), level, outer, holes, once);
    }
    uint32_t relation(int64_t id, uint8_t level, Box box, const Tags& tags = {}) {
        return relation(id, level, square(box), tags);
    }
    // A closed administrative way.
    uint32_t way(int64_t id, uint8_t level, Box box, const Tags& tags = {}) {
        return add(way_boundary_id(id), level, square(box), {}, tags);
    }
    // A place area of `type`, a relation or a closed way.
    uint32_t place_area(int64_t id, AdminPlaceType type, const Ring& outer, Tags tags = {}) {
        tags.own = type;
        return add(admin_ring_id(id, 0), 15, outer, {}, tags);
    }
    uint32_t place_area(int64_t id, AdminPlaceType type, Box box, Tags tags = {}) {
        return place_area(id, type, square(box), tags);
    }
    uint32_t place_way(int64_t id, AdminPlaceType type, Box box) {
        Tags tags;
        tags.own = type;
        return add(way_boundary_id(id), 15, square(box), {}, tags);
    }
    // A postal area (POSTAL_AREA_LEVEL).
    uint32_t postal(int64_t id, Box box) { return add(admin_ring_id(id, 0), POSTAL_AREA_LEVEL, square(box), {}, {}); }

    uint32_t node(PlaceType type, double lat, double lng, bool hidden = false) {
        PlaceNode pn{};
        pn.lat = static_cast<float>(lat);
        pn.lng = static_cast<float>(lng);
        pn.name_id = static_cast<uint32_t>(data.place_nodes.size());
        pn.place_type = static_cast<uint8_t>(type);
        pn.parent_poly_id = NO_DATA;
        if (hidden) pn._pad1 = PLACE_FLAG_HIDDEN;
        data.place_nodes.push_back(pn);
        data.place_points.push_back(point_e7(lat, lng));
        return pn.name_id;
    }

    // The boundary of polygon pid links the node (place_node_links).
    void link(uint32_t node, uint32_t pid) {
        data.place_node_links.push_back({node, admin_boundary_id(data.admin_osm_ids[pid])});
    }

    // Every polygon a candidate at every point, which its rings cover where the point is inside
    // them or on an edge (AdminRings::covers).
    RankLookups lookups() {
        RankLookups l;
        l.polygons_near = [this](PackedLocation, const std::function<void(uint32_t)>& visit) {
            for (uint32_t pid = 0; pid < data.admin_polygons.size(); pid++) visit(pid);
        };
        l.covers = [this](uint32_t pid, PackedLocation p) {
            const AdminExactRings& exact = data.admin_exact_rings;
            bool inside = false;
            for (uint32_t r = 0; r < exact.polygons[pid].ring_count; r++) {
                const RingSpan& ring = exact.rings_of(pid)[r];
                const PackedLocation* v = exact.vertices.data() + ring.begin;
                if (exact_ring_on_edge(v, ring.count, p)) return true;
                inside ^= exact_ring_contains(v, ring.count, p);
            }
            return inside;
        };
        l.country_at = [this](PackedLocation) { return country; };
        return l;
    }

    RankStats rank(unsigned threads = 4) { return compute_rank_addresses(data, lookups(), threads); }

    uint8_t of(uint32_t pid) const { return data.admin_polygons[pid].rank_address; }
    uint8_t node_rank(uint32_t node) const { return data.place_nodes[node].rank_address; }
};

}  // namespace

TEST(a_boundary_nests_under_the_relation_round_its_point_on_surface_not_round_a_query_point) {
    // Bugisa (admin_level 6) is fifteen times the size of Mbale City (admin_level 4, linked to
    // its city node), which lies in a corner of it: Bugisa's point on surface is in the Eastern
    // Region (admin_level 4) alone, so it keeps its own rank, 12, though every point of Mbale
    // City lies in both.
    World w("UG");
    const uint32_t eastern = w.relation(1, 4, Box{0, 33, 3, 36});
    const uint32_t mbale = w.relation(2, 4, Box{1.0, 34.1, 1.2, 34.3}, {AdminPlaceType::NONE, AdminPlaceType::CITY});
    const uint32_t bugisa = w.relation(3, 6, Box{0.9, 34.0, 2.0, 35.0});
    w.rank();
    CHECK_EQ(w.of(eastern), uint8_t(8));
    CHECK_EQ(w.of(mbale), uint8_t(16));
    CHECK_EQ(w.of(bugisa), uint8_t(12));
}

TEST(a_boundary_whose_point_on_surface_lies_in_a_higher_ranked_parent_ranks_above_it_everywhere) {
    // Mexico City's admin_level 8 boundary has its point on surface in Cuauhtémoc (admin_level 6,
    // 18 by the borough node it links): it ranks 20, at points outside Cuauhtémoc too.
    World w("MX");
    w.relation(1, 4, Box{19.0, -99.5, 19.8, -98.8});
    const uint32_t cuauhtemoc = w.relation(2, 6, Box{19.38, -99.26, 19.48, -99.18}, {AdminPlaceType::NONE, AdminPlaceType::BOROUGH});
    // An L whose point on surface, (19.425, -99.225), is in Cuauhtémoc.
    const uint32_t mexico = w.relation(3, 8, ring_of({{19.3, -99.3}, {19.3, -98.9}, {19.35, -98.9}, {19.35, -99.15},
                                                      {19.5, -99.15}, {19.5, -99.3}}));
    w.rank();
    CHECK_EQ(w.of(cuauhtemoc), uint8_t(18));
    CHECK_EQ(w.of(mexico), uint8_t(20));
}

TEST(a_boundary_repeating_its_parents_wikidata_and_rings_is_in_no_address) {
    // Lublin's admin_level 6, 7 and 8 relations have one wikidata and one outline: the level 6
    // one links the city node, the others repeat it and rank 0, the level 8 one skipping the
    // level 7 one of rank 0 for its parent.
    World w("PL");
    w.relation(1, 4, Box{50, 21, 52, 24});
    const Box city{51.14, 22.45, 51.30, 22.67};
    const uint32_t l6 = w.relation(2, 6, city, {AdminPlaceType::NONE, AdminPlaceType::CITY, "Q37333"});
    const uint32_t l7 = w.relation(3, 7, city, {AdminPlaceType::NONE, AdminPlaceType::NONE, "Q37333"});
    const uint32_t l8 = w.relation(4, 8, city, {AdminPlaceType::NONE, AdminPlaceType::NONE, "Q37333"});
    // Another wikidata, or another outline, is a boundary of its own.
    const uint32_t other_wikidata = w.relation(5, 9, city, {AdminPlaceType::NONE, AdminPlaceType::NONE, "Q1"});
    const uint32_t other_ring = w.relation(6, 9, Box{51.15, 22.46, 51.29, 22.66}, {AdminPlaceType::NONE, AdminPlaceType::NONE, "Q37333"});
    const RankStats stats = w.rank();
    CHECK_EQ(w.of(l6), uint8_t(16));
    CHECK_EQ(w.of(l7), uint8_t(0));
    CHECK_EQ(w.of(l8), uint8_t(0));
    CHECK_EQ(w.of(other_wikidata), uint8_t(18));
    CHECK_EQ(w.of(other_ring), uint8_t(18));
    CHECK_EQ(stats.duplicates, size_t(2));
}

TEST(a_duplicate_boundary_that_links_a_place_node_takes_the_nodes_rank) {
    // Its parent level stays 3, so the linked town's 16 is above it.
    World w("PL");
    w.relation(1, 4, Box{50, 21, 52, 24});
    const Box town{51.0, 22.0, 51.1, 22.1};
    w.relation(2, 6, town, {AdminPlaceType::NONE, AdminPlaceType::NONE, "Q5"});
    const uint32_t l7 = w.relation(3, 7, town, {AdminPlaceType::NONE, AdminPlaceType::TOWN, "Q5"});
    w.rank();
    CHECK_EQ(w.of(l7), uint8_t(16));
}

TEST(a_child_reads_its_parents_rank_after_the_parents_link) {
    // A level 9 boundary (18) linked to a suburb node takes 20, above its level 8 parent's 16;
    // its level 10 child (20) is indexed later and nests under that 20, at 22.
    World w("FR");
    w.relation(1, 8, Box{48.8, 2.2, 48.9, 2.4});
    const uint32_t l9 = w.relation(2, 9, Box{48.82, 2.25, 48.88, 2.35}, {AdminPlaceType::NONE, AdminPlaceType::SUBURB});
    const uint32_t l10 = w.relation(3, 10, Box{48.83, 2.26, 48.87, 2.34});
    w.rank();
    CHECK_EQ(w.of(l9), uint8_t(20));
    CHECK_EQ(w.of(l10), uint8_t(22));
}

TEST(a_place_area_of_rank_24_lifts_no_boundary_and_an_overlapping_one_none) {
    // Westland, a place=neighbourhood area (24) about the size of Prairie Township (14), lifts
    // nothing: a container's rank must be 23 or less, and it must hold the whole boundary.
    World w("US");
    w.relation(1, 4, Box{38, -85, 42, -80});
    w.relation(2, 6, Box{39.8, -83.3, 40.1, -82.9});
    const uint32_t township = w.relation(3, 7, Box{39.90, -83.20, 39.99, -83.10});
    const uint32_t westland = w.place_area(4, AdminPlaceType::NEIGHBOURHOOD, Box{39.89, -83.21, 40.00, -83.09});
    // Hagåtña's town area overlaps its municipality without holding it.
    const uint32_t municipality = w.relation(5, 8, Box{39.85, -83.0, 39.88, -82.95});
    const uint32_t town = w.place_area(6, AdminPlaceType::TOWN, Box{39.86, -83.01, 39.90, -82.97});
    w.rank();
    CHECK_EQ(w.of(township), uint8_t(14));
    CHECK_EQ(w.of(westland), uint8_t(24));
    CHECK_EQ(w.of(municipality), uint8_t(16));
    CHECK_EQ(w.of(town), uint8_t(16));
}

TEST(a_boundary_inside_a_place_area_of_its_rank_or_more_takes_its_rank_plus_2) {
    // From rank 10 a boundary inside a place polygon of a place rank from its own up to 23 takes
    // that polygon's rank + 2: a level 8 boundary (16) inside a place=city area (16) 18. The area
    // of the highest place rank wins.
    World w("KE");
    w.relation(1, 4, Box{-2, 36, 0, 38});
    const uint32_t nairobi = w.place_area(2, AdminPlaceType::CITY, Box{-1.45, 36.65, -1.15, 37.10});
    const uint32_t suburb = w.place_area(3, AdminPlaceType::SUBURB, Box{-1.40, 36.70, -1.20, 37.00});
    const uint32_t ward = w.relation(4, 8, Box{-1.30, 36.80, -1.25, 36.85});
    const uint32_t big = w.relation(5, 8, Box{-1.42, 36.66, -1.18, 37.05});
    w.rank();
    CHECK_EQ(w.of(nairobi), uint8_t(16));
    // The suburb area (20) inside the city area (16) is not lifted: the city ranks below it.
    CHECK_EQ(w.of(suburb), uint8_t(20));
    CHECK_EQ(w.of(ward), uint8_t(22));
    CHECK_EQ(w.of(big), uint8_t(18));
}

TEST(a_place_area_inside_a_polygon_of_its_rank_or_more_takes_its_rank_plus_2) {
    // A place=village area (16) inside a level 8 relation (16) goes to 18, and so does a closed
    // administrative way with place=village at level 8; a place=village area across the
    // relation's border stays at 16.
    World w("DE");
    w.relation(1, 4, Box{50, 8, 52, 10});
    w.relation(2, 6, Box{50.5, 8.5, 51.5, 9.5});
    const uint32_t gemeinde = w.relation(3, 8, Box{50.8, 8.8, 51.0, 9.0});
    const uint32_t village = w.place_area(4, AdminPlaceType::VILLAGE, Box{50.85, 8.85, 50.90, 8.90});
    const uint32_t way = w.way(5, 8, Box{50.91, 8.91, 50.95, 8.95}, {AdminPlaceType::VILLAGE});
    const uint32_t across = w.place_area(6, AdminPlaceType::VILLAGE, Box{50.95, 8.95, 51.05, 9.05});
    // A place area with the relation's own outline is not inside it (ST_Equals).
    const uint32_t same = w.place_area(8, AdminPlaceType::VILLAGE, Box{50.8, 8.8, 51.0, 9.0});
    w.rank();
    CHECK_EQ(w.of(gemeinde), uint8_t(16));
    CHECK_EQ(w.of(village), uint8_t(18));
    CHECK_EQ(w.of(way), uint8_t(18));
    CHECK_EQ(w.of(across), uint8_t(16));
    CHECK_EQ(w.of(same), uint8_t(16));
}

TEST(a_place_area_takes_the_area_step_and_no_nested_step) {
    // Singapore's place=city area has the country's outline: no polygon of its rank or more holds
    // it, and the level 7 boundary of 24 at its point on surface is no parent of a place area.
    World w("SG");
    const Box island{1.2, 103.6, 1.47, 104.0};
    w.relation(1, 2, island);
    w.relation(2, 5, Box{1.25, 103.75, 1.36, 103.90}, {AdminPlaceType::BOROUGH});
    const uint32_t city = w.place_area(3, AdminPlaceType::CITY, island);
    w.relation(4, 7, Box{1.30, 103.80, 1.34, 103.86}, {AdminPlaceType::NEIGHBOURHOOD});
    w.rank();
    CHECK_EQ(w.of(city), uint8_t(16));
}

TEST(a_link_sets_the_rank_above_the_parents_and_lowers_it_too) {
    // Winsford (a parish, admin_level 10, 20) links its town node: 16, above its unitary
    // authority's 12. A link of a rank not above the parent's leaves the rank alone.
    World w("GB");
    w.relation(1, 4, Box{50, -6, 56, 2});
    w.relation(2, 6, Box{53.0, -3.0, 53.4, -2.4});
    const uint32_t winsford = w.relation(3, 10, Box{53.15, -2.55, 53.22, -2.48}, {AdminPlaceType::NONE, AdminPlaceType::TOWN});
    w.relation(4, 8, Box{53.25, -2.95, 53.38, -2.75});
    const uint32_t under_16 = w.relation(5, 10, Box{53.30, -2.90, 53.35, -2.80}, {AdminPlaceType::NONE, AdminPlaceType::TOWN});
    const RankStats stats = w.rank();
    CHECK_EQ(w.of(winsford), uint8_t(16));
    CHECK_EQ(w.of(under_16), uint8_t(20));
    CHECK_EQ(stats.lowered, size_t(1));
}

TEST(an_own_place_tag_ranks_a_boundary_nothing_links_up_or_down) {
    // Baralla (admin_level 8, 16, own place=municipality, 14) ranks 14, above Lugo's 10 (ES
    // admin_level 6); a level 8 boundary tagged place=suburb (20) takes 20.
    World w("ES");
    w.relation(1, 4, Box{42, -8, 44, -6});
    w.relation(2, 6, Box{42.5, -7.6, 43.5, -6.8});
    const uint32_t baralla = w.relation(3, 8, Box{42.85, -7.30, 42.95, -7.20}, {AdminPlaceType::MUNICIPALITY});
    const uint32_t suburb = w.relation(4, 8, Box{43.00, -7.30, 43.05, -7.20}, {AdminPlaceType::SUBURB});
    const RankStats stats = w.rank();
    CHECK_EQ(w.of(baralla), uint8_t(14));
    CHECK_EQ(w.of(suburb), uint8_t(20));
    CHECK_EQ(stats.own_tags, size_t(2));
    CHECK_EQ(stats.own_lowered, size_t(1));
}

TEST(an_own_place_tag_ranks_by_the_whole_place_table_but_not_below_the_parent) {
    // Town Parks (IE admin_level 10, 20, own place=locality) ranks 25, which no address shows; a
    // Spanish level 8 boundary tagged place=civil_parish (18 in Spain) 18. A place=county level 8
    // boundary (12) under a level 6 one of 12 keeps its 16: the tag's rank is not above the
    // parent's.
    World ie("IE");
    ie.relation(1, 4, Box{51, -11, 56, -5});
    ie.relation(2, 6, Box{53, -7, 54, -6});
    const uint32_t parks = ie.relation(3, 10, Box{53.5, -6.5, 53.6, -6.4}, {AdminPlaceType::NONE, AdminPlaceType::NONE, "", false,
                                                                           PlaceValue::LOCALITY});
    const uint32_t county = ie.relation(4, 8, Box{53.2, -6.9, 53.4, -6.7}, {AdminPlaceType::COUNTY});
    ie.rank();
    CHECK_EQ(ie.of(parks), uint8_t(25));
    CHECK_EQ(ie.of(county), uint8_t(16));

    World es("ES");
    es.relation(1, 4, Box{42, -8, 44, -6});
    const uint32_t parish = es.relation(2, 8, Box{42.85, -7.30, 42.95, -7.20}, {AdminPlaceType::NONE, AdminPlaceType::NONE, "", false,
                                                                               PlaceValue::CIVIL_PARISH});
    es.rank();
    CHECK_EQ(es.of(parish), uint8_t(18));
}

TEST(a_boundary_of_rank_0_in_its_country_stays_out_whatever_its_place_tag_or_link) {
    // Cairns Regional (AU admin_level 6, own place=municipality) and Regierungsbezirk Köln (DE
    // admin_level 5, own place=region) rank 0, as do BR levels 5 to 7. Colchester County (CA
    // admin_level 5, 10) links a place=county node, which ranks 0 in Canada: it keeps 10.
    World au("AU");
    au.relation(1, 4, Box{-20, 140, -10, 150});
    const uint32_t cairns = au.relation(2, 6, Box{-17.5, 145.3, -16.5, 146.0}, {AdminPlaceType::MUNICIPALITY});
    au.rank();
    CHECK_EQ(au.of(cairns), uint8_t(0));

    World de("DE");
    de.relation(1, 4, Box{50, 6, 52, 8});
    const uint32_t koeln = de.relation(2, 5, Box{50.4, 6.4, 51.2, 7.6}, {AdminPlaceType::REGION});
    de.rank();
    CHECK_EQ(de.of(koeln), uint8_t(0));

    World br("BR");
    br.relation(1, 4, Box{-24, -48, -22, -46});
    uint32_t levels[3];
    for (uint8_t level = 5; level <= 7; level++)
        levels[level - 5] = br.relation(level, level, Box{-23.8 + level * 0.01, -47.0, -23.2, -46.3});
    const uint32_t municipality = br.relation(8, 8, Box{-23.7, -46.8, -23.4, -46.4});
    br.rank();
    for (uint32_t pid : levels) CHECK_EQ(br.of(pid), uint8_t(0));
    CHECK_EQ(br.of(municipality), uint8_t(16));

    World ca("CA");
    ca.relation(1, 4, Box{44, -66, 46, -62});
    const uint32_t colchester = ca.relation(2, 5, Box{45.1, -64.0, 45.6, -62.9}, {AdminPlaceType::NONE, AdminPlaceType::COUNTY});
    ca.rank();
    CHECK_EQ(ca.of(colchester), uint8_t(10));
}

TEST(a_country_ranks_its_levels_by_its_own_table_at_one_rank_search) {
    // Czech levels 6 and 7 rank 0 (rank_search 13 and 14), level 8 14 (rank_search 14): a level
    // 8 boundary skips the level 7 and 6 ones round it for its parent, the level 5 one (12).
    World w("CZ");
    w.relation(1, 4, Box{49, 14, 51, 16});
    const uint32_t l5 = w.relation(2, 5, Box{49.5, 14.5, 50.5, 15.5});
    const uint32_t l6 = w.relation(3, 6, Box{49.6, 14.6, 50.4, 15.4});
    const uint32_t l7 = w.relation(4, 7, Box{49.7, 14.7, 50.3, 15.3});
    const uint32_t l8 = w.relation(5, 8, Box{49.8, 14.8, 50.2, 15.2});
    w.rank();
    CHECK_EQ(w.of(l5), uint8_t(12));
    CHECK_EQ(w.of(l6), uint8_t(0));
    CHECK_EQ(w.of(l7), uint8_t(0));
    CHECK_EQ(w.of(l8), uint8_t(14));
}

TEST(a_place_node_in_a_relation_of_its_rank_ranks_2_more_wherever_the_query_is) {
    // A village node (16) in a neighbouring township's level 8 relation (16) ranks 18; a suburb
    // node in a closed-way suburb area ranks 20, as ways promote nothing; a hidden node and a
    // neighbourhood (24) keep their type's rank.
    World w("US");
    w.relation(1, 4, Box{40, -80, 42, -76});
    w.relation(2, 8, Box{40.5, -79.5, 40.6, -79.4});
    const uint32_t village = w.node(PlaceType::VILLAGE, 40.55, -79.45);
    const uint32_t outside = w.node(PlaceType::VILLAGE, 40.65, -79.45);
    w.place_way(3, AdminPlaceType::SUBURB, Box{41.0, -78.0, 41.1, -77.9});
    const uint32_t suburb = w.node(PlaceType::SUBURB, 41.05, -77.95);
    w.relation(4, 10, Box{41.2, -78.0, 41.3, -77.9});
    const uint32_t neighbourhood = w.node(PlaceType::NEIGHBOURHOOD, 41.25, -77.95);
    const uint32_t hamlet = w.node(PlaceType::HAMLET, 41.26, -77.95);
    const RankStats stats = w.rank();
    CHECK_EQ(w.node_rank(village), uint8_t(18));
    CHECK_EQ(w.node_rank(outside), uint8_t(16));
    CHECK_EQ(w.node_rank(suburb), uint8_t(20));
    CHECK_EQ(w.node_rank(neighbourhood), uint8_t(24));
    CHECK_EQ(w.node_rank(hamlet), uint8_t(22));
    CHECK_EQ(stats.nodes_promoted, size_t(2));
}

TEST(a_place_node_is_promoted_on_its_own_point_where_a_relation_covers_it) {
    // A level 8 relation (16) whose east edge lies at 138.9999998°: a village one e7 step inside
    // it, whose f32 longitude rounds to 139°, east of the edge, a village on its north edge and
    // one on the edge of its hole rank 18, as _ST_Covers holds them (placex_triggers.sql:
    // 951-971); one a step beyond its west edge stays 16.
    World w("DE");
    w.relation(1, 4, Box{34, 138, 36, 140});
    const Ring outer = {{352000000, 1389000000}, {352000000, 1389999998}, {352500000, 1389999998},
                        {352500000, 1389000000}, {352000000, 1389000000}};
    const Ring hole = {{352100000, 1389100000}, {352100000, 1389200000}, {352200000, 1389200000},
                       {352200000, 1389100000}, {352100000, 1389100000}};
    w.relation(2, 8, outer, {}, 0, {hole});
    const uint32_t inside = w.node(PlaceType::VILLAGE, 35.22, 138.9999997);
    const uint32_t north = w.node(PlaceType::VILLAGE, 35.25, 138.95);
    const uint32_t on_hole = w.node(PlaceType::VILLAGE, 35.215, 138.91);
    const uint32_t beyond = w.node(PlaceType::VILLAGE, 35.22, 138.8999999);
    w.rank();
    CHECK_EQ(w.node_rank(inside), uint8_t(18));
    CHECK_EQ(w.node_rank(north), uint8_t(18));
    CHECK_EQ(w.node_rank(on_hole), uint8_t(18));
    CHECK_EQ(w.node_rank(beyond), uint8_t(16));
}

TEST(a_place_node_is_not_promoted_by_a_boundary_that_links_it) {
    // A village node linked by its level 9 commune (18, 16 by the link) is that commune in an
    // address, and a place of its own only where the commune is left out (admin-minimal): there
    // it ranks 16, not 18 by the commune it stands for. A village linked by a level 10 boundary
    // (20) is still promoted by the level 8 relation of 16 round it, which links nothing.
    World w("FR");
    w.relation(1, 4, Box{45, 2, 47, 4});
    w.relation(2, 6, Box{45.5, 2.5, 46.5, 3.5});
    const uint32_t commune = w.relation(3, 9, Box{45.6, 2.6, 45.7, 2.7}, {AdminPlaceType::NONE, AdminPlaceType::VILLAGE});
    const uint32_t village = w.node(PlaceType::VILLAGE, 45.65, 2.65, true);
    w.link(village, commune);
    w.relation(4, 8, Box{46.0, 3.0, 46.2, 3.2});
    const uint32_t l10 = w.relation(5, 10, Box{46.05, 3.05, 46.10, 3.10}, {AdminPlaceType::NONE, AdminPlaceType::VILLAGE});
    const uint32_t in_l8 = w.node(PlaceType::VILLAGE, 46.07, 3.07, true);
    w.link(in_l8, l10);
    const RankStats stats = w.rank();
    CHECK_EQ(w.of(commune), uint8_t(16));
    CHECK_EQ(w.node_rank(village), uint8_t(16));
    CHECK_EQ(w.of(l10), uint8_t(20));
    CHECK_EQ(w.node_rank(in_l8), uint8_t(18));
    CHECK_EQ(stats.nodes_promoted, size_t(1));
}

TEST(a_place_tagged_relation_promotes_a_node_by_its_place_rank_and_a_boundary_by_its_rank) {
    // Carrick Hill, a suburb node (20), lies in Town Parks (admin_level 10, 20): 22. A place=city
    // area lifted to 18 by the closed way round it still promotes a city node (16) by its place
    // rank. A boundary=historic place=village area lifted to 22 promotes a quarter node (22) by
    // its rank, which a place area without a boundary tag doesn't.
    World w("GB");
    w.relation(1, 4, Box{54, -8, 55.5, -5});
    w.way(2, 8, Box{54.5, -6.0, 54.7, -5.8});
    w.relation(3, 10, Box{54.59, -5.94, 54.61, -5.92});
    const uint32_t carrick_hill = w.node(PlaceType::SUBURB, 54.60, -5.93);
    const uint32_t lifted = w.place_area(4, AdminPlaceType::CITY, Box{54.55, -5.95, 54.65, -5.85});
    const uint32_t city = w.node(PlaceType::CITY, 54.62, -5.86);
    w.place_area(5, AdminPlaceType::SUBURB, Box{54.0, -7.5, 54.2, -7.3});
    Tags historic;
    historic.boundary_category = true;
    const uint32_t historic_village = w.place_area(6, AdminPlaceType::VILLAGE, Box{54.05, -7.45, 54.10, -7.40}, historic);
    const uint32_t plain_village = w.place_area(7, AdminPlaceType::VILLAGE, Box{54.12, -7.45, 54.17, -7.40});
    const uint32_t in_historic = w.node(PlaceType::QUARTER, 54.07, -7.42);
    const uint32_t in_plain = w.node(PlaceType::QUARTER, 54.14, -7.42);
    w.rank();
    CHECK_EQ(w.node_rank(carrick_hill), uint8_t(22));
    CHECK_EQ(w.of(lifted), uint8_t(18));
    CHECK_EQ(w.node_rank(city), uint8_t(18));
    CHECK_EQ(w.of(historic_village), uint8_t(22));
    CHECK_EQ(w.of(plain_village), uint8_t(22));
    CHECK_EQ(w.node_rank(in_historic), uint8_t(24));
    CHECK_EQ(w.node_rank(in_plain), uint8_t(22));
}

TEST(a_relations_rings_rank_as_one_boundary_and_postal_areas_rank_0) {
    // A level 8 relation of two rings, one in a level 6 relation linked to a borough (18): its
    // widest ring's point on surface is in the other one's parent alone, so both rings take 16,
    // not 20. A postal area ranks 0.
    World w("NO");
    w.relation(1, 2, Box{58, 5, 71, 31});
    w.relation(2, 4, Box{59, 9, 61, 12});
    w.relation(3, 6, Box{59.80, 10.6, 59.95, 10.9}, {AdminPlaceType::NONE, AdminPlaceType::BOROUGH});
    const uint32_t small = w.relation(4, 8, square(Box{59.85, 10.7, 59.90, 10.8}), {}, 0);
    const uint32_t wide = w.relation(4, 8, square(Box{60.2, 10.0, 60.6, 11.5}), {}, 1);
    const uint32_t postal = w.postal(5, Box{59.86, 10.71, 59.89, 10.79});
    w.rank();
    CHECK_EQ(w.of(small), uint8_t(16));
    CHECK_EQ(w.of(wide), uint8_t(16));
    CHECK_EQ(w.of(postal), uint8_t(0));
}

TEST(levels_11_and_12_and_closed_ways_rank_as_boundaries_and_a_closed_level_2_way_in_no_address) {
    // A German level 11 relation inside a level 10 one (20) ranks 22, a level 12 inside it 24, and
    // a Japanese level 11 24. A closed administrative way of level 2 with a country code is no
    // boundary of Nominatim's, whose import drops a way of level 4 or less: it gives the country
    // and ranks 0. A closed way of level 6 is a boundary of its level's 12, which takes no nested
    // step (a relation's) and ranks by its own place tag above level 3 only: a level 6 way with
    // place=city ranks 16 inside the level 10 relation's parent. A postal area ranks 0.
    World w("DE");
    w.relation(1, 4, Box{50, 8, 52, 10});
    w.relation(2, 10, Box{50.5, 8.5, 50.9, 8.9});
    const uint32_t l11 = w.relation(3, 11, Box{50.6, 8.6, 50.8, 8.8});
    const uint32_t l12 = w.relation(4, 12, Box{50.65, 8.65, 50.75, 8.75});
    const uint32_t country_way = w.way(5, 2, Box{49, 7, 53, 11}, {AdminPlaceType::NONE, AdminPlaceType::NONE, "Q183"});
    const uint32_t postal = w.postal(6, Box{50.70, 8.70, 50.72, 8.72});
    const uint32_t county_way = w.way(7, 6, Box{51.2, 8.2, 51.6, 8.6});
    const uint32_t city_way = w.way(8, 6, Box{51.3, 9.2, 51.5, 9.6}, {AdminPlaceType::CITY});
    w.rank();
    CHECK_EQ(w.of(l11), uint8_t(22));
    CHECK_EQ(w.of(l12), uint8_t(24));
    CHECK_EQ(w.of(country_way), uint8_t(0));
    CHECK_EQ(w.of(postal), uint8_t(0));
    CHECK_EQ(w.of(county_way), uint8_t(12));
    CHECK_EQ(w.of(city_way), uint8_t(16));

    World jp("JP");
    jp.relation(1, 4, Box{35, 139, 36, 140});
    const uint32_t jp11 = jp.relation(2, 11, Box{35.6, 139.6, 35.7, 139.7});
    jp.rank();
    CHECK_EQ(jp.of(jp11), uint8_t(24));
}

TEST(the_ranks_dont_hang_on_the_thread_count) {
    World one("DE"), many("DE");
    for (World* w : {&one, &many}) {
        w->relation(1, 4, Box{50, 8, 52, 10});
        for (int k = 0; k < 40; k++) {
            const double s = 50.1 + (k % 8) * 0.2, west = 8.1 + (k / 8) * 0.3;
            w->relation(10 + k, static_cast<uint8_t>(6 + k % 4), Box{s, west, s + 0.15, west + 0.25},
                        {k % 3 ? AdminPlaceType::NONE : AdminPlaceType::SUBURB});
            w->place_area(100 + k, AdminPlaceType::VILLAGE, Box{s + 0.01, west + 0.01, s + 0.05, west + 0.05});
            w->node(PlaceType::HAMLET, s + 0.02, west + 0.02);
        }
    }
    one.rank(1);
    many.rank(8);
    for (uint32_t pid = 0; pid < one.data.admin_polygons.size(); pid++) CHECK_EQ(one.of(pid), many.of(pid));
    for (uint32_t n = 0; n < one.data.place_nodes.size(); n++) CHECK_EQ(one.node_rank(n), many.node_rank(n));
}

TEST(the_trace_tells_how_each_boundary_came_to_its_rank) {
    // Winsford: nested under its unitary authority (12), 20 before its town link, 16 after. A
    // Lublin level 7 relation repeating its parent; a place=village area lifted by a gemeinde.
    World w("GB");
    w.relation(1, 4, Box{50, -6, 56, 2});
    const uint32_t authority = w.relation(2, 6, Box{53.0, -3.0, 53.4, -2.4});
    const uint32_t winsford = w.relation(3, 10, Box{53.15, -2.55, 53.22, -2.48}, {AdminPlaceType::NONE, AdminPlaceType::TOWN});
    const Box city{53.30, -2.90, 53.35, -2.80};
    w.relation(4, 8, city, {AdminPlaceType::NONE, AdminPlaceType::NONE, "Q1"});
    const uint32_t repeat = w.relation(5, 9, city, {AdminPlaceType::NONE, AdminPlaceType::NONE, "Q1"});
    const uint32_t village = w.place_area(6, AdminPlaceType::VILLAGE, Box{53.31, -2.89, 53.32, -2.88});
    std::vector<RankTrace> trace;
    compute_rank_addresses(w.data, w.lookups(), 2, &trace);
    auto of = [&](uint32_t pid) {
        const uint64_t id = admin_boundary_id(w.data.admin_osm_ids[pid]);
        for (const RankTrace& t : trace)
            if (t.id == id) return t;
        return RankTrace{};
    };
    REQUIRE(trace.size() == w.data.admin_polygons.size());
    const RankTrace t = of(winsford);
    CHECK_EQ(t.level, uint8_t(10));
    CHECK_EQ(t.country, w.country);
    CHECK(t.initial == (PlaceRanks{20, 20}));
    CHECK_EQ(t.parent, admin_boundary_id(w.data.admin_osm_ids[authority]));
    CHECK_EQ(t.parent_rank, uint8_t(12));
    CHECK(!t.duplicate);
    CHECK_EQ(t.before_link, uint8_t(20));
    CHECK_EQ(t.step, 'L');
    CHECK_EQ(t.rank, uint8_t(16));
    CHECK(of(repeat).duplicate);
    CHECK_EQ(of(repeat).rank, uint8_t(0));
    CHECK_EQ(of(village).container_rank, uint8_t(16));
    CHECK_EQ(of(village).rank, uint8_t(18));
}

TEST(a_boundarys_place_tag_is_its_extratag_but_an_islands) {
    // Nominatim's import keeps the place tag of a boundary=administrative object in its
    // extratags, which rank, link and label it, but not one of an island or islet, which makes a
    // place row of its own (init.lua process_tags).
    CHECK(boundary_place_value("town") == PlaceValue::TOWN);
    CHECK(boundary_place_value("municipality") == PlaceValue::MUNICIPALITY);
    CHECK(boundary_place_value("department") == PlaceValue::OTHER);
    CHECK(boundary_place_value("island") == PlaceValue::NONE);
    CHECK(boundary_place_value("islet") == PlaceValue::NONE);
    CHECK(boundary_place_value(nullptr) == PlaceValue::NONE);
    CHECK(boundary_place_value("") == PlaceValue::NONE);
}

TEST(an_admin_level_is_the_number_nominatims_import_reads_from_its_tag) {
    // init.lua :306-311: tonumber of the tag, kept where it is a whole number of 1 to 15, else
    // 15, which address-levels.json gives no administrative rank (boundary's [25, 0]).
    CHECK_EQ(nominatim_admin_level("6"), uint8_t(6));
    CHECK_EQ(nominatim_admin_level("4"), uint8_t(4));
    CHECK_EQ(nominatim_admin_level("12"), uint8_t(12));
    CHECK_EQ(nominatim_admin_level("15"), uint8_t(15));
    // Lua's tonumber skips the blanks round a number and reads a whole float as its integer.
    CHECK_EQ(nominatim_admin_level(" 7 "), uint8_t(7));
    CHECK_EQ(nominatim_admin_level("7.0"), uint8_t(7));
    CHECK_EQ(nominatim_admin_level("07"), uint8_t(7));
    // No number, not a whole one, or one out of 1 to 15.
    CHECK_EQ(nominatim_admin_level("5;6"), uint8_t(15));
    CHECK_EQ(nominatim_admin_level("6.5"), uint8_t(15));
    CHECK_EQ(nominatim_admin_level("6 7"), uint8_t(15));
    CHECK_EQ(nominatim_admin_level("261"), uint8_t(15));
    CHECK_EQ(nominatim_admin_level("16"), uint8_t(15));
    CHECK_EQ(nominatim_admin_level("0"), uint8_t(15));
    CHECK_EQ(nominatim_admin_level("-6"), uint8_t(15));
    CHECK_EQ(nominatim_admin_level("six"), uint8_t(15));
    CHECK_EQ(nominatim_admin_level("nan"), uint8_t(15));
    CHECK_EQ(nominatim_admin_level(""), uint8_t(15));
    CHECK_EQ(nominatim_admin_level(nullptr), uint8_t(15));
}

// The cases of #533's is_same pass (take_out_repeated_boundaries there) and of its query-time
// rank steps, on the rank pass.

namespace {

// The closed ring through ring's distinct vertices from its k-th, closed again.
Ring rotated(const Ring& ring, size_t k) {
    Ring out(ring.begin() + static_cast<std::ptrdiff_t>(k), ring.end() - 1);
    out.insert(out.end(), ring.begin(), ring.begin() + static_cast<std::ptrdiff_t>(k));
    out.push_back(out.front());
    return out;
}

Ring reversed(Ring ring) {
    std::reverse(ring.begin(), ring.end());
    return ring;
}

}  // namespace

TEST(the_nested_step_stops_at_25_under_a_parent_of_24) {
    // Upper Ward (admin_level 9, 18) takes its linked neighbourhood's 24, and Lower Ward (10, 20,
    // linked to a neighbourhood too) inside it 25, the most the admin-nested step gives under a
    // parent of 24 or more (placex_triggers.sql:903-904): its node's 24 is no higher than the
    // parent's, so the link leaves it there.
    World w("GB");
    w.relation(1, 4, Box{50, -2, 53, 2});
    const uint32_t upper = w.relation(2, 9, Box{51.0, -0.5, 51.5, 0.5}, {AdminPlaceType::NONE, AdminPlaceType::NEIGHBOURHOOD});
    const uint32_t lower = w.relation(3, 10, Box{51.1, -0.4, 51.4, 0.4}, {AdminPlaceType::NONE, AdminPlaceType::NEIGHBOURHOOD});
    w.rank();
    CHECK_EQ(w.of(upper), uint8_t(24));
    CHECK_EQ(w.of(lower), uint8_t(25));
}

TEST(a_duplicate_boundary_takes_its_nodes_rank_under_a_parent_of_24_and_none_of_a_node_of_rank_0) {
    // is_same takes a boundary that repeats its parent out of addresses and leaves
    // parent_address_level at 3, so the city node it links gives it 16 back (:1161) under a parent
    // of 24, where it would rank 25, and a boundary inside it ranks under 16. One linking a
    // place=state node, of no address rank as a node, stays out.
    World w("GB");
    w.relation(1, 4, Box{50, -2, 53, 2});
    const Box ward{51.0, -0.5, 51.5, 0.5};
    const uint32_t l8 = w.relation(2, 8, ward, {AdminPlaceType::NONE, AdminPlaceType::NEIGHBOURHOOD, "Q5"});
    const uint32_t l10 = w.relation(3, 10, ward, {AdminPlaceType::NONE, AdminPlaceType::CITY, "Q5"});
    const uint32_t l11 = w.relation(4, 11, Box{51.1, -0.4, 51.4, 0.4});
    w.rank();
    CHECK_EQ(w.of(l8), uint8_t(24));
    CHECK_EQ(w.of(l10), uint8_t(16));
    CHECK_EQ(w.of(l11), uint8_t(22));

    World v("GB");
    v.relation(1, 4, Box{50, -2, 53, 2});
    v.relation(2, 8, ward, {AdminPlaceType::NONE, AdminPlaceType::NONE, "Q6"});
    const uint32_t state = v.relation(3, 10, ward, {AdminPlaceType::NONE, AdminPlaceType::STATE, "Q6"});
    v.rank();
    CHECK_EQ(v.of(state), uint8_t(0));
}

TEST(a_boundary_relation_repeating_its_closest_parent_relation_ranks_0) {
    // placex_update (placex_triggers.sql:909-931): a boundary=administrative relation of a rank
    // above 0 whose closest boundary relation round it, of a lower admin level above 3 and a rank
    // between 1 and 25, has its wikidata and geometry ranks 0. In the Netherlands, where
    // admin_level 9 ranks 0: a woonplaats (10) along its gemeente's (8) outline, written from
    // another vertex and the other way round, of its wikidata, ranks 0; one that links a place
    // node repeats it too but takes the node's rank back (:1161); one of another wikidata does
    // not, nor one round another outline; one under a level 9 boundary of the same outline and
    // wikidata does, by the gemeente past it; one under a closed way of its outline does not, the
    // province past the way having no wikidata of its; and a province along the country's
    // outline, of its wikidata, does not, the country's level being under 4.
    World w("NL");
    using T = Tags;
    const auto none = AdminPlaceType::NONE;
    w.relation(2, 2, Box{0, 0, 1, 1}, T{none, none, "Q8"});
    w.relation(4, 4, Box{0.1, 0.1, 0.9, 0.9});
    const Ring sq = square(Box{0.2, 0.2, 0.3, 0.3});
    w.relation(8, 8, sq, T{none, none, "Q1"});
    const uint32_t repeats = w.relation(10, 10, reversed(rotated(sq, 2)), T{none, none, "Q1"});
    w.relation(18, 8, Box{0.2, 0.4, 0.3, 0.5}, T{none, AdminPlaceType::CITY, "Q2"});
    const uint32_t takes_back = w.relation(20, 10, Box{0.2, 0.4, 0.3, 0.5}, T{none, AdminPlaceType::TOWN, "Q2"});
    w.relation(28, 8, Box{0.2, 0.6, 0.3, 0.7}, T{none, none, "Q3"});
    const uint32_t other_wikidata = w.relation(30, 10, Box{0.2, 0.6, 0.3, 0.7}, T{none, none, "Q4"});
    w.relation(38, 8, Box{0.4, 0.2, 0.5, 0.3}, T{none, none, "Q5"});
    const uint32_t other_outline = w.relation(40, 10, Box{0.41, 0.21, 0.49, 0.29}, T{none, none, "Q5"});
    w.relation(48, 8, Box{0.4, 0.4, 0.5, 0.5}, T{none, none, "Q6"});
    w.relation(49, 9, Box{0.4, 0.4, 0.5, 0.5}, T{none, none, "Q6"});
    const uint32_t past_level_9 = w.relation(50, 10, Box{0.4, 0.4, 0.5, 0.5}, T{none, none, "Q6"});
    w.way(58, 8, Box{0.4, 0.6, 0.5, 0.7});
    const uint32_t under_way = w.relation(60, 10, Box{0.4, 0.6, 0.5, 0.7}, T{none, none, "Q7"});
    const uint32_t province = w.relation(64, 4, Box{0, 0, 1, 1}, T{none, none, "Q8"});
    const RankStats stats = w.rank();
    CHECK_EQ(w.of(repeats), uint8_t(0));
    CHECK_EQ(w.of(takes_back), uint8_t(16));
    CHECK_EQ(w.of(other_wikidata), uint8_t(16));
    CHECK_EQ(w.of(other_outline), uint8_t(16));
    CHECK_EQ(w.of(past_level_9), uint8_t(0));
    CHECK_EQ(w.of(under_way), uint8_t(16));
    CHECK_EQ(w.of(province), uint8_t(8));
    CHECK_EQ(stats.duplicates, size_t(3));
}

TEST(a_boundary_repeats_the_closest_relation_round_its_own_point) {
    // placex_update compares a boundary with the boundary relation of the highest admin level
    // round its centroid, of a lower level above 3 and a rank of 1 to 25 (placex_triggers.sql:
    // 916-931). A level 10 boundary along a level 8 one, of its wikidata, lies under a level 9
    // closed way, and under a level 9 relation of rank 0 in the Netherlands, each holding its
    // point but reaching far east, their own points outside the pair: both repeat their level 8
    // parent, found round their own point.
    World w("NL");
    const auto none = AdminPlaceType::NONE;
    w.relation(4, 4, Box{0.1, 0.1, 0.9, 0.9});
    w.relation(8, 8, Box{0.2, 0.2, 0.3, 0.3}, Tags{none, none, "Q1"});
    const uint32_t under_way = w.relation(10, 10, rotated(square(Box{0.2, 0.2, 0.3, 0.3}), 1), Tags{none, none, "Q1"});
    w.way(1, 9, Box{0.24, 0.24, 0.26, 0.8});
    w.relation(18, 8, Box{0.5, 0.2, 0.6, 0.3}, Tags{none, none, "Q2"});
    const uint32_t under_rank0 = w.relation(20, 10, Box{0.5, 0.2, 0.6, 0.3}, Tags{none, none, "Q2"});
    w.relation(19, 9, Box{0.54, 0.24, 0.56, 0.8});
    w.rank();
    CHECK_EQ(w.of(under_way), uint8_t(0));
    CHECK_EQ(w.of(under_rank0), uint8_t(0));
}

TEST(a_boundary_repeats_a_parent_whose_outline_has_more_vertices_along_its_edges) {
    // ST_Equals compares the outlines: a vertex along a straight edge, or two in a row, adds no
    // point. A level 10 boundary with one on its south edge and two on its east edge, and a hole
    // with one more, repeats its level 8 parent; one with a vertex an e7 step off the edge
    // does not.
    World w("PL");
    const auto none = AdminPlaceType::NONE;
    w.relation(4, 4, Box{0.1, 0.1, 0.9, 0.9});
    const Ring hole = reversed(square(Box{0.24, 0.24, 0.26, 0.26}));
    w.relation(8, 8, square(Box{0.2, 0.2, 0.3, 0.3}), Tags{none, none, "Q1"}, 0, {hole});
    const Ring more = ring_of({{0.2, 0.2}, {0.2, 0.25}, {0.2, 0.3}, {0.24, 0.3}, {0.27, 0.3}, {0.3, 0.3}, {0.3, 0.2}});
    const Ring hole_more = ring_of({{0.24, 0.24}, {0.26, 0.24}, {0.26, 0.26}, {0.25, 0.26}, {0.24, 0.26}});
    const uint32_t repeats = w.relation(10, 10, more, Tags{none, none, "Q1"}, 0, {hole_more});
    w.relation(28, 8, Box{0.5, 0.2, 0.6, 0.3}, Tags{none, none, "Q2"});
    Ring off = ring_of({{0.5, 0.2}, {0.5, 0.25}, {0.5, 0.3}, {0.6, 0.3}, {0.6, 0.2}});
    off[1].lat_e7 += 1;
    const uint32_t differs = w.relation(30, 10, off, Tags{none, none, "Q2"});
    w.rank();
    CHECK_EQ(w.of(repeats), uint8_t(0));
    CHECK_EQ(w.of(differs), uint8_t(20));
}

TEST(every_ring_of_a_boundary_that_repeats_its_parent_ranks_0_and_only_of_equal_rings) {
    // ST_Equals: a relation of two outer rings, one with a hole, repeats one of the same rings in
    // another order, and every ring of it ranks 0; one whose hole differs does not, nor one
    // under it that lacks its other ring.
    World w("PL");
    const auto none = AdminPlaceType::NONE;
    w.relation(1, 4, Box{0.1, 0.1, 0.9, 0.9});
    const Ring west = square(Box{0.2, 0.2, 0.4, 0.4}), east = square(Box{0.2, 0.5, 0.4, 0.7});
    const Ring hole = reversed(square(Box{0.25, 0.55, 0.3, 0.6})), other_hole = reversed(square(Box{0.25, 0.55, 0.31, 0.6}));
    const Tags q{none, none, "Q270"};
    const uint32_t parent_west = w.relation(6, 6, west, q, 0), parent_east = w.relation(6, 6, east, q, 1, {hole});
    const uint32_t same_east = w.relation(7, 7, rotated(east, 1), q, 0, {hole}), same_west = w.relation(7, 7, west, q, 1);
    const uint32_t holed_west = w.relation(8, 8, west, q, 0), holed_east = w.relation(8, 8, east, q, 1, {other_hole});
    const uint32_t lacking = w.relation(9, 9, east, q, 0, {other_hole});
    w.rank();
    CHECK_EQ(w.of(same_east), uint8_t(0));
    CHECK_EQ(w.of(same_west), uint8_t(0));
    for (uint32_t pid : {parent_west, parent_east, holed_west, holed_east, lacking}) CHECK(w.of(pid) != 0);
}
