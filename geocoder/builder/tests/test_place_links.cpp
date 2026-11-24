// place_links.h: the place node each boundary and place area links, as
// Nominatim's find_linked_place finds them in the order its indexer runs.
// Each case is one of its rules, laid out as the rings of the objects it was
// found on.
#include "place_links.h"

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

struct Box {
    double s, w, n, e;
};

std::vector<PackedLocation> square(Box b) {
    std::vector<PackedLocation> ring{pack_location(b.s, b.w), pack_location(b.s, b.e), pack_location(b.n, b.e),
                                     pack_location(b.n, b.w)};
    ring.push_back(ring.front());
    return ring;
}

uint8_t value(PlaceValue v) { return static_cast<uint8_t>(v); }

// What a boundary or place area links by.
struct Tags {
    PlaceValue own = PlaceValue::NONE;
    std::string name;
    std::string wikidata;
    int64_t label = -1;
};

// Boundaries, place areas and place nodes, every point in `country`.
struct World {
    ParsedData data;
    uint16_t country = 0;

    explicit World(const char* cc) : country(pack_country_code(cc[0], cc[1])) {}

    uint32_t add(uint64_t osm_id, uint8_t level, Box box, const Tags& tags) {
        const uint32_t pid = static_cast<uint32_t>(data.admin_polygons.size());
        data.admin_exact_rings.append(square(box), {});
        AdminPolygon p{};
        p.name_id = pid;
        p.admin_level = level;
        p.own_place_value = value(tags.own);
        p.area = data.admin_exact_rings.area(pid);
        data.admin_polygons.push_back(p);
        data.admin_osm_ids.push_back(osm_id);
        AdminTags t;
        t.boundary = admin_boundary_id(osm_id);
        t.label = tags.label;
        t.name = lowercase_utf8(tags.name);
        t.wikidata = tags.wikidata;
        data.admin_tags.push_back(t);
        return pid;
    }
    uint32_t relation(int64_t id, uint8_t level, Box box, const Tags& tags = {}) {
        return add(admin_ring_id(id, 0), level, box, tags);
    }
    uint32_t way(int64_t id, uint8_t level, Box box, const Tags& tags = {}) {
        return add(way_boundary_id(id), level, box, tags);
    }
    uint32_t place_area(int64_t id, PlaceValue type, Box box, Tags tags = {}) {
        tags.own = type;
        return add(admin_ring_id(id, 0), 15, box, tags);
    }

    // A place node of osm id `id`; a PlaceNode too where it is a settlement.
    void node(int64_t id, PlaceValue type, double lat, double lng, const std::string& name,
              const std::string& wikidata = "") {
        data.link_candidates.add(id, point_e7(lat, lng), value(type), lowercase_utf8(name), wikidata);
        PlaceType settlement = PlaceType::UNKNOWN;
        switch (type) {
            case PlaceValue::CITY: settlement = PlaceType::CITY; break;
            case PlaceValue::TOWN: settlement = PlaceType::TOWN; break;
            case PlaceValue::VILLAGE: settlement = PlaceType::VILLAGE; break;
            case PlaceValue::SUBURB: settlement = PlaceType::SUBURB; break;
            case PlaceValue::HAMLET: settlement = PlaceType::HAMLET; break;
            default: return;
        }
        PlaceNode pn{};
        pn.lat = static_cast<float>(lat);
        pn.lng = static_cast<float>(lng);
        pn.place_type = static_cast<uint8_t>(settlement);
        pn.parent_poly_id = NO_DATA;
        data.place_nodes.push_back(pn);
        data.place_osm_ids.push_back(pack_osm_id(ObjectType::OSM_NODE, id));
    }

    LinkStats link(unsigned threads = 4) {
        RankLookups lookups;
        lookups.country_at = [this](PackedLocation) { return country; };
        return find_linked_places(data, lookups, threads);
    }

    // The link of the polygon's boundary, step NONE for none.
    AdminLink link_of(uint32_t pid) const {
        const uint64_t id = admin_boundary_id(data.admin_osm_ids[pid]);
        for (const AdminLink& l : data.admin_links)
            if (l.boundary == id) return l;
        return {};
    }
    // The boundary that claims node `id`, 0 for none.
    uint64_t claimant(int64_t id) const {
        for (const PlaceClaim& c : data.place_claims)
            if (c.node == pack_osm_id(ObjectType::OSM_NODE, id)) return c.boundary;
        return 0;
    }
    uint64_t boundary(uint32_t pid) const { return admin_boundary_id(data.admin_osm_ids[pid]); }
    AdminPlaceType label(uint32_t pid) const { return static_cast<AdminPlaceType>(data.admin_polygons[pid].place_type_override); }
};

}  // namespace

TEST(lowercase_utf8_lowers_each_letter_as_postgres_lower_does) {
    CHECK_EQ(lowercase_utf8("Ferraz de Vasconcelos"), std::string("ferraz de vasconcelos"));
    CHECK_EQ(lowercase_utf8("ÉCIJA Ñuñoa Øster ŁÓDŹ"), std::string("écija ñuñoa øster łódź"));
    CHECK_EQ(lowercase_utf8("ΑΘΉΝΑ Москва ԵՐԵՎԱՆ"), std::string("αθήνα москва երեվան"));
    CHECK_EQ(lowercase_utf8("İstanbul ẞ Ｔｏｋｙｏ 東京"), std::string("istanbul ß ｔｏｋｙｏ 東京"));
    // A byte that is no UTF-8 stays as it is.
    CHECK_EQ(lowercase_utf8(std::string("A\xff" "B")), std::string("a\xff" "b"));
}

TEST(a_label_member_links_whatever_its_value_name_or_link_and_a_boundary_of_rank_0_links_none) {
    // Town Parks' label member, a place=locality node, links it (the node ranks 25); an unnamed
    // village node is a label member too. Gold Coast City (AU admin_level 6, rank 0) links its
    // label member nothing (:285).
    World w("IE");
    w.node(1, PlaceValue::LOCALITY, 53.5, -6.5, "Town Parks");
    const uint32_t parks = w.relation(10, 10, Box{53.4, -6.6, 53.6, -6.4}, {PlaceValue::NONE, "Town Parks", "", 1});
    w.node(2, PlaceValue::VILLAGE, 54.0, -7.0, "");
    const uint32_t parish = w.relation(11, 10, Box{53.9, -7.1, 54.1, -6.9}, {PlaceValue::NONE, "Kilmore", "", 2});
    w.link();
    CHECK(w.link_of(parks).step == LinkStep::LABEL);
    CHECK_EQ(w.link_of(parks).rank, uint8_t(25));
    CHECK(w.label(parks) == AdminPlaceType::NONE);
    CHECK(w.link_of(parish).step == LinkStep::LABEL);
    CHECK(w.label(parish) == AdminPlaceType::VILLAGE);

    World au("AU");
    au.node(1, PlaceValue::CITY, -28.0, 153.43, "Gold Coast");
    const uint32_t gold_coast = au.relation(1, 6, Box{-28.2, 153.2, -27.7, 153.6}, {PlaceValue::NONE, "Gold Coast City", "", 1});
    au.link();
    CHECK(au.link_of(gold_coast).step == LinkStep::NONE);
    CHECK_EQ(au.claimant(1), uint64_t(0));
}

TEST(a_label_member_is_left_to_its_relation_by_every_other_step) {
    // Den Haag (R192736, NL admin_level 8, 14) shares its wikidata (Q36600) with the city node, the
    // label member of the level 10 relation of its outline (R192737), a later runner. On a fresh
    // import the level 8 one would link the node first and the level 10 one take it over by its
    // label (:291-312, which reads no link); the public server, which indexes each polygon again
    // after its import, finds the node linked then. So the level 8 one links nothing, by its
    // wikidata or by name, and keeps its 14; so does Grad Sarajevo (R2567380, BA level 6),
    // whose wikidata node is Sarajevo's label member (level 9). A label member of a relation of
    // rank 0, which links nothing, is any step's.
    World w("NL");
    w.node(1, PlaceValue::CITY, 52.08, 4.31, "Den Haag", "Q36600");
    const Box city{52.0, 4.2, 52.15, 4.4};
    const uint32_t gemeente = w.relation(192736, 8, city, {PlaceValue::NONE, "Den Haag", "Q36600"});
    const uint32_t woonplaats = w.relation(192737, 10, city, {PlaceValue::NONE, "Den Haag", "Q36600", 1});
    w.node(2, PlaceValue::TOWN, 52.5, 4.6, "Velsen", "Q9");
    const uint32_t velsen = w.relation(3, 8, Box{52.4, 4.5, 52.6, 4.7}, {PlaceValue::NONE, "Velsen", "Q9"});
    w.relation(4, 9, Box{52.45, 4.55, 52.55, 4.65}, {PlaceValue::NONE, "Velsen-Zuid", "", 2});
    const LinkStats stats = w.link();
    CHECK(w.link_of(gemeente).step == LinkStep::NONE);
    CHECK(w.link_of(woonplaats).step == LinkStep::LABEL);
    CHECK_EQ(w.claimant(1), w.boundary(woonplaats));
    // NL admin_level 9 ranks 0: Velsen-Zuid links nothing, and Velsen links the town node.
    CHECK(w.link_of(velsen).step == LinkStep::WIKIDATA);
    CHECK_EQ(stats.taken, size_t(0));
}

TEST(a_wikidata_link_takes_one_covered_unlinked_node_its_namesake_first) {
    // Of the two nodes of Springfield's wikidata inside it, the one named as it is links; the
    // other stays a place of its own, and one outside links nothing. A node an earlier runner's
    // boundary links is taken: Lublin's level 6 links the city node, and its level 8 of the same
    // wikidata finds none.
    World w("US");
    w.node(1, PlaceValue::SUBURB, 40.01, -80.01, "Springfield Centre", "Q5");
    w.node(2, PlaceValue::TOWN, 40.02, -80.02, "Springfield", "Q5");
    w.node(3, PlaceValue::TOWN, 41.5, -80.0, "Springfield", "Q6");
    const uint32_t springfield = w.relation(1, 8, Box{39.9, -80.1, 40.1, -79.9}, {PlaceValue::NONE, "Springfield", "Q5"});
    const uint32_t other = w.relation(2, 8, Box{40.5, -80.1, 40.7, -79.9}, {PlaceValue::NONE, "Elsewhere", "Q6"});
    World pl("PL");
    pl.node(9, PlaceValue::CITY, 51.25, 22.57, "Lublin", "Q37333");
    const uint32_t l6 = pl.relation(2, 6, Box{51.14, 22.45, 51.30, 22.67}, {PlaceValue::NONE, "Lublin", "Q37333"});
    const uint32_t l8 = pl.relation(4, 8, Box{51.14, 22.45, 51.30, 22.67}, {PlaceValue::NONE, "Lublin", "Q37333"});
    w.link();
    pl.link();
    CHECK(w.link_of(springfield).step == LinkStep::WIKIDATA);
    CHECK_EQ(w.link_of(springfield).node, int64_t(2));
    CHECK_EQ(w.claimant(1), uint64_t(0));
    CHECK(w.link_of(other).step == LinkStep::NONE);
    CHECK(pl.link_of(l6).step == LinkStep::WIKIDATA);
    CHECK(pl.link_of(l8).step == LinkStep::NONE);
    CHECK_EQ(pl.claimant(9), pl.boundary(l6));
}

TEST(a_wikidata_link_takes_an_unnamed_node_before_its_namesake) {
    // Nominatim orders the wikidata's nodes by lower(name) = the boundary's name, descending: an
    // unnamed node's comparison is null, which comes first.
    World w("DE");
    w.node(1, PlaceValue::TOWN, 50.01, 8.01, "Bergheim", "Q7");
    w.node(5, PlaceValue::HAMLET, 50.02, 8.02, "", "Q7");
    const uint32_t b = w.relation(1, 8, Box{49.9, 7.9, 50.1, 8.1}, {PlaceValue::NONE, "Bergheim", "Q7"});
    w.link();
    CHECK_EQ(w.link_of(b).node, int64_t(5));
}

TEST(a_place_tagged_boundary_links_a_node_of_its_value_whose_name_overlaps_its_own) {
    // Bezirk Kreuzberg (place=suburb) links the suburb node Kreuzberg, whose name lies in its own;
    // not a town node of the name, nor a suburb node of another wikidata, nor one outside it.
    World w("DE");
    w.node(1, PlaceValue::TOWN, 52.50, 13.40, "Kreuzberg");
    w.node(2, PlaceValue::SUBURB, 52.51, 13.41, "Kreuzberg", "Q99");
    w.node(3, PlaceValue::SUBURB, 52.52, 13.42, "Kreuzberg");
    w.node(4, PlaceValue::SUBURB, 52.70, 13.42, "Kreuzberg");
    const uint32_t bezirk =
        w.relation(1, 9, Box{52.45, 13.35, 52.55, 13.45}, {PlaceValue::SUBURB, "Bezirk Kreuzberg", "Q1"});
    w.link();
    CHECK(w.link_of(bezirk).step == LinkStep::TYPE);
    CHECK_EQ(w.link_of(bezirk).node, int64_t(3));
    CHECK(w.label(bezirk) == AdminPlaceType::SUBURB);
}

TEST(a_name_link_takes_a_covered_node_of_the_boundarys_name_and_rank) {
    // Ferraz de Vasconcelos (BR admin_level 8, 16, own place=municipality) links its city node by
    // name (16); a namesake village outside it links nothing, nor a town of its name in a level 9
    // boundary (18), whose rank is not the node's 16, nor one of another wikidata.
    World w("BR");
    w.node(1, PlaceValue::CITY, -23.54, -46.37, "Ferraz de Vasconcelos");
    const uint32_t ferraz = w.relation(1, 8, Box{-23.6, -46.4, -23.5, -46.3},
                                       {PlaceValue::MUNICIPALITY, "Ferraz de Vasconcelos"});
    w.node(2, PlaceValue::VILLAGE, -23.0, -46.0, "Northfield");
    const uint32_t northfield = w.relation(2, 8, Box{-22.8, -46.1, -22.7, -45.9}, {PlaceValue::NONE, "Northfield"});
    w.node(3, PlaceValue::TOWN, -22.0, -45.0, "Ninth");
    const uint32_t ninth = w.relation(3, 9, Box{-22.1, -45.1, -21.9, -44.9}, {PlaceValue::NONE, "Ninth"});
    w.node(4, PlaceValue::TOWN, -21.0, -44.0, "Tenth", "Q4");
    const uint32_t tenth = w.relation(4, 8, Box{-21.1, -44.1, -20.9, -43.9}, {PlaceValue::NONE, "Tenth", "Q40"});
    w.link();
    CHECK(w.link_of(ferraz).step == LinkStep::NAME);
    CHECK_EQ(w.link_of(ferraz).rank, uint8_t(16));
    CHECK(w.label(ferraz) == AdminPlaceType::CITY);
    CHECK(w.link_of(northfield).step == LinkStep::NONE);
    CHECK(w.label(northfield) == AdminPlaceType::NONE);
    CHECK(w.link_of(ninth).step == LinkStep::NONE);
    CHECK(w.link_of(tenth).step == LinkStep::NONE);
    CHECK_EQ(w.claimant(1), w.boundary(ferraz));
}

TEST(boundaries_of_one_runner_link_one_node_each_taking_its_rank_the_last_keeping_it) {
    // Two level 8 boundaries (one rank_search) of one wikidata round its town node both find it
    // free when their runner starts: both link it, and the later keeps it. A level 9 boundary of
    // the wikidata, a later runner, finds it taken.
    World w("FR");
    w.node(1, PlaceValue::TOWN, 45.05, 2.05, "Saint-Flour", "Q8");
    const uint32_t a = w.relation(5, 8, Box{45.0, 2.0, 45.1, 2.1}, {PlaceValue::NONE, "A", "Q8"});
    const uint32_t b = w.relation(6, 8, Box{44.9, 1.9, 45.2, 2.2}, {PlaceValue::NONE, "B", "Q8"});
    const uint32_t c = w.relation(7, 9, Box{45.04, 2.04, 45.06, 2.06}, {PlaceValue::NONE, "C", "Q8"});
    w.link();
    CHECK(w.link_of(a).step == LinkStep::WIKIDATA);
    CHECK(w.link_of(b).step == LinkStep::WIKIDATA);
    CHECK(w.link_of(c).step == LinkStep::NONE);
    CHECK_EQ(w.claimant(1), w.boundary(b));
    // Every boundary linking a place node is listed with it.
    const PlaceNodeLinks expected{{0, w.boundary(a)}, {0, w.boundary(b)}};
    CHECK(w.data.place_node_links == expected);
}

TEST(place_areas_link_after_the_boundaries_by_their_rank) {
    // A place=suburb area (20) and a level 10 boundary (rank_search 20) both look for the suburb
    // node of their wikidata: the boundaries' runners come first, so the boundary links it and
    // the area finds it taken. A place=village area links its village node by its own place
    // value, a closed way its hamlet node by its wikidata.
    World w("GB");
    w.node(1, PlaceValue::SUBURB, 51.5, -0.1, "Southwark", "Q2");
    const uint32_t area = w.place_area(1, PlaceValue::SUBURB, Box{51.4, -0.2, 51.6, 0.0}, {PlaceValue::NONE, "Southwark", "Q2"});
    const uint32_t parish = w.relation(2, 10, Box{51.45, -0.15, 51.55, -0.05}, {PlaceValue::NONE, "Southwark Parish", "Q2"});
    w.node(2, PlaceValue::VILLAGE, 52.0, -1.0, "Ufton");
    const uint32_t village = w.place_area(3, PlaceValue::VILLAGE, Box{51.9, -1.1, 52.1, -0.9}, {PlaceValue::NONE, "Ufton"});
    w.node(3, PlaceValue::HAMLET, 53.0, -2.0, "Wardle", "Q3");
    const uint32_t way = w.way(4, 10, Box{52.9, -2.1, 53.1, -1.9}, {PlaceValue::NONE, "Wardle", "Q3"});
    w.link();
    CHECK(w.link_of(parish).step == LinkStep::WIKIDATA);
    CHECK(w.link_of(area).step == LinkStep::NONE);
    CHECK(w.label(area) == AdminPlaceType::SUBURB);
    CHECK(w.link_of(village).step == LinkStep::TYPE);
    CHECK(w.link_of(way).step == LinkStep::WIKIDATA);
    CHECK(w.label(way) == AdminPlaceType::HAMLET);
}

TEST(the_links_dont_hang_on_the_thread_count) {
    World one("DE"), many("DE");
    for (World* w : {&one, &many}) {
        for (int k = 0; k < 40; k++) {
            const double s = 50.1 + (k % 8) * 0.2, west = 8.1 + (k / 8) * 0.3;
            const std::string name = "Ort " + std::to_string(k % 5);
            w->node(100 + k, k % 2 ? PlaceValue::VILLAGE : PlaceValue::SUBURB, s + 0.02, west + 0.02, name,
                    k % 3 ? "" : "Q" + std::to_string(k % 4));
            w->relation(10 + k, static_cast<uint8_t>(6 + k % 4), Box{s, west, s + 0.15, west + 0.25},
                        {k % 3 ? PlaceValue::NONE : PlaceValue::SUBURB, name, k % 2 ? "" : "Q" + std::to_string(k % 4)});
        }
    }
    one.link(1);
    many.link(8);
    REQUIRE(one.data.admin_links.size() == many.data.admin_links.size());
    for (size_t k = 0; k < one.data.admin_links.size(); k++) {
        CHECK_EQ(one.data.admin_links[k].boundary, many.data.admin_links[k].boundary);
        CHECK_EQ(one.data.admin_links[k].node, many.data.admin_links[k].node);
    }
    CHECK(one.data.place_node_links == many.data.place_node_links);
    for (uint32_t p = 0; p < one.data.admin_polygons.size(); p++) CHECK_EQ(one.label(p), many.label(p));
    CHECK(!one.data.admin_links.empty());
}
