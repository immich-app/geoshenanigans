// place_links.h: the place node each boundary and place area links, as
// Nominatim's find_linked_place finds them in the order its indexer runs.
// Each case is one of its rules, laid out as the rings of the objects it was
// found on.
#include "place_links.h"

#include <cstdint>
#include <initializer_list>
#include <stdexcept>
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
    bool name_en = false;  // a name:en of its own names it
    std::vector<int64_t> more_labels;  // its label members after `label`
};

// Boundaries, place areas and place nodes, every point in `country`.
struct World {
    ParsedData data;
    uint16_t country = 0;

    // A polygon's name_id is its own index, below the offset of any name the pass interns.
    explicit World(const char* cc) : country(pack_country_code(cc[0], cc[1])) { data.string_pool.intern(std::string(4096, '-')); }

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
        if (tags.label >= 0) t.labels.push_back(tags.label);
        t.labels.insert(t.labels.end(), tags.more_labels.begin(), tags.more_labels.end());
        t.name = lowercase_utf8(tags.name);
        t.wikidata = tags.wikidata;
        t.name_en = tags.name_en;
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
              const std::string& wikidata = "", const std::string& name_en = "") {
        data.link_candidates.add(id, point_e7(lat, lng), value(type), lowercase_utf8(name), wikidata, name_en, name);
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
    // The polygon's name where the pass named it (an id of the string pool), else empty.
    std::string name(uint32_t pid) const {
        const uint32_t id = data.admin_polygons[pid].name_id;
        if (id == pid) return "";
        return std::string(data.string_pool.data().data() + id);
    }
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

TEST(link_text_takes_no_string_whose_offset_or_end_passes_32_bits) {
    // Offsets are 32 bits and kNone stands for none: a string that would end past them throws
    // rather than wrap onto an earlier one.
    bool threw = false;
    try {
        (void)LinkText::offset_at(size_t(LinkText::kNone) - 3, 3);
    } catch (const std::length_error&) {
        threw = true;
    }
    CHECK(threw);
    CHECK_EQ(LinkText::offset_at(size_t(LinkText::kNone) - 5, 3), LinkText::kNone - 5);
}

TEST(lowercase_utf8_keeps_the_bytes_of_a_sequence_that_is_no_utf_8) {
    // An overlong A, of two bytes and of three, a surrogate and a code point past U+10FFFF decode
    // to no scalar UTF-8 allows: each byte stays as it is, and the letters round them lower.
    for (const std::string bad : {std::string("\xc1\x81"), std::string("\xe0\x81\x81"), std::string("\xed\xa0\x80"),
                                  std::string("\xf4\x90\x80\x80")})
        CHECK_EQ(lowercase_utf8("X" + bad + "Y"), "x" + bad + "y");
}

TEST(a_label_member_links_whatever_its_value_name_or_link_and_a_boundary_of_rank_0_links_none) {
    // Town Parks' label member, a place=locality node, links it (the node ranks 25) and labels it
    // a locality; an unnamed village node is a label member too. Gold Coast City (AU admin_level
    // 6, rank 0) links its label member nothing (:285).
    World w("IE");
    w.node(1, PlaceValue::LOCALITY, 53.5, -6.5, "Town Parks");
    const uint32_t parks = w.relation(10, 10, Box{53.4, -6.6, 53.6, -6.4}, {PlaceValue::NONE, "Town Parks", "", 1});
    w.node(2, PlaceValue::VILLAGE, 54.0, -7.0, "");
    const uint32_t parish = w.relation(11, 10, Box{53.9, -7.1, 54.1, -6.9}, {PlaceValue::NONE, "Kilmore", "", 2});
    w.link();
    CHECK(w.link_of(parks).step == LinkStep::LABEL);
    CHECK_EQ(w.link_of(parks).rank, uint8_t(25));
    CHECK(w.label(parks) == AdminPlaceType::LOCALITY);
    CHECK(w.link_of(parish).step == LinkStep::LABEL);
    CHECK(w.label(parish) == AdminPlaceType::VILLAGE);

    World au("AU");
    au.node(1, PlaceValue::CITY, -28.0, 153.43, "Gold Coast");
    const uint32_t gold_coast = au.relation(1, 6, Box{-28.2, 153.2, -27.7, 153.6}, {PlaceValue::NONE, "Gold Coast City", "", 1});
    au.link();
    CHECK(au.link_of(gold_coast).step == LinkStep::NONE);
    CHECK_EQ(au.claimant(1), uint64_t(0));
}

TEST(a_relation_links_the_first_of_its_label_members_that_is_a_place_node) {
    // Kilbeg's first label member, node 9, is no place node; its second, the hamlet Ballyboy of
    // another name and no wikidata, is: find_linked_place takes the label members in turn until
    // one is a place (placex_triggers.sql:297-312), so Kilbeg links Ballyboy by its label.
    World w("IE");
    w.node(2, PlaceValue::HAMLET, 53.5, -6.5, "Ballyboy");
    Tags tags{PlaceValue::NONE, "Kilbeg", "", 9};
    tags.more_labels.push_back(2);
    const uint32_t kilbeg = w.relation(10, 10, Box{53.4, -6.6, 53.6, -6.4}, tags);
    w.link();
    CHECK(w.link_of(kilbeg).step == LinkStep::LABEL);
    CHECK_EQ(w.link_of(kilbeg).node, int64_t(2));
}

TEST(two_relations_of_one_label_member_both_link_it_and_the_later_runner_claims_it) {
    // Den Haag's city node is the label member of its level 8 relation (rank_search 14) and of
    // its level 10 one (16), a later runner: each links it by its label (:291-312, which reads no
    // link), and the level 10 one, indexed after, holds the claim.
    World w("NL");
    w.node(1, PlaceValue::CITY, 52.08, 4.31, "Den Haag");
    const Box city{52.0, 4.2, 52.15, 4.4};
    const uint32_t gemeente = w.relation(192736, 8, city, {PlaceValue::NONE, "Den Haag", "", 1});
    const uint32_t woonplaats = w.relation(192737, 10, city, {PlaceValue::NONE, "Den Haag", "", 1});
    w.link();
    CHECK(w.link_of(gemeente).step == LinkStep::LABEL);
    CHECK(w.link_of(woonplaats).step == LinkStep::LABEL);
    CHECK_EQ(w.claimant(1), w.boundary(woonplaats));
}

TEST(a_label_member_is_left_to_its_relation_by_every_other_step) {
    // Den Haag (R192736, NL admin_level 8, 14) shares its wikidata (Q36600) with the city node, the
    // label member of the level 10 relation of its outline (R192737), a later runner. On a fresh
    // import the level 8 one would link the node first and the level 10 one take it over by its
    // label (:291-312, which reads no link); the public server, which indexes each polygon again
    // after its import, finds the node linked then. So the level 8 one links nothing, by its
    // wikidata or by name, and keeps its 14; so does Grad Sarajevo (R2567380, BA level 6),
    // whose wikidata node is Sarajevo's label member (level 9). The level 8 one tagged place=city
    // takes no type step to it either. A label member of a relation of rank 0, which links
    // nothing, is any step's.
    World w("NL");
    w.node(1, PlaceValue::CITY, 52.08, 4.31, "Den Haag", "Q36600");
    const Box city{52.0, 4.2, 52.15, 4.4};
    const uint32_t gemeente = w.relation(192736, 8, city, {PlaceValue::CITY, "Den Haag", "Q36600"});
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

TEST(a_node_another_relation_holds_as_its_label_is_no_link_whatever_the_public_server_keeps) {
    // Ferraz de Vasconcelos: the municipality (R298265, admin_level 8, place=municipality) holds
    // the city node of its name, the label member of the city's boundary (R5683099, admin_level
    // 9). The builder leaves the node to its label relation (place_links.h), as Nominatim's
    // indexer finds it linked when it takes a polygon again: the municipality links nothing, and
    // the city its label. Nominatim's public server still holds the municipality's link of its
    // first index, before the city's label took the node (its extratags keep linked_place, and its
    // /details its rank 16), as it does for Queluz (R298199) and Estiva Gerbi (R297955), whose
    // nodes another relation labels; a polygon it indexes again links as here.
    World w("BR");
    w.node(1, PlaceValue::CITY, -23.54, -46.37, "Ferraz de Vasconcelos");
    const uint32_t municipality = w.relation(298265, 8, Box{-23.6, -46.4, -23.5, -46.3},
                                             {PlaceValue::MUNICIPALITY, "Ferraz de Vasconcelos"});
    const uint32_t city = w.relation(5683099, 9, Box{-23.58, -46.38, -23.52, -46.32},
                                     {PlaceValue::NONE, "Ferraz de Vasconcelos", "", 1});
    w.link();
    CHECK(w.link_of(municipality).step == LinkStep::NONE);
    CHECK(w.label(municipality) == AdminPlaceType::MUNICIPALITY);
    CHECK(w.link_of(city).step == LinkStep::LABEL);
    CHECK(w.label(city) == AdminPlaceType::CITY);
    CHECK_EQ(w.claimant(1), w.boundary(city));
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
    // A BR boundary of admin_level 8 (16) tagged place=municipality, whose city node is no
    // relation's label member, links the node by name (16); a namesake village outside it links
    // nothing, nor a town of its name in a level 9 boundary (18), whose rank is not the node's 16,
    // nor one of another wikidata.
    World w("BR");
    w.node(1, PlaceValue::CITY, -23.54, -46.37, "Vila Exemplo");
    const uint32_t ferraz = w.relation(1, 8, Box{-23.6, -46.4, -23.5, -46.3},
                                       {PlaceValue::MUNICIPALITY, "Vila Exemplo"});
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
    // 200 boundaries, 140 of them at admin_level 8, one runner of more rows than a worker takes
    // at once. 40 of those share a wikidata round one town node, their ids spread over the
    // runner's last chunks: each finds the node free as the runner starts and links it, and the
    // last keeps it, however many workers looked.
    World one("DE"), many("DE");
    for (World* w : {&one, &many}) {
        for (int k = 0; k < 160; k++) {
            const double s = 50.1 + (k % 16) * 0.2, west = 8.1 + (k / 16) * 0.3;
            const std::string name = "Ort " + std::to_string(k % 5);
            w->node(100 + k, k % 2 ? PlaceValue::VILLAGE : PlaceValue::SUBURB, s + 0.02, west + 0.02, name,
                    k % 3 ? "" : "Q" + std::to_string(k % 4));
            const uint8_t level = k < 100 ? 8 : static_cast<uint8_t>(6 + k % 4);
            w->relation(10 + k, level, Box{s, west, s + 0.15, west + 0.25},
                        {k % 3 ? PlaceValue::NONE : PlaceValue::SUBURB, name, k % 2 ? "" : "Q" + std::to_string(k % 4)});
        }
        w->node(1, PlaceValue::TOWN, 49.5, 7.5, "Mitte", "Q77");
        for (int k = 0; k < 40; k++)
            w->relation(1000 + 7 * k, 8, Box{49.4 - 0.001 * k, 7.4, 49.6, 7.6 + 0.001 * k},
                        {PlaceValue::NONE, "Kreis " + std::to_string(k), "Q77"});
    }
    one.link(1);
    many.link(8);
    REQUIRE(one.data.admin_links.size() == many.data.admin_links.size());
    for (size_t k = 0; k < one.data.admin_links.size(); k++) {
        CHECK_EQ(one.data.admin_links[k].boundary, many.data.admin_links[k].boundary);
        CHECK_EQ(one.data.admin_links[k].node, many.data.admin_links[k].node);
        CHECK(one.data.admin_links[k].step == many.data.admin_links[k].step);
    }
    CHECK(one.data.place_node_links == many.data.place_node_links);
    REQUIRE(one.data.place_claims.size() == many.data.place_claims.size());
    for (size_t k = 0; k < one.data.place_claims.size(); k++) {
        CHECK_EQ(one.data.place_claims[k].node, many.data.place_claims[k].node);
        CHECK_EQ(one.data.place_claims[k].boundary, many.data.place_claims[k].boundary);
    }
    for (uint32_t p = 0; p < one.data.admin_polygons.size(); p++) CHECK_EQ(one.label(p), many.label(p));
    size_t mitte = 0;
    for (const AdminLink& l : many.data.admin_links) mitte += l.node == 1;
    CHECK_EQ(mitte, size_t(40));
    CHECK_EQ(many.claimant(1), admin_boundary_id(admin_ring_id(1000 + 7 * 39, 0)));
    CHECK(!one.data.admin_links.empty());
}

TEST(a_linking_polygon_goes_by_its_nodes_english_name_where_it_has_none_of_its_own) {
    // Nominatim gives a polygon the names of the node it links that it lacks as _place_ names
    // (placex_indexing_prepare), and an English answer reads name:en, then _place_name:en, then
    // name (localization.py): Sosnovka (RU), whose boundary has no name:en, goes by its town
    // node's, as Nominatim names it; a polygon of its own name:en keeps it, and one whose node
    // has none keeps its name. A place area linking its namesake suburb node goes by the node's
    // name:en too.
    World w("RU");
    w.node(1, PlaceValue::TOWN, 53.24, 41.37, "Сосновка", "", "Sosnovka");
    const uint32_t sosnovka = w.relation(1, 8, Box{53.2, 41.3, 53.3, 41.4}, {PlaceValue::NONE, "Сосновка", "", 1});
    w.node(2, PlaceValue::TOWN, 52.65, 42.73, "Кирсанов", "", "Kirsanov Town");
    Tags own_en{PlaceValue::NONE, "Кирсанов", "", 2};
    own_en.name_en = true;
    const uint32_t kirsanov = w.relation(2, 8, Box{52.6, 42.7, 52.7, 42.8}, own_en);
    w.node(3, PlaceValue::TOWN, 52.9, 40.5, "Уварово");
    const uint32_t uvarovo = w.relation(3, 8, Box{52.85, 40.45, 52.95, 40.55}, {PlaceValue::NONE, "Уварово", "", 3});
    w.node(4, PlaceValue::SUBURB, 53.25, 41.35, "Центральный", "", "Tsentralny");
    const uint32_t centre = w.place_area(4, PlaceValue::SUBURB, Box{53.245, 41.345, 53.255, 41.355}, {PlaceValue::NONE, "Центральный"});
    const LinkStats stats = w.link();
    CHECK(w.link_of(centre).step != LinkStep::NONE);
    CHECK_EQ(w.name(sosnovka), std::string("Sosnovka"));
    CHECK_EQ(w.name(kirsanov), std::string(""));
    CHECK_EQ(w.name(uvarovo), std::string(""));
    CHECK_EQ(w.name(centre), std::string("Tsentralny"));
    CHECK_EQ(stats.named, size_t(2));
}

TEST(a_linking_polygon_of_an_english_only_country_keeps_its_own_name) {
    // Where English is the country's one language, Nominatim gives a polygon's name tag its
    // name:en as well (add_default_place_name), which comes before the node's: the City of
    // Testford keeps its name. Uganda has two languages, so Kampala Capital City (R10546821) goes
    // by its city node's name:en, Kampala, as Nominatim's answer names it.
    World gb("GB");
    gb.node(1, PlaceValue::TOWN, 52.0, -1.0, "Testford", "", "Testford Town");
    const uint32_t testford = gb.relation(1, 8, Box{51.9, -1.1, 52.1, -0.9}, {PlaceValue::NONE, "City of Testford", "", 1});
    gb.link();
    CHECK(gb.link_of(testford).step == LinkStep::LABEL);
    CHECK_EQ(gb.name(testford), std::string(""));

    World ug("UG");
    ug.node(1, PlaceValue::CITY, 0.3177, 32.5814, "Kampala", "Q3894", "Kampala");
    const uint32_t kcc = ug.relation(10546821, 6, Box{0.2, 32.5, 0.4, 32.7},
                                     {PlaceValue::NONE, "Kampala Capital City", "Q3894", 1});
    ug.link();
    CHECK_EQ(ug.name(kcc), std::string("Kampala"));
}

TEST(the_english_name_of_a_linking_polygon_follows_nominatims_name_order) {
    const uint16_t ru = pack_country_code('R', 'U'), gb = pack_country_code('G', 'B'), ug = pack_country_code('U', 'G');
    CHECK(english_only_country(gb));
    CHECK(english_only_country(pack_country_code('U', 'S')));
    CHECK(!english_only_country(ug));
    CHECK(!english_only_country(pack_country_code('C', 'A')));
    CHECK(!english_only_country(0));
    // Its own name:en first.
    CHECK_EQ(linked_english_name(true, ru, "Sosnovka", "Сосновка", ru), std::string_view());
    // In an English-only country its name tag, its name:en too.
    CHECK_EQ(linked_english_name(false, gb, "Testford Town", "Testford", gb), std::string_view());
    // Else the node's name:en.
    CHECK_EQ(linked_english_name(false, ru, "Sosnovka", "Сосновка", ru), std::string_view("Sosnovka"));
    // A node without one names it only where its own country is English-only, which gives its
    // name tag its name:en.
    CHECK_EQ(linked_english_name(false, ru, "", "Сосновка", ru), std::string_view());
    CHECK_EQ(linked_english_name(false, ug, "", "Busia", pack_country_code('K', 'E')), std::string_view());
    CHECK_EQ(linked_english_name(false, ru, "", "Ipswich", gb), std::string_view("Ipswich"));
}

TEST(a_polygon_named_by_its_node_keeps_its_own_name_aside) {
    // Nominatim's addr:city crosscheck reads every name of the polygon, its own among the _place_
    // ones (results.py:606), and its search finds it by them: Sosnovka's boundary keeps the name
    // it had as own_name_id. One whose node's English name is its own name already, or that the
    // pass names nothing, keeps none.
    World w("RU");
    w.node(1, PlaceValue::TOWN, 53.24, 41.37, "Сосновка", "", "Sosnovka");
    const uint32_t sosnovka = w.relation(1, 8, Box{53.2, 41.3, 53.3, 41.4}, {PlaceValue::NONE, "Сосновка", "", 1});
    w.node(2, PlaceValue::TOWN, 52.9, 40.5, "Уварово", "", "Uvarovo");
    const uint32_t uvarovo = w.relation(2, 8, Box{52.85, 40.45, 52.95, 40.55}, {PlaceValue::NONE, "Уварово", "", 2});
    const uint32_t uvarovo_en = w.data.string_pool.intern("Uvarovo");
    w.data.admin_polygons[uvarovo].name_id = uvarovo_en;
    w.node(3, PlaceValue::TOWN, 52.65, 42.73, "Кирсанов");
    const uint32_t kirsanov = w.relation(3, 8, Box{52.6, 42.7, 52.7, 42.8}, {PlaceValue::NONE, "Кирсанов", "", 3});
    for (const AdminPolygon& p : w.data.admin_polygons) CHECK_EQ(p.own_name_id, NO_DATA);
    const LinkStats stats = w.link();
    CHECK_EQ(w.name(sosnovka), std::string("Sosnovka"));
    CHECK_EQ(w.data.admin_polygons[sosnovka].own_name_id, sosnovka);
    CHECK_EQ(w.data.admin_polygons[uvarovo].name_id, uvarovo_en);
    CHECK_EQ(w.data.admin_polygons[uvarovo].own_name_id, NO_DATA);
    CHECK_EQ(w.name(kirsanov), std::string(""));
    CHECK_EQ(w.data.admin_polygons[kirsanov].own_name_id, NO_DATA);
    CHECK_EQ(stats.named, size_t(1));
}

// The cases of #533's wikidata step (place_claims.h's link_wikidata_places there), on the link pass.

TEST(lowercase_utf8_lowers_the_capitals_postgres_lower_lowers_in_the_wikidata_steps_order) {
    // lower(name->'name') = bnd_name orders the nodes of a boundary's wikidata.
    CHECK(lowercase_utf8("").empty());
    CHECK_EQ(lowercase_utf8("Lent"), std::string("lent"));
    CHECK_EQ(lowercase_utf8("ÉVORA"), std::string("évora"));
    CHECK_EQ(lowercase_utf8("ŁÓDŹ"), std::string("łódź"));
    CHECK_EQ(lowercase_utf8("İZMİR"), std::string("izmir"));
    CHECK_EQ(lowercase_utf8("ΆΘΗΝΑ"), std::string("άθηνα"));
    CHECK_EQ(lowercase_utf8("МОСКВА"), std::string("москва"));
    CHECK_EQ(lowercase_utf8("ЁЛКИ"), std::string("ёлки"));
    CHECK_EQ(lowercase_utf8("ԵՐԵՎԱՆ"), std::string("երեվան"));
    CHECK_EQ(lowercase_utf8("Straße ×2 東京"), std::string("straße ×2 東京"));
}

TEST(a_wikidata_link_takes_one_node_in_the_steps_order) {
    // find_linked_place's wikidata step: ORDER BY lower(name->'name') = bnd_name desc, nulls
    // first, the first node found linked. A place=suburb area, relation 100, covers the town node
    // 1 and the village node 2 of its wikidata; node 2 has the area's name and node 3, outside it,
    // none. The area links node 2, its label, and keeps its own place tag.
    World w("GB");
    const uint32_t area = w.place_area(100, PlaceValue::SUBURB, Box{0, 0, 1, 1}, {PlaceValue::NONE, "Riverside", "Q1"});
    w.node(1, PlaceValue::TOWN, 0.5, 0.5, "Riverside Town", "Q1");
    w.node(2, PlaceValue::VILLAGE, 0.6, 0.6, "Riverside", "Q1");
    w.node(3, PlaceValue::VILLAGE, 5, 5, "", "Q1");
    w.link();
    CHECK(w.link_of(area).step == LinkStep::WIKIDATA);
    CHECK_EQ(w.link_of(area).node, int64_t(2));
    CHECK_EQ(w.claimant(2), w.boundary(area));
    CHECK(w.label(area) == AdminPlaceType::VILLAGE);
    CHECK_EQ(w.data.admin_polygons[area].own_place_value, value(PlaceValue::SUBURB));

    // A node without a name comes first, and equals go to the lower node id.
    World v("GB");
    const uint32_t town = v.relation(100, 8, Box{0, 0, 1, 1}, {PlaceValue::NONE, "Riverside", "Q1"});
    v.node(7, PlaceValue::TOWN, 0.5, 0.5, "Riverside", "Q1");
    v.node(6, PlaceValue::VILLAGE, 0.5, 0.6, "", "Q1");
    v.node(5, PlaceValue::TOWN, 0.5, 0.7, "", "Q1");
    v.link();
    CHECK_EQ(v.link_of(town).node, int64_t(5));
    CHECK(v.label(town) == AdminPlaceType::TOWN);
}

TEST(a_node_a_boundary_before_links_is_no_later_ones) {
    // Nominatim indexes the boundaries by rank, the place areas after them: the wijk, relation 200
    // (L10), links the village node 5 of their wikidata first, the quarter area, relation 100,
    // passes it over for the hamlet node 6, and the quarter area of way 300, round node 5 alone,
    // finds none left and stays a quarter. Whatever order the nodes come in.
    for (const bool reversed : {false, true}) {
        World w("NL");
        const uint32_t quarter = w.place_area(100, PlaceValue::QUARTER, Box{0, 0, 1, 1}, {PlaceValue::NONE, "Lent", "Q9"});
        const uint32_t wijk = w.relation(200, 10, Box{0, 0, 0.7, 0.7}, {PlaceValue::NONE, "Lent", "Q9"});
        Tags way_tags{PlaceValue::QUARTER, "Lent", "Q9"};
        const uint32_t way = w.add(way_boundary_id(300), 15, Box{0.4, 0.4, 0.6, 0.6}, way_tags);
        if (reversed) w.node(6, PlaceValue::HAMLET, 0.9, 0.9, "Lent-Oost", "Q9");
        w.node(5, PlaceValue::VILLAGE, 0.5, 0.5, "Lent", "Q9");
        if (!reversed) w.node(6, PlaceValue::HAMLET, 0.9, 0.9, "Lent-Oost", "Q9");
        w.link();
        CHECK_EQ(w.link_of(wijk).node, int64_t(5));
        CHECK(w.label(wijk) == AdminPlaceType::VILLAGE);
        CHECK_EQ(w.link_of(quarter).node, int64_t(6));
        CHECK(w.label(quarter) == AdminPlaceType::HAMLET);
        CHECK_EQ(w.data.admin_polygons[quarter].own_place_value, value(PlaceValue::QUARTER));
        CHECK(w.link_of(way).step == LinkStep::NONE);
        CHECK(w.label(way) == AdminPlaceType::QUARTER);
        CHECK_EQ(w.claimant(5), w.boundary(wijk));
        CHECK_EQ(w.claimant(6), w.boundary(quarter));
    }
}

TEST(a_boundary_of_rank_0_reserves_no_node_of_its_wikidata) {
    // find_linked_place returns at once for a boundary of rank_address 0 (placex_triggers.sql:284),
    // so it links nothing and leaves the node to the polygons after it: an Australian local
    // government area (admin_level 6, rank 0 there), relation 100, and a place=suburb area,
    // relation 200, both cover the town node 5 of their wikidata. The area links it.
    World w("AU");
    const uint32_t lga = w.relation(100, 6, Box{0, 0, 1, 1}, {PlaceValue::NONE, "Gold Coast City", "Q5"});
    const uint32_t area = w.place_area(200, PlaceValue::SUBURB, Box{0.2, 0.2, 0.8, 0.8}, {PlaceValue::NONE, "Southport", "Q5"});
    w.node(5, PlaceValue::TOWN, 0.5, 0.5, "Southport", "Q5");
    w.link();
    CHECK(w.link_of(lga).step == LinkStep::NONE);
    CHECK(w.label(lga) == AdminPlaceType::NONE);
    CHECK_EQ(w.claimant(5), w.boundary(area));
    CHECK(w.label(area) == AdminPlaceType::TOWN);
    CHECK_EQ(w.data.admin_polygons[area].own_place_value, value(PlaceValue::SUBURB));
}

TEST(a_label_member_is_left_to_the_wikidata_step_only_by_a_boundary_of_rank_0) {
    // The label step comes first, but find_linked_place returns at once for a boundary of
    // rank_address 0: an Australian local government area (admin_level 6, rank 0 there), relation
    // 100, names the town node 5 as its label, which the place=suburb area, relation 200, of its
    // wikidata links. A boundary of rank 16, relation 300, links its label member, the town node
    // 6, which the suburb area 400 of its wikidata passes over.
    World w("AU");
    const uint32_t lga = w.relation(100, 6, Box{0, 0, 1, 1}, {PlaceValue::NONE, "Gold Coast City", "", 5});
    const uint32_t area = w.place_area(200, PlaceValue::SUBURB, Box{0.2, 0.2, 0.8, 0.8}, {PlaceValue::NONE, "Southport", "Q5"});
    w.node(5, PlaceValue::TOWN, 0.5, 0.5, "Southport", "Q5");
    const uint32_t city = w.relation(300, 8, Box{2, 2, 3, 3}, {PlaceValue::NONE, "Ipswich", "", 6});
    const uint32_t other = w.place_area(400, PlaceValue::SUBURB, Box{2.2, 2.2, 2.8, 2.8}, {PlaceValue::NONE, "Ipswich Central", "Q6"});
    w.node(6, PlaceValue::TOWN, 2.5, 2.5, "Ipswich", "Q6");
    w.link();
    CHECK(w.link_of(lga).step == LinkStep::NONE);
    CHECK(w.link_of(area).step == LinkStep::WIKIDATA);
    CHECK_EQ(w.claimant(5), w.boundary(area));
    CHECK(w.label(area) == AdminPlaceType::TOWN);
    CHECK(w.link_of(city).step == LinkStep::LABEL);
    CHECK_EQ(w.claimant(6), w.boundary(city));
    CHECK(w.link_of(other).step == LinkStep::NONE);
    CHECK(w.label(other) == AdminPlaceType::SUBURB);
}

TEST(place_areas_link_in_the_order_of_their_address_ranks) {
    // Nominatim indexes the place areas after every boundary, one rank_address after another
    // (indexer.py index_by_rank, RankRunner): a place=suburb area (rank 20), relation 300, takes
    // the village node 5 of its wikidata before a place=quarter area (rank 22) of a lower id,
    // relation 100, which links nothing and stays a quarter.
    World w("GB");
    const uint32_t quarter = w.place_area(100, PlaceValue::QUARTER, Box{0, 0, 1, 1}, {PlaceValue::NONE, "Oost", "Q5"});
    const uint32_t suburb = w.place_area(300, PlaceValue::SUBURB, Box{0.1, 0.1, 0.9, 0.9}, {PlaceValue::NONE, "Oost", "Q5"});
    w.node(5, PlaceValue::VILLAGE, 0.5, 0.5, "Oostdorp", "Q5");
    w.link();
    CHECK_EQ(w.claimant(5), w.boundary(suburb));
    CHECK(w.label(suburb) == AdminPlaceType::VILLAGE);
    CHECK_EQ(w.data.admin_polygons[suburb].own_place_value, value(PlaceValue::SUBURB));
    CHECK(w.link_of(quarter).step == LinkStep::NONE);
    CHECK(w.label(quarter) == AdminPlaceType::QUARTER);
}

TEST(of_nodes_equal_in_the_steps_order_the_one_of_the_lowest_address_rank_links) {
    // The step's ORDER BY leaves nodes of one name unordered: Espanola (relation 100) covers the
    // town node 20 and the hamlet node 10 of its wikidata, both named Española. The town, of
    // address rank 16, comes before the hamlet's 20 whatever their ids, and equals go to the
    // lower.
    World w("US");
    const uint32_t espanola = w.relation(100, 8, Box{0, 0, 1, 1}, {PlaceValue::NONE, "Espanola", "Q5"});
    w.node(10, PlaceValue::HAMLET, 0.5, 0.5, "Española", "Q5");
    w.node(20, PlaceValue::TOWN, 0.6, 0.6, "Española", "Q5");
    w.link();
    CHECK_EQ(w.link_of(espanola).node, int64_t(20));
    CHECK(w.label(espanola) == AdminPlaceType::TOWN);

    World v("US");
    const uint32_t other = v.relation(100, 8, Box{0, 0, 1, 1}, {PlaceValue::NONE, "Espanola", "Q5"});
    v.node(40, PlaceValue::VILLAGE, 0.5, 0.5, "Española", "Q5");
    v.node(30, PlaceValue::TOWN, 0.6, 0.6, "Española", "Q5");
    v.link();
    CHECK_EQ(v.link_of(other).node, int64_t(30));
}

TEST(every_copy_of_a_place_node_two_extracts_share_takes_the_boundary_linking_it) {
    // Two overlapping extracts list the city node Kreuzberg twice, as two place nodes of one osm
    // id. The link pass takes the node once, and records the boundary that links it against each
    // copy, so the rank pass leaves both unpromoted by it.
    World w("DE");
    const uint32_t district = w.relation(55764, 9, {52.48, 13.36, 52.51, 13.45}, {PlaceValue::NONE, "kreuzberg", "", 240109189});
    w.node(240109189, PlaceValue::CITY, 52.49, 13.40, "Kreuzberg");
    w.node(240109189, PlaceValue::CITY, 52.49, 13.40, "Kreuzberg");
    w.link();
    CHECK_EQ(w.link_of(district).node, int64_t{240109189});
    const PlaceNodeLinks expected{{0, w.boundary(district)}, {1, w.boundary(district)}};
    CHECK((w.data.place_node_links == expected));
}
