// build-index's place links and postal areas, from a PBF to the server's
// answers: a boundary of rank 0 in its country links no place node, a
// boundary links a place node of its wikidata only where it holds it, a
// place=province area is an address area where its country ranks it, a
// postal area names a point only of its own country and of a postcode its
// country takes, and a place area linked to a place node of another type
// ranks by its own place tag.
#include <cstdio>
#include <string>
#include <vector>

#include "pbf_fixture.h"
#include "test_framework.h"

TEST(a_boundary_of_rank_0_in_its_country_links_no_place_node) {
    // Gold Coast City is a local government area, admin_level 6, which ranks 0 in Australia
    // (address-levels.json): find_linked_place links nothing to it (placex_triggers.sql:285), so
    // its label member, the city node of Gold Coast, is a place of its own, named in the address
    // round it.
    Fixture f;
    f.country(-30, 150, -26, 154, "Australia", "AU");
    f.boundary(-29, 152, -27, 153.9, 4, "Queensland");
    const int64_t gold_coast = f.node(-28.0, 153.43, {{"place", "city"}, {"name", "Gold Coast"}});
    f.boundary(-28.2, 153.2, -27.7, 153.6, 6, "Gold Coast City", {}, {{OsmType::Node, gold_coast, "label"}});
    // A level 8 boundary there, of rank 16, does link its label member.
    const int64_t ipswich = f.node(-27.6, 152.76, {{"place", "city"}, {"name", "Ipswich"}});
    f.boundary(-27.7, 152.6, -27.5, 152.9, 8, "City of Ipswich", {}, {{OsmType::Node, ipswich, "label"}});
    const std::vector<std::string> answers = answers_of(f, "rank0-label", {{-27.95, 153.40}, {-27.62, 152.75}});
    REQUIRE(answers.size() == 2);
    CHECK(names(answers[0], "city", "Gold Coast"));
    CHECK(names(answers[1], "city", "City of Ipswich"));
    CHECK(answers[1].find("\"Ipswich\"") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_links_a_place_node_of_its_wikidata_only_where_it_holds_it) {
    // The 9th Division (admin_level 7) shares its wikidata with a place=neighbourhood node outside
    // it, which find_linked_place does not link (_st_covers(bnd.geometry, placex.geometry)): the
    // division ranks 14, as its level does, and the regiment inside it, a place=town boundary of
    // level 8, 16, a town in the address. The node stays a place of its own.
    Fixture f;
    f.country(-30, 140, -20, 150, "Australia", "AU");
    f.node(-25.5, 144.5, {{"place", "neighbourhood"}, {"name", "Division Seat"}, {"wikidata", "Q100"}});
    f.boundary(-24, 141, -22, 143, 7, "9th Division", {{"wikidata", "Q100"}});
    f.boundary(-23.5, 141.5, -22.5, 142.5, 8, "170th Regiment", {{"place", "town"}});
    // A division round the node of its wikidata links it, and takes its rank.
    f.node(-27.0, 147.0, {{"place", "suburb"}, {"name", "Old Quarter"}, {"wikidata", "Q200"}});
    f.boundary(-28, 146, -26, 148, 9, "Old Quarter District", {{"wikidata", "Q200"}});
    const std::vector<std::string> answers =
        answers_of(f, "wikidata-cover", {{-23.0, 142.0}, {-25.5, 144.5}, {-27.0, 147.01}});
    REQUIRE(answers.size() == 3);
    CHECK(names(answers[0], "town", "170th Regiment"));
    CHECK(names(answers[0], "municipality", "9th Division"));
    CHECK(names(answers[1], "neighbourhood", "Division Seat"));
    CHECK(names(answers[2], "suburb", "Old Quarter District"));
    CHECK(answers[2].find("\"Old Quarter\"") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_province_area_is_an_address_area_where_its_country_ranks_it) {
    // place=province ranks 10 in Spain and 0 by default (address-levels.json): a province
    // multipolygon names the province in Spain and nothing in Australia.
    Fixture f;
    f.country(40, -4, 41, -3, "Spain", "ES");
    f.area(40.2, -3.8, 40.8, -3.2, {{"type", "multipolygon"}, {"place", "province"}, {"name", "Provincia de Prueba"}});
    f.country(-30, 140, -20, 150, "Australia", "AU");
    f.area(-26, 142, -24, 144, {{"type", "multipolygon"}, {"place", "province"}, {"name", "Outback Province"}});
    const std::vector<std::string> answers = answers_of(f, "province-area", {{40.5, -3.5}, {-25, 143}});
    REQUIRE(answers.size() == 2);
    CHECK(names(answers[0], "province", "Provincia de Prueba"));
    CHECK(answers[1].find("Outback Province") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_postal_area_names_a_point_only_of_its_country_and_of_a_postcode_its_country_takes) {
    // Nominatim makes a postcode area only of a postcode its country's pattern takes
    // (postcodes.py _update_postcode_areas), and gives it the country of its centroid:
    // get_nearest_postcode takes the point's country's. A German postal area of 1234 (Germany's
    // postcodes have five digits) leaves the point to the centroid of the address 2 km off, and a
    // Swiss one of 8000 over the border names no German point.
    Fixture f;
    f.country(50, 8, 51, 9, "Germany", "DE");
    f.country(51, 8, 52, 9, "Switzerland", "CH");
    f.area(50.4, 8.4, 50.6, 8.6, {{"type", "boundary"}, {"boundary", "postal_code"}, {"postal_code", "1234"}});
    f.node(50.52, 8.5, {{"addr:postcode", "60311"}, {"addr:housenumber", "1"}, {"addr:street", "Zeil"}});
    f.area(50.95, 8.4, 51.5, 8.6, {{"type", "boundary"}, {"boundary", "postal_code"}, {"postal_code", "8000"}});
    const std::vector<std::string> answers = answers_of(f, "postal-areas", {{50.5, 8.5}, {50.97, 8.5}, {51.2, 8.5}});
    REQUIRE(answers.size() == 3);
    CHECK(names(answers[0], "postcode", "60311"));
    CHECK(answers[1].find("8000") == std::string::npos);
    CHECK(names(answers[2], "postcode", "8000"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_place_area_linked_to_a_place_node_of_another_type_ranks_by_its_own_place_tag) {
    // Weesp (R15419242) is a boundary=place area tagged place=suburb whose label member is the
    // town node Weesp: find_linked_place links the node, which labels the area town, but
    // placex_update keeps a place area of rank 16 to 23 at its own place tag's rank, the
    // suburb's 20 (placex_triggers.sql:930-950, :1129). It comes before Driemond, a village
    // node promoted to 18 inside Amsterdam (admin_level 10, rank 16 in the Netherlands, linked to
    // its city node), as Nominatim's line has it.
    Fixture f;
    f.country(52, 4.5, 53, 5.5, "Netherlands", "NL");
    f.boundary(52.1, 4.6, 52.9, 5.4, 4, "North Holland");
    f.boundary(52.2, 4.8, 52.5, 5.2, 8, "Amsterdam");
    const int64_t amsterdam = f.node(52.30, 4.96, {{"place", "city"}, {"name", "Amsterdam"}});
    f.boundary(52.29, 4.94, 52.32, 5.02, 10, "Amsterdam", {}, {{OsmType::Node, amsterdam, "label"}});
    const int64_t weesp = f.node(52.31, 5.04, {{"place", "town"}, {"name", "Weesp"}});
    f.area(52.295, 5.0, 52.325, 5.06, {{"type", "boundary"}, {"boundary", "place"}, {"place", "suburb"}, {"name", "Weesp"}},
           {{OsmType::Node, weesp, "label"}});
    f.node(52.305, 5.015, {{"place", "village"}, {"name", "Driemond"}});
    const std::vector<std::string> answers = answers_of(f, "place-area-own-rank", {{52.31, 5.01}});
    REQUIRE(answers.size() == 1);
    CHECK(answers[0].find("\"display_name\":\"Weesp, Driemond, Amsterdam, North Holland, Netherlands\"") !=
          std::string::npos);
    CHECK(names(answers[0], "town", "Weesp"));
    CHECK(names(answers[0], "village", "Driemond"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}
