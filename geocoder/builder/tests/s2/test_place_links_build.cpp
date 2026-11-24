// build-index's place links and postal areas, from a PBF to the server's
// answers: a boundary of rank 0 in its country links no place node, a
// boundary links a place node of its wikidata only where it holds it, a
// place=province area is an address area where its country ranks it, a
// postal area names a point only of its own country and of a postcode its
// country takes, a place area linked to a place node of another type ranks
// by its own place tag, and each step of find_linked_place links as
// Nominatim's does.
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <map>
#include <string>
#include <tuple>
#include <vector>

#include "pbf_fixture.h"
#include "place_files.h"
#include "side_blocks.h"
#include "string_files.h"
#include "test_framework.h"
#include "types.h"

namespace {

std::vector<char> file_bytes(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::vector<char>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

}  // namespace

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

TEST(a_boundary_with_a_place_tag_of_its_own_links_its_namesake_city_node_by_name) {
    // A boundary of admin_level 8 tagged place=municipality, with no label member and no
    // wikidata: find_linked_place links its city node by name, the node ranking 16 as the
    // boundary does (placex_triggers.sql:363-386). The boundary is the address's city, labelled
    // by the node, and no municipality.
    Fixture f;
    f.country(-34, -54, -5, -35, "Brazil", "BR");
    f.boundary(-25, -53, -20, -44, 4, "São Paulo");
    f.boundary(-23.6, -46.4, -23.5, -46.3, 8, "Vila Exemplo", {{"place", "municipality"}});
    f.node(-23.54, -46.37, {{"place", "city"}, {"name", "Vila Exemplo"}});
    const std::vector<std::string> answers = answers_of(f, "link-by-name", {{-23.545, -46.365}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "city", "Vila Exemplo"));
    CHECK(answers[0].find("\"municipality\"") == std::string::npos);
    std::printf("    %s\n", answers[0].c_str());
}

TEST(a_municipality_whose_city_node_another_relation_labels_ranks_by_its_own_tag) {
    // Ferraz de Vasconcelos: the municipality (R298265, admin_level 8, place=municipality) holds
    // the city node, the label member of the city's boundary (R5683099, admin_level 9). The node
    // is the city's (place_links.h), as Nominatim's indexer finds it when it takes a polygon
    // again: the municipality ranks 14 by its own tag, and the city 16 by its node, above the
    // municipality's 14. Nominatim's public server still ranks them 16 and 18 from the link the
    // municipality made on its first index, before the city's label took the node, as it keeps
    // Queluz (R298199) and Estiva Gerbi (R297955) at 16; indexed again they rank as here.
    Fixture f;
    f.country(-34, -54, -5, -35, "Brazil", "BR");
    f.boundary(-25, -53, -20, -44, 4, "São Paulo");
    const int64_t node = f.node(-23.54, -46.37, {{"place", "city"}, {"name", "Ferraz de Vasconcelos"}});
    f.boundary(-23.6, -46.4, -23.5, -46.3, 8, "Ferraz de Vasconcelos", {{"place", "municipality"}});
    f.boundary(-23.58, -46.38, -23.52, -46.32, 9, "Ferraz de Vasconcelos", {}, {{OsmType::Node, node, "label"}});
    const std::vector<std::string> answers = answers_of(f, "label-held", {{-23.545, -46.365}});
    REQUIRE(answers.size() == 1);
    std::printf("    %s\n", answers[0].c_str());
    CHECK(names(answers[0], "city", "Ferraz de Vasconcelos"));
    CHECK(names(answers[0], "municipality", "Ferraz de Vasconcelos"));
    CHECK(answers[0].find("\"display_name\":\"Ferraz de Vasconcelos, São Paulo, Brazil\"") != std::string::npos);
}

TEST(a_townland_of_a_locality_ranks_25_and_leaves_the_address) {
    // Irish townlands: one whose label member is a place=locality node takes the node's 25
    // (placex_triggers.sql:1129), and one tagged place=locality itself its tag's 25 (:1157-1172):
    // the address of a street in either shows neither. A third, linked to nothing, ranks 20 by
    // its level. Far from every street the townland answers itself (reverse.py lookup_area),
    // labelled a locality, by its node's type or its own tag.
    Fixture f;
    f.country(51, -11, 56, -5, "Ireland", "IE");
    f.boundary(53, -7, 54, -6, 6, "County Meath");
    const int64_t locality = f.node(53.55, -6.55, {{"place", "locality"}, {"name", "Rathmore"}});
    f.boundary(53.5, -6.6, 53.6, -6.5, 10, "Rathmore", {}, {{OsmType::Node, locality, "label"}});
    f.boundary(53.5, -6.4, 53.6, -6.3, 10, "Town Parks", {{"place", "locality"}});
    f.boundary(53.7, -6.4, 53.8, -6.3, 10, "Ballyboy");
    for (const auto& [lat, west, name] : {std::tuple{53.56, -6.57, "Bog Road"}, std::tuple{53.55, -6.36, "Main Street"}}) {
        const int64_t a = f.node(lat, west), b = f.node(lat, west + 0.02);
        TestObject street;
        street.type = OsmType::Way;
        street.id = f.next_way++;
        street.refs = {a, b};
        street.tags = {{"highway", "residential"}, {"name", name}};
        f.ways.push_back(street);
    }
    const std::vector<std::string> answers = answers_of(
        f, "locality-25", {{53.5601, -6.56}, {53.5501, -6.35}, {53.75, -6.35}, {53.51, -6.51}, {53.51, -6.31}});
    REQUIRE(answers.size() == 5);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
    CHECK(answers[0].find("Bog Road") != std::string::npos);
    CHECK(answers[0].find("Rathmore") == std::string::npos);
    CHECK(answers[1].find("Main Street") != std::string::npos);
    CHECK(answers[1].find("Town Parks") == std::string::npos);
    CHECK(answers[2].find("Ballyboy") != std::string::npos);
    CHECK(answers[3].find("\"locality\":\"Rathmore\"") != std::string::npos);
    CHECK(answers[3].find("\"display_name\":\"Rathmore, County Meath, Ireland\"") != std::string::npos);
    CHECK(answers[4].find("\"locality\":\"Town Parks\"") != std::string::npos);
}

TEST(a_boundary_links_one_node_of_its_wikidata_its_namesake_first) {
    // Springfield's wikidata is also on a suburb node inside it: find_linked_place takes one
    // node, the one of its name (:323-337), and the suburb stays a place of its own.
    Fixture f;
    f.country(30, -100, 45, -70, "United States", "US");
    f.boundary(38, -91, 42, -87, 4, "Illinois");
    f.boundary(39.6, -89.8, 39.9, -89.5, 8, "Springfield", {{"wikidata", "Q28515"}});
    f.node(39.78, -89.65, {{"place", "city"}, {"name", "Springfield"}, {"wikidata", "Q28515"}});
    f.node(39.70, -89.60, {{"place", "suburb"}, {"name", "Springfield Centre"}, {"wikidata", "Q28515"}});
    const std::vector<std::string> answers = answers_of(f, "wikidata-one", {{39.701, -89.601}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "suburb", "Springfield Centre"));
    CHECK(names(answers[0], "city", "Springfield"));
    std::printf("    %s\n", answers[0].c_str());
}

TEST(a_boundary_links_only_a_node_it_covers_by_name) {
    // Northfield (admin_level 8) links no namesake village node outside it, however near: the
    // village is a place of its own.
    Fixture f;
    f.country(30, -100, 45, -70, "United States", "US");
    f.boundary(40, -80, 42, -76, 4, "Pennsylvania");
    f.boundary(40.5, -79.0, 40.9, -78.5, 6, "Clearfield County");
    f.boundary(40.70, -78.80, 40.80, -78.70, 8, "Northfield");
    f.node(40.65, -78.75, {{"place", "village"}, {"name", "Northfield"}});
    const std::vector<std::string> answers = answers_of(f, "name-covers", {{40.651, -78.751}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "village", "Northfield"));
    std::printf("    %s\n", answers[0].c_str());
}

TEST(a_place_tagged_boundary_links_a_node_of_its_value_by_part_of_its_name) {
    // Stadt Kreuzberg (admin_level 8, place=city) links the city node Kreuzberg, whose name lies
    // in its own (:339-361), and hides it; a town node of the name, of another value, stays a
    // place of its own.
    Fixture f;
    f.country(47, 6, 55, 15, "Germany", "DE");
    f.boundary(52, 12, 53, 14, 4, "Brandenburg");
    f.boundary(52.4, 13.3, 52.6, 13.5, 8, "Stadt Kreuzberg", {{"place", "city"}});
    f.node(52.50, 13.40, {{"place", "city"}, {"name", "Kreuzberg"}});
    f.node(52.45, 13.35, {{"place", "town"}, {"name", "Kreuzberg"}});
    const ScratchDir root("link-by-type");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, f.objects(), 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, root.path() + "/out", pbf}, log));
    std::map<uint8_t, size_t> visible;
    const std::string dir = root.path() + "/out/planet/";
    const std::vector<char> cells = file_bytes(dir + place_files::CELLS_FILE), nodes = file_bytes(dir + place_files::NODES_FILE),
                            hidden = file_bytes(dir + place_files::HIDDEN_FILE);
    auto u8 = [](const std::vector<char>& v) { return reinterpret_cast<const uint8_t*>(v.data()); };
    place_files::decode(u8(cells), cells.size(), u8(nodes), nodes.size(), u8(hidden), hidden.size(),
                        [&](const place_files::LogicalPlace& p) { visible[p.node.place_type] += place_files::is_visible(p.node); });
    CHECK_EQ(visible[uint8_t(PlaceType::CITY)], size_t(0));
    CHECK_EQ(visible[uint8_t(PlaceType::TOWN)], size_t(1));
    const std::vector<std::string> answers = server_answers(dir, {{52.501, 13.401}}, root.path() + "/answers", log);
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "city", "Stadt Kreuzberg"));
    std::printf("    %s\n", answers[0].c_str());
}

TEST(an_unnamed_label_member_links_its_boundary) {
    // Kilmore (a parish, admin_level 10, 20) has an unnamed village node as its label member:
    // find_linked_place links it all the same, and the parish ranks 16 as a village.
    Fixture f;
    f.country(49, -8, 59, 2, "United Kingdom", "GB");
    f.boundary(50, -6, 56, 2, 4, "England");
    f.boundary(53.0, -3.0, 53.4, -2.4, 6, "Cheshire East");
    const int64_t village = f.node(53.20, -2.70, {{"place", "village"}});
    f.boundary(53.15, -2.75, 53.25, -2.65, 10, "Kilmore", {}, {{OsmType::Node, village, "label"}});
    const std::vector<std::string> answers = answers_of(f, "unnamed-label", {{53.201, -2.701}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "village", "Kilmore"));
    std::printf("    %s\n", answers[0].c_str());
}

TEST(a_place_area_admin_minimal_keeps_by_the_label_of_its_new_link) {
    // A place=suburb multipolygon of the wikidata of a town node inside it links the node
    // (:323-337), which labels it town: admin-minimal, which keeps place areas down to rank 16
    // by the type that labels them, keeps it.
    Fixture f;
    f.country(52, 4.5, 53, 5.5, "Netherlands", "NL");
    f.boundary(52.1, 4.6, 52.9, 5.4, 4, "North Holland");
    f.area(52.295, 5.0, 52.325, 5.06, {{"type", "multipolygon"}, {"place", "suburb"}, {"name", "Weesp"}, {"wikidata", "Q9"}});
    f.node(52.31, 5.04, {{"place", "town"}, {"name", "Weesp"}, {"wikidata", "Q9"}});
    const ScratchDir root("minimal-link-label");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, f.objects(), 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, root.path() + "/out", pbf, "--multi-output", "--multi-quality"}, log));
    const std::vector<char> bytes = file_bytes(root.path() + "/out/planet/admin-minimal/admin_polygons.bin");
    size_t towns = 0;
    for (size_t at = 0; at + sizeof(AdminPolygonRecord) <= bytes.size(); at += sizeof(AdminPolygonRecord)) {
        AdminPolygonRecord r;
        std::memcpy(&r, bytes.data() + at, sizeof r);
        towns += r.name_id != NO_DATA && r.level_override == (15 << 4 | uint8_t(AdminPlaceType::TOWN));
    }
    CHECK_EQ(towns, size_t(1));
}

TEST(a_linked_boundary_goes_by_its_nodes_english_name_where_it_has_none) {
    // Sosnovka (Tambov Oblast): its boundary has no name:en and links its town node, whose
    // name:en Nominatim's English answer gives it (_place_name:en after name:en); the node,
    // hidden, names nothing else. A boundary of a name:en of its own keeps it, and so does the
    // City of Ipswich, England being English-only (the name tag is its name:en there).
    Fixture f;
    f.country(41, 19, 82, 180, "Россия", "RU");
    f.boundary(51.5, 39.5, 54.0, 43.5, 4, "Тамбовская область", {{"name:en", "Tambov Oblast"}});
    const int64_t town = f.node(53.24, 41.37, {{"place", "town"}, {"name", "Сосновка"}, {"name:en", "Sosnovka"}});
    f.boundary(53.20, 41.30, 53.30, 41.45, 8, "Сосновка", {}, {{OsmType::Node, town, "label"}});
    const int64_t kirsanov = f.node(52.65, 42.73, {{"place", "town"}, {"name", "Кирсанов"}, {"name:en", "Kirsanov Town"}});
    f.boundary(52.60, 42.70, 52.70, 42.80, 8, "Кирсанов", {{"name:en", "Kirsanov"}}, {{OsmType::Node, kirsanov, "label"}});
    f.country(49, -8, 59, 2, "United Kingdom", "GB");
    f.boundary(50, -6, 56, 1.9, 4, "England");
    const int64_t ipswich = f.node(52.06, 1.15, {{"place", "town"}, {"name", "Ipswich"}, {"name:en", "Ipswich Town"}});
    f.boundary(52.00, 1.05, 52.10, 1.25, 8, "City of Ipswich", {}, {{OsmType::Node, ipswich, "label"}});
    const std::vector<std::string> answers =
        answers_of(f, "linked-names", {{53.245, 41.375}, {52.655, 42.735}, {52.055, 1.155}});
    REQUIRE(answers.size() == 3);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
    CHECK(names(answers[0], "town", "Sosnovka"));
    CHECK(answers[0].find("Сосновка") == std::string::npos);
    CHECK(names(answers[1], "town", "Kirsanov"));
    CHECK(names(answers[2], "town", "City of Ipswich"));
    CHECK(answers[2].find("Ipswich Town") == std::string::npos);
}

TEST(a_linked_boundary_keeps_its_own_name_beside_the_english_one_it_goes_by) {
    // Sosnovka's boundary goes by its town node's name:en, and keeps its own name, Сосновка, in
    // admin_own_names.bin at its slot, for the addr:city crosscheck and place search: a core
    // string, which a house's addr:city of that name keeps in addr_cities.bin. Кирсанов, of a
    // name:en of its own, keeps none. The planet's dirs and Europe's, of its own strings, alike.
    Fixture f;
    f.country(41, 19, 82, 180, "Россия", "RU");
    f.boundary(51.5, 39.5, 54.0, 43.5, 4, "Тамбовская область", {{"name:en", "Tambov Oblast"}});
    const int64_t town = f.node(53.24, 41.37, {{"place", "town"}, {"name", "Сосновка"}, {"name:en", "Sosnovka"}});
    f.boundary(53.20, 41.30, 53.30, 41.45, 8, "Сосновка", {}, {{OsmType::Node, town, "label"}});
    const int64_t kirsanov = f.node(52.65, 42.73, {{"place", "town"}, {"name", "Кирсанов"}, {"name:en", "Kirsanov Town"}});
    f.boundary(52.60, 42.70, 52.70, 42.80, 8, "Кирсанов", {{"name:en", "Kirsanov"}}, {{OsmType::Node, kirsanov, "label"}});
    f.line({{53.25, 41.36}, {53.25, 41.39}}, {{"highway", "residential"}, {"name", "улица Ленина"}});
    f.node(53.2502, 41.37, {{"addr:housenumber", "1"}, {"addr:street", "улица Ленина"}, {"addr:city", "Сосновка"}});
    const ScratchDir root("own-names");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log", out = root.path() + "/out";
    write_test_pbf(pbf, f.objects(), 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output", "--multi-quality"}, log));
    for (const char* region : {"planet", "europe"}) {
        const std::string dir = out + "/" + region + "/";
        std::string core;
        const std::vector<char> core_file = file_bytes(dir + "full/strings_core.bin");
        string_files::decode(reinterpret_cast<const uint8_t*>(core_file.data()), core_file.size(),
                             [&](const char* p, size_t n) { core.append(p, n); });
        auto core_string = [&](uint32_t id) { return id < core.size() ? std::string(core.c_str() + id) : std::string(); };
        const std::vector<char> polygons = file_bytes(dir + "quality/exact/admin_polygons.bin"),
                                own = file_bytes(dir + "quality/exact/" + kAdminOwnNamesFile);
        REQUIRE(own.size() == sizeof(AdminOwnName));
        CHECK(file_bytes(dir + "quality/simplified/" + kAdminOwnNamesFile) == own);
        AdminOwnName name;
        std::memcpy(&name, own.data(), sizeof name);
        REQUIRE(size_t(name.slot + 1) * sizeof(AdminPolygonRecord) <= polygons.size());
        AdminPolygonRecord polygon;
        std::memcpy(&polygon, polygons.data() + size_t(name.slot) * sizeof polygon, sizeof polygon);
        CHECK_EQ(core_string(polygon.name_id), std::string("Sosnovka"));
        CHECK_EQ(core_string(name.name_id), std::string("Сосновка"));

        const std::vector<char> cities_file = file_bytes(dir + "full/addr_cities.bin");
        std::vector<uint32_t> cities;
        side_blocks::decode("addr_cities.bin", reinterpret_cast<const uint8_t*>(cities_file.data()), cities_file.size(),
                            [&](const uint32_t* v, size_t n) { cities.insert(cities.end(), v, v + n); });
        REQUIRE(cities.size() == 1);
        CHECK_EQ(cities[0], name.name_id);
        if (gctest::failures()) std::printf("    in %s\n", region);
    }
}
