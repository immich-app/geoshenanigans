// build-index's place links and postal areas, from a PBF to the server's
// answers: a boundary of rank 0 in its country links no place node, its
// country taken at a point inside it, off every outline that of the cell
// there, and leaves a node of its wikidata or its label to the boundaries
// after it, a boundary or a place area links a place node of its wikidata only
// where it covers it, its edges included, and only one, one of its name first,
// that no boundary before it links, and a node of its name only of no other
// wikidata, a boundary of a place tag of its own, not a linked_place tag, a
// node of that type whose name holds its name or is held in it, and one of
// several parts one node for all of them, a boundary that repeats its parent
// and links no place node is in no address, nor one whose label is a node of
// no address rank, one that links another takes the node's rank, a boundary
// whose label links a node links none of its name, a place=province area is an
// address area where its country ranks it, a postal area names a point only of
// its own country, one for a relation, and of a postcode its country's pattern
// takes, whatever its length, a US ZIP+4 its ZIP, a house, a centroid, a
// postal area and an external centroid take the postcode their country's
// pattern makes of their tag, a house's tag makes its centroid whatever the
// house keeps, a place area linked to a place node of another type ranks by
// its own place tag, a boundary round the smallest of its level at the point
// is in the address as well, and a place node ranked above the boundary of its
// label names the field, and each step of find_linked_place links as
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

TEST(a_postal_area_of_an_extended_postcode_its_country_takes_names_a_point_and_a_street) {
    // Saudi Arabia's pattern ddddd(?:-dddd)? has no output, so Nominatim keeps 12345-6789 whole and
    // makes a postcode area of it (postcodes.py _update_postcode_areas): it names a point inside it
    // and the street through it.
    Fixture f;
    f.country(20, 40, 25, 45, "Saudi Arabia", "SA");
    f.area(22.4, 42.4, 22.6, 42.6, {{"type", "boundary"}, {"boundary", "postal_code"}, {"postal_code", "12345-6789"}});
    f.line({{22.5, 42.45}, {22.5, 42.55}}, {{"highway", "residential"}, {"name", "Shari Al Test"}});
    // One of letters after the hyphen, which the pattern rejects, names nothing: the area of
    // 54321 round it names the point, whose centroid lies farther from it.
    f.area(23.49, 43.49, 23.51, 43.51, {{"type", "boundary"}, {"boundary", "postal_code"}, {"postal_code", "12345-ABCD"}});
    f.area(23.4, 43.4, 23.8, 43.8, {{"type", "boundary"}, {"boundary", "postal_code"}, {"postal_code", "54321"}});
    const std::vector<std::string> answers =
        answers_of(f, "extended-postcode", {{22.45, 42.45}, {22.5002, 42.5}, {23.5, 43.5}});
    REQUIRE(answers.size() == 3);
    CHECK(names(answers[0], "postcode", "12345-6789"));
    CHECK(names(answers[1], "road", "Shari Al Test"));
    CHECK(names(answers[1], "postcode", "12345-6789"));
    CHECK(names(answers[2], "postcode", "54321"));
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

TEST(a_houses_addr_city_takes_a_boundary_by_the_own_name_it_no_longer_goes_by) {
    // Улица Ленина runs from Кирсанов (Kirsanov, of a name:en of its own) into Сосновка, which
    // goes by its town node's name:en, Sosnovka: both rank 16, Kirsanov the house's. Nominatim's
    // names of Сосновка hold its own name beside the node's, so a house in Kirsanov tagged
    // addr:city=Сосновка, as one tagged Sosnovka, puts Sosnovka first at its rank (CrosscheckNames,
    // results.py:606), shown by the name it goes by; an untagged house answers Kirsanov.
    Fixture f;
    f.country(41, 19, 82, 180, "Россия", "RU");
    f.boundary(51.5, 39.5, 54.0, 43.5, 4, "Тамбовская область", {{"name:en", "Tambov Oblast"}});
    const int64_t kirsanov = f.node(53.24, 41.25, {{"place", "town"}, {"name", "Кирсанов"}, {"name:en", "Kirsanov Town"}});
    f.boundary(53.20, 41.20, 53.30, 41.30, 8, "Кирсанов", {{"name:en", "Kirsanov"}}, {{OsmType::Node, kirsanov, "label"}});
    const int64_t town = f.node(53.24, 41.37, {{"place", "town"}, {"name", "Сосновка"}, {"name:en", "Sosnovka"}});
    f.boundary(53.20, 41.30, 53.30, 41.45, 8, "Сосновка", {}, {{OsmType::Node, town, "label"}});
    f.line({{53.25, 41.28}, {53.25, 41.32}}, {{"highway", "residential"}, {"name", "улица Ленина"}});
    const std::vector<std::pair<double, double>> houses = {{53.2502, 41.284}, {53.2502, 41.286}, {53.2502, 41.288}};
    const char* cities[] = {"Сосновка", "Sosnovka", nullptr};
    for (size_t i = 0; i < houses.size(); i++) {
        Tags tags = {{"addr:housenumber", std::to_string(i + 1)}, {"addr:street", "улица Ленина"}};
        if (cities[i]) tags.push_back({"addr:city", cities[i]});
        f.node(houses[i].first, houses[i].second, tags);
    }
    const std::vector<std::string> answers = answers_of(f, "own-name-crosscheck", houses);
    REQUIRE(answers.size() == 3);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
    for (size_t i = 0; i < 2; i++) {
        CHECK(answers[i].find("\"Sosnovka\"") != std::string::npos);
        CHECK(answers[i].find("\"Kirsanov\"") == std::string::npos);
    }
    CHECK(answers[2].find("\"Kirsanov\"") != std::string::npos);
    CHECK(answers[2].find("\"Sosnovka\"") == std::string::npos);
}

TEST(a_boundary_links_a_place_node_of_its_wikidata_on_its_edge) {
    // find_linked_place's wikidata step takes a node the boundary covers (_st_covers), its edges
    // included: a suburb node on a district's north edge, or on the east edge of a district
    // mapped as a closed way, is the district's label and no place of its own.
    Fixture f;
    f.country(-30, 140, -20, 150, "Australia", "AU");
    f.node(-28.5, 142.0, {{"place", "suburb"}, {"name", "North Edge"}, {"wikidata", "Q300"}});
    f.boundary(-29.5, 141, -28.5, 143, 9, "North Edge District", {{"wikidata", "Q300"}});
    f.node(-27.0, 147.0, {{"place", "suburb"}, {"name", "East Edge"}, {"wikidata", "Q301"}});
    Tags east = Fixture::boundary_tags(9, "East Edge District");
    east.emplace_back("wikidata", "Q301");
    f.square(-27.5, 145, -26.5, 147, east);
    const std::vector<std::string> answers = answers_of(f, "wikidata-edge", {{-28.52, 142.0}, {-27.0, 146.98}});
    REQUIRE(answers.size() == 2);
    CHECK(names(answers[0], "suburb", "North Edge District"));
    CHECK(answers[0].find("\"North Edge\"") == std::string::npos);
    CHECK(names(answers[1], "suburb", "East Edge District"));
    CHECK(answers[1].find("\"East Edge\"") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_place_area_relation_links_a_place_node_of_its_wikidata) {
    // find_linked_place runs its wikidata step for a place area as for a boundary
    // (placex_triggers.sql: rank_search < 26, rank_address > 0): Weesp, a place=suburb area of
    // the town node Weesp Town's wikidata, is labelled town by it, hides it, and ranks by its own
    // place tag, the suburb's 20, before Driemond.
    Fixture f;
    f.country(52, 4.5, 53, 5.5, "Netherlands", "NL");
    f.boundary(52.1, 4.6, 52.9, 5.4, 4, "North Holland");
    f.boundary(52.2, 4.8, 52.5, 5.2, 8, "Amsterdam");
    const int64_t amsterdam = f.node(52.30, 4.96, {{"place", "city"}, {"name", "Amsterdam"}});
    f.boundary(52.29, 4.94, 52.32, 5.02, 10, "Amsterdam", {}, {{OsmType::Node, amsterdam, "label"}});
    f.node(52.31, 5.04, {{"place", "town"}, {"name", "Weesp Town"}, {"wikidata", "Q400"}});
    f.area(52.295, 5.0, 52.325, 5.06,
           {{"type", "boundary"}, {"boundary", "place"}, {"place", "suburb"}, {"name", "Weesp"}, {"wikidata", "Q400"}});
    f.node(52.305, 5.015, {{"place", "village"}, {"name", "Driemond"}});
    const std::vector<std::string> answers = answers_of(f, "place-area-wikidata", {{52.31, 5.01}});
    REQUIRE(answers.size() == 1);
    CHECK(answers[0].find("\"display_name\":\"Weesp, Driemond, Amsterdam, North Holland, Netherlands\"") !=
          std::string::npos);
    CHECK(names(answers[0], "town", "Weesp"));
    CHECK(answers[0].find("Weesp Town") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_place_area_links_the_one_place_node_of_its_wikidata_named_as_it) {
    // find_linked_place's wikidata step links one node of the area's wikidata, the first it finds
    // ORDER BY lower(name->'name') = bnd_name desc (placex_triggers.sql:323-336): Riverside, a
    // place=suburb area of Q600, covers the town node Riverside Town and the village node
    // Riverside, both of Q600. It links the village of its own name, which labels it, and the town
    // stays a place of its own, which names the address just outside the area.
    Fixture f;
    f.country(-30, 140, -20, 150, "Australia", "AU");
    f.node(-25.495, 145.0, {{"place", "town"}, {"name", "Riverside Town"}, {"wikidata", "Q600"}});
    f.node(-24.8, 144.8, {{"place", "village"}, {"name", "Riverside"}, {"wikidata", "Q600"}});
    f.area(-25.5, 144.5, -24.5, 145.5,
           {{"type", "boundary"}, {"boundary", "place"}, {"place", "suburb"}, {"name", "Riverside"}, {"wikidata", "Q600"}});
    const std::vector<std::string> answers = answers_of(f, "wikidata-one-node", {{-25.0, 145.0}, {-25.505, 145.0}});
    REQUIRE(answers.size() == 2);
    CHECK(names(answers[0], "village", "Riverside"));
    CHECK(names(answers[1], "town", "Riverside Town"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_place_node_a_boundary_links_is_no_wikidata_link_of_a_place_area) {
    // Lent: the wijk (admin_level 10, rank 16 in the Netherlands), the place=quarter area and the
    // village node Lent all carry Q2068947, and both polygons cover the node. Nominatim indexes the
    // boundaries before the place areas (indexer.py index_full): the wijk links the node, and the
    // area's wikidata step passes it over (linked_place_id is null or = bnd.place_id,
    // placex_triggers.sql:328). The area links nothing and stays a quarter.
    Fixture f;
    f.country(51, 5, 52.5, 6.5, "Netherlands", "NL");
    f.boundary(51.5, 5.5, 52.2, 6.3, 4, "Gelderland");
    f.boundary(51.8, 5.7, 51.95, 5.95, 8, "Nijmegen");
    f.node(51.86, 5.86, {{"place", "village"}, {"name", "Lent"}, {"wikidata", "Q2068947"}});
    f.boundary(51.85, 5.84, 51.88, 5.90, 10, "Lent", {{"wikidata", "Q2068947"}});
    f.area(51.855, 5.85, 51.875, 5.89,
           {{"type", "multipolygon"}, {"place", "quarter"}, {"name", "Lent"}, {"wikidata", "Q2068947"}});
    const std::vector<std::string> answers = answers_of(f, "wikidata-linked-first", {{51.858, 5.87}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "village", "Lent"));
    CHECK(names(answers[0], "quarter", "Lent"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_of_rank_0_in_its_country_leaves_the_place_node_of_its_wikidata_to_a_place_area) {
    // find_linked_place returns at once for a boundary of rank_address 0 (placex_triggers.sql:284):
    // Logan City, a local government area (admin_level 6, rank 0 in Australia), links nothing, and
    // the place=suburb area Logan Central, of the same wikidata, links the town node Logan, which
    // labels it town and leaves the results.
    Fixture f;
    f.country(-30, 140, -20, 154, "Australia", "AU");
    f.boundary(-29, 151, -26, 153.9, 4, "Queensland");
    f.boundary(-27.8, 152.9, -27.5, 153.3, 6, "Logan City", {{"wikidata", "Q700"}});
    f.node(-27.64, 153.11, {{"place", "town"}, {"name", "Logan"}, {"wikidata", "Q700"}});
    f.area(-27.66, 153.08, -27.62, 153.14,
           {{"type", "multipolygon"}, {"place", "suburb"}, {"name", "Logan Central"}, {"wikidata", "Q700"}});
    const std::vector<std::string> answers = answers_of(f, "rank0-wikidata", {{-27.645, 153.105}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "town", "Logan Central"));
    CHECK(answers[0].find("\"Logan\"") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_of_rank_0_in_its_country_leaves_its_label_member_to_a_place_area) {
    // find_linked_place returns at once for a boundary of rank_address 0, its label step too
    // (placex_triggers.sql:284): Logan City (admin_level 6, rank 0 in Australia) names the town
    // node Logan as its label and links nothing, and the place=suburb area Logan Central, of the
    // node's wikidata, links it, which labels it town and leaves the results.
    Fixture f;
    f.country(-30, 140, -20, 154, "Australia", "AU");
    f.boundary(-29, 151, -26, 153.9, 4, "Queensland");
    const int64_t logan = f.node(-27.64, 153.11, {{"place", "town"}, {"name", "Logan"}, {"wikidata", "Q700"}});
    f.boundary(-27.8, 152.9, -27.5, 153.3, 6, "Logan City", {{"wikidata", "Q700"}}, {{OsmType::Node, logan, "label"}});
    f.area(-27.66, 153.08, -27.62, 153.14,
           {{"type", "multipolygon"}, {"place", "suburb"}, {"name", "Logan Central"}, {"wikidata", "Q700"}});
    const std::vector<std::string> answers = answers_of(f, "rank0-label-wikidata", {{-27.645, 153.105}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "town", "Logan Central"));
    CHECK(answers[0].find("\"Logan\"") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_whose_point_lies_off_its_countrys_outline_takes_the_country_of_its_cell) {
    // get_country_code falls back to the country grid where no country polygon covers a place's
    // point, so a boundary's rank, which decides whether it links a place node, is its
    // country's there. Redland City (admin_level 6) reaches past Australia's outline, a point
    // inside it 11 m off, in a cell Australia's boundary crosses: it ranks 0, links nothing, and
    // the place=suburb area Redland Central, of the town node's wikidata, links the node.
    Fixture f;
    f.country(-30, 140, -20, 154, "Australia", "AU");
    f.boundary(-27.66, 153.9992, -27.64, 154.001, 6, "Redland City", {{"wikidata", "Q800"}});
    f.node(-27.65, 153.9995, {{"place", "town"}, {"name", "Redland"}, {"wikidata", "Q800"}});
    f.area(-27.651, 153.9993, -27.649, 153.9998,
           {{"type", "multipolygon"}, {"place", "suburb"}, {"name", "Redland Central"}, {"wikidata", "Q800"}});
    const std::vector<std::string> answers = answers_of(f, "offshore-point", {{-27.6502, 153.9996}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "town", "Redland Central"));
    CHECK(answers[0].find("\"Redland\"") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_that_repeats_its_parent_and_links_no_place_node_is_in_no_address) {
    // placex_update takes a boundary=administrative relation out of addresses (rank_address 0) when
    // the closest boundary round its centroid of a lower admin level, above 3 and of a rank
    // between 1 and 25, has its wikidata and geometry (is_same, placex_triggers.sql:909-931); one
    // that links a place node takes the node's rank back (:1161). Arnhem's gemeente (admin_level 8,
    // rank 14 in the Netherlands) and woonplaats (10, rank 16) run along the same ways and share
    // Q1310: the gemeente, indexed first, links the city node by its wikidata and ranks 16, and the
    // woonplaats, which links nothing, leaves the address, where it was a city_district.
    Fixture f;
    f.country(51, 3.5, 53.5, 7.2, "Netherlands", "NL");
    f.boundary(51.7, 5.0, 52.6, 7.0, 4, "Gelderland");
    const int64_t arnhem = f.square(51.95, 5.85, 52.05, 5.95);
    for (int level : {8, 10}) {
        Tags tags = Fixture::boundary_tags(level, "Arnhem");
        tags.emplace_back("wikidata", "Q1310");
        f.relation(tags, {arnhem});
    }
    f.node(51.98, 5.9, {{"place", "city"}, {"name", "Arnhem"}, {"wikidata", "Q1310"}});
    // Borne's woonplaats shares its gemeente's wikidata, not its outline, so it stays: a
    // city_district under the city the gemeente's link makes it.
    f.boundary(52.25, 6.70, 52.35, 6.85, 8, "Borne", {{"wikidata", "Q2"}});
    f.boundary(52.28, 6.74, 52.32, 6.80, 10, "Borne", {{"wikidata", "Q2"}});
    f.node(52.30, 6.77, {{"place", "city"}, {"name", "Borne"}, {"wikidata", "Q2"}});
    // Almere's woonplaats repeats its gemeente but links the city node, its label member, and so
    // keeps the city's rank; the gemeente, whose wikidata step passes over a label member, stays
    // a municipality.
    const int64_t almere = f.square(52.32, 5.10, 52.42, 5.30);
    const int64_t almere_city = f.node(52.37, 5.22, {{"place", "city"}, {"name", "Almere"}, {"wikidata", "Q3"}});
    Tags gemeente = Fixture::boundary_tags(8, "Almere"), woonplaats = Fixture::boundary_tags(10, "Almere");
    gemeente.emplace_back("wikidata", "Q3");
    woonplaats.emplace_back("wikidata", "Q3");
    f.relation(gemeente, {almere});
    f.relation(woonplaats, {almere}, {{OsmType::Node, almere_city, "label"}});
    const std::vector<std::string> answers =
        answers_of(f, "repeated-boundary", {{51.99, 5.91}, {52.301, 6.771}, {52.371, 5.221}});
    REQUIRE(answers.size() == 3);
    CHECK(names(answers[0], "city", "Arnhem"));
    CHECK(answers[0].find("city_district") == std::string::npos);
    CHECK(answers[0].find("municipality") == std::string::npos);
    CHECK(names(answers[1], "city", "Borne"));
    CHECK(names(answers[1], "city_district", "Borne"));
    CHECK(names(answers[2], "city", "Almere"));
    CHECK(names(answers[2], "municipality", "Almere"));
    CHECK(answers[2].find("city_district") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(boundaries_repeating_their_parent_on_two_levels_are_both_in_no_address) {
    // Warsaw's city (admin_level 6), its county (7) and its gmina (8) run along the same ways and
    // share Q270, the usual tagging in Poland. The city links the city node; the county repeats it
    // and leaves the address, and the gmina, whose closest boundary of a rank above 0 is then the
    // city, leaves it too. The district Śródmieście (9) ranks 18 under the city, a city_district.
    Fixture f;
    f.country(49, 14, 55, 24, "Polska", "PL");
    f.boundary(51, 19, 53.5, 23.5, 4, "województwo mazowieckie");
    const int64_t warsaw = f.square(52.10, 20.85, 52.37, 21.27);
    for (int level : {6, 7, 8}) {
        Tags tags = Fixture::boundary_tags(level, "Warszawa");
        tags.emplace_back("wikidata", "Q270");
        f.relation(tags, {warsaw});
    }
    f.node(52.23, 21.01, {{"place", "city"}, {"name", "Warszawa"}, {"wikidata", "Q270"}});
    f.boundary(52.20, 20.98, 52.26, 21.05, 9, "Śródmieście");
    const std::vector<std::string> answers = answers_of(f, "repeated-twice", {{52.2297, 21.0118}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "city", "Warszawa"));
    CHECK(answers[0].find("\"suburb\":\"Warszawa\"") == std::string::npos);
    CHECK(answers[0].find("\"city_district\":\"Warszawa\"") == std::string::npos);
    CHECK(names(answers[0], "city_district", "Śródmieście"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_that_repeats_its_parent_and_links_a_place_node_takes_the_nodes_rank) {
    // is_same leaves parent_address_level at 3, so a boundary that repeats its parent and links a
    // place node takes the node's rank back where that is above 3 and below 26 (placex_triggers.sql
    // :1161), whatever its parent's. The gmina Nowe Miasto (admin_level 8) is tagged
    // place=neighbourhood and ranks 24; the town boundary along its ways (10), of its wikidata,
    // links the city node its label names and ranks 16, a city, where under a parent of 24 it
    // would rank 25, in no address.
    Fixture f;
    f.country(49, 14, 55, 24, "Polska", "PL");
    f.boundary(51, 19, 53.5, 23.5, 4, "województwo mazowieckie");
    const int64_t ways = f.square(52.00, 20.00, 52.10, 20.20);
    Tags gmina = Fixture::boundary_tags(8, "Nowe Miasto"), town = Fixture::boundary_tags(10, "Nowe Miasto");
    gmina.insert(gmina.end(), {{"wikidata", "Q900"}, {"place", "neighbourhood"}});
    town.emplace_back("wikidata", "Q900");
    const int64_t centre = f.node(52.05, 20.10, {{"place", "city"}, {"name", "Śródmieście Nowego Miasta"}});
    f.relation(gmina, {ways});
    f.relation(town, {ways}, {{OsmType::Node, centre, "label"}});
    const std::vector<std::string> answers = answers_of(f, "repeated-linked", {{52.051, 20.101}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "city", "Nowe Miasto"));
    CHECK(names(answers[0], "neighbourhood", "Nowe Miasto"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_that_repeats_its_parent_and_labels_a_locality_is_in_no_address) {
    // find_linked_place's label step links a node of any place type (placex_triggers.sql:296-309),
    // so the boundary's own place tag counts only where nothing links (:1192). A town boundary
    // tagged place=city along its gmina's ways, of its wikidata, whose label member is a
    // place=locality node, takes the locality's rank, 25, back (:1161): no street or house has an
    // address area of that rank (getNearFeatures' rank_address < maxrank), so the street at the
    // point names no city. (Far from any street the area of rank 25 answers itself, as
    // reverse.py's _lookup_area_address finds it.)
    Fixture f;
    f.country(49, 14, 55, 24, "Polska", "PL");
    f.boundary(51, 19, 53.5, 23.5, 4, "województwo mazowieckie");
    const int64_t ways = f.square(52.00, 20.00, 52.10, 20.20);
    Tags gmina = Fixture::boundary_tags(8, "Nowe Miasto"), town = Fixture::boundary_tags(10, "Stare Miasto");
    gmina.insert(gmina.end(), {{"wikidata", "Q900"}, {"place", "neighbourhood"}});
    town.insert(town.end(), {{"wikidata", "Q900"}, {"place", "city"}});
    const int64_t locality = f.node(52.05, 20.10, {{"place", "locality"}, {"name", "Rynek"}});
    f.relation(gmina, {ways});
    f.relation(town, {ways}, {{OsmType::Node, locality, "label"}});
    f.line({{52.0511, 20.099}, {52.0511, 20.103}}, {{"highway", "residential"}, {"name", "Rynkowa"}});
    const std::vector<std::string> answers = answers_of(f, "repeated-locality", {{52.051, 20.101}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "neighbourhood", "Nowe Miasto"));
    CHECK(answers[0].find("Stare Miasto") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_whose_label_links_a_locality_links_no_place_node_of_its_name) {
    // find_linked_place returns the label member, a node of any place type (placex_triggers.sql
    // :296-309), before its name step: a town boundary of no place tag along its gmina's ways, of
    // its wikidata, labelled by a place=locality node, links the locality and takes its rank, 25,
    // back (:1161), in no address. The place=suburb node of the town's name inside it stays a
    // place of its own and names the suburb.
    Fixture f;
    f.country(49, 14, 55, 24, "Polska", "PL");
    f.boundary(51, 19, 53.5, 23.5, 4, "województwo mazowieckie");
    const int64_t ways = f.square(52.00, 20.00, 52.10, 20.20);
    Tags gmina = Fixture::boundary_tags(8, "Nowe Miasto"), town = Fixture::boundary_tags(10, "Stare Miasto");
    gmina.emplace_back("wikidata", "Q900");
    town.emplace_back("wikidata", "Q900");
    const int64_t locality = f.node(52.05, 20.10, {{"place", "locality"}, {"name", "Rynek"}});
    f.node(52.06, 20.12, {{"place", "suburb"}, {"name", "Stare Miasto"}});
    f.relation(gmina, {ways});
    f.relation(town, {ways}, {{OsmType::Node, locality, "label"}});
    const std::vector<std::string> answers = answers_of(f, "locality-label-name", {{52.061, 20.121}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "suburb", "Stare Miasto"));
    CHECK(names(answers[0], "city", "Nowe Miasto"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_round_the_smallest_of_its_level_at_the_point_is_in_the_address) {
    // Nominatim takes every boundary of a rank above 0 covering the point (getNearFeatures,
    // placex_triggers.sql:1273), one row a rank. Brussels' city (admin_level 8, Q239) links the
    // city node and ranks 16; the level 9 boundary along its ways, of its wikidata, repeats it
    // and ranks 0; Bruxelles - Brussel (10, 18 in Belgium) round Quays Quarter (10,
    // place=neighbourhood, 24) has the city for its parent, keeps 18 and is the city_district.
    Fixture f;
    f.country(49.5, 2.5, 51.5, 6.4, "Belgium", "BE");
    f.boundary(50.75, 4.2, 50.95, 4.5, 4, "Brussels-Capital");
    const int64_t city = f.square(50.80, 4.30, 50.90, 4.42);
    for (int level : {8, 9}) {
        Tags tags = Fixture::boundary_tags(level, "Brussels");
        tags.emplace_back("wikidata", "Q239");
        f.relation(tags, {city});
    }
    f.node(50.846, 4.352, {{"place", "city"}, {"name", "Brussels"}, {"wikidata", "Q239"}});
    f.boundary(50.83, 4.33, 50.87, 4.37, 10, "Bruxelles - Brussel");
    f.boundary(50.848, 4.345, 50.853, 4.352, 10, "Quays Quarter", {{"place", "neighbourhood"}});
    const std::vector<std::string> answers = answers_of(f, "round-the-smallest", {{50.8504, 4.3487}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "city", "Brussels"));
    CHECK(names(answers[0], "city_district", "Bruxelles - Brussel"));
    CHECK(names(answers[0], "neighbourhood", "Quays Quarter"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_place_node_ranked_above_the_boundary_of_its_label_names_the_field) {
    // Upton's parish (admin_level 10, rank 20) is labelled suburb by its rank, and the
    // place=suburb node St Crispin's inside it ranks 22 (placex_triggers.sql:984-1003). The
    // address keeps the first row of a label, the finest first (format_json.py, results.py): the
    // node is the suburb, and display_name joins the parish's row as well.
    Fixture f;
    f.country(50, -6, 55.8, 1.8, "United Kingdom", "GB");
    f.boundary(50.5, -5.5, 55.5, 1.5, 4, "England");
    f.boundary(52.0, -1.3, 52.5, -0.7, 6, "West Northamptonshire");
    f.boundary(52.22, -0.96, 52.245, -0.91, 10, "Upton");
    f.node(52.2325, -0.9335, {{"place", "suburb"}, {"name", "St Crispin's"}});
    const std::vector<std::string> answers = answers_of(f, "label-by-rank", {{52.231927, -0.936684}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "suburb", "St Crispin's"));
    CHECK(answers[0].find("\"display_name\":\"St Crispin's, Upton, West Northamptonshire, England, United Kingdom\"") !=
          std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_place_area_of_a_closed_way_raises_no_place_node_of_its_rank) {
    // Bat Yam (32.012156, 34.756722): the place=suburb way בת ים (W1219784953, rank 20) holds the
    // place=suburb node Ramat HaNasi (rank 20). placex_update raises a place node two ranks only
    // inside a relation of its rank (osm_type = 'R', placex_triggers.sql:984-1004), so the node
    // stays at 20, where the area's row comes first (isguess asc, :576), and the area is the
    // suburb. As a place=suburb relation the area raises the node to 22, the finer row and the
    // suburb.
    const auto suburb_at = [](bool way) {
        Fixture f;
        f.country(29.5, 34.2, 33.3, 35.9, "Israel", "IL");
        f.boundary(31.9, 34.7, 32.2, 34.9, 4, "Tel-Aviv District");
        f.boundary(31.99, 34.73, 32.03, 34.78, 8, "Bat Yam");
        const Tags area = {{"place", "suburb"}, {"name", "Bat Yam West"}};
        if (way) {
            f.square(32.005, 34.745, 32.02, 34.765, area);
        } else {
            Tags tags = area;
            tags.push_back({"type", "multipolygon"});
            f.area(32.005, 34.745, 32.02, 34.765, tags);
        }
        f.node(32.013, 34.7575, {{"place", "suburb"}, {"name", "Ramat HaNasi"}});
        const std::vector<std::string> answers = answers_of(f, way ? "suburb-way" : "suburb-relation", {{32.012156, 34.756722}});
        for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
        return answers;
    };
    const std::vector<std::string> of_way = suburb_at(true);
    REQUIRE(of_way.size() == 1);
    CHECK(names(of_way[0], "suburb", "Bat Yam West"));
    const std::vector<std::string> of_relation = suburb_at(false);
    REQUIRE(of_relation.size() == 1);
    CHECK(names(of_relation[0], "suburb", "Ramat HaNasi"));
}

TEST(a_boundary_with_a_place_tag_of_its_own_links_a_place_node_of_its_name) {
    // Cetinje in planet-b (42.3931, 18.9115): a closed way, admin_level 8, tagged place=town,
    // round the city node Cetinje. find_linked_place's place-type step finds no town node of its
    // name, and its name step takes the city node, of its rank (16) and inside it
    // (placex_triggers.sql:338-381); the boundary's own place tag is the last resort when nothing
    // links (:1190-1205). The boundary is the city, labelled by the node, which it hides.
    Fixture f;
    f.country(41.8, 18.4, 43.6, 20.4, "Montenegro", "ME");
    f.boundary(42.2, 18.7, 42.6, 19.1, 6, "Old Royal Capital Cetinje");
    f.square(42.38, 18.89, 42.40, 18.93, {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", "8"},
                                          {"name", "Cetinje"}, {"place", "town"}});
    f.node(42.3896, 18.9246, {{"place", "city"}, {"name", "Cetinje"}});
    const std::vector<std::string> answers = answers_of(f, "own-place-tag-name-link", {{42.3931, 18.9115}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "city", "Cetinje"));
    CHECK(answers[0].find("\"town\"") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_links_no_place_node_of_its_name_outside_it) {
    // Salzweg (48.615515, 13.511634): the village node Witzmannsberg (N452414137) lies in the
    // Gemeinde Salzweg, about 10 km from the Gemeinde Witzmannsberg (R958107) of its name.
    // find_linked_place's name step takes only a node the boundary covers (ST_Covers,
    // placex_triggers.sql:359-381), so the Gemeinde links nothing and the node is the village of
    // the address round it.
    Fixture f;
    f.country(47.2, 5.8, 55.1, 15.1, "Germany", "DE");
    f.boundary(47.3, 8.9, 50.6, 13.9, 4, "Bavaria");
    f.boundary(48.4, 13.2, 48.8, 13.8, 6, "Landkreis Passau");
    f.boundary(48.58, 13.46, 48.65, 13.56, 8, "Salzweg");
    f.boundary(48.66, 13.36, 48.72, 13.44, 8, "Witzmannsberg");
    f.node(48.6175, 13.5150, {{"place", "village"}, {"name", "Witzmannsberg"}});
    const std::vector<std::string> answers = answers_of(f, "name-link-covers", {{48.615515, 13.511634}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "village", "Witzmannsberg"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_with_a_place_tag_of_its_own_links_a_node_of_that_type_whose_name_its_name_holds) {
    // find_linked_place's place-type step, before its name step, takes a node of the boundary's
    // own place type whose name contains the boundary's or is contained in it, of any rank, that
    // the boundary covers (position(...) > 0, placex_triggers.sql:338-356). Greater Foo,
    // admin_level 8 and place=town, holds the town node Foo and the city node Greater Foo: it
    // links the town, whose type labels it, and the city node, a place of its own, finds rank 16
    // held by the boundary.
    Fixture f;
    f.country(47.2, 5.8, 55.1, 15.1, "Germany", "DE");
    f.boundary(47.3, 8.9, 50.6, 13.9, 4, "Bavaria");
    f.square(48.50, 11.50, 48.60, 11.60, {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", "8"},
                                          {"name", "Greater Foo"}, {"place", "town"}});
    f.node(48.56, 11.56, {{"place", "town"}, {"name", "Foo"}});
    f.node(48.55, 11.55, {{"place", "city"}, {"name", "Greater Foo"}});
    const std::vector<std::string> answers = answers_of(f, "place-type-step-names-held", {{48.551, 11.551}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "town", "Greater Foo"));
    CHECK(answers[0].find("\"city\"") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_with_a_linked_place_tag_takes_no_place_type_step) {
    // The place-type step asks for the boundary's place tag (extratags 'place',
    // placex_triggers.sql:338-356), which a linked_place tag is not. Greater Foo, admin_level 8
    // and linked_place=town, links no town node Foo inside it, of another name, and the node is
    // the town of a point beside the boundary.
    Fixture f;
    f.country(47.2, 5.8, 55.1, 15.1, "Germany", "DE");
    f.boundary(47.3, 8.9, 50.6, 13.9, 4, "Bavaria");
    f.square(48.50, 11.50, 48.60, 11.60, {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", "8"},
                                          {"name", "Greater Foo"}, {"linked_place", "town"}});
    f.node(48.595, 11.595, {{"place", "town"}, {"name", "Foo"}});
    const std::vector<std::string> answers = answers_of(f, "linked-place-no-type-step", {{48.605, 11.605}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "town", "Foo"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_of_two_parts_links_one_place_node_for_both) {
    // Nominatim links a place node to the whole boundary, one row of every part (find_linked_place
    // returns one node, placex_triggers.sql:359-381, placex_update :1141-1180). Bar, admin_level 8,
    // has a part round its village node Bar and another a degree east: the village labels both.
    // Its other part holds a town node Bar too, which the boundary does not link: a place of its
    // own, it finds rank 16 held by the boundary.
    Fixture f;
    f.country(47.2, 5.8, 55.1, 15.1, "Germany", "DE");
    f.boundary(47.3, 7.9, 50.6, 13.9, 4, "Hesse");
    f.relation(Fixture::boundary_tags(8, "Bar"),
               {f.square(50.00, 8.00, 50.10, 8.10), f.square(50.00, 9.00, 50.10, 9.10)});
    f.node(50.05, 8.05, {{"place", "village"}, {"name", "Bar"}});
    f.node(50.02, 9.08, {{"place", "town"}, {"name", "Bar"}});
    const std::vector<std::string> answers = answers_of(f, "two-part-boundary-link", {{50.051, 8.051}, {50.06, 9.04}});
    REQUIRE(answers.size() == 2);
    CHECK(names(answers[0], "village", "Bar"));
    CHECK(names(answers[1], "village", "Bar"));
    CHECK(answers[1].find("\"town\"") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_links_no_place_node_of_its_name_whose_wikidata_is_another) {
    // find_linked_place's name step takes a node of the boundary's name only where either has no
    // wikidata or both the same (placex_triggers.sql:376-377). Ottignies-Louvain-la-Neuve (admin
    // level 8, Q329642) links the town node of its name and wikidata; the town node Louvain-la-
    // Neuve, of Q329642 too, which it leaves, is no name link of the section Louvain-la-Neuve (9,
    // Q665868). The section ranks 16, under the town of 16, and so 18, a city_district.
    Fixture f;
    f.country(49.5, 2.5, 51.5, 6.4, "Belgium", "BE");
    f.boundary(50.5, 4.2, 50.9, 5.0, 6, "Walloon Brabant");
    f.boundary(50.63, 4.52, 50.71, 4.65, 8, "Ottignies-Louvain-la-Neuve", {{"wikidata", "Q329642"}});
    f.node(50.665, 4.57, {{"place", "town"}, {"name", "Ottignies-Louvain-la-Neuve"}, {"wikidata", "Q329642"}});
    f.node(50.668, 4.612, {{"place", "town"}, {"name", "Louvain-la-Neuve"}, {"wikidata", "Q329642"}});
    f.boundary(50.655, 4.59, 50.68, 4.63, 9, "Louvain-la-Neuve", {{"wikidata", "Q665868"}});
    const std::vector<std::string> answers = answers_of(f, "name-link-wikidata", {{50.6684, 4.6005}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "town", "Ottignies-Louvain-la-Neuve"));
    CHECK(names(answers[0], "city_district", "Louvain-la-Neuve"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_concave_boundary_takes_the_country_at_a_point_inside_it) {
    // Nominatim gives a boundary the country at its centroid, ST_PointOnSurface for an area
    // (get_center_point). Gold Coast City (admin_level 6, rank 0 in Australia) wraps round a
    // notch of Australia that another country fills, where the average of its vertices lies: it
    // links no place node all the same, and its label member, the city node of Gold Coast, names
    // the address round it.
    Fixture f;
    Tags australia = Fixture::boundary_tags(2, "Australia");
    australia.insert(australia.end(), {{"ISO3166-1", "AU"}, {"ISO3166-1:alpha2", "AU"}});
    f.relation(australia,
               {f.ring({{-30, 140}, {-30, 150}, {-26, 150}, {-26, 145}, {-24, 145}, {-24, 150}, {-20, 150}, {-20, 140}})});
    f.country(-26, 145, -24, 155, "New Zealand", "NZ");
    f.boundary(-29.5, 141, -26.2, 149.8, 4, "Queensland");
    const int64_t gold_coast = f.node(-26.75, 146.5, {{"place", "city"}, {"name", "Gold Coast"}});
    f.relation(Fixture::boundary_tags(6, "Gold Coast City"),
               {f.ring({{-27, 144}, {-27, 149}, {-26.5, 149}, {-26.5, 144.5}, {-23.5, 144.5}, {-23.5, 149}, {-23, 149},
                        {-23, 144}})},
               {{OsmType::Node, gold_coast, "label"}});
    const std::vector<std::string> answers = answers_of(f, "concave-claimant", {{-26.8, 146.45}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "city", "Gold Coast"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_postal_relation_takes_one_country_at_its_whole_centroid) {
    // Nominatim makes one place_postcode row of a postal relation, its centroid the whole
    // multipolygon's, and gives it that point's country (postcodes.py). 12345 is a large part in
    // Switzerland and a small one in Germany: Swiss, of a postcode Switzerland can't have, it
    // names no point, the German part's included, which takes the address's 1 km off.
    Fixture f;
    f.country(50, 8, 51, 9, "Germany", "DE");
    f.country(51, 8, 52, 9, "Switzerland", "CH");
    f.relation({{"type", "boundary"}, {"boundary", "postal_code"}, {"postal_code", "12345"}},
               {f.square(51.1, 8.2, 51.9, 8.8), f.square(50.8, 8.4, 50.9, 8.6)});
    f.node(50.86, 8.5, {{"addr:postcode", "60311"}, {"addr:housenumber", "1"}, {"addr:street", "Zeil"}});
    const std::vector<std::string> answers = answers_of(f, "postal-relation-country", {{50.85, 8.5}});
    REQUIRE(answers.size() == 1);
    CHECK(answers[0].find("12345") == std::string::npos);
    CHECK(names(answers[0], "postcode", "60311"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_house_and_a_centroid_take_the_postcode_their_countrys_pattern_makes_of_the_tag) {
    // Nominatim stores the postcode the pattern's output makes of a tag (PostcodeFormatter.match,
    // match.expand), the sanitizer's for a house and update_postcodes' for a centroid, which it sums
    // by that postcode. A house tagged DE-10115 answers 10115; DE 10117 and 10117 are one centroid,
    // halfway between their houses; a house of 10-115, whose separator Germany's ddddd has no room
    // for, keeps no postcode of its own, nor makes a centroid.
    Fixture f;
    f.country(50, 8, 51, 9, "Germany", "DE");
    f.node(50.5, 8.5, {{"addr:postcode", "DE-10115"}, {"addr:housenumber", "1"}, {"addr:street", "Zeil"}});
    f.node(50.7, 8.3, {{"addr:postcode", "10117"}, {"addr:housenumber", "2"}, {"addr:street", "Zeil"}});
    f.node(50.7, 8.32, {{"addr:postcode", "DE 10117"}, {"addr:housenumber", "3"}, {"addr:street", "Zeil"}});
    f.node(50.2, 8.2, {{"addr:postcode", "10-115"}, {"addr:housenumber", "4"}, {"addr:street", "Zeil"}});
    const std::vector<std::string> answers =
        answers_of(f, "normalised-house-postcodes", {{50.5, 8.5}, {50.708, 8.318}, {50.2, 8.2}});
    REQUIRE(answers.size() == 3);
    CHECK(names(answers[0], "postcode", "10115"));
    CHECK(names(answers[1], "postcode", "10117"));
    CHECK(answers[2].find("10-115") == std::string::npos);
    CHECK(answers[2].find("10115") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_house_whose_own_postcode_the_sanitizer_rejects_still_makes_a_centroid_of_its_tag) {
    // update_postcodes sums the houses' tags by the postcode PostcodeFormatter makes of them, of the
    // default .* in a country without a pattern (_PostcodeCollector.add), whatever the
    // clean-postcodes sanitizer, of the default [A-Z0-9- ]{3,12}, keeps for the house. Montserrat
    // has no pattern: a house tagged 12.A keeps no postcode of its own and takes
    // get_nearest_postcode's at itself (placex_update), that of the centroid of MSR 1110 500 m west
    // rather than the one of 12.A, 1 km east, that its tag and its neighbour's make, which names a
    // point by it.
    Fixture f;
    f.country(16.6, -62.3, 16.9, -62.1, "Montserrat", "MS");
    f.node(16.75, -62.2, {{"addr:postcode", "12.A"}, {"addr:housenumber", "1"}, {"addr:street", "Old Road"}});
    f.node(16.75, -62.18, {{"addr:postcode", "12.A"}, {"addr:housenumber", "2"}, {"addr:street", "Old Road"}});
    f.node(16.75, -62.205, {{"addr:postcode", "MSR 1110"}, {"addr:housenumber", "3"}, {"addr:street", "Old Road"}});
    const std::vector<std::string> answers =
        answers_of(f, "centroid-of-a-rejected-tag", {{16.75, -62.2}, {16.758, -62.19}});
    REQUIRE(answers.size() == 2);
    CHECK(names(answers[0], "postcode", "MSR 1110"));
    CHECK(answers[0].find("12.A") == std::string::npos);
    CHECK(names(answers[1], "postcode", "12.A"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_postal_area_and_an_external_centroid_take_the_postcode_their_countrys_pattern_makes) {
    // update_postcodes keeps a postal area's and an external centroid's postcode as the pattern's
    // output makes it (m.norm): Latvia's (dddd) gives LV-\1, so the area of 1050 is LV-1050's, and
    // Britain's \1 \2 makes a GeoNames row of sw1a1aa the centroid of SW1A 1AA.
    Fixture f;
    f.country(56, 24, 57, 25, "Latvija", "LV");
    f.area(56.4, 24.4, 56.6, 24.6, {{"type", "boundary"}, {"boundary", "postal_code"}, {"postal_code", "1050"}});
    f.country(51, -1, 52, 0, "United Kingdom", "GB");
    const std::vector<std::string> answers =
        answers_of(f, "normalised-area-postcodes", {{56.5, 24.5}, {51.5, -0.5}},
                   {{"external.csv", "postcode,lat,lon,country_code\nsw1a1aa,51.501,-0.501,GB\n"}},
                   {"--external-postcodes", "{dir}/external.csv"});
    REQUIRE(answers.size() == 2);
    CHECK(names(answers[0], "postcode", "LV-1050"));
    CHECK(names(answers[1], "postcode", "SW1A 1AA"));
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_us_postal_area_of_a_zip_plus_4_names_its_zip) {
    // The US pattern is (ddddd)(?:-dddd)? with output \1 (country_settings.yaml), and Nominatim
    // stores a postcode area's normalised postcode: a postal relation tagged 10001-2062 is the
    // area of 10001.
    Fixture f;
    f.country(40, -75, 41, -73, "United States", "US");
    f.area(40.7, -74.1, 40.8, -73.9, {{"type", "boundary"}, {"boundary", "postal_code"}, {"postal_code", "10001-2062"}});
    const std::vector<std::string> answers = answers_of(f, "zip-plus-4", {{40.75, -74.0}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "postcode", "10001"));
    CHECK(answers[0].find("2062") == std::string::npos);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
}

TEST(a_boundary_whose_name_en_is_empty_goes_by_its_nodes_english_name) {
    // Sosnovka's boundary with an empty name:en: Nominatim's name:en of '' is no name, so the
    // boundary goes by its town node's name:en as one without the tag does.
    Fixture f;
    f.country(41, 19, 82, 180, "Россия", "RU");
    f.boundary(51.5, 39.5, 54.0, 43.5, 4, "Тамбовская область", {{"name:en", "Tambov Oblast"}});
    const int64_t town = f.node(53.24, 41.37, {{"place", "town"}, {"name", "Сосновка"}, {"name:en", "Sosnovka"}});
    f.boundary(53.20, 41.30, 53.30, 41.45, 8, "Сосновка", {{"name:en", ""}}, {{OsmType::Node, town, "label"}});
    const std::vector<std::string> answers = answers_of(f, "empty-name-en", {{53.245, 41.375}});
    REQUIRE(answers.size() == 1);
    std::printf("    %s\n", answers[0].c_str());
    CHECK(names(answers[0], "town", "Sosnovka"));
}
