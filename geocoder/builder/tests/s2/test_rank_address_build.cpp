// build-index's ranks, from a PBF to the server's answers: each boundary,
// place area and place node of an address at the rank_address Nominatim's
// placex_update gives it, which the builder computes (rank_address.h) and the
// server reads, laid out as the objects each rule was found on. The ranks are
// the same in every dir of a build, a chained one's too.
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "pbf_fixture.h"
#include "place_files.h"
#include "test_framework.h"
#include "types.h"

namespace {

using Members = std::vector<std::tuple<OsmType, int64_t, std::string>>;

// A boundary relation of the given levels, names and tags round one shared way: boundaries
// replicated on several levels, as Poland and Belgium tag them.
void replicated(Fixture& f, double s, double w, double n, double e, const std::vector<std::pair<int, Members>>& levels,
                const std::string& name, const Tags& more) {
    const int64_t way = f.square(s, w, n, e);
    for (const auto& [level, members] : levels) {
        TestObject o;
        o.type = OsmType::Relation;
        o.id = f.next_relation++;
        o.tags = {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", std::to_string(level)}, {"name", name}};
        o.tags.insert(o.tags.end(), more.begin(), more.end());
        o.members.emplace_back(OsmType::Way, way, "outer");
        o.members.insert(o.members.end(), members.begin(), members.end());
        f.relations.push_back(o);
    }
}

// A boundary relation round the box (south, west, north, east) in whole e7 units.
int64_t boundary_e7(Fixture& f, int32_t s, int32_t w, int32_t n, int32_t e, int level, const std::string& name) {
    const int64_t a = f.node_e7(s, w), b = f.node_e7(s, e), c = f.node_e7(n, e), d = f.node_e7(n, w);
    TestObject way;
    way.type = OsmType::Way;
    way.id = f.next_way++;
    way.refs = {a, b, c, d, a};
    f.ways.push_back(way);
    TestObject o;
    o.type = OsmType::Relation;
    o.id = f.next_relation++;
    o.tags = {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", std::to_string(level)}, {"name", name}};
    o.members.emplace_back(OsmType::Way, way.id, "outer");
    f.relations.push_back(o);
    return o.id;
}

// The answer at the point from a build of the fixture.
std::string answer_of(const Fixture& f, const std::string& name, double lat, double lng) {
    const std::vector<std::string> answers = answers_of(f, name, {{lat, lng}});
    if (answers.size() != 1) return "";
    std::printf("    %s\n", answers[0].c_str());
    return answers[0];
}

bool displays(const std::string& answer, const std::string& display_name) {
    return answer.find("\"display_name\":\"" + display_name + "\"") != std::string::npos;
}

std::vector<char> file_bytes(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::vector<char>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

// The rank_address of each admin level's records in a dir's admin_polygons.bin (tombstones
// left out), and of each place type's visible rows in its place files.
struct DirRanks {
    std::multimap<uint8_t, uint8_t> by_level, by_place_type;
};

DirRanks ranks_in(const std::string& dir) {
    DirRanks out;
    const std::vector<char> records = file_bytes(dir + "/admin_polygons.bin");
    for (size_t at = 0; at + sizeof(AdminPolygonRecord) <= records.size(); at += sizeof(AdminPolygonRecord)) {
        AdminPolygonRecord r;
        std::memcpy(&r, records.data() + at, sizeof r);
        if (r.name_id != NO_DATA) out.by_level.insert({uint8_t(r.level_override >> 4), r.rank_address});
    }
    const std::vector<char> cells = file_bytes(dir + "/" + place_files::CELLS_FILE),
                            nodes = file_bytes(dir + "/" + place_files::NODES_FILE),
                            hidden = file_bytes(dir + "/" + place_files::HIDDEN_FILE);
    if (nodes.empty()) return out;
    auto u8 = [](const std::vector<char>& v) { return reinterpret_cast<const uint8_t*>(v.data()); };
    place_files::decode(u8(cells), cells.size(), u8(nodes), nodes.size(), u8(hidden), hidden.size(),
                        [&](const place_files::LogicalPlace& p) {
                            if (place_files::is_visible(p.node)) out.by_place_type.insert({p.node.place_type, p.node.rank_address});
                        });
    return out;
}

// Winsford (R1434434), a parish (admin_level 10) linked to its town node: 16, where its level
// gives 20, under Cheshire West and Chester (admin_level 6, 12). The village node of Clive in it
// is promoted to 18.
Fixture winsford() {
    Fixture f;
    f.country(49, -8, 59, 2, "United Kingdom", "GB");
    f.boundary(50, -6, 56, 2, 4, "England");
    f.boundary(53.0, -3.0, 53.4, -2.4, 6, "Cheshire West and Chester");
    const int64_t town = f.node(53.19, -2.52, {{"place", "town"}, {"name", "Winsford"}});
    f.boundary(53.15, -2.55, 53.22, -2.48, 10, "Winsford", {}, {{OsmType::Node, town, "label"}});
    f.node(53.200, -2.500, {{"place", "village"}, {"name", "Clive"}});
    return f;
}

}  // namespace

TEST(a_boundary_nests_under_the_relation_round_its_point_on_surface) {
    // Bugisa sub-region (R5940222, admin_level 6) lies round Mbale City (R14698374, admin_level 4,
    // linked to its city node: 16), fifteen times its size, with its point on surface outside
    // it: Nominatim ranks Bugisa 12, a county, under the Eastern Region alone. So rank 18 is left
    // to Somero, a village node promoted inside Mbale City.
    Fixture f;
    f.country(0, 33, 3, 36, "Uganda", "UG");
    f.boundary(0.5, 33.5, 2.5, 35.5, 4, "Eastern Region");
    f.boundary(0.9, 34.0, 2.0, 35.0, 6, "Bugisa");
    const int64_t mbale = f.node(1.08, 34.17, {{"place", "city"}, {"name", "Mbale"}});
    f.boundary(1.0, 34.1, 1.2, 34.3, 4, "Mbale City", {}, {{OsmType::Node, mbale, "label"}});
    f.node(1.085, 34.180, {{"place", "village"}, {"name", "Somero"}});
    const std::string answer = answer_of(f, "rank-mbale", 1.084, 34.178);
    CHECK(names(answer, "village", "Somero"));
    CHECK(names(answer, "city", "Mbale City"));
    CHECK(names(answer, "county", "Bugisa"));
    CHECK(displays(answer, "Somero, Mbale City, Bugisa, Eastern Region, Uganda"));
}

TEST(boundaries_repeating_their_parents_wikidata_and_outline_rank_0) {
    // Lublin's admin_level 6, 7 and 8 relations share one outline and wikidata (Q37333). The
    // level 6 one, indexed first, links the city node and ranks 16 by it; the level 7 and 8
    // ones find the node linked, and repeat their parent: they rank 0, in no address.
    // Zemborzyce (admin_level 9, place=suburb) inside them ranks 20 under the level 6 one, not
    // 22, and a hamlet node in it 22, a rank no boundary holds.
    Fixture f;
    f.country(49, 14, 55, 24, "Poland", "PL");
    f.boundary(50, 21, 52, 24, 4, "Lubelskie");
    f.node(51.25, 22.57, {{"place", "city"}, {"name", "Lublin"}, {"wikidata", "Q37333"}});
    replicated(f, 51.14, 22.45, 51.30, 22.67, {{6, {}}, {7, {}}, {8, {}}}, "Lublin", {{"wikidata", "Q37333"}});
    f.boundary(51.15, 22.50, 51.20, 22.60, 9, "Zemborzyce", {{"place", "suburb"}});
    f.node(51.170, 22.550, {{"place", "hamlet"}, {"name", "Zemborzyce Dolne"}});
    const std::string answer = answer_of(f, "rank-lublin", 51.172, 22.552);
    CHECK(names(answer, "suburb", "Zemborzyce"));
    CHECK(names(answer, "hamlet", "Zemborzyce Dolne"));
    CHECK(names(answer, "city", "Lublin"));
    CHECK(displays(answer, "Zemborzyce Dolne, Zemborzyce, Lublin, Lubelskie, Poland"));
}

TEST(a_linked_place_lowers_a_boundarys_rank) {
    // Winsford ranks 16 by its town node, not 20, and the village node of Clive in it 18: the
    // answer has both.
    const std::string answer = answer_of(winsford(), "rank-winsford", 53.198, -2.502);
    CHECK(names(answer, "village", "Clive"));
    CHECK(names(answer, "town", "Winsford"));
    CHECK(displays(answer, "Clive, Winsford, Cheshire West and Chester, England, United Kingdom"));
}

TEST(an_own_place_tag_labels_and_ranks_a_boundary_that_links_nothing) {
    // Baralla (R342114, admin_level 8, place=municipality) links nothing, its village node being
    // of another wikidata: its own tag labels it municipality and ranks it 14, below its level's
    // 16 (placex_triggers.sql:1157-1172). The village node of Baralla in it ranks 18, promoted by
    // the boundary's place rank, 16, and the two Baralla meet in the line.
    Fixture f;
    f.country(36, -9, 44, 3, "Spain", "ES");
    f.boundary(42, -9, 44, -6.7, 4, "Galicia");
    f.boundary(42.5, -7.9, 43.8, -6.8, 6, "Lugo");
    f.boundary(42.85, -7.30, 42.95, -7.20, 8, "Baralla", {{"place", "municipality"}, {"wikidata", "Q1442553"}});
    f.node(42.890, -7.250, {{"place", "village"}, {"name", "Baralla"}, {"wikidata", "Q5858186"}});
    const std::string answer = answer_of(f, "rank-baralla", 42.891, -7.251);
    CHECK(names(answer, "municipality", "Baralla"));
    CHECK(names(answer, "village", "Baralla"));
    CHECK(displays(answer, "Baralla, Lugo, Galicia, Spain"));
}

TEST(a_historic_place_area_keeps_its_place_rank) {
    // Belfast: County Antrim (R1119534, boundary=historic, place=county) is a place area of rank
    // 12 round Belfast City District (admin_level 7, place=city: 16), and the County Borough of
    // Belfast (boundary=historic, place=city) one of 16 inside it, which no polygon of its rank
    // holds: the county comes after the city district.
    Fixture f;
    f.country(49, -8.2, 59, 2, "United Kingdom", "GB");
    f.boundary(54, -8.2, 55.4, -5.4, 4, "Northern Ireland");
    f.area(54.3, -6.6, 55.3, -5.6,
           {{"type", "boundary"}, {"boundary", "historic"}, {"admin_level", "6"}, {"place", "county"}, {"name", "County Antrim"}});
    f.boundary(54.53, -6.1, 54.66, -5.8, 7, "Belfast City District", {{"place", "city"}});
    f.area(54.55, -6.05, 54.65, -5.85,
           {{"type", "boundary"}, {"boundary", "historic"}, {"place", "city"}, {"name", "County Borough of Belfast"},
            {"name:en", "Belfast City"}});
    const std::string answer = answer_of(f, "rank-belfast", 54.597, -5.930);
    CHECK(names(answer, "county", "County Antrim"));
    CHECK(names(answer, "city", "Belfast City District"));
    CHECK(displays(answer, "Belfast City District, County Antrim, Northern Ireland, United Kingdom"));
}

TEST(every_boundary_holding_the_point_ranks_and_a_replicated_one_ranks_0) {
    // Brussels: Bruxelles - Brussel at admin_level 9 repeats Brussels (admin_level 8, linked to
    // the city node, Q239) and ranks 0. At admin_level 10, R2404021 (18, named apart here)
    // lies round Quays Quarter (linked to a neighbourhood: 24); Nominatim's address has both,
    // and the Pentagon (boundary=place, place=suburb, 20) between them.
    Fixture f;
    f.country(49.5, 2.5, 51.5, 6.4, "Belgium", "BE");
    f.boundary(50.75, 4.24, 50.92, 4.48, 4, "Brussels-Capital");
    const int64_t city = f.node(50.846, 4.352, {{"place", "city"}, {"name", "Brussels"}, {"wikidata", "Q239"}});
    replicated(f, 50.79, 4.30, 50.90, 4.42, {{8, {{OsmType::Node, city, "label"}}}}, "Brussels", {{"wikidata", "Q239"}});
    // Its own relation of the same outline at level 9.
    replicated(f, 50.79, 4.30, 50.90, 4.42, {{9, {}}}, "Bruxelles - Brussel", {{"wikidata", "Q239"}});
    f.boundary(50.83, 4.33, 50.87, 4.38, 10, "City of Brussels");
    const int64_t quays = f.node(50.851, 4.349, {{"place", "neighbourhood"}, {"name", "Quays Quarter"}});
    f.boundary(50.849, 4.346, 50.853, 4.352, 10, "Quays Quarter", {}, {{OsmType::Node, quays, "label"}});
    f.area(50.836, 4.335, 50.860, 4.375, {{"type", "boundary"}, {"boundary", "place"}, {"place", "suburb"}, {"name", "Pentagon"}});
    const std::string answer = answer_of(f, "rank-brussels", 50.8505, 4.3488);
    CHECK(names(answer, "neighbourhood", "Quays Quarter"));
    CHECK(names(answer, "suburb", "Pentagon"));
    CHECK(names(answer, "city_district", "City of Brussels"));
    CHECK(names(answer, "city", "Brussels"));
    CHECK(displays(answer, "Quays Quarter, Pentagon, City of Brussels, Brussels, Brussels-Capital, Belgium"));
}

TEST(boundaries_inside_a_city_area_nest_under_it) {
    // Singapore: the place=city area R17140517 has the country's outline (16). Central Region
    // (admin_level 5, place=borough) inside it ranks 18, River Valley (6, place=suburb) 20 under
    // it, and One Tree Hill (7, place=neighbourhood) 24.
    Fixture f;
    f.country(1.2, 103.6, 1.47, 104.0, "Singapore", "SG");
    f.area(1.2, 103.6, 1.47, 104.0, {{"type", "multipolygon"}, {"place", "city"}, {"name", "Singapore"}});
    f.boundary(1.25, 103.75, 1.36, 103.90, 5, "Central Region", {{"place", "borough"}});
    f.boundary(1.28, 103.82, 1.31, 103.85, 6, "River Valley", {{"place", "suburb"}});
    f.boundary(1.300, 103.825, 1.305, 103.832, 7, "One Tree Hill", {{"place", "neighbourhood"}});
    const std::string answer = answer_of(f, "rank-singapore", 1.3017, 103.8269);
    CHECK(names(answer, "neighbourhood", "One Tree Hill"));
    CHECK(names(answer, "suburb", "River Valley"));
    CHECK(names(answer, "borough", "Central Region"));
    CHECK(names(answer, "city", "Singapore"));
    CHECK(displays(answer, "One Tree Hill, River Valley, Central Region, Singapore"));
}

TEST(levels_11_and_12_are_boundaries_and_so_are_closed_ways_from_level_5) {
    // Germany: a level 11 and a level 12 relation inside a level 10 one are parts of the address
    // of a house on a street in them, at 22 and 24. Nominatim 5.3.0 imports a named closed
    // administrative way as a boundary, its place tag in its extratags, and drops one of level 4
    // or less (placex_insert): a level 6 way with place=town is a boundary its tag ranks 16 and
    // labels town, one of level 6 without a place tag a county of 12, as Iferouane (W1333677402)
    // is in Niger, and one of level 4 with place=town names nothing.
    Fixture f;
    f.country(47, 6, 55, 15, "Germany", "DE");
    f.boundary(52, 12, 53, 14, 4, "Brandenburg");
    f.boundary(52.4, 13.3, 52.6, 13.5, 10, "Ortsteil Mitte");
    f.boundary(52.45, 13.35, 52.55, 13.45, 11, "Ortslage Nord");
    f.boundary(52.48, 13.38, 52.52, 13.42, 12, "Block Sieben");
    const int64_t a = f.node(52.500, 13.395), b = f.node(52.500, 13.405);
    TestObject street;
    street.type = OsmType::Way;
    street.id = f.next_way++;
    street.refs = {a, b};
    street.tags = {{"highway", "residential"}, {"name", "Hauptstraße"}};
    f.ways.push_back(street);
    f.node(52.5002, 13.400, {{"addr:housenumber", "7"}, {"addr:street", "Hauptstraße"}});
    f.square(52.70, 12.50, 52.80, 12.60,
             {{"boundary", "administrative"}, {"admin_level", "6"}, {"place", "town"}, {"name", "Wegstadt"}});
    f.square(52.10, 12.10, 52.30, 12.40, {{"boundary", "administrative"}, {"admin_level", "6"}, {"name", "Kreisweg"}});
    f.square(52.05, 13.60, 52.30, 13.90,
             {{"boundary", "administrative"}, {"admin_level", "4"}, {"place", "town"}, {"name", "Landweg"}});
    const std::vector<std::string> answers =
        answers_of(f, "rank-levels-11-12", {{52.5002, 13.4001}, {52.75, 12.55}, {52.2, 12.2}, {52.2, 13.7}});
    REQUIRE(answers.size() == 4);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
    CHECK(answers[0].find("Hauptstraße") != std::string::npos);
    CHECK(answers[0].find("Block Sieben, Ortslage Nord, Ortsteil Mitte, Brandenburg, Germany\"") != std::string::npos);
    CHECK(names(answers[1], "town", "Wegstadt"));
    CHECK(names(answers[2], "county", "Kreisweg"));
    CHECK(displays(answers[2], "Kreisweg, Brandenburg, Germany"));
    CHECK(answers[3].find("Landweg") == std::string::npos);
    CHECK(displays(answers[3], "Brandenburg, Germany"));
}

TEST(an_admin_level_that_is_no_whole_number_of_1_to_15_makes_no_boundary) {
    // Nominatim's import reads admin_level as a number, and one that is none, not a whole one or
    // past 15 is 15 (init.lua :306-311), which ranks 0: such a way or relation is in no address
    // and links no place node. Germany: a level 6 way is a county of 12; ways of "6;7", "6.5"
    // and "262" (6 past 256) and a relation of "6;7" name nothing.
    Fixture f;
    f.country(47, 6, 55, 15, "Germany", "DE");
    f.boundary(52, 12, 53, 14, 4, "Brandenburg");
    f.square(52.10, 12.10, 52.30, 12.40, {{"boundary", "administrative"}, {"admin_level", "6"}, {"name", "Kreisweg"}});
    f.square(52.10, 12.60, 52.30, 12.90, {{"boundary", "administrative"}, {"admin_level", "6;7"}, {"name", "Kreisstrich"}});
    f.square(52.10, 13.10, 52.30, 13.40, {{"boundary", "administrative"}, {"admin_level", "6.5"}, {"name", "Kreishalb"}});
    f.square(52.50, 12.10, 52.70, 12.40, {{"boundary", "administrative"}, {"admin_level", "262"}, {"name", "Kreisrund"}});
    f.area(52.50, 12.60, 52.70, 12.90,
           {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", "6;7"}, {"name", "Kreisbund"}});
    const std::vector<std::string> answers = answers_of(
        f, "rank-admin-level-tag", {{52.2, 12.2}, {52.2, 12.7}, {52.2, 13.2}, {52.6, 12.2}, {52.6, 12.7}});
    REQUIRE(answers.size() == 5);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
    CHECK(names(answers[0], "county", "Kreisweg"));
    for (size_t i = 1; i < answers.size(); i++) CHECK(displays(answers[i], "Brandenburg, Germany"));
}

TEST(the_ranks_are_the_same_in_every_dir_and_in_a_chained_build) {
    // Winsford's build with every region and variant, then a chained build of the same data:
    // each dir holding a level's records holds its rank, and each holding the village node of
    // Clive its promotion. The town node, Winsford's label, is a place of its own in
    // admin-minimal, which keeps no level 10, at its type's 16: Winsford, which it stands for
    // there, promotes it nowhere.
    const ScratchDir root("rank-dirs");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, winsford().objects(), 1000);
    const std::string first = root.path() + "/first", chained = root.path() + "/chained";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, first, pbf, "--continents", "--multi-output", "--multi-quality"}, log));
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, chained, pbf, "--continents", "--multi-output", "--multi-quality",
                         "--prev-output", first},
                        log));
    const std::multimap<uint8_t, uint8_t> levels = {{2, 4}, {4, 8}, {6, 12}, {10, 16}};
    const std::multimap<uint8_t, uint8_t> minimal_levels = {{2, 4}, {4, 8}, {6, 12}};
    const uint8_t village = uint8_t(PlaceType::VILLAGE), town = uint8_t(PlaceType::TOWN);
    size_t dirs = 0;
    for (const std::string& out : {first, chained})
        for (const char* region : {"planet", "europe"}) {
            const std::string at = out + "/" + region + "/";
            for (const char* dir : {"quality/exact", "quality/simplified"}) {
                CHECK(ranks_in(at + dir).by_level == levels);
                dirs++;
            }
            for (const char* dir : {"admin-minimal", "admin-minimal-poi"}) {
                const DirRanks ranks = ranks_in(at + dir);
                CHECK(ranks.by_level == minimal_levels);
                CHECK((ranks.by_place_type == std::multimap<uint8_t, uint8_t>{{village, 18}, {town, 16}}));
                dirs++;
            }
            for (const char* dir : {"full", "no-addresses", "admin"}) {
                CHECK((ranks_in(at + dir).by_place_type == std::multimap<uint8_t, uint8_t>{{village, 18}}));
                dirs++;
            }
        }
    CHECK_EQ(dirs, size_t(28));
}

TEST(place_nodes_rank_by_their_type_in_an_extract_without_boundaries) {
    // An extract of a town and a hamlet and no boundary to rank or promote them: each ranks its
    // type's, 16 and 20, and the answer names the hamlet.
    Fixture f;
    f.node(47.80, 12.10, {{"place", "town"}, {"name", "Rosenheim"}});
    f.node(47.82, 12.14, {{"place", "hamlet"}, {"name", "Aising"}});
    const ScratchDir root("rank-no-boundaries");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log", out = root.path() + "/out";
    write_test_pbf(pbf, f.objects(), 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));
    const uint8_t town = uint8_t(PlaceType::TOWN), hamlet = uint8_t(PlaceType::HAMLET);
    CHECK((ranks_in(out + "/planet").by_place_type == std::multimap<uint8_t, uint8_t>{{town, 16}, {hamlet, 20}}));
    const std::vector<std::string> answers = server_answers(out + "/planet", {{47.8201, 12.1401}}, root.path() + "/answers", log);
    REQUIRE(answers.size() == 1);
    std::printf("    %s\n", answers[0].c_str());
    CHECK(names(answers[0], "hamlet", "Aising"));
}

TEST(a_closed_country_way_names_the_country_and_leaves_the_relation_round_it_its_rank) {
    // Liechtenstein as a closed administrative way of level 2 with its country code, and no
    // relation: Nominatim makes no boundary of such a way, but its answer names the country of
    // the point (get_country_code, _finalize_entry), and so does the server's. Inside Austria's
    // relation a closed level 2 way of its code is the smallest country polygon round the point,
    // and the country the answer names is still the relation's.
    Fixture f;
    f.square(47.05, 9.47, 47.27, 9.64,
             {{"boundary", "administrative"}, {"admin_level", "2"}, {"ISO3166-1", "LI"}, {"ISO3166-1:alpha2", "LI"},
              {"name", "Liechtenstein"}});
    f.country(46.4, 9.7, 49.0, 17.2, "Österreich", "AT");
    f.square(47.20, 10.00, 47.30, 10.20,
             {{"boundary", "administrative"}, {"admin_level", "2"}, {"ISO3166-1", "AT"}, {"ISO3166-1:alpha2", "AT"},
              {"name", "Austria"}});
    const std::vector<std::string> answers = answers_of(f, "rank-country-ways", {{47.16, 9.55}, {47.25, 10.10}, {48.2, 16.37}});
    REQUIRE(answers.size() == 3);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
    CHECK(names(answers[0], "country", "Liechtenstein"));
    CHECK(answers[0].find("\"country_code\":\"LI\"") != std::string::npos);
    CHECK(names(answers[1], "country", "Österreich"));
    CHECK(answers[1].find("\"country_code\":\"AT\"") != std::string::npos);
    CHECK(names(answers[2], "country", "Österreich"));
}

TEST(a_place_node_on_a_relations_edge_or_an_e7_step_inside_it_is_promoted) {
    // Hayama (admin_level 7 in Japan: 16) holds the village node of Isshiki on its north edge,
    // and that of Horiuchi one e7 step inside its east edge at 138.9999997°, whose f32 is 139°,
    // east of the edge. Nominatim's _ST_Covers holds both on their own coordinates and promotes
    // them to 18 (placex_triggers.sql:951-971). A village beyond its west edge stays 16.
    Fixture f;
    f.country(30, 128, 46, 146, "日本", "JP");
    boundary_e7(f, 352000000, 1389000000, 352500000, 1389999998, 7, "Hayama");
    f.node_e7(352500000, 1389500000, {{"place", "village"}, {"name", "Isshiki"}});
    f.node_e7(352200000, 1389999997, {{"place", "village"}, {"name", "Horiuchi"}});
    f.node_e7(352200000, 1388999999, {{"place", "village"}, {"name", "Nagae"}});
    const ScratchDir root("rank-edges");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log", out = root.path() + "/out";
    write_test_pbf(pbf, f.objects(), 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log));
    std::vector<uint8_t> villages;
    for (const auto& [type, rank] : ranks_in(out + "/planet").by_place_type)
        if (type == uint8_t(PlaceType::VILLAGE)) villages.push_back(rank);
    std::sort(villages.begin(), villages.end());
    CHECK((villages == std::vector<uint8_t>{16, 18, 18}));
}

TEST(a_boundary_of_rank_25_answers_where_nothing_nearer_does) {
    // Telfer (R11690317, admin_level 9 in Australia, rank_search 18) links its locality node: 25,
    // in no street's or house's address. Far from any street Nominatim answers with the area
    // itself (reverse.py _lookup_area_address), labelled by the node it links: "Telfer, Western
    // Australia, Australia". On a street in it the street's address leaves it out, and beside a
    // hamlet node in it, of a higher rank_search and near enough, the node answers, its own
    // address leaving it out too.
    Fixture f;
    f.country(-44, 112, -10, 154, "Australia", "AU");
    f.boundary(-35, 112, -13, 129, 4, "Western Australia");
    const int64_t locality = f.node(-21.71, 122.23, {{"place", "locality"}, {"name", "Telfer"}});
    f.boundary(-23.5, 120.0, -19.5, 126.0, 9, "Telfer", {}, {{OsmType::Node, locality, "label"}});
    const int64_t a = f.node(-22.0, 120.99), b = f.node(-22.0, 121.01);
    TestObject road;
    road.type = OsmType::Way;
    road.id = f.next_way++;
    road.refs = {a, b};
    road.tags = {{"highway", "primary"}, {"name", "Telfer Road"}};
    f.ways.push_back(road);
    f.node(-20.0, 125.0, {{"place", "hamlet"}, {"name", "Punmu"}});
    const std::vector<std::string> answers =
        answers_of(f, "rank-area-answer", {{-20.512272, 123.338368}, {-22.0005, 121.0}, {-20.0005, 125.0005}});
    REQUIRE(answers.size() == 3);
    for (const std::string& answer : answers) std::printf("    %s\n", answer.c_str());
    CHECK(displays(answers[0], "Telfer, Western Australia, Australia"));
    CHECK(answers[0].find("\"locality\":\"Telfer\"") != std::string::npos);
    CHECK(displays(answers[1], "Telfer Road, Western Australia, Australia"));
    CHECK(displays(answers[2], "Punmu, Western Australia, Australia"));
}
