// build-index's place links and postal areas, from a PBF to the server's
// answers: a boundary of rank 0 in its country links no place node, a
// boundary links a place node of its wikidata only where it holds it, a
// place=province area is an address area where its country ranks it, and a
// postal area names a point only of its own country and of a postcode its
// country takes.
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"

namespace {

using Tags = std::vector<std::pair<std::string, std::string>>;

// The objects of a fixture, each kind numbered from its own base.
struct Fixture {
    std::vector<TestObject> nodes, ways, relations;
    int64_t next_node = 1, next_way = 1, next_relation = 1;

    int64_t node(double lat, double lng, Tags tags = {}) {
        TestObject o;
        o.type = OsmType::Node;
        o.id = next_node++;
        o.lat = static_cast<int32_t>(lat * 1e7);
        o.lon = static_cast<int32_t>(lng * 1e7);
        o.tags = std::move(tags);
        nodes.push_back(o);
        return o.id;
    }

    // A closed way round the box (south, west, north, east).
    int64_t square(double s, double w, double n, double e, Tags tags = {}) {
        const int64_t a = node(s, w), b = node(s, e), c = node(n, e), d = node(n, w);
        TestObject o;
        o.type = OsmType::Way;
        o.id = next_way++;
        o.refs = {a, b, c, d, a};
        o.tags = std::move(tags);
        ways.push_back(o);
        return o.id;
    }

    // A relation of the box's way as its outer ring, and its other members.
    int64_t area(double s, double w, double n, double e, Tags tags,
                 std::vector<std::tuple<OsmType, int64_t, std::string>> members = {}) {
        TestObject o;
        o.type = OsmType::Relation;
        o.id = next_relation++;
        o.tags = std::move(tags);
        o.members.emplace_back(OsmType::Way, square(s, w, n, e), "outer");
        for (auto& m : members) o.members.push_back(std::move(m));
        relations.push_back(o);
        return o.id;
    }

    int64_t boundary(double s, double w, double n, double e, int level, const std::string& name, Tags more = {},
                     std::vector<std::tuple<OsmType, int64_t, std::string>> members = {}) {
        Tags tags = {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", std::to_string(level)},
                     {"name", name}};
        tags.insert(tags.end(), more.begin(), more.end());
        return area(s, w, n, e, tags, std::move(members));
    }

    int64_t country(double s, double w, double n, double e, const std::string& name, const std::string& code) {
        return boundary(s, w, n, e, 2, name, {{"ISO3166-1", code}, {"ISO3166-1:alpha2", code}});
    }

    std::vector<TestObject> objects() const {
        std::vector<TestObject> all = nodes;
        all.insert(all.end(), ways.begin(), ways.end());
        all.insert(all.end(), relations.begin(), relations.end());
        return all;
    }
};

// The server's answers (its query_json, through the grid_query example) at
// each point from the index under dir, one JSON line each; none when it
// can't load the index. Its stderr goes to log.
std::vector<std::string> server_answers(const std::string& dir, const std::vector<std::pair<double, double>>& points,
                                        const std::string& scratch, const std::string& log) {
#ifdef GEOCODER_GRID_QUERY_BIN
    const std::string coords = scratch + ".coords", answers = scratch + ".answers";
    {
        std::ofstream f(coords);
        f.precision(10);
        for (const auto& [lat, lng] : points) f << lat << " " << lng << "\n";
    }
    const std::string cmd = gctest::quoted(GEOCODER_GRID_QUERY_BIN) + " " + gctest::quoted(dir) + " < " +
                            gctest::quoted(coords) + " > " + gctest::quoted(answers) + " 2>> " + gctest::quoted(log);
    if (std::system(cmd.c_str()) != 0) return {};
    std::vector<std::string> lines;
    std::ifstream f(answers);
    for (std::string line; std::getline(f, line);) lines.push_back(line);
    return lines;
#else
    (void)dir, (void)points, (void)scratch, (void)log;
    std::printf("    no grid_query: configure with cargo on PATH to build the server's read path\n");
    return {};
#endif
}

// Whether the answer names `value` as address field `field`.
bool names(const std::string& answer, const std::string& field, const std::string& value) {
    return answer.find("\"" + field + "\":\"" + value + "\"") != std::string::npos;
}

// The answers at the points from a build of the fixture, one each.
std::vector<std::string> answers_of(const Fixture& fixture, const std::string& name,
                                    const std::vector<std::pair<double, double>>& points) {
    const ScratchDir root(name);
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture.objects(), 1000);
    const std::string out = root.path() + "/out";
    if (!gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf}, log)) return {};
    return server_answers(out + "/planet", points, root.path() + "/answers", log);
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
