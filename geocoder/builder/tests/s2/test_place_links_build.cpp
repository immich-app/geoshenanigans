// build-index's place links and postal areas, from a PBF to the server's
// answers: a boundary of rank 0 in its country links no place node, its
// country taken at a point inside it, off every outline that of the cell
// there, and leaves a node of its wikidata or its label to the boundaries
// after it, a boundary or a place area links a place node of its wikidata
// only where it covers it, its edges included, and only one, one of its name
// first, that no boundary before it links, and a node of its name only of no
// other wikidata, a boundary that repeats its parent and links no place node
// is in no address, nor one whose label is a node of no address rank, one
// that links another takes the node's rank, a place=province area is an
// address area where its country ranks it, a postal area names a point only
// of its own country, one for a relation, and of a postcode its country's
// pattern takes, whatever its length, a US ZIP+4 its ZIP, a house, a
// centroid, a postal area and an external centroid take the postcode their
// country's pattern makes of their tag, a place area linked to a place node
// of another type ranks by its own place tag, a boundary round the smallest
// of its level at the point is in the address as well, and a place node
// ranked above the boundary of its label names the field.
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

    // A closed way through the points (lat, lng).
    int64_t ring(const std::vector<std::pair<double, double>>& points, Tags tags = {}) {
        TestObject o;
        o.type = OsmType::Way;
        o.id = next_way++;
        for (const auto& [lat, lng] : points) o.refs.push_back(node(lat, lng));
        o.refs.push_back(o.refs.front());
        o.tags = std::move(tags);
        ways.push_back(o);
        return o.id;
    }

    // An open way through the points (lat, lng).
    int64_t line(const std::vector<std::pair<double, double>>& points, Tags tags = {}) {
        TestObject o;
        o.type = OsmType::Way;
        o.id = next_way++;
        for (const auto& [lat, lng] : points) o.refs.push_back(node(lat, lng));
        o.tags = std::move(tags);
        ways.push_back(o);
        return o.id;
    }

    // A closed way round the box (south, west, north, east).
    int64_t square(double s, double w, double n, double e, Tags tags = {}) {
        return ring({{s, w}, {s, e}, {n, e}, {n, w}}, std::move(tags));
    }

    // A relation of the ways as its outer rings, and its other members.
    int64_t relation(Tags tags, const std::vector<int64_t>& outer,
                     std::vector<std::tuple<OsmType, int64_t, std::string>> members = {}) {
        TestObject o;
        o.type = OsmType::Relation;
        o.id = next_relation++;
        o.tags = std::move(tags);
        for (int64_t way : outer) o.members.emplace_back(OsmType::Way, way, "outer");
        for (auto& m : members) o.members.push_back(std::move(m));
        relations.push_back(o);
        return o.id;
    }

    // A relation of the box's way as its outer ring, and its other members.
    int64_t area(double s, double w, double n, double e, Tags tags,
                 std::vector<std::tuple<OsmType, int64_t, std::string>> members = {}) {
        return relation(std::move(tags), {square(s, w, n, e)}, std::move(members));
    }

    int64_t boundary(double s, double w, double n, double e, int level, const std::string& name, Tags more = {},
                     std::vector<std::tuple<OsmType, int64_t, std::string>> members = {}) {
        Tags tags = boundary_tags(level, name);
        tags.insert(tags.end(), more.begin(), more.end());
        return area(s, w, n, e, tags, std::move(members));
    }

    int64_t country(double s, double w, double n, double e, const std::string& name, const std::string& code) {
        return boundary(s, w, n, e, 2, name, {{"ISO3166-1", code}, {"ISO3166-1:alpha2", code}});
    }

    static Tags boundary_tags(int level, const std::string& name) {
        return {{"type", "boundary"}, {"boundary", "administrative"}, {"admin_level", std::to_string(level)},
                {"name", name}};
    }

    std::vector<TestObject> objects() const {
        std::vector<TestObject> all = nodes;
        all.insert(all.end(), ways.begin(), ways.end());
        all.insert(all.end(), relations.begin(), relations.end());
        return all;
    }
};

// Says that `tool` failed and prints its log, which the scratch dir takes
// with it.
void print_failure(const std::string& tool, const std::string& log) {
    const std::vector<char> text = read_file(log);
    std::printf("    %s failed; its log:\n%.*s\n", tool.c_str(), static_cast<int>(text.size()), text.data());
}

// The server's answers (its query_json, through the grid_query example) at
// each point from the index under dir, one JSON line each; none when it
// can't load the index. Its stderr goes to log, printed when it fails.
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
    if (std::system(cmd.c_str()) != 0) {
        print_failure("grid_query", log);
        return {};
    }
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

// The answers at the points from a build of the fixture, one each; none,
// the log printed, when the build fails. `files` are written into the
// scratch dir, by name, before the build, whose further arguments are
// `extra`, each with every "{dir}" in it the scratch dir.
std::vector<std::string> answers_of(const Fixture& fixture, const std::string& name,
                                    const std::vector<std::pair<double, double>>& points,
                                    const std::vector<std::pair<std::string, std::string>>& files = {},
                                    const std::vector<std::string>& extra = {}) {
    const ScratchDir root(name);
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture.objects(), 1000);
    for (const auto& [file, text] : files) std::ofstream(root.path() + "/" + file) << text;
    const std::string out = root.path() + "/out";
    std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf};
    for (std::string word : extra) {
        for (size_t at; (at = word.find("{dir}")) != std::string::npos;) word.replace(at, 5, root.path());
        words.push_back(word);
    }
    if (!gctest::run(words, log)) {
        print_failure("build-index", log);
        return {};
    }
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
    // address area of that rank (getNearFeatures' rank_address < maxrank), so it names no city.
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
    const std::vector<std::string> answers = answers_of(f, "repeated-locality", {{52.051, 20.101}});
    REQUIRE(answers.size() == 1);
    CHECK(names(answers[0], "neighbourhood", "Nowe Miasto"));
    CHECK(answers[0].find("Stare Miasto") == std::string::npos);
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
