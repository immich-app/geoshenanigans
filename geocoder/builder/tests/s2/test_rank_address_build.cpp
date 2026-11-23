// build-index's ranks, from a PBF to the server's answers: each boundary,
// place area and place node of an address at the rank_address Nominatim's
// placex_update gives it, which the builder computes (rank_address.h) and the
// server reads, laid out as the objects each rule was found on. The ranks are
// the same in every dir of a build, a chained one's too.
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
