// build-index's water layer from a PBF: a lake relation with an island, the
// closed-way kinds (a pond, a land-use reservoir, which no other dir reads),
// a river area its waterway relation names and one nothing names, in every
// region's water dirs; the same PBF chained on its own build writes them
// again, and a build without --multi-output writes the exact tier beside its
// mode's files.
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "string_files.h"
#include "test_framework.h"
#include "tool_runs.h"
#include "types.h"

namespace fs = std::filesystem;

namespace {

TestObject node(int64_t id, int32_t lat, int32_t lon) {
    TestObject o;
    o.type = OsmType::Node;
    o.id = id;
    o.lat = lat;
    o.lon = lon;
    return o;
}

TestObject way(int64_t id, std::vector<int64_t> refs, std::vector<std::pair<std::string, std::string>> tags = {}) {
    TestObject o;
    o.type = OsmType::Way;
    o.id = id;
    o.refs = std::move(refs);
    o.tags = std::move(tags);
    return o;
}

TestObject relation(int64_t id, std::vector<std::pair<std::string, std::string>> tags,
                    std::vector<std::pair<int64_t, std::string>> ways) {
    TestObject o;
    o.type = OsmType::Relation;
    o.id = id;
    o.tags = std::move(tags);
    for (const auto& [w, role] : ways) o.members.emplace_back(OsmType::Way, w, role);
    return o;
}

// Water near Zurich, in Europe.
std::vector<TestObject> fixture() {
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> nodes, ways, relations;
    int64_t next = 1;
    // A square's four corner nodes, counterclockwise from (lat, lng).
    auto corners = [&](int32_t lat, int32_t lng, int32_t side) {
        std::vector<int64_t> ids;
        for (const auto& [dl, dg] : std::vector<std::pair<int32_t, int32_t>>{{0, 0}, {0, side}, {side, side}, {side, 0}}) {
            nodes.push_back(node(next, lat + dl, lng + dg));
            ids.push_back(next++);
        }
        return ids;
    };
    // The lake: two ways meeting at its south-west and north-east corners,
    // an island way inside.
    const std::vector<int64_t> lake = corners(lat0, lng0, 100000), isle = corners(lat0 + 40000, lng0 + 40000, 20000);
    ways.push_back(way(101, {lake[0], lake[1], lake[2]}));
    ways.push_back(way(102, {lake[2], lake[3], lake[0]}));
    ways.push_back(way(103, {isle[0], isle[1], isle[2], isle[3], isle[0]}));
    relations.push_back(relation(500, {{"type", "multipolygon"}, {"natural", "water"}, {"water", "lake"},
                                       {"name", "Testsee"}, {"name:en", "Test Lake"}},
                                 {{101, "outer"}, {102, "outer"}, {103, "inner"}}));
    const std::vector<int64_t> pond = corners(lat0 + 200000, lng0, 2000);
    ways.push_back(way(110, {pond[0], pond[1], pond[2], pond[3], pond[0]},
                       {{"natural", "water"}, {"water", "pond"}, {"name", "Weiher"}}));
    const std::vector<int64_t> reservoir = corners(lat0 + 210000, lng0, 2000);
    ways.push_back(way(111, {reservoir[0], reservoir[1], reservoir[2], reservoir[3], reservoir[0]},
                       {{"landuse", "reservoir"}, {"name", "Speicher"}}));
    // A river area and the waterway through it, and an unnamed one nothing
    // runs through.
    const std::vector<int64_t> river = corners(lat0 + 300000, lng0, 10000);
    ways.push_back(way(120, {river[0], river[1], river[2], river[3], river[0]}, {{"natural", "water"}, {"water", "river"}}));
    nodes.push_back(node(next, lat0 + 302000, lng0 + 2000));
    nodes.push_back(node(next + 1, lat0 + 306000, lng0 + 6000));
    ways.push_back(way(121, {next, next + 1}, {{"waterway", "river"}}));
    next += 2;
    relations.push_back(relation(900, {{"type", "waterway"}, {"waterway", "river"}, {"name", "Limmat"}}, {{121, ""}}));
    const std::vector<int64_t> dry = corners(lat0 + 400000, lng0, 10000);
    ways.push_back(way(130, {dry[0], dry[1], dry[2], dry[3], dry[0]}, {{"natural", "water"}, {"water", "river"}}));
    std::vector<TestObject> all = nodes;
    all.insert(all.end(), ways.begin(), ways.end());
    all.insert(all.end(), relations.begin(), relations.end());
    return all;
}

std::vector<char> bytes_of(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    return std::vector<char>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

std::map<std::string, std::vector<char>> tree(const std::string& dir) {
    std::map<std::string, std::vector<char>> files;
    for (const auto& e : fs::recursive_directory_iterator(dir))
        if (e.is_regular_file()) files[fs::relative(e.path(), dir).string()] = bytes_of(e.path());
    return files;
}

// The names a strings_core.bin (FCC2) holds, in its order.
std::vector<std::string> names_in(const std::vector<char>& file) {
    string_files::Decoder d(reinterpret_cast<const uint8_t*>(file.data()), file.size());
    std::vector<char> pool;
    while (d.next()) pool.insert(pool.end(), d.data(), d.data() + d.size());
    std::vector<std::string> out;
    for (size_t at = 0; at < pool.size(); at += out.back().size() + 1) out.push_back(pool.data() + at);
    return out;
}

const std::vector<std::string> kRegions = {"planet", "africa", "asia", "europe", "north-america", "south-america",
                                           "oceania", "central-america", "antarctica"};

}  // namespace

TEST(build_index_writes_every_regions_water_dirs_and_a_chained_build_writes_them_again) {
    const ScratchDir root("water-dirs-build");
    const std::string pbf = root.path() + "/water.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture(), 1000);
    auto build = [&](const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string first = root.path() + "/first";
    REQUIRE(build(first, {}));
    const auto files = tree(first);
    for (const std::string& region : kRegions)
        for (const char* dir : {"water/all/simplified", "water/all/exact", "water/major/simplified"})
            for (const char* f : {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin",
                                  "admin_cells.bin", "admin_entries.bin", "strings_core.bin", "strings_layout.json",
                                  "water_meta.json", "attribution.json"})
                CHECK(files.count(region + "/" + dir + "/" + f));
    // The lake, the pond, the reservoir and the river area its waterway
    // names, in Europe as on the planet; the river area nothing names is out.
    for (const char* region : {"planet", "europe"}) {
        const std::string dir = std::string(region) + "/water/all/exact/";
        CHECK_EQ(files.at(dir + "admin_polygons.bin").size(), 4 * sizeof(AdminPolygonRecord));
        const std::vector<std::string> names = names_in(files.at(dir + "strings_core.bin"));
        CHECK(names == std::vector<std::string>({"Limmat", "Speicher", "Test Lake", "Weiher"}));
        const std::vector<char>& meta = files.at(dir + "water_meta.json");
        const std::string text(meta.begin(), meta.end());
        for (const char* kind : {"\"class\": \"water\", \"type\": \"lake\"", "\"class\": \"water\", \"type\": \"pond\"",
                                 "\"class\": \"landuse\", \"type\": \"reservoir\"",
                                 "\"class\": \"waterway\", \"type\": \"river\""})
            CHECK(text.find(kind) != std::string::npos);
    }
    CHECK(files.at("asia/water/all/exact/admin_polygons.bin").empty());
    CHECK(files.at("planet/water/all/simplified/arc_vertices.bin").size() <=
          files.at("planet/water/all/exact/arc_vertices.bin").size());
    const std::vector<char> said = bytes_of(log);
    const std::string text(said.begin(), said.end());
    CHECK(text.find("ARC_CHECK set=water junctions=") != std::string::npos);
    CHECK(text.find("ARC_CHECK tier=water/all/simplified set=water ") != std::string::npos);

    // The same data again on top of that build: the same files.
    const std::string again = root.path() + "/again";
    REQUIRE(build(again, {"--prev-output", first}));
    CHECK(tree(again) == files);
}

TEST(a_build_without_multi_output_writes_the_exact_water_tier_beside_its_modes_files) {
    const ScratchDir root("water-single-output");
    const std::string pbf = root.path() + "/water.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture(), 1000);
    const std::string single = root.path() + "/single", tiers = root.path() + "/tiers";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, single, pbf}, log));
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, tiers, pbf, "--continents", "--multi-output", "--multi-quality"}, log));
    const auto one = tree(single), multi = tree(tiers);
    for (const char* f : {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin", "admin_cells.bin",
                          "admin_entries.bin", "strings_core.bin", "water_meta.json"}) {
        REQUIRE(one.count(std::string("planet/water/") + f));
        CHECK(one.at(std::string("planet/water/") + f) == multi.at(std::string("planet/water/all/exact/") + f));
    }
    CHECK(!one.count("planet/water/admin_polygons.osm_ids"));
}
