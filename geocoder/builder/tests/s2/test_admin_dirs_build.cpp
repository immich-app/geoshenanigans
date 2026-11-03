// build-index's admin dirs: every region's boundary tiers (with
// --multi-quality) and both admin-minimal dirs (with --multi-output) hold
// their admin geometry as arcs, and nothing else does; a chained build of the
// same data (--prev-output) writes them again unchanged.
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"
#include "types.h"

namespace fs = std::filesystem;

namespace {

TestObject node(int64_t id, int32_t lat, int32_t lon, std::vector<std::pair<std::string, std::string>> tags = {}) {
    TestObject o;
    o.type = OsmType::Node;
    o.id = id;
    o.lat = lat;
    o.lon = lon;
    o.tags = std::move(tags);
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

TestObject boundary(int64_t id, int level, const std::string& name, const std::vector<int64_t>& outer,
                    const std::vector<int64_t>& inner = {}) {
    TestObject o;
    o.type = OsmType::Relation;
    o.id = id;
    o.tags = {{"admin_level", std::to_string(level)}, {"boundary", "administrative"}, {"name", name},
              {"type", "boundary"}};
    if (level == 2) o.tags.push_back({"ISO3166-1", "CH"});
    for (int64_t w : outer) o.members.emplace_back(OsmType::Way, w, "outer");
    for (int64_t w : inner) o.members.emplace_back(OsmType::Way, w, "inner");
    return o;
}

// A country of 2 x 2 municipalities near Zurich, one with a hole a village
// of another level fills, a region over them all, a city, a street and an
// address: every arc dir has polygons, shared borders and a hole.
std::vector<TestObject> fixture() {
    const int32_t lat0 = 470000000, lng0 = 80000000, side = 1000000;
    std::vector<TestObject> nodes, ways, relations;
    auto corner = [](int r, int c) { return int64_t(1 + 3 * r + c); };
    for (int r = 0; r <= 2; r++)
        for (int c = 0; c <= 2; c++) nodes.push_back(node(corner(r, c), lat0 + r * side, lng0 + c * side));
    int64_t next_node = 100;
    // An edge's way, its 5 inner nodes off the straight line by up to 200 m.
    auto edge = [&](int64_t id, int64_t from, int64_t to, int32_t lat_a, int32_t lng_a, int32_t lat_b, int32_t lng_b) {
        std::vector<int64_t> refs = {from};
        for (int k = 1; k <= 5; k++) {
            const int32_t off = ((k * 7 + int(id)) % 5 - 2) * 1000;
            const int32_t lat = lat_a + (lat_b - lat_a) / 6 * k + (lat_a == lat_b ? off : 0);
            const int32_t lng = lng_a + (lng_b - lng_a) / 6 * k + (lng_a == lng_b ? off : 0);
            nodes.push_back(node(next_node, lat, lng));
            refs.push_back(next_node++);
        }
        refs.push_back(to);
        ways.push_back(way(id, refs));
    };
    auto across = [](int r, int c) { return int64_t(1000 + 2 * r + c); };
    auto upward = [](int r, int c) { return int64_t(2000 + 3 * r + c); };
    for (int r = 0; r <= 2; r++)
        for (int c = 0; c < 2; c++)
            edge(across(r, c), corner(r, c), corner(r, c + 1), lat0 + r * side, lng0 + c * side, lat0 + r * side,
                 lng0 + (c + 1) * side);
    for (int r = 0; r < 2; r++)
        for (int c = 0; c <= 2; c++)
            edge(upward(r, c), corner(r, c), corner(r + 1, c), lat0 + r * side, lng0 + c * side, lat0 + (r + 1) * side,
                 lng0 + c * side);
    const int64_t hole = 3000;
    for (int k = 0; k < 4; k++)
        nodes.push_back(node(500 + k, lat0 + (k == 1 || k == 2 ? 500000 : 200000), lng0 + (k >= 2 ? 500000 : 200000)));
    ways.push_back(way(hole, {500, 501, 502, 503, 500}));
    nodes.push_back(node(900, lat0 + 1500000, lng0 + 1500000, {{"name", "Testville"}, {"place", "city"}}));
    nodes.push_back(node(950, lat0 + 1400000, lng0 + 1400000));
    nodes.push_back(node(951, lat0 + 1400000, lng0 + 1600000));
    nodes.push_back(node(952, lat0 + 1410000, lng0 + 1500000, {{"addr:housenumber", "1"}, {"addr:street", "Teststrasse"}}));
    ways.push_back(way(4000, {950, 951}, {{"highway", "residential"}, {"name", "Teststrasse"}}));
    for (int r = 0; r < 2; r++)
        for (int c = 0; c < 2; c++)
            relations.push_back(boundary(10 + 2 * r + c, 8, "Gemeinde " + std::to_string(2 * r + c),
                                         {across(r, c), upward(r, c + 1), across(r + 1, c), upward(r, c)},
                                         r == 0 && c == 0 ? std::vector<int64_t>{hole} : std::vector<int64_t>{}));
    relations.push_back(boundary(20, 10, "Weiler", {hole}));
    const std::vector<int64_t> rim = {across(0, 0), across(0, 1), upward(0, 2), upward(1, 2),
                                      across(2, 1), across(2, 0), upward(1, 0), upward(0, 0)};
    relations.push_back(boundary(30, 4, "Kanton", rim));
    relations.push_back(boundary(40, 2, "Testland", rim));
    std::sort(nodes.begin(), nodes.end(), [](const TestObject& a, const TestObject& b) { return a.id < b.id; });
    std::vector<TestObject> all = nodes;
    all.insert(all.end(), ways.begin(), ways.end());
    all.insert(all.end(), relations.begin(), relations.end());
    return all;
}

std::vector<char> bytes_of(const fs::path& p) {
    std::ifstream f(p, std::ios::binary);
    return std::vector<char>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

// Every file under dir by its path there.
std::map<std::string, std::vector<char>> tree(const std::string& dir) {
    std::map<std::string, std::vector<char>> files;
    for (const auto& e : fs::recursive_directory_iterator(dir))
        if (e.is_regular_file()) files[fs::relative(e.path(), dir).string()] = bytes_of(e.path());
    return files;
}

std::set<std::string> names_of(const std::map<std::string, std::vector<char>>& files) {
    std::set<std::string> out;
    for (const auto& [name, bytes] : files) out.insert(name);
    return out;
}

const std::vector<std::string> kRegions = {"planet",        "africa",        "asia",
                                           "europe",        "north-america", "south-america",
                                           "oceania",       "central-america", "antarctica"};

// The regions with polygons and place nodes: only these have admin-minimal.
const std::set<std::string> kRegionsHeld = {"planet", "europe"};

// The admin geometry files of every region's boundary tiers and, where it
// has place nodes, its admin-minimal dirs.
std::set<std::string> admin_dir_layout() {
    const std::vector<std::string> client = {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin"};
    std::set<std::string> out;
    for (const std::string& region : kRegions)
        for (const char* dir : {"quality/simplified", "quality/exact", "admin-minimal", "admin-minimal-poi"})
            if (kRegionsHeld.count(region) || std::string(dir).rfind("quality/", 0) == 0)
                for (const std::string& f : client) out.insert(region + "/" + dir + "/" + f);
    return out;
}

bool admin_geometry(const std::string& name) {
    const std::string file = fs::path(name).filename().string();
    return file == "admin_polygons.bin" || file == "admin_rings.bin" || file == "arc_index.bin" ||
           file == "arc_vertices.bin";
}

}  // namespace

TEST(build_index_writes_every_regions_admin_dirs_as_arcs_and_a_chained_build_writes_them_again) {
    const ScratchDir root("admin-dirs-build");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
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

    // Admin geometry in the four admin dirs of every region and nowhere else:
    // no quality ladder, no keyhole vertices, no postal files.
    std::set<std::string> geometry;
    for (const std::string& name : names_of(files)) {
        if (admin_geometry(name)) geometry.insert(name);
        const std::string file = fs::path(name).filename().string();
        CHECK(file != "admin_vertices.bin" && file.rfind("postal_", 0) != 0);
        CHECK(name.find("/quality/q") == std::string::npos && name.find("/quality/uncapped") == std::string::npos);
    }
    CHECK(geometry == admin_dir_layout());
    // The full set's slot sidecars beside the exact tier, the minimal set's
    // in admin-minimal/.
    for (const std::string& region : kRegions) {
        const bool held = kRegionsHeld.count(region) > 0;
        CHECK_EQ(files.count(region + "/quality/exact/admin_polygons.osm_ids") > 0, held);
        CHECK(files.count(region + "/quality/exact/arc_index.osm_ids"));
        CHECK_EQ(files.count(region + "/admin-minimal/arc_index.osm_ids") > 0, held);
        CHECK(!files.count(region + "/quality/simplified/admin_polygons.osm_ids"));
        CHECK(!files.count(region + "/admin-minimal-poi/arc_index.osm_ids"));
    }
    auto file = [&](const std::string& region, const std::string& name) { return files.at(region + "/" + name); };
    // The tiers' records and rings are one; the minimal dirs' arcs are one.
    CHECK(file("planet", "quality/simplified/admin_polygons.bin") == file("planet", "quality/exact/admin_polygons.bin"));
    CHECK(file("planet", "quality/simplified/admin_rings.bin") == file("planet", "quality/exact/admin_rings.bin"));
    for (const char* f : {"arc_index.bin", "arc_vertices.bin"})
        CHECK(file("planet", std::string("admin-minimal/") + f) == file("planet", std::string("admin-minimal-poi/") + f));
    // In the shared slots, one 24 B record each: the seven boundaries.
    CHECK_EQ(file("planet", "quality/exact/admin_polygons.bin").size(), 7 * sizeof(AdminPolygon));
    CHECK(file("planet", "quality/exact/arc_vertices.bin").size() > file("planet", "quality/simplified/arc_vertices.bin").size());
    CHECK(!file("planet", "admin-minimal/admin_rings.bin").empty());
    // Every polygon is in Europe, whose arcs are then the planet's in the
    // same slots; Asia has none.
    for (const char* dir : {"quality/simplified", "quality/exact", "admin-minimal", "admin-minimal-poi"})
        for (const char* f : {"admin_rings.bin", "arc_index.bin", "arc_vertices.bin"})
            CHECK(file("europe", std::string(dir) + "/" + f) == file("planet", std::string(dir) + "/" + f));
    CHECK(file("asia", "quality/exact/arc_vertices.bin").empty());
    CHECK(file("asia", "quality/exact/admin_polygons.bin").empty());

    // The same data again on top of that build: the same files.
    const std::string again = root.path() + "/again";
    REQUIRE(build(again, {"--prev-output", first}));
    CHECK(tree(again) == files);
}

TEST(multi_quality_takes_no_scale_list_and_without_it_no_boundary_tier_is_written) {
    const ScratchDir root("admin-dirs-flags");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture(), 1000);
    CHECK(!gctest::run({GEOCODER_BUILD_INDEX_BIN, root.path() + "/scales", pbf, "--multi-output", "--multi-quality",
                        "0,0.5,2.5"},
                       log));
    CHECK(bytes_of(log).size() > 0);
    const std::vector<char> refusal = bytes_of(log);
    CHECK(std::string(refusal.begin(), refusal.end()).find("--multi-quality takes no scale list") != std::string::npos);

    const std::string out = root.path() + "/no-tiers";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf, "--multi-output"}, log));
    const auto files = tree(out);
    for (const std::string& name : names_of(files)) CHECK(name.find("/quality/") == std::string::npos);
    // admin-minimal still holds its arcs.
    for (const char* dir : {"admin-minimal", "admin-minimal-poi"})
        for (const char* f : {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin"})
            CHECK(files.count(std::string("planet/") + dir + "/" + f));
    CHECK(!files.at("planet/admin-minimal/arc_vertices.bin").empty());
}
