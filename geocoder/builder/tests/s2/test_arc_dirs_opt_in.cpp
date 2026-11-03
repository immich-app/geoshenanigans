// build-index's arc stage is opt-in: without --arcs-out a build writes what
// one with it writes into its output dir, file for file and byte for byte,
// and no arc file anywhere; with it, --arcs-out holds every region's arc
// dirs, and a chained build of the same data (--prev-output, --prev-arcs)
// writes them again unchanged. A chained build of edited data keeps the slot
// of every arc the previous day had, which a fresh build of it doesn't.
#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "id_allocator.h"
#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"

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
// address: every arc dir has polygons, shared borders and a hole. With
// `hole_moved` (the next day's edit), the hole and its village are in the
// opposite municipality: that day has one arc fewer and one more in each
// set, the rest as they were.
std::vector<TestObject> fixture(bool hole_moved = false) {
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
    // In the first municipality, or moved to the last, clear of the city,
    // the street and the address.
    const int32_t near = hole_moved ? 1650000 : 200000, far = hole_moved ? 1850000 : 500000;
    for (int k = 0; k < 4; k++)
        nodes.push_back(node(500 + k, lat0 + (k == 1 || k == 2 ? far : near), lng0 + (k >= 2 ? far : near)));
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
                                         r == c && r == (hole_moved ? 1 : 0) ? std::vector<int64_t>{hole}
                                                                             : std::vector<int64_t>{}));
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

// What --arcs-out holds for every region, as PR 3 ships it.
std::set<std::string> arc_dir_layout() {
    const std::vector<std::string> client = {"admin_polygons.bin", "admin_rings.bin", "arc_index.bin", "arc_vertices.bin"};
    std::set<std::string> out;
    for (const std::string& region : kRegions) {
        for (const char* dir : {"quality/simplified", "quality/exact", "admin-minimal", "admin-minimal-poi"})
            for (const std::string& f : client) out.insert(region + "/" + dir + "/" + f);
        out.insert(region + "/quality/exact/arc_index.osm_ids");
        out.insert(region + "/admin-minimal/arc_index.osm_ids");
        out.insert(region + "/admin-minimal/admin_polygons.osm_ids");
    }
    return out;
}

// An arc sidecar's slots: each live one's arc key, UINT64_MAX for a
// tombstone; none when it can't be read.
std::vector<uint64_t> slot_keys(const std::string& path) {
    gc::id_alloc::IdAllocator alloc;
    if (!alloc.load_previous(path)) return {};
    std::vector<uint64_t> keys;
    for (const gc::id_alloc::SidecarSlot& s : alloc.slots())
        keys.push_back(gc::id_alloc::is_tombstone(s) ? UINT64_MAX : s.stable_id);
    return keys;
}

std::map<uint64_t, size_t> slot_of_key(const std::vector<uint64_t>& keys) {
    std::map<uint64_t, size_t> out;
    for (size_t s = 0; s < keys.size(); s++)
        if (keys[s] != UINT64_MAX) out[keys[s]] = s;
    return out;
}

}  // namespace

TEST(build_index_writes_arc_dirs_only_into_arcs_out_and_a_chained_build_writes_them_again) {
    const ScratchDir root("arc-dirs-opt-in");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture(), 1000);
    auto build = [&](const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string plain = root.path() + "/plain", with = root.path() + "/with", arcs = root.path() + "/arcs";
    REQUIRE(build(plain, {}));
    REQUIRE(build(with, {"--arcs-out", arcs}));

    // The output dir: the same files, byte for byte, none of them an arc file.
    const auto plain_files = tree(plain), with_files = tree(with);
    CHECK(names_of(plain_files) == names_of(with_files));
    CHECK(plain_files == with_files);
    for (const std::string& name : names_of(plain_files)) {
        const std::string file = fs::path(name).filename().string();
        CHECK(file != "admin_rings.bin" && file != "arc_index.bin" && file != "arc_vertices.bin" &&
              file != "arc_index.osm_ids");
    }
    REQUIRE(plain_files.count("planet/quality/uncapped/admin_polygons.bin"));

    // --arcs-out: every region's four dirs and their sidecars, nothing else.
    const auto arc_files = tree(arcs);
    CHECK(names_of(arc_files) == arc_dir_layout());
    auto file = [&](const std::string& region, const std::string& name) { return arc_files.at(region + "/" + name); };
    // The tiers' records and rings are one; the minimal dirs' arcs are one.
    CHECK(file("planet", "quality/simplified/admin_polygons.bin") == file("planet", "quality/exact/admin_polygons.bin"));
    CHECK(file("planet", "quality/simplified/admin_rings.bin") == file("planet", "quality/exact/admin_rings.bin"));
    for (const char* f : {"arc_index.bin", "arc_vertices.bin"})
        CHECK(file("planet", std::string("admin-minimal/") + f) == file("planet", std::string("admin-minimal-poi/") + f));
    // In the shared slots, one 24 B record each.
    CHECK_EQ(file("planet", "quality/exact/admin_polygons.bin").size(),
             plain_files.at("planet/quality/uncapped/admin_polygons.bin").size());
    CHECK(file("planet", "quality/exact/arc_vertices.bin").size() > file("planet", "quality/simplified/arc_vertices.bin").size());
    CHECK(!file("planet", "admin-minimal/admin_rings.bin").empty());
    // Every polygon is in Europe, whose arcs are then the planet's in the
    // same slots; Asia has none.
    for (const char* dir : {"quality/simplified", "quality/exact", "admin-minimal", "admin-minimal-poi"})
        for (const char* f : {"admin_rings.bin", "arc_index.bin", "arc_vertices.bin"})
            CHECK(file("europe", std::string(dir) + "/" + f) == file("planet", std::string(dir) + "/" + f));
    CHECK(file("asia", "quality/exact/arc_vertices.bin").empty());
    CHECK(file("asia", "quality/exact/admin_polygons.bin").empty());

    // The same data again on top of that build: the same arc dirs.
    const std::string again = root.path() + "/again", arcs_again = root.path() + "/arcs-again";
    REQUIRE(build(again, {"--prev-output", with, "--arcs-out", arcs_again, "--prev-arcs", arcs}));
    CHECK(tree(arcs_again) == arc_files);
}

TEST(a_chained_build_of_edited_data_keeps_each_retained_arcs_slot_where_a_fresh_build_moves_some) {
    const ScratchDir root("arc-dirs-chained-edit");
    const std::string day_one = root.path() + "/day1.osm.pbf", day_two = root.path() + "/day2.osm.pbf",
                      log = root.path() + "/build.log";
    write_test_pbf(day_one, fixture(), 1000);
    write_test_pbf(day_two, fixture(true), 1000);
    auto build = [&](const std::string& pbf, const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string out1 = root.path() + "/out1", arcs1 = root.path() + "/arcs1";
    const std::string chained = root.path() + "/chained", arcs_chained = root.path() + "/arcs-chained";
    const std::string fresh = root.path() + "/fresh", arcs_fresh = root.path() + "/arcs-fresh";
    REQUIRE(build(day_one, out1, {"--arcs-out", arcs1}));
    REQUIRE(build(day_two, chained, {"--prev-output", out1, "--arcs-out", arcs_chained, "--prev-arcs", arcs1}));
    REQUIRE(build(day_two, fresh, {"--arcs-out", arcs_fresh}));
    // The chained arc dirs aren't the fresh build's: --prev-arcs was read.
    CHECK(tree(arcs_chained) != tree(arcs_fresh));

    // In each set of the planet and of Europe (which holds every polygon,
    // its own slots against its own previous sidecars).
    for (const char* region : {"planet", "europe"})
        for (const char* sidecar : {"quality/exact/arc_index.osm_ids", "admin-minimal/arc_index.osm_ids"}) {
            const std::string name = std::string(region) + "/" + sidecar;
            const int failures_before = gctest::failures();
            const std::vector<uint64_t> before = slot_keys(arcs1 + "/" + name);
            const std::vector<uint64_t> after = slot_keys(arcs_chained + "/" + name);
            const std::vector<uint64_t> anew = slot_keys(arcs_fresh + "/" + name);
            REQUIRE(!before.empty() && !after.empty() && !anew.empty());
            const auto after_slot = slot_of_key(after), anew_slot = slot_of_key(anew);
            // Every arc both days have keeps its slot chained; a fresh build
            // presents the moved hole elsewhere and shifts some of them.
            size_t retained = 0, kept = 0, kept_fresh = 0;
            for (size_t s = 0; s < before.size(); s++) {
                REQUIRE(before[s] != UINT64_MAX);  // a fresh build's sidecar has no tombstone
                const auto it = after_slot.find(before[s]);
                if (it == after_slot.end()) continue;
                retained++;
                kept += it->second == s;
                const auto f = anew_slot.find(before[s]);
                kept_fresh += f != anew_slot.end() && f->second == s;
            }
            CHECK_EQ(retained, before.size() - 1);  // all but the old hole's
            CHECK_EQ(kept, retained);
            CHECK(kept_fresh < retained);
            // The old hole's slot is a tombstone, not reused the day it is
            // freed; the new hole's comes after the first day's slots.
            CHECK_EQ(after.size(), before.size() + 1);
            CHECK_EQ(static_cast<size_t>(std::count(after.begin(), after.end(), UINT64_MAX)), size_t(1));
            CHECK(after.back() != UINT64_MAX && std::find(before.begin(), before.end(), after.back()) == before.end());
            CHECK_EQ(anew.size(), before.size());
            if (gctest::failures() != failures_before) std::printf("    in %s\n", name.c_str());
        }
}
