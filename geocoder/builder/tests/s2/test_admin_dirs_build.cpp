// build-index's admin dirs: every region's boundary tiers (with
// --multi-quality) and both admin-minimal dirs (with --multi-output) hold
// their admin geometry as arcs, and nothing else does; a chained build of the
// same data (--prev-output) writes them again unchanged. A chained build of
// edited data keeps the slot of every arc the previous day had, which a fresh
// build of it doesn't, and an arc whose key changes keeps its slot by its way
// set or its ends. A previous build without its arc sidecars leaves every
// arc slot cold and the admin slots as they were; one without their fallback
// sidecars, the slots of arcs whose keys changed.
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
// address: every arc dir has polygons, shared borders and a hole. With
// `hole_moved` (the next day's edit), the hole and its village are in the
// opposite municipality, mapped again as a new way of new nodes: that day
// has one arc fewer and one more in each set (no fallback key of the old
// one's the new one's), the rest as they were.
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
    const int64_t hole = hole_moved ? 3001 : 3000, hole_node = hole_moved ? 504 : 500;
    // In the first municipality, or moved to the last, clear of the city,
    // the street and the address.
    const int32_t near = hole_moved ? 1650000 : 200000, far = hole_moved ? 1850000 : 500000;
    for (int k = 0; k < 4; k++)
        nodes.push_back(node(hole_node + k, lat0 + (k == 1 || k == 2 ? far : near), lng0 + (k >= 2 ? far : near)));
    ways.push_back(way(hole, {hole_node, hole_node + 1, hole_node + 2, hole_node + 3, hole_node}));
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

// The next day's edits of the fixture's way 1002, the border from the
// corner (1, 0) between the first and third municipality to the middle
// (1, 1), where four borders meet: its inner nodes moved 3 cm north
// (its arc's smallest vertex with them), the middle moved, or the way
// mapped again as a new one, 1009, on its inner nodes moved.
enum class Edit { InnerNodesMoved, MiddleMoved, WayRemapped };

std::vector<TestObject> edited(Edit edit) {
    std::vector<TestObject> objects = fixture();
    std::set<int64_t> inner;
    for (const TestObject& o : objects)
        if (o.type == OsmType::Way && o.id == 1002) inner.insert(o.refs.begin() + 1, o.refs.end() - 1);
    for (TestObject& o : objects) {
        if (o.type == OsmType::Node && edit != Edit::MiddleMoved && inner.count(o.id)) o.lat += 300;
        if (o.type == OsmType::Node && edit == Edit::MiddleMoved && o.id == 5) {
            o.lat += 2000;
            o.lon -= 1500;
        }
        if (edit != Edit::WayRemapped) continue;
        if (o.type == OsmType::Way && o.id == 1002) o.id = 1009;
        for (auto& [type, id, role] : o.members)
            if (type == OsmType::Way && id == 1002) id = 1009;
    }
    std::stable_sort(objects.begin(), objects.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    return objects;
}

// The admin geometry files and arc sidecars of a build's tree.
std::map<std::string, std::vector<char>> arc_files(const std::string& out) {
    std::map<std::string, std::vector<char>> files = tree(out);
    for (auto it = files.begin(); it != files.end();) {
        const std::string file = fs::path(it->first).filename().string();
        it = admin_geometry(it->first) || file == "arc_index.osm_ids" ? std::next(it) : files.erase(it);
    }
    return files;
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

TEST(an_admin_polygon_is_built_whatever_simplifying_would_leave_of_its_ring) {
    // A closed-way village whose six nodes sit at one point: Douglas-Peucker
    // keeps two of its seven vertices at any tolerance, which once dropped
    // the polygon. The exact tier keeps every polygon the ways make: the
    // village is a record of its own, and the build's arc checks pass.
    std::vector<TestObject> objects = fixture();
    std::vector<int64_t> refs;
    for (int64_t id = 600; id < 606; id++) {
        objects.push_back(node(id, 470800000, 80800000));
        refs.push_back(id);
    }
    refs.push_back(refs.front());
    objects.push_back(way(5000, refs, {{"admin_level", "9"}, {"boundary", "administrative"}, {"name", "Punkt"}}));
    std::stable_sort(objects.begin(), objects.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    const ScratchDir root("admin-dirs-one-point");
    const std::string pbf = root.path() + "/fixture.osm.pbf", log = root.path() + "/build.log", out = root.path() + "/out";
    write_test_pbf(pbf, objects, 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf, "--multi-output", "--multi-quality"}, log));
    const auto files = tree(out);
    for (const char* dir : {"quality/simplified", "quality/exact"})
        CHECK_EQ(files.at(std::string("planet/") + dir + "/admin_polygons.bin").size(), 8 * sizeof(AdminPolygon));
    const std::vector<char> strings = files.at("planet/full/strings_core.bin");
    CHECK(std::search(strings.begin(), strings.end(), std::begin("Punkt"), std::end("Punkt")) != strings.end());
}

TEST(a_chained_build_of_edited_data_keeps_each_retained_arcs_slot_where_a_fresh_build_moves_some) {
    const ScratchDir root("admin-dirs-chained-edit");
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
    const std::string out1 = root.path() + "/out1", chained = root.path() + "/chained",
                      fresh = root.path() + "/fresh";
    REQUIRE(build(day_one, out1, {}));
    REQUIRE(build(day_two, chained, {"--prev-output", out1}));
    REQUIRE(build(day_two, fresh, {}));
    // The chained arcs aren't the fresh build's: --prev-output's arc
    // sidecars were read.
    CHECK(arc_files(chained) != arc_files(fresh));

    // In each set of the planet and of Europe (which holds every polygon,
    // its own slots against its own previous sidecars).
    for (const char* region : {"planet", "europe"})
        for (const char* sidecar : {"quality/exact/arc_index.osm_ids", "admin-minimal/arc_index.osm_ids"}) {
            const std::string name = std::string(region) + "/" + sidecar;
            const int failures_before = gctest::failures();
            const std::vector<uint64_t> before = slot_keys(out1 + "/" + name);
            const std::vector<uint64_t> after = slot_keys(chained + "/" + name);
            const std::vector<uint64_t> anew = slot_keys(fresh + "/" + name);
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

TEST(a_chained_build_on_a_previous_build_without_arc_sidecars_allocates_every_arc_slot_cold) {
    const ScratchDir root("admin-dirs-cold-arcs");
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
    const std::string out1 = root.path() + "/out1", prev = root.path() + "/prev", cold = root.path() + "/cold",
                      chained = root.path() + "/chained", fresh = root.path() + "/fresh";
    REQUIRE(build(day_one, out1, {}));
    // The previous build as it is but for its arc sidecars, every region's.
    fs::copy(out1, prev, fs::copy_options::recursive);
    size_t removed = 0;
    for (const auto& e : fs::recursive_directory_iterator(prev))
        if (e.path().filename() == "arc_index.osm_ids") removed++;
    for (const std::string& region : kRegions)
        for (const char* sidecar : {"/quality/exact/arc_index.osm_ids", "/admin-minimal/arc_index.osm_ids"})
            fs::remove(prev + "/" + region + sidecar);
    REQUIRE(removed > 0);
    REQUIRE(build(day_two, cold, {"--prev-output", prev}));
    const std::vector<char> text = bytes_of(log);
    const std::string said(text.begin(), text.end());
    for (const char* set : {"full", "minimal"}) {
        CHECK(said.find(std::string("ARC_CHECK slots=arcs set=") + set + " region=planet previous_sidecar_missing=1") !=
              std::string::npos);
        CHECK(said.find(std::string("planet arcs: no previous ") + set + " arc slots at " + prev + "/planet/") !=
              std::string::npos);
    }
    CHECK(said.find("region=europe previous_sidecar_missing=1") != std::string::npos);
    REQUIRE(build(day_two, chained, {"--prev-output", out1}));
    REQUIRE(build(day_two, fresh, {}));
    // Its arcs are a fresh build's, slots and all; its admin slots the chained build's.
    auto arcs_only = [](std::map<std::string, std::vector<char>> files) {
        for (auto it = files.begin(); it != files.end();) {
            const std::string file = fs::path(it->first).filename().string();
            it = file == "arc_index.bin" || file == "arc_vertices.bin" || file == "arc_index.osm_ids" ? std::next(it)
                                                                                                      : files.erase(it);
        }
        return files;
    };
    CHECK(arcs_only(arc_files(cold)) == arcs_only(arc_files(fresh)));
    CHECK(arcs_only(arc_files(cold)) != arcs_only(arc_files(chained)));
    for (const std::string& region : {std::string("planet"), std::string("europe")}) {
        const std::string sidecar = region + "/quality/exact/admin_polygons.osm_ids";
        CHECK(bytes_of(cold + "/" + sidecar) == bytes_of(chained + "/" + sidecar));
    }
}

TEST(a_chained_build_keeps_the_slot_of_an_arc_whose_key_changes_by_its_way_set_or_its_ends) {
    const ScratchDir root("admin-dirs-fallbacks");
    const std::string day_one = root.path() + "/day1.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(day_one, fixture(), 1000);
    auto build = [&](const std::string& pbf, const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string out1 = root.path() + "/out1";
    REQUIRE(build(day_one, out1, {}));
    // Each set's arcs' fallback keys beside its slots.
    const auto files = tree(out1);
    for (const std::string& region : kRegions) {
        const bool held = kRegionsHeld.count(region) > 0;
        CHECK(files.count(region + "/quality/exact/arc_index.fallback.osm_ids"));
        CHECK_EQ(files.count(region + "/admin-minimal/arc_index.fallback.osm_ids") > 0, held);
        CHECK(!files.count(region + "/quality/simplified/arc_index.fallback.osm_ids"));
        CHECK(!files.count(region + "/admin-minimal-poi/arc_index.fallback.osm_ids"));
    }

    struct Case {
        Edit edit;
        size_t changed, by_way_set, by_ends;
        const char* name;
    };
    for (const Case& c : {Case{Edit::InnerNodesMoved, 1, 1, 0, "inner nodes moved"},
                          Case{Edit::MiddleMoved, 4, 4, 0, "middle moved"},
                          Case{Edit::WayRemapped, 1, 0, 1, "way remapped"}}) {
        const std::string pbf = root.path() + "/day2.osm.pbf", out = root.path() + "/out2";
        fs::remove_all(out);
        write_test_pbf(pbf, edited(c.edit), 1000);
        REQUIRE(build(pbf, out, {"--prev-output", out1}));
        const std::vector<char> text = bytes_of(log);
        const std::string said(text.begin(), text.end());
        const int failures_before = gctest::failures();
        // In each set of the planet and of Europe (every polygon, its own
        // slots against its own previous sidecars): every slot held both
        // days, the changed arcs' by their new keys, none appended.
        for (const char* region : {"planet", "europe"})
            for (const std::string set : {"full", "minimal"}) {
                const std::string sidecar =
                    std::string("/") + region + (set == "full" ? "/quality/exact" : "/admin-minimal") + "/arc_index.osm_ids";
                const std::vector<uint64_t> before = slot_keys(out1 + sidecar), after = slot_keys(out + sidecar);
                REQUIRE(!before.empty());
                CHECK_EQ(after.size(), before.size());
                CHECK_EQ(static_cast<size_t>(std::count(after.begin(), after.end(), UINT64_MAX)), size_t(0));
                size_t changed = 0;
                for (size_t s = 0; s < std::min(before.size(), after.size()); s++) changed += after[s] != before[s];
                CHECK_EQ(changed, c.changed);
                CHECK(said.find("ARC_CHECK fallbacks=arcs set=" + set + " region=" + region +
                                " previous_fallback_sidecar_missing=0 by_way_set=" + std::to_string(c.by_way_set) +
                                " by_ends=" + std::to_string(c.by_ends) + "\n") != std::string::npos);
                if (gctest::failures() != failures_before) std::printf("    in %s %s\n", region, sidecar.c_str());
            }
        if (gctest::failures() != failures_before) std::printf("    with the %s\n", c.name);
    }
}

TEST(a_chained_build_on_a_previous_build_without_arc_fallback_sidecars_takes_slots_by_arc_keys_alone) {
    const ScratchDir root("admin-dirs-first-fallback-night");
    const std::string day_one = root.path() + "/day1.osm.pbf", day_two = root.path() + "/day2.osm.pbf",
                      log = root.path() + "/build.log";
    write_test_pbf(day_one, fixture(), 1000);
    write_test_pbf(day_two, edited(Edit::InnerNodesMoved), 1000);
    auto build = [&](const std::string& pbf, const std::string& out, std::vector<std::string> extra) {
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out, pbf, "--continents", "--multi-output",
                                          "--multi-quality"};
        words.insert(words.end(), extra.begin(), extra.end());
        return gctest::run(words, log);
    };
    const std::string out1 = root.path() + "/out1", prev = root.path() + "/prev", out2 = root.path() + "/out2";
    REQUIRE(build(day_one, out1, {}));
    // The previous build as it is but for its arcs' fallback sidecars, every region's.
    fs::copy(out1, prev, fs::copy_options::recursive);
    std::vector<fs::path> fallbacks;
    for (const auto& e : fs::recursive_directory_iterator(prev))
        if (e.path().filename() == "arc_index.fallback.osm_ids") fallbacks.push_back(e.path());
    REQUIRE(fallbacks.size() == kRegions.size() + kRegionsHeld.size());
    for (const fs::path& p : fallbacks) fs::remove(p);
    REQUIRE(build(day_two, out2, {"--prev-output", prev}));
    const std::vector<char> text = bytes_of(log);
    const std::string said(text.begin(), text.end());
    for (const char* region : {"planet", "europe"})
        for (const std::string set : {"full", "minimal"}) {
            const std::string dir = std::string("/") + region + (set == "full" ? "/quality/exact" : "/admin-minimal");
            CHECK(said.find("ARC_CHECK fallbacks=arcs set=" + set + " region=" + region +
                            " previous_fallback_sidecar_missing=1 by_way_set=0 by_ends=0\n") != std::string::npos);
            CHECK(said.find(std::string(region) + " arcs: no previous " + set + " arc fallback keys beside " + prev + dir +
                            "/arc_index.osm_ids, slots by arc keys alone") != std::string::npos);
            // The moved arc is a new one: its old slot a tombstone, its new one appended.
            const std::vector<uint64_t> before = slot_keys(out1 + dir + "/arc_index.osm_ids"),
                                        after = slot_keys(out2 + dir + "/arc_index.osm_ids");
            REQUIRE(!before.empty());
            CHECK_EQ(after.size(), before.size() + 1);
            CHECK_EQ(static_cast<size_t>(std::count(after.begin(), after.end(), UINT64_MAX)), size_t(1));
            // And it writes its own fallback keys for the next night.
            CHECK(fs::exists(out2 + dir + "/arc_index.fallback.osm_ids"));
        }
}
