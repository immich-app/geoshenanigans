// build-index's water layer from a PBF: a lake relation with an island, the
// closed-way kinds (a pond, a land-use reservoir, which no other dir reads),
// a river area its waterway relation names and one nothing names, in every
// region's water dirs; the same PBF chained on its own build writes them
// again, a build with --marine holds a marine areas file's seas and oceans
// too, and a build without --multi-output writes the exact tier beside its
// mode's files.
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "id_allocator.h"
#include "marine_test_file.h"
#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "string_files.h"
#include "test_framework.h"
#include "tool_runs.h"
#include "types.h"
#include "water_types.h"

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

// The name of each slot of a water dir's admin_polygons.bin, from its
// strings_core.bin: "" for a tombstone.
std::vector<std::string> slot_names(const std::string& dir) {
    const std::vector<char> core = bytes_of(dir + "/strings_core.bin"), records = bytes_of(dir + "/admin_polygons.bin");
    string_files::Decoder d(reinterpret_cast<const uint8_t*>(core.data()), core.size());
    std::vector<char> pool;
    while (d.next()) pool.insert(pool.end(), d.data(), d.data() + d.size());
    std::vector<std::string> out;
    for (size_t at = 0; at + sizeof(AdminPolygonRecord) <= records.size(); at += sizeof(AdminPolygonRecord)) {
        AdminPolygonRecord r;
        std::memcpy(&r, records.data() + at, sizeof r);
        out.push_back(r.name_id == NO_DATA ? "" : r.name_id < pool.size() ? pool.data() + r.name_id : "(out of the pool)");
    }
    return out;
}

// The name of each live slot of a water dir that a closed way holds, by
// the way's id, from the dir's polygon sidecar (admin_polygons.osm_ids).
std::map<int64_t, std::string> names_by_way(const std::string& dir) {
    gc::id_alloc::IdAllocator alloc;
    if (!alloc.load_previous(dir + "/admin_polygons.osm_ids")) return {{0, "(sidecar unread)"}};
    const std::vector<std::string> names = slot_names(dir);
    std::map<int64_t, std::string> out;
    for (size_t s = 0; s < alloc.slots().size(); s++) {
        const gc::id_alloc::SidecarSlot& slot = alloc.slots()[s];
        if (gc::id_alloc::is_tombstone(slot) || slot.object_type != uint8_t(gc::id_alloc::ObjectType::OSM_WAY)) continue;
        out[static_cast<int64_t>(slot.stable_id)] = s < names.size() ? names[s] : "(no record)";
    }
    return out;
}

// Each named slot's kind of a water dir, by its name: the class/type its
// record's type code (country_code) has in the dir's water_meta.json.
std::map<std::string, std::string> kinds_by_name(const std::string& dir) {
    const std::vector<char> meta = bytes_of(dir + "/water_meta.json"), records = bytes_of(dir + "/admin_polygons.bin");
    const std::optional<std::vector<water::MetaType>> kinds = water::parse_meta_json(std::string(meta.begin(), meta.end()));
    if (!kinds) return {{"", "(water_meta.json unread)"}};
    std::map<uint16_t, std::string> by_code;
    for (const water::MetaType& k : *kinds) by_code[k.code] = k.cls + "/" + k.type;
    const std::vector<std::string> names = slot_names(dir);
    std::map<std::string, std::string> out;
    for (size_t s = 0; s < names.size(); s++) {
        if (names[s].empty()) continue;
        AdminPolygonRecord r;
        std::memcpy(&r, records.data() + s * sizeof r, sizeof r);
        const auto it = by_code.find(r.country_code);
        out[names[s]] = it == by_code.end() ? "(no kind)" : it->second;
    }
    return out;
}

const std::vector<std::string> kRegions = {"planet", "africa", "asia", "europe", "north-america", "south-america",
                                           "oceania", "central-america", "antarctica"};

// The objects by type, then id, as a PBF holds them.
std::vector<TestObject> sorted(std::vector<TestObject> objs) {
    std::stable_sort(objs.begin(), objs.end(), [](const TestObject& a, const TestObject& b) {
        return a.type != b.type ? a.type < b.type : a.id < b.id;
    });
    return objs;
}

// The names of the planet's water/all/exact dir of a build-index run on
// `objects` (and `extra` arguments), into dir.
std::vector<std::string> planet_water_names(const std::vector<TestObject>& objects, const std::string& dir,
                                            std::vector<std::string> extra = {}) {
    const std::string pbf = dir + ".osm.pbf";
    write_test_pbf(pbf, sorted(objects), 1000);
    std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, dir, pbf, "--multi-output", "--multi-quality"};
    words.insert(words.end(), extra.begin(), extra.end());
    if (!gctest::run(words, dir + ".log")) return {"build failed"};
    return names_in(bytes_of(dir + "/planet/water/all/exact/strings_core.bin"));
}

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
        // Each of its own kind.
        CHECK(kinds_by_name(first + "/" + dir) == (std::map<std::string, std::string>{{"Limmat", "waterway/river"},
                                                                                     {"Speicher", "landuse/reservoir"},
                                                                                     {"Test Lake", "water/lake"},
                                                                                     {"Weiher", "water/pond"}}));
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

    // The pond gone, on top of the first build: its slot is a tombstone and
    // the others keep theirs, on the planet and in Europe, where a build of
    // its own holds three records.
    std::vector<TestObject> drained;
    for (const TestObject& o : fixture())
        if (o.type != OsmType::Way || o.id != 110) drained.push_back(o);
    const std::string drained_pbf = root.path() + "/drained.osm.pbf", after = root.path() + "/drained";
    write_test_pbf(drained_pbf, drained, 1000);
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, after, drained_pbf, "--continents", "--multi-output", "--multi-quality",
                         "--prev-output", first},
                        log));
    for (const char* region : {"planet", "europe"}) {
        const std::string dir = std::string("/") + region + "/water/all/exact";
        std::vector<std::string> slots = slot_names(first + dir);
        REQUIRE(slots.size() == 4u);
        const auto pond = std::find(slots.begin(), slots.end(), "Weiher");
        REQUIRE(pond != slots.end());
        *pond = "";
        CHECK(slot_names(after + dir) == slots);
    }
}

TEST(a_way_only_the_water_layer_reads_stays_out_of_the_other_layers) {
    // A pond named in English alone, which the way pass resolves for the
    // water layer only (way_needs_nodes reads `name`), and a street of an
    // English name alone that is a member of a water relation.
    const int32_t lat0 = 470000000, lng0 = 80000000;
    // Both builds hold a street of a local name.
    std::vector<TestObject> base = fixture();
    base.push_back(node(8001, lat0 + 700000, lng0));
    base.push_back(node(8002, lat0 + 700000, lng0 + 3000));
    base.push_back(way(8100, {8001, 8002}, {{"highway", "residential"}, {"name", "Teststrasse"}}));
    auto sorted = [](std::vector<TestObject> objs) {
        std::stable_sort(objs.begin(), objs.end(), [](const TestObject& a, const TestObject& b) {
            return a.type != b.type ? a.type < b.type : a.id < b.id;
        });
        return objs;
    };
    std::vector<TestObject> objects = base;
    for (int64_t id = 9001; id <= 9006; id++)
        objects.push_back(node(id, lat0 + 600000 + int32_t(id - 9001) % 2 * 2000, lng0 + int32_t((id - 9001) / 2) * 2000));
    objects.push_back(way(9100, {9001, 9003, 9004, 9002, 9001}, {{"natural", "water"}, {"name:en", "English Pond"}}));
    objects.push_back(way(9101, {9003, 9005, 9006, 9004, 9003}, {{"highway", "residential"}, {"name:en", "English Street"}}));
    objects.push_back(relation(9200, {{"type", "multipolygon"}, {"natural", "wetland"}, {"name", "Ried"}}, {{9101, "outer"}}));
    const ScratchDir root("water-other-layers");
    const std::string pbf = root.path() + "/water.osm.pbf", log = root.path() + "/build.log";
    auto build = [&](const std::vector<TestObject>& objs, const std::string& out) {
        write_test_pbf(pbf, objs, 1000);
        return gctest::run({GEOCODER_BUILD_INDEX_BIN, out, pbf, "--multi-output", "--multi-quality"}, log);
    };
    const std::string with = root.path() + "/with", without = root.path() + "/without";
    REQUIRE(build(sorted(objects), with));
    REQUIRE(build(sorted(base), without));
    const auto a = tree(with), b = tree(without);
    // The water layer holds the pond and the wetland ...
    const std::vector<std::string> names = names_in(a.at("planet/water/all/exact/strings_core.bin"));
    CHECK(std::find(names.begin(), names.end(), "English Pond") != names.end());
    CHECK(std::find(names.begin(), names.end(), "Ried") != names.end());
    // ... and every other file is as it is without them, byte for byte: the
    // same POIs and streets, slot by slot, and their strings and geometry.
    REQUIRE(a.count("planet/poi/all/poi_records.osm_ids") && a.count("planet/full/street_ways.osm_ids"));
    std::set<std::string> outside_water;
    for (const auto* files : {&a, &b})
        for (const auto& [name, bytes] : *files)
            if (name.rfind("planet/water/", 0) != 0) outside_water.insert(name);
    size_t differing = 0;
    for (const std::string& f : outside_water)
        if (!a.count(f) || !b.count(f) || a.at(f) != b.at(f)) {
            std::printf("    %s differs\n", f.c_str());
            differing++;
        }
    CHECK_EQ(differing, size_t(0));
}

TEST(build_index_loads_the_marine_areas_it_is_given_into_the_water_dirs) {
    const ScratchDir root("water-marine-build");
    const std::string marine = gctest::two_areas_path();
    // The ocean and the sea beside the fixture's water, credited to Marine
    // Regions; without the file, neither.
    const std::string with = root.path() + "/with", without = root.path() + "/without";
    CHECK(planet_water_names(fixture(), with, {"--marine", marine}) ==
          std::vector<std::string>({"Limmat", "North Atlantic Ocean", "North Sea", "Speicher", "Test Lake", "Weiher"}));
    CHECK(planet_water_names(fixture(), without) ==
          std::vector<std::string>({"Limmat", "Speicher", "Test Lake", "Weiher"}));
    const std::vector<char> credited = bytes_of(with + "/planet/water/all/exact/attribution.json"),
                            uncredited = bytes_of(without + "/planet/water/all/exact/attribution.json");
    CHECK(std::string(credited.begin(), credited.end()).find("\"id\": \"marine-regions\"") != std::string::npos);
    CHECK(std::string(uncredited.begin(), uncredited.end()).find("\"id\": \"osm\"") != std::string::npos);
    CHECK(std::string(uncredited.begin(), uncredited.end()).find("marine-regions") == std::string::npos);
}

TEST(build_index_refuses_a_marine_areas_file_cut_short_before_it_reads_the_pbf) {
    // Its header as it should be, its body cut short: refused at the start,
    // before the passes over the PBF.
    const ScratchDir root("water-marine-cut-short");
    const std::string good = gctest::two_areas_bytes(), marine = root.path() + "/marine_areas.bin";
    std::ofstream(marine, std::ios::binary) << good.substr(0, good.size() - 3);
    const std::string pbf = root.path() + "/water.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture(), 1000);
    CHECK(!gctest::run({GEOCODER_BUILD_INDEX_BIN, root.path() + "/out", pbf, "--multi-output", "--multi-quality",
                        "--marine", marine},
                       log));
    const std::vector<char> text = bytes_of(log);
    const std::string said(text.begin(), text.end());
    CHECK(said.find("cut short") != std::string::npos);
    CHECK(said.find("Processing " + pbf) == std::string::npos);
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
                          "admin_entries.bin", "strings_core.bin", "strings_layout.json", "water_meta.json",
                          "attribution.json"}) {
        REQUIRE(one.count(std::string("planet/water/") + f));
        CHECK(one.at(std::string("planet/water/") + f) == multi.at(std::string("planet/water/all/exact/") + f));
    }
    CHECK(!one.count("planet/water/admin_polygons.osm_ids"));
}

TEST(a_river_area_shows_its_waterway_relations_name_as_the_datasets_names_go) {
    // name:en, else name, as admin, place and POI names go. A relation with
    // a name:en of its own shows that. One with only a local name shows the
    // name:en its river ways carry over the most length if all its ways
    // with a name:en together cover more than half its river ways' length,
    // else its own name.
    const int32_t lat0 = 350000000, lng0 = 1380000000;
    std::vector<TestObject> objects;
    int64_t next = 1;
    auto at = [&](int32_t lat, int32_t lng) {
        objects.push_back(node(next, lat, lng));
        return next++;
    };
    auto square = [&](int32_t lat, int32_t lng, int32_t side) {
        const int64_t a = at(lat, lng), b = at(lat, lng + side), c = at(lat + side, lng + side), d = at(lat + side, lng);
        return std::vector<int64_t>{a, b, c, d, a};
    };
    objects.push_back(way(100, square(lat0, lng0, 10000), {{"natural", "water"}, {"water", "river"}}));
    objects.push_back(way(101, {at(lat0 + 2000, lng0 + 2000), at(lat0 + 6000, lng0 + 6000)},
                          {{"waterway", "river"}, {"name", "小渋川"}, {"name:en", "Koshibu River"}}));
    objects.push_back(way(102, {at(lat0 + 50000, lng0), at(lat0 + 52000, lng0)},
                          {{"waterway", "river"}, {"name", "小渋川"}, {"name:en", "Koshibu River"}}));
    objects.push_back(way(103, {at(lat0 + 60000, lng0), at(lat0 + 62000, lng0)},
                          {{"waterway", "river"}, {"name", "小渋川"}, {"name:en", "Koshibu-gawa"}}));
    objects.push_back(way(104, {at(lat0 + 70000, lng0), at(lat0 + 72000, lng0)}, {{"waterway", "river"}, {"name", "小渋川"}}));
    objects.push_back(relation(950, {{"type", "waterway"}, {"waterway", "river"}, {"name", "小渋川"}},
                               {{101, ""}, {102, ""}, {103, ""}, {104, ""}}));
    objects.push_back(way(110, square(lat0 + 100000, lng0, 10000), {{"natural", "water"}, {"water", "river"}}));
    objects.push_back(way(111, {at(lat0 + 102000, lng0 + 2000), at(lat0 + 106000, lng0 + 6000)},
                          {{"waterway", "river"}, {"name", "Donau"}, {"name:en", "Danube River"}}));
    objects.push_back(relation(951, {{"type", "waterway"}, {"waterway", "river"}, {"name", "Donau"}, {"name:en", "Danube"}},
                               {{111, ""}}));
    // The Spree: one of its river ways carries the name:en Sprewa, and the
    // longer rest the name Spree alone, or no name.
    objects.push_back(way(120, square(lat0 + 200000, lng0, 10000), {{"natural", "water"}, {"water", "river"}}));
    objects.push_back(way(121, {at(lat0 + 202000, lng0 + 2000), at(lat0 + 206000, lng0 + 6000)},
                          {{"waterway", "river"}, {"name", "Spree"}, {"name:en", "Sprewa"}}));
    objects.push_back(way(122, {at(lat0 + 250000, lng0), at(lat0 + 256000, lng0)}, {{"waterway", "river"}, {"name", "Spree"}}));
    objects.push_back(way(123, {at(lat0 + 260000, lng0), at(lat0 + 263000, lng0)}, {{"waterway", "river"}}));
    objects.push_back(relation(952, {{"type", "waterway"}, {"waterway", "river"}, {"name", "Spree"}}, {{121, ""}, {122, ""}, {123, ""}}));
    // The Havel: the river way through its area, and most of its length,
    // carry the name of a reach, Schnelle Havel, and none a name:en.
    objects.push_back(way(130, square(lat0 + 300000, lng0, 10000), {{"natural", "water"}, {"water", "river"}}));
    objects.push_back(way(131, {at(lat0 + 302000, lng0 + 2000), at(lat0 + 306000, lng0 + 6000)},
                          {{"waterway", "river"}, {"name", "Schnelle Havel"}}));
    objects.push_back(way(132, {at(lat0 + 350000, lng0), at(lat0 + 352000, lng0)}, {{"waterway", "river"}, {"name", "Havel"}}));
    objects.push_back(relation(953, {{"type", "waterway"}, {"waterway", "river"}, {"name", "Havel"}}, {{131, ""}, {132, ""}}));
    const ScratchDir root("water-river-names");
    const std::string out = root.path() + "/out";
    const std::vector<std::string> names = planet_water_names(objects, out);
    CHECK(names == std::vector<std::string>({"Danube", "Havel", "Koshibu River", "Spree"}));
    // Each on its own area.
    CHECK(names_by_way(out + "/planet/water/all/exact") ==
          (std::map<int64_t, std::string>{{100, "Koshibu River"}, {110, "Danube"}, {120, "Spree"}, {130, "Havel"}}));
}

TEST(a_node_the_input_lacks_leaves_a_gap_that_no_water_ring_or_line_crosses) {
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> objects;
    int64_t next = 1;
    auto at = [&](int32_t lat, int32_t lng) {
        objects.push_back(node(next, lat, lng));
        return next++;
    };
    auto square = [&](int32_t lat, int32_t lng, int32_t side) {
        const int64_t a = at(lat, lng), b = at(lat, lng + side), c = at(lat + side, lng + side), d = at(lat + side, lng);
        return std::vector<int64_t>{a, b, c, d, a};
    };
    const int64_t missing = 99999;
    // A lake relation whose way lacks a corner: left out, not closed across
    // the gap.
    std::vector<int64_t> corner_gone = square(lat0, lng0, 10000);
    corner_gone[2] = missing;
    objects.push_back(way(100, corner_gone));
    objects.push_back(relation(500, {{"type", "multipolygon"}, {"natural", "water"}, {"water", "lake"}, {"name", "Chord Lake"}},
                               {{100, "outer"}}));
    // A whole lake beside it stays.
    objects.push_back(way(101, square(lat0, lng0 + 20000, 10000)));
    objects.push_back(relation(501, {{"type", "multipolygon"}, {"natural", "water"}, {"water", "lake"}, {"name", "Whole Lake"}},
                               {{101, "outer"}}));
    // A river area the line of a river would cross only along a gap: its
    // runs of resolved nodes either side lie west and east of it.
    objects.push_back(way(110, square(lat0 + 100000, lng0, 10000), {{"natural", "water"}, {"water", "river"}}));
    objects.push_back(way(111, {at(lat0 + 105000, lng0 - 8000), at(lat0 + 105000, lng0 - 5000), missing,
                                at(lat0 + 105000, lng0 + 15000), at(lat0 + 105000, lng0 + 18000)},
                          {{"waterway", "river"}, {"name", "Gap River"}}));
    // One a run of a river's resolved nodes runs through names.
    objects.push_back(way(120, square(lat0 + 200000, lng0, 10000), {{"natural", "water"}, {"water", "river"}}));
    objects.push_back(way(121, {at(lat0 + 202000, lng0 + 2000), at(lat0 + 206000, lng0 + 6000), missing,
                                at(lat0 + 250000, lng0 + 50000)},
                          {{"waterway", "river"}, {"name", "Run River"}}));
    const ScratchDir root("water-missing-nodes");
    const std::vector<std::string> names = planet_water_names(objects, root.path() + "/out");
    CHECK(names == std::vector<std::string>({"Run River", "Whole Lake"}));
}

TEST(two_inputs_that_hold_the_same_objects_write_the_water_dirs_one_does) {
    const ScratchDir root("water-two-inputs");
    const std::string pbf = root.path() + "/water.osm.pbf", log = root.path() + "/build.log";
    write_test_pbf(pbf, fixture(), 1000);
    const std::string one = root.path() + "/one", two = root.path() + "/two";
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, one, pbf, "--multi-output", "--multi-quality"}, log));
    REQUIRE(gctest::run({GEOCODER_BUILD_INDEX_BIN, two, pbf, pbf, "--multi-output", "--multi-quality"}, log));
    const auto a = tree(one + "/planet/water"), b = tree(two + "/planet/water");
    REQUIRE(a.size() > 10);
    CHECK(a == b);
}

TEST(a_river_way_is_its_waterway_relations_whichever_input_holds_the_relation) {
    // The first input holds a river area and a river way through it of a
    // local name alone; the second, the same way and the waterway relation
    // it is a member of, named in English. Either way round, the area is
    // the relation's river.
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> first, second;
    const int32_t corners[4][2] = {{0, 0}, {0, 10000}, {10000, 10000}, {10000, 0}};
    for (int k = 0; k < 4; k++) first.push_back(node(k + 1, lat0 + corners[k][0], lng0 + corners[k][1]));
    first.push_back(way(120, {1, 2, 3, 4, 1}, {{"natural", "water"}, {"water", "river"}}));
    for (std::vector<TestObject>* input : {&first, &second}) {
        input->push_back(node(5, lat0 + 2000, lng0 + 2000));
        input->push_back(node(6, lat0 + 6000, lng0 + 6000));
        input->push_back(way(121, {5, 6}, {{"waterway", "river"}, {"name", "Limmat"}}));
    }
    second.push_back(relation(900, {{"type", "waterway"}, {"waterway", "river"}, {"name", "Limmat"},
                                    {"name:en", "Limmat River"}, {"wikidata", "Q14373"}},
                              {{121, ""}}));
    const ScratchDir root("water-inputs-relation");
    const std::string a = root.path() + "/first.osm.pbf", b = root.path() + "/second.osm.pbf";
    write_test_pbf(a, sorted(first), 1000);
    write_test_pbf(b, sorted(second), 1000);
    for (const auto& [name, inputs] : std::vector<std::pair<std::string, std::vector<std::string>>>{
             {"ab", {a, b}}, {"ba", {b, a}}}) {
        const std::string out = root.path() + "/" + name;
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out};
        words.insert(words.end(), inputs.begin(), inputs.end());
        words.insert(words.end(), {"--multi-output", "--multi-quality"});
        REQUIRE(gctest::run(words, out + ".log"));
        CHECK(names_in(bytes_of(out + "/planet/water/all/exact/strings_core.bin")) ==
              std::vector<std::string>({"Limmat River"}));
    }
}

TEST(a_way_is_read_for_the_water_relations_of_every_input_whichever_input_holds_them) {
    // The first input holds a river area, an unnamed river way through it
    // and an untagged way round a lake; the second, the waterway relation
    // the river way is a member of and the lake relation of the untagged
    // way, which nothing in the first asks the water layer to read.
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> first, second;
    const int32_t corners[4][2] = {{0, 0}, {0, 10000}, {10000, 10000}, {10000, 0}};
    for (int k = 0; k < 4; k++) {
        first.push_back(node(k + 1, lat0 + corners[k][0], lng0 + corners[k][1]));
        first.push_back(node(k + 11, lat0 + 100000 + corners[k][0], lng0 + corners[k][1]));
    }
    first.push_back(way(120, {1, 2, 3, 4, 1}, {{"natural", "water"}, {"water", "river"}}));
    first.push_back(node(5, lat0 + 2000, lng0 + 2000));
    first.push_back(node(6, lat0 + 6000, lng0 + 6000));
    first.push_back(way(121, {5, 6}, {{"waterway", "river"}}));
    first.push_back(way(130, {11, 12, 13, 14, 11}));
    second.push_back(relation(900, {{"type", "waterway"}, {"waterway", "river"}, {"name", "Limmat"}}, {{121, ""}}));
    second.push_back(relation(500, {{"type", "multipolygon"}, {"natural", "water"}, {"water", "lake"}, {"name", "Testsee"}},
                              {{130, "outer"}}));
    const ScratchDir root("water-inputs-relations");
    const std::string a = root.path() + "/first.osm.pbf", b = root.path() + "/second.osm.pbf";
    write_test_pbf(a, sorted(first), 1000);
    write_test_pbf(b, sorted(second), 1000);
    for (const auto& [name, inputs] : std::vector<std::pair<std::string, std::vector<std::string>>>{
             {"ab", {a, b}}, {"ba", {b, a}}}) {
        const std::string out = root.path() + "/" + name;
        std::vector<std::string> words = {GEOCODER_BUILD_INDEX_BIN, out};
        words.insert(words.end(), inputs.begin(), inputs.end());
        words.insert(words.end(), {"--multi-output", "--multi-quality"});
        REQUIRE(gctest::run(words, out + ".log"));
        CHECK(names_in(bytes_of(out + "/planet/water/all/exact/strings_core.bin")) ==
              std::vector<std::string>({"Limmat", "Testsee"}));
    }
}

TEST(a_water_relations_member_way_of_an_id_past_two_billion_is_read) {
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> objects;
    const int32_t corners[4][2] = {{0, 0}, {0, 10000}, {10000, 10000}, {10000, 0}};
    for (int k = 0; k < 4; k++) objects.push_back(node(k + 1, lat0 + corners[k][0], lng0 + corners[k][1]));
    objects.push_back(way(2000000130, {1, 2, 3, 4, 1}));
    objects.push_back(relation(500, {{"type", "multipolygon"}, {"natural", "water"}, {"water", "lake"}, {"name", "Hochsee"}},
                               {{2000000130, "outer"}}));
    const ScratchDir root("water-high-way-id");
    CHECK(planet_water_names(objects, root.path() + "/out") == std::vector<std::string>({"Hochsee"}));
}

TEST(a_closed_water_way_tagged_area_no_is_water_as_nominatim_fills_it) {
    // Nominatim's process_way reads `area` only to fill a highway, and its
    // water rows never read it: a closed natural=water way is an area,
    // area=no or not.
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> objects;
    const int32_t corners[4][2] = {{0, 0}, {0, 10000}, {10000, 10000}, {10000, 0}};
    for (int k = 0; k < 4; k++) objects.push_back(node(k + 1, lat0 + corners[k][0], lng0 + corners[k][1]));
    objects.push_back(way(140, {1, 2, 3, 4, 1}, {{"natural", "water"}, {"area", "no"}, {"name", "Kein Gebiet"}}));
    const ScratchDir root("water-area-no");
    CHECK(planet_water_names(objects, root.path() + "/out") == std::vector<std::string>({"Kein Gebiet"}));
}

TEST(a_member_way_of_a_river_relation_tagged_as_a_stream_names_the_area_it_runs_through) {
    // The Rhine's relation takes a reach tagged waterway=stream as its main
    // stream, the only line through an unnamed river area: the area is the
    // relation's river. A stream of its own name in no relation names
    // nothing.
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> objects;
    int64_t next = 1;
    auto at = [&](int32_t lat, int32_t lng) {
        objects.push_back(node(next, lat, lng));
        return next++;
    };
    auto square = [&](int32_t lat, int32_t lng, int32_t side) {
        const int64_t a = at(lat, lng), b = at(lat, lng + side), c = at(lat + side, lng + side), d = at(lat + side, lng);
        return std::vector<int64_t>{a, b, c, d, a};
    };
    objects.push_back(way(100, square(lat0, lng0, 10000), {{"natural", "water"}, {"water", "river"}}));
    objects.push_back(way(101, {at(lat0 + 2000, lng0 + 2000), at(lat0 + 6000, lng0 + 6000)},
                          {{"waterway", "stream"}, {"name", "Rhein"}}));
    objects.push_back(relation(900, {{"type", "waterway"}, {"waterway", "river"}, {"name", "Rhein"}, {"name:en", "Rhine"}},
                               {{101, "main_stream"}}));
    objects.push_back(way(110, square(lat0 + 100000, lng0, 10000), {{"natural", "water"}, {"water", "river"}}));
    objects.push_back(way(111, {at(lat0 + 102000, lng0 + 2000), at(lat0 + 106000, lng0 + 6000)},
                          {{"waterway", "stream"}, {"name", "Dorfbach"}}));
    const ScratchDir root("water-stream-member");
    CHECK(planet_water_names(objects, root.path() + "/out") == std::vector<std::string>({"Rhine"}));
}

TEST(a_river_relations_tributary_or_bank_doesnt_name_the_area_it_runs_through) {
    // The Rhine's relation: its main stream, of no role, through one unnamed
    // river area and a side stream through another, which both take its
    // name; a tributary of no name of its own through a third, and the
    // outline of a fourth as its riverbank, which name nothing.
    const int32_t lat0 = 470000000, lng0 = 80000000;
    std::vector<TestObject> objects;
    int64_t next = 1;
    auto at = [&](int32_t lat, int32_t lng) {
        objects.push_back(node(next, lat, lng));
        return next++;
    };
    auto square = [&](int32_t lat, int32_t lng, int32_t side) {
        const int64_t a = at(lat, lng), b = at(lat, lng + side), c = at(lat + side, lng + side), d = at(lat + side, lng);
        return std::vector<int64_t>{a, b, c, d, a};
    };
    const std::vector<std::pair<std::string, std::string>> river_area = {{"natural", "water"}, {"water", "river"}};
    for (int k = 0; k < 3; k++) {
        const int32_t lat = lat0 + k * 100000;
        objects.push_back(way(100 + 10 * k, square(lat, lng0, 10000), river_area));
        objects.push_back(way(101 + 10 * k, {at(lat + 2000, lng0 + 2000), at(lat + 6000, lng0 + 6000)},
                              {{"waterway", k == 2 ? "stream" : "river"}}));
    }
    // The bank: a river area's own outline, run so that the area is to its
    // left.
    objects.push_back(way(130, square(lat0 + 300000, lng0, 10000), river_area));
    objects.push_back(relation(900, {{"type", "waterway"}, {"waterway", "river"}, {"name", "Rhein"}, {"name:en", "Rhine"}},
                               {{101, ""}, {111, "side_stream"}, {121, "tributary"}, {130, "riverbank"}}));
    const ScratchDir root("water-river-roles");
    const std::string out = root.path() + "/out";
    REQUIRE(planet_water_names(objects, out) == std::vector<std::string>({"Rhine"}));
    std::vector<std::string> slots = slot_names(out + "/planet/water/all/exact");
    std::sort(slots.begin(), slots.end());
    CHECK(slots == std::vector<std::string>({"Rhine", "Rhine"}));
}
