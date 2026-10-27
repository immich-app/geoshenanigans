// Round trips through the geocoder-diff and geocoder-patch tools: two small
// build dirs, the patch between them, and the patched dir compared with the
// new one file by file.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include "patch_format.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "types.h"

namespace {

// A build dir's files by name.
using BuildDir = std::map<std::string, std::vector<char>>;

template <typename T>
std::vector<char> bytes_of(const std::vector<T>& records) {
    std::vector<char> out(records.size() * sizeof(T));
    if (!out.empty()) memcpy(out.data(), records.data(), out.size());
    return out;
}

template <typename T>
void append(std::vector<char>& out, const T& v) {
    const char* p = reinterpret_cast<const char*>(&v);
    out.insert(out.end(), p, p + sizeof v);
}

// Old pool A B C, new pool B C D: offsets 2 and 4 name B and C on the old day
// and C and D on the new one. The string remap moves B to 0 and C to 2, so
// two records named 2 and 4 on both days, otherwise alike, merge by content
// as old 1 -> new 0, with old 0 deleted and new 1 inserted.
const std::string OLD_POOL("A\0B\0C\0", 6);
const std::string NEW_POOL("B\0C\0D\0", 6);
constexpr uint32_t NAME_2 = 2, NAME_4 = 4;

constexpr uint64_t CELL = 0x89c2590000000000ull;

BuildDir with_pool(const std::string& pool) {
    BuildDir d;
    d["strings_core.bin"].assign(pool.begin(), pool.end());
    return d;
}

std::vector<char> entry_list(const std::vector<uint32_t>& ids) {
    std::vector<char> out;
    append(out, static_cast<uint16_t>(ids.size()));
    for (uint32_t id : ids) append(out, id);
    return out;
}

// geo_cells.bin holding CELL, which lists `streets`, `addrs` and `interps`.
void put_geo_cell(BuildDir& d, const std::vector<uint32_t>& streets, const std::vector<uint32_t>& addrs,
                  const std::vector<uint32_t>& interps) {
    std::vector<char> geo;
    append(geo, CELL);
    const std::pair<const char*, const std::vector<uint32_t>*> lists[] = {
        {"street_entries.bin", &streets}, {"addr_entries.bin", &addrs}, {"interp_entries.bin", &interps}};
    for (const auto& [name, ids] : lists) {
        if (ids->empty()) { append(geo, NO_DATA); continue; }
        append(geo, uint32_t(0));
        d[name] = entry_list(*ids);
    }
    d["geo_cells.bin"] = geo;
}

// <prefix>_cells.bin and <prefix>_entries.bin holding CELL, which lists ids.
void put_cell_index(BuildDir& d, const std::string& prefix, const std::vector<uint32_t>& ids) {
    auto [cells, entries] = write_cell_lists({{CELL, ids}});
    d[prefix + "_cells.bin"] = cells;
    d[prefix + "_entries.bin"] = entries;
}

bool write_dir(const std::string& path, const BuildDir& files) {
    std::filesystem::create_directories(path);
    for (const auto& [name, bytes] : files)
        if (!write_file(path + "/" + name, bytes)) return false;
    return true;
}

// Runs cmd with its output appended to log; true on exit status 0.
bool run(const std::string& cmd, const std::string& log) {
    return std::system((cmd + " >> '" + log + "' 2>&1").c_str()) == 0;
}

// Diffs old -> new, applies the patch to old and lists every file of the
// patched dir that is missing, extra or not new's bytes ("diff failed" /
// "patch failed" when a tool exits non-zero). Prints the tools' log on a
// failure.
std::vector<std::string> round_trip_mismatches(const BuildDir& old_files, const BuildDir& new_files) {
    ScratchDir dir("patch-roundtrip");
    const std::string old_dir = dir.path() + "/old", new_dir = dir.path() + "/new", out_dir = dir.path() + "/out";
    const std::string patch = dir.path() + "/p.gcpatch", log = dir.path() + "/tools.log";
    std::vector<std::string> bad;
    if (!write_dir(old_dir, old_files) || !write_dir(new_dir, new_files)) bad.push_back("fixture not written");
    else if (!run(std::string(GEOCODER_DIFF_BIN) + " '" + old_dir + "' '" + new_dir + "' -o '" + patch + "'", log))
        bad.push_back("diff failed");
    else if (!run(std::string(GEOCODER_PATCH_BIN) + " '" + old_dir + "' '" + patch + "' -o '" + out_dir + "'", log))
        bad.push_back("patch failed");
    else {
        for (const auto& [name, bytes] : new_files)
            if (!std::filesystem::exists(out_dir + "/" + name) || read_file(out_dir + "/" + name) != bytes)
                bad.push_back(name);
        for (const auto& e : std::filesystem::directory_iterator(out_dir))
            if (!new_files.count(e.path().filename().string())) bad.push_back("extra " + e.path().filename().string());
    }
    if (!bad.empty()) {
        for (const auto& b : bad) std::printf("    round trip: %s\n", b.c_str());
        auto text = read_file(log);
        std::printf("%.*s\n", static_cast<int>(text.size()), text.data());
    }
    return bad;
}

// Ways named 2 and 4 on both days. The old nodes are X then Y, the new ones Y
// then Z, so street_nodes.bin changes while street_ways.bin does not, and the
// node fixup pairs old way 1 (C, nodes Y) with new way 0 (C, nodes Y).
void put_shifted_streets(BuildDir& old_d, BuildDir& new_d) {
    const std::vector<WayHeader> ways = {{0, 1, 0, NAME_2}, {1, 1, 0, NAME_4}};
    const NodeCoord x{10.0f, 20.0f}, y{11.0f, 21.0f}, z{12.0f, 22.0f};
    old_d["street_ways.bin"] = new_d["street_ways.bin"] = bytes_of(ways);
    old_d["street_nodes.bin"] = bytes_of(std::vector<NodeCoord>{x, y});
    new_d["street_nodes.bin"] = bytes_of(std::vector<NodeCoord>{y, z});
}

AddrPoint addr_point(uint32_t name, uint32_t parent_way) {
    return {48.0f, 11.0f, name, name, parent_way, NO_DATA, 0};
}

PoiRecord poi(uint32_t name) {
    PoiRecord p{};
    p.lat = 48.0f;
    p.lng = 11.0f;
    p.vertex_offset = NO_DATA;
    p.name_id = name;
    p.category = 1;
    p.tier = 1;
    return p;
}

PlaceNode place(uint32_t name) {
    PlaceNode p{};
    p.lat = 48.0f;
    p.lng = 11.0f;
    p.name_id = name;
    p.place_type = 1;
    return p;
}

}  // namespace

TEST(patch_round_trip_keeps_the_street_ids_of_unchanged_street_ways) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    put_shifted_streets(old_d, new_d);
    put_geo_cell(old_d, {0, 1}, {}, {});
    put_geo_cell(new_d, {0, 1}, {}, {});

    CHECK(round_trip_mismatches(old_d, new_d).empty());
}

TEST(patch_round_trip_keeps_the_parent_ways_of_unchanged_street_ways) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    put_shifted_streets(old_d, new_d);
    old_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{addr_point(NO_DATA, 1)});
    new_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{addr_point(NO_DATA, 0)});

    CHECK(round_trip_mismatches(old_d, new_d).empty());
}

TEST(patch_round_trip_keeps_the_addr_ids_of_unchanged_addr_points) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    old_d["addr_points.bin"] = new_d["addr_points.bin"] =
        bytes_of(std::vector<AddrPoint>{addr_point(NAME_2, NO_DATA), addr_point(NAME_4, NO_DATA)});
    put_geo_cell(old_d, {}, {0, 1}, {});
    put_geo_cell(new_d, {}, {0, 1}, {});

    CHECK(round_trip_mismatches(old_d, new_d).empty());
}

TEST(patch_round_trip_keeps_the_interp_ids_of_unchanged_interp_ways) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    InterpWay a{0, 1, 0, NAME_2, 1, 9, 0}, b{1, 1, 0, NAME_4, 1, 9, 0};
    old_d["interp_ways.bin"] = new_d["interp_ways.bin"] = bytes_of(std::vector<InterpWay>{a, b});
    const NodeCoord node{10.0f, 20.0f};
    old_d["interp_nodes.bin"] = new_d["interp_nodes.bin"] = bytes_of(std::vector<NodeCoord>{node, node});
    put_geo_cell(old_d, {}, {}, {0, 1});
    put_geo_cell(new_d, {}, {}, {0, 1});

    CHECK(round_trip_mismatches(old_d, new_d).empty());
}

TEST(patch_round_trip_keeps_the_poi_ids_of_unchanged_poi_records) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    old_d["poi_records.bin"] = new_d["poi_records.bin"] = bytes_of(std::vector<PoiRecord>{poi(NAME_2), poi(NAME_4)});
    put_cell_index(old_d, "poi", {0, 1});
    put_cell_index(new_d, "poi", {0, 1});

    CHECK(round_trip_mismatches(old_d, new_d).empty());
}

TEST(patch_round_trip_keeps_the_place_ids_of_unchanged_place_nodes) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    old_d["place_nodes.bin"] = new_d["place_nodes.bin"] = bytes_of(std::vector<PlaceNode>{place(NAME_2), place(NAME_4)});
    put_cell_index(old_d, "place", {0, 1});
    put_cell_index(new_d, "place", {0, 1});

    CHECK(round_trip_mismatches(old_d, new_d).empty());
}
