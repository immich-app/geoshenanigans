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
// The pools' strings by day.
constexpr uint32_t OLD_A = 0, OLD_B = 2, OLD_C = 4, NEW_B = 0, NEW_C = 2, NEW_D = 4;

constexpr uint64_t CELL = 0x89c2590000000000ull, CELL_2 = 0x89c25b0000000000ull;

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

// A geo cell and the street, addr and interp ids it lists.
struct GeoCell {
    uint64_t id;
    std::vector<uint32_t> streets, addrs, interps;
};

// geo_cells.bin and the entries files of `cells` (ascending ids), laid out as
// the builder writes them: each list after the one before, an empty one NO_DATA.
void put_geo_cells(BuildDir& d, const std::vector<GeoCell>& cells) {
    std::vector<char> geo;
    for (const auto& cell : cells) {
        append(geo, cell.id);
        const std::pair<const char*, const std::vector<uint32_t>*> lists[] = {
            {"street_entries.bin", &cell.streets}, {"addr_entries.bin", &cell.addrs},
            {"interp_entries.bin", &cell.interps}};
        for (const auto& [name, ids] : lists) {
            if (ids->empty()) { append(geo, NO_DATA); continue; }
            auto& entries = d[name];
            append(geo, static_cast<uint32_t>(entries.size()));
            auto list = entry_list(*ids);
            entries.insert(entries.end(), list.begin(), list.end());
        }
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

// s as one shell word.
std::string quoted(const std::string& s) {
    std::string out = "'";
    for (char c : s) out += c == '\'' ? std::string("'\\''") : std::string(1, c);
    return out + "'";
}

// Runs the words with their output appended to log; true on exit status 0.
bool run(const std::vector<std::string>& words, const std::string& log) {
    std::string cmd;
    for (const auto& w : words) cmd += quoted(w) + " ";
    return std::system((cmd + ">> " + quoted(log) + " 2>&1").c_str()) == 0;
}

// A day's identity sidecars by family ("street", "interp", "addr"): one
// entity key per record (tools/identity_sidecar.h).
using Sidecars = std::map<std::string, std::vector<uint32_t>>;

// Both days' sidecars, passed as geocoder-diff --identity; without any the
// diff merges every family by content.
struct Identity {
    Sidecars old_keys, new_keys;
};

bool write_sidecars(const std::string& path, const Sidecars& keys) {
    BuildDir files;
    for (const auto& [family, k] : keys) files[family + ".slot.u32"] = bytes_of(k);
    return write_dir(path, files);
}

// The files of the patched dir that are missing, extra or not new's bytes
// ("diff failed" / "patch failed" when a tool exits non-zero), and the
// tools' log.
struct RoundTrip {
    std::vector<std::string> mismatches;
    std::string log;
};

// Diffs old -> new, applies the patch to old and compares the patched dir
// with new. Prints the tools' log on a mismatch.
RoundTrip round_trip(const BuildDir& old_files, const BuildDir& new_files, const Identity& identity = {}) {
    ScratchDir dir("patch-roundtrip");
    const std::string old_dir = dir.path() + "/old", new_dir = dir.path() + "/new", out_dir = dir.path() + "/out";
    const std::string old_meta = dir.path() + "/old-meta", new_meta = dir.path() + "/new-meta";
    const std::string patch = dir.path() + "/p.gcpatch", log = dir.path() + "/tools.log";
    const bool by_identity = !identity.old_keys.empty() || !identity.new_keys.empty();
    std::vector<std::string> diff = {GEOCODER_DIFF_BIN, old_dir, new_dir, "-o", patch};
    if (by_identity) diff.insert(diff.end(), {"--identity", old_meta, new_meta});
    RoundTrip r;
    auto& bad = r.mismatches;
    if (!write_dir(old_dir, old_files) || !write_dir(new_dir, new_files) ||
        (by_identity && (!write_sidecars(old_meta, identity.old_keys) || !write_sidecars(new_meta, identity.new_keys))))
        bad.push_back("fixture not written");
    else if (!run(diff, log))
        bad.push_back("diff failed");
    else if (!run({GEOCODER_PATCH_BIN, old_dir, patch, "-o", out_dir}, log))
        bad.push_back("patch failed");
    else {
        for (const auto& [name, bytes] : new_files)
            if (!std::filesystem::exists(out_dir + "/" + name) || read_file(out_dir + "/" + name) != bytes)
                bad.push_back(name);
        for (const auto& e : std::filesystem::directory_iterator(out_dir))
            if (!new_files.count(e.path().filename().string())) bad.push_back("extra " + e.path().filename().string());
    }
    auto text = read_file(log);
    r.log.assign(text.begin(), text.end());
    if (!bad.empty()) {
        for (const auto& b : bad) std::printf("    round trip: %s\n", b.c_str());
        std::printf("%s\n", r.log.c_str());
    }
    return r;
}

bool logged(const RoundTrip& r, const std::string& line) { return r.log.find(line) != std::string::npos; }

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

// Nodes (or vertices) numbered i, as street_nodes.bin holds them.
std::vector<char> nodes_of(const std::vector<int>& numbers) {
    std::vector<NodeCoord> nodes;
    for (int i : numbers) nodes.push_back({10.0f + i, 20.0f + i});
    return bytes_of(nodes);
}

// Streets matched by identity, one of each kind of record an identity merge
// sends (key: old id -> new id):
//   7:  0 -> gone           (A, nodes 1 2)
//   1:  1 -> 1  MATCH       (B, node 3), node offset 2 -> 1
//   2:  2 -> 2  REPLACE     (C -> D, nodes 4 5 -> 4 9 5), node offset 3 -> 2
//   10: 3 -> 0  moved       (B, node 6): an INSERT and the pair 3 -> 0
//   4:  4 -> 3  MATCH       (C, node 7), node offset 6 -> 5
//   11:      4  new         (C, node 8)
// Keys below the record count take identity_sidecar.h's table, the others its
// sorted list. way_postcodes (strings) and way_parents (admin ids, kept: no
// admin polygons here) follow the ways: MATCH and REPLACE take old's value
// remapped, moved and new ways an inline one, and new 1's postcode and new
// 3's parent change.
void put_identity_streets(BuildDir& old_d, BuildDir& new_d, Identity& ids) {
    old_d["street_ways.bin"] = bytes_of(std::vector<WayHeader>{
        {0, 2, 0, OLD_A}, {2, 1, 0, OLD_B}, {3, 2, 0, OLD_C}, {5, 1, 0, OLD_B}, {6, 1, 0, OLD_C}});
    old_d["street_nodes.bin"] = nodes_of({1, 2, 3, 4, 5, 6, 7});
    new_d["street_ways.bin"] = bytes_of(std::vector<WayHeader>{
        {0, 1, 0, NEW_B}, {1, 1, 0, NEW_B}, {2, 3, 0, NEW_D}, {5, 1, 0, NEW_C}, {6, 1, 0, NEW_C}});
    new_d["street_nodes.bin"] = nodes_of({6, 3, 4, 9, 5, 7, 8});
    ids.old_keys["street"] = {7, 1, 2, 10, 4};
    ids.new_keys["street"] = {10, 1, 2, 4, 11};
    old_d["way_postcodes.bin"] = bytes_of(std::vector<uint32_t>{OLD_A, OLD_B, OLD_C, OLD_B, NO_DATA});
    new_d["way_postcodes.bin"] = bytes_of(std::vector<uint32_t>{NEW_B, NEW_D, NEW_C, NO_DATA, NEW_C});
    old_d["way_parents.bin"] = bytes_of(std::vector<uint32_t>{5, 6, 7, 8, 9});
    new_d["way_parents.bin"] = bytes_of(std::vector<uint32_t>{8, 6, 7, 3, 1});
}

// Addresses on put_identity_streets' streets, matched by identity
// (key: old id -> new id):
//   0:  0 -> 1  MATCH       (B, on moved street 3 -> 0)
//   21: 1 -> 2  REPLACE     (C -> D, on street 2), polygon of vertices 1 2 -> 1 9 2
//   22: 2 -> gone           (A, on the deleted street 0)
//   3:  3 -> 3  MATCH       (B, on street 4 -> 3), polygon 3 4, vertex offset 16 -> 24
//   24: 4 -> 0  moved       (C, on street 1)
//   25:      4  new         (D, on the new street 4)
// addr_postcodes follows the addresses; new 3's postcode changes.
void put_identity_addresses(BuildDir& old_d, BuildDir& new_d, Identity& ids) {
    old_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{
        {48.0f, 11.0f, OLD_B, OLD_B, 3, NO_DATA, 0}, {48.0f, 11.0f, OLD_C, OLD_C, 2, 0, 2},
        {48.0f, 11.0f, OLD_A, OLD_A, 0, NO_DATA, 0}, {48.0f, 11.0f, OLD_B, OLD_B, 4, 16, 2},
        {48.0f, 11.0f, OLD_C, OLD_C, 1, NO_DATA, 0}});
    old_d["addr_vertices.bin"] = nodes_of({1, 2, 3, 4});
    new_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{
        {48.0f, 11.0f, NEW_C, NEW_C, 1, NO_DATA, 0}, {48.0f, 11.0f, NEW_B, NEW_B, 0, NO_DATA, 0},
        {48.0f, 11.0f, NEW_D, NEW_D, 2, 0, 3}, {48.0f, 11.0f, NEW_B, NEW_B, 3, 24, 2},
        {48.0f, 11.0f, NEW_D, NEW_D, 4, NO_DATA, 0}});
    new_d["addr_vertices.bin"] = nodes_of({1, 9, 2, 3, 4});
    ids.old_keys["addr"] = {0, 21, 22, 3, 24};
    ids.new_keys["addr"] = {24, 0, 21, 3, 25};
    old_d["addr_postcodes.bin"] = bytes_of(std::vector<uint32_t>{OLD_B, OLD_C, OLD_A, NO_DATA, OLD_C});
    new_d["addr_postcodes.bin"] = bytes_of(std::vector<uint32_t>{NEW_D, NEW_B, NEW_C, NEW_B, NO_DATA});
}

// Interpolations matched by identity (key: old id -> new id):
//   0:  0 -> 1  REPLACE     (B 1-9 -> 1-11, nodes 1 2 -> 1 9 2)
//   1:  1 -> 2  MATCH       (C 2-8, node 3), node offset 2 -> 4
//   12: 2 -> gone           (A 1-5, node 4)
//   3:  3 -> 0  moved       (B 3-7, node 5)
//   13:      3  new         (D 1-3, node 6)
// interp_postcodes follows the ways; new 2's postcode changes.
void put_identity_interps(BuildDir& old_d, BuildDir& new_d, Identity& ids) {
    old_d["interp_ways.bin"] = bytes_of(std::vector<InterpWay>{
        {0, 2, 0, OLD_B, 1, 9, 0}, {2, 1, 0, OLD_C, 2, 8, 0}, {3, 1, 0, OLD_A, 1, 5, 0}, {4, 1, 0, OLD_B, 3, 7, 0}});
    old_d["interp_nodes.bin"] = nodes_of({1, 2, 3, 4, 5});
    new_d["interp_ways.bin"] = bytes_of(std::vector<InterpWay>{
        {0, 1, 0, NEW_B, 3, 7, 0}, {1, 3, 0, NEW_B, 1, 11, 0}, {4, 1, 0, NEW_C, 2, 8, 0}, {5, 1, 0, NEW_D, 1, 3, 0}});
    new_d["interp_nodes.bin"] = nodes_of({5, 1, 9, 2, 3, 6});
    ids.old_keys["interp"] = {0, 1, 12, 3};
    ids.new_keys["interp"] = {3, 0, 1, 13};
    old_d["interp_postcodes.bin"] = bytes_of(std::vector<uint32_t>{OLD_B, OLD_C, OLD_A, OLD_C});
    new_d["interp_postcodes.bin"] = bytes_of(std::vector<uint32_t>{NEW_C, NEW_B, NEW_D, NO_DATA});
}

}  // namespace

TEST(patch_round_trip_keeps_the_street_ids_of_unchanged_street_ways) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    put_shifted_streets(old_d, new_d);
    put_geo_cells(old_d, {{CELL, {0, 1}, {}, {}}});
    put_geo_cells(new_d, {{CELL, {0, 1}, {}, {}}});

    CHECK(round_trip(old_d, new_d).mismatches.empty());
}

TEST(patch_round_trip_walks_the_side_files_of_unchanged_street_ways_as_kept) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    put_shifted_streets(old_d, new_d);
    old_d["way_postcodes.bin"] = bytes_of(std::vector<uint32_t>{NAME_2, NAME_4});
    new_d["way_postcodes.bin"] = bytes_of(std::vector<uint32_t>{NAME_4, NAME_2});

    CHECK(round_trip(old_d, new_d).mismatches.empty());
}

TEST(patch_round_trip_keeps_the_parent_ways_of_unchanged_street_ways) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    put_shifted_streets(old_d, new_d);
    old_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{addr_point(NO_DATA, 1)});
    new_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{addr_point(NO_DATA, 0)});

    CHECK(round_trip(old_d, new_d).mismatches.empty());
}

TEST(patch_round_trip_keeps_the_addr_ids_of_unchanged_addr_points) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    old_d["addr_points.bin"] = new_d["addr_points.bin"] =
        bytes_of(std::vector<AddrPoint>{addr_point(NAME_2, NO_DATA), addr_point(NAME_4, NO_DATA)});
    put_geo_cells(old_d, {{CELL, {}, {0, 1}, {}}});
    put_geo_cells(new_d, {{CELL, {}, {0, 1}, {}}});

    CHECK(round_trip(old_d, new_d).mismatches.empty());
}

TEST(patch_round_trip_keeps_the_interp_ids_of_unchanged_interp_ways) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    InterpWay a{0, 1, 0, NAME_2, 1, 9, 0}, b{1, 1, 0, NAME_4, 1, 9, 0};
    old_d["interp_ways.bin"] = new_d["interp_ways.bin"] = bytes_of(std::vector<InterpWay>{a, b});
    const NodeCoord node{10.0f, 20.0f};
    old_d["interp_nodes.bin"] = new_d["interp_nodes.bin"] = bytes_of(std::vector<NodeCoord>{node, node});
    put_geo_cells(old_d, {{CELL, {}, {}, {0, 1}}});
    put_geo_cells(new_d, {{CELL, {}, {}, {0, 1}}});

    CHECK(round_trip(old_d, new_d).mismatches.empty());
}

TEST(patch_round_trip_keeps_the_poi_ids_of_unchanged_poi_records) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    old_d["poi_records.bin"] = new_d["poi_records.bin"] = bytes_of(std::vector<PoiRecord>{poi(NAME_2), poi(NAME_4)});
    put_cell_index(old_d, "poi", {0, 1});
    put_cell_index(new_d, "poi", {0, 1});

    CHECK(round_trip(old_d, new_d).mismatches.empty());
}

TEST(patch_round_trip_keeps_the_place_ids_of_unchanged_place_nodes) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    old_d["place_nodes.bin"] = new_d["place_nodes.bin"] = bytes_of(std::vector<PlaceNode>{place(NAME_2), place(NAME_4)});
    put_cell_index(old_d, "place", {0, 1});
    put_cell_index(new_d, "place", {0, 1});

    CHECK(round_trip(old_d, new_d).mismatches.empty());
}

// The second cell of each identity round trip lists only records the day
// kept, so no correction rebuilds it: the derived remaps (MATCH, REPLACE and
// the moved pairs) alone must give its new lists. The logged identity stats
// show the diff took the identity path: a family whose sidecars it didn't
// read would merge by content and round-trip all the same.
TEST(patch_round_trip_by_identity_rebuilds_streets_and_the_addresses_on_them) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    Identity ids;
    put_identity_streets(old_d, new_d, ids);
    put_identity_addresses(old_d, new_d, ids);
    put_geo_cells(old_d, {{CELL, {0, 1, 2, 3, 4}, {0, 1, 2, 3, 4}, {}}, {CELL_2, {2, 3, 4}, {1, 4}, {}}});
    put_geo_cells(new_d, {{CELL, {0, 1, 2, 3, 4}, {0, 1, 2, 3, 4}, {}}, {CELL_2, {0, 2, 3}, {0, 2}, {}}});

    auto r = round_trip(old_d, new_d, ids);

    CHECK(r.mismatches.empty());
    CHECK(logged(r, "street_ways identity: match=2 replace=1 insert=2 delete=2 moved=1"));
    CHECK(logged(r, "addr_points identity: match=2 replace=1 insert=2 delete=2 moved=1"));
    CHECK(logged(r, "street_pairs=1"));
}

TEST(patch_round_trip_by_identity_sends_the_moved_streets_of_a_dir_without_addresses_as_secondary_pairs) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    Identity ids;
    put_identity_streets(old_d, new_d, ids);
    put_geo_cells(old_d, {{CELL, {0, 1, 2, 3, 4}, {}, {}}, {CELL_2, {2, 3, 4}, {}, {}}});
    put_geo_cells(new_d, {{CELL, {0, 1, 2, 3, 4}, {}, {}}, {CELL_2, {0, 2, 3}, {}, {}}});

    auto r = round_trip(old_d, new_d, ids);

    CHECK(r.mismatches.empty());
    CHECK(logged(r, "street_ways identity: match=2 replace=1 insert=2 delete=2 moved=1"));
    CHECK(logged(r, "Secondary remap 1: 1 pairs"));
}

TEST(patch_round_trip_by_identity_rebuilds_interp_ways) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    Identity ids;
    put_identity_interps(old_d, new_d, ids);
    put_geo_cells(old_d, {{CELL, {}, {}, {0, 1, 2, 3}}, {CELL_2, {}, {}, {0, 1, 3}}});
    put_geo_cells(new_d, {{CELL, {}, {}, {0, 1, 2, 3}}, {CELL_2, {}, {}, {0, 1, 2}}});

    auto r = round_trip(old_d, new_d, ids);

    CHECK(r.mismatches.empty());
    CHECK(logged(r, "interp_ways identity: match=1 replace=1 insert=2 delete=2 moved=1"));
}
