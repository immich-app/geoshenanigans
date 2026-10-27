// Round trips through the geocoder-diff and geocoder-patch tools: two small
// build dirs, the patch between them, and the patched dir compared with the
// new one file by file. Then patch files the patcher must take as named or
// refuse: a patch is untrusted input.
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iterator>
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

// Runs the words (in dir cwd when given) with their output appended to log;
// true on exit status 0. Stdin is /dev/null: after a usage error zstd waits
// for Enter at a console.
bool run(const std::vector<std::string>& words, const std::string& log, const std::string& cwd = "") {
    std::string cmd = cwd.empty() ? "" : "cd " + quoted(cwd) + " && ";
    for (const auto& w : words) cmd += quoted(w) + " ";
    return std::system((cmd + "< /dev/null >> " + quoted(log) + " 2>&1").c_str()) == 0;
}

// payload compressed as the diff compresses a patch, at path.
bool write_patch(const std::string& path, const std::vector<char>& payload, const std::string& log) {
    const std::string raw = path + ".raw";
    return write_file(raw, payload) && run({"zstd", "-q", "-f", raw, "-o", path}, log);
}

// A geocoder-patch run: exit status 0, and its log.
struct PatchRun {
    bool ok;
    std::string log;
};

// Applies patch to root/old into out, both paths relative to root (the
// patcher's current dir).
PatchRun run_patch(const std::string& root, const std::string& patch, const std::string& out) {
    const std::string log = root + "/" + out + ".log";
    PatchRun r;
    r.ok = run({GEOCODER_PATCH_BIN, "old", patch, "-o", out}, log, root);
    auto text = read_file(log);
    r.log.assign(text.begin(), text.end());
    return r;
}

bool logged(const PatchRun& r, const std::string& line) { return r.log.find(line) != std::string::npos; }

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

// way_parents.bin of SPARSE_RECORDS admin ids, the SPARSE_CHANGES (position,
// id) changed on the new day. With no street ways to follow, the diff sends
// the new file as a sparse delta.
constexpr uint32_t SPARSE_RECORDS = 4096;
constexpr std::pair<uint32_t, uint32_t> SPARSE_CHANGES[] = {{10, 7777}, {2000, 8888}, {4000, 9999}};

std::vector<uint32_t> old_way_parents() {
    std::vector<uint32_t> ids(SPARSE_RECORDS);
    for (uint32_t i = 0; i < SPARSE_RECORDS; i++) ids[i] = i % 1000;
    return ids;
}

std::vector<uint32_t> new_way_parents() {
    auto ids = old_way_parents();
    for (const auto& [pos, id] : SPARSE_CHANGES) ids[pos] = id;
    return ids;
}

template <typename T>
void put_at(std::vector<char>& out, size_t at, T v) {
    memcpy(out.data() + at, &v, sizeof v);
}

// The offset of needle in hay, or npos unless it occurs exactly once.
size_t find_once(const std::vector<char>& hay, const std::vector<char>& needle) {
    auto at = std::search(hay.begin(), hay.end(), needle.begin(), needle.end());
    if (at == hay.end() || std::search(at + 1, hay.end(), needle.begin(), needle.end()) != hay.end())
        return std::string::npos;
    return static_cast<size_t>(at - hay.begin());
}

// The diff of old_way_parents() -> new_way_parents(), uncompressed, and
// where it holds the fields a test edits. raw is empty when a tool failed or
// the patch holds no single way_parents sparse delta.
struct SparsePatch {
    std::vector<char> raw;
    size_t new_size_at = 0, last_pos_at = 0, listed_size_at = 0;

    // The client file list is kept in step, so only the section is wrong.
    void set_new_size(uint64_t size) {
        put_at(raw, new_size_at, size);
        put_at(raw, listed_size_at, size);
    }
    void set_last_pos(uint32_t pos) { put_at(raw, last_pos_at, pos); }
};

// Writes root/old and root/new and diffs them.
SparsePatch diff_way_parents(const std::string& root) {
    const std::string patch = root + "/p.gcpatch", raw = root + "/p.raw", log = root + "/tools.log";
    const uint64_t size = uint64_t{SPARSE_RECORDS} * 4;
    SparsePatch p;
    if (!write_dir(root + "/old", {{"way_parents.bin", bytes_of(old_way_parents())}}) ||
        !write_dir(root + "/new", {{"way_parents.bin", bytes_of(new_way_parents())}}) ||
        !run({GEOCODER_DIFF_BIN, root + "/old", root + "/new", "-o", patch}, log) ||
        !run({"zstd", "-d", "-q", "-f", patch, "-o", raw}, log))
        return p;
    auto bytes = read_file(raw);
    // file_id, stride, old_size, new_size, value_stride, remap_kind, n_changes
    std::vector<char> section;
    append(section, static_cast<uint32_t>(PatchFileId::WAY_PARENTS));
    append(section, SPARSE_DELTA_STRIDE);
    append(section, size);
    append(section, size);
    append(section, uint32_t{4});
    append(section, uint32_t{1});
    append(section, static_cast<uint32_t>(std::size(SPARSE_CHANGES)));
    // name_len, name, size, inline
    const std::string name = "way_parents.bin";
    std::vector<char> listed;
    append(listed, static_cast<uint16_t>(name.size()));
    listed.insert(listed.end(), name.begin(), name.end());
    append(listed, size);
    append(listed, uint8_t{0});
    const size_t section_at = find_once(bytes, section), listed_at = find_once(bytes, listed);
    if (section_at == std::string::npos || listed_at == std::string::npos) return p;
    p.new_size_at = section_at + 16;
    p.last_pos_at = section_at + section.size() + (std::size(SPARSE_CHANGES) - 1) * 8;
    p.listed_size_at = listed_at + 2 + name.size();
    p.raw = std::move(bytes);
    return p;
}

// raw compressed to root/<name>.gcpatch and applied to root/old into
// root/<name>.
PatchRun apply_raw(const std::string& root, const std::vector<char>& raw, const std::string& name) {
    if (!write_patch(root + "/" + name + ".gcpatch", raw, root + "/zstd.log")) return {false, "patch not written"};
    return run_patch(root, name + ".gcpatch", name);
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

// The patch path is one argument to the decompressor whatever its
// characters: a compressed non-patch under each name is decompressed as
// named (the patcher gets as far as its magic), and no part of a name runs
// as a command.
TEST(patch_decompresses_the_patch_path_as_named) {
    ScratchDir dir("patch-named");
    const std::string& root = dir.path();
    const std::string not_a_patch = "NOT A GCPATCH";
    REQUIRE(write_dir(root + "/old", {}));
    REQUIRE(write_patch(root + "/not-a-patch.gcpatch", std::vector<char>(not_a_patch.begin(), not_a_patch.end()),
                        root + "/zstd.log"));
    const std::string names[] = {"user's patch.gcpatch", "x'; touch pwned; 'y.gcpatch", "-patch.gcpatch"};
    for (size_t i = 0; i < std::size(names); i++) {
        std::filesystem::copy_file(root + "/not-a-patch.gcpatch", root + "/" + names[i]);

        auto r = run_patch(root, names[i], "out" + std::to_string(i));

        CHECK(!r.ok);
        CHECK(logged(r, "Bad magic"));
    }
    CHECK(!std::filesystem::exists(root + "/pwned"));
}

// A patch shorter than its 8-byte magic is refused as truncated: the magic
// is read through the bounds check like every later field.
TEST(patch_refuses_a_patch_shorter_than_its_magic) {
    ScratchDir dir("patch-short");
    const std::string& root = dir.path();
    REQUIRE(write_dir(root + "/old", {}));
    REQUIRE(write_patch(root + "/short.gcpatch", std::vector<char>(GCPATCH_MAGIC, GCPATCH_MAGIC + 7),
                        root + "/zstd.log"));

    auto r = run_patch(root, "short.gcpatch", "out");

    CHECK(!r.ok);
    CHECK(logged(r, "Truncated patch: need 8 bytes for magic"));
}

// A sparse delta's change at its file's last record applies: the patcher
// refuses records past the end, not the last one.
TEST(patch_applies_a_sparse_delta_change_at_the_last_record) {
    ScratchDir dir("patch-sparse");
    SparsePatch p = diff_way_parents(dir.path());
    REQUIRE(!p.raw.empty());
    p.set_last_pos(SPARSE_RECORDS - 1);

    auto r = apply_raw(dir.path(), p.raw, "out");

    auto want = new_way_parents();
    const auto [last_pos, last_id] = SPARSE_CHANGES[std::size(SPARSE_CHANGES) - 1];
    want[last_pos] = old_way_parents()[last_pos];
    want[SPARSE_RECORDS - 1] = last_id;
    CHECK(r.ok);
    CHECK(read_file(dir.path() + "/out/way_parents.bin") == bytes_of(want));
}

// A change writes its record whole, so a sparse delta whose new size ends in
// part of a record is refused.
TEST(patch_refuses_a_sparse_delta_ending_in_part_of_a_record) {
    ScratchDir dir("patch-sparse");
    SparsePatch p = diff_way_parents(dir.path());
    REQUIRE(!p.raw.empty());
    p.set_new_size(uint64_t{SPARSE_RECORDS} * 4 - 1);
    p.set_last_pos(SPARSE_RECORDS - 1);

    auto r = apply_raw(dir.path(), p.raw, "out");

    CHECK(!r.ok);
    CHECK(logged(r, "Malformed sparse delta"));
}

// A sparse delta's change past its file's last record is refused, not
// dropped unread.
TEST(patch_refuses_a_sparse_delta_change_past_the_last_record) {
    ScratchDir dir("patch-sparse");
    SparsePatch p = diff_way_parents(dir.path());
    REQUIRE(!p.raw.empty());
    p.set_last_pos(SPARSE_RECORDS);

    auto r = apply_raw(dir.path(), p.raw, "out");

    CHECK(!r.ok);
    CHECK(logged(r, "Malformed sparse delta"));
}
