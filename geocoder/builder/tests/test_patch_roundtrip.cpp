// Round trips through the geocoder-diff and geocoder-patch tools: two small
// build dirs, the patch between them, and the patched dir compared with the
// new one file by file. Then patch files the patcher must take as named or
// refuse: a patch is untrusted input.
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <map>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "geo_index.h"
#include "in_place_edits.h"
#include "merge_sequence.h"
#include "patch_decompress.h"
#include "patch_format.h"
#include "place_test_files.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"
#include "types.h"

namespace {

using gctest::BuildDir;
using gctest::run;
using gctest::write_dir;

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

// A geo cell and the street, addr and interp ids it lists.
struct GeoCell {
    uint64_t id;
    std::vector<uint32_t> streets, addrs, interps;
};

// The geo index of `cells` (ascending ids), as the builder writes it
// (geo_index.h): its top file and its blocks file.
void put_geo_cells(BuildDir& d, const std::vector<GeoCell>& cells) {
    std::vector<char> top, blocks;
    geo_index::Writer writer([&](const char* p, size_t n) { top.insert(top.end(), p, p + n); },
                             [&](const char* p, size_t n) { blocks.insert(blocks.end(), p, p + n); });
    for (const auto& cell : cells) writer.add(cell.id, {{{cell.streets.data(), cell.streets.size()},
                                                         {cell.addrs.data(), cell.addrs.size()},
                                                         {cell.interps.data(), cell.interps.size()}}});
    writer.finish();
    d["geo_cells.bin"] = top;
    d["geo_entries.bin"] = blocks;
}

// <prefix>_cells.bin and <prefix>_entries.bin holding CELL, which lists ids.
void put_cell_index(BuildDir& d, const std::string& prefix, const std::vector<uint32_t>& ids) {
    auto [cells, entries] = write_cell_lists({{CELL, ids}}, cell_pairs::format_of(prefix));
    d[prefix + "_cells.bin"] = cells;
    d[prefix + "_entries.bin"] = entries;
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
// ("diff failed" / "patch failed" when a tool exits non-zero), the tools'
// log, and the patch file the diff wrote.
struct RoundTrip {
    std::vector<std::string> mismatches;
    std::string log;
    std::vector<char> patch;
};

// Diffs old -> new (with diff_options), applies the patch to old and
// compares the patched dir with new. Prints the tools' log on a mismatch.
RoundTrip round_trip(const BuildDir& old_files, const BuildDir& new_files, const Identity& identity = {},
                     const std::vector<std::string>& diff_options = {}) {
    ScratchDir dir("patch-roundtrip");
    const std::string old_dir = dir.path() + "/old", new_dir = dir.path() + "/new", out_dir = dir.path() + "/out";
    const std::string old_meta = dir.path() + "/old-meta", new_meta = dir.path() + "/new-meta";
    const std::string patch = dir.path() + "/p.gcpatch", log = dir.path() + "/tools.log";
    const bool by_identity = !identity.old_keys.empty() || !identity.new_keys.empty();
    std::vector<std::string> diff = {GEOCODER_DIFF_BIN, old_dir, new_dir, "-o", patch};
    if (by_identity) diff.insert(diff.end(), {"--identity", old_meta, new_meta});
    diff.insert(diff.end(), diff_options.begin(), diff_options.end());
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
        const BuildDir want = gctest::as_written(new_files);
        for (const auto& [name, bytes] : want)
            if (!std::filesystem::exists(out_dir + "/" + name) || read_file(out_dir + "/" + name) != bytes)
                bad.push_back(name);
        for (const auto& e : std::filesystem::directory_iterator(out_dir))
            if (!want.count(e.path().filename().string())) bad.push_back("extra " + e.path().filename().string());
    }
    auto text = read_file(log);
    r.log.assign(text.begin(), text.end());
    r.patch = read_file(patch);
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

PlaceNode place(uint32_t name) { return test_place(48.0f, 11.0f, name); }

// Nodes numbered i, as street_nodes.bin holds them.
std::vector<char> nodes_of(const std::vector<int>& numbers) {
    std::vector<NodeCoord> nodes;
    for (int i : numbers) nodes.push_back({10.0f + i, 20.0f + i});
    return bytes_of(nodes);
}

// Footprints back to back, as the logical addr_vertices.bin holds them
// (addr_files.h): each a header of encoding 0 and the bbox minimum 48, 11,
// then its numbered vertices, vertex i that many steps north and east of the
// minimum: 10 bytes and 4 a vertex.
std::vector<char> footprints_of(const std::vector<std::vector<int>>& footprints) {
    std::vector<char> blobs;
    for (const std::vector<int>& numbers : footprints) {
        append(blobs, uint8_t{0});
        append(blobs, uint8_t{0});
        append(blobs, 48.0f);
        append(blobs, 11.0f);
        for (int i : numbers) {
            append(blobs, static_cast<uint16_t>(i));
            append(blobs, static_cast<uint16_t>(i));
        }
    }
    return blobs;
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
//   21: 1 -> 2  REPLACE     (C -> D, on street 2), footprint of vertices 1 2 -> 1 9 2
//   22: 2 -> gone           (A, on the deleted street 0)
//   3:  3 -> 3  MATCH       (B, on street 4 -> 3), footprint 3 4, at byte 18 -> 22 of the footprints
//   24: 4 -> 0  moved       (C, on street 1)
//   25:      4  new         (D, on the new street 4)
// addr_postcodes follows the addresses; new 3's postcode changes.
void put_identity_addresses(BuildDir& old_d, BuildDir& new_d, Identity& ids) {
    old_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{
        {48.0f, 11.0f, OLD_B, OLD_B, 3, NO_DATA, 0}, {48.0f, 11.0f, OLD_C, OLD_C, 2, 0, 2},
        {48.0f, 11.0f, OLD_A, OLD_A, 0, NO_DATA, 0}, {48.0f, 11.0f, OLD_B, OLD_B, 4, addr_files::vertex_offset_of(18), 2},
        {48.0f, 11.0f, OLD_C, OLD_C, 1, NO_DATA, 0}});
    old_d["addr_vertices.bin"] = footprints_of({{1, 2}, {3, 4}});
    new_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{
        {48.0f, 11.0f, NEW_C, NEW_C, 1, NO_DATA, 0}, {48.0f, 11.0f, NEW_B, NEW_B, 0, NO_DATA, 0},
        {48.0f, 11.0f, NEW_D, NEW_D, 2, 0, 3}, {48.0f, 11.0f, NEW_B, NEW_B, 3, addr_files::vertex_offset_of(22), 2},
        {48.0f, 11.0f, NEW_D, NEW_D, 4, NO_DATA, 0}});
    new_d["addr_vertices.bin"] = footprints_of({{1, 9, 2}, {3, 4}});
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

// A day's keys by slot, as geocoder-diff --identity-slots takes them from the
// records: record i's key is i, or NO_DATA where `dead` lists it.
std::vector<uint32_t> slot_keys(uint32_t n, const std::vector<uint32_t>& dead) {
    std::vector<uint32_t> keys(n);
    for (uint32_t i = 0; i < n; i++) keys[i] = i;
    for (uint32_t i : dead) keys[i] = NO_DATA;
    return keys;
}

// Streets of two days built in stable slots (slot: old -> new), and the
// sidecars of their slot keys:
//   0: MATCH               (B, nodes 1 2)
//   1: dies                (A, node 3 -> a tombstone)
//   2: REPLACE             (C -> D, nodes 4 5 -> 4 9 5), node offset 3 -> 2
//   3: refilled            (a tombstone -> B, node 6)
//   4: MATCH               (C, node 7), node offset 5 -> 6
//   5: appended            (C, node 8)
void put_slot_streets(BuildDir& old_d, BuildDir& new_d, Identity& ids) {
    const WayHeader dead = street_way_tombstone();
    old_d["street_ways.bin"] = bytes_of(std::vector<WayHeader>{
        {0, 2, 0, OLD_B}, {2, 1, 0, OLD_A}, {3, 2, 0, OLD_C}, dead, {5, 1, 0, OLD_C}});
    old_d["street_nodes.bin"] = nodes_of({1, 2, 3, 4, 5, 7});
    new_d["street_ways.bin"] = bytes_of(std::vector<WayHeader>{
        {0, 2, 0, NEW_B}, dead, {2, 3, 0, NEW_D}, {5, 1, 0, NEW_B}, {6, 1, 0, NEW_C}, {7, 1, 0, NEW_C}});
    new_d["street_nodes.bin"] = nodes_of({1, 2, 4, 9, 5, 6, 7, 8});
    ids.old_keys["street"] = slot_keys(5, {3});
    ids.new_keys["street"] = slot_keys(6, {1});
}

// Addresses on put_slot_streets' streets, in stable slots (slot: old -> new):
//   0: MATCH               (B, on street 0)
//   1: refilled            (a tombstone -> D, on the appended street 5)
//   2: REPLACE             (C -> D, on street 2), footprint of vertices 1 2 -> 1 9 2
//   3: dies                (A, on the dead street 1 -> a tombstone)
//   4: MATCH               (B, on street 4), footprint 3 4, at byte 18 -> 22 of the footprints
void put_slot_addresses(BuildDir& old_d, BuildDir& new_d, Identity& ids) {
    const AddrPoint dead = addr_point_tombstone();
    old_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{
        {48.0f, 11.0f, OLD_B, OLD_B, 0, NO_DATA, 0}, dead, {48.0f, 11.0f, OLD_C, OLD_C, 2, 0, 2},
        {48.0f, 11.0f, OLD_A, OLD_A, 1, NO_DATA, 0}, {48.0f, 11.0f, OLD_B, OLD_B, 4, addr_files::vertex_offset_of(18), 2}});
    old_d["addr_vertices.bin"] = footprints_of({{1, 2}, {3, 4}});
    new_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{
        {48.0f, 11.0f, NEW_B, NEW_B, 0, NO_DATA, 0}, {48.0f, 11.0f, NEW_D, NEW_D, 5, NO_DATA, 0},
        {48.0f, 11.0f, NEW_D, NEW_D, 2, 0, 3}, dead, {48.0f, 11.0f, NEW_B, NEW_B, 4, addr_files::vertex_offset_of(22), 2}});
    new_d["addr_vertices.bin"] = footprints_of({{1, 9, 2}, {3, 4}});
    ids.old_keys["addr"] = slot_keys(5, {1});
    ids.new_keys["addr"] = slot_keys(5, {3});
}

// Interpolations in stable slots (slot: old -> new):
//   0: REPLACE             (B 1-9 -> 1-11, nodes 1 2 -> 1 9 2)
//   1: MATCH               (C 2-8, node 3), node offset 2 -> 3
//   2: dies                (A 1-5, node 4 -> a tombstone)
//   3: refilled            (a tombstone -> D 1-3, node 6)
void put_slot_interps(BuildDir& old_d, BuildDir& new_d, Identity& ids) {
    const InterpWay dead = interp_way_tombstone();
    old_d["interp_ways.bin"] = bytes_of(std::vector<InterpWay>{
        {0, 2, 0, OLD_B, 1, 9, 0}, {2, 1, 0, OLD_C, 2, 8, 0}, {3, 1, 0, OLD_A, 1, 5, 0}, dead});
    old_d["interp_nodes.bin"] = nodes_of({1, 2, 3, 4});
    new_d["interp_ways.bin"] = bytes_of(std::vector<InterpWay>{
        {0, 3, 0, NEW_B, 1, 11, 0}, {3, 1, 0, NEW_C, 2, 8, 0}, dead, {4, 1, 0, NEW_D, 1, 3, 0}});
    new_d["interp_nodes.bin"] = nodes_of({1, 9, 2, 3, 6});
    ids.old_keys["interp"] = slot_keys(4, {3});
    ids.new_keys["interp"] = slot_keys(4, {2});
}

// way_parents.bin of SPARSE_RECORDS admin slots, the SPARSE_CHANGES
// (position, slot) changed on the new day. With no street ways to follow, the diff sends
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

// way_parents.bin of ids as the builder writes it.
std::vector<char> written_way_parents(const std::vector<uint32_t>& ids) {
    return gctest::as_written({{"way_parents.bin", bytes_of(ids)}}).at("way_parents.bin");
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
// the patch holds no single way_parents sparse delta of the values' logical
// sizes, or does not list the file once at its size on disk.
struct SparsePatch {
    std::vector<char> raw;
    size_t new_size_at = 0, last_pos_at = 0, listed_size_at = 0;

    // The section's new size, the values' logical bytes.
    void set_new_size(uint64_t size) { put_at(raw, new_size_at, size); }
    // The client file list's size of the file on disk, which the patcher
    // holds its output to.
    void set_listed_size(uint64_t size) { put_at(raw, listed_size_at, size); }
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
    append(section, uint32_t{0});
    append(section, static_cast<uint32_t>(std::size(SPARSE_CHANGES)));
    // name_len, name, size, inline
    const std::string name = "way_parents.bin";
    std::vector<char> listed;
    append(listed, static_cast<uint16_t>(name.size()));
    listed.insert(listed.end(), name.begin(), name.end());
    append(listed, static_cast<uint64_t>(written_way_parents(new_way_parents()).size()));
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

// MATCH_POIS polygon POIs of MATCH_RING vertices (a 10-byte header, 4 bytes
// a vertex: an open ring, each POI's of its own) each; the new day drops POI
// MATCH_DROPPED. The diff sends both
// files as MATCH, DELETE, MATCH runs: poi_records.bin by record (replayed
// record by record) and poi_vertices.bin by byte (copied a run at a time).
constexpr uint32_t MATCH_POIS = 300, MATCH_DROPPED = 100, MATCH_RING = 3;
constexpr uint32_t MATCH_BLOCK = 10 + 4 * MATCH_RING;

BuildDir poi_polygons(bool drop) {
    std::vector<PoiRecord> records;
    std::vector<char> vertices;
    for (uint32_t i = 0; i < MATCH_POIS; i++) {
        if (drop && i == MATCH_DROPPED) continue;
        PoiRecord p = poi(NO_DATA);
        p.lat = 48.0f + static_cast<float>(i) * 1e-3f;
        p.vertex_offset = static_cast<uint32_t>(vertices.size());
        p.vertex_count = MATCH_RING;
        records.push_back(p);
        for (uint32_t b = 0; b < MATCH_BLOCK; b++) vertices.push_back(b < 2 ? 0 : static_cast<char>(i * 7 + b));
    }
    return {{"poi_records.bin", bytes_of(records)}, {"poi_vertices.bin", vertices}};
}

// n polygon POIs, POI i's vertex block holding i; the slots [dead, dead +
// dead_n) hold strategy-2 tombstones instead, as a day's in-place deaths
// leave them, and the vertex stream lacks their blocks.
BuildDir pois_with_dead_run(uint32_t n, uint32_t dead, uint32_t dead_n) {
    std::vector<PoiRecord> records;
    std::vector<char> vertices;
    for (uint32_t i = 0; i < n; i++) {
        if (i >= dead && i < dead + dead_n) {
            PoiRecord tombstone{};
            tombstone.vertex_offset = NO_DATA;
            tombstone.name_id = NO_DATA;
            records.push_back(tombstone);
            continue;
        }
        PoiRecord p = poi(NO_DATA);
        p.lat = 48.0f + static_cast<float>(i) * 1e-4f;
        p.vertex_offset = static_cast<uint32_t>(vertices.size());
        p.vertex_count = 1;
        records.push_back(p);
        vertices.resize(vertices.size() + 10);
        append(vertices, i);
    }
    return {{"poi_records.bin", bytes_of(records)}, {"poi_vertices.bin", vertices}};
}

// pois_with_dead_run(n, 0, 0) with the importance of the POIs `edited` marks
// raised, as wikidata sitelinks raise it; their vertex blocks stay.
BuildDir pois_reranked(uint32_t n, const std::vector<bool>& edited) {
    BuildDir d = pois_with_dead_run(n, 0, 0);
    std::vector<char>& records = d["poi_records.bin"];
    for (uint32_t i = 0; i < n; i++)
        if (edited[i]) records[size_t(i) * POI_RECORD_STRIDE + offsetof(PoiRecord, importance)] = 40;
    return d;
}

// The record counts by op of the diff's merge of file `name` ("m/i/d/r=..."
// of its log line), printed; "" without one.
std::string merge_records(const RoundTrip& r, const std::string& name) {
    const size_t line = r.log.find(name + ": seq=");
    const size_t at = line == std::string::npos ? line : r.log.find("recs ", line);
    if (at == std::string::npos) return "";
    const std::string recs = r.log.substr(at + 5, r.log.find('\n', at) - at - 5);
    std::printf("    %s merge records %s\n", name.c_str(), recs.c_str());
    return recs;
}

// The runs of one file's merge sequence: MATCH kept, DELETE gone, MATCH rest.
std::vector<char> match_delete_match(uint32_t kept, uint32_t gone, uint32_t rest) {
    std::vector<char> runs;
    for (const auto& [op, count] : {std::pair<uint8_t, uint32_t>{OP_MATCH_RUN, kept}, {OP_DELETE_RUN, gone}, {OP_MATCH_RUN, rest}}) {
        append(runs, op);
        append(runs, count);
    }
    return runs;
}

// raw in one zstd frame of raw blocks whose header declares a window of
// 2^window_log bytes and no content size, as a compressor of a stream writes
// one (the diff's zstd reads the patch from a pipe).
std::vector<char> zstd_frame(const std::vector<char>& raw, int window_log) {
    std::vector<char> frame = {'\x28', '\xb5', '\x2f', '\xfd', 0, static_cast<char>((window_log - 10) << 3)};
    constexpr size_t BLOCK = 128 << 10;
    for (size_t at = 0; at < raw.size(); at += BLOCK) {
        const size_t n = std::min(BLOCK, raw.size() - at);
        // Last_Block, then Block_Type 0 (raw), then Block_Size.
        const uint32_t head = static_cast<uint32_t>(n << 3) | (at + n == raw.size() ? 1u : 0u);
        for (int k = 0; k < 3; k++) frame.push_back(static_cast<char>(head >> (8 * k)));
        frame.insert(frame.end(), raw.begin() + static_cast<std::ptrdiff_t>(at), raw.begin() + static_cast<std::ptrdiff_t>(at + n));
    }
    return frame;
}

// A patch file's bytes decompressed, as the patcher reads them; none when
// zstd fails.
std::vector<char> unpacked(const std::vector<char>& patch) {
    ScratchDir dir("patch-unpacked");
    const std::string packed = dir.path() + "/p.gcpatch", raw = dir.path() + "/p.raw";
    if (!write_file(packed, patch) || !run({"zstd", "-d", "-q", "-f", packed, "-o", raw}, dir.path() + "/zstd.log")) return {};
    return read_file(raw);
}

// n small JSON files: client files the diff carries inline, whatever their
// names.
BuildDir json_files(int n) {
    BuildDir d;
    for (int i = 0; i < n; i++) {
        char name[16];
        std::snprintf(name, sizeof name, "f%04d.json", i);
        d[name] = {'{', '}'};
    }
    return d;
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

    const RoundTrip r = round_trip(old_d, new_d);

    CHECK(r.mismatches.empty());
    // The second way's postcode is the old one where the remap puts it; the
    // first's is another.
    CHECK(logged(r, "way_postcodes.bin: parallel delta along street_ways.bin, 0 inserted, 1 changes"));
}

// A side file whose values a day keeps goes as unchanged, beside its parent's
// records and without them, and the patched dir holds it as the builder
// writes it.
TEST(patch_round_trip_keeps_a_side_file_a_day_leaves_unchanged) {
    for (const bool with_parent : {true, false}) {
        BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(OLD_POOL);
        if (with_parent) put_shifted_streets(old_d, new_d);
        old_d["way_parents.bin"] = new_d["way_parents.bin"] = bytes_of(std::vector<uint32_t>{7, NO_DATA});

        const RoundTrip r = round_trip(old_d, new_d);

        CHECK(r.mismatches.empty());
        CHECK(logged(r, "way_parents.bin: unchanged (copy old, 8 bytes saved)"));
    }
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

// A geo index of many blocks in three groups, whose lists repeat and edit
// the row before's as the builder's do, a day on: cells dropped, cells new,
// lists that gain and lose ids in every table. The patcher's index is the
// builder's, byte for byte (geo_index::Writer on both sides).
TEST(patch_round_trip_rebuilds_a_geo_index_of_many_groups_as_the_builder_writes_it) {
    std::mt19937_64 rng(20261109);
    std::vector<GeoCell> old_cells, new_cells;
    std::vector<uint32_t> prev[3] = {{1, 2}, {1}, {4}};
    for (uint64_t k = 0; old_cells.size() < 9000; k += 2 * (1 + rng() % 3)) {
        GeoCell c{geo_index::cell_of(1000 + k, 17), {}, {}, {}};
        std::vector<uint32_t>* lists[3] = {&c.streets, &c.addrs, &c.interps};
        for (int t = 0; t < 3; t++) {
            const uint64_t kind = rng() % 6;
            if (kind == 0) continue;
            std::vector<uint32_t>& l = *lists[t];
            if (kind < 3) l = prev[t];
            else if (kind == 3) { l = prev[t]; l.push_back(static_cast<uint32_t>(rng() % 3000)); }
            else for (int j = 0; j < 1 + int(rng() % 4); j++) l.push_back(static_cast<uint32_t>(rng() % 3000));
            std::sort(l.begin(), l.end());
            l.erase(std::unique(l.begin(), l.end()), l.end());
            prev[t] = l;
        }
        if (c.streets.empty() && c.addrs.empty() && c.interps.empty()) c.streets = {7};
        old_cells.push_back(c);
    }
    for (const GeoCell& c : old_cells) {
        const uint64_t fate = rng() % 10;
        if (fate == 0) continue;  // dropped
        GeoCell n = c;
        if (fate == 1) {
            std::vector<uint32_t>& l = rng() % 2 ? n.streets : n.interps;
            if (!l.empty()) l.erase(l.begin());
            l.push_back(3000 + static_cast<uint32_t>(rng() % 100));
            std::sort(l.begin(), l.end());
        }
        if (fate == 2) n.addrs.push_back(5000 + static_cast<uint32_t>(new_cells.size()));
        new_cells.push_back(n);
        if (fate == 3) new_cells.push_back({c.id + (uint64_t(1) << 27), {9}, {}, {2}});  // a new cell beside it
    }
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(OLD_POOL);
    // The records the lists name, the same on both days: every id keeps its value.
    std::vector<WayHeader> ways;
    std::vector<InterpWay> interps;
    std::vector<NodeCoord> nodes;
    std::vector<AddrPoint> points;
    for (uint32_t w = 0; w < 3200; w++) {
        ways.push_back({w, 1, 0, NO_DATA});
        interps.push_back({w, 1, 0, NO_DATA, 1, 9, 0});
        nodes.push_back({10.0f + float(w) * 1e-4f, 20.0f});
    }
    for (uint32_t k = 0; k < 15000; k++) points.push_back({48.0f + float(k) * 1e-5f, 11.0f, NO_DATA, NO_DATA, NO_DATA, NO_DATA, 0});
    for (BuildDir* d : {&old_d, &new_d}) {
        (*d)["street_ways.bin"] = bytes_of(ways);
        (*d)["street_nodes.bin"] = (*d)["interp_nodes.bin"] = bytes_of(nodes);
        (*d)["interp_ways.bin"] = bytes_of(interps);
        (*d)["addr_points.bin"] = bytes_of(points);
    }
    put_geo_cells(old_d, old_cells);
    put_geo_cells(new_d, new_cells);
    REQUIRE(old_d["geo_cells.bin"].size() == 8 + 3 * 12 + 8);

    auto r = round_trip(old_d, new_d);

    CHECK(r.mismatches.empty());
    CHECK(logged(r, "Geo: " + std::to_string(new_cells.size()) + " cells written"));
}

TEST(patch_round_trip_keeps_the_poi_ids_of_unchanged_poi_records) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    old_d["poi_records.bin"] = new_d["poi_records.bin"] = bytes_of(std::vector<PoiRecord>{poi(NAME_2), poi(NAME_4)});
    put_cell_index(old_d, "poi", {0, 1});
    put_cell_index(new_d, "poi", {0, 1});

    CHECK(round_trip(old_d, new_d).mismatches.empty());
}

// The first nightly without unnamed POIs tombstoned 24,543 contiguous planet
// poi/all records in place: the POIs after the run stay MATCHed (their
// vertex offsets fixed up) instead of going as DELETE + INSERT.
TEST(patch_round_trip_matches_the_pois_after_a_long_run_of_tombstones) {
    constexpr uint32_t HEAD = 1000, DEAD = 12000, TAIL = 5000;
    auto r = round_trip(pois_with_dead_run(HEAD + DEAD + TAIL, HEAD, 0), pois_with_dead_run(HEAD + DEAD + TAIL, HEAD, DEAD));

    CHECK(r.mismatches.empty());
    CHECK_EQ(merge_records(r, "poi_records.bin"), "m/i/d/r=" + std::to_string(HEAD + TAIL) + "/" + std::to_string(DEAD) +
                                                      "/" + std::to_string(DEAD) + "/0");
}

// The 2026-11-03 nightly raised the importance of ~1 planet poi/all record in
// 23, in stretches where 7 in 10 changed: the POIs kept among them stay
// MATCHed instead of the rest of the file going as DELETE + INSERT.
TEST(patch_round_trip_matches_the_pois_kept_among_reranked_ones) {
    constexpr uint32_t N = 30000, DENSE = 1000, DENSE_N = 12000;
    std::mt19937 rng(20261103);
    const auto edited = gctest::edited_in_place(rng, N, DENSE, DENSE_N);
    const auto n_edited = static_cast<uint32_t>(std::count(edited.begin(), edited.end(), true));
    auto r = round_trip(pois_with_dead_run(N, 0, 0), pois_reranked(N, edited));

    CHECK(r.mismatches.empty());
    CHECK_EQ(merge_records(r, "poi_records.bin"), "m/i/d/r=" + std::to_string(N - n_edited) + "/" +
                                                      std::to_string(n_edited) + "/" + std::to_string(n_edited) + "/0");
}

// The place files, their names moved by the string remap, round-trip: the
// three of them from place_nodes.bin's logical section.
TEST(patch_round_trip_keeps_the_place_files) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    PlaceNode hidden = place(NAME_4);
    hidden._pad1 = PLACE_FLAG_HIDDEN;
    put_places(old_d, {place(NAME_2), hidden});
    put_places(new_d, {place(NAME_2), hidden});

    const RoundTrip r = round_trip(old_d, new_d);

    CHECK(r.mismatches.empty());
}

// geocoder-diff --decode-logical DIR OUT writes the logical form the merges
// read of each family the builder writes encoded: the POI files as 36-byte
// records and their vertex stream, the place files as place_nodes.bin's
// 24-byte rows (the visible nodes with their cells' keys, then the hidden
// ones). A dir with one of the families gives that family's files and a dir
// with neither an empty OUT; no other file of DIR is written. An OUT that
// holds a file, a DIR that is no dir and a DIR with one of the two POI files
// are refused.
TEST(decode_logical_writes_the_poi_and_place_files_a_dir_holds_and_none_it_lacks) {
    ScratchDir dir("decode-logical");
    const std::string log = dir.path() + "/tools.log";
    // The tool's exit status 0, and its log.
    auto decode = [&](const std::string& in, const std::string& out) {
        std::filesystem::remove(log);
        const bool ok = run({GEOCODER_DIFF_BIN, "--decode-logical", in, out}, log);
        const std::vector<char> text = read_file(log);
        return std::pair<bool, std::string>(ok, std::string(text.begin(), text.end()));
    };
    const BuildDir pois = poi_polygons(false);
    PlaceNode hidden = place(NAME_4);
    hidden._pad1 = PLACE_FLAG_HIDDEN;
    const std::vector<PlaceNode> visible = {place(NAME_2), place(NAME_4), place(NAME_2), place(NAME_4), place(NAME_2)};
    std::vector<PlaceNode> nodes = visible;
    nodes.insert(nodes.begin() + 2, hidden);
    // put_places' rows: visible node i in the cell of key i / 3.
    std::vector<place_files::LogicalPlace> rows;
    for (uint32_t i = 0; i < visible.size(); i++) rows.push_back({visible[i], i / 3});
    rows.push_back({hidden, NO_DATA});
    const BuildDir places = {{place_files::NODES_FILE, bytes_of(rows)}};

    struct Case {
        std::string name;
        bool poi, places;
    };
    for (const Case& e : {Case{"both", true, true}, {"poi", true, false}, {"places", false, true}, {"neither", false, false}}) {
        // Beside a string tier and a file of u32s, which are no such family's.
        BuildDir d = with_pool(OLD_POOL), want;
        d["admin_parents.bin"] = bytes_of(std::vector<uint32_t>{NAME_2});
        if (e.poi) {
            d.insert(pois.begin(), pois.end());
            want.insert(pois.begin(), pois.end());
        }
        if (e.places) {
            put_places(d, nodes);
            want.insert(places.begin(), places.end());
        }
        const std::string in = dir.path() + "/" + e.name, out = in + ".logical";
        REQUIRE(write_dir(in, d));

        const auto [ok, text] = decode(in, out);

        CHECK(ok);
        CHECK(gctest::read_dir(out) == want);
        // Each family's logical bytes, as its files' footers give them.
        const std::string poi_line = "  poi: " + std::to_string(pois.at("poi_records.bin").size() + pois.at("poi_vertices.bin").size()) +
                                     " logical bytes";
        const std::string places_line = "  places: " + std::to_string(rows.size() * sizeof(rows[0])) + " logical bytes";
        CHECK_EQ(text.find(poi_line) != std::string::npos, e.poi);
        CHECK_EQ(text.find(places_line) != std::string::npos, e.places);
        CHECK_EQ(text.find("  poi: none") != std::string::npos, !e.poi);
        CHECK_EQ(text.find("  places: none") != std::string::npos, !e.places);
    }

    const std::string both = dir.path() + "/both", used = dir.path() + "/used";
    const BuildDir held = {{"held.bin", {'x'}}};
    REQUIRE(write_dir(used, held));
    auto r = decode(both, used);
    CHECK(!r.first);
    CHECK(r.second.find("Output dir is not empty: " + used) != std::string::npos);
    CHECK(gctest::read_dir(used) == held);

    r = decode(dir.path() + "/nowhere", dir.path() + "/nowhere.logical");
    CHECK(!r.first);
    CHECK(r.second.find("Not a dir: " + dir.path() + "/nowhere") != std::string::npos);
    CHECK(!std::filesystem::exists(dir.path() + "/nowhere.logical"));

    REQUIRE(std::filesystem::remove(dir.path() + "/poi/poi_vertices.bin"));
    r = decode(dir.path() + "/poi", dir.path() + "/half.logical");
    CHECK(!r.first);
    CHECK(r.second.find("has one of poi_records.bin and poi_vertices.bin") != std::string::npos);

    // Without both of its paths the option is a usage error.
    std::filesystem::remove(log);
    CHECK(!run({GEOCODER_DIFF_BIN, "--decode-logical", both}, log));
    const std::vector<char> usage = read_file(log);
    CHECK(std::string(usage.begin(), usage.end()).find("geocoder-diff --decode-logical <dir> <out-dir>") != std::string::npos);
}

// --decode-logical writes the values of each side file a dir holds, 4 bytes a
// value, and nothing for one it lacks or for a file of u32s that is no side
// file. A side file the builder would not write is refused by name.
TEST(decode_logical_writes_the_values_of_the_side_files_a_dir_holds) {
    ScratchDir dir("decode-logical-side");
    const std::string log = dir.path() + "/tools.log", in = dir.path() + "/in", out = dir.path() + "/out";
    auto decode = [&](const std::string& to) {
        std::filesystem::remove(log);
        const bool ok = run({GEOCODER_DIFF_BIN, "--decode-logical", in, to}, log);
        const std::vector<char> text = read_file(log);
        return std::pair<bool, std::string>(ok, std::string(text.begin(), text.end()));
    };
    std::vector<uint32_t> parents, postcodes;
    for (uint32_t i = 0; i < 200; i++) {
        parents.push_back(i % 7 == 3 ? NO_DATA : 5000 + i * i);
        postcodes.push_back(i < 70 ? NAME_2 : NAME_4);
    }
    const BuildDir sides = {{"way_parents.bin", bytes_of(parents)}, {"addr_postcodes.bin", bytes_of(postcodes)}};
    BuildDir d = sides;
    d["admin_parents.bin"] = bytes_of(std::vector<uint32_t>{1, 2, 3});
    REQUIRE(write_dir(in, d));
    REQUIRE(read_file(in + "/way_parents.bin") != sides.at("way_parents.bin"));

    auto r = decode(out);

    CHECK(r.first);
    CHECK(gctest::read_dir(out) == sides);
    CHECK(r.second.find("  way_parents: 800 logical bytes") != std::string::npos);
    CHECK(r.second.find("  addr_postcodes: 800 logical bytes") != std::string::npos);
    CHECK(r.second.find("  way_postcodes: none") != std::string::npos);
    CHECK(r.second.find("  interp_postcodes: none") != std::string::npos);

    // The file's magic, its last byte.
    std::vector<char> other = read_file(in + "/addr_postcodes.bin");
    other.back() = '2';
    REQUIRE(write_file(in + "/addr_postcodes.bin", other));
    r = decode(dir.path() + "/refused");
    CHECK(!r.first);
    CHECK(r.second.find("Malformed addr_postcodes.bin: no SDB1 in its footer") != std::string::npos);
}

// --decode-logical writes each way pair a dir holds as the merges read it:
// the 12-byte street ways or 24-byte interpolations, and their ways' nodes
// back to back, 8 bytes a node. A pair of no ways, two footers on disk, gives
// two empty files, and a pair the dir lacks none. A dir with one file of a
// pair, and a file the builder would not write, are refused by name.
TEST(decode_logical_writes_the_way_pairs) {
    ScratchDir dir("decode-logical-ways");
    const std::string log = dir.path() + "/tools.log";
    auto decode = [&](const std::string& in) {
        std::filesystem::remove(log);
        const bool ok = run({GEOCODER_DIFF_BIN, "--decode-logical", in, in + ".logical"}, log);
        const std::vector<char> text = read_file(log);
        return std::pair<bool, std::string>(ok, std::string(text.begin(), text.end()));
    };
    const std::vector<WayHeader> street_ways = {{0, 2, 0, NAME_2}, {0, 0, 0, NAME_4}, {2, 3, 0, NO_DATA}};
    const std::vector<InterpWay> interp_ways = {{0, 1, 0, NAME_2, 1, 9, 1}, {1, 2, 0, NAME_4, 2, 8, 2}};
    const BuildDir streets = {{"street_ways.bin", bytes_of(street_ways)}, {"street_nodes.bin", nodes_of({1, 2, 3, 4, 5})}};
    const BuildDir interps = {{"interp_ways.bin", bytes_of(interp_ways)}, {"interp_nodes.bin", nodes_of({6, 7, 8})}};
    const BuildDir no_streets = {{"street_ways.bin", {}}, {"street_nodes.bin", {}}};
    struct Case {
        std::string name;
        BuildDir streets, interps;
        std::string streets_line, interps_line;
    };
    for (const Case& e : {Case{"both", streets, interps, "  streets: 76 logical bytes", "  interps: 72 logical bytes"},
                          Case{"streets", streets, {}, "  streets: 76 logical bytes", "  interps: none"},
                          Case{"no-ways", no_streets, interps, "  streets: 0 logical bytes", "  interps: 72 logical bytes"}}) {
        BuildDir want = e.streets;
        want.insert(e.interps.begin(), e.interps.end());
        // Beside a file of u32s, which is no pair's.
        BuildDir d = want;
        d["admin_parents.bin"] = bytes_of(std::vector<uint32_t>{1, 2, 3});
        const std::string in = dir.path() + "/" + e.name;
        REQUIRE(write_dir(in, d));
        REQUIRE(read_file(in + "/street_ways.bin") != e.streets.at("street_ways.bin"));

        const auto [ok, text] = decode(in);

        CHECK(ok);
        CHECK(gctest::read_dir(in + ".logical") == want);
        CHECK(text.find(e.streets_line) != std::string::npos);
        CHECK(text.find(e.interps_line) != std::string::npos);
    }
    REQUIRE(read_file(dir.path() + "/no-ways/street_ways.bin").size() == 8);
    REQUIRE(read_file(dir.path() + "/no-ways/street_nodes.bin").size() == 16);

    const std::string half = dir.path() + "/half", other = dir.path() + "/other";
    BuildDir both = streets;
    both.insert(interps.begin(), interps.end());
    REQUIRE(write_dir(half, both) && write_dir(other, both));
    REQUIRE(std::filesystem::remove(half + "/street_nodes.bin"));
    auto r = decode(half);
    CHECK(!r.first);
    CHECK(r.second.find("Malformed way files: street_ways.bin without street_nodes.bin") != std::string::npos);

    // The interpolations' count of their nodes, the first byte of the nodes
    // file's footer.
    std::vector<char> nodes = read_file(other + "/interp_nodes.bin");
    nodes[nodes.size() - 16]++;
    REQUIRE(write_file(other + "/interp_nodes.bin", nodes));
    r = decode(other);
    CHECK(!r.first);
    CHECK(r.second.find("Malformed way files: interp_nodes.bin counts 4 nodes and its ways have 3") != std::string::npos);
}

// --decode-logical writes the address files a dir holds as the merges read
// them: the 28-byte address points, a dead slot's among them, and their
// footprints' logical blobs back to back; addr_index.bin has no logical
// form. The three footers of no address give two empty files, and a dir
// without the files none. A dir with some of the three, and files the builder
// would not write, are refused by name.
TEST(decode_logical_writes_the_address_pair) {
    ScratchDir dir("decode-logical-addr");
    const std::string log = dir.path() + "/tools.log";
    auto decode = [&](const std::string& in) {
        std::filesystem::remove(log);
        const bool ok = run({GEOCODER_DIFF_BIN, "--decode-logical", in, in + ".logical"}, log);
        const std::vector<char> text = read_file(log);
        return std::pair<bool, std::string>(ok, std::string(text.begin(), text.end()));
    };
    // A point, an open footprint, a dead slot and a closed footprint.
    const std::vector<AddrPoint> points = {{48.0f, 11.0f, NAME_2, NAME_4, 7, NO_DATA, 0},
                                           {48.1f, 11.1f, NAME_4, NAME_2, NO_DATA, 0, 2},
                                           addr_point_tombstone(),
                                           {48.2f, 11.2f, NAME_2, NAME_2, 9, addr_files::vertex_offset_of(18), 3}};
    const BuildDir addrs = {{"addr_points.bin", bytes_of(points)}, {"addr_vertices.bin", footprints_of({{1, 2}, {3, 4, 3}})}};
    const BuildDir no_addrs = {{"addr_points.bin", {}}, {"addr_vertices.bin", {}}};
    struct Case {
        std::string name;
        BuildDir addrs;
        std::string line;
    };
    for (const Case& e : {Case{"some", addrs, "  addr: 152 logical bytes"}, Case{"none", no_addrs, "  addr: 0 logical bytes"},
                          Case{"without", {}, "  addr: none"}}) {
        // Beside a file of u32s, which is no address file.
        BuildDir d = e.addrs;
        d["admin_parents.bin"] = bytes_of(std::vector<uint32_t>{1, 2, 3});
        const std::string in = dir.path() + "/" + e.name;
        REQUIRE(write_dir(in, d));
        REQUIRE(std::filesystem::exists(in + "/addr_index.bin") == !e.addrs.empty());

        const auto [ok, text] = decode(in);

        CHECK(ok);
        CHECK(gctest::read_dir(in + ".logical") == e.addrs);
        CHECK(text.find(e.line) != std::string::npos);
    }
    REQUIRE(read_file(dir.path() + "/some/addr_points.bin") != addrs.at("addr_points.bin"));
    REQUIRE(read_file(dir.path() + "/none/addr_points.bin").size() == 8);
    REQUIRE(read_file(dir.path() + "/none/addr_vertices.bin").size() == 16);
    REQUIRE(read_file(dir.path() + "/none/addr_index.bin").size() == 8);

    const std::string part = dir.path() + "/part", other = dir.path() + "/other";
    REQUIRE(write_dir(part, addrs) && write_dir(other, addrs));
    REQUIRE(std::filesystem::remove(part + "/addr_index.bin"));
    auto r = decode(part);
    CHECK(!r.first);
    CHECK(r.second.find("Malformed address files: a dir with some of addr_points.bin, addr_vertices.bin and addr_index.bin "
                        "and not all three") != std::string::npos);

    // The footprints' count, after the eight bytes of the logical stream's
    // length in addr_vertices.bin's footer.
    std::vector<char> vertices = read_file(other + "/addr_vertices.bin");
    vertices[vertices.size() - 8]++;
    REQUIRE(write_file(other + "/addr_vertices.bin", vertices));
    r = decode(other);
    CHECK(!r.first);
    CHECK(r.second.find("Malformed address files: addr_vertices.bin counts 3 blobs and holds 2") != std::string::npos);
}

// geocoder-diff --logical-size DIR prints two numbers, from DIR's file sizes
// and footers alone. The bytes a diff maps for DIR's record files: every .bin
// but the string tiers at its size, the way pairs and the address, POI, place
// and side files at their logical size. And the bytes it writes to scratch for DIR:
// those logical files and the pool of each string tier as the diff finds it,
// in DIR, else ../full, else ../../full, an empty pool as none. A DIR that is
// no dir, and the option without its one path, are refused.
TEST(logical_size_prints_the_mapped_and_the_scratch_bytes_of_a_dir) {
    ScratchDir dir("logical-size");
    const std::string log = dir.path() + "/tools.log", region = dir.path() + "/region";
    // The tool's exit status 0, and its output.
    auto sizes_of = [&](const std::vector<std::string>& paths) {
        std::filesystem::remove(log);
        std::vector<std::string> words = {GEOCODER_DIFF_BIN, "--logical-size"};
        words.insert(words.end(), paths.begin(), paths.end());
        const bool ok = run(words, log);
        const std::vector<char> text = read_file(log);
        return std::pair<bool, std::string>(ok, std::string(text.begin(), text.end()));
    };
    auto two = [](uint64_t mapped, uint64_t scratch) { return std::to_string(mapped) + " " + std::to_string(scratch) + "\n"; };
    auto pool = [](const std::string& strings) { return std::vector<char>(strings.begin(), strings.end()); };

    const BuildDir pois = poi_polygons(false);
    const uint64_t poi_bytes = pois.at("poi_records.bin").size() + pois.at("poi_vertices.bin").size();
    PlaceNode hidden = place(NAME_4);
    hidden._pad1 = PLACE_FLAG_HIDDEN;
    const std::vector<PlaceNode> nodes = {place(NAME_2), place(NAME_4), hidden, place(NAME_2)};
    const uint64_t place_bytes = nodes.size() * place_files::LOGICAL_STRIDE;
    const std::vector<char> way_parents = bytes_of(std::vector<uint32_t>{5, 6});
    const uint64_t side_bytes = way_parents.size();
    const std::vector<char> street_ways = bytes_of(std::vector<WayHeader>{{0, 2, 0, NAME_2}, {2, 1, 0, NAME_4}});
    const std::vector<char> street_nodes = nodes_of({1, 2, 3});
    const uint64_t street_bytes = street_ways.size() + street_nodes.size();
    const std::vector<char> addr_points =
        bytes_of(std::vector<AddrPoint>{addr_point(NAME_2, 0), {48.0f, 11.0f, NAME_4, NAME_4, 1, 0, 2}});
    const std::vector<char> addr_vertices = footprints_of({{1, 2}});
    const uint64_t addr_bytes = addr_points.size() + addr_vertices.size();
    const std::string street_pool("Elm\0Oak\0Pine\0", 13);

    // full: two string tiers of its own, the street pair, the address files,
    // a geo index, the POI and place files, a side file, and files that are
    // no .bin.
    BuildDir full = with_pool(OLD_POOL);
    full["strings_street.bin"] = pool(street_pool);
    full["street_ways.bin"] = street_ways;
    full["street_nodes.bin"] = street_nodes;
    full["addr_points.bin"] = addr_points;
    full["addr_vertices.bin"] = addr_vertices;
    full["way_parents.bin"] = way_parents;
    put_geo_cells(full, {{CELL, {0, 1}, {}, {}}});
    full.insert(pois.begin(), pois.end());
    put_places(full, nodes);
    full["strings_layout.json"] = {'{', '}'};
    full["street_ways.bin.osm_ids"] = std::vector<char>(64, 'i');
    REQUIRE(write_dir(region + "/full", full));
    const BuildDir written = gctest::as_written(full);
    const uint64_t raw_bytes = written.at("geo_cells.bin").size() + written.at("geo_entries.bin").size();
    // The encoded files are not their logical size, so neither number is a
    // sum of file sizes.
    REQUIRE(written.at("street_ways.bin").size() + written.at("street_nodes.bin").size() != street_bytes);
    REQUIRE(written.at("addr_points.bin").size() + written.at("addr_vertices.bin").size() + written.at("addr_index.bin").size() !=
            addr_bytes);
    REQUIRE(written.at("poi_records.bin").size() + written.at("poi_vertices.bin").size() != poi_bytes);
    REQUIRE(written.at("way_parents.bin").size() != side_bytes);
    REQUIRE(written.at("strings_street.bin").size() != street_pool.size());

    auto r = sizes_of({region + "/full"});
    CHECK(r.first);
    const uint64_t family_bytes = street_bytes + addr_bytes + poi_bytes + place_bytes + side_bytes;
    CHECK_EQ(r.second, two(raw_bytes + family_bytes, family_bytes + OLD_POOL.size() + street_pool.size()));

    // A dir beside full with one tier of its own, another of its own that
    // is an empty pool, and no family: the tiers it lacks are ../full's.
    const std::string own_pool("X\0Y\0", 4);
    BuildDir beside;
    beside["strings_core.bin"] = pool(own_pool);
    beside["strings_street.bin"] = {};
    beside["admin_parents.bin"] = bytes_of(std::vector<uint32_t>{5, 6, 7, 8});
    REQUIRE(write_dir(region + "/admin", beside));
    r = sizes_of({region + "/admin"});
    CHECK(r.first);
    CHECK_EQ(r.second, two(16, own_pool.size() + street_pool.size()));

    // A dir two levels below full's parent, with a family and no tier.
    REQUIRE(write_dir(region + "/poi/major", pois));
    r = sizes_of({region + "/poi/major"});
    CHECK(r.first);
    CHECK_EQ(r.second, two(poi_bytes, poi_bytes + OLD_POOL.size() + street_pool.size()));

    // No full beside or above it: nothing is decoded for it.
    REQUIRE(write_dir(dir.path() + "/alone/deep/lone", {{"admin_parents.bin", bytes_of(std::vector<uint32_t>{5, 6})}}));
    r = sizes_of({dir.path() + "/alone/deep/lone"});
    CHECK(r.first);
    CHECK_EQ(r.second, two(8, 0));

    r = sizes_of({dir.path() + "/nowhere"});
    CHECK(!r.first);
    CHECK_EQ(r.second, "Not a dir: " + dir.path() + "/nowhere\n");

    for (const std::vector<std::string>& paths : {std::vector<std::string>{}, {region + "/full", region + "/admin"}}) {
        r = sizes_of(paths);
        CHECK(!r.first);
        CHECK(r.second.find("geocoder-diff --logical-size <dir>") != std::string::npos);
    }
}

// An admin or POI cell index in dirs that hold none of its records (admin
// polygons, POI records) still comes as a cell index delta, which the patcher
// needs to rebuild it: a cell's list changes and a cell is added.
TEST(patch_round_trip_rebuilds_a_cell_index_without_its_records) {
    for (const std::string prefix : {"admin", "poi"}) {
        BuildDir old_d, new_d;
        const cell_pairs::Format format = cell_pairs::format_of(prefix);
        std::tie(old_d[prefix + "_cells.bin"], old_d[prefix + "_entries.bin"]) = write_cell_lists({{CELL, {0, 1}}}, format);
        std::tie(new_d[prefix + "_cells.bin"], new_d[prefix + "_entries.bin"]) =
            write_cell_lists({{CELL, {0, 2}}, {CELL_2, {1}}}, format);

        CHECK(round_trip(old_d, new_d).mismatches.empty());
    }
}

// A postcode the new day moves to another tier, "10115" from the core tier to
// the postcode tier, named by an address's housenumber, addr_postcodes and
// postcode_centroids. "A" moves within the core tier (a run of the remap) and
// "20095" stays where it was.
static void put_moved_postcode(BuildDir& old_d, BuildDir& new_d) {
    constexpr uint32_t OLD_POSTCODE = 0, NEW_POSTCODE = 2, KEPT_POSTCODE = 8, OLD_NAME = 6, NEW_NAME = 0;
    old_d["strings_core.bin"] = {'1', '0', '1', '1', '5', 0, 'A', 0};
    new_d["strings_core.bin"] = {'A', 0};
    old_d["strings_postcode.bin"] = {'2', '0', '0', '9', '5', 0};
    new_d["strings_postcode.bin"] = {'1', '0', '1', '1', '5', 0, '2', '0', '0', '9', '5', 0};
    old_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{{48.0f, 11.0f, OLD_NAME, OLD_NAME, NO_DATA, NO_DATA, 0},
                                                               {48.1f, 11.0f, OLD_POSTCODE, OLD_NAME, NO_DATA, NO_DATA, 0},
                                                               {48.2f, 11.0f, NO_DATA, NO_DATA, NO_DATA, NO_DATA, 0}});
    new_d["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{{48.0f, 11.0f, NEW_NAME, NEW_NAME, NO_DATA, NO_DATA, 0},
                                                               {48.1f, 11.0f, NEW_POSTCODE, NEW_NAME, NO_DATA, NO_DATA, 0},
                                                               {48.2f, 11.0f, NO_DATA, NO_DATA, NO_DATA, NO_DATA, 0}});
    old_d["addr_vertices.bin"] = new_d["addr_vertices.bin"] = {};
    old_d["addr_postcodes.bin"] = bytes_of(std::vector<uint32_t>{OLD_POSTCODE, KEPT_POSTCODE, NO_DATA});
    new_d["addr_postcodes.bin"] = bytes_of(std::vector<uint32_t>{NEW_POSTCODE, KEPT_POSTCODE, NO_DATA});
    // lat, lng, postcode, country code and padding
    auto centroids = [&](uint32_t moved) {
        std::vector<char> out;
        for (const auto& [lat, postcode] : {std::pair<float, uint32_t>{52.5f, moved}, {53.5f, KEPT_POSTCODE}}) {
            append(out, lat);
            append(out, 13.4f);
            append(out, postcode);
            append(out, uint32_t{'D' | 'E' << 8});
        }
        return out;
    };
    old_d["postcode_centroids.bin"] = centroids(OLD_POSTCODE);
    new_d["postcode_centroids.bin"] = centroids(NEW_POSTCODE);
}

// The moved postcode (put_moved_postcode) past the strings section's pairs
// (--string-pairs-max 0): the patcher's string remap keeps its old offset.
// The address whose housenumber names it, a MATCH record, takes its new
// offset as a string fixup, and addr_postcodes (a parallel delta along the
// addresses, remap kind 2) and postcode_centroids (a sparse delta, kind 3)
// take it as a change each. With the pair sent instead, neither has a change.
TEST(patch_round_trip_sends_moved_postcodes_past_the_pair_table_as_changes) {
    BuildDir old_d, new_d;
    put_moved_postcode(old_d, new_d);

    const RoundTrip fixed = round_trip(old_d, new_d, {}, {"--string-pairs-max", "0"});
    CHECK(fixed.mismatches.empty());
    CHECK(logged(fixed, "addr_points.bin: 1 string fixups ("));
    CHECK(logged(fixed, "addr_postcodes.bin: parallel delta along addr_points.bin, 0 inserted, 1 changes"));
    CHECK(logged(fixed, "postcode_centroids.bin: sparse delta 1 changes"));
    const RoundTrip paired = round_trip(old_d, new_d);
    CHECK(paired.mismatches.empty());
    CHECK(!logged(paired, "string fixups"));
    CHECK(logged(paired, "addr_postcodes.bin: parallel delta along addr_points.bin, 0 inserted, 0 changes"));
    CHECK(logged(paired, "postcode_centroids.bin: sparse delta 0 changes"));
}

// The moved postcode's string fixup takes 2 bytes. Under a limit of 2
// (--string-fixup-bytes-max, lower for tests) it round-trips; under 1 the
// diff refuses it, and no patch is written: a merge section's header gives
// its fixups' size as a u32, which fixups past the real limit would wrap,
// and the patcher refuse the section. A limit past a u32 is refused.
TEST(patch_diff_refuses_string_fixups_past_the_sections_limit) {
    BuildDir old_d, new_d;
    put_moved_postcode(old_d, new_d);
    const RoundTrip at_limit = round_trip(old_d, new_d, {}, {"--string-pairs-max", "0", "--string-fixup-bytes-max", "2"});
    CHECK(at_limit.mismatches.empty());
    CHECK(logged(at_limit, "addr_points.bin: 1 string fixups (2 bytes)"));

    ScratchDir dir("patch-diff-string-fixup-limit");
    const std::string& root = dir.path();
    const std::string patch = root + "/p.gcpatch";
    REQUIRE(write_dir(root + "/old", old_d));
    REQUIRE(write_dir(root + "/new", new_d));
    auto diff_refusal = [&](const std::string& limit, const std::string& log) {
        const bool ran = run({GEOCODER_DIFF_BIN, root + "/old", root + "/new", "-o", patch, "--string-pairs-max", "0",
                              "--string-fixup-bytes-max", limit}, log);
        const std::vector<char> text = read_file(log);
        return ran ? std::string() : std::string(text.begin(), text.end());
    };
    CHECK(diff_refusal("1", root + "/past.log").find("String fixups of addr_points.bin: more than the section's 1 bytes") !=
          std::string::npos);
    CHECK(!std::filesystem::exists(patch));
    CHECK(diff_refusal("4294967296", root + "/option.log").find("--string-fixup-bytes-max takes 0 to 4294967295, not 4294967296") !=
          std::string::npos);
    CHECK(!std::filesystem::exists(patch));
}

// Street ways named 2 and 4, the first a node longer on the new day: the
// node fixup moves the second way's node_offset from 1 to 2, a run of one
// record, 3 bytes of offset fixups.
TEST(patch_diff_refuses_offset_fixups_past_the_sections_limit) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(OLD_POOL);
    const NodeCoord x{10.0f, 20.0f}, y{11.0f, 21.0f}, z{12.0f, 22.0f};
    old_d["street_ways.bin"] = bytes_of(std::vector<WayHeader>{{0, 1, 0, NAME_2}, {1, 1, 0, NAME_4}});
    new_d["street_ways.bin"] = bytes_of(std::vector<WayHeader>{{0, 2, 0, NAME_2}, {2, 1, 0, NAME_4}});
    old_d["street_nodes.bin"] = bytes_of(std::vector<NodeCoord>{x, y});
    new_d["street_nodes.bin"] = bytes_of(std::vector<NodeCoord>{x, z, y});
    // Under a limit of 3 (--offset-fixup-bytes-max, lower for tests) they
    // round-trip.
    const RoundTrip at_limit = round_trip(old_d, new_d, {}, {"--offset-fixup-bytes-max", "3"});
    CHECK(at_limit.mismatches.empty());
    CHECK(logged(at_limit, "street_ways.bin: seq="));
    CHECK(logged(at_limit, " fixup_runs=1 fixup_values=0 fixup_bytes=3 "));

    // Under 2 the diff refuses them, and no patch is written: a merge
    // section's header gives its fixups' sizes as u32s, which fixups past the
    // real limit would wrap, and the patcher refuse the section. A limit past
    // a u32 is refused.
    ScratchDir dir("patch-diff-offset-fixup-limit");
    const std::string& root = dir.path();
    const std::string patch = root + "/p.gcpatch";
    REQUIRE(write_dir(root + "/old", old_d));
    REQUIRE(write_dir(root + "/new", new_d));
    auto diff_refusal = [&](const std::string& limit, const std::string& log) {
        const bool ran =
            run({GEOCODER_DIFF_BIN, root + "/old", root + "/new", "-o", patch, "--offset-fixup-bytes-max", limit}, log);
        const std::vector<char> text = read_file(log);
        return ran ? std::string() : std::string(text.begin(), text.end());
    };
    CHECK(diff_refusal("2", root + "/past.log")
              .find("Offset fixups of street_ways.bin: more than the section's 2 bytes of runs") != std::string::npos);
    CHECK(!std::filesystem::exists(patch));
    CHECK(diff_refusal("4294967296", root + "/option.log").find("--offset-fixup-bytes-max takes 0 to 4294967295, not 4294967296") !=
          std::string::npos);
    CHECK(!std::filesystem::exists(patch));
}

// A record file of more records than the diff's limit (--file-records-max,
// FILE_RECORDS_MAX: the u32 record indices' own, lower for tests) is refused
// before it is diffed, by its name and day, and no patch is written: street
// ways of 2 and 3 records, either way round, under a limit of 2; and their
// nodes (8 bytes a record), 2 and 4, under 3. Under 3 the ways round-trip. So
// are address points, and their footprints by the 2-byte unit. A limit past
// FILE_RECORDS_MAX is refused.
TEST(patch_diff_refuses_a_record_file_past_the_limit) {
    const NodeCoord x{10.0f, 20.0f}, y{11.0f, 21.0f}, z{12.0f, 22.0f}, w{13.0f, 23.0f};
    BuildDir two = with_pool(OLD_POOL), three = with_pool(OLD_POOL), four_nodes = with_pool(OLD_POOL);
    two["street_ways.bin"] = bytes_of(std::vector<WayHeader>{{0, 1, 0, NAME_2}, {1, 1, 0, NAME_4}});
    two["street_nodes.bin"] = bytes_of(std::vector<NodeCoord>{x, y});
    three["street_ways.bin"] = bytes_of(std::vector<WayHeader>{{0, 1, 0, NAME_2}, {1, 1, 0, NAME_4}, {2, 1, 0, NAME_4}});
    three["street_nodes.bin"] = bytes_of(std::vector<NodeCoord>{x, y, z});
    four_nodes["street_ways.bin"] = bytes_of(std::vector<WayHeader>{{0, 1, 0, NAME_2}, {1, 3, 0, NAME_4}});
    four_nodes["street_nodes.bin"] = bytes_of(std::vector<NodeCoord>{x, y, z, w});
    const RoundTrip at_limit = round_trip(two, three, {}, {"--file-records-max", "3"});
    CHECK(at_limit.mismatches.empty());

    ScratchDir dir("patch-diff-file-records");
    const std::string& root = dir.path();
    // The diff of old_d to new_d (in root/<name>/old and new) under `limit`:
    // its log when it refused, else "", and whether it wrote no patch.
    auto refusal = [&](const std::string& name, const BuildDir& old_d, const BuildDir& new_d, const std::string& limit) {
        const std::string at = root + "/" + name, patch = at + "/p.gcpatch", log = at + "/diff.log";
        REQUIRE(write_dir(at + "/old", old_d) && write_dir(at + "/new", new_d));
        const bool ran = run({GEOCODER_DIFF_BIN, at + "/old", at + "/new", "-o", patch, "--file-records-max", limit}, log);
        const std::vector<char> text = read_file(log);
        return std::pair(ran ? std::string() : std::string(text.begin(), text.end()), !std::filesystem::exists(patch));
    };
    auto refused_with = [](const std::pair<std::string, bool>& r, const std::string& why) {
        return r.first.find(why) != std::string::npos && r.second;
    };
    CHECK(refused_with(refusal("grows", two, three, "2"), "New street_ways.bin holds 3 records, more than the 2 a patch can index"));
    CHECK(refused_with(refusal("shrinks", three, two, "2"), "Old street_ways.bin holds 3 records, more than the 2 a patch can index"));
    CHECK(refused_with(refusal("nodes", two, four_nodes, "3"), "New street_nodes.bin holds 4 records, more than the 3 a patch can index"));
    // The address files count by their logical form, not by their bytes on
    // disk: 3 old and 2 new address points round-trip under 3 and are refused
    // under 2, and a footprint of two vertices, a logical blob of 18 bytes
    // and so 9 of the units its merge counts, round-trips under 9 and is
    // refused under 8.
    BuildDir three_addrs = with_pool(OLD_POOL), two_addrs = with_pool(OLD_POOL), footprint = with_pool(OLD_POOL);
    const AddrPoint a = addr_point(NAME_2, NO_DATA), b = addr_point(NAME_4, NO_DATA);
    three_addrs["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{a, b, a});
    two_addrs["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{a, b});
    footprint["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{a, {48.0f, 11.0f, NAME_4, NAME_4, NO_DATA, 0, 2}});
    footprint["addr_vertices.bin"] = footprints_of({{1, 2}});
    REQUIRE(footprint.at("addr_vertices.bin").size() == 18 && gctest::as_written(footprint).at("addr_vertices.bin").size() != 18);
    CHECK(round_trip(three_addrs, two_addrs, {}, {"--file-records-max", "3"}).mismatches.empty());
    CHECK(refused_with(refusal("addresses", three_addrs, two_addrs, "2"),
                       "Old addr_points.bin holds 3 records, more than the 2 a patch can index"));
    CHECK(round_trip(two_addrs, footprint, {}, {"--file-records-max", "9"}).mismatches.empty());
    CHECK(refused_with(refusal("footprints", two_addrs, footprint, "8"),
                       "New addr_vertices.bin holds 9 records, more than the 8 a patch can index"));
    CHECK(refused_with(refusal("option", two, three, "4294967295"), "--file-records-max takes 0 to 4294967294, not 4294967295"));
}

// The logical addr_vertices.bin is a stream a patch indexes by its 2-byte
// units in a u32 below NO_DATA. Address files whose footer gives one unit
// more, on the old day or the new, are refused by the file's name and the
// bytes before a block or a blob is decoded (the first block is one no
// encoder writes, and is not what the diff refuses), and no patch is written.
TEST(patch_diff_refuses_address_files_whose_footer_gives_a_footprint_stream_past_the_limit) {
    BuildDir day = with_pool(OLD_POOL);
    day["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{{48.0f, 11.0f, NAME_2, NAME_2, NO_DATA, 0, 2}});
    day["addr_vertices.bin"] = footprints_of({{1, 2}});
    ScratchDir dir("patch-diff-footprint-limit");
    for (const std::string past : {"old", "new"}) {
        const std::string root = dir.path() + "/" + past, patch = root + "/p.gcpatch", log = root + "/diff.log";
        REQUIRE(write_dir(root + "/old", day) && write_dir(root + "/new", day));
        std::vector<char> points = read_file(root + "/" + past + "/addr_points.bin");
        std::vector<char> vertices = read_file(root + "/" + past + "/addr_vertices.bin");
        REQUIRE(points.size() > addr_files::POINTS_FOOTER_BYTES && vertices.size() > addr_files::VERTICES_FOOTER_BYTES);
        points[0] = static_cast<char>(0xFF);
        put_at(vertices, vertices.size() - addr_files::VERTICES_FOOTER_BYTES, addr_files::MAX_LOGICAL_VERTEX_BYTES + 2);
        REQUIRE(write_file(root + "/" + past + "/addr_points.bin", points));
        REQUIRE(write_file(root + "/" + past + "/addr_vertices.bin", vertices));

        const bool ran = run({GEOCODER_DIFF_BIN, root + "/old", root + "/new", "-o", patch}, log);

        const std::vector<char> text = read_file(log);
        const std::string said(text.begin(), text.end());
        CHECK(!ran);
        CHECK(said.find("Malformed address files: a logical addr_vertices.bin of 8589934590 bytes is more than the 8589934588 a "
                        "patch can index") != std::string::npos);
        CHECK(said.find("block 0") == std::string::npos);
        CHECK(!std::filesystem::exists(patch));
    }
}

// The footprints' logical stream travels in 2-byte units. Three addresses
// with footprints of 18, 22 and 18 bytes; the new day moves a vertex of the
// second and appends an address with a footprint of 26. In the patch as the
// diff writes it, addr_vertices.bin's merge section is at stride 2 over the
// streams' 58 and 84 bytes, its runs count units (the edited footprint keeps
// its head and tail in whole ones), and the appended record's vertex_offset
// is 29, half the 58 bytes of the footprints before its own. The patch
// applies.
TEST(patch_sends_the_footprints_in_two_byte_units) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(OLD_POOL);
    const std::vector<AddrPoint> three = {{48.0f, 11.0f, NAME_2, NAME_2, NO_DATA, 0, 2},
                                          {48.1f, 11.1f, NAME_2, NAME_2, NO_DATA, 9, 3},
                                          {48.2f, 11.2f, NAME_2, NAME_2, NO_DATA, 20, 2}};
    const AddrPoint appended = {48.3f, 11.3f, NAME_4, NAME_4, NO_DATA, 29, 4};
    std::vector<AddrPoint> four = three;
    four.push_back(appended);
    old_d["addr_points.bin"] = bytes_of(three);
    old_d["addr_vertices.bin"] = footprints_of({{1, 2}, {3, 4, 5}, {6, 7}});
    new_d["addr_points.bin"] = bytes_of(four);
    new_d["addr_vertices.bin"] = footprints_of({{1, 2}, {3, 9, 5}, {6, 7}, {1, 2, 3, 4}});
    const std::vector<char>& new_stream = new_d.at("addr_vertices.bin");
    REQUIRE(old_d.at("addr_vertices.bin").size() == 58 && new_stream.size() == 84);

    const RoundTrip r = round_trip(old_d, new_d);
    CHECK(r.mismatches.empty());
    ScratchDir dir("patch-footprint-units");
    const std::string packed = dir.path() + "/p.gcpatch", unpacked = dir.path() + "/p.raw";
    REQUIRE(write_file(packed, r.patch) && run({"zstd", "-d", "-q", "-f", packed, "-o", unpacked}, dir.path() + "/zstd.log"));
    const std::vector<char> raw = read_file(unpacked);

    // The section's head, then the offset fixups' and the string fixups'
    // counts and sizes (none), the sequence's length and the sequence.
    std::vector<char> head;
    append(head, static_cast<uint32_t>(PatchFileId::ADDR_VERTICES));
    append(head, uint32_t{2});
    append(head, uint64_t{58});
    append(head, uint64_t{84});
    for (int word = 0; word < 6; word++) append(head, uint32_t{0});
    const size_t at = find_once(raw, head);
    REQUIRE(at != std::string::npos && raw.size() >= at + head.size() + 8);
    uint64_t seq_size = 0;
    memcpy(&seq_size, raw.data() + at + head.size(), 8);
    const size_t seq = at + head.size() + 8;
    REQUIRE(seq_size <= raw.size() - seq);
    // The second footprint's bytes 14 to 17 are its moved vertex.
    std::vector<char> want;
    auto run_of = [&want](uint8_t op, uint32_t units, const char* bytes = nullptr) {
        append(want, op);
        append(want, units);
        if (bytes) want.insert(want.end(), bytes, bytes + size_t{units} * 2);
    };
    run_of(OP_MATCH_RUN, 16);
    run_of(OP_DELETE_RUN, 2);
    run_of(OP_INSERT_RUN, 2, new_stream.data() + 18 + 14);
    run_of(OP_MATCH_RUN, 11);
    run_of(OP_INSERT_RUN, 13, new_stream.data() + 58);
    CHECK(std::vector<char>(raw.begin() + static_cast<std::ptrdiff_t>(seq), raw.begin() + static_cast<std::ptrdiff_t>(seq + seq_size)) == want);

    // The appended record travels as it is in the logical addr_points.bin.
    const std::vector<char> record = bytes_of(std::vector<AddrPoint>{appended});
    const size_t record_at = find_once(raw, record);
    REQUIRE(record_at != std::string::npos);
    uint32_t vertex_offset = 0;
    memcpy(&vertex_offset, raw.data() + record_at + ADDR_POINT_VERTEX_OFFSET_OFF, 4);
    CHECK_EQ(vertex_offset, 29u);
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

// The four side files, each beside the records it follows.
const char* const SIDE_FILES[] = {"way_parents.bin", "way_postcodes.bin", "interp_postcodes.bin", "addr_postcodes.bin"};

// A side file is written only on a day that has a value for it. One the old
// day lacks comes whole, a full replacement of its values (4 bytes each),
// which the patcher writes as the builder does; the other files follow their
// records as on any day.
TEST(patch_round_trip_adds_a_side_file_the_old_day_lacks) {
    for (const char* added : SIDE_FILES) {
        BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
        Identity ids;
        put_identity_streets(old_d, new_d, ids);
        put_identity_addresses(old_d, new_d, ids);
        put_identity_interps(old_d, new_d, ids);
        const std::string values = std::to_string(new_d.at(added).size());
        old_d.erase(added);

        const RoundTrip r = round_trip(old_d, new_d, ids);

        CHECK(r.mismatches.empty());
        CHECK(logged(r, std::string(added) + ": no old file, using full replace (" + values + " bytes)"));
        CHECK(logged(r, std::string(added) + ": full replace " + values + " bytes"));
        for (const char* kept : SIDE_FILES)
            if (kept != added) CHECK(logged(r, std::string(kept) + ": parallel delta along "));
    }
}

// A side file the new day lacks gets no section, and the patched dir does not
// hold it; the other files follow their records.
TEST(patch_round_trip_drops_a_side_file_the_new_day_lacks) {
    for (const char* dropped : SIDE_FILES) {
        BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
        Identity ids;
        put_identity_streets(old_d, new_d, ids);
        put_identity_addresses(old_d, new_d, ids);
        put_identity_interps(old_d, new_d, ids);
        new_d.erase(dropped);

        const RoundTrip r = round_trip(old_d, new_d, ids);

        CHECK(r.mismatches.empty());
        CHECK(!logged(r, dropped));
        for (const char* kept : SIDE_FILES)
            if (kept != dropped) CHECK(logged(r, std::string(kept) + ": parallel delta along "));
    }
}

// A side file of no values is its footer alone, which no build writes: the
// patcher still gives it for a day that has it, from an old day without the
// file, with an empty one or with values, and takes it as an old file of no
// values.
TEST(patch_round_trip_keeps_a_side_file_of_no_values) {
    const std::vector<char> none, some = bytes_of(std::vector<uint32_t>{5, NO_DATA, 7});
    REQUIRE(gctest::as_written({{"way_parents.bin", none}}).at("way_parents.bin").size() == 8);
    const BuildDir without, empty = {{"way_parents.bin", none}}, with = {{"way_parents.bin", some}};

    CHECK(round_trip(without, empty).mismatches.empty());
    CHECK(round_trip(empty, empty).mismatches.empty());
    CHECK(round_trip(with, empty).mismatches.empty());
    CHECK(round_trip(empty, with).mismatches.empty());
}

// A full dir holds both way pairs, and a pair of no ways is its two footers
// (8 and 16 bytes): a family without records, which merges as nothing and
// which the patcher writes again, beside the other pair's day and with both
// pairs so. A dir gains the footers and loses them, and its first
// interpolations come and its last go, from and to the footers and from and
// to no files.
TEST(patch_round_trip_of_a_dir_with_no_ways_no_interpolations_or_neither) {
    const BuildDir no_streets = {{"street_ways.bin", {}}, {"street_nodes.bin", {}}};
    const BuildDir no_interps = {{"interp_ways.bin", {}}, {"interp_nodes.bin", {}}};
    const BuildDir footers = gctest::as_written(no_interps);
    REQUIRE(footers.at("interp_ways.bin").size() == 8 && footers.at("interp_nodes.bin").size() == 16);
    struct Shape {
        bool streets, interps;
    };
    for (const Shape shape : {Shape{true, false}, Shape{false, true}, Shape{false, false}}) {
        BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
        Identity ids;
        if (shape.streets) put_identity_streets(old_d, new_d, ids);
        if (shape.interps) put_identity_interps(old_d, new_d, ids);
        for (BuildDir* d : {&old_d, &new_d}) {
            if (!shape.streets) d->insert(no_streets.begin(), no_streets.end());
            if (!shape.interps) d->insert(no_interps.begin(), no_interps.end());
        }

        const RoundTrip r = round_trip(old_d, new_d, ids);

        CHECK(r.mismatches.empty());
        CHECK_EQ(logged(r, "street_ways identity: match=2 replace=1 insert=2 delete=2 moved=1"), shape.streets);
        CHECK_EQ(logged(r, "interp_ways identity: match=1 replace=1 insert=2 delete=2 moved=1"), shape.interps);
    }

    const BuildDir without = with_pool(OLD_POOL);
    BuildDir empty = without, some = without;
    empty.insert(no_streets.begin(), no_streets.end());
    empty.insert(no_interps.begin(), no_interps.end());
    some.insert(no_streets.begin(), no_streets.end());
    some["interp_ways.bin"] = bytes_of(std::vector<InterpWay>{{0, 2, 0, OLD_B, 1, 9, 0}, {2, 1, 0, OLD_C, 2, 8, 0}});
    some["interp_nodes.bin"] = nodes_of({1, 2, 3});
    CHECK(round_trip(without, empty).mismatches.empty());
    CHECK(round_trip(empty, without).mismatches.empty());
    CHECK(round_trip(empty, some).mismatches.empty());
    CHECK(round_trip(some, empty).mismatches.empty());
    CHECK(round_trip(without, some).mismatches.empty());
    CHECK(round_trip(some, without).mismatches.empty());
}

// A full dir holds the address files, and those of no address are three
// footers (8, 16 and 8 bytes): a family without records, which merges as
// nothing and which the patcher writes again, beside streets that move. Their
// moved pairs go in the secondary remap, as in a dir without the files. A dir
// gains the footers and loses them.
TEST(patch_round_trip_of_a_full_dir_with_no_addresses) {
    const BuildDir no_addrs = {{"addr_points.bin", {}}, {"addr_vertices.bin", {}}};
    const BuildDir footers = gctest::as_written(no_addrs);
    REQUIRE(footers.size() == 3 && footers.at("addr_points.bin").size() == 8 && footers.at("addr_vertices.bin").size() == 16 &&
            footers.at("addr_index.bin").size() == 8);
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    Identity ids;
    put_identity_streets(old_d, new_d, ids);
    for (BuildDir* d : {&old_d, &new_d}) d->insert(no_addrs.begin(), no_addrs.end());

    const RoundTrip r = round_trip(old_d, new_d, ids);

    CHECK(r.mismatches.empty());
    CHECK(logged(r, "street_ways identity: match=2 replace=1 insert=2 delete=2 moved=1"));
    CHECK(logged(r, "Secondary remap 1: 1 pairs"));
    CHECK(logged(r, "addr_vertices.bin: full replace 0 bytes"));

    const BuildDir without = with_pool(OLD_POOL);
    BuildDir empty = without;
    empty.insert(no_addrs.begin(), no_addrs.end());
    CHECK(round_trip(empty, empty).mismatches.empty());
    CHECK(round_trip(without, empty).mismatches.empty());
    CHECK(round_trip(empty, without).mismatches.empty());
}

// The footprints merge as the address points' child stream whether or not
// either day has one, so the day a dir's address points get their first
// footprint and the day they lose their last have a patch: an
// addr_vertices.bin of no blob, its footer alone, on one side.
TEST(patch_round_trip_of_address_points_that_gain_their_first_footprint_or_lose_their_last) {
    BuildDir points = with_pool(OLD_POOL), footprints = with_pool(OLD_POOL);
    const AddrPoint a = addr_point(NAME_2, NO_DATA), b = addr_point(NAME_4, NO_DATA);
    points["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{a, b, a});
    footprints["addr_points.bin"] = bytes_of(std::vector<AddrPoint>{a, {48.0f, 11.0f, NAME_4, NAME_4, NO_DATA, 0, 3}, a});
    footprints["addr_vertices.bin"] = footprints_of({{1, 2, 1}});
    REQUIRE(gctest::as_written(points).at("addr_vertices.bin").size() == addr_files::VERTICES_FOOTER_BYTES);
    REQUIRE(gctest::as_written(footprints).at("addr_vertices.bin").size() > addr_files::VERTICES_FOOTER_BYTES);

    for (const RoundTrip& r : {round_trip(points, footprints), round_trip(footprints, points)}) {
        CHECK(r.mismatches.empty());
        CHECK(logged(r, "  addr_vertices.bin: seq="));
        CHECK(!logged(r, "addr_vertices.bin: full replace"));
    }
}

// A dir gets its first address, or loses its last, on a day with a patch like
// any other: addr_points.bin merges at its records' stride with or without
// records, so the points go as one INSERT of every record or one DELETE, and
// their footprints as that merge's child stream. From and to the three
// footers, and from and to a dir without the files, each merged by content
// and by slot. The day with addresses holds five on streets that move, two
// with a footprint, the second's after the first's, in geo cells that list
// them, with their postcodes; the day without has no addr_postcodes.bin.
TEST(patch_round_trip_of_a_dir_that_gains_its_first_address_or_loses_its_last) {
    struct Day {
        std::string name;
        // Whether the old day is the one without addresses, and whether that
        // day holds the three footers.
        bool gains, footers;
    };
    const std::vector<uint32_t> every = {0, 1, 2, 3, 4}, none;
    for (const Day& day : {Day{"gains", true, true}, Day{"loses", false, true}, Day{"gains-the-files", true, false},
                           Day{"loses-the-files", false, false}})
        for (const bool by_slot : {false, true}) {
            BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
            Identity ids;
            put_identity_streets(old_d, new_d, ids);
            put_identity_addresses(old_d, new_d, ids);
            BuildDir& without = day.gains ? old_d : new_d;
            for (const char* file : {"addr_points.bin", "addr_vertices.bin", "addr_postcodes.bin"}) without.erase(file);
            if (day.footers) without["addr_points.bin"] = without["addr_vertices.bin"] = {};
            put_geo_cells(old_d, {{CELL, every, day.gains ? none : every, {}},
                                  {CELL_2, {2, 3, 4}, day.gains ? none : std::vector<uint32_t>{1, 4}, {}}});
            put_geo_cells(new_d, {{CELL, every, day.gains ? every : none, {}},
                                  {CELL_2, {0, 2, 3}, day.gains ? std::vector<uint32_t>{0, 2} : none, {}}});

            const RoundTrip r = round_trip(old_d, new_d, {}, by_slot ? std::vector<std::string>{"--identity-slots"} : std::vector<std::string>{});

            const int failures_before = gctest::failures();
            CHECK(r.mismatches.empty());
            CHECK(!r.patch.empty());
            CHECK_EQ(merge_records(r, "addr_points.bin"), std::string(day.gains ? "m/i/d/r=0/5/0/0" : "m/i/d/r=0/0/5/0"));
            CHECK(logged(r, "  addr_vertices.bin: seq="));
            CHECK(!logged(r, "addr_vertices.bin: full replace"));
            CHECK(!logged(r, "addr_points: stride mismatch"));
            if (gctest::failures() > failures_before) std::printf("    on the day that %s, %s\n", day.name.c_str(), by_slot ? "by slot" : "by content");
        }
}

// An address family without a record on either day has its sections all the
// same: addr_points.bin's is an empty merge at its records' stride, in a dir
// with the three footers and in one without the files, and the footers'
// addr_vertices.bin goes whole, as no bytes.
TEST(patch_sends_an_empty_address_family_at_its_records_stride) {
    const BuildDir footers = {{"addr_points.bin", {}}, {"addr_vertices.bin", {}}};
    for (const bool with_footers : {true, false}) {
        BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
        Identity ids;
        put_identity_streets(old_d, new_d, ids);
        if (with_footers)
            for (BuildDir* d : {&old_d, &new_d}) d->insert(footers.begin(), footers.end());

        const RoundTrip r = round_trip(old_d, new_d, ids);

        CHECK(r.mismatches.empty());
        CHECK_EQ(logged(r, "addr_vertices.bin: full replace 0 bytes"), with_footers);
        const std::vector<char> raw = unpacked(r.patch);
        // The section's head (its file, its stride and both sizes), then no
        // offset fixups, no string fixups and a sequence of no bytes.
        auto empty_merge_at = [](uint32_t stride) {
            std::vector<char> section;
            append(section, static_cast<uint32_t>(PatchFileId::ADDR_POINTS));
            append(section, stride);
            for (int size = 0; size < 2; size++) append(section, uint64_t{0});
            for (int word = 0; word < 6; word++) append(section, uint32_t{0});
            append(section, uint64_t{0});
            return section;
        };
        CHECK(find_once(raw, empty_merge_at(static_cast<uint32_t>(ADDR_POINT_STRIDE))) != std::string::npos);
        const std::vector<char> at_16 = empty_merge_at(16);
        CHECK(std::search(raw.begin(), raw.end(), at_16.begin(), at_16.end()) == raw.end());
    }
}

// A dir without street files of its own (an admin or a POI variant) merges
// no streets, and the diff reads none of the full dir's beside or above it,
// which are another variant's: under a limit of 3 records a file, a POI dir
// two levels below a full dir of 40 street ways diffs and round-trips.
TEST(patch_diff_takes_no_streets_from_a_sibling) {
    ScratchDir dir("patch-sibling");
    const std::string& root = dir.path();
    const std::string old_dir = root + "/old/poi/major", new_dir = root + "/new/poi/major", log = root + "/tools.log";
    BuildDir full = with_pool(OLD_POOL), old_d, new_d;
    std::vector<WayHeader> ways;
    std::vector<int> nodes;
    for (uint32_t w = 0; w < 40; w++) {
        ways.push_back({w, 1, 0, NAME_2});
        nodes.push_back(static_cast<int>(w));
    }
    full["street_ways.bin"] = bytes_of(ways);
    full["street_nodes.bin"] = nodes_of(nodes);
    old_d["poi_records.bin"] = bytes_of(std::vector<PoiRecord>{poi(NAME_2), poi(NAME_4)});
    new_d["poi_records.bin"] = bytes_of(std::vector<PoiRecord>{poi(NAME_2), poi(NAME_4), poi(NAME_2)});
    REQUIRE(write_dir(root + "/old/full", full) && write_dir(root + "/new/full", full));
    REQUIRE(write_dir(old_dir, old_d) && write_dir(new_dir, new_d));

    const bool diffed = run({GEOCODER_DIFF_BIN, old_dir, new_dir, "-o", root + "/p.gcpatch", "--file-records-max", "3"}, log);
    const bool patched = diffed && run({GEOCODER_PATCH_BIN, old_dir, root + "/p.gcpatch", "-o", root + "/out"}, log);

    if (!patched) {
        const std::vector<char> text = read_file(log);
        std::printf("%s\n", std::string(text.begin(), text.end()).c_str());
    }
    CHECK(diffed);
    CHECK(patched);
    CHECK(patched && gctest::read_dir(root + "/out") == gctest::as_written(new_d));
}

// A sidecar that is not one key per record (short, long or missing) is
// rejected: the log names its family, which merges by content.
TEST(patch_round_trip_warns_of_each_rejected_identity_sidecar_and_merges_its_family_by_content) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    Identity ids;
    put_identity_streets(old_d, new_d, ids);
    put_identity_addresses(old_d, new_d, ids);
    put_identity_interps(old_d, new_d, ids);
    ids.new_keys["street"].pop_back();
    ids.old_keys["addr"].push_back(5);
    ids.old_keys.erase("interp");

    auto r = round_trip(old_d, new_d, ids);

    CHECK(r.mismatches.empty());
    CHECK(logged(r, "new-meta/street.slot.u32 rejected (16 bytes for 5 records); street merges by content"));
    CHECK(logged(r, "old-meta/addr.slot.u32 rejected (24 bytes for 5 records); addr merges by content"));
    CHECK(logged(r, "old-meta/interp.slot.u32 rejected (0 bytes for 4 records); interp merges by content"));
    CHECK(!logged(r, "old-meta/street.slot.u32"));
    CHECK(!logged(r, " identity: match="));
}

// geocoder-diff --identity-slots takes the keys of a day built in stable
// slots from its records, a live record's index and NO_DATA for a tombstone:
// the patch is the one --identity writes from sidecars of those keys, byte
// for byte.
TEST(patch_round_trip_by_slot_identity_gives_the_patch_the_sidecars_gave) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    Identity ids;
    put_slot_streets(old_d, new_d, ids);
    put_slot_addresses(old_d, new_d, ids);
    put_slot_interps(old_d, new_d, ids);
    put_geo_cells(old_d, {{CELL, {0, 1, 2, 4}, {0, 2, 3, 4}, {0, 1, 2}}, {CELL_2, {2, 4}, {0, 4}, {1}}});
    put_geo_cells(new_d, {{CELL, {0, 2, 3, 4, 5}, {0, 1, 2, 4}, {0, 1, 3}}, {CELL_2, {2, 4}, {0, 4}, {1}}});

    const RoundTrip by_sidecar = round_trip(old_d, new_d, ids);
    const RoundTrip by_slot = round_trip(old_d, new_d, {}, {"--identity-slots"});

    for (const RoundTrip* r : {&by_sidecar, &by_slot}) {
        CHECK(r->mismatches.empty());
        CHECK(logged(*r, "street_ways identity: match=2 replace=1 insert=3 delete=2 moved=0"));
        CHECK(logged(*r, "addr_points identity: match=2 replace=1 insert=2 delete=2 moved=0"));
        CHECK(logged(*r, "interp_ways identity: match=1 replace=1 insert=2 delete=2 moved=0"));
    }
    CHECK(!by_slot.patch.empty());
    CHECK(by_slot.patch == by_sidecar.patch);
}

// A family a day has no record of has no keys that day: under
// --identity-slots it merges by content, as it does without its sidecar, and
// the other families by slot.
TEST(patch_round_trip_by_slot_identity_merges_a_family_a_day_lacks_by_content) {
    BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
    Identity ids, interp_ids;
    put_slot_streets(old_d, new_d, ids);
    BuildDir no_interps;
    put_slot_interps(no_interps, new_d, interp_ids);
    ids.new_keys["interp"] = interp_ids.new_keys["interp"];
    put_geo_cells(old_d, {{CELL, {0, 1, 2, 4}, {}, {}}});
    put_geo_cells(new_d, {{CELL, {0, 2, 3, 4, 5}, {}, {0, 1, 3}}});

    const RoundTrip by_sidecar = round_trip(old_d, new_d, ids);
    const RoundTrip by_slot = round_trip(old_d, new_d, {}, {"--identity-slots"});

    for (const RoundTrip* r : {&by_sidecar, &by_slot}) {
        CHECK(r->mismatches.empty());
        CHECK(logged(*r, "street_ways identity: match=2 replace=1 insert=3 delete=2 moved=0"));
        CHECK(!logged(*r, "interp_ways identity:"));
        CHECK(!logged(*r, "addr_points identity:"));
    }
    CHECK(!by_slot.patch.empty());
    CHECK(by_slot.patch == by_sidecar.patch);
}

// The keys come from the sidecars or from the records, never both.
TEST(patch_diff_refuses_identity_slots_beside_identity_sidecars) {
    ScratchDir dir("identity-options");
    const std::string old_dir = dir.path() + "/old", new_dir = dir.path() + "/new", patch = dir.path() + "/p.gcpatch";
    const std::string log = dir.path() + "/tools.log";
    REQUIRE(write_dir(old_dir, with_pool(OLD_POOL)) && write_dir(new_dir, with_pool(NEW_POOL)));

    for (const std::vector<std::string>& options : {std::vector<std::string>{"--identity", old_dir, new_dir, "--identity-slots"},
                                                    std::vector<std::string>{"--identity-slots", "--identity", old_dir, new_dir}}) {
        std::filesystem::remove(log);
        std::vector<std::string> diff = {GEOCODER_DIFF_BIN, old_dir, new_dir, "-o", patch};
        diff.insert(diff.end(), options.begin(), options.end());
        CHECK(!run(diff, log));
        const std::vector<char> text = read_file(log);
        CHECK(std::string(text.begin(), text.end()).find("--identity and --identity-slots both give the keys") != std::string::npos);
        CHECK(!std::filesystem::exists(patch));
    }
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

// The patcher decompresses the patch with libzstd in process, so it applies
// with no zstd on PATH; one built on the zstd CLI (-DGEOCODER_PATCH_LIBZSTD=OFF)
// runs the zstd on PATH, and without one fails to decompress.
TEST(patch_needs_no_zstd_on_path_unless_built_on_the_zstd_cli) {
    ScratchDir dir("patch-no-zstd");
    const std::string& root = dir.path();
    REQUIRE(!diff_way_parents(root).raw.empty());
    std::filesystem::create_directories(root + "/no-zstd");
    const std::string log = root + "/out.log";

    PatchRun r;
    r.ok = run({"env", "PATH=" + root + "/no-zstd", GEOCODER_PATCH_BIN, "old", "p.gcpatch", "-o", "out"}, log, root);

    auto text = read_file(log);
    r.log.assign(text.begin(), text.end());
#ifdef GEOCODER_PATCH_ZSTD_CLI
    CHECK(!r.ok);
    CHECK(logged(r, "Failed to decompress"));
#else
    CHECK(r.ok);
    CHECK(read_file(root + "/out/way_parents.bin") == written_way_parents(new_way_parents()));
#endif
}

// The patch's zstd window is the diff's, 8 MiB (PATCH_ZSTD_WINDOW_LOG 23):
// its frame declares no more, and a build on the zstd command, which holds
// the window, refuses a frame declaring more (the command's own limit is
// 128 MiB). A build on libzstd decompresses into the mapped output and holds
// no window: it applies both.
TEST(patch_holds_a_zstd_window_no_larger_than_the_diffs) {
    ScratchDir dir("patch-window");
    const std::string& root = dir.path();
    SparsePatch p = diff_way_parents(root);
    REQUIRE(!p.raw.empty());
    const std::vector<char> diffs = read_file(root + "/p.gcpatch");
    REQUIRE(diffs.size() > 5);
    // Not a single segment (whose window is its content), and a window
    // descriptor of at most 2^23 bytes.
    CHECK_EQ(diffs[4] & 0x20, 0);
    CHECK(static_cast<uint8_t>(diffs[5]) <= (23 - 10) << 3);
    for (const int window_log : {23, 24}) {
        const std::string name = "window-" + std::to_string(window_log);
        REQUIRE(write_file(root + "/" + name + ".gcpatch", zstd_frame(p.raw, window_log)));

        auto r = run_patch(root, name + ".gcpatch", name);

#ifdef GEOCODER_PATCH_ZSTD_CLI
        const bool refused = window_log > 23;
#else
        const bool refused = false;
#endif
        CHECK_EQ(r.ok, !refused);
        if (refused) CHECK(logged(r, "Failed to decompress"));
        else CHECK(read_file(root + "/" + name + "/way_parents.bin") == written_way_parents(new_way_parents()));
    }
}

#ifndef GEOCODER_PATCH_ZSTD_CLI
// A filesystem that flushes late writes (NFS, FUSE) may report a failed one
// only when the file closes: the decompressed patch is refused then, rather
// than mapped and read back. The frame decompresses with a close that works.
TEST(patch_decompression_refuses_a_patch_file_that_fails_to_close) {
    ScratchDir dir("patch-close");
    const std::string& root = dir.path();
    std::vector<char> raw(300 << 10);
    for (size_t i = 0; i < raw.size(); i++) raw[i] = static_cast<char>(i * 7);
    REQUIRE(write_file(root + "/p.gcpatch", zstd_frame(raw, 23)));
    decompressed_patch_close() = [](int fd) {
        ::close(fd);
        errno = EIO;
        return -1;
    };

    const bool closed = decompress_to_file(root + "/p.gcpatch", root + "/failing.raw");
    decompressed_patch_close() = ::close;

    CHECK(!closed);
    CHECK(decompress_to_file(root + "/p.gcpatch", root + "/p.raw"));
    CHECK(read_file(root + "/p.raw") == raw);
}
#endif

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
// refuses records past the end, not the last one. The value moves from one
// block of the file to another and with it the bits a value of each takes, so
// the patch lists the size of the file the edit gives.
TEST(patch_applies_a_sparse_delta_change_at_the_last_record) {
    ScratchDir dir("patch-sparse");
    SparsePatch p = diff_way_parents(dir.path());
    REQUIRE(!p.raw.empty());
    auto want = new_way_parents();
    const auto [last_pos, last_id] = SPARSE_CHANGES[std::size(SPARSE_CHANGES) - 1];
    want[last_pos] = old_way_parents()[last_pos];
    want[SPARSE_RECORDS - 1] = last_id;
    REQUIRE(written_way_parents(want).size() != written_way_parents(new_way_parents()).size());
    p.set_last_pos(SPARSE_RECORDS - 1);
    p.set_listed_size(written_way_parents(want).size());

    auto r = apply_raw(dir.path(), p.raw, "out");

    CHECK(r.ok);
    CHECK(read_file(dir.path() + "/out/way_parents.bin") == written_way_parents(want));
}

// The diff reads a side file as side blocks and as nothing else: one of bare
// u32s is refused by name, and no patch is written.
TEST(patch_diff_refuses_a_side_file_that_is_not_side_blocks) {
    ScratchDir dir("patch-diff-side");
    const std::string& root = dir.path();
    const std::string patch = root + "/p.gcpatch", log = root + "/tools.log";
    const BuildDir day = {{"way_parents.bin", bytes_of(std::vector<uint32_t>{5, 6, 7, 8})}};
    REQUIRE(write_dir(root + "/old", day) && write_dir(root + "/new", day));
    REQUIRE(write_file(root + "/new/way_parents.bin", day.at("way_parents.bin")));

    CHECK(!run({GEOCODER_DIFF_BIN, root + "/old", root + "/new", "-o", patch}, log));

    const std::vector<char> text = read_file(log);
    CHECK(std::string(text.begin(), text.end()).find("Malformed way_parents.bin: no SDB1 in its footer") != std::string::npos);
    CHECK(!std::filesystem::exists(patch));
}

// A side file's size on disk follows its values' widths, its section their
// count: the client file list gives the file's size on disk, which the
// patcher holds its output to, and the section the old and the new values'
// logical bytes, 4 a value, which it holds the old file's values to.
TEST(patch_lists_a_side_file_at_its_encoded_size_and_sends_its_section_at_its_logical_size) {
    ScratchDir dir("patch-side-sizes");
    const std::string& root = dir.path();
    const uint64_t logical = uint64_t{SPARSE_RECORDS} * 4, written = written_way_parents(new_way_parents()).size();
    REQUIRE(written != logical);
    // The section and the list's entry, found by those sizes.
    SparsePatch p = diff_way_parents(root);
    REQUIRE(!p.raw.empty());
    const size_t old_size_at = p.new_size_at - 8;

    auto r = apply_raw(root, p.raw, "out");
    CHECK(r.ok);
    CHECK(read_file(root + "/out/way_parents.bin") == written_way_parents(new_way_parents()));

    SparsePatch listed_logical = p;
    listed_logical.set_listed_size(logical);
    r = apply_raw(root, listed_logical.raw, "listed-logical");
    CHECK(!r.ok);
    CHECK(logged(r, "Output size mismatch: way_parents.bin is " + std::to_string(written) + " bytes, the patch lists " +
                        std::to_string(logical)));

    SparsePatch old_written = p;
    put_at(old_written.raw, old_size_at, static_cast<uint64_t>(written_way_parents(old_way_parents()).size()));
    r = apply_raw(root, old_written.raw, "old-written");
    CHECK(!r.ok);
    CHECK(logged(r, "old way_parents.bin decodes to " + std::to_string(logical) + " bytes, the patch expects " +
                        std::to_string(written_way_parents(old_way_parents()).size())));
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

// A MATCH run longer than what is left of the old file is refused before any
// of it is written: of the POI records (replayed record by record with their
// child), 5 longer, and of their vertices (the child stream, which the
// records size), as bytes past the last record. The replay stopped at the old
// file's end and went on as if the run were whole.
TEST(patch_refuses_a_match_run_past_the_old_file) {
    ScratchDir dir("patch-match");
    const std::string& root = dir.path();
    const std::string raw = root + "/p.raw", log = root + "/tools.log";
    REQUIRE(write_dir(root + "/old", poi_polygons(false)));
    REQUIRE(write_dir(root + "/new", poi_polygons(true)));
    REQUIRE(run({GEOCODER_DIFF_BIN, root + "/old", root + "/new", "-o", root + "/p.gcpatch"}, log));
    REQUIRE(run({"zstd", "-d", "-q", "-f", root + "/p.gcpatch", "-o", raw}, log));
    const std::vector<char> patch = read_file(raw);
    const uint32_t rest = MATCH_POIS - MATCH_DROPPED - 1;
    struct Edit {
        std::string name;
        std::vector<char> runs;
        std::string refusal;
    };
    const Edit files[] = {
        {"poi_records.bin", match_delete_match(MATCH_DROPPED, 1, rest), "Malformed merge sequence: poi_records.bin (a MATCH run past the old file)"},
        {"poi_vertices.bin", match_delete_match(MATCH_DROPPED * MATCH_BLOCK, MATCH_BLOCK, rest * MATCH_BLOCK),
         "poi_vertices.bin has bytes past its last record"}};
    for (const auto& [name, runs, refusal] : files) {
        const size_t at = find_once(patch, runs);
        REQUIRE(at != std::string::npos);
        std::vector<char> longer = patch;
        uint32_t count;
        memcpy(&count, runs.data() + runs.size() - 4, 4);
        put_at(longer, at + runs.size() - 4, count + 5);

        auto r = apply_raw(root, longer, "out-" + name);

        CHECK(!r.ok);
        CHECK(logged(r, refusal));
    }
    auto r = apply_raw(root, patch, "out");
    CHECK(r.ok);
    const BuildDir want = gctest::as_written(poi_polygons(true));
    CHECK(read_file(root + "/out/poi_records.bin") == want.at("poi_records.bin"));
    CHECK(read_file(root + "/out/poi_vertices.bin") == want.at("poi_vertices.bin"));
}

// A new dir of more client files than a patch may list (1025, past
// MAX_CLIENT_FILES) fails the diff with the patcher's refusal, and no patch
// is written: the patcher would refuse the list. A dir of 1024 makes a patch
// that applies.
TEST(patch_diff_refuses_a_dir_of_more_client_files_than_a_patch_may_list) {
    ScratchDir dir("patch-diff-client-files");
    const std::string& root = dir.path();
    const std::string patch = root + "/p.gcpatch", log = root + "/tools.log";
    REQUIRE(write_dir(root + "/old", json_files(1)));
    REQUIRE(write_dir(root + "/new", json_files(1025)));

    CHECK(!run({GEOCODER_DIFF_BIN, root + "/old", root + "/new", "-o", patch}, log));

    const std::vector<char> text = read_file(log);
    CHECK(std::string(text.begin(), text.end()).find("Too many client files: 1025") != std::string::npos);
    CHECK(!std::filesystem::exists(patch));
    CHECK(round_trip(json_files(1), json_files(1024)).mismatches.empty());
}

// A new admin or POI cell index in a layout the patcher's rebuild doesn't
// write (cell_pairs::Writer: its first list, {0, 1}, flags repeats it doesn't
// have, so its gap is 1, not 0) fails the diff, and no patch is written: the
// patcher could not rebuild it. The same lists as the builder writes them
// round-trip.
TEST(patch_diff_refuses_a_cell_index_the_writer_wouldnt_write) {
    const std::pair<std::string, BuildDir> indexes[] = {
        {"admin", {}},
        {"poi", {{"poi_records.bin", bytes_of(std::vector<PoiRecord>{poi(NAME_2), poi(NAME_4)})}}},
    };
    for (const auto& [prefix, records] : indexes) {
        ScratchDir dir("patch-diff-cell-layout");
        const std::string& root = dir.path();
        const std::string patch = root + "/p.gcpatch", log = root + "/tools.log";
        BuildDir old_d = with_pool(OLD_POOL), new_d = with_pool(NEW_POOL);
        for (const auto& [name, bytes] : records) old_d[name] = new_d[name] = bytes;
        put_cell_index(old_d, prefix, {0, 1});
        // CELL lists 0 and 1, CELL_2 none.
        std::tie(new_d[prefix + "_cells.bin"], new_d[prefix + "_entries.bin"]) =
            write_cell_lists({{CELL, {0, 1}}, {CELL_2, {}}}, cell_pairs::format_of(prefix));
        BuildDir unwritten = new_d;
        // Admin: a block (2 bytes of codes), then {0, 1} as PLAIN, its header, no flagged ids, 0, the gap. POI: the
        // list alone, its gap after a byte of gap lengths.
        std::vector<char>& entries = unwritten[prefix + "_entries.bin"];
        const size_t list = prefix == "admin" ? 2 : 0, gap = prefix == "admin" ? 5 : 4;
        REQUIRE(entries[list] == 4 && entries[gap] == 0);
        entries[list] = 5;
        entries[gap] = 1;
        REQUIRE(parse_cell_lists(unwritten[prefix + "_cells.bin"], entries, cell_pairs::format_of(prefix)) ==
                CellLists({{CELL, {0, 1}}, {CELL_2, {}}}));
        REQUIRE(write_dir(root + "/old", old_d));
        REQUIRE(write_dir(root + "/new", unwritten));

        CHECK(!run({GEOCODER_DIFF_BIN, root + "/old", root + "/new", "-o", patch}, log));

        const std::vector<char> text = read_file(log);
        CHECK(std::string(text.begin(), text.end()).find("Cell index " + prefix + " has a layout write_cell_lists can't reproduce") !=
              std::string::npos);
        CHECK(!std::filesystem::exists(patch));
        CHECK(round_trip(old_d, new_d).mismatches.empty());
    }
}

namespace {

// A dir's admin geometry files as the builder lays them out: a 16-byte record
// per polygon of keys (name and area the key) whose ring_offset starts its
// ring block in admin_rings.bin (the next one's ends it), of 8 + key % 5 bytes,
// and n_arcs arc blocks of 16 + i % 7 bytes in arc_vertices.bin at the u32
// offsets of arc_index.bin. The blocks' bytes come from seed: the patch tools
// read none of them.
BuildDir admin_geometry(const std::vector<uint32_t>& keys, uint32_t n_arcs, uint32_t seed) {
    std::mt19937 rng(seed);
    std::vector<AdminPolygonRecord> records;
    std::vector<char> rings, index, arcs;
    for (const uint32_t key : keys) {
        AdminPolygon p{};
        p.name_id = key;
        p.admin_level = 8;
        p.area = static_cast<float>(key);
        records.push_back(admin_polygon_record(p, static_cast<uint32_t>(rings.size())));
        for (uint32_t b = 0; b < 8 + key % 5; b++) rings.push_back(static_cast<char>(rng()));
    }
    for (uint32_t i = 0; i < n_arcs; i++) {
        append(index, static_cast<uint32_t>(arcs.size()));
        for (uint32_t b = 0; b < 16 + i % 7; b++) arcs.push_back(static_cast<char>(rng()));
    }
    return {{"admin_polygons.bin", bytes_of(records)}, {"admin_rings.bin", rings}, {"arc_index.bin", index}, {"arc_vertices.bin", arcs}};
}

// Ten polygons by key.
const std::vector<uint32_t> POLYGON_KEYS = {10, 11, 12, 13, 14, 15, 16, 17, 18, 19};

}  // namespace

// The admin geometry files merge by their records at their own strides
// (ADMIN_SLOT_FAMILIES), so each counts against the diff's record limit
// (--file-records-max): the arc index at 4 bytes a record, never the admin
// stride at which the postal polygons' id 26 merged, the byte streams at 1.
// Ten polygons and 40 arcs, then three and 41: under a limit of 40 the new
// arc index's 41 records are refused, under one less than the largest
// stream's bytes that stream is, and under that many the dirs round-trip.
// The parent refused none of them: it sent the files whole.
TEST(patch_diff_counts_the_admin_geometry_records_against_the_limit) {
    const BuildDir old_d = admin_geometry(POLYGON_KEYS, 40, 1), new_d = admin_geometry({10, 11, 12}, 41, 2);
    size_t most = 0;
    std::string largest;
    for (const BuildDir* d : {&old_d, &new_d})
        for (const char* name : {"arc_vertices.bin", "admin_rings.bin"})
            if (d->at(name).size() > most) {
                most = d->at(name).size();
                largest = std::string(d == &old_d ? "Old " : "New ") + name + " holds " + std::to_string(most) + " records";
            }

    const RoundTrip index = round_trip(old_d, new_d, {}, {"--file-records-max", "40"});
    const RoundTrip stream = round_trip(old_d, new_d, {}, {"--file-records-max", std::to_string(most - 1)});
    const RoundTrip fits = round_trip(old_d, new_d, {}, {"--file-records-max", std::to_string(most)});

    CHECK(index.mismatches == std::vector<std::string>{"diff failed"});
    CHECK(logged(index, "New arc_index.bin holds 41 records, more than the 40 a patch can index"));
    CHECK(stream.mismatches == std::vector<std::string>{"diff failed"});
    CHECK(logged(stream, largest + ", more than the " + std::to_string(most - 1) + " a patch can index"));
    CHECK(fits.mismatches.empty());
}

// Admin polygons keep their slots, so no file is remapped by admin polygon
// content: a polygon new at slot 0 that moves the old ones' content up a
// slot (the slots of a cold start) leaves the POIs' and places' parent slots
// and the admin cell index's ids as they are. The POIs and places the new day
// keeps MATCH, the admin index's delta changes no cell, and the admin
// polygons merge by slot: the two old slots REPLACEd, the third INSERTed.
// The parent sent the admin polygons whole.
TEST(patch_round_trip_moves_no_admin_slot_by_admin_polygon_content) {
    auto day = [](const std::vector<uint32_t>& keys, const std::vector<uint32_t>& poi_parents, const std::vector<uint32_t>& place_parents) {
        BuildDir d = admin_geometry(keys, 0, 7);
        d.erase("admin_rings.bin");
        d.erase("arc_index.bin");
        d.erase("arc_vertices.bin");
        std::vector<PoiRecord> pois;
        for (size_t i = 0; i < poi_parents.size(); i++) {
            PoiRecord p = poi(NO_DATA);
            p.lat = 48.0f + static_cast<float>(i) * 1e-3f;
            p.parent_poly_id = poi_parents[i];
            pois.push_back(p);
        }
        // Labels of the polygons, hidden.
        std::vector<PlaceNode> places;
        for (size_t i = 0; i < place_parents.size(); i++) {
            PlaceNode p = place(0);
            p.lat = 48.0f + static_cast<float>(i) * 1e-3f;
            p._pad1 = PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF;
            p.parent_poly_id = place_parents[i];
            places.push_back(p);
        }
        d["poi_records.bin"] = bytes_of(pois);
        put_places(d, places);
        put_cell_index(d, "admin", {0, 1});
        return d;
    };
    const BuildDir old_d = day({10, 11}, {0, 1}, {1}), new_d = day({9, 10, 11}, {0, 1, 1}, {1, 0});
    const CellLists lists = {{CELL, {0, 1}}};
    std::vector<char> unchanged_index;
    append_cell_list_delta(unchanged_index, lists, lists);

    const RoundTrip r = round_trip(old_d, new_d);

    CHECK(r.mismatches.empty());
    CHECK_EQ(merge_records(r, "poi_records.bin"), std::string("m/i/d/r=2/1/0/0"));
    CHECK_EQ(merge_records(r, "place_nodes.bin"), std::string("m/i/d/r=1/1/0/0"));
    CHECK(logged(r, "admin_entries.bin: cell index delta (" + std::to_string(unchanged_index.size()) +
                        " bytes: 0 gone and 0 new cells, 0 lost and 0 gained runs)"));
    CHECK_EQ(merge_records(r, "admin_polygons.bin"), std::string("m/i/d/r=0/1/0/2"));
}
