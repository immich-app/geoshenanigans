// geocoder-patch's low-memory mode end to end: small synthetic today's-format
// dirs, the geocoder-diff patch between them, geocoder-gc's encoding of the
// old dir patched (in sync and --threads) and the new encoded set compared
// byte for byte with geocoder-gc's encoding of the new dir. Then the
// patcher's heap peak, in every mode, as a case grows tenfold; and patches
// and command lines the patcher must refuse.
#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <map>
#include <optional>
#include <random>
#include <regex>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "arc_blocks.h"
#include "in_place_edits.h"
#include "merge_sequence.h"
#include "patch_format.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"
#include "types.h"

namespace {

using gctest::BuildDir;
using gctest::read_dir;
using gctest::run;
using gctest::write_dir;

constexpr uint32_t NO = 0xFFFFFFFFu;
// The most bytes a family's decoder may hold queued while the replay skips
// old bytes (a deleted run, replaced children or parents).
constexpr uint64_t DECODER_HOLD = 1 << 20;
// The most vertices an abs2 address footprint holds (closing vertex
// included); one of more goes through the raw escape.
constexpr uint32_t ABS2_MAX_VERTICES = 69999;
// The most bytes a family's encoder may hold queued, however large a write:
// a piece of it (64 KiB) and the largest unit the encoder takes whole. Each
// family's own, so each check is as tight as its encoder allows (one bound
// for all would be set by the largest unit of any encoder, a street way's
// 65535 nodes (512 KiB), and let a POI encoder hold 256 KiB more than it
// may):
//   POI: a block up to big_block(), 256 KiB; a larger one goes plane by
//   plane;
//   addr: an abs2 footprint, a 10-byte header and 4-byte vertices, 280,006 B
//   at ABS2_MAX_VERTICES; an escaped one goes through in pieces.
constexpr uint64_t ENCODER_PIECE = 64 << 10;
constexpr uint64_t POI_ENCODER_HOLD = ENCODER_PIECE + (256 << 10);
constexpr uint64_t ADDR_ENCODER_HOLD = ENCODER_PIECE + 10 + 4 * uint64_t(ABS2_MAX_VERTICES);
// The most the patcher's heap peak may grow when a heap test's case grows
// tenfold in the one structure it varies, which the patcher reads in place.
constexpr uint64_t HEAP_GROWTH_SLACK = 256 << 10;
// How many times a heap test applies its case with --threads at each size:
// THREADS_HEAP_RUNS, and up to THREADS_HEAP_MAX_RUNS while the check fails
// and no codec state (codec_state) came up at both sizes, or none that did
// has THREADS_STATE_RUNS runs at each. A family's encoder thread starts on
// the patcher's first write and drains its rings as the scheduler runs it;
// when it runs later than the patcher writes a few pieces, each ring and the
// buffer the thread swaps with it grow to 128 KiB, and its own queue after
// them, so one run of a case can peak up to 1 MB above another, quiet or
// loaded, at any size (the streets family: 1.08 to 2.10 MB from 10,000 to
// 2,000,000 moved ways). The codec report lines show which state a run ended
// in; runs in one state still differ, by up to 470 KB in the streets family,
// past the slack, so one run at each size in a state can fail it alone.
constexpr int THREADS_HEAP_RUNS = 7;
constexpr int THREADS_HEAP_MAX_RUNS = 35;
constexpr size_t THREADS_STATE_RUNS = 2;

template <typename T>
void append(std::vector<char>& out, const T& v) {
    const char* p = reinterpret_cast<const char*>(&v);
    out.insert(out.end(), p, p + sizeof v);
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

// A POI by record: its id fills the record's first 8 bytes, so a record kept
// in the new set matches; no block is a point, else the bytes of its vertex
// block (a 10-byte header, then 4-byte vertices).
struct Poi {
    uint64_t id;
    std::vector<char> block;
};

std::vector<char> block(std::mt19937& rng, uint32_t vertices) {
    std::vector<char> b(10, 0);
    for (uint32_t k = 0; k < 4 * vertices; k++) b.push_back(static_cast<char>(rng()));
    return b;
}

// poi_records.bin and poi_vertices.bin of pois, into d; names: each record's
// name_id (none: 0).
void put_pois(BuildDir& d, const std::vector<Poi>& pois, const std::vector<uint32_t>& names = {}) {
    std::vector<char> recs, verts;
    for (size_t i = 0; i < pois.size(); i++) {
        const Poi& p = pois[i];
        append(recs, p.id);
        append(recs, p.block.empty() ? NO : static_cast<uint32_t>(verts.size()));
        append(recs, p.block.empty() ? 0u : static_cast<uint32_t>((p.block.size() - 10) / 4));
        append(recs, names.empty() ? 0u : names[i]);
        for (int k = 0; k < 4; k++) append(recs, 0u);
        verts.insert(verts.end(), p.block.begin(), p.block.end());
    }
    d["poi_records.bin"] = recs;
    d["poi_vertices.bin"] = verts;
}

// An address point by its latitude (unique, so a point kept in the new set
// matches) and its footprint: none, or a vertex block as a POI's (8-byte
// vertices at encoding 3, the block's first byte).
struct Addr {
    float lat;
    std::vector<char> block;
};

// addr_points.bin and addr_vertices.bin of addrs, into d: no strings, on
// street way 0.
void put_addrs(BuildDir& d, const std::vector<Addr>& addrs) {
    std::vector<char> points, verts;
    for (const Addr& a : addrs) {
        append(points, a.lat);
        append(points, 8.0f);
        append(points, 0u);
        append(points, 0u);
        append(points, 0u);
        append(points, a.block.empty() ? NO : static_cast<uint32_t>(verts.size()));
        append(points, a.block.empty() ? 0u : static_cast<uint32_t>((a.block.size() - 10) / (a.block[0] == 3 ? 8 : 4)));
        verts.insert(verts.end(), a.block.begin(), a.block.end());
    }
    d["addr_points.bin"] = points;
    d["addr_vertices.bin"] = verts;
}

// The admin geometry of an admin-minimal-shaped dir by polygon slot and arc
// slot: each polygon's key (its name and area; NO for a tombstone, which has
// no ring block) and ring block, and each arc's block. The blocks' bytes are
// random: the patch tools read none of them.
struct ArcGeometry {
    std::vector<uint32_t> keys;
    std::vector<std::vector<char>> rings, arcs;
};

std::vector<char> random_bytes(std::mt19937& rng, size_t n) {
    std::vector<char> b(n);
    for (char& c : b) c = static_cast<char>(rng());
    return b;
}

// n_polygons slots, every seventh a tombstone, each other's ring block of 8
// to 39 bytes, and n_arcs arc blocks of arc_bytes bytes (0: 8 to 71 each).
ArcGeometry arc_geometry(std::mt19937& rng, uint32_t n_polygons, uint32_t n_arcs, uint32_t arc_bytes = 0) {
    ArcGeometry g;
    for (uint32_t i = 0; i < n_polygons; i++) {
        const bool tombstone = i % 7 == 3;
        g.keys.push_back(tombstone ? NO : 100 + i);
        g.rings.push_back(tombstone ? std::vector<char>{} : random_bytes(rng, 8 + rng() % 32));
    }
    for (uint32_t i = 0; i < n_arcs; i++) g.arcs.push_back(random_bytes(rng, arc_bytes ? arc_bytes : 8 + rng() % 64));
    return g;
}

// g's four admin geometry files, laid out as the builder writes them, into d:
// a 24-byte record per slot (ring_offset, ring_bytes, name_id, level 8,
// area, no country; a tombstone's {NO, 0, NO}), the ring blocks in slot order,
// a u32 offset per arc slot and the arc blocks in slot order.
void put_arc_geometry(BuildDir& d, const ArcGeometry& g) {
    std::vector<char> recs, rings, index, arcs;
    for (size_t i = 0; i < g.keys.size(); i++) {
        const bool tombstone = g.keys[i] == NO;
        append(recs, tombstone ? NO : static_cast<uint32_t>(rings.size()));
        append(recs, static_cast<uint32_t>(g.rings[i].size()));
        append(recs, g.keys[i]);
        append(recs, uint32_t{8});
        append(recs, tombstone ? 0.0f : static_cast<float>(g.keys[i]));
        append(recs, 0u);
        rings.insert(rings.end(), g.rings[i].begin(), g.rings[i].end());
    }
    for (const std::vector<char>& arc : g.arcs) {
        append(index, static_cast<uint32_t>(arcs.size()));
        arcs.insert(arcs.end(), arc.begin(), arc.end());
    }
    d["admin_polygons.bin"] = recs;
    d["admin_rings.bin"] = rings;
    d["arc_index.bin"] = index;
    d["arc_vertices.bin"] = arcs;
}

// The id of a fixture's cell k.
uint64_t cell_id(uint32_t k) { return uint64_t(k + 1) << 27; }

// A geo row: its cell index and the street ids it lists.
using GeoRow = std::pair<uint32_t, std::vector<uint32_t>>;

// The geo index of rows (in cell order, no address or interp lists) over
// `ways` street ways without nodes (a patch drops ids no way replay maps).
void put_geo(BuildDir& d, const std::vector<GeoRow>& rows, uint32_t ways = 1000) {
    std::vector<char> street_ways, geo, entries;
    for (uint32_t w = 0; w < ways; w++) {
        append(street_ways, 0u);
        append(street_ways, uint16_t{0});
        append(street_ways, uint16_t{0});
        append(street_ways, w);
    }
    for (const auto& [k, ids] : rows) {
        append(geo, cell_id(k));
        append(geo, ids.empty() ? NO : static_cast<uint32_t>(entries.size()));
        append(geo, NO);
        append(geo, NO);
        if (ids.empty()) continue;
        append(entries, static_cast<uint16_t>(ids.size()));
        for (uint32_t id : ids) append(entries, id);
    }
    d["street_ways.bin"] = street_ways;
    d["street_nodes.bin"] = {};
    d["geo_cells.bin"] = geo;
    d["street_entries.bin"] = entries;
    d["addr_entries.bin"] = {};
    d["interp_entries.bin"] = {};
}

// The cell index <prefix>_cells.bin + <prefix>_entries.bin of lists (cell
// id → ids), into d, as the builder writes it.
void put_cells(BuildDir& d, const std::string& prefix, const CellLists& lists) {
    auto [cells, entries] = write_cell_lists(lists);
    d[prefix + "_cells.bin"] = cells;
    d[prefix + "_entries.bin"] = entries;
}

// The top bit of an admin cell index entry: the cell lies inside the polygon.
constexpr uint32_t INTERIOR = 0x80000000u;

// An admin-minimal-shaped dir (the files the builder writes there): g's
// admin geometry, n_places place nodes each labelled by the polygon of its
// slot (place i by slot i mod the slots, none by a tombstone), the admin
// cell index (cell k listing slot k and slot k + 1 as interior), the core
// strings and the strings layout.
void put_admin_minimal(BuildDir& d, const ArcGeometry& g, uint32_t n_places) {
    put_arc_geometry(d, g);
    std::vector<char> places;
    for (uint32_t i = 0; i < n_places; i++) {
        const uint32_t slot = i % static_cast<uint32_t>(g.keys.size());
        append(places, 47.0f + static_cast<float>(i) * 1e-4f);
        append(places, 8.0f);
        append(places, 0u);
        append(places, uint32_t{1});
        append(places, g.keys[slot] == NO ? NO : slot);
    }
    d["place_nodes.bin"] = places;
    CellLists cells;
    for (uint32_t k = 0; k + 1 < g.keys.size(); k++) cells[cell_id(k)] = {k, (k + 1) | INTERIOR};
    put_cells(d, "admin", cells);
    const std::string pool("Testland\0Ville\0", 15);
    d["strings_core.bin"].assign(pool.begin(), pool.end());
    const std::string layout = "{\"tiers\": []}\n";
    d["strings_layout.json"].assign(layout.begin(), layout.end());
}

// Street ways of two nodes each, named by names, in one geo cell that lists
// entity 0's way; with the identity sidecar for a diff's --identity: record i
// is entity entities[i] (by default entity i), its nodes placed by its entity.
void put_named_ways(BuildDir& d, BuildDir& meta, const std::vector<uint32_t>& names, std::vector<uint32_t> entities = {}) {
    if (entities.empty())
        for (uint32_t w = 0; w < names.size(); w++) entities.push_back(w);
    const uint32_t first = static_cast<uint32_t>(std::find(entities.begin(), entities.end(), 0u) - entities.begin());
    put_geo(d, {{0, {first}}}, static_cast<uint32_t>(names.size()));
    std::vector<char> ways, nodes, slots;
    for (uint32_t w = 0; w < names.size(); w++) {
        append(ways, 2 * w);
        append(ways, uint16_t{2});
        append(ways, uint16_t{0});
        append(ways, names[w]);
        for (const double v : {47.0, 8.0, 47.001, 8.001}) append(nodes, static_cast<float>(v + entities[w] * 1e-5));
        append(slots, entities[w]);
    }
    d["street_ways.bin"] = ways;
    d["street_nodes.bin"] = nodes;
    meta["street.slot.u32"] = slots;
}

// 2 × moved + kept street ways, record i entity i, and the new day's with
// the first 2 × moved swapped in pairs: by the identity sidecars every even
// one of those and the kept ways stay in order, and each odd one moves on
// its own, so no codec queue grows with the moves (moves in one reversed run
// would delete one long run of old ways, whose nodes --threads decodes
// ahead). 100 address points lie on entity 0's way. Into old and new, and
// their sidecars. An entity's name is its parity, so the street file
// changes.
void put_moved_ways(BuildDir& old_d, BuildDir& new_d, BuildDir& old_meta, BuildDir& new_meta, uint32_t moved, uint32_t kept) {
    std::vector<uint32_t> entities, new_entities;
    for (uint32_t w = 0; w < 2 * moved + kept; w++) {
        entities.push_back(w);
        new_entities.push_back(w < 2 * moved ? w ^ 1 : w);
    }
    auto names_of = [](const std::vector<uint32_t>& e) {
        std::vector<uint32_t> names;
        for (const uint32_t w : e) names.push_back(w % 2);
        return names;
    };
    put_named_ways(old_d, old_meta, names_of(entities), entities);
    put_named_ways(new_d, new_meta, names_of(new_entities), new_entities);
    std::vector<Addr> addrs;
    for (uint32_t i = 0; i < 100; i++) addrs.push_back({47.0f + static_cast<float>(i) * 1e-4f, {}});
    put_addrs(old_d, addrs);
    put_addrs(new_d, addrs);
    std::vector<char>& points = new_d["addr_points.bin"];
    for (size_t at = ADDR_POINT_PARENT_WAY_ID_OFF; at < points.size(); at += ADDR_POINT_STRIDE) put_at(points, at, 1u);
}

// n POIs, points without names, and the new day's with new ids: each record
// goes as deleted and inserted, and the diff's secondary matching (by name
// and category) pairs every old one with its new one; into old and new.
void put_changed_pois(BuildDir& old_d, BuildDir& new_d, uint32_t n) {
    std::vector<Poi> old_pois, new_pois;
    for (uint32_t i = 0; i < n; i++) {
        old_pois.push_back({i, {}});
        new_pois.push_back({uint64_t{n} + i, {}});
    }
    put_pois(old_d, old_pois);
    put_pois(new_d, new_pois);
}

// n POIs, points, each named by a POI name of its own and without street or
// postcode, and the POI string tier of their names, the new day's in reverse
// order, every POI but `dropped` (if one is) kept; into old and new. The
// tier's edit keeps the last name and moves the other n - 1, which leave it
// as moved-string pairs or, past the strings section's cap, as the string
// fixups of the POIs that keep them. n even: no name stays where it was.
void put_reversed_poi_names(BuildDir& old_d, BuildDir& new_d, uint32_t n, std::optional<uint32_t> dropped = std::nullopt) {
    constexpr uint32_t NAME = 12;  // "poi 0000000\0"
    auto name = [](uint32_t i) {
        char b[16];
        std::snprintf(b, sizeof b, "poi %07u", i);
        return std::string(b, NAME);
    };
    std::string old_pool, new_pool;
    std::vector<Poi> old_pois, new_pois;
    std::vector<uint32_t> old_names, new_names;
    for (uint32_t i = 0; i < n; i++) {
        old_pool += name(i);
        new_pool += name(n - 1 - i);
        old_pois.push_back({i, {}});
        old_names.push_back(i * NAME);
        if (dropped == i) continue;
        new_pois.push_back({i, {}});
        new_names.push_back((n - 1 - i) * NAME);
    }
    put_pois(old_d, old_pois, old_names);
    put_pois(new_d, new_pois, new_names);
    for (BuildDir* d : {&old_d, &new_d}) {
        std::vector<char>& records = (*d)["poi_records.bin"];
        for (size_t at = 0; at < records.size(); at += POI_RECORD_STRIDE) {
            put_at(records, at + POI_RECORD_PARENT_STREET_ID_OFF, NO);
            put_at(records, at + POI_RECORD_PARENT_POSTCODE_ID_OFF, NO);
        }
    }
    old_d["strings_poi.bin"].assign(old_pool.begin(), old_pool.end());
    new_d["strings_poi.bin"].assign(new_pool.begin(), new_pool.end());
}

// The diff options that send every moved string as a string fixup.
const std::vector<std::string> NO_STRING_PAIRS = {"--string-pairs-max", "0"};

// The head of a POI records merge section of old_n records to new_n without
// offset fixups, up to its string fixups' count.
std::vector<char> poi_merge_head(uint32_t old_n, uint32_t new_n, uint32_t string_fixups) {
    std::vector<char> head;
    append(head, static_cast<uint32_t>(PatchFileId::POI_RECORDS));
    append(head, static_cast<uint32_t>(POI_RECORD_STRIDE));
    append(head, uint64_t{old_n} * POI_RECORD_STRIDE);
    append(head, uint64_t{new_n} * POI_RECORD_STRIDE);
    for (const uint32_t w : {0u, 0u, 0u, 0u, string_fixups}) append(head, w);
    return head;
}

// The secondary remap's head as the diff writes it, up to the POI records'
// pair count: no street, address or interp pairs, then poi_pairs.
std::vector<char> secondary_remap_head(uint32_t poi_pairs) {
    std::vector<char> head;
    for (const uint32_t w : {SECONDARY_REMAP_MARKER, 4u, static_cast<uint32_t>(PatchFileId::STREET_WAYS), 0u,
                             static_cast<uint32_t>(PatchFileId::ADDR_POINTS), 0u, static_cast<uint32_t>(PatchFileId::INTERP_WAYS), 0u,
                             static_cast<uint32_t>(PatchFileId::POI_RECORDS), poi_pairs})
        append(head, w);
    return head;
}

// A tool's log: exit status 0, and its output.
struct ToolRun {
    bool ok;
    std::string log;
};

ToolRun run_tool(const std::vector<std::string>& words, const std::string& log) {
    std::filesystem::remove(log);
    ToolRun r;
    r.ok = run(words, log);
    auto text = read_file(log);
    r.log.assign(text.begin(), text.end());
    return r;
}

bool logged(const ToolRun& r, const std::string& line) { return r.log.find(line) != std::string::npos; }

// Checks of a patcher run's log beyond its exit status: an error, or "".
using LogCheck = std::function<std::string(const std::string& log)>;

// The queue (and --threads ring) peaks of the family's codec ("decode" or
// "encode"), from the patcher's report line, all at most limit. A line
// without peaks, or with a "peaks" that isn't a list of numbers, fails: a
// changed report format can't pass as a codec that held nothing.
LogCheck peaks_at_most(const std::string& codec, const std::string& family, uint64_t limit) {
    return [codec, family, limit](const std::string& log) -> std::string {
        const std::string head = codec + " " + family + ":";
        const size_t at = log.find(head);
        if (at == std::string::npos) return "no " + head + " line";
        const std::string line = log.substr(at, log.find('\n', at) - at);
        uint64_t most = 0;
        size_t lists = 0, named = 0;
        static const std::regex peaks("peaks ((?:\\d+ )+)B");
        for (std::sregex_iterator it(line.begin(), line.end(), peaks), end; it != end; ++it, lists++) {
            const std::string values = (*it)[1];
            for (size_t p = 0; p < values.size();) {
                size_t q = values.find(' ', p);
                most = std::max<uint64_t>(most, std::stoull(values.substr(p, q - p)));
                p = q + 1;
            }
        }
        for (size_t p = line.find("peaks"); p != std::string::npos; p = line.find("peaks", p + 1)) named++;
        if (!lists || lists != named) return "no peaks to check: " + line;
        return most > limit ? "held " + std::to_string(most) + " B: " + line : "";
    };
}
LogCheck decoder_peaks_at_most(const std::string& family, uint64_t limit) { return peaks_at_most("decode", family, limit); }
LogCheck encoder_peaks_at_most(const std::string& family, uint64_t limit) { return peaks_at_most("encode", family, limit); }

// The old and new dirs of a case under root/<name>.{old,new}, their encoded
// sets, the patch between them, and what applying it gave.
struct LowMemCase {
    std::string root, name;
    std::string dir(const std::string& kind) const { return root + "/" + name + "." + kind; }
    std::string patch() const { return dir("gcpatch"); }
    std::string log() const { return dir("log"); }
};

// Writes old and new (and their sidecars when given), diffs them (with
// diff_options), and encodes both with geocoder-gc. Empty on success, else
// what failed.
std::string prepare(const LowMemCase& c, const BuildDir& old_d, const BuildDir& new_d, const BuildDir& old_meta = {},
                    const BuildDir& new_meta = {}, const std::vector<std::string>& diff_options = {}) {
    if (!write_dir(c.dir("old"), old_d) || !write_dir(c.dir("new"), new_d)) return "fixture not written";
    std::vector<std::string> diff = {GEOCODER_DIFF_BIN, c.dir("old"), c.dir("new"), "-o", c.patch()};
    if (!old_meta.empty()) {
        if (!write_dir(c.dir("old-meta"), old_meta) || !write_dir(c.dir("new-meta"), new_meta)) return "sidecars not written";
        diff.insert(diff.end(), {"--identity", c.dir("old-meta"), c.dir("new-meta")});
    }
    diff.insert(diff.end(), diff_options.begin(), diff_options.end());
    for (const auto& words : {diff, {GEOCODER_GC_BIN, "encode", c.dir("old"), c.dir("old.enc")},
                              {GEOCODER_GC_BIN, "encode", c.dir("new"), c.dir("new.enc")}}) {
        auto r = run_tool(words, c.log());
        if (!r.ok) return words[0] + " failed: " + r.log;
    }
    return "";
}

// c's patch decompressed, as the patcher reads it; nothing when zstd fails.
std::vector<char> raw_patch(const LowMemCase& c) {
    if (!run({"zstd", "-d", "-q", "-f", c.patch(), "-o", c.dir("raw")}, c.log())) return {};
    return read_file(c.dir("raw"));
}

// raw compressed into the patch file `patch`: whether it was written.
bool write_patch(const LowMemCase& c, const std::string& patch, const std::vector<char>& raw) {
    return write_file(patch + ".raw", raw) && run({"zstd", "-q", "-f", patch + ".raw", "-o", patch}, c.log());
}

// A heap test's check that its case holds the structure it varies: "" when
// c's patch holds `head` (a section's first bytes) exactly once, else that
// it has no `what`.
std::string patch_holds(const LowMemCase& c, const std::vector<char>& head, const std::string& what) {
    return find_once(raw_patch(c), head) == std::string::npos ? "no " + what : "";
}

// The modes a patch applies in: to the old dir, and to its encoded set (the
// low-memory mode) in sync and with --threads.
const std::vector<std::string> ENCODED_MODES = {"sync", "threads"};
const std::vector<std::string> ALL_MODES = {"plain", "sync", "threads"};

// The patcher's command line applying patch to c's old dir ("plain") or
// encoded set ("sync", "threads") into out, with options.
std::vector<std::string> patch_words(const LowMemCase& c, const std::string& mode, const std::string& patch, const std::string& out,
                                     const std::vector<std::string>& options = {}) {
    std::vector<std::string> words = {GEOCODER_PATCH_BIN, c.dir(mode == "plain" ? "old" : "old.enc"), patch, "-o", out};
    if (mode == "threads") words.push_back("--threads");
    words.insert(words.end(), options.begin(), options.end());
    return words;
}

// patch (an edit of c's, named name) applied in each of modes, the patcher
// given options: every mode in which the patcher doesn't refuse it with
// `refusal`, and what it did instead, printed.
std::vector<std::string> not_refused(const LowMemCase& c, const std::vector<std::string>& modes, const std::string& patch,
                                     const std::string& name, const std::string& refusal,
                                     const std::vector<std::string>& options = {}) {
    std::vector<std::string> bad;
    for (const std::string& mode : modes) {
        const ToolRun r = run_tool(patch_words(c, mode, patch, c.dir(name + ".out." + mode), options), c.log());
        if (!r.ok && logged(r, refusal)) continue;
        bad.push_back(name + " (" + mode + "): " + (r.ok ? "applied" : r.log));
        std::printf("    %s\n", bad.back().c_str());
    }
    return bad;
}

// The state a --threads run left its codec buffers in, from the patcher's
// report lines ("encode streets: thread cpu 0.02 s, ring peaks 122880 131072
// B, queue peaks 122892 65536 B"): each line's codec and family, and each of
// its peaks rounded up to a power of two of at least 4 KiB, as ByteQueue
// doubles its buffer to hold them. "" without such lines.
std::string codec_state(const std::string& log) {
    static const std::regex line(R"(((?:en|de)code \S+): thread cpu [^\n]*)"), peaks(R"(peaks ((?:\d+ )+)B)");
    std::string state;
    for (std::sregex_iterator l(log.begin(), log.end(), line), end; l != end; ++l) {
        const std::string text = (*l)[0];
        state += (state.empty() ? "" : "; ") + std::string((*l)[1]);
        for (std::sregex_iterator p(text.begin(), text.end(), peaks); p != end; ++p) {
            const std::string values = (*p)[1];
            for (size_t at = 0; at < values.size();) {
                const size_t next = values.find(' ', at);
                const uint64_t peak = std::stoull(values.substr(at, next - at));
                uint64_t held = peak ? 4096 : 0;
                while (held < peak) held *= 2;
                state += " " + std::to_string(held >> 10) + "K";
                at = next + 1;
            }
        }
    }
    return state;
}

// A case's patch applied in some modes: every mismatch, and the patcher's
// heap peak by mode where the heap shim measured one, and its codec state by
// mode.
struct Applied {
    std::vector<std::string> bad;
    std::map<std::string, uint64_t> heap_peaks;
    std::map<std::string, std::string> codec_states;
};

// The patch applied in each of modes ("plain": to the old dir, compared with
// the new dir; "sync" and "threads": to the old encoded set, compared with
// the new encoded set), the patcher under the heap shim and given options;
// check runs on each log. Every mismatch, printed.
Applied apply_modes(const LowMemCase& c, const std::vector<std::string>& modes, const LogCheck& check = {},
                    const std::vector<std::string>& options = {}) {
    Applied a;
    // The patcher writes only into an empty dir: a case applied again starts
    // from none.
    for (const std::string& mode : modes) std::filesystem::remove_all(c.dir("out." + mode));
    for (const std::string& mode : modes) {
        const bool plain = mode == "plain";
        const std::string out = c.dir("out." + mode);
        std::filesystem::remove(c.log());
        const auto [ok, heap_peak] = gctest::run_with_heap_peak(patch_words(c, mode, c.patch(), out, options), c.log());
        const std::vector<char> text = read_file(c.log());
        const std::string log(text.begin(), text.end());
        if (heap_peak) a.heap_peaks[mode] = *heap_peak;
        a.codec_states[mode] = codec_state(log);
        if (!ok) {
            a.bad.push_back(mode + ": patch failed: " + log);
            continue;
        }
        if (read_dir(out) != read_dir(c.dir(plain ? "new" : "new.enc")))
            a.bad.push_back(mode + (plain ? ": the patched dir is not the new dir" : ": the new set is not gc's encoding of the new dir"));
        if (check)
            if (std::string e = check(log); !e.empty()) a.bad.push_back(mode + ": " + e);
    }
    for (const auto& b : a.bad) std::printf("    %s: %s\n", c.name.c_str(), b.c_str());
    return a;
}

// The low-memory mode's mismatches.
std::vector<std::string> apply_both_modes(const LowMemCase& c, const LogCheck& check = {}) {
    return apply_modes(c, ENCODED_MODES, check).bad;
}

// A heap test's case of n elements of the one structure it varies, written
// into c as prepare() writes one: "" or what failed.
using CaseOfSize = std::function<std::string(const LowMemCase& c, uint32_t n)>;

// "" when the least of a mode's heap peaks at 10 n (in what: the mode, and
// the codec state the runs ended in) is at most HEAP_GROWTH_SLACK above the
// greatest at n, else how far it grew. Each size has a peak.
std::string peak_grew(const std::string& what, const std::vector<uint64_t>& at_n, const std::vector<uint64_t>& at_10n, uint32_t n) {
    const uint64_t from = *std::max_element(at_n.begin(), at_n.end()), to = *std::min_element(at_10n.begin(), at_10n.end());
    if (to <= from + HEAP_GROWTH_SLACK) return "";
    auto of_runs = [](const char* which, const std::vector<uint64_t>& v) {
        return v.size() > 1 ? " (the " + std::string(which) + " of " + std::to_string(v.size()) + " runs)" : std::string();
    };
    return what + ": the heap peak grew from " + std::to_string(from) + " B at n = " + std::to_string(n) + of_runs("greatest", at_n) + " to " +
           std::to_string(to) + " B at n = " + std::to_string(10 * n) + of_runs("least", at_10n) + ", past the " +
           std::to_string(HEAP_GROWTH_SLACK) + " B of slack";
}

// A --threads run of a heap test's case: its heap peak and codec state.
struct ThreadsRun {
    uint64_t peak;
    std::string state;
};

// heap_flat's check of the --threads runs at n and at 10 n so far: its
// failures (none: the heap peak stayed flat), and whether they stand
// (settled) or both sizes run again. The runs are compared within each codec
// state that came up at both sizes, so that the state the scheduler left a
// run in stays out of the comparison: the peak grew if in every such state
// the least at 10 n is past the slack above the greatest at n, as growth
// lifts every run at 10 n, whatever its state. That is settled once one of
// those states has THREADS_STATE_RUNS runs at each size, as one run at each
// size can fail a state on its spread alone. Only when no state came up at
// both sizes are the runs at 10 n held to the slack above those at n as a
// whole, the first THREADS_HEAP_RUNS of each size; a failure there can be
// the scheduler's alone (no run at n filled its rings, every run at 10 n
// did), so it is not settled: both sizes run again until a state comes up at
// both. Later runs stay out of the whole, as they would only widen it until
// it passed, clearing growth that moves every run at 10 n into a new state.
struct ThreadsCheck {
    std::vector<std::string> failures;
    bool settled;
};

ThreadsCheck threads_check(const std::vector<ThreadsRun>& runs_n, const std::vector<ThreadsRun>& runs_10n, uint32_t n) {
    std::map<std::string, std::vector<uint64_t>> by_state[2];
    std::vector<uint64_t> all[2];
    for (size_t s = 0; s < 2; s++)
        for (const ThreadsRun& r : s == 0 ? runs_n : runs_10n) {
            by_state[s][r.state].push_back(r.peak);
            all[s].push_back(r.peak);
        }
    ThreadsCheck c{{}, false};
    bool shared = false;
    for (const auto& [state, at_n] : by_state[0]) {
        const auto at_10n = by_state[1].find(state);
        if (at_10n == by_state[1].end()) continue;
        shared = true;
        const std::string grew = peak_grew("threads, codec state [" + state + "]", at_n, at_10n->second, n);
        if (grew.empty()) return {{}, true};
        c.failures.push_back(grew);
        c.settled |= std::min(at_n.size(), at_10n->second.size()) >= THREADS_STATE_RUNS;
    }
    if (shared) return c;
    if (all[0].empty() || all[1].empty()) return {{}, true};
    for (std::vector<uint64_t>& peaks : all) peaks.resize(std::min(peaks.size(), size_t(THREADS_HEAP_RUNS)));
    if (const std::string grew = peak_grew("threads", all[0], all[1], n); !grew.empty())
        return {{grew + ", and no codec state came up at both sizes"}, false};
    return {{}, true};
}

// The case at n and at 10 n, each applied in every mode: each output as
// apply_modes checks it, and in each mode the heap peak at 10 n at most
// HEAP_GROWTH_SLACK above the one at n. Plain and sync runs peak the same
// every time, so one run at each size decides; --threads runs
// THREADS_HEAP_RUNS times at each size, the sizes taking turns so that both
// see the same load, and threads_check decides, both sizes running again
// while its failures aren't settled, up to THREADS_HEAP_MAX_RUNS times.
// Every failure, printed; skipped without a heap shim (the sanitizer build).
std::vector<std::string> heap_flat(const CaseOfSize& make_case, uint32_t n) {
    if (!gctest::heap_peak_shim()) {
        std::printf("    SKIP heap peaks at n = %u: no heap shim under the sanitizers\n", n);
        return {};
    }
    ScratchDir dir("lowmem-heap");
    std::vector<std::string> bad;
    auto fail = [&](const std::string& why) {
        std::printf("    %s\n", why.c_str());
        bad.push_back(why);
    };
    const uint32_t sizes[] = {n, 10 * n};
    std::vector<LowMemCase> cases;
    for (const uint32_t size : sizes) {
        cases.push_back({dir.path(), "n" + std::to_string(size)});
        if (std::string e = make_case(cases.back(), size); !e.empty()) {
            fail(cases.back().name + ": " + e);
            return bad;
        }
    }
    // Each size's plain and sync heap peaks, and its --threads runs.
    std::map<std::string, uint64_t> peaks[2];
    std::vector<ThreadsRun> threads[2];
    for (int run = 0; run < THREADS_HEAP_MAX_RUNS && (run == 0 || bad.empty()) &&
                      (run < THREADS_HEAP_RUNS || !threads_check(threads[0], threads[1], n).settled);
         run++)
        for (size_t s = 0; s < 2; s++) {
            const std::vector<std::string> modes = run == 0 ? ALL_MODES : std::vector<std::string>{"threads"};
            const Applied a = apply_modes(cases[s], modes);
            bad.insert(bad.end(), a.bad.begin(), a.bad.end());
            for (const std::string& mode : modes) {
                const auto peak = a.heap_peaks.find(mode);
                if (peak == a.heap_peaks.end()) fail(cases[s].name + ": " + mode + ": no heap peak");
                else if (mode == "threads") threads[s].push_back({peak->second, a.codec_states.at(mode)});
                else peaks[s][mode] = peak->second;
            }
        }
    for (const auto& [mode, at_n] : peaks[0])
        if (const auto at_10n = peaks[1].find(mode); at_10n != peaks[1].end())
            if (const std::string grew = peak_grew(mode, {at_n}, {at_10n->second}, n); !grew.empty()) fail(grew);
    for (const std::string& grew : threads_check(threads[0], threads[1], n).failures) fail(grew);
    return bad;
}

std::vector<Poi> pois_of(std::mt19937& rng, uint32_t n, uint32_t vertices, bool points_too) {
    std::vector<Poi> pois;
    for (uint32_t i = 0; i < n; i++)
        pois.push_back({i, points_too && i % 3 == 0 ? std::vector<char>{} : block(rng, vertices ? vertices : 1 + rng() % 8)});
    return pois;
}

}  // namespace

// An unchanged parent of an encoded family (poi_records.bin sent as kept)
// replays with its child: both files unchanged, then the records unchanged
// and one vertex block edited.
TEST(patch_lowmem_replays_an_unchanged_parent_with_its_child) {
    ScratchDir dir("lowmem-same");
    std::mt19937 rng(20261027);
    const auto pois = pois_of(rng, 300, 0, true);
    auto edited = pois;
    std::fill(edited[100].block.begin() + 10, edited[100].block.end(), 0);
    for (const auto& [name, new_pois] : {std::pair<std::string, std::vector<Poi>>{"unchanged", pois}, {"edited", edited}}) {
        BuildDir old_d, new_d;
        put_pois(old_d, pois);
        put_pois(new_d, new_pois);
        const LowMemCase c{dir.path(), name};
        REQUIRE(prepare(c, old_d, new_d).empty());

        CHECK(apply_both_modes(c).empty());
    }
}

// Old child bytes the replay skips are dropped as the shared decoder produces
// them, not queued until the child's next read: 4 MiB of vertices of a
// deleted run of POI polygons (the parent's DELETE run), and of polygons
// whose records stay but whose blocks all change (encoding byte 1 instead of
// 0, the same vertex size: no byte of a block is kept, no MATCH reads).
TEST(patch_lowmem_drops_the_child_bytes_of_deleted_and_replaced_records) {
    ScratchDir dir("lowmem-run");
    std::mt19937 rng(20261027);
    const auto big = pois_of(rng, 20000, 50, false);
    std::vector<Poi> kept(big.begin(), big.begin() + 1000), moved;
    kept.insert(kept.end(), big.begin() + 19000, big.end());
    for (size_t i = 0; i < big.size(); i++) {
        Poi p = big[i];
        if (i < 19000) {
            p.block[0] = 1;
            for (size_t k = 2; k < p.block.size(); k++) p.block[k] = static_cast<char>(p.block[k] ^ 0x5A);
        }
        moved.push_back(p);
    }
    for (const auto& [name, new_pois] : {std::pair<std::string, std::vector<Poi>>{"deleted-run", kept}, {"replaced", moved}}) {
        BuildDir old_d, new_d;
        put_pois(old_d, big);
        put_pois(new_d, new_pois);
        const LowMemCase c{dir.path(), name};
        REQUIRE(prepare(c, old_d, new_d).empty());

        CHECK(apply_both_modes(c, decoder_peaks_at_most("poi", DECODER_HOLD)).empty());
    }
}

// The same for the geo index: the lists of a removed stretch of cells (4 MiB
// of street lists).
TEST(patch_lowmem_drops_the_lists_of_removed_cells) {
    ScratchDir dir("lowmem-geo");
    std::vector<GeoRow> rows, kept;
    for (uint32_t k = 0; k < 20000; k++) {
        std::vector<uint32_t> ids(50);
        for (uint32_t i = 0; i < 50; i++) ids[i] = k % 950 + i;
        rows.push_back({k, ids});
    }
    kept.assign(rows.begin(), rows.begin() + 1000);
    kept.insert(kept.end(), rows.begin() + 19000, rows.end());
    BuildDir old_d, new_d;
    put_geo(old_d, rows);
    put_geo(new_d, kept);
    const LowMemCase c{dir.path(), "removed-cells"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    CHECK(apply_both_modes(c, decoder_peaks_at_most("gxc", DECODER_HOLD)).empty());
}

// The first nightly without unnamed POIs tombstoned 24,543 contiguous planet
// poi/all records in place. The POIs after the run stay one MATCH run, so
// the patch carries the run's tombstones but not those POIs, and the dead
// POIs' vertex bytes are dropped as the shared decoder produces them.
TEST(patch_lowmem_replays_a_long_run_of_tombstones) {
    ScratchDir dir("lowmem-tombstones");
    std::mt19937 rng(20261102);
    constexpr uint32_t HEAD = 1000, DEAD = 12000, TAIL = 8000;
    const auto pois = pois_of(rng, HEAD + DEAD + TAIL, 0, true);
    auto today = pois;
    for (uint32_t i = HEAD; i < HEAD + DEAD; i++) today[i] = {0, {}};
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, today);
    std::vector<char>& records = new_d["poi_records.bin"];
    for (size_t at = size_t(HEAD) * POI_RECORD_STRIDE; at < size_t(HEAD + DEAD) * POI_RECORD_STRIDE; at += POI_RECORD_STRIDE)
        for (size_t field : {POI_RECORD_NAME_ID_OFF, POI_RECORD_PARENT_STREET_ID_OFF, POI_RECORD_PARENT_POSTCODE_ID_OFF,
                             POI_RECORD_PARENT_POLY_ID_OFF})
            put_at(records, at + field, NO);
    const LowMemCase c{dir.path(), "tombstones"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    CHECK(raw_patch(c).size() < size_t(DEAD + TAIL / 2) * POI_RECORD_STRIDE);
    CHECK(apply_modes(c, {"plain"}).bad.empty());
    CHECK(apply_both_modes(c, decoder_peaks_at_most("poi", DECODER_HOLD)).empty());
}

// The 2026-11-03 nightly raised the importance of ~1 planet poi/all record in
// 23, in stretches where 7 in 10 changed. The POIs kept among them stay
// MATCHed, so the patch holds less than twice the edited records' bytes
// (main's sent the rest of the file), and their DELETE + INSERT pairs replay
// with the vertex blocks they keep.
TEST(patch_lowmem_replays_records_edited_in_place) {
    ScratchDir dir("lowmem-edited");
    std::mt19937 rng(20261103);
    constexpr uint32_t N = 30000, DENSE = 1000, DENSE_N = 12000;
    const auto pois = pois_of(rng, N, 0, true);
    const auto edited = gctest::edited_in_place(rng, N, DENSE, DENSE_N);
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, pois);
    std::vector<char>& records = new_d["poi_records.bin"];
    size_t n_edited = 0;
    for (uint32_t i = 0; i < N; i++)
        if (edited[i]) { records[size_t(i) * POI_RECORD_STRIDE + offsetof(PoiRecord, importance)] = 40; n_edited++; }
    const LowMemCase c{dir.path(), "edited"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    const size_t raw = raw_patch(c).size();
    std::printf("    raw patch %zu bytes, %zu records edited\n", raw, n_edited);
    CHECK(raw < n_edited * POI_RECORD_STRIDE * 2);
    CHECK(apply_modes(c, {"plain"}).bad.empty());
    CHECK(apply_both_modes(c, decoder_peaks_at_most("poi", DECODER_HOLD)).empty());
}

// An encoded set whose two address files are empty: the diff sends
// addr_points.bin's (empty) merge with the streets after it and
// addr_vertices.bin later, as a full replacement. An empty parent has no
// child bytes to replay with it.
TEST(patch_lowmem_applies_to_a_set_with_empty_address_files) {
    ScratchDir dir("lowmem-noaddr");
    std::vector<GeoRow> rows;
    for (uint32_t k = 0; k < 50; k++) rows.push_back({k, {k % 10}});
    std::vector<GeoRow> new_rows(rows.begin(), rows.begin() + 40);
    new_rows.push_back({60, {3}});
    BuildDir old_d, new_d;
    put_geo(old_d, rows);
    put_geo(new_d, new_rows);
    for (BuildDir* d : {&old_d, &new_d}) (*d)["addr_points.bin"] = (*d)["addr_vertices.bin"] = {};
    const LowMemCase c{dir.path(), "empty-addresses"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    CHECK(apply_both_modes(c).empty());
}

// A parent's REPLACE run (the same street ways, renamed) whose children stay
// one MATCH run: the old records are dropped as the node reads make the
// shared decoder produce them, not queued for the whole run (2.4 MB of them).
TEST(patch_lowmem_drops_the_old_records_of_a_replace_run) {
    ScratchDir dir("lowmem-named");
    std::vector<uint32_t> names(200000), renamed(200000);
    for (uint32_t i = 0; i < names.size(); i++) {
        names[i] = i;
        renamed[i] = i + 1000000;
    }
    BuildDir old_d, new_d, old_meta, new_meta;
    put_named_ways(old_d, old_meta, names);
    put_named_ways(new_d, new_meta, renamed);
    const LowMemCase c{dir.path(), "replaced-parents"};
    REQUIRE(prepare(c, old_d, new_d, old_meta, new_meta).empty());

    CHECK(apply_both_modes(c, decoder_peaks_at_most("streets", DECODER_HOLD)).empty());
}

// An INSERT stretch of POI polygons (2 MB of new vertex blocks between kept
// ones), which the replay hands the encoder record by record, each record's
// block after it: the encoder never holds more than a piece and the largest
// block it takes whole.
TEST(patch_lowmem_encodes_a_poi_insert_stretch_within_its_hold) {
    ScratchDir dir("lowmem-poi-insert");
    std::mt19937 rng(20261030);
    std::vector<Poi> old_pois, inserted;
    for (uint32_t i = 0; i < 300; i++) old_pois.push_back({i, block(rng, 50)});
    for (uint32_t i = 0; i < 500; i++) inserted.push_back({1000 + i, block(rng, 1000)});
    auto new_pois = old_pois;
    new_pois.insert(new_pois.begin() + 150, inserted.begin(), inserted.end());
    BuildDir old_d, new_d;
    put_pois(old_d, old_pois);
    put_pois(new_d, new_pois);
    const LowMemCase c{dir.path(), "poi-insert"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    CHECK(apply_both_modes(c, encoder_peaks_at_most("poi", POI_ENCODER_HOLD)).empty());
}

// An escaped address footprint (encoding 3, 800K vertices: 6.4 MB) kept
// between small ones and abs2 ones of ABS2_MAX_VERTICES while the address
// before them goes, so the replay MATCHes them and moves their
// vertex_offsets; and another of ABS2_MAX_VERTICES inserted, which reaches
// the encoder in one write of most of it: the decoder hands the escaped one
// over and the encoder takes it in pieces, neither holding more than
// DECODER_HOLD, nor the encoder more than a piece and the largest abs2
// footprint, which it takes whole.
TEST(patch_lowmem_copies_an_escaped_footprint_through_in_pieces) {
    ScratchDir dir("lowmem-footprint");
    std::mt19937 rng(20261030);
    std::vector<Addr> addrs;
    for (uint32_t i = 0; i < 300; i++)
        addrs.push_back({47.0f + static_cast<float>(i) * 1e-4f, i % 3 == 2 ? std::vector<char>{} : block(rng, 3 + rng() % 8)});
    std::vector<char> escaped = block(rng, 2 * 800000);
    escaped[0] = 3;
    addrs[150].block = escaped;
    for (const uint32_t i : {60u, 61u, 240u}) addrs[i].block = block(rng, ABS2_MAX_VERTICES);
    std::vector<Addr> fewer_addrs(addrs.begin() + 1, addrs.end());
    fewer_addrs.insert(fewer_addrs.begin() + 200, Addr{48.0f, block(rng, ABS2_MAX_VERTICES)});
    BuildDir old_d, new_d;
    put_addrs(old_d, addrs);
    put_addrs(new_d, fewer_addrs);
    const LowMemCase c{dir.path(), "escaped-footprint"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const LogCheck decoder = decoder_peaks_at_most("addr", DECODER_HOLD), encoder = encoder_peaks_at_most("addr", ADDR_ENCODER_HOLD);

    CHECK(apply_both_modes(c, [&](const std::string& log) {
              const std::string e = decoder(log);
              return e.empty() ? encoder(log) : e;
          }).empty());
}

// The memory checks of the replay tests above, on report lines as the
// patcher writes them (sync: queue peaks; --threads: ring and queue peaks):
// every peak at most the limit passes, one past it fails, a line whose peaks
// are missing or not numbers fails instead of passing as a peak of 0, and
// each check reads its own codec's line.
TEST(codec_peaks_at_most_checks_every_peak_of_its_report_line) {
    for (const bool decoder : {true, false}) {
        const LogCheck check = decoder ? decoder_peaks_at_most("poi", 100) : encoder_peaks_at_most("poi", 100);
        const std::string mine = decoder ? "decode" : "encode", other = decoder ? "encode" : "decode";

        CHECK(check(other + " poi: queue peaks 999 B\n" + mine + " poi: queue peaks 10 100 B\n").empty());
        CHECK(check(mine + " poi: thread cpu 0.1 s, ring peaks 7 8 B, queue peaks 9 100 B\n").empty());
        CHECK(!check(mine + " poi: queue peaks 10 101 B\n").empty());
        CHECK(!check(mine + " poi: thread cpu 0.1 s, ring peaks 101 8 B, queue peaks 9 10 B\n").empty());
        for (const std::string& bad : {mine + " poi:", mine + " poi: queue B", mine + " poi: queue peaks B", mine + " poi: queue peaks ten B",
                                       mine + " poi: thread cpu 0.1 s, ring peaks 7 8 B, queue peaks 9 x B", mine + " streets: queue peaks 1 B",
                                       other + " poi: queue peaks 1 B"})
            CHECK(!check(bad + "\n").empty());
    }
}

// run_with_heap_peak measures the process it runs and nothing else: a dd of
// one 16 MiB block peaks at least 15 MiB above a dd of one 1 MiB block, and a
// timeout running the 16 MiB dd as its child (as the patcher runs the zstd
// CLI) peaks below 1 MiB.
TEST(run_with_heap_peak_measures_the_process_it_runs_and_not_its_children) {
    if (!gctest::heap_peak_shim()) {
        std::printf("    SKIP heap peaks of dd: no heap shim under the sanitizers\n");
        return;
    }
    ScratchDir dir("heap-peak");
    auto peak_of = [&](const std::vector<std::string>& words) {
        const auto [ok, heap_peak] = gctest::run_with_heap_peak(words, dir.path() + "/log");
        REQUIRE(ok && heap_peak);
        return *heap_peak;
    };
    auto dd = [](const std::string& block) { return std::vector<std::string>{"dd", "if=/dev/zero", "of=/dev/null", "bs=" + block, "count=1"}; };
    std::vector<std::string> in_child = dd("16M");
    in_child.insert(in_child.begin(), {"timeout", "60"});

    const uint64_t one = peak_of(dd("1M")), sixteen = peak_of(dd("16M")), parent = peak_of(in_child);

    CHECK(sixteen >= one + (uint64_t{15} << 20));
    CHECK(parent < (uint64_t{1} << 20));
}

// The tools build with _FILE_OFFSET_BITS=64, which links their mmap() calls
// (SequentialFileReader's read windows) to mmap64: heap-peak-probe, built the
// same way and allocating nothing itself, peaks exactly the most MiB it held
// mapped at once above its peak mapping nothing. Every page of a mapping
// counts until a munmap() or mremap() unmaps it or a MAP_FIXED mapping
// replaces it, and counts where an mremap() moves it, however the ranges cut
// the mappings; a refused call changes nothing. Each case ends mapping more
// than it started with, so the pages left from its cuts decide the peak.
TEST(run_with_heap_peak_counts_the_mapped_pages_of_a_program_built_as_the_tools_are) {
#ifndef GEOCODER_HEAP_PEAK_PROBE
    std::printf("    SKIP heap peaks of heap-peak-probe: no heap shim under the sanitizers\n");
#else
    ScratchDir dir("heap-peak-probe");
    auto peak_of = [&](const std::vector<std::string>& steps) {
        std::vector<std::string> words = {GEOCODER_HEAP_PEAK_PROBE};
        words.insert(words.end(), steps.begin(), steps.end());
        const auto [ok, heap_peak] = gctest::run_with_heap_peak(words, dir.path() + "/log");
        return ok ? heap_peak : std::nullopt;
    };
    struct Case {
        const char* what;
        std::vector<std::string> steps;
        uint64_t mib;
    };
    const Case cases[] = {
        {"16 MiB unmapped whole", {"map", "0", "16", "unmap", "0", "16"}, 16},
        {"the first 6 MiB of 8 unmapped, then 8 MiB more", {"map", "0", "8", "unmap", "0", "6", "map", "16", "8"}, 10},
        {"the last 6 MiB of 8 unmapped, then 8 MiB more", {"map", "0", "8", "unmap", "2", "6", "map", "16", "8"}, 10},
        {"a 4 MiB hole punched in the middle of 8, then 8 MiB more", {"map", "0", "8", "unmap", "2", "4", "map", "16", "8"}, 12},
        {"a 4 MiB hole punched in 8, the 2 MiB before it then unmapped, then 8 MiB more",
         {"map", "0", "8", "unmap", "2", "4", "unmap", "0", "2", "map", "16", "8"}, 10},
        {"a 4 MiB hole punched in 8, the 2 MiB after it then unmapped, then 8 MiB more",
         {"map", "0", "8", "unmap", "2", "4", "unmap", "6", "2", "map", "16", "8"}, 10},
        {"one munmap() of the last 2 MiB of a 4 MiB mapping and the first 2 of the next, then 8 MiB more",
         {"map", "0", "4", "map", "4", "4", "unmap", "2", "4", "map", "16", "8"}, 12},
        {"one munmap() of the end of a 4 MiB mapping, all of the next and the start of a third, then 16 MiB more",
         {"map", "0", "4", "map", "4", "4", "map", "8", "4", "unmap", "2", "8", "map", "16", "16"}, 20},
        {"4 MiB in the middle of 8 mapped over, then 8 MiB more", {"map", "0", "8", "over", "2", "4", "map", "16", "8"}, 16},
        {"a refused munmap() of 0 bytes at the start of 8 MiB, then 8 MiB more",
         {"map", "0", "8", "unmap-refused", "0", "0", "map", "16", "8"}, 16},
        {"a refused munmap() of 4 MiB from 1 byte into 8, then 8 MiB more",
         {"map", "0", "8", "unmap-refused", "0+1", "4", "map", "16", "8"}, 16},
        {"8 MiB shrunk to 2 in place by mremap(), then 8 MiB more", {"map", "0", "8", "resize", "0", "8", "2", "map", "16", "8"}, 10},
        {"a 4 MiB hole punched in 8, the 8 then shrunk to 6 in place by mremap(), then 8 MiB more",
         {"map", "0", "8", "unmap", "2", "4", "resize", "0", "8", "6", "map", "16", "8"}, 10},
        {"a 4 MiB hole punched in 8, the 8 then moved by mremap() as 2, then 8 MiB more",
         {"map", "0", "8", "unmap", "2", "4", "move", "0", "8", "16", "2", "map", "32", "8"}, 10},
        {"4 MiB grown to 8 in place by mremap(), then 8 MiB more", {"map", "0", "4", "resize", "0", "4", "8", "map", "16", "8"}, 16},
        {"a refused mremap() growing 4 MiB into the next 4, then 8 MiB more",
         {"map", "0", "4", "map", "4", "4", "resize-refused", "0", "4", "8", "map", "16", "8"}, 16},
        {"8 MiB moved by mremap() as 4 over the start of another 8, then 16 MiB more",
         {"map", "0", "8", "map", "16", "8", "move", "0", "8", "16", "4", "map", "32", "16"}, 24},
        {"4 MiB moved by mremap() as 8, then 8 MiB more", {"map", "0", "4", "move", "0", "4", "16", "8", "map", "32", "8"}, 16},
        {"the middle 4 MiB of 8 moved away by mremap() as 2, then 8 MiB more",
         {"map", "0", "8", "move", "2", "4", "16", "2", "map", "32", "8"}, 14},
        {"the middle 4 MiB of 8 moved by mremap() over two whole 2 MiB mappings, then 8 MiB more",
         {"map", "0", "8", "map", "16", "2", "map", "18", "2", "move", "2", "4", "16", "4", "map", "32", "8"}, 16},
        {"three 2 MiB mappings moved by mremap() as 2 onto the middle of 8, then 8 MiB more",
         {"map", "0", "2", "map", "2", "2", "map", "4", "2", "map", "16", "8", "move", "0", "6", "18", "2", "map", "32", "8"}, 16},
        {"8 MiB copied by mremap() with MREMAP_DONTUNMAP, then 8 MiB more",
         {"map", "0", "8", "copy", "0", "8", "16", "map", "32", "8"}, 24},
        {"4 MiB of shared 8 mapped a second time by mremap(), then 8 MiB more",
         {"share", "0", "8", "clone", "2", "16", "4", "map", "32", "8"}, 20},
    };

    const std::optional<uint64_t> none = peak_of({});
    REQUIRE(none);
    std::vector<std::string> bad;
    for (const Case& c : cases) {
        const std::optional<uint64_t> peak = peak_of(c.steps);
        const uint64_t want = *none + (c.mib << 20);
        if (!peak) bad.push_back(std::string(c.what) + ": the probe failed");
        else if (*peak != want) bad.push_back(std::string(c.what) + ": peak " + std::to_string(*peak) + " B, not " + std::to_string(want));
    }

    for (const std::string& why : bad) std::printf("    %s\n", why.c_str());
    CHECK(bad.empty());
#endif
}

// The heap shim's table holds 4096 mappings: heap-peak-probe cutting a
// mapping into 4096 pieces runs and peaks at the mapping whole, as does one
// cutting 4094 beside a shared mapping and a second mapping of its pages
// (which leaves the first whole), and so do ones that fill the table, then
// with mremap() move the middle of a mapping over two whole ones, or three
// mappings as one onto the middle of another: each move cuts a mapping in
// two, yet leaves no more mappings than it found. Cutting one into 4097 ends
// with the shim's message and no peak, since a piece left out of the table
// would count wrong from then on.
TEST(run_with_heap_peak_ends_a_program_with_more_mappings_than_the_shim_holds) {
#ifndef GEOCODER_HEAP_PEAK_PROBE
    std::printf("    SKIP heap peaks of heap-peak-probe: no heap shim under the sanitizers\n");
#else
    ScratchDir dir("heap-peak-probe-full");
    const std::string log = dir.path() + "/log";
    const uint64_t page = uint64_t(sysconf(_SC_PAGESIZE));

    const auto [none_ok, none] = gctest::run_with_heap_peak({GEOCODER_HEAP_PEAK_PROBE}, dir.path() + "/none");
    REQUIRE(none_ok && none);
    const auto [fits, fits_peak] = gctest::run_with_heap_peak({GEOCODER_HEAP_PEAK_PROBE, "shatter", "0", "4096"}, dir.path() + "/fits");
    CHECK(fits);
    CHECK(fits_peak && *fits_peak == *none + 2 * 4096 * page);
    const auto [cloned, cloned_peak] = gctest::run_with_heap_peak(
        {GEOCODER_HEAP_PEAK_PROBE, "share", "0", "8", "clone", "2", "16", "4", "shatter", "32", "4094"}, dir.path() + "/cloned");
    CHECK(cloned);
    CHECK(cloned_peak && *cloned_peak == *none + (uint64_t{12} << 20) + 2 * 4094 * page);
    const auto [moved_over, moved_over_peak] = gctest::run_with_heap_peak(
        {GEOCODER_HEAP_PEAK_PROBE, "map", "0", "8", "map", "16", "2", "map", "18", "2", "shatter", "32", "4093", "move", "2", "4", "16", "4"},
        dir.path() + "/moved-over");
    CHECK(moved_over);
    CHECK(moved_over_peak && *moved_over_peak == *none + (uint64_t{12} << 20) + 2 * 4093 * page);
    const auto [moved_into, moved_into_peak] = gctest::run_with_heap_peak(
        {GEOCODER_HEAP_PEAK_PROBE, "map", "0", "2", "map", "2", "2", "map", "4", "2", "map", "16", "8", "shatter", "32", "4092",
         "move", "0", "6", "18", "2"},
        dir.path() + "/moved-into");
    CHECK(moved_into);
    CHECK(moved_into_peak && *moved_into_peak == *none + (uint64_t{14} << 20) + 2 * 4092 * page);

    const auto [ok, heap_peak] = gctest::run_with_heap_peak({GEOCODER_HEAP_PEAK_PROBE, "shatter", "0", "4097"}, log);

    CHECK(!ok);
    CHECK(!heap_peak);
    const std::vector<char> text = read_file(log);
    const std::string said(text.begin(), text.end());
    CHECK(said.find("heap_peak_shim.c: more anonymous mappings than its table holds\n") != std::string::npos);
#endif
}

// A --threads run's codec state: each report line with a thread's peaks, its
// peaks rounded up to powers of two of at least 4 KiB; sync's report lines
// (queue peaks only) and other lines add nothing.
TEST(codec_state_rounds_the_peaks_of_each_threaded_codec) {
    CHECK_EQ(codec_state("applied\nencode streets: thread cpu 0.022917 s, ring peaks 122880 131072 B, queue peaks 122892 65536 B\n"
                         "encode poi: queue peaks 5 B\ndecode addr: thread cpu 0.000357 s, ring peaks 2800 0 B, queue peaks 4097 0 B\n"),
             std::string("encode streets 128K 128K 128K 64K; decode addr 4K 0K 8K 0K"));
    CHECK_EQ(codec_state("encode poi: queue peaks 5 B\n"), std::string());
}

// threads_check on made-up runs (peaks in KiB, codec states a, b and c):
//   512 KiB of growth in the state both sizes' runs end in fails, settled,
//   and an eighth run at n in another state 384 KiB higher leaves it failed,
//   though it would pass the runs as a whole;
//   when the growth moved every run at 10 n into a state of its own (c), it
//   still fails, unsettled: the runs as a whole are the first 7 at each
//   size, without that eighth run;
//   runs at n that never filled their rings (a) against runs at 10 n that
//   all did (b) fail unsettled, and pass once a run at n ends in b;
//   growth past the difference between the states still fails then, settled
//   once b has two runs at each size;
//   a failure in the only state both sizes share, with one run in it at
//   each, is unsettled;
//   the peak stays flat if it does so in one state both sizes share;
//   runs in states that never meet are held to the slack as a whole.
TEST(heap_threads_check_tells_growth_from_the_scheduler) {
    auto runs = [](std::initializer_list<std::pair<uint64_t, int>> kib_times, const std::string& state) {
        std::vector<ThreadsRun> v;
        for (const auto& [kib, times] : kib_times)
            for (int i = 0; i < times; i++) v.push_back({kib << 10, state});
        return v;
    };
    auto plus = [](std::vector<ThreadsRun> v, const std::vector<ThreadsRun>& more) {
        v.insert(v.end(), more.begin(), more.end());
        return v;
    };
    auto failed = [](const ThreadsCheck& c, bool settled) { return !c.failures.empty() && c.settled == settled; };
    auto passed = [](const ThreadsCheck& c) { return c.failures.empty() && c.settled; };
    constexpr uint32_t N = 10000;

    const auto grown_n = runs({{1024, 7}}, "a"), grown_10n = runs({{1536, 7}}, "a");
    CHECK(failed(threads_check(grown_n, grown_10n, N), true));
    CHECK(failed(threads_check(plus(grown_n, runs({{1408, 1}}, "b")), plus(grown_10n, runs({{1536, 1}}, "a")), N), true));
    CHECK(passed(threads_check(runs({{1024, 7}, {1408, 1}}, ""), runs({{1536, 8}}, ""), N)));
    CHECK(failed(threads_check(plus(grown_n, runs({{1408, 1}}, "b")), runs({{1536, 8}}, "c"), N), false));

    const auto empty_n = runs({{1212, 7}}, "a"), full_10n = runs({{1872, 7}}, "b");
    CHECK(failed(threads_check(empty_n, full_10n, N), false));
    CHECK(passed(threads_check(plus(empty_n, runs({{1900, 1}}, "b")), plus(full_10n, runs({{1872, 1}}, "b")), N)));
    const auto grown_full_10n = runs({{2384, 8}}, "b");
    CHECK(failed(threads_check(plus(empty_n, runs({{1900, 1}}, "b")), grown_full_10n, N), false));
    CHECK(failed(threads_check(plus(empty_n, runs({{1900, 2}}, "b")), grown_full_10n, N), true));

    CHECK(failed(threads_check(plus(runs({{1212, 1}}, "a"), runs({{1900, 6}}, "b")),
                               plus(runs({{1600, 1}}, "a"), runs({{1872, 6}}, "c")), N), false));
    CHECK(passed(threads_check(plus(runs({{1212, 6}}, "a"), runs({{1900, 1}}, "b")),
                               plus(runs({{1300, 6}}, "a"), runs({{2300, 1}}, "b")), N)));
    CHECK(passed(threads_check(runs({{1000, 7}}, "a"), runs({{1200, 7}}, "b"), N)));
    CHECK(failed(threads_check(runs({{1000, 7}}, "a"), runs({{1300, 7}}, "b"), N), false));
}

// n geo cells whose street lists all change (each cell's street becomes the
// next one) over street ways that stay: n street entry corrections, which the
// patcher reads where they sit in the patch.
TEST(patch_heap_stays_flat_as_geo_entry_deltas_grow) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::vector<GeoRow> rows, changed;
        for (uint32_t k = 0; k < n; k++) {
            rows.push_back({k, {k % 950}});
            changed.push_back({k, {k % 950 + 1}});
        }
        BuildDir old_d, new_d;
        put_geo(old_d, rows);
        put_geo(new_d, changed);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        std::vector<char> corrections;
        append(corrections, GEO_ENTRY_DELTA_MARKER);
        append(corrections, static_cast<uint32_t>(PatchFileId::STREET_ENTRIES));
        append(corrections, n);
        return patch_holds(c, corrections, std::to_string(n) + " street entry corrections");
    }, 20000).empty());
}

// n geo cells removed and n added (the old cells are the even ones, the new
// cells the odd ones, none with a list): cell changes of n and n cells, which
// the patcher reads where they sit in the patch.
TEST(patch_heap_stays_flat_as_geo_cells_change) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::vector<GeoRow> old_rows, new_rows;
        for (uint32_t k = 0; k < n; k++) {
            old_rows.push_back({2 * k, {}});
            new_rows.push_back({2 * k + 1, {}});
        }
        BuildDir old_d, new_d;
        put_geo(old_d, old_rows);
        put_geo(new_d, new_rows);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        std::vector<char> changes;
        append(changes, CELL_CHANGES_GEO_MARKER);
        append(changes, n);
        append(changes, n);
        return patch_holds(c, changes, std::to_string(n) + " added and " + std::to_string(n) + " removed geo cells");
    }, 20000).empty());
}

// n POI cells whose lists all change (each cell's POI becomes the next one)
// over POIs that stay: a POI cell index delta of n cells, which the patcher
// reads where it sits in the patch.
TEST(patch_heap_stays_flat_as_a_poi_cell_index_delta_grows) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::mt19937 rng(20261028);
        const auto pois = pois_of(rng, 1000, 0, true);
        CellLists old_cells, new_cells;
        for (uint32_t k = 0; k < n; k++) {
            old_cells[cell_id(k)] = {k % 950};
            new_cells[cell_id(k)] = {k % 950 + 1};
        }
        BuildDir old_d, new_d;
        put_pois(old_d, pois);
        put_pois(new_d, pois);
        put_cells(old_d, "poi", old_cells);
        put_cells(new_d, "poi", new_cells);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        // No removed cells, then n cells of one lost and one gained id.
        std::vector<char> delta;
        append(delta, CELL_INDEX_DELTA_MARKER);
        append(delta, static_cast<uint32_t>(PatchFileId::POI_ENTRIES));
        append(delta, uint64_t{8} + uint64_t{24} * n);
        return patch_holds(c, delta, "POI cell index delta of " + std::to_string(n) + " cells");
    }, 10000).empty());
}

// n POI cells removed and n added (the old cells are the even ones, the new
// cells the odd ones, all with empty lists) over POIs that stay: cell changes
// of n and n cells, which the patcher reads where they sit in the patch.
TEST(patch_heap_stays_flat_as_poi_cells_change) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::mt19937 rng(20261028);
        const auto pois = pois_of(rng, 1000, 0, true);
        CellLists old_cells, new_cells;
        for (uint32_t k = 0; k < n; k++) {
            old_cells[cell_id(2 * k)] = {};
            new_cells[cell_id(2 * k + 1)] = {};
        }
        BuildDir old_d, new_d;
        put_pois(old_d, pois);
        put_pois(new_d, pois);
        put_cells(old_d, "poi", old_cells);
        put_cells(new_d, "poi", new_cells);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        std::vector<char> changes;
        append(changes, CELL_CHANGES_POI_MARKER);
        append(changes, n);
        append(changes, n);
        return patch_holds(c, changes, std::to_string(n) + " added and " + std::to_string(n) + " removed POI cells");
    }, 10000).empty());
}

// n POI names in the POI string tier, the new day's in reverse order, over
// POIs that keep theirs: the tier's edit keeps one name and deletes and adds
// the other n - 1, whose offsets go as n - 1 moved-string pairs, all of which
// the patcher reads where they sit in the patch.
TEST(patch_heap_stays_flat_as_strings_move) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        constexpr uint32_t NAME = 12;  // "poi 0000000\0"
        auto name = [](uint32_t i) {
            char b[16];
            std::snprintf(b, sizeof b, "poi %07u", i);
            return std::string(b, NAME);
        };
        std::string old_pool, new_pool;
        for (uint32_t i = 0; i < n; i++) {
            old_pool += name(i);
            new_pool += name(n - 1 - i);
        }
        std::mt19937 rng(20261029);
        const auto pois = pois_of(rng, 1000, 0, true);
        std::vector<uint32_t> old_names, new_names;
        for (uint32_t k = 0; k < pois.size(); k++) {
            old_names.push_back(k * NAME);
            new_names.push_back((n - 1 - k) * NAME);
        }
        BuildDir old_d, new_d;
        put_pois(old_d, pois, old_names);
        put_pois(new_d, pois, new_names);
        old_d["strings_poi.bin"].assign(old_pool.begin(), old_pool.end());
        new_d["strings_poi.bin"].assign(new_pool.begin(), new_pool.end());
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        // The tier's n_added, n_deleted and order.
        std::vector<char> tier;
        append(tier, n - 1);
        append(tier, n - 1);
        append(tier, STRING_TIER_INDEXED);
        return patch_holds(c, tier, std::to_string(n - 1) + " moved POI names");
    }, 5000).empty());
}

// n POIs, each named by its own POI name, and the new day's names in reverse
// order, sent past the strings section's pairs (--string-pairs-max 0): the
// tier's edit moves n - 1 names, and the POI merge carries the n - 1 string
// fixups of the POIs that keep them, which the patcher reads in record order
// where they sit in the patch.
TEST(patch_heap_stays_flat_as_strings_move_past_the_pair_table) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        BuildDir old_d, new_d;
        put_reversed_poi_names(old_d, new_d, n);
        if (std::string e = prepare(c, old_d, new_d, {}, {}, NO_STRING_PAIRS); !e.empty()) return e;
        return patch_holds(c, poi_merge_head(n, n, n - 1), std::to_string(n - 1) + " POI name fixups");
    }, 5000).empty());
}

// n strings in the core tier, which sorts, and the new day without every
// second one: each string kept after the first moves by its own amount, so
// the patcher derives n / 2 - 1 string remap runs, which it spills to its
// scratch dir.
TEST(patch_heap_stays_flat_as_string_runs_grow) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::string old_pool, new_pool;
        for (uint32_t i = 0; i < n; i++) {
            char b[24];
            std::snprintf(b, sizeof b, "street %07u", i);
            old_pool.append(b, strlen(b) + 1);
            if (i % 2 == 0) new_pool.append(b, strlen(b) + 1);
        }
        BuildDir old_d, new_d;
        old_d["strings_core.bin"].assign(old_pool.begin(), old_pool.end());
        new_d["strings_core.bin"].assign(new_pool.begin(), new_pool.end());
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        // The tier's n_added, n_deleted, order and first deleted indices.
        std::vector<char> tier;
        for (const uint32_t v : {0u, n / 2, STRING_TIER_SORTED, 1u, 3u}) append(tier, v);
        return patch_holds(c, tier, std::to_string(n / 2) + " deleted core strings");
    }, 10000).empty());
}

// n street ways, the new day without every second one: the street replay
// keeps n / 2 single records, each a record remap run of its own, which the
// patcher spills to its scratch dir. n is large enough (2.4 MB of ways) for
// --threads to fill its codec rings at n as it does at 10 n: at 240 KB it may
// not, and under load its peak at 10 n then came out up to 300 KB higher.
TEST(patch_heap_stays_flat_as_record_runs_grow) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        BuildDir old_d, new_d;
        put_geo(old_d, {{0, {0}}}, n);
        put_geo(new_d, {{0, {0}}}, n / 2);
        const std::vector<char>& ways = old_d["street_ways.bin"];
        std::vector<char> kept;
        for (size_t at = 0; at < ways.size(); at += 2 * WAY_HEADER_STRIDE)
            kept.insert(kept.end(), ways.begin() + at, ways.begin() + at + WAY_HEADER_STRIDE);
        new_d["street_ways.bin"] = kept;
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        // The street merge: n ops of 5 bytes, a kept way, a deleted one and
        // so on.
        std::vector<char> merge;
        append(merge, uint64_t{5} * n);
        for (const MergeOp op : {OP_MATCH_RUN, OP_DELETE_RUN, OP_MATCH_RUN, OP_DELETE_RUN}) {
            merge.push_back(static_cast<char>(op));
            append(merge, 1u);
        }
        return patch_holds(c, merge, std::to_string(n / 2) + " street ways kept one by one");
    }, 200000).empty());
}

// n POIs that change and keep their relaxed key: n secondary-matched pairs,
// which the patcher reads where they sit in the patch.
TEST(patch_heap_stays_flat_as_records_move) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        BuildDir old_d, new_d;
        put_changed_pois(old_d, new_d, n);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        return patch_holds(c, secondary_remap_head(n), std::to_string(n) + " secondary-matched POIs");
    }, 10000).empty());
}

// n street ways moved by the identity sidecars, with addresses on the
// streets: the parent-id remap's street leg of n moved ways, which the
// patcher reads where it sits in the patch as the street remap's pairs.
// 200,000 ways kept in order (2.4 MB) after them.
TEST(patch_heap_stays_flat_as_streets_move) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        BuildDir old_d, new_d, old_meta, new_meta;
        put_moved_ways(old_d, new_d, old_meta, new_meta, n, 200000);
        if (std::string e = prepare(c, old_d, new_d, old_meta, new_meta); !e.empty()) return e;
        // The street leg.
        std::vector<char> legs;
        for (const uint32_t w : {POI_PARENT_REMAP_MARKER, n}) append(legs, w);
        return patch_holds(c, legs, std::to_string(n) + " moved street ways");
    }, 10000).empty());
}

// The diff sends the moved strings as pairs up to its cap and as string
// fixups past it (GCPATCH v12): 99 moved POI names go as 99 pairs under a cap
// of 99, and as the POI merge's 99 string fixups, with no pair, under a cap of
// 98. Both patches apply in every mode.
TEST(patch_diff_sends_moved_strings_as_string_fixups_past_the_pair_cap) {
    ScratchDir dir("lowmem-pair-cap");
    constexpr uint32_t MOVED = 99;
    BuildDir old_d, new_d;
    put_reversed_poi_names(old_d, new_d, MOVED + 1);
    struct Cap {
        uint32_t max, pairs, fixups;
    };
    for (const Cap cap : {Cap{MOVED, MOVED, 0}, Cap{MOVED - 1, 0, MOVED}}) {
        const LowMemCase c{dir.path(), "cap" + std::to_string(cap.max)};
        REQUIRE(prepare(c, old_d, new_d, {}, {}, {"--string-pairs-max", std::to_string(cap.max)}).empty());
        CHECK_EQ(patch_holds(c, poi_merge_head(MOVED + 1, MOVED + 1, cap.fixups), "POI merge"), std::string());
        // The patcher's log: the strings section's pairs, and the POI merge's
        // fixups when it has any.
        const LogCheck sent = [cap](const std::string& log) -> std::string {
            const std::string pairs = ", " + std::to_string(cap.pairs) + " moved-string pairs\n";
            const std::string fixups = ", " + std::to_string(cap.fixups) + " string fixups)";
            if (log.find(pairs) == std::string::npos) return "no \"" + pairs + "\"";
            if ((log.find(" string fixups") != std::string::npos) != (cap.fixups > 0)) return "string fixups: " + log;
            if (cap.fixups > 0 && log.find(fixups) == std::string::npos) return "no \"" + fixups + "\"";
            return "";
        };

        CHECK(apply_modes(c, ALL_MODES, sent).bad.empty());
    }
}

// The POI string tier in record order (GCPATCH v9): the new pool moves a name
// out of sorted order (zoo cafe bank -> zoo bank cafe tea), so the tier's
// diff sends new indices and a moved-string pair, and the encoded POI records
// take the remapped names as the plain patcher's do.
TEST(patch_lowmem_rebuilds_a_string_tier_in_any_order) {
    ScratchDir dir("lowmem-strings");
    std::mt19937 rng(20261027);
    const std::string old_pool("zoo\0cafe\0bank\0", 14), new_pool("zoo\0bank\0cafe\0tea\0", 18);
    // The records keep their names: zoo cafe bank, then a new one named tea.
    const std::vector<uint32_t> old_names = {0, 4, 9}, new_names = {0, 9, 4, 14};
    const auto pois = pois_of(rng, 4, 3, false);
    BuildDir old_d, new_d;
    put_pois(old_d, {pois.begin(), pois.begin() + 3}, old_names);
    put_pois(new_d, pois, new_names);
    old_d["strings_poi.bin"].assign(old_pool.begin(), old_pool.end());
    new_d["strings_poi.bin"].assign(new_pool.begin(), new_pool.end());
    const LowMemCase c{dir.path(), "strings"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    CHECK(apply_both_modes(c).empty());
    auto r = run_tool({GEOCODER_GC_BIN, "decode", c.dir("out.sync"), c.dir("out.dec")}, c.log());
    REQUIRE(r.ok);
    CHECK(read_dir(c.dir("out.dec")) == read_dir(c.dir("new")));
}

// A new set without an encoded family's files (the POI family, both files
// gone) gets a gc.manifest naming only the families it ships, as gc's
// encoding of the new dir does, and decodes to the new dir.
TEST(patch_lowmem_new_manifest_names_only_the_families_the_patch_ships) {
    ScratchDir dir("lowmem-family");
    std::mt19937 rng(20261027);
    std::vector<GeoRow> rows;
    for (uint32_t k = 0; k < 50; k++) rows.push_back({k, {k % 10}});
    BuildDir old_d, new_d;
    put_geo(old_d, rows);
    put_pois(old_d, pois_of(rng, 40, 0, true));
    put_geo(new_d, rows);
    const LowMemCase c{dir.path(), "removed-family"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    CHECK(apply_both_modes(c).empty());
    auto r = run_tool({GEOCODER_GC_BIN, "decode", c.dir("out.sync"), c.dir("out.dec")}, c.log());
    CHECK(r.ok);
    CHECK(r.ok && read_dir(c.dir("out.dec")) == new_d);
}

// A new set with part of an encoded family (one of the POI family's two
// files) is refused before the replay.
TEST(patch_lowmem_refuses_a_new_set_with_part_of_a_family) {
    ScratchDir dir("lowmem-part");
    std::mt19937 rng(20261027);
    std::vector<GeoRow> rows;
    for (uint32_t k = 0; k < 50; k++) rows.push_back({k, {k % 10}});
    BuildDir old_d;
    put_geo(old_d, rows);
    put_pois(old_d, pois_of(rng, 40, 0, true));
    for (const std::string gone : {"poi_vertices.bin", "poi_records.bin"}) {
        BuildDir new_d = old_d;
        new_d.erase(gone);
        const LowMemCase c{dir.path(), "without-" + gone};
        REQUIRE(prepare(c, old_d, new_d).empty());

        CHECK(not_refused(c, ENCODED_MODES, c.patch(), "part", "family poi: the patch ships 1 of its 2 files").empty());
    }
}

// A MATCH run longer than what is left of the old file is refused in the
// low-memory mode too: the last run of POI records (replayed with their
// child), 5 longer, as the plain replay refuses it; of their vertices (the
// child stream, which the records size), as bytes past the last record.
TEST(patch_lowmem_refuses_a_match_run_past_the_old_file) {
    ScratchDir dir("lowmem-match");
    std::mt19937 rng(20261027);
    const auto pois = pois_of(rng, 300, 3, false);
    std::vector<Poi> dropped = pois;
    dropped.erase(dropped.begin() + 100);
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, dropped);
    const LowMemCase c{dir.path(), "match"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    struct Edit {
        std::string name;
        uint32_t kept, gone, rest;
        std::string refusal;
    };
    for (const Edit& e : {Edit{"poi_records.bin", 100, 1, 199, "Malformed merge sequence: poi_records.bin (a MATCH run past the old file)"},
                          Edit{"poi_vertices.bin", 2200, 22, 4378, "poi_vertices.bin has bytes past its last record"}}) {
        std::vector<char> runs;
        for (const auto& [op, count] : {std::pair<uint8_t, uint32_t>{OP_MATCH_RUN, e.kept}, {OP_DELETE_RUN, e.gone}, {OP_MATCH_RUN, e.rest}}) {
            append(runs, op);
            append(runs, count);
        }
        const size_t at = find_once(raw, runs);
        REQUIRE(at != std::string::npos);
        std::vector<char> longer = raw;
        put_at(longer, at + runs.size() - 4, e.rest + 5);
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, longer));

        CHECK(not_refused(c, ENCODED_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_both_modes(c).empty());
}

// A merge of an encoded family's parent at a stride other than its records'
// is refused before the replay, in sync and --threads: addresses at 24 bytes
// (the string and vertex_offset fields fit, the vertex_count at 24 that sizes
// a record's child bytes does not; with the first footprint deleted, the
// first record replayed moves its vertex_offset, so it is rewritten in a
// 24-byte copy that sizing its child bytes reads past) and POI records at 32.
// The patch as made applies.
TEST(patch_lowmem_refuses_a_parent_stride_other_than_its_records) {
    ScratchDir dir("lowmem-stride");
    std::mt19937 rng(20261027);
    std::vector<Addr> addrs;
    for (uint32_t i = 0; i < 300; i++)
        addrs.push_back({47.0f + static_cast<float>(i) * 1e-4f, i % 3 == 2 ? std::vector<char>{} : block(rng, 3 + rng() % 8)});
    std::vector<Addr> fewer_addrs(addrs.begin() + 1, addrs.end());
    const auto pois = pois_of(rng, 300, 3, false);
    std::vector<Poi> fewer_pois = pois;
    fewer_pois.erase(fewer_pois.begin() + 100);
    std::vector<GeoRow> rows;
    for (uint32_t k = 0; k < 50; k++) rows.push_back({k, {k % 10}});
    BuildDir old_d, new_d;
    put_geo(old_d, rows);
    put_geo(new_d, rows);
    put_addrs(old_d, addrs);
    put_addrs(new_d, fewer_addrs);
    put_pois(old_d, pois);
    put_pois(new_d, fewer_pois);
    const LowMemCase c{dir.path(), "stride"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    struct Edit {
        PatchFileId fid;
        uint32_t stride, records, wrong;
    };
    for (const Edit& e : {Edit{PatchFileId::ADDR_POINTS, ADDR_POINT_STRIDE, 300, 24}, Edit{PatchFileId::POI_RECORDS, POI_RECORD_STRIDE, 300, 32}}) {
        const std::string name = patch_file_names[static_cast<uint32_t>(e.fid)];
        std::vector<char> header;
        append(header, static_cast<uint32_t>(e.fid));
        append(header, e.stride);
        append(header, uint64_t{e.records} * e.stride);
        append(header, uint64_t{e.records - 1} * e.stride);
        const size_t at = find_once(raw, header);
        REQUIRE(at != std::string::npos);
        std::vector<char> edited = raw;
        put_at(edited, at + 4, e.wrong);
        const std::string patch = c.dir(name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ENCODED_MODES, patch, name, "Malformed record stride: " + name).empty());
    }
    CHECK(apply_both_modes(c).empty());
}

// The street ways' merge (MATCH, REPLACE and INSERT runs, replayed with their
// nodes) with a run's count 2^30 past the patch's, or the merge sequence's or
// the nodes section's length 2^32 past it, is refused, in sync and --threads.
// A 32-bit size_t wraps 12 x 2^30 and 2^32 to nothing: the lengths read as the
// patch's, and a run sized as the patch's replayed its records one by one past
// it.
TEST(patch_lowmem_refuses_counts_and_lengths_that_wrap_32_bits) {
    ScratchDir dir("lowmem-wrap");
    std::vector<uint32_t> names(30), renamed(31);
    for (uint32_t i = 0; i < 31; i++) {
        if (i < 30) names[i] = i;
        renamed[i] = i == 10 ? 1000 : i;
    }
    BuildDir old_d, new_d, old_meta, new_meta;
    put_named_ways(old_d, old_meta, names);
    put_named_ways(new_d, new_meta, renamed);
    const LowMemCase c{dir.path(), "wrap"};
    REQUIRE(prepare(c, old_d, new_d, old_meta, new_meta).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    std::vector<char> header;
    append(header, static_cast<uint32_t>(PatchFileId::STREET_WAYS));
    append(header, static_cast<uint32_t>(WAY_HEADER_STRIDE));
    append(header, uint64_t{30 * WAY_HEADER_STRIDE});
    append(header, uint64_t{31 * WAY_HEADER_STRIDE});
    const size_t at = find_once(raw, header);
    REQUIRE(at != std::string::npos);
    auto u32_at = [&](size_t p) { uint32_t v; memcpy(&v, raw.data() + p, 4); return v; };
    auto u64_at = [&](size_t p) { uint64_t v; memcpy(&v, raw.data() + p, 8); return v; };
    // A merge section's sequence length: after its offset fixups' counts and
    // sizes, the fixups, its string fixups' count and size, and those.
    auto seq_len_at_of = [&](size_t section) {
        const size_t string_fixups = section + 40 + u32_at(section + 32) + u32_at(section + 36);
        return string_fixups + 8 + u32_at(string_fixups + 4);
    };
    // The merge: its sequence's length, its ops.
    const size_t seq_len_at = seq_len_at_of(at), seq = seq_len_at + 8;
    const size_t seq_end = seq + u64_at(seq_len_at);
    std::map<uint8_t, size_t> count_at;  // each kind's first run: where its count is
    for (size_t p = seq; p < seq_end;) {
        const uint8_t op = static_cast<uint8_t>(raw[p]);
        count_at.emplace(op, p + 1);
        p += 5 + (op == OP_INSERT_RUN || op == OP_REPLACE_RUN ? u32_at(p + 1) * WAY_HEADER_STRIDE : 0);
    }
    REQUIRE(count_at.count(OP_MATCH_RUN) && count_at.count(OP_REPLACE_RUN) && count_at.count(OP_INSERT_RUN));
    // The nodes section right after: a merge's sequence length, or a full
    // replacement's (or a copy's) data length.
    REQUIRE(u32_at(seq_end) == static_cast<uint32_t>(PatchFileId::STREET_NODES));
    const uint32_t child_stride = u32_at(seq_end + 4);
    const size_t child_len_at = child_stride == 0 || child_stride == COPY_OLD_STRIDE ? seq_end + 28 : seq_len_at_of(seq_end);
    struct Edit {
        std::string name;
        size_t at;
        uint64_t add;
        bool wide;
        std::string refusal;
    };
    const Edit edits[] = {
        {"match", count_at[OP_MATCH_RUN], uint64_t(1) << 30, false, "Malformed merge sequence: street_ways.bin (a MATCH run past the old file)"},
        {"replace", count_at[OP_REPLACE_RUN], uint64_t(1) << 30, false, "Malformed merge sequence: street_ways.bin"},
        {"insert", count_at[OP_INSERT_RUN], uint64_t(1) << 30, false, "Malformed merge sequence: street_ways.bin"},
        {"sequence", seq_len_at, uint64_t(1) << 32, true, "bytes for merge sequence"},
        {"nodes", child_len_at, uint64_t(1) << 32, true, "Truncated patch: child section"},
    };
    for (const Edit& e : edits) {
        std::vector<char> edited = raw;
        if (e.wide) put_at(edited, e.at, u64_at(e.at) + e.add);
        else put_at(edited, e.at, static_cast<uint32_t>(u32_at(e.at) + e.add));
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ENCODED_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_both_modes(c).empty());
}

// A parent and its child stream replayed out of step are refused in sync and
// --threads once the shared decoder holds 4 MiB (MAX_DECODER_SKEW) of the
// file left behind. 150,000 POIs with the same 8-vertex block, and the two
// POI sections rebuilt by hand, both giving the new set (new = old): the
// child runs ahead when every record is inserted before the old ones are
// deleted while its block is matched from the old ones (the decoder queues
// each old record it passes for the parent's reader: 5.4 MB of them), and
// the parent runs ahead when the records are matched while their blocks are
// inserted but for the last, matched from the first old block (it queues
// each old block for the child's reader: 6.3 MB). The patch as made applies.
TEST(patch_lowmem_refuses_a_parent_and_child_replayed_out_of_step) {
    ScratchDir dir("lowmem-skew");
    std::mt19937 rng(20261030);
    const std::vector<char> shared = block(rng, 8);
    std::vector<Poi> pois;
    for (uint32_t i = 0; i < 150000; i++) pois.push_back({i, shared});
    BuildDir d;
    put_pois(d, pois);
    const LowMemCase c{dir.path(), "skew"};
    REQUIRE(prepare(c, d, d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    const std::vector<char>& records = d.at("poi_records.bin");
    const std::vector<char>& vertices = d.at("poi_vertices.bin");
    const uint32_t n = static_cast<uint32_t>(pois.size()), v = static_cast<uint32_t>(vertices.size());
    const uint32_t b = static_cast<uint32_t>(shared.size());
    // The two sections as the diff sends them: copies of the old files.
    std::vector<char> copies;
    for (const auto& [fid, size] : {std::pair<PatchFileId, uint64_t>{PatchFileId::POI_RECORDS, records.size()},
                                    {PatchFileId::POI_VERTICES, vertices.size()}}) {
        append(copies, static_cast<uint32_t>(fid));
        append(copies, COPY_OLD_STRIDE);
        append(copies, size);
        append(copies, size);
        append(copies, uint32_t{0});
        append(copies, uint64_t{0});
    }
    const size_t at = find_once(raw, copies);
    REQUIRE(at != std::string::npos);
    // An op of a merge sequence, with its records (count of them at stride) when it carries some.
    auto op = [](std::vector<char>& ops, uint8_t kind, uint32_t count, const char* data = nullptr, size_t stride = 0) {
        append(ops, kind);
        append(ops, count);
        if (data) ops.insert(ops.end(), data, data + size_t{count} * stride);
    };
    // The merge section of a file of `size` bytes on both days, without offset or string fixups.
    auto merge = [](PatchFileId fid, uint32_t stride, uint64_t size, const std::vector<char>& ops) {
        std::vector<char> s;
        append(s, static_cast<uint32_t>(fid));
        append(s, stride);
        append(s, size);
        append(s, size);
        for (int k = 0; k < 6; k++) append(s, uint32_t{0});
        append(s, static_cast<uint64_t>(ops.size()));
        s.insert(s.end(), ops.begin(), ops.end());
        return s;
    };
    struct Case {
        std::string name;
        std::vector<char> parent_ops, child_ops;
        std::string refusal;
    };
    Case child_ahead{"child-ahead", {}, {}, "Malformed merge sequence: poi_vertices.bin runs ahead of poi_records.bin"};
    op(child_ahead.parent_ops, OP_INSERT_RUN, n, records.data(), POI_RECORD_STRIDE);
    op(child_ahead.parent_ops, OP_DELETE_RUN, n);
    op(child_ahead.child_ops, OP_MATCH_RUN, v);
    Case parent_ahead{"parent-ahead", {}, {}, "Malformed merge sequence: poi_records.bin runs ahead of poi_vertices.bin"};
    op(parent_ahead.parent_ops, OP_MATCH_RUN, n);
    op(parent_ahead.child_ops, OP_INSERT_RUN, v - b, vertices.data(), 1);
    op(parent_ahead.child_ops, OP_MATCH_RUN, b);
    for (const Case& e : {child_ahead, parent_ahead}) {
        std::vector<char> edited(raw.begin(), raw.begin() + at);
        for (const auto& section : {merge(PatchFileId::POI_RECORDS, POI_RECORD_STRIDE, records.size(), e.parent_ops),
                                    merge(PatchFileId::POI_VERTICES, 1, vertices.size(), e.child_ops)})
            edited.insert(edited.end(), section.begin(), section.end());
        edited.insert(edited.end(), raw.begin() + at + copies.size(), raw.end());
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ENCODED_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_both_modes(c).empty());
}

// Counts and lengths the patch gives, each made to name 2^32 bytes more than
// it holds, are refused in every mode: a parallel delta's inserted values and
// changes (way_parents along the street ways' merge), a sparse delta's
// changes (admin_parents), and the lengths of a full replacement
// (postcode_centroids.bin, new) and of an unchanged file's empty payload
// (place_nodes.bin). A 32-bit size_t wrapped each back to what the patch
// holds: the deltas read the bytes after their changes as more of them, and
// the lengths applied as the patch's. The patch as made applies.
TEST(patch_refuses_change_counts_and_lengths_that_wrap_32_bits) {
    ScratchDir dir("patch-wrap");
    std::vector<uint32_t> names(30), renamed(31);
    for (uint32_t i = 0; i < 31; i++) {
        if (i < 30) names[i] = i;
        renamed[i] = i == 10 ? 1000 : i;
    }
    BuildDir old_d, new_d, old_meta, new_meta;
    put_named_ways(old_d, old_meta, names);
    put_named_ways(new_d, new_meta, renamed);
    std::vector<char> old_parents, new_parents, old_admin_parents, new_admin_parents, centroids, places;
    for (uint32_t i = 0; i < 31; i++) {
        if (i < 30) append(old_parents, 100 + i);
        append(new_parents, i == 20 ? 777u : 100 + i);
    }
    for (uint32_t i = 0; i < 64; i++) {
        append(old_admin_parents, i);
        append(new_admin_parents, i % 16 == 5 ? i + 1000 : i);
    }
    for (uint32_t i = 0; i < 4; i++)
        for (const uint32_t w : {i, i, NO, 0u}) append(centroids, w);
    for (uint32_t i = 0; i < 5; i++)
        for (const uint32_t w : {i, i, NO, NO, NO}) append(places, w);
    old_d["way_parents.bin"] = old_parents;
    new_d["way_parents.bin"] = new_parents;
    old_d["admin_parents.bin"] = old_admin_parents;
    new_d["admin_parents.bin"] = new_admin_parents;
    new_d["postcode_centroids.bin"] = centroids;
    old_d["place_nodes.bin"] = new_d["place_nodes.bin"] = places;
    const LowMemCase c{dir.path(), "wrap32"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    auto u32_at = [&](size_t p) { uint32_t x; memcpy(&x, raw.data() + p, 4); return x; };
    auto u64_at = [&](size_t p) { uint64_t x; memcpy(&x, raw.data() + p, 8); return x; };
    // A section's head (file id, stride, old and new sizes, then words): where it starts.
    auto at_of = [&](PatchFileId fid, uint32_t stride, uint64_t old_size, uint64_t new_size, std::initializer_list<uint32_t> words) {
        std::vector<char> head;
        append(head, static_cast<uint32_t>(fid));
        append(head, stride);
        append(head, old_size);
        append(head, new_size);
        for (const uint32_t w : words) append(head, w);
        const size_t at = find_once(raw, head);
        REQUIRE(at != std::string::npos);
        return at;
    };
    // Parallel: parent file, value stride, remap kind, then n_inserted, the values, n_changes.
    const size_t parallel = at_of(PatchFileId::WAY_PARENTS, PARALLEL_DELTA_STRIDE, 30 * 4, 31 * 4,
                                  {static_cast<uint32_t>(PatchFileId::STREET_WAYS), 4, 0});
    const size_t inserted_at = parallel + 36, parallel_changes_at = inserted_at + 4 + 4 * size_t{u32_at(inserted_at)};
    // Sparse: value stride, remap kind, then n_changes.
    const size_t sparse_changes_at = at_of(PatchFileId::ADMIN_PARENTS, SPARSE_DELTA_STRIDE, 64 * 4, 64 * 4, {4, 0}) + 32;
    // Full replacement and copy: a u32 (no fixups), then the data's length.
    const size_t full_length_at = at_of(PatchFileId::POSTCODE_CENTROIDS, 0, 0, centroids.size(), {0}) + 28;
    const size_t copy_length_at = at_of(PatchFileId::PLACE_NODES, COPY_OLD_STRIDE, places.size(), places.size(), {0}) + 28;
    REQUIRE(u32_at(inserted_at) > 0 && u32_at(parallel_changes_at) > 0 && u32_at(sparse_changes_at) > 0);
    REQUIRE(u64_at(full_length_at) == centroids.size() && u64_at(copy_length_at) == 0);
    struct Edit {
        std::string name;
        size_t at;
        uint64_t add;
        bool wide;
        std::string refusal;
    };
    const Edit edits[] = {
        {"parallel-values", inserted_at, uint64_t(1) << 30, false, "bytes for parallel values"},
        {"parallel-changes", parallel_changes_at, uint64_t(1) << 29, false, "bytes for parallel changes"},
        {"sparse-changes", sparse_changes_at, uint64_t(1) << 29, false, "bytes for sparse changes"},
        {"full-replacement", full_length_at, uint64_t(1) << 32, true, "bytes for full replacement"},
        {"copy", copy_length_at, uint64_t(1) << 32, true, "bytes for copy-old payload"},
    };
    for (const Edit& e : edits) {
        std::vector<char> edited = raw;
        if (e.wide) put_at(edited, e.at, u64_at(e.at) + e.add);
        else put_at(edited, e.at, static_cast<uint32_t>(u32_at(e.at) + e.add));
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A merge section whose stride is past any record's (0x100: from 0xFA up,
// strides name section kinds) is refused in every mode before a record
// buffer is sized by it: the place nodes' merge, 20 bytes a record as made
// (a plain file in every mode). The patch as made applies.
TEST(patch_refuses_a_record_stride_past_any_records) {
    ScratchDir dir("patch-stride");
    std::vector<char> places;
    for (uint32_t i = 0; i < 50; i++)
        for (const uint32_t w : {i, i, NO, 1u, NO}) append(places, w);
    std::vector<char> fewer = places;
    fewer.erase(fewer.begin() + 20 * PLACE_NODE_STRIDE, fewer.begin() + 21 * PLACE_NODE_STRIDE);
    BuildDir old_d, new_d;
    old_d["place_nodes.bin"] = places;
    new_d["place_nodes.bin"] = fewer;
    const LowMemCase c{dir.path(), "stride"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    std::vector<char> header;
    append(header, static_cast<uint32_t>(PatchFileId::PLACE_NODES));
    append(header, static_cast<uint32_t>(PLACE_NODE_STRIDE));
    append(header, uint64_t{50 * PLACE_NODE_STRIDE});
    append(header, uint64_t{49 * PLACE_NODE_STRIDE});
    const size_t at = find_once(raw, header);
    REQUIRE(at != std::string::npos);
    std::vector<char> edited = raw;
    put_at(edited, at + 4, uint32_t{0x100});
    const std::string patch = c.dir("wide.gcpatch");
    REQUIRE(write_patch(c, patch, edited));

    CHECK(not_refused(c, ALL_MODES, patch, "wide", "Malformed record stride: place_nodes.bin").empty());
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A cell index delta whose cell loses, or gains, more ids than a list holds
// (0xFFFF, its u16 count) is refused in every mode before the ids are read:
// the POI index's first changed cell, with 65,536 ids the cell doesn't hold
// added to the ids it loses (the replay took them, dropping none) or with as
// many new ones added to those it gains. The patch as made applies.
TEST(patch_refuses_a_cell_losing_or_gaining_more_ids_than_a_list_holds) {
    ScratchDir dir("patch-cell-ids");
    std::mt19937 rng(20261030);
    CellLists old_cells, new_cells;
    for (uint32_t k = 0; k < 20; k++) {
        old_cells[cell_id(k)] = {k % 7};
        new_cells[cell_id(k)] = {k % 7 + 1};
    }
    const auto pois = pois_of(rng, 40, 0, true);
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, pois);
    put_cells(old_d, "poi", old_cells);
    put_cells(new_d, "poi", new_cells);
    const LowMemCase c{dir.path(), "cell-ids"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    std::vector<char> head;
    append(head, CELL_INDEX_DELTA_MARKER);
    append(head, static_cast<uint32_t>(PatchFileId::POI_ENTRIES));
    const size_t at = find_once(raw, head);
    REQUIRE(at != std::string::npos);
    // marker, file id, payload size; the payload: n_removed, removed cells,
    // n_set, then the first cell: id, n_lost, n_gained, lost ids, gained ids.
    uint32_t n_removed, n_lost;
    memcpy(&n_removed, raw.data() + at + 16, 4);
    const size_t first = at + 24 + 8 * size_t{n_removed};
    uint32_t n_gained;
    memcpy(&n_lost, raw.data() + first + 8, 4);
    memcpy(&n_gained, raw.data() + first + 12, 4);
    REQUIRE(n_lost == 1 && n_gained == 1);
    for (const bool gained : {false, true}) {
        const std::string name = gained ? "gained" : "lost";
        std::vector<char> ids;
        for (uint32_t k = 0; k < 0x10000; k++) append(ids, 0x40000000u + k);
        std::vector<char> edited = raw;
        uint64_t payload;
        uint32_t count;
        memcpy(&payload, edited.data() + at + 8, 8);
        put_at(edited, at + 8, payload + ids.size());
        const size_t count_at = first + (gained ? 12 : 8);
        memcpy(&count, edited.data() + count_at, 4);
        put_at(edited, count_at, count + 0x10000);
        // After the cell's ids it loses (one), or those it gains too (one).
        edited.insert(edited.begin() + static_cast<std::ptrdiff_t>(first + 16 + 4 + (gained ? 4 : 0)), ids.begin(), ids.end());
        const std::string patch = c.dir(name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ALL_MODES, patch, name, "Malformed cell list delta").empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A client file list of more files than a variant holds (1025, past
// MAX_CLIENT_FILES), with a name longer than a file's (256 bytes), or with
// a file listed twice, is refused in every mode before the patcher writes
// anything: the files added are empty .bin files sorted after the others,
// which the patcher made as listed. The patch as made applies.
TEST(patch_refuses_too_many_client_files_a_long_name_or_one_listed_twice) {
    ScratchDir dir("patch-client-files");
    std::vector<char> parents, changed;
    for (uint32_t v = 0; v < 4096; v++) {
        append(parents, v);
        append(changed, v + (v == 2000));
    }
    const LowMemCase c{dir.path(), "client-files"};
    REQUIRE(prepare(c, {{"way_parents.bin", parents}, {"meta.json", {'{', '}'}}},
                    {{"way_parents.bin", changed}, {"meta.json", {'{', ' ', '}'}}}).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    // After the header (magic, version, flags): marker, n, then each entry
    // (name length, name, size, inline flag, an inline file's bytes).
    uint32_t marker, n;
    memcpy(&marker, raw.data() + 16, 4);
    memcpy(&n, raw.data() + 20, 4);
    REQUIRE(marker == CLIENT_FILES_MARKER);
    std::vector<size_t> entry_at = {24};
    for (uint32_t i = 0; i < n; i++) {
        const size_t e = entry_at.back();
        uint16_t len;
        uint64_t size;
        memcpy(&len, raw.data() + e, 2);
        memcpy(&size, raw.data() + e + 2 + len, 8);
        entry_at.push_back(e + 2 + len + 9 + (raw[e + 2 + len + 8] ? size : 0));
    }
    // An empty .bin file's entry.
    auto empty_bin = [](const std::string& name) {
        std::vector<char> e;
        append(e, static_cast<uint16_t>(name.size()));
        e.insert(e.end(), name.begin(), name.end());
        append(e, uint64_t{0});
        append(e, uint8_t{0});
        return e;
    };
    // The list with `more` entries inserted at entry i.
    auto with = [&](size_t i, const std::vector<char>& more, uint32_t added) {
        std::vector<char> p = raw;
        put_at(p, 20, n + added);
        p.insert(p.begin() + static_cast<std::ptrdiff_t>(entry_at[i]), more.begin(), more.end());
        return p;
    };
    std::vector<char> many;
    for (uint32_t i = n; i < 1025; i++) {
        char name[16];
        std::snprintf(name, sizeof name, "zz%04u.bin", i);
        const std::vector<char> e = empty_bin(name);
        many.insert(many.end(), e.begin(), e.end());
    }
    const std::vector<char> first(raw.begin() + static_cast<std::ptrdiff_t>(entry_at[0]),
                                  raw.begin() + static_cast<std::ptrdiff_t>(entry_at[1]));
    struct Case {
        std::string name;
        std::vector<char> patch;
        std::string refusal;
    };
    const Case cases[] = {
        {"many", with(n, many, 1025 - n), "Too many client files: 1025"},
        {"long-name", with(n, empty_bin(std::string(252, 'z') + ".bin"), 1), "Client file name too long: 256 bytes"},
        {"twice", with(1, first, 1), "Client file listed out of order or twice: meta.json"},
    };
    for (const Case& e : cases) {
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, e.patch));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A patch with the cell index delta of an admin or POI index cut out is
// refused in every mode, whether the old and the new dir hold the index
// (admin), only the new one, which the patch lists, or only the old one (POI):
// without it the patcher would rebuild the index uncorrected. Each patch as
// made applies.
TEST(patch_refuses_a_patch_without_the_delta_of_a_cell_index) {
    ScratchDir dir("patch-no-cell-index-delta");
    std::mt19937 rng(20261031);
    CellLists old_cells, new_cells;
    for (uint32_t k = 0; k < 20; k++) {
        old_cells[cell_id(k)] = {k % 7};
        new_cells[cell_id(k)] = {k % 7 + 1};
    }
    const auto pois = pois_of(rng, 40, 0, true);
    struct Case {
        std::string name, prefix;
        PatchFileId entries;
        bool in_old, in_new;
    };
    const Case cases[] = {
        {"admin", "admin", PatchFileId::ADMIN_ENTRIES, true, true},
        {"poi-new", "poi", PatchFileId::POI_ENTRIES, false, true},
        {"poi-old", "poi", PatchFileId::POI_ENTRIES, true, false},
    };
    for (const Case& e : cases) {
        BuildDir old_d, new_d;
        put_pois(old_d, pois);
        put_pois(new_d, pois);
        if (e.in_old) put_cells(old_d, e.prefix, old_cells);
        if (e.in_new) put_cells(new_d, e.prefix, new_cells);
        const LowMemCase c{dir.path(), e.name};
        REQUIRE(prepare(c, old_d, new_d).empty());
        const std::vector<char> raw = raw_patch(c);
        REQUIRE(!raw.empty());
        std::vector<char> head;
        append(head, CELL_INDEX_DELTA_MARKER);
        append(head, static_cast<uint32_t>(e.entries));
        const size_t at = find_once(raw, head);
        REQUIRE(at != std::string::npos);
        // marker, file id, payload size, payload
        uint64_t payload;
        memcpy(&payload, raw.data() + at + 8, 8);
        std::vector<char> cut = raw;
        cut.erase(cut.begin() + static_cast<std::ptrdiff_t>(at), cut.begin() + static_cast<std::ptrdiff_t>(at + 16 + payload));
        const std::string patch = c.dir("cut.gcpatch");
        REQUIRE(write_patch(c, patch, cut));

        CHECK(not_refused(c, ALL_MODES, patch, "cut", "Patch has no cell index delta for " + e.prefix).empty());
        CHECK(apply_modes(c, ALL_MODES).bad.empty());
    }
}

// The first patch after datasets stopped shipping a place index: from an old
// dir that holds place_cells.bin and place_entries.bin to a new dir of the same
// place nodes, one of them changed, without them. The patch reads neither, so
// it applies, in every mode, to the old dir as it is and to the old dir
// without them.
TEST(patch_to_a_dir_without_a_place_index_applies_to_an_old_dir_with_or_without_one) {
    ScratchDir dir("patch-place-index-gone");
    std::vector<char> old_places, new_places;
    for (uint32_t i = 0; i < 40; i++) {
        for (const uint32_t w : {i, i, NO, NO, NO}) append(old_places, w);
        for (const uint32_t w : {i, i == 7 ? 99 : i, NO, NO, NO}) append(new_places, w);
    }
    CellLists place_lists;
    for (uint32_t k = 0; k < 20; k++) place_lists[cell_id(k)] = {k, k + 20};
    BuildDir with_index, without_index, new_d;
    with_index["place_nodes.bin"] = without_index["place_nodes.bin"] = old_places;
    put_cells(with_index, "place", place_lists);
    new_d["place_nodes.bin"] = new_places;
    const LowMemCase made{dir.path(), "with-index"}, stripped{dir.path(), "without-index"};
    REQUIRE(prepare(made, with_index, new_d).empty());
    REQUIRE(prepare(stripped, without_index, new_d).empty());
    std::filesystem::copy_file(made.patch(), stripped.patch(), std::filesystem::copy_options::overwrite_existing);

    CHECK(apply_modes(made, ALL_MODES).bad.empty());
    CHECK(apply_modes(stripped, ALL_MODES).bad.empty());
}

// 0xFFFFFFF4 marked the place cell changes until v13: now it is no section's
// marker, and the patcher refuses it as a file it doesn't know, in every mode.
// The patch as made applies.
TEST(patch_refuses_the_old_place_cell_changes_marker_as_an_unknown_file) {
    ScratchDir dir("patch-place-cell-changes");
    std::mt19937 rng(20261102);
    CellLists old_cells, new_cells;
    for (uint32_t k = 0; k < 30; k++) old_cells[cell_id(k)] = {k % 7};
    for (uint32_t k = 0; k < 20; k++) new_cells[cell_id(k)] = {k % 7 + 1};
    for (uint32_t k = 40; k < 50; k++) new_cells[cell_id(k)] = {2};
    const auto pois = pois_of(rng, 40, 0, true);
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, pois);
    put_cells(old_d, "poi", old_cells);
    put_cells(new_d, "poi", new_cells);
    const LowMemCase c{dir.path(), "place-cells"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    std::vector<char> raw = raw_patch(c), poi_changes, place_changes;
    REQUIRE(!raw.empty());
    for (const uint32_t w : {CELL_CHANGES_POI_MARKER, 10u, 10u}) append(poi_changes, w);
    // As the diff wrote one for a region without place nodes: no cell added or removed.
    for (const uint32_t w : {0xFFFFFFF4u, 0u, 0u}) append(place_changes, w);
    const size_t at = find_once(raw, poi_changes);
    REQUIRE(at != std::string::npos);
    raw.insert(raw.begin() + static_cast<std::ptrdiff_t>(at), place_changes.begin(), place_changes.end());
    const std::string patch = c.dir("place-cell-changes.gcpatch");
    REQUIRE(write_patch(c, patch, raw));

    CHECK(not_refused(c, ALL_MODES, patch, "place-cell-changes", "Unknown file 4294967284").empty());
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// Cell changes out of order (two ids of one list swapped: the geo index's
// added or removed cells, the POI index's), and a geo entry delta or a cell
// index delta for a file that has one already or for a file not of its kind,
// are refused in every mode. The patch as made applies.
TEST(patch_refuses_cell_changes_out_of_order_and_a_second_or_foreign_delta) {
    ScratchDir dir("patch-cells");
    std::mt19937 rng(20261028);
    // Geo: cells 0-39 kept with their street lists changed, 40-49 removed,
    // 60-69 added. POI: cells 0-19 kept with their lists changed, 20-29
    // removed, 40-49 added.
    std::vector<GeoRow> old_rows, new_rows;
    for (uint32_t k = 0; k < 50; k++) old_rows.push_back({k, {k % 10}});
    for (uint32_t k = 0; k < 40; k++) new_rows.push_back({k, {(k + 1) % 10}});
    for (uint32_t k = 60; k < 70; k++) new_rows.push_back({k, {3}});
    CellLists old_cells, new_cells;
    for (uint32_t k = 0; k < 30; k++) old_cells[cell_id(k)] = {k % 7};
    for (uint32_t k = 0; k < 20; k++) new_cells[cell_id(k)] = {k % 7 + 1};
    for (uint32_t k = 40; k < 50; k++) new_cells[cell_id(k)] = {2};
    const auto pois = pois_of(rng, 40, 0, true);
    BuildDir old_d, new_d;
    put_geo(old_d, old_rows);
    put_geo(new_d, new_rows);
    put_pois(old_d, pois);
    put_pois(new_d, pois);
    put_cells(old_d, "poi", old_cells);
    put_cells(new_d, "poi", new_cells);
    const LowMemCase c{dir.path(), "cells"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    // A section's first u32s, as the patch holds them.
    auto head = [](std::initializer_list<uint32_t> words) {
        std::vector<char> h;
        for (const uint32_t w : words) append(h, w);
        return h;
    };
    auto at_of = [&](const std::vector<char>& h) {
        const size_t at = find_once(raw, h);
        REQUIRE(at != std::string::npos);
        return at;
    };
    const uint32_t street = static_cast<uint32_t>(PatchFileId::STREET_ENTRIES), poi = static_cast<uint32_t>(PatchFileId::POI_ENTRIES);
    // Where each list's ids start (after marker, n_added, n_removed), and
    // where the street entries' geo entry delta and the POI cell index delta
    // start.
    const size_t geo_added = at_of(head({CELL_CHANGES_GEO_MARKER, 10, 10})) + 12, geo_removed = geo_added + 10 * 8;
    const size_t poi_added = at_of(head({CELL_CHANGES_POI_MARKER, 10, 10})) + 12, poi_removed = poi_added + 10 * 8;
    const size_t street_delta = at_of(head({GEO_ENTRY_DELTA_MARKER, street}));
    const size_t poi_delta = at_of(head({CELL_INDEX_DELTA_MARKER, poi}));
    uint64_t poi_delta_size;
    memcpy(&poi_delta_size, raw.data() + poi_delta + 8, 8);
    const std::vector<char> poi_delta_section(raw.begin() + poi_delta, raw.begin() + poi_delta + 16 + poi_delta_size);

    using Edit = std::function<void(std::vector<char>&)>;
    // Swaps ids i and i + 1 of the list at ids.
    auto swap_ids = [](size_t ids, size_t i) -> Edit {
        return [at = ids + 8 * i](std::vector<char>& p) { std::swap_ranges(p.begin() + at, p.begin() + at + 8, p.begin() + at + 8); };
    };
    auto insert_at = [](size_t at, const std::vector<char>& section) -> Edit {
        return [at, section](std::vector<char>& p) { p.insert(p.begin() + at, section.begin(), section.end()); };
    };
    struct Case {
        std::string name;
        Edit edit;
        std::string refusal;
    };
    const Case cases[] = {
        {"geo-added", swap_ids(geo_added, 3), "Malformed cell changes"},
        {"geo-removed", swap_ids(geo_removed, 3), "Malformed cell changes"},
        {"poi-added", swap_ids(poi_added, 3), "Malformed cell changes"},
        {"poi-removed", swap_ids(poi_removed, 3), "Malformed cell changes"},
        // The last two: the walk asks for no cell past them, so the step back is among the ids it left.
        {"geo-removed-last", swap_ids(geo_removed, 8), "Malformed cell changes"},
        {"poi-removed-last", swap_ids(poi_removed, 8), "Malformed cell changes"},
        // Each before the real section: an empty one (no cells; a u64 size of 0), or its copy.
        {"second-geo-delta", insert_at(street_delta, head({GEO_ENTRY_DELTA_MARKER, street, 0})),
         "Malformed geo entry delta: a second one for street_entries.bin"},
        {"foreign-geo-delta", insert_at(street_delta, head({GEO_ENTRY_DELTA_MARKER, poi, 0})), "Malformed geo entry delta for file 17"},
        {"second-cell-index-delta", insert_at(poi_delta, poi_delta_section), "Malformed cell index delta: a second one for poi_entries.bin"},
        {"foreign-cell-index-delta", insert_at(poi_delta, head({CELL_INDEX_DELTA_MARKER, 999, 0, 0})), "Malformed cell index delta for file 999"},
    };
    for (const Case& e : cases) {
        std::vector<char> edited = raw;
        e.edit(edited);
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A file's secondary pairs out of order or with an old id twice, a second
// list of them, and street pairs in the secondary remap as well as in the
// parent-id remap's street leg are refused in every mode. The patch as made
// (20 secondary-matched POIs, 20 moved street ways with addresses) applies.
TEST(patch_refuses_remap_pairs_out_of_order_or_twice_and_street_pairs_in_both_remaps) {
    ScratchDir dir("patch-remap-pairs");
    BuildDir old_d, new_d, old_meta, new_meta;
    put_moved_ways(old_d, new_d, old_meta, new_meta, 20, 0);
    put_changed_pois(old_d, new_d, 20);
    const LowMemCase c{dir.path(), "pairs"};
    REQUIRE(prepare(c, old_d, new_d, old_meta, new_meta).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    std::vector<char> legs;
    for (const uint32_t w : {POI_PARENT_REMAP_MARKER, 20u}) append(legs, w);
    REQUIRE(find_once(raw, legs) != std::string::npos);
    const size_t head = find_once(raw, secondary_remap_head(20));
    REQUIRE(head != std::string::npos);
    // Where the secondary remap's file count, its street pair count and the
    // POIs' first pair sit, and where the section ends (after the POIs' pairs).
    const size_t n_files = head + 4, street_pairs = head + 12, poi_pairs = head + 40, end = poi_pairs + 20 * 8;

    using Edit = std::function<void(std::vector<char>&)>;
    struct Case {
        std::string name;
        Edit edit;
        std::string refusal;
    };
    const Case cases[] = {
        {"out-of-order", [&](std::vector<char>& p) {
             std::swap_ranges(p.begin() + poi_pairs, p.begin() + poi_pairs + 8, p.begin() + poi_pairs + 8);
         }, "Malformed secondary remap"},
        {"old-id-twice", [&](std::vector<char>& p) {
             std::copy(p.begin() + poi_pairs, p.begin() + poi_pairs + 4, p.begin() + poi_pairs + 8);
         }, "Malformed secondary remap"},
        {"second-list", [&](std::vector<char>& p) {
             put_at(p, n_files, 5u);
             std::vector<char> list;
             for (const uint32_t w : {static_cast<uint32_t>(PatchFileId::POI_RECORDS), 1u, 0u, 0u}) append(list, w);
             p.insert(p.begin() + end, list.begin(), list.end());
         }, "Malformed secondary remap: a second one for poi_records.bin"},
        {"street-pairs-in-both", [&](std::vector<char>& p) {
             put_at(p, street_pairs, 1u);
             std::vector<char> pair;
             for (const uint32_t w : {0u, 1u}) append(pair, w);
             p.insert(p.begin() + street_pairs + 4, pair.begin(), pair.end());
         }, "Malformed secondary remap: street_ways.bin's moved pairs came in the POI parent-id remap already"},
    };
    for (const Case& e : cases) {
        std::vector<char> edited = raw;
        e.edit(edited);
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A second list with pairs for a file sent unchanged, which no replay gives a
// remap to take them, is refused in every mode as one for a replayed file is:
// for POI records or address points, in the secondary remap or in a second
// one. So are street pairs in the secondary remap as well as in
// the parent-id remap's street leg when the street ways go unchanged. The
// patch as made (every file unchanged) applies.
TEST(patch_refuses_a_second_pair_list_or_street_pairs_in_both_remaps_for_unchanged_files) {
    ScratchDir dir("patch-unchanged-pairs");
    std::mt19937 rng(20261031);
    const auto pois = pois_of(rng, 20, 0, true);
    std::vector<Addr> addrs;
    for (uint32_t i = 0; i < 10; i++) addrs.push_back({47.0f + static_cast<float>(i) * 1e-4f, {}});
    BuildDir old_d, new_d;
    for (BuildDir* d : {&old_d, &new_d}) {
        put_geo(*d, {{0, {0}}}, 10);
        put_addrs(*d, addrs);
        put_pois(*d, pois);
    }
    const LowMemCase c{dir.path(), "unchanged"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    // u32s as the patch holds them.
    auto words = [](std::initializer_list<uint32_t> ws) {
        std::vector<char> b;
        for (const uint32_t w : ws) append(b, w);
        return b;
    };
    const uint32_t street = static_cast<uint32_t>(PatchFileId::STREET_WAYS), addr = static_cast<uint32_t>(PatchFileId::ADDR_POINTS),
                   poi = static_cast<uint32_t>(PatchFileId::POI_RECORDS);
    // Each file the edits give pairs for goes unchanged: its section's head
    // (file id, COPY_OLD_STRIDE, old and new sizes, no fixups).
    for (const uint32_t fid : {street, addr, poi}) {
        const uint64_t size = old_d[patch_file_names[fid]].size();
        std::vector<char> copied = words({fid, COPY_OLD_STRIDE});
        append(copied, size);
        append(copied, size);
        append(copied, 0u);
        REQUIRE(find_once(raw, copied) != std::string::npos);
    }
    const size_t parent = find_once(raw, words({POI_PARENT_REMAP_MARKER, 0}));
    const size_t head = find_once(raw, secondary_remap_head(0));
    REQUIRE(parent != std::string::npos && head != std::string::npos && parent < head);
    // Where the parent-id remap's street pair count, and the secondary
    // remap's file count and street pair count sit, and where the secondary
    // remap ends (after the POI records' empty list).
    const size_t parent_street_pairs = parent + 4, n_files = head + 4, street_pairs = head + 12, end = head + 40;
    REQUIRE(std::vector<char>(raw.begin() + head + 32, raw.begin() + end) == words({poi, 0}));

    using Edit = std::function<void(std::vector<char>&)>;
    auto insert_at = [](std::vector<char>& p, size_t at, const std::vector<char>& b) { p.insert(p.begin() + at, b.begin(), b.end()); };
    // Two lists for fid at the secondary remap's end, each of one pair (old
    // id 0 to new id 1).
    auto two_lists = [&](uint32_t fid) -> Edit {
        return [&, fid](std::vector<char>& p) {
            put_at(p, n_files, 6u);
            insert_at(p, end, words({fid, 1, 0, 1, fid, 1, 0, 1}));
        };
    };
    struct Case {
        std::string name;
        Edit edit;
        std::string refusal;
    };
    const Case cases[] = {
        {"poi-lists", two_lists(poi), "Malformed secondary remap: a second one for poi_records.bin"},
        {"addr-lists", two_lists(addr), "Malformed secondary remap: a second one for addr_points.bin"},
        // A list for the POIs, then a second secondary remap with another.
        {"poi-second-remap", [&](std::vector<char>& p) {
             put_at(p, n_files, 5u);
             insert_at(p, end, words({poi, 1, 0, 1, SECONDARY_REMAP_MARKER, 1, poi, 1, 0, 1}));
         }, "Malformed secondary remap: a second one for poi_records.bin"},
        // A pair in each remap's street list, the later one's first.
        {"street-pairs-in-both", [&](std::vector<char>& p) {
             put_at(p, street_pairs, 1u);
             insert_at(p, street_pairs + 4, words({0, 1}));
             put_at(p, parent_street_pairs, 1u);
             insert_at(p, parent_street_pairs + 4, words({0, 1}));
         }, "Malformed secondary remap: street_ways.bin's moved pairs came in the POI parent-id remap already"},
    };
    for (const Case& e : cases) {
        std::vector<char> edited = raw;
        e.edit(edited);
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A second POI parent-id remap is refused in every mode, whichever comes
// first: an empty one (no pair) before or after the one the diff made (the
// 20 moved street ways with addresses), or that one twice. The patch as made
// applies.
TEST(patch_refuses_a_second_parent_id_remap) {
    ScratchDir dir("patch-second-parent-remap");
    BuildDir old_d, new_d, old_meta, new_meta;
    put_moved_ways(old_d, new_d, old_meta, new_meta, 20, 0);
    put_changed_pois(old_d, new_d, 20);
    const LowMemCase c{dir.path(), "parent"};
    REQUIRE(prepare(c, old_d, new_d, old_meta, new_meta).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    std::vector<char> legs, empty;
    for (const uint32_t w : {POI_PARENT_REMAP_MARKER, 20u}) append(legs, w);
    for (const uint32_t w : {POI_PARENT_REMAP_MARKER, 0u}) append(empty, w);
    const size_t at = find_once(raw, legs);
    REQUIRE(at != std::string::npos);
    // The section's end: past its marker, the street count and the street
    // pairs.
    const size_t end = at + 8 + 20 * 8;
    const std::vector<char> made(raw.begin() + at, raw.begin() + end);

    struct Case {
        std::string name;
        size_t at;
        std::vector<char> section;
    };
    const Case cases[] = {{"empty-before", at, empty}, {"empty-after", end, empty}, {"twice", end, made}};
    for (const Case& e : cases) {
        std::vector<char> edited = raw;
        edited.insert(edited.begin() + e.at, e.section.begin(), e.section.end());
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, "Malformed POI parent-id remap: a second one").empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// Secondary pairs for a file the patcher keeps no record remap of are refused
// in every mode: a list of one pair for admin_polygons.bin (admin polygons
// keep their slots), for place_nodes.bin (no index the patcher rebuilds
// holds place node ids) or for file 35, past every file (PatchFileId::COUNT).
// Empty lists for them pass, as the diff's own empty lists do: that patch, and
// the patch as made (20 secondary-matched POIs, 20 moved street ways with
// addresses), apply.
TEST(patch_refuses_secondary_pairs_for_a_file_without_a_record_remap) {
    ScratchDir dir("patch-foreign-pairs");
    BuildDir old_d, new_d, old_meta, new_meta;
    put_moved_ways(old_d, new_d, old_meta, new_meta, 20, 0);
    put_changed_pois(old_d, new_d, 20);
    const LowMemCase c{dir.path(), "foreign"};
    REQUIRE(prepare(c, old_d, new_d, old_meta, new_meta).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    const size_t head = find_once(raw, secondary_remap_head(20));
    REQUIRE(head != std::string::npos);
    // Where the secondary remap's file count sits, and where it ends (after
    // the POIs' pairs).
    const size_t n_files = head + 4, end = head + 40 + 20 * 8;
    const uint32_t admin = static_cast<uint32_t>(PatchFileId::ADMIN_POLYGONS), place = static_cast<uint32_t>(PatchFileId::PLACE_NODES),
                   past = static_cast<uint32_t>(PatchFileId::COUNT);
    // The patch with lists (file id, pair count, pairs) after the four the
    // diff wrote.
    auto with_lists = [&](uint32_t n, std::initializer_list<uint32_t> words) {
        std::vector<char> p = raw, lists;
        put_at(p, n_files, 4 + n);
        for (const uint32_t w : words) append(lists, w);
        p.insert(p.begin() + end, lists.begin(), lists.end());
        return p;
    };

    struct Case {
        std::string name;
        std::vector<char> patch;
        std::string refusal;
    };
    const Case cases[] = {
        {"admin", with_lists(1, {admin, 1, 0, 1}), "Malformed secondary remap: pairs for admin_polygons.bin, which has none"},
        {"place", with_lists(1, {place, 1, 0, 1}), "Malformed secondary remap: pairs for place_nodes.bin, which has none"},
        {"past-every-file", with_lists(1, {past, 1, 0, 1}), "Malformed secondary remap: pairs for file 35, which has none"},
    };
    for (const Case& e : cases) {
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, e.patch));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
    REQUIRE(write_patch(c, c.patch(), with_lists(3, {admin, 0, place, 0, past, 0})));
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A merge section of more records than the patcher's limit (--file-records-max,
// FILE_RECORDS_MAX: the u32 record indices' own, lower for tests) is refused
// in every mode before it is replayed, by its file and day: ten POIs, four of
// them points, and the new day's with an eleventh, a polygon, under a limit
// of 10 (the new POI records), the other way round under 10 too (the old
// ones), and under one less than the new day's vertex bytes (the POIs' child
// stream, a byte a record, which the low-memory mode replays with its parent).
// Under the most records a file of either patch holds (those vertex bytes),
// both patches apply.
TEST(patch_refuses_a_merge_section_of_more_records_than_its_limit) {
    ScratchDir dir("patch-file-records");
    std::mt19937 rng(20261101);
    const std::vector<Poi> ten = pois_of(rng, 10, 0, true);
    std::vector<Poi> eleven = ten;
    eleven.push_back({10, block(rng, 4)});
    BuildDir small, large;
    put_pois(small, ten);
    put_pois(large, eleven);
    const LowMemCase grows{dir.path(), "grows"}, shrinks{dir.path(), "shrinks"};
    REQUIRE(prepare(grows, small, large).empty());
    REQUIRE(prepare(shrinks, large, small).empty());
    const uint64_t vertices = large["poi_vertices.bin"].size();
    auto limit = [](uint64_t n) { return std::vector<std::string>{"--file-records-max", std::to_string(n)}; };
    auto refusal = [](const char* side, const char* file, uint64_t n, uint64_t max) {
        return std::string(side) + " " + file + " holds " + std::to_string(n) + " records, more than the " + std::to_string(max) +
               " a patch can index";
    };

    struct Case {
        const LowMemCase& c;
        std::string name;
        uint64_t limit;
        std::string refusal;
    };
    const Case cases[] = {
        {grows, "new-records", 10, refusal("New", "poi_records.bin", 11, 10)},
        {shrinks, "old-records", 10, refusal("Old", "poi_records.bin", 11, 10)},
        {grows, "new-vertices", vertices - 1, refusal("New", "poi_vertices.bin", vertices, vertices - 1)},
    };
    for (const Case& e : cases) CHECK(not_refused(e.c, ALL_MODES, e.c.patch(), e.name, e.refusal, limit(e.limit)).empty());
    for (const LowMemCase& c : {grows, shrinks}) CHECK(apply_modes(c, ALL_MODES, {}, limit(vertices)).bad.empty());
}

// String fixups the replay can't take are refused in every mode: 10 POIs
// whose names move past the strings section's pairs, the new day without POI
// 1, so the POI merge carries the fixups of POIs 0 and 2 to 8, and
//   deleted: the first moved to POI 1's name, which the replay deletes,
//   left-over: one more, past the last POI,
//   child: one in the merge of the POI vertices, a file without string
//     fields, which the low-memory mode replays as the POIs' child stream.
// The patch as made applies.
TEST(patch_refuses_string_fixups_on_a_deleted_record_left_over_or_in_a_child_stream) {
    ScratchDir dir("patch-string-fixups");
    BuildDir old_d, new_d;
    put_reversed_poi_names(old_d, new_d, 10, 1);
    const LowMemCase c{dir.path(), "fixups"};
    REQUIRE(prepare(c, old_d, new_d, {}, {}, NO_STRING_PAIRS).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    const size_t head = find_once(raw, poi_merge_head(10, 9, 8));
    REQUIRE(head != std::string::npos);
    // The POI merge's string fixups: their count, their size, then POI 0's
    // (slot 0: no slot skipped).
    const size_t n_at = head + 40, first = n_at + 8;
    REQUIRE(raw[first] == 0);
    // The POI vertices' merge: empty on both days, no offset or string fixups.
    std::vector<char> child;
    append(child, static_cast<uint32_t>(PatchFileId::POI_VERTICES));
    append(child, 1u);
    append(child, uint64_t{0});
    append(child, uint64_t{0});
    for (int k = 0; k < 6; k++) append(child, 0u);
    const size_t child_at = find_once(raw, child);
    REQUIRE(child_at != std::string::npos);
    // One fixup more at the end of a section's string fixups (count at n_at,
    // size after it): slot 0 or the 128th past the last, value unchanged.
    auto add_fixup = [](std::vector<char>& p, size_t n_at, char skipped) {
        uint32_t n, size;
        memcpy(&n, p.data() + n_at, 4);
        memcpy(&size, p.data() + n_at + 4, 4);
        put_at(p, n_at, n + 1);
        put_at(p, n_at + 4, size + 2);
        const std::vector<char> fixup = {skipped, 0};
        p.insert(p.begin() + static_cast<std::ptrdiff_t>(n_at + 8 + size), fixup.begin(), fixup.end());
    };

    using Edit = std::function<void(std::vector<char>&)>;
    struct Case {
        std::string name;
        Edit edit;
        std::vector<std::string> modes;
        std::string refusal;
    };
    const Case cases[] = {
        {"deleted", [&](std::vector<char>& p) { p[first] = 3; }, ALL_MODES, "Malformed string fixups"},
        {"left-over", [&](std::vector<char>& p) { add_fixup(p, n_at, 0x7F); }, ALL_MODES, "Malformed string fixups"},
        {"child", [&](std::vector<char>& p) { add_fixup(p, child_at + 40, 0); }, {"plain"}, "Malformed string fixups"},
        {"child", [&](std::vector<char>& p) { add_fixup(p, child_at + 40, 0); }, ENCODED_MODES,
         "Encoded set: a child stream with string fixups"},
    };
    for (const Case& e : cases) {
        std::vector<char> edited = raw;
        e.edit(edited);
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, e.modes, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// Offset fixups the replay never reaches or can't take, refused in every
// mode. Ten POIs with blocks, POI 4's a vertex longer on the new day, so the
// POI merge shifts the later POIs' vertex_offset in a run of offset fixups;
// the merge given
//   value: a value for record 10, past the old file's records,
//   offset: a value for record 3, which has an offset (values give records
//     without one theirs),
//   run: a run 1000 records past the last one,
//   byte: a byte past the last run,
//   child: a byte in the runs of the POI vertices' merge, without a run (a
//     file without offset fields, which the low-memory mode replays as the
//     POIs' child stream).
// The patch as made applies.
TEST(patch_refuses_offset_fixups_left_over_or_in_a_child_stream) {
    ScratchDir dir("patch-offset-fixups");
    std::mt19937 rng(20261031);
    const std::vector<Poi> pois = pois_of(rng, 10, 3, false);
    std::vector<Poi> grown = pois;
    grown[4].block.insert(grown[4].block.end(), {1, 2, 3, 4});
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, grown);
    const LowMemCase c{dir.path(), "offsets"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    auto u32_at = [&](size_t p) { uint32_t v; memcpy(&v, raw.data() + p, 4); return v; };
    // A merge section by its head: file, stride, old and new size. Its offset
    // fixups' run and value counts and sizes follow, then the runs and values.
    auto merge_at = [&](PatchFileId fid, uint32_t stride, uint64_t old_size, uint64_t new_size) {
        std::vector<char> head;
        append(head, static_cast<uint32_t>(fid));
        append(head, stride);
        append(head, old_size);
        append(head, new_size);
        return find_once(raw, head);
    };
    const size_t records =
        merge_at(PatchFileId::POI_RECORDS, static_cast<uint32_t>(POI_RECORD_STRIDE), 10 * POI_RECORD_STRIDE, 10 * POI_RECORD_STRIDE);
    REQUIRE(records != std::string::npos);
    REQUIRE(u32_at(records + 24) >= 1);   // runs
    REQUIRE(u32_at(records + 28) == 0);   // values
    const size_t vertices = merge_at(PatchFileId::POI_VERTICES, 1, 10 * 22, 10 * 22 + 4);
    REQUIRE(vertices != std::string::npos);
    // bytes after the last of a section's runs or values, counted as one
    // fixup more or not.
    enum Stream { RUNS, VALUES };
    auto add = [](std::vector<char>& p, size_t section, Stream stream, const std::vector<char>& bytes, bool counted) {
        uint32_t count, size, runs_size;
        const size_t count_at = section + 24 + 4 * stream, size_at = section + 32 + 4 * stream;
        memcpy(&count, p.data() + count_at, 4);
        memcpy(&size, p.data() + size_at, 4);
        memcpy(&runs_size, p.data() + section + 32, 4);
        const size_t end = section + 40 + runs_size + (stream == VALUES ? size : 0);
        if (counted) put_at(p, count_at, count + 1);
        put_at(p, size_at, static_cast<uint32_t>(size + bytes.size()));
        p.insert(p.begin() + static_cast<std::ptrdiff_t>(end), bytes.begin(), bytes.end());
    };

    using Edit = std::function<void(std::vector<char>&)>;
    struct Case {
        std::string name;
        Edit edit;
        std::vector<std::string> modes;
        std::string refusal;
    };
    // A value: record 10 or 3 (from record 0), offset 0. A run: 1000 records
    // skipped (varint E8 07), one record, no change of the shift.
    const Case cases[] = {
        {"value", [&](std::vector<char>& p) { add(p, records, VALUES, {10, 0}, true); }, ALL_MODES, "Malformed offset fixups"},
        {"offset", [&](std::vector<char>& p) { add(p, records, VALUES, {3, 0}, true); }, ALL_MODES, "Malformed offset fixups"},
        {"run", [&](std::vector<char>& p) { add(p, records, RUNS, {'\xE8', 7, 1, 0}, true); }, ALL_MODES, "Malformed offset fixups"},
        {"byte", [&](std::vector<char>& p) { add(p, records, RUNS, {0}, false); }, ALL_MODES, "Malformed offset fixups"},
        {"child", [&](std::vector<char>& p) { add(p, vertices, RUNS, {0}, false); }, {"plain"}, "Malformed offset fixups"},
        {"child", [&](std::vector<char>& p) { add(p, vertices, RUNS, {0}, false); }, ENCODED_MODES,
         "Encoded set: a child stream with offset fixups"},
    };
    for (const Case& e : cases) {
        std::vector<char> edited = raw;
        e.edit(edited);
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, e.modes, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A stale old core tier, which the patcher reads before anything checks it:
// with a letter changed, or with a NUL lost so that the old pool holds one
// string fewer and the edit's last deleted index lies past it, it's refused in
// every mode as an old file the patch wasn't made from, not as a malformed
// patch. The patch applies to the tier it was made from.
TEST(patch_refuses_a_stale_old_string_tier) {
    ScratchDir dir("patch-stale-strings");
    // 10,000 streets, every third and the last deleted and every hundredth
    // followed by an added one.
    std::string old_pool, new_pool;
    for (uint32_t i = 0; i < 10000; i++) {
        char b[24];
        std::snprintf(b, sizeof b, "street %07u", i);
        old_pool.append(b, strlen(b) + 1);
        if (i % 3 != 0 && i != 9999) new_pool.append(b, strlen(b) + 1);
        if (i % 100 == 7) new_pool.append(b).append("+", 2);
    }
    BuildDir old_d, new_d;
    old_d["strings_core.bin"].assign(old_pool.begin(), old_pool.end());
    new_d["strings_core.bin"].assign(new_pool.begin(), new_pool.end());
    const LowMemCase c{dir.path(), "stale"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    // The old dir's tier and the encoded set's (a link to it, or a copy):
    // whether both were written.
    auto put_old_pool = [&](const std::string& pool) {
        return write_file(c.dir("old") + "/strings_core.bin", pool.data(), pool.size()) &&
               write_file(c.dir("old.enc") + "/strings_core.bin", pool.data(), pool.size());
    };
    std::string letter = old_pool, fewer = old_pool;
    letter[3] = 'E';
    fewer[fewer.size() - 16] = 'x';  // the NUL before the last street
    const std::pair<std::string, std::string> stale_pools[] = {{"letter", letter}, {"fewer", fewer}};
    const std::string stale = "Old strings_core.bin is not the one the patch was made from";

    for (const auto& [name, pool] : stale_pools) {
        REQUIRE(put_old_pool(pool));

        CHECK(not_refused(c, ALL_MODES, c.patch(), name, stale).empty());
    }
    REQUIRE(put_old_pool(old_pool));
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A frame declaring 2^32 + 1 bytes of content (then 8 KiB of them, an RLE
// block, and the end) fails the run with an error. A 64-bit build maps that
// decompression bound and finds the 8 KiB are no patch; a 32-bit size_t can't
// hold it, so it is refused (narrowed, it was a one-byte mapping the block was
// written past).
TEST(patch_refuses_a_frame_declaring_content_past_32_bits) {
    ScratchDir dir("patch-bound");
    const std::string& root = dir.path();
    REQUIRE(write_dir(root + "/plain", {}));
    const unsigned char frame[] = {0x28, 0xb5, 0x2f, 0xfd, 0xc0, 0x38, 0x01, 0x00, 0x00, 0x00, 0x01,
                                   0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x41, 0x01, 0x00, 0x00};
    REQUIRE(write_file(root + "/p.gcpatch", std::vector<char>(frame, frame + sizeof frame)));

    auto r = run_tool({GEOCODER_PATCH_BIN, root + "/plain", root + "/p.gcpatch", "-o", root + "/out"}, root + "/log");

    CHECK(!r.ok);
    CHECK(logged(r, sizeof(size_t) > 4 ? "Bad magic" : "Failed to decompress"));
}

// An option other than --threads (a staged apply's --only, on an encoded
// installed set among them) is refused before anything is written.
TEST(patch_refuses_an_unknown_option) {
    ScratchDir dir("patch-option");
    const std::string& root = dir.path();
    REQUIRE(write_dir(root + "/cur", {{"gc.manifest", {}}}));
    REQUIRE(write_file(root + "/p.gcpatch", std::vector<char>{'n', 'o', 't'}));

    auto r = run_tool({GEOCODER_PATCH_BIN, root + "/cur", root + "/p.gcpatch", "-o", root + "/out", "--only", "street_ways.bin"},
                      root + "/log");

    CHECK(!r.ok);
    CHECK(logged(r, "Unknown option --only"));
    CHECK(!std::filesystem::exists(root + "/out"));
}

// The output dir is created as named, quotes and all, and no part of its name
// runs as a command.
TEST(patch_creates_the_output_dir_as_named) {
    ScratchDir dir("patch-outdir");
    const std::string& root = dir.path();
    REQUIRE(write_dir(root + "/plain", {}));
    REQUIRE(write_file(root + "/p.gcpatch", std::vector<char>{'n', 'o', 't'}));
    for (const std::string name : {"user's update/new", "x'; touch pwned; 'y/new"}) {
        run({GEOCODER_PATCH_BIN, "plain", "p.gcpatch", "-o", name}, root + "/log", root);

        CHECK(std::filesystem::is_directory(root + "/" + name));
    }
    CHECK(!std::filesystem::exists(root + "/pwned"));
}

// An output dir that holds a file already is refused before anything is
// written, so no output can be a current file (a hard link or a symlink to
// one, in a plain or an encoded set) and the current set keeps its bytes;
// and so is the current dir itself.
TEST(patch_refuses_an_output_dir_holding_files) {
    ScratchDir dir("patch-alias");
    const std::string& root = dir.path();
    std::vector<char> parents, changed;
    for (uint32_t v = 0; v < 4096; v++) {
        append(parents, v);
        append(changed, v + (v == 2000));
    }
    const LowMemCase c{root, "wp"};
    REQUIRE(prepare(c, {{"way_parents.bin", parents}, {"meta.json", {'{', '}'}}},
                    {{"way_parents.bin", changed}, {"meta.json", {'{', ' ', '}'}}}).empty());
    struct Alias {
        std::string cur, name;
        bool symlink;
    };
    int n = 0;
    for (const Alias& a : {Alias{"old", "meta.json", false}, Alias{"old", "way_parents.bin", true},
                           Alias{"old.enc", "way_parents.u24", false}, Alias{"old.enc", "gc.manifest", true}}) {
        const std::string cur = c.dir(a.cur), out = root + "/out" + std::to_string(n++);
        std::filesystem::create_directories(out);
        if (a.symlink) std::filesystem::create_symlink(cur + "/" + a.name, out + "/" + a.name);
        else std::filesystem::create_hard_link(cur + "/" + a.name, out + "/" + a.name);
        const BuildDir before = read_dir(cur);

        auto r = run_tool({GEOCODER_PATCH_BIN, cur, c.patch(), "-o", out}, c.log());

        CHECK(!r.ok);
        CHECK(logged(r, "Output dir is not empty"));
        CHECK(read_dir(cur) == before);
    }
    const BuildDir before = read_dir(c.dir("old.enc"));

    auto r = run_tool({GEOCODER_PATCH_BIN, c.dir("old.enc"), c.patch(), "-o", c.dir("old.enc")}, c.log());

    CHECK(!r.ok);
    CHECK(logged(r, "Output dir must differ from the current dir"));
    CHECK(read_dir(c.dir("old.enc")) == before);
}

// A TMPDIR too small for the decompressed patch fails the run with an error
// (no SIGBUS from a page of the mapped patch.raw with no storage behind it)
// and the scratch dir goes; one with room for the patch but not for its
// decompression bound (a frame without a content size bounds every block at
// 128 KiB) still decompresses it. TMPDIR there is a size-limited tmpfs in a
// private mount namespace (unshare), gone with the run; skipped where user
// namespaces are unavailable, except under
// GEOCODER_TESTS_NEED_PRIVATE_TMPFS=1 (CI, which allows them), where that
// fails.
TEST(patch_fails_cleanly_on_a_tmpdir_too_small_for_the_patch) {
    const char* need = std::getenv("GEOCODER_TESTS_NEED_PRIVATE_TMPFS");
    const bool needed = need && std::string(need) == "1";
    ScratchDir dir("patch-tmpfs");
    const std::string& root = dir.path();
    REQUIRE(write_dir(root + "/plain", {}));
    const std::string log = root + "/zstd.log";
    REQUIRE(write_file(root + "/zeros", std::vector<char>(8 << 20, 0)));
    REQUIRE(run({"zstd", "-q", "-f", "--no-content-size", root + "/zeros", "-o", root + "/full.gcpatch"}, log));
    // 64 frames of 16 KiB without a content size: 1 MiB, bounded at 8 MiB.
    REQUIRE(write_file(root + "/piece", std::vector<char>(16 << 10, 0)));
    REQUIRE(run({"zstd", "-q", "-f", "--no-content-size", root + "/piece", "-o", root + "/piece.zst"}, log));
    const std::vector<char> piece = read_file(root + "/piece.zst");
    std::vector<char> framed;
    for (int i = 0; i < 64; i++) framed.insert(framed.end(), piece.begin(), piece.end());
    REQUIRE(write_file(root + "/framed.gcpatch", framed));
    // The script exits with the patcher's status, so a sanitizer report there
    // fails the test as in any tool run. The patcher's output goes to the
    // run's log; <name>.after gets its exit status, then what the tmpfs still
    // holds after it.
    const std::string script = "mount -t tmpfs -o size=\"$1\" tmpfs \"$2\" || exit 99; "
                               "TMPDIR=\"$2\" \"$3\" \"$4\" \"$5\" -o \"$6\"; rc=$?; "
                               "{ echo \"rc=$rc\"; ls -A \"$2\"; } > \"$7\"; exit $rc";
    struct Run {
        bool namespaced;
        std::string status, err, left;
    };
    auto on_small_tmpdir = [&](const std::string& size, const std::string& patch, const std::string& name) {
        const std::string mnt = root + "/" + name + ".tmpfs", after = root + "/" + name + ".after";
        std::filesystem::create_directories(mnt);
        Run r;
        r.err = run_tool({"unshare", "--map-root-user", "--mount", "sh", "-c", script, "sh", size, mnt, GEOCODER_PATCH_BIN,
                          root + "/plain", patch, root + "/" + name, after},
                         root + "/" + name + ".log")
                    .log;
        auto text = read_file(after);
        const std::string said(text.begin(), text.end());
        r.namespaced = said.rfind("rc=", 0) == 0;
        r.status = said.substr(0, said.find('\n'));
        r.left = said.find('\n') == std::string::npos ? "" : said.substr(said.find('\n') + 1);
        if (!r.namespaced)
            std::printf("    %s %s: no private tmpfs (%s)\n", needed ? "FAIL" : "SKIP", name.c_str(), r.err.substr(0, 200).c_str());
        CHECK(r.namespaced || !needed);
        return r;
    };

    const Run full = on_small_tmpdir("1m", root + "/full.gcpatch", "full");
    if (full.namespaced) {
        CHECK_EQ(full.status, std::string("rc=1"));
        CHECK(full.err.find("Failed to decompress") != std::string::npos);
        CHECK(full.left.empty());
    }
    const Run bound = on_small_tmpdir("4m", root + "/framed.gcpatch", "framed");
    if (bound.namespaced) {
        CHECK(bound.err.find("Bad magic") != std::string::npos);
        CHECK(bound.left.empty());
    }
}

// --- GCPATCH v14: admin geometry from arcs, merged by slot ---

namespace {

// A section's head: its file, its stride and its old and new sizes.
std::vector<char> section_head(PatchFileId fid, uint32_t stride, uint64_t old_size, uint64_t new_size) {
    std::vector<char> head;
    append(head, static_cast<uint32_t>(fid));
    append(head, stride);
    append(head, old_size);
    append(head, new_size);
    return head;
}

// Where a raw patch sends fid's file of size bytes as COPY_OLD (unchanged),
// or npos unless it does so once.
size_t copied_old_at(const std::vector<char>& raw, PatchFileId fid, uint64_t size) {
    std::vector<char> head = section_head(fid, COPY_OLD_STRIDE, size, size);
    append(head, 0u);
    append(head, uint64_t{0});
    return find_once(raw, head);
}

// A merge section's ops, each (op, records).
using Ops = std::vector<std::pair<uint8_t, uint32_t>>;

// The merge section of a raw patch with that head: its offset fixups' run
// and value counts and its ops; found is false unless the patch holds the
// head once and a whole section after it.
struct MergeSection {
    bool found = false;
    uint32_t fixup_runs = 0, fixup_values = 0;
    Ops ops;
};

MergeSection merge_section(const std::vector<char>& raw, PatchFileId fid, uint32_t stride, uint64_t old_size,
                           uint64_t new_size) {
    MergeSection s;
    const std::vector<char> head = section_head(fid, stride, old_size, new_size);
    size_t at = find_once(raw, head);
    if (at == std::string::npos) return s;
    at += head.size();
    auto skip = [&](uint64_t n) {
        if (n > raw.size() - at) return false;
        at += n;
        return true;
    };
    auto read = [&](void* out, size_t n) {
        if (n > raw.size() - at) return false;
        memcpy(out, raw.data() + at, n);
        at += n;
        return true;
    };
    uint32_t runs_size = 0, values_size = 0, string_fixups = 0, string_fixups_size = 0;
    uint64_t seq_size = 0;
    if (!read(&s.fixup_runs, 4) || !read(&s.fixup_values, 4) || !read(&runs_size, 4) || !read(&values_size, 4) ||
        !skip(uint64_t{runs_size} + values_size) || !read(&string_fixups, 4) || !read(&string_fixups_size, 4) ||
        !skip(string_fixups_size) || !read(&seq_size, 8) || seq_size > raw.size() - at)
        return s;
    const size_t end = at + seq_size;
    while (at < end) {
        uint8_t op = 0;
        uint32_t count = 0;
        if (!read(&op, 1) || !read(&count, 4)) return s;
        if ((op == OP_INSERT_RUN || op == OP_REPLACE_RUN) && !skip(uint64_t{count} * stride)) return s;
        s.ops.push_back({op, count});
    }
    s.found = at == end;
    return s;
}

// An admin arc dir by slot, with real blocks (arc_blocks.h): each arc slot's
// vertices (none for a dead slot, whose block is empty) and each polygon
// slot's one ring, the arc slots it runs over (none for a tombstone), and its
// name.
struct ArcSlots {
    std::vector<std::vector<PackedLocation>> arcs;
    std::vector<std::vector<uint32_t>> rings;
    std::vector<uint32_t> names;
};

// Arc i of n vertices: a line north-east from (47° + i · 0.01°, 8°), 1000
// e7 units of latitude and 1500 of longitude a step, so its ends are its box.
std::vector<PackedLocation> arc_line(uint32_t i, uint32_t n) {
    std::vector<PackedLocation> v;
    for (uint32_t k = 0; k < n; k++)
        v.push_back({470000000 + static_cast<int32_t>(i) * 100000 + static_cast<int32_t>(k) * 1000,
                     80000000 + static_cast<int32_t>(k) * 1500});
    return v;
}

// n_arcs arcs of `vertices` each and n_polygons polygons, polygon p's ring
// over arcs 3p, 3p + 1 and 3p + 2 (mod n_arcs), named 100 + p.
ArcSlots arc_slots(uint32_t n_arcs, uint32_t vertices, uint32_t n_polygons) {
    ArcSlots a;
    for (uint32_t i = 0; i < n_arcs; i++) a.arcs.push_back(arc_line(i, vertices));
    for (uint32_t p = 0; p < n_polygons; p++) {
        a.rings.push_back({(3 * p) % n_arcs, (3 * p + 1) % n_arcs, (3 * p + 2) % n_arcs});
        a.names.push_back(100 + p);
    }
    return a;
}

// An arc's block at k = 0 (encode_arc); a dead slot's is empty.
std::vector<char> arc_block(const std::vector<PackedLocation>& v) {
    if (v.empty()) return {};
    auto at = [&](size_t i) { return v[i]; };
    std::vector<char> b(arc_blocks::encode_arc(v.size(), at, 0, nullptr));
    arc_blocks::encode_arc(v.size(), at, 0, reinterpret_cast<uint8_t*>(b.data()));
    return b;
}

// A polygon's ring block of one ring (encode_ring); a tombstone has none.
std::vector<char> ring_block(const std::vector<uint32_t>& slots) {
    if (slots.empty()) return {};
    auto ref = [&](size_t i) {
        arc_blocks::RingRef r;
        r.slot = r.planet_slot = slots[i];
        return r;
    };
    std::vector<char> b(arc_blocks::encode_ring(slots.size(), ref, nullptr));
    arc_blocks::encode_ring(slots.size(), ref, reinterpret_cast<uint8_t*>(b.data()));
    return b;
}

// a's four admin geometry files into d, laid out as put_arc_geometry lays
// them out (a tombstone's record {NO, 0, NO}).
void put_arc_slots(BuildDir& d, const ArcSlots& a) {
    ArcGeometry g;
    for (size_t p = 0; p < a.rings.size(); p++) {
        g.keys.push_back(a.rings[p].empty() ? NO : a.names[p]);
        g.rings.push_back(ring_block(a.rings[p]));
    }
    for (const auto& arc : a.arcs) g.arcs.push_back(arc_block(arc));
    put_arc_geometry(d, g);
}

// The bytes of a's arc blocks of slots [from, to), and of its ring blocks.
uint32_t arc_bytes(const ArcSlots& a, size_t from, size_t to) {
    size_t n = 0;
    for (size_t s = from; s < to; s++) n += arc_block(a.arcs[s]).size();
    return static_cast<uint32_t>(n);
}
uint32_t ring_bytes(const ArcSlots& a, size_t from, size_t to) {
    size_t n = 0;
    for (size_t p = from; p < to; p++) n += ring_block(a.rings[p]).size();
    return static_cast<uint32_t>(n);
}

// Each admin geometry file's line in a patcher log: a merge section's
// ("<name>: <size> bytes"), or COPY_OLD's when it is unchanged, and never a
// full replacement. "" or what is wrong.
LogCheck merged_by_slot(const BuildDir& old_d, const BuildDir& new_d) {
    return [old_d, new_d](const std::string& log) -> std::string {
        for (const AdminSlotFamily& family : ADMIN_SLOT_FAMILIES)
            for (const PatchFileId fid : {family.slots, family.blocks}) {
                const std::string name = patch_file_names[uint32_t(fid)];
                const std::string size = std::to_string(new_d.at(name).size());
                const std::string line = old_d.at(name) == new_d.at(name) ? name + ": unchanged (copied " + size + " bytes from old)"
                                                                           : name + ": " + size + " bytes";
                if (log.find(line) == std::string::npos) return "no \"" + line + "\"";
                if (log.find(name + ": full replace") != std::string::npos) return name + " replaced whole";
            }
        return "";
    };
}

}  // namespace

// An admin-minimal-shaped dir (its arc, ring, record, place and admin cell
// files, core strings and layout) round-trips in every mode, the admin
// geometry files merged by slot: one arc edited, one longer, one new; one
// ring edited in place, so the records stay and go as copied; a place's label
// and an admin cell's list changed. The parent's diff refused the dir: no
// patch section named admin_rings.bin.
TEST(patch_lowmem_round_trips_an_admin_minimal_arc_dir) {
    ScratchDir dir("lowmem-arc-dir");
    std::mt19937 rng(20261103);
    const ArcGeometry old_g = arc_geometry(rng, 200, 600);
    ArcGeometry new_g = old_g;
    new_g.arcs[5] = random_bytes(rng, new_g.arcs[5].size());
    new_g.arcs[100].resize(new_g.arcs[100].size() + 8, 'x');
    new_g.arcs.push_back(random_bytes(rng, 40));
    new_g.rings[4] = random_bytes(rng, new_g.rings[4].size());
    BuildDir old_d, new_d;
    put_admin_minimal(old_d, old_g, 300);
    put_admin_minimal(new_d, new_g, 300);
    put_at(new_d["place_nodes.bin"], 7 * PLACE_NODE_STRIDE + 16, 2u);
    CellLists cells = parse_cell_lists(new_d["admin_cells.bin"], new_d["admin_entries.bin"]);
    cells[cell_id(9)] = {9, 11 | INTERIOR};
    put_cells(new_d, "admin", cells);
    const LowMemCase c{dir.path(), "arc-dir"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    CHECK(apply_modes(c, ALL_MODES, merged_by_slot(old_d, new_d)).bad.empty());
}

// An encoded family (the POIs, encoded by geocoder-gc) beside the plain arc
// files: the low-memory mode replays the family through its codecs and the
// arc files as plain merge sections, and the new set is gc's encoding of the
// new dir, byte for byte. The parent's diff refused the dir.
TEST(patch_lowmem_applies_an_encoded_set_beside_plain_arc_files) {
    ScratchDir dir("lowmem-arcs-beside");
    std::mt19937 rng(20261103);
    const ArcGeometry old_g = arc_geometry(rng, 60, 200);
    ArcGeometry new_g = old_g;
    new_g.arcs[17] = random_bytes(rng, new_g.arcs[17].size() + 4);
    new_g.rings[20] = random_bytes(rng, new_g.rings[20].size());
    const auto pois = pois_of(rng, 300, 0, true);
    auto edited = pois;
    std::fill(edited[100].block.begin() + 10, edited[100].block.end(), 0);
    BuildDir old_d, new_d;
    put_arc_geometry(old_d, old_g);
    put_arc_geometry(new_d, new_g);
    put_pois(old_d, pois);
    put_pois(new_d, edited);
    const LowMemCase c{dir.path(), "beside"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    REQUIRE(read_dir(c.dir("old.enc")).count("poi_records.r3") == 1);

    const LogCheck merged = merged_by_slot(old_d, new_d);
    CHECK(apply_both_modes(c, [&](const std::string& log) -> std::string {
              if (log.find("encode poi:") == std::string::npos) return "no encode poi line";
              return merged(log);
          }).empty());
}

// One interior vertex of one arc moved, its box and every delta's length
// kept: the arc vertices' merge keeps every byte but those of the vertex's
// two deltas that changed, at most 16 (a MATCH of the blocks before the arc
// and its head, a DELETE and an INSERT of the changed bytes, a MATCH of its
// tail and every block after it), and the arc index, the records and the
// rings go as unchanged. Every mode applies it. The parent sent the arc
// vertices whole.
TEST(patch_arc_vertices_send_only_the_bytes_of_a_moved_vertex) {
    ScratchDir dir("lowmem-arc-vertex");
    const ArcSlots old_a = arc_slots(40, 30, 12);
    ArcSlots new_a = old_a;
    new_a.arcs[7][15].lat_e7 += 3;
    new_a.arcs[7][15].lon_e7 -= 2;
    BuildDir old_d, new_d;
    put_arc_slots(old_d, old_a);
    put_arc_slots(new_d, new_a);
    const uint64_t size = new_d.at("arc_vertices.bin").size();
    REQUIRE(old_d.at("arc_vertices.bin").size() == size);
    REQUIRE(old_d.at("arc_vertices.bin") != new_d.at("arc_vertices.bin"));
    const LowMemCase c{dir.path(), "moved-vertex"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    const std::vector<char> raw = raw_patch(c);
    const MergeSection v = merge_section(raw, PatchFileId::ARC_VERTICES, 1, size, size);
    CHECK(v.found);
    REQUIRE(v.ops.size() == 4);
    CHECK(v.ops[0].first == OP_MATCH_RUN && v.ops[1].first == OP_DELETE_RUN && v.ops[2].first == OP_INSERT_RUN &&
          v.ops[3].first == OP_MATCH_RUN);
    std::printf("    moved vertex: %u of %u bytes sent\n", v.ops[2].second, static_cast<uint32_t>(size));
    CHECK(v.ops[0].second > arc_bytes(old_a, 0, 7));
    CHECK_EQ(v.ops[1].second, v.ops[2].second);
    CHECK(v.ops[2].second <= 16);
    CHECK_EQ(uint64_t{v.ops[0].second} + v.ops[1].second + v.ops[3].second, size);
    for (const PatchFileId fid : {PatchFileId::ARC_INDEX, PatchFileId::ADMIN_POLYGONS, PatchFileId::ADMIN_RINGS})
        CHECK(copied_old_at(raw, fid, new_d.at(patch_file_names[uint32_t(fid)]).size()) != std::string::npos);
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// An arc that grows, a vertex added at its end inside its box: the arc index
// MATCHes every slot, the slots after the arc moved by one offset fixup run
// of the bytes it grew, and the arc vertices insert those bytes behind its
// block; the rings, which name arc slots, not offsets, and the records go as
// unchanged. Every mode applies it. The parent sent the arc files whole.
TEST(patch_arc_index_moves_the_arcs_after_a_grown_arc_by_one_fixup_run) {
    ScratchDir dir("lowmem-arc-grown");
    const ArcSlots old_a = arc_slots(40, 30, 12);
    ArcSlots new_a = old_a;
    const PackedLocation end = new_a.arcs[7].back();
    new_a.arcs[7].push_back({end.lat_e7 - 500, end.lon_e7 - 700});
    BuildDir old_d, new_d;
    put_arc_slots(old_d, old_a);
    put_arc_slots(new_d, new_a);
    const uint32_t old_size = arc_bytes(old_a, 0, 40), new_size = arc_bytes(new_a, 0, 40);
    REQUIRE(new_size > old_size);
    const LowMemCase c{dir.path(), "grown-arc"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    const std::vector<char> raw = raw_patch(c);
    const MergeSection index = merge_section(raw, PatchFileId::ARC_INDEX, 4, 40 * 4, 40 * 4);
    CHECK(index.found);
    CHECK(index.ops == (Ops{{OP_MATCH_RUN, 40}}));
    CHECK_EQ(index.fixup_runs, 1u);
    CHECK_EQ(index.fixup_values, 0u);
    const MergeSection v = merge_section(raw, PatchFileId::ARC_VERTICES, 1, old_size, new_size);
    const uint32_t through_7 = arc_bytes(old_a, 0, 8);
    CHECK(v.ops == (Ops{{OP_MATCH_RUN, through_7}, {OP_INSERT_RUN, new_size - old_size}, {OP_MATCH_RUN, old_size - through_7}}));
    for (const PatchFileId fid : {PatchFileId::ADMIN_POLYGONS, PatchFileId::ADMIN_RINGS})
        CHECK(copied_old_at(raw, fid, new_d.at(patch_file_names[uint32_t(fid)]).size()) != std::string::npos);
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// 1,000 tombstones both days hold, among 100 live polygons (each eleventh
// slot), MATCH in place: the one change, polygon 0's ring running over one
// arc more, is a REPLACE of its record (its ring_bytes) and of its ring
// block (a DELETE and an INSERT), every later record MATCHing with its
// ring_offset moved by one fixup run. Nothing is inserted or deleted by
// record. Every mode applies it. The parent sent the records whole.
TEST(patch_admin_tombstones_both_days_hold_merge_in_place) {
    ScratchDir dir("lowmem-admin-tombstones");
    ArcSlots old_a = arc_slots(40, 10, 1100);
    for (size_t p = 0; p < old_a.rings.size(); p++)
        if (p % 11) old_a.rings[p].clear();
    ArcSlots new_a = old_a;
    new_a.rings[0].push_back(30);
    BuildDir old_d, new_d;
    put_arc_slots(old_d, old_a);
    put_arc_slots(new_d, new_a);
    const LowMemCase c{dir.path(), "tombstones"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    const std::vector<char> raw = raw_patch(c);
    const uint64_t records = 1100 * ADMIN_POLYGON_STRIDE;
    const MergeSection polygons = merge_section(raw, PatchFileId::ADMIN_POLYGONS, ADMIN_POLYGON_STRIDE, records, records);
    CHECK(polygons.found);
    CHECK(polygons.ops == (Ops{{OP_REPLACE_RUN, 1}, {OP_MATCH_RUN, 1099}}));
    CHECK_EQ(polygons.fixup_runs, 1u);
    const uint32_t old_rings = ring_bytes(old_a, 0, 1100), new_rings = ring_bytes(new_a, 0, 1100);
    const uint32_t ring_0 = ring_bytes(old_a, 0, 1), new_ring_0 = ring_bytes(new_a, 0, 1);
    const MergeSection rings = merge_section(raw, PatchFileId::ADMIN_RINGS, 1, old_rings, new_rings);
    CHECK(rings.ops == (Ops{{OP_DELETE_RUN, ring_0}, {OP_INSERT_RUN, new_ring_0}, {OP_MATCH_RUN, old_rings - ring_0}}));
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// The arc index by slot, where a slot present on both days is a MATCH (an
// arc index record is only its offset): arc 3, dead on the old day, is born,
// arc 5 dies, slot 8 is recycled for an arc that shares its first five
// vertices, and arcs 20 and 21 are appended. The arc vertices show the birth
// as an INSERT of its block, the death as a DELETE of its block, the recycle
// as a DELETE and an INSERT of what the two blocks don't share (their head
// kept) and the appended arcs as an INSERT; the index INSERTs the appended
// slots. The records and rings go as unchanged, and every mode applies it.
// The parent sent the arc files whole.
TEST(patch_arc_slots_born_dead_recycled_and_appended_merge_by_slot) {
    ScratchDir dir("lowmem-arc-slots");
    ArcSlots old_a = arc_slots(20, 10, 6);
    old_a.arcs[3].clear();
    ArcSlots new_a = old_a;
    new_a.arcs[3] = arc_line(30, 10);
    new_a.arcs[5].clear();
    for (size_t k = 5; k < 10; k++) new_a.arcs[8][k].lon_e7 += 40 * static_cast<int32_t>(k - 4);
    new_a.arcs.push_back(arc_line(32, 10));
    new_a.arcs.push_back(arc_line(33, 10));
    BuildDir old_d, new_d;
    put_arc_slots(old_d, old_a);
    put_arc_slots(new_d, new_a);
    const LowMemCase c{dir.path(), "arc-slots"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    const std::vector<char> raw = raw_patch(c);
    const MergeSection index = merge_section(raw, PatchFileId::ARC_INDEX, 4, 20 * 4, 22 * 4);
    CHECK(index.found);
    CHECK(index.ops == (Ops{{OP_MATCH_RUN, 20}, {OP_INSERT_RUN, 2}}));
    // The recycled slot's blocks: their shared head, then what differs.
    const std::vector<char> was = arc_block(old_a.arcs[8]), now = arc_block(new_a.arcs[8]);
    REQUIRE(was.size() == now.size());
    uint32_t head = 0;
    while (head < was.size() && was[head] == now[head]) head++;
    REQUIRE(head > 9 && head < was.size());
    uint32_t tail = 0;
    while (tail < was.size() - head && was[was.size() - 1 - tail] == now[now.size() - 1 - tail]) tail++;
    const uint32_t middle = static_cast<uint32_t>(was.size()) - head - tail;
    const MergeSection v = merge_section(raw, PatchFileId::ARC_VERTICES, 1, arc_bytes(old_a, 0, 20), arc_bytes(new_a, 0, 22));
    CHECK(v.ops == (Ops{{OP_MATCH_RUN, arc_bytes(old_a, 0, 3)},
                        {OP_INSERT_RUN, arc_bytes(new_a, 3, 4)},
                        {OP_MATCH_RUN, arc_bytes(old_a, 4, 5)},
                        {OP_DELETE_RUN, arc_bytes(old_a, 5, 6)},
                        {OP_MATCH_RUN, arc_bytes(old_a, 6, 8) + head},
                        {OP_DELETE_RUN, middle},
                        {OP_INSERT_RUN, middle},
                        {OP_MATCH_RUN, tail + arc_bytes(old_a, 9, 20)},
                        {OP_INSERT_RUN, arc_bytes(new_a, 20, 22)}}));
    for (const PatchFileId fid : {PatchFileId::ADMIN_POLYGONS, PatchFileId::ADMIN_RINGS})
        CHECK(copied_old_at(raw, fid, new_d.at(patch_file_names[uint32_t(fid)]).size()) != std::string::npos);
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// Admin polygons by slot: slot 2, a tombstone on the old day, is recycled for
// a new polygon (a REPLACE of its record and an INSERT of its ring block),
// polygon 6 dies (a REPLACE to the tombstone and a DELETE of its ring block),
// and a polygon is appended (an INSERT of both); every other slot MATCHes.
// The arc files go as unchanged, and every mode applies it. The parent sent
// the records and rings whole.
TEST(patch_admin_polygon_slots_recycled_dead_and_appended_merge_by_slot) {
    ScratchDir dir("lowmem-polygon-slots");
    ArcSlots old_a = arc_slots(30, 10, 12);
    old_a.rings[2].clear();
    ArcSlots new_a = old_a;
    new_a.rings[2] = {20, 21};
    new_a.names[2] = 500;
    new_a.rings[6].clear();
    new_a.rings.push_back({25, 26, 27, 28});
    new_a.names.push_back(600);
    BuildDir old_d, new_d;
    put_arc_slots(old_d, old_a);
    put_arc_slots(new_d, new_a);
    const LowMemCase c{dir.path(), "polygon-slots"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    const std::vector<char> raw = raw_patch(c);
    const MergeSection polygons = merge_section(raw, PatchFileId::ADMIN_POLYGONS, ADMIN_POLYGON_STRIDE,
                                                12 * ADMIN_POLYGON_STRIDE, 13 * ADMIN_POLYGON_STRIDE);
    CHECK(polygons.found);
    CHECK(polygons.ops == (Ops{{OP_MATCH_RUN, 2}, {OP_REPLACE_RUN, 1}, {OP_MATCH_RUN, 3}, {OP_REPLACE_RUN, 1},
                               {OP_MATCH_RUN, 5}, {OP_INSERT_RUN, 1}}));
    const MergeSection rings = merge_section(raw, PatchFileId::ADMIN_RINGS, 1, ring_bytes(old_a, 0, 12), ring_bytes(new_a, 0, 13));
    CHECK(rings.ops == (Ops{{OP_MATCH_RUN, ring_bytes(old_a, 0, 2)},
                            {OP_INSERT_RUN, ring_bytes(new_a, 2, 3)},
                            {OP_MATCH_RUN, ring_bytes(old_a, 3, 6)},
                            {OP_DELETE_RUN, ring_bytes(old_a, 6, 7)},
                            {OP_MATCH_RUN, ring_bytes(old_a, 7, 12)},
                            {OP_INSERT_RUN, ring_bytes(new_a, 12, 13)}}));
    for (const PatchFileId fid : {PatchFileId::ARC_INDEX, PatchFileId::ARC_VERTICES})
        CHECK(copied_old_at(raw, fid, new_d.at(patch_file_names[uint32_t(fid)]).size()) != std::string::npos);
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// Identical days: each admin geometry file goes as COPY_OLD, arc_index,
// arc_vertices, admin_polygons and admin_rings in that order, ahead of the
// admin cells (their cell changes, then their index delta). Every mode
// applies it. The parent sent them last, behind the admin cells.
TEST(patch_identical_days_copy_the_admin_geometry_ahead_of_the_admin_cells) {
    ScratchDir dir("lowmem-arcs-same");
    BuildDir d;
    put_arc_slots(d, arc_slots(40, 30, 12));
    CellLists cells;
    for (uint32_t k = 0; k < 12; k++) cells[cell_id(k)] = {k};
    put_cells(d, "admin", cells);
    const LowMemCase c{dir.path(), "same"};
    REQUIRE(prepare(c, d, d).empty());

    const std::vector<char> raw = raw_patch(c);
    std::vector<size_t> at;
    for (const AdminSlotFamily& family : ADMIN_SLOT_FAMILIES)
        for (const PatchFileId fid : {family.slots, family.blocks})
            at.push_back(copied_old_at(raw, fid, d.at(patch_file_names[uint32_t(fid)]).size()));
    std::vector<char> admin_cells;
    for (const uint32_t w : {CELL_CHANGES_ADMIN_MARKER, 0u, 0u}) append(admin_cells, w);
    at.push_back(find_once(raw, admin_cells));
    for (size_t i = 0; i < at.size(); i++) CHECK(at[i] != std::string::npos);
    CHECK(std::is_sorted(at.begin(), at.end()));
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// arc_vertices.bin's every byte new, 1 MiB of it and 10 MiB (n arcs of a
// KiB), over the same 100 polygons: its merge sends every block (head and
// tail trimmed only where a byte happens to repeat), which the patcher writes
// from the patch where it sits, so its heap stays flat in every mode. The
// parent's diff refused the dir.
TEST(patch_heap_stays_flat_as_arc_vertices_are_replaced_whole) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::mt19937 rng(20261103);
        const ArcGeometry old_g = arc_geometry(rng, 100, n, 1024);
        ArcGeometry new_g = old_g;
        for (std::vector<char>& arc : new_g.arcs) arc = random_bytes(rng, arc.size());
        BuildDir old_d, new_d;
        put_admin_minimal(old_d, old_g, 100);
        put_admin_minimal(new_d, new_g, 100);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        return patch_holds(c, section_head(PatchFileId::ARC_VERTICES, 1, uint64_t{n} << 10, uint64_t{n} << 10),
                           "merge of " + std::to_string(n) + " KiB of new arc vertices");
    }, 1024).empty());
}

// n arcs edited, 20,000 and 200,000, each a byte longer, among 2n: every
// later slot's offset moves by one byte more after each, n offset fixup runs
// of the arc index, which the patcher decodes as its MATCH records pass, and
// the arc vertices copy the old bytes between the edits. Its heap stays flat
// in every mode. The parent sent both files whole.
TEST(patch_heap_stays_flat_as_arcs_change) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::mt19937 rng(20261104);
        const ArcGeometry old_g = arc_geometry(rng, 10, 2 * n, 24);
        ArcGeometry new_g = old_g;
        for (uint32_t i = 0; i < 2 * n; i += 2) new_g.arcs[i].push_back('x');
        BuildDir old_d, new_d;
        put_arc_geometry(old_d, old_g);
        put_arc_geometry(new_d, new_g);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        const uint64_t old_size = old_d.at("arc_vertices.bin").size(), new_size = new_d.at("arc_vertices.bin").size();
        return patch_holds(c, section_head(PatchFileId::ARC_VERTICES, 1, old_size, new_size),
                           "merge of " + std::to_string(n) + " edited arcs");
    }, 20000).empty());
}

// n polygons' ring blocks edited, 20,000 and 200,000, each a byte longer,
// among 2n slots: n REPLACEs of the records (their ring_bytes), the MATCH
// records between them moved by offset fixups, and the ring blocks' merge
// along them. The patcher's heap stays flat in every mode. The parent sent
// both files whole.
TEST(patch_heap_stays_flat_as_rings_change) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::mt19937 rng(20261104);
        const ArcGeometry old_g = arc_geometry(rng, 2 * n, 10, 24);
        ArcGeometry new_g = old_g;
        for (uint32_t p = 0, edited = 0; p < 2 * n && edited < n; p++)
            if (new_g.keys[p] != NO) {
                new_g.rings[p].push_back('x');
                edited++;
            }
        BuildDir old_d, new_d;
        put_arc_geometry(old_d, old_g);
        put_arc_geometry(new_d, new_g);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        const uint64_t old_size = old_d.at("admin_rings.bin").size(), new_size = new_d.at("admin_rings.bin").size();
        if (new_size - old_size != n) return std::string("not n rings edited");
        return patch_holds(c, section_head(PatchFileId::ADMIN_RINGS, 1, old_size, new_size),
                           "merge of " + std::to_string(n) + " edited rings");
    }, 20000).empty());
}

// The low-memory admin family (layout B of admin_vertices.bin) is gone: an
// encoded set whose gc.manifest names it is refused by the patcher in either
// encoded mode and by geocoder-gc's decode, never read as a set without it.
// The parent took the family, and applied the patch beside it.
TEST(patch_lowmem_refuses_a_manifest_naming_the_admin_family) {
    ScratchDir dir("lowmem-admin-family");
    std::mt19937 rng(20261103);
    const auto pois = pois_of(rng, 40, 0, true);
    auto edited = pois;
    std::fill(edited[10].block.begin() + 10, edited[10].block.end(), 0);
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, edited);
    const LowMemCase c{dir.path(), "admin-family"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::string manifest = c.dir("old.enc") + "/gc.manifest";
    const std::vector<char> was = read_file(manifest);
    std::string text(was.begin(), was.end());
    const size_t at = text.find("gc.families poi\n");
    REQUIRE(at != std::string::npos);
    text.replace(at, 16, "gc.families poi,admin\n");
    REQUIRE(write_file(manifest, std::vector<char>(text.begin(), text.end())));
    const std::string refusal = "unknown family admin in gc.manifest";

    CHECK(not_refused(c, ENCODED_MODES, c.patch(), "admin-family", refusal).empty());
    const ToolRun decoded = run_tool({GEOCODER_GC_BIN, "decode", c.dir("old.enc"), c.dir("old.dec")}, c.log());
    CHECK(!decoded.ok);
    CHECK(logged(decoded, refusal));
}

// v14's parent-id remap is its street leg alone: one carrying v13's three
// (an admin leg before the street leg, a postcode leg after it), empty or
// with an admin pair, is refused in every mode, its stray words read as a
// section for file 0, which no patch sends. The parent read three legs and
// took the words after them as such a section. The patch as made (addresses
// on street ways that stay) applies.
TEST(patch_refuses_a_three_leg_parent_id_remap) {
    ScratchDir dir("patch-three-legs");
    BuildDir old_d, new_d, old_meta, new_meta;
    put_moved_ways(old_d, new_d, old_meta, new_meta, 0, 50);
    const LowMemCase c{dir.path(), "three-legs"};
    REQUIRE(prepare(c, old_d, new_d, old_meta, new_meta).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    std::vector<char> street_leg;
    for (const uint32_t w : {POI_PARENT_REMAP_MARKER, 0u}) append(street_leg, w);
    const size_t at = find_once(raw, street_leg);
    REQUIRE(at != std::string::npos);

    struct Case {
        std::string name;
        std::vector<uint32_t> admin_leg;
    };
    const Case cases[] = {{"empty-legs", {0}}, {"an-admin-pair", {1, 3, 4}}};
    for (const Case& e : cases) {
        std::vector<char> edited(raw.begin(), raw.begin() + at + 4), postcode_leg;
        for (const uint32_t w : e.admin_leg) append(edited, w);
        edited.insert(edited.end(), raw.begin() + at + 4, raw.begin() + at + 8);
        append(edited, 0u);
        edited.insert(edited.end(), raw.begin() + at + 8, raw.end());
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, "Unknown file 0").empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}
