// geocoder-patch end to end: small synthetic dirs as the builder writes
// them, the geocoder-diff patch between them, the old dir patched (its codecs
// in the patcher's thread and with --threads) and the output compared byte
// for byte with the new dir. Then the patcher's heap peak, in every mode, as
// a case grows tenfold; and patches and command lines the patcher must
// refuse.
#include <algorithm>
#include <array>
#include <cinttypes>
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
#include "geo_index.h"
#include "in_place_edits.h"
#include "merge_sequence.h"
#include "patch_format.h"
#include "place_test_files.h"
#include "scratch_dir.h"
#include "sequential_file_reader.h"
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
// The most vertices an address footprint's blob holds (addr_files.h); one of
// more is an escape.
constexpr uint32_t MAX_BLOB_VERTICES = static_cast<uint32_t>(addr_files::MAX_BLOB_VERTICES);
// The most bytes a family's encoder may hold queued, however large a write:
// a piece of it (64 KiB) and the largest unit the encoder takes whole. Each
// family's own, so each check is as tight as its encoder allows (one bound
// for all would be set by the largest unit of any encoder, a street way's
// 65535 nodes (512 KiB), and let a POI encoder hold 256 KiB more than it
// may):
//   POI: a block up to big_block(), 256 KiB; a larger one goes plane by
//   plane;
//   addr: a footprint a blob holds, a 10-byte header and 4-byte vertices,
//   262,150 B at MAX_BLOB_VERTICES; an escaped one goes through in pieces.
constexpr uint64_t ENCODER_PIECE = 64 << 10;
constexpr uint64_t POI_ENCODER_HOLD = ENCODER_PIECE + (256 << 10);
constexpr uint64_t ADDR_ENCODER_HOLD = ENCODER_PIECE + 10 + 4 * uint64_t(MAX_BLOB_VERTICES);
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
        append(points, a.block.empty() ? NO : addr_files::vertex_offset_of(verts.size()));
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
// a 16-byte record per slot (its ring block's offset, name_id, area, no
// country, level 8; a tombstone's name NO, its block empty at the running
// offset), the ring blocks in slot order, a u32 offset per arc slot and the
// arc blocks in slot order.
void put_arc_geometry(BuildDir& d, const ArcGeometry& g) {
    std::vector<char> recs, rings, index, arcs;
    for (size_t i = 0; i < g.keys.size(); i++) {
        const bool tombstone = g.keys[i] == NO;
        AdminPolygon p{};
        p.name_id = g.keys[i];
        p.admin_level = 8;
        p.area = tombstone ? 0.0f : static_cast<float>(g.keys[i]);
        append(recs, admin_polygon_record(p, static_cast<uint32_t>(rings.size())));
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

// The id of a fixture's geo cell k, at the street level.
uint64_t cell_id(uint32_t k) { return geo_index::cell_of(k, 17); }
// Cell k of a cell pair index, which holds cells of one level.
uint64_t pair_cell(uint32_t k) { return cell_pairs::cell_of(k, 10); }

// A geo row: its cell index and the street ids it lists.
using GeoRow = std::pair<uint32_t, std::vector<uint32_t>>;

// The geo index of rows (in cell order, street lists alone, each sorted)
// over `ways` street ways without nodes (a patch drops ids no way replay
// maps).
void put_geo(BuildDir& d, const std::vector<GeoRow>& rows, uint32_t ways = 1000) {
    std::vector<char> street_ways, top, blocks;
    for (uint32_t w = 0; w < ways; w++) {
        append(street_ways, 0u);
        append(street_ways, uint16_t{0});
        append(street_ways, uint16_t{0});
        append(street_ways, w);
    }
    geo_index::Writer writer([&](const char* p, size_t n) { top.insert(top.end(), p, p + n); },
                             [&](const char* p, size_t n) { blocks.insert(blocks.end(), p, p + n); });
    for (const auto& [k, ids] : rows) writer.add(cell_id(k), {{{ids.data(), ids.size()}, {}, {}}});
    writer.finish();
    d["street_ways.bin"] = street_ways;
    d["street_nodes.bin"] = {};
    d["geo_cells.bin"] = top;
    d["geo_entries.bin"] = blocks;
}

// The cell index <prefix>_cells.bin + <prefix>_entries.bin of lists (cell
// id → ids), into d, as the builder writes it (in the admin index's format
// for a prefix of no index's, the place index an old dir held).
void put_cells(BuildDir& d, const std::string& prefix, const CellLists& lists) {
    auto [cells, entries] = write_cell_lists(lists, cell_pairs::format_named(prefix).value_or(cell_pairs::ADMIN));
    d[prefix + "_cells.bin"] = cells;
    d[prefix + "_entries.bin"] = entries;
}

// The top bit of an admin cell index entry: the cell lies inside the polygon.
constexpr uint32_t INTERIOR = 0x80000000u;

// n_places place nodes, place i the hidden label of slot i mod the slots, or
// visible where that slot is a tombstone.
std::vector<PlaceNode> admin_minimal_places(const ArcGeometry& g, uint32_t n_places) {
    std::vector<PlaceNode> places;
    for (uint32_t i = 0; i < n_places; i++) {
        const uint32_t slot = i % static_cast<uint32_t>(g.keys.size());
        PlaceNode p = test_place(47.0f + static_cast<float>(i) * 1e-4f, 8.0f, 0);
        if (g.keys[slot] != NO) {
            p._pad1 = PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF;
            p.parent_poly_id = slot;
        }
        places.push_back(p);
    }
    return places;
}

// An admin-minimal-shaped dir (the files the builder writes there): g's
// admin geometry, n_places place nodes (admin_minimal_places), the admin
// cell index (cell k listing slot k and slot k + 1 as interior), the core
// strings and the strings layout.
void put_admin_minimal(BuildDir& d, const ArcGeometry& g, uint32_t n_places) {
    put_arc_geometry(d, g);
    put_places(d, admin_minimal_places(g, n_places));
    CellLists cells;
    for (uint32_t k = 0; k + 1 < g.keys.size(); k++) cells[pair_cell(k)] = {k, (k + 1) | INTERIOR};
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

// An old file's hash section as the diff writes it.
std::vector<char> old_file_hash(PatchFileId file, uint64_t hash) {
    std::vector<char> section;
    append(section, OLD_FILE_HASH_MARKER);
    append(section, static_cast<uint32_t>(file));
    append(section, hash);
    return section;
}

// A COPY_OLD section of a file of `size` logical bytes as the diff writes it.
std::vector<char> copy_old_section(PatchFileId file, uint64_t size) {
    std::vector<char> section;
    append(section, static_cast<uint32_t>(file));
    append(section, static_cast<uint32_t>(COPY_OLD_STRIDE));
    append(section, size);
    append(section, size);
    append(section, 0u);
    append(section, uint64_t{0});
    return section;
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

// The old and new dirs of a case under root/<name>.{old,new}, the patch
// between them, and what applying it gave.
struct LowMemCase {
    std::string root, name;
    std::string dir(const std::string& kind) const { return root + "/" + name + "." + kind; }
    std::string patch() const { return dir("gcpatch"); }
    std::string log() const { return dir("log"); }
};

// Writes old and new (and their sidecars when given) and diffs them (with
// diff_options). Empty on success, else what failed.
std::string prepare(const LowMemCase& c, const BuildDir& old_d, const BuildDir& new_d, const BuildDir& old_meta = {},
                    const BuildDir& new_meta = {}, const std::vector<std::string>& diff_options = {}) {
    if (!write_dir(c.dir("old"), old_d) || !write_dir(c.dir("new"), new_d)) return "fixture not written";
    std::vector<std::string> diff = {GEOCODER_DIFF_BIN, c.dir("old"), c.dir("new"), "-o", c.patch()};
    if (!old_meta.empty()) {
        if (!write_dir(c.dir("old-meta"), old_meta) || !write_dir(c.dir("new-meta"), new_meta)) return "sidecars not written";
        diff.insert(diff.end(), {"--identity", c.dir("old-meta"), c.dir("new-meta")});
    }
    diff.insert(diff.end(), diff_options.begin(), diff_options.end());
    auto r = run_tool(diff, c.log());
    if (!r.ok) return diff[0] + " failed: " + r.log;
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

// The modes a patch applies in: the codecs in the patcher's own thread
// ("sync"), or each in a thread of its own ("threads", with --threads).
const std::vector<std::string> ALL_MODES = {"sync", "threads"};

// The patcher's command line applying patch to c's old dir into out in the
// mode, with options.
std::vector<std::string> patch_words(const LowMemCase& c, const std::string& mode, const std::string& patch, const std::string& out,
                                     const std::vector<std::string>& options = {}) {
    std::vector<std::string> words = {GEOCODER_PATCH_BIN, c.dir("old"), patch, "-o", out};
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

// The patch applied to the old dir in each of modes and the output compared
// with the new dir, the patcher under the heap shim and given options; check
// runs on each log. Every mismatch, printed.
Applied apply_modes(const LowMemCase& c, const std::vector<std::string>& modes, const LogCheck& check = {},
                    const std::vector<std::string>& options = {}) {
    Applied a;
    // The patcher writes only into an empty dir: a case applied again starts
    // from none.
    for (const std::string& mode : modes) std::filesystem::remove_all(c.dir("out." + mode));
    for (const std::string& mode : modes) {
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
        if (read_dir(out) != read_dir(c.dir("new"))) a.bad.push_back(mode + ": the patched dir is not the new dir");
        if (check)
            if (std::string e = check(log); !e.empty()) a.bad.push_back(mode + ": " + e);
    }
    for (const auto& b : a.bad) std::printf("    %s: %s\n", c.name.c_str(), b.c_str());
    return a;
}

// A heap test's case of n elements of the one structure it varies, written
// into c as prepare() writes one: "" or what failed.
using CaseOfSize = std::function<std::string(const LowMemCase& c, uint32_t n)>;

// "" when the least of a mode's heap peaks at factor n (10 n unless said; in
// what: the mode, and the codec state the runs ended in) is at most
// HEAP_GROWTH_SLACK above the greatest at n, else how far it grew. Each size
// has a peak.
std::string peak_grew(const std::string& what, const std::vector<uint64_t>& at_n, const std::vector<uint64_t>& at_10n, uint32_t n,
                      uint32_t factor = 10) {
    const uint64_t from = *std::max_element(at_n.begin(), at_n.end()), to = *std::min_element(at_10n.begin(), at_10n.end());
    if (to <= from + HEAP_GROWTH_SLACK) return "";
    auto of_runs = [](const char* which, const std::vector<uint64_t>& v) {
        return v.size() > 1 ? " (the " + std::string(which) + " of " + std::to_string(v.size()) + " runs)" : std::string();
    };
    return what + ": the heap peak grew from " + std::to_string(from) + " B at n = " + std::to_string(n) + of_runs("greatest", at_n) + " to " +
           std::to_string(to) + " B at n = " + std::to_string(uint64_t{factor} * n) + of_runs("least", at_10n) + ", past the " +
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

ThreadsCheck threads_check(const std::vector<ThreadsRun>& runs_n, const std::vector<ThreadsRun>& runs_10n, uint32_t n,
                           uint32_t factor = 10) {
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
        const std::string grew = peak_grew("threads, codec state [" + state + "]", at_n, at_10n->second, n, factor);
        if (grew.empty()) return {{}, true};
        c.failures.push_back(grew);
        c.settled |= std::min(at_n.size(), at_10n->second.size()) >= THREADS_STATE_RUNS;
    }
    if (shared) return c;
    if (all[0].empty() || all[1].empty()) return {{}, true};
    for (std::vector<uint64_t>& peaks : all) peaks.resize(std::min(peaks.size(), size_t(THREADS_HEAP_RUNS)));
    if (const std::string grew = peak_grew("threads", all[0], all[1], n, factor); !grew.empty())
        return {{grew + ", and no codec state came up at both sizes"}, false};
    return {{}, true};
}

// The case at n and at factor n (10 n unless said), each applied in every
// mode: each output as apply_modes checks it, and in each mode the heap peak
// at factor n at most HEAP_GROWTH_SLACK above the one at n. A sync run peaks
// the same every time, so one run at each size decides; --threads runs
// THREADS_HEAP_RUNS times at each size, the sizes taking turns so that both
// see the same load, and threads_check decides, both sizes running again
// while its failures aren't settled, up to THREADS_HEAP_MAX_RUNS times.
// Every failure, printed; skipped without a heap shim (the sanitizer build).
std::vector<std::string> heap_flat(const CaseOfSize& make_case, uint32_t n, uint32_t factor = 10) {
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
    const uint32_t sizes[] = {n, factor * n};
    std::vector<LowMemCase> cases;
    for (const uint32_t size : sizes) {
        cases.push_back({dir.path(), "n" + std::to_string(size)});
        if (std::string e = make_case(cases.back(), size); !e.empty()) {
            fail(cases.back().name + ": " + e);
            return bad;
        }
    }
    // Each size's sync heap peak, and its --threads runs.
    std::map<std::string, uint64_t> peaks[2];
    std::vector<ThreadsRun> threads[2];
    for (int run = 0; run < THREADS_HEAP_MAX_RUNS && (run == 0 || bad.empty()) &&
                      (run < THREADS_HEAP_RUNS || !threads_check(threads[0], threads[1], n, factor).settled);
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
            if (const std::string grew = peak_grew(mode, {at_n}, {at_10n->second}, n, factor); !grew.empty()) fail(grew);
    for (const std::string& grew : threads_check(threads[0], threads[1], n, factor).failures) fail(grew);
    return bad;
}

std::vector<Poi> pois_of(std::mt19937& rng, uint32_t n, uint32_t vertices, bool points_too) {
    std::vector<Poi> pois;
    for (uint32_t i = 0; i < n; i++)
        pois.push_back({i, points_too && i % 3 == 0 ? std::vector<char>{} : block(rng, vertices ? vertices : 1 + rng() % 8)});
    return pois;
}

// n address points, every third without a footprint, the others with one of
// 1 to 8 random vertices, on a street of their own: the address family
// beside the street pair, into d.
void put_addr_family(BuildDir& d, std::mt19937& rng, uint32_t n) {
    std::vector<GeoRow> rows;
    for (uint32_t k = 0; k < 50; k++) rows.push_back({k, {k % 10}});
    put_geo(d, rows);
    std::vector<Addr> addrs;
    for (uint32_t i = 0; i < n; i++)
        addrs.push_back({47.0f + static_cast<float>(i) * 1e-4f, i % 3 == 0 ? std::vector<char>{} : block(rng, 1 + rng() % 8)});
    put_addrs(d, addrs);
}

}  // namespace

// Random days of POIs as strategy 2 leaves them: an old day of 400 records
// (points, polygons of every encoding at random widths, closed and open, one
// past big_block(), tombstones with and without their blocks) and a new day
// that edits fields of some, replaces the polygons of some, kills some in
// place and appends 40, a polygon past big_block() among them. In every mode
// the patcher writes the builder's POI files of the new day (write_dir
// writes them through poi_files::Writer), byte for byte.
TEST(patch_rebuilds_the_builders_poi_files_on_random_days) {
    ScratchDir dir("lowmem-poi-days");
    constexpr uint32_t NAME = 12, NAMES = 64;  // "poi 0000000\0"
    std::string pool;
    for (uint32_t i = 0; i < NAMES; i++) {
        char b[16];
        std::snprintf(b, sizeof b, "poi %07u", i);
        pool += std::string(b, NAME);
    }
    struct Poi36 {
        std::vector<char> rec;    // 36 bytes, vertex_offset and vertex_count set by dir_of
        std::vector<char> block;  // its logical polygon, or none
    };
    for (uint32_t seed = 1; seed <= 4; seed++) {
        std::mt19937 rng(20261110 + seed);
        auto string_id = [&] { return rng() % 4 == 0 ? NO : static_cast<uint32_t>(rng() % NAMES) * NAME; };
        auto polygon = [&](uint32_t vc) {
            const uint8_t e = static_cast<uint8_t>(rng() % 4);
            const uint32_t max_w = e == 3 ? 32 : 16, wl = static_cast<uint32_t>(rng() % (max_w + 1)), wg = static_cast<uint32_t>(rng() % (max_w + 1));
            auto q = [&](uint32_t w) { return w == 32 ? static_cast<uint32_t>(rng()) : static_cast<uint32_t>(rng() & ((1u << w) - 1)); };
            std::vector<char> b(10, 0);
            b[0] = static_cast<char>(e);
            put_at(b, 2, static_cast<uint32_t>(rng()));
            put_at(b, 6, static_cast<uint32_t>(rng()));
            const bool closed = vc >= 2 && rng() % 2;
            for (uint32_t k = 0; k < vc; k++) {
                const size_t at = b.size();
                if (closed && k + 1 == vc) {
                    // The first vertex again, through a copy: insert takes no range of its own vector.
                    const std::vector<char> first(b.begin() + 10, b.begin() + 10 + (e == 3 ? 8 : 4));
                    b.insert(b.end(), first.begin(), first.end());
                    continue;
                }
                b.resize(at + (e == 3 ? 8 : 4));
                poi_files::put_q(reinterpret_cast<uint8_t*>(&b[at]), e, q(wl), q(wg));
            }
            return b;
        };
        auto random_poi = [&](uint32_t big = 0) {
            Poi36 p{std::vector<char>(36, 0), {}};
            put_at(p.rec, 0, static_cast<float>(-90.0 + 180.0 * (rng() / 4294967296.0)));
            put_at(p.rec, 4, static_cast<float>(-180.0 + 360.0 * (rng() / 4294967296.0)));
            put_at(p.rec, 16, static_cast<uint32_t>(rng() % NAMES) * NAME);
            for (int k = 0; k < 4; k++) p.rec[20 + k] = static_cast<char>(k == 1 ? 1 + rng() % 3 : rng() % (k == 0 ? 55 : 256));
            put_at(p.rec, 24, string_id());
            put_at(p.rec, 28, string_id());
            put_at(p.rec, 32, rng() % 5 == 0 ? NO : static_cast<uint32_t>(rng() % 1000));
            if (big || rng() % 3) p.block = polygon(big ? big : 1 + static_cast<uint32_t>(rng() % 40));
            return p;
        };
        auto kill = [&](Poi36& p) {
            p.rec.assign(36, 0);
            put_at(p.rec, 16, NO);
            if (rng() % 2) p.block.clear();
        };
        std::vector<Poi36> old_day;
        for (uint32_t i = 0; i < 400; i++) old_day.push_back(random_poi(i == 123 ? 35000 : 0));
        for (uint32_t i = 0; i < 400; i += 37) kill(old_day[i]);
        std::vector<Poi36> new_day = old_day;
        for (Poi36& p : new_day) {
            const uint32_t r = rng() % 20;
            if (r == 0) p.rec[23] = static_cast<char>(rng());                                     // importance
            if (r == 1) put_at(p.rec, 16, static_cast<uint32_t>(rng() % NAMES) * NAME);          // name
            if (r == 2) put_at(p.rec, 28, string_id());                                           // postcode
            if (r == 3) p.block = polygon(1 + static_cast<uint32_t>(rng() % 40));                 // polygon replaced
            if (r == 4 && !p.block.empty()) p.block.clear();                                      // polygon gone
            if (r == 5) kill(p);
        }
        for (uint32_t i = 0; i < 40; i++) new_day.push_back(random_poi(i == 17 ? 40000 : 0));
        auto dir_of = [&](const std::vector<Poi36>& day) {
            BuildDir d;
            std::vector<char>& recs = d["poi_records.bin"];
            std::vector<char>& verts = d["poi_vertices.bin"];
            for (const Poi36& p : day) {
                std::vector<char> rec = p.rec;
                const bool poly = !p.block.empty();
                put_at(rec, 8, poly ? static_cast<uint32_t>(verts.size()) : NO);
                put_at(rec, 12, poly ? static_cast<uint32_t>((p.block.size() - 10) / (p.block[0] == 3 ? 8 : 4)) : 0u);
                recs.insert(recs.end(), rec.begin(), rec.end());
                verts.insert(verts.end(), p.block.begin(), p.block.end());
            }
            d["strings_poi.bin"].assign(pool.begin(), pool.end());
            return d;
        };
        const BuildDir old_d = dir_of(old_day), new_d = dir_of(new_day);
        const LowMemCase c{dir.path(), "seed-" + std::to_string(seed)};
        REQUIRE(prepare(c, old_d, new_d).empty());

        CHECK(apply_modes(c, ALL_MODES).bad.empty());
        const BuildDir want = gctest::as_written(new_d);
        for (const std::string& mode : ALL_MODES)
            for (const char* f : {"poi_records.bin", "poi_vertices.bin"}) CHECK(read_file(c.dir("out." + mode) + "/" + f) == want.at(f));
    }
}

// A day that leaves most POI polygons alone, its edits at their blocks'
// edges: names changed with the polygon kept (in place, at the same lat and
// lng), a POI moved with its polygon kept, a polygon's last vertex moved, a
// polygon replaced, dropped or given to a point, POIs killed in place and
// appended, and two polygons past big_block(), one kept and one moved. In every
// mode the patcher writes the builder's POI files of the new day, byte for
// byte, copying the blocks of the polygons kept at their place (the log counts
// them) and encoding the others, their old bytes decoded from the old blocks
// at hand. A byte flipped in a copied block of the old
// dir goes through undecoded, and the old poi_vertices.bin's hash refuses it;
// a bit flipped in the longitude of that block's row is a row like any other,
// and the old poi_records.bin's hash refuses it.
TEST(patch_copies_the_poi_blocks_a_day_leaves_whole) {
    ScratchDir dir("lowmem-poi-copy");
    constexpr uint32_t NAME = 12, NAMES = 64, N = 500;
    std::string pool;
    for (uint32_t i = 0; i < NAMES; i++) {
        char b[16];
        std::snprintf(b, sizeof b, "poi %07u", i);
        pool += std::string(b, NAME);
    }
    struct Poi36 {
        std::vector<char> rec;    // 36 bytes, vertex_offset and vertex_count set by dir_of
        std::vector<char> block;  // its logical polygon, or none
    };
    std::mt19937 rng(20261112);
    auto polygon = [&](uint32_t vc) {
        const uint8_t e = static_cast<uint8_t>(rng() % 4);
        std::vector<char> b(10, 0);
        b[0] = static_cast<char>(e);
        put_at(b, 2, static_cast<float>(-1.0 + rng() % 1000 * 1e-3));
        put_at(b, 6, static_cast<float>(-1.0 + rng() % 1000 * 1e-3));
        for (uint32_t k = 0; k < vc; k++) {
            const size_t at = b.size();
            b.resize(at + (e == 3 ? 8 : 4));
            poi_files::put_q(reinterpret_cast<uint8_t*>(&b[at]), e, static_cast<uint32_t>(rng() % 5000), static_cast<uint32_t>(rng() % 5000));
        }
        return b;
    };
    auto random_poi = [&](uint32_t vc) {
        Poi36 p{std::vector<char>(36, 0), {}};
        put_at(p.rec, 0, static_cast<float>(-90.0 + 180.0 * (rng() / 4294967296.0)));
        put_at(p.rec, 4, static_cast<float>(-180.0 + 360.0 * (rng() / 4294967296.0)));
        put_at(p.rec, 16, static_cast<uint32_t>(rng() % NAMES) * NAME);
        for (int k = 0; k < 4; k++) p.rec[20 + k] = static_cast<char>(k == 1 ? 1 + rng() % 3 : rng() % (k == 0 ? 55 : 256));
        put_at(p.rec, 24, NO);
        put_at(p.rec, 28, rng() % 4 ? NO : static_cast<uint32_t>(rng() % NAMES) * NAME);
        put_at(p.rec, 32, static_cast<uint32_t>(rng() % 1000));
        if (vc) p.block = polygon(vc);
        return p;
    };
    std::vector<Poi36> old_day;
    for (uint32_t i = 0; i < N; i++) old_day.push_back(random_poi(i == 200 || i == 300 ? 35000 : i % 4 == 0 ? 0 : 1 + rng() % 40));
    std::vector<Poi36> new_day = old_day;
    uint32_t kept_in_place = 0;  // polygons the new day keeps at their place
    for (uint32_t i = 0; i < N; i++) {
        Poi36& p = new_day[i];
        const bool poly = !p.block.empty();
        const uint32_t r = i == 300 ? 1 : i == 200 ? 99 : rng() % 16;
        if (r == 0) put_at(p.rec, 16, static_cast<uint32_t>(rng() % NAMES) * NAME);      // renamed, kept
        if (r == 1) put_at(p.rec, 0, static_cast<float>(p.rec[0] == 0 ? 1.5 : -1.5));       // moved
        if (r == 2 && poly) p.block[p.block.size() - 1] ^= 1;                               // last vertex moved
        if (r == 3 && poly) p.block = polygon(1 + rng() % 40);                              // replaced
        if (r == 4) p.block.clear();                                                        // dropped
        if (r == 5 && !poly) p.block = polygon(1 + rng() % 40);                             // given one
        if (r == 6) {                                                                       // killed in place
            p.rec.assign(36, 0);
            put_at(p.rec, 16, NO);
            p.block.clear();
        }
        kept_in_place += poly && (r == 0 || r == 5 || r > 6);
    }
    for (uint32_t i = 0; i < 30; i++) new_day.push_back(random_poi(i % 3 ? 1 + rng() % 40 : 0));
    auto dir_of = [&](const std::vector<Poi36>& day) {
        BuildDir d;
        std::vector<char>& recs = d["poi_records.bin"];
        std::vector<char>& verts = d["poi_vertices.bin"];
        for (const Poi36& p : day) {
            std::vector<char> rec = p.rec;
            const bool poly = !p.block.empty();
            put_at(rec, 8, poly ? static_cast<uint32_t>(verts.size()) : NO);
            put_at(rec, 12, poly ? static_cast<uint32_t>((p.block.size() - 10) / (p.block[0] == 3 ? 8 : 4)) : 0u);
            recs.insert(recs.end(), rec.begin(), rec.end());
            verts.insert(verts.end(), p.block.begin(), p.block.end());
        }
        d["strings_poi.bin"].assign(pool.begin(), pool.end());
        return d;
    };
    const BuildDir old_d = dir_of(old_day), new_d = dir_of(new_day);
    const LowMemCase c{dir.path(), "copy"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    // Every mode's own counts. The polygons kept at their place are copied
    // (the one past big_block() too) but for those the vertex merge resyncs
    // on past an edit before them (their first bytes go as the patch's), and
    // the others are encoded. The old vertex bytes the encoded polygons keep
    // come from the old block the replay is at, or from one it passed just
    // before (where the vertex merge matched a few), none through the vertex
    // cursor.
    uint64_t copied = 0, encoded = 0, at_hand = 0, by_cursor = 0;
    const LogCheck counts = [&](const std::string& log) {
        const size_t at = log.find("replayed with poi_records.bin: blocks ");
        if (at == std::string::npos) return std::string("no block counts in the log");
        if (std::sscanf(log.c_str() + at,
                        "replayed with poi_records.bin: blocks %" SCNu64 " copied, %" SCNu64 " encoded; old vertex bytes %" SCNu64
                        " at hand, %" SCNu64 " by the cursor",
                        &copied, &encoded, &at_hand, &by_cursor) != 4)
            return std::string("no block counts in the log");
        const std::string got = std::to_string(copied) + " copied, " + std::to_string(encoded) + " encoded, " +
                                std::to_string(at_hand) + " old vertex bytes at hand, " + std::to_string(by_cursor) + " by the cursor";
        if (copied > kept_in_place || copied * 10 < uint64_t(kept_in_place) * 9)
            return "not nine in ten of the " + std::to_string(kept_in_place) + " polygons kept in place copied: " + got;
        if (encoded <= 30) return "30 polygons or fewer encoded: " + got;
        if (at_hand == 0) return "no old vertex bytes at hand: " + got;
        if (by_cursor != 0) return "old vertex bytes read through the cursor: " + got;
        return std::string();
    };
    CHECK(apply_modes(c, ALL_MODES, counts).bad.empty());
    const BuildDir want = gctest::as_written(new_d);
    for (const std::string& mode : ALL_MODES)
        for (const char* f : {"poi_records.bin", "poi_vertices.bin"}) CHECK(read_file(c.dir("out." + mode) + "/" + f) == want.at(f));
    std::printf("    %u polygons kept in place: %" PRIu64 " copied, %" PRIu64 " encoded; old vertex bytes %" PRIu64
                " at hand, %" PRIu64 " by the cursor (the last mode's)\n",
                kept_in_place, copied, encoded, at_hand, by_cursor);

    // A byte flipped in the kept big polygon's block (record 200's, the first
    // vertex past its header): copied, and refused by the hash.
    std::vector<char> v2 = read_file(c.dir("old") + "/poi_vertices.bin");
    const std::vector<char> r2 = read_file(c.dir("old") + "/poi_records.bin");
    uint32_t voff;
    memcpy(&voff, r2.data() + 200 * poi_files::ROW_BYTES + poi_files::ROW_VOFF, 4);
    v2[voff + poi_files::MAX_BLOCK_HEADER + 100] ^= 0x10;
    REQUIRE(write_file(c.dir("old") + "/poi_vertices.bin", v2.data(), v2.size()));
    std::filesystem::remove_all(c.dir("out.flipped"));
    const auto r = run_tool(patch_words(c, "sync", c.patch(), c.dir("out.flipped")), c.log());
    CHECK(!r.ok);
    CHECK(r.log.find("Old poi_vertices.bin is not the one the patch was made from") != std::string::npos);
    v2[voff + poi_files::MAX_BLOCK_HEADER + 100] ^= 0x10;
    REQUIRE(write_file(c.dir("old") + "/poi_vertices.bin", v2.data(), v2.size()));

    std::vector<char> moved = r2;
    moved[200 * poi_files::ROW_BYTES + poi_files::ROW_LAT + 4] ^= 0x10;
    REQUIRE(write_file(c.dir("old") + "/poi_records.bin", moved.data(), moved.size()));
    CHECK(not_refused(c, ALL_MODES, c.patch(), "moved", "Old poi_records.bin is not the one the patch was made from").empty());
}

// The hashes of the old poi_records.bin and poi_vertices.bin, which the diff
// writes once each, the records' and then the vertices', right before the POI
// records' section. In every mode a patch with a second hash of either (the
// same hash or another, before it or after) is refused, and so is the hash of
// a file no replay checks: place_nodes.bin, or a file id past the table. A
// patch whose POI replay would copy blocks without either hash is refused
// too. The patch as made applies.
TEST(patch_refuses_a_second_hash_of_a_file_and_a_hash_for_a_file_outside_the_hashed_set) {
    ScratchDir dir("patch-old-file-hash");
    std::mt19937 rng(20261114);
    const auto pois = pois_of(rng, 300, 0, true);
    // A block edited and a POI appended: the records change, so they replay.
    auto edited = pois;
    std::fill(edited[100].block.begin() + 10, edited[100].block.end(), 0);
    edited.push_back({300, block(rng, 5)});
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, edited);
    const LowMemCase c{dir.path(), "hash"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    struct Case {
        std::string name;
        size_t at;
        std::vector<char> section;
        std::string refusal;
    };
    std::vector<Case> cases;
    size_t records_hash_end = 0;
    for (const PatchFileId hashed : {PatchFileId::POI_RECORDS, PatchFileId::POI_VERTICES}) {
        const std::string file = patch_file_names[static_cast<uint32_t>(hashed)];
        const std::vector<char> bytes = read_file(c.dir("old") + "/" + file);
        const uint64_t hash = lane_hash(bytes.data(), bytes.size());
        const std::vector<char> made = old_file_hash(hashed, hash);
        const size_t at = find_once(raw, made);
        if (at == std::string::npos) std::printf("    no hash of the old %s\n", file.c_str());
        REQUIRE(at != std::string::npos);
        const size_t end = at + made.size();
        // The vertices' hash follows the records', and the POI records' section the two.
        if (hashed == PatchFileId::POI_RECORDS) records_hash_end = end;
        else CHECK_EQ(at, records_hash_end);
        const std::string second = "Malformed patch: a second hash of the old " + file;
        cases.push_back({file + "-twice-after", end, made, second});
        cases.push_back({file + "-twice-before", at, made, second});
        cases.push_back({file + "-another-hash", end, old_file_hash(hashed, hash ^ 1), second});
        if (hashed == PatchFileId::POI_VERTICES) {
            cases.push_back({"places", at, old_file_hash(PatchFileId::PLACE_NODES, hash),
                             "Malformed patch: a hash of the old place_nodes.bin, which no replay checks"});
            cases.push_back({"past-the-table", at, old_file_hash(PatchFileId::COUNT, hash),
                             "Malformed patch: a hash of the old file " + std::to_string(static_cast<uint32_t>(PatchFileId::COUNT)) +
                                 ", which no replay checks"});
        }
        std::vector<char> cut = raw;
        cut.erase(cut.begin() + static_cast<std::ptrdiff_t>(at), cut.begin() + static_cast<std::ptrdiff_t>(end));
        const std::string patch = c.dir(file + "-cut.gcpatch");
        REQUIRE(write_patch(c, patch, cut));

        CHECK(not_refused(c, ALL_MODES, patch, file + "-cut", "Malformed patch: POI records without the old " + file + "'s hash").empty());
    }
    for (const Case& e : cases) {
        std::vector<char> edited_patch = raw;
        edited_patch.insert(edited_patch.begin() + static_cast<std::ptrdiff_t>(e.at), e.section.begin(), e.section.end());
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited_patch));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A day that leaves both POI files as they are: the diff sends the hashes of
// the old poi_records.bin and poi_vertices.bin before the two unchanged
// sections all the same, and the patcher, which copies the family as its
// files sit, holds each file to its hash first. A patch with another hash in
// the place of either is refused in every mode.
TEST(patch_holds_an_unchanged_poi_family_to_its_hashes) {
    ScratchDir dir("patch-unused-poi-hash");
    std::mt19937 rng(20261115);
    BuildDir d;
    put_pois(d, pois_of(rng, 300, 0, true));
    const LowMemCase c{dir.path(), "unused"};
    REQUIRE(prepare(c, d, d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    std::vector<char> sections;
    for (const char* name : {"poi_records.bin", "poi_vertices.bin"}) {
        const std::vector<char> bytes = read_file(c.dir("old") + "/" + name);
        const std::vector<char> hash =
            old_file_hash(name == std::string("poi_records.bin") ? PatchFileId::POI_RECORDS : PatchFileId::POI_VERTICES,
                          lane_hash(bytes.data(), bytes.size()));
        sections.insert(sections.end(), hash.begin(), hash.end());
    }
    for (const auto& [file, name] : {std::pair<PatchFileId, const char*>{PatchFileId::POI_RECORDS, "poi_records.bin"},
                                     {PatchFileId::POI_VERTICES, "poi_vertices.bin"}}) {
        const std::vector<char> unchanged = copy_old_section(file, d.at(name).size());
        sections.insert(sections.end(), unchanged.begin(), unchanged.end());
    }
    const size_t at = find_once(raw, sections);
    REQUIRE(at != std::string::npos);

    const LogCheck no_replay = [](const std::string& log) {
        return log.find("replayed with poi_records.bin: blocks ") == std::string::npos ? std::string() : std::string("the POI replay ran");
    };
    CHECK(apply_modes(c, ALL_MODES, no_replay).bad.empty());
    // Each hash section is its marker, its file and the hash: 16 bytes.
    const char* const files[] = {"poi_records.bin", "poi_vertices.bin"};
    for (size_t k = 0; k < 2; k++) {
        std::vector<char> another = raw;
        another[at + 16 * k + 8] ^= 1;
        REQUIRE(write_patch(c, c.patch(), another));
        CHECK(not_refused(c, ALL_MODES, c.patch(), files[k], std::string("Old ") + files[k] + " is not the one the patch was made from").empty());
    }
}

// Random days of the five string tiers: old pools of random strings (empty
// ones, one of the longest length a tier holds, runs of names sharing a
// prefix, every other tier sorted), and new days that delete, edit and add
// strings, one tier empty on the old day and one on the new. In every mode the
// patcher writes the builder's tier files of the new day (string_files.h:
// FCC2, encoded front to back with the bucket table set aside), byte for
// byte.
TEST(patch_rebuilds_the_builders_string_tiers_on_random_days) {
    ScratchDir dir("lowmem-string-days");
    for (uint32_t seed = 1; seed <= 3; seed++) {
        std::mt19937 rng(20261111 + seed);
        auto name = [&] {
            const uint32_t r = rng() % 40;
            if (r == 0) return std::string();
            std::string s = "Stra\xC3\x9F" "e " + std::to_string(rng() % 50) + " " + std::to_string(rng() % (r < 20 ? 30 : 100000));
            return s;
        };
        BuildDir old_d, new_d;
        for (int t = 0; t < STRING_TIER_COUNT; t++) {
            const bool sorted = t % 2 == 0;
            std::vector<std::string> o;
            const bool old_empty = t == int(seed), new_empty = t == int(seed + 1) % STRING_TIER_COUNT;
            if (!old_empty)
                for (uint32_t i = 0, n = 200 + rng() % 3000; i < n; i++) o.push_back(name());
            if (!old_empty && t == 2) o.insert(o.begin() + o.size() / 2, std::string(string_files::MAX_STRING, 'L'));
            std::vector<std::string> n;
            if (!new_empty)
                for (const std::string& w : o) {
                    const uint32_t r = rng() % 30;
                    if (r < 3) continue;                     // deleted
                    if (r == 3) n.push_back(w + "x");        // edited
                    else n.push_back(w);
                    if (r == 4 || r == 5) n.push_back(name());  // added
                }
            if (!new_empty && n.empty()) n.push_back(name());
            for (std::vector<std::string>* v : {&o, &n})
                if (sorted) {
                    std::sort(v->begin(), v->end());
                    v->erase(std::unique(v->begin(), v->end()), v->end());
                }
            auto pool = [](const std::vector<std::string>& v) {
                std::vector<char> p;
                for (const std::string& w : v) p.insert(p.end(), w.c_str(), w.c_str() + w.size() + 1);
                return p;
            };
            old_d[STRING_TIER_FILES[t]] = pool(o);
            new_d[STRING_TIER_FILES[t]] = pool(n);
        }
        const LowMemCase c{dir.path(), "seed-" + std::to_string(seed)};
        REQUIRE(prepare(c, old_d, new_d).empty());

        CHECK(apply_modes(c, ALL_MODES).bad.empty());
        for (const std::string& mode : ALL_MODES)
            for (const char* f : STRING_TIER_FILES) {
                const std::vector<char>& pool = new_d.at(f);
                const std::vector<uint8_t> built = string_files::encode(pool.data(), pool.size());
                CHECK(read_file(c.dir("out." + mode) + "/" + f) == std::vector<char>(built.begin(), built.end()));
            }
    }
}

// An unchanged parent of an encoded family (poi_records.bin sent as kept):
// both files unchanged, which are copied, then the records unchanged and one
// vertex block edited, which replay together.
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

        CHECK(apply_modes(c, ALL_MODES).bad.empty());
    }
}

namespace {

// An encoded family by the name its codecs report under, and its files as a
// dir holds them.
struct FamilyFiles {
    const char* family;
    std::vector<const char*> files;
};
const std::vector<FamilyFiles> FAMILY_FILES = {
    {"streets", {"street_ways.bin", "street_nodes.bin"}},
    {"interps", {"interp_ways.bin", "interp_nodes.bin"}},
    {"addr", {"addr_points.bin", "addr_vertices.bin", "addr_index.bin"}},
    {"poi", {"poi_records.bin", "poi_vertices.bin"}},
    {"places", {"place_cells.bin", "place_nodes.bin", "place_hidden.bin"}},
    {"way_parents", {"way_parents.bin"}},
    {"addr_postcodes", {"addr_postcodes.bin"}},
};

// n place nodes, every fourth the hidden label of a polygon.
std::vector<PlaceNode> some_places(uint32_t n) {
    std::vector<PlaceNode> places;
    for (uint32_t i = 0; i < n; i++) {
        PlaceNode p = test_place(47.0f + static_cast<float>(i) * 1e-4f, 8.0f, i);
        if (i % 4 == 3) {
            p._pad1 = PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF;
            p.parent_poly_id = i;
        }
        places.push_back(p);
    }
    return places;
}

// A dir of every encoded family: 40 street ways of two nodes in a geo cell,
// with a parent each; 30 interpolations of three nodes; 60 address points,
// two in three with a footprint, with a postcode each; 50 POIs, two in three
// polygons; 20 place nodes.
BuildDir every_family(std::mt19937& rng) {
    BuildDir d, meta;
    std::vector<uint32_t> names;
    for (uint32_t w = 0; w < 40; w++) names.push_back(w % 7);
    put_named_ways(d, meta, names);
    std::vector<char> interps, interp_nodes, parents, postcodes;
    for (uint32_t i = 0; i < 30; i++) {
        append(interps, InterpWay{3 * i, 3, 0, i % 5, 1 + 2 * i, 21 + 2 * i, 1});
        for (uint32_t k = 0; k < 3; k++) {
            append(interp_nodes, 47.5f + static_cast<float>(i) * 1e-3f + static_cast<float>(k) * 1e-4f);
            append(interp_nodes, 8.5f + static_cast<float>(k) * 1e-4f);
        }
    }
    d["interp_ways.bin"] = interps;
    d["interp_nodes.bin"] = interp_nodes;
    std::vector<Addr> addrs;
    for (uint32_t i = 0; i < 60; i++) {
        addrs.push_back({47.0f + static_cast<float>(i) * 1e-4f, i % 3 == 0 ? std::vector<char>{} : block(rng, 1 + rng() % 8)});
        append(postcodes, i / 3 * 8);
    }
    put_addrs(d, addrs);
    for (uint32_t w = 0; w < 40; w++) append(parents, w % 9);
    d["way_parents.bin"] = parents;
    d["addr_postcodes.bin"] = postcodes;
    put_pois(d, pois_of(rng, 50, 0, true));
    put_places(d, some_places(20));
    return d;
}

// The patcher's log of a day that changes the families of `replayed` and no
// other: each of those went through its decoder and its encoder (their
// report lines), and every other family had no codec run and each of its
// files, as `written` holds them, copied from the old dir at its size there.
// "" or what is wrong.
LogCheck copied_all_but(const std::vector<std::string>& replayed, const BuildDir& written) {
    return [replayed, written](const std::string& log) -> std::string {
        for (const FamilyFiles& f : FAMILY_FILES) {
            const bool decoded = log.find(std::string("decode ") + f.family + ":") != std::string::npos;
            const bool encoded = log.find(std::string("encode ") + f.family + ":") != std::string::npos;
            if (std::find(replayed.begin(), replayed.end(), f.family) != replayed.end()) {
                if (!decoded || !encoded) return std::string("no codec of ") + f.family + " ran";
                continue;
            }
            if (decoded || encoded) return std::string("a codec of ") + f.family + " ran";
            for (const char* file : f.files) {
                const std::string line =
                    std::string("  ") + file + ": unchanged (copied " + std::to_string(written.at(file).size()) + " bytes from old)";
                if (log.find(line) == std::string::npos) return "no \"" + line + "\"";
            }
        }
        return "";
    };
}

// raw without the client list's entry of `file` (the list follows the magic,
// the version and the flags: its marker, its count, then each file's name
// length, name, size and inline byte); nothing when it lists no such file.
std::vector<char> unlisted(const std::vector<char>& raw, const std::string& file) {
    const size_t list_at = 16;
    uint32_t marker = 0, listed = 0;
    if (raw.size() < list_at + 8) return {};
    memcpy(&marker, raw.data() + list_at, 4);
    memcpy(&listed, raw.data() + list_at + 4, 4);
    std::vector<char> entry;
    append(entry, static_cast<uint16_t>(file.size()));
    entry.insert(entry.end(), file.begin(), file.end());
    const size_t at = find_once(raw, entry);
    if (marker != CLIENT_FILES_MARKER || at == std::string::npos) return {};
    std::vector<char> cut = raw;
    cut.erase(cut.begin() + static_cast<std::ptrdiff_t>(at), cut.begin() + static_cast<std::ptrdiff_t>(at + entry.size() + 8 + 1));
    put_at(cut, list_at + 4, listed - 1);
    return cut;
}

}  // namespace

// A family the day leaves as it is, each of its sections sent as unchanged
// (a way pair's, the address files', the POI files': the records' section and
// the child's after it; the place files' and a side file's: their one
// section), is copied from the old dir as its files sit there, the files no
// section names with them (addr_index.bin; place_cells.bin and
// place_hidden.bin), and none of its codecs runs: on a day that changes
// nothing, and beside each family in turn that a day does change, in its
// records alone or in its child alone, which goes through its codecs. In
// every mode the patched dir is the new dir.
TEST(patch_copies_a_family_both_of_whose_sections_are_unchanged) {
    ScratchDir dir("lowmem-family-copy");
    std::mt19937 rng(20261115);
    const BuildDir base = every_family(rng);
    struct Day {
        std::string name;
        std::vector<std::string> replayed;
        std::function<void(BuildDir&)> edit;
    };
    const Day days[] = {
        {"same", {}, [](BuildDir&) {}},
        {"street-renamed", {"streets"},
         [](BuildDir& d) { put_at(d["street_ways.bin"], 7 * WAY_HEADER_STRIDE + WAY_HEADER_NAME_ID_OFF, 1000u); }},
        {"street-node-moved", {"streets"}, [](BuildDir& d) { put_at(d["street_nodes.bin"], 11 * 8, 46.0f); }},
        {"interp-renamed", {"interps"},
         [](BuildDir& d) { put_at(d["interp_ways.bin"], 4 * INTERP_WAY_STRIDE + INTERP_WAY_STREET_ID_OFF, 1000u); }},
        {"interp-node-moved", {"interps"}, [](BuildDir& d) { put_at(d["interp_nodes.bin"], 20 * 8, 46.0f); }},
        {"address-moved", {"addr"}, [](BuildDir& d) { put_at(d["addr_points.bin"], 10 * ADDR_POINT_STRIDE + 4, 9.0f); }},
        {"footprint-edited", {"addr"}, [](BuildDir& d) { d["addr_vertices.bin"][10] ^= 1; }},
        {"poi-edited", {"poi"}, [](BuildDir& d) { d["poi_records.bin"][20 * POI_RECORD_STRIDE + offsetof(PoiRecord, importance)] ^= 1; }},
        {"poi-polygon-edited", {"poi"}, [](BuildDir& d) { d["poi_vertices.bin"][10] ^= 1; }},
        {"place-renamed", {"places"},
         [](BuildDir& d) {
             std::vector<PlaceNode> places = some_places(20);
             places[5].name_id = 1000;
             put_places(d, places);
         }},
        {"way-parent", {"way_parents"}, [](BuildDir& d) { put_at(d["way_parents.bin"], 11 * 4, 99u); }},
        {"postcode", {"addr_postcodes"}, [](BuildDir& d) { put_at(d["addr_postcodes.bin"], 11 * 4, 800u); }},
    };
    for (const Day& day : days) {
        BuildDir new_d = base;
        day.edit(new_d);
        REQUIRE((new_d == base) == day.replayed.empty());
        const LowMemCase c{dir.path(), day.name};
        REQUIRE(prepare(c, base, new_d).empty());

        const Applied a = apply_modes(c, ALL_MODES, copied_all_but(day.replayed, gctest::as_written(new_d)));
        if (!a.bad.empty()) std::printf("    %s\n", day.name.c_str());
        CHECK(a.bad.empty());
    }
}

// A family a day leaves unchanged is copied as its files sit, and each of its
// files the patch gives a hash of is held to it first: with one byte of such
// a file changed, its size kept, no mode applies the patch.
TEST(patch_holds_the_files_of_a_family_it_copies_to_their_hashes) {
    ScratchDir dir("lowmem-copied-hashes");
    std::mt19937 rng(20261116);
    const BuildDir d = every_family(rng);
    const LowMemCase c{dir.path(), "copied"};
    REQUIRE(prepare(c, d, d).empty());
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
    for (const PatchFileId file : HASHED_OLD_FILES) {
        const std::string name = patch_file_names[static_cast<uint32_t>(file)];
        const std::string path = c.dir("old") + "/" + name;
        const std::vector<char> written = read_file(path);
        REQUIRE(written.size() > 8);
        std::vector<char> damaged = written;
        damaged[written.size() / 2] ^= 1;
        REQUIRE(write_file(path, damaged));
        CHECK(not_refused(c, ALL_MODES, c.patch(), name, "Old " + name + " is not the one the patch was made from").empty());
        REQUIRE(write_file(path, written));
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// What the patcher checks of an old family it copies as its files sit,
// refused in every mode: old files whose footers give the child stream
// another size than the patch (a street way of three nodes for one of two:
// the records' bytes are as many), an address family without its index, a
// place family without its cells file, and in the patch the family's sections
// a second time, the records' section without the child's after it, and a
// client list without a file the family holds that no section names. The
// patch as made applies.
TEST(patch_refuses_what_it_can_check_of_a_family_it_copies) {
    ScratchDir dir("lowmem-family-copy-checks");
    std::mt19937 rng(20261115);
    const BuildDir base = every_family(rng);
    const LowMemCase c{dir.path(), "same"};
    REQUIRE(prepare(c, base, base).empty());
    const BuildDir written = gctest::as_written(base);
    const LogCheck copied = copied_all_but({}, written);
    CHECK(apply_modes(c, ALL_MODES, copied).bad.empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());

    // The street pair's two sections, the nodes' after the ways'.
    const std::vector<char> ways = copy_old_section(PatchFileId::STREET_WAYS, base.at("street_ways.bin").size()),
                            nodes = copy_old_section(PatchFileId::STREET_NODES, base.at("street_nodes.bin").size());
    std::vector<char> pair = ways;
    pair.insert(pair.end(), nodes.begin(), nodes.end());
    const size_t at = find_once(raw, pair);
    REQUIRE(at != std::string::npos);
    std::vector<char> twice = raw, childless = raw;
    twice.insert(twice.begin() + static_cast<std::ptrdiff_t>(at), pair.begin(), pair.end());
    childless.erase(childless.begin() + static_cast<std::ptrdiff_t>(at + ways.size()),
                    childless.begin() + static_cast<std::ptrdiff_t>(at + pair.size()));
    struct Edit {
        std::string name;
        std::vector<char> patch;
        std::string refusal;
    };
    for (const Edit& e : {Edit{"twice", twice, "old street_ways.bin opened twice"},
                          Edit{"childless", childless, "Encoded family: street_nodes.bin does not follow its parent in the patch"},
                          Edit{"index-unlisted", unlisted(raw, "addr_index.bin"), "addr: the patch lists no addr_index.bin"},
                          Edit{"cells-unlisted", unlisted(raw, "place_cells.bin"), "places: the patch lists no place_cells.bin"},
                          Edit{"hidden-unlisted", unlisted(raw, "place_hidden.bin"), "places: the patch lists no place_hidden.bin"}}) {
        REQUIRE(!e.patch.empty());
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, e.patch));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    // Nothing of a family is written before its list is checked.
    for (const std::string& mode : ALL_MODES) {
        CHECK(!std::filesystem::exists(c.dir("index-unlisted.out." + mode) + "/addr_points.bin"));
        CHECK(!std::filesystem::exists(c.dir("cells-unlisted.out." + mode) + "/place_nodes.bin"));
    }

    auto put = [&](const std::string& name, const std::optional<std::vector<char>>& bytes) {
        const std::string path = c.dir("old") + "/" + name;
        std::filesystem::remove(path);
        if (bytes) REQUIRE(write_file(path, *bytes));
    };
    BuildDir longer = {{"street_ways.bin", base.at("street_ways.bin")}, {"street_nodes.bin", base.at("street_nodes.bin")}};
    put_at(longer["street_ways.bin"], 39 * WAY_HEADER_STRIDE + 4, uint16_t{3});
    for (const float v : {47.002f, 8.002f}) append(longer["street_nodes.bin"], v);
    const BuildDir stale = gctest::as_written(longer);
    for (const char* file : {"street_ways.bin", "street_nodes.bin"}) put(file, stale.at(file));
    CHECK(not_refused(c, ALL_MODES, c.patch(), "stale-nodes", "old street_nodes.bin decodes to 648 bytes, the patch expects 640").empty());
    for (const char* file : {"street_ways.bin", "street_nodes.bin"}) put(file, written.at(file));

    put("addr_index.bin", std::nullopt);
    CHECK(not_refused(c, ALL_MODES, c.patch(), "no-index",
                      "Malformed address files: a dir with some of addr_points.bin, addr_vertices.bin and addr_index.bin and not all three")
              .empty());
    put("addr_index.bin", written.at("addr_index.bin"));

    // A file no footer of the family counts: missed as it is copied.
    put("place_cells.bin", std::nullopt);
    CHECK(not_refused(c, ALL_MODES, c.patch(), "no-cells", "place_cells.bin: copy-old failed (src MISSING)").empty());
    put("place_cells.bin", written.at("place_cells.bin"));

    CHECK(apply_modes(c, ALL_MODES, copied).bad.empty());
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

        CHECK(apply_modes(c, ALL_MODES, decoder_peaks_at_most("poi", DECODER_HOLD)).bad.empty());
    }
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
    CHECK(apply_modes(c, ALL_MODES, decoder_peaks_at_most("poi", DECODER_HOLD)).bad.empty());
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
    CHECK(apply_modes(c, ALL_MODES, decoder_peaks_at_most("poi", DECODER_HOLD)).bad.empty());
}

// A set whose address files hold no address, their three footers: the diff
// sends addr_points.bin's (empty) merge with the streets after it and
// addr_vertices.bin later, as a full replacement of no bytes. An empty parent
// has no child bytes to replay with it.
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

    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A parent's REPLACE run (the same street ways, renamed) whose children stay
// one MATCH run: the replay holds a group's row of the old records, not the
// whole run (2.4 MB of them).
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

    CHECK(apply_modes(c, ALL_MODES, decoder_peaks_at_most("streets", DECODER_HOLD)).bad.empty());
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

    CHECK(apply_modes(c, ALL_MODES, encoder_peaks_at_most("poi", POI_ENCODER_HOLD)).bad.empty());
}

// Two escaped address footprints (one in encoding 3, 800K vertices: 6.4 MB;
// one of a vertex more than a blob holds) kept between small ones and ones of
// MAX_BLOB_VERTICES while the address before them goes, so the replay MATCHes
// them and moves their vertex_offsets; and another of MAX_BLOB_VERTICES
// inserted, which reaches the encoder in one write of most of it: the decoder
// hands the escaped ones over and the encoder takes them in pieces, neither
// holding more than DECODER_HOLD, nor the encoder more than a piece and the
// largest footprint a blob holds, which it takes whole. In every mode.
TEST(patch_lowmem_copies_an_escaped_footprint_through_in_pieces) {
    ScratchDir dir("lowmem-footprint");
    std::mt19937 rng(20261030);
    std::vector<Addr> addrs;
    for (uint32_t i = 0; i < 300; i++)
        addrs.push_back({47.0f + static_cast<float>(i) * 1e-4f, i % 3 == 2 ? std::vector<char>{} : block(rng, 3 + rng() % 8)});
    std::vector<char> escaped = block(rng, 2 * 800000);
    escaped[0] = 3;
    addrs[150].block = escaped;
    addrs[151].block = block(rng, MAX_BLOB_VERTICES + 1);
    for (const uint32_t i : {60u, 61u, 240u}) addrs[i].block = block(rng, MAX_BLOB_VERTICES);
    std::vector<Addr> fewer_addrs(addrs.begin() + 1, addrs.end());
    fewer_addrs.insert(fewer_addrs.begin() + 200, Addr{48.0f, block(rng, MAX_BLOB_VERTICES)});
    BuildDir old_d, new_d;
    put_addrs(old_d, addrs);
    put_addrs(new_d, fewer_addrs);
    const LowMemCase c{dir.path(), "escaped-footprint"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const LogCheck decoder = decoder_peaks_at_most("addr", DECODER_HOLD), encoder = encoder_peaks_at_most("addr", ADDR_ENCODER_HOLD);

    CHECK(apply_modes(c, ALL_MODES, [&](const std::string& log) {
              const std::string e = decoder(log);
              return e.empty() ? encoder(log) : e;
          }).bad.empty());
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
        append(corrections, static_cast<uint32_t>(PatchFileId::STREET_LISTS));
        append(corrections, n);
        return patch_holds(c, corrections, std::to_string(n) + " street entry corrections");
    }, 20000).empty());
}

// n geo cells removed and n added (the old cells are the even ones, the new
// cells the odd ones, each listing a street): cell changes of n and n cells,
// which the patcher reads where they sit in the patch.
TEST(patch_heap_stays_flat_as_geo_cells_change) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::vector<GeoRow> old_rows, new_rows;
        for (uint32_t k = 0; k < n; k++) {
            old_rows.push_back({2 * k, {k % 950}});
            new_rows.push_back({2 * k + 1, {k % 950}});
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

// The patcher's walk over a geo index of 60,000 cells whose street lists
// all change (a blocks file of ~190 KiB, past a read window) holds a
// read window for the old blocks and a few pages besides: the old top file
// and the group footers, read 12 and 768 bytes at a time, each through a
// page, and the new blocks handed to the output's FILE buffer a page at a
// time. Its heap peak is at most a read window and 48 KiB above that of the
// same patch over an empty geo index. It was 258 KiB above it when the top
// and footer readers had read windows of their own and the writer held
// 64 KiB of blocks. Skipped without a heap shim (the sanitizer build).
TEST(patch_heap_of_the_geo_walk_is_a_read_window_and_a_few_pages) {
    if (!gctest::heap_peak_shim()) {
        std::printf("    SKIP heap peaks: no heap shim under the sanitizers\n");
        return;
    }
    ScratchDir dir("lowmem-heap-geo-walk");
    const uint32_t sizes[] = {0, 60000};
    uint64_t peaks[2] = {}, blocks[2] = {};
    for (size_t s = 0; s < 2; s++) {
        const LowMemCase c{dir.path(), "n" + std::to_string(sizes[s])};
        std::vector<GeoRow> rows, changed;
        for (uint32_t k = 0; k < sizes[s]; k++) {
            rows.push_back({k, {k % 950}});
            changed.push_back({k, {k % 950 + 1}});
        }
        BuildDir old_d, new_d;
        put_geo(old_d, rows);
        put_geo(new_d, changed);
        REQUIRE(prepare(c, old_d, new_d).empty());
        blocks[s] = read_file(c.dir("old") + "/geo_entries.bin").size();
        if (s == 1) REQUIRE(blocks[s] > 2 * SEQUENTIAL_READ_BUFFER_BYTES);
        const Applied a = apply_modes(c, {"sync"});
        CHECK(a.bad.empty());
        REQUIRE(a.heap_peaks.count("sync"));
        peaks[s] = a.heap_peaks.at("sync");
    }
    std::printf("    heap peak %llu B over an empty geo index, %llu B over %u cells of %llu B of blocks (%lld B above)\n",
                (unsigned long long)peaks[0], (unsigned long long)peaks[1], sizes[1], (unsigned long long)blocks[1],
                (long long)peaks[1] - (long long)peaks[0]);
    CHECK(peaks[1] <= peaks[0] + SEQUENTIAL_READ_BUFFER_BYTES + (48 << 10));
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
            old_cells[pair_cell(k)] = {k % 950};
            new_cells[pair_cell(k)] = {k % 950 + 1};
        }
        BuildDir old_d, new_d;
        put_pois(old_d, pois);
        put_pois(new_d, pois);
        put_cells(old_d, "poi", old_cells);
        put_cells(new_d, "poi", new_cells);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        // The POIs stay, so the rebuilt index is the old one: n lost and n gained runs of one cell.
        std::vector<char> payload, delta;
        append_cell_list_delta(payload, old_cells, new_cells);
        append(delta, CELL_INDEX_DELTA_MARKER);
        append(delta, static_cast<uint32_t>(PatchFileId::POI_ENTRIES));
        append(delta, uint64_t{payload.size()});
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
            old_cells[pair_cell(2 * k)] = {};
            new_cells[pair_cell(2 * k + 1)] = {};
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

// Eight POI cells that share one list of n POIs (as a dense city's poi/all
// cells do) and a ninth whose POI changes: the patcher holds a long list as
// few times as the rebuild needs (the old index's row, the writer's previous
// list and its encoding), so from n = 6,000 to 60,000 its heap peak grows by
// at most three times the ids added (2.42 times; the POI files, which stay,
// are copied and hold nothing). It grew by 4.7 times them when the old
// index's reader kept its own copy of a shared list, and the rebuild a second
// list it swapped with the reader's. Skipped without a heap shim (the
// sanitizer build).
TEST(patch_heap_holds_a_long_shared_poi_cell_list_at_most_three_times) {
    if (!gctest::heap_peak_shim()) {
        std::printf("    SKIP heap peaks: no heap shim under the sanitizers\n");
        return;
    }
    ScratchDir dir("lowmem-heap-shared-list");
    std::mt19937 rng(20261110);
    const uint32_t sizes[] = {6000, 60000};
    const auto pois = pois_of(rng, sizes[1] + 2, 0, true);
    uint64_t peaks[2] = {};
    for (size_t s = 0; s < 2; s++) {
        const LowMemCase c{dir.path(), "n" + std::to_string(sizes[s])};
        std::vector<uint32_t> shared(sizes[s]);
        for (uint32_t i = 0; i < sizes[s]; i++) shared[i] = i;
        CellLists old_cells, new_cells;
        for (uint32_t k = 0; k < 8; k++) old_cells[pair_cell(k)] = new_cells[pair_cell(k)] = shared;
        old_cells[pair_cell(8)] = {sizes[1]};
        new_cells[pair_cell(8)] = {sizes[1] + 1};
        BuildDir old_d, new_d;
        put_pois(old_d, pois);
        put_pois(new_d, pois);
        put_cells(old_d, "poi", old_cells);
        put_cells(new_d, "poi", new_cells);
        REQUIRE(prepare(c, old_d, new_d).empty());
        const Applied a = apply_modes(c, {"sync"});
        CHECK(a.bad.empty());
        REQUIRE(a.heap_peaks.count("sync"));
        peaks[s] = a.heap_peaks.at("sync");
    }
    const uint64_t added = 4 * uint64_t(sizes[1] - sizes[0]), from = peaks[0], to = peaks[1];
    std::printf("    heap peak %llu B at n = %u, %llu B at n = %u (%.2f times the %llu B of ids added)\n", (unsigned long long)from,
                sizes[0], (unsigned long long)to, sizes[1], to > from ? double(to - from) / double(added) : 0.0,
                (unsigned long long)added);
    CHECK(to <= from + 3 * added);
}

namespace {

// The admin index of n_cells cells: cell k lists ids_of(k). The admin
// polygons keep their slots, so the patch rebuilds the old index as it is.
CellLists admin_cells_of(uint32_t n_cells, const std::function<std::vector<uint32_t>(uint32_t)>& ids_of) {
    CellLists lists;
    for (uint32_t k = 0; k < n_cells; k++) lists[pair_cell(k)] = ids_of(k);
    return lists;
}

// The head of a cell index delta of payload: marker, the entries file, the payload's size.
std::vector<char> cell_index_delta_head(PatchFileId entries, const std::vector<char>& payload) {
    std::vector<char> head;
    append(head, CELL_INDEX_DELTA_MARKER);
    append(head, static_cast<uint32_t>(entries));
    append(head, uint64_t{payload.size()});
    return head;
}

}  // namespace

// One polygon (slot 9) that leaves every other one of 2n cells, all of which
// list slot 2 too: an admin cell index delta of n lost runs of one cell, each
// 2 ranks after the one before, which the patcher reads where they sit; the
// lost set holds one id at a time. From 1K to 100K cells lost.
TEST(patch_heap_stays_flat_as_a_polygon_loses_cells) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        BuildDir old_d, new_d;
        put_cells(old_d, "admin", admin_cells_of(2 * n, [](uint32_t) { return std::vector<uint32_t>{2, 9}; }));
        put_cells(new_d, "admin", admin_cells_of(2 * n, [](uint32_t k) { return k % 2 ? std::vector<uint32_t>{2, 9} : std::vector<uint32_t>{2}; }));
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        // No gone or new cells; n lost runs (start gap, length - 1, 9 << 1); no gained runs.
        std::vector<char> payload;
        for (const uint64_t v : {uint64_t{0}, uint64_t{0}, uint64_t{n}}) write_varint64(payload, v);
        for (uint32_t r = 0; r < n; r++)
            for (const uint64_t v : {uint64_t{r ? 2u : 0u}, uint64_t{0}, uint64_t{9 << 1}}) write_varint64(payload, v);
        write_varint64(payload, 0);
        return patch_holds(c, cell_index_delta_head(PatchFileId::ADMIN_ENTRIES, payload),
                           "admin cell index delta of " + std::to_string(n) + " lost runs");
    }, 1000, 100).empty());
}

// n polygons that each leave their one cell (cell k lists slot 7919 k mod n,
// which it loses, and a slot every cell lists): an admin cell index delta of n
// lost runs of one cell each, in rank order, their ids out of order, which the
// patcher reads where they sit. From 10K to 100K polygons.
TEST(patch_heap_stays_flat_as_polygons_lose_a_cell_each) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        constexpr uint32_t SHARED = 0x7FFFFFF0;
        auto slot = [n](uint32_t k) { return static_cast<uint32_t>(uint64_t{k} * 7919 % n); };
        BuildDir old_d, new_d;
        put_cells(old_d, "admin", admin_cells_of(n, [&](uint32_t k) { return std::vector<uint32_t>{slot(k), SHARED}; }));
        put_cells(new_d, "admin", admin_cells_of(n, [](uint32_t) { return std::vector<uint32_t>{SHARED}; }));
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        std::vector<char> payload;
        for (const uint64_t v : {uint64_t{0}, uint64_t{0}, uint64_t{n}}) write_varint64(payload, v);
        for (uint32_t k = 0; k < n; k++) {
            write_varint64(payload, k ? 1 : 0);
            write_varint64(payload, 0);
            write_varint(payload, slot(k) << 1);
        }
        write_varint64(payload, 0);
        return patch_holds(c, cell_index_delta_head(PatchFileId::ADMIN_ENTRIES, payload),
                           "admin cell index delta of " + std::to_string(n) + " polygons' lost runs");
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

// Two side files of n values that follow no records: way_parents.bin, a
// value in a thousand another on the new day (a sparse delta over the old
// values), and way_postcodes.bin, which the old day lacks (a full replacement,
// handed to its encoder whole). The codecs hold a block of either, and each
// new file's block table waits in its scratch file.
TEST(patch_heap_stays_flat_as_side_files_grow) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::vector<char> parents, changed, postcodes;
        for (uint32_t v = 0; v < n; v++) {
            append(parents, v % 50000);
            append(changed, v % 50000 + (v % 1000 == 7));
            append(postcodes, v / 3 * 8);
        }
        if (std::string e = prepare(c, {{"way_parents.bin", parents}}, {{"way_parents.bin", changed}, {"way_postcodes.bin", postcodes}});
            !e.empty())
            return e;
        // way_postcodes.bin's section: a full replacement (stride 0, no
        // fixups) of no old values by the new ones' bytes.
        std::vector<char> head;
        append(head, static_cast<uint32_t>(PatchFileId::WAY_POSTCODES));
        append(head, 0u);
        append(head, uint64_t{0});
        append(head, uint64_t{4} * n);
        append(head, 0u);
        append(head, uint64_t{4} * n);
        return patch_holds(c, head, "full replacement of " + std::to_string(n) + " values");
    }, 400000).empty());
}

// n address points, two in three with a footprint of 1 to 8 vertices, and
// the new day's without every fiftieth: the address merge keeps runs of 49
// records, whose footprints the replay takes from the old stream. The codecs
// hold a block's records, one footprint and an index row of either day's
// files.
TEST(patch_heap_stays_flat_as_addresses_grow) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::mt19937 rng(20261114);
        std::vector<Addr> addrs, kept;
        for (uint32_t i = 0; i < n; i++) {
            const std::vector<char> footprint = i % 3 == 0 ? std::vector<char>{} : block(rng, 1 + rng() % 8);
            addrs.push_back({47.0f + static_cast<float>(i) * 1e-4f, footprint});
            if (i % 50 != 7) kept.push_back(addrs.back());
        }
        BuildDir old_d, new_d;
        put_addrs(old_d, addrs);
        put_addrs(new_d, kept);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        // The address points' merge: n old records, n less a fiftieth new.
        std::vector<char> head;
        append(head, static_cast<uint32_t>(PatchFileId::ADDR_POINTS));
        append(head, static_cast<uint32_t>(ADDR_POINT_STRIDE));
        append(head, uint64_t{ADDR_POINT_STRIDE} * n);
        append(head, uint64_t{ADDR_POINT_STRIDE} * kept.size());
        return patch_holds(c, head, "merge of " + std::to_string(n) + " address points");
    }, 100000).empty());
}

// A family a day leaves as it is, n street ways of two nodes with a parent
// each: its files go from the old dir to the new one as they sit, through no
// codec, and the patcher holds none of them.
TEST(patch_heap_stays_flat_as_a_copied_family_grows) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        BuildDir d, meta;
        std::vector<uint32_t> names(n);
        std::vector<char> parents;
        for (uint32_t w = 0; w < n; w++) {
            names[w] = w % 1000;
            append(parents, w % 50000);
        }
        put_named_ways(d, meta, names);
        d["way_parents.bin"] = parents;
        if (std::string e = prepare(c, d, d); !e.empty()) return e;
        // The street pair's two sections, each sent as unchanged.
        std::vector<char> pair = copy_old_section(PatchFileId::STREET_WAYS, uint64_t{WAY_HEADER_STRIDE} * n);
        const std::vector<char> nodes = copy_old_section(PatchFileId::STREET_NODES, uint64_t{16} * n);
        pair.insert(pair.end(), nodes.begin(), nodes.end());
        return patch_holds(c, pair, "unchanged street pair of " + std::to_string(n) + " ways");
    }, 100000).empty());
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
// diff sends new indices and a moved-string pair, and the POI records take
// the remapped names.
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

    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A new dir without an encoded family's files (the POI family, both files
// gone) gets none of them, in every mode: the patch lists neither, so what
// their sections replay to goes to the patcher's scratch dir.
TEST(patch_lowmem_writes_no_file_of_a_family_the_new_dir_lacks) {
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

    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A new set with part of an encoded family is refused, in every mode: a
// client file list without the address family's addr_vertices.bin before the
// replay, and one without its addr_index.bin, which no section names, before
// an address file is written. The builder writes the three or none, so the
// lists are the diff's with an entry cut out. The patch as made applies.
TEST(patch_lowmem_refuses_a_new_set_with_part_of_a_family) {
    ScratchDir dir("lowmem-part");
    std::mt19937 rng(20261027);
    BuildDir old_d, new_d;
    put_addr_family(old_d, rng, 40);
    put_addr_family(new_d, rng, 40);
    const LowMemCase c{dir.path(), "part"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    // The list follows the magic, the version and the flags: its marker, its
    // count, then each file's name length, name, size and inline byte.
    const size_t list_at = 16;
    uint32_t marker = 0, listed = 0;
    REQUIRE(raw.size() > list_at + 8);
    memcpy(&marker, raw.data() + list_at, 4);
    memcpy(&listed, raw.data() + list_at + 4, 4);
    REQUIRE(marker == CLIENT_FILES_MARKER);
    struct Cut {
        std::string file, refusal;
    };
    for (const Cut& e : {Cut{"addr_vertices.bin", "family addr: the patch ships 1 of its 2 files"},
                         Cut{"addr_index.bin", "addr: the patch lists no addr_index.bin"}}) {
        std::vector<char> entry;
        append(entry, static_cast<uint16_t>(e.file.size()));
        entry.insert(entry.end(), e.file.begin(), e.file.end());
        const size_t at = find_once(raw, entry);
        REQUIRE(at != std::string::npos);
        std::vector<char> cut = raw;
        cut.erase(cut.begin() + static_cast<std::ptrdiff_t>(at),
                  cut.begin() + static_cast<std::ptrdiff_t>(at + entry.size() + 8 + 1));
        put_at(cut, list_at + 4, listed - 1);
        const std::string patch = c.dir(e.file + ".gcpatch");
        REQUIRE(write_patch(c, patch, cut));

        CHECK(not_refused(c, ALL_MODES, patch, "without-" + e.file, e.refusal).empty());
        for (const std::string& mode : ALL_MODES)
            CHECK(!std::filesystem::exists(c.dir("without-" + e.file + ".out." + mode) + "/addr_points.bin"));
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A MATCH run longer than what is left of the old file is refused in every
// mode: the last run of POI records (replayed with their child), 5 longer;
// of their vertices (the child stream, which the records size), as bytes
// past the last record.
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

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A merge of an encoded family's parent at a stride other than its records'
// is refused before the replay, in every mode: addresses at 24 bytes
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

        CHECK(not_refused(c, ALL_MODES, patch, name, "Malformed record stride: " + name).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// The street ways' merge (MATCH, REPLACE and INSERT runs, replayed with their
// nodes) with a run's count 2^30 past the patch's, or the merge sequence's or
// the nodes section's length 2^32 past it, is refused in every mode.
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

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// The POI replay copies an old block only for a new record at its old
// record's lat and lng whose polygon is that very block's bytes. Three POIs
// and two days made by hand from the diff's patch of the unchanged set, its
// two POI sections rebuilt: the records REPLACEd in place one ulp of latitude
// away, their vertices one MATCH of the old stream (each block encoded again
// against its new lat); and the records MATCHed, the first one's vertices
// MATCHed from the second's old block (its own deleted) and the second's
// inserted. Each patch gives the edited day's own files.
TEST(patch_encodes_the_poi_blocks_it_cannot_copy) {
    ScratchDir dir("lowmem-poi-nocopy");
    std::mt19937 rng(20261113);
    std::vector<Poi> pois;
    for (uint32_t i = 0; i < 3; i++) pois.push_back({1000 + i, block(rng, 8)});
    BuildDir d;
    put_pois(d, pois);
    const LowMemCase c{dir.path(), "nocopy"};
    REQUIRE(prepare(c, d, d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    const std::vector<char>& records = d.at("poi_records.bin");
    const std::vector<char>& vertices = d.at("poi_vertices.bin");
    const uint32_t n = static_cast<uint32_t>(pois.size()), b = static_cast<uint32_t>(pois[0].block.size());
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
    auto op = [](std::vector<char>& ops, uint8_t kind, uint32_t count, const char* data = nullptr, size_t stride = 0) {
        append(ops, kind);
        append(ops, count);
        if (data) ops.insert(ops.end(), data, data + size_t{count} * stride);
    };
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
    // The patch with the POI sections rebuilt, applied to the old dir: the
    // patched POI files against the day's own (the same sizes as the old).
    auto check = [&](const char* name, const BuildDir& day, const std::vector<char>& rec_ops, const std::vector<char>& vert_ops) {
        const int failures_before = gctest::failures();
        const BuildDir want = gctest::as_written(day), had = gctest::as_written(d);
        for (const char* f : {"poi_records.bin", "poi_vertices.bin"}) REQUIRE(want.at(f).size() == had.at(f).size());
        std::vector<char> edited(raw.begin(), raw.begin() + static_cast<std::ptrdiff_t>(at));
        for (const auto& section : {merge(PatchFileId::POI_RECORDS, POI_RECORD_STRIDE, records.size(), rec_ops),
                                    merge(PatchFileId::POI_VERTICES, 1, vertices.size(), vert_ops)})
            edited.insert(edited.end(), section.begin(), section.end());
        edited.insert(edited.end(), raw.begin() + static_cast<std::ptrdiff_t>(at + copies.size()), raw.end());
        const std::string patch = c.dir(std::string(name) + ".gcpatch"), out = c.dir(std::string("out.") + name);
        REQUIRE(write_patch(c, patch, edited));
        std::filesystem::remove_all(out);
        const auto r = run_tool(patch_words(c, "sync", patch, out), c.log());
        CHECK(r.ok);
        for (const char* f : {"poi_records.bin", "poi_vertices.bin"}) CHECK(read_file(out + "/" + f) == want.at(f));
        if (gctest::failures() > failures_before) std::printf("    in the %s day\n", name);
    };
    // One ulp of latitude away: lat is the record's first four bytes.
    BuildDir moved = d;
    std::vector<char>& moved_records = moved["poi_records.bin"];
    for (uint32_t i = 0; i < n; i++) put_at(moved_records, size_t{i} * POI_RECORD_STRIDE, static_cast<uint32_t>(1000 + i + 1));
    std::vector<char> rec_ops, vert_ops;
    op(rec_ops, OP_REPLACE_RUN, n, moved_records.data(), POI_RECORD_STRIDE);
    op(vert_ops, OP_MATCH_RUN, static_cast<uint32_t>(vertices.size()));
    check("moved", moved, rec_ops, vert_ops);
    // The first POI's polygon the second's.
    BuildDir second = d;
    std::vector<char>& second_vertices = second["poi_vertices.bin"];
    std::copy(vertices.begin() + b, vertices.begin() + 2 * b, second_vertices.begin());
    rec_ops.clear();
    vert_ops.clear();
    op(rec_ops, OP_MATCH_RUN, n);
    op(vert_ops, OP_DELETE_RUN, b);
    op(vert_ops, OP_MATCH_RUN, b);
    op(vert_ops, OP_INSERT_RUN, b, vertices.data() + b, 1);
    op(vert_ops, OP_MATCH_RUN, b);
    check("second", second, rec_ops, vert_ops);
}

// The POI records and their vertex stream replayed out of step apply in
// every mode: the POI replay reads the old rows and the old vertex stream
// through cursors of their own (poi_replay.h), so neither holds what the
// other passes. 150,000 POIs with the same 8-vertex block, and the two POI
// sections rebuilt by hand, both giving the new set (new = old): the child
// runs ahead when every record is inserted before the old ones are deleted
// while its block is matched from the old ones (5.4 MB of old records a
// shared decoder would hold for the parent's reader), and the parent runs
// ahead when the records are matched while their blocks are inserted but for
// the last, matched from the first old block (6.3 MB of old blocks it would
// hold for the child's). The patch as made applies.
TEST(patch_lowmem_replays_poi_records_and_vertices_out_of_step) {
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
    };
    Case child_ahead{"child-ahead", {}, {}};
    op(child_ahead.parent_ops, OP_INSERT_RUN, n, records.data(), POI_RECORD_STRIDE);
    op(child_ahead.parent_ops, OP_DELETE_RUN, n);
    op(child_ahead.child_ops, OP_MATCH_RUN, v);
    Case parent_ahead{"parent-ahead", {}, {}};
    op(parent_ahead.parent_ops, OP_MATCH_RUN, n);
    op(parent_ahead.child_ops, OP_INSERT_RUN, v - b, vertices.data(), 1);
    op(parent_ahead.child_ops, OP_MATCH_RUN, b);
    for (const Case& e : {child_ahead, parent_ahead}) {
        std::vector<char> edited(raw.begin(), raw.begin() + at);
        for (const auto& section : {merge(PatchFileId::POI_RECORDS, POI_RECORD_STRIDE, records.size(), e.parent_ops),
                                    merge(PatchFileId::POI_VERTICES, 1, vertices.size(), e.child_ops)})
            edited.insert(edited.end(), section.begin(), section.end());
        edited.insert(edited.end(), raw.begin() + at + copies.size(), raw.end());
        REQUIRE(write_patch(c, c.patch(), edited));

        CHECK(apply_modes(c, ALL_MODES).bad.empty());
    }
    REQUIRE(write_patch(c, c.patch(), raw));
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
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
    std::vector<char> old_parents, new_parents, old_admin_parents, new_admin_parents, centroids;
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
    std::vector<PlaceNode> places;
    for (uint32_t i = 0; i < 5; i++) places.push_back(test_place(float(i), float(i), i));
    old_d["way_parents.bin"] = old_parents;
    new_d["way_parents.bin"] = new_parents;
    old_d["admin_parents.bin"] = old_admin_parents;
    new_d["admin_parents.bin"] = new_admin_parents;
    new_d["postcode_centroids.bin"] = centroids;
    put_places(old_d, places);
    put_places(new_d, places);
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
    const uint64_t place_rows = places.size() * PLACE_NODE_STRIDE;
    const size_t copy_length_at = at_of(PatchFileId::PLACE_NODES, COPY_OLD_STRIDE, place_rows, place_rows, {0}) + 28;
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
// buffer is sized by it: the place nodes' merge, 24 bytes a logical row as
// made. The patch as made applies.
TEST(patch_refuses_a_record_stride_past_any_records) {
    ScratchDir dir("patch-stride");
    std::vector<PlaceNode> places;
    for (uint32_t i = 0; i < 50; i++) places.push_back(test_place(float(i), float(i), i));
    std::vector<PlaceNode> fewer = places;
    fewer.erase(fewer.begin() + 20);
    BuildDir old_d, new_d;
    put_places(old_d, places);
    put_places(new_d, fewer);
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

namespace {

// A cell list delta of hand-written varints: the gone ranks' and the new
// cells' gaps, then the lost and the gained runs as (start gap, length - 1,
// bare id << 1 | interior).
std::vector<char> cell_delta_payload(const std::vector<uint64_t>& gone, const std::vector<uint64_t>& fresh,
                                     const std::vector<std::array<uint64_t, 3>>& lost,
                                     const std::vector<std::array<uint64_t, 3>>& gained) {
    std::vector<char> p;
    for (const auto* list : {&gone, &fresh}) {
        write_varint64(p, list->size());
        for (const uint64_t v : *list) write_varint64(p, v);
    }
    for (const auto* runs : {&lost, &gained}) {
        write_varint64(p, runs->size());
        for (const auto& r : *runs) {
            write_varint64(p, r[0]);
            write_varint64(p, r[1]);
            write_varint(p, static_cast<uint32_t>(r[2]));
        }
    }
    return p;
}

// raw with the payload of its cell index delta for entries replaced.
std::vector<char> with_cell_index_delta(const std::vector<char>& raw, PatchFileId entries, const std::vector<char>& payload) {
    std::vector<char> head;
    append(head, CELL_INDEX_DELTA_MARKER);
    append(head, static_cast<uint32_t>(entries));
    const size_t at = find_once(raw, head);
    REQUIRE(at != std::string::npos);
    uint64_t size;
    memcpy(&size, raw.data() + at + 8, 8);
    std::vector<char> out(raw.begin(), raw.begin() + static_cast<std::ptrdiff_t>(at + 8));
    append(out, uint64_t{payload.size()});
    out.insert(out.end(), payload.begin(), payload.end());
    out.insert(out.end(), raw.begin() + static_cast<std::ptrdiff_t>(at + 16 + size), raw.end());
    return out;
}

// The POI index of 20 cells (cell k lists POI k mod 7, the new day's POI k mod
// 7 + 1) over 40 POIs that stay: the rebuilt index is the old one, ranks 0 to
// 19. Each case its patch with the POI cell index delta replaced, and the
// refusal it must earn in every mode.
struct DeltaCase {
    std::string name;
    std::vector<char> payload;
    std::string refusal;
};

void check_poi_delta_refusals(const std::string& dir_name, const std::vector<DeltaCase>& cases) {
    ScratchDir dir(dir_name);
    std::mt19937 rng(20261030);
    CellLists old_cells, new_cells;
    for (uint32_t k = 0; k < 20; k++) {
        old_cells[pair_cell(k)] = {k % 7};
        new_cells[pair_cell(k)] = {k % 7 + 1};
    }
    const auto pois = pois_of(rng, 40, 0, true);
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, pois);
    put_cells(old_d, "poi", old_cells);
    put_cells(new_d, "poi", new_cells);
    const LowMemCase c{dir.path(), "cell-runs"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    for (const DeltaCase& e : cases) {
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, with_cell_index_delta(raw, PatchFileId::POI_ENTRIES, e.payload)));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

}  // namespace

// A POI cell index delta with a malformed run is refused in every mode: a lost
// run over a cell without its id (cell 0 lists POI 0, not 5); 0x10000 lost or
// gained runs starting at one cell, refused before the patcher holds their
// ids; a run below the one before it (the same start, a lower id); a run past
// the last of the index's 20 ranks. The patch as made applies.
TEST(patch_refuses_each_malformed_run_of_a_cell_index_delta) {
    std::vector<std::array<uint64_t, 3>> many;
    for (uint64_t k = 0; k < 0x10000; k++) many.push_back({0, 0, (k + 0x1000) << 1});
    const std::string active = "Malformed cell list delta: an active set above 65535 ids";
    check_poi_delta_refusals("patch-cell-runs", {
        {"missing", cell_delta_payload({}, {}, {{0, 0, 5 << 1}}, {}), "Malformed cell list delta: a lost id missing from its cell"},
        {"lost-set", cell_delta_payload({}, {}, many, {}), active},
        {"gained-set", cell_delta_payload({}, {}, {}, many), active},
        {"step-back", cell_delta_payload({}, {}, {}, {{0, 0, 9 << 1}, {0, 0, 8 << 1}}), "Malformed cell list delta: a step back"},
        {"past-last", cell_delta_payload({}, {}, {}, {{20, 0, 9 << 1}}), "Malformed cell list delta: a run past the last rank"},
    });
}

// The ranks of a cell index delta are u64s in the patcher whatever its size_t
// (the i386 runner runs this): a run starting at 2^32 + 3 (rank 3 on a 32-bit
// wrap), one at rank 1 of 2^32 cells (it would end at rank 0), a gone rank of
// 2^32 + 2 (rank 2), and 2^32 + 1 gained runs where one is (the count wraps to
// 1) are refused in every mode. The patch as made applies.
TEST(patch_refuses_cell_index_delta_ranks_that_wrap_32_bits) {
    const uint64_t two32 = uint64_t{1} << 32;
    const std::string past = "Malformed cell list delta: a run past the last rank";
    std::vector<char> counted = cell_delta_payload({}, {}, {}, {});
    counted.pop_back();  // n_gained, 2^32 + 1 over one run
    write_varint64(counted, two32 + 1);
    for (const uint64_t v : {uint64_t{3}, uint64_t{0}}) write_varint64(counted, v);
    write_varint(counted, 9 << 1);
    check_poi_delta_refusals("patch-cell-runs-32", {
        {"start", cell_delta_payload({}, {}, {}, {{two32 + 3, 0, 9 << 1}}), past},
        {"length", cell_delta_payload({}, {}, {}, {{1, two32 - 1, 9 << 1}}), past},
        {"gone", cell_delta_payload({two32 + 2}, {}, {}, {}), "Malformed cell list delta: a gone rank past the last source cell"},
        {"count", counted, "Malformed cell list delta"},
    });
}

// An old side file the builder would not write (its first block's mode byte
// is neither of the two) is refused by name in every mode as the patcher
// reads its values, though its footer gives the size the patch expects. The
// patch applies to the file as written.
TEST(patch_refuses_an_old_side_file_the_builder_would_not_write) {
    ScratchDir dir("patch-old-side");
    std::vector<char> parents, changed;
    for (uint32_t v = 0; v < 4096; v++) {
        append(parents, v);
        append(changed, v + (v == 2000));
    }
    const LowMemCase c{dir.path(), "side"};
    REQUIRE(prepare(c, {{"way_parents.bin", parents}}, {{"way_parents.bin", changed}}).empty());
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
    std::vector<char> file = read_file(c.dir("old") + "/way_parents.bin");
    REQUIRE(!file.empty() && file[0] == 0);
    file[0] = 2;
    REQUIRE(write_file(c.dir("old") + "/way_parents.bin", file));

    CHECK(not_refused(c, ALL_MODES, c.patch(), "mode-byte", "Malformed way_parents.bin: block 0 is neither FOR nor a dictionary").empty());
}

// Old street files the builder would not write are refused by name in every
// mode as the patcher reads them, though their footers give the sizes the
// patch expects: a group's row whose blocks start a byte past where the
// blocks before them end, and a nodes file a byte short of its last block. A
// dir with one file of the pair is refused before anything is read. The patch
// applies to the files as written.
TEST(patch_refuses_old_way_files_the_builder_would_not_write) {
    ScratchDir dir("patch-old-ways");
    std::vector<uint32_t> names(40), renamed(40);
    for (uint32_t i = 0; i < 40; i++) {
        names[i] = i;
        renamed[i] = i == 30 ? 1000 : i;
    }
    BuildDir old_d, new_d, old_meta, new_meta;
    put_named_ways(old_d, old_meta, names);
    put_named_ways(new_d, new_meta, renamed);
    const LowMemCase c{dir.path(), "ways"};
    REQUIRE(prepare(c, old_d, new_d, old_meta, new_meta).empty());
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
    const std::vector<char> rows = read_file(c.dir("old") + "/street_ways.bin"), nodes = read_file(c.dir("old") + "/street_nodes.bin");
    // Three groups' rows, 12 bytes and 8 a way, and the footer.
    REQUIRE(rows.size() == 2 * (12 + 16 * 8) + (12 + 8 * 8) + 8 && nodes.size() > 16);
    auto put = [&](const char* name, const std::optional<std::vector<char>>& bytes) {
        const std::string path = c.dir("old") + "/" + name;
        std::filesystem::remove(path);
        if (bytes) REQUIRE(write_file(path, *bytes));
    };
    std::vector<char> moved = rows, cut = nodes;
    moved[12 + 16 * 8]++;
    cut.erase(cut.end() - 17);
    struct Edit {
        std::string name;
        const char* file;
        std::optional<std::vector<char>> bytes;
        std::string refusal;
    };
    for (const Edit& e : {Edit{"row", "street_ways.bin", moved, "street_ways.bin group 1 is not where the blocks before it end"},
                          Edit{"block", "street_nodes.bin", cut, "street_nodes.bin ends inside the block of way 39"},
                          Edit{"half", "street_nodes.bin", std::nullopt, "street_ways.bin without street_nodes.bin"}}) {
        put(e.file, e.bytes);

        CHECK(not_refused(c, ALL_MODES, c.patch(), e.name, "Malformed way files: " + e.refusal).empty());

        put("street_ways.bin", rows);
        put("street_nodes.bin", nodes);
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// Old address files the builder would not write are refused by name in every
// mode, though their footers give the sizes the patch expects: a block the
// index puts a byte past where the blocks before it end, a first block with
// a flag no block has, and a vertices file a byte short of its last blob. A
// footer that gives a logical footprint stream a byte past what a patch
// indexes, and a dir with two of the three files, are refused before a block
// is read. The patch applies to the files as written.
TEST(patch_refuses_old_address_files_the_builder_would_not_write) {
    ScratchDir dir("patch-old-addrs");
    std::mt19937 rng(20261114);
    std::vector<Addr> addrs;
    for (uint32_t i = 0; i < 40; i++)
        addrs.push_back({47.0f + static_cast<float>(i) * 1e-4f, i % 3 == 2 ? std::vector<char>{} : block(rng, 3 + rng() % 8)});
    std::vector<Addr> fewer_addrs(addrs.begin() + 1, addrs.end());
    BuildDir old_d, new_d;
    put_addrs(old_d, addrs);
    put_addrs(new_d, fewer_addrs);
    const LowMemCase c{dir.path(), "addrs"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
    const char* const names[] = {"addr_points.bin", "addr_vertices.bin", "addr_index.bin"};
    std::map<std::string, std::vector<char>> written;
    for (const char* name : names) written[name] = read_file(c.dir("old") + "/" + name);
    // One superblock's row, 16 bytes and 6 a block, and the footer.
    REQUIRE(written["addr_index.bin"].size() == 16 + 3 * 6 + 8 && written["addr_vertices.bin"].size() > 16);
    auto put = [&](const std::string& name, const std::optional<std::vector<char>>& bytes) {
        const std::string path = c.dir("old") + "/" + name;
        std::filesystem::remove(path);
        if (bytes) REQUIRE(write_file(path, *bytes));
    };
    std::vector<char> moved = written["addr_index.bin"], flagged = written["addr_points.bin"], cut = written["addr_vertices.bin"];
    std::vector<char> past = written["addr_vertices.bin"];
    moved[16 + 6]++;
    flagged[0] = static_cast<char>(0x80);
    cut.erase(cut.end() - 17);
    put_at(past, past.size() - 16, uint64_t{FILE_RECORDS_MAX} + 1);
    struct Edit {
        std::string name, file;
        std::optional<std::vector<char>> bytes;
        std::string refusal;
    };
    for (const Edit& e :
         {Edit{"index", "addr_index.bin", moved, "addr_index.bin block 1 is not where the blocks and the blobs before it end"},
          Edit{"block", "addr_points.bin", flagged, "addr_points.bin block 0 has a flag no block has"},
          Edit{"blob", "addr_vertices.bin", cut, "addr_vertices.bin ends inside the blob of record 39"},
          Edit{"limit", "addr_vertices.bin", past,
               "a logical addr_vertices.bin of 4294967295 bytes is more than the 4294967294 a patch can index"},
          Edit{"part", "addr_index.bin", std::nullopt,
               "a dir with some of addr_points.bin, addr_vertices.bin and addr_index.bin and not all three"}}) {
        put(e.file, e.bytes);

        CHECK(not_refused(c, ALL_MODES, c.patch(), e.name, "Malformed address files: " + e.refusal).empty());

        for (const char* name : names) put(name, written[name]);
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
        old_cells[pair_cell(k)] = {k % 7};
        new_cells[pair_cell(k)] = {k % 7 + 1};
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


// 0xFFFFFFF4 marked the place cell changes until v13: now it is no section's
// marker, and the patcher refuses it as a file it doesn't know, in every mode.
// The patch as made applies.
TEST(patch_refuses_the_old_place_cell_changes_marker_as_an_unknown_file) {
    ScratchDir dir("patch-place-cell-changes");
    std::mt19937 rng(20261102);
    CellLists old_cells, new_cells;
    for (uint32_t k = 0; k < 30; k++) old_cells[pair_cell(k)] = {k % 7};
    for (uint32_t k = 0; k < 20; k++) new_cells[pair_cell(k)] = {k % 7 + 1};
    for (uint32_t k = 40; k < 50; k++) new_cells[pair_cell(k)] = {2};
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

// Ids 9 to 11 name the geo index's lists by table since build 19, not files:
// a file section for one (a full replacement) is refused as a file the
// patcher doesn't know, in every mode. Build 19's first patcher took it, its
// bytes written to the scratch dir as a file the new dir doesn't list. The
// patch as made applies.
TEST(patch_refuses_a_file_section_for_a_geo_list) {
    ScratchDir dir("patch-geo-list-section");
    std::mt19937 rng(20261109);
    CellLists old_cells, new_cells;
    for (uint32_t k = 0; k < 30; k++) old_cells[pair_cell(k)] = {k % 7};
    for (uint32_t k = 0; k < 20; k++) new_cells[pair_cell(k)] = {k % 7 + 1};
    for (uint32_t k = 40; k < 50; k++) new_cells[pair_cell(k)] = {2};
    const auto pois = pois_of(rng, 40, 0, true);
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, pois);
    put_cells(old_d, "poi", old_cells);
    put_cells(new_d, "poi", new_cells);
    const LowMemCase c{dir.path(), "geo-list-section"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    std::vector<char> poi_changes;
    for (const uint32_t w : {CELL_CHANGES_POI_MARKER, 10u, 10u}) append(poi_changes, w);
    const size_t at = find_once(raw, poi_changes);
    REQUIRE(at != std::string::npos);
    for (const PatchFileId fid : GEO_ENTRIES_FILES) {
        const uint32_t id = static_cast<uint32_t>(fid);
        // file id, stride 0 (a full replacement), old size 0, new size 4, no files, 4 bytes of data.
        std::vector<char> section, edited(raw);
        append(section, id);
        append(section, uint32_t(0));
        append(section, uint64_t(0));
        append(section, uint64_t(4));
        append(section, uint32_t(0));
        append(section, uint64_t(4));
        append(section, uint32_t(0x01020304));
        edited.insert(edited.begin() + static_cast<std::ptrdiff_t>(at), section.begin(), section.end());
        const std::string name = "geo-list-" + std::to_string(id), patch = c.dir(name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ALL_MODES, patch, name, "Unknown file " + std::to_string(id)).empty());
    }
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
    for (uint32_t k = 0; k < 30; k++) old_cells[pair_cell(k)] = {k % 7};
    for (uint32_t k = 0; k < 20; k++) new_cells[pair_cell(k)] = {k % 7 + 1};
    for (uint32_t k = 40; k < 50; k++) new_cells[pair_cell(k)] = {2};
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
    const uint32_t street = static_cast<uint32_t>(PatchFileId::STREET_LISTS), poi = static_cast<uint32_t>(PatchFileId::POI_ENTRIES);
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
         "Malformed geo entry delta: a second one for geo street lists"},
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
        {"past-every-file", with_lists(1, {past, 1, 0, 1}),
         "Malformed secondary remap: pairs for file " + std::to_string(past) + ", which has none"},
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
// stream, a byte a record, which the patcher replays with its parent).
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
//     fields, which the patcher replays as the POIs' child stream.
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
        {"child", [&](std::vector<char>& p) { add_fixup(p, child_at + 40, 0); }, ALL_MODES,
         "Encoded family: a child stream with string fixups"},
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
//     file without offset fields, which the patcher replays as the POIs'
//     child stream).
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
        {"child", [&](std::vector<char>& p) { add(p, vertices, RUNS, {0}, false); }, ALL_MODES,
         "Encoded family: a child stream with offset fixups"},
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
// patch; so is a pool of another size, and the file missing. An old file that
// isn't FCC2 (string_files.h) is refused as malformed, named. The patch
// applies to the tier it was made from.
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
    // The old dir's tier: whether it was written.
    auto put_old_pool = [&](const std::string& pool) {
        const std::vector<uint8_t> f = string_files::encode(pool.data(), pool.size());
        return write_file(c.dir("old") + "/strings_core.bin", reinterpret_cast<const char*>(f.data()), f.size());
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
    REQUIRE(put_old_pool(old_pool + std::string("one more", 9)));
    CHECK(not_refused(c, ALL_MODES, c.patch(), "longer", stale + " (a pool of " + std::to_string(old_pool.size() + 9) +
                                                         " bytes, the patch expects " + std::to_string(old_pool.size()) + ")")
              .empty());
    {
        std::vector<uint8_t> f = string_files::encode(old_pool.data(), old_pool.size());
        f.back() = '3';  // FCC3
        const char* p = reinterpret_cast<const char*>(f.data());
        REQUIRE(write_file(c.dir("old") + "/strings_core.bin", p, f.size()));
        CHECK(not_refused(c, ALL_MODES, c.patch(), "not-fcc2", "Malformed old strings_core.bin: no FCC2 footer").empty());
    }
    REQUIRE(unlink((c.dir("old") + "/strings_core.bin").c_str()) == 0);
    CHECK(not_refused(c, ALL_MODES, c.patch(), "missing", stale + " (missing)").empty());
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

// An option other than --threads (a staged apply's --only among them) is
// refused before anything is written.
TEST(patch_refuses_an_unknown_option) {
    ScratchDir dir("patch-option");
    const std::string& root = dir.path();
    REQUIRE(write_dir(root + "/cur", {}));
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
// one) and the current dir keeps its bytes; and so is the current dir
// itself.
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
        std::string name;
        bool symlink;
    };
    const std::string cur = c.dir("old");
    int n = 0;
    for (const Alias& a : {Alias{"meta.json", false}, Alias{"meta.json", true}, Alias{"way_parents.bin", false},
                           Alias{"way_parents.bin", true}}) {
        const std::string out = root + "/out" + std::to_string(n++);
        std::filesystem::create_directories(out);
        if (a.symlink) std::filesystem::create_symlink(cur + "/" + a.name, out + "/" + a.name);
        else std::filesystem::create_hard_link(cur + "/" + a.name, out + "/" + a.name);
        const BuildDir before = read_dir(cur);

        auto r = run_tool({GEOCODER_PATCH_BIN, cur, c.patch(), "-o", out}, c.log());

        CHECK(!r.ok);
        CHECK(logged(r, "Output dir is not empty"));
        CHECK(read_dir(cur) == before);
    }
    const BuildDir before = read_dir(cur);

    auto r = run_tool({GEOCODER_PATCH_BIN, cur, c.patch(), "-o", cur}, c.log());

    CHECK(!r.ok);
    CHECK(logged(r, "Output dir must differ from the current dir"));
    CHECK(read_dir(cur) == before);
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
// them out (a polygon without rings a tombstone).
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
// and an admin cell's list changed. The parent sent the admin geometry files
// whole.
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
    std::vector<PlaceNode> new_places = admin_minimal_places(new_g, 300);
    REQUIRE(new_places[7].parent_poly_id == 7);
    new_places[7].parent_poly_id = 2;
    put_places(new_d, new_places);
    CellLists cells = parse_cell_lists(new_d["admin_cells.bin"], new_d["admin_entries.bin"], cell_pairs::ADMIN);
    cells[pair_cell(9)] = {9, 11 | INTERIOR};
    put_cells(new_d, "admin", cells);
    const LowMemCase c{dir.path(), "arc-dir"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    CHECK(apply_modes(c, ALL_MODES, merged_by_slot(old_d, new_d)).bad.empty());
}

// The address files beside the plain arc files: every mode replays the
// address family through its codecs and the arc files as plain merge
// sections, and the new dir holds the address files as the builder writes
// them, byte for byte. The parent sent the admin geometry files whole.
TEST(patch_lowmem_applies_the_address_files_beside_plain_arc_files) {
    ScratchDir dir("lowmem-arcs-beside");
    std::mt19937 rng(20261103);
    const ArcGeometry old_g = arc_geometry(rng, 60, 200);
    ArcGeometry new_g = old_g;
    new_g.arcs[17] = random_bytes(rng, new_g.arcs[17].size() + 4);
    new_g.rings[20] = random_bytes(rng, new_g.rings[20].size());
    BuildDir old_d, new_d;
    put_arc_geometry(old_d, old_g);
    put_arc_geometry(new_d, new_g);
    put_addr_family(old_d, rng, 300);
    put_addr_family(new_d, rng, 300);
    const LowMemCase c{dir.path(), "beside"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    const LogCheck merged = merged_by_slot(old_d, new_d);
    CHECK(apply_modes(c, ALL_MODES, [&](const std::string& log) -> std::string {
              if (log.find("encode addr:") == std::string::npos) return "no encode addr line";
              return merged(log);
          }).bad.empty());
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
    // At most the arc's block: the bytes of its delta group, whose bits the vertex moved.
    CHECK(v.ops[2].second < arc_bytes(new_a, 7, 8));
    CHECK_EQ(uint64_t{v.ops[0].second} + v.ops[1].second + v.ops[3].second, size);
    for (const PatchFileId fid : {PatchFileId::ARC_INDEX, PatchFileId::ADMIN_POLYGONS, PatchFileId::ADMIN_RINGS})
        CHECK(copied_old_at(raw, fid, new_d.at(patch_file_names[uint32_t(fid)]).size()) != std::string::npos);
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// An arc that grows, a vertex added at its end inside its box: the arc index
// MATCHes every slot, the slots after the arc moved by one offset fixup run
// of the bytes it grew, and the arc vertices rewrite its block;
// the rings, which name arc slots, not offsets, and the records go as
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
    // Arc 7's block is rewritten with the vertex in it: a MATCH of every block before it (into
    // it when its flag byte held, the new delta no wider), its changed bytes out and in, and a
    // MATCH of every block after it.
    const uint32_t through_7 = arc_bytes(old_a, 0, 8);
    REQUIRE(v.ops.size() >= 3);
    CHECK(v.ops.front().first == OP_MATCH_RUN && v.ops.front().second >= arc_bytes(old_a, 0, 7));
    CHECK(v.ops.back().first == OP_MATCH_RUN && v.ops.back().second == old_size - through_7);
    uint64_t out = 0, in = 0;
    for (size_t i = 1; i + 1 < v.ops.size(); i++) {
        CHECK(v.ops[i].first == OP_DELETE_RUN || v.ops[i].first == OP_INSERT_RUN);
        (v.ops[i].first == OP_DELETE_RUN ? out : in) += v.ops[i].second;
    }
    CHECK_EQ(in - out, uint64_t{new_size - old_size});
    for (const PatchFileId fid : {PatchFileId::ADMIN_POLYGONS, PatchFileId::ADMIN_RINGS})
        CHECK(copied_old_at(raw, fid, new_d.at(patch_file_names[uint32_t(fid)]).size()) != std::string::npos);
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// 1,000 tombstones both days hold, among 100 live polygons (each eleventh
// slot), MATCH in place: the one change, polygon 0's ring running over one
// arc more, is a REPLACE of its ring block (a DELETE and an INSERT), every
// record MATCHing, those after it with their ring_offset moved by one fixup
// run. Nothing is inserted or deleted by record. Every mode applies it. The
// parent sent the records whole.
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
    CHECK(polygons.ops == (Ops{{OP_MATCH_RUN, 1100}}));
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
    for (uint32_t k = 0; k < 12; k++) cells[pair_cell(k)] = {k};
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

// A dir without admin geometry (a POI dir), one POI edited and on identical
// days: its patch holds no section of the four admin geometry files, not even
// an empty merge (four section heads, 14 to 21 bytes of a central-america
// identical-day patch), as the parent's held none. Every mode applies it.
TEST(patch_dir_without_admin_geometry_sends_no_admin_geometry_section) {
    ScratchDir dir("lowmem-no-arcs");
    std::mt19937 rng(20261104);
    const auto pois = pois_of(rng, 300, 0, true);
    auto edited = pois;
    std::fill(edited[100].block.begin() + 10, edited[100].block.end(), 0);
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, edited);
    const std::pair<const char*, const BuildDir*> days[] = {{"edited", &new_d}, {"same", &old_d}};
    for (const auto& [name, new_day] : days) {
        const LowMemCase c{dir.path(), name};
        REQUIRE(prepare(c, old_d, *new_day).empty());
        const std::vector<char> raw = raw_patch(c);
        REQUIRE(!raw.empty());
        std::vector<std::string> empty_merges;
        for (const AdminSlotFamily& family : ADMIN_SLOT_FAMILIES)
            for (const auto& [fid, stride] : {std::pair{family.slots, family.stride}, std::pair{family.blocks, size_t{1}}}) {
                const std::vector<char> head = section_head(fid, static_cast<uint32_t>(stride), 0, 0);
                if (std::search(raw.begin(), raw.end(), head.begin(), head.end()) != raw.end())
                    empty_merges.push_back(patch_file_names[uint32_t(fid)]);
            }
        for (const std::string& f : empty_merges) std::printf("    %s: an empty merge of %s\n", name, f.c_str());
        CHECK(empty_merges.empty());
        CHECK(apply_modes(c, ALL_MODES).bad.empty());
    }
}

// A dir whose four admin geometry files are empty on both days (Antarctica's
// admin-minimal, admin-minimal-poi and quality dirs hold no admin polygon),
// one POI edited and on identical days: each of the four goes as a full
// replacement of nothing (a 36-byte section, as the parent sent it), not as
// an empty merge (56 bytes); COPY_OLD never stands for an empty file. Every
// mode applies it.
TEST(patch_empty_admin_geometry_files_go_as_empty_full_replacements) {
    ScratchDir dir("lowmem-empty-arcs");
    std::mt19937 rng(20261108);
    const auto pois = pois_of(rng, 300, 0, true);
    auto edited = pois;
    std::fill(edited[100].block.begin() + 10, edited[100].block.end(), 0);
    BuildDir old_d, new_d;
    put_pois(old_d, pois);
    put_pois(new_d, edited);
    for (const AdminSlotFamily& family : ADMIN_SLOT_FAMILIES)
        for (const PatchFileId fid : {family.slots, family.blocks})
            old_d[patch_file_names[uint32_t(fid)]] = new_d[patch_file_names[uint32_t(fid)]] = {};
    const std::pair<const char*, const BuildDir*> days[] = {{"edited", &new_d}, {"same", &old_d}};
    for (const auto& [name, new_day] : days) {
        const LowMemCase c{dir.path(), name};
        REQUIRE(prepare(c, old_d, *new_day).empty());
        const std::vector<char> raw = raw_patch(c);
        REQUIRE(!raw.empty());
        for (const AdminSlotFamily& family : ADMIN_SLOT_FAMILIES)
            for (const auto& [fid, stride] : {std::pair{family.slots, family.stride}, std::pair{family.blocks, size_t{1}}}) {
                std::vector<char> nothing = section_head(fid, 0, 0, 0);
                append(nothing, 0u);
                append(nothing, uint64_t{0});
                const bool replaced = find_once(raw, nothing) != std::string::npos;
                const std::vector<char> merge = section_head(fid, static_cast<uint32_t>(stride), 0, 0);
                const bool merged = std::search(raw.begin(), raw.end(), merge.begin(), merge.end()) != raw.end();
                if (!replaced || merged)
                    std::printf("    %s: %s %s\n", name, patch_file_names[uint32_t(fid)],
                                merged ? "went as an empty merge" : "got no full replacement of nothing");
                CHECK(replaced);
                CHECK(!merged);
            }
        CHECK(apply_modes(c, ALL_MODES).bad.empty());
    }
}

// arc_vertices.bin's every byte new, 1 MiB of it and 10 MiB (n arcs of a
// KiB), over the same 100 polygons: its merge sends every block (head and
// tail trimmed only where a byte happens to repeat), which the patcher writes
// from the patch where it sits, so its heap stays flat in every mode. The
// parent sent the file whole.
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
// among 2n slots: the records MATCH, moved by offset fixups, and the ring
// blocks' merge runs along them. The patcher's heap stays flat in every mode. The parent sent
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

// --- A way pair replayed on its files as they sit (tools/way_replay.h) ---

namespace {

const way_files::Kind* const BOTH_WAY_KINDS[] = {&way_files::STREETS, &way_files::INTERPS};

PatchFileId ways_id(const way_files::Kind& kind) { return kind.interp ? PatchFileId::INTERP_WAYS : PatchFileId::STREET_WAYS; }
PatchFileId nodes_id(const way_files::Kind& kind) { return kind.interp ? PatchFileId::INTERP_NODES : PatchFileId::STREET_NODES; }

// A way of a pair: a street's name, or an interpolation's street, numbers
// and kind, and its nodes, a lat and a lng each as f32 bits.
struct Way {
    uint32_t name = 0, start = 0, end = 0;
    uint8_t interpolation = 0;
    std::vector<uint32_t> nodes;
    uint16_t count() const { return static_cast<uint16_t>(nodes.size() / 2); }
};
// A slot without a way, as strategy 2 leaves it (types.h).
const Way DEAD_WAY{NO, 0, 0, 0, {}};

// The kind's logical pair of ways, into d: each way's record, its nodes after
// those of the ways before it, and the nodes back to back.
void put_ways(BuildDir& d, const way_files::Kind& kind, const std::vector<Way>& ways) {
    std::vector<char> records, nodes;
    uint32_t before = 0;
    for (const Way& w : ways) {
        const uint16_t cnt = w.count();
        const uint32_t offset = cnt ? before : 0;
        if (kind.interp) append(records, InterpWay{offset, cnt, 0, w.name, w.start, w.end, w.interpolation});
        else append(records, WayHeader{offset, cnt, 0, w.name});
        for (const uint32_t bits : w.nodes) append(nodes, bits);
        before += cnt;
    }
    d[kind.ways_file] = records;
    d[kind.nodes_file] = nodes;
}

// The f32 nearest v x 1e-7 degrees, which has a hybrid key (coord_keys.h).
uint32_t seven_decimals(int64_t v) { return coord_keys::bits_of(static_cast<float>(static_cast<double>(v) / 1e7)); }

// cnt nodes of a way. Flavour 0: a walk in steps of up to 2,000 x 1e-7
// degrees, every coordinate with a hybrid key, one walk in five below 1.0 in
// magnitude; 1: floats below 1.0 as they come and a -0 among them, which has
// no hybrid key, so the way packs under the order key (way_files.h); 2: any
// float below 16,384 in magnitude (the diff's content merge takes a
// coordinate x 1e5 as an int), so the way's values take up to 32 bits.
std::vector<uint32_t> way_nodes(std::mt19937& rng, uint32_t cnt, uint32_t flavour) {
    std::vector<uint32_t> nodes;
    if (flavour == 0) {
        const bool small = rng() % 5 == 0;
        int64_t lat = small ? static_cast<int64_t>(rng() % 9000000) : 400000000 + static_cast<int64_t>(rng() % 200000000);
        int64_t lng = small ? -static_cast<int64_t>(rng() % 9000000) : -static_cast<int64_t>(rng() % 1500000000);
        for (uint32_t k = 0; k < cnt; k++) {
            nodes.push_back(seven_decimals(lat));
            nodes.push_back(seven_decimals(lng));
            lat += static_cast<int64_t>(rng() % 4001) - 2000;
            lng += static_cast<int64_t>(rng() % 4001) - 2000;
        }
        return nodes;
    }
    for (uint32_t k = 0; k < 2 * cnt; k++) {
        const uint32_t sign = static_cast<uint32_t>(rng()) & 0x80000000u, any = static_cast<uint32_t>(rng());
        nodes.push_back(sign | (flavour == 1 ? any % coord_keys::ONE_BITS : (any & 0x007FFFFFu) | (((any >> 23) % 141) << 23)));
    }
    if (flavour == 1 && cnt) nodes[2 * (rng() % cnt)] = 0x80000000u;
    return nodes;
}

// A random way's fields: one of 300 names, and an interpolation's numbers
// (its last below its first one time in four) and kind.
Way way_fields(std::mt19937& rng, bool interp) {
    Way w;
    w.name = static_cast<uint32_t>(rng() % 300) * 8;
    if (interp) {
        w.start = 1 + static_cast<uint32_t>(rng() % 500);
        w.end = rng() % 4 ? w.start + static_cast<uint32_t>(rng() % 60) * 2 : w.start - 1;
        w.interpolation = static_cast<uint8_t>(1 + rng() % 3);
    }
    return w;
}

// A random way: one in sixteen without nodes, one in sixteen of one node,
// the others of 2 to 31; one in eight on coordinates without a hybrid key and
// one in eight on any floats.
Way random_way(std::mt19937& rng, bool interp) {
    Way w = way_fields(rng, interp);
    const uint32_t r = rng() % 16, flavour = rng() % 8;
    w.nodes = way_nodes(rng, r == 0 ? 0 : r == 1 ? 1 : 2 + rng() % 30, flavour == 0 ? 1 : flavour == 1 ? 2 : 0);
    return w;
}

// n ways of 3 to 9 nodes, the sixth of every sixteen on coordinates without
// a hybrid key and the tenth on any floats: each group of the pair's files
// holds a way under each key.
std::vector<Way> ways_with_nodes(std::mt19937& rng, bool interp, uint32_t n) {
    std::vector<Way> ways;
    for (uint32_t i = 0; i < n; i++) {
        Way w = way_fields(rng, interp);
        w.nodes = way_nodes(rng, 3 + rng() % 7, i % 16 == 5 ? 1 : i % 16 == 9 ? 2 : 0);
        ways.push_back(w);
    }
    return ways;
}

// The nodes of ways [from, to) of day.
uint64_t nodes_in(const std::vector<Way>& day, size_t from, size_t to) {
    uint64_t n = 0;
    for (size_t i = from; i < std::min(to, day.size()); i++) n += day[i].count();
    return n;
}

// The groups of new_day whose node blocks are the old day's group's at the
// same place: way for way the same nodes, and a node among them.
uint64_t groups_kept(const std::vector<Way>& old_day, const std::vector<Way>& new_day) {
    uint64_t kept = 0;
    for (size_t first = 0; first < new_day.size(); first += way_files::GROUP_WAYS) {
        const size_t end = std::min(new_day.size(), first + way_files::GROUP_WAYS);
        bool same = end <= old_day.size();
        for (size_t i = first; same && i < end; i++) same = new_day[i].nodes == old_day[i].nodes;
        kept += same && nodes_in(new_day, first, end) > 0;
    }
    return kept;
}

// What the way replay of a kind's pair did, from the patcher's log: the new
// groups it copied and those it encoded, and the old node bytes the encoded
// ones kept, by where their blocks were: in the old group the replay was at,
// in one it had passed (read again), or anywhere else (found by its cursor).
struct WayReplay {
    uint64_t copied = 0, encoded = 0, at_hand = 0, again = 0, by_cursor = 0;
    std::string text() const {
        return std::to_string(copied) + " copied, " + std::to_string(encoded) + " encoded; old node bytes " + std::to_string(at_hand) +
               " at hand, " + std::to_string(again) + " read again, " + std::to_string(by_cursor) + " by the cursor";
    }
    bool operator==(const WayReplay& o) const {
        return copied == o.copied && encoded == o.encoded && at_hand == o.at_hand && again == o.again && by_cursor == o.by_cursor;
    }
};

std::optional<WayReplay> way_replay_of(const std::string& log, const way_files::Kind& kind) {
    const std::string head = std::string("replayed with ") + kind.ways_file + ": groups ";
    const size_t at = log.find(head);
    WayReplay r;
    if (at == std::string::npos ||
        std::sscanf(log.c_str() + at + head.size(),
                    "%" SCNu64 " copied, %" SCNu64 " encoded; old node bytes %" SCNu64 " at hand, %" SCNu64 " read again, %" SCNu64
                    " by the cursor",
                    &r.copied, &r.encoded, &r.at_hand, &r.again, &r.by_cursor) != 5)
        return std::nullopt;
    return r;
}

// The log of a patcher run whose way replay of the kind's pair did `want`:
// "" or what it did instead.
LogCheck way_replay_did(const way_files::Kind& kind, const WayReplay& want) {
    return [&kind, want](const std::string& log) -> std::string {
        const std::optional<WayReplay> got = way_replay_of(log, kind);
        if (!got) return std::string("no way replay of ") + kind.ways_file + " in the log";
        return *got == want ? "" : std::string(kind.ways_file) + ": " + got->text() + ", not " + want.text();
    };
}

// Every check of checks on a log: the first failure.
LogCheck every_check(std::vector<LogCheck> checks) {
    return [checks](const std::string& log) -> std::string {
        for (const LogCheck& check : checks)
            if (std::string e = check(log); !e.empty()) return e;
        return "";
    };
}

// The out dir's two files of the kind's pair are the builder's for day.
bool wrote_the_builders_pair(const std::string& out, const way_files::Kind& kind, const BuildDir& day) {
    const BuildDir want = gctest::as_written(day);
    return read_file(out + "/" + kind.ways_file) == want.at(kind.ways_file) && read_file(out + "/" + kind.nodes_file) == want.at(kind.nodes_file);
}

// An op of a merge sequence, with its records (count of them at stride) when
// it carries some.
void put_op(std::vector<char>& ops, uint8_t kind, uint32_t count, const char* data = nullptr, size_t stride = 0) {
    append(ops, kind);
    append(ops, count);
    if (data) ops.insert(ops.end(), data, data + size_t{count} * stride);
}

// A merge section of a file of old_size bytes and new_size: no offset fixup
// runs, the offset fixup values of `values` and the string fixups of
// `strings`, each its count and its bytes (none unless given).
using Fixups = std::pair<uint32_t, std::vector<char>>;
std::vector<char> merge_of(PatchFileId fid, uint32_t stride, uint64_t old_size, uint64_t new_size, const std::vector<char>& ops,
                           const Fixups& values = {}, const Fixups& strings = {}) {
    std::vector<char> s = section_head(fid, stride, old_size, new_size);
    for (const uint32_t w : {0u, values.first, 0u, static_cast<uint32_t>(values.second.size())}) append(s, w);
    s.insert(s.end(), values.second.begin(), values.second.end());
    for (const uint32_t w : {strings.first, static_cast<uint32_t>(strings.second.size())}) append(s, w);
    s.insert(s.end(), strings.second.begin(), strings.second.end());
    append(s, static_cast<uint64_t>(ops.size()));
    s.insert(s.end(), ops.begin(), ops.end());
    return s;
}

// Where the merge section at `at` of a raw patch ends.
size_t merge_section_end(const std::vector<char>& raw, size_t at) {
    auto u32_at = [&](size_t p) {
        uint32_t v;
        memcpy(&v, raw.data() + p, 4);
        return v;
    };
    const size_t strings = at + 40 + u32_at(at + 32) + u32_at(at + 36), seq = strings + 8 + u32_at(strings + 4);
    uint64_t seq_size;
    memcpy(&seq_size, raw.data() + seq, 8);
    return seq + 8 + static_cast<size_t>(seq_size);
}

// A full replacement of a file of old_size bytes by `bytes`.
std::vector<char> replacement_of(PatchFileId fid, uint64_t old_size, const std::vector<char>& bytes) {
    std::vector<char> s = section_head(fid, 0, old_size, bytes.size());
    append(s, uint32_t{0});
    append(s, static_cast<uint64_t>(bytes.size()));
    s.insert(s.end(), bytes.begin(), bytes.end());
    return s;
}

// A day that leaves the kind's pair of d as it is, diffed, and the patch
// with the pair's two sections (each sent as unchanged) put again by hand.
struct WaySections {
    LowMemCase c;
    std::vector<char> raw;
    size_t at = std::string::npos, size = 0;
    uint64_t ways_bytes = 0, nodes_bytes = 0;
    const way_files::Kind* kind = nullptr;

    WaySections(const std::string& root, const std::string& name, const BuildDir& d, const way_files::Kind& k) : c{root, name}, kind(&k) {
        if (!prepare(c, d, d).empty()) return;
        raw = raw_patch(c);
        ways_bytes = d.at(k.ways_file).size();
        nodes_bytes = d.at(k.nodes_file).size();
        std::vector<char> pair = copy_old_section(ways_id(k), ways_bytes);
        const std::vector<char> nodes = copy_old_section(nodes_id(k), nodes_bytes);
        pair.insert(pair.end(), nodes.begin(), nodes.end());
        at = find_once(raw, pair);
        size = pair.size();
    }
    bool found() const { return at != std::string::npos; }
    // The patch with `sections` in place of the pair's two.
    std::vector<char> with(const std::vector<char>& sections) const {
        std::vector<char> edited(raw.begin(), raw.begin() + static_cast<std::ptrdiff_t>(at));
        edited.insert(edited.end(), sections.begin(), sections.end());
        edited.insert(edited.end(), raw.begin() + static_cast<std::ptrdiff_t>(at + size), raw.end());
        return edited;
    }
    // The ways' merge by ways_ops (with the offset fixup values and the
    // string fixups given) and the nodes' by nodes_ops, each file of its size
    // on both days.
    std::vector<char> merges(const std::vector<char>& ways_ops, const std::vector<char>& nodes_ops, const Fixups& values = {},
                             const Fixups& strings = {}) const {
        std::vector<char> s = merge_of(ways_id(*kind), static_cast<uint32_t>(kind->stride), ways_bytes, ways_bytes, ways_ops, values, strings);
        const std::vector<char> n = merge_of(nodes_id(*kind), 8, nodes_bytes, nodes_bytes, nodes_ops);
        s.insert(s.end(), n.begin(), n.end());
        return s;
    }
};

}  // namespace

// Random days of both way pairs as strategy 2 leaves them: an old day of 300
// to 500 ways a kind (without nodes, of one node, of up to 31; on
// coordinates of seven decimals, on ones without a hybrid key and on any
// floats; dead slots among them) and a new day that renames some, moves, adds
// or drops a node of some or gives them other nodes, kills some in place,
// fills dead slots and appends up to 40. Merged by slot identity and by
// content, in every mode the patcher writes the builder's way files of the
// new day byte for byte, copying the groups whose ways keep their nodes (the
// log counts them: each of them when merged by slot, nine in ten at least
// when by content) and encoding the others, their old nodes decoded from the
// old group the replay is at.
TEST(patch_rebuilds_the_builders_way_files_on_random_days) {
    ScratchDir dir("lowmem-way-days");
    for (uint32_t seed = 1; seed <= 4; seed++) {
        std::mt19937 rng(20261116 + seed);
        const bool by_slot = seed % 2 == 1;
        BuildDir old_d, new_d;
        struct Pair {
            const way_files::Kind* kind;
            uint64_t groups, kept;
        };
        std::vector<Pair> pairs;
        for (const way_files::Kind* kind : BOTH_WAY_KINDS) {
            std::vector<Way> old_day;
            for (uint32_t i = 0, n = 300 + rng() % 200; i < n; i++) old_day.push_back(i % 41 == 7 ? DEAD_WAY : random_way(rng, kind->interp));
            std::vector<Way> new_day = old_day;
            for (Way& w : new_day) {
                const uint32_t r = rng() % 160, cnt = w.count();
                const bool dead = w.name == NO;
                if (dead && r < 40) w = random_way(rng, kind->interp);                         // a dead slot filled
                if (dead) continue;
                if (r == 0) w.name += 8;                                                       // renamed
                if (r == 1 && kind->interp) w.end += 2;                                        // another last number
                if (r == 2 && cnt) w.nodes[rng() % w.nodes.size()] ^= 1u << (rng() % 12);      // a node moved
                if (r == 3) w.nodes.insert(w.nodes.end(), {seven_decimals(471234567), seven_decimals(81234567)});  // a node more
                if (r == 4 && cnt) w.nodes.resize(w.nodes.size() - 2);                         // a node fewer
                if (r == 5) w.nodes = way_nodes(rng, cnt, rng() % 3);                          // other nodes
                if (r == 6) w = DEAD_WAY;                                                      // dead in place
            }
            for (uint32_t i = 0, n = rng() % 41; i < n; i++) new_day.push_back(random_way(rng, kind->interp));
            put_ways(old_d, *kind, old_day);
            put_ways(new_d, *kind, new_day);
            pairs.push_back({kind, (new_day.size() + way_files::GROUP_WAYS - 1) / way_files::GROUP_WAYS, groups_kept(old_day, new_day)});
        }
        const LowMemCase c{dir.path(), "seed-" + std::to_string(seed)};
        REQUIRE(prepare(c, old_d, new_d, {}, {}, by_slot ? std::vector<std::string>{"--identity-slots"} : std::vector<std::string>{}).empty());

        std::string did;
        const LogCheck counts = [&](const std::string& log) -> std::string {
            did.clear();
            for (const Pair& p : pairs) {
                const std::optional<WayReplay> r = way_replay_of(log, *p.kind);
                if (!r) return std::string("no way replay of ") + p.kind->ways_file + " in the log";
                did += std::string(did.empty() ? "" : "; ") + p.kind->ways_file + ": " + std::to_string(p.kept) + " of " + std::to_string(p.groups) +
                       " groups kept, " + r->text();
                if (r->copied + r->encoded != p.groups) return "not every group copied or encoded: " + did;
                if (r->copied > p.kept || (by_slot ? r->copied != p.kept : r->copied * 10 < p.kept * 9)) return "kept groups not copied: " + did;
                if (r->encoded == 0 || r->at_hand == 0) return "no group encoded from the old group at hand: " + did;
                if (r->by_cursor != 0) return "old nodes read through the cursor: " + did;
            }
            return "";
        };
        CHECK(apply_modes(c, ALL_MODES, counts).bad.empty());
        for (const std::string& mode : ALL_MODES)
            for (const Pair& p : pairs) CHECK(wrote_the_builders_pair(c.dir("out." + mode), *p.kind, new_d));
        std::printf("    seed %u, by %s: %s (the last mode's)\n", seed, by_slot ? "slot" : "content", did.c_str());
    }
}

// A group with one edited way is encoded and the groups beside it are
// copied. 87 ways of 3 to 9 nodes, five groups and one of seven ways, a way
// under each key in each. The new day moves the second node of way 20,
// gives way 40 a node more and moves the first node of way 64, its group's
// anchor; it renames way 50. Groups 1, 2 and 4 are encoded, every node they
// keep decoded from the old group at hand (all but the two moved ones), and
// groups 0, 3 (its row written again with the new name) and 5 are copied.
// The same by slot identity, where the changed records go as REPLACE runs,
// and by content, where each is deleted and inserted in its place.
TEST(patch_encodes_a_way_group_with_one_edited_way) {
    ScratchDir dir("lowmem-way-edit");
    for (const way_files::Kind* kind : BOTH_WAY_KINDS)
        for (const bool by_slot : {true, false}) {
            std::mt19937 rng(20261117);
            const std::vector<Way> old_day = ways_with_nodes(rng, kind->interp, 87);
            std::vector<Way> new_day = old_day;
            new_day[20].nodes[2] ^= 0x10;
            new_day[40].nodes.insert(new_day[40].nodes.end(), {new_day[40].nodes[0], new_day[40].nodes[1]});
            new_day[50].name += 8;
            new_day[64].nodes[0] ^= 4;
            BuildDir old_d, new_d;
            put_ways(old_d, *kind, old_day);
            put_ways(new_d, *kind, new_day);
            const LowMemCase c{dir.path(), std::string(kind->interp ? "interps" : "streets") + (by_slot ? "-by-slot" : "-by-content")};
            REQUIRE(prepare(c, old_d, new_d, {}, {}, by_slot ? std::vector<std::string>{"--identity-slots"} : std::vector<std::string>{}).empty());
            const MergeSection merge = merge_section(raw_patch(c), ways_id(*kind), static_cast<uint32_t>(kind->stride),
                                                     old_d.at(kind->ways_file).size(), new_d.at(kind->ways_file).size());
            REQUIRE(merge.found);
            const uint8_t sent_as = by_slot ? OP_REPLACE_RUN : OP_INSERT_RUN;
            REQUIRE(std::count_if(merge.ops.begin(), merge.ops.end(), [&](const auto& op) { return op.first == sent_as; }) > 0);
            REQUIRE(merge.fixup_runs + merge.fixup_values > 0);

            const WayReplay want{3, 3, 8 * (nodes_in(old_day, 16, 32) - 1 + nodes_in(old_day, 32, 48) + nodes_in(old_day, 64, 80) - 1), 0, 0};
            CHECK(apply_modes(c, ALL_MODES, way_replay_did(*kind, want)).bad.empty());
            for (const std::string& mode : ALL_MODES) CHECK(wrote_the_builders_pair(c.dir("out." + mode), *kind, new_d));
        }
}

// A run of ways that is not at its old position is encoded: the blocks of a
// group are packed against its first node, so they are another group's only
// when the group holds the same ways. 100 ways of 3 to 9 nodes, merged by
// content:
//   a way more at index 20: group 0 is copied and the six after it are
//     encoded, each from the old group the replay is at and the last way of
//     the one before it, which it reads again;
//   way 20 gone: the same, each group from the old group the replay passed
//     last and the first way of the one it is at;
//   sixteen ways more at index 32: only their own group is encoded, since
//     every later group is an old group again, one further on;
//   ways 32 to 47 gone: every group is an old one.
TEST(patch_encodes_a_run_of_ways_that_is_not_at_its_old_position) {
    ScratchDir dir("lowmem-way-shift");
    for (const way_files::Kind* kind : BOTH_WAY_KINDS) {
        std::mt19937 rng(20261118);
        const std::vector<Way> old_day = ways_with_nodes(rng, kind->interp, 100);
        const std::vector<Way> more = ways_with_nodes(rng, kind->interp, 16);
        auto nodes_of = [&](std::initializer_list<size_t> ways) {
            uint64_t n = 0;
            for (const size_t i : ways) n += old_day[i].count();
            return n;
        };
        struct Case {
            std::string name;
            std::vector<Way> new_day;
            WayReplay want;
        };
        std::vector<Case> cases(4);
        cases[0].name = "a-way-more";
        cases[0].new_day = old_day;
        cases[0].new_day.insert(cases[0].new_day.begin() + 20, more[0]);
        const uint64_t before_each = nodes_of({31, 47, 63, 79, 95});
        cases[0].want = {1, 6, 8 * (nodes_in(old_day, 16, 100) - before_each), 8 * before_each, 0};
        cases[1].name = "a-way-fewer";
        cases[1].new_day = old_day;
        cases[1].new_day.erase(cases[1].new_day.begin() + 20);
        const uint64_t at_hand = nodes_of({32, 48, 64, 80, 96, 97, 98, 99});
        cases[1].want = {1, 6, 8 * at_hand, 8 * (nodes_in(old_day, 16, 100) - old_day[20].count() - at_hand), 0};
        cases[2].name = "a-group-more";
        cases[2].new_day = old_day;
        cases[2].new_day.insert(cases[2].new_day.begin() + 32, more.begin(), more.end());
        cases[2].want = {7, 1, 0, 0, 0};
        cases[3].name = "a-group-fewer";
        cases[3].new_day = old_day;
        cases[3].new_day.erase(cases[3].new_day.begin() + 32, cases[3].new_day.begin() + 48);
        cases[3].want = {6, 0, 0, 0, 0};
        for (const Case& e : cases) {
            BuildDir old_d, new_d;
            put_ways(old_d, *kind, old_day);
            put_ways(new_d, *kind, e.new_day);
            const LowMemCase c{dir.path(), std::string(kind->interp ? "interps-" : "streets-") + e.name};
            REQUIRE(prepare(c, old_d, new_d).empty());

            CHECK(apply_modes(c, ALL_MODES, way_replay_did(*kind, e.want)).bad.empty());
            for (const std::string& mode : ALL_MODES) CHECK(wrote_the_builders_pair(c.dir("out." + mode), *kind, new_d));
        }
    }
}

// The last group of a pair as the new day shortens it, lengthens it or
// leaves it without nodes. 40 ways, two groups and one of eight ways, the
// first two of those without nodes, merged by content:
//   the last five ways gone: the three left are the old group's first three
//     and the last of them has nodes, so their blocks are copied and the row
//     cut;
//   the last six gone: the two left have no nodes, and the old row's anchor
//     is the first node of a way that is gone, so the group is encoded,
//     with no anchor;
//   five ways more: the group is encoded, its old ways' nodes from the old
//     group at hand;
//   five ways without nodes more: they have no block, so the group's blocks
//     are the old group's, copied;
//   the pair's every way gone: the patcher writes the two footers.
// And 32 ways without nodes, one renamed: a group without nodes has no block
// to copy.
TEST(patch_replays_a_last_way_group_that_shrinks_or_grows) {
    ScratchDir dir("lowmem-way-last");
    for (const way_files::Kind* kind : BOTH_WAY_KINDS) {
        std::mt19937 rng(20261119);
        std::vector<Way> old_day = ways_with_nodes(rng, kind->interp, 40);
        old_day[32].nodes.clear();
        old_day[33].nodes.clear();
        const std::vector<Way> more = ways_with_nodes(rng, kind->interp, 5);
        struct Case {
            std::string name;
            std::vector<Way> new_day;
            WayReplay want;
        };
        std::vector<Case> cases(5);
        cases[0] = {"five-fewer", {old_day.begin(), old_day.begin() + 35}, {3, 0, 0, 0, 0}};
        cases[1] = {"six-fewer", {old_day.begin(), old_day.begin() + 34}, {2, 1, 0, 0, 0}};
        cases[2] = {"five-more", old_day, {2, 1, 8 * nodes_in(old_day, 32, 40), 0, 0}};
        cases[2].new_day.insert(cases[2].new_day.end(), more.begin(), more.end());
        cases[3] = {"five-bare-more", old_day, {3, 0, 0, 0, 0}};
        for (Way w : more) {
            w.nodes.clear();
            cases[3].new_day.push_back(w);
        }
        cases[4] = {"none", {}, {0, 0, 0, 0, 0}};
        for (const Case& e : cases) {
            // A street pair cannot lose its last node by patch.
            if (e.new_day.empty() && !kind->interp) continue;
            BuildDir old_d, new_d;
            put_ways(old_d, *kind, old_day);
            put_ways(new_d, *kind, e.new_day);
            const LowMemCase c{dir.path(), std::string(kind->interp ? "interps-" : "streets-") + e.name};
            REQUIRE(prepare(c, old_d, new_d).empty());

            CHECK(apply_modes(c, ALL_MODES, way_replay_did(*kind, e.want)).bad.empty());
            for (const std::string& mode : ALL_MODES) CHECK(wrote_the_builders_pair(c.dir("out." + mode), *kind, new_d));
        }

        std::vector<Way> bare(32), renamed;
        for (Way& w : bare) w = way_fields(rng, kind->interp);
        renamed = bare;
        renamed[20].name += 8;
        BuildDir old_d, new_d;
        put_ways(old_d, *kind, bare);
        put_ways(new_d, *kind, renamed);
        const LowMemCase c{dir.path(), std::string(kind->interp ? "interps" : "streets") + "-without-nodes"};
        REQUIRE(prepare(c, old_d, new_d).empty());
        CHECK(apply_modes(c, ALL_MODES, way_replay_did(*kind, {0, 2, 0, 0, 0})).bad.empty());
    }
}

// The dedicated replay runs when one of a pair's two sections is sent as
// unchanged, that side read as one MATCH of all of it, and copies the groups
// the day leaves whole. 87 ways of 3 to 9 nodes:
//   a node of way 20 moved: the records are unchanged and the nodes merge;
//     group 1 is encoded and the other five copied;
//   way 50 renamed: the records merge and the nodes are unchanged; every
//     group is copied, group 3's row with the new name;
//   the last node of way 20 given to way 21 as its first: the nodes are
//     unchanged and group 1 is encoded all the same, since its blocks are
//     packed a way at a time.
// In each the log names the pair's decoder and encoder, as it does for any
// family that is replayed.
TEST(patch_copies_way_groups_under_an_unchanged_parent_or_child) {
    ScratchDir dir("lowmem-way-half");
    for (const way_files::Kind* kind : BOTH_WAY_KINDS) {
        std::mt19937 rng(20261120);
        const std::vector<Way> old_day = ways_with_nodes(rng, kind->interp, 87);
        std::vector<Way> moved = old_day, renamed = old_day, handed = old_day;
        moved[20].nodes[2] ^= 0x10;
        renamed[50].name += 8;
        handed[21].nodes.insert(handed[21].nodes.begin(), handed[20].nodes.end() - 2, handed[20].nodes.end());
        handed[20].nodes.resize(handed[20].nodes.size() - 2);
        const std::string family = kind->interp ? "interps" : "streets";
        struct Case {
            std::string name;
            const std::vector<Way>& new_day;
            PatchFileId unchanged;
            WayReplay want;
        };
        const Case cases[] = {
            {"nodes-moved", moved, ways_id(*kind), {5, 1, 8 * (nodes_in(old_day, 16, 32) - 1), 0, 0}},
            {"renamed", renamed, nodes_id(*kind), {6, 0, 0, 0, 0}},
            {"node-handed-over", handed, nodes_id(*kind), {5, 1, 8 * nodes_in(old_day, 16, 32), 0, 0}},
        };
        for (const Case& e : cases) {
            BuildDir old_d, new_d;
            put_ways(old_d, *kind, old_day);
            put_ways(new_d, *kind, e.new_day);
            const LowMemCase c{dir.path(), family + "-" + e.name};
            REQUIRE(prepare(c, old_d, new_d).empty());
            const std::string unchanged = patch_file_names[static_cast<uint32_t>(e.unchanged)];
            REQUIRE(copied_old_at(raw_patch(c), e.unchanged, old_d.at(unchanged).size()) != std::string::npos);

            const LogCheck codecs = [&](const std::string& log) -> std::string {
                for (const char* codec : {"decode ", "encode "})
                    if (log.find(codec + family + ": queue peaks ") == std::string::npos) return "no " + std::string(codec) + family + " line";
                return "";
            };
            CHECK(apply_modes(c, ALL_MODES, every_check({way_replay_did(*kind, e.want), codecs})).bad.empty());
            for (const std::string& mode : ALL_MODES) CHECK(wrote_the_builders_pair(c.dir("out." + mode), *kind, new_d));
        }
    }
}

// Old nodes the encoded groups keep that are not in the old group the replay
// is at: 6,000 ways of two nodes on a day that changes nothing, the pair's
// two sections put again by hand, each giving the old pair back in every
// mode:
//   every way deleted and then inserted, the nodes one MATCH: the replay is
//     at the old pair's last group when the first new way comes. That group
//     it copies; the ways of the groups it passed last it reads again, and
//     those of the groups before them it finds with its cursor;
//   every way inserted and then deleted, the nodes one MATCH: no old group
//     is at hand, and the cursor finds every way;
//   the first group's ways matched, every other way inserted and then
//     deleted, the nodes one MATCH: the first group is copied, and the cursor
//     finds every way after it, the first of them the one right after the
//     group at hand;
//   every way matched, the nodes sent whole but for the last way's, which
//     are the first way's and MATCH those: the cursor finds the first way
//     from the pair's last group.
// The cursor reads a row before the replay has checked it: with the last
// group's row giving its blocks a megabyte past the nodes file, a patch that
// inserts every way first is refused by name at that group.
TEST(patch_way_replay_reads_old_nodes_again_and_through_its_cursor) {
    ScratchDir dir("lowmem-way-cursor");
    constexpr uint32_t WAYS = 6000;
    for (const way_files::Kind* kind : BOTH_WAY_KINDS) {
        std::mt19937 rng(20261121);
        std::vector<Way> day;
        for (uint32_t i = 0; i < WAYS; i++) {
            Way w = way_fields(rng, kind->interp);
            w.nodes = way_nodes(rng, 2, i % 16 == 5 ? 1 : 0);
            day.push_back(w);
        }
        day[WAYS - 1].nodes = day[0].nodes;
        BuildDir d;
        put_ways(d, *kind, day);
        const WaySections s(dir.path(), kind->interp ? "interps" : "streets", d, *kind);
        REQUIRE(s.found());
        const std::vector<char>& records = d.at(kind->ways_file);
        const std::vector<char>& nodes = d.at(kind->nodes_file);
        const uint32_t n_nodes = static_cast<uint32_t>(nodes.size() / 8);
        const uint64_t groups = WAYS / way_files::GROUP_WAYS, group_bytes = 8 * 2 * way_files::GROUP_WAYS;

        struct Case {
            std::string name;
            std::vector<char> ways_ops, nodes_ops;
            std::function<std::string(const WayReplay&)> wrong;
        };
        Case deleted_first{"deleted-first", {}, {}, [&](const WayReplay& r) {
            return r.copied == 1 && r.encoded == groups - 1 && r.at_hand == 0 && r.again > 0 && r.by_cursor > 0 &&
                           r.again + r.by_cursor == (groups - 1) * group_bytes
                       ? ""
                       : "not the last group copied and the others encoded from groups read again and found by the cursor";
        }};
        put_op(deleted_first.ways_ops, OP_DELETE_RUN, WAYS);
        put_op(deleted_first.ways_ops, OP_INSERT_RUN, WAYS, records.data(), kind->stride);
        put_op(deleted_first.nodes_ops, OP_MATCH_RUN, n_nodes);
        Case inserted_first{"inserted-first", {}, {}, [&](const WayReplay& r) {
            return r.copied == 0 && r.encoded == groups && r.at_hand == 0 && r.again == 0 && r.by_cursor == groups * group_bytes
                       ? ""
                       : "not every group encoded from ways the cursor found";
        }};
        put_op(inserted_first.ways_ops, OP_INSERT_RUN, WAYS, records.data(), kind->stride);
        put_op(inserted_first.ways_ops, OP_DELETE_RUN, WAYS);
        put_op(inserted_first.nodes_ops, OP_MATCH_RUN, n_nodes);
        Case one_matched{"one-group-matched", {}, {}, [&](const WayReplay& r) {
            return r.copied == 1 && r.encoded == groups - 1 && r.at_hand == 0 && r.again == 0 && r.by_cursor == (groups - 1) * group_bytes
                       ? ""
                       : "not the first group copied and the others encoded from ways the cursor found";
        }};
        put_op(one_matched.ways_ops, OP_MATCH_RUN, way_files::GROUP_WAYS);
        put_op(one_matched.ways_ops, OP_INSERT_RUN, WAYS - way_files::GROUP_WAYS, records.data() + way_files::GROUP_WAYS * kind->stride,
               kind->stride);
        put_op(one_matched.ways_ops, OP_DELETE_RUN, WAYS - way_files::GROUP_WAYS);
        put_op(one_matched.nodes_ops, OP_MATCH_RUN, n_nodes);
        Case matched_late{"matched-late", {}, {}, [&](const WayReplay& r) {
            return r.copied == 0 && r.encoded == groups && r.at_hand == 0 && r.again == 0 && r.by_cursor == 16
                       ? ""
                       : "not every group encoded and the first way found by the cursor";
        }};
        put_op(matched_late.ways_ops, OP_MATCH_RUN, WAYS);
        put_op(matched_late.nodes_ops, OP_INSERT_RUN, n_nodes - 2, nodes.data(), 8);
        put_op(matched_late.nodes_ops, OP_MATCH_RUN, 2);
        for (const Case& e : {deleted_first, inserted_first, one_matched, matched_late}) {
            REQUIRE(write_patch(s.c, s.c.patch(), s.with(s.merges(e.ways_ops, e.nodes_ops))));
            const LogCheck check = [&](const std::string& log) -> std::string {
                const std::optional<WayReplay> r = way_replay_of(log, *kind);
                if (!r) return "no way replay in the log";
                const std::string wrong = e.wrong(*r);
                return wrong.empty() ? "" : e.name + ": " + wrong + ": " + r->text();
            };
            CHECK(apply_modes(s.c, ALL_MODES, check).bad.empty());
        }

        // The last group's row: its off, the row's first four bytes.
        const std::string rows_path = s.c.dir("old") + "/" + kind->ways_file;
        const std::vector<char> rows = read_file(rows_path);
        const size_t last_row = (groups - 1) * kind->row_bytes(way_files::GROUP_WAYS);
        REQUIRE(rows.size() == groups * kind->row_bytes(way_files::GROUP_WAYS) + way_files::WAYS_FOOTER_BYTES);
        std::vector<char> far = rows;
        uint32_t off;
        memcpy(&off, rows.data() + last_row, 4);
        put_at(far, last_row, off + (1u << 20));
        REQUIRE(write_file(rows_path, far));
        const std::string patch = s.c.dir("inserted-first.gcpatch");
        REQUIRE(write_patch(s.c, patch, s.with(s.merges(inserted_first.ways_ops, inserted_first.nodes_ops))));
        CHECK(not_refused(s.c, ALL_MODES, patch, "far-row",
                          std::string("Malformed way files: ") + kind->nodes_file + " ends inside the block of way " +
                              std::to_string(WAYS - way_files::GROUP_WAYS))
                  .empty());
        REQUIRE(write_file(rows_path, rows));

        REQUIRE(write_patch(s.c, s.c.patch(), s.raw));
        CHECK(apply_modes(s.c, ALL_MODES).bad.empty());
    }
}

// A way of the most nodes a way holds, 65,535 (524,280 bytes of them), on
// any floats: way 10 in a group the day leaves alone, whose blocks are copied
// in pieces; way 20 beside a way that moves a node, decoded and encoded
// whole; and way 37 with its own last node moved, its other nodes taken from
// its old block. The replay holds one old way's block and its nodes and one
// new way's nodes, and none of the pair's files.
TEST(patch_way_replay_holds_one_way_of_the_most_nodes) {
    ScratchDir dir("lowmem-way-most");
    constexpr uint32_t MOST = way_files::MAX_WAY_NODES;
    constexpr uint64_t WAY_BYTES = 8 * uint64_t{MOST};
    std::mt19937 rng(20261122);
    std::vector<Way> old_day = ways_with_nodes(rng, false, 48);
    for (const size_t i : {10, 20, 37}) old_day[i].nodes = way_nodes(rng, MOST, 2);
    std::vector<Way> new_day = old_day;
    new_day[21].nodes[2] ^= 0x10;
    new_day[37].nodes[2 * MOST - 2] ^= 0x10;
    BuildDir old_d, new_d;
    put_ways(old_d, way_files::STREETS, old_day);
    put_ways(new_d, way_files::STREETS, new_day);
    const LowMemCase c{dir.path(), "most"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    const WayReplay want{1, 2, 8 * (nodes_in(old_day, 16, 48) - 2), 0, 0};
    // An old way's block and its nodes, and the blocks of a small group read
    // with one read; a new way's nodes and its group's records.
    const LogCheck hold = every_check({way_replay_did(way_files::STREETS, want), decoder_peaks_at_most("streets", 2 * WAY_BYTES + (64 << 10)),
                                  encoder_peaks_at_most("streets", WAY_BYTES)});
    CHECK(apply_modes(c, ALL_MODES, hold).bad.empty());
    for (const std::string& mode : ALL_MODES) CHECK(wrote_the_builders_pair(c.dir("out." + mode), way_files::STREETS, new_d));
}

// The patcher's heap as a pair it replays grows tenfold: n street ways of two
// nodes, merged by slot identity, one in 777 renamed (its group copied, the
// row written again) and one in 1,000 with a node moved (its group encoded).
// The replay holds a group's row and blocks of each day's files.
TEST(patch_heap_stays_flat_as_way_groups_are_copied_and_encoded) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::vector<Way> old_day(n);
        for (uint32_t i = 0; i < n; i++) {
            old_day[i].name = i % 1000 * 8;
            const int64_t lat = 470000000 + static_cast<int64_t>(i % 100000) * 37, lng = 80000000 + static_cast<int64_t>(i % 77777) * 53;
            old_day[i].nodes = {seven_decimals(lat), seven_decimals(lng), seven_decimals(lat + 1234), seven_decimals(lng - 987)};
        }
        std::vector<Way> new_day = old_day;
        for (uint32_t i = 0; i < n; i++) {
            if (i % 777 == 5) new_day[i].name += 8;
            if (i % 1000 == 9) new_day[i].nodes[2] ^= 0x10;
        }
        BuildDir old_d, new_d;
        put_ways(old_d, way_files::STREETS, old_day);
        put_ways(new_d, way_files::STREETS, new_day);
        if (std::string e = prepare(c, old_d, new_d, {}, {}, {"--identity-slots"}); !e.empty()) return e;
        const uint64_t bytes = uint64_t{WAY_HEADER_STRIDE} * n;
        return patch_holds(c, section_head(PatchFileId::STREET_WAYS, WAY_HEADER_STRIDE, bytes, bytes),
                           "merge of " + std::to_string(n) + " street ways");
    }, 100000).empty());
}

// The hashes of a pair's two old files, which the diff sends before the
// ways' section, each once, are what the way replay checks the bytes it
// copies by: a patch without either is refused in every mode before a way is
// replayed, and one with another hash for either when the replay has passed
// the file. 40 streets, one renamed, and 30 interpolations, a node of one
// moved; the patch as made applies. On a day that changes neither pair the
// pairs are copied as their files sit, each file held to its hash first: a
// patch with another hash for any of the four is refused. The hash of a pair
// the old day holds without ways, which has nothing to copy, is read and
// checks nothing: a patch with other hashes applies.
TEST(patch_refuses_a_way_replay_without_its_hashes) {
    ScratchDir dir("lowmem-way-hashes");
    std::mt19937 rng(20261123);
    const std::vector<Way> streets = ways_with_nodes(rng, false, 40), interps = ways_with_nodes(rng, true, 30);
    std::vector<Way> renamed = streets, moved = interps;
    renamed[30].name += 8;
    moved[5].nodes[2] ^= 0x10;
    // The four hash sections of c's patch, where they sit, each before its
    // pair's first section; none unless each is there once.
    struct Hash {
        const way_files::Kind* kind;
        PatchFileId file;
        size_t at;
    };
    auto hashes_of = [](const LowMemCase& c, const std::vector<char>& raw) {
        std::vector<Hash> hashes;
        for (const way_files::Kind* kind : BOTH_WAY_KINDS) {
            std::vector<char> first;
            append(first, static_cast<uint32_t>(ways_id(*kind)));
            for (const PatchFileId file : {ways_id(*kind), nodes_id(*kind)}) {
                const std::vector<char> old_file = read_file(c.dir("old") + "/" + patch_file_names[static_cast<uint32_t>(file)]);
                const size_t at = find_once(raw, old_file_hash(file, lane_hash(old_file.data(), old_file.size())));
                if (at == std::string::npos) return std::vector<Hash>{};
                hashes.push_back({kind, file, at});
            }
            // The two back to back, and the ways' section after them.
            const size_t n = hashes.size();
            if (hashes[n - 1].at != hashes[n - 2].at + 16 || raw.size() < hashes[n - 1].at + 20 ||
                memcmp(raw.data() + hashes[n - 1].at + 16, first.data(), 4) != 0)
                return std::vector<Hash>{};
        }
        return hashes;
    };

    BuildDir old_d, new_d;
    put_ways(old_d, way_files::STREETS, streets);
    put_ways(old_d, way_files::INTERPS, interps);
    put_ways(new_d, way_files::STREETS, renamed);
    put_ways(new_d, way_files::INTERPS, moved);
    const LowMemCase c{dir.path(), "replayed"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    const std::vector<Hash> hashes = hashes_of(c, raw);
    REQUIRE(hashes.size() == 4);
    for (const Hash& h : hashes) {
        const std::string file = patch_file_names[static_cast<uint32_t>(h.file)];
        std::vector<char> cut = raw, other = raw;
        cut.erase(cut.begin() + static_cast<std::ptrdiff_t>(h.at), cut.begin() + static_cast<std::ptrdiff_t>(h.at + 16));
        other[h.at + 8] ^= 1;
        REQUIRE(write_patch(c, c.dir(file + ".cut.gcpatch"), cut));
        REQUIRE(write_patch(c, c.dir(file + ".other.gcpatch"), other));

        CHECK(not_refused(c, ALL_MODES, c.dir(file + ".cut.gcpatch"), file + ".cut",
                          std::string("Malformed patch: ") + h.kind->ways_file + " replayed without the old " + file + "'s hash")
                  .empty());
        CHECK(not_refused(c, ALL_MODES, c.dir(file + ".other.gcpatch"), file + ".other", "Old " + file + " is not the one the patch was made from")
                  .empty());
        // Refused before the pair is replayed: no file of it is written.
        for (const std::string& mode : ALL_MODES)
            CHECK(!std::filesystem::exists(c.dir(file + ".cut.out." + mode) + "/" + h.kind->ways_file));
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());

    BuildDir none;
    put_ways(none, way_files::STREETS, streets);
    put_ways(none, way_files::INTERPS, {});
    struct Unused {
        std::string name;
        const BuildDir& old_day;
        const BuildDir& new_day;
    };
    for (const Unused& u : {Unused{"unchanged", old_d, old_d}, Unused{"first-ways", none, old_d}}) {
        const LowMemCase day{dir.path(), u.name};
        REQUIRE(prepare(day, u.old_day, u.new_day).empty());
        std::vector<char> other = raw_patch(day);
        const std::vector<Hash> sent = hashes_of(day, other);
        REQUIRE(sent.size() == 4);
        const LogCheck no_replay = [&](const std::string& log) {
            return way_replay_of(log, way_files::INTERPS) || way_replay_of(log, way_files::STREETS) ? std::string("a way replay ran") : std::string();
        };
        CHECK(apply_modes(day, ALL_MODES, no_replay).bad.empty());
        if (std::string(u.name) == "unchanged") {
            for (const Hash& h : sent) {
                std::vector<char> another = other;
                another[h.at + 8] ^= 1;
                REQUIRE(write_patch(day, day.patch(), another));
                const std::string file = patch_file_names[static_cast<uint32_t>(h.file)];
                CHECK(not_refused(day, ALL_MODES, day.patch(), file, "Old " + file + " is not the one the patch was made from").empty());
            }
            continue;
        }
        // The interpolations' first ways: the old pair holds nothing to copy or to check.
        for (const Hash& h : sent) {
            if (h.kind == &way_files::INTERPS) other[h.at + 8] ^= 1;
        }
        REQUIRE(write_patch(day, day.patch(), other));
        CHECK(apply_modes(day, ALL_MODES, no_replay).bad.empty());
    }
}

// A child stream's merge section counts its runs by its file's unit, a
// node's 8 bytes: one at another stride is refused in every mode. On the day
// a dir gets its first interpolation ways the nodes replay beside their ways
// with no old pair to replay on: their section at a stride of 4 is refused by
// name. The patch as made applies.
TEST(patch_refuses_a_child_section_at_another_stride_than_its_files) {
    ScratchDir dir("lowmem-child-stride");
    std::mt19937 rng(20261116);
    BuildDir none, some;
    put_ways(none, way_files::INTERPS, {});
    put_ways(some, way_files::INTERPS, ways_with_nodes(rng, true, 20));
    const LowMemCase c{dir.path(), "first-ways"};
    REQUIRE(prepare(c, none, some).empty());
    const std::vector<char> raw = raw_patch(c);
    const size_t at = find_once(raw, section_head(PatchFileId::INTERP_NODES, 8, 0, some.at("interp_nodes.bin").size()));
    REQUIRE(at != std::string::npos);
    std::vector<char> edited = raw;
    put_at(edited, at + 4, 4u);
    const std::string patch = c.dir("stride.gcpatch");
    REQUIRE(write_patch(c, patch, edited));

    CHECK(not_refused(c, ALL_MODES, patch, "stride", "Encoded family: unexpected section kind for a child stream").empty());
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A byte of an old way file that is not the one the diff read is refused,
// whichever it is: every byte of the pair's two files flipped in turn (one
// in five under the sanitizers), of 20 streets and of 18 interpolations of 2
// to 4 nodes on a day that moves a node of way 3, so that its group is
// encoded, and renames way 17, whose group is copied. The files' own checks
// refuse some (a footer, a row's place, a block the decoder refuses), the
// replay some as a patch whose nodes do not fit the ways, and the hash of the
// file every other: a block of the copied group, which the replay never
// decodes, and that group's anchor and the name of a way that keeps it,
// which go into the new row as they are.
TEST(patch_refuses_a_flipped_byte_in_an_old_way_file_by_its_hash) {
    ScratchDir dir("lowmem-way-flip");
    const size_t step = gctest::heap_peak_shim() ? 1 : 5;
    for (const way_files::Kind* kind : BOTH_WAY_KINDS) {
        std::mt19937 rng(20261124);
        std::vector<Way> old_day;
        for (uint32_t i = 0; i < (kind->interp ? 18u : 20u); i++) {
            Way w = way_fields(rng, kind->interp);
            w.nodes = way_nodes(rng, 2 + rng() % 3, i % 16 == 5 ? 1 : 0);
            old_day.push_back(w);
        }
        std::vector<Way> new_day = old_day;
        new_day[3].nodes[2] ^= 0x10;
        new_day[17].name += 8;
        BuildDir old_d, new_d;
        put_ways(old_d, *kind, old_day);
        put_ways(new_d, *kind, new_day);
        const LowMemCase c{dir.path(), kind->interp ? "interps" : "streets"};
        REQUIRE(prepare(c, old_d, new_d, {}, {}, {"--identity-slots"}).empty());
        CHECK(apply_modes(c, ALL_MODES, way_replay_did(*kind, {1, 1, 8 * (nodes_in(old_day, 0, 16) - 1), 0, 0})).bad.empty());

        // The second group's row, after the first's, and where its blocks
        // start.
        const std::vector<char> rows = read_file(c.dir("old") + "/" + kind->ways_file);
        const size_t row_at = kind->row_bytes(way_files::GROUP_WAYS), second_ways = old_day.size() - way_files::GROUP_WAYS;
        REQUIRE(rows.size() == row_at + kind->row_bytes(second_ways) + way_files::WAYS_FOOTER_BYTES);
        const way_files::Row second = way_files::decode_row(*kind, reinterpret_cast<const uint8_t*>(rows.data()) + row_at, second_ways);
        const std::string hash_of[2] = {std::string("Old ") + kind->ways_file + " is not the one the patch was made from",
                                        std::string("Old ") + kind->nodes_file + " is not the one the patch was made from"};
        // What the files' checks say, and what the replay says of a patch
        // that does not fit the old files.
        const std::string checks = "Malformed way files: ";
        const std::vector<std::string> misfits = {"decodes to ", "ends before its records do", "has bytes past its last record",
                                                  "does not match the new build", "Malformed merge sequence: "};
        for (int f = 0; f < 2; f++) {
            const std::string name = f == 0 ? kind->ways_file : kind->nodes_file, path = c.dir("old") + "/" + name;
            const std::vector<char> file = read_file(path);
            // How the patcher refuses the old file with the bit at % 8 of
            // byte `at` flipped: by the hash, by the files' checks, as a
            // misfit, or "" when it does not refuse it by any of those.
            auto refused_by = [&](size_t at) -> std::string {
                std::vector<char> flipped = file;
                flipped[at] = static_cast<char>(flipped[at] ^ (1 << (at % 8)));
                if (!write_file(path, flipped)) return "";
                const std::string out = c.dir("out.flipped");
                std::filesystem::remove_all(out);
                const ToolRun r = run_tool(patch_words(c, "sync", c.patch(), out), c.log());
                if (r.ok) return "";
                if (logged(r, hash_of[f])) return "hash";
                if (logged(r, checks)) return "checks";
                for (const std::string& misfit : misfits)
                    if (logged(r, misfit)) return "misfit";
                return "";
            };
            // The bytes only the hash holds: the copied group's anchor (its
            // lat and its lng) and its first way's name, which that way
            // keeps; the first and the last byte of its blocks.
            const std::vector<size_t> by_hash_alone =
                f == 0 ? std::vector<size_t>{row_at + 4, row_at + 9,
                                             row_at + way_files::ROW_HEAD_BYTES + second_ways * (kind->row_way_bytes - 4)}
                       : std::vector<size_t>{second.off, second.off + static_cast<size_t>(second.blocks_bytes()) - 1};
            for (const size_t at : by_hash_alone) CHECK_EQ(refused_by(at), std::string("hash"));
            std::map<std::string, size_t> refused;
            for (size_t at = 0; at < file.size(); at += step) {
                const std::string by = refused_by(at);
                if (by.empty()) std::printf("    %s: byte %zu flipped is not refused by name\n", name.c_str(), at);
                refused[by]++;
            }
            REQUIRE(write_file(path, file));
            CHECK_EQ(refused[""], size_t{0});
            CHECK(refused["hash"] > 0);
            CHECK(refused["checks"] > 0);
            std::printf("    %s: %zu bytes flipped in turn: %zu refused by the hash, %zu by the files' checks, %zu as a patch that does not fit\n",
                        name.c_str(), (file.size() + step - 1) / step, refused["hash"], refused["checks"], refused["misfit"]);
        }
        CHECK(apply_modes(c, ALL_MODES).bad.empty());
    }
}

// The nodes of a pair sent whole, as a full replacement, are the patch's and
// not the old pair's, whatever the old group at hand holds: 40 ways of 3 to 9
// nodes, the second node of each moved, so that every group has the old
// group's node counts and none its nodes. The diff's patch, its nodes'
// section put again by hand as the new nodes whole, gives the new pair in
// every mode, every group encoded and no old node read: under the ways'
// section sent as unchanged, and under their merge when a way is renamed too.
TEST(patch_way_replay_takes_the_nodes_a_patch_sends_whole) {
    ScratchDir dir("lowmem-way-whole");
    for (const way_files::Kind* kind : BOTH_WAY_KINDS) {
        std::mt19937 rng(20261127);
        const std::vector<Way> old_day = ways_with_nodes(rng, kind->interp, 40);
        std::vector<Way> moved = old_day;
        for (Way& w : moved) w.nodes[2] ^= 0x10;
        std::vector<Way> renamed = moved;
        renamed[30].name += 8;
        for (const auto& [name, new_day] : {std::pair<std::string, std::vector<Way>>{"nodes-alone", moved}, {"renamed-too", renamed}}) {
            BuildDir old_d, new_d;
            put_ways(old_d, *kind, old_day);
            put_ways(new_d, *kind, new_day);
            const LowMemCase c{dir.path(), std::string(kind->interp ? "interps-" : "streets-") + name};
            REQUIRE(prepare(c, old_d, new_d).empty());
            const std::vector<char> raw = raw_patch(c);
            const std::vector<char>& nodes = new_d.at(kind->nodes_file);
            const size_t at = find_once(raw, section_head(nodes_id(*kind), 8, nodes.size(), nodes.size()));
            REQUIRE(at != std::string::npos);
            std::vector<char> whole(raw.begin(), raw.begin() + static_cast<std::ptrdiff_t>(at));
            const std::vector<char> sent = replacement_of(nodes_id(*kind), nodes.size(), nodes);
            whole.insert(whole.end(), sent.begin(), sent.end());
            whole.insert(whole.end(), raw.begin() + static_cast<std::ptrdiff_t>(merge_section_end(raw, at)), raw.end());
            REQUIRE(write_patch(c, c.patch(), whole));

            CHECK(apply_modes(c, ALL_MODES, way_replay_did(*kind, {0, 3, 0, 0, 0})).bad.empty());
            for (const std::string& mode : ALL_MODES) CHECK(wrote_the_builders_pair(c.dir("out." + mode), *kind, new_d));
        }
    }
}

// Old way files whose rows do not give the nodes file its footer counts are
// refused by name in every mode, though each row's blocks start where the
// blocks before it end and the footers give the sizes the patch expects. 40
// street ways, the last of three nodes at one place, whose block is its
// first node alone whatever its count:
//   that way's count 4 in its row, on a day that drops the way: the ways
//     have a node more than the footer counts;
//   its code a byte less for its first node, on that day: its block ends two
//     bytes before the footer;
//   its count 2, on a day that renames the way: the patch keeps three nodes
//     of it, and the replay's cursor, looking for the third, finds the ways
//     a node short.
// The patches apply to the files as written.
TEST(patch_refuses_old_way_files_whose_rows_do_not_give_their_nodes) {
    ScratchDir dir("patch-old-way-rows");
    std::mt19937 rng(20261128);
    std::vector<Way> day = ways_with_nodes(rng, false, 40);
    const uint32_t lat = seven_decimals(471234567), lng = seven_decimals(81234567);
    day[39].nodes = {lat, lng, lat, lng, lat, lng};
    std::vector<Way> dropped(day.begin(), day.begin() + 39), renamed = day;
    renamed[39].name += 8;
    const uint64_t nodes = nodes_in(day, 0, 40);
    // The last way's count and code, the eighth pair of the third row's.
    const size_t count_at = 2 * way_files::STREETS.row_bytes(way_files::GROUP_WAYS) + way_files::ROW_HEAD_BYTES + 7 * 4;
    struct Edit {
        std::string name;
        const std::vector<Way>& new_day;
        size_t at;
        int by;
        std::string refusal;
    };
    const Edit edits[] = {
        {"a-node-more", dropped, count_at, 1,
         "street_nodes.bin counts " + std::to_string(nodes) + " nodes and its ways have " + std::to_string(nodes + 1)},
        {"a-shorter-block", dropped, count_at + 2, -0x2000, "street_nodes.bin has bytes between its last block and its footer"},
        {"a-node-fewer", renamed, count_at, -1,
         "street_nodes.bin counts " + std::to_string(nodes) + " nodes and its ways have " + std::to_string(nodes - 1)},
    };
    for (const Edit& e : edits) {
        BuildDir old_d, new_d;
        put_ways(old_d, way_files::STREETS, day);
        put_ways(new_d, way_files::STREETS, e.new_day);
        const LowMemCase c{dir.path(), e.name};
        REQUIRE(prepare(c, old_d, new_d).empty());
        const std::string path = c.dir("old") + "/street_ways.bin";
        const std::vector<char> rows = read_file(path);
        std::vector<char> edited = rows;
        uint16_t field;
        memcpy(&field, rows.data() + e.at, 2);
        // The way is not its group's first with nodes: its first node takes bytes.
        REQUIRE((e.at == count_at ? field == 3 : way_files::first_bytes(field) > 0));
        put_at(edited, e.at, static_cast<uint16_t>(field + e.by));
        REQUIRE(write_file(path, edited));

        CHECK(not_refused(c, ALL_MODES, c.patch(), e.name, "Malformed way files: " + e.refusal).empty());

        REQUIRE(write_file(path, rows));
        CHECK(apply_modes(c, ALL_MODES).bad.empty());
    }
}

// An old pair whose footers give its nodes another size than the patch is
// refused by name in every mode before a way is replayed: 40 ways, and in
// the old dir the pair of a day whose last way has a node more (the records'
// bytes are as many), under the records' merge of a day that renames a way
// and under their section sent as unchanged on a day that moves a node. The
// patches apply to the pair they were made from.
TEST(patch_refuses_to_replay_an_old_way_pair_whose_nodes_are_of_another_size) {
    ScratchDir dir("patch-old-way-sizes");
    for (const way_files::Kind* kind : BOTH_WAY_KINDS) {
        std::mt19937 rng(20261129);
        const std::vector<Way> day = ways_with_nodes(rng, kind->interp, 40);
        std::vector<Way> renamed = day, moved = day, longer = day;
        renamed[30].name += 8;
        moved[5].nodes[2] ^= 0x10;
        longer[39].nodes.insert(longer[39].nodes.end(), {longer[39].nodes[0], longer[39].nodes[1]});
        BuildDir old_d, other;
        put_ways(old_d, *kind, day);
        put_ways(other, *kind, longer);
        const BuildDir stale = gctest::as_written(other), written = gctest::as_written(old_d);
        const uint64_t nodes_bytes = old_d.at(kind->nodes_file).size();
        for (const auto& [name, new_day] : {std::pair<std::string, std::vector<Way>>{"renamed", renamed}, {"moved", moved}}) {
            BuildDir new_d;
            put_ways(new_d, *kind, new_day);
            const LowMemCase c{dir.path(), std::string(kind->interp ? "interps-" : "streets-") + name};
            REQUIRE(prepare(c, old_d, new_d).empty());
            for (const char* file : {kind->ways_file, kind->nodes_file}) REQUIRE(write_file(c.dir("old") + "/" + file, stale.at(file)));

            CHECK(not_refused(c, ALL_MODES, c.patch(), name,
                              std::string("old ") + kind->nodes_file + " decodes to " + std::to_string(nodes_bytes + 8) +
                                  " bytes, the patch expects " + std::to_string(nodes_bytes))
                      .empty());
            for (const std::string& mode : ALL_MODES) CHECK(!std::filesystem::exists(c.dir(name + ".out." + mode) + "/" + kind->ways_file));

            for (const char* file : {kind->ways_file, kind->nodes_file}) REQUIRE(write_file(c.dir("old") + "/" + file, written.at(file)));
            CHECK(apply_modes(c, ALL_MODES).bad.empty());
        }
    }
}

// Merge sequences the way replay cannot apply are refused by name in every
// mode. 40 street ways of 3 to 9 nodes on a day that changes nothing, the
// pair's two sections put again by hand:
//   the ways: a DELETE and a REPLACE run past the old pair's last way, an op
//     no merge has, an INSERT run longer than its records, a way more and a
//     way fewer than the section's new size, and a way whose node_offset is
//     not the nodes before it;
//   the ways' fixups: an offset fixup value and a string fixup for a way
//     past the old pair's last, which the replay leaves over;
//   the nodes: a MATCH run that ends past the old pair's last node, a
//     sequence that ends before the ways' nodes do, one with a node after
//     them, and a new size the sequence does not give;
//   the two sections a second time.
// The sections as one MATCH run each, and the patch as made, apply.
TEST(patch_refuses_merge_sequences_the_way_replay_cannot_apply) {
    ScratchDir dir("lowmem-way-refusals");
    std::mt19937 rng(20261125);
    const std::vector<Way> day = ways_with_nodes(rng, false, 40);
    BuildDir d;
    put_ways(d, way_files::STREETS, day);
    const WaySections s(dir.path(), "streets", d, way_files::STREETS);
    REQUIRE(s.found());
    const std::vector<char>& records = d.at("street_ways.bin");
    const std::vector<char>& nodes = d.at("street_nodes.bin");
    const uint32_t n_nodes = static_cast<uint32_t>(nodes.size() / 8);
    std::vector<char> every_way, every_node;
    put_op(every_way, OP_MATCH_RUN, 40);
    put_op(every_node, OP_MATCH_RUN, n_nodes);
    auto ways_then = [&](uint8_t op, uint32_t count, const char* data = nullptr) {
        std::vector<char> ops = every_way;
        put_op(ops, op, count, data, WAY_HEADER_STRIDE);
        return ops;
    };
    std::vector<char> cut_insert = ways_then(OP_INSERT_RUN, 1, records.data());
    put_at(cut_insert, cut_insert.size() - WAY_HEADER_STRIDE - 4, 2u);
    std::vector<char> one_fewer, misplaced, late_nodes, early_end, node_more = every_node;
    put_op(one_fewer, OP_MATCH_RUN, 39);
    put_op(one_fewer, OP_DELETE_RUN, 1);
    std::vector<char> last(records.end() - WAY_HEADER_STRIDE, records.end());
    put_at(last, 0, 7u);
    put_op(misplaced, OP_MATCH_RUN, 39);
    put_op(misplaced, OP_REPLACE_RUN, 1, last.data(), WAY_HEADER_STRIDE);
    put_op(late_nodes, OP_DELETE_RUN, 1);
    put_op(late_nodes, OP_MATCH_RUN, n_nodes);
    put_op(early_end, OP_MATCH_RUN, n_nodes - 1);
    put_op(node_more, OP_INSERT_RUN, 1, nodes.data(), 8);
    std::vector<char> grown = s.merges(every_way, every_node);
    put_at(grown, grown.size() - (56 + every_node.size()) + 16, uint64_t{nodes.size() + 8});
    std::vector<char> twice = s.merges(every_way, every_node);
    const std::vector<char> duplicate = twice;
    twice.insert(twice.end(), duplicate.begin(), duplicate.end());

    struct Edit {
        std::string name;
        std::vector<char> sections;
        std::string refusal;
    };
    const Edit edits[] = {
        {"delete-past", s.merges(ways_then(OP_DELETE_RUN, 1), every_node),
         "Malformed merge sequence: street_ways.bin (a DELETE run past the old file)"},
        {"replace-past", s.merges(ways_then(OP_REPLACE_RUN, 1, records.data()), every_node),
         "Malformed merge sequence: street_ways.bin (a REPLACE run past the old file)"},
        {"no-such-op", s.merges(ways_then(9, 1), every_node), "Malformed merge sequence: street_ways.bin"},
        {"cut-insert", s.merges(cut_insert, every_node), "Malformed merge sequence: street_ways.bin"},
        {"a-way-more", s.merges(ways_then(OP_INSERT_RUN, 1, records.data()), every_node), "Rebuilt street_ways.bin does not match the new build"},
        {"a-way-fewer", s.merges(one_fewer, every_node), "Rebuilt street_ways.bin does not match the new build"},
        {"node-offset", s.merges(misplaced, every_node), "Malformed way files: street_ways.bin way 39 has a node_offset of 7, not the "},
        // Varints: way 40 from way 0, an offset of 0; slot 40 from slot 0, no change of the value.
        {"offset-fixup-left", s.merges(every_way, every_node, {1, {40, 0}}), "Malformed offset fixups"},
        {"string-fixup-left", s.merges(every_way, every_node, {}, {1, {40, 0}}), "Malformed string fixups"},
        {"match-past", s.merges(every_way, late_nodes), "Malformed merge sequence: street_nodes.bin"},
        {"nodes-end-early", s.merges(every_way, early_end), "street_nodes.bin ends before its records do"},
        {"a-node-more", s.merges(every_way, node_more), "street_nodes.bin has bytes past its last record"},
        {"nodes-grown", grown, "Rebuilt street_nodes.bin does not match the new build"},
        {"twice", twice, "old street_ways.bin opened twice"},
    };
    for (const Edit& e : edits) {
        const std::string patch = s.c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(s.c, patch, s.with(e.sections)));

        CHECK(not_refused(s.c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    REQUIRE(write_patch(s.c, s.c.patch(), s.with(s.merges(every_way, every_node))));
    CHECK(apply_modes(s.c, ALL_MODES, way_replay_did(way_files::STREETS, {3, 0, 0, 0, 0})).bad.empty());
    REQUIRE(write_patch(s.c, s.c.patch(), s.raw));
    CHECK(apply_modes(s.c, ALL_MODES).bad.empty());
}

// What the files after a way pair need of its replay, on a day that moves
// street ways: 40 street ways by entity, the first 16 swapped in pairs and
// every way after the fortieth renamed, named from a street tier whose
// strings the new day reverses; a parent and a postcode a way; 30
// interpolations, one gone and one with a node moved, with a postcode each;
// 100 address points on the first entity's way; and a geo index whose cells
// list ways, address points and interpolations. The ways' merge sequences are
// where the side files' deltas walk them, their MATCH and REPLACE runs and
// the moved ways' pairs make the remaps the address points' parent ways and
// the geo lists move by, and their MATCH records' names move by the string
// remap or, with every moved string sent past the pairs, by the section's
// string fixups, their node_offsets by its offset fixups. In every mode the
// patched dir is the new dir.
TEST(the_way_replay_records_the_sequences_and_runs_the_side_files_and_geo_lists_need) {
    ScratchDir dir("lowmem-way-duties");
    constexpr uint32_t STREETS = 56, SWAPPED = 16, NAMES = 20, NAME = 12, INTERPS = 30;  // "way 0000000\0"
    std::string old_pool, new_pool;
    for (uint32_t i = 0; i < NAMES; i++) {
        char b[16];
        std::snprintf(b, sizeof b, "way %07u", i);
        old_pool += std::string(b, NAME);
        std::snprintf(b, sizeof b, "way %07u", NAMES - 1 - i);
        new_pool += std::string(b, NAME);
    }
    // The dir of a day: its street ways' entities in record order.
    auto day = [&](const std::vector<uint32_t>& entities, bool is_new) {
        std::mt19937 rng(20261126);
        BuildDir d;
        std::vector<Way> streets(STREETS), interps;
        std::vector<char> slots, parents, way_postcodes, interp_postcodes;
        // An entity's name in the day's pool; the renamed ones take the next.
        auto name_of = [&](uint32_t name) { return (is_new ? NAMES - 1 - name : name) * NAME; };
        std::vector<uint32_t> record_of(STREETS);
        for (uint32_t w = 0; w < STREETS; w++) {
            const uint32_t e = entities[w];
            record_of[e] = w;
            std::mt19937 own(1000 + e);
            streets[w].name = name_of((e + (is_new && e >= 40)) % NAMES);
            streets[w].nodes = way_nodes(own, 2 + e % 5, e % 16 == 5 ? 1 : 0);
            append(slots, e);
            append(parents, e % 9);
            append(way_postcodes, e % 3 ? name_of(e % 7) : NO);
        }
        for (uint32_t i = 0; i < INTERPS; i++) {
            if (is_new && i == 12) continue;
            std::mt19937 own(5000 + i);
            Way w = way_fields(own, true);
            w.name = name_of(i % NAMES);
            w.nodes = way_nodes(own, 3, 0);
            if (is_new && i == 20) w.nodes[2] ^= 0x10;
            interps.push_back(w);
            append(interp_postcodes, name_of(i % 5));
        }
        put_ways(d, way_files::STREETS, streets);
        put_ways(d, way_files::INTERPS, interps);
        d["way_parents.bin"] = parents;
        d["way_postcodes.bin"] = way_postcodes;
        d["interp_postcodes.bin"] = interp_postcodes;
        std::vector<Addr> addrs;
        for (uint32_t i = 0; i < 100; i++) addrs.push_back({47.0f + static_cast<float>(i) * 1e-4f, {}});
        put_addrs(d, addrs);
        std::vector<char>& points = d["addr_points.bin"];
        for (size_t at = ADDR_POINT_PARENT_WAY_ID_OFF; at < points.size(); at += ADDR_POINT_STRIDE) put_at(points, at, record_of[0]);
        // Twenty cells: each lists two street entities' ways, five address
        // points and its own interpolation, while the day has it.
        std::vector<char> top, blocks;
        geo_index::Writer writer([&](const char* p, size_t n) { top.insert(top.end(), p, p + n); },
                                 [&](const char* p, size_t n) { blocks.insert(blocks.end(), p, p + n); });
        for (uint32_t k = 0; k < 20; k++) {
            geo_index::RowLists lists;
            lists[0] = {record_of[2 * k], record_of[2 * k + 1]};
            std::sort(lists[0].begin(), lists[0].end());
            for (uint32_t a = 0; a < 5; a++) lists[1].push_back(5 * k + a);
            if (!(is_new && k == 12)) lists[2] = {is_new && k > 12 ? k - 1 : k};
            writer.add(cell_id(k), geo_index::ids_of(lists));
        }
        writer.finish();
        d["geo_cells.bin"] = top;
        d["geo_entries.bin"] = blocks;
        const std::string& pool = is_new ? new_pool : old_pool;
        d["strings_street.bin"].assign(pool.begin(), pool.end());
        BuildDir meta;
        meta["street.slot.u32"] = slots;
        return std::pair<BuildDir, BuildDir>{d, meta};
    };
    std::vector<uint32_t> entities, new_entities;
    for (uint32_t w = 0; w < STREETS; w++) {
        entities.push_back(w);
        new_entities.push_back(w < SWAPPED ? w ^ 1 : w);
    }
    const auto [old_d, old_meta] = day(entities, false);
    const auto [new_d, new_meta] = day(new_entities, true);
    for (const bool pairs : {true, false}) {
        const LowMemCase c{dir.path(), pairs ? "string-pairs" : "string-fixups"};
        REQUIRE(prepare(c, old_d, new_d, old_meta, new_meta, pairs ? std::vector<std::string>{} : NO_STRING_PAIRS).empty());
        const std::vector<char> raw = raw_patch(c);
        // The moved ways' pairs, in the parent-id remap's street leg.
        std::vector<char> leg;
        for (const uint32_t w : {POI_PARENT_REMAP_MARKER, SWAPPED / 2}) append(leg, w);
        REQUIRE(find_once(raw, leg) != std::string::npos);
        // The street ways' section: its offset fixups, and its string fixups
        // when no pair is sent.
        const size_t ways_at = find_once(raw, section_head(PatchFileId::STREET_WAYS, WAY_HEADER_STRIDE, old_d.at("street_ways.bin").size(),
                                                           new_d.at("street_ways.bin").size()));
        REQUIRE(ways_at != std::string::npos);
        uint32_t offset_fixups[2] = {0, 0}, fixup_bytes[2] = {0, 0}, string_fixups = 0;
        memcpy(offset_fixups, raw.data() + ways_at + 24, 8);
        memcpy(fixup_bytes, raw.data() + ways_at + 32, 8);
        memcpy(&string_fixups, raw.data() + ways_at + 40 + fixup_bytes[0] + fixup_bytes[1], 4);
        REQUIRE(offset_fixups[0] + offset_fixups[1] > 0);
        REQUIRE((string_fixups > 0) == !pairs);

        const LogCheck ran = [](const std::string& log) -> std::string {
            for (const way_files::Kind* kind : BOTH_WAY_KINDS)
                if (!way_replay_of(log, *kind)) return std::string("no way replay of ") + kind->ways_file;
            for (const char* line : {"way_parents.bin: parallel delta along street_ways.bin",
                                     "way_postcodes.bin: parallel delta along street_ways.bin",
                                     "interp_postcodes.bin: parallel delta along interp_ways.bin"})
                if (log.find(line) == std::string::npos) return std::string("no \"") + line + "\"";
            return "";
        };
        CHECK(apply_modes(c, ALL_MODES, ran).bad.empty());
    }
}

// --- The address replay: the address files replayed as they sit ---

namespace {

// An address by record: its point, its ids and its footprint's logical blob
// (none: a point).
struct Address {
    float lat, lng;
    uint32_t housenumber, street, parent;
    std::vector<char> blob;
};

// The record the builder fills a slot without an address with.
Address dead_slot() { return {0.0f, 0.0f, 0, 0, NO, {}}; }

// The vertices of a footprint's logical blob: 8 bytes each in encoding 3,
// else 4.
uint32_t vertices_of(const std::vector<char>& blob) {
    return static_cast<uint32_t>((blob.size() - addr_files::LOGICAL_HEADER) / addr_files::vertex_bytes(static_cast<uint8_t>(blob[0])));
}

// addr_points.bin and addr_vertices.bin of addresses, into d.
void put_addresses(BuildDir& d, const std::vector<Address>& addresses) {
    std::vector<char> points, verts;
    for (const Address& a : addresses) {
        append(points, a.lat);
        append(points, a.lng);
        append(points, a.housenumber);
        append(points, a.street);
        append(points, a.parent);
        append(points, a.blob.empty() ? NO : addr_files::vertex_offset_of(verts.size()));
        append(points, a.blob.empty() ? 0u : vertices_of(a.blob));
        verts.insert(verts.end(), a.blob.begin(), a.blob.end());
    }
    d["addr_points.bin"] = points;
    d["addr_vertices.bin"] = verts;
}

// The logical blob of a footprint of n vertices in encoding enc (0 to 2)
// under a record at (lat, lng): steps below span (closed: the last vertex the
// first) from a bbox minimum `off` order keys past where the point predicts
// each axis's (addr_files.h). Within 127 either way its blob holds the two
// residuals, a byte each, and so is that point's alone: the same footprint
// under a point one step away has another blob.
std::vector<char> footprint_at(std::mt19937& rng, float lat, float lng, uint32_t n, uint8_t enc = 0, int off = 3,
                               uint32_t span = 4000, bool closed = false) {
    std::vector<char> b(addr_files::LOGICAL_HEADER + size_t{4} * n, 0);
    b[0] = static_cast<char>(enc);
    uint64_t sum_lat = 0, sum_lng = 0;
    for (uint32_t k = 0; k < n; k++) {
        uint16_t q_lat = static_cast<uint16_t>(rng() % span), q_lng = static_cast<uint16_t>(rng() % span);
        if (closed && k > 0 && k + 1 == n) {
            memcpy(&q_lat, b.data() + addr_files::LOGICAL_HEADER, 2);
            memcpy(&q_lng, b.data() + addr_files::LOGICAL_HEADER + 2, 2);
        }
        put_at(b, addr_files::LOGICAL_HEADER + size_t{4} * k, q_lat);
        put_at(b, addr_files::LOGICAL_HEADER + size_t{4} * k + 2, q_lng);
        sum_lat += q_lat;
        sum_lng += q_lng;
    }
    const addr_files::Prediction p = addr_files::predict(coord_keys::bits_of(lat), coord_keys::bits_of(lng), sum_lat, sum_lng, n, enc);
    put_at(b, 2, coord_keys::from_order_key(coord_keys::order_key(p.lat) + static_cast<uint32_t>(off)));
    put_at(b, 6, coord_keys::from_order_key(coord_keys::order_key(p.lng) + static_cast<uint32_t>(off)));
    return b;
}

// The logical blob of an escaped footprint: n vertices of 8 random bytes in
// encoding 3.
std::vector<char> escape_of(std::mt19937& rng, uint32_t n) {
    std::vector<char> b = block(rng, 2 * n);
    b[0] = 3;
    return b;
}

// Where each address's blob starts in the addr_vertices.bin the builder
// writes, and its bytes there (none for a point).
struct BlobAt {
    uint64_t at;
    uint32_t len;
};
std::vector<BlobAt> blobs_at(const std::vector<Address>& addresses) {
    std::vector<BlobAt> at;
    uint64_t pos = 0;
    std::vector<uint8_t> blob;
    for (const Address& a : addresses) {
        blob.clear();
        if (!a.blob.empty())
            addr_files::encode_blob(reinterpret_cast<const uint8_t*>(a.blob.data()), vertices_of(a.blob), coord_keys::bits_of(a.lat),
                                    coord_keys::bits_of(a.lng), blob);
        at.push_back({pos, static_cast<uint32_t>(blob.size())});
        pos += blob.size();
    }
    return at;
}

// How many of the addresses have a footprint.
uint64_t footprints_in(const std::vector<Address>& addresses) {
    uint64_t n = 0;
    for (const Address& a : addresses) n += !a.blob.empty();
    return n;
}

// Whether an address keeps its point and its footprint, byte for byte: its
// blob in the old files is the builder's for it in the new ones.
bool keeps_its_blob(const Address& was, const Address& is) {
    return !is.blob.empty() && is.blob == was.blob && coord_keys::bits_of(is.lat) == coord_keys::bits_of(was.lat) &&
           coord_keys::bits_of(is.lng) == coord_keys::bits_of(was.lng);
}

// What the address replay says of the new footprints in the patcher's log:
// the blobs it copied from the old file as bytes and those it encoded, and
// the old footprint bytes the encoded ones kept, by where it read them: the
// blob of the old record it was at, one it had passed (read again), or the
// cursor that decodes the old files a second time. found: the log has the
// line.
struct AddrReplayed {
    bool found = false;
    uint64_t copied = 0, encoded = 0, at_hand = 0, read_again = 0, by_cursor = 0;
    std::string text() const {
        return std::to_string(copied) + " copied, " + std::to_string(encoded) + " encoded; old footprint bytes " + std::to_string(at_hand) +
               " at hand, " + std::to_string(read_again) + " read again, " + std::to_string(by_cursor) + " by the cursor";
    }
};

AddrReplayed addr_replayed(const std::string& log) {
    AddrReplayed n;
    const size_t at = log.find("replayed with addr_points.bin: blobs ");
    if (at == std::string::npos) return n;
    n.found = std::sscanf(log.c_str() + at,
                          "replayed with addr_points.bin: blobs %" SCNu64 " copied, %" SCNu64 " encoded; old footprint bytes %" SCNu64
                          " at hand, %" SCNu64 " read again, %" SCNu64 " by the cursor",
                          &n.copied, &n.encoded, &n.at_hand, &n.read_again, &n.by_cursor) == 5;
    return n;
}

// A check that the address replay ran and copied and encoded exactly these
// many blobs, with these many old footprint bytes from each place.
LogCheck addr_replayed_is(uint64_t copied, uint64_t encoded, uint64_t at_hand = 0, uint64_t read_again = 0, uint64_t by_cursor = 0) {
    return [=](const std::string& log) -> std::string {
        const AddrReplayed n = addr_replayed(log);
        if (!n.found) return "no address replay in the log";
        AddrReplayed want;
        want.copied = copied;
        want.encoded = encoded;
        want.at_hand = at_hand;
        want.read_again = read_again;
        want.by_cursor = by_cursor;
        return n.text() == want.text() ? "" : "the address replay gave " + n.text() + ", not " + want.text();
    };
}

// The hash section of an old file as the diff writes it for c's old dir.
std::vector<char> old_hash_section(const LowMemCase& c, PatchFileId file) {
    const std::vector<char> bytes = read_file(c.dir("old") + "/" + patch_file_names[static_cast<uint32_t>(file)]);
    return old_file_hash(file, lane_hash(bytes.data(), bytes.size()));
}

const char* const ADDRESS_FILES[] = {"addr_points.bin", "addr_vertices.bin", "addr_index.bin"};
const std::string NOT_THE_PATCHS_POINTS = "Old addr_points.bin is not the one the patch was made from";
const std::string NOT_THE_PATCHS_FOOTPRINTS = "Old addr_vertices.bin is not the one the patch was made from";

// The old dir's addr_vertices.bin with the byte at `at` flipped is refused in
// every mode by its hash; the file is then as it was. Every mode that did not
// refuse it so.
std::vector<std::string> flipped_blob_byte_not_refused(const LowMemCase& c, const std::string& name, uint64_t at) {
    const std::string path = c.dir("old") + "/addr_vertices.bin";
    const std::vector<char> file = read_file(path);
    if (at >= file.size()) return {name + ": no byte " + std::to_string(at)};
    std::vector<char> flipped = file;
    flipped[at] ^= 0x10;
    if (!write_file(path, flipped)) return {name + ": not written"};
    const std::vector<std::string> bad = not_refused(c, ALL_MODES, c.patch(), name, NOT_THE_PATCHS_FOOTPRINTS);
    if (!write_file(path, file)) return {name + ": not restored"};
    return bad;
}

}  // namespace

// Random days of addresses as strategy 2 leaves them: an old day of 700 slots
// (points, footprints in every encoding, closed and open, at every width,
// their bbox minimum a residual's byte from where the point predicts it or
// further off, an escape by its vertices and the largest footprint a blob
// holds, dead slots) on named streets, and a new day that changes ids, moves
// points a step, edits, replaces, drops and adds footprints, kills addresses
// and appends 40, while the names move: in runs, as pairs, or as the string
// fixups of the records that keep them. By slot every address stays at its
// record; by content the new day drops and inserts records too. In every mode
// the patcher writes the builder's address files of the new day (write_dir
// writes them through addr_files::encode), byte for byte, and its log counts
// the blobs it copied: by slot those of the addresses that keep their point
// and their footprint, each one.
TEST(patch_rebuilds_the_builders_address_files_on_random_days) {
    ScratchDir dir("lowmem-addr-days");
    constexpr uint32_t NAME = 12, NAMES = 60, N = 700;  // "str 0000000\0"
    std::string names;
    for (uint32_t i = 0; i < NAMES; i++) {
        char b[16];
        std::snprintf(b, sizeof b, "str %07u", i);
        names += std::string(b, NAME);
    }
    struct Slot {
        Address a;  // its housenumber and street the names' numbers
        bool dead;
    };
    for (uint32_t seed = 1; seed <= 4; seed++) {
        std::mt19937 rng(20261116 + seed);
        const bool by_slot = seed % 2 == 0, reversed = seed > 2;
        // The new day's names: without every fifth, so that the others move in
        // runs; or in reverse order, so that each moves on its own.
        std::string new_names;
        for (uint32_t i = 0; i < NAMES; i++) {
            const uint32_t k = reversed ? NAMES - 1 - i : i;
            if (reversed || k % 5 != 4) new_names += names.substr(size_t{k} * NAME, NAME);
        }
        const std::function<uint32_t(uint32_t)> old_name = [&](uint32_t i) { return i * NAME; };
        const std::function<uint32_t(uint32_t)> new_name = [&](uint32_t i) {
            return reversed ? (NAMES - 1 - i) * NAME : (i - (i + 1) / 5) * NAME;
        };
        auto name = [&] {
            uint32_t i = rng() % NAMES;
            while (i % 5 == 4) i = rng() % NAMES;
            return i;
        };
        auto footprint = [&](float lat, float lng) {
            const uint32_t r = rng() % 12;
            if (r == 0) return escape_of(rng, 1 + rng() % 30);
            const int off = r < 3 ? static_cast<int>(rng() % 2000) - 1000 : static_cast<int>(rng() % 255) - 127;
            return footprint_at(rng, lat, lng, 1 + rng() % 40, static_cast<uint8_t>(rng() % 3), off, uint32_t{1} << (1 + rng() % 16),
                                rng() % 2 == 0);
        };
        auto slot = [&] {
            Slot s{{static_cast<float>(47.0 + (rng() % 200000) * 1e-5), static_cast<float>(-8.0 + (rng() % 200000) * 1e-5), name(), name(),
                    static_cast<uint32_t>(rng() % 500), {}},
                   false};
            if (rng() % 3) s.a.blob = footprint(s.a.lat, s.a.lng);
            return s;
        };
        const Slot dead{dead_slot(), true};
        std::vector<Slot> old_day;
        for (uint32_t i = 0; i < N; i++) old_day.push_back(i % 37 == 5 ? dead : slot());
        old_day[123].a.blob = block(rng, MAX_BLOB_VERTICES + 1);
        old_day[321].a.blob = footprint_at(rng, old_day[321].a.lat, old_day[321].a.lng, MAX_BLOB_VERTICES, 1, 9, 50000);
        // Each new record and the old one it was, when it keeps its record's
        // point and footprint.
        std::vector<Slot> new_day;
        uint64_t kept = 0, untouched = 0;
        for (uint32_t i = 0; i < N; i++) {
            Slot s = old_day[i];
            const uint32_t r = s.dead || i == 123 || i == 321 ? 99 : rng() % 30;
            if (r == 0) s.a.housenumber = name();
            if (r == 1) s.a.street = name();
            if (r == 2) s.a.parent = static_cast<uint32_t>(rng() % 500);
            if (r == 3) s.a.lat = std::nextafter(s.a.lat, 100.0f);          // a step away, its footprint's bytes kept
            if (r == 4 && !s.a.blob.empty()) s.a.blob.back() ^= 1;         // a vertex moved
            if (r == 5) s.a.blob = footprint(s.a.lat, s.a.lng);            // replaced, or a point given one
            if (r == 6) s.a.blob.clear();                                  // dropped
            if (r == 7) s = dead;                                          // killed in place
            if (r == 8 && !by_slot) continue;                              // gone
            if (r == 9 && !by_slot) new_day.push_back(slot());             // one before it
            if (!s.dead && keeps_its_blob(old_day[i].a, s.a)) {
                kept++;
                untouched += r > 9;
            }
            new_day.push_back(s);
        }
        for (uint32_t i = 0; i < 40; i++) new_day.push_back(slot());
        auto dir_of = [&](const std::vector<Slot>& day, const std::function<uint32_t(uint32_t)>& name_at, const std::string& pool,
                          uint64_t& footprints) {
            std::vector<Address> addresses;
            for (const Slot& s : day) {
                addresses.push_back(s.a);
                if (s.dead) continue;
                addresses.back().housenumber = name_at(s.a.housenumber);
                addresses.back().street = name_at(s.a.street);
            }
            BuildDir d;
            put_addresses(d, addresses);
            d[reversed ? "strings_poi.bin" : "strings_street.bin"].assign(pool.begin(), pool.end());
            footprints = footprints_in(addresses);
            return d;
        };
        uint64_t old_footprints = 0, footprints = 0;
        const BuildDir old_d = dir_of(old_day, old_name, names, old_footprints), new_d = dir_of(new_day, new_name, new_names, footprints);
        std::vector<std::string> options;
        if (by_slot) options.push_back("--identity-slots");
        if (seed == 4) options.insert(options.end(), NO_STRING_PAIRS.begin(), NO_STRING_PAIRS.end());
        const LowMemCase c{dir.path(), "seed-" + std::to_string(seed)};
        REQUIRE(prepare(c, old_d, new_d, {}, {}, options).empty());

        AddrReplayed n;
        const LogCheck counts = [&](const std::string& log) -> std::string {
            n = addr_replayed(log);
            if (!n.found) return "no address replay in the log";
            if (n.copied + n.encoded != footprints) return "not the new day's " + std::to_string(footprints) + " footprints: " + n.text();
            if (by_slot ? n.copied != kept : n.copied > kept || n.copied * 10 < untouched * 9)
                return "of " + std::to_string(kept) + " blobs to keep (" + std::to_string(untouched) + " of records untouched): " + n.text();
            return "";
        };
        CHECK(apply_modes(c, ALL_MODES, counts).bad.empty());
        const BuildDir want = gctest::as_written(new_d);
        for (const std::string& mode : ALL_MODES)
            for (const char* f : ADDRESS_FILES) CHECK(read_file(c.dir("out." + mode) + "/" + f) == want.at(f));
        std::printf("    seed %u, by %s: %" PRIu64 " old footprints, %" PRIu64 " to keep: %s (the last mode's)\n", seed,
                    by_slot ? "slot" : "content", old_footprints, kept, n.text().c_str());
    }
}

// A day that leaves most footprints alone, by slot: addresses with another
// house number (the footprint kept), moved a step of latitude or of longitude
// with their footprint's bytes kept, a footprint's last vertex moved or one
// added after it (the old footprint's bytes all kept), a footprint replaced,
// dropped or given to a point, addresses killed in place and appended; an
// escape kept and one under another house number, and the largest footprint
// a blob holds kept. In every mode the patcher writes the builder's address
// files of the new day, copying the blob of each address that keeps its point
// and its footprint (the log counts them) and encoding every other, its old
// bytes from the old blob the replay is at. A byte flipped in a copied blob
// of the old dir (a small one, each escape, the largest) goes through
// undecoded, and one in the blob of a killed address is never read for a
// footprint: the old addr_vertices.bin's hash refuses each.
TEST(patch_copies_the_address_blobs_a_day_leaves_whole) {
    ScratchDir dir("lowmem-addr-copy");
    constexpr uint32_t N = 600;
    std::mt19937 rng(20261117);
    std::vector<Address> old_day;
    for (uint32_t i = 0; i < N; i++) {
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f + static_cast<float>(i % 7) * 1e-4f, i, 0, i % 30, {}};
        if (i == 200) a.blob = escape_of(rng, 3000);
        else if (i == 300) a.blob = block(rng, MAX_BLOB_VERTICES + 1);
        else if (i == 400) a.blob = footprint_at(rng, a.lat, a.lng, MAX_BLOB_VERTICES, 0, 5, 60000);
        else if (i % 4) a.blob = footprint_at(rng, a.lat, a.lng, 8 + rng() % 30, static_cast<uint8_t>(i % 3), static_cast<int>(rng() % 200) - 100);
        old_day.push_back(a);
    }
    std::vector<Address> new_day = old_day;
    for (uint32_t i = 0; i < N; i++) {
        Address& a = new_day[i];
        const uint32_t r = i == 200 || i == 400 ? 99 : i == 300 ? 0 : i % 16;
        if (r == 0) a.housenumber += 1000000;
        if (r == 1) a.lat = std::nextafter(a.lat, 100.0f);
        if (r == 2) a.blob.back() ^= 1;
        if (r == 3) a.blob = footprint_at(rng, a.lat, a.lng, 8 + rng() % 30);
        if (r == 4) a.blob = footprint_at(rng, a.lat, a.lng, 8 + rng() % 30);
        if (r == 5) a.blob.clear();
        if (r == 6) a.blob.insert(a.blob.end(), {1, 2, 3, 4});
        if (r == 7) a = dead_slot();
        if (r == 9) a.lng = std::nextafter(a.lng, 100.0f);
    }
    for (uint32_t i = 0; i < 30; i++) {
        Address a{48.0f + static_cast<float>(i) * 1e-4f, 8.0f, 5000 + i, 0, i % 30, {}};
        if (i % 3) a.blob = footprint_at(rng, a.lat, a.lng, 8 + rng() % 30);
        new_day.push_back(a);
    }
    uint64_t kept = 0, moved_bytes = 0;
    for (uint32_t i = 0; i < N; i++) {
        kept += keeps_its_blob(old_day[i], new_day[i]);
        // Moved a step, or a vertex more: the old footprint's bytes are all kept.
        if (i % 16 == 1 || i % 16 == 6 || i % 16 == 9) moved_bytes += old_day[i].blob.size();
    }
    const uint64_t footprints = footprints_in(new_day);
    BuildDir old_d, new_d;
    put_addresses(old_d, old_day);
    put_addresses(new_d, new_day);
    const LowMemCase c{dir.path(), "copy"};
    REQUIRE(prepare(c, old_d, new_d, {}, {}, {"--identity-slots"}).empty());

    AddrReplayed n;
    const LogCheck counts = [&](const std::string& log) -> std::string {
        n = addr_replayed(log);
        if (!n.found) return "no address replay in the log";
        if (n.copied != kept || n.encoded != footprints - kept)
            return "not " + std::to_string(kept) + " blobs copied and " + std::to_string(footprints - kept) + " encoded: " + n.text();
        // An edited or replaced footprint keeps what the footprint merge matched
        // of its old bytes.
        if (n.at_hand < moved_bytes) return "fewer than the " + std::to_string(moved_bytes) + " old bytes kept whole at hand: " + n.text();
        if (n.read_again || n.by_cursor) return "old footprint bytes from a blob the replay was not at: " + n.text();
        return "";
    };
    CHECK(apply_modes(c, ALL_MODES, counts).bad.empty());
    const BuildDir want = gctest::as_written(new_d);
    for (const std::string& mode : ALL_MODES)
        for (const char* f : ADDRESS_FILES) CHECK(read_file(c.dir("out." + mode) + "/" + f) == want.at(f));
    std::printf("    %" PRIu64 " of %" PRIu64 " footprints kept at their address: %s (the last mode's)\n", kept, footprints, n.text().c_str());

    const std::vector<BlobAt> at = blobs_at(old_day);
    REQUIRE(keeps_its_blob(old_day[10], new_day[10]) && at[10].len > 20 && new_day[7].blob.empty() && at[7].len > 20);
    CHECK(flipped_blob_byte_not_refused(c, "small", at[10].at + at[10].len - 1).empty());
    CHECK(flipped_blob_byte_not_refused(c, "escape", at[200].at + 2000).empty());
    CHECK(flipped_blob_byte_not_refused(c, "escape-by-count", at[300].at + 5000).empty());
    CHECK(flipped_blob_byte_not_refused(c, "largest", at[400].at + 100000).empty());
    CHECK(flipped_blob_byte_not_refused(c, "killed", at[7].at + at[7].len - 1).empty());
    CHECK(apply_modes(c, ALL_MODES, counts).bad.empty());
}

// The hashes of the old addr_points.bin and addr_vertices.bin, which the diff
// writes once each, the points' and then the footprints', right before the
// address points' section. On a day that changes the points, and on one that
// changes a footprint alone (the points sent unchanged), the address replay
// copies old blobs unread under the old records' points, so in every mode a
// patch without either hash is refused, one with a second of either is, and
// one with another hash in the place of either ends on an old file that is
// not the patch's. On a day that leaves both files as they are the family is
// copied as its files sit, each of the two held to its hash first: a patch
// without either hash, or with another in the place of either, is refused
// all the same.
TEST(patch_refuses_an_address_replay_without_the_old_address_files_hashes) {
    ScratchDir dir("patch-addr-hash");
    std::mt19937 rng(20261118);
    std::vector<Address> addresses;
    for (uint32_t i = 0; i < 90; i++) {
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f, i, 0, 0, {}};
        if (i % 3) a.blob = footprint_at(rng, a.lat, a.lng, 4 + rng() % 9);
        addresses.push_back(a);
    }
    std::vector<Address> renumbered = addresses, edited = addresses;
    renumbered[31].housenumber = 5000;
    edited[31].blob.back() ^= 1;
    struct Day {
        std::string name;
        std::vector<Address> addresses;
        bool replayed;
    };
    // A hashed file, what an old one of another hash is refused as, and the
    // first word of what follows its hash in the patch: the footprints' hash
    // after the points', the address points' section after that.
    struct Hashed {
        PatchFileId file;
        std::string tag, stale;
        uint32_t next;
    };
    const Hashed hashed[] = {{PatchFileId::ADDR_POINTS, "points", NOT_THE_PATCHS_POINTS, OLD_FILE_HASH_MARKER},
                             {PatchFileId::ADDR_VERTICES, "footprints", NOT_THE_PATCHS_FOOTPRINTS, static_cast<uint32_t>(PatchFileId::ADDR_POINTS)}};
    for (const Day& day : {Day{"points", renumbered, true}, Day{"footprint", edited, true}, Day{"same", addresses, false}}) {
        BuildDir old_d, new_d;
        put_addresses(old_d, addresses);
        put_addresses(new_d, day.addresses);
        const LowMemCase c{dir.path(), day.name};
        REQUIRE(prepare(c, old_d, new_d).empty());
        const std::vector<char> raw = raw_patch(c);
        REQUIRE(!raw.empty());
        const LogCheck replay = [&](const std::string& log) -> std::string {
            return addr_replayed(log).found == day.replayed ? "" : day.replayed ? "no address replay ran" : "the address replay ran";
        };
        CHECK(apply_modes(c, ALL_MODES, replay).bad.empty());
        for (const Hashed& h : hashed) {
            const std::string file = patch_file_names[static_cast<uint32_t>(h.file)];
            const std::vector<char> made = old_hash_section(c, h.file);
            const size_t at = find_once(raw, made);
            if (at == std::string::npos) std::printf("    %s: no hash of the old %s\n", day.name.c_str(), file.c_str());
            REQUIRE(at != std::string::npos);
            const size_t end = at + made.size();
            uint32_t next = 0;
            REQUIRE(raw.size() >= end + 4);
            memcpy(&next, raw.data() + end, 4);
            CHECK_EQ(next, h.next);

            std::vector<char> another = raw, cut = raw, twice = raw;
            another[at + 8] ^= 1;
            cut.erase(cut.begin() + static_cast<std::ptrdiff_t>(at), cut.begin() + static_cast<std::ptrdiff_t>(end));
            twice.insert(twice.begin() + static_cast<std::ptrdiff_t>(end), made.begin(), made.end());
            struct Edit {
                std::string name;
                std::vector<char> patch;
                std::string refusal;  // none: it applies
            };
            for (const Edit& e : {Edit{h.tag + "-another", another, h.stale},
                                  Edit{h.tag + "-cut", cut,
                                       std::string("Malformed patch: ") + (day.replayed ? "address points" : "addr") + " without the old " + file + "'s hash"},
                                  Edit{h.tag + "-twice", twice, "Malformed patch: a second hash of the old " + file}}) {
                const std::string patch = c.dir(e.name + ".gcpatch");
                REQUIRE(write_patch(c, patch, e.patch));
                if (!e.refusal.empty()) {
                    CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
                    continue;
                }
                REQUIRE(write_patch(c, c.patch(), e.patch));
                const Applied a = apply_modes(c, ALL_MODES, replay);
                if (!a.bad.empty()) std::printf("    %s, %s\n", day.name.c_str(), e.name.c_str());
                CHECK(a.bad.empty());
            }
            REQUIRE(write_patch(c, c.patch(), raw));
        }
    }
}

// Address points sent unchanged over footprints that change (a vertex moved:
// as many vertices, so the records keep their bytes), and footprints sent
// unchanged under points that change (another house number, by slot a REPLACE
// and by content a DELETE and an INSERT): the address replay runs with the
// unchanged side as one MATCH of all of it, copies every blob the day leaves
// at its record and encodes the other. In every mode.
TEST(the_address_replay_copies_blobs_under_points_or_footprints_sent_unchanged) {
    ScratchDir dir("lowmem-addr-unchanged-side");
    std::mt19937 rng(20261119);
    std::vector<Address> addresses;
    for (uint32_t i = 0; i < 90; i++) {
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f, i, 0, 0, {}};
        if (i % 3) a.blob = footprint_at(rng, a.lat, a.lng, 4 + rng() % 9);
        addresses.push_back(a);
    }
    const uint64_t footprints = footprints_in(addresses), bytes = addresses[31].blob.size();
    std::vector<Address> renumbered = addresses, edited = addresses;
    renumbered[31].housenumber = 5000;
    edited[31].blob.back() ^= 1;
    BuildDir old_d;
    put_addresses(old_d, addresses);
    struct Day {
        std::string name;
        std::vector<Address> addresses;
        std::vector<std::string> options;
        PatchFileId unchanged;
        LogCheck replayed;
    };
    // The edited footprint keeps its bytes but the last unit: its old ones are
    // a MATCH of the old blob at hand, as the whole footprint of the address
    // inserted under its new number is.
    const Day days[] = {
        {"vertex", edited, {}, PatchFileId::ADDR_POINTS, addr_replayed_is(footprints - 1, 1, bytes - ADDR_VERTEX_STRIDE)},
        {"number-by-slot", renumbered, {"--identity-slots"}, PatchFileId::ADDR_VERTICES, addr_replayed_is(footprints, 0)},
        {"number-by-content", renumbered, {}, PatchFileId::ADDR_VERTICES, addr_replayed_is(footprints - 1, 1, bytes)},
    };
    for (const Day& day : days) {
        BuildDir new_d;
        put_addresses(new_d, day.addresses);
        const LowMemCase c{dir.path(), day.name};
        REQUIRE(prepare(c, old_d, new_d, {}, {}, day.options).empty());
        const std::vector<char> raw = raw_patch(c);
        // One of the two files goes as unchanged, the other does not.
        for (const PatchFileId fid : {PatchFileId::ADDR_POINTS, PatchFileId::ADDR_VERTICES}) {
            const char* file = patch_file_names[static_cast<uint32_t>(fid)];
            REQUIRE((copied_old_at(raw, fid, old_d.at(file).size()) != std::string::npos) == (fid == day.unchanged));
        }

        const Applied a = apply_modes(c, ALL_MODES, day.replayed);
        if (!a.bad.empty()) std::printf("    %s\n", day.name.c_str());
        CHECK(a.bad.empty());
    }
}

// A record that moves its point one step of latitude and keeps its footprint,
// byte for byte, gets a new blob: a blob holds its bbox minimum as residuals
// from where the point predicts it, so the old record's is not the builder's
// for the new one, though the footprints go as unchanged. By slot the record
// is a REPLACE, by content a DELETE and an INSERT: either way the replay
// encodes that one footprint from the old blob's bytes and copies the others.
TEST(the_address_replay_gives_a_record_that_moved_its_point_a_new_blob) {
    ScratchDir dir("lowmem-addr-moved");
    std::mt19937 rng(20261120);
    std::vector<Address> addresses;
    for (uint32_t i = 0; i < 90; i++) {
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f, i, 0, 0, {}};
        if (i % 3) a.blob = footprint_at(rng, a.lat, a.lng, 4 + rng() % 9);
        addresses.push_back(a);
    }
    std::vector<Address> moved = addresses;
    moved[31].lat = std::nextafter(moved[31].lat, 100.0f);
    BuildDir old_d, new_d;
    put_addresses(old_d, addresses);
    put_addresses(new_d, moved);
    // The same footprints, and another blob for the moved record's.
    REQUIRE(old_d.at("addr_vertices.bin") == new_d.at("addr_vertices.bin"));
    const std::vector<BlobAt> old_at = blobs_at(addresses), new_at = blobs_at(moved);
    REQUIRE(old_at[31].at == new_at[31].at && old_at[31].len == new_at[31].len);
    const std::vector<char> old_blobs = gctest::as_written(old_d).at("addr_vertices.bin"),
                            new_blobs = gctest::as_written(new_d).at("addr_vertices.bin");
    REQUIRE(!std::equal(old_blobs.begin() + old_at[31].at, old_blobs.begin() + old_at[31].at + old_at[31].len, new_blobs.begin() + new_at[31].at));
    const uint64_t footprints = footprints_in(addresses);
    for (const bool by_slot : {true, false}) {
        const LowMemCase c{dir.path(), by_slot ? "by-slot" : "by-content"};
        REQUIRE(prepare(c, old_d, new_d, {}, {}, by_slot ? std::vector<std::string>{"--identity-slots"} : std::vector<std::string>{}).empty());

        CHECK(apply_modes(c, ALL_MODES, addr_replayed_is(footprints - 1, 1, addresses[31].blob.size())).bad.empty());
    }
}

// A footprint that changes under a record sent unchanged (its vertices' steps
// grow from 4 bits to 16; as many vertices, so the logical record keeps its
// bytes) gets a new blob, and its record's block the new blob's len: the
// points are sent unchanged and the patcher writes another addr_points.bin,
// the builder's.
TEST(the_address_replay_gives_a_footprint_that_changed_under_an_unchanged_record_a_new_blob_and_len) {
    ScratchDir dir("lowmem-addr-len");
    std::mt19937 rng(20261121);
    std::vector<Address> addresses;
    for (uint32_t i = 0; i < 90; i++) {
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f, i, 0, 0, {}};
        if (i % 3) a.blob = footprint_at(rng, a.lat, a.lng, 20, 0, 3, 16);
        addresses.push_back(a);
    }
    std::vector<Address> wider = addresses;
    wider[31].blob = footprint_at(rng, wider[31].lat, wider[31].lng, 20, 0, 3, 60000);
    BuildDir old_d, new_d;
    put_addresses(old_d, addresses);
    put_addresses(new_d, wider);
    REQUIRE(old_d.at("addr_points.bin") == new_d.at("addr_points.bin"));
    REQUIRE(blobs_at(addresses)[31].len < blobs_at(wider)[31].len);
    REQUIRE(gctest::as_written(old_d).at("addr_points.bin") != gctest::as_written(new_d).at("addr_points.bin"));
    const LowMemCase c{dir.path(), "wider"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    REQUIRE(copied_old_at(raw_patch(c), PatchFileId::ADDR_POINTS, old_d.at("addr_points.bin").size()) != std::string::npos);

    const uint64_t footprints = footprints_in(addresses);
    AddrReplayed n;
    CHECK(apply_modes(c, ALL_MODES, [&](const std::string& log) -> std::string {
              n = addr_replayed(log);
              return n.found && n.copied == footprints - 1 && n.encoded == 1 ? "" : "not every blob but one copied: " + n.text();
          }).bad.empty());
}

// Escaped footprints through the address replay, none held whole. By slot: one
// kept at its record and one under another house number (each copied from the
// old file in pieces), one whose record moves a step (its bytes read from the
// old file where they sit, a piece at a time, into the encoder) and a new one
// (from the patch). By content: one whose record and the next change their
// house numbers, two records deleted and two inserted over footprints sent
// unchanged, so that the replay reads the escape again after passing it. In
// every mode the replay holds no more of the old files than DECODER_HOLD, nor
// more of the new ones than a piece and the largest footprint a blob holds;
// and a byte flipped in the escape copied from the old dir is refused by the
// old addr_vertices.bin's hash.
TEST(the_address_replay_passes_an_escape_in_pieces) {
    ScratchDir dir("lowmem-addr-escape");
    std::mt19937 rng(20261122);
    std::vector<Address> addresses;
    for (uint32_t i = 0; i < 120; i++) {
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f, i, 0, 0, {}};
        if (i % 3 != 2) a.blob = footprint_at(rng, a.lat, a.lng, 4 + rng() % 9);
        addresses.push_back(a);
    }
    addresses[30].blob = escape_of(rng, 200000);
    addresses[60].blob = block(rng, MAX_BLOB_VERTICES + 1);
    addresses[90].blob = escape_of(rng, 100000);
    const uint64_t footprints = footprints_in(addresses);
    std::vector<Address> by_slot = addresses, by_content = addresses;
    by_slot[60].housenumber = 5000;
    by_slot[90].lat = std::nextafter(by_slot[90].lat, 100.0f);
    by_slot.push_back({48.0f, 8.0f, 6000, 0, 0, escape_of(rng, 50000)});
    by_content[30].housenumber = 5000;
    by_content[31].housenumber = 5001;
    REQUIRE(!addresses[31].blob.empty());
    struct Day {
        std::string name;
        std::vector<Address> addresses;
        std::vector<std::string> options;
        LogCheck replayed;
    };
    const Day days[] = {
        {"by-slot", by_slot, {"--identity-slots"}, addr_replayed_is(footprints - 1, 2, addresses[90].blob.size())},
        {"by-content", by_content, {}, addr_replayed_is(footprints - 2, 2, addresses[31].blob.size(), addresses[30].blob.size())},
    };
    BuildDir old_d;
    put_addresses(old_d, addresses);
    const LogCheck decoder = decoder_peaks_at_most("addr", DECODER_HOLD), encoder = encoder_peaks_at_most("addr", ADDR_ENCODER_HOLD);
    for (const Day& day : days) {
        BuildDir new_d;
        put_addresses(new_d, day.addresses);
        const LowMemCase c{dir.path(), day.name};
        REQUIRE(prepare(c, old_d, new_d, {}, {}, day.options).empty());

        const Applied a = apply_modes(c, ALL_MODES, [&](const std::string& log) {
            for (const LogCheck& check : {day.replayed, decoder, encoder})
                if (const std::string e = check(log); !e.empty()) return e;
            return std::string();
        });
        if (!a.bad.empty()) std::printf("    %s\n", day.name.c_str());
        CHECK(a.bad.empty());
        if (day.name == "by-slot") CHECK(flipped_blob_byte_not_refused(c, "flipped", blobs_at(addresses)[30].at + 700000).empty());
    }
}

// Street ways moved by the identity sidecars (each pair of the first 40 swaps
// its records) under addresses on every way: the address records stay MATCHed,
// each parent_way_id moved by the street replay's remap, the ways that stay
// in order by its runs and the moved ones by the parent-id remap's pairs, and
// their blobs are copied. In every mode the patched dir is the new dir.
TEST(the_address_replay_moves_parent_ways_by_the_street_remap) {
    ScratchDir dir("lowmem-addr-parents");
    constexpr uint32_t MOVED = 20, KEPT = 30, WAYS = 2 * MOVED + KEPT;
    std::mt19937 rng(20261123);
    BuildDir old_d, new_d, old_meta, new_meta;
    put_moved_ways(old_d, new_d, old_meta, new_meta, MOVED, KEPT);
    std::vector<Address> old_addresses, new_addresses;
    for (uint32_t i = 0; i < 280; i++) {
        // On the way of entity i mod WAYS: its old record, and its new one.
        const uint32_t way = i % WAYS;
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f, i, 0, way, {}};
        if (i % 3) a.blob = footprint_at(rng, a.lat, a.lng, 4 + rng() % 9);
        old_addresses.push_back(a);
        a.parent = way < 2 * MOVED ? way ^ 1 : way;
        new_addresses.push_back(a);
    }
    put_addresses(old_d, old_addresses);
    put_addresses(new_d, new_addresses);
    REQUIRE(old_d.at("addr_points.bin") != new_d.at("addr_points.bin"));
    const LowMemCase c{dir.path(), "parents"};
    REQUIRE(prepare(c, old_d, new_d, old_meta, new_meta).empty());
    // The street leg: the moved ways' pairs.
    std::vector<char> leg;
    for (const uint32_t w : {POI_PARENT_REMAP_MARKER, MOVED}) append(leg, w);
    REQUIRE(find_once(raw_patch(c), leg) != std::string::npos);

    CHECK(apply_modes(c, ALL_MODES, addr_replayed_is(footprints_in(old_addresses), 0)).bad.empty());
    const BuildDir want = gctest::as_written(new_d);
    for (const std::string& mode : ALL_MODES) CHECK(read_file(c.dir("out." + mode) + "/addr_points.bin") == want.at("addr_points.bin"));
}

// Street ways sent unchanged have no replay and so no remap: address points
// then move their parent_way_id by the parent-id remap's street leg alone.
// No day gives such a patch (ways that move change their file), so one made
// by hand from the diff's patch of an unchanged set: a pair in the street leg
// (way 3 to way 7) and the address points' section one MATCH of every record.
// In every mode the addresses on way 3 are on way 7, the others where they
// were, and every blob is copied.
TEST(the_address_replay_moves_parent_ways_by_the_street_legs_pairs_under_street_ways_sent_unchanged) {
    ScratchDir dir("lowmem-addr-leg");
    std::mt19937 rng(20261129);
    std::vector<Address> addresses;
    for (uint32_t i = 0; i < 40; i++) {
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f, i, 0, i % 5, {}};
        if (i % 3) a.blob = footprint_at(rng, a.lat, a.lng, 4 + rng() % 9);
        addresses.push_back(a);
    }
    std::vector<Address> moved = addresses;
    for (Address& a : moved)
        if (a.parent == 3) a.parent = 7;
    BuildDir d, moved_d;
    put_geo(d, {{0, {0}}}, 10);
    put_addresses(d, addresses);
    moved_d = d;
    put_addresses(moved_d, moved);
    const LowMemCase c{dir.path(), "leg"};
    REQUIRE(prepare(c, d, d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    std::vector<char> leg, pair;
    for (const uint32_t w : {POI_PARENT_REMAP_MARKER, 0u}) append(leg, w);
    for (const uint32_t w : {3u, 7u}) append(pair, w);
    const std::vector<char>& records = d.at("addr_points.bin");
    const std::vector<char> unchanged = copy_old_section(PatchFileId::ADDR_POINTS, records.size());
    const size_t leg_at = find_once(raw, leg), points_at = find_once(raw, unchanged);
    REQUIRE(leg_at != std::string::npos && points_at != std::string::npos && leg_at < points_at);
    REQUIRE(copied_old_at(raw, PatchFileId::STREET_WAYS, d.at("street_ways.bin").size()) != std::string::npos);
    std::vector<char> every_record;
    put_op(every_record, OP_MATCH_RUN, static_cast<uint32_t>(addresses.size()));
    const std::vector<char> merge =
        merge_of(PatchFileId::ADDR_POINTS, static_cast<uint32_t>(ADDR_POINT_STRIDE), records.size(), records.size(), every_record);
    // The later edit first: the points' section a merge, then the pair.
    std::vector<char> edited = raw;
    edited.erase(edited.begin() + static_cast<std::ptrdiff_t>(points_at), edited.begin() + static_cast<std::ptrdiff_t>(points_at + unchanged.size()));
    edited.insert(edited.begin() + static_cast<std::ptrdiff_t>(points_at), merge.begin(), merge.end());
    put_at(edited, leg_at + 4, 1u);
    edited.insert(edited.begin() + static_cast<std::ptrdiff_t>(leg_at + 8), pair.begin(), pair.end());
    const std::string patch = c.dir("leg.gcpatch");
    REQUIRE(write_patch(c, patch, edited));

    const BuildDir want = gctest::as_written(moved_d);
    const LogCheck replayed = addr_replayed_is(footprints_in(addresses), 0);
    for (const std::string& mode : ALL_MODES) {
        const std::string out = c.dir("out.leg." + mode);
        std::filesystem::remove_all(out);
        const ToolRun r = run_tool(patch_words(c, mode, patch, out), c.log());
        CHECK(r.ok);
        CHECK(read_dir(out) == want);
        CHECK_EQ(replayed(r.log), std::string());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// What follows the address records through a day that deletes, inserts and
// renumbers them: addr_postcodes.bin, a value an address, which the patch
// sends as a parallel delta along the address points' merge sequence, and the
// geo index's address lists, whose ids move by the address replay's record
// remap. The replay leaves the patcher both, as the generic replay does: in
// every mode the patched dir is the new dir.
TEST(the_address_replay_records_the_sequence_and_the_runs_the_side_file_and_the_geo_lists_need) {
    ScratchDir dir("lowmem-addr-followers");
    constexpr uint32_t CELLS = 50;
    std::mt19937 rng(20261124);
    struct Entity {
        Address a;
        uint32_t postcode, cell;
    };
    std::vector<Entity> old_day, new_day;
    for (uint32_t e = 0; e < 300; e++) {
        Address a{47.0f + static_cast<float>(e) * 1e-4f, 8.0f, e, 0, e % 10, {}};
        if (e % 3) a.blob = footprint_at(rng, a.lat, a.lng, 4 + rng() % 9);
        old_day.push_back({a, e / 3 * 8, e % CELLS});
    }
    for (uint32_t e = 0; e < 300; e++) {
        Entity n = old_day[e];
        if (e % 40 == 20) {
            Address a{48.0f + static_cast<float>(e) * 1e-4f, 8.0f, 1000 + e, 0, e % 10, {}};
            if (e % 80) a.blob = footprint_at(rng, a.lat, a.lng, 4 + rng() % 9);
            new_day.push_back({a, 4000 + e, (e + 7) % CELLS});
        }
        if (e % 11 == 3) continue;
        if (e % 13 == 5) n.a.housenumber += 5000;
        if (e % 17 == 2) n.postcode += 8;
        new_day.push_back(n);
    }
    // A day's dir: its addresses, their postcodes, and the geo index of
    // CELLS cells over 10 street ways, cell k listing street k mod 10 and the
    // addresses in it.
    auto dir_of = [&](const std::vector<Entity>& day) {
        BuildDir d;
        std::vector<Address> addresses;
        std::vector<char> postcodes, street_ways, top, blocks;
        std::vector<geo_index::RowLists> rows(CELLS);
        for (uint32_t k = 0; k < CELLS; k++) rows[k][0] = {k % 10};
        for (uint32_t id = 0; id < day.size(); id++) {
            addresses.push_back(day[id].a);
            append(postcodes, day[id].postcode);
            rows[day[id].cell][geo_index::ADDRESSES].push_back(id);
        }
        put_addresses(d, addresses);
        d["addr_postcodes.bin"] = postcodes;
        for (uint32_t w = 0; w < 10; w++) {
            append(street_ways, 0u);
            append(street_ways, uint16_t{0});
            append(street_ways, uint16_t{0});
            append(street_ways, w);
        }
        geo_index::Writer writer([&](const char* p, size_t n) { top.insert(top.end(), p, p + n); },
                                 [&](const char* p, size_t n) { blocks.insert(blocks.end(), p, p + n); });
        for (uint32_t k = 0; k < CELLS; k++) writer.add(cell_id(k), geo_index::ids_of(rows[k]));
        writer.finish();
        d["street_ways.bin"] = street_ways;
        d["street_nodes.bin"] = {};
        d["geo_cells.bin"] = top;
        d["geo_entries.bin"] = blocks;
        return d;
    };
    const BuildDir old_d = dir_of(old_day), new_d = dir_of(new_day);
    REQUIRE(old_d.at("geo_entries.bin") != new_d.at("geo_entries.bin"));
    const LowMemCase c{dir.path(), "followers"};
    REQUIRE(prepare(c, old_d, new_d).empty());

    CHECK(apply_modes(c, ALL_MODES, [](const std::string& log) -> std::string {
              if (!addr_replayed(log).found) return "no address replay in the log";
              if (log.find("addr_postcodes.bin: parallel delta along addr_points.bin") == std::string::npos)
                  return "addr_postcodes.bin did not follow the address points' merge";
              return "";
          }).bad.empty());
    const BuildDir want = gctest::as_written(new_d);
    for (const std::string& mode : ALL_MODES)
        for (const char* f : {"addr_postcodes.bin", "geo_cells.bin", "geo_entries.bin"})
            CHECK(read_file(c.dir("out." + mode) + "/" + f) == want.at(f));
}

// n address points, two in three with a footprint of 1 to 8 vertices; the new
// day's with another house number on every fiftieth and the one after it (by
// content each pair goes as two records deleted and two inserted, so the
// replay reads the first one's old blob again after passing it and the
// second's where it is at) and a vertex moved in every ninety-seventh's
// footprint (encoded from the old blob at hand, under a record kept). The
// replay holds a block's records, a blob and the places of the blobs it
// passed last, whatever n.
TEST(patch_heap_stays_flat_as_address_blobs_are_copied_and_encoded) {
    CHECK(heap_flat([](const LowMemCase& c, uint32_t n) {
        std::mt19937 rng(20261125);
        std::vector<Address> addresses;
        for (uint32_t i = 0; i < n; i++) {
            Address a{47.0f + static_cast<float>(i % 100000) * 1e-4f, 8.0f + static_cast<float>(i / 100000) * 1e-2f, i, 0, 0, {}};
            if (i % 3) a.blob = footprint_at(rng, a.lat, a.lng, 1 + rng() % 8);
            addresses.push_back(a);
        }
        std::vector<Address> changed = addresses;
        for (uint32_t i = 0; i < n; i++) {
            if (i % 50 == 7 || i % 50 == 8) changed[i].housenumber += n;
            if (i % 97 == 11 && !changed[i].blob.empty()) changed[i].blob.back() ^= 1;
        }
        BuildDir old_d, new_d;
        put_addresses(old_d, addresses);
        put_addresses(new_d, changed);
        if (std::string e = prepare(c, old_d, new_d); !e.empty()) return e;
        return patch_holds(c, section_head(PatchFileId::ADDR_POINTS, static_cast<uint32_t>(ADDR_POINT_STRIDE), uint64_t{ADDR_POINT_STRIDE} * n,
                                           uint64_t{ADDR_POINT_STRIDE} * n),
                           "merge of " + std::to_string(n) + " address points");
    }, 50000).empty());
}

namespace {

// What addr_files::decode checks of the address files between its units,
// the blobs' values left unread: the footers, each block where the index puts
// it, each blob's header and len, and the files' ends. The stage that refused
// them and its words, or none.
struct Walked {
    enum Stage { NONE, FOOTERS, INDEX, BLOCK, BLOB, END };
    Stage stage = NONE;
    std::string refusal;
};

Walked walk_address_files(const std::vector<char>& points_file, const std::vector<char>& vertices_file, const std::vector<char>& index_file) {
    namespace af = addr_files;
    const uint8_t* points = reinterpret_cast<const uint8_t*>(points_file.data());
    const uint8_t* vertices = reinterpret_cast<const uint8_t*>(vertices_file.data());
    const uint8_t* index = reinterpret_cast<const uint8_t*>(index_file.data());
    Walked w;
    w.stage = Walked::FOOTERS;
    try {
        auto end_of = [](const uint8_t* file, size_t size, size_t footer) { return file + (size - std::min(size, footer)); };
        const af::Footers f = af::read_footers(end_of(points, points_file.size(), af::POINTS_FOOTER_BYTES), points_file.size(),
                                               end_of(vertices, vertices_file.size(), af::VERTICES_FOOTER_BYTES), vertices_file.size(),
                                               end_of(index, index_file.size(), af::INDEX_FOOTER_BYTES), index_file.size());
        const char* const misplaced = " is not where the blocks and the blobs before it end";
        af::Block block;
        uint64_t blocks_at = 0, blobs_at = 0, row_blocks_at = 0, row_blobs_at = 0, logical = 0, blobs = 0;
        for (uint64_t b = 0; b < f.blocks(); b++) {
            w.stage = Walked::INDEX;
            const uint8_t* row = index + (b / af::SUPERBLOCK_BLOCKS) * af::INDEX_ROW_BYTES;
            if (b % af::SUPERBLOCK_BLOCKS == 0) {
                if (af::get64(row) != blocks_at || af::get64(row + 8) != blobs_at)
                    af::malformed(std::string(af::INDEX_FILE) + " superblock " + std::to_string(b / af::SUPERBLOCK_BLOCKS) + misplaced);
                row_blocks_at = blocks_at;
                row_blobs_at = blobs_at;
            }
            const uint8_t* entry = row + af::INDEX_HEAD_BYTES + (b % af::SUPERBLOCK_BLOCKS) * af::INDEX_ENTRY_BYTES;
            if (af::get16(entry) != blocks_at - row_blocks_at || af::get32(entry + 2) != blobs_at - row_blobs_at)
                af::malformed(std::string(af::INDEX_FILE) + " block " + std::to_string(b) + misplaced);
            w.stage = Walked::BLOCK;
            const size_t n = f.block_records(b);
            blocks_at += af::decode_block(b, points + blocks_at, points_file.size() - blocks_at, n, block);
            w.stage = Walked::BLOB;
            for (size_t j = 0; j < n; j++) {
                const uint64_t i = b * af::BLOCK_RECORDS + j;
                const uint32_t len = block.len[j];
                if (!len) continue;
                if (len > f.blobs_bytes - blobs_at) af::malformed(std::string(af::VERTICES_FILE) + " ends inside the blob of record " + std::to_string(i));
                try {
                    const af::BlobHeader h = af::blob_header(vertices + blobs_at, len);
                    if (h.logical_bytes() > f.logical_vertex_bytes - logical)
                        af::malformed("a footprint past the footer's logical vertex stream of " + std::to_string(f.logical_vertex_bytes) + " bytes");
                    if (!h.escape) af::blob_values(h, vertices + blobs_at, len);
                    logical += h.logical_bytes();
                } catch (const std::runtime_error& e) {
                    throw std::runtime_error(std::string(e.what()) + ": " + af::VERTICES_FILE + ", record " + std::to_string(i));
                }
                blobs_at += len;
                blobs++;
            }
        }
        w.stage = Walked::END;
        if (blocks_at != f.blocks_bytes) af::malformed(std::string(af::POINTS_FILE) + " has bytes between its last block and its footer");
        if (blobs_at != f.blobs_bytes) af::malformed(std::string(af::VERTICES_FILE) + " has bytes between its last blob and its footer");
        if (logical != f.logical_vertex_bytes)
            af::malformed(std::string(af::VERTICES_FILE) + " counts " + std::to_string(f.logical_vertex_bytes) +
                          " bytes of logical blobs and its blobs hold " + std::to_string(logical));
        if (blobs != f.blobs)
            af::malformed(std::string(af::VERTICES_FILE) + " counts " + std::to_string(f.blobs) + " blobs and holds " + std::to_string(blobs));
        w.stage = Walked::NONE;
    } catch (const std::runtime_error& e) {
        w.refusal = e.what();
    }
    return w;
}

}  // namespace

// Old address files the builder would not write, 300 sets each a byte away
// from the files a patch was made from (a bit or a byte of any of the three
// files changed, or a byte cut out or added): the address replay refuses them
// as addr_files::decode does. A set the decoder refuses by its footers, its
// index or a block, or by a blob's header or len when addr_vertices.bin holds
// the damage, is refused there in the decoder's words, which name the old
// record, before a byte of it is used. Blobs' values the replay does not
// decode, and a block that decodes to other records is one the encoder
// writes: any set whose addr_points.bin or addr_vertices.bin is not the
// patch's is refused, at the latest by the old file's hash. No set applies.
// Then three sets the decoder refuses only where the files end, each refused
// in its words: a byte between the last block and its footer, one between
// the last blob and its footer, and a footer that counts a blob more.
TEST(the_address_replay_refuses_old_address_files_as_the_strict_decoder_does) {
    ScratchDir dir("lowmem-addr-damage");
    std::mt19937 rng(20261126);
    std::vector<Address> addresses;
    for (uint32_t i = 0; i < 60; i++) {
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f + static_cast<float>(i % 5) * 1e-4f, i * 3, i % 4, i % 6, {}};
        if (i % 3) a.blob = footprint_at(rng, a.lat, a.lng, 2 + rng() % 9, static_cast<uint8_t>(i % 3), static_cast<int>(rng() % 200) - 100);
        addresses.push_back(a);
    }
    addresses[22].blob = escape_of(rng, 5);
    // The day, by content: an address moved a step, a vertex moved, an address
    // more, and two gone, the first early: a record's place in the old files
    // is not its place in the new ones.
    std::vector<Address> changed = addresses;
    changed[26].lat = std::nextafter(changed[26].lat, 100.0f);
    changed[40].blob.back() ^= 1;
    changed.push_back({48.0f, 8.0f, 6000, 0, 0, footprint_at(rng, 48.0f, 8.0f, 7)});
    changed.erase(changed.begin() + 50);
    changed.erase(changed.begin() + 4);
    BuildDir old_d, new_d;
    put_addresses(old_d, addresses);
    put_addresses(new_d, changed);
    const LowMemCase c{dir.path(), "damage"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
    std::map<std::string, std::vector<char>> written;
    for (const char* f : ADDRESS_FILES) written[f] = read_file(c.dir("old") + "/" + f);
    REQUIRE(walk_address_files(written["addr_points.bin"], written["addr_vertices.bin"], written["addr_index.bin"]).stage == Walked::NONE);

    size_t at_stage[6] = {}, by_hash = 0, applied = 0, wrong = 0;
    for (uint32_t k = 0; k < 300; k++) {
        std::map<std::string, std::vector<char>> damaged = written;
        const std::string file = ADDRESS_FILES[rng() % 3];
        std::vector<char>& bytes = damaged[file];
        const size_t at = rng() % bytes.size();
        const uint32_t how = rng() % 20;
        if (how == 0) bytes.erase(bytes.begin() + static_cast<std::ptrdiff_t>(at));
        else if (how == 1) bytes.insert(bytes.begin() + static_cast<std::ptrdiff_t>(at), static_cast<char>(rng()));
        else if (how < 11) bytes[at] = static_cast<char>(bytes[at] ^ (1 << (rng() % 8)));
        else bytes[at] = static_cast<char>(bytes[at] ^ (1 + rng() % 255));
        const std::string what = "set " + std::to_string(k) + " (" + file + " at " + std::to_string(at) + ")";
        const Walked w = walk_address_files(damaged["addr_points.bin"], damaged["addr_vertices.bin"], damaged["addr_index.bin"]);
        std::string strict;
        try {
            auto u8 = [&](const char* f) { return reinterpret_cast<const uint8_t*>(damaged[f].data()); };
            addr_files::decode(u8("addr_points.bin"), damaged["addr_points.bin"].size(), u8("addr_vertices.bin"), damaged["addr_vertices.bin"].size(),
                               u8("addr_index.bin"), damaged["addr_index.bin"].size());
        } catch (const std::runtime_error& e) {
            strict = e.what();
        }
        for (const char* f : ADDRESS_FILES) REQUIRE(write_file(c.dir("old") + "/" + f, damaged[f]));
        std::filesystem::remove_all(c.dir("out.damaged"));
        const ToolRun r = run_tool(patch_words(c, "sync", c.patch(), c.dir("out.damaged")), c.log());

        at_stage[w.stage]++;
        applied += r.ok;
        // Refused where the damage is: by the footers, the index or a block,
        // or by a blob's header when the blobs' file holds the damage (the
        // blocks then give each blob its place).
        const bool at_the_damage = w.stage == Walked::FOOTERS || w.stage == Walked::INDEX || w.stage == Walked::BLOCK ||
                                   (w.stage == Walked::BLOB && file == "addr_vertices.bin");
        std::string bad;
        // The replay decodes a few blobs, and so may refuse one before the
        // walk's: then in the strict decoder's words.
        if (at_the_damage && !(!r.ok && (logged(r, w.refusal) || (!strict.empty() && logged(r, strict)))))
            bad = "not refused as \"" + w.refusal + "\"";
        else if (r.ok) bad = w.stage != Walked::NONE ? "applied, though \"" + w.refusal + "\"" : "applied with another " + file;
        // Whatever else the replay says of the old files, the strict decoder says.
        else if (!r.ok && w.stage == Walked::NONE && logged(r, "Malformed address files: ") && !logged(r, "addr_points.bin record ") &&
                 (strict.empty() || !logged(r, strict)))
            bad = "refused in words that are not the strict decoder's \"" + strict + "\"";
        by_hash += !r.ok && (logged(r, NOT_THE_PATCHS_POINTS) || logged(r, NOT_THE_PATCHS_FOOTPRINTS));
        if (!bad.empty()) {
            wrong++;
            std::printf("    %s: %s: %s\n", what.c_str(), bad.c_str(), r.ok ? "applied" : r.log.substr(0, 600).c_str());
        }
    }
    {
        std::map<std::string, std::vector<char>> between_blocks = written, between_blobs = written, a_blob_more = written;
        std::vector<char>& blocks = between_blocks["addr_points.bin"];
        blocks.insert(blocks.end() - static_cast<std::ptrdiff_t>(addr_files::POINTS_FOOTER_BYTES), 0);
        std::vector<char>& blobs = between_blobs["addr_vertices.bin"];
        blobs.insert(blobs.end() - static_cast<std::ptrdiff_t>(addr_files::VERTICES_FOOTER_BYTES), 0);
        std::vector<char>& counted = a_blob_more["addr_vertices.bin"];
        uint32_t n_blobs = 0;
        memcpy(&n_blobs, counted.data() + counted.size() - 8, 4);
        put_at(counted, counted.size() - 8, n_blobs + 1);
        for (const auto& [name, set] : {std::pair<const char*, std::map<std::string, std::vector<char>>>{"between-blocks", between_blocks},
                                        {"between-blobs", between_blobs},
                                        {"a-blob-more", a_blob_more}}) {
            const Walked w = walk_address_files(set.at("addr_points.bin"), set.at("addr_vertices.bin"), set.at("addr_index.bin"));
            REQUIRE(w.stage == Walked::END);
            for (const char* f : ADDRESS_FILES) REQUIRE(write_file(c.dir("old") + "/" + f, set.at(f)));
            const std::vector<std::string> bad = not_refused(c, ALL_MODES, c.patch(), name, w.refusal);
            if (!bad.empty()) std::printf("    not refused as \"%s\"\n", w.refusal.c_str());
            CHECK(bad.empty());
        }
    }
    for (const char* f : ADDRESS_FILES) REQUIRE(write_file(c.dir("old") + "/" + f, written[f]));
    std::printf("    300 sets: the walk refused %zu by the footers, %zu by the index, %zu by a block, %zu by a blob, %zu at the end, %zu not; "
                "%zu refused by the hash, %zu applied\n",
                at_stage[Walked::FOOTERS], at_stage[Walked::INDEX], at_stage[Walked::BLOCK], at_stage[Walked::BLOB], at_stage[Walked::END],
                at_stage[Walked::NONE], by_hash, applied);
    CHECK_EQ(wrong, size_t{0});
    CHECK_EQ(applied, size_t{0});
    // Every kind of refusal came up.
    CHECK(at_stage[Walked::FOOTERS] >= 3);
    CHECK(at_stage[Walked::INDEX] >= 20);
    CHECK(at_stage[Walked::BLOCK] >= 20);
    CHECK(at_stage[Walked::BLOB] >= 5);
    CHECK(by_hash >= 20);
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// An address replay's sections that do not fit the old files or each other
// are refused by name in every mode. A day by content that drops an address
// with a footprint and appends one, so the points' merge is a MATCH, a
// DELETE, a MATCH and an INSERT with the footprints' beside it; then, in the
// patch: the points' last MATCH run or their DELETE run past the old records;
// the footprints' last MATCH run past the old stream; the new record's
// vertex_offset one off and its footprint in an encoding none has; the
// footprints' section at another stride than its own, its sequence's length
// 2^32 past the patch, and its old size a byte more than the old files';
// both sections a second time; the points' new size a record more than the
// merge gives, or the footprints' a byte more; and, left over when the replay
// ends, an offset fixup for a record past the old file's and a string fixup
// for a slot past them. The patch as made applies.
TEST(the_address_replay_refuses_sections_that_do_not_fit_the_old_files_or_each_other) {
    ScratchDir dir("lowmem-addr-sections");
    std::mt19937 rng(20261127);
    std::vector<Address> addresses;
    for (uint32_t i = 0; i < 60; i++) {
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f, i, 0, 0, {}};
        if (i % 3 != 2) a.blob = footprint_at(rng, a.lat, a.lng, 4 + rng() % 9);
        addresses.push_back(a);
    }
    std::vector<Address> changed = addresses;
    changed.erase(changed.begin() + 21);
    changed.push_back({48.0f, 8.0f, 6000, 0, 0, footprint_at(rng, 48.0f, 8.0f, 12)});
    BuildDir old_d, new_d;
    put_addresses(old_d, addresses);
    put_addresses(new_d, changed);
    const LowMemCase c{dir.path(), "sections"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    const uint64_t old_points = old_d.at("addr_points.bin").size(), new_points = new_d.at("addr_points.bin").size();
    const uint64_t old_bytes = old_d.at("addr_vertices.bin").size(), new_bytes = new_d.at("addr_vertices.bin").size();
    // The two sections' heads, and the points' and the footprints' runs.
    const size_t points = find_once(raw, section_head(PatchFileId::ADDR_POINTS, static_cast<uint32_t>(ADDR_POINT_STRIDE), old_points, new_points));
    const size_t footprints = find_once(raw, section_head(PatchFileId::ADDR_VERTICES, ADDR_VERTEX_STRIDE, old_bytes, new_bytes));
    REQUIRE(points != std::string::npos && footprints != std::string::npos && points < footprints);
    uint64_t before = 0;
    for (uint32_t i = 0; i < 21; i++) before += addresses[i].blob.size();
    const uint32_t gone = static_cast<uint32_t>(addresses[21].blob.size());
    REQUIRE(gone > 0);
    std::vector<char> point_runs, footprint_runs;
    put_op(point_runs, OP_MATCH_RUN, 21);
    put_op(point_runs, OP_DELETE_RUN, 1);
    put_op(point_runs, OP_MATCH_RUN, 38);
    put_op(footprint_runs, OP_MATCH_RUN, static_cast<uint32_t>(before / ADDR_VERTEX_STRIDE));
    put_op(footprint_runs, OP_DELETE_RUN, gone / ADDR_VERTEX_STRIDE);
    put_op(footprint_runs, OP_MATCH_RUN, static_cast<uint32_t>((old_bytes - before - gone) / ADDR_VERTEX_STRIDE));
    const size_t point_runs_at = find_once(raw, point_runs), footprint_runs_at = find_once(raw, footprint_runs);
    REQUIRE(point_runs_at != std::string::npos && footprint_runs_at != std::string::npos);
    // The new record and its footprint, as the patch carries them.
    const std::vector<char>& new_records = new_d.at("addr_points.bin");
    const std::vector<char> record(new_records.end() - static_cast<std::ptrdiff_t>(ADDR_POINT_STRIDE), new_records.end());
    const size_t record_at = find_once(raw, record), blob_at = find_once(raw, changed.back().blob);
    REQUIRE(record_at != std::string::npos && blob_at != std::string::npos);
    // The footprints' sequence length: after the head, the offset fixups'
    // counts and sizes (none) and the string fixups' (none).
    const size_t footprints_len_at = footprints + 24 + 24;
    uint64_t footprints_len = 0;
    memcpy(&footprints_len, raw.data() + footprints_len_at, 8);
    const size_t footprints_end = footprints_len_at + 8 + footprints_len;
    REQUIRE(footprints_end <= raw.size() && footprint_runs_at == footprints_len_at + 8);

    // The points' offset fixups: after the head their run and value counts and
    // sizes, the runs, the values (none); then the string fixups' count and
    // size (none).
    uint32_t fixup_words[4] = {}, string_fixup_words[2] = {};
    memcpy(fixup_words, raw.data() + points + 24, sizeof fixup_words);
    const size_t values_at = points + 40 + fixup_words[2];
    memcpy(string_fixup_words, raw.data() + values_at, sizeof string_fixup_words);
    REQUIRE(fixup_words[0] > 0 && fixup_words[1] == 0 && fixup_words[3] == 0 && string_fixup_words[0] == 0 && string_fixup_words[1] == 0);

    using Edit = std::function<void(std::vector<char>&)>;
    struct Case {
        std::string name;
        Edit edit;
        std::string refusal;
    };
    uint32_t vertex_offset = 0;
    memcpy(&vertex_offset, record.data() + ADDR_POINT_VERTEX_OFFSET_OFF, 4);
    const Case cases[] = {
        {"points-match", [&](std::vector<char>& p) { put_at(p, point_runs_at + 11, 43u); },
         "Malformed merge sequence: addr_points.bin (a MATCH run past the old file)"},
        {"points-delete", [&](std::vector<char>& p) { put_at(p, point_runs_at + 6, 40u); },
         "Malformed merge sequence: addr_points.bin (a DELETE run past the old file)"},
        {"footprints-match",
         [&](std::vector<char>& p) {
             put_at(p, footprint_runs_at + 11, static_cast<uint32_t>((old_bytes - before - gone + 6) / ADDR_VERTEX_STRIDE));
         },
         "Malformed merge sequence: addr_vertices.bin"},
        {"vertex-offset", [&](std::vector<char>& p) { put_at(p, record_at + ADDR_POINT_VERTEX_OFFSET_OFF, vertex_offset + 1); },
         "addr_points.bin record 59 has a vertex_offset of " + std::to_string(vertex_offset + 1) + ", not the " + std::to_string(vertex_offset) +
             " bytes of the footprints before it"},
        {"encoding", [&](std::vector<char>& p) { p[blob_at] = 7; }, "addr_points.bin record 59 has a footprint in encoding 7"},
        {"stride", [&](std::vector<char>& p) { put_at(p, footprints + 4, 3u); }, "Encoded family: unexpected section kind for a child stream"},
        {"length", [&](std::vector<char>& p) { put_at(p, footprints_len_at, footprints_len + (uint64_t{1} << 32)); },
         "Truncated patch: child section"},
        {"twice",
         [&](std::vector<char>& p) {
             const std::vector<char> both(raw.begin() + static_cast<std::ptrdiff_t>(points), raw.begin() + static_cast<std::ptrdiff_t>(footprints_end));
             p.insert(p.begin() + static_cast<std::ptrdiff_t>(footprints_end), both.begin(), both.end());
         },
         "old addr_points.bin opened twice"},
        {"new-size", [&](std::vector<char>& p) { put_at(p, points + 16, new_points + ADDR_POINT_STRIDE); },
         "Rebuilt addr_points.bin does not match the new build"},
        {"footprints-old-size", [&](std::vector<char>& p) { put_at(p, footprints + 8, old_bytes + 1); },
         "Old addr_vertices.bin is not the one the patch was made from (" + std::to_string(old_bytes) + " bytes, the patch expects " +
             std::to_string(old_bytes + 1) + ")"},
        {"footprints-new-size", [&](std::vector<char>& p) { put_at(p, footprints + 16, new_bytes + 1); },
         "Rebuilt addr_vertices.bin does not match the new build"},
        // A value for record 60 (from record 0), offset 0.
        {"offset-fixup",
         [&](std::vector<char>& p) {
             put_at(p, points + 28, 1u);
             put_at(p, points + 36, 2u);
             p.insert(p.begin() + static_cast<std::ptrdiff_t>(values_at), {60, 0});
         },
         "Malformed offset fixups"},
        // A fixup for the 128th slot, value unchanged: the 60 records have 120.
        {"string-fixup",
         [&](std::vector<char>& p) {
             put_at(p, values_at, 1u);
             put_at(p, values_at + 4, 2u);
             p.insert(p.begin() + static_cast<std::ptrdiff_t>(values_at + 8), {0x7F, 0});
         },
         "Malformed string fixups"},
    };
    for (const Case& e : cases) {
        std::vector<char> edited = raw;
        e.edit(edited);
        const std::string patch = c.dir(e.name + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));

        CHECK(not_refused(c, ALL_MODES, patch, e.name, e.refusal).empty());
    }
    CHECK(apply_modes(c, ALL_MODES, addr_replayed_is(footprints_in(addresses) - 1, 1)).bad.empty());
}

// The address replay copies an old blob only for a new record that is its old
// record at its point, whose footprint is that very blob's bytes. Four
// addresses and five days made by hand from the diff's patch of the unchanged
// set, its two address sections rebuilt, each giving the day's own files:
//   moved: the records REPLACEd a step of latitude away, their footprints one
//     MATCH of the old stream: each encoded from the old blob the replay is
//     at;
//   second: the records MATCHed, the first one's footprint MATCHed from the
//     second's old blob (its own deleted) and the second's inserted: the first
//     through the cursor, the replay not being at that blob yet;
//   ahead: every record inserted before the old ones are deleted, their
//     footprints one MATCH: all through the cursor;
//   behind: every old record deleted before the new ones are inserted: the
//     footprints of the three the replay passed read again, the last one's
//     from the blob it is at;
//   whole: the records MATCHed, or sent unchanged, over footprints sent
//     whole: all from the patch.
// And the footprints one MATCH six bytes longer than the old stream, or six
// shorter, is refused by name, as is a REPLACE run of a record more than the
// old file's.
TEST(the_address_replay_encodes_the_blobs_it_cannot_copy) {
    ScratchDir dir("lowmem-addr-nocopy");
    std::mt19937 rng(20261128);
    std::vector<Address> addresses;
    for (uint32_t i = 0; i < 4; i++) {
        Address a{47.0f + static_cast<float>(i) * 1e-4f, 8.0f, i, 0, 0, {}};
        a.blob = footprint_at(rng, a.lat, a.lng, 8);
        addresses.push_back(a);
    }
    BuildDir d;
    put_addresses(d, addresses);
    const LowMemCase c{dir.path(), "nocopy"};
    REQUIRE(prepare(c, d, d).empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    const std::vector<char>& records = d.at("addr_points.bin");
    const std::vector<char>& vertices = d.at("addr_vertices.bin");
    const uint32_t n = 4, b = static_cast<uint32_t>(addresses[0].blob.size()), v = static_cast<uint32_t>(vertices.size());
    REQUIRE(v == n * b);
    // The footprints' runs count units of this many bytes.
    const uint32_t unit = ADDR_VERTEX_STRIDE;
    std::vector<char> copies = copy_old_section(PatchFileId::ADDR_POINTS, records.size());
    const std::vector<char> copied_vertices = copy_old_section(PatchFileId::ADDR_VERTICES, vertices.size());
    copies.insert(copies.end(), copied_vertices.begin(), copied_vertices.end());
    const size_t at = find_once(raw, copies);
    REQUIRE(at != std::string::npos);
    // The patch with the address sections rebuilt (the points' a merge of
    // rec_ops, or sent unchanged when it has none; the footprints' a merge of
    // vert_ops, or sent whole when whole is given), and where it is written.
    auto rebuilt = [&](const char* name, const std::vector<char>& rec_ops, const std::vector<char>& vert_ops, const std::vector<char>* whole = nullptr) {
        std::vector<char> edited(raw.begin(), raw.begin() + static_cast<std::ptrdiff_t>(at));
        std::vector<char> points = copy_old_section(PatchFileId::ADDR_POINTS, records.size());
        if (!rec_ops.empty())
            points = merge_of(PatchFileId::ADDR_POINTS, static_cast<uint32_t>(ADDR_POINT_STRIDE), records.size(), records.size(), rec_ops);
        std::vector<char> footprints = merge_of(PatchFileId::ADDR_VERTICES, unit, v, v, vert_ops);
        if (whole) {
            footprints = section_head(PatchFileId::ADDR_VERTICES, 0, v, whole->size());
            append(footprints, 0u);
            append(footprints, static_cast<uint64_t>(whole->size()));
            footprints.insert(footprints.end(), whole->begin(), whole->end());
        }
        for (const auto& section : {points, footprints})
            edited.insert(edited.end(), section.begin(), section.end());
        edited.insert(edited.end(), raw.begin() + static_cast<std::ptrdiff_t>(at + copies.size()), raw.end());
        const std::string patch = c.dir(std::string(name) + ".gcpatch");
        REQUIRE(write_patch(c, patch, edited));
        return patch;
    };
    // The rebuilt patch applied to the old dir: the patched address files
    // against the day's own, and what the replay says it did.
    auto check = [&](const char* name, const std::vector<Address>& day, const std::vector<char>& rec_ops, const std::vector<char>& vert_ops,
                     const LogCheck& replayed, const std::vector<char>* whole = nullptr) {
        const int failures_before = gctest::failures();
        BuildDir day_d;
        put_addresses(day_d, day);
        const BuildDir want = gctest::as_written(day_d);
        const std::string patch = rebuilt(name, rec_ops, vert_ops, whole);
        for (const std::string& mode : ALL_MODES) {
            const std::string out = c.dir(std::string("out.") + name + "." + mode);
            std::filesystem::remove_all(out);
            const ToolRun r = run_tool(patch_words(c, mode, patch, out), c.log());
            CHECK(r.ok);
            for (const char* f : ADDRESS_FILES) CHECK(read_file(out + "/" + f) == want.at(f));
            CHECK_EQ(replayed(r.log), std::string());
        }
        if (gctest::failures() > failures_before) std::printf("    in the %s day\n", name);
    };
    std::vector<Address> moved = addresses, second = addresses;
    for (Address& a : moved) a.lat = std::nextafter(a.lat, 100.0f);
    second[0].blob = addresses[1].blob;
    BuildDir moved_d;
    put_addresses(moved_d, moved);
    std::vector<char> rec_ops, vert_ops;
    put_op(rec_ops, OP_REPLACE_RUN, n, moved_d.at("addr_points.bin").data(), ADDR_POINT_STRIDE);
    put_op(vert_ops, OP_MATCH_RUN, v / unit);
    check("moved", moved, rec_ops, vert_ops, addr_replayed_is(0, n, v));

    rec_ops.clear();
    vert_ops.clear();
    put_op(rec_ops, OP_MATCH_RUN, n);
    put_op(vert_ops, OP_DELETE_RUN, b / unit);
    put_op(vert_ops, OP_MATCH_RUN, b / unit);
    put_op(vert_ops, OP_INSERT_RUN, b / unit, vertices.data() + b, unit);
    put_op(vert_ops, OP_MATCH_RUN, 2 * b / unit);
    check("second", second, rec_ops, vert_ops, addr_replayed_is(2, 2, 0, 0, b));

    rec_ops.clear();
    vert_ops.clear();
    put_op(rec_ops, OP_INSERT_RUN, n, records.data(), ADDR_POINT_STRIDE);
    put_op(rec_ops, OP_DELETE_RUN, n);
    put_op(vert_ops, OP_MATCH_RUN, v / unit);
    check("ahead", addresses, rec_ops, vert_ops, addr_replayed_is(0, n, 0, 0, v));

    rec_ops.clear();
    put_op(rec_ops, OP_DELETE_RUN, n);
    put_op(rec_ops, OP_INSERT_RUN, n, records.data(), ADDR_POINT_STRIDE);
    check("behind", addresses, rec_ops, vert_ops, addr_replayed_is(0, n, b, 3 * b));

    rec_ops.clear();
    put_op(rec_ops, OP_MATCH_RUN, n);
    check("whole", addresses, rec_ops, {}, addr_replayed_is(0, n), &vertices);
    check("whole-under-unchanged", addresses, {}, {}, addr_replayed_is(0, n), &vertices);

    for (const auto& [name, bytes, refusal] :
         {std::tuple<const char*, uint32_t, const char*>{"long", v + 6, "addr_vertices.bin has bytes past its last record"},
          {"short", v - 6, "addr_vertices.bin ends before its records do"}}) {
        vert_ops.clear();
        put_op(vert_ops, OP_MATCH_RUN, bytes / unit);
        CHECK(not_refused(c, ALL_MODES, rebuilt(name, rec_ops, vert_ops), name, refusal).empty());
    }
    std::vector<char> one_more = records;
    one_more.insert(one_more.end(), records.begin(), records.begin() + static_cast<std::ptrdiff_t>(ADDR_POINT_STRIDE));
    rec_ops.clear();
    vert_ops.clear();
    put_op(rec_ops, OP_REPLACE_RUN, n + 1, one_more.data(), ADDR_POINT_STRIDE);
    put_op(vert_ops, OP_MATCH_RUN, v / unit);
    CHECK(not_refused(c, ALL_MODES, rebuilt("replace-past", rec_ops, vert_ops), "replace-past",
                      "Malformed merge sequence: addr_points.bin (a REPLACE run past the old file)")
              .empty());
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A blob is its footprint's under its record's point alone: a small one holds
// the bbox minimum as two residuals from where the point predicts it. So the
// old points the address replay copies blobs under are checked as the blobs
// are, by their file's hash. One address at (1, 1) whose footprint is one
// vertex at its bbox minimum, (1, 1): its blob is 08 01 00 00 00, and a patch
// that MATCHes the record and its footprint's 14 bytes copies it. With bit 6
// of the top byte of the block's latitude base flipped in the old
// addr_points.bin, the block is one the encoder writes, of a record at
// latitude +infinity; every length is as it was, and so is addr_vertices.bin.
// Under that point the encoder writes the bbox minimum in full and the strict
// decoder refuses the small blob. In every mode the patcher refuses the old
// addr_points.bin by its hash; the patch applies to the files as written.
TEST(the_address_replay_refuses_an_old_record_moved_under_a_blob_it_would_copy) {
    ScratchDir dir("lowmem-addr-point");
    Address address{1.0f, 1.0f, 0, 0, 0, std::vector<char>(addr_files::LOGICAL_HEADER + 4, 0)};
    put_at(address.blob, 2, 1.0f);
    put_at(address.blob, 6, 1.0f);
    BuildDir d;
    put_addresses(d, {address});
    const LowMemCase c{dir.path(), "point"};
    REQUIRE(prepare(c, d, d).empty());
    const std::string points_path = c.dir("old") + "/addr_points.bin";
    const std::vector<char> points = read_file(points_path), blobs = read_file(c.dir("old") + "/addr_vertices.bin");
    const std::vector<char> blob = {0x08, 0x01, 0x00, 0x00, 0x00};
    REQUIRE(blobs.size() == blob.size() + addr_files::VERTICES_FOOTER_BYTES && std::equal(blob.begin(), blob.end(), blobs.begin()));

    // The unchanged day's patch with its two address sections as merges: the
    // record a MATCH, its footprint's bytes a MATCH.
    const std::vector<char> raw = raw_patch(c);
    std::vector<char> copies = copy_old_section(PatchFileId::ADDR_POINTS, ADDR_POINT_STRIDE);
    const std::vector<char> copied_footprints = copy_old_section(PatchFileId::ADDR_VERTICES, address.blob.size());
    copies.insert(copies.end(), copied_footprints.begin(), copied_footprints.end());
    const size_t at = find_once(raw, copies);
    REQUIRE(at != std::string::npos);
    std::vector<char> record_ops, footprint_ops;
    put_op(record_ops, OP_MATCH_RUN, 1);
    put_op(footprint_ops, OP_MATCH_RUN, static_cast<uint32_t>(address.blob.size() / ADDR_VERTEX_STRIDE));
    std::vector<char> matched(raw.begin(), raw.begin() + static_cast<std::ptrdiff_t>(at));
    for (const std::vector<char>& section :
         {merge_of(PatchFileId::ADDR_POINTS, static_cast<uint32_t>(ADDR_POINT_STRIDE), ADDR_POINT_STRIDE, ADDR_POINT_STRIDE, record_ops),
          merge_of(PatchFileId::ADDR_VERTICES, ADDR_VERTEX_STRIDE, address.blob.size(), address.blob.size(), footprint_ops)})
        matched.insert(matched.end(), section.begin(), section.end());
    matched.insert(matched.end(), raw.begin() + static_cast<std::ptrdiff_t>(at + copies.size()), raw.end());
    REQUIRE(write_patch(c, c.patch(), matched));
    CHECK(apply_modes(c, ALL_MODES, addr_replayed_is(1, 0)).bad.empty());

    std::vector<char> moved = points;
    moved[addr_files::LAT_BASE_AT + 3] = static_cast<char>(moved[addr_files::LAT_BASE_AT + 3] ^ 0x40);
    addr_files::Block block;
    REQUIRE(addr_files::decode_block(0, reinterpret_cast<const uint8_t*>(moved.data()), moved.size(), 1, block) ==
            moved.size() - addr_files::POINTS_FOOTER_BYTES);
    REQUIRE(block.lat[0] == 0x7F800000u && block.lng[0] == coord_keys::bits_of(1.0f) && block.len[0] == blob.size());
    REQUIRE(write_file(points_path, moved));
    const std::vector<std::string> bad = not_refused(c, ALL_MODES, c.patch(), "moved", "Old addr_points.bin is not the one the patch was made from");
    CHECK(bad.empty());
    // What the whole-file decoder says of the files a mode wrote.
    if (!bad.empty())
        for (const std::string& mode : ALL_MODES) {
            std::map<std::string, std::vector<char>> out;
            for (const char* f : ADDRESS_FILES) out[f] = read_file(c.dir("moved.out." + mode) + "/" + f);
            std::string refusal;
            try {
                auto u8 = [&](const char* f) { return reinterpret_cast<const uint8_t*>(out[f].data()); };
                addr_files::decode(u8("addr_points.bin"), out["addr_points.bin"].size(), u8("addr_vertices.bin"), out["addr_vertices.bin"].size(),
                                   u8("addr_index.bin"), out["addr_index.bin"].size());
            } catch (const std::runtime_error& e) {
                refusal = e.what();
            }
            std::printf("    %s: the patched address files decode: %s\n", mode.c_str(), refusal.empty() ? "yes" : refusal.c_str());
        }
    REQUIRE(write_file(points_path, points));
    CHECK(apply_modes(c, ALL_MODES, addr_replayed_is(1, 0)).bad.empty());
}

// A section sent as unchanged says the new file is the old one, so its two
// sizes are one. In every mode the patcher refuses, by the file's name, such
// a section whose new size is not its old one, though the old file has the
// old size and the client list holds the new dir's files: each section of a
// day that changes nothing (the records' and the child's of a way pair, of
// the address files and of the POI files, the place files' and a side
// file's, families the patcher copies as their files sit), and that of a file
// of no family (the arcs' index). The patches as made apply.
TEST(patch_refuses_a_section_sent_as_unchanged_whose_new_size_is_not_its_old_one) {
    ScratchDir dir("lowmem-copy-old-sizes");
    std::mt19937 rng(20261115);
    BuildDir families = every_family(rng), arcs;
    put_arc_slots(arcs, arc_slots(40, 30, 12));
    CellLists cells;
    for (uint32_t k = 0; k < 12; k++) cells[pair_cell(k)] = {k};
    put_cells(arcs, "admin", cells);
    struct Section {
        PatchFileId file;
        uint64_t size, more;  // its bytes, and those of one of its records
    };
    auto logical = [&](const char* name) { return static_cast<uint64_t>(families.at(name).size()); };
    const std::vector<Section> of_families = {
        {PatchFileId::STREET_WAYS, logical("street_ways.bin"), WAY_HEADER_STRIDE},
        {PatchFileId::STREET_NODES, logical("street_nodes.bin"), 8},
        {PatchFileId::INTERP_WAYS, logical("interp_ways.bin"), INTERP_WAY_STRIDE},
        {PatchFileId::INTERP_NODES, logical("interp_nodes.bin"), 8},
        {PatchFileId::ADDR_POINTS, logical("addr_points.bin"), ADDR_POINT_STRIDE},
        {PatchFileId::ADDR_VERTICES, logical("addr_vertices.bin"), ADDR_VERTEX_STRIDE},
        {PatchFileId::POI_RECORDS, logical("poi_records.bin"), POI_RECORD_STRIDE},
        {PatchFileId::POI_VERTICES, logical("poi_vertices.bin"), 1},
        {PatchFileId::PLACE_NODES, place_files::LOGICAL_STRIDE * 20, place_files::LOGICAL_STRIDE},
        {PatchFileId::WAY_PARENTS, logical("way_parents.bin"), 4},
        {PatchFileId::ADDR_POSTCODES, logical("addr_postcodes.bin"), 4},
    };
    const std::vector<Section> of_arcs = {{PatchFileId::ARC_INDEX, arcs.at("arc_index.bin").size(), ARC_INDEX_STRIDE}};
    for (const auto& [name, d, sections] : {std::tuple<const char*, const BuildDir&, const std::vector<Section>&>{"families", families, of_families},
                                            {"arcs", arcs, of_arcs}}) {
        const LowMemCase c{dir.path(), name};
        REQUIRE(prepare(c, d, d).empty());
        CHECK(apply_modes(c, ALL_MODES).bad.empty());
        const std::vector<char> raw = raw_patch(c);
        REQUIRE(!raw.empty());
        for (const Section& s : sections) {
            const std::string file = patch_file_names[static_cast<uint32_t>(s.file)];
            const size_t at = find_once(raw, copy_old_section(s.file, s.size));
            if (at == std::string::npos) std::printf("    no unchanged section of %s\n", file.c_str());
            REQUIRE(at != std::string::npos);
            std::vector<char> longer = raw;
            put_at(longer, at + 16, s.size + s.more);
            const std::string patch = c.dir(file + ".gcpatch");
            REQUIRE(write_patch(c, patch, longer));

            CHECK(not_refused(c, ALL_MODES, patch, file,
                              "Malformed copy-old section: " + file + " is " + std::to_string(s.size) + " bytes old and " +
                                  std::to_string(s.size + s.more) + " new")
                      .empty());
        }
    }
}

namespace {

// The patcher's heap peak as it refuses `patch` in the mode: none without a
// heap shim, or when the patch applied.
std::optional<uint64_t> refused_heap_peak(const LowMemCase& c, const std::string& mode, const std::string& patch, const std::string& name) {
    const std::string out = c.dir(name + ".heap." + mode);
    std::filesystem::remove_all(out);
    const auto [ok, peak] = gctest::run_with_heap_peak(patch_words(c, mode, patch, out), c.log());
    return ok ? std::nullopt : peak;
}

}  // namespace

// A family's encoder takes a record once its child bytes are in and child
// bytes once their record is, so bytes of one file that no unit will take are
// refused as they reach it, not held until the family's files close. A dir
// whose street pair, address files and POI files hold no record, and patches
// of an unchanged day with megabytes of bytes no record owns: each family's
// child file replaced whole after its records' section, of no record, closed
// the records' file; the street ways' section given records of one node each
// and no node; and the street nodes replaced whole before the ways' section.
// In every mode the patcher refuses each as the first bytes come (the bytes
// of a file that is closed for good) or once one file runs a megabyte ahead of
// the other (while both may still come), and its heap peak is the same for 4
// MiB of them as for 40. The patch as made applies.
TEST(patch_refuses_bytes_no_record_of_a_family_owns_as_they_come) {
    ScratchDir dir("lowmem-unowned");
    BuildDir d;
    for (const char* file : {"street_ways.bin", "street_nodes.bin", "addr_points.bin", "addr_vertices.bin", "poi_records.bin", "poi_vertices.bin"})
        d[file] = {};
    const LowMemCase c{dir.path(), "empty"};
    REQUIRE(prepare(c, d, d).empty());
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
    const std::vector<char> raw = raw_patch(c);
    REQUIRE(!raw.empty());
    // Where the patch holds a section, and where the section ends.
    auto found = [&](const std::vector<char>& section) {
        const size_t at = find_once(raw, section);
        return std::pair<size_t, size_t>{at, at == std::string::npos ? at : at + section.size()};
    };
    // The sections of the files without a byte on either day: merges without
    // an op, and the footprints' a full replacement by nothing.
    const auto ways = found(merge_of(PatchFileId::STREET_WAYS, static_cast<uint32_t>(WAY_HEADER_STRIDE), 0, 0, {})),
               nodes = found(merge_of(PatchFileId::STREET_NODES, 8, 0, 0, {})), footprints = found(replacement_of(PatchFileId::ADDR_VERTICES, 0, {})),
               vertices = found(merge_of(PatchFileId::POI_VERTICES, 1, 0, 0, {}));
    REQUIRE(ways.first != std::string::npos && nodes.first == ways.second && footprints.first != std::string::npos &&
            vertices.first != std::string::npos);
    // The patch with [at.first, at.second) replaced by section.
    auto with = [&](std::pair<size_t, size_t> at, const std::vector<char>& section) {
        std::vector<char> edited(raw.begin(), raw.begin() + static_cast<std::ptrdiff_t>(at.first));
        edited.insert(edited.end(), section.begin(), section.end());
        edited.insert(edited.end(), raw.begin() + static_cast<std::ptrdiff_t>(at.second), raw.end());
        return edited;
    };
    auto whole = [](PatchFileId fid, uint32_t mib) { return replacement_of(fid, 0, std::vector<char>(size_t{mib} << 20, 0)); };
    struct Unowned {
        std::string name;
        std::function<std::vector<char>(uint32_t mib)> patch;
        std::string refusal;
    };
    const Unowned cases[] = {
        {"nodes", [&](uint32_t mib) { return with(nodes, whole(PatchFileId::STREET_NODES, mib)); }, "streets: 65536 bytes left over on input 1"},
        {"footprints", [&](uint32_t mib) { return with(footprints, whole(PatchFileId::ADDR_VERTICES, mib)); },
         "addr: 65536 bytes left over on input 1"},
        {"vertices", [&](uint32_t mib) { return with(vertices, whole(PatchFileId::POI_VERTICES, mib)); }, "poi: 65536 bytes left over on input 1"},
        {"ways",
         [&](uint32_t mib) {
             const uint32_t n = (mib << 20) / static_cast<uint32_t>(WAY_HEADER_STRIDE);
             std::vector<char> records;
             for (uint32_t w = 0; w < n; w++) append(records, WayHeader{w, 1, 0, 0});
             std::vector<char> ops;
             put_op(ops, OP_INSERT_RUN, n, records.data(), WAY_HEADER_STRIDE);
             return with(ways, merge_of(PatchFileId::STREET_WAYS, static_cast<uint32_t>(WAY_HEADER_STRIDE), 0, 0, ops));
         },
         "streets: input 0 runs 1114112 bytes ahead of input 1"},
        {"nodes-first",
         [&](uint32_t mib) {
             std::vector<char> both = whole(PatchFileId::STREET_NODES, mib);
             both.insert(both.end(), raw.begin() + static_cast<std::ptrdiff_t>(ways.first), raw.begin() + static_cast<std::ptrdiff_t>(ways.second));
             return with({ways.first, nodes.second}, both);
         },
         "streets: input 1 runs 1114112 bytes ahead of input 0"},
    };
    constexpr uint32_t MIB = 4, FACTOR = 10, THREADS_RUNS = 3;
    for (const Unowned& u : cases) {
        const int failures_before = gctest::failures();
        const std::string patch = c.dir(u.name + ".gcpatch"), larger = c.dir(u.name + ".larger.gcpatch");
        REQUIRE(write_patch(c, patch, u.patch(MIB)));
        REQUIRE(write_patch(c, larger, u.patch(MIB * FACTOR)));

        CHECK(not_refused(c, ALL_MODES, patch, u.name, u.refusal).empty());
        CHECK(not_refused(c, ALL_MODES, larger, u.name + ".larger", u.refusal).empty());
        if (gctest::heap_peak_shim()) {
            for (const std::string& mode : ALL_MODES) {
                std::vector<uint64_t> peaks[2];
                for (uint32_t run = 0; run < (mode == "threads" ? THREADS_RUNS : 1); run++)
                    for (size_t s = 0; s < 2; s++)
                        if (const auto peak = refused_heap_peak(c, mode, s == 0 ? patch : larger, u.name)) peaks[s].push_back(*peak);
                REQUIRE(!peaks[0].empty() && !peaks[1].empty());
                const std::string grew = peak_grew(mode, peaks[0], peaks[1], MIB, FACTOR);
                if (!grew.empty()) std::printf("    %s\n", grew.c_str());
                CHECK(grew.empty());
            }
        } else {
            std::printf("    SKIP heap peaks of %s: no heap shim under the sanitizers\n", u.name.c_str());
        }
        if (gctest::failures() > failures_before) std::printf("    in: %s\n", u.name.c_str());
    }
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}

// A reader that knows an old file's decoded size reads no byte past it, so a
// decoder makes the checks of how its files end as it decodes their last
// unit. Old files with a byte between the last unit and what follows it,
// which a day's patch reads to their end, are refused in every mode in the
// whole-file decoder's words: a side file alone (a sparse delta) and beside
// its ways (a parallel delta along ways sent unchanged), of 100 values, of
// 2,048 (a read ahead of the decoder's) and of 4,096, a byte between its
// blocks and its table; POI files whose records stay while a polygon changes
// (the records sent unchanged replay with their vertices through the codecs),
// a byte between the last block and the pad; and place files with a place
// renamed, a byte between the blocks and the table. Each patch applies to the
// files as written.
TEST(patch_refuses_old_files_with_a_byte_between_their_last_unit_and_what_follows_it) {
    ScratchDir dir("lowmem-file-ends");
    std::mt19937 rng(20261115);
    struct Case {
        std::string name;
        BuildDir old_d, new_d;
        std::string file;
        size_t before;  // the byte goes in this many bytes from the file's end
        std::string refusal;
    };
    std::vector<Case> cases;
    const std::string side = "Malformed way_parents.bin: bytes between its blocks and its table";
    for (const uint32_t n : {100u, 2048u, 4096u}) {
        std::vector<char> parents, changed;
        for (uint32_t v = 0; v < n; v++) {
            append(parents, v % 700);
            append(changed, v % 700 + (v == n / 2));
        }
        const size_t table = size_t{4} * ((n + 63) / 64) + 8;
        cases.push_back({"side-" + std::to_string(n), {{"way_parents.bin", parents}}, {{"way_parents.bin", changed}}, "way_parents.bin", table, side});
        BuildDir with_ways, meta;
        std::vector<uint32_t> names(n);
        for (uint32_t w = 0; w < n; w++) names[w] = w % 9;
        put_named_ways(with_ways, meta, names);
        BuildDir with_ways_changed = with_ways;
        with_ways["way_parents.bin"] = parents;
        with_ways_changed["way_parents.bin"] = changed;
        cases.push_back({"side-beside-" + std::to_string(n), with_ways, with_ways_changed, "way_parents.bin", table, side});
    }
    {
        const std::vector<Poi> pois = pois_of(rng, 300, 0, true);
        std::vector<Poi> edited = pois;
        std::fill(edited[100].block.begin() + 10, edited[100].block.end(), 0);
        BuildDir old_d, new_d;
        put_pois(old_d, pois);
        put_pois(new_d, edited);
        // Records sent unchanged over one block that changes: the old POI files are held to
        // their hashes before the codecs read them.
        cases.push_back({"poi", old_d, new_d, "poi_vertices.bin", poi_files::VERTEX_PAD, "Old poi_vertices.bin is not the one the patch was made from"});
    }
    {
        std::vector<PlaceNode> places = some_places(20);
        BuildDir old_d, new_d;
        put_places(old_d, places);
        places[5].name_id = 1000;
        put_places(new_d, places);
        // 15 visible nodes: one block.
        cases.push_back({"places", old_d, new_d, "place_nodes.bin", place_files::TABLE_ENTRY_BYTES + place_files::FOOTER_BYTES,
                         "Malformed place files: place_nodes.bin has bytes between its blocks and its table"});
    }
    for (const Case& e : cases) {
        const int failures_before = gctest::failures();
        const LowMemCase c{dir.path(), e.name};
        REQUIRE(prepare(c, e.old_d, e.new_d).empty());
        CHECK(apply_modes(c, ALL_MODES).bad.empty());
        const std::string path = c.dir("old") + "/" + e.file;
        const std::vector<char> written = read_file(path);
        REQUIRE(written.size() > e.before);
        std::vector<char> longer = written;
        longer.insert(longer.end() - static_cast<std::ptrdiff_t>(e.before), 0);
        REQUIRE(write_file(path, longer));

        CHECK(not_refused(c, ALL_MODES, c.patch(), "longer", e.refusal).empty());

        REQUIRE(write_file(path, written));
        CHECK(apply_modes(c, ALL_MODES).bad.empty());
        if (gctest::failures() > failures_before) std::printf("    in: %s\n", e.name.c_str());
    }
}

// A V2 block holds its polygon's bbox minimum as steps from its record's lat
// and lng, and a MATCH record is the old row's, so the old records the POI
// replay copies blocks under are checked as the blocks are, by their file's
// hash. 30 POIs, 20 with a polygon, and a patch that MATCHes every record and
// every vertex byte: each block is copied. With one bit of a row's latitude
// flipped in the old poi_records.bin, or a byte of a combo of its dictionary
// changed, the files are still ones the encoder writes (poi_files::decode
// takes them) and poi_vertices.bin is as it was: the patched files would hold
// a POI and its polygon the new day does not. In every mode the patcher
// refuses the old poi_records.bin by its hash; the patch applies to the files
// as written.
TEST(the_poi_replay_refuses_an_old_record_moved_under_a_block_it_would_copy) {
    ScratchDir dir("lowmem-poi-point");
    std::mt19937 rng(20261115);
    BuildDir d;
    put_pois(d, pois_of(rng, 30, 0, true));
    const LowMemCase c{dir.path(), "point"};
    REQUIRE(prepare(c, d, d).empty());
    const std::vector<char>& records = d.at("poi_records.bin");
    const std::vector<char>& vertices = d.at("poi_vertices.bin");

    // The unchanged day's patch with its two POI sections as merges: every
    // record a MATCH, every vertex byte a MATCH.
    const std::vector<char> raw = raw_patch(c);
    std::vector<char> copies = copy_old_section(PatchFileId::POI_RECORDS, records.size());
    const std::vector<char> copied_vertices = copy_old_section(PatchFileId::POI_VERTICES, vertices.size());
    copies.insert(copies.end(), copied_vertices.begin(), copied_vertices.end());
    const size_t at = find_once(raw, copies);
    REQUIRE(at != std::string::npos);
    std::vector<char> record_ops, vertex_ops;
    put_op(record_ops, OP_MATCH_RUN, 30);
    put_op(vertex_ops, OP_MATCH_RUN, static_cast<uint32_t>(vertices.size()));
    std::vector<char> matched(raw.begin(), raw.begin() + static_cast<std::ptrdiff_t>(at));
    for (const std::vector<char>& section :
         {merge_of(PatchFileId::POI_RECORDS, static_cast<uint32_t>(POI_RECORD_STRIDE), records.size(), records.size(), record_ops),
          merge_of(PatchFileId::POI_VERTICES, 1, vertices.size(), vertices.size(), vertex_ops)})
        matched.insert(matched.end(), section.begin(), section.end());
    matched.insert(matched.end(), raw.begin() + static_cast<std::ptrdiff_t>(at + copies.size()), raw.end());
    REQUIRE(write_patch(c, c.patch(), matched));
    const LogCheck copied = [](const std::string& log) {
        return log.find("replayed with poi_records.bin: blocks 20 copied, 0 encoded") == std::string::npos ? std::string("not 20 blocks copied")
                                                                                                             : std::string();
    };
    CHECK(apply_modes(c, ALL_MODES, copied).bad.empty());

    const std::string path = c.dir("old") + "/poi_records.bin";
    const std::vector<char> written = read_file(path), blocks = read_file(c.dir("old") + "/poi_vertices.bin");
    // Record 1 has a polygon; the combos follow the 30 rows.
    std::vector<char> moved = written, another_combo = written;
    moved[1 * poi_files::ROW_BYTES + poi_files::ROW_LAT] ^= 1;
    another_combo[30 * poi_files::ROW_BYTES + 3] ^= 5;
    for (const auto& [name, damaged] : {std::pair<const char*, const std::vector<char>&>{"moved", moved}, {"combo", another_combo}}) {
        std::string strict;
        try {
            poi_files::decode(reinterpret_cast<const uint8_t*>(damaged.data()), damaged.size(), reinterpret_cast<const uint8_t*>(blocks.data()),
                              blocks.size());
        } catch (const std::runtime_error& e) {
            strict = e.what();
        }
        if (!strict.empty()) std::printf("    %s: %s\n", name, strict.c_str());
        REQUIRE(strict.empty());
        REQUIRE(write_file(path, damaged));
        const std::vector<std::string> bad = not_refused(c, ALL_MODES, c.patch(), name, "Old poi_records.bin is not the one the patch was made from");
        CHECK(bad.empty());
        REQUIRE(write_file(path, written));
    }
    CHECK(apply_modes(c, ALL_MODES, copied).bad.empty());
}

TEST(patch_holds_the_old_poi_files_to_their_hashes_when_only_the_blocks_change) {
    ScratchDir dir("lowmem-poi-unchanged-records");
    std::mt19937 rng(20261115);
    BuildDir d;
    put_pois(d, pois_of(rng, 30, 0, true));
    const LowMemCase c{dir.path(), "records"};
    REQUIRE(prepare(c, d, d).empty());
    const std::vector<char>& records = d.at("poi_records.bin");
    const std::vector<char>& vertices = d.at("poi_vertices.bin");

    // The unchanged day's patch with the records still sent unchanged and the
    // vertices as a merge, every byte a MATCH: the codecs replay the family.
    const std::vector<char> raw = raw_patch(c);
    const std::vector<char> copied_records = copy_old_section(PatchFileId::POI_RECORDS, records.size());
    const std::vector<char> copied_vertices = copy_old_section(PatchFileId::POI_VERTICES, vertices.size());
    std::vector<char> copies = copied_records;
    copies.insert(copies.end(), copied_vertices.begin(), copied_vertices.end());
    const size_t at = find_once(raw, copies);
    REQUIRE(at != std::string::npos);
    std::vector<char> vertex_ops;
    put_op(vertex_ops, OP_MATCH_RUN, static_cast<uint32_t>(vertices.size()));
    const std::vector<char> section = merge_of(PatchFileId::POI_VERTICES, 1, vertices.size(), vertices.size(), vertex_ops);
    std::vector<char> merged(raw.begin(), raw.begin() + static_cast<std::ptrdiff_t>(at));
    merged.insert(merged.end(), copied_records.begin(), copied_records.end());
    merged.insert(merged.end(), section.begin(), section.end());
    merged.insert(merged.end(), raw.begin() + static_cast<std::ptrdiff_t>(at + copies.size()), raw.end());
    REQUIRE(write_patch(c, c.patch(), merged));
    CHECK(apply_modes(c, ALL_MODES).bad.empty());

    // A combo of the records' dictionary changed (the file still decodes), then
    // a byte of the blocks: each file is refused by its hash, in every mode.
    const std::string records_path = c.dir("old") + "/poi_records.bin", blocks_path = c.dir("old") + "/poi_vertices.bin";
    const std::vector<char> written = read_file(records_path), blocks = read_file(blocks_path);
    std::vector<char> another_combo = written, another_block = blocks;
    another_combo[30 * poi_files::ROW_BYTES + 3] ^= 5;
    another_block[blocks.size() / 2] ^= 1;
    REQUIRE(write_file(records_path, another_combo));
    CHECK(not_refused(c, ALL_MODES, c.patch(), "combo", "Old poi_records.bin is not the one the patch was made from").empty());
    REQUIRE(write_file(records_path, written));
    REQUIRE(write_file(blocks_path, another_block));
    CHECK(not_refused(c, ALL_MODES, c.patch(), "block", "Old poi_vertices.bin is not the one the patch was made from").empty());
    REQUIRE(write_file(blocks_path, blocks));
    CHECK(apply_modes(c, ALL_MODES).bad.empty());
}
