// geocoder-patch's low-memory mode end to end: small synthetic today's-format
// dirs, the geocoder-diff patch between them, geocoder-gc's encoding of the
// old dir patched (in sync and --threads) and the new encoded set compared
// byte for byte with geocoder-gc's encoding of the new dir. Then patches and
// command lines the patcher must refuse.
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <functional>
#include <random>
#include <regex>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "merge_sequence.h"
#include "patch_format.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"

namespace {

using gctest::BuildDir;
using gctest::read_dir;
using gctest::run;
using gctest::write_dir;

constexpr uint32_t NO = 0xFFFFFFFFu;
// The most bytes a family's decoder may hold queued while the replay skips
// old bytes (a deleted run, replaced children or parents).
constexpr uint64_t DECODER_HOLD = 1 << 20;

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
// matches) and its footprint: none, or a vertex block as a POI's.
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
        append(points, a.block.empty() ? 0u : static_cast<uint32_t>((a.block.size() - 10) / 4));
        verts.insert(verts.end(), a.block.begin(), a.block.end());
    }
    d["addr_points.bin"] = points;
    d["addr_vertices.bin"] = verts;
}

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
        append(geo, uint64_t(k + 1) << 27);
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

// Street ways of two nodes each, named by names, in one geo cell; with the
// identity sidecar (record i is entity i) for a diff's --identity.
void put_named_ways(BuildDir& d, BuildDir& meta, const std::vector<uint32_t>& names) {
    put_geo(d, {{0, {0}}}, static_cast<uint32_t>(names.size()));
    std::vector<char> ways, nodes, slots;
    for (uint32_t w = 0; w < names.size(); w++) {
        append(ways, 2 * w);
        append(ways, uint16_t{2});
        append(ways, uint16_t{0});
        append(ways, names[w]);
        for (const double v : {47.0, 8.0, 47.001, 8.001}) append(nodes, static_cast<float>(v + w * 1e-5));
        append(slots, w);
    }
    d["street_ways.bin"] = ways;
    d["street_nodes.bin"] = nodes;
    meta["street.slot.u32"] = slots;
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

// The family's decoder queue (and --threads ring) peaks, from the patcher's
// report line, all at most limit.
LogCheck decoder_peaks_at_most(const std::string& family, uint64_t limit) {
    return [family, limit](const std::string& log) -> std::string {
        const std::string head = "decode " + family + ":";
        const size_t at = log.find(head);
        if (at == std::string::npos) return "no " + head + " line";
        const std::string line = log.substr(at, log.find('\n', at) - at);
        uint64_t most = 0;
        static const std::regex peaks("peaks ((?:\\d+ )+)B");
        for (std::sregex_iterator it(line.begin(), line.end(), peaks), end; it != end; ++it) {
            const std::string values = (*it)[1];
            for (size_t p = 0; p < values.size();) {
                size_t q = values.find(' ', p);
                most = std::max<uint64_t>(most, std::stoull(values.substr(p, q - p)));
                p = q + 1;
            }
        }
        return most > limit ? "decoder held " + std::to_string(most) + " B: " + line : "";
    };
}

// The old and new dirs of a case under root/<name>.{old,new}, their encoded
// sets, the patch between them, and what applying it gave.
struct LowMemCase {
    std::string root, name;
    std::string dir(const std::string& kind) const { return root + "/" + name + "." + kind; }
    std::string patch() const { return dir("gcpatch"); }
    std::string log() const { return dir("log"); }
};

// Writes old and new (and their sidecars when given), diffs them, and
// encodes both with geocoder-gc. Empty on success, else what failed.
std::string prepare(const LowMemCase& c, const BuildDir& old_d, const BuildDir& new_d, const BuildDir& old_meta = {},
                    const BuildDir& new_meta = {}) {
    if (!write_dir(c.dir("old"), old_d) || !write_dir(c.dir("new"), new_d)) return "fixture not written";
    std::vector<std::string> diff = {GEOCODER_DIFF_BIN, c.dir("old"), c.dir("new"), "-o", c.patch()};
    if (!old_meta.empty()) {
        if (!write_dir(c.dir("old-meta"), old_meta) || !write_dir(c.dir("new-meta"), new_meta)) return "sidecars not written";
        diff.insert(diff.end(), {"--identity", c.dir("old-meta"), c.dir("new-meta")});
    }
    for (const auto& words : {diff, {GEOCODER_GC_BIN, "encode", c.dir("old"), c.dir("old.enc")},
                              {GEOCODER_GC_BIN, "encode", c.dir("new"), c.dir("new.enc")}}) {
        auto r = run_tool(words, c.log());
        if (!r.ok) return words[0] + " failed: " + r.log;
    }
    return "";
}

// The patch applied to the old encoded set, in sync and --threads, compared
// with the new encoded set; check runs on each log. Every mismatch, printed.
std::vector<std::string> apply_both_modes(const LowMemCase& c, const LogCheck& check = {}) {
    std::vector<std::string> bad;
    for (const bool threads : {false, true}) {
        const std::string mode = threads ? "threads" : "sync", out = c.dir("out." + mode);
        std::vector<std::string> words = {GEOCODER_PATCH_BIN, c.dir("old.enc"), c.patch(), "-o", out};
        if (threads) words.push_back("--threads");
        auto r = run_tool(words, c.log());
        if (!r.ok) {
            bad.push_back(mode + ": patch failed: " + r.log);
            continue;
        }
        if (read_dir(out) != read_dir(c.dir("new.enc"))) bad.push_back(mode + ": the new set is not gc's encoding of the new dir");
        if (check)
            if (std::string e = check(r.log); !e.empty()) bad.push_back(mode + ": " + e);
    }
    for (const auto& b : bad) std::printf("    %s: %s\n", c.name.c_str(), b.c_str());
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

// An encoded set whose addresses are in geocoder-gc's pax170 layout, which
// the address codec doesn't stream, is refused before anything is written
// (meta.json, an inline client file, was written first).
TEST(patch_lowmem_refuses_a_set_in_the_pax170_address_layout) {
    ScratchDir dir("lowmem-pax");
    std::vector<GeoRow> rows;
    for (uint32_t k = 0; k < 50; k++) rows.push_back({k, {k % 10}});
    BuildDir old_d, new_d;
    put_geo(old_d, rows);
    put_geo(new_d, {rows.begin(), rows.begin() + 40});
    for (BuildDir* d : {&old_d, &new_d}) {
        (*d)["addr_points.bin"] = (*d)["addr_vertices.bin"] = {};
        (*d)["meta.json"] = {'{', '}'};
    }
    const LowMemCase c{dir.path(), "pax"};
    REQUIRE(prepare(c, old_d, new_d).empty());
    REQUIRE(run_tool({GEOCODER_GC_BIN, "encode", c.dir("old"), c.dir("old.pax"), "--addr", "pax170"}, c.log()).ok);

    auto r = run_tool({GEOCODER_PATCH_BIN, c.dir("old.pax"), c.patch(), "-o", c.dir("out")}, c.log());

    CHECK(!r.ok);
    CHECK(logged(r, "the encoded set's addresses are in the pax170 layout; only col sets are patched"));
    CHECK(read_dir(c.dir("out")).empty());
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
    REQUIRE(run({"zstd", "-d", "-q", "-f", c.patch(), "-o", c.dir("raw")}, c.log()));
    const std::vector<char> raw = read_file(c.dir("raw"));
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
        REQUIRE(write_file(patch + ".raw", longer));
        REQUIRE(run({"zstd", "-q", "-f", patch + ".raw", "-o", patch}, c.log()));
        for (const bool threads : {false, true}) {
            std::vector<std::string> words = {GEOCODER_PATCH_BIN, c.dir("old.enc"), patch, "-o", c.dir(e.name + ".out" + (threads ? ".t" : ""))};
            if (threads) words.push_back("--threads");

            auto r = run_tool(words, c.log());

            CHECK(!r.ok);
            CHECK(logged(r, e.refusal));
        }
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
    REQUIRE(run({"zstd", "-d", "-q", "-f", c.patch(), "-o", c.dir("raw")}, c.log()));
    const std::vector<char> raw = read_file(c.dir("raw"));
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
        REQUIRE(write_file(patch + ".raw", edited));
        REQUIRE(run({"zstd", "-q", "-f", patch + ".raw", "-o", patch}, c.log()));
        for (const bool threads : {false, true}) {
            std::vector<std::string> words = {GEOCODER_PATCH_BIN, c.dir("old.enc"), patch, "-o", c.dir(name + ".out" + (threads ? ".t" : ""))};
            if (threads) words.push_back("--threads");

            auto r = run_tool(words, c.log());

            CHECK(!r.ok);
            CHECK(logged(r, "Malformed record stride: " + name));
        }
    }
    CHECK(apply_both_modes(c).empty());
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
// namespaces are unavailable.
TEST(patch_fails_cleanly_on_a_tmpdir_too_small_for_the_patch) {
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
    // The patcher's output goes to <name>.err; <name>.out gets its exit
    // status, then what the tmpfs still holds after it.
    const std::string script = "mount -t tmpfs -o size=\"$1\" tmpfs \"$2\" || exit 99; "
                               "TMPDIR=\"$2\" \"$3\" \"$4\" \"$5\" -o \"$6\" 2> \"$7\"; echo \"rc=$?\"; ls -A \"$2\"";
    struct Run {
        bool namespaced;
        std::string status, err, left;
    };
    auto on_small_tmpdir = [&](const std::string& size, const std::string& patch, const std::string& name) {
        const std::string mnt = root + "/" + name + ".tmpfs", out = root + "/" + name + ".out";
        std::filesystem::create_directories(mnt);
        Run r;
        r.namespaced = std::system(("unshare --map-root-user --mount sh -c " + gctest::quoted(script) + " sh " +
                                    gctest::quoted(size) + " " + gctest::quoted(mnt) + " " + gctest::quoted(GEOCODER_PATCH_BIN) +
                                    " " + gctest::quoted(root + "/plain") + " " + gctest::quoted(patch) + " " +
                                    gctest::quoted(root + "/" + name) + " " + gctest::quoted(root + "/" + name + ".err") +
                                    " > " + gctest::quoted(out) + " 2>&1")
                                       .c_str()) == 0;
        auto text = read_file(out);
        const std::string said(text.begin(), text.end());
        r.status = said.substr(0, said.find('\n'));
        r.left = said.find('\n') == std::string::npos ? "" : said.substr(said.find('\n') + 1);
        auto err = read_file(root + "/" + name + ".err");
        r.err.assign(err.begin(), err.end());
        if (!r.namespaced) std::printf("    SKIP %s: no private tmpfs (%s)\n", name.c_str(), said.substr(0, 200).c_str());
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
