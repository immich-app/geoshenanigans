// geocoder-patch's low-memory mode end to end: small synthetic today's-format
// dirs, the geocoder-diff patch between them, geocoder-gc's encoding of the
// old dir patched (in sync and --threads) and the new encoded set compared
// byte for byte with geocoder-gc's encoding of the new dir. Then the
// patcher's heap peak, in every mode, as a case grows tenfold; and patches
// and command lines the patcher must refuse.
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <map>
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
// The most the patcher's heap peak may grow when a heap test's case grows
// tenfold in the one structure it varies, which the patcher reads in place.
constexpr uint64_t HEAP_GROWTH_SLACK = 256 << 10;

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

// The POI cell index of lists (cell id → POI ids), into d, as the builder
// writes it.
void put_poi_cells(BuildDir& d, const CellLists& lists) {
    auto [cells, entries] = write_cell_lists(lists);
    d["poi_cells.bin"] = cells;
    d["poi_entries.bin"] = entries;
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
// report line, all at most limit. A line without peaks, or with a "peaks"
// that isn't a list of numbers, fails: a changed report format can't pass as
// a decoder that held nothing.
LogCheck decoder_peaks_at_most(const std::string& family, uint64_t limit) {
    return [family, limit](const std::string& log) -> std::string {
        const std::string head = "decode " + family + ":";
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
// encoded set ("sync", "threads") into out.
std::vector<std::string> patch_words(const LowMemCase& c, const std::string& mode, const std::string& patch, const std::string& out) {
    std::vector<std::string> words = {GEOCODER_PATCH_BIN, c.dir(mode == "plain" ? "old" : "old.enc"), patch, "-o", out};
    if (mode == "threads") words.push_back("--threads");
    return words;
}

// patch (an edit of c's, named name) applied in each of modes: every mode in
// which the patcher doesn't refuse it with `refusal`, and what it did instead,
// printed.
std::vector<std::string> not_refused(const LowMemCase& c, const std::vector<std::string>& modes, const std::string& patch,
                                     const std::string& name, const std::string& refusal) {
    std::vector<std::string> bad;
    for (const std::string& mode : modes) {
        const ToolRun r = run_tool(patch_words(c, mode, patch, c.dir(name + ".out." + mode)), c.log());
        if (!r.ok && logged(r, refusal)) continue;
        bad.push_back(name + " (" + mode + "): " + (r.ok ? "applied" : r.log));
        std::printf("    %s\n", bad.back().c_str());
    }
    return bad;
}

// A case's patch applied in some modes: every mismatch, and the patcher's
// heap peak by mode where the heap shim measured one.
struct Applied {
    std::vector<std::string> bad;
    std::map<std::string, uint64_t> heap_peaks;
};

// The patch applied in each of modes ("plain": to the old dir, compared with
// the new dir; "sync" and "threads": to the old encoded set, compared with
// the new encoded set), the patcher under the heap shim; check runs on each
// log. Every mismatch, printed.
Applied apply_modes(const LowMemCase& c, const std::vector<std::string>& modes, const LogCheck& check = {}) {
    Applied a;
    for (const std::string& mode : modes) {
        const bool plain = mode == "plain";
        const std::string out = c.dir("out." + mode);
        std::filesystem::remove(c.log());
        const auto [ok, heap_peak] = gctest::run_with_heap_peak(patch_words(c, mode, c.patch(), out), c.log());
        const std::vector<char> text = read_file(c.log());
        const std::string log(text.begin(), text.end());
        if (heap_peak) a.heap_peaks[mode] = *heap_peak;
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

// The case at n and at 10 n, each applied in every mode: each output as
// apply_modes checks it, and in each mode the heap peak at 10 n at most
// HEAP_GROWTH_SLACK above the one at n. Every failure, printed; skipped
// without a heap shim (the sanitizer build).
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
    std::map<std::string, uint64_t> at_n;
    for (const uint32_t size : {n, 10 * n}) {
        const LowMemCase c{dir.path(), "n" + std::to_string(size)};
        if (std::string e = make_case(c, size); !e.empty()) {
            fail(c.name + ": " + e);
            return bad;
        }
        const Applied a = apply_modes(c, ALL_MODES);
        bad.insert(bad.end(), a.bad.begin(), a.bad.end());
        for (const std::string& mode : ALL_MODES) {
            const auto peak = a.heap_peaks.find(mode);
            if (peak == a.heap_peaks.end()) fail(c.name + ": " + mode + ": no heap peak");
            else if (size == n) at_n[mode] = peak->second;
            else if (at_n.count(mode) && peak->second > at_n[mode] + HEAP_GROWTH_SLACK)
                fail(mode + ": the heap peak grew from " + std::to_string(at_n[mode]) + " B at n = " + std::to_string(n) + " to " +
                     std::to_string(peak->second) + " B at n = " + std::to_string(size) + ", past the " +
                     std::to_string(HEAP_GROWTH_SLACK) + " B of slack");
        }
    }
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

// The memory check of the replay tests above, on report lines as the patcher
// writes them (sync: queue peaks; --threads: ring and queue peaks): every
// peak at most the limit passes, one past it fails, and a line whose peaks
// are missing or not numbers fails instead of passing as a peak of 0.
TEST(decoder_peaks_at_most_checks_every_peak_of_the_report_line) {
    const LogCheck check = decoder_peaks_at_most("poi", 100);

    CHECK(check("encode poi: queue peaks 999 B\ndecode poi: queue peaks 10 100 B\n").empty());
    CHECK(check("decode poi: thread cpu 0.1 s, ring peaks 7 8 B, queue peaks 9 100 B\n").empty());
    CHECK(!check("decode poi: queue peaks 10 101 B\n").empty());
    CHECK(!check("decode poi: thread cpu 0.1 s, ring peaks 101 8 B, queue peaks 9 10 B\n").empty());
    for (const char* bad : {"decode poi:\n", "decode poi: queue B\n", "decode poi: queue peaks B\n", "decode poi: queue peaks ten B\n",
                            "decode poi: thread cpu 0.1 s, ring peaks 7 8 B, queue peaks 9 x B\n", "decode streets: queue peaks 1 B\n"})
        CHECK(!check(bad).empty());
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
// same way, mapping 16 MiB peaks at least 15 MiB above it mapping 1 MiB.
TEST(run_with_heap_peak_counts_the_mappings_of_a_program_built_as_the_tools_are) {
#ifndef GEOCODER_HEAP_PEAK_PROBE
    std::printf("    SKIP heap peaks of heap-peak-probe: no heap shim under the sanitizers\n");
#else
    ScratchDir dir("heap-peak-probe");
    auto peak_of = [&](const std::string& mib) {
        const auto [ok, heap_peak] = gctest::run_with_heap_peak({GEOCODER_HEAP_PEAK_PROBE, mib}, dir.path() + "/log");
        REQUIRE(ok && heap_peak);
        return *heap_peak;
    };

    const uint64_t one = peak_of("1"), sixteen = peak_of("16");

    CHECK(sixteen >= one + (uint64_t{15} << 20));
#endif
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
        put_poi_cells(old_d, old_cells);
        put_poi_cells(new_d, new_cells);
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
        put_poi_cells(old_d, old_cells);
        put_poi_cells(new_d, new_cells);
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
// patcher spills to its scratch dir.
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
    }, 20000).empty());
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
    // The merge: fixup counts and sizes, fixups, the sequence's length, its ops.
    const size_t seq_len_at = at + 40 + u32_at(at + 32) + u32_at(at + 36), seq = seq_len_at + 8;
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
    const size_t child_len_at = child_stride == 0 || child_stride == COPY_OLD_STRIDE
                                    ? seq_end + 28
                                    : seq_end + 40 + u32_at(seq_end + 32) + u32_at(seq_end + 36);
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
    put_poi_cells(old_d, old_cells);
    put_poi_cells(new_d, new_cells);
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
        if (!r.namespaced)
            std::printf("    %s %s: no private tmpfs (%s)\n", needed ? "FAIL" : "SKIP", name.c_str(), said.substr(0, 200).c_str());
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
