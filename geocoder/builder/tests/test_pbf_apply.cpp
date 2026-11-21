// Unit tests for pbf-apply (tools/pbf_apply.h): each merge rule of osmium
// apply-changes, and whole runs on synthetic files checked against a
// literal re-implementation of osmium-tool's CommandApplyChanges::run().
#include "pbf_apply.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <linux/magic.h>
#include <set>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <tuple>
#include <utility>
#include <vector>

#include "captured_stderr.h"
#include "pbf_test_objects.h"
#include "scratch_dir.h"
#include "test_framework.h"

using gctest::refusal_of;

namespace {

constexpr uint32_t T0 = 1783502088;

struct Change {
    char action;  // 'c', 'm' or 'd'
    TestObject object;
};
using ChangeFile = std::vector<Change>;

uint64_t positive_id(int64_t id) { return id < 0 ? uint64_t(0) - uint64_t(id) : uint64_t(id); }

// osmium-tool's CommandApplyChanges::run() without history: reverse the
// change list, stable_sort it by object_order_type_id_reverse_version,
// set_union it with the input, copy_first_with_id.
std::vector<TestObject> osmium_apply(const std::vector<TestObject>& input, const std::vector<ChangeFile>& files) {
    std::vector<TestObject> changes;
    for (const auto& f : files)
        for (const auto& c : f) {
            changes.push_back(c.object);
            changes.back().visible = c.action != 'd';
        }
    auto order = [](const TestObject* l, const TestObject* r) {
        bool both = l->timestamp != 0 && r->timestamp != 0;
        return std::make_tuple(int(l->type), l->id > 0, positive_id(l->id), r->version, both ? r->timestamp : 0u) <
               std::make_tuple(int(r->type), r->id > 0, positive_id(r->id), l->version, both ? l->timestamp : 0u);
    };
    std::vector<const TestObject*> objects, in, merged;
    for (const auto& c : changes) objects.push_back(&c);
    std::reverse(objects.begin(), objects.end());
    std::stable_sort(objects.begin(), objects.end(), order);
    for (const auto& o : input) in.push_back(&o);
    std::set_union(objects.begin(), objects.end(), in.begin(), in.end(), std::back_inserter(merged), order);
    std::vector<TestObject> out;
    int64_t id = 0;
    for (const TestObject* o : merged) {
        if (o->id != id) {
            if (o->visible) out.push_back(*o);
            id = o->id;
        }
    }
    return out;
}

std::string xml_escape(const std::string& s) {
    std::string out;
    for (char c : s) {
        switch (c) {
            case '&': out += "&amp;"; break;
            case '<': out += "&lt;"; break;
            case '>': out += "&gt;"; break;
            case '"': out += "&quot;"; break;
            case '\n': out += "&#10;"; break;
            default: out += c;
        }
    }
    return out;
}

std::string coordinate(int32_t v) {
    char buf[32];
    std::snprintf(buf, sizeof buf, "%s%d.%07d", v < 0 ? "-" : "", std::abs(v / 10000000), std::abs(v % 10000000));
    return buf;
}

std::string to_osc(const ChangeFile& file) {
    static const char* sections[] = {"create", "modify", "delete"};
    std::string out = "<?xml version='1.0' encoding='UTF-8'?>\n<osmChange version=\"0.6\" generator=\"test\">\n";
    const char* open = nullptr;
    for (const Change& c : file) {
        const char* section = sections[c.action == 'c' ? 0 : c.action == 'm' ? 1 : 2];
        if (open != section) {
            if (open) out += std::string("  </") + open + ">\n";
            out += std::string("  <") + section + ">\n";
            open = section;
        }
        const TestObject& o = c.object;
        static const char* names[] = {"node", "way", "relation"};
        out += std::string("    <") + names[int(o.type)] + " id=\"" + std::to_string(o.id) + "\" version=\"" +
               std::to_string(o.version) + "\"";
        if (o.timestamp) {
            std::time_t t = o.timestamp;
            char ts[32];
            std::strftime(ts, sizeof ts, "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&t));
            out += std::string(" timestamp=\"") + ts + "\"";
        }
        out += " uid=\"" + std::to_string(o.uid) + "\" user=\"" + xml_escape(o.user) + "\" changeset=\"" +
               std::to_string(o.changeset) + "\"";
        if (o.type == OsmType::Node && o.lat != kUndefinedCoordinate)
            out += " lat=\"" + coordinate(o.lat) + "\" lon=\"" + coordinate(o.lon) + "\"";
        out += ">\n";
        for (int64_t r : o.refs) out += "      <nd ref=\"" + std::to_string(r) + "\"/>\n";
        for (const auto& [t, r, role] : o.members)
            out += std::string("      <member type=\"") + names[int(t)] + "\" ref=\"" + std::to_string(r) +
                   "\" role=\"" + xml_escape(role) + "\"/>\n";
        for (const auto& [k, v] : o.tags)
            out += "      <tag k=\"" + xml_escape(k) + "\" v=\"" + xml_escape(v) + "\"/>\n";
        out += std::string("    </") + names[int(o.type)] + ">\n";
    }
    if (open) out += std::string("  </") + open + ">\n";
    return out + "</osmChange>\n";
}

struct Run {
    std::vector<TestObject> output;
    std::vector<TestObject> expected;
    std::vector<size_t> blob_objects;
    size_t verbatim_blobs = 0;  // output blobs byte-identical to an input blob
    ApplyStats stats;
};

std::set<std::string> raw_blobs(const std::string& path) {
    std::set<std::string> out;
    std::vector<BlobInfo> blobs = scan_pbf_blobs(path, 1);
    std::FILE* f = std::fopen(path.c_str(), "rb");
    for (size_t i = 1; i < blobs.size(); i++) {
        std::string b(4 + blobs[i].header_size + blobs[i].data_size, '\0');
        std::fseek(f, long(blobs[i].offset), SEEK_SET);
        if (std::fread(b.data(), 1, b.size(), f) != b.size()) break;
        out.insert(b);
    }
    std::fclose(f);
    return out;
}

Run run_apply(const std::vector<TestObject>& input, const std::vector<ChangeFile>& files, size_t input_per_block = 5,
              size_t max_block_objects = 4) {
    ScratchDir dir("pbf-apply-test");
    ApplyOptions opt;
    opt.input = dir.path() + "/in.osm.pbf";
    opt.output = dir.path() + "/out.osm.pbf";
    write_test_pbf(opt.input, input, input_per_block);
    for (size_t f = 0; f < files.size(); f++) {
        std::string path = dir.path() + "/c" + std::to_string(f) + ".osc";
        std::string text = to_osc(files[f]);
        std::FILE* fp = std::fopen(path.c_str(), "wb");
        std::fwrite(text.data(), 1, text.size(), fp);
        std::fclose(fp);
        opt.changes.push_back(path);
    }
    opt.max_block_objects = max_block_objects;
    opt.change_chunk_bytes = 300;
    opt.threads = 3;
    opt.verbose = false;
    Run run;
    run.stats = apply_changes(opt);
    run.output = read_test_pbf(opt.output, &run.blob_objects);
    run.expected = osmium_apply(input, files);
    std::set<std::string> in = raw_blobs(opt.input), out = raw_blobs(opt.output);
    for (const auto& b : out) run.verbatim_blobs += in.count(b);
    return run;
}

TestObject node(int64_t id, uint32_t version, uint32_t timestamp = T0, const std::string& tag = "") {
    TestObject o;
    o.id = id;
    o.version = version;
    o.timestamp = timestamp;
    o.lon = int32_t(id * 1000);
    o.lat = int32_t(-id * 1000);
    if (!tag.empty()) o.tags = {{"t", tag}};
    return o;
}

TestObject of_type(OsmType type, TestObject o) {
    o.type = type;
    if (type != OsmType::Node) o.lon = o.lat = kUndefinedCoordinate;
    return o;
}

// A small deterministic generator.
struct Lcg {
    uint64_t s;
    uint32_t next(uint32_t n) {
        s = s * 6364136223846793005ull + 1442695040888963407ull;
        return uint32_t((s >> 33) % n);
    }
};

}  // namespace

TEST(apply_merge_rules_match_osmium) {
    struct Case {
        const char* name;
        std::vector<TestObject> input;
        std::vector<ChangeFile> files;
        std::vector<std::string> expected;
    };
    TestObject n1 = node(1, 1, T0, "in");
    auto line = [](const TestObject& o) { return describe(o); };
    const Case cases[] = {
        {"the newest change wins whatever its position",
         {n1},
         {{{'m', node(1, 3, T0, "a")}, {'m', node(1, 2, T0, "b")}}},
         {line(node(1, 3, T0, "a"))}},
        {"the last-read change wins a tie, across files",
         {n1},
         {{{'m', node(1, 2, T0, "a")}}, {{'m', node(1, 2, T0, "b")}}},
         {line(node(1, 2, T0, "b"))}},
        {"a later timestamp breaks a version tie",
         {n1},
         {{{'m', node(1, 2, T0 + 1, "a")}, {'m', node(1, 2, T0, "b")}}},
         {line(node(1, 2, T0 + 1, "a"))}},
        {"a missing timestamp ties",
         {n1},
         {{{'m', node(1, 2, T0 + 1, "a")}, {'m', node(1, 2, 0, "b")}}},
         {line(node(1, 2, 0, "b"))}},
        {"a newer input object survives a stale change",
         {node(1, 5, T0, "in")},
         {{{'m', node(1, 4, T0, "a")}}},
         {line(node(1, 5, T0, "in"))}},
        {"a change equal to the input object wins",
         {node(1, 2, T0, "in")},
         {{{'m', node(1, 2, T0, "a")}}},
         {line(node(1, 2, T0, "a"))}},
        {"the newest change being a delete removes the object", {n1}, {{{'d', node(1, 2)}}}, {}},
        {"a delete older than a modify keeps the object",
         {n1},
         {{{'m', node(1, 3, T0, "a")}, {'d', node(1, 2)}}},
         {line(node(1, 3, T0, "a"))}},
        {"a delete of a missing object writes nothing", {n1}, {{{'d', node(2, 2)}}}, {line(n1)}},
        {"a create for an existing id is a newer version like any other",
         {n1},
         {{{'c', node(1, 2, T0, "a")}}},
         {line(node(1, 2, T0, "a"))}},
        {"objects before the first and after the last id land in order",
         {node(5, 1), node(6, 1)},
         {{{'c', node(9, 1)}, {'c', node(1, 1)}, {'c', node(-3, 1)}}},
         {line(node(-3, 1)), line(node(1, 1)), line(node(5, 1)), line(node(6, 1)), line(node(9, 1))}},
    };
    for (const Case& c : cases) {
        Run run = run_apply(c.input, c.files);
        bool ok = describe(run.output) == c.expected && describe(run.expected) == c.expected;
        if (!ok) std::printf("    case: %s\n", c.name);
        CHECK(ok);
    }
}

TEST(apply_drops_a_first_id_equal_to_the_previous_types_last_like_osmium) {
    // copy_first_with_id remembers only the id: way 7 follows node 7, and
    // the first object (id 0) follows the initial id 0.
    std::vector<TestObject> input = {node(0, 1), node(3, 1), node(7, 1), of_type(OsmType::Way, node(7, 1)),
                                     of_type(OsmType::Way, node(9, 1)), of_type(OsmType::Relation, node(1, 1))};
    Run run = run_apply(input, {{{'c', of_type(OsmType::Relation, node(2, 1))}}});
    CHECK(describe(run.output) == describe(run.expected));
    REQUIRE(run.output.size() == 5);
    CHECK_EQ(run.output[0].id, int64_t(3));
    CHECK(run.output[2].type == OsmType::Way && run.output[2].id == 9);

    // The same for a type only the changes hold.
    Run created = run_apply({node(1, 1), node(5, 1)}, {{{'c', of_type(OsmType::Way, node(5, 1))},
                                                        {'c', of_type(OsmType::Way, node(6, 1))}}});
    CHECK(describe(created.output) == describe(created.expected));
    REQUIRE(created.output.size() == 3);
    CHECK(created.output[2].type == OsmType::Way && created.output[2].id == 6);
}

TEST(apply_copies_every_blob_of_an_empty_change_file) {
    std::vector<TestObject> input;
    for (int64_t id = 1; id <= 12; id++) input.push_back(node(id, 1));
    input.push_back(of_type(OsmType::Way, node(1, 1)));
    Run run = run_apply(input, {{}});
    CHECK(describe(run.output) == describe(input));
    CHECK_EQ(run.stats.merged_blobs, size_t(0));
    CHECK_EQ(run.verbatim_blobs, run.blob_objects.size());
}

TEST(apply_matches_osmium_on_random_files_and_copies_untouched_blobs) {
    for (uint64_t seed = 1; seed <= 12; seed++) {
        Lcg rng{seed};
        std::vector<TestObject> input;
        const int64_t max_id[3] = {240, 90, 30};
        const char* values[] = {"a", "b&c", "x<y>", "q\"r", "line\nbreak", ""};
        for (int t = 0; t < 3; t++) {
            for (int64_t id = 1; id <= max_id[t]; id++) {
                if (rng.next(4) == 0) continue;
                TestObject o = of_type(OsmType(t), node(id, 1 + rng.next(3), T0 - rng.next(1000)));
                o.uid = rng.next(5);
                o.user = o.uid ? "user" + std::to_string(o.uid) : "";
                o.changeset = rng.next(100000);
                if (rng.next(3) == 0) o.tags = {{"k" + std::to_string(rng.next(4)), values[rng.next(6)]}};
                if (t == 1)
                    for (uint32_t k = rng.next(5); k-- > 0;) o.refs.push_back(1 + rng.next(240));
                if (t == 2)
                    for (uint32_t k = rng.next(4); k-- > 0;)
                        o.members.emplace_back(OsmType(rng.next(3)), 1 + rng.next(90), values[rng.next(6)]);
                input.push_back(o);
            }
        }
        std::vector<ChangeFile> files(2);
        for (auto& file : files) {
            for (uint32_t n = 20 + rng.next(40); n-- > 0;) {
                int t = int(rng.next(3));
                int64_t id = int64_t(rng.next(uint32_t(max_id[t] + 20))) - 3;
                TestObject o = of_type(OsmType(t), node(id, 1 + rng.next(4), rng.next(5) ? T0 + rng.next(3) : 0));
                o.user = "editor";
                o.uid = 99;
                if (rng.next(2)) o.tags = {{"name", values[rng.next(6)]}};
                if (t == 1) o.refs = {int64_t(rng.next(250)), 13000000000};
                if (t == 2) o.members = {{OsmType::Way, 5, "outer"}};
                char action = "cmmd"[rng.next(4)];
                file.push_back({action, o});
            }
        }
        // Creates past every existing id, enough for tail blobs.
        for (int t = 0; t < 3; t++)
            for (int64_t id = max_id[t] + 30; id < max_id[t] + 40; id++)
                files[1].push_back({'c', of_type(OsmType(t), node(id, 1))});
        Run run = run_apply(input, files);
        bool same = describe(run.output) == describe(run.expected);
        if (!same) std::printf("    seed %llu differs\n", (unsigned long long)seed);
        CHECK(same);
        CHECK(run.stats.copied_blobs > 0);
        CHECK(run.stats.merged_blobs > 0);
        CHECK_EQ(run.verbatim_blobs, run.stats.copied_blobs);
        CHECK(std::all_of(run.blob_objects.begin(), run.blob_objects.end(), [](size_t n) { return n >= 1 && n <= 5; }));
    }
}

TEST(apply_writes_the_header_the_options_ask_for) {
    ScratchDir dir("pbf-apply-test");
    ApplyOptions opt;
    opt.input = dir.path() + "/in.osm.pbf";
    opt.output = dir.path() + "/out.osm.pbf";
    PbfHeader in;
    in.bbox = PbfHeader::Box{-1, 2, 3, -4};
    in.required_features = {"OsmSchema-V0.6", "DenseNodes"};
    in.source = "test source";
    in.replication_timestamp = 5;
    in.replication_sequence = 6;
    in.replication_base_url = "https://example.org/replication";
    write_test_pbf(opt.input, {node(1, 1)}, 5, in);
    std::string change = dir.path() + "/c.osc";
    std::string text = to_osc({{'m', node(1, 2)}});
    std::FILE* fp = std::fopen(change.c_str(), "wb");
    std::fwrite(text.data(), 1, text.size(), fp);
    std::fclose(fp);
    opt.changes = {change};
    opt.verbose = false;
    opt.replication_timestamp = T0;
    apply_changes(opt);
    std::vector<BlobInfo> blobs = scan_pbf_blobs(opt.output, 1);
    int fd = open(opt.output.c_str(), O_RDONLY);
    PbfHeader out = decode_header_block(read_and_decompress_blob(fd, blobs[0]));
    close(fd);
    REQUIRE(out.bbox.has_value());
    CHECK_EQ(out.bbox->bottom, int64_t(-4));
    CHECK(out.required_features == std::vector<std::string>({"OsmSchema-V0.6", "DenseNodes"}));
    CHECK(out.optional_features == std::vector<std::string>({"Sort.Type_then_ID"}));
    CHECK_EQ(out.source, std::string("test source"));
    CHECK_EQ(out.writingprogram, std::string("pbf-apply"));
    CHECK(out.replication_timestamp == std::optional<int64_t>(T0));
    CHECK(!out.replication_sequence.has_value());
    CHECK_EQ(out.replication_base_url, in.replication_base_url);

    in.required_features.push_back("HistoricalInformation");
    write_test_pbf(opt.input, {node(1, 1)}, 5, in);
    CHECK_EQ(refusal_of([&] { apply_changes(opt); }),
             std::string("input PBF requires unsupported feature HistoricalInformation (history files are not supported)"));
}

namespace {

// The share of a file's pages in the page cache, or a negative number where
// that can't be seen: a tmpfs keeps every page.
double resident_share(const std::string& path) {
    struct statfs fs;
    if (statfs(path.c_str(), &fs) != 0 || fs.f_type == TMPFS_MAGIC) return -1;
    int fd = open(path.c_str(), O_RDONLY);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) != 0 || st.st_size == 0) {
        if (fd >= 0) close(fd);
        return -1;
    }
    const size_t size = size_t(st.st_size), page = size_t(sysconf(_SC_PAGESIZE));
    void* map = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return -1;
    std::vector<unsigned char> pages((size + page - 1) / page);
    double share = -1;
    if (mincore(map, size, pages.data()) == 0)
        share = double(std::count_if(pages.begin(), pages.end(), [](unsigned char p) { return p & 1; })) / double(pages.size());
    munmap(map, size);
    return share;
}

std::string file_bytes(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    std::stringstream s;
    s << f.rdbuf();
    return s.str();
}

}  // namespace

// The writer drops the input's pages once the output after them is written:
// ranges from the start, in order, cut at 1 MiB and at least a step apart,
// never past the input of the first item whose output is not yet written;
// the rest of the file when the output is finished.
TEST(apply_drops_the_input_behind_the_written_output) {
    constexpr uint64_t MiB = 1 << 20;
    std::vector<std::pair<uint64_t, uint64_t>> drops;
    InputPageDropper sut([&](uint64_t offset, uint64_t length) { drops.emplace_back(offset, length); }, 4 * MiB);

    sut.appended(3 * MiB, 3 * MiB);        // a copy: output and input to 3 MiB
    sut.appended(6 * MiB, 7 * MiB + 100);  // a merge: its blob ends past 7 MiB
    sut.written(0);
    CHECK(drops.empty());
    CHECK_EQ(sut.dropped(), uint64_t(0));
    sut.written(4 * MiB);  // the copy's output is written: 3 MiB of input, under the step
    CHECK(drops.empty());
    sut.appended(8 * MiB, 0);  // a tail, which reads no input
    sut.written(5 * MiB);      // the merge's output is not all written
    CHECK(drops.empty());
    sut.written(8 * MiB);
    CHECK(drops == (std::vector<std::pair<uint64_t, uint64_t>>{{0, 7 * MiB}}));
    CHECK_EQ(sut.dropped(), 7 * MiB);
    sut.appended(9 * MiB, 9 * MiB);
    sut.written(9 * MiB);  // 2 MiB more: under the step
    CHECK_EQ(drops.size(), size_t(1));
    sut.appended(20 * MiB, 20 * MiB);
    sut.written(20 * MiB);
    CHECK(drops.back() == std::make_pair(7 * MiB, 13 * MiB));
    sut.finish();  // length 0: to the end of the file
    CHECK(drops.back() == std::make_pair(20 * MiB, uint64_t(0)));
    CHECK_EQ(drops.size(), size_t(3));

    // A step of 0 drops whatever the input read gained, and nothing while it
    // stays put: an empty range would be posix_fadvise's "to the end".
    drops.clear();
    InputPageDropper each([&](uint64_t offset, uint64_t length) { drops.emplace_back(offset, length); }, 0);
    each.written(0);
    CHECK(drops.empty());
    each.appended(2 * MiB, MiB + 100);
    each.written(2 * MiB);
    each.written(2 * MiB);  // nothing more read
    each.appended(3 * MiB, MiB + 200);
    each.written(3 * MiB);  // still within the first MiB past the drop
    each.appended(4 * MiB, 2 * MiB);
    each.written(4 * MiB);
    CHECK(drops == (std::vector<std::pair<uint64_t, uint64_t>>{{0, MiB}, {MiB, MiB}}));
    CHECK_EQ(each.dropped(), 2 * MiB);
}

// The point of the drops: after a run the output, which build-index reads
// next, is in the page cache and the input, which the nightly then deletes,
// is not.
TEST(apply_leaves_the_output_in_the_page_cache_in_place_of_the_input) {
    ScratchDir dir("pbf-apply-test");
    ApplyOptions opt;
    opt.input = dir.path() + "/in.osm.pbf";
    opt.output = dir.path() + "/out.osm.pbf";
    std::vector<TestObject> input;
    for (int64_t id = 1; id <= 20000; id++) input.push_back(node(id, 1, T0, "value " + std::to_string(id * 7919 % 10007)));
    write_test_pbf(opt.input, input, 100);
    int fd = open(opt.input.c_str(), O_RDONLY);
    REQUIRE(fd >= 0);
    fsync(fd);
    close(fd);
    double before = resident_share(opt.input);
    if (before <= 0) {
        std::printf("    SKIP apply_leaves_the_output_in_the_page_cache_in_place_of_the_input: page cache not visible here\n");
        return;
    }
    std::string change = dir.path() + "/c.osc";
    std::string text = to_osc({{'m', node(5, 2, T0, "changed")}, {'c', node(30000, 1)}});
    std::FILE* fp = std::fopen(change.c_str(), "wb");
    std::fwrite(text.data(), 1, text.size(), fp);
    std::fclose(fp);
    opt.changes = {change};
    opt.verbose = false;
    opt.threads = 3;

    apply_changes(opt);

    CHECK_EQ(resident_share(opt.input), 0.0);
    CHECK(resident_share(opt.output) > 0.9);
    CHECK(describe(read_test_pbf(opt.output)) == describe(osmium_apply(input, {{{'m', node(5, 2, T0, "changed")},
                                                                                 {'c', node(30000, 1)}}})));
}

// In a run, not only at its end: the input leaves the page cache in ranges
// from its start, in order, as the output after them is written, the rest
// once the output is finished. And the drops change no byte of the output:
// it is the bytes of a run that drops nothing.
TEST(apply_drops_the_input_while_it_writes_and_changes_no_byte_of_the_output) {
    constexpr uint64_t MiB = 1 << 20;
    ScratchDir dir("pbf-apply-test");
    ApplyOptions opt;
    opt.input = dir.path() + "/in.osm.pbf";
    // ~5 MiB of blobs, their tags too random to compress away.
    std::vector<TestObject> input;
    Lcg lcg{7};
    for (int64_t id = 1; id <= 250000; id++) {
        std::string tag;
        for (int i = 0; i < 32; i++) tag += char('a' + lcg.next(26));
        input.push_back(node(id, 1, T0, tag));
    }
    write_test_pbf(opt.input, input, 100);
    const uint64_t input_bytes = file_bytes(opt.input).size();
    REQUIRE(input_bytes > 4 * MiB);
    std::string change = dir.path() + "/c.osc";
    std::string text = to_osc({{'m', node(100000, 2, T0, "changed")}, {'c', node(300000, 1)}});
    std::FILE* fp = std::fopen(change.c_str(), "wb");
    std::fwrite(text.data(), 1, text.size(), fp);
    std::fclose(fp);
    opt.changes = {change};
    opt.verbose = false;
    opt.threads = 3;
    opt.write_block_bytes = 64 << 10;
    opt.input_drop_step_bytes = MiB;

    opt.output = dir.path() + "/reference.osm.pbf";
    opt.drop_input_pages = [](uint64_t, uint64_t) {};
    apply_changes(opt);
    std::vector<std::pair<uint64_t, uint64_t>> drops;
    int fd = open(opt.input.c_str(), O_RDONLY);
    REQUIRE(fd >= 0);
    opt.output = dir.path() + "/out.osm.pbf";
    opt.drop_input_pages = [&](uint64_t offset, uint64_t length) {
        drops.emplace_back(offset, length);
        posix_fadvise(fd, static_cast<off_t>(offset), static_cast<off_t>(length), POSIX_FADV_DONTNEED);
    };
    opt.verbose = true;
    opt.input_drop_log_bytes = 2 * MiB;
    std::string logged;
    {
        gctest::CapturedStderr captured;
        apply_changes(opt);
        logged = captured.str();
    }
    close(fd);

    CHECK(file_bytes(opt.output) == file_bytes(dir.path() + "/reference.osm.pbf"));
    REQUIRE(drops.size() >= 3);
    uint64_t dropped = 0;
    for (size_t i = 0; i + 1 < drops.size(); i++) {
        CHECK_EQ(drops[i].first, dropped);
        CHECK(drops[i].second >= MiB);
        CHECK_EQ((drops[i].first + drops[i].second) % MiB, uint64_t(0));
        dropped += drops[i].second;
    }
    // Most of the input went while the output was written: at most the
    // writer's blocks in flight, and the step, are left for the end.
    CHECK(dropped <= input_bytes);
    CHECK(dropped + 3 * MiB >= input_bytes);
    CHECK(drops.back() == std::make_pair(dropped, uint64_t(0)));
    // A line each 2 MiB more of the input asked out of the page cache, at a
    // drop's end, with the output written then, which is past it: this input
    // and its output differ by a blob or two.
    std::istringstream lines(logged);
    std::set<uint64_t> ends;
    for (size_t i = 0; i + 1 < drops.size(); i++) ends.insert((drops[i].first + drops[i].second) >> 20);
    uint64_t previous = 0;
    int seen = 0;
    for (std::string line; std::getline(lines, line);) {
        unsigned long long to = 0, written = 0;
        if (std::sscanf(line.c_str(), "[pbf-apply] input asked out of the page cache up to %llu MiB, %llu MiB of the output written",
                        &to, &written) != 2)
            continue;
        seen++;
        CHECK(to >= previous + 2);
        CHECK(ends.count(to) == 1);
        CHECK(written + 1 >= to);
        previous = to;
    }
    CHECK(seen >= 2);
}
