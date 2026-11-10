// The cell pair index files (cell_pairs.h): every list shape reads back from
// each kind of index, the Writer's bytes are fixed (the server's reader and
// fixture writer check the same bytes, server/src/cell_pairs/tests.rs), the
// Reader refuses each malformed part, and a small region's index stays small.
#include "cell_pairs.h"

#include <cstdio>
#include <fstream>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "patch_format.h"
#include "scratch_dir.h"
#include "test_framework.h"

using gctest::refusal_of;
using namespace cell_pairs;

namespace {

using Lists = std::vector<std::pair<uint64_t, std::vector<uint32_t>>>;
using Files = std::pair<std::vector<char>, std::vector<char>>;

constexpr Format FORMATS[] = {ADMIN, POI, POSTCODE_CENTROID};

// The level-10 cell of number n (face * 4^10 + its position).
uint64_t cell(uint64_t n) { return cell_of(n, 10); }

Files encode(const Lists& lists, Format format) {
    Files out;
    Writer writer(
        format, [&](const char* p, size_t n) { out.first.insert(out.first.end(), p, p + n); },
        [&](const char* p, size_t n) { out.second.insert(out.second.end(), p, p + n); });
    for (const auto& [c, ids] : lists) writer.add(c, ids.data(), ids.size());
    writer.finish();
    return out;
}

template <typename Src>
Lists decode_from(Src& cells, Src& entries, Format format) {
    Reader reader(cells, entries, format);
    Lists out;
    uint64_t c;
    std::vector<uint32_t> ids;
    while (reader.next(c, ids)) out.push_back({c, ids});
    return out;
}

Lists decode(const Files& files, Format format) {
    ByteSpan c{files.first.data(), files.first.size()}, e{files.second.data(), files.second.size()};
    return decode_from(c, e, format);
}

// Lists of every shape over parents at the bitmap's word ends and the last
// face's last parent, each holding its first and last children, runs of
// neighbours and lone cells: empty lists, one id, runs of ids, sparse ids
// with gaps of 1 to 4 bytes, repeated ids, interior flags, ids at the top of
// the bare and the u32 range, a list the row before has (SAME, a shared
// offset) and ones it has but for an id dropped, added or its flag toggled
// (EDIT), and at the last cell one of 0xFFFF ids.
Lists shapes(std::mt19937_64& rng) {
    Lists out;
    std::vector<uint32_t> prev = {3, 9};
    for (uint64_t parent : {0, 1, 63, 64, 65, 1000, 4095, 6143}) {
        std::vector<uint64_t> children = {0, 1, 2, 63, 64, 511, 1022, 1023};
        for (int k = 0; k < 40; k++) children.push_back(rng() % 1024);
        std::sort(children.begin(), children.end());
        children.erase(std::unique(children.begin(), children.end()), children.end());
        for (uint64_t child : children) {
            std::vector<uint32_t> l;
            switch (rng() % 14) {
            case 0: break;
            case 1: l = {static_cast<uint32_t>(rng() % 1000)}; break;
            case 2: l = {BARE}; break;
            case 3: l = {0xFFFFFFFFu}; break;
            case 4: for (uint32_t v = 100; v < 140; v++) l.push_back(v); break;
            case 5: for (int k = 0; k < 30; k++) l.push_back(static_cast<uint32_t>(rng() >> (8 * (rng() % 4) + 33))); break;
            case 6: for (int k = 0; k < 12; k++) l.push_back(static_cast<uint32_t>(rng() % 50)); break;
            case 7:
                for (int k = 0; k < 10; k++) l.push_back(static_cast<uint32_t>(rng() % 5000) | (rng() % 3 == 0 ? FLAG : 0));
                break;
            case 8: case 9: l = prev; break;
            case 10: l = prev; if (!l.empty()) l.erase(l.begin() + static_cast<long>(rng() % l.size())); break;
            case 11: l = prev; l.push_back(static_cast<uint32_t>(rng() % 6000)); break;
            case 12: l = prev; if (!l.empty()) l[0] ^= FLAG; break;
            default:
                for (uint32_t k = 0; k < 4; k++) {
                    const uint32_t at[] = {0, 255, 256, 65535, 65536, 16777215, 16777216, BARE};
                    l.push_back(at[rng() % 8] + k);
                }
            }
            std::sort(l.begin(), l.end());
            prev = l;
            out.push_back({cell(parent << 10 | child), l});
        }
    }
    out.back().second.clear();
    for (uint32_t k = 0; k < LIST_MAX; k++) out.back().second.push_back(k * 7);
    return out;
}

void write_bytes(const std::string& path, const std::vector<char>& bytes) {
    std::ofstream(path, std::ios::binary).write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

}  // namespace

TEST(cell_pairs_every_list_shape_reads_back_from_each_kind) {
    std::mt19937_64 rng(20261109);
    ScratchDir dir("gctest-cellpairs");
    for (Format format : FORMATS) {
        const Lists lists = shapes(rng);
        const Files files = encode(lists, format);
        CHECK(decode(files, format) == lists);
        // From files through a 16-byte window: refilled mid-list, grown for long lists, read front to back.
        write_bytes(dir.path() + "/c.bin", files.first);
        write_bytes(dir.path() + "/e.bin", files.second);
        SequentialFileReader c(dir.path() + "/c.bin", 16), e(dir.path() + "/e.bin", 16);
        CHECK(decode_from(c, e, format) == lists);
        CHECK_EQ(c.rewinds() + e.rewinds(), uint64_t(0));
    }
}

// Every level an index can be at (5 to 13): lists in its first and last
// parents and those at the bitmap's word ends among them read back, and the
// bitmap runs to the last parent's word. Below level 8 a level's parents (6,
// 24 and 96) end inside a word.
TEST(cell_pairs_every_level_reads_back) {
    std::mt19937_64 rng(5);
    for (int level = MIN_LEVEL; level <= MAX_LEVEL; level++) {
        const uint64_t parents = parents_at(level);
        std::vector<uint64_t> at = {0, 1, parents - 2, parents - 1};
        for (uint64_t p : {63, 64, 65, 127, 128})
            if (p < parents) at.push_back(p);
        std::sort(at.begin(), at.end());
        at.erase(std::unique(at.begin(), at.end()), at.end());
        Lists lists;
        for (uint64_t parent : at)
            for (uint64_t child : {0, 1, 512, 1023}) {
                std::vector<uint32_t> l(1 + rng() % 5);
                for (uint32_t& id : l) id = static_cast<uint32_t>(rng() % 1000) | (rng() % 4 == 0 ? FLAG : 0);
                std::sort(l.begin(), l.end());
                lists.push_back({cell_of(parent << (2 * LEAF_LEVELS) | child, level), l});
            }
        for (Format format : FORMATS) {
            const Files files = encode(lists, format);
            CHECK(decode(files, format) == lists);
            uint32_t words;
            std::memcpy(&words, files.first.data() + files.first.size() - 4, 4);
            CHECK_EQ(words, static_cast<uint32_t>((parents - 1) / 64 + 1));
        }
    }
}

// The Writer's bytes for small indexes of each kind, which the server's tests
// read and its fixture writer writes. ADMIN: a block of a PLAIN row, a SAME
// row, an EDIT (9 toggled, 12 added) and a row repeating an id, then an empty
// list in another parent. POI: gaps of 1 to 4 bytes and a flagged id, a shared
// list, another parent. Postcode centroids: a shared list and an id past
// 2^31, ids alone.
static const Lists GOLDEN_ADMIN = {{cell(5), {3, 9}}, {cell(6), {3, 9}}, {cell(7), {3, 12, 9 | FLAG}},
                                   {cell(8), {7, 7}}, {cell(1030), {}}};
static const Lists GOLDEN_POI = {{cell(5), {1, 300, 70000, 20000000, 5 | FLAG}}, {cell(6), {1, 300, 70000, 20000000, 5 | FLAG}},
                                 {cell(2048), {42}}};
static const Lists GOLDEN_POSTCODE = {{cell(5), {10, 11, 400}}, {cell(6), {10, 11, 400}}, {cell(7), {0xFFFFFFF0u}}};

// FNV-1a 64 of bytes: the long cells files of the golden indexes, compared by hash.
static uint64_t fnv1a(const std::vector<char>& bytes) {
    uint64_t h = 0xcbf29ce484222325ull;
    for (char b : bytes) h = (h ^ static_cast<uint8_t>(b)) * 0x100000001b3ull;
    return h;
}

TEST(cell_pairs_writer_bytes_are_fixed) {
    struct Case {
        const Lists* lists;
        Format format;
        size_t cells_bytes;
        uint64_t cells_hash;
        std::vector<uint8_t> entries;
    };
    const Case cases[] = {
        {&GOLDEN_ADMIN, ADMIN, 312, 0x736bd71a2827593bull, {36, 0, 4, 0, 6, 5, 8, 1, 24, 5, 0, 8, 0, 0, 0, 0, 0}},
        {&GOLDEN_POI, POI, 316, 0xa98150bcae9640a2ull, {8, 1, 1, 57, 42, 1, 67, 16, 1, 143, 27, 48, 1, 5, 2, 0, 42}},
        {&GOLDEN_POSTCODE, POSTCODE_CENTROID, 180, 0x1b32dba5fd24c3ccull, {6, 10, 0, 132, 3, 2, 240, 255, 255, 255, 15}},
    };
    for (const Case& c : cases) {
        const Files files = encode(*c.lists, c.format);
        CHECK_EQ(files.first.size(), c.cells_bytes);
        CHECK_EQ(fnv1a(files.first), c.cells_hash);
        CHECK(std::vector<uint8_t>(files.second.begin(), files.second.end()) == c.entries);
        CHECK(decode(files, c.format) == *c.lists);
    }
    // The admin block's parts: codes PLAIN, SAME, EDIT, PLAIN; {3, 9} as 2 << 1, no flagged ids, zigzag 3, gap
    // 5; the edit's ops (3 kept, 9 toggled: 2 << 2) and the added 12 as 12 << 1; {7, 7} with repeats against the
    // last first, 3. The cells file: the header, then parent 0's record, its leaf (rows 5 to 8) and its block at 0.
    const Files admin = encode(GOLDEN_ADMIN, ADMIN);
    const std::vector<uint8_t> head = {'G', 'C', 'P', '1', 1, 1, 10, 0, 0, 0, 0, 0, 0xE0, 1};
    CHECK(std::vector<uint8_t>(admin.first.begin(), admin.first.begin() + head.size()) == head);
}

// No cells, as Antarctica's admin dirs have: two empty files. A region of one
// cell: its bitmap is the words up to its parent's, never the 6 * 4^10 bits
// of a bitmap of its cells (CI's flat bitmap was 816 KiB at any size).
TEST(cell_pairs_small_regions_stay_small) {
    for (Format format : FORMATS) {
        const Files none = encode({}, format);
        CHECK(none.first.empty() && none.second.empty());
        CHECK(decode(none, format).empty());
    }
    for (auto [n, words] : {std::pair<uint64_t, size_t>{5, 1}, {64 << 10, 2}, {(uint64_t(6) << 20) - 1, 96}}) {
        const Files one = encode({{cell(n), {7}}}, ADMIN);
        CHECK_EQ(one.first.size(), HEADER_BYTES + (4 + LEAF_BYTES + 4) + 4 + words * 12 + 4 + FOOTER_BYTES);
        CHECK_EQ(one.second.size(), size_t(5));
    }
}

TEST(cell_pairs_writer_refuses_what_it_cant_write) {
    auto refusal = [](const Lists& lists, Format format = ADMIN) {
        return refusal_of<std::exception>([&] { encode(lists, format); });
    };
    std::vector<uint32_t> longest(LIST_MAX), past(LIST_MAX + 1);
    for (uint32_t k = 0; k < past.size(); k++) past[k] = k;
    CHECK_EQ(refusal({{cell(1), longest}}), std::string());
    CHECK_EQ(refusal({{cell(1), past}}), std::string("Cell holds 65536 entries, more than 65535"));
    CHECK_EQ(refusal({{cell(2), {1}}, {cell(1), {1}}}), std::string("cell_pairs::Writer: cells out of order"));
    CHECK_EQ(refusal({{cell(2), {1}}, {cell(2), {1}}}), std::string("cell_pairs::Writer: cells out of order"));
    CHECK_EQ(refusal({{cell(2), {1}}, {cell_of(9, 11), {1}}}), std::string("cell_pairs::Writer: cells of two levels"));
    CHECK(refusal({{cell_of(1, 4), {1}}}).find("is not a cell of levels 5 to 13") != std::string::npos);
    CHECK(refusal({{cell_of(1, 14), {1}}}).find("is not a cell of levels 5 to 13") != std::string::npos);
    CHECK(refusal({{7, {1}}}).find("is not a cell of levels 5 to 13") != std::string::npos);
    CHECK_EQ(refusal({{cell(1), {2, 1}}}), std::string("cell_pairs::Writer: a list out of order"));
}

namespace {

// A cells file put together by hand: the header, the records (a parent, its
// children, its offsets) and END_OF_RECORDS, then the tail they make.
struct Record {
    uint32_t parent;
    std::vector<unsigned> children;
    std::vector<uint32_t> offsets;
};
template <typename T>
void put(std::vector<char>& out, T v) {
    out.insert(out.end(), reinterpret_cast<const char*>(&v), reinterpret_cast<const char*>(&v) + sizeof v);
}
std::vector<char> cells_by_hand(Format format, const std::vector<Record>& records, uint8_t level = 10) {
    std::vector<char> out = {'G', 'C', 'P', '1', char(format.kind), char(format.flags()), char(level), 0};
    std::vector<uint32_t> starts;
    std::vector<uint64_t> bits;
    for (const Record& r : records) {
        starts.push_back(static_cast<uint32_t>(out.size()));
        put(out, r.parent);
        uint64_t leaf[LEAF_WORDS] = {};
        for (unsigned c : r.children) leaf[c / 64] |= uint64_t(1) << (c % 64);
        for (uint64_t w : leaf) put(out, w);
        for (uint32_t o : r.offsets) put(out, o);
        if (bits.size() <= r.parent / 64) bits.resize(r.parent / 64 + 1);
        bits[r.parent / 64] |= uint64_t(1) << (r.parent % 64);
    }
    put(out, END_OF_RECORDS);
    for (uint64_t w : bits) put(out, w);
    uint32_t before = 0;
    for (uint64_t w : bits) {
        put(out, before);
        before += static_cast<uint32_t>(__builtin_popcountll(w));
    }
    for (uint32_t s : starts) put(out, s);
    put(out, static_cast<uint32_t>(records.size()));
    put(out, static_cast<uint32_t>(bits.size()));
    return out;
}
std::vector<char> bytes(std::initializer_list<int> v) {
    std::vector<char> out;
    for (int b : v) out.push_back(static_cast<char>(b));
    return out;
}
std::string refusal_reading(const std::vector<char>& cells, const std::vector<char>& entries, Format format) {
    return refusal_of([&] { decode({cells, entries}, format); });
}

}  // namespace

// Each part the Reader checks, malformed by hand, and refused as it is.
TEST(cell_pairs_reader_refuses_each_malformed_part) {
    const std::string m = "Malformed cell index: ";
    // One block of two rows: {3, 9} PLAIN and SAME.
    const std::vector<Record> one = {{0, {5, 6}, {0}}};
    const std::vector<char> good_block = bytes({4, 0, 4, 0, 6, 5});
    CHECK_EQ(refusal_reading(cells_by_hand(ADMIN, one), good_block, ADMIN), std::string());
    const std::vector<char> cells = cells_by_hand(ADMIN, one);
    struct Case { const char* why; std::vector<char> cells, entries; Format format; };
    std::vector<Case> cases;
    auto with = [&](std::vector<char> c, size_t at, char v) { c[at] = v; return c; };
    cases.push_back({"entries without cells", {}, good_block, ADMIN});
    cases.push_back({"a cells file too short for its header", std::vector<char>(cells.begin(), cells.begin() + 12), good_block, ADMIN});
    cases.push_back({"not a GCP1 cells file", with(cells, 0, 'X'), good_block, ADMIN});
    cases.push_back({"a cells file of another kind of index", cells, good_block, POI});
    cases.push_back({"a cells file of another kind of index", with(cells, 5, 3), good_block, ADMIN});
    cases.push_back({"a header past the levels it takes", with(cells, 6, 4), good_block, ADMIN});
    cases.push_back({"a header past the levels it takes", with(cells, 6, 14), good_block, ADMIN});
    cases.push_back({"a header past the levels it takes", with(cells, 7, 1), good_block, ADMIN});
    cases.push_back({"a cells file without records", cells_by_hand(ADMIN, {}), {}, ADMIN});
    cases.push_back({"parent records out of order", cells_by_hand(ADMIN, {{1, {5, 6}, {0}}, {1, {7, 8}, {6}}}), good_block, ADMIN});
    cases.push_back({"parent records out of order", cells_by_hand(ADMIN, {{6144, {5}, {0}}}), good_block, ADMIN});
    cases.push_back({"records past the end of the cells file", std::vector<char>(cells.begin(), cells.begin() + 8 + 136 + 2), good_block, ADMIN});
    cases.push_back({"a parent record past the end of the cells file", std::vector<char>(cells.begin(), cells.begin() + 8 + 100), good_block, ADMIN});
    cases.push_back({"a parent record past the end of the cells file", std::vector<char>(cells.begin(), cells.begin() + 8 + 134), good_block, ADMIN});
    cases.push_back({"a parent record without rows", cells_by_hand(ADMIN, {{0, {}, {}}}), {}, ADMIN});
    cases.push_back({"a block where none starts", cells_by_hand(ADMIN, {{0, {5, 6}, {1}}}), good_block, ADMIN});
    cases.push_back({"codes past a block's rows", cells, bytes({4, 1, 4, 0, 6, 5}), ADMIN});
    cases.push_back({"a block whose first row isn't PLAIN", cells, bytes({5, 0, 4, 0, 6, 5}), ADMIN});
    cases.push_back({"a row code of 3", cells, bytes({12, 0, 4, 0, 6, 5}), ADMIN});
    cases.push_back({"a list of more than 65535 ids", cells, bytes({4, 0, 0x80, 0x80, 0x08, 0}), ADMIN});
    cases.push_back({"a list of more than 65535 ids", cells, bytes({4, 0, 0xFE, 0xFF, 0x07, 4, 0}), ADMIN});
    cases.push_back({"an id past the ids a list holds", cells, bytes({4, 0, 2, 0, 1}), ADMIN});           // zigzag -1
    cases.push_back({"an id past the ids a list holds", cells, bytes({4, 0, 2, 0, 0x80, 0x80, 0x80, 0x80, 0x10}), ADMIN});
    cases.push_back({"an id past the ids a list holds", cells, bytes({4, 0, 4, 0, 6, 0xFF, 0xFF, 0xFF, 0xFF, 0x07}), ADMIN});
    // Row 1 an EDIT of {3, 9}.
    cases.push_back({"an edit op of 3", cells, bytes({8, 0, 4, 0, 6, 5, 3, 0}), ADMIN});
    cases.push_back({"edit ops past the list", cells, bytes({8, 0, 4, 0, 6, 5, 0x10, 0}), ADMIN});
    cases.push_back({"an edit of a list with a repeated id", cells, bytes({8, 0, 5, 0, 6, 0, 0, 0}), ADMIN});
    cases.push_back({"an edit to a list with a repeated id", cells, bytes({8, 0, 4, 0, 6, 5, 0, 1, 6}), ADMIN});
    cases.push_back({"an added id past 32 bits", cells, bytes({8, 0, 4, 0, 6, 5, 0, 1, 0x80, 0x80, 0x80, 0x80, 0x10}), ADMIN});
    cases.push_back({"a varint past 64 bits", cells, bytes({4, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}), ADMIN});
    cases.push_back({"a list past the end of the entries file", cells, bytes({4, 0, 4, 0, 6}), ADMIN});
    cases.push_back({"entries past the last list", cells, bytes({4, 0, 4, 0, 6, 5, 0}), ADMIN});
    // The tail.
    const size_t tail = cells.size() - (12 + 4 + FOOTER_BYTES);
    cases.push_back({"a tail that isn't the records'", [&] { auto c = cells; c.insert(c.end(), 0); return c; }(), good_block, ADMIN});
    cases.push_back({"a parent bitmap that isn't the records'", with(cells, tail, 3), good_block, ADMIN});
    cases.push_back({"ranks that aren't the bitmap's", with(cells, tail + 8, 1), good_block, ADMIN});
    cases.push_back({"a directory that isn't the records'", with(cells, tail + 12, 9), good_block, ADMIN});
    cases.push_back({"a footer that isn't the tail's", with(cells, tail + 20, 2), good_block, ADMIN});
    // Direct lists: two rows, the second sharing the first's offset.
    const std::vector<char> direct = cells_by_hand(POI, {{0, {5, 6}, {0, 0}}});
    CHECK_EQ(refusal_reading(direct, bytes({4, 0, 3, 0, 5}), POI), std::string());
    cases.push_back({"a list where none starts", cells_by_hand(POI, {{0, {5, 6}, {0, 2}}}), bytes({4, 0, 3, 0, 5}), POI});
    cases.push_back({"a list where none starts", cells_by_hand(POI, {{0, {5}, {1}}}), bytes({4, 0, 3, 0, 5}), POI});
    // Group varint gaps: {3, 4, 300}, lengths 1 and 2 bytes; then lengths past the list, and a gap in 2 bytes
    // that takes 1.
    CHECK_EQ(refusal_reading(cells_by_hand(POI, {{0, {5}, {0}}}), bytes({6, 0, 3, 0x04, 0, 0x2B, 1}), POI), std::string());
    cases.push_back({"gap lengths past a list", cells_by_hand(POI, {{0, {5}, {0}}}), bytes({6, 0, 3, 0x14, 0, 0x2B, 1}), POI});
    cases.push_back({"a gap longer than it takes", cells_by_hand(POI, {{0, {5}, {0}}}), bytes({6, 0, 3, 0x05, 0, 0, 0x2B, 1}), POI});
    for (const Case& c : cases) {
        const std::string got = refusal_reading(c.cells, c.entries, c.format);
        if (got != m + c.why) std::printf("    want %s, got %s\n", c.why, got.c_str());
        CHECK_EQ(got, m + c.why);
    }
}
