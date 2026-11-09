// The geo index files (geo_index.h): random rows of every list shape read
// back through the streaming Reader (from memory, and from files through a
// small window with no rewinds) and the View, whose every block is the
// Writer's; the Writer's bytes are fixed (the server's reader and fixture
// writer check the same bytes, server/src/geo_index/tests.rs); the Writer
// refuses what it can't write and the Reader each malformed part.
#include "geo_index.h"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <random>
#include <string>
#include <vector>

#include "patch_format.h"
#include "scratch_dir.h"
#include "test_framework.h"

using gctest::refusal_of;
using namespace geo_index;

namespace {

struct Row {
    uint64_t cell;
    RowLists lists;
    bool operator==(const Row& o) const { return cell == o.cell && lists == o.lists; }
};
using Rows = std::vector<Row>;
using Files = std::pair<std::vector<char>, std::vector<char>>;

// The level-17 cell of number n (face * 4^17 + its position).
uint64_t cell(uint64_t n) { return cell_of(n, 17); }

Files encode(const Rows& rows) {
    Files out;
    Writer writer([&](const char* p, size_t n) { out.first.insert(out.first.end(), p, p + n); },
                  [&](const char* p, size_t n) { out.second.insert(out.second.end(), p, p + n); });
    for (const Row& r : rows) writer.add(r.cell, ids_of(r.lists));
    writer.finish();
    return out;
}

template <typename Src>
Rows decode_from(Src& top, Src& blocks, Src& footers) {
    Reader reader(top, blocks, footers);
    Rows out;
    Row r;
    while (reader.next(r.cell, r.lists)) out.push_back(r);
    return out;
}

Rows decode(const Files& files) {
    ByteSpan t{files.first.data(), files.first.size()}, b{files.second.data(), files.second.size()},
        f{files.second.data(), files.second.size()};
    return decode_from(t, b, f);
}

// The rows through the View, a block at a time, each block checked to be
// the Writer's.
Rows view_rows(const Files& files, bool& canonical) {
    View view(files.first.data(), files.first.size(), files.second.data(), files.second.size());
    View::Scratch s = view.scratch();
    Rows out;
    canonical = true;
    for (size_t b = 0; b < view.blocks(); b++) {
        view.rows_of(b, s, [&](uint64_t c, const RowLists& l) { out.push_back({c, l}); });
        canonical &= view.canonical(b, s);
    }
    return out;
}

// A list of one of many shapes: one id, runs, sparse ids with gaps of 1 to
// 5 bytes, repeated ids, ids at 0 and at the top of the u32 range, the
// table's list of the row before (SAME) and ones it has but for ids dropped
// or added (EDIT), and now and then 0xFFFF ids.
std::vector<uint32_t> shape(std::mt19937_64& rng, const std::vector<uint32_t>& prev) {
    std::vector<uint32_t> l;
    switch (rng() % 13) {
    case 0: l = {static_cast<uint32_t>(rng() % 1000)}; break;
    case 1: l = {0}; break;
    case 2: l = {0xFFFFFFFFu}; break;
    case 3: for (uint32_t v = 100; v < 140; v++) l.push_back(v); break;
    case 4: for (int k = 0; k < 30; k++) l.push_back(static_cast<uint32_t>(rng() >> (7 * (rng() % 5) + 32))); break;
    case 5: for (int k = 0; k < 12; k++) l.push_back(static_cast<uint32_t>(rng() % 50)); break;  // repeats
    case 6: case 7: l = prev; break;
    case 8: l = prev; if (l.size() > 1) l.erase(l.begin() + static_cast<long>(rng() % l.size())); break;
    case 9: l = prev; l.push_back(static_cast<uint32_t>(rng() % 6000)); break;
    case 10: l = prev; for (int k = 0; k < 3; k++) l.push_back(static_cast<uint32_t>(rng())); break;
    case 11:
        if (rng() % 40 == 0) for (uint32_t k = 0; k < LIST_MAX; k++) l.push_back(k * 3);
        else l = {static_cast<uint32_t>(rng() % 7), 0xFFFFFFF0u};
        break;
    default:
        for (uint32_t k = 0; k < 4; k++) {
            const uint32_t at[] = {0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455, 268435456};
            l.push_back(at[rng() % 9] + k);
        }
    }
    if (l.empty()) l = {1};
    std::sort(l.begin(), l.end());
    if (l.size() > LIST_MAX) l.resize(LIST_MAX);
    return l;
}

// `n` rows at `level`: steps of 1 to 3 and escaped ones of 4 up to a face
// or more, from the level's first cell (or near it) and up to its last, each
// with one to three tables' lists of every shape, addresses most often
// fresh ranges as the builder numbers them.
Rows random_rows(std::mt19937_64& rng, size_t n, int level = 17) {
    Rows out;
    const uint64_t cells = cells_at(level);
    uint64_t number = rng() % 2 ? 0 : rng() % 1000;
    std::array<std::vector<uint32_t>, TABLES> prev = {{{1}, {1}, {1}}};
    uint32_t next_addr = 0;
    for (size_t i = 0; i < n && number < cells; i++) {
        Row r{cell_of(number, level), {}};
        const unsigned mask = 1 + static_cast<unsigned>(rng() % 7);
        for (size_t t = 0; t < TABLES; t++) {
            if (!(mask >> t & 1)) continue;
            if (t == ADDRESSES && rng() % 3 != 0) {
                const uint32_t k = 1 + static_cast<uint32_t>(rng() % 20);
                for (uint32_t j = 0; j < k; j++) r.lists[t].push_back(next_addr + j);
                next_addr += k;
            } else {
                r.lists[t] = shape(rng, prev[t]);
            }
            prev[t] = r.lists[t];
        }
        out.push_back(r);
        const uint64_t kind = rng() % 10;
        const uint64_t step = kind < 4 ? 1 + kind % 3 : kind < 8 ? 4 + rng() % 200 : kind < 9 ? 1 + (rng() >> (rng() % 40 + 24)) : 3;
        if (step >= cells - number) break;
        number += step;
    }
    // The level's last cell.
    if (number + 1 < cells && rng() % 2) out.push_back({cell_of(cells - 1, level), {{{5}, {}, {}}}});
    return out;
}

void write_bytes(const std::string& path, const std::vector<char>& bytes) {
    std::ofstream(path, std::ios::binary).write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

}  // namespace

TEST(geo_index_random_rows_read_back_and_are_the_writers) {
    std::mt19937_64 rng(20261109);
    ScratchDir dir("gctest-geoindex");
    // Around a block's and a group's rows: one row, a block, a block and one, a group, a group and one, more.
    const size_t sizes[] = {1, 63, 64, 65, 200, 4095, 4096, 4097, 9000};
    for (size_t round = 0; round < std::size(sizes) + 3; round++) {
        const int level = round < std::size(sizes) ? 17 : round == std::size(sizes) ? 1 : round == std::size(sizes) + 1 ? 30 : 10;
        const Rows rows = random_rows(rng, round < std::size(sizes) ? sizes[round] : 3000, level);
        const Files files = encode(rows);
        CHECK(decode(files) == rows);
        bool canonical = false;
        CHECK(view_rows(files, canonical) == rows);
        CHECK(canonical);
        // From files through a 16-byte window: refilled mid-row, grown for long footers, read front to back.
        write_bytes(dir.path() + "/t.bin", files.first);
        write_bytes(dir.path() + "/b.bin", files.second);
        SequentialFileReader t(dir.path() + "/t.bin", 16), b(dir.path() + "/b.bin", 16), f(dir.path() + "/b.bin", 16);
        CHECK(decode_from(t, b, f) == rows);
        CHECK_EQ(t.rewinds() + b.rewinds() + f.rewinds(), uint64_t(0));
        // Every row's block, and every cell before it, as the View finds them.
        View view(files.first.data(), files.first.size(), files.second.data(), files.second.size());
        CHECK_EQ(view.rows(), uint64_t(rows.size()));
        for (size_t i = 0; i < rows.size(); i += 1 + rng() % 7) {
            const size_t b = view.block_of(rows[i].cell);
            CHECK_EQ(b, i / BLOCK_ROWS);
            CHECK_EQ(view.block_of(rows[i].cell - 2), i % BLOCK_ROWS == 0 ? (i == 0 ? view.blocks() : b - 1) : b);
        }
    }
}

// The Writer's bytes for a small index, which the server's tests read and
// its fixture writer writes: four rows in a block. Row 0: streets {3, 9}
// and address {100}, PLAIN. Row 1, a step of 1: the streets SAME, addresses
// {101, 101} (a repeat) and interpolation {7} PLAIN. Row 2, a step of 3: the
// streets an EDIT adding 12 (3 bytes, PLAIN 4), the interpolation SAME. Row
// 3, a step of 91 (escaped as 87): streets {200}, coded against the last
// PLAIN first, 3.
static const Rows GOLDEN = {{cell(5), {{{3, 9}, {100}, {}}}},
                            {cell(6), {{{3, 9}, {101, 101}, {7}}}},
                            {cell(9), {{{3, 9, 12}, {}, {7}}}},
                            {cell(100), {{{200}, {}, {}}}}};

// FNV-1a 64 of bytes: the long golden index's files, compared by hash.
static uint64_t fnv1a(const std::vector<char>& bytes) {
    uint64_t h = 0xcbf29ce484222325ull;
    for (char b : bytes) h = (h ^ static_cast<uint8_t>(b)) * 0x100000001b3ull;
    return h;
}

// The long golden index: 4,100 rows (65 blocks in 2 groups), each a step of
// k % 5 + 1 from the row before, streets {k / 3, k / 3 + 2} (in threes:
// PLAIN, SAME, SAME), addresses {k} on every other row, interpolations
// {k % 7, 9} on every seventh.
static Rows golden_long() {
    Rows out;
    uint64_t number = 1;
    for (uint32_t k = 0; k < 4100; k++) {
        Row r{cell(number), {}};
        r.lists[0] = {k / 3, k / 3 + 2};
        if (k % 2 == 0) r.lists[1] = {k};
        if (k % 7 == 0) r.lists[2] = {k % 7, 9};
        out.push_back(r);
        number += k % 5 + 1;
    }
    return out;
}

TEST(geo_index_writer_bytes_are_fixed) {
    const Files files = encode(GOLDEN);
    const std::vector<uint8_t> top = {'G', 'X', 'T', '2', 17, 64, 64, 0,  // header
                                      0, 0, 0, 0x2C, 0, 0, 0, 0, 22, 0, 0, 0,  // the group: cell 5, footer at 22
                                      4, 0, 0, 0, 0, 0, 0, 0};  // 4 rows
    const std::vector<uint8_t> blocks = {5, 4, 6, 5, 2, 0xC8, 0x01,  // row 0
                                         22, 5, 2, 0, 2, 14,  // row 1
                                         163, 0, 1, 18,  // row 2
                                         193, 87, 2, 0x8A, 0x03,  // row 3
                                         0, 0, 0, 0x2C, 0, 0, 0, 0, 0, 0, 0, 0};  // the footer: cell 5 at 0
    CHECK(std::vector<uint8_t>(files.first.begin(), files.first.end()) == top);
    CHECK(std::vector<uint8_t>(files.second.begin(), files.second.end()) == blocks);
    CHECK(decode(files) == GOLDEN);
    const Files long_files = encode(golden_long());
    CHECK_EQ(long_files.first.size(), size_t(8 + 2 * 12 + 8));
    CHECK_EQ(long_files.second.size(), size_t(15142));
    CHECK_EQ(fnv1a(long_files.first), 0x825ad00c323593c7ull);
    CHECK_EQ(fnv1a(long_files.second), 0xeccb53b69187e3e3ull);
    CHECK(decode(long_files) == golden_long());
}

// No rows: two empty files, as a region without streets has.
TEST(geo_index_an_index_of_no_rows_is_two_empty_files) {
    const Files none = encode({});
    CHECK(none.first.empty() && none.second.empty());
    CHECK(decode(none).empty());
    View view(nullptr, 0, nullptr, 0);
    CHECK_EQ(view.blocks(), size_t(0));
    CHECK_EQ(view.level(), -1);
}

TEST(geo_index_writer_refuses_what_it_cant_write) {
    auto refusal = [](const Rows& rows) { return refusal_of<std::exception>([&] { encode(rows); }); };
    std::vector<uint32_t> longest(LIST_MAX), past(LIST_MAX + 1);
    for (uint32_t k = 0; k < past.size(); k++) past[k] = k;
    CHECK_EQ(refusal({{cell(1), {{longest, longest, longest}}}}), std::string());
    CHECK_EQ(refusal({{cell(1), {{{1}, past, {}}}}}), std::string("Cell holds 65536 entries, more than 65535"));
    CHECK_EQ(refusal({{cell(1), {{{}, {}, {}}}}}), std::string("geo_index: a cell without lists"));
    CHECK_EQ(refusal({{cell(2), {{{1}, {}, {}}}}, {cell(1), {{{1}, {}, {}}}}}), std::string("geo_index::Writer: cells out of order"));
    CHECK_EQ(refusal({{cell(2), {{{1}, {}, {}}}}, {cell(2), {{{1}, {}, {}}}}}), std::string("geo_index::Writer: cells out of order"));
    CHECK_EQ(refusal({{cell(2), {{{1}, {}, {}}}}, {cell_of(9, 16), {{{1}, {}, {}}}}}),
             std::string("geo_index::Writer: cells of two levels"));
    CHECK(refusal({{cell_of(0, 0), {{{1}, {}, {}}}}}).find("is not a cell of levels 1 to 30") != std::string::npos);
    CHECK(refusal({{0, {{{1}, {}, {}}}}}).find("is not a cell of levels 1 to 30") != std::string::npos);
    CHECK(refusal({{uint64_t(6) << 61 | uint64_t(1) << 26, {{{1}, {}, {}}}}}).find("is not a cell of levels 1 to 30") !=
          std::string::npos);
    CHECK_EQ(refusal({{cell(1), {{{2, 1}, {}, {}}}}}), std::string("geo_index: a list out of order"));
    // Whole blocks: only the last can be short.
    auto blocks = [](std::vector<EncodedBlock> bs) {
        return refusal_of<std::exception>([&] {
            std::vector<char> t, b;
            Writer w([&](const char* p, size_t n) { t.insert(t.end(), p, p + n); },
                     [&](const char* p, size_t n) { b.insert(b.end(), p, p + n); });
            for (const auto& x : bs) w.add_block(x);
            w.finish();
        });
    };
    const EncodedBlock one{cell(1), cell(1), 1, {1, 2, 2}}, two{cell(3), cell(3), 1, {1, 2, 2}};
    CHECK_EQ(blocks({one}), std::string());
    CHECK_EQ(blocks({one, two}), std::string("geo_index::Writer: a block after a short one"));
    CHECK_EQ(blocks({{cell(1), cell(1), 0, {1}}}), std::string("geo_index::Writer: a block of 0 rows"));
    CHECK_EQ(blocks({{cell(1), cell(1), 65, {1}}}), std::string("geo_index::Writer: a block of 65 rows"));
    CHECK_EQ(blocks({{cell(5), cell(4), 2, {1}}}), std::string("geo_index::Writer: a block whose last cell isn't past its first"));
}

namespace {

// Files put together by hand: groups of blocks (a first cell and its rows'
// bytes each), the footers and the top they make; then any byte changed.
struct HandBlock {
    uint64_t first;
    std::vector<uint8_t> rows;
};
Files by_hand(const std::vector<std::vector<HandBlock>>& groups, uint64_t rows, uint8_t level = 17) {
    std::vector<uint8_t> top = {'G', 'X', 'T', '2', level, 64, 64, 0}, blocks;
    for (const auto& g : groups) {
        std::vector<uint8_t> footer;
        for (const HandBlock& b : g) {
            put_u64(footer, b.first);
            put_u32(footer, static_cast<uint32_t>(blocks.size()));
            blocks.insert(blocks.end(), b.rows.begin(), b.rows.end());
        }
        put_u64(top, g.empty() ? 0 : g[0].first);
        put_u32(top, static_cast<uint32_t>(blocks.size()));
        blocks.insert(blocks.end(), footer.begin(), footer.end());
    }
    put_u64(top, rows);
    return {std::vector<char>(top.begin(), top.end()), std::vector<char>(blocks.begin(), blocks.end())};
}
Files with(Files f, bool top, size_t at, uint8_t v) {
    (top ? f.first : f.second)[at] = static_cast<char>(v);
    return f;
}
std::string refusal_reading(const Files& f) { return refusal_of([&] { decode(f); }); }
std::string refusal_viewing(const Files& f) {
    return refusal_of([&] {
        bool canonical;
        view_rows(f, canonical);
    });
}
std::vector<uint8_t> repeat(std::vector<uint8_t> row, size_t n) {
    std::vector<uint8_t> out;
    for (size_t i = 0; i < n; i++) out.insert(out.end(), row.begin(), row.end());
    return out;
}

}  // namespace

// Each part the Reader and the View check, malformed by hand or in a
// Writer's files, and refused as it is: `read` the Reader's refusal, `view`
// the View's (none: it reads the files another way and refuses them for
// something else, or reads them fine).
TEST(geo_index_reader_refuses_each_malformed_part) {
    const std::string m = "Malformed geo index: ";
    // One block of two rows: streets {3, 9} PLAIN, then SAME.
    const Files one = by_hand({{{cell(5), {1, 4, 6, 5, 2}}}}, 2);
    CHECK_EQ(refusal_reading(one), std::string());
    CHECK_EQ(refusal_viewing(one), std::string());
    struct Case { const char *read, *view; Files files; };
    std::vector<Case> cases;
    auto rows = [&](std::vector<uint8_t> bytes, uint64_t n = 2) { return by_hand({{{cell(5), std::move(bytes)}}}, n); };
    auto both = [&](const char* why, Files f) { cases.push_back({why, why, std::move(f)}); };
    // The top file.
    both("blocks without a top file", {{}, one.second});
    both("a top file of no whole groups", {std::vector<char>(one.first.begin(), one.first.end() - 1), one.second});
    both("a top file of no whole groups", {std::vector<char>(one.first.begin(), one.first.begin() + 20), one.second});
    both("not a GXT2 top file", with(one, true, 3, '1'));
    both("a header past the format it takes", with(one, true, 4, 0));
    both("a header past the format it takes", with(one, true, 4, 31));
    both("a header past the format it takes", with(one, true, 5, 32));
    both("a header past the format it takes", with(one, true, 6, 128));
    both("a header past the format it takes", with(one, true, 7, 1));
    cases.push_back({"a top footer that isn't the rows'", "a row past its block's end", with(one, true, 20, 3)});
    both("a top footer that isn't the rows'", with(one, true, 20, 0));
    both("a top footer that isn't the rows'", with(one, true, 27, 1));
    // The groups and their footers.
    cases.push_back({"a group's footer out of place", "a last footer that doesn't end the blocks file", with(one, true, 16, 0)});
    both("a last footer that doesn't end the blocks file", with(one, true, 16, 4));
    both("a last footer that doesn't end the blocks file", {one.first, [&] { auto b = one.second; b.push_back(0); return b; }()});
    both("a group whose first cell isn't its first block's", with(one, true, 11, 0x30));
    cases.push_back({"a block where none starts", "an id past 32 bits", with(one, false, 13, 1)});
    cases.push_back({"a block where none starts", "a block out of place", with(one, false, 13, 5)});
    both("a block's first cell not at the index's level", by_hand({{{cell_of(5, 16), {1, 4, 6, 5, 2}}}}, 2));
    const Files two_blocks = by_hand({{{cell(5), repeat({1, 2, 2}, 64)}, {cell(80), {1, 2, 2}}}}, 65);
    CHECK_EQ(refusal_reading(two_blocks), std::string());
    CHECK_EQ(refusal_viewing(two_blocks), std::string());
    // The footer at 195 (64 rows of 3 bytes, one more): block 1 starting at 0, or past the footer.
    cases.push_back({"a block without rows", "a block out of place", with(two_blocks, false, 195 + 12 + 8, 0)});
    cases.push_back({"a block of more than 64 rows", "a block out of place", with(two_blocks, false, 195 + 12 + 9, 1)});
    // Rows.
    both("a step on a block's first row", rows({0x41, 4, 6, 5, 2}));
    both("a row without lists", rows({1, 4, 6, 5, 0}));
    both("a cell past the level's cells", rows({1, 4, 6, 5, 0xC2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x3F}));
    both("a cell past the level's cells", by_hand({{{cell(cells_at(17) - 2), {1, 4, 6, 5, 0x42}}}}, 2));
    both("an address list that isn't PLAIN", rows({5, 4, 6, 5, 2, 0xC8, 1, 8}));
    both("an address list that isn't PLAIN", rows({5, 4, 6, 5, 2, 0xC8, 1, 12, 0, 0}));
    both("a list against none before it", rows({2}, 1));
    both("a list against none before it", rows({32 | 1, 2, 2}, 1));
    both("an empty list", rows({1, 0, 6, 1, 2, 2}));
    both("a list of more than 65535 ids", rows({1, 0x80, 0x80, 0x08, 0, 1, 2, 2}));
    both("an id past 32 bits", rows({1, 2, 1, 1, 2, 2}));  // zigzag -1
    both("an id past 32 bits", rows({1, 2, 0x80, 0x80, 0x80, 0x80, 0x40, 1, 2, 2}));  // a first past 2^34
    both("an id past 32 bits", rows({1, 2, 0x80, 0x80, 0x80, 0x80, 0x20, 1, 2, 2}));  // 2^32
    both("an id past 32 bits", rows({1, 4, 6, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 1, 2, 2}));  // 3 + 2^32
    // Row 1 an EDIT of {3, 9}.
    both("edit ops past the list", rows({1, 4, 6, 5, 3, 0x04, 0}));
    both("an edit of a list with a repeated id", rows({1, 5, 6, 0, 3, 0, 0}));
    both("an empty list", rows({1, 4, 6, 5, 3, 3, 0}));
    both("an edit to a list with a repeated id", rows({1, 4, 6, 5, 3, 0, 1, 0}));
    both("an id past 32 bits", rows({1, 4, 6, 5, 3, 0, 1, 7}));  // 3 + zigzag -4
    both("an id past 32 bits", rows({1, 4, 6, 5, 3, 0, 2, 18, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F}));
    both("an id past 32 bits", rows({1, 4, 6, 5, 3, 0, 1, 0x80, 0x80, 0x80, 0x80, 0x40}));
    both("a list of more than 65535 ids", rows({1, 4, 6, 5, 3, 0, 0xFF, 0xFF, 0x03}));
    both("a varint past 64 bits", rows({1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 1, 2, 2}));
    // Blocks' extents.
    cases.push_back({"a row past its block's end", "a block of more rows than it holds", rows({1, 4, 6, 5, 2, 1})});
    both("a row past its block's end", rows({1, 4, 6}, 1));
    cases.push_back({"a top footer that isn't the rows'", "a block of more rows than it holds", rows({1, 4, 6, 5, 2, 2})});
    cases.push_back({"a top footer that isn't the rows'", "a block of more rows than it holds", rows({1, 4, 6, 5, 2}, 1)});
    cases.push_back({"a top footer that isn't the rows'", "a row past its block's end", rows({1, 4, 6, 5, 2}, 3)});
    cases.push_back({"a block of more than 64 rows", "a last footer that doesn't end the blocks file", rows(repeat({1, 2, 2}, 65), 65)});
    cases.push_back({"a block of fewer than 64 rows before the last", "a last footer that doesn't end the blocks file",
                     by_hand({{{cell(5), {1, 2, 2}}, {cell(7), repeat({1, 2, 2}, 2)}}}, 3)});
    both("blocks out of cell order", by_hand({{{cell(5), repeat({1, 2, 2}, 64)}, {cell(60), {1, 2, 2}}}}, 65));
    // Two groups from a Writer: the first group's footer moved near the end, the second's first cell changed.
    Rows long_rows;
    for (uint64_t k = 0; k < 4097; k++) long_rows.push_back({cell(2 * k + 1), {{{1}, {}, {}}}});
    const Files two = encode(long_rows);
    CHECK_EQ(refusal_reading(two), std::string());
    CHECK_EQ(refusal_viewing(two), std::string());
    Files near_end = two;
    const uint32_t at = static_cast<uint32_t>(two.second.size() - 100);
    std::memcpy(&near_end.first[16], &at, 4);
    both("a group's footer past the blocks file", near_end);
    both("a group whose first cell isn't its first block's", with(two, true, 20, 1));
    for (const Case& c : cases) {
        const std::string read = refusal_reading(c.files), view = refusal_viewing(c.files);
        const std::string want_read = c.read ? m + c.read : "", want_view = c.view ? m + c.view : "";
        if (read != want_read) std::printf("    read: want %s, got %s\n", want_read.c_str(), read.c_str());
        if (view != want_view) std::printf("    view: want %s, got %s\n", want_view.c_str(), view.c_str());
        CHECK_EQ(read, want_read);
        CHECK_EQ(view, want_view);
    }
    // A footer reader of another file.
    ByteSpan t{one.first.data(), one.first.size()}, b{one.second.data(), one.second.size()}, f{one.second.data(), 4};
    CHECK_EQ(refusal_of([&] { Reader r(t, b, f); }), m + "a footer reader of another file");
}

// Bytes a Writer didn't write for their rows decode, but aren't canonical: a
// PLAIN list where the Writer writes SAME, a PLAIN with the repeat flag on a
// list without repeats, and a first block that doesn't start the file.
TEST(geo_index_view_tells_the_writers_blocks_from_others) {
    auto canonical = [](const Files& f) {
        bool c = false;
        view_rows(f, c);
        return c;
    };
    CHECK(canonical(by_hand({{{cell(5), {1, 4, 6, 5, 2}}}}, 2)));
    CHECK(!canonical(by_hand({{{cell(5), {1, 4, 6, 5, 1, 4, 0, 5}}}}, 2)));
    CHECK(!canonical(by_hand({{{cell(5), {1, 5, 6, 5}}}}, 1)));
    std::vector<uint8_t> top = {'G', 'X', 'T', '2', 17, 64, 64, 0}, blocks = {0xAA, 1, 4, 6, 5, 2};
    put_u64(top, cell(5));
    put_u32(top, 6);
    put_u64(top, 2);
    put_u64(blocks, cell(5));
    put_u32(blocks, 1);
    const Files padded{std::vector<char>(top.begin(), top.end()), std::vector<char>(blocks.begin(), blocks.end())};
    CHECK_EQ(refusal_viewing(padded), std::string());
    CHECK(!canonical(padded));
}
