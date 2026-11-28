// The side files (side_blocks.h): the encoder's file decodes to the values it
// took, each block is in the mode of fewer bits, the values have one
// encoding, and the files the encoder would not write are refused.
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "coord_keys.h"
#include "side_blocks.h"
#include "test_framework.h"

namespace sb = side_blocks;
using gctest::refusal_of;

namespace {

// NO_DATA, which a side file holds as it holds any value.
constexpr uint32_t NONE = 0xFFFFFFFFu;
const char* const NAME = "way_postcodes.bin";

std::vector<uint8_t> encode(const std::vector<uint32_t>& values) { return sb::encode(NAME, values.data(), values.size()); }

std::vector<uint32_t> decode(const std::vector<uint8_t>& file) {
    std::vector<uint32_t> out;
    sb::decode(NAME, file.data(), file.size(), [&](const uint32_t* v, size_t n) { out.insert(out.end(), v, v + n); });
    return out;
}

sb::Footer footer_of(const std::vector<uint8_t>& file) {
    return sb::read_footer(NAME, file.data() + file.size() - std::min(file.size(), sb::FOOTER_BYTES), file.size());
}

// Where block b of a file starts, and where it ends: the next one's start, or
// the table's.
std::pair<size_t, size_t> block_range(const std::vector<uint8_t>& file, size_t b) {
    const sb::Footer f = footer_of(file);
    const uint8_t* table = file.data() + f.table_at;
    return {sb::get32(table + 4 * b), b + 1 < f.blocks ? sb::get32(table + 4 * (b + 1)) : size_t(f.table_at)};
}

// n values of one of four kinds: a postcode column (runs of one id, NO_DATA
// among them), parents (ids near each other, a few of them NO_DATA), any 32
// bits, and values within a range of some width.
std::vector<uint32_t> random_values(std::mt19937_64& rng, size_t n, int kind) {
    std::vector<uint32_t> out;
    uint32_t run = uint32_t(rng() % 100000);
    const uint32_t width = uint32_t(rng() % 33);
    const uint32_t low = uint32_t(rng());
    // One draw a statement: the same values whatever order a compiler evaluates arguments in.
    for (size_t i = 0; i < n; i++) {
        const uint64_t r = rng();
        if (kind == 0) {
            if (r % 9 == 0) run = uint32_t(rng() % 100000);
            out.push_back((r >> 8) % 6 == 0 ? NONE : run);
        } else if (kind == 1) {
            run += uint32_t(r % 5);
            out.push_back((r >> 8) % 40 == 0 ? NONE : run - uint32_t((r >> 16) % 3));
        } else if (kind == 2) {
            out.push_back(uint32_t(r));
        } else {
            const uint32_t d = width == 32 ? uint32_t(r) : uint32_t(r) & ((1u << width) - 1);
            out.push_back(d > NONE - low ? d : low + d);
        }
    }
    return out;
}

using Fields = std::vector<std::pair<uint32_t, uint32_t>>;  // value, width

// The fields a bit at a time, LSB first, the last byte's spare bits zero.
std::vector<uint8_t> packed(const Fields& fields) {
    std::vector<uint8_t> out;
    uint64_t bit = 0;
    for (const auto& [v, w] : fields)
        for (uint32_t k = 0; k < w; k++, bit++) {
            if (bit % 8 == 0) out.push_back(0);
            out[bit / 8] |= uint8_t(((v >> k) & 1) << (bit % 8));
        }
    return out;
}

void put32(std::vector<uint8_t>& out, uint32_t v) {
    for (int k = 0; k < 4; k++) out.push_back(uint8_t(v >> (8 * k)));
}

// A FOR block written by hand: the base, the width, and each value's
// difference from the base.
std::vector<uint8_t> for_block(uint32_t base, uint32_t w, const std::vector<uint32_t>& diffs) {
    std::vector<uint8_t> out = {0};
    put32(out, base);
    out.push_back(uint8_t(w));
    Fields fields;
    for (uint32_t d : diffs) fields.push_back({d, w});
    const std::vector<uint8_t> bits = packed(fields);
    out.insert(out.end(), bits.begin(), bits.end());
    return out;
}

// A dictionary block written by hand: the entries, the width, and each
// value's index.
std::vector<uint8_t> dictionary_block(const std::vector<uint32_t>& entries, uint32_t w, const std::vector<uint32_t>& indexes) {
    std::vector<uint8_t> out = {1, uint8_t(entries.size())};
    for (uint32_t e : entries) put32(out, e);
    out.push_back(uint8_t(w));
    Fields fields;
    for (uint32_t x : indexes) fields.push_back({x, w});
    const std::vector<uint8_t> bits = packed(fields);
    out.insert(out.end(), bits.begin(), bits.end());
    return out;
}

// The blocks with the table of their offsets and a footer of `values` values.
std::vector<uint8_t> file_of(const std::vector<std::vector<uint8_t>>& blocks, uint32_t values) {
    std::vector<uint8_t> out, table;
    for (const auto& b : blocks) {
        put32(table, uint32_t(out.size()));
        out.insert(out.end(), b.begin(), b.end());
    }
    out.insert(out.end(), table.begin(), table.end());
    put32(out, values);
    out.insert(out.end(), {'S', 'D', 'B', '1'});
    return out;
}

// What the decoder says of a file it refuses; "" for one it takes.
std::string refusal(const std::vector<uint8_t>& file) {
    return refusal_of([&] { decode(file); });
}

// The bits of a block of these values in each mode, counted apart from the
// encoder: FOR's and the dictionary's.
std::pair<uint64_t, uint64_t> costs(const std::vector<uint32_t>& v) {
    const auto [lo, hi] = std::minmax_element(v.begin(), v.end());
    const size_t c = v.size(), k = std::set<uint32_t>(v.begin(), v.end()).size();
    return {48 + c * coord_keys::bit_length(*hi - *lo), (3 + 4 * k) * 8 + c * coord_keys::bit_length(uint32_t(k - 1))};
}

// Whether a file of these values, one block of them, is that block in the
// mode of fewer bits (FOR when they tie) and of no more bytes than its bits.
bool block_is_the_smaller_mode(const std::vector<uint32_t>& v) {
    const std::vector<uint8_t> file = encode(v);
    const auto [for_bits, dictionary_bits] = costs(v);
    const bool dictionary = dictionary_bits < for_bits;
    const size_t bytes = file.size() - sb::TABLE_ENTRY_BYTES - sb::FOOTER_BYTES;
    return file[0] == (dictionary ? 1 : 0) && bytes == (std::min(for_bits, dictionary_bits) + 7) / 8 && decode(file) == v;
}

// c values that take the k distinct ones in turn.
std::vector<uint32_t> cycle(size_t c, const std::vector<uint32_t>& distinct) {
    std::vector<uint32_t> out;
    for (size_t j = 0; j < c; j++) out.push_back(distinct[j % distinct.size()]);
    return out;
}

}  // namespace

TEST(side_blocks_decode_to_the_values_the_encoder_took) {
    std::mt19937_64 rng(20261114);
    for (size_t n : {size_t(0), size_t(1), size_t(2), size_t(63), size_t(64), size_t(65), size_t(127), size_t(128), size_t(129),
                     size_t(5000)})
        for (int kind = 0; kind < 4; kind++) {
            const std::vector<uint32_t> values = random_values(rng, n, kind);
            const std::vector<uint8_t> file = encode(values);
            CHECK(decode(file) == values);
            CHECK(encode(decode(file)) == file);
            // The footer counts the values, the table holds an offset a block of 64, and the blocks
            // fill the file up to it.
            const size_t blocks = (n + 63) / 64;
            const sb::Footer f = footer_of(file);
            CHECK_EQ(f.values, uint32_t(n));
            CHECK_EQ(f.blocks, uint64_t(blocks));
            CHECK_EQ(f.table_at, uint64_t(file.size() - 4 * blocks - 8));
            CHECK_EQ(f.logical_bytes(), uint64_t(4 * n));
            if (blocks) {
                CHECK_EQ(block_range(file, 0).first, size_t(0));
                CHECK_EQ(f.values_of(blocks - 1), n - 64 * (blocks - 1));
                CHECK_EQ(f.values_of(0), std::min<size_t>(n, 64));
            }
            for (size_t b = 0; b < blocks; b++) {
                const auto [from, to] = block_range(file, b);
                CHECK(from < to && to - from <= sb::BLOCK_MAX_BYTES);
            }
        }
}

// The patcher reads a block at a time and holds no byte past it.
TEST(a_block_decodes_from_its_own_bytes_and_not_from_fewer) {
    std::mt19937_64 rng(3);
    for (int kind = 0; kind < 4; kind++) {
        const std::vector<uint32_t> values = random_values(rng, 1000, kind);
        const std::vector<uint8_t> file = encode(values);
        const sb::Footer f = footer_of(file);
        for (size_t b = 0; b < f.blocks; b++) {
            const auto [from, to] = block_range(file, b);
            const std::vector<uint8_t> own(file.begin() + from, file.begin() + to);
            const size_t count = f.values_of(b);
            std::array<uint32_t, sb::BLOCK_VALUES> out;
            CHECK_EQ(sb::decode_block(NAME, b, own.data(), own.size(), count, out.data()), own.size());
            CHECK(std::equal(out.begin(), out.begin() + count, values.begin() + 64 * b));
            // The same with the bytes after it in reach.
            out.fill(0);
            CHECK_EQ(sb::decode_block(NAME, b, file.data() + from, size_t(f.table_at) - from, count, out.data()), own.size());
            CHECK(std::equal(out.begin(), out.begin() + count, values.begin() + 64 * b));
            for (size_t cut : {own.size() - 1, own.size() / 2, size_t(1), size_t(0)}) {
                const std::vector<uint8_t> shorter(own.begin(), own.begin() + cut);
                const std::string said =
                    refusal_of([&] { sb::decode_block(NAME, b, shorter.data(), shorter.size(), count, out.data()); });
                CHECK(said.find("block " + std::to_string(b) + " runs past the blocks' end") != std::string::npos);
            }
        }
    }
}

TEST(a_block_is_its_mode_its_header_and_its_fields_lsb_first) {
    // FOR: the least value, three bits a value, each value less the least.
    CHECK(encode({1000, 1003, 1001, 1007, 1000}) ==
          (std::vector<uint8_t>{0x00, 0xE8, 0x03, 0x00, 0x00, 0x03, 0x58, 0x0E, 0, 0, 0, 0, 5, 0, 0, 0, 'S', 'D', 'B', '1'}));
    // A dictionary: the three values in the order they first come, two bits an index.
    CHECK(encode({NONE, 300, NONE, 5, 300, 5, NONE}) ==
          (std::vector<uint8_t>{0x01, 0x03, 0xFF, 0xFF, 0xFF, 0xFF, 0x2C, 0x01, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x02,
                                0x84, 0x09, 0,    0,    0,    0,    7,    0,    0,    0,    'S',  'D',  'B',  '1'}));
    // The hand-written blocks the refusals below are cut from are the encoder's.
    CHECK(encode({5, 6}) == file_of({for_block(5, 1, {0, 1})}, 2));
    CHECK(encode({0, 1u << 28, 0}) == file_of({dictionary_block({0, 1u << 28}, 1, {0, 1, 0})}, 3));
    std::vector<uint32_t> two_blocks(64, 9);
    two_blocks.push_back(NONE);
    CHECK(encode(two_blocks) == file_of({for_block(9, 0, {}), for_block(NONE, 0, {})}, 65));
}

TEST(a_block_of_one_value_repeated_is_for_in_six_bytes) {
    for (uint32_t v : {0u, 7u, 0x12345678u, NONE})
        for (size_t c : {size_t(1), size_t(2), size_t(64)}) {
            // FOR, the value and no bits a value; then the table's one offset and the footer.
            const std::vector<uint8_t> want = {0, uint8_t(v), uint8_t(v >> 8), uint8_t(v >> 16), uint8_t(v >> 24), 0,
                                               0, 0,          0,               0,                uint8_t(c),       0,
                                               0, 0,          'S',             'D',              'B',              '1'};
            CHECK(encode(std::vector<uint32_t>(c, v)) == want);
        }
}

TEST(a_block_is_a_dictionary_exactly_when_that_takes_fewer_bits) {
    // Two values 2 apart: 88 + c bits as a dictionary, 48 + 2c as FOR, the same at 40 values.
    CHECK(costs(cycle(40, {7, 9})).first == costs(cycle(40, {7, 9})).second);
    CHECK_EQ(encode(cycle(39, {7, 9}))[0], 0);
    CHECK_EQ(encode(cycle(40, {7, 9}))[0], 0);
    CHECK_EQ(encode(cycle(41, {7, 9}))[0], 1);
    // A full block of three values: 248 bits as a dictionary, 240 as FOR at 3 bits and 304 at 4.
    CHECK_EQ(encode(cycle(64, {0, 1, 7}))[0], 0);
    CHECK_EQ(encode(cycle(64, {0, 1, 8}))[0], 1);
    // An id among NO_DATA is 32 bits a value as FOR.
    CHECK_EQ(encode(cycle(64, {NONE, 12}))[0], 1);
    CHECK_EQ(encode(cycle(64, {NONE, 12})).size(), size_t(19 + 4 + 8));
    // The most entries a dictionary wins with: 52 of 64 values 32 bits apart, 2,072 bits against
    // FOR's 2,096; with 53 it is 2,104 and the block is FOR at its largest.
    std::vector<uint32_t> distinct;
    for (uint32_t k = 0; k < 53; k++) distinct.push_back(k == 52 ? NONE : k << 26);
    const std::vector<uint32_t> fifty_two(distinct.begin() + 1, distinct.end());
    CHECK_EQ(encode(cycle(64, fifty_two))[0], 1);
    CHECK_EQ(encode(cycle(64, fifty_two)).size(), size_t(259 + 4 + 8));
    CHECK_EQ(encode(cycle(64, distinct))[0], 0);
    CHECK_EQ(encode(cycle(64, distinct)).size(), sb::BLOCK_MAX_BYTES + 4 + 8);
    for (const auto& v : {cycle(39, {7, 9}), cycle(40, {7, 9}), cycle(41, {7, 9}), cycle(64, {0, 1, 7}), cycle(64, {0, 1, 8}),
                          cycle(64, {NONE, 12}), cycle(64, fifty_two), cycle(64, distinct)})
        CHECK(block_is_the_smaller_mode(v));
    // Blocks of every length, of few and of many distinct values, near each other and far apart.
    std::mt19937_64 rng(5);
    size_t wrong = 0, dictionaries = 0, fors = 0;
    for (int t = 0; t < 30000; t++) {
        const size_t c = 1 + rng() % 64;
        const size_t k = 1 + rng() % (t % 2 ? 6 : 64);
        const uint32_t width = uint32_t(rng() % 33);
        std::vector<uint32_t> pool;
        for (size_t i = 0; i < k; i++) {
            const uint32_t any = uint32_t(rng());
            pool.push_back(width == 32 ? any : any & ((1u << width) - 1));
        }
        std::vector<uint32_t> v;
        for (size_t j = 0; j < c; j++) v.push_back(pool[rng() % k]);
        wrong += !block_is_the_smaller_mode(v);
        const auto [for_bits, dictionary_bits] = costs(v);
        dictionaries += dictionary_bits < for_bits;
        fors += dictionary_bits >= for_bits;
    }
    CHECK_EQ(wrong, size_t(0));
    CHECK(dictionaries > 1000 && fors > 1000);
}

TEST(no_values_are_the_footer_alone) {
    const std::vector<uint8_t> file = encode({});
    CHECK(file == (std::vector<uint8_t>{0, 0, 0, 0, 'S', 'D', 'B', '1'}));
    CHECK(decode(file).empty());
    const sb::Footer f = footer_of(file);
    CHECK_EQ(f.values, 0u);
    CHECK_EQ(f.blocks, uint64_t(0));
    CHECK_EQ(f.table_at, uint64_t(0));
    CHECK_EQ(f.logical_bytes(), uint64_t(0));
}

// The builder encodes slices of whole blocks on its threads and places each
// at the bytes before it; the patcher feeds values as they come and spills
// the table as it grows.
TEST(the_blocks_are_the_same_however_the_values_come_and_wherever_slices_are_cut) {
    std::mt19937_64 rng(11);
    for (int kind = 0; kind < 4; kind++) {
        const size_t n = 3000 + rng() % 200;
        const std::vector<uint32_t> values = random_values(rng, n, kind);
        const std::vector<uint8_t> want = encode(values);
        auto into = [](std::vector<uint8_t>& v) { return [&v](const uint8_t* p, size_t len) { v.insert(v.end(), p, p + len); }; };

        // One encoder, the table taken as it grows.
        {
            std::vector<uint8_t> blocks, table;
            sb::Encoder enc(into(blocks));
            auto spill = [&] {
                for (uint64_t offset : enc.table()) {
                    uint8_t e[sb::TABLE_ENTRY_BYTES];
                    sb::write_table_entry(NAME, offset, e);
                    table.insert(table.end(), e, e + sb::TABLE_ENTRY_BYTES);
                }
                enc.clear_table();
            };
            for (size_t i = 0; i < n; i++) {
                enc.add(values[i]);
                if (rng() % 50 == 0) spill();
                CHECK_EQ(enc.values(), uint64_t(i + 1));
                // A block is written when its last value comes.
                CHECK_EQ(table.size() / 4 + enc.table().size(), (i + 1) / 64);
            }
            enc.finish();
            spill();
            CHECK_EQ(enc.block_bytes(), uint64_t(blocks.size()));
            blocks.insert(blocks.end(), table.begin(), table.end());
            const auto footer = sb::footer(NAME, enc.values());
            blocks.insert(blocks.end(), footer.begin(), footer.end());
            CHECK(blocks == want);
        }

        // Slices of whole blocks, each through its own encoder, its offsets from its first block.
        {
            std::vector<uint8_t> blocks, table;
            for (size_t at = 0; at < n;) {
                const size_t len = std::min(n - at, 64 * size_t(1 + rng() % 9));
                std::vector<uint8_t> slice;
                sb::Encoder enc(into(slice));
                enc.add(values.data() + at, len);
                enc.finish();
                for (uint64_t offset : enc.table()) {
                    uint8_t e[sb::TABLE_ENTRY_BYTES];
                    sb::write_table_entry(NAME, blocks.size() + offset, e);
                    table.insert(table.end(), e, e + sb::TABLE_ENTRY_BYTES);
                }
                blocks.insert(blocks.end(), slice.begin(), slice.end());
                at += len;
            }
            blocks.insert(blocks.end(), table.begin(), table.end());
            const auto footer = sb::footer(NAME, n);
            blocks.insert(blocks.end(), footer.begin(), footer.end());
            CHECK(blocks == want);
        }
    }
}

TEST(more_values_than_a_u32_counts_and_a_block_past_4_gib_are_refused) {
    uint8_t e[sb::TABLE_ENTRY_BYTES] = {};
    sb::write_table_entry(NAME, UINT32_MAX, e);
    CHECK_EQ(sb::get32(e), UINT32_MAX);
    const std::string past = refusal_of([&] { sb::write_table_entry(NAME, uint64_t(UINT32_MAX) + 1, e); });
    CHECK_EQ(past, std::string("Malformed way_postcodes.bin: blocks past 4 GiB"));
    CHECK(sb::footer(NAME, UINT32_MAX) == (std::array<uint8_t, sb::FOOTER_BYTES>{0xFF, 0xFF, 0xFF, 0xFF, 'S', 'D', 'B', '1'}));
    const std::string many = refusal_of([&] { sb::footer(NAME, uint64_t(UINT32_MAX) + 1); });
    CHECK_EQ(many, std::string("Malformed way_postcodes.bin: more than 2^32 - 1 values"));
}

TEST(the_decoder_refuses_each_file_the_encoder_would_not_write) {
    const uint32_t far = 1u << 28;
    struct Case {
        std::vector<uint8_t> file;
        std::string refusal;
    };
    const std::vector<uint8_t> good = file_of({for_block(5, 1, {0, 1})}, 2);
    auto edited = [](std::vector<uint8_t> file, size_t at, uint8_t byte) {
        file[at] = byte;
        return file;
    };
    // A zero byte between the block and the table.
    std::vector<uint8_t> gap = good;
    gap.insert(gap.begin() + 7, 0);
    // The table's second offset one past the first block's end.
    std::vector<uint8_t> moved = file_of({for_block(9, 0, {}), for_block(5, 1, {0, 1})}, 66);
    moved[13 + 4] = 7;
    const Case cases[] = {
        // The footer and the table.
        {{'S', 'D', 'B', '1'}, "shorter than its footer"},
        {{}, "shorter than its footer"},
        {edited(good, good.size() - 1, '2'), "no SDB1 in its footer"},
        {edited(good, good.size() - 8, 200), "shorter than its block table"},
        {edited(good, 7, 1), "block 0 is not where the blocks before it end"},
        {moved, "block 1 is not where the blocks before it end"},
        {gap, "bytes between its blocks and its table"},
        {file_of({for_block(5, 1, {0, 1})}, 9), "block 0 runs past the blocks' end"},
        {file_of({}, 1), "shorter than its block table"},
        {file_of({for_block(9, 0, {}), {}}, 65), "block 1 runs past the blocks' end"},
        // A mode byte that is neither.
        {file_of({edited(for_block(5, 1, {0, 1}), 0, 2)}, 2), "block 0 is neither FOR nor a dictionary"},
        // FOR: a width or a base the values do not need, a value past 32 bits, the other mode, padding.
        {file_of({for_block(5, 2, {0, 1})}, 2), "block 0 is wider than its values need"},
        {file_of({for_block(4, 2, {1, 2})}, 2), "block 0 has a base that is not its least value"},
        {file_of({for_block(5, 33, {})}, 2), "block 0 has a width past 32 bits"},
        {file_of({for_block(NONE, 1, {0, 1})}, 2), "block 0 holds a value past 32 bits"},
        {file_of({for_block(0, 29, {0, far, 0})}, 3), "block 0 is FOR where a dictionary takes fewer bits"},
        {file_of({edited(for_block(5, 1, {0, 1}), 6, 0x82)}, 2), "block 0 has bits set past its last value"},
        {file_of({for_block(9, 0, {}), for_block(5, 2, {0, 1})}, 66), "block 1 is wider than its values need"},
        // A dictionary: the other mode (fewer bits, and as many), its entries, its width, its indexes.
        {file_of({dictionary_block({5, 6}, 1, {0, 1})}, 2), "block 0 is a dictionary where FOR takes no more bits"},
        {file_of({dictionary_block({7, 9}, 1, cycle(40, {0, 1}))}, 40), "block 0 is a dictionary where FOR takes no more bits"},
        {file_of({dictionary_block({}, 0, {})}, 2), "block 0 has a dictionary of no entries, or of more than its values"},
        {file_of({dictionary_block({0, far, 5}, 2, {0, 1})}, 2),
         "block 0 has a dictionary of no entries, or of more than its values"},
        {file_of({dictionary_block({0, far}, 2, {0, 1, 0})}, 3),
         "block 0 has indexes of another width than its dictionary needs"},
        {file_of({dictionary_block({far, 0}, 1, {1, 0, 1})}, 3),
         "block 0 has an index out of first-use order, or past its dictionary"},
        {file_of({dictionary_block({0, far, 5}, 2, {0, 1, 2, 3})}, 4),
         "block 0 has an index out of first-use order, or past its dictionary"},
        {file_of({dictionary_block({0, far, 5}, 2, {0, 1, 0})}, 3), "block 0 has a dictionary entry no value uses"},
        {file_of({dictionary_block({0, far, 0}, 2, {0, 1, 2})}, 3), "block 0 has a dictionary entry twice"},
        {file_of({edited(dictionary_block({0, far}, 1, {0, 1, 0}), 11, 0x0A)}, 3), "block 0 has bits set past its last value"},
    };
    for (const Case& c : cases) {
        const std::string got = refusal(c.file), want = "Malformed way_postcodes.bin: " + c.refusal;
        CHECK_EQ(got, want);
        if (got != want) std::printf("    want %s, got %s\n", want.c_str(), got.c_str());
    }
    CHECK_EQ(refusal(good), std::string());
    CHECK_EQ(refusal(file_of({dictionary_block({0, far}, 1, {0, 1, 0})}, 3)), std::string());
    CHECK_EQ(refusal(file_of({dictionary_block({7, 9}, 1, cycle(41, {0, 1}))}, 41)), std::string());
}

// Every file a byte changed in, or cut short, either is refused or decodes
// to values whose encoding it is: no values have two encodings.
TEST(side_values_have_one_encoding) {
    std::mt19937_64 rng(77);
    size_t refused = 0, decoded = 0;
    for (int kind = 0; kind < 4; kind++) {
        const std::vector<uint8_t> f = encode(random_values(rng, 300, kind));
        for (int t = 0; t < 4000; t++) {
            std::vector<uint8_t> g = f;
            if (t % 50 == 0) {
                g.resize(rng() % g.size());
            } else {
                const size_t at = rng() % g.size();
                g[at] ^= uint8_t(1u << (rng() % 8));
            }
            // Only the decoder may refuse.
            std::vector<uint32_t> values;
            try {
                values = decode(g);
            } catch (const std::runtime_error&) {
                refused++;
                continue;
            }
            decoded++;
            CHECK(encode(values) == g);
        }
    }
    CHECK(refused > 0 && decoded > 0);
}

// The same of blocks written by hand, of any base, width, entries and
// indexes: the ones the decoder takes are the ones the encoder writes.
TEST(a_hand_written_block_decodes_only_where_it_is_the_encoders) {
    std::mt19937_64 rng(78);
    size_t refused = 0, decoded[2] = {0, 0};
    for (int t = 0; t < 60000; t++) {
        const size_t c = 1 + rng() % (t % 4 ? 6 : 64);
        const bool dictionary = rng() % 2;
        std::vector<uint8_t> block;
        if (dictionary) {
            // A few entries, far apart or near enough to repeat, and any index of the width.
            const size_t k = 1 + rng() % 5;
            const uint32_t w = coord_keys::bit_length(uint32_t(k - 1)) + (rng() % 8 == 0);
            std::vector<uint32_t> entries, indexes;
            for (size_t i = 0; i < k; i++) {
                const uint32_t any = uint32_t(rng());
                entries.push_back(t % 2 ? any : any % 4);
            }
            for (size_t j = 0; j < c; j++) indexes.push_back(uint32_t(rng() % (uint64_t(1) << w)));
            block = dictionary_block(entries, w, indexes);
        } else {
            const uint32_t w = uint32_t(rng() % (t % 3 ? 4 : 33));
            const uint32_t base = t % 5 ? uint32_t(rng() % 100) : uint32_t(rng());
            std::vector<uint32_t> diffs;
            for (size_t j = 0; j < c; j++) {
                const uint32_t any = uint32_t(rng());
                diffs.push_back(w == 32 ? any : any & ((1u << w) - 1));
            }
            block = for_block(base, w, diffs);
        }
        const std::vector<uint8_t> file = file_of({block}, uint32_t(c));
        std::vector<uint32_t> values;
        try {
            values = decode(file);
        } catch (const std::runtime_error&) {
            refused++;
            continue;
        }
        decoded[dictionary]++;
        CHECK(encode(values) == file);
    }
    CHECK(refused > 10000 && decoded[0] > 5000 && decoded[1] > 500);
}

TEST(side_blocks_are_the_shared_vectors_byte_for_byte) {
    // geocoder/testdata/side_blocks.tsv, which the server's reader and test encoder read too.
    std::ifstream in(std::string(GEOCODER_TESTDATA_DIR) + "/side_blocks.tsv");
    REQUIRE(in.good());
    auto bytes = [](const std::string& hex) {
        std::vector<uint8_t> out;
        for (size_t i = 0; i + 1 < hex.size(); i += 2) out.push_back(uint8_t(std::stoi(hex.substr(i, 2), nullptr, 16)));
        return out;
    };
    size_t cases = 0, dictionaries = 0, fors = 0;
    bool header = true;
    for (std::string line; std::getline(in, line);) {
        if (line.empty() || line[0] == '#') continue;
        if (header) {
            header = false;
            continue;
        }
        std::vector<std::string> field;
        size_t a = 0;
        for (size_t b; (b = line.find('\t', a)) != std::string::npos; a = b + 1) field.push_back(line.substr(a, b - a));
        field.push_back(line.substr(a));
        REQUIRE(field.size() == 3);
        std::vector<uint32_t> values;
        if (field[1] != "-") {
            std::stringstream vs(field[1]);
            for (std::string v; std::getline(vs, v, ',');) values.push_back(uint32_t(std::stoul(v)));
        }
        const std::vector<uint8_t> want = bytes(field[2]);
        CHECK(encode(values) == want);
        CHECK(decode(want) == values);
        for (size_t b = 0; b < footer_of(want).blocks; b++) (want[block_range(want, b).first] ? dictionaries : fors)++;
        cases++;
    }
    CHECK(cases >= 75);
    CHECK(dictionaries >= 20 && fors >= 20);
}

TEST(search_offset_and_importance_columns_use_the_side_block_codec) {
    for (const std::string kind : {"place", "admin", "poi"}) {
        for (const std::string suffix : {"_tags.bin", "_imp.bin"}) {
            const auto name = kind + suffix;
            size_t registered = 0;
            for (const auto& file : sb::SIDE_FILES) registered += file.file == name;
            CHECK(registered == 1);
            const std::vector<uint32_t> values(sb::BLOCK_VALUES, suffix == "_imp.bin" ? 0 : NONE);
            const auto bytes = sb::encode(name, values.data(), values.size());
            CHECK(decode(bytes) == values);
            CHECK(bytes.size() == sb::FOR_HEAD_BYTES + sb::TABLE_ENTRY_BYTES + sb::FOOTER_BYTES);
        }
    }
}
