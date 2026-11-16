// A side file as it sits on disk: a u32 for each record of the record file it
// sits beside, in that file's order (way_parents.bin and way_postcodes.bin
// beside street_ways.bin, interp_postcodes.bin beside interp_ways.bin,
// addr_postcodes.bin beside addr_points.bin). The builder writes one and the
// patcher rebuilds it through Encoder; the server reads a value in place
// (server/src/side_blocks.rs). The patch tools diff and replay the logical
// form, the values as u32s back to back, which the file holds losslessly:
// NO_DATA is a value like any other. The encoding has no choices, so the
// values have one encoding, and the decoder refuses any other.
//
// The file, front to back:
//   the blocks, back to back, each from a byte: BLOCK_VALUES values a block
//     in the file's order, the last block shorter. A block of c values is in
//     the mode of fewer bits, FOR when they tie:
//       FOR         0x00, the least value (u32), w (u8), then each value
//                   less the least at w bits, w the bit length of the largest
//                   such difference: 48 + c x w bits. One value repeated is
//                   those 6 bytes, w = 0.
//       dictionary  0x01, k (u8), the k distinct values in the order they
//                   first come (u32 each), w (u8), then each value's index
//                   among them at w bits, w the bit length of k - 1:
//                   (3 + 4k) x 8 + c x w bits.
//     The fields are packed LSB first (coord_keys.h) and a block's last byte
//     is filled with zero bits. A block is BLOCK_MAX_BYTES at most;
//   the block table: the offset of each block's first byte (u32);
//   footer, FOOTER_BYTES: the values (u32), "SDB1".
// A file of no values is the footer alone. Value i is in block i /
// BLOCK_VALUES, and a bit reader's 8-byte loads past the last block's bits
// land in the table and the footer.
#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "coord_keys.h"

namespace side_blocks {

inline constexpr size_t BLOCK_VALUES = 64, TABLE_ENTRY_BYTES = 4, FOOTER_BYTES = 8, FOR_HEAD_BYTES = 6;
// FOR at 32 bits a value. A dictionary is written only where it is smaller.
inline constexpr size_t BLOCK_MAX_BYTES = FOR_HEAD_BYTES + BLOCK_VALUES * 4;
inline constexpr uint8_t MODE_FOR = 0, MODE_DICTIONARY = 1;
inline constexpr char MAGIC[4] = {'S', 'D', 'B', '1'};

// Every refusal names the file it is of.
[[noreturn]] inline void malformed(std::string_view file, const std::string& what) {
    throw std::runtime_error("Malformed " + std::string(file) + ": " + what);
}

inline void put32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
inline uint32_t get32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}

// The bits of a block of c values as FOR at w bits a value, and as a
// dictionary of k entries.
constexpr uint64_t for_bits(size_t c, uint32_t w) { return FOR_HEAD_BYTES * 8 + uint64_t(c) * w; }
constexpr uint64_t dictionary_bits(size_t c, size_t k) {
    return (3 + 4 * uint64_t(k)) * 8 + uint64_t(c) * coord_keys::bit_length(uint32_t(k - 1));
}

// A block's distinct values in the order they first come, and each value's
// index among them.
struct Dictionary {
    size_t k = 0;
    std::array<uint32_t, BLOCK_VALUES> entries;
    std::array<uint8_t, BLOCK_VALUES> index;
};

// Whether the c values at v (BLOCK_VALUES at most), which are w bits a value
// as FOR, take fewer bits as a dictionary: d is then theirs.
inline bool dictionary_wins(const uint32_t* v, size_t c, uint32_t w, Dictionary& d) {
    const uint64_t limit = for_bits(c, w);
    d.k = 0;
    for (size_t j = 0; j < c; j++) {
        size_t i = 0;
        while (i < d.k && d.entries[i] != v[j]) i++;
        if (i == d.k) {
            // An entry more never takes fewer bits: a dictionary that loses at this many loses.
            if (dictionary_bits(c, d.k + 1) >= limit) return false;
            d.entries[d.k++] = v[j];
        }
        d.index[j] = uint8_t(i);
    }
    return true;
}

// The block of the c values at v (1 to BLOCK_VALUES) to out, which has
// BLOCK_MAX_BYTES and 8 to spare; its bytes.
inline size_t encode_block(const uint32_t* v, size_t c, uint8_t* out) {
    uint32_t least = v[0], most = v[0];
    for (size_t j = 1; j < c; j++) {
        least = std::min(least, v[j]);
        most = std::max(most, v[j]);
    }
    uint32_t w = coord_keys::bit_length(most - least);
    Dictionary d;
    const bool dictionary = dictionary_wins(v, c, w, d);
    size_t head = FOR_HEAD_BYTES;
    if (dictionary) {
        w = coord_keys::bit_length(uint32_t(d.k - 1));
        out[0] = MODE_DICTIONARY;
        out[1] = uint8_t(d.k);
        for (size_t i = 0; i < d.k; i++) put32(out + 2 + 4 * i, d.entries[i]);
        out[2 + 4 * d.k] = uint8_t(w);
        head = 3 + 4 * d.k;
    } else {
        out[0] = MODE_FOR;
        put32(out + 1, least);
        out[5] = uint8_t(w);
    }
    // Zeroed, with the 8 bytes to spare put_bits asks for.
    const size_t bytes = (c * w + 7) / 8;
    std::memset(out + head, 0, bytes + 8);
    for (size_t j = 0; j < c; j++) coord_keys::put_bits(out + head, uint64_t(j) * w, dictionary ? d.index[j] : v[j] - least, w);
    return head + bytes;
}

// The values in, the blocks out through the sink (called with bytes and a
// length) as each fills. The caller writes the table and the footer after
// them: table() holds an offset a block, counted from the encoder's first
// block, which a caller that puts those blocks after others adds their bytes
// to (write_table_entry); it may take the offsets as they come.
template <class Sink>
class Encoder {
public:
    explicit Encoder(Sink blocks) : blocks_(blocks) {}

    void add(uint32_t v) {
        block_[held_++] = v;
        if (held_ == BLOCK_VALUES) flush_block();
    }
    void add(const uint32_t* v, size_t n) {
        for (size_t j = 0; j < n; j++) add(v[j]);
    }

    // Writes the last block.
    void finish() { flush_block(); }

    // The values taken, and the bytes of the blocks written.
    uint64_t values() const { return values_ + held_; }
    uint64_t block_bytes() const { return offset_; }
    // The offsets of the blocks written since the last clear_table.
    const std::vector<uint64_t>& table() const { return table_; }
    void clear_table() { table_.clear(); }

private:
    void flush_block() {
        if (!held_) return;
        const size_t bytes = encode_block(block_.data(), held_, bytes_.data());
        blocks_(static_cast<const uint8_t*>(bytes_.data()), bytes);
        table_.push_back(offset_);
        offset_ += bytes;
        values_ += held_;
        held_ = 0;
    }

    Sink blocks_;
    std::array<uint32_t, BLOCK_VALUES> block_;
    std::array<uint8_t, BLOCK_MAX_BYTES + 8> bytes_;
    std::vector<uint64_t> table_;
    uint64_t offset_ = 0, values_ = 0;
    size_t held_ = 0;
};

// The table entry of a block at `offset` of its file, to out
// (TABLE_ENTRY_BYTES). Refuses one the entry can't hold.
inline void write_table_entry(std::string_view file, uint64_t offset, uint8_t* out) {
    if (offset > UINT32_MAX) malformed(file, "blocks past 4 GiB");
    put32(out, uint32_t(offset));
}

// The footer of a file of `values` values. Refuses a count it can't hold.
inline std::array<uint8_t, FOOTER_BYTES> footer(std::string_view file, uint64_t values) {
    if (values > UINT32_MAX) malformed(file, "more than 2^32 - 1 values");
    std::array<uint8_t, FOOTER_BYTES> f;
    put32(f.data(), uint32_t(values));
    std::memcpy(f.data() + 4, MAGIC, 4);
    return f;
}

// The whole file of n values, in memory.
inline std::vector<uint8_t> encode(std::string_view file, const uint32_t* values, size_t n) {
    std::vector<uint8_t> out;
    Encoder enc([&out](const uint8_t* p, size_t len) { out.insert(out.end(), p, p + len); });
    enc.add(values, n);
    enc.finish();
    for (uint64_t offset : enc.table()) {
        uint8_t e[TABLE_ENTRY_BYTES];
        write_table_entry(file, offset, e);
        out.insert(out.end(), e, e + TABLE_ENTRY_BYTES);
    }
    const auto f = footer(file, n);
    out.insert(out.end(), f.begin(), f.end());
    return out;
}

// The footer of a file of size bytes: its values and blocks, and where the
// blocks end and the table starts.
struct Footer {
    uint32_t values = 0;
    uint64_t blocks = 0, table_at = 0;
    // Block b's values.
    size_t values_of(uint64_t b) const { return size_t(std::min<uint64_t>(BLOCK_VALUES, values - b * BLOCK_VALUES)); }
    // The bytes of the logical form.
    uint64_t logical_bytes() const { return 4 * uint64_t(values); }
};
// footer is the file's last FOOTER_BYTES (not read of a shorter file).
inline Footer read_footer(std::string_view file, const uint8_t* footer, uint64_t size) {
    if (size < FOOTER_BYTES) malformed(file, "shorter than its footer");
    if (std::memcmp(footer + 4, MAGIC, 4) != 0) malformed(file, "no SDB1 in its footer");
    Footer f;
    f.values = get32(footer);
    f.blocks = (uint64_t(f.values) + BLOCK_VALUES - 1) / BLOCK_VALUES;
    const uint64_t table = f.blocks * TABLE_ENTRY_BYTES + FOOTER_BYTES;
    if (table > size) malformed(file, "shorter than its block table");
    f.table_at = size - table;
    return f;
}

// Block b of a file, at p, to its count values at out; its bytes. avail is
// how many of the blocks' bytes there are from p (BLOCK_MAX_BYTES of them
// hold any block), and nothing past them is read. Refuses a block the encoder
// would not write.
inline size_t decode_block(std::string_view file, uint64_t b, const uint8_t* p, size_t avail, size_t count, uint32_t* out) {
    auto refuse = [&](const char* what) { malformed(file, "block " + std::to_string(b) + " " + what); };
    const char* const past = "runs past the blocks' end";
    // The w bits at bit `bit` of the fields, which start head bytes into the block: one 8-byte
    // load where the bytes from the field's first are 8.
    auto field = [&](size_t head, uint64_t bit, uint32_t w) {
        const size_t at = head + size_t(bit >> 3);
        if (at + 8 <= avail) return coord_keys::get_bits(p + head, bit, w);
        return coord_keys::get_bits_bounded(p + head, avail - head, bit, w);
    };
    if (avail < 2) refuse(past);
    if (p[0] == MODE_FOR) {
        if (avail < FOR_HEAD_BYTES) refuse(past);
        const uint32_t least = get32(p + 1), w = p[5];
        if (w > 32) refuse("has a width past 32 bits");
        const size_t bytes = FOR_HEAD_BYTES + (count * w + 7) / 8;
        if (bytes > avail) refuse(past);
        uint32_t most = 0;
        bool at_least = false;
        for (size_t j = 0; j < count; j++) {
            const uint32_t d = field(FOR_HEAD_BYTES, uint64_t(j) * w, w);
            if (d > UINT32_MAX - least) refuse("holds a value past 32 bits");
            most = std::max(most, d);
            at_least |= d == 0;
            out[j] = least + d;
        }
        if (!at_least) refuse("has a base that is not its least value");
        if (coord_keys::bit_length(most) != w) refuse("is wider than its values need");
        Dictionary d;
        if (dictionary_wins(out, count, w, d)) refuse("is FOR where a dictionary takes fewer bits");
        if (!coord_keys::padding_is_zero(p + FOR_HEAD_BYTES, uint64_t(count) * w)) refuse("has bits set past its last value");
        return bytes;
    }
    if (p[0] != MODE_DICTIONARY) refuse("is neither FOR nor a dictionary");
    const size_t k = p[1];
    if (k == 0 || k > count) refuse("has a dictionary of no entries, or of more than its values");
    const size_t head = 3 + 4 * k;
    if (head > avail) refuse(past);
    const uint32_t w = p[head - 1];
    if (w != coord_keys::bit_length(uint32_t(k - 1))) refuse("has indexes of another width than its dictionary needs");
    const size_t bytes = head + (count * w + 7) / 8;
    if (bytes > avail) refuse(past);
    size_t seen = 0;
    for (size_t j = 0; j < count; j++) {
        const uint32_t x = field(head, uint64_t(j) * w, w);
        if (x > seen || x >= k) refuse("has an index out of first-use order, or past its dictionary");
        seen += x == seen;
        out[j] = get32(p + 2 + 4 * x);
    }
    if (seen != k) refuse("has a dictionary entry no value uses");
    uint32_t least = UINT32_MAX, most = 0;
    for (size_t i = 0; i < k; i++) {
        const uint32_t e = get32(p + 2 + 4 * i);
        for (size_t m = 0; m < i; m++)
            if (get32(p + 2 + 4 * m) == e) refuse("has a dictionary entry twice");
        least = std::min(least, e);
        most = std::max(most, e);
    }
    if (dictionary_bits(count, k) >= for_bits(count, coord_keys::bit_length(most - least)))
        refuse("is a dictionary where FOR takes no more bits");
    if (!coord_keys::padding_is_zero(p + head, uint64_t(count) * w)) refuse("has bits set past its last value");
    return bytes;
}

// The values of a file (whole, in memory), block by block to
// put(const uint32_t* values, size_t count). Refuses a file the encoder would
// not write.
template <class Put>
void decode(std::string_view file, const uint8_t* bytes, size_t size, Put put) {
    const Footer f = read_footer(file, bytes + size - std::min(size, FOOTER_BYTES), size);
    std::array<uint32_t, BLOCK_VALUES> values;
    uint64_t at = 0;
    for (uint64_t b = 0; b < f.blocks; b++) {
        if (get32(bytes + f.table_at + b * TABLE_ENTRY_BYTES) != at)
            malformed(file, "block " + std::to_string(b) + " is not where the blocks before it end");
        const size_t count = f.values_of(b);
        at += decode_block(file, b, bytes + at, size_t(f.table_at - at), count, values.data());
        put(static_cast<const uint32_t*>(values.data()), count);
    }
    if (at != f.table_at) malformed(file, "bytes between its blocks and its table");
}

}  // namespace side_blocks
