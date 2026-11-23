// The postcode grid as it sits on disk (postcode_grid.bin): in the countries
// it covers, the postcode of each S2 cell at the grid's level, as runs of
// cells in cell order. The builder writes it (postcode_grid_build.h says
// what each cell holds) and the patcher rebuilds it through Encoder; the
// server reads it in place (server/src/postcode_grid.rs). The patch tools
// diff and replay its logical form, which the file holds losslessly: a
// 24-byte LogicalRow a run, its first cell (an S2 cell id at the grid's
// level), its country and its postcode, country 0 and no code for cells of
// no postcode, ascending by cell, each of another (country, code) than the
// row before it, the first of a postcode. A run holds every cell from its
// own to the next row's, the last one every cell after it; a cell before the
// first row has no postcode. The grid holds its postcodes as text, so it
// reads alike beside any dir's strings. The encoding has no choices, so a
// logical stream has one encoding, and the decoder refuses any other.
//
// postcode_grid.bin:
//   the blocks, back to back, each from a byte: the rows in blocks of
//     BLOCK_ROWS, the last one shorter. A block holds its entries, the
//     distinct (country, code) of its rows: their count (a varint), then
//     each one's country (u16) and code's bytes and a 0 byte, ascending by
//     country and then by the code's bytes; its parameter (a byte); and its
//     bits, LSB first, the last byte filled with zero bits: row 0's entry (its
//     index, in index_bits(count) bits), then per row after it its step, the
//     cells from the row before it's less 1, as the Rice code of the
//     parameter (put_rice64), and its entry's rank among the entries other
//     than the row before it's (in index_bits(count - 1) bits). The parameter
//     is the least k of the fewest bits for the block's steps (best_rice64_k),
//     0 for a block of one row.
//   the block table, TABLE_ENTRY_BYTES a block: row 0's cell (u64) and the
//     offset of the block (u32).
//   footer, FOOTER_BYTES: the rows (u32), the level (u8), three 0 bytes,
//     "PGR1". A grid of no rows is the footer alone, of level 0.
// A country is its two upper-case letters, the first in the high byte; a
// code at most CODE_BYTES bytes of printable ASCII (0x20 to 0x7E).
#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "coord_keys.h"

namespace postcode_grid {

inline constexpr size_t LOGICAL_STRIDE = 24, CODE_BYTES = 14, BLOCK_ROWS = 128, TABLE_ENTRY_BYTES = 12,
                        FOOTER_BYTES = 12;
inline constexpr char MAGIC[4] = {'P', 'G', 'R', '1'};
inline constexpr const char* FILE_NAME = "postcode_grid.bin";
// The finest level a grid can be at: its cells' positions and a code's index
// share a u64 (postcode_grid_build.h).
inline constexpr int MAX_LEVEL = 20;
// A Rice code's zero bits before its escape, and the parameters a block may
// give.
inline constexpr uint32_t RICE_ESCAPE = 12, RICE_K_MAX = 63;

// A run as the patch tools diff and replay it.
struct LogicalRow {
    uint64_t cell = 0;
    uint16_t country_code = 0;
    char code[CODE_BYTES] = {};
};
static_assert(sizeof(LogicalRow) == LOGICAL_STRIDE, "LogicalRow must be 24 bytes");

[[noreturn]] inline void malformed(const std::string& what) { throw std::runtime_error("Malformed postcode grid: " + what); }

inline std::string_view code_of(const LogicalRow& r) { return std::string_view(r.code, strnlen(r.code, CODE_BYTES)); }
inline bool same_postcode(const LogicalRow& a, const LogicalRow& b) {
    return a.country_code == b.country_code && std::memcmp(a.code, b.code, CODE_BYTES) == 0;
}
// (country, code) order, the entries' and the vote's tie-break.
inline bool postcode_less(uint16_t ca, std::string_view a, uint16_t cb, std::string_view b) {
    return ca != cb ? ca < cb : a < b;
}
inline LogicalRow row_of(uint64_t cell, uint16_t country, std::string_view code) {
    if (code.size() > CODE_BYTES) malformed("a code longer than " + std::to_string(CODE_BYTES) + " bytes");
    LogicalRow r;
    r.cell = cell;
    r.country_code = country;
    std::memcpy(r.code, code.data(), code.size());
    return r;
}

// The bits a cell id's position at `level` starts past, and back.
inline constexpr int cell_shift(int level) { return 2 * (30 - level) + 1; }
inline constexpr uint64_t cell_at(uint64_t pos, int level) {
    return pos << cell_shift(level) | uint64_t(1) << (cell_shift(level) - 1);
}
// The positions a level's cells take along the curve: six faces of 4^level.
inline constexpr uint64_t cells_at(int level) { return uint64_t(6) << (2 * level); }
// The level of a cell id: false for an id of no level (no set bit, or one
// of a face's bits only).
inline bool level_of(uint64_t cell, int& level) {
    if (!cell) return false;
    const int low = __builtin_ctzll(cell);
    if (low % 2 || low > 60 || (cell >> 61) > 5) return false;
    level = 30 - low / 2;
    return true;
}

// The fewest bits that hold an index below n (0 for n of at most 1).
inline constexpr uint32_t index_bits(size_t n) { return n <= 1 ? 0 : 64 - uint32_t(__builtin_clzll(uint64_t(n - 1))); }

// The bits of z's Rice code with parameter k.
inline constexpr uint64_t rice64_bits(uint64_t z, uint32_t k) {
    return (z >> k) < RICE_ESCAPE ? (z >> k) + 1 + k : RICE_ESCAPE + 64;
}
// The least k of RICE_K_MAX at most for the fewest bits of z[0, n), 0 for
// none.
inline uint32_t best_rice64_k(const uint64_t* z, size_t n) {
    uint64_t best = UINT64_MAX;
    uint32_t best_k = 0;
    for (uint32_t k = 0; n && k <= RICE_K_MAX; k++) {
        uint64_t bits = 0;
        for (size_t i = 0; i < n; i++) bits += rice64_bits(z[i], k);
        if (bits < best) best = bits, best_k = k;
    }
    return best_k;
}

// Bits LSB first into a byte vector, which grows to hold them.
struct BitWriter {
    std::vector<uint8_t>& out;
    uint64_t bit = 0;
    void put(uint64_t v, uint32_t w) {
        for (uint32_t i = 0; i < w; i++, bit++) {
            if (bit / 8 >= out.size()) out.push_back(0);
            out[bit / 8] |= uint8_t((v >> i & 1) << (bit % 8));
        }
    }
    void rice(uint64_t z, uint32_t k) {
        const uint64_t q = z >> k;
        if (q >= RICE_ESCAPE) {
            put(0, RICE_ESCAPE);
            put(z, 64);
            return;
        }
        put(0, uint32_t(q));
        put(1, 1);
        put(z, k);
    }
};
// Bits LSB first from [p, p + size): false once a read passes the end.
struct BitReader {
    const uint8_t* p;
    size_t size;
    uint64_t bit = 0;
    bool get(uint32_t w, uint64_t& v) {
        v = 0;
        for (uint32_t i = 0; i < w; i++, bit++) {
            if (bit / 8 >= size) return false;
            v |= uint64_t(p[bit / 8] >> (bit % 8) & 1) << i;
        }
        return true;
    }
    bool rice(uint32_t k, uint64_t& z) {
        uint64_t q = 0, b;
        for (;; q++) {
            if (!get(1, b)) return false;
            if (b) break;
            if (q + 1 == RICE_ESCAPE) return get(64, z);
        }
        uint64_t low;
        if (!get(k, low)) return false;
        z = q << k | low;
        return true;
    }
};

inline void put16(uint8_t* p, uint16_t v) { std::memcpy(p, &v, 2); }
inline void put32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
inline void put64(uint8_t* p, uint64_t v) { std::memcpy(p, &v, 8); }
inline uint16_t get16(const uint8_t* p) {
    uint16_t v;
    std::memcpy(&v, p, 2);
    return v;
}
inline uint32_t get32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}
inline uint64_t get64(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    return v;
}

// Whether a row is one the format holds: a cell of a level the grid can be
// at, a country of two upper-case letters with a code of printable ASCII,
// or country 0 and no code.
inline void check_row(const LogicalRow& r) {
    int level = 0;
    if (!level_of(r.cell, level) || level > MAX_LEVEL) malformed("a row whose cell is no cell of level 0 to 20");
    const std::string_view code = code_of(r);
    for (size_t i = code.size(); i < CODE_BYTES; i++)
        if (r.code[i]) malformed("a code with bytes after its end");
    for (char c : code)
        if (c < 0x20 || c > 0x7E) malformed("a code of other than printable ASCII");
    const char hi = char(r.country_code >> 8), lo = char(r.country_code & 0xFF);
    if (r.country_code == 0) {
        if (!code.empty()) malformed("a code without a country");
    } else if (hi < 'A' || hi > 'Z' || lo < 'A' || lo > 'Z') {
        malformed("a country of other than two upper-case letters");
    } else if (code.empty()) {
        malformed("a country without a code");
    }
}

// Block rows r[0, n) (1 to BLOCK_ROWS, of one level) as their bytes, to `out`
// (cleared).
inline void encode_block(const LogicalRow* r, size_t n, int level, std::vector<uint8_t>& out) {
    out.clear();
    std::vector<const LogicalRow*> entries;
    for (size_t i = 0; i < n; i++) entries.push_back(r + i);
    auto less = [](const LogicalRow* a, const LogicalRow* b) {
        return postcode_less(a->country_code, code_of(*a), b->country_code, code_of(*b));
    };
    std::sort(entries.begin(), entries.end(), less);
    entries.erase(std::unique(entries.begin(), entries.end(), [](const LogicalRow* a, const LogicalRow* b) { return same_postcode(*a, *b); }),
                  entries.end());
    uint8_t varint[coord_keys::VARINT_MAX_BYTES];
    out.insert(out.end(), varint, varint + coord_keys::put_varint(varint, entries.size()));
    for (const LogicalRow* e : entries) {
        uint8_t cc[2];
        put16(cc, e->country_code);
        out.insert(out.end(), cc, cc + 2);
        const std::string_view code = code_of(*e);
        out.insert(out.end(), code.begin(), code.end());
        out.push_back(0);
    }
    auto index = [&](const LogicalRow& row) {
        return size_t(std::lower_bound(entries.begin(), entries.end(), &row, less) - entries.begin());
    };
    uint64_t steps[BLOCK_ROWS];
    for (size_t i = 1; i < n; i++) steps[i - 1] = ((r[i].cell - r[i - 1].cell) >> cell_shift(level)) - 1;
    const uint32_t k = best_rice64_k(steps, n - 1);
    out.push_back(uint8_t(k));
    std::vector<uint8_t> bits;
    BitWriter w{bits};
    const uint32_t entry_bits = index_bits(entries.size()), rank_bits = index_bits(entries.size() - 1);
    w.put(index(r[0]), entry_bits);
    for (size_t i = 1; i < n; i++) {
        w.rice(steps[i - 1], k);
        const size_t prev = index(r[i - 1]), at = index(r[i]);
        w.put(at < prev ? at : at - 1, rank_bits);
    }
    out.insert(out.end(), bits.begin(), bits.end());
}

// The logical stream in, postcode_grid.bin out through the sink (called with
// bytes and a length), front to back but for the table: the blocks go to the
// sink, and the caller writes the table (an entry a block, table(), which it
// may take as they come) and the footer after them. Holds a block of rows.
// Refuses a stream the file can't hold.
template <class Sink>
class Encoder {
public:
    explicit Encoder(Sink sink) : sink_(std::move(sink)) {}
    Encoder(const Encoder&) = delete;
    Encoder& operator=(const Encoder&) = delete;

    void add(const LogicalRow& r) {
        if (finished_) throw std::logic_error("postcode_grid::Encoder: add after finish");
        check_row(r);
        int level = 0;
        level_of(r.cell, level);
        if (rows_ == 0) {
            if (r.country_code == 0) malformed("a first row of no postcode");
            level_ = level;
        } else {
            if (level != level_) malformed("rows of two levels");
            if (r.cell <= last_.cell) malformed("rows out of cell order");
            if (same_postcode(r, last_)) malformed("a row of the postcode of the row before it");
        }
        if (rows_ == UINT32_MAX - 1) malformed("more than 2^32 - 2 rows: their count is a u32");
        last_ = r;
        rows_++;
        block_[block_rows_++] = r;
        if (block_rows_ == BLOCK_ROWS) flush_block();
    }

    // Writes the last block.
    void finish() {
        if (finished_) return;
        finished_ = true;
        flush_block();
    }

    // The footer, after the table.
    std::array<uint8_t, FOOTER_BYTES> footer() const {
        std::array<uint8_t, FOOTER_BYTES> f{};
        put32(f.data(), rows_);
        f[4] = uint8_t(rows_ ? level_ : 0);
        std::memcpy(f.data() + 8, MAGIC, 4);
        return f;
    }

    uint32_t rows() const { return rows_; }
    uint64_t block_bytes() const { return offset_; }
    // The table entries of the blocks written since the last clear_table.
    const std::vector<std::array<uint8_t, TABLE_ENTRY_BYTES>>& table() const { return table_; }
    void clear_table() { table_.clear(); }
    // The bytes of the block written last.
    const std::vector<uint8_t>& last_block() const { return bytes_; }

private:
    void flush_block() {
        if (!block_rows_) return;
        if (offset_ > UINT32_MAX) malformed("postcode_grid.bin's blocks past 4 GiB");
        encode_block(block_.data(), block_rows_, level_, bytes_);
        std::array<uint8_t, TABLE_ENTRY_BYTES> entry;
        put64(entry.data(), block_[0].cell);
        put32(entry.data() + 8, uint32_t(offset_));
        table_.push_back(entry);
        sink_(bytes_.data(), bytes_.size());
        offset_ += bytes_.size();
        block_rows_ = 0;
    }

    Sink sink_;
    std::array<LogicalRow, BLOCK_ROWS> block_{};
    size_t block_rows_ = 0;
    std::vector<uint8_t> bytes_;
    std::vector<std::array<uint8_t, TABLE_ENTRY_BYTES>> table_;
    uint64_t offset_ = 0;
    uint32_t rows_ = 0;
    int level_ = 0;
    LogicalRow last_{};
    bool finished_ = false;
};

// A whole file of the rows, in memory.
inline std::vector<uint8_t> encode_file(const std::vector<LogicalRow>& rows) {
    std::vector<uint8_t> out;
    Encoder enc([&out](const uint8_t* p, size_t n) { out.insert(out.end(), p, p + n); });
    for (const LogicalRow& r : rows) enc.add(r);
    enc.finish();
    for (const auto& e : enc.table()) out.insert(out.end(), e.begin(), e.end());
    const auto f = enc.footer();
    out.insert(out.end(), f.begin(), f.end());
    return out;
}

// The footer of a postcode_grid.bin of size bytes: its rows, blocks and
// level, and where the table starts.
struct Footer {
    uint32_t rows = 0;
    int level = 0;
    size_t blocks = 0;
    uint64_t table_at = 0;
    // Block b's rows.
    size_t rows_of(size_t b) const { return std::min<size_t>(BLOCK_ROWS, rows - b * BLOCK_ROWS); }
};
inline Footer read_footer(const uint8_t* footer, uint64_t size) {
    if (size < FOOTER_BYTES) malformed("postcode_grid.bin is shorter than its footer");
    if (std::memcmp(footer + 8, MAGIC, 4) != 0) malformed("postcode_grid.bin has no PGR1 footer");
    if (footer[5] || footer[6] || footer[7]) malformed("postcode_grid.bin's footer has its reserved bytes set");
    Footer f;
    f.rows = get32(footer);
    f.level = footer[4];
    if (f.rows == UINT32_MAX) malformed("postcode_grid.bin gives more rows than a u32 counts");
    if (f.rows ? f.level > MAX_LEVEL : f.level != 0) malformed("postcode_grid.bin's level is not one it can be at");
    const uint64_t blocks = (uint64_t(f.rows) + BLOCK_ROWS - 1) / BLOCK_ROWS;
    f.blocks = size_t(blocks);
    const uint64_t table = blocks * TABLE_ENTRY_BYTES + FOOTER_BYTES;
    if (table > size) malformed("postcode_grid.bin is shorter than its block table");
    f.table_at = size - table;
    if (!f.rows && f.table_at) malformed("postcode_grid.bin has block bytes but no rows");
    return f;
}
// The bytes of the logical stream of a file with this footer.
inline uint64_t logical_size(const Footer& f) { return uint64_t(f.rows) * LOGICAL_STRIDE; }

// The most bytes a block of `rows` rows (1 to BLOCK_ROWS) can have: a count,
// an entry of a country, CODE_BYTES and a 0 a row, a parameter, and an index
// and an escaped step a row. The decoders refuse a longer block before they
// read it.
inline constexpr size_t max_block_bytes(size_t rows) {
    return coord_keys::VARINT_MAX_BYTES + rows * (3 + CODE_BYTES) + 1 + (rows * (64 + RICE_ESCAPE + 64) + 7) / 8;
}

// The strict decoder of a file: given its footer, then each block's table
// entry and bytes in turn, it hands each row to put(const LogicalRow&) and
// refuses every block the encoder would not write for its rows (one longer
// than max_block_bytes of them before it reads it; an Encoder writes each
// again, and its entry and bytes must be the block's). Holds a block of
// rows.
class Decoder {
    struct Discard {
        void operator()(const uint8_t*, size_t) const {}
    };

public:
    explicit Decoder(const Footer& f) : f_(f), check_(Discard{}) {}
    Decoder(const Decoder&) = delete;
    Decoder& operator=(const Decoder&) = delete;

    // The next block's rows, from its table entry (TABLE_ENTRY_BYTES at
    // entry) and its bytes [p, p + n).
    template <class Put>
    void block(const uint8_t* entry, const uint8_t* p, size_t n, Put put) {
        if (b_ >= f_.blocks) throw std::logic_error("postcode_grid::Decoder: a block past the last");
        const std::string name = "block " + std::to_string(b_);
        const size_t rows = f_.rows_of(b_);
        if (n > max_block_bytes(rows)) malformed(name + " has more bytes than its rows can take");
        size_t at = 0;
        uint64_t count;
        if (!coord_keys::get_varint(p, n, at, coord_keys::VARINT_MAX_BYTES, count) || count == 0 || count > rows)
            malformed(name + " has no count of its entries that its rows can take");
        std::vector<LogicalRow> entries(count);
        for (LogicalRow& e : entries) {
            if (n - at < 3) malformed(name + "'s entries run past its end");
            e.country_code = get16(p + at);
            at += 2;
            size_t len = 0;
            while (at + len < n && p[at + len]) len++;
            if (at + len == n) malformed(name + "'s entries run past its end");
            if (len > CODE_BYTES) malformed(name + " has a code longer than " + std::to_string(CODE_BYTES) + " bytes");
            std::memcpy(e.code, p + at, len);
            at += len + 1;
        }
        if (at == n) malformed(name + " has no parameter");
        const uint32_t k = p[at++];
        if (k > RICE_K_MAX) malformed(name + "'s parameter is past " + std::to_string(RICE_K_MAX));
        BitReader bits{p + at, n - at};
        const uint32_t entry_bits = index_bits(count), rank_bits = index_bits(count - 1);
        uint64_t index, step;
        if (!bits.get(entry_bits, index) || index >= count) malformed(name + "'s row 0 has no entry");
        LogicalRow r = entries[index];
        r.cell = get64(entry);
        for (size_t i = 0;; i++) {
            check_.add(r);
            put(static_cast<const LogicalRow&>(r));
            if (i + 1 == rows) break;
            uint64_t rank;
            if (!bits.rice(k, step) || !bits.get(rank_bits, rank)) malformed(name + "'s codes run past its end");
            if (rank + 1 >= count) malformed(name + " has a rank past its entries");
            const uint64_t cell = r.cell;
            const size_t now = size_t(rank < index ? rank : rank + 1);
            // A step past the curve's end is refused by the encoder's order check.
            const uint64_t cells = (cell >> cell_shift(f_.level)) + step + 1;
            if (step >= cells_at(f_.level) || cells >= cells_at(f_.level)) malformed(name + " steps past the last cell");
            r = entries[now];
            r.cell = cell_at(cells, f_.level);
            index = now;
        }
        if (++b_ == f_.blocks) check_.finish();
        if (check_.table().size() != 1 || std::memcmp(check_.table()[0].data(), entry, TABLE_ENTRY_BYTES) != 0 ||
            check_.last_block().size() != n || std::memcmp(check_.last_block().data(), p, n) != 0)
            malformed(name + " is not the encoder's");
        check_.clear_table();
    }

    // After the last block: the file's level is its rows'.
    void finish() {
        if (b_ != f_.blocks) throw std::logic_error("postcode_grid::Decoder: finished before the last block");
        if (check_.footer()[4] != f_.level) malformed("postcode_grid.bin's level is not its rows'");
    }

private:
    Footer f_;
    Encoder<Discard> check_;
    size_t b_ = 0;
};

// The logical stream of a file (whole, in memory), row by row to
// put(const LogicalRow&). Refuses a file the encoder would not write.
template <class Put>
void decode(const uint8_t* file, size_t size, Put put) {
    const Footer f = read_footer(file + size - std::min(size, FOOTER_BYTES), size);
    Decoder d(f);
    uint64_t at = 0;
    for (size_t b = 0; b < f.blocks; b++) {
        const uint8_t* e = file + f.table_at + b * TABLE_ENTRY_BYTES;
        const uint64_t end = b + 1 < f.blocks ? get32(e + TABLE_ENTRY_BYTES + 8) : f.table_at;
        if (get32(e + 8) != at) malformed("block " + std::to_string(b) + " is not where the blocks before it end");
        if (end < at || end > f.table_at) malformed("block " + std::to_string(b) + " ends past the blocks or before it starts");
        d.block(e, file + at, size_t(end - at), put);
        at = end;
    }
    d.finish();
}

}  // namespace postcode_grid
