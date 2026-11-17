// The postcode centroid files as they sit on disk: the centroids bit-packed
// in blocks (postcode_centroids.bin) and the rows each admin cell holds of
// them (postcode_centroid_cells.bin). The builder writes them and the patcher
// rebuilds them through Encoder; the server reads them in place
// (server/src/postcode_files.rs). The patch tools diff and replay the logical
// form, which these files hold losslessly: a 20-byte LogicalCentroid per
// centroid, its point, postcode string id and country and its cell's key,
// ascending by (cell key, postcode_id, country_code). The encoding has no
// choices, so a logical stream has one encoding, and the decoder refuses any
// other.
//
// A centroid's cell is the admin cell (S2, at the build's admin cell level)
// of its point, and the cell's key that id >> 32: an id at level 14 or
// coarser ends in 32 zero bits. A cell holds at most one centroid of a
// postcode string of a country. Its coordinates are finite floats.
//
// postcode_centroids.bin:
//   the blocks' bits, back to back, each block from a byte: the centroids in
//     their logical order in blocks of BLOCK_ROWS, the last one shorter. A
//     block's row 0 is in its table entry; per row after it, LSB first: its
//     kind, a bit, when the block's rows after row 0 are of both kinds; its
//     lat step, its lng step, its postcode_id step, and its country step when
//     the block's rows are not all of row 0's country, each the Rice code
//     (coord_keys.h) of the step's zigzag with the block's parameter for the
//     field; the last byte filled with zero bits.
//     A row is of kind 1 when both its coordinates have a decimal key
//     (coord_keys.h), else of kind 0. A coordinate's step is, in a kind-1
//     row, its decimal key less the previous row's coordinate's decimal round
//     (its key, when it has one), in a kind-0 row its order key less the
//     previous row's order key; the postcode_id step the id less the previous
//     row's, the country step the code less the previous row's as signed
//     numbers; each mod 2^32. A field's parameter is the least k of the
//     fewest bits for the block's codes of it (best_rice_k), 0 for one the
//     block codes nothing of.
//   the block table, TABLE_ENTRY_BYTES a block: row 0's lat and lng bits,
//     its postcode_id and the offset of the block's bits (u32); row 0's
//     country_code (u16); the lat, lng, postcode_id and country parameters (5
//     bits each) and the flags, LSB first in 3 bytes, the last bit 0 (bit 0:
//     the rows after row 0 are of both kinds; bit 1: there are some, all of
//     kind 1; bit 2: every row is of row 0's country); the box, lat_lo,
//     lat_hi, lng_lo and lng_hi (i16).
//     The box holds the block's coordinates in units of 2^-14 degrees from
//     row 0's: lat_lo is the largest n of -BOX_MAX to BOX_MAX for which row
//     0's lat + n 2^-14 is at most the block's least lat, -BOX_OPEN when
//     there is none (an open side); lat_hi the least such n for which it is
//     at least the greatest lat, BOX_OPEN when none; lng's likewise. Each sum
//     is an f64's, rounded once (n 2^-14 is exact), as a reader computes it,
//     so every coordinate of the block as an f64 lies in its box.
//   footer, FOOTER_BYTES: the rows (u32), "PCC1".
//   A bit reader's 8-byte loads past the last block's bits land in the
//   table. A file of no rows is the footer alone.
// postcode_centroid_cells.bin, a ranges index (cell_pairs.h, POSTCODE_CELLS)
//   at the admin cell level: each cell holding centroids and its rows of
//   postcode_centroids.bin. No cells, an empty file.
#pragma once

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "cell_pairs.h"
#include "coord_keys.h"
#include "types.h"

namespace postcode_files {

inline constexpr size_t LOGICAL_STRIDE = 20, BLOCK_ROWS = 32, TABLE_ENTRY_BYTES = 29, FOOTER_BYTES = 8;
inline constexpr char MAGIC[4] = {'P', 'C', 'C', '1'};
inline constexpr const char* CENTROIDS_FILE = "postcode_centroids.bin";
inline constexpr const char* CELLS_FILE = "postcode_centroid_cells.bin";
// The box's units, 2^BOX_UNIT_LOG degrees, its farthest closed side and its open one.
inline constexpr int BOX_UNIT_LOG = -14;
inline constexpr int16_t BOX_MAX = 32766, BOX_OPEN = 32767;
inline constexpr uint8_t FLAG_MIXED = 1, FLAG_KIND1 = 2, FLAG_ONE_COUNTRY = 4;

// A centroid as the patch tools diff and replay it.
struct LogicalCentroid {
    float lat = 0, lng = 0;
    uint32_t postcode_id = NO_DATA;
    uint16_t country_code = 0, pad = 0;
    uint32_t cell_key = 0;
};
static_assert(sizeof(LogicalCentroid) == LOGICAL_STRIDE, "LogicalCentroid must be 20 bytes");

[[noreturn]] inline void malformed(const std::string& what) {
    throw std::runtime_error("Malformed postcode centroid files: " + what);
}

// The key of an admin cell id, which must end in 32 zero bits.
inline uint32_t cell_key(uint64_t cell_id) {
    if (cell_id & 0xFFFFFFFFull) throw std::logic_error("a postcode centroid cell finer than level 14");
    return static_cast<uint32_t>(cell_id >> 32);
}

inline void put32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
inline uint32_t get32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}

// A block's table entry.
struct BlockEntry {
    uint32_t lat_bits = 0, lng_bits = 0, postcode_id = 0, offset = 0;
    uint16_t country_code = 0;
    uint8_t k_lat = 0, k_lng = 0, k_pid = 0, k_cc = 0, flags = 0;
    int16_t box[4] = {};  // lat_lo, lat_hi, lng_lo, lng_hi
};

inline void write_entry(const BlockEntry& e, uint8_t* out) {
    put32(out, e.lat_bits);
    put32(out + 4, e.lng_bits);
    put32(out + 8, e.postcode_id);
    put32(out + 12, e.offset);
    std::memcpy(out + 16, &e.country_code, 2);
    const uint32_t packed = uint32_t(e.k_lat) | uint32_t(e.k_lng) << 5 | uint32_t(e.k_pid) << 10 | uint32_t(e.k_cc) << 15 |
                            uint32_t(e.flags) << 20;
    out[18] = uint8_t(packed);
    out[19] = uint8_t(packed >> 8);
    out[20] = uint8_t(packed >> 16);
    std::memcpy(out + 21, e.box, 8);
}

// An entry's fields, whatever its bytes: the decoder's check refuses any the
// encoder would not write.
inline BlockEntry read_entry(const uint8_t* in) {
    BlockEntry e;
    e.lat_bits = get32(in);
    e.lng_bits = get32(in + 4);
    e.postcode_id = get32(in + 8);
    e.offset = get32(in + 12);
    std::memcpy(&e.country_code, in + 16, 2);
    const uint32_t packed = uint32_t(in[18]) | uint32_t(in[19]) << 8 | uint32_t(in[20]) << 16;
    e.k_lat = uint8_t(packed & 31);
    e.k_lng = uint8_t(packed >> 5 & 31);
    e.k_pid = uint8_t(packed >> 10 & 31);
    e.k_cc = uint8_t(packed >> 15 & 31);
    e.flags = uint8_t(packed >> 20 & 7);
    std::memcpy(e.box, in + 21, 8);
    return e;
}

// A box side, first + n 2^-14 in f64, as a reader computes it.
inline double box_side(double first, int n) { return first + std::ldexp(double(n), BOX_UNIT_LOG); }
// The largest n of -BOX_MAX to BOX_MAX with box_side(first, n) <= least, -BOX_OPEN for none.
inline int16_t box_lo(double first, double least) {
    if (!(box_side(first, -BOX_MAX) <= least)) return -BOX_OPEN;
    int lo = -BOX_MAX, hi = BOX_MAX;
    while (lo < hi) {
        const int mid = lo + (hi - lo + 1) / 2;
        if (box_side(first, mid) <= least) lo = mid;
        else hi = mid - 1;
    }
    return int16_t(lo);
}
// The least n of -BOX_MAX to BOX_MAX with box_side(first, n) >= greatest, BOX_OPEN for none.
inline int16_t box_hi(double first, double greatest) {
    if (!(box_side(first, BOX_MAX) >= greatest)) return BOX_OPEN;
    int lo = -BOX_MAX, hi = BOX_MAX;
    while (lo < hi) {
        const int mid = lo + (hi - lo) / 2;
        if (box_side(first, mid) >= greatest) hi = mid;
        else lo = mid + 1;
    }
    return int16_t(lo);
}

// A coordinate's step from the previous row's, as its row's kind codes it.
inline uint32_t coordinate_step(uint32_t bits, uint32_t prev, bool kind1) {
    using namespace coord_keys;
    if (!kind1) return order_key(bits) - order_key(prev);
    int32_t key = 0;
    dec4_key(bits, key);
    return uint32_t(key) - uint32_t(dec4_round(prev));
}
// The coordinate a step from the previous row's gives.
inline uint32_t coordinate_of(uint32_t step, uint32_t prev, bool kind1) {
    using namespace coord_keys;
    if (!kind1) return from_order_key(order_key(prev) + step);
    return from_dec4_key(int32_t(uint32_t(dec4_round(prev)) + step));
}

inline bool is_kind1(const LogicalCentroid& c) {
    int32_t k;
    return coord_keys::dec4_key(coord_keys::bits_of(c.lat), k) && coord_keys::dec4_key(coord_keys::bits_of(c.lng), k);
}

// Block rows r[0, n) (1 to BLOCK_ROWS) as their entry, its offset `offset`,
// and their bits, to `bits` with 8 zero bytes to spare after them; returns the
// bytes of the bits.
inline size_t encode_block(const LogicalCentroid* r, size_t n, uint32_t offset, BlockEntry& e, std::vector<uint8_t>& bits) {
    using namespace coord_keys;
    e = BlockEntry{};
    e.lat_bits = bits_of(r[0].lat);
    e.lng_bits = bits_of(r[0].lng);
    e.postcode_id = r[0].postcode_id;
    e.offset = offset;
    e.country_code = r[0].country_code;
    uint32_t zl[BLOCK_ROWS], zg[BLOCK_ROWS], zp[BLOCK_ROWS], zc[BLOCK_ROWS];
    bool kind[BLOCK_ROWS];
    size_t kind1 = 0;
    bool one_country = true;
    double lat_lo = r[0].lat, lat_hi = r[0].lat, lng_lo = r[0].lng, lng_hi = r[0].lng;
    for (size_t i = 1; i < n; i++) {
        const size_t s = i - 1;
        kind[s] = is_kind1(r[i]);
        kind1 += kind[s];
        zl[s] = zigzag32(coordinate_step(bits_of(r[i].lat), bits_of(r[i - 1].lat), kind[s]));
        zg[s] = zigzag32(coordinate_step(bits_of(r[i].lng), bits_of(r[i - 1].lng), kind[s]));
        zp[s] = zigzag32(r[i].postcode_id - r[i - 1].postcode_id);
        zc[s] = zigzag32(uint32_t(int32_t(r[i].country_code) - int32_t(r[i - 1].country_code)));
        one_country &= r[i].country_code == r[0].country_code;
        lat_lo = std::min(lat_lo, double(r[i].lat));
        lat_hi = std::max(lat_hi, double(r[i].lat));
        lng_lo = std::min(lng_lo, double(r[i].lng));
        lng_hi = std::max(lng_hi, double(r[i].lng));
    }
    const size_t steps = n - 1;
    const bool mixed = kind1 && kind1 < steps;
    e.flags = uint8_t((mixed ? FLAG_MIXED : 0) | (steps && kind1 == steps ? FLAG_KIND1 : 0) | (one_country ? FLAG_ONE_COUNTRY : 0));
    e.k_lat = uint8_t(best_rice_k(zl, steps));
    e.k_lng = uint8_t(best_rice_k(zg, steps));
    e.k_pid = uint8_t(best_rice_k(zp, steps));
    e.k_cc = uint8_t(one_country ? 0 : best_rice_k(zc, steps));
    e.box[0] = box_lo(r[0].lat, lat_lo);
    e.box[1] = box_hi(r[0].lat, lat_hi);
    e.box[2] = box_lo(r[0].lng, lng_lo);
    e.box[3] = box_hi(r[0].lng, lng_hi);
    uint64_t total = 0;
    for (size_t s = 0; s < steps; s++) {
        total += (mixed ? 1 : 0) + rice_bits(zl[s], e.k_lat) + rice_bits(zg[s], e.k_lng) + rice_bits(zp[s], e.k_pid) +
                 (one_country ? 0 : rice_bits(zc[s], e.k_cc));
    }
    const size_t bytes = size_t((total + 7) / 8);
    bits.assign(bytes + 8, 0);
    uint64_t bit = 0;
    for (size_t s = 0; s < steps; s++) {
        if (mixed) put_bits(bits.data(), bit++, kind[s], 1);
        put_rice(bits.data(), bit, zl[s], e.k_lat);
        put_rice(bits.data(), bit, zg[s], e.k_lng);
        put_rice(bits.data(), bit, zp[s], e.k_pid);
        if (!one_country) put_rice(bits.data(), bit, zc[s], e.k_cc);
    }
    if (bit != total) throw std::logic_error("a postcode centroid block's bits don't add up");
    return bytes;
}

// The logical stream in, the two files out through the sinks (called with
// bytes and a length), front to back but for postcode_centroids.bin's table:
// its blocks go to `blocks`, and the caller writes the table (an entry a
// block, table(), which it may take as they come) and the footer after them.
// Holds a block of rows and the cells index's parent record. Refuses a
// stream the files can't hold.
template <class Sink>
class Encoder {
    struct CellsOut {
        Sink* sink;
        void operator()(const char* p, size_t n) const { (*sink)(reinterpret_cast<const uint8_t*>(p), n); }
    };
    struct NoEntries {
        void operator()(const char*, size_t) const { throw std::logic_error("a ranges index writes no entries"); }
    };

public:
    Encoder(Sink blocks, Sink cells)
        : blocks_(std::move(blocks)), cells_(std::move(cells)), ranges_(cell_pairs::POSTCODE_CELLS, CellsOut{&cells_}, NoEntries{}) {}
    Encoder(const Encoder&) = delete;
    Encoder& operator=(const Encoder&) = delete;

    void add(const LogicalCentroid& c) {
        if (finished_) throw std::logic_error("postcode_files::Encoder: add after finish");
        if (c.pad) malformed("a centroid with its reserved bytes set");
        if (c.postcode_id == NO_DATA) malformed("a centroid without a postcode");
        if (!std::isfinite(c.lat) || !std::isfinite(c.lng)) malformed("a coordinate that isn't finite");
        if (rows_ == UINT32_MAX) malformed("more than 2^32 - 1 postcode centroids: their rows are u32");
        if (rows_ && c.cell_key < last_.cell_key) malformed("centroids out of cell order");
        if (rows_ && c.cell_key == last_.cell_key) {
            if (c.postcode_id == last_.postcode_id && c.country_code == last_.country_code)
                malformed("a postcode of one country twice in a cell");
            if (c.postcode_id < last_.postcode_id || (c.postcode_id == last_.postcode_id && c.country_code < last_.country_code))
                malformed("centroids out of order in a cell");
        }
        if (rows_ && c.cell_key != last_.cell_key) end_cell();
        cell_rows_++;
        last_ = c;
        rows_++;
        block_[block_rows_++] = c;
        if (block_rows_ == BLOCK_ROWS) flush_block();
    }

    // Writes the last block and the cells file.
    void finish() {
        if (finished_) return;
        finished_ = true;
        flush_block();
        if (rows_) end_cell();
        ranges_.finish();
    }

    // The footer, after the table.
    std::array<uint8_t, FOOTER_BYTES> footer() const {
        std::array<uint8_t, FOOTER_BYTES> f;
        put32(f.data(), rows_);
        std::memcpy(f.data() + 4, MAGIC, 4);
        return f;
    }

    uint32_t rows() const { return rows_; }
    uint64_t block_bytes() const { return offset_; }
    // The table entries of the blocks written since the last clear_table.
    const std::vector<std::array<uint8_t, TABLE_ENTRY_BYTES>>& table() const { return table_; }
    void clear_table() { table_.clear(); }
    // The bits of the block written last.
    const uint8_t* last_block() const { return bits_.data(); }
    size_t last_block_bytes() const { return last_bytes_; }

private:
    void end_cell() {
        ranges_.add_range(uint64_t(last_.cell_key) << 32, cell_rows_);
        cell_rows_ = 0;
    }

    void flush_block() {
        if (!block_rows_) return;
        if (offset_ > UINT32_MAX) malformed("postcode_centroids.bin's blocks past 4 GiB");
        BlockEntry e;
        last_bytes_ = encode_block(block_.data(), block_rows_, uint32_t(offset_), e, bits_);
        std::array<uint8_t, TABLE_ENTRY_BYTES> entry;
        write_entry(e, entry.data());
        table_.push_back(entry);
        if (last_bytes_) blocks_(bits_.data(), last_bytes_);
        offset_ += last_bytes_;
        block_rows_ = 0;
    }

    Sink blocks_, cells_;
    cell_pairs::Writer<CellsOut, NoEntries> ranges_;
    std::array<LogicalCentroid, BLOCK_ROWS> block_{};
    size_t block_rows_ = 0, last_bytes_ = 0;
    std::vector<uint8_t> bits_;
    std::vector<std::array<uint8_t, TABLE_ENTRY_BYTES>> table_;
    uint64_t offset_ = 0;
    uint32_t rows_ = 0, cell_rows_ = 0;
    LogicalCentroid last_{};
    bool finished_ = false;
};

// The footer of a postcode_centroids.bin of size bytes: its rows and blocks,
// and where the table starts.
struct Footer {
    uint32_t rows = 0;
    size_t blocks = 0;
    uint64_t table_at = 0;
    // Block b's rows.
    size_t rows_of(size_t b) const { return std::min<size_t>(BLOCK_ROWS, rows - b * BLOCK_ROWS); }
};
inline Footer read_footer(const uint8_t* footer, uint64_t size) {
    if (size < FOOTER_BYTES) malformed("postcode_centroids.bin is shorter than its footer");
    if (std::memcmp(footer + 4, MAGIC, 4) != 0) malformed("postcode_centroids.bin has no PCC1 footer");
    Footer f;
    f.rows = get32(footer);
    f.blocks = (size_t(f.rows) + BLOCK_ROWS - 1) / BLOCK_ROWS;
    const uint64_t table = uint64_t(f.blocks) * TABLE_ENTRY_BYTES + FOOTER_BYTES;
    if (table > size) malformed("postcode_centroids.bin is shorter than its block table");
    f.table_at = size - table;
    return f;
}
// The bytes of the logical stream of files with this footer.
inline uint64_t logical_size(const Footer& f) { return uint64_t(f.rows) * LOGICAL_STRIDE; }

// Bytes in memory as a cell_pairs::Reader reads a file: size() and at(off, n).
struct Span {
    const uint8_t* data;
    size_t bytes;
    uint64_t size() const { return bytes; }
    const char* at(uint64_t off, size_t) const { return reinterpret_cast<const char*>(data) + off; }
};

// The strict decoder of a set of files: given its footer and its cells file
// (read front to back through size() and at(off, n)), then each block's
// table entry and bits in turn, it hands each centroid with its cell's key
// to put(const LogicalCentroid&) and refuses every block the encoder would
// not write for its rows (an Encoder writes each again and its entry and bits
// must be the block's), every row out of the encoder's order, and cells that
// do not tile the rows. Holds a block's bits and rows and the cells index's
// parent record.
template <class Cells>
class Decoder {
    struct Discard {
        void operator()(const uint8_t*, size_t) const {}
    };

public:
    Decoder(const Footer& f, Cells& cells) : f_(f), cells_(cells, no_entries_, cell_pairs::POSTCODE_CELLS), check_(Discard{}, Discard{}) {}
    Decoder(const Decoder&) = delete;
    Decoder& operator=(const Decoder&) = delete;

    // The next block's rows, from its table entry (TABLE_ENTRY_BYTES at
    // entry) and its bits [p, p + n).
    template <class Put>
    void block(const uint8_t* entry, const uint8_t* p, size_t n, Put put) {
        using namespace coord_keys;
        if (b_ >= f_.blocks) throw std::logic_error("postcode_files::Decoder: a block past the last");
        const std::string name = "block " + std::to_string(b_);
        const BlockEntry e = read_entry(entry);
        const size_t rows = f_.rows_of(b_);
        bits_.assign(p, p + n);
        bits_.resize(n + 8, 0);
        const uint8_t* q = bits_.data();
        const uint64_t end = uint64_t(n) * 8;
        uint64_t bit = 0;
        auto rice = [&](uint32_t k) {
            const uint32_t z = get_rice(q, bit, k);
            if (bit > end) malformed(name + "'s codes run past its end");
            return z;
        };
        LogicalCentroid c;
        c.lat = float_of(e.lat_bits);
        c.lng = float_of(e.lng_bits);
        c.postcode_id = e.postcode_id;
        c.country_code = e.country_code;
        emit(c, put);
        const bool mixed = e.flags & FLAG_MIXED, kind1 = e.flags & FLAG_KIND1, one_country = e.flags & FLAG_ONE_COUNTRY;
        for (size_t i = 1; i < rows; i++) {
            bool k = kind1;
            if (mixed) {
                k = get_bits(q, bit++, 1);
                if (bit > end) malformed(name + "'s codes run past its end");
            }
            const uint32_t dl = unzigzag32(rice(e.k_lat)), dg = unzigzag32(rice(e.k_lng)), dp = unzigzag32(rice(e.k_pid));
            const uint32_t dc = one_country ? 0 : unzigzag32(rice(e.k_cc));
            c.lat = float_of(coordinate_of(dl, bits_of(c.lat), k));
            c.lng = float_of(coordinate_of(dg, bits_of(c.lng), k));
            c.postcode_id += dp;
            c.country_code = uint16_t(c.country_code + dc);
            emit(c, put);
        }
        if (++b_ == f_.blocks) check_.finish();
        if (check_.table().size() != 1 || std::memcmp(check_.table()[0].data(), entry, TABLE_ENTRY_BYTES) != 0 ||
            check_.last_block_bytes() != n || (n && std::memcmp(check_.last_block(), p, n) != 0))
            malformed(name + " is not the encoder's");
        check_.clear_table();
    }

    // After the last block: the cells end with the rows.
    void finish() {
        if (b_ != f_.blocks) throw std::logic_error("postcode_files::Decoder: finished before the last block");
        uint64_t cell;
        uint32_t first, end;
        if (row_ == cell_end_ && cells_.next_range(cell, first, end)) malformed("postcode_centroid_cells.bin has cells past the last row");
        if (cells_.ranges_end() != f_.rows) malformed("postcode_centroid_cells.bin's ranges don't end at the last row");
    }

private:
    template <class Put>
    void emit(LogicalCentroid& c, Put& put) {
        if (row_ == cell_end_) {
            uint64_t cell;
            uint32_t first;
            if (!cells_.next_range(cell, first, cell_end_)) malformed("rows past the last cell of postcode_centroid_cells.bin");
            key_ = cell_key(cell);
        }
        c.cell_key = key_;
        check_.add(c);
        put(static_cast<const LogicalCentroid&>(c));
        row_++;
    }

    Footer f_;
    Span no_entries_{nullptr, 0};
    cell_pairs::Reader<Cells, Span> cells_;
    Encoder<Discard> check_;
    std::vector<uint8_t> bits_;
    size_t b_ = 0;
    uint32_t row_ = 0, cell_end_ = 0, key_ = 0;
};

// The logical stream of the two files (whole, in memory), centroid by
// centroid to put(const LogicalCentroid&). Refuses files the encoder would
// not write.
template <class Put>
void decode(const uint8_t* centroids, size_t size, const uint8_t* cells, size_t cells_size, Put put) {
    const Footer f = read_footer(centroids + size - std::min(size, FOOTER_BYTES), size);
    Span c{cells, cells_size};
    Decoder<Span> d(f, c);
    uint64_t at = 0;
    for (size_t b = 0; b < f.blocks; b++) {
        const uint8_t* e = centroids + f.table_at + b * TABLE_ENTRY_BYTES;
        const uint64_t end = b + 1 < f.blocks ? get32(e + TABLE_ENTRY_BYTES + 12) : f.table_at;
        if (get32(e + 12) != at) malformed("block " + std::to_string(b) + " is not where the blocks before it end");
        if (end < at || end > f.table_at) malformed("block " + std::to_string(b) + " ends past the blocks or before it starts");
        d.block(e, centroids + at, size_t(end - at), put);
        at = end;
    }
    d.finish();
}

}  // namespace postcode_files
