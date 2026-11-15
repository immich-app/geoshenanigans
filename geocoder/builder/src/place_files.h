// The place node files as they sit on disk: the cell table (place_cells.bin),
// the visible nodes bit-packed in blocks (place_nodes.bin) and the hidden ones
// (place_hidden.bin). The builder writes them and the patcher rebuilds them
// through Encoder; the server reads them in place (server/src/place_files.rs).
// The patch tools diff and replay the logical form, which these files hold
// losslessly: a 24-byte LogicalPlace per node, a PlaceNode (types.h) and its
// cell's key, the visible nodes in cell order and then the hidden ones. The
// encoding has no choices, so a logical set has one encoding, and the decoder
// refuses any other.
//
// A visible node is one reverse geocoding reads: no PLACE_FLAG_* bit set (a
// label is hidden too) and no parent. Its cell is the admin cell (S2, at the
// build's admin cell level) of its point, and the cell's key that id >> 32: an
// id at level 14 or coarser ends in 32 zero bits. A hidden node's key is
// NO_DATA. A node's id is its row: a visible node's in place_nodes.bin, a
// hidden one's the visible rows plus its row in place_hidden.bin.
//
// place_cells.bin, a row per cell holding visible nodes, ascending by key:
//   the key (u32) and the cell's first row (u32); a cell's rows run to the
//   next one's first, the last one's to the last row.
// place_nodes.bin, the visible nodes in their logical order (by key), in
//   blocks of BLOCK_ROWS rows, the last one shorter:
//   the blocks' bits, back to back, each block from a byte: per row, LSB
//     first, its lat key less the block's lat base (wl bits), its lng key
//     less its lng base (wg bits), place_type (TYPE_BITS) and name_id (wn
//     bits); the last byte filled with zero bits. A coordinate's key is its
//     f32 bit pattern ordered as the floats are (order_key). A block's bases
//     are its least keys, wl and wg the widths of its largest differences
//     from them and wn that of its largest name_id;
//   the block table, TABLE_ENTRY_BYTES a block: the lat base and lng base
//     (u32), the offset of the block's bits (u32), wl, wg, wn (u8) and 0;
//   footer, FOOTER_BYTES: the rows (u32), "PNL3".
//   A bit reader's 8-byte loads past the last block's bits land in the table.
// place_hidden.bin, HIDDEN_ROW_BYTES a hidden node, in the logical order: lat,
//   lng (f32), name_id | place_type << 28 | (label) << 31 (u32, name_id below
//   2^28), parent_poly_id (u32): a label's (PLACE_FLAG_LABEL_OF) boundary
//   polygon, else NO_DATA.
#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "types.h"

namespace place_files {

inline constexpr size_t LOGICAL_STRIDE = 24, BLOCK_ROWS = 64, TABLE_ENTRY_BYTES = 16, FOOTER_BYTES = 8,
                        CELL_ROW_BYTES = 8, HIDDEN_ROW_BYTES = 16;
inline constexpr uint32_t TYPE_BITS = 3, HIDDEN_NAME_BITS = 28;
inline constexpr char MAGIC[4] = {'P', 'N', 'L', '3'};
inline constexpr const char* CELLS_FILE = "place_cells.bin";
inline constexpr const char* NODES_FILE = "place_nodes.bin";
inline constexpr const char* HIDDEN_FILE = "place_hidden.bin";

// A node as the patch tools diff and replay it.
struct LogicalPlace {
    PlaceNode node;
    uint32_t cell_key = NO_DATA;
};
static_assert(sizeof(LogicalPlace) == LOGICAL_STRIDE, "LogicalPlace must be 24 bytes");

[[noreturn]] inline void malformed(const std::string& what) { throw std::runtime_error("Malformed place files: " + what); }

inline bool is_visible(const PlaceNode& p) { return p._pad1 == 0 && p.parent_poly_id == NO_DATA; }

// The key of an admin cell id, which must end in 32 zero bits.
inline uint32_t cell_key(uint64_t cell_id) {
    if (cell_id & 0xFFFFFFFFull) throw std::logic_error("a place cell finer than level 14");
    return static_cast<uint32_t>(cell_id >> 32);
}

// An f32 bit pattern's place in the floats' order (-0 before +0), and back.
inline uint32_t order_key(uint32_t bits) { return bits & 0x80000000u ? ~bits : bits | 0x80000000u; }
inline uint32_t from_order_key(uint32_t key) { return key & 0x80000000u ? key & 0x7FFFFFFFu : ~key; }
inline uint32_t bits_of(float f) {
    uint32_t b;
    std::memcpy(&b, &f, 4);
    return b;
}
inline float float_of(uint32_t b) {
    float f;
    std::memcpy(&f, &b, 4);
    return f;
}
inline uint32_t width_of(uint32_t v) { return v ? 32 - static_cast<uint32_t>(__builtin_clz(v)) : 0; }

inline void put32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
inline uint32_t get32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}

// A block's table entry.
struct BlockEntry {
    uint32_t lat_base = 0, lng_base = 0, offset = 0;
    uint8_t wl = 0, wg = 0, wn = 0;
    uint32_t row_bits() const { return uint32_t(wl) + wg + TYPE_BITS + wn; }
    size_t bytes(size_t rows) const { return (rows * row_bits() + 7) / 8; }
};

inline void write_entry(const BlockEntry& e, uint8_t* out) {
    put32(out, e.lat_base);
    put32(out + 4, e.lng_base);
    put32(out + 8, e.offset);
    out[12] = e.wl;
    out[13] = e.wg;
    out[14] = e.wn;
    out[15] = 0;
}

inline BlockEntry read_entry(const uint8_t* in) {
    BlockEntry e;
    e.lat_base = get32(in);
    e.lng_base = get32(in + 4);
    e.offset = get32(in + 8);
    e.wl = in[12];
    e.wg = in[13];
    e.wn = in[14];
    if (in[15] || e.wl > 32 || e.wg > 32 || e.wn > 32) malformed("a block table entry out of range");
    return e;
}

// The logical stream in, the three files out through the sinks (called with
// bytes and a length), front to back but for place_nodes.bin: its blocks go
// to `blocks`, and the caller writes its table (an entry a block, table(),
// which it may take as they come) and footer after them. Refuses a stream the
// files can't hold.
template <class Sink>
class Encoder {
public:
    Encoder(Sink cells, Sink blocks, Sink hidden) : cells_(cells), blocks_(blocks), hidden_(hidden) {}

    void add(const LogicalPlace& p) {
        const PlaceNode& n = p.node;
        if (n.place_type >= 1u << TYPE_BITS) malformed("a place type past " + std::to_string(TYPE_BITS) + " bits");
        if (n._pad2) malformed("a node with its reserved bytes set");
        if (n.name_id == NO_DATA) malformed("a node without a name");
        if (uint64_t(rows_) + hidden_rows_ >= UINT32_MAX) malformed("more than 2^32 - 1 place nodes: their ids are u32");
        if (is_visible(n)) {
            if (hidden_rows_) malformed("a visible node after a hidden one");
            if (p.cell_key == NO_DATA) malformed("a visible node without a cell");
            if (rows_ && p.cell_key < last_key_) malformed("visible nodes out of cell order");
            if (!rows_ || p.cell_key != last_key_) {
                uint8_t row[CELL_ROW_BYTES];
                put32(row, p.cell_key);
                put32(row + 4, rows_);
                cells_(row, CELL_ROW_BYTES);
                last_key_ = p.cell_key;
            }
            block_.push_back(n);
            rows_++;
            if (block_.size() == BLOCK_ROWS) flush_block();
            return;
        }
        if (p.cell_key != NO_DATA) malformed("a hidden node with a cell");
        if (!(n._pad1 & PLACE_FLAG_HIDDEN) || (n._pad1 & ~(PLACE_FLAG_HIDDEN | PLACE_FLAG_LABEL_OF)))
            malformed("a node neither visible nor hidden");
        const bool label = n._pad1 & PLACE_FLAG_LABEL_OF;
        if (label != (n.parent_poly_id != NO_DATA)) malformed("a hidden node's parent without its label, or its label without one");
        if (n.name_id >> HIDDEN_NAME_BITS) malformed("a hidden node's name_id past " + std::to_string(HIDDEN_NAME_BITS) + " bits");
        uint8_t row[HIDDEN_ROW_BYTES];
        put32(row, bits_of(n.lat));
        put32(row + 4, bits_of(n.lng));
        put32(row + 8, n.name_id | uint32_t(n.place_type) << HIDDEN_NAME_BITS | uint32_t(label) << 31);
        put32(row + 12, n.parent_poly_id);
        hidden_(row, HIDDEN_ROW_BYTES);
        hidden_rows_++;
    }

    // Writes the last block.
    void finish() { flush_block(); }

    // The footer, after the table.
    std::array<uint8_t, FOOTER_BYTES> footer() const {
        std::array<uint8_t, FOOTER_BYTES> f;
        put32(f.data(), rows_);
        std::memcpy(f.data() + 4, MAGIC, 4);
        return f;
    }

    uint32_t rows() const { return rows_; }
    uint64_t hidden_rows() const { return hidden_rows_; }
    uint64_t block_bytes() const { return offset_; }
    // The table entries of the blocks written since the last clear_table.
    const std::vector<std::array<uint8_t, TABLE_ENTRY_BYTES>>& table() const { return table_; }
    void clear_table() { table_.clear(); }

private:
    void flush_block() {
        if (block_.empty()) return;
        BlockEntry e;
        e.lat_base = e.lng_base = UINT32_MAX;
        uint32_t lat_max = 0, lng_max = 0, name_max = 0;
        std::array<uint32_t, BLOCK_ROWS> lk, gk;
        for (size_t k = 0; k < block_.size(); k++) {
            const PlaceNode& n = block_[k];
            const uint32_t l = lk[k] = order_key(bits_of(n.lat)), g = gk[k] = order_key(bits_of(n.lng));
            e.lat_base = std::min(e.lat_base, l);
            e.lng_base = std::min(e.lng_base, g);
            lat_max = std::max(lat_max, l);
            lng_max = std::max(lng_max, g);
            name_max = std::max(name_max, n.name_id);
        }
        e.wl = uint8_t(width_of(lat_max - e.lat_base));
        e.wg = uint8_t(width_of(lng_max - e.lng_base));
        e.wn = uint8_t(width_of(name_max));
        if (offset_ > UINT32_MAX) malformed("place_nodes.bin's blocks past 4 GiB");
        e.offset = uint32_t(offset_);
        std::array<uint8_t, TABLE_ENTRY_BYTES> entry;
        write_entry(e, entry.data());
        table_.push_back(entry);
        // Each field ORed in with one 8-byte load and store at its first byte (a field of at most 32
        // bits shifted by at most 7 fits 64), into zeroed bytes with 8 to spare.
        const size_t bytes = e.bytes(block_.size());
        bytes_.assign(bytes + 8, 0);
        uint8_t* out = bytes_.data();
        uint64_t bit = 0;
        auto add = [&](uint64_t v, uint32_t w) {
            uint64_t word;
            std::memcpy(&word, out + (bit >> 3), 8);
            word |= v << (bit & 7);
            std::memcpy(out + (bit >> 3), &word, 8);
            bit += w;
        };
        for (size_t k = 0; k < block_.size(); k++) {
            add(lk[k] - e.lat_base, e.wl);
            add(gk[k] - e.lng_base, e.wg);
            add(block_[k].place_type, TYPE_BITS);
            add(block_[k].name_id, e.wn);
        }
        if ((bit + 7) / 8 != bytes) throw std::logic_error("a place block's bytes don't add up");
        blocks_(out, bytes);
        offset_ += bytes;
        block_.clear();
    }

    Sink cells_, blocks_, hidden_;
    std::vector<PlaceNode> block_;
    std::vector<uint8_t> bytes_;
    std::vector<std::array<uint8_t, TABLE_ENTRY_BYTES>> table_;
    uint64_t offset_ = 0, hidden_rows_ = 0;
    uint32_t rows_ = 0, last_key_ = 0;
};

// A block's rows' fields, LSB first, from its bytes [p, end).
struct RowReader {
    const uint8_t* p;
    const uint8_t* end;
    uint64_t bit = 0;
    uint32_t take(uint32_t w) {
        if (!w) return 0;
        const uint8_t* at = p + (bit >> 3);
        uint64_t word = 0;
        // One 8-byte load where the block holds 8 bytes from here (the fixed size inlines).
        if (end - at >= 8) std::memcpy(&word, at, 8);
        else std::memcpy(&word, at, size_t(end - at));
        const uint32_t v = uint32_t((word >> (bit & 7)) & ((uint64_t(1) << w) - 1));
        bit += w;
        return v;
    }
};

// The footer of a place_nodes.bin of size bytes: its rows and blocks, and
// where the table starts.
struct Footer {
    uint32_t rows = 0;
    size_t blocks = 0;
    uint64_t table_at = 0;
    // Block b's rows.
    size_t rows_of(size_t b) const { return std::min<size_t>(BLOCK_ROWS, rows - b * BLOCK_ROWS); }
};
inline Footer read_footer(const uint8_t* footer, uint64_t size) {
    if (size < FOOTER_BYTES) malformed("place_nodes.bin is shorter than its footer");
    if (std::memcmp(footer + 4, MAGIC, 4) != 0) malformed("place_nodes.bin's footer has no PNL3");
    Footer f;
    f.rows = get32(footer);
    f.blocks = (size_t(f.rows) + BLOCK_ROWS - 1) / BLOCK_ROWS;
    const uint64_t table = uint64_t(f.blocks) * TABLE_ENTRY_BYTES + FOOTER_BYTES;
    if (table > size) malformed("place_nodes.bin is shorter than its block table");
    f.table_at = size - table;
    return f;
}

// The visible node of a row: lat, lng, place type and name; no flags, no
// parent.
inline PlaceNode visible_node(uint32_t lat_bits, uint32_t lng_bits, uint32_t type, uint32_t name) {
    PlaceNode n{};
    n.lat = float_of(lat_bits);
    n.lng = float_of(lng_bits);
    n.name_id = name;
    n.place_type = uint8_t(type);
    n._pad1 = 0;
    n.parent_poly_id = NO_DATA;
    return n;
}

inline LogicalPlace hidden_place(const uint8_t* row) {
    const uint32_t w = get32(row + 8);
    LogicalPlace p;
    PlaceNode& n = p.node;
    n = PlaceNode{};
    n.lat = float_of(get32(row));
    n.lng = float_of(get32(row + 4));
    n.name_id = w & ((1u << HIDDEN_NAME_BITS) - 1);
    n.place_type = uint8_t(w >> HIDDEN_NAME_BITS & ((1u << TYPE_BITS) - 1));
    n._pad1 = uint8_t(PLACE_FLAG_HIDDEN | (w >> 31 ? PLACE_FLAG_LABEL_OF : 0));
    n.parent_poly_id = get32(row + 12);
    if ((w >> 31) != (n.parent_poly_id != NO_DATA)) malformed("a hidden node's parent without its label, or its label without one");
    p.cell_key = NO_DATA;
    return p;
}

// The key of each visible row's cell, the rows asked in order, from
// place_cells.bin's rows as next() hands them (8 bytes each): checked to
// ascend by key from row 0 and to cover the rows without an empty cell.
class CellCursor {
public:
    CellCursor(uint32_t rows, uint64_t cells) : rows_(rows), cells_(cells) {
        if ((cells == 0) != (rows == 0)) malformed("place_cells.bin and place_nodes.bin disagree on whether there are visible nodes");
    }
    template <class Next>
    uint32_t key_of(uint32_t row, Next next) {
        while (row >= end_) {
            if (started_ == cells_) malformed("visible rows past place_cells.bin's last cell");
            uint32_t key, first;
            if (started_) {
                key = next_key_;
                first = end_;
            } else {
                const uint8_t* c = next();
                key = get32(c);
                first = get32(c + 4);
            }
            if (first != row || (started_ && key <= key_)) malformed("place_cells.bin's rows are not ascending from row 0");
            // The key a hidden node's row holds: no visible node has it.
            if (key == NO_DATA) malformed("a visible node without a cell");
            key_ = key;
            if (++started_ < cells_) {
                const uint8_t* c = next();
                next_key_ = get32(c);
                end_ = get32(c + 4);
            } else {
                end_ = rows_;
            }
            if (end_ <= first || end_ > rows_) malformed("a cell without rows, or with rows past the last");
        }
        return key_;
    }
    void finish() const {
        if (started_ != cells_) malformed("place_cells.bin has cells past the last visible row");
    }

private:
    uint32_t rows_;
    uint64_t cells_, started_ = 0;
    uint32_t key_ = 0, next_key_ = 0, end_ = 0;
};

// Block b's rows (its entry e, rows of them, its bytes [p, p + e.bytes(rows)))
// to put(const LogicalPlace&), each with key(row), its cell's key. Refuses a
// block the encoder would not write.
template <class Key, class Put>
void decode_block(size_t b, const BlockEntry& e, size_t rows, const uint8_t* p, Key key, Put put) {
    const size_t bytes = e.bytes(rows);
    RowReader r{p, p + bytes};
    uint32_t lat_max = 0, lng_max = 0, name_max = 0;
    bool lat_at_base = false, lng_at_base = false;
    for (size_t k = 0; k < rows; k++) {
        const uint32_t dl = r.take(e.wl), dg = r.take(e.wg), type = r.take(TYPE_BITS), name = r.take(e.wn);
        if (uint64_t(e.lat_base) + dl > UINT32_MAX || uint64_t(e.lng_base) + dg > UINT32_MAX) malformed("a row's key past 32 bits");
        lat_max = std::max(lat_max, dl);
        lng_max = std::max(lng_max, dg);
        name_max = std::max(name_max, name);
        lat_at_base |= dl == 0;
        lng_at_base |= dg == 0;
        LogicalPlace pl;
        pl.node = visible_node(from_order_key(e.lat_base + dl), from_order_key(e.lng_base + dg), type, name);
        pl.cell_key = key(uint32_t(b * BLOCK_ROWS + k));
        if (pl.node.name_id == NO_DATA) malformed("a node without a name");
        put(pl);
    }
    if (!lat_at_base || !lng_at_base || width_of(lat_max) != e.wl || width_of(lng_max) != e.wg || width_of(name_max) != e.wn)
        malformed("block " + std::to_string(b) + " is not the encoder's");
    const uint64_t used = uint64_t(rows) * e.row_bits();
    if (used % 8 && (p[bytes - 1] >> (used % 8))) malformed("block " + std::to_string(b) + " has bits set past its last row");
}

// The logical stream of the three files (whole, in memory), node by node to
// put(const LogicalPlace&). Refuses files the encoder would not write.
template <class Put>
void decode(const uint8_t* cells, size_t cells_size, const uint8_t* nodes, size_t nodes_size, const uint8_t* hidden,
            size_t hidden_size, Put put) {
    const Footer f = read_footer(nodes + nodes_size - std::min(nodes_size, FOOTER_BYTES), nodes_size);
    if (cells_size % CELL_ROW_BYTES) malformed("place_cells.bin is not whole rows");
    if (hidden_size % HIDDEN_ROW_BYTES) malformed("place_hidden.bin is not whole rows");
    CellCursor cursor(f.rows, cells_size / CELL_ROW_BYTES);
    const uint8_t* next_cell = cells;
    auto next = [&] {
        const uint8_t* c = next_cell;
        next_cell += CELL_ROW_BYTES;
        return c;
    };
    uint64_t at = 0;
    for (size_t b = 0; b < f.blocks; b++) {
        const BlockEntry e = read_entry(nodes + f.table_at + b * TABLE_ENTRY_BYTES);
        const size_t rows = f.rows_of(b), bytes = e.bytes(rows);
        if (e.offset != at) malformed("block " + std::to_string(b) + " is not where the blocks before it end");
        if (at + bytes > f.table_at) malformed("block " + std::to_string(b) + " runs into the block table");
        decode_block(b, e, rows, nodes + at, [&](uint32_t row) { return cursor.key_of(row, next); }, put);
        at += bytes;
    }
    if (at != f.table_at) malformed("place_nodes.bin has bytes between its blocks and its table");
    cursor.finish();
    for (size_t h = 0; h < hidden_size; h += HIDDEN_ROW_BYTES) put(hidden_place(hidden + h));
}

}  // namespace place_files
