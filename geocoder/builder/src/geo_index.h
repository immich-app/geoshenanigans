// The geo index ("GXT2"): each street-level cell's lists of street ways,
// address points and interpolation ways, in two files that are both written
// front to back in one pass, so the patcher rebuilds it with a block's state
// in memory (Writer) from the old one read the same way (Reader). No S2 here,
// so the tools and the unit tests can read and write it.
//
// geo_cells.bin, the top file:
//   header, 8 bytes: "GXT2", the cell level L, BLOCK_ROWS (64), GROUP_BLOCKS
//     (64), 0.
//   per group, in order, 12 bytes: the first cell of its first block (u64),
//     where its footer starts in geo_entries.bin (u32).
//   footer, 8 bytes: the number of rows (u64).
// geo_entries.bin, the blocks file: the groups back to back, each its blocks
//   and then its footer, 12 bytes per block: the block's first cell (u64)
//   and where it starts in this file (u32). A block holds 64 rows and a group
//   64 blocks, but for the index's last, which can hold fewer.
//   An index of no rows is an empty top file and an empty blocks file.
// A row is a level-L cell that has a list in one of the tables (0 streets,
// 1 addresses, 2 interpolations), in ascending cell order: a byte
// s | a << 2 | i << 4 | d << 6, where s, a and i are the codes of tables 0,
// 1 and 2 (0 none, 1 PLAIN, 2 SAME, 3 EDIT; at least one isn't 0) and d is
// the step from the row before in level-L cells less 1, or 3 for a step of
// 4 or more, then given as varint(step - 4). A block's first row has d = 0:
// its cell is the block's first cell. Then each table's list, in table
// order. Numbers are varints (LEB128) unless said otherwise.
//   A list holds 1 to LIST_MAX ids, ascending (an id can repeat). A table's
//   last first is the first id of its last PLAIN list in the block, 0 at the
//   block's start.
//   PLAIN: n << 1 | repeats; zigzag(first - the table's last first); each
//     next id as its gap from the one before less 1 (less 0 with repeats).
//   SAME: nothing: the list is the table's previous one in the block.
//   EDIT of the table's previous list in the block, P, into L (neither with
//     a repeated id): ceil(|P| / 8) bytes of 1-bit ops over P, LSB first
//     (0 keep, 1 drop, 0 past the last); n_add; the ids L adds, ascending,
//     the first as zigzag(first - the table's last first), each next as its
//     gap from the one before less 1. L is the ids kept and added, sorted.
//   Address lists are all PLAIN. With the last firsts moving on PLAIN lists
//   only, a reader steps over a row's lists without decoding their ids.
// The Writer's choices are fixed, so a set of rows has one encoding: a
// street or interpolation list is SAME when it is the table's previous list
// in the block, else EDIT when neither has a repeated id and the edit is
// shorter than the PLAIN list, else PLAIN.
#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "cell_pairs.h"

namespace geo_index {

inline constexpr char MAGIC[4] = {'G', 'X', 'T', '2'};
inline constexpr size_t HEADER_BYTES = 8, FOOTER_BYTES = 8, ENTRY_BYTES = 12;
inline constexpr size_t BLOCK_ROWS = 64, GROUP_BLOCKS = 64;
inline constexpr size_t TABLES = 3, ADDRESSES = 1;
// The blocks file's bytes a Writer holds before it hands them on: a page,
// what a FILE or stream buffer takes in at once, so its callers' buffers do
// the batching and the Writer doesn't hold a second 64 KiB.
inline constexpr size_t FLUSH_BYTES = 4 << 10;
// A list's count is that of a u16, as the server's lists always were.
inline constexpr uint32_t LIST_MAX = 0xFFFF;
inline constexpr int MIN_LEVEL = 1, MAX_LEVEL = 30;
enum Code : unsigned { NONE = 0, PLAIN = 1, SAME = 2, EDIT = 3 };

[[noreturn, gnu::cold, gnu::noinline]] inline void malformed(const std::string& what) {
    throw std::runtime_error("Malformed geo index: " + what);
}

// A table's list in a row: n ids at p, none when n is 0.
struct Ids {
    const uint32_t* p = nullptr;
    size_t n = 0;
};
using RowIds = std::array<Ids, TABLES>;
using RowLists = std::array<std::vector<uint32_t>, TABLES>;
inline RowIds ids_of(const RowLists& lists) {
    RowIds out;
    for (size_t t = 0; t < TABLES; t++) out[t] = {lists[t].data(), lists[t].size()};
    return out;
}

inline int level_of(uint64_t cell) { return cell_pairs::level_of(cell); }
inline uint64_t number_of(uint64_t cell, int level) { return cell_pairs::number_of(cell, level); }
inline uint64_t cell_of(uint64_t number, int level) { return cell_pairs::cell_of(number, level); }
// The level's cells: 6 faces of 4^L.
inline uint64_t cells_at(int level) { return uint64_t(6) << (2 * level); }

inline void put_u32(std::vector<uint8_t>& out, uint32_t v) {
    for (int b = 0; b < 4; b++) out.push_back(static_cast<uint8_t>(v >> (8 * b)));
}
inline void put_u64(std::vector<uint8_t>& out, uint64_t v) {
    for (int b = 0; b < 8; b++) out.push_back(static_cast<uint8_t>(v >> (8 * b)));
}
inline uint32_t get_u32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}
inline uint64_t get_u64(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    return v;
}

// A varint's length, and the varint written at p (returning past it).
inline unsigned varint_bytes(uint64_t v) {
    unsigned n = 1;
    for (; v >= 0x80; v >>= 7) n++;
    return n;
}
inline uint8_t* write_varint(uint8_t* p, uint64_t v) {
    for (; v >= 0x80; v >>= 7) *p++ = static_cast<uint8_t>(v | 0x80);
    *p++ = static_cast<uint8_t>(v);
    return p;
}

// Bytes written through a pointer: room(n) gives at least n bytes past
// size() (zeroed once, kept between uses), end_at(p) sets where they end.
class ByteBuffer {
public:
    uint8_t* room(size_t n) {
        if (size_ + n > buf_.size()) buf_.resize(std::max(buf_.size() * 2, size_ + n));
        return buf_.data() + size_;
    }
    void end_at(const uint8_t* p) { size_ = static_cast<size_t>(p - buf_.data()); }
    void append(const uint8_t* p, size_t n) {
        if (n) std::memcpy(room(n), p, n);
        size_ += n;
    }
    const uint8_t* data() const { return buf_.data(); }
    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }
    void clear() { size_ = 0; }

private:
    std::vector<uint8_t> buf_;
    size_t size_ = 0;
};

// Encodes the rows of the blocks it is given, a block at a time: start() at
// each block's first row, then row() for each of its rows in cell order.
// Holds the previous list of each table in the block. Each list is sized as
// PLAIN and, where it can be one, as EDIT, then written once as the shorter.
class BlockEncoder {
public:
    void start() {
        rows_ = 0;
        for (size_t t = 0; t < TABLES; t++) {
            have_[t] = false;
            lf_[t] = 0;
        }
    }

    // Appends the row of the cell numbered `number` (its number at the
    // index's level, past the block's last row) to out. Its lists are
    // ascending, at most LIST_MAX ids each, and at least one has ids.
    void row(ByteBuffer& out, uint64_t number, const RowIds& lists) {
        bool any = false, rep[TABLES] = {};
        // The most a row takes: its header and step, and each list as PLAIN (an EDIT is only taken shorter).
        size_t most = 11;
        for (size_t t = 0; t < TABLES; t++) {
            const Ids& l = lists[t];
            if (l.n > LIST_MAX)
                throw std::runtime_error("Cell holds " + std::to_string(l.n) + " entries, more than " +
                                         std::to_string(LIST_MAX));
            for (size_t i = 1; i < l.n; i++) {
                if (l.p[i] < l.p[i - 1]) throw std::runtime_error("geo_index: a list out of order");
                rep[t] |= l.p[i] == l.p[i - 1];
            }
            any |= l.n > 0;
            most += 3 + 5 * l.n;
        }
        if (!any) throw std::runtime_error("geo_index: a cell without lists");
        if (rows_ > 0 && number <= prev_) throw std::runtime_error("geo_index: cells out of order");
        uint8_t* const start = out.room(most);
        uint8_t* p = start + 1;
        const uint64_t step = rows_ > 0 ? number - prev_ : 0;
        const unsigned d = rows_ == 0 ? 0 : step <= 3 ? static_cast<unsigned>(step - 1) : 3;
        if (d == 3) p = write_varint(p, step - 4);
        unsigned header = d << 6;
        for (size_t t = 0; t < TABLES; t++)
            if (lists[t].n > 0) header |= put_list(p, t, lists[t], rep[t]) << (2 * t);
        *start = static_cast<uint8_t>(header);
        out.end_at(p);
        prev_ = number;
        rows_++;
    }

    size_t rows() const { return rows_; }

private:
    // Writes the list at p (moved past it) and returns its code.
    unsigned put_list(uint8_t*& p, size_t t, const Ids& l, bool rep) {
        std::vector<uint32_t>& prev = prev_list_[t];
        const bool prior = have_[t] && t != ADDRESSES;
        unsigned code = PLAIN;
        if (prior && prev.size() == l.n && std::equal(l.p, l.p + l.n, prev.begin())) {
            return SAME;  // the list kept is this one
        } else {
            const uint64_t head = uint64_t(l.n) << 1 | rep;
            const uint64_t first = cell_pairs::zigzag(int64_t(l.p[0]) - int64_t(lf_[t]));
            const uint32_t inc = rep ? 0 : 1;
            size_t plain = varint_bytes(head) + varint_bytes(first);
            for (size_t i = 1; i < l.n; i++) plain += varint_bytes(l.p[i] - l.p[i - 1] - inc);
            // An edit takes its ops and its count at least: past PLAIN's length it isn't sized.
            size_t edit = 0;
            if (prior && !rep && !prev_rep_[t] && plain > (prev.size() + 7) / 8 + 1) {
                edit = size_edit(prev, l, lf_[t]);
                if (edit < plain) code = EDIT;
            }
            if (code == EDIT) {
                p = std::copy(ops_.begin(), ops_.end(), p);
                p = write_varint(p, adds_.size());
                for (size_t k = 0; k < adds_.size(); k++)
                    p = write_varint(p, k ? uint64_t(adds_[k]) - adds_[k - 1] - 1
                                          : cell_pairs::zigzag(int64_t(adds_[0]) - int64_t(lf_[t])));
            } else {
                p = write_varint(write_varint(p, head), first);
                for (size_t i = 1; i < l.n; i++) p = write_varint(p, l.p[i] - l.p[i - 1] - inc);
                lf_[t] = l.p[0];
            }
        }
        prev.assign(l.p, l.p + l.n);
        prev_rep_[t] = rep;
        have_[t] = true;
        return code;
    }

    // The EDIT of prev into l (neither with a repeated id) sized, its ops
    // and adds kept to write: one walk of both.
    size_t size_edit(const std::vector<uint32_t>& prev, const Ids& l, uint32_t lf) {
        ops_.assign((prev.size() + 7) / 8, 0);
        adds_.clear();
        size_t i = 0, j = 0;
        while (i < prev.size() || j < l.n) {
            if (j == l.n || (i < prev.size() && prev[i] < l.p[j])) {
                ops_[i / 8] |= static_cast<uint8_t>(1u << (i % 8));  // dropped
                i++;
            } else if (i == prev.size() || l.p[j] < prev[i]) {
                adds_.push_back(l.p[j++]);
            } else {
                i++;
                j++;
            }
        }
        size_t n = ops_.size() + varint_bytes(adds_.size());
        for (size_t k = 0; k < adds_.size(); k++)
            n += varint_bytes(k ? uint64_t(adds_[k]) - adds_[k - 1] - 1 : cell_pairs::zigzag(int64_t(adds_[0]) - int64_t(lf)));
        return n;
    }

    size_t rows_ = 0;
    uint64_t prev_ = 0;
    bool have_[TABLES] = {}, prev_rep_[TABLES] = {};
    uint32_t lf_[TABLES] = {};
    std::array<std::vector<uint32_t>, TABLES> prev_list_;
    std::vector<uint8_t> ops_;
    std::vector<uint32_t> adds_;
};

// A block encoded on its own (the builder's cores encode them in parallel):
// its first and last cells, its rows and its bytes.
struct EncodedBlock {
    uint64_t first_cell = 0, last_cell = 0;
    size_t rows = 0;
    std::vector<uint8_t> bytes;
};

// Writes an index's two files front to back through top(bytes, n) and
// blocks(bytes, n): add() each cell's lists in cell order, then finish().
// Holds the previous list of each table in the block, the footer of the
// group it is at (64 entries) and up to FLUSH_BYTES of the blocks file before
// it hands them on.
template <typename TopOut, typename BlocksOut>
class Writer {
public:
    Writer(TopOut top, BlocksOut blocks) : top_out_(std::move(top)), blocks_out_(std::move(blocks)) {}

    // The next row: a cell past the last one, at the level of the first, and
    // its lists (ascending, repeats allowed, at most LIST_MAX ids each; an
    // empty one is none), at least one of them with ids.
    void add(uint64_t cell, const RowIds& lists) {
        const uint64_t number = next_cell(cell);
        if (rows_ % BLOCK_ROWS == 0) {
            begin_block(cell);
            encoder_.start();
        }
        const size_t before = pending_.size();
        encoder_.row(pending_, number, lists);
        blocks_bytes_ += pending_.size() - before;
        last_ = number;
        rows_++;
        if (pending_.size() >= FLUSH_BYTES) flush();
    }

    // A whole block of rows encoded by a BlockEncoder from its first row:
    // the same bytes add() would write for them. Every block but the last
    // holds BLOCK_ROWS rows.
    void add_block(const EncodedBlock& b) {
        if (b.rows == 0 || b.rows > BLOCK_ROWS || b.bytes.empty())
            throw std::logic_error("geo_index::Writer: a block of " + std::to_string(b.rows) + " rows");
        if (rows_ % BLOCK_ROWS != 0) throw std::logic_error("geo_index::Writer: a block after a short one");
        next_cell(b.first_cell);
        if (level_of(b.last_cell) != level_ || b.last_cell < b.first_cell || (b.rows > 1) != (b.last_cell > b.first_cell))
            throw std::logic_error("geo_index::Writer: a block whose last cell isn't past its first");
        begin_block(b.first_cell);
        put_blocks(b.bytes.data(), b.bytes.size());
        last_ = number_of(b.last_cell, level_);
        rows_ += b.rows;
    }

    // Writes what is left: the last group's footer and the top's footer.
    void finish() {
        if (finished_) return;
        finished_ = true;
        if (level_ < 0) return;
        end_group();
        flush();
        std::vector<uint8_t> footer;
        put_u64(footer, rows_);
        put_top(footer);
    }

    uint64_t rows() const { return rows_; }
    uint64_t top_bytes() const { return top_bytes_; }
    uint64_t blocks_bytes() const { return blocks_bytes_; }

private:
    // The cell's number, the header written at the first one.
    uint64_t next_cell(uint64_t cell) {
        if (finished_) throw std::logic_error("geo_index::Writer: add after finish");
        if (level_ < 0) {
            const int level = level_of(cell);
            if (level < MIN_LEVEL || level > MAX_LEVEL)
                throw std::runtime_error("geo_index::Writer: cell " + std::to_string(cell) + " is not a cell of levels " +
                                         std::to_string(MIN_LEVEL) + " to " + std::to_string(MAX_LEVEL));
            level_ = level;
            const std::vector<uint8_t> header = {uint8_t(MAGIC[0]), uint8_t(MAGIC[1]), uint8_t(MAGIC[2]),
                                                 uint8_t(MAGIC[3]), uint8_t(level_), uint8_t(BLOCK_ROWS),
                                                 uint8_t(GROUP_BLOCKS), 0};
            put_top(header);
            return number_of(cell, level_);
        }
        if (level_of(cell) != level_) throw std::runtime_error("geo_index::Writer: cells of two levels");
        const uint64_t number = number_of(cell, level_);
        if (number <= last_) throw std::runtime_error("geo_index::Writer: cells out of order");
        return number;
    }

    void begin_block(uint64_t first_cell) {
        if (group_blocks_ == GROUP_BLOCKS) end_group();
        if (group_blocks_ == 0) group_first_ = first_cell;
        put_u64(footer_, first_cell);
        put_u32(footer_, offset());
        group_blocks_++;
    }

    void end_group() {
        if (group_blocks_ == 0) return;
        std::vector<uint8_t> entry;
        put_u64(entry, group_first_);
        put_u32(entry, offset());
        put_blocks(footer_.data(), footer_.size());
        put_top(entry);
        footer_.clear();
        group_blocks_ = 0;
    }

    // Where the next bytes of the blocks file go.
    uint32_t offset() const {
        if (blocks_bytes_ > 0xFFFFFFFFull) throw std::runtime_error("Geo index blocks past 4 GiB");
        return static_cast<uint32_t>(blocks_bytes_);
    }
    void put_top(const std::vector<uint8_t>& bytes) {
        top_out_(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        top_bytes_ += bytes.size();
    }
    void put_blocks(const uint8_t* p, size_t n) {
        blocks_bytes_ += n;
        if (n >= FLUSH_BYTES) {
            flush();
            blocks_out_(reinterpret_cast<const char*>(p), n);
            return;
        }
        pending_.append(p, n);
        if (pending_.size() >= FLUSH_BYTES) flush();
    }
    void flush() {
        if (pending_.empty()) return;
        blocks_out_(reinterpret_cast<const char*>(pending_.data()), pending_.size());
        pending_.clear();
    }

    TopOut top_out_;
    BlocksOut blocks_out_;
    bool finished_ = false;
    int level_ = -1;
    uint64_t rows_ = 0, last_ = 0, group_first_ = 0, top_bytes_ = 0, blocks_bytes_ = 0;
    size_t group_blocks_ = 0;
    std::vector<uint8_t> footer_;
    ByteBuffer pending_;
    BlockEncoder encoder_;
};

template <typename TopOut, typename BlocksOut>
Writer(TopOut, BlocksOut) -> Writer<TopOut, BlocksOut>;

// The varint at p, at least 10 bytes before the input's end, p past it; as
// the inputs' varint() refuses one past 64 bits.
inline uint64_t fast_varint(const uint8_t*& p) {
    uint64_t v = 0;
    for (int shift = 0; shift < 64; shift += 7) {
        const uint8_t b = *p++;
        if (shift == 63 && b > 1) break;
        v |= uint64_t(b & 0x7F) << shift;
        if (!(b & 0x80)) return v;
    }
    malformed("a varint past 64 bits");
}

// Bytes in memory, read from `pos` up to `limit` (a block's end).
class SpanIn {
public:
    SpanIn(const uint8_t* data, uint64_t pos, uint64_t limit) : data_(data), pos_(pos), limit_(limit) {}
    uint64_t pos() const { return pos_; }
    uint8_t byte() {
        if (pos_ >= limit_) malformed("a row past its block's end");
        return data_[pos_++];
    }
    uint64_t varint() {
        if (limit_ - pos_ >= 10) {
            const uint8_t* p = data_ + pos_;
            const uint64_t v = fast_varint(p);
            pos_ = static_cast<uint64_t>(p - data_);
            return v;
        }
        uint64_t v = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            const uint8_t b = byte();
            if (shift == 63 && b > 1) break;
            v |= uint64_t(b & 0x7F) << shift;
            if (!(b & 0x80)) return v;
        }
        malformed("a varint past 64 bits");
    }

private:
    const uint8_t* data_;
    uint64_t pos_, limit_;
};

// A file read front to back through size() and at(off, n), a view valid
// until the next call (SequentialFileReader), a byte at a time, never past
// limit (a block's end); skip() steps forward.
template <typename Src>
class ByteIn {
public:
    explicit ByteIn(Src& src) : src_(src), size_(src.size()), limit_(size_) {}
    uint64_t pos() const { return base_ + static_cast<uint64_t>(p_ - begin_); }
    uint64_t size() const { return size_; }
    // Reads stop at `end`: bytes already in the window past it are dropped.
    void limit(uint64_t end) {
        const uint64_t at = pos();
        limit_ = end;
        if (end_ && at + static_cast<uint64_t>(end_ - p_) > end) end_ = p_ + (end > at ? end - at : 0);
    }
    void skip(uint64_t n) {
        base_ = pos() + n;
        begin_ = p_ = end_ = nullptr;
    }
    uint8_t byte() {
        if (p_ == end_) refill();
        return *p_++;
    }
    uint64_t varint() {
        if (end_ - p_ >= 10) return fast_varint(p_);
        uint64_t v = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            const uint8_t b = byte();
            if (shift == 63 && b > 1) break;
            v |= uint64_t(b & 0x7F) << shift;
            if (!(b & 0x80)) return v;
        }
        malformed("a varint past 64 bits");
    }

private:
    static constexpr uint64_t CHUNK = 4096;
    void refill() {
        base_ = pos();
        const uint64_t n = std::min<uint64_t>(CHUNK, limit_ - std::min(base_, limit_));
        if (n == 0) malformed("a row past its block's end");
        begin_ = p_ = reinterpret_cast<const uint8_t*>(src_.at(base_, static_cast<size_t>(n)));
        end_ = p_ + n;
    }
    Src& src_;
    uint64_t size_, limit_, base_ = 0;
    const uint8_t *begin_ = nullptr, *p_ = nullptr, *end_ = nullptr;
};

// Decodes a block's rows from an input (SpanIn, ByteIn) at the block's
// start: start(first cell's number), then row() for each, checking each row
// as it goes. Holds each table's previous list in the block.
class BlockDecoder {
public:
    explicit BlockDecoder(int level) : cells_(cells_at(level)) {}

    void start(uint64_t first) {
        rows_ = 0;
        number_ = first;
        for (size_t t = 0; t < TABLES; t++) {
            have_[t] = false;
            lf_[t] = 0;
        }
    }

    // The next row's cell number and lists (an empty one is none).
    template <typename In>
    uint64_t row(In& in, RowLists& lists) {
        const unsigned header = in.byte();
        const unsigned d = header >> 6;
        if (rows_ == 0) {
            if (d != 0) malformed("a step on a block's first row");
        } else {
            uint64_t step = d + 1;
            if (d == 3) {
                const uint64_t more = in.varint();
                if (more >= cells_) malformed("a cell past the level's cells");
                step = more + 4;
            }
            if (step >= cells_ - number_) malformed("a cell past the level's cells");
            number_ += step;
        }
        if ((header & 0x3F) == 0) malformed("a row without lists");
        for (size_t t = 0; t < TABLES; t++) {
            const unsigned code = (header >> (2 * t)) & 3;
            std::vector<uint32_t>& l = lists[t];
            if (code == NONE) {
                l.clear();
                continue;
            }
            if (code != PLAIN && (t == ADDRESSES || !have_[t]))
                malformed(t == ADDRESSES ? "an address list that isn't PLAIN" : "a list against none before it");
            if (code == PLAIN) {
                get_plain(in, l, lf_[t]);
                lf_[t] = l[0];
                prev_[t] = l;
            } else if (code == SAME) {
                l = prev_[t];
            } else {
                get_edit(in, prev_[t], l, lf_[t]);
                prev_[t] = l;
            }
            have_[t] = true;
        }
        rows_++;
        return number_;
    }

    size_t rows() const { return rows_; }

private:
    template <typename In>
    static void get_plain(In& in, std::vector<uint32_t>& ids, uint32_t lf) {
        const uint64_t h = in.varint();
        const bool rep = h & 1;
        const uint64_t n = h >> 1;
        if (n == 0) malformed("an empty list");
        if (n > LIST_MAX) malformed("a list of more than 65535 ids");
        // A first past 2^34 (zigzag of the gap from the last first) is past any id; bounded so before it adds up.
        const uint64_t coded = in.varint();
        if (coded >> 34) malformed("an id past 32 bits");
        int64_t v = int64_t(lf) + cell_pairs::unzigzag(coded);
        if (v < 0 || v > int64_t(0xFFFFFFFF)) malformed("an id past 32 bits");
        ids.clear();
        ids.push_back(static_cast<uint32_t>(v));
        for (uint64_t i = 1; i < n; i++) {
            const uint64_t gap = in.varint();
            if (gap > 0xFFFFFFFFull || uint64_t(v) + gap + (rep ? 0 : 1) > 0xFFFFFFFFull) malformed("an id past 32 bits");
            v += static_cast<int64_t>(gap + (rep ? 0 : 1));
            ids.push_back(static_cast<uint32_t>(v));
        }
    }

    template <typename In>
    static void get_edit(In& in, const std::vector<uint32_t>& prev, std::vector<uint32_t>& ids, uint32_t lf) {
        if (cell_pairs::has_repeats(prev.data(), prev.size())) malformed("an edit of a list with a repeated id");
        ids.clear();
        uint8_t byte = 0;
        for (size_t i = 0; i < prev.size(); i++) {
            if (i % 8 == 0) byte = in.byte();
            if (!((byte >> (i % 8)) & 1)) ids.push_back(prev[i]);
        }
        if (prev.size() % 8 != 0 && (byte >> (prev.size() % 8)) != 0) malformed("edit ops past the list");
        const size_t kept = ids.size();
        const uint64_t adds = in.varint();
        if (adds > LIST_MAX - kept) malformed("a list of more than 65535 ids");
        // The kept ids move to the end and the adds (ascending) merge in from the front as they are read, a kept id
        // before an add of its value as std::inplace_merge puts it: the merge never writes past what it has still to
        // read, so it needs no buffer (inplace_merge took one for every edit).
        const size_t n = kept + static_cast<size_t>(adds);
        ids.resize(n);
        uint32_t* const d = ids.data();
        if (kept > 0 && adds > 0) std::memmove(d + adds, d, kept * sizeof(uint32_t));
        size_t i = static_cast<size_t>(adds), k = 0;
        int64_t v = 0;
        for (uint64_t j = 0; j < adds; j++) {
            const uint64_t coded = in.varint();
            if (coded >> 34) malformed("an id past 32 bits");
            v = j ? v + int64_t(coded) + 1 : int64_t(lf) + cell_pairs::unzigzag(coded);
            if (v < 0 || v > int64_t(0xFFFFFFFF)) malformed("an id past 32 bits");
            const uint32_t id = static_cast<uint32_t>(v);
            while (i < n && d[i] <= id) d[k++] = d[i++];
            d[k++] = id;
        }
        if (ids.empty()) malformed("an empty list");
        if (cell_pairs::has_repeats(ids.data(), ids.size())) malformed("an edit to a list with a repeated id");
    }

    uint64_t cells_;
    size_t rows_ = 0;
    uint64_t number_ = 0;
    bool have_[TABLES] = {};
    uint32_t lf_[TABLES] = {};
    std::array<std::vector<uint32_t>, TABLES> prev_;
};

// The header's level, checking the rest of it.
inline int header_level(const uint8_t* h) {
    if (std::memcmp(h, MAGIC, 4) != 0) malformed("not a GXT2 top file");
    const int level = h[4];
    if (level < MIN_LEVEL || level > MAX_LEVEL || h[5] != BLOCK_ROWS || h[6] != GROUP_BLOCKS || h[7] != 0)
        malformed("a header past the format it takes");
    return level;
}

// The blocks and groups of an index of `rows` rows.
inline uint64_t blocks_of(uint64_t rows) { return (rows + BLOCK_ROWS - 1) / BLOCK_ROWS; }
inline uint64_t groups_of(uint64_t rows) { return (blocks_of(rows) + GROUP_BLOCKS - 1) / GROUP_BLOCKS; }

// Reads an index's two files front to back (each through size() and
// at(off, n), as ByteIn; `footers` is a second reader of the blocks file,
// which reads each group's footer ahead of its blocks), checking them as it
// goes: next() gives each row's cell and lists in cell order, then checks
// both files end there. Holds a group's footer, a row's lists and each
// table's previous one in the block. Every refusal is a "Malformed geo
// index" runtime_error.
template <typename Top, typename Blocks, typename Footers>
class Reader {
public:
    Reader(Top& top, Blocks& blocks, Footers& footers)
        : top_(top), footers_(footers), in_(blocks), top_size_(top.size()), decoder_(MIN_LEVEL) {
        if (top_size_ == 0) {
            if (in_.size() != 0) malformed("blocks without a top file");
            done_ = true;
            return;
        }
        if (top_size_ < HEADER_BYTES + ENTRY_BYTES + FOOTER_BYTES || (top_size_ - HEADER_BYTES - FOOTER_BYTES) % ENTRY_BYTES)
            malformed("a top file of no whole groups");
        if (footers.size() != in_.size()) malformed("a footer reader of another file");
        level_ = header_level(reinterpret_cast<const uint8_t*>(top_.at(0, HEADER_BYTES)));
        decoder_ = BlockDecoder(level_);
        groups_ = (top_size_ - HEADER_BYTES - FOOTER_BYTES) / ENTRY_BYTES;
    }
    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;

    // The index's level, -1 for an index of no rows.
    int level() const { return level_; }
    uint64_t rows() const { return rows_; }

    // The next row's cell and lists (replaced; an empty one is none), false
    // past the last.
    bool next(uint64_t& cell, RowLists& lists) {
        if (done_) return false;
        while (!in_block_ || in_.pos() == block_end_) {
            if (in_block_) end_block();
            if (block_ == group_blocks_ && !next_group()) {
                finish();
                return false;
            }
            start_block();
        }
        const uint64_t number = decoder_.row(in_, lists);
        if (decoder_.rows() > BLOCK_ROWS) malformed("a block of more than 64 rows");
        cell = cell_of(number, level_);
        last_ = number;
        rows_++;
        return true;
    }

private:
    bool next_group() {
        if (group_ > 0) {
            // Past the group's footer, read ahead: its last block ended there.
            in_.limit(in_.size());
            in_.skip(group_blocks_ * ENTRY_BYTES);
        }
        if (group_ == groups_) return false;
        const uint8_t* e = reinterpret_cast<const uint8_t*>(top_.at(HEADER_BYTES + group_ * ENTRY_BYTES, ENTRY_BYTES));
        const uint64_t first = get_u64(e);
        footer_at_ = get_u32(e + 8);
        const uint64_t blocks_size = in_.size();
        if (footer_at_ <= in_.pos() || footer_at_ > blocks_size) malformed("a group's footer out of place");
        if (group_ + 1 < groups_) {
            group_blocks_ = GROUP_BLOCKS;
            if (blocks_size - footer_at_ < GROUP_BLOCKS * ENTRY_BYTES) malformed("a group's footer past the blocks file");
        } else {
            const uint64_t tail = blocks_size - footer_at_;
            if (tail % ENTRY_BYTES != 0 || tail == 0 || tail > GROUP_BLOCKS * ENTRY_BYTES)
                malformed("a last footer that doesn't end the blocks file");
            group_blocks_ = static_cast<size_t>(tail / ENTRY_BYTES);
        }
        const uint8_t* f = reinterpret_cast<const uint8_t*>(footers_.at(footer_at_, group_blocks_ * ENTRY_BYTES));
        footer_.assign(f, f + group_blocks_ * ENTRY_BYTES);
        if (get_u64(footer_.data()) != first) malformed("a group whose first cell isn't its first block's");
        group_++;
        block_ = 0;
        return true;
    }

    void start_block() {
        const uint8_t* e = footer_.data() + block_ * ENTRY_BYTES;
        const uint64_t cell = get_u64(e), at = get_u32(e + 8);
        if (at != in_.pos()) malformed("a block where none starts");
        if (level_of(cell) != level_) malformed("a block's first cell not at the index's level");
        const uint64_t number = number_of(cell, level_);
        if (rows_ > 0 && number <= last_) malformed("blocks out of cell order");
        block_end_ = block_ + 1 < group_blocks_ ? get_u32(e + ENTRY_BYTES + 8) : footer_at_;
        if (block_end_ <= at) malformed("a block without rows");
        in_.limit(block_end_);
        decoder_.start(number);
        in_block_ = true;
    }

    void end_block() {
        in_block_ = false;
        block_++;
        // Only the index's last block can be short.
        const bool last = group_ == groups_ && block_ == group_blocks_;
        if (!last && decoder_.rows() != BLOCK_ROWS) malformed("a block of fewer than 64 rows before the last");
    }

    void finish() {
        done_ = true;
        const uint8_t* f = reinterpret_cast<const uint8_t*>(top_.at(top_size_ - FOOTER_BYTES, FOOTER_BYTES));
        if (get_u64(f) != rows_ || groups_of(rows_) != groups_) malformed("a top footer that isn't the rows'");
    }

    Top& top_;
    Footers& footers_;
    ByteIn<Blocks> in_;
    uint64_t top_size_;
    int level_ = -1;
    bool done_ = false, in_block_ = false;
    uint64_t groups_ = 0, group_ = 0, rows_ = 0, last_ = 0, footer_at_ = 0, block_end_ = 0;
    size_t group_blocks_ = 0, block_ = 0;
    std::vector<uint8_t> footer_;
    BlockDecoder decoder_;
};

// An index in memory (mapped files), its blocks read in any order: the
// diff's walks over both days, split across cores. The constructor checks
// the header and both files' sizes against the top footer; a block's
// accessors check its group, and its rows are checked as they decode.
class View {
public:
    struct Block {
        uint64_t first_cell, begin, end;
        size_t rows;
    };

    View(const char* top, size_t top_size, const char* blocks, size_t blocks_size)
        : top_(reinterpret_cast<const uint8_t*>(top)), blocks_(reinterpret_cast<const uint8_t*>(blocks)),
          blocks_size_(blocks_size) {
        if (top_size == 0) {
            if (blocks_size != 0) malformed("blocks without a top file");
            return;
        }
        if (top_size < HEADER_BYTES + ENTRY_BYTES + FOOTER_BYTES) malformed("a top file of no whole groups");
        level_ = header_level(top_);
        rows_ = get_u64(top_ + top_size - FOOTER_BYTES);
        groups_ = groups_of(rows_);
        if (rows_ == 0 || top_size != HEADER_BYTES + groups_ * ENTRY_BYTES + FOOTER_BYTES)
            malformed("a top footer that isn't the rows'");
        blocks_count_ = blocks_of(rows_);
        const uint64_t last_blocks = blocks_count_ - (groups_ - 1) * GROUP_BLOCKS;
        if (footer_at(groups_ - 1) + last_blocks * ENTRY_BYTES != blocks_size)
            malformed("a last footer that doesn't end the blocks file");
    }

    int level() const { return level_; }
    uint64_t rows() const { return rows_; }
    size_t blocks() const { return static_cast<size_t>(blocks_count_); }

    // Block b: its first cell, its bytes [begin, end) and its rows.
    Block block(size_t b) const {
        const uint64_t g = b / GROUP_BLOCKS, j = b % GROUP_BLOCKS;
        const uint64_t at = footer_at(g);
        const uint64_t n = g + 1 < groups_ ? GROUP_BLOCKS : blocks_count_ - g * GROUP_BLOCKS;
        if (at + n * ENTRY_BYTES > blocks_size_) malformed("a group's footer past the blocks file");
        const uint8_t* e = blocks_ + at + j * ENTRY_BYTES;
        Block out{get_u64(e), get_u32(e + 8), j + 1 < n ? get_u32(e + ENTRY_BYTES + 8) : at,
                  b + 1 < blocks_count_ ? BLOCK_ROWS : static_cast<size_t>(rows_ - (blocks_count_ - 1) * BLOCK_ROWS)};
        if (out.begin >= out.end || out.end > at) malformed("a block out of place");
        if (j == 0 && out.first_cell != get_u64(top_ + HEADER_BYTES + g * ENTRY_BYTES))
            malformed("a group whose first cell isn't its first block's");
        if (level_of(out.first_cell) != level_) malformed("a block's first cell not at the index's level");
        return out;
    }

    // The last block whose first cell is at or before `cell`, blocks() when
    // none is.
    size_t block_of(uint64_t cell) const {
        if (blocks_count_ == 0) return 0;
        size_t lo = 0, hi = static_cast<size_t>(groups_);
        while (lo < hi) {
            const size_t mid = lo + (hi - lo) / 2;
            if (get_u64(top_ + HEADER_BYTES + mid * ENTRY_BYTES) <= cell) lo = mid + 1;
            else hi = mid;
        }
        if (lo == 0) return blocks();
        const size_t g = lo - 1, first = g * GROUP_BLOCKS;
        const size_t n = static_cast<size_t>(std::min<uint64_t>(GROUP_BLOCKS, blocks_count_ - first));
        size_t a = 0, z = n;
        while (a < z) {
            const size_t mid = a + (z - a) / 2;
            if (block(first + mid).first_cell <= cell) a = mid + 1;
            else z = mid;
        }
        return a == 0 ? blocks() : first + a - 1;
    }

    // A walk's state: the decoder and a row's lists, and for canonical() an
    // encoder and its bytes.
    struct Scratch {
        explicit Scratch(int level) : decoder(level) {}
        BlockDecoder decoder;
        RowLists lists;
        BlockEncoder encoder;
        ByteBuffer bytes;
    };
    Scratch scratch() const { return Scratch(level_ < 0 ? MIN_LEVEL : level_); }

    // Decodes block b's rows into f(cell, lists), checking that it holds
    // its rows exactly and that they stay before the next block's.
    template <typename F>
    void rows_of(size_t b, Scratch& s, F&& f) const {
        const Block k = block(b);
        SpanIn in(blocks_, k.begin, k.end);
        s.decoder.start(number_of(k.first_cell, level_));
        const uint64_t next = b + 1 < blocks() ? number_of(block(b + 1).first_cell, level_) : cells_at(level_);
        for (size_t r = 0; r < k.rows; r++) {
            const uint64_t number = s.decoder.row(in, s.lists);
            if (number >= next) malformed("blocks out of cell order");
            f(cell_of(number, level_), static_cast<const RowLists&>(s.lists));
        }
        if (in.pos() != k.end) malformed("a block of more rows than it holds");
    }

    // Whether block b is the bytes a Writer writes for its rows, starting
    // where the Writer starts it.
    bool canonical(size_t b, Scratch& s) const {
        const Block k = block(b);
        // A group's first block starts where the group before it ends.
        if (b % GROUP_BLOCKS == 0 && k.begin != (b == 0 ? 0 : footer_at(b / GROUP_BLOCKS - 1) + GROUP_BLOCKS * ENTRY_BYTES))
            return false;
        s.encoder.start();
        s.bytes.clear();
        rows_of(b, s, [&](uint64_t cell, const RowLists& l) { s.encoder.row(s.bytes, number_of(cell, level_), ids_of(l)); });
        return s.bytes.size() == k.end - k.begin && std::memcmp(s.bytes.data(), blocks_ + k.begin, s.bytes.size()) == 0;
    }

private:
    uint64_t footer_at(uint64_t g) const { return get_u32(top_ + HEADER_BYTES + g * ENTRY_BYTES + 8); }

    const uint8_t *top_, *blocks_;
    size_t blocks_size_;
    int level_ = -1;
    uint64_t rows_ = 0, groups_ = 0, blocks_count_ = 0;
};

}  // namespace geo_index
