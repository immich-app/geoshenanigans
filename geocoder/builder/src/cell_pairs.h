// The cell pair indexes: the admin, POI and postcode centroid indexes, each
// a sorted list of ids per cell, in a cells file and an entries file that are
// both written front to back in one pass, so the patcher rebuilds them with a
// block in memory (Writer) from the old ones read the same way (Reader). No
// S2 here, so the tools and the unit tests can read and write them.
//
// <prefix>_cells.bin, the cells file:
//   header, 8 bytes: "GCP1", the kind (1 blocks, 2 direct), flags (bit 0:
//     interior groups; bit 1: group varint gaps), the cell level L, 0.
//   the parent records, ascending by parent. A parent is a cell at level
//     L - 5, numbered face * 4^(L - 5) + its position along the face; its
//     record, at a 4-byte boundary:
//       its number (u32);
//       its leaf, 16 u64 words: bit j % 64 of word j / 64 says its child j at
//       level L, in S2 order, has a list; such a cell is a row of the index;
//       u32 offsets into the entries file: blocks kind, one per block of 8 of
//       its rows (block m holds its rows 8m to 8m + 7); direct kind, one per
//       row.
//   0xFFFFFFFF, past the last record;
//   the parent bitmap: u64 words, bit p % 64 of word p / 64 set for each
//     parent p with a record, up to the word of the last one;
//   the ranks: per word of the bitmap, the number of parents before it (u32);
//   the directory: per record, in order, where it starts in this file (u32);
//   footer, 8 bytes: the number of records (u32), of bitmap words (u32).
//   An index of no cells is an empty cells file and an empty entries file.
// <prefix>_entries.bin, the entries file: the blocks (blocks kind) or the
//   lists (direct kind) back to back, each at its offset.
//   A block: a u16 of 2-bit codes, row k's at bit 2k (0 past the block's
//   rows), then each row's payload in order. Code 0 PLAIN: the list. Code 1
//   SAME: nothing, the list is the previous row's. Code 2 EDIT: the edit of
//   the previous row's list into the row's (neither with a repeated id). Row 0
//   is PLAIN.
//   Direct: a row's list; a row whose list is the previous row's has that
//   row's offset.
// A list holds at most LIST_MAX ids, ascending (an id can repeat). With
// interior groups, the ids flagged INTERIOR_FLAG are its group 1, the others
// its group 0 (all are group 0 without). Numbers are varints (LEB128) unless
// said otherwise.
//   PLAIN: n0 << 1 | repeats; with groups, n1; then each group that has ids:
//     its first id without the flag (the bare id), as zigzag(first - the
//     group's last first) in a block or as it is (direct), then each next one
//     as its gap from the one before less 1 (less 0 with repeats): varints,
//     or with group varint gaps, quads of them: a byte of 2-bit lengths less
//     1, gap k's at bit 2k (0 past the group's last gap), then the gaps in as
//     few little-endian bytes as each takes (at least 1). A group's last first
//     in a block is the bare first id of that group in the last list before
//     that has one, 0 at the block's start.
//   EDIT of P into L: ops over P, LSB first: with groups 2 bits each (0 keep,
//     1 drop, 2 keep with the flag toggled), without 1 bit (0 keep, 1 drop),
//     ceil(|P| * bits / 8) bytes, 0 past the last; then n_add and the ids L
//     adds by t(id) (groups: bare id << 1 | flag; else the id) ascending, the
//     first as it is, each next as its gap less 1. L is the ids kept, toggled
//     and added, sorted.
// The Writer's choices are fixed, so a set of lists has one encoding: a
// block row is SAME when its list is the previous row's, else EDIT when both
// lists lack repeats and the edit is shorter than the PLAIN list, else PLAIN;
// an edit toggles an id of P when L holds it with the flag toggled and P
// doesn't; a direct row shares the previous row's offset when its list is the
// same.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace cell_pairs {

inline constexpr char MAGIC[4] = {'G', 'C', 'P', '1'};
inline constexpr size_t HEADER_BYTES = 8, FOOTER_BYTES = 8;
// A parent holds the 4^5 cells below it at the index's level.
inline constexpr int LEAF_LEVELS = 5;
inline constexpr size_t LEAF_CELLS = size_t(1) << (2 * LEAF_LEVELS), LEAF_WORDS = LEAF_CELLS / 64,
                        LEAF_BYTES = LEAF_WORDS * 8;
inline constexpr size_t BLOCK_ROWS = 8;
// A list's count is that of a u16, as the server's lists always were.
inline constexpr uint32_t LIST_MAX = 0xFFFF;
inline constexpr uint32_t FLAG = 0x80000000u, BARE = 0x7FFFFFFFu;
inline constexpr uint32_t END_OF_RECORDS = 0xFFFFFFFFu;
// The levels an index can be at: below 5 there is no parent, and past 13 the
// parent bitmap (6 * 4^(L - 5) bits) outgrows a small region's index.
inline constexpr int MIN_LEVEL = LEAF_LEVELS, MAX_LEVEL = 13;

enum class Kind : uint8_t { BLOCKS = 1, DIRECT = 2 };
enum class Gaps : uint8_t { VARINT = 0, GROUP_VARINT = 1 };
struct Format {
    Kind kind;
    bool groups;  // INTERIOR_FLAG ids coded as a group of their own
    Gaps gaps;    // how a PLAIN list's gaps are coded
    uint8_t flags() const { return uint8_t((groups ? 1 : 0) | (gaps == Gaps::GROUP_VARINT ? 2 : 0)); }
    bool operator==(const Format& o) const { return kind == o.kind && groups == o.groups && gaps == o.gaps; }
};
// Admin cells list a few polygons each, much like their neighbours': blocks
// of 8 rows, each list against the one before. POI and postcode centroid
// cells are read nine at a time and list many ids: one list per row. A POI
// list's gaps vary in length (planet poi/all: 22 ids a cell), which varints
// decode slower than today's u32s; a postcode centroid list's are short and
// alike, which they decode fastest.
inline constexpr Format ADMIN{Kind::BLOCKS, true, Gaps::VARINT}, POI{Kind::DIRECT, true, Gaps::GROUP_VARINT},
                        POSTCODE_CENTROID{Kind::DIRECT, false, Gaps::VARINT};

// The format of the index whose files are <prefix>_cells.bin and
// <prefix>_entries.bin, none for a prefix no index has.
inline std::optional<Format> format_named(const std::string& prefix) {
    if (prefix == "admin") return ADMIN;
    if (prefix == "poi") return POI;
    if (prefix == "postcode_centroid") return POSTCODE_CENTROID;
    return std::nullopt;
}
inline Format format_of(const std::string& prefix) {
    if (const std::optional<Format> f = format_named(prefix)) return *f;
    throw std::logic_error("No cell pair index is named " + prefix);
}

[[noreturn, gnu::cold, gnu::noinline]] inline void malformed(const std::string& what) {
    throw std::runtime_error("Malformed cell index: " + what);
}

// The level of an S2 cell id, -1 when it isn't one.
inline int level_of(uint64_t cell) {
    if (cell == 0 || (cell >> 61) > 5) return -1;
    const int zeros = __builtin_ctzll(cell);
    return zeros % 2 ? -1 : 30 - zeros / 2;
}
// A level-L cell's number among the level's cells: face * 4^L + its position.
inline uint64_t number_of(uint64_t cell, int level) { return cell >> (61 - 2 * level); }
inline uint64_t cell_of(uint64_t number, int level) {
    return number << (61 - 2 * level) | uint64_t(1) << (60 - 2 * level);
}
// The parents of a level-L index: the cells at level L - 5.
inline uint64_t parents_at(int level) { return uint64_t(6) << (2 * (level - LEAF_LEVELS)); }
// The words of a bitmap of every parent at the level: past the last whole
// one below level 8 (6, 24 and 96 parents), at least one.
inline size_t parent_words_at(int level) { return static_cast<size_t>((parents_at(level) + 63) / 64); }

inline void put_varint(std::vector<uint8_t>& out, uint64_t v) {
    for (; v >= 0x80; v >>= 7) out.push_back(static_cast<uint8_t>(v | 0x80));
    out.push_back(static_cast<uint8_t>(v));
}
inline uint64_t zigzag(int64_t v) { return v >= 0 ? uint64_t(v) << 1 : (uint64_t(-(v + 1)) << 1) | 1; }
inline int64_t unzigzag(uint64_t v) { return v & 1 ? -int64_t(v >> 1) - 1 : int64_t(v >> 1); }

inline bool has_repeats(const uint32_t* ids, size_t n) {
    for (size_t i = 1; i < n; i++)
        if (ids[i] == ids[i - 1]) return true;
    return false;
}

// The ids of group 1 (flagged) start at the first flagged id: they sort last.
inline size_t group0_size(const uint32_t* ids, size_t n, bool groups) {
    if (!groups) return n;
    return static_cast<size_t>(std::lower_bound(ids, ids + n, FLAG) - ids);
}

// The gaps of ids[from, to) in quads: a byte of lengths, then the gaps.
inline void put_group_gaps(std::vector<uint8_t>& out, const uint32_t* ids, size_t from, size_t to, bool rep) {
    for (size_t i = from + 1; i < to; i += 4) {
        const size_t lengths_at = out.size();
        out.push_back(0);
        for (size_t q = 0; q < 4 && i + q < to; q++) {
            const uint32_t gap = ids[i + q] - ids[i + q - 1] - (rep ? 0 : 1);
            const unsigned bytes = gap < 1u << 8 ? 1 : gap < 1u << 16 ? 2 : gap < 1u << 24 ? 3 : 4;
            out[lengths_at] |= static_cast<uint8_t>((bytes - 1) << (2 * q));
            for (unsigned b = 0; b < bytes; b++) out.push_back(static_cast<uint8_t>(gap >> (8 * b)));
        }
    }
}

// The PLAIN list of ids[0, n), sorted: lf is the block's last firsts, or
// null for absolute firsts.
inline void put_plain(std::vector<uint8_t>& out, const uint32_t* ids, size_t n, bool groups, Gaps gaps,
                      const uint32_t* lf) {
    const bool rep = has_repeats(ids, n);
    const size_t n0 = group0_size(ids, n, groups);
    put_varint(out, uint64_t(groups ? n0 : n) << 1 | rep);
    if (groups) put_varint(out, n - n0);
    const uint32_t bare = groups ? BARE : 0xFFFFFFFFu;
    for (int g = 0; g < (groups ? 2 : 1); g++) {
        const size_t from = g ? n0 : 0, to = g ? n : (groups ? n0 : n);
        if (from == to) continue;
        const uint32_t first = ids[from] & bare;
        if (lf) put_varint(out, zigzag(int64_t(first) - int64_t(lf[g])));
        else put_varint(out, first);
        if (gaps == Gaps::GROUP_VARINT) put_group_gaps(out, ids, from, to, rep);
        else
            for (size_t i = from + 1; i < to; i++) put_varint(out, uint64_t(ids[i]) - ids[i - 1] - (rep ? 0 : 1));
    }
}

// The firsts a block's later lists code theirs against, after ids[0, n).
inline void update_firsts(const uint32_t* ids, size_t n, bool groups, uint32_t lf[2]) {
    if (n == 0) return;
    const size_t n0 = group0_size(ids, n, groups);
    if (n0 > 0) lf[0] = groups ? ids[0] & BARE : ids[0];
    if (n0 < n) lf[1] = ids[n0] & BARE;
}

inline uint32_t edit_order(uint32_t id, bool groups) { return groups ? (id & BARE) << 1 | id >> 31 : id; }
inline uint32_t edit_id(uint32_t t, bool groups) { return groups ? t >> 1 | t << 31 : t; }

// The EDIT of prev into ids[0, n), neither with a repeated id.
inline void put_edit(std::vector<uint8_t>& out, const std::vector<uint32_t>& prev, const uint32_t* ids, size_t n,
                     bool groups, std::vector<uint32_t>& adds, std::vector<char>& used) {
    const int bits = groups ? 2 : 1;
    const size_t at = out.size();
    out.resize(at + (prev.size() * bits + 7) / 8, 0);
    used.assign(n, 0);
    const uint32_t* end = ids + n;
    for (size_t i = 0; i < prev.size(); i++) {
        unsigned op = 1;
        const uint32_t* hit = std::lower_bound(ids, end, prev[i]);
        if (hit != end && *hit == prev[i]) {
            op = 0;
            used[hit - ids] = 1;
        } else if (groups) {
            const uint32_t toggled = prev[i] ^ FLAG;
            const uint32_t* t = std::lower_bound(ids, end, toggled);
            if (t != end && *t == toggled && !std::binary_search(prev.begin(), prev.end(), toggled)) {
                op = 2;
                used[t - ids] = 1;
            }
        }
        out[at + i * bits / 8] |= static_cast<uint8_t>(op << (i * bits % 8));
    }
    adds.clear();
    for (size_t j = 0; j < n; j++)
        if (!used[j]) adds.push_back(edit_order(ids[j], groups));
    std::sort(adds.begin(), adds.end());
    put_varint(out, adds.size());
    for (size_t j = 0; j < adds.size(); j++) put_varint(out, j ? uint64_t(adds[j]) - adds[j - 1] - 1 : adds[0]);
}

// Writes an index's two files front to back through cells(bytes, n) and
// entries(bytes, n): add() each cell's list in cell order, then finish().
// Holds the block or the parent record it is at (at most 1,024 offsets and 8
// lists), the previous list, and the parent bitmap and directory (a word per
// 64 parents and a u32 per record, 24 KiB at most at level 10).
template <typename CellsOut, typename EntriesOut>
class Writer {
public:
    Writer(Format format, CellsOut cells, EntriesOut entries)
        : format_(format), cells_out_(std::move(cells)), entries_out_(std::move(entries)) {}

    // The next row: a cell past the last one, at the level of the first, and
    // its ids, ascending (repeats allowed), at most LIST_MAX of them.
    void add(uint64_t cell, const uint32_t* ids, size_t n) {
        if (finished_) throw std::logic_error("cell_pairs::Writer: add after finish");
        if (n > LIST_MAX)
            throw std::runtime_error("Cell holds " + std::to_string(n) + " entries, more than " +
                                     std::to_string(LIST_MAX));
        for (size_t i = 1; i < n; i++)
            if (ids[i] < ids[i - 1]) throw std::runtime_error("cell_pairs::Writer: a list out of order");
        if (level_ < 0) start(cell);
        else if (level_of(cell) != level_) throw std::runtime_error("cell_pairs::Writer: cells of two levels");
        else if (cell <= last_cell_) throw std::runtime_error("cell_pairs::Writer: cells out of order");
        last_cell_ = cell;
        const uint64_t number = number_of(cell, level_);
        const uint64_t parent = number >> (2 * LEAF_LEVELS);
        if (parent != parent_) {
            if (parent_ != NONE) end_parent();
            parent_ = parent;
            std::fill(leaf_, leaf_ + LEAF_WORDS, 0);
        }
        const size_t child = number & (LEAF_CELLS - 1);
        leaf_[child / 64] |= uint64_t(1) << (child % 64);
        rows_++;
        if (format_.kind == Kind::BLOCKS) add_block_row(ids, n);
        else add_direct_row(ids, n);
    }

    // Writes what is left: the last block and record, then the tail.
    void finish() {
        if (finished_) return;
        finished_ = true;
        if (level_ < 0) return;
        end_parent();
        put_cells_u32(END_OF_RECORDS);
        const size_t words = static_cast<size_t>(last_parent_ / 64 + 1);
        uint32_t before = 0;
        std::vector<uint32_t> ranks(words);
        for (size_t w = 0; w < words; w++) {
            ranks[w] = before;
            before += static_cast<uint32_t>(__builtin_popcountll(pbits_[w]));
        }
        put_cells(pbits_.data(), words * 8);
        put_cells(ranks.data(), words * 4);
        put_cells(directory_.data(), directory_.size() * 4);
        put_cells_u32(static_cast<uint32_t>(directory_.size()));
        put_cells_u32(static_cast<uint32_t>(words));
    }

    uint64_t rows() const { return rows_; }
    uint64_t cells_bytes() const { return cells_bytes_; }
    uint64_t entries_bytes() const { return entries_bytes_; }

private:
    static constexpr uint64_t NONE = ~uint64_t(0);

    void start(uint64_t cell) {
        level_ = level_of(cell);
        if (level_ < MIN_LEVEL || level_ > MAX_LEVEL)
            throw std::runtime_error("cell_pairs::Writer: cell " + std::to_string(cell) + " is not a cell of levels " +
                                   std::to_string(MIN_LEVEL) + " to " + std::to_string(MAX_LEVEL));
        pbits_.assign(parent_words_at(level_), 0);
        const uint8_t header[HEADER_BYTES] = {uint8_t(MAGIC[0]), uint8_t(MAGIC[1]), uint8_t(MAGIC[2]), uint8_t(MAGIC[3]),
                                              uint8_t(format_.kind), format_.flags(),
                                              uint8_t(level_), 0};
        put_cells(header, sizeof header);
    }

    void put_cells(const void* p, size_t n) {
        if (n == 0) return;
        cells_out_(static_cast<const char*>(p), n);
        cells_bytes_ += n;
    }
    void put_cells_u32(uint32_t v) { put_cells(&v, 4); }
    void put_entries(const std::vector<uint8_t>& bytes) {
        if (bytes.empty()) return;
        entries_out_(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        entries_bytes_ += bytes.size();
    }
    // Where the next bytes of the entries file go, as an offset.
    uint32_t entries_offset() const {
        if (entries_bytes_ > 0xFFFFFFFFull) throw std::runtime_error("Cell index entries past 4 GiB");
        return static_cast<uint32_t>(entries_bytes_);
    }

    void add_block_row(const uint32_t* ids, size_t n) {
        const bool groups = format_.groups;
        if (block_rows_ == 0) {
            block_.assign(2, 0);
            lf_[0] = lf_[1] = 0;
        }
        unsigned code = 0;
        if (block_rows_ > 0 && prev_.size() == n && std::equal(ids, ids + n, prev_.begin())) {
            code = 1;
        } else {
            plain_.clear();
            put_plain(plain_, ids, n, groups, format_.gaps, lf_);
            if (block_rows_ > 0 && !has_repeats(ids, n) && !has_repeats(prev_.data(), prev_.size())) {
                edit_.clear();
                put_edit(edit_, prev_, ids, n, groups, adds_, used_);
                if (edit_.size() < plain_.size()) code = 2;
            }
            const std::vector<uint8_t>& payload = code == 2 ? edit_ : plain_;
            block_.insert(block_.end(), payload.begin(), payload.end());
        }
        const uint16_t codes = static_cast<uint16_t>(block_[0] | block_[1] << 8) | uint16_t(code << (2 * block_rows_));
        block_[0] = static_cast<uint8_t>(codes);
        block_[1] = static_cast<uint8_t>(codes >> 8);
        update_firsts(ids, n, groups, lf_);
        prev_.assign(ids, ids + n);
        if (++block_rows_ == BLOCK_ROWS) end_block();
    }

    void end_block() {
        if (block_rows_ == 0) return;
        offsets_.push_back(entries_offset());
        put_entries(block_);
        block_rows_ = 0;
    }

    void add_direct_row(const uint32_t* ids, size_t n) {
        if (rows_ > 1 && prev_.size() == n && std::equal(ids, ids + n, prev_.begin())) {
            offsets_.push_back(prev_offset_);
            return;
        }
        prev_offset_ = entries_offset();
        offsets_.push_back(prev_offset_);
        plain_.clear();
        put_plain(plain_, ids, n, format_.groups, format_.gaps, nullptr);
        put_entries(plain_);
        prev_.assign(ids, ids + n);
    }

    void end_parent() {
        // A block never spans two parents: the next parent's rows start one.
        if (format_.kind == Kind::BLOCKS) end_block();
        if (cells_bytes_ > 0xFFFFFFFFull) throw std::runtime_error("Cell index cells past 4 GiB");
        directory_.push_back(static_cast<uint32_t>(cells_bytes_));
        pbits_[parent_ / 64] |= uint64_t(1) << (parent_ % 64);
        last_parent_ = parent_;
        put_cells_u32(static_cast<uint32_t>(parent_));
        put_cells(leaf_, LEAF_BYTES);
        put_cells(offsets_.data(), offsets_.size() * 4);
        offsets_.clear();
    }

    Format format_;
    CellsOut cells_out_;
    EntriesOut entries_out_;
    bool finished_ = false;
    int level_ = -1;
    uint64_t last_cell_ = 0, parent_ = NONE, last_parent_ = 0;
    uint64_t leaf_[LEAF_WORDS] = {};
    std::vector<uint32_t> offsets_, directory_;
    std::vector<uint64_t> pbits_;
    std::vector<uint8_t> block_, plain_, edit_;
    size_t block_rows_ = 0;
    uint32_t lf_[2] = {0, 0};
    std::vector<uint32_t> prev_, adds_;
    std::vector<char> used_;
    uint32_t prev_offset_ = 0;
    uint64_t rows_ = 0, cells_bytes_ = 0, entries_bytes_ = 0;
};

template <typename CellsOut, typename EntriesOut>
Writer(Format, CellsOut, EntriesOut) -> Writer<CellsOut, EntriesOut>;

// A file read front to back through size() and at(off, n), a view valid
// until the next call (SequentialFileReader, ByteSpan), a byte at a time.
template <typename Src>
class ByteIn {
public:
    explicit ByteIn(Src& src) : src_(src), size_(src.size()) {}
    uint64_t pos() const { return base_ + static_cast<uint64_t>(p_ - begin_); }
    uint64_t size() const { return size_; }
    uint8_t byte() {
        if (p_ == end_) refill();
        return *p_++;
    }
    uint64_t varint() {
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
        const uint64_t n = std::min<uint64_t>(CHUNK, size_ - base_);
        if (n == 0) malformed("a list past the end of the entries file");
        begin_ = p_ = reinterpret_cast<const uint8_t*>(src_.at(base_, static_cast<size_t>(n)));
        end_ = p_ + n;
    }
    Src& src_;
    uint64_t size_, base_ = 0;
    const uint8_t *begin_ = nullptr, *p_ = nullptr, *end_ = nullptr;
};

// Reads an index's two files front to back (each through size() and
// at(off, n), as ByteIn), checking them as it goes: next() gives each row's
// cell and list in cell order, then checks the tail and that both files end
// there. Holds one parent record, the row's list and the previous one, and
// the parent bitmap and record starts (as the Writer). Every refusal is a
// "Malformed cell index" runtime_error.
template <typename Cells, typename Entries>
class Reader {
public:
    Reader(Cells& cells, Entries& entries, Format format)
        : cells_(cells), entries_(entries), in_(entries), format_(format), cells_size_(cells.size()) {
        if (cells_size_ == 0) {
            if (in_.size() != 0) malformed("entries without cells");
            done_ = true;
            return;
        }
        if (cells_size_ < HEADER_BYTES + 4 + FOOTER_BYTES) malformed("a cells file too short for its header");
        uint8_t h[HEADER_BYTES];
        std::memcpy(h, cells_.at(0, HEADER_BYTES), HEADER_BYTES);
        if (std::memcmp(h, MAGIC, 4) != 0) malformed("not a GCP1 cells file");
        if (h[4] != uint8_t(format.kind) || h[5] != format.flags())
            malformed("a cells file of another kind of index");
        level_ = h[6];
        if (level_ < MIN_LEVEL || level_ > MAX_LEVEL || h[7] != 0) malformed("a header past the levels it takes");
        pbits_.assign(parent_words_at(level_), 0);
        pos_ = HEADER_BYTES;
    }
    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;

    // The index's level, -1 for an index of no cells.
    int level() const { return level_; }
    uint64_t rows() const { return rows_; }

    // The next row's cell and list (ids replaced), false past the last.
    bool next(uint64_t& cell, std::vector<uint32_t>& ids) {
        if (done_) return false;
        while (row_ == row_count_) {
            if (!next_record()) {
                finish();
                return false;
            }
        }
        while (word_bits_ == 0) word_bits_ = leaf_[++word_];
        const unsigned bit = static_cast<unsigned>(__builtin_ctzll(word_bits_));
        word_bits_ &= word_bits_ - 1;
        cell = cell_of(parent_ << (2 * LEAF_LEVELS) | (word_ * 64 + bit), level_);
        if (format_.kind == Kind::BLOCKS) block_row(ids);
        else direct_row(ids);
        row_++;
        rows_++;
        return true;
    }

private:
    uint32_t cells_u32(uint64_t off) {
        uint32_t v;
        std::memcpy(&v, cells_.at(off, 4), 4);
        return v;
    }

    bool next_record() {
        if (pos_ + 4 > cells_size_) malformed("records past the end of the cells file");
        const uint32_t number = cells_u32(pos_);
        if (number == END_OF_RECORDS) {
            if (records_.empty()) malformed("a cells file without records");
            pos_ += 4;
            return false;
        }
        if (number >= parents_at(level_) || (!records_.empty() && number <= parent_))
            malformed("parent records out of order");
        if (pos_ + 4 + LEAF_BYTES > cells_size_) malformed("a parent record past the end of the cells file");
        records_.push_back(static_cast<uint32_t>(pos_));
        parent_ = number;
        pbits_[number / 64] |= uint64_t(1) << (number % 64);
        std::memcpy(leaf_, cells_.at(pos_ + 4, LEAF_BYTES), LEAF_BYTES);
        size_t n = 0;
        for (uint64_t w : leaf_) n += static_cast<size_t>(__builtin_popcountll(w));
        if (n == 0) malformed("a parent record without rows");
        const size_t m = format_.kind == Kind::BLOCKS ? (n + BLOCK_ROWS - 1) / BLOCK_ROWS : n;
        if (pos_ + 4 + LEAF_BYTES + 4 * m > cells_size_) malformed("a parent record past the end of the cells file");
        offsets_.resize(m);
        std::memcpy(offsets_.data(), cells_.at(pos_ + 4 + LEAF_BYTES, 4 * m), 4 * m);
        pos_ += 4 + LEAF_BYTES + 4 * m;
        row_ = 0;
        row_count_ = n;
        word_ = 0;
        word_bits_ = leaf_[0];
        return true;
    }

    void block_row(std::vector<uint32_t>& ids) {
        const size_t k = row_ % BLOCK_ROWS;
        if (k == 0) {
            if (offsets_[row_ / BLOCK_ROWS] != in_.pos()) malformed("a block where none starts");
            codes_ = static_cast<uint16_t>(in_.byte());
            codes_ |= static_cast<uint16_t>(in_.byte() << 8);
            const size_t rows = std::min(BLOCK_ROWS, row_count_ - row_);
            if (rows < BLOCK_ROWS && (codes_ >> (2 * rows)) != 0) malformed("codes past a block's rows");
            if ((codes_ & 3) != 0) malformed("a block whose first row isn't PLAIN");
            lf_[0] = lf_[1] = 0;
        }
        const unsigned code = (codes_ >> (2 * k)) & 3;
        if (code == 0) get_plain(ids, lf_);
        else if (code == 1) ids = prev_;
        else if (code == 2) get_edit(ids);
        else malformed("a row code of 3");
        update_firsts(ids.data(), ids.size(), format_.groups, lf_);
        prev_ = ids;
    }

    void direct_row(std::vector<uint32_t>& ids) {
        const uint32_t off = offsets_[row_];
        if (off == in_.pos()) {
            get_plain(ids, nullptr);
            prev_ = ids;
            prev_offset_ = off;
        } else if (rows_ > 0 && off == prev_offset_) {
            ids = prev_;
        } else {
            malformed("a list where none starts");
        }
    }

    void get_plain(std::vector<uint32_t>& ids, const uint32_t* lf) {
        const bool groups = format_.groups;
        const uint64_t h = in_.varint();
        const bool rep = h & 1;
        const uint64_t n0 = h >> 1, n1 = groups ? in_.varint() : 0;
        if (n0 > LIST_MAX || n1 > LIST_MAX - n0) malformed("a list of more than 65535 ids");
        const uint64_t top = groups ? BARE : 0xFFFFFFFFull;
        ids.clear();
        for (int g = 0; g < 2; g++) {
            const uint64_t n = g ? n1 : n0;
            if (n == 0) continue;
            // A first past 2^34 (zigzag of the gap from the last first) is past any id; bounded so before
            // it adds up.
            const uint64_t coded = in_.varint();
            if (coded >> 34) malformed("an id past the ids a list holds");
            const int64_t v0 = lf ? int64_t(lf[g]) + unzigzag(coded) : static_cast<int64_t>(coded);
            if (v0 < 0 || uint64_t(v0) > top) malformed("an id past the ids a list holds");
            int64_t v = v0;
            const uint32_t flag = g ? FLAG : 0;
            ids.push_back(static_cast<uint32_t>(v) | flag);
            unsigned lengths = 0;
            for (uint64_t i = 1; i < n; i++) {
                uint64_t gap;
                if (format_.gaps == Gaps::GROUP_VARINT) {
                    const unsigned q = static_cast<unsigned>((i - 1) % 4);
                    if (q == 0) {
                        lengths = in_.byte();
                        const uint64_t left = n - i;
                        if (left < 4 && (lengths >> (2 * left)) != 0) malformed("gap lengths past a list");
                    }
                    const unsigned bytes = ((lengths >> (2 * q)) & 3) + 1;
                    gap = 0;
                    for (unsigned b = 0; b < bytes; b++) gap |= uint64_t(in_.byte()) << (8 * b);
                    if (bytes > 1 && gap >> (8 * (bytes - 1)) == 0) malformed("a gap longer than it takes");
                } else {
                    gap = in_.varint();
                }
                if (gap > top || uint64_t(v) + gap + (rep ? 0 : 1) > top) malformed("an id past the ids a list holds");
                v += static_cast<int64_t>(gap + (rep ? 0 : 1));
                ids.push_back(static_cast<uint32_t>(v) | flag);
            }
        }
    }

    void get_edit(std::vector<uint32_t>& ids) {
        const bool groups = format_.groups;
        if (has_repeats(prev_.data(), prev_.size())) malformed("an edit of a list with a repeated id");
        const unsigned bits = groups ? 2 : 1;
        ids.clear();
        uint8_t byte = 0;
        for (size_t i = 0; i < prev_.size(); i++) {
            if (i * bits % 8 == 0) byte = in_.byte();
            const unsigned op = (byte >> (i * bits % 8)) & ((1u << bits) - 1);
            if (op == 0) ids.push_back(prev_[i]);
            else if (op == 2) ids.push_back(prev_[i] ^ FLAG);
            else if (op == 3) malformed("an edit op of 3");
        }
        const size_t used_bits = prev_.size() * bits % 8;
        if (used_bits != 0 && (byte >> used_bits) != 0) malformed("edit ops past the list");
        const uint64_t adds = in_.varint();
        if (adds > LIST_MAX - ids.size()) malformed("a list of more than 65535 ids");
        uint64_t t = 0;
        for (uint64_t j = 0; j < adds; j++) {
            const uint64_t gap = in_.varint();
            t = j ? t + gap + 1 : gap;
            if (gap > 0xFFFFFFFFull || t > 0xFFFFFFFFull) malformed("an added id past 32 bits");
            ids.push_back(edit_id(static_cast<uint32_t>(t), groups));
        }
        std::sort(ids.begin(), ids.end());
        if (has_repeats(ids.data(), ids.size())) malformed("an edit to a list with a repeated id");
    }

    // The tail: the bitmap, ranks and directory of the records read, the
    // footer, and both files ending there.
    void finish() {
        done_ = true;
        const size_t words = static_cast<size_t>(parent_ / 64 + 1);
        const uint64_t tail = words * 12 + records_.size() * 4 + FOOTER_BYTES;
        if (cells_size_ - pos_ != tail) malformed("a tail that isn't the records'");
        uint32_t before = 0;
        for (size_t w = 0; w < words; w++, pos_ += 8) {
            uint64_t v;
            std::memcpy(&v, cells_.at(pos_, 8), 8);
            if (v != pbits_[w]) malformed("a parent bitmap that isn't the records'");
        }
        for (size_t w = 0; w < words; w++, pos_ += 4) {
            if (cells_u32(pos_) != before) malformed("ranks that aren't the bitmap's");
            before += static_cast<uint32_t>(__builtin_popcountll(pbits_[w]));
        }
        for (uint32_t start : records_) {
            if (cells_u32(pos_) != start) malformed("a directory that isn't the records'");
            pos_ += 4;
        }
        if (cells_u32(pos_) != records_.size() || cells_u32(pos_ + 4) != words) malformed("a footer that isn't the tail's");
        if (in_.pos() != in_.size()) malformed("entries past the last list");
    }

    Cells& cells_;
    Entries& entries_;
    ByteIn<Entries> in_;
    Format format_;
    uint64_t cells_size_, pos_ = 0, rows_ = 0;
    int level_ = -1;
    bool done_ = false;
    uint64_t parent_ = 0;
    uint64_t leaf_[LEAF_WORDS] = {};
    size_t word_ = 0, row_ = 0, row_count_ = 0;
    uint64_t word_bits_ = 0;
    std::vector<uint32_t> offsets_, records_, prev_;
    std::vector<uint64_t> pbits_;
    uint16_t codes_ = 0;
    uint32_t lf_[2] = {0, 0};
    uint32_t prev_offset_ = 0;
};

}  // namespace cell_pairs
