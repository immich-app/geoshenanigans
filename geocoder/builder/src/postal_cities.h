// The postal cities as they sit on disk (postal_cities.bin): per postcode of
// the countries it covers, the place name mail to it is addressed with. The
// builder writes it (postal_cities_build.h says how each name is chosen) and
// the patcher rebuilds it through Encoder; the server reads it in place
// (server/src/postal_cities.rs). The patch tools diff and replay its logical
// form, which the file holds losslessly: a 64-byte LogicalRow a postcode, its
// country, its code and its name, ascending by (country, code), no two of
// one. The table holds its codes and names as text, so it reads alike beside
// any dir's strings. The encoding has no choices, so a logical stream has one
// encoding, and the decoder refuses any other.
//
// postal_cities.bin:
//   the blocks, back to back, each from a byte: the rows in blocks of
//     BLOCK_ROWS, a new block at each new country, so a block shorter than
//     BLOCK_ROWS is its country's last. A block holds per row a byte, the
//     bytes of its code it shares with the row before it (the high nibble; 0
//     for row 0) and the bytes after them (the low nibble, at least 1),
//     those bytes, and its name's index among the block's names (a byte);
//     then its names, the distinct names of its rows ascending by bytes, each
//     its bytes and a 0 byte.
//   the block table, TABLE_ENTRY_BYTES a block: its country (u16), its rows
//     (u8), a 0 byte and the offset of the block (u32).
//   footer, FOOTER_BYTES: the rows (u32), the blocks (u32), "PCY1". A table
//     of no rows is the footer alone.
// A country is its two upper-case letters, the first in the high byte; a
// code 1 to CODE_BYTES bytes of printable ASCII (0x20 to 0x7E); a name 1 to
// NAME_BYTES bytes of UTF-8 without a control character.
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

namespace postal_cities {

inline constexpr size_t LOGICAL_STRIDE = 64, CODE_BYTES = 14, NAME_BYTES = 48, BLOCK_ROWS = 64, TABLE_ENTRY_BYTES = 8,
                        FOOTER_BYTES = 12;
inline constexpr char MAGIC[4] = {'P', 'C', 'Y', '1'};
inline constexpr const char* FILE_NAME = "postal_cities.bin";

// A postcode's row as the patch tools diff and replay it.
struct LogicalRow {
    uint16_t country_code = 0;
    char code[CODE_BYTES] = {};
    char name[NAME_BYTES] = {};
};
static_assert(sizeof(LogicalRow) == LOGICAL_STRIDE, "LogicalRow must be 64 bytes");

[[noreturn]] inline void malformed(const std::string& what) { throw std::runtime_error("Malformed postal cities: " + what); }

inline std::string_view code_of(const LogicalRow& r) { return std::string_view(r.code, strnlen(r.code, CODE_BYTES)); }
inline std::string_view name_of(const LogicalRow& r) { return std::string_view(r.name, strnlen(r.name, NAME_BYTES)); }
// (country, code) order, the rows'.
inline bool row_less(const LogicalRow& a, const LogicalRow& b) {
    return a.country_code != b.country_code ? a.country_code < b.country_code : code_of(a) < code_of(b);
}
inline LogicalRow row_of(uint16_t country, std::string_view code, std::string_view name) {
    if (code.size() > CODE_BYTES) malformed("a code longer than " + std::to_string(CODE_BYTES) + " bytes");
    if (name.size() > NAME_BYTES) malformed("a name longer than " + std::to_string(NAME_BYTES) + " bytes");
    LogicalRow r;
    r.country_code = country;
    std::memcpy(r.code, code.data(), code.size());
    std::memcpy(r.name, name.data(), name.size());
    return r;
}

// Whether s is UTF-8 of no control character (U+0000 to U+001F, U+007F):
// the names the format holds.
inline bool name_text(std::string_view s) {
    const auto* p = reinterpret_cast<const unsigned char*>(s.data());
    for (size_t i = 0, n = s.size(); i < n;) {
        const unsigned char c = p[i];
        if (c < 0x20 || c == 0x7F) return false;
        if (c < 0x80) {
            i++;
            continue;
        }
        size_t len;
        uint32_t cp;
        if (c >= 0xC2 && c <= 0xDF) len = 2, cp = c & 0x1F;
        else if (c >= 0xE0 && c <= 0xEF) len = 3, cp = c & 0x0F;
        else if (c >= 0xF0 && c <= 0xF4) len = 4, cp = c & 0x07;
        else return false;
        if (i + len > n) return false;
        for (size_t k = 1; k < len; k++) {
            if ((p[i + k] & 0xC0) != 0x80) return false;
            cp = cp << 6 | (p[i + k] & 0x3F);
        }
        // Overlong forms, surrogates and code points past U+10FFFF.
        if ((len == 3 && cp < 0x800) || (len == 4 && (cp < 0x10000 || cp > 0x10FFFF)) || (cp >= 0xD800 && cp <= 0xDFFF))
            return false;
        i += len;
    }
    return true;
}

// Whether a row is one the format holds: a country of two upper-case
// letters, a code of 1 to CODE_BYTES bytes of printable ASCII, a name of 1
// to NAME_BYTES bytes of name_text, each padded with 0 bytes.
inline void check_row(const LogicalRow& r) {
    const char hi = char(r.country_code >> 8), lo = char(r.country_code & 0xFF);
    if (hi < 'A' || hi > 'Z' || lo < 'A' || lo > 'Z') malformed("a country of other than two upper-case letters");
    const std::string_view code = code_of(r), name = name_of(r);
    for (size_t i = code.size(); i < CODE_BYTES; i++)
        if (r.code[i]) malformed("a code with bytes after its end");
    for (size_t i = name.size(); i < NAME_BYTES; i++)
        if (r.name[i]) malformed("a name with bytes after its end");
    if (code.empty()) malformed("a row of no code");
    for (char c : code)
        if (c < 0x20 || c > 0x7E) malformed("a code of other than printable ASCII");
    if (name.empty()) malformed("a row of no name");
    if (!name_text(name)) malformed("a name of other than UTF-8 text");
}

inline void put16(uint8_t* p, uint16_t v) { std::memcpy(p, &v, 2); }
inline void put32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
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

// The bytes two codes share from their start.
inline size_t shared_prefix(std::string_view a, std::string_view b) {
    size_t n = 0;
    while (n < a.size() && n < b.size() && a[n] == b[n]) n++;
    return n;
}

// Block rows r[0, n) (1 to BLOCK_ROWS, of one country, ascending) as their
// bytes, to `out` (cleared).
inline void encode_block(const LogicalRow* r, size_t n, std::vector<uint8_t>& out) {
    out.clear();
    std::vector<std::string_view> names;
    for (size_t i = 0; i < n; i++) names.push_back(name_of(r[i]));
    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    for (size_t i = 0; i < n; i++) {
        const std::string_view code = code_of(r[i]);
        const size_t shared = i ? shared_prefix(code_of(r[i - 1]), code) : 0;
        out.push_back(uint8_t(shared << 4 | (code.size() - shared)));
        out.insert(out.end(), code.begin() + shared, code.end());
        out.push_back(uint8_t(std::lower_bound(names.begin(), names.end(), name_of(r[i])) - names.begin()));
    }
    for (const std::string_view name : names) {
        out.insert(out.end(), name.begin(), name.end());
        out.push_back(0);
    }
}

// The logical stream in, postal_cities.bin out through the sink (called with
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
        if (finished_) throw std::logic_error("postal_cities::Encoder: add after finish");
        check_row(r);
        if (rows_ && !row_less(last_, r)) malformed("rows out of (country, code) order");
        if (rows_ == UINT32_MAX - 1) malformed("more than 2^32 - 2 rows: their count is a u32");
        if (block_rows_ && (block_rows_ == BLOCK_ROWS || r.country_code != block_[0].country_code)) flush_block();
        last_ = r;
        rows_++;
        block_[block_rows_++] = r;
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
        put32(f.data() + 4, blocks_);
        std::memcpy(f.data() + 8, MAGIC, 4);
        return f;
    }

    uint32_t rows() const { return rows_; }
    uint64_t block_bytes() const { return offset_; }
    // The table entries of the blocks written since the last clear_table.
    const std::vector<std::array<uint8_t, TABLE_ENTRY_BYTES>>& table() const { return table_; }
    void clear_table() { table_.clear(); }

private:
    void flush_block() {
        if (!block_rows_) return;
        if (offset_ > UINT32_MAX) malformed("postal_cities.bin's blocks past 4 GiB");
        encode_block(block_.data(), block_rows_, bytes_);
        std::array<uint8_t, TABLE_ENTRY_BYTES> entry{};
        put16(entry.data(), block_[0].country_code);
        entry[2] = uint8_t(block_rows_);
        put32(entry.data() + 4, uint32_t(offset_));
        table_.push_back(entry);
        sink_(bytes_.data(), bytes_.size());
        offset_ += bytes_.size();
        blocks_++;
        block_rows_ = 0;
    }

    Sink sink_;
    std::array<LogicalRow, BLOCK_ROWS> block_{};
    size_t block_rows_ = 0;
    std::vector<uint8_t> bytes_;
    std::vector<std::array<uint8_t, TABLE_ENTRY_BYTES>> table_;
    uint64_t offset_ = 0;
    uint32_t rows_ = 0, blocks_ = 0;
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

// The footer of a postal_cities.bin of size bytes: its rows and blocks, and
// where the table starts.
struct Footer {
    uint32_t rows = 0, blocks = 0;
    uint64_t table_at = 0;
};
inline Footer read_footer(const uint8_t* footer, uint64_t size) {
    if (size < FOOTER_BYTES) malformed("postal_cities.bin is shorter than its footer");
    if (std::memcmp(footer + 8, MAGIC, 4) != 0) malformed("postal_cities.bin has no PCY1 footer");
    Footer f;
    f.rows = get32(footer);
    f.blocks = get32(footer + 4);
    if (f.rows == UINT32_MAX) malformed("postal_cities.bin gives more rows than a u32 counts");
    if (f.blocks > f.rows || uint64_t(f.blocks) * BLOCK_ROWS < f.rows)
        malformed("postal_cities.bin gives blocks its rows can't fill");
    const uint64_t table = uint64_t(f.blocks) * TABLE_ENTRY_BYTES + FOOTER_BYTES;
    if (table > size) malformed("postal_cities.bin is shorter than its block table");
    f.table_at = size - table;
    if (!f.rows && f.table_at) malformed("postal_cities.bin has block bytes but no rows");
    return f;
}
// The bytes of the logical stream of a file with this footer.
inline uint64_t logical_size(const Footer& f) { return uint64_t(f.rows) * LOGICAL_STRIDE; }

// The most bytes a block of `rows` rows (1 to BLOCK_ROWS) can have: a byte,
// a code and an index a row, and a name of NAME_BYTES and its 0 a row. The
// decoders refuse a longer block before they read it.
inline constexpr size_t max_block_bytes(size_t rows) { return rows * (2 + CODE_BYTES + NAME_BYTES + 1); }

// The strict decoder of a file: given its footer, then each block's table
// entry and bytes in turn, it hands each row to put(const LogicalRow&) and
// refuses every block the encoder would not write for its rows (one longer
// than max_block_bytes of them before it reads it; each block is encoded
// again, and its bytes must be the block's), and rows that would not cut
// into blocks where the encoder cuts them. Holds a block of rows.
class Decoder {
public:
    explicit Decoder(const Footer& f) : f_(f) {}
    Decoder(const Decoder&) = delete;
    Decoder& operator=(const Decoder&) = delete;

    // The next block's rows, from its table entry (TABLE_ENTRY_BYTES at
    // entry) and its bytes [p, p + n).
    template <class Put>
    void block(const uint8_t* entry, const uint8_t* p, size_t n, Put put) {
        if (b_ >= f_.blocks) throw std::logic_error("postal_cities::Decoder: a block past the last");
        const std::string name = "block " + std::to_string(b_);
        const uint16_t country = get16(entry);
        const size_t rows = entry[2];
        if (entry[3]) malformed(name + "'s table entry has its reserved byte set");
        if (rows == 0 || rows > BLOCK_ROWS) malformed(name + " has no row count of 1 to " + std::to_string(BLOCK_ROWS));
        if (uint64_t(rows_) + rows > f_.rows) malformed(name + " holds rows past the footer's count");
        if (n > max_block_bytes(rows)) malformed(name + " has more bytes than its rows can take");
        // The encoder cuts a block only at BLOCK_ROWS rows or a new country.
        if (b_ && last_rows_ < BLOCK_ROWS && country == last_.country_code)
            malformed(name + " follows a short block of its country");
        size_t at = 0;
        uint8_t index[BLOCK_ROWS];
        for (size_t i = 0; i < rows; i++) {
            LogicalRow& r = block_[i];
            r = LogicalRow{};
            r.country_code = country;
            if (at == n) malformed(name + "'s rows run past its end");
            const size_t shared = p[at] >> 4, suffix = p[at] & 0x0F;
            at++;
            if ((i == 0 && shared) || suffix == 0 || shared + suffix > CODE_BYTES)
                malformed(name + " has a row of a code no row can have");
            if (i && shared > code_of(block_[i - 1]).size()) malformed(name + " shares more of a code than the row before has");
            if (n - at < suffix + 1) malformed(name + "'s rows run past its end");
            if (i) std::memcpy(r.code, block_[i - 1].code, shared);
            std::memcpy(r.code + shared, p + at, suffix);
            at += suffix;
            index[i] = p[at++];
        }
        std::vector<std::string_view> names;
        while (at < n) {
            size_t len = 0;
            while (at + len < n && p[at + len]) len++;
            if (at + len == n) malformed(name + "'s names run past its end");
            if (len == 0 || len > NAME_BYTES) malformed(name + " has a name of no length or of more than " + std::to_string(NAME_BYTES) + " bytes");
            names.emplace_back(reinterpret_cast<const char*>(p + at), len);
            at += len + 1;
        }
        for (size_t i = 0; i < rows; i++) {
            if (index[i] >= names.size()) malformed(name + " has a row of no name");
            std::memcpy(block_[i].name, names[index[i]].data(), names[index[i]].size());
            check_row(block_[i]);
            if ((rows_ || i) && !row_less(i ? block_[i - 1] : last_, block_[i])) malformed(name + " has rows out of (country, code) order");
        }
        encode_block(block_.data(), rows, bytes_);
        if (bytes_.size() != n || std::memcmp(bytes_.data(), p, n) != 0) malformed(name + " is not the encoder's");
        for (size_t i = 0; i < rows; i++) put(static_cast<const LogicalRow&>(block_[i]));
        last_ = block_[rows - 1];
        last_rows_ = rows;
        rows_ += uint32_t(rows);
        b_++;
    }

    // After the last block: the blocks hold the footer's rows.
    void finish() {
        if (b_ != f_.blocks) throw std::logic_error("postal_cities::Decoder: finished before the last block");
        if (rows_ != f_.rows) malformed("postal_cities.bin's blocks hold other than its footer's rows");
    }

private:
    Footer f_;
    std::array<LogicalRow, BLOCK_ROWS> block_{};
    std::vector<uint8_t> bytes_;
    LogicalRow last_{};
    size_t last_rows_ = 0;
    uint32_t rows_ = 0, b_ = 0;
};

// The logical stream of a file (whole, in memory), row by row to
// put(const LogicalRow&). Refuses a file the encoder would not write.
template <class Put>
void decode(const uint8_t* file, size_t size, Put put) {
    const Footer f = read_footer(file + size - std::min(size, FOOTER_BYTES), size);
    Decoder d(f);
    uint64_t at = 0;
    for (uint32_t b = 0; b < f.blocks; b++) {
        const uint8_t* e = file + f.table_at + uint64_t(b) * TABLE_ENTRY_BYTES;
        const uint64_t end = b + 1 < f.blocks ? get32(e + TABLE_ENTRY_BYTES + 4) : f.table_at;
        if (get32(e + 4) != at) malformed("block " + std::to_string(b) + " is not where the blocks before it end");
        if (end < at || end > f.table_at) malformed("block " + std::to_string(b) + " ends past the blocks or before it starts");
        d.block(e, file + at, size_t(end - at), put);
        at = end;
    }
    d.finish();
}

}  // namespace postal_cities
