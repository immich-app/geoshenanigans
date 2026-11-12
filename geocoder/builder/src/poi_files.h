// The POI files as they sit on disk: R2 records (poi_records.bin) and V2
// polygon blocks (poi_vertices.bin). The builder writes them and the patcher
// rebuilds them through Writer and the codec below; the server reads them in
// place (server/src/poi_files.rs). The patch tools diff and replay the
// logical form, which these files hold losslessly: a 36-byte PoiRecord per
// record (types.h) and the vertex stream of 10-byte polygon headers and
// packed vertices it points into. The encoder's choices are fixed, so a
// logical POI set has one encoding, and the decoder refuses any other.
//
// poi_records.bin, the R2 records:
//   a 27-byte row per record, in record order:
//     lat, lng (f32): the record's;
//     voff (u32): where the record's block starts in poi_vertices.bin; for
//       a point, where the next block would (the blocks' bytes so far);
//     name_id, parent_street_id (u32): the record's;
//     parent_poly_id (u24): the record's, NO_POLY (0xFFFFFF) for NO_DATA;
//     postcode (u16): parent_postcode_id's index in the postcode dictionary,
//       NO_POSTCODE (0xFFFF) for NO_DATA;
//     combo (u16): the index of the record's (category, tier, flags,
//       importance, polygon) in the combo dictionary;
//   the combo dictionary: 5 bytes an entry, category, tier, flags,
//     importance and polygon (1: the record has a block, else 0);
//   the postcode dictionary: a u32 string offset an entry;
//   footer, 24 bytes: the rows, the combo entries, the postcode entries
//     (u32 each), the length of the logical vertex stream (u64), "POR2".
//   A dictionary holds each value once, at most DICT_MAX of them, in the
//   order the rows first use them. The footer goes last, so a writer writes
//   the file front to back holding only the dictionaries.
// poi_vertices.bin, the V2 blocks: each polygon's block, in record order,
//   back to back, then VERTEX_PAD zero bytes (a bit reader's 8-byte loads
//   past the last block). The block of a polygon of vertex_count vertices,
//   each (q_lat, q_lng) at its encoding's width (u16 for encodings 0 to 2,
//   u32 for 3):
//     tag (u8): the encoding | closed << 2. Closed: vertex_count >= 2 and
//       the last vertex equals the first, which the block leaves out;
//     m (varint): the vertices stored, vertex_count - closed, at least 1;
//     the bbox minimum's latitude and longitude (varints): zigzag of its f32
//       bit pattern less the record's lat (lng), both as signed 32-bit
//       numbers;
//     wl, wg (u8): the bits of the largest q_lat and the largest q_lng;
//     the m vertices: q_lat in wl bits then q_lng in wg bits, LSB first, the
//       last byte filled with zero bits.
// Varints are LEB128 in their fewest bytes.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

namespace poi_files {

inline constexpr char MAGIC[4] = {'P', 'O', 'R', '2'};
inline constexpr size_t LOGICAL_STRIDE = 36, ROW_BYTES = 27, COMBO_BYTES = 5, POSTCODE_BYTES = 4, FOOTER_BYTES = 24;
inline constexpr size_t VERTEX_PAD = 8;
// A logical polygon's header: the encoding, a zero byte, the bbox minimum.
inline constexpr size_t LOGICAL_HEADER = 10;
// The longest block header: the tag, m and both bbox varints in 5 bytes
// each (m is a u32, a zigzag of a 33-bit difference fits 35 bits), wl, wg.
inline constexpr size_t MAX_BLOCK_HEADER = 1 + 5 + 5 + 5 + 2;
inline constexpr uint32_t NO = 0xFFFFFFFFu;
inline constexpr uint32_t DICT_MAX = 0xFFFF;
inline constexpr uint32_t NO_POLY = 0xFFFFFF, NO_POSTCODE = 0xFFFF;

// The logical record's fields (types.h PoiRecord).
inline constexpr size_t OFF_LAT = 0, OFF_LNG = 4, OFF_VERTEX_OFFSET = 8, OFF_VERTEX_COUNT = 12, OFF_NAME = 16,
                        OFF_COMBO = 20, OFF_STREET = 24, OFF_POSTCODE = 28, OFF_POLY = 32;
// The row's.
inline constexpr size_t ROW_LAT = 0, ROW_VOFF = 8, ROW_NAME = 12, ROW_STREET = 16, ROW_POLY = 20, ROW_POSTCODE = 23,
                        ROW_COMBO = 25;

[[noreturn]] inline void malformed(const std::string& what) { throw std::runtime_error("Malformed POI files: " + what); }
// Logical POIs these files can't hold.
[[noreturn]] inline void unencodable(const std::string& what) { throw std::runtime_error("POI files can't hold " + what); }

inline uint32_t rd32(const uint8_t* p) { uint32_t v; memcpy(&v, p, 4); return v; }
inline uint64_t rd64(const uint8_t* p) { uint64_t v; memcpy(&v, p, 8); return v; }
inline void wr32(uint8_t* p, uint32_t v) { memcpy(p, &v, 4); }
inline void wr64(uint8_t* p, uint64_t v) { memcpy(p, &v, 8); }
inline int bit_width(uint64_t v) { return v ? 64 - __builtin_clzll(v) : 0; }
inline uint64_t zigzag(int64_t v) { return (uint64_t(v) << 1) ^ uint64_t(v >> 63); }
inline int64_t unzigzag(uint64_t z) { return int64_t(z >> 1) ^ -int64_t(z & 1); }
inline size_t put_varint(uint8_t* o, uint64_t v) {
    size_t n = 0;
    while (v >= 0x80) { o[n++] = uint8_t(v) | 0x80; v >>= 7; }
    o[n++] = uint8_t(v);
    return n;
}
// The varint at p[at, avail) in its fewest bytes and at most max_bytes, or
// false.
inline bool get_varint(const uint8_t* p, size_t avail, size_t& at, size_t max_bytes, uint64_t& v) {
    v = 0;
    for (size_t i = 0; i < max_bytes && at < avail; i++) {
        const uint8_t c = p[at++];
        v |= uint64_t(c & 0x7f) << (7 * i);
        if (!(c & 0x80)) return i == 0 || c != 0;
    }
    return false;
}
// A logical vertex's bytes: two u16 (encodings 0 to 2) or two u32 (3).
inline size_t vertex_bytes(uint8_t encoding) { return encoding == 3 ? 8 : 4; }
inline uint32_t max_width(uint8_t encoding) { return encoding == 3 ? 32 : 16; }
inline uint32_t q_at(const uint8_t* v, uint8_t encoding, int comp) {
    if (encoding == 3) return rd32(v + 4 * comp);
    uint16_t q;
    memcpy(&q, v + 2 * comp, 2);
    return q;
}
inline void put_q(uint8_t* v, uint8_t encoding, uint32_t lat, uint32_t lng) {
    if (encoding == 3) {
        wr32(v, lat);
        wr32(v + 4, lng);
        return;
    }
    const uint16_t a = uint16_t(lat), b = uint16_t(lng);
    memcpy(v, &a, 2);
    memcpy(v + 2, &b, 2);
}
// The 8 bytes at p, of which avail can be read (zero past them).
inline uint64_t load64(const uint8_t* p, size_t avail) {
    if (avail >= 8) return rd64(p);
    uint64_t v = 0;
    if (avail) memcpy(&v, p, avail);
    return v;
}

// ------------------------------------------------------------------ dictionaries
// Values of W bytes (at most 8) numbered in the order first added, at most
// DICT_MAX of them, looked up through an open-addressing table of u16
// indexes. bytes() is the dictionary as the file holds it.
template <size_t W>
class Dict {
public:
    size_t size() const { return values_.size() / W; }
    const std::vector<uint8_t>& bytes() const { return values_; }
    uint64_t value(uint32_t i) const {
        uint64_t v = 0;
        memcpy(&v, &values_[size_t(i) * W], W);
        return v;
    }
    // v's index; added is set when v is new. Refuses a value past DICT_MAX.
    uint32_t find_or_add(uint64_t v, bool& added, const char* what) {
        added = false;
        if (slots_.empty()) slots_.assign(64, EMPTY);
        size_t s = slot_of(v);
        if (slots_[s] != EMPTY) return slots_[s];
        if (size() == DICT_MAX) unencodable(std::string("more than 65535 distinct ") + what);
        const uint32_t i = uint32_t(size());
        values_.resize(values_.size() + W);
        memcpy(&values_[size_t(i) * W], &v, W);
        slots_[s] = uint16_t(i);
        added = true;
        if ((size() + 1) * 4 > slots_.size() * 3) grow();
        return i;
    }

private:
    static constexpr uint16_t EMPTY = 0xFFFF;
    std::vector<uint8_t> values_;
    std::vector<uint16_t> slots_;
    static uint64_t mix(uint64_t x) {
        x ^= x >> 31;
        x *= 0x9E3779B97F4A7C15ull;
        x ^= x >> 29;
        return x;
    }
    size_t slot_of(uint64_t v) const {
        const size_t mask = slots_.size() - 1;
        size_t s = size_t(mix(v)) & mask;
        while (slots_[s] != EMPTY && value(slots_[s]) != v) s = (s + 1) & mask;
        return s;
    }
    void grow() {
        slots_.assign(slots_.size() * 2, EMPTY);
        for (uint32_t i = 0; i < size(); i++) slots_[slot_of(value(i))] = uint16_t(i);
    }
};

// ------------------------------------------------------------------ records
// A logical record's combo: category, tier, flags, importance and polygon.
inline uint64_t combo_of(const uint8_t* rec, bool polygon) {
    uint64_t c = 0;
    memcpy(&c, rec + OFF_COMBO, 4);
    return c | uint64_t(polygon) << 32;
}

// Rows, in record order, and the dictionaries and footer after them.
class RowEncoder {
public:
    // The row of logical record rec, whose block starts at voff (for a
    // point: where the next would).
    void encode(const uint8_t* rec, uint64_t voff, uint8_t* row) {
        const uint32_t vo = rd32(rec + OFF_VERTEX_OFFSET), vc = rd32(rec + OFF_VERTEX_COUNT);
        if ((vc == 0) != (vo == NO))
            unencodable("record " + std::to_string(n_) + ": vertex_count " + std::to_string(vc) + " with vertex_offset " +
                        std::to_string(vo));
        if (voff > NO) unencodable("a poi_vertices.bin of 4 GiB or more");
        if (n_ == NO) unencodable("4294967295 records or more");
        const uint32_t poly = rd32(rec + OFF_POLY), postcode = rd32(rec + OFF_POSTCODE);
        if (poly != NO && poly >= NO_POLY) unencodable("parent_poly_id " + std::to_string(poly));
        bool added;
        const uint32_t ci = combos_.find_or_add(combo_of(rec, vc != 0), added, "combos");
        const uint32_t pi = postcode == NO ? NO_POSTCODE : postcodes_.find_or_add(postcode, added, "postcodes");
        memcpy(row + ROW_LAT, rec + OFF_LAT, 8);
        wr32(row + ROW_VOFF, uint32_t(voff));
        memcpy(row + ROW_NAME, rec + OFF_NAME, 4);
        memcpy(row + ROW_STREET, rec + OFF_STREET, 4);
        const uint32_t p24 = poly == NO ? NO_POLY : poly;
        row[ROW_POLY] = uint8_t(p24);
        row[ROW_POLY + 1] = uint8_t(p24 >> 8);
        row[ROW_POLY + 2] = uint8_t(p24 >> 16);
        row[ROW_POSTCODE] = uint8_t(pi);
        row[ROW_POSTCODE + 1] = uint8_t(pi >> 8);
        row[ROW_COMBO] = uint8_t(ci);
        row[ROW_COMBO + 1] = uint8_t(ci >> 8);
        n_++;
    }
    uint64_t rows() const { return n_; }
    // The bytes after the last row, for a logical vertex stream of
    // logical_vertex_bytes.
    std::vector<uint8_t> tail(uint64_t logical_vertex_bytes) const {
        std::vector<uint8_t> t(combos_.bytes());
        t.insert(t.end(), postcodes_.bytes().begin(), postcodes_.bytes().end());
        uint8_t f[FOOTER_BYTES];
        wr32(f, uint32_t(n_));
        wr32(f + 4, uint32_t(combos_.size()));
        wr32(f + 8, uint32_t(postcodes_.size()));
        wr64(f + 12, logical_vertex_bytes);
        memcpy(f + 20, MAGIC, 4);
        t.insert(t.end(), f, f + FOOTER_BYTES);
        return t;
    }

private:
    uint64_t n_ = 0;
    Dict<COMBO_BYTES> combos_;
    Dict<POSTCODE_BYTES> postcodes_;
};

// An R2 file's footer.
struct Footer {
    uint64_t rows = 0, combos = 0, postcodes = 0, logical_vertex_bytes = 0;
    uint64_t rows_end() const { return rows * ROW_BYTES; }
    uint64_t dict_bytes() const { return combos * COMBO_BYTES + postcodes * POSTCODE_BYTES; }
};
// Row i's block decodes past the footer's logical vertex stream: refused at
// the block, before its vertices (which take no bytes of the file at no bits
// each) are decoded.
[[noreturn]] inline void past_vertex_stream(uint64_t i, const Footer& ft) {
    malformed("row " + std::to_string(i) + ": a block past the footer's vertex stream of " +
              std::to_string(ft.logical_vertex_bytes) + " bytes");
}

// The footer of an R2 file of file_size bytes, its last FOOTER_BYTES at f.
inline Footer read_footer(const uint8_t* f, uint64_t file_size) {
    if (file_size < FOOTER_BYTES) malformed("poi_records.bin is shorter than its footer");
    if (memcmp(f + 20, MAGIC, 4) != 0) malformed("poi_records.bin is not R2");
    Footer ft{rd32(f), rd32(f + 4), rd32(f + 8), rd64(f + 12)};
    if (ft.combos > DICT_MAX || ft.postcodes > DICT_MAX) malformed("a dictionary of more than 65535 entries");
    if (ft.rows == NO) malformed("4294967295 rows");
    if (ft.rows_end() + ft.dict_bytes() + FOOTER_BYTES != file_size)
        malformed("poi_records.bin is " + std::to_string(file_size) + " bytes, its footer gives " +
                  std::to_string(ft.rows_end() + ft.dict_bytes() + FOOTER_BYTES));
    return ft;
}

// Rows back to logical records, the dictionaries held. The rows must use
// each dictionary's entries in order, every one of them.
class RowDecoder {
public:
    RowDecoder() = default;
    // dicts: the footer's dictionaries, its combo then its postcode entries.
    RowDecoder(const Footer& f, const uint8_t* dicts) : footer_(f) {
        combos_.assign(dicts, dicts + f.combos * COMBO_BYTES);
        postcodes_.resize(size_t(f.postcodes));
        if (f.postcodes) memcpy(postcodes_.data(), dicts + f.combos * COMBO_BYTES, size_t(f.postcodes) * POSTCODE_BYTES);
        Dict<COMBO_BYTES> seen_c;
        Dict<POSTCODE_BYTES> seen_p;
        bool added;
        for (size_t i = 0; i < f.combos; i++) {
            const uint8_t* c = &combos_[i * COMBO_BYTES];
            if (c[4] > 1) malformed("combo " + std::to_string(i) + " has polygon " + std::to_string(c[4]));
            uint64_t v = 0;
            memcpy(&v, c, COMBO_BYTES);
            seen_c.find_or_add(v, added, "combos");
            if (!added) malformed("combo " + std::to_string(i) + " repeats an entry");
        }
        for (size_t i = 0; i < f.postcodes; i++) {
            seen_p.find_or_add(postcodes_[i], added, "postcodes");
            if (!added) malformed("postcode " + std::to_string(i) + " repeats an entry");
        }
    }
    const Footer& footer() const { return footer_; }
    // Row k into the logical record rec, but for vertex_offset and
    // vertex_count (the caller's, from the stream); returns the row's voff,
    // and polygon: whether the record has a block.
    uint32_t decode(const uint8_t* row, uint8_t* rec, bool& polygon) {
        const uint32_t ci = uint32_t(row[ROW_COMBO]) | uint32_t(row[ROW_COMBO + 1]) << 8;
        const uint32_t pi = uint32_t(row[ROW_POSTCODE]) | uint32_t(row[ROW_POSTCODE + 1]) << 8;
        const uint32_t p24 = uint32_t(row[ROW_POLY]) | uint32_t(row[ROW_POLY + 1]) << 8 | uint32_t(row[ROW_POLY + 2]) << 16;
        use(ci, used_c_, footer_.combos, "combo");
        if (pi != NO_POSTCODE) use(pi, used_p_, footer_.postcodes, "postcode");
        const uint8_t* c = &combos_[size_t(ci) * COMBO_BYTES];
        polygon = c[4] != 0;
        memcpy(rec + OFF_LAT, row + ROW_LAT, 8);
        wr32(rec + OFF_VERTEX_OFFSET, NO);
        wr32(rec + OFF_VERTEX_COUNT, 0);
        memcpy(rec + OFF_NAME, row + ROW_NAME, 4);
        memcpy(rec + OFF_COMBO, c, 4);
        memcpy(rec + OFF_STREET, row + ROW_STREET, 4);
        wr32(rec + OFF_POSTCODE, pi == NO_POSTCODE ? NO : postcodes_[pi]);
        wr32(rec + OFF_POLY, p24 == NO_POLY ? NO : p24);
        return rd32(row + ROW_VOFF);
    }
    // After the last row: every entry used.
    void finish() const {
        if (used_c_ != footer_.combos || used_p_ != footer_.postcodes)
            malformed("a dictionary entry no row uses");
    }

private:
    Footer footer_;
    std::vector<uint8_t> combos_;
    std::vector<uint32_t> postcodes_;
    uint64_t used_c_ = 0, used_p_ = 0;
    static void use(uint32_t i, uint64_t& used, uint64_t entries, const char* what) {
        if (i < used) return;
        if (i != used || i >= entries)
            malformed(std::string("a row uses ") + what + " " + std::to_string(i) + " with " + std::to_string(used) +
                      " used before it of " + std::to_string(entries));
        used++;
    }
};

// ------------------------------------------------------------------ blocks
struct Block {
    uint8_t encoding = 0;
    bool closed = false;
    uint64_t m = 0;  // vertices stored
    uint32_t bmin_lat = 0, bmin_lng = 0;  // f32 bit patterns
    uint32_t wl = 0, wg = 0;
    size_t header = 0;  // bytes before the vertices
    uint64_t vertex_count() const { return m + closed; }
    uint64_t payload() const { return (m * (wl + wg) + 7) / 8; }
    uint64_t bytes() const { return header + payload(); }
    uint64_t logical_bytes() const { return LOGICAL_HEADER + vertex_count() * vertex_bytes(encoding); }
};

inline int64_t signed_bits(uint32_t b) { return int64_t(int32_t(b)); }

// b's header (its header field unset) for a record at lat, lng (f32 bit
// patterns) into out (MAX_BLOCK_HEADER bytes); returns its length.
inline size_t write_block_header(const Block& b, uint32_t lat, uint32_t lng, uint8_t* out) {
    size_t n = 0;
    out[n++] = uint8_t(b.encoding | (b.closed ? 4 : 0));
    n += put_varint(out + n, b.m);
    n += put_varint(out + n, zigzag(signed_bits(b.bmin_lat) - signed_bits(lat)));
    n += put_varint(out + n, zigzag(signed_bits(b.bmin_lng) - signed_bits(lng)));
    out[n++] = uint8_t(b.wl);
    out[n++] = uint8_t(b.wg);
    return n;
}

// The block header at p, of which avail bytes can be read, for a record at
// lat, lng.
inline Block read_block_header(const uint8_t* p, size_t avail, uint32_t lat, uint32_t lng) {
    Block b;
    if (avail < 1) malformed("a block past the end of poi_vertices.bin");
    const uint8_t tag = p[0];
    if (tag > 7) malformed("a block tag of " + std::to_string(tag));
    b.encoding = tag & 3;
    b.closed = (tag & 4) != 0;
    size_t at = 1;
    uint64_t dl, dg;
    if (!get_varint(p, avail, at, 5, b.m) || !get_varint(p, avail, at, 5, dl) || !get_varint(p, avail, at, 5, dg))
        malformed("a block header's varint");
    if (b.m == 0) malformed("a block of no vertices");
    if (b.m + b.closed > NO) malformed("a block of more vertices than a record holds");
    const int64_t bl = signed_bits(lat) + unzigzag(dl), bg = signed_bits(lng) + unzigzag(dg);
    if (bl < INT32_MIN || bl > INT32_MAX || bg < INT32_MIN || bg > INT32_MAX) malformed("a block's bbox past 32 bits");
    b.bmin_lat = uint32_t(int32_t(bl));
    b.bmin_lng = uint32_t(int32_t(bg));
    if (at + 2 > avail) malformed("a block header past the end of poi_vertices.bin");
    b.wl = p[at];
    b.wg = p[at + 1];
    b.header = at + 2;
    if (b.wl > max_width(b.encoding) || b.wg > max_width(b.encoding)) malformed("a block's vertex width");
    return b;
}

// Packs bits LSB first into a byte sink, flushing whole bytes.
template <typename Put>
class BitPacker {
public:
    explicit BitPacker(Put put) : put_(put) {}
    void add(uint64_t v, uint32_t w) {
        if (!w) return;
        acc_ |= v << n_;
        n_ += w;
        while (n_ >= 8) {
            put_(uint8_t(acc_));
            acc_ >>= 8;
            n_ -= 8;
        }
    }
    void finish() {
        if (n_) put_(uint8_t(acc_));
        acc_ = 0;
        n_ = 0;
    }

private:
    Put put_;
    uint64_t acc_ = 0;
    uint32_t n_ = 0;
};

// The block of a logical polygon b (its header filled in but for the widths
// and header length) from its vertices' maxima.
inline void set_widths(Block& b, uint32_t max_lat, uint32_t max_lng) {
    b.wl = uint32_t(bit_width(max_lat));
    b.wg = uint32_t(bit_width(max_lng));
}

// Appends the V2 block of the logical polygon at t (its 10-byte header and
// vc vertices) of a record at lat, lng to out.
inline void encode_block(const uint8_t* t, uint64_t vc, uint32_t lat, uint32_t lng, std::vector<uint8_t>& out) {
    Block b;
    b.encoding = t[0];
    if (b.encoding > 3) unencodable("vertex encoding " + std::to_string(b.encoding));
    if (t[1] != 0) unencodable("a polygon header with a non-zero pad byte");
    if (vc == 0) unencodable("a polygon of no vertices");
    b.bmin_lat = rd32(t + 2);
    b.bmin_lng = rd32(t + 6);
    const size_t vs = vertex_bytes(b.encoding);
    const uint8_t* v = t + LOGICAL_HEADER;
    b.closed = vc >= 2 && memcmp(v, v + (vc - 1) * vs, vs) == 0;
    b.m = vc - b.closed;
    uint32_t ml = 0, mg = 0;
    for (uint64_t k = 0; k < b.m; k++) {
        ml = std::max(ml, q_at(v + k * vs, b.encoding, 0));
        mg = std::max(mg, q_at(v + k * vs, b.encoding, 1));
    }
    set_widths(b, ml, mg);
    uint8_t h[MAX_BLOCK_HEADER];
    const size_t hl = write_block_header(b, lat, lng, h);
    out.insert(out.end(), h, h + hl);
    out.reserve(out.size() + size_t(b.payload()));
    BitPacker pack([&](uint8_t c) { out.push_back(c); });
    for (uint64_t k = 0; k < b.m; k++) {
        pack.add(q_at(v + k * vs, b.encoding, 0), b.wl);
        pack.add(q_at(v + k * vs, b.encoding, 1), b.wg);
    }
    pack.finish();
}

// Decodes a block's vertices in pieces into the logical vertices, checking
// that the block is the encoder's: its widths those of its largest values,
// closed exactly when its first and last logical vertices are equal, its
// last byte's spare bits zero.
class VertexDecoder {
public:
    explicit VertexDecoder(const Block& b) : b_(b), w_(b.wl + b.wg) {}
    uint64_t done() const { return k_; }
    // count vertices from the next one, whose bits start on a byte at p (k
    // a multiple of 8, or the block's first), of which avail bytes can be
    // read, into out (vertex_bytes(encoding) each).
    void decode(const uint8_t* p, size_t avail, uint64_t count, uint8_t* out) {
        const size_t vs = vertex_bytes(b_.encoding);
        const uint64_t ml = b_.wl == 32 ? 0xFFFFFFFFull : (1ull << b_.wl) - 1, mg = b_.wg == 32 ? 0xFFFFFFFFull : (1ull << b_.wg) - 1;
        for (uint64_t j = 0; j < count; j++) {
            const uint64_t pos = j * w_;
            const size_t byte = size_t(pos >> 3);
            const uint32_t sh = uint32_t(pos & 7);
            uint32_t ql, qg;
            if (w_ + sh <= 64) {
                const uint64_t x = load64(p + byte, avail - std::min(avail, byte)) >> sh;
                ql = uint32_t(x & ml);
                qg = uint32_t((x >> b_.wl) & mg);
            } else {  // wl + wg > 56: two loads
                ql = uint32_t((load64(p + byte, avail - std::min(avail, byte)) >> sh) & ml);
                const uint64_t p2 = pos + b_.wl;
                const size_t byte2 = size_t(p2 >> 3);
                qg = uint32_t((load64(p + byte2, avail - std::min(avail, byte2)) >> (p2 & 7)) & mg);
            }
            max_l_ = std::max(max_l_, ql);
            max_g_ = std::max(max_g_, qg);
            if (k_ == 0) {
                first_l_ = ql;
                first_g_ = qg;
            }
            last_l_ = ql;
            last_g_ = qg;
            put_q(out + j * vs, b_.encoding, ql, qg);
            k_++;
        }
    }
    // After the last vertex: the closing vertex (b.closed) into out, and the
    // checks. last_byte: the block's last payload byte.
    void finish(uint8_t last_byte, uint8_t* out) const {
        if (k_ != b_.m) malformed("a block decoded in part");
        if (uint32_t(bit_width(max_l_)) != b_.wl || uint32_t(bit_width(max_g_)) != b_.wg)
            malformed("a block's vertex width is not its largest vertex's");
        if (!b_.closed && b_.m >= 2 && first_l_ == last_l_ && first_g_ == last_g_)
            malformed("an open block whose last vertex is its first");
        const uint32_t spare = uint32_t(b_.m * w_ % 8);
        if (spare && (last_byte >> spare)) malformed("a block's last byte has bits past its vertices");
        if (b_.closed) put_q(out, b_.encoding, first_l_, first_g_);
    }

private:
    Block b_;
    uint32_t w_;
    uint64_t k_ = 0;
    uint32_t max_l_ = 0, max_g_ = 0, first_l_ = 0, first_g_ = 0, last_l_ = 0, last_g_ = 0;
};

// The logical header of block b into out (LOGICAL_HEADER bytes).
inline void logical_header(const Block& b, uint8_t* out) {
    out[0] = b.encoding;
    out[1] = 0;
    wr32(out + 2, b.bmin_lat);
    wr32(out + 6, b.bmin_lng);
}

// ------------------------------------------------------------------ whole files
// Writes R2 and V2 from logical records in order, each with its logical
// polygon, through two byte sinks.
class Writer {
public:
    using Sink = std::function<void(const uint8_t*, size_t)>;
    Writer(Sink records, Sink vertices) : rs_(std::move(records)), vs_(std::move(vertices)) {}
    // Logical record rec and, for a polygon, its logical block (header and
    // vertex_count vertices), which must start where the last one ended.
    void add(const uint8_t* rec, const uint8_t* block) {
        uint8_t row[ROW_BYTES];
        const uint32_t vc = rd32(rec + OFF_VERTEX_COUNT), vo = rd32(rec + OFF_VERTEX_OFFSET);
        rows_.encode(rec, voff_, row);
        rs_(row, ROW_BYTES);
        if (!vc) return;
        if (vo != logical_)
            unencodable("record " + std::to_string(rows_.rows() - 1) + ": a polygon at " + std::to_string(vo) +
                        " where the last one ended at " + std::to_string(logical_));
        buf_.clear();
        encode_block(block, vc, rd32(rec + OFF_LAT), rd32(rec + OFF_LNG), buf_);
        vs_(buf_.data(), buf_.size());
        voff_ += buf_.size();
        logical_ += LOGICAL_HEADER + uint64_t(vc) * vertex_bytes(block[0]);
    }
    // The logical vertex stream's bytes so far.
    uint64_t logical() const { return logical_; }
    void finish() {
        const std::vector<uint8_t> t = rows_.tail(logical_);
        rs_(t.data(), t.size());
        const uint8_t pad[VERTEX_PAD] = {};
        vs_(pad, VERTEX_PAD);
    }

private:
    Sink rs_, vs_;
    RowEncoder rows_;
    std::vector<uint8_t> buf_;
    uint64_t voff_ = 0, logical_ = 0;
};

struct Files {
    std::vector<uint8_t> records, vertices;
};
// The R2 and V2 files of n logical records and their logical vertex stream.
inline Files encode(const uint8_t* records, uint64_t n, const uint8_t* vertices, uint64_t vertices_size) {
    Files f;
    Writer w([&](const uint8_t* p, size_t k) { f.records.insert(f.records.end(), p, p + k); },
             [&](const uint8_t* p, size_t k) { f.vertices.insert(f.vertices.end(), p, p + k); });
    for (uint64_t i = 0; i < n; i++) {
        const uint8_t* rec = records + i * LOGICAL_STRIDE;
        const uint32_t vo = rd32(rec + OFF_VERTEX_OFFSET), vc = rd32(rec + OFF_VERTEX_COUNT);
        const uint8_t* block = nullptr;
        if (vc && vo != NO) {
            if (vo >= vertices_size || vertices_size - vo < LOGICAL_HEADER ||
                (vertices_size - vo - LOGICAL_HEADER) / vertex_bytes(vertices[vo]) < vc)
                unencodable("record " + std::to_string(i) + ": a polygon past the end of its vertex stream");
            block = vertices + vo;
        }
        w.add(rec, block);
    }
    if (w.logical() != vertices_size) unencodable("vertex bytes past the last polygon");
    w.finish();
    return f;
}

// The logical records and vertex stream of an R2 and a V2 file in memory,
// handed to the sinks in order (a record, then its polygon's bytes).
template <typename RecordSink, typename VertexSink>
inline void decode(const uint8_t* r2, uint64_t r2_size, const uint8_t* v2, uint64_t v2_size, RecordSink&& on_record,
                   VertexSink&& on_vertices) {
    const Footer ft = read_footer(r2 + (r2_size >= FOOTER_BYTES ? r2_size - FOOTER_BYTES : 0), r2_size);
    RowDecoder rows(ft, r2 + ft.rows_end());
    if (v2_size < VERTEX_PAD) malformed("poi_vertices.bin is shorter than its pad");
    const uint64_t blocks_end = v2_size - VERTEX_PAD;
    for (size_t i = 0; i < VERTEX_PAD; i++)
        if (v2[blocks_end + i]) malformed("poi_vertices.bin's pad is not zero");
    uint64_t at = 0, logical = 0;
    std::vector<uint8_t> out;
    for (uint64_t i = 0; i < ft.rows; i++) {
        uint8_t rec[LOGICAL_STRIDE];
        bool polygon;
        const uint32_t voff = rows.decode(r2 + i * ROW_BYTES, rec, polygon);
        if (voff != at) malformed("row " + std::to_string(i) + ": voff " + std::to_string(voff) + " where the blocks are at " + std::to_string(at));
        if (!polygon) {
            on_record(rec);
            continue;
        }
        const Block b = read_block_header(v2 + at, size_t(std::min<uint64_t>(blocks_end - std::min(at, blocks_end), MAX_BLOCK_HEADER)),
                                          rd32(rec + OFF_LAT), rd32(rec + OFF_LNG));
        if (b.bytes() > blocks_end - at) malformed("row " + std::to_string(i) + ": a block past the end of poi_vertices.bin");
        if (b.logical_bytes() > ft.logical_vertex_bytes - logical) past_vertex_stream(i, ft);
        if (logical >= NO) malformed("a logical vertex stream of 4 GiB or more");
        wr32(rec + OFF_VERTEX_OFFSET, uint32_t(logical));
        wr32(rec + OFF_VERTEX_COUNT, uint32_t(b.vertex_count()));
        on_record(rec);
        out.resize(size_t(b.logical_bytes()));
        logical_header(b, out.data());
        VertexDecoder vd(b);
        const uint8_t* payload = v2 + at + b.header;
        vd.decode(payload, size_t(v2_size - at - b.header), b.m, out.data() + LOGICAL_HEADER);
        vd.finish(b.payload() ? payload[b.payload() - 1] : 0, out.data() + LOGICAL_HEADER + b.m * vertex_bytes(b.encoding));
        on_vertices(out.data(), out.size());
        at += b.bytes();
        logical += out.size();
    }
    rows.finish();
    if (at != blocks_end) malformed("poi_vertices.bin has bytes past its last block");
    if (logical != ft.logical_vertex_bytes)
        malformed("the vertex stream is " + std::to_string(logical) + " bytes, the footer gives " + std::to_string(ft.logical_vertex_bytes));
}

struct Logical {
    std::vector<char> records, vertices;
};
inline Logical decode(const uint8_t* r2, uint64_t r2_size, const uint8_t* v2, uint64_t v2_size) {
    Logical l;
    decode(r2, r2_size, v2, v2_size, [&](const uint8_t* rec) { l.records.insert(l.records.end(), rec, rec + LOGICAL_STRIDE); },
           [&](const uint8_t* p, size_t n) { l.vertices.insert(l.vertices.end(), p, p + n); });
    return l;
}

}  // namespace poi_files
