// The address files as they sit on disk: the records in blocks
// (addr_points.bin), their footprints as blobs (addr_vertices.bin) and where
// the blocks and their blobs start (addr_index.bin). The builder writes them
// and the patcher rebuilds them through Encoder; the server reads them in
// place (server/src/addr_files.rs). The patch tools diff and replay the
// logical form, which the files hold losslessly: an AddrPoint (types.h) a
// record in the logical addr_points.bin, and the footprints' logical blobs
// back to back in the logical addr_vertices.bin. A logical blob is a
// LOGICAL_HEADER (the encoding, a zero byte, the bbox minimum's lat and lng
// as f32) and then the vertices, each (q_lat, q_lng), its steps from the bbox
// minimum on the encoding's grid: u16 each in encodings 0 to 2, u32 each in
// encoding 3. The encoding has no choices, so a logical pair has one
// encoding, and the decoder refuses any other. A dead slot's record is a
// record like any other. geocoder/testdata/addr_files.tsv holds blobs and
// files both sides must write and read.
//
// A record's id is its place in the logical addr_points.bin. The records sit
// in blocks of BLOCK_RECORDS, the last block shorter, and the blocks in
// superblocks of SUPERBLOCK_BLOCKS.
//
// addr_points.bin: the blocks, back to back, each from a byte. A block of n
//   records:
//     flags (u8): bit 0, the lat keys are order keys (else hybrid keys); bit
//       1, the same of the lng keys; bit 2, split; the other bits 0;
//     nd (u8): the dictionary's entries, 1 to n;
//     the widths, 5 bytes: six numbers of 6 bits, LSB first, the top 4 bits
//       0: the bits of a record's lat, lng, housenumber, index, parent and
//       len fields;
//     lat_base, lng_base, hn_base (u32): the block's least lat key, lng key
//       and housenumber_id;
//     the dictionary. Paired: the nd distinct (street_id, parent_way_id) of
//       the block's records (u32, u32), in the order the records first have
//       them. Split: the nd distinct street_id (u32) in that order, then
//       pw_base (u32), the least parent_way_id;
//     the records' fields, one stream, LSB first, record j at bit j x the
//       widths' sum, the last byte filled with zero bits: its lat key less
//       lat_base, its lng key less lng_base, its housenumber_id less
//       hn_base, the index of its dictionary entry, in a split block its
//       parent_way_id less pw_base, and len, the bytes of its footprint's
//       blob, 0 for a point.
//   An axis's keys are its coordinates' hybrid keys, each with its sign bit
//   flipped, where every record of the block has one, else their order keys
//   (coord_keys.h). A field's width is the bit length of its largest value:
//   of nd - 1 for the index, and 0 for the parent of a paired block. Split
//   exactly when that takes fewer bits: 32 S + 32 + n (ws + wp) < 64 P + n
//   wi, for S distinct streets and P distinct pairs, ws and wi the bit
//   lengths of S - 1 and P - 1, and wp that of the largest parent_way_id less
//   the least. A block is BLOCK_MAX_BYTES at most.
//   Footer, POINTS_FOOTER_BYTES: the records (u32), "ADB1".
// addr_vertices.bin: the blobs of the records with a footprint, in record
//   order, back to back. Footer, VERTICES_FOOTER_BYTES: the bytes of the
//   logical addr_vertices.bin (u64), the blobs (u32), "ADF1".
// addr_index.bin: a row a superblock, INDEX_ROW_BYTES, the last one shorter:
//   where the superblock's first block starts in addr_points.bin (u64) and
//   where its records' first blob starts in addr_vertices.bin, or would
//   (u64); then, for each of its blocks, where the block starts (u16) and
//   where its records' first blob starts, or would (u32), both counted from
//   the superblock's. Footer, INDEX_FOOTER_BYTES: the records (u32), "ADX1".
// No records are the three footers. A bit reader's 8-byte loads past the last
// block's or the last blob's bits land in the footer.
//
// Record i is record i % BLOCK_RECORDS of block i / BLOCK_RECORDS, which its
// superblock's row places; its blob starts where the block's first does plus
// the len of the block's records before it.
//
// The blob of a footprint of vertex_count vertices in encoding 0, 1 or 2,
// its record at the point lat, lng (f32):
//   tag (u8): the encoding | closed << 2 | small << 3 | delta << 4. Closed:
//     vertex_count >= 2 and the last vertex equals the first, which the blob
//     leaves out;
//   oc (varint): the vertices stored, vertex_count - closed, at least 1;
//   the bbox minimum: small, its lat residual and its lng residual (i8
//     each); else its lat bits and its lng bits (u32 each). An axis's
//     residual is the bbox minimum's order key less the prediction's
//     (coord_keys.h), the prediction being
//       f32(f64(point) - (f64(sum) * scale) / f64(vertex_count)),
//     sum the axis's q over all vertex_count vertices and scale the degrees
//     of a step, 1e-5, 1e-4 and 1e-6 in encodings 0, 1 and 2: the bbox
//     minimum of a footprint whose point is its vertices' mean. The product
//     comes before the quotient, so no product feeds a difference. Small
//     exactly when the point's lat and lng and both predictions are finite
//     (a NaN's bits differ by CPU) and both residuals are within
//     MAX_RESIDUAL either way;
//   widths (u8): wl | wg << 4, a nibble of 15 standing for 16 bits. wl is
//     the bit length of the lat values ORed together and wg that of the lng
//     values, each 16 where that is 15;
//   the values: the oc lat values at wl bits each, then the oc lng values at
//     wg bits, one stream, LSB first, its last byte filled with zero bits.
//     Plain, a value is its vertex's q. As deltas (delta set), the first is
//     its vertex's q and each later one zigzag16 of its q less the q before
//     it, mod 2^16. Deltas exactly when they make wl + wg smaller.
// An escape, of a footprint in encoding 3 or of more than MAX_BLOB_VERTICES
// vertices: ESCAPE_TAG, vertex_count (varint), the logical blob as it is.
// Varints are LEB128 in their fewest bytes.
//
// What the files leave out of a logical record is derived: vertex_offset is
// the bytes of the logical blobs of the records before it (NO_DATA for a
// point) and vertex_count its blob's (0 for a point). The encoder refuses a
// pair in which they are anything else, and one the files can't hold: a
// footprint in an encoding past 3, with its zero byte set or of no vertices;
// more than MAX_RECORDS records or a logical addr_vertices.bin of more than
// MAX_LOGICAL_VERTEX_BYTES (a patch indexes a file's records, and a byte
// stream's bytes, in a u32 below NO_DATA, and a vertex_offset is a u32); a
// blob of 4 GiB or more (len is a u32), or as many bytes of a superblock's
// blobs before one of its blocks.
#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "coord_keys.h"
#include "types.h"

namespace addr_files {

// The records of a block. A build that defines ADDR_FILES_B sizes the files
// at that many (a tool that measures them); the files the builder writes and
// the server reads are of 16.
#ifdef ADDR_FILES_B
inline constexpr size_t BLOCK_RECORDS = ADDR_FILES_B;
#else
inline constexpr size_t BLOCK_RECORDS = 16;
#endif
inline constexpr size_t SUPERBLOCK_BLOCKS = 64, SUPERBLOCK_RECORDS = SUPERBLOCK_BLOCKS * BLOCK_RECORDS;
inline constexpr size_t LOGICAL_STRIDE = 28, BLOCK_HEAD_BYTES = 19, POINTS_FOOTER_BYTES = 8, VERTICES_FOOTER_BYTES = 16,
                        INDEX_FOOTER_BYTES = 8, INDEX_HEAD_BYTES = 16, INDEX_ENTRY_BYTES = 6,
                        INDEX_ROW_BYTES = INDEX_HEAD_BYTES + SUPERBLOCK_BLOCKS * INDEX_ENTRY_BYTES;
// The longest block a decoder reads before it refuses one: the head, a street
// a record and the parent base, and five fields of 32 bits and an index a
// record. The encoder's are shorter: such a block takes fewer bits paired.
inline constexpr size_t BLOCK_MAX_BYTES =
    BLOCK_HEAD_BYTES + 4 * BLOCK_RECORDS + 4 +
    (BLOCK_RECORDS * (5 * 32 + coord_keys::bit_length(uint32_t(BLOCK_RECORDS - 1))) + 7) / 8;
static_assert(BLOCK_RECORDS >= 1 && BLOCK_RECORDS <= 255, "a block's dictionary entries are counted in a byte");
static_assert((SUPERBLOCK_BLOCKS - 1) * BLOCK_MAX_BYTES <= UINT16_MAX, "a block's start from its superblock's is a u16");
// The most records a pair holds, and the most bytes a logical
// addr_vertices.bin is.
inline constexpr uint64_t MAX_RECORDS = UINT32_MAX - 1, MAX_LOGICAL_VERTEX_BYTES = UINT32_MAX - 1;
// The size to give for a file the dir does not have. A file of no bytes is
// a file, and a malformed one.
inline constexpr uint64_t ABSENT = ~uint64_t(0);
inline constexpr char POINTS_MAGIC[4] = {'A', 'D', 'B', '1'}, VERTICES_MAGIC[4] = {'A', 'D', 'F', '1'},
                      INDEX_MAGIC[4] = {'A', 'D', 'X', '1'};
inline constexpr const char* POINTS_FILE = "addr_points.bin";
inline constexpr const char* VERTICES_FILE = "addr_vertices.bin";
inline constexpr const char* INDEX_FILE = "addr_index.bin";

// Where a logical record's fields sit.
inline constexpr size_t LAT_AT = 0, LNG_AT = 4, HOUSENUMBER_AT = 8, STREET_AT = 12, PARENT_AT = 16, VERTEX_OFFSET_AT = 20,
                        VERTEX_COUNT_AT = 24;
static_assert(sizeof(AddrPoint) == LOGICAL_STRIDE && offsetof(AddrPoint, lat) == LAT_AT && offsetof(AddrPoint, lng) == LNG_AT &&
                  offsetof(AddrPoint, housenumber_id) == HOUSENUMBER_AT && offsetof(AddrPoint, street_id) == STREET_AT &&
                  offsetof(AddrPoint, parent_way_id) == PARENT_AT && offsetof(AddrPoint, vertex_offset) == VERTEX_OFFSET_AT &&
                  offsetof(AddrPoint, vertex_count) == VERTEX_COUNT_AT,
              "a logical record is an AddrPoint");

// A logical blob's header, and the bytes of a vertex after it.
inline constexpr size_t LOGICAL_HEADER = 10;
constexpr size_t vertex_bytes(uint8_t enc) { return enc == 3 ? 8 : 4; }
constexpr uint64_t logical_blob_bytes(uint8_t enc, uint64_t vertex_count) {
    return LOGICAL_HEADER + vertex_count * vertex_bytes(enc);
}
// The degrees of a q step in encodings 0 to 2.
inline constexpr double SCALE[3] = {1e-5, 1e-4, 1e-6};
static_assert(LOGICAL_HEADER == POLY_HEADER_BYTES && vertex_bytes(0) == VERTEX_STRIDE[0] && vertex_bytes(1) == VERTEX_STRIDE[1] &&
                  vertex_bytes(2) == VERTEX_STRIDE[2] && vertex_bytes(3) == VERTEX_STRIDE[3] && SCALE[0] == VERTEX_SCALE[0] &&
                  SCALE[1] == VERTEX_SCALE[1] && SCALE[2] == VERTEX_SCALE[2],
              "a logical blob is a packed polygon (vertex_pack.h)");

[[noreturn]] inline void malformed(const std::string& what) { throw std::runtime_error("Malformed address files: " + what); }

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
inline void put16(uint8_t* p, uint16_t v) { std::memcpy(p, &v, 2); }
inline void put32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
inline void put64(uint8_t* p, uint64_t v) { std::memcpy(p, &v, 8); }

// ------------------------------------------------------------------ footprint blobs
// The most vertices a blob holds: a footprint of more is an escape.
inline constexpr uint64_t MAX_BLOB_VERTICES = UINT16_MAX;
inline constexpr uint8_t ESCAPE_TAG = 0x80, TAG_CLOSED = 1 << 2, TAG_SMALL = 1 << 3, TAG_DELTA = 1 << 4;
inline constexpr int64_t MAX_RESIDUAL = 127;
// The most bytes of an escape before its logical blob: the tag and a count
// of 32 bits. And the most bytes blob_header reads: those and the logical
// blob's encoding and zero byte.
inline constexpr size_t ESCAPE_HEAD_MAX = 1 + 5, BLOB_HEAD_MAX = ESCAPE_HEAD_MAX + 2;
// The longest blob that is no escape: the tag, a count in 3 bytes, the bbox
// minimum in full, the widths and MAX_BLOB_VERTICES vertices at 16 + 16 bits.
inline constexpr size_t BLOB_MAX_BYTES = 1 + 3 + 8 + 1 + size_t(MAX_BLOB_VERTICES) * 4;
// What a blob of other bytes than its record's len is refused as.
inline constexpr const char* NOT_LEN = "a footprint blob that is not its record's len";

// Whether a footprint's blob is an escape.
constexpr bool is_escape(uint8_t enc, uint64_t vertex_count) { return enc >= 3 || vertex_count > MAX_BLOB_VERTICES; }

// The bits of a value of an axis whose values, ORed together, are `any`.
constexpr uint32_t value_bits(uint32_t any) { return coord_keys::bit_length(any) == 15 ? 16 : coord_keys::bit_length(any); }

// How a blob writes its values, given each axis's plain values and its
// values as deltas, ORed together: as deltas or plain, and at which widths.
struct ValueCoding {
    bool delta;
    uint32_t wl, wg;
};
constexpr ValueCoding value_coding(uint32_t plain_lat, uint32_t plain_lng, uint32_t delta_lat, uint32_t delta_lng) {
    const uint32_t pl = value_bits(plain_lat), pg = value_bits(plain_lng), dl = value_bits(delta_lat), dg = value_bits(delta_lng);
    return dl + dg < pl + pg ? ValueCoding{true, dl, dg} : ValueCoding{false, pl, pg};
}

// Whether a float is neither an infinity nor a NaN.
constexpr bool is_finite(uint32_t bits) { return (bits & 0x7F800000u) != 0x7F800000u; }

// Where one axis of a footprint's bbox minimum is predicted to be: the point
// less the mean of the axis's q, whose sum over the vertex_count vertices is
// `sum`, in degrees.
inline uint32_t predicted_bits(uint32_t point, uint64_t sum, uint64_t vertex_count, uint8_t enc) {
    return coord_keys::bits_of(float(double(coord_keys::float_of(point)) - (double(sum) * SCALE[enc]) / double(vertex_count)));
}

// Both axes' predictions, and whether a bbox minimum may be written as its
// residuals from them: the point and the predictions are finite.
struct Prediction {
    uint32_t lat, lng;
    bool finite;
};
inline Prediction predict(uint32_t lat, uint32_t lng, uint64_t sum_lat, uint64_t sum_lng, uint64_t vertex_count, uint8_t enc) {
    Prediction p{predicted_bits(lat, sum_lat, vertex_count, enc), predicted_bits(lng, sum_lng, vertex_count, enc), false};
    p.finite = is_finite(lat) && is_finite(lng) && is_finite(p.lat) && is_finite(p.lng);
    return p;
}
// A bbox minimum's residual from its prediction, and whether a byte holds
// one.
constexpr int64_t residual(uint32_t bmin, uint32_t predicted) {
    return int64_t(coord_keys::order_key(bmin)) - int64_t(coord_keys::order_key(predicted));
}
constexpr bool fits(int64_t residual) { return residual >= -MAX_RESIDUAL && residual <= MAX_RESIDUAL; }

// The encoding of a logical blob of vertex_count vertices. Refuses one no
// blob holds.
inline uint8_t checked_encoding(const uint8_t* logical, uint64_t vertex_count) {
    if (!vertex_count) malformed("a footprint of no vertices");
    if (logical[0] > 3) malformed("a footprint in encoding " + std::to_string(logical[0]));
    if (logical[1]) malformed("a footprint with its pad byte set");
    return logical[0];
}

// The bytes of an escape before its logical blob, to out (ESCAPE_HEAD_MAX);
// how many.
inline size_t escape_head(uint32_t vertex_count, uint8_t* out) {
    out[0] = ESCAPE_TAG;
    return 1 + coord_keys::put_varint(out + 1, vertex_count);
}

// The blob of a footprint, appended to out: its logical blob (the header and
// vertex_count vertices, at any alignment) and its record's lat and lng (f32
// bits). Refuses a footprint no blob holds.
inline void encode_blob(const uint8_t* logical, uint64_t vertex_count, uint32_t lat, uint32_t lng, std::vector<uint8_t>& out) {
    using namespace coord_keys;
    const uint8_t enc = checked_encoding(logical, vertex_count);
    if (is_escape(enc, vertex_count)) {
        if (vertex_count > UINT32_MAX) malformed("a footprint of more vertices than a record counts");
        uint8_t head[ESCAPE_HEAD_MAX];
        const size_t n = escape_head(uint32_t(vertex_count), head);
        out.insert(out.end(), head, head + n);
        out.insert(out.end(), logical, logical + logical_blob_bytes(enc, vertex_count));
        return;
    }
    const uint8_t* q = logical + LOGICAL_HEADER;
    const size_t n = size_t(vertex_count);
    const uint16_t first_lat = get16(q), first_lng = get16(q + 2);
    const bool closed = n >= 2 && get16(q + 4 * (n - 1)) == first_lat && get16(q + 4 * (n - 1) + 2) == first_lng;
    const size_t oc = n - closed;
    // Each axis's sum, and its values ORed together, plain and as deltas.
    uint64_t sum_lat = first_lat, sum_lng = first_lng;
    uint32_t plain_lat = first_lat, plain_lng = first_lng, delta_lat = first_lat, delta_lng = first_lng;
    uint16_t last_lat = first_lat, last_lng = first_lng;
    for (size_t k = 1; k < oc; k++) {
        const uint16_t ql = get16(q + 4 * k), qg = get16(q + 4 * k + 2);
        sum_lat += ql;
        sum_lng += qg;
        plain_lat |= ql;
        plain_lng |= qg;
        delta_lat |= zigzag16(uint16_t(ql - last_lat));
        delta_lng |= zigzag16(uint16_t(qg - last_lng));
        last_lat = ql;
        last_lng = qg;
    }
    if (closed) {
        sum_lat += first_lat;
        sum_lng += first_lng;
    }
    const ValueCoding c = value_coding(plain_lat, plain_lng, delta_lat, delta_lng);
    const uint32_t bmin_lat = get32(logical + 2), bmin_lng = get32(logical + 6);
    const Prediction predicted = predict(lat, lng, sum_lat, sum_lng, n, enc);
    const int64_t rl = residual(bmin_lat, predicted.lat), rg = residual(bmin_lng, predicted.lng);
    const bool small = predicted.finite && fits(rl) && fits(rg);

    const size_t at = out.size(), bbox = small ? 2 : 8, head = 1 + varint_size(oc) + bbox + 1;
    const uint64_t bits = uint64_t(oc) * (c.wl + c.wg);
    // Zeroed, with the 8 bytes to spare put_bits asks for.
    out.resize(at + head + size_t((bits + 7) / 8) + 8);
    uint8_t* p = out.data() + at;
    *p++ = uint8_t(enc | (closed ? TAG_CLOSED : 0) | (small ? TAG_SMALL : 0) | (c.delta ? TAG_DELTA : 0));
    p += put_varint(p, oc);
    if (small) {
        p[0] = uint8_t(int8_t(rl));
        p[1] = uint8_t(int8_t(rg));
    } else {
        put32(p, bmin_lat);
        put32(p + 4, bmin_lng);
    }
    p += bbox;
    *p++ = uint8_t((c.wl == 16 ? 15 : c.wl) | (c.wg == 16 ? 15 : c.wg) << 4);
    const uint64_t lng_at = uint64_t(oc) * c.wl;
    last_lat = last_lng = 0;
    for (size_t k = 0; k < oc; k++) {
        const uint16_t ql = get16(q + 4 * k), qg = get16(q + 4 * k + 2);
        put_bits(p, uint64_t(k) * c.wl, c.delta && k ? zigzag16(uint16_t(ql - last_lat)) : ql, c.wl);
        put_bits(p, lng_at + uint64_t(k) * c.wg, c.delta && k ? zigzag16(uint16_t(qg - last_lng)) : qg, c.wg);
        last_lat = ql;
        last_lng = qg;
    }
    out.resize(out.size() - 8);
}

// What a blob's first bytes say of it: its footprint's encoding and vertex
// count, how its values are written, and its bytes before the bbox minimum
// (an escape's before its logical blob).
struct BlobHeader {
    bool escape = false, closed = false, small = false, delta = false;
    uint8_t enc = 0;
    // The vertices stored: all of an escape's.
    uint32_t stored = 0;
    size_t bytes = 0;
    uint64_t vertex_count() const { return uint64_t(stored) + closed; }
    // The bytes of the footprint's logical blob.
    uint64_t logical_bytes() const { return logical_blob_bytes(enc, vertex_count()); }
};

// The header of a blob of len bytes (its record's len) from its first
// min(len, BLOB_HEAD_MAX) bytes at p: no value is read. Refuses a header the
// encoder would not write, and an escape that is not len bytes.
inline BlobHeader blob_header(const uint8_t* p, uint64_t len) {
    BlobHeader h;
    if (!len) malformed(NOT_LEN);
    const size_t avail = size_t(std::min<uint64_t>(len, BLOB_HEAD_MAX));
    const uint8_t tag = p[0];
    size_t at = 1;
    uint64_t count;
    if (tag == ESCAPE_TAG) {
        if (!coord_keys::get_varint(p, avail, at, 5, count) || !count || count > UINT32_MAX)
            malformed("an escaped footprint whose count is not a varint of 1 to 4294967295 vertices");
        if (avail - at < 2) malformed(NOT_LEN);
        h.escape = true;
        h.enc = p[at];
        h.stored = uint32_t(count);
        h.bytes = at;
        if (h.enc > 3) malformed("an escaped footprint in encoding " + std::to_string(h.enc));
        if (p[at + 1]) malformed("an escaped footprint with its pad byte set");
        if (!is_escape(h.enc, count)) malformed("an escape of a footprint a blob holds");
        if (len != h.bytes + h.logical_bytes()) malformed(NOT_LEN);
        return h;
    }
    if ((tag & 0xE0) || (tag & 3) == 3) malformed("a footprint blob with a tag none has");
    h.enc = tag & 3;
    h.closed = tag & TAG_CLOSED;
    h.small = tag & TAG_SMALL;
    h.delta = tag & TAG_DELTA;
    if (!coord_keys::get_varint(p, avail, at, 3, count) || !count || count + h.closed > MAX_BLOB_VERTICES)
        malformed("a footprint blob whose count is not a varint of 1 to 65535 vertices");
    h.stored = uint32_t(count);
    h.bytes = at;
    return h;
}

// Where the values of a blob that is no escape start, and their widths, from
// its header h and its widths byte: read from p, which holds the blob's first
// bytes up to that one. Refuses a blob that is not len bytes.
struct BlobValues {
    size_t at;
    uint32_t wl, wg;
};
inline BlobValues blob_values(const BlobHeader& h, const uint8_t* p, uint64_t len) {
    const size_t at = h.bytes + (h.small ? 2 : 8) + 1;
    if (len < at) malformed(NOT_LEN);
    const uint32_t nl = p[at - 1] & 15, ng = p[at - 1] >> 4;
    const BlobValues v{at, nl == 15 ? 16 : nl, ng == 15 ? 16 : ng};
    if (len != at + (uint64_t(h.stored) * (v.wl + v.wg) + 7) / 8) malformed(NOT_LEN);
    return v;
}

// The logical blob of the blob at p, its header h (blob_header's) and its
// record's len, lat and lng, to out, which has h.logical_bytes(). The 8 bytes
// after the blob can be read. Refuses a blob the encoder would not write for
// its footprint at that point.
inline void decode_blob(const BlobHeader& h, const uint8_t* p, uint64_t len, uint32_t lat, uint32_t lng, uint8_t* out) {
    using namespace coord_keys;
    if (h.escape) {
        std::memcpy(out, p + h.bytes, size_t(h.logical_bytes()));
        return;
    }
    const BlobValues v = blob_values(h, p, len);
    const size_t oc = h.stored;
    const uint32_t wl = v.wl, wg = v.wg;
    const uint64_t bits = uint64_t(oc) * (wl + wg);

    const uint8_t* values = p + v.at;
    const uint64_t lng_at = uint64_t(oc) * wl;
    uint8_t* q = out + LOGICAL_HEADER;
    uint64_t sum_lat = 0, sum_lng = 0;
    uint32_t plain_lat = 0, plain_lng = 0, delta_lat = 0, delta_lng = 0;
    uint16_t first_lat = 0, first_lng = 0, last_lat = 0, last_lng = 0;
    for (size_t k = 0; k < oc; k++) {
        const uint32_t vl = get_bits(values, uint64_t(k) * wl, wl), vg = get_bits(values, lng_at + uint64_t(k) * wg, wg);
        // A value is a q, or a later vertex's step from the q before it; the other is made from it.
        uint16_t ql = uint16_t(vl), qg = uint16_t(vg);
        uint32_t dl = vl, dg = vg;
        if (k && h.delta) {
            ql = uint16_t(last_lat + unzigzag16(uint16_t(vl)));
            qg = uint16_t(last_lng + unzigzag16(uint16_t(vg)));
        } else if (k) {
            dl = zigzag16(uint16_t(ql - last_lat));
            dg = zigzag16(uint16_t(qg - last_lng));
        } else {
            first_lat = ql;
            first_lng = qg;
        }
        put16(q + 4 * k, ql);
        put16(q + 4 * k + 2, qg);
        sum_lat += ql;
        sum_lng += qg;
        plain_lat |= ql;
        plain_lng |= qg;
        delta_lat |= dl;
        delta_lng |= dg;
        last_lat = ql;
        last_lng = qg;
    }
    if (h.closed) {
        put16(q + 4 * oc, first_lat);
        put16(q + 4 * oc + 2, first_lng);
        sum_lat += first_lat;
        sum_lng += first_lng;
    } else if (oc >= 2 && last_lat == first_lat && last_lng == first_lng) {
        malformed("an open footprint blob whose last vertex is its first");
    }
    const ValueCoding c = value_coding(plain_lat, plain_lng, delta_lat, delta_lng);
    if (c.delta != h.delta)
        malformed(h.delta ? "a footprint blob of deltas that make its widths no smaller"
                          : "a footprint blob of plain values where deltas make its widths smaller");
    if (c.wl != wl || c.wg != wg) malformed("a footprint blob with a width its values do not need");
    if (!padding_is_zero(values, bits)) malformed("a footprint blob with bits set past its last value");

    const Prediction predicted = predict(lat, lng, sum_lat, sum_lng, h.vertex_count(), h.enc);
    uint32_t bmin_lat, bmin_lng;
    if (h.small) {
        if (!predicted.finite) malformed("a small footprint blob at a point that is not finite");
        const int64_t rl = int8_t(p[h.bytes]), rg = int8_t(p[h.bytes + 1]);
        if (!fits(rl) || !fits(rg)) malformed("a small footprint blob with a residual of -128");
        // A finite float's order key is more than a byte's residual from either end of the keys.
        bmin_lat = from_order_key(uint32_t(int64_t(order_key(predicted.lat)) + rl));
        bmin_lng = from_order_key(uint32_t(int64_t(order_key(predicted.lng)) + rg));
    } else {
        bmin_lat = get32(p + h.bytes);
        bmin_lng = get32(p + h.bytes + 4);
        if (predicted.finite && fits(residual(bmin_lat, predicted.lat)) && fits(residual(bmin_lng, predicted.lng)))
            malformed("a footprint blob with its bbox minimum in full where its residuals fit a byte each");
    }
    out[0] = h.enc;
    out[1] = 0;
    put32(out + 2, bmin_lat);
    put32(out + 6, bmin_lng);
}

// ------------------------------------------------------------------ blocks
inline constexpr uint8_t FLAG_LAT_ORDER = 1, FLAG_LNG_ORDER = 2, FLAG_SPLIT = 4;
// The widths' places among the six, and where the head's fields sit.
inline constexpr size_t W_LAT = 0, W_LNG = 1, W_HOUSENUMBER = 2, W_INDEX = 3, W_PARENT = 4, W_LEN = 5, WIDTHS = 6;
inline constexpr size_t ENTRIES_AT = 1, WIDTHS_AT = 2, LAT_BASE_AT = 7, LNG_BASE_AT = 11, HOUSENUMBER_BASE_AT = 15;

// A block's records: each one's point (f32 bits), its ids and the bytes of
// its footprint's blob, 0 for a point.
struct Block {
    size_t records = 0;
    std::array<uint32_t, BLOCK_RECORDS> lat{}, lng{}, housenumber{}, street{}, parent{}, len{};

    // The bytes of the blobs of the records before record j, and of the
    // block's blobs.
    uint64_t blob_bytes_before(size_t j) const {
        uint64_t n = 0;
        for (size_t k = 0; k < j; k++) n += len[k];
        return n;
    }
    uint64_t blob_bytes() const { return blob_bytes_before(records); }
};

// The logical record of the block's record j, LOGICAL_STRIDE bytes to out,
// given its vertex_offset and vertex_count.
inline void logical_record(const Block& b, size_t j, uint32_t vertex_offset, uint32_t vertex_count, uint8_t* out) {
    put32(out + LAT_AT, b.lat[j]);
    put32(out + LNG_AT, b.lng[j]);
    put32(out + HOUSENUMBER_AT, b.housenumber[j]);
    put32(out + STREET_AT, b.street[j]);
    put32(out + PARENT_AT, b.parent[j]);
    put32(out + VERTEX_OFFSET_AT, vertex_offset);
    put32(out + VERTEX_COUNT_AT, vertex_count);
}

// The keys of one axis of a block's n points, to keys: each coordinate's
// hybrid key, its sign bit flipped, where every one has one, else each one's
// order key. Whether they are order keys.
inline bool axis_keys(const uint32_t* bits, size_t n, uint32_t* keys) {
    size_t j = 0;
    for (uint32_t key; j < n && coord_keys::hybrid_key(bits[j], key); j++) keys[j] = coord_keys::biased(key);
    if (j == n) return false;
    for (j = 0; j < n; j++) keys[j] = coord_keys::order_key(bits[j]);
    return true;
}

// The least of n values, to least; the bit length of the largest less it.
inline uint32_t spread_bits(const uint32_t* v, size_t n, uint32_t& least) {
    uint32_t most = least = v[0];
    for (size_t j = 1; j < n; j++) {
        least = std::min(least, v[j]);
        most = std::max(most, v[j]);
    }
    return coord_keys::bit_length(most - least);
}

// How a block holds its records' streets and parents: its dictionary's
// entries in the order the records first have them (the parents a paired
// block's), each record's index among them, and the widths of the index and
// of a split block's parents.
struct Dictionary {
    bool split = false;
    size_t entries = 0;
    std::array<uint32_t, BLOCK_RECORDS> street{}, parent{};
    std::array<uint8_t, BLOCK_RECORDS> index{};
    uint32_t index_bits = 0, parent_base = 0, parent_bits = 0;
    // Its bytes in the block.
    size_t bytes() const { return split ? 4 * entries + 4 : 8 * entries; }
};

// The dictionary of a block's records: split exactly when that takes fewer
// bits than pairs.
inline Dictionary dictionary_of(const Block& b) {
    using coord_keys::bit_length;
    const size_t n = b.records;
    Dictionary d;
    std::array<uint32_t, BLOCK_RECORDS> streets{};
    std::array<uint8_t, BLOCK_RECORDS> street_index{};
    size_t distinct = 0;
    for (size_t j = 0; j < n; j++) {
        size_t i = 0;
        while (i < d.entries && (d.street[i] != b.street[j] || d.parent[i] != b.parent[j])) i++;
        if (i == d.entries) {
            d.street[i] = b.street[j];
            d.parent[i] = b.parent[j];
            d.entries++;
        }
        d.index[j] = uint8_t(i);
        i = 0;
        while (i < distinct && streets[i] != b.street[j]) i++;
        if (i == distinct) streets[distinct++] = b.street[j];
        street_index[j] = uint8_t(i);
    }
    uint32_t parent_base;
    const uint32_t parent_bits = spread_bits(b.parent.data(), n, parent_base), street_bits = bit_length(uint32_t(distinct - 1));
    d.index_bits = bit_length(uint32_t(d.entries - 1));
    if (32 * distinct + 32 + n * (street_bits + parent_bits) < 64 * d.entries + n * d.index_bits) {
        d.split = true;
        d.entries = distinct;
        d.street = streets;
        d.index = street_index;
        d.index_bits = street_bits;
        d.parent_base = parent_base;
        d.parent_bits = parent_bits;
    }
    return d;
}

// The block of b's records (1 to BLOCK_RECORDS of them) to out, which has
// BLOCK_MAX_BYTES and 8 to spare; its bytes.
inline size_t encode_block(const Block& b, uint8_t* out) {
    using namespace coord_keys;
    const size_t n = b.records;
    std::array<uint32_t, BLOCK_RECORDS> lat{}, lng{};
    const bool lat_order = axis_keys(b.lat.data(), n, lat.data()), lng_order = axis_keys(b.lng.data(), n, lng.data());
    const Dictionary d = dictionary_of(b);
    uint32_t lat_base, lng_base, housenumber_base, w[WIDTHS];
    w[W_LAT] = spread_bits(lat.data(), n, lat_base);
    w[W_LNG] = spread_bits(lng.data(), n, lng_base);
    w[W_HOUSENUMBER] = spread_bits(b.housenumber.data(), n, housenumber_base);
    w[W_INDEX] = d.index_bits;
    w[W_PARENT] = d.parent_bits;
    w[W_LEN] = bit_length(*std::max_element(b.len.begin(), b.len.begin() + n));

    out[0] = uint8_t((lat_order ? FLAG_LAT_ORDER : 0) | (lng_order ? FLAG_LNG_ORDER : 0) | (d.split ? FLAG_SPLIT : 0));
    out[ENTRIES_AT] = uint8_t(d.entries);
    uint64_t widths = 0;
    uint32_t record_bits = 0;
    for (size_t k = 0; k < WIDTHS; k++) {
        widths |= uint64_t(w[k]) << (6 * k);
        record_bits += w[k];
    }
    for (size_t k = 0; k < 5; k++) out[WIDTHS_AT + k] = uint8_t(widths >> (8 * k));
    put32(out + LAT_BASE_AT, lat_base);
    put32(out + LNG_BASE_AT, lng_base);
    put32(out + HOUSENUMBER_BASE_AT, housenumber_base);
    uint8_t* p = out + BLOCK_HEAD_BYTES;
    for (size_t i = 0; i < d.entries; i++) {
        put32(p + (d.split ? 4 : 8) * i, d.street[i]);
        if (!d.split) put32(p + 8 * i + 4, d.parent[i]);
    }
    if (d.split) put32(p + 4 * d.entries, d.parent_base);
    p += d.bytes();
    // Zeroed, with the 8 bytes to spare put_bits asks for.
    const size_t bytes = (n * record_bits + 7) / 8;
    std::memset(p, 0, bytes + 8);
    for (size_t j = 0; j < n; j++) {
        uint64_t bit = uint64_t(j) * record_bits;
        auto field = [&](uint32_t v, uint32_t width) {
            put_bits(p, bit, v, width);
            bit += width;
        };
        field(lat[j] - lat_base, w[W_LAT]);
        field(lng[j] - lng_base, w[W_LNG]);
        field(b.housenumber[j] - housenumber_base, w[W_HOUSENUMBER]);
        field(d.index[j], w[W_INDEX]);
        field(b.parent[j] - d.parent_base, w[W_PARENT]);
        field(b.len[j], w[W_LEN]);
    }
    return BLOCK_HEAD_BYTES + d.bytes() + bytes;
}

// What a block's bytes before its records' fields say: its flags, its
// dictionary's entries, the six widths and their sum, the bases (parent_base
// 0 of a paired block), where the fields start and the block's bytes. The
// dictionary starts BLOCK_HEAD_BYTES in, entry_bytes() an entry.
struct BlockHead {
    uint8_t flags = 0;
    size_t entries = 0;
    uint32_t w[WIDTHS] = {}, record_bits = 0;
    uint32_t lat_base = 0, lng_base = 0, housenumber_base = 0, parent_base = 0;
    size_t fields_at = 0, bytes = 0;
    bool split() const { return flags & FLAG_SPLIT; }
    size_t entry_bytes() const { return split() ? 4 : 8; }
};

// The head of block `block` of addr_points.bin, of `records` records, at p.
// avail is how many bytes can be read from p, the 8 after the block among
// them (blocks, or the footer). Refuses a head the encoder would not write
// and a block that ends past those bytes.
inline BlockHead block_head(uint64_t block, const uint8_t* p, uint64_t avail, size_t records) {
    auto refuse = [&](const char* what) { malformed(std::string(POINTS_FILE) + " block " + std::to_string(block) + " " + what); };
    const char* const past = "runs past the blocks' end";
    if (avail < BLOCK_HEAD_BYTES + 8) refuse(past);
    BlockHead h;
    h.flags = p[0];
    h.entries = p[ENTRIES_AT];
    if (h.flags & ~(FLAG_LAT_ORDER | FLAG_LNG_ORDER | FLAG_SPLIT)) refuse("has a flag no block has");
    if (!h.entries || h.entries > records) refuse("has a dictionary of no entries, or of more than its records");
    uint64_t widths = 0;
    std::memcpy(&widths, p + WIDTHS_AT, 5);
    if (widths >> (6 * WIDTHS)) refuse("has bits set past its widths");
    for (size_t k = 0; k < WIDTHS; k++) {
        h.w[k] = uint32_t(widths >> (6 * k)) & 63;
        if (h.w[k] > 32) refuse("has a width past 32 bits");
        h.record_bits += h.w[k];
    }
    if (h.w[W_INDEX] != coord_keys::bit_length(uint32_t(h.entries - 1)))
        refuse("has indexes of another width than its dictionary needs");
    if (!h.split() && h.w[W_PARENT]) refuse("has a parent width and no parents of its own");
    h.fields_at = BLOCK_HEAD_BYTES + (h.split() ? 4 * h.entries + 4 : 8 * h.entries);
    h.bytes = h.fields_at + (records * h.record_bits + 7) / 8;
    if (h.bytes + 8 > avail) refuse(past);
    h.lat_base = get32(p + LAT_BASE_AT);
    h.lng_base = get32(p + LNG_BASE_AT);
    h.housenumber_base = get32(p + HOUSENUMBER_BASE_AT);
    if (h.split()) h.parent_base = get32(p + BLOCK_HEAD_BYTES + 4 * h.entries);
    return h;
}

// Block `block` of addr_points.bin, of `records` records, at p, to out; its
// bytes. avail is block_head's. Refuses a block the encoder would not write.
inline size_t decode_block(uint64_t block, const uint8_t* p, uint64_t avail, size_t records, Block& out) {
    using namespace coord_keys;
    auto refuse = [&](const char* what) { malformed(std::string(POINTS_FILE) + " block " + std::to_string(block) + " " + what); };
    const BlockHead h = block_head(block, p, avail, records);
    const bool split = h.split(), lat_order = h.flags & FLAG_LAT_ORDER, lng_order = h.flags & FLAG_LNG_ORDER;
    const uint8_t* entry = p + BLOCK_HEAD_BYTES;
    const uint8_t* fields = p + h.fields_at;

    out.records = records;
    // Each field's largest value, and whether a record has its least: 0.
    uint32_t most[WIDTHS] = {};
    bool least[WIDTHS] = {};
    // Whether every coordinate of an axis under the order key has a hybrid key, which the encoder
    // would have written it under.
    bool lat_hybrid = true, lng_hybrid = true;
    // A coordinate from its key: an order key's, which may have a hybrid key too, or a hybrid key's.
    auto coordinate = [&](uint32_t key, bool order, bool& all_hybrid) {
        if (order) {
            const uint32_t bits = from_order_key(key);
            uint32_t unused;
            all_hybrid = all_hybrid && hybrid_key(bits, unused);
            return bits;
        }
        if (!is_hybrid_key(biased(key))) refuse("holds a hybrid key no coordinate has");
        return from_hybrid_key(biased(key));
    };
    for (size_t j = 0; j < records; j++) {
        uint64_t bit = uint64_t(j) * h.record_bits;
        uint32_t v[WIDTHS];
        for (size_t k = 0; k < WIDTHS; k++) {
            v[k] = get_bits(fields, bit, h.w[k]);
            bit += h.w[k];
            most[k] = std::max(most[k], v[k]);
            least[k] |= v[k] == 0;
        }
        if (v[W_LAT] > UINT32_MAX - h.lat_base || v[W_LNG] > UINT32_MAX - h.lng_base ||
            v[W_HOUSENUMBER] > UINT32_MAX - h.housenumber_base || v[W_PARENT] > UINT32_MAX - h.parent_base)
            refuse("holds a value past 32 bits");
        if (v[W_INDEX] >= h.entries) refuse("has an index past its dictionary");
        out.lat[j] = coordinate(h.lat_base + v[W_LAT], lat_order, lat_hybrid);
        out.lng[j] = coordinate(h.lng_base + v[W_LNG], lng_order, lng_hybrid);
        out.housenumber[j] = h.housenumber_base + v[W_HOUSENUMBER];
        out.street[j] = get32(entry + h.entry_bytes() * v[W_INDEX]);
        out.parent[j] = split ? h.parent_base + v[W_PARENT] : get32(entry + 8 * v[W_INDEX] + 4);
        out.len[j] = v[W_LEN];
    }
    if ((lat_order && lat_hybrid) || (lng_order && lng_hybrid))
        refuse("is under the order key where every coordinate has a hybrid key");
    if (!least[W_LAT] || !least[W_LNG] || !least[W_HOUSENUMBER] || (split && !least[W_PARENT]))
        refuse("has a base none of its records is at");
    for (size_t k : {W_LAT, W_LNG, W_HOUSENUMBER, W_PARENT, W_LEN})
        if (bit_length(most[k]) != h.w[k]) refuse("has a width its values do not need");
    // The dictionary is the one the records give, in the mode of fewer bits: the same entries in the
    // same order. No two of those are equal, so each record's index is the one the encoder writes.
    const Dictionary d = dictionary_of(out);
    if (d.split != split) refuse(split ? "is split where pairs take no more bits" : "is paired where a split takes fewer bits");
    bool same = d.entries == h.entries;
    for (size_t i = 0; same && i < h.entries; i++)
        same = d.street[i] == get32(entry + h.entry_bytes() * i) && (split || d.parent[i] == get32(entry + 8 * i + 4));
    if (!same) refuse("has a dictionary that is not its records' streets and parents in the order they first come");
    if (!padding_is_zero(fields, uint64_t(records) * h.record_bits)) refuse("has bits set past its last record");
    return h.bytes;
}

// ------------------------------------------------------------------ footers
// Refuses more records than a pair holds, and a logical addr_vertices.bin of
// more bytes than one is.
inline void check_records(uint64_t records) {
    if (records > MAX_RECORDS)
        malformed(std::to_string(records) + " records in " + POINTS_FILE + " are more than the " + std::to_string(MAX_RECORDS) +
                  " a patch can index");
}
inline void check_logical_vertex_bytes(uint64_t bytes) {
    if (bytes > MAX_LOGICAL_VERTEX_BYTES)
        malformed(std::string("a logical ") + VERTICES_FILE + " of " + std::to_string(bytes) + " bytes is more than the " +
                  std::to_string(MAX_LOGICAL_VERTEX_BYTES) + " a patch can index");
}

// The three files' footers, for this many records, and for this many blobs
// that hold this many bytes of logical blobs.
inline std::array<uint8_t, POINTS_FOOTER_BYTES> points_footer(uint64_t records) {
    check_records(records);
    std::array<uint8_t, POINTS_FOOTER_BYTES> f;
    put32(f.data(), uint32_t(records));
    std::memcpy(f.data() + 4, POINTS_MAGIC, 4);
    return f;
}
inline std::array<uint8_t, VERTICES_FOOTER_BYTES> vertices_footer(uint64_t logical_vertex_bytes, uint64_t blobs) {
    check_logical_vertex_bytes(logical_vertex_bytes);
    check_records(blobs);
    std::array<uint8_t, VERTICES_FOOTER_BYTES> f;
    put64(f.data(), logical_vertex_bytes);
    put32(f.data() + 8, uint32_t(blobs));
    std::memcpy(f.data() + 12, VERTICES_MAGIC, 4);
    return f;
}
inline std::array<uint8_t, INDEX_FOOTER_BYTES> index_footer(uint64_t records) {
    check_records(records);
    std::array<uint8_t, INDEX_FOOTER_BYTES> f;
    put32(f.data(), uint32_t(records));
    std::memcpy(f.data() + 4, INDEX_MAGIC, 4);
    return f;
}

// What the three footers say of a pair: its records and blobs, the bytes of
// the logical addr_vertices.bin, and the bytes of its blocks and of its
// blobs.
struct Footers {
    uint32_t records = 0, blobs = 0;
    uint64_t logical_vertex_bytes = 0, blocks_bytes = 0, blobs_bytes = 0;
    uint64_t blocks() const { return (uint64_t(records) + BLOCK_RECORDS - 1) / BLOCK_RECORDS; }
    uint64_t superblocks() const { return (blocks() + SUPERBLOCK_BLOCKS - 1) / SUPERBLOCK_BLOCKS; }
    // The records of block b.
    size_t block_records(uint64_t b) const { return size_t(std::min<uint64_t>(BLOCK_RECORDS, records - b * BLOCK_RECORDS)); }
    // The bytes of addr_index.bin.
    uint64_t index_bytes() const { return superblocks() * INDEX_HEAD_BYTES + blocks() * INDEX_ENTRY_BYTES + INDEX_FOOTER_BYTES; }
};

// The footers of a pair's files, each given by its size and its last bytes
// (its footer; all of a shorter file), or by the size ABSENT when the dir has
// no such file. A dir with none of the three holds no records. Refuses a dir
// with some of them, a footer without its magic, a logical addr_vertices.bin
// or records past what a pair holds, an addr_index.bin that counts other
// records than addr_points.bin or is not their rows, and more blobs than
// records: before a block or a blob is read.
inline Footers read_footers(const uint8_t* points_end, uint64_t points_size, const uint8_t* vertices_end, uint64_t vertices_size,
                            const uint8_t* index_end, uint64_t index_size) {
    Footers f;
    const size_t absent = (points_size == ABSENT) + (vertices_size == ABSENT) + (index_size == ABSENT);
    if (absent == 3) return f;
    const std::string points = POINTS_FILE, vertices = VERTICES_FILE, index = INDEX_FILE;
    if (absent) malformed("a dir with some of " + points + ", " + vertices + " and " + index + " and not all three");
    if (points_size < POINTS_FOOTER_BYTES || std::memcmp(points_end + 4, POINTS_MAGIC, 4) != 0)
        malformed(points + " has no ADB1 footer");
    if (vertices_size < VERTICES_FOOTER_BYTES || std::memcmp(vertices_end + 12, VERTICES_MAGIC, 4) != 0)
        malformed(vertices + " has no ADF1 footer");
    if (index_size < INDEX_FOOTER_BYTES || std::memcmp(index_end + 4, INDEX_MAGIC, 4) != 0)
        malformed(index + " has no ADX1 footer");
    f.records = get32(points_end);
    f.logical_vertex_bytes = get64(vertices_end);
    f.blobs = get32(vertices_end + 8);
    f.blocks_bytes = points_size - POINTS_FOOTER_BYTES;
    f.blobs_bytes = vertices_size - VERTICES_FOOTER_BYTES;
    check_logical_vertex_bytes(f.logical_vertex_bytes);
    check_records(f.records);
    if (get32(index_end) != f.records) malformed(points + " and " + index + " count different records");
    if (index_size != f.index_bytes()) malformed(index + " is not the rows of its " + std::to_string(f.records) + " records");
    if (f.blobs > f.records) malformed(vertices + " counts more blobs than " + points + " has records");
    return f;
}

// The bytes of the logical addr_points.bin and of the logical
// addr_vertices.bin.
struct LogicalSizes {
    uint64_t points = 0, vertices = 0;
};
inline LogicalSizes logical_sizes(const Footers& f) { return {uint64_t(f.records) * LOGICAL_STRIDE, f.logical_vertex_bytes}; }

// ------------------------------------------------------------------ encoder
// The logical pair in, a record at a time with its footprint, and the three
// files out through the sinks (called with bytes and a length): a blob as
// its record comes, a block when its last record has, an index row when its
// superblock's last block has. The caller writes the footers after finish().
// Holds one block's records, one index row and one blob. Refuses a pair the
// files can't hold.
//
// A footprint comes as its logical blob, or as the blob another pair's files
// hold for it, which is written as it is: a blob is the encoder's wherever
// its footprint and its record's point are the same. An escape may come in
// pieces.
//
// An encoder may start at any superblock's first record, given the records
// and the bytes of the logical blobs before it, and encode a slice of a
// pair, which is whole superblocks unless it is the pair's last: its index
// rows count their offsets from the slice's first block and first blob
// (rebase_index moves them on), and the footers are made from the pair's
// counts (points_footer, vertices_footer, index_footer).
template <class Blocks, class Blobs, class Index>
class Encoder {
public:
    Encoder(Blocks blocks, Blobs blobs, Index index, uint64_t records_before = 0, uint64_t logical_vertex_bytes_before = 0)
        : blocks_(blocks), blobs_(blobs), index_(index), records_(records_before), logical_(logical_vertex_bytes_before) {
        if (records_before % SUPERBLOCK_RECORDS)
            throw std::logic_error("an address encoder starts at a superblock's first record");
    }

    // The next record (LOGICAL_STRIDE bytes, at any alignment) and its
    // footprint's logical blob (the header and vertex_count vertices, at any
    // alignment; not read for a point).
    void add(const uint8_t* record, const uint8_t* logical) {
        const uint32_t count = checked_count(record);
        if (!count) {
            place(record, 0);
            return;
        }
        const uint8_t enc = checked_footprint(logical, count);
        if (is_escape(enc, count)) {
            escape(record, logical, enc, count);
            put_escape_bytes(logical + LOGICAL_HEADER, size_t(owed_));
            return;
        }
        blob_.clear();
        encode_blob(logical, count, get32(record + LAT_AT), get32(record + LNG_AT), blob_);
        grow(logical_blob_bytes(enc, count), blob_.size());
        blobs_(static_cast<const uint8_t*>(blob_.data()), blob_.size());
        place(record, uint32_t(blob_.size()));
    }

    // The next record, which has a footprint, and that footprint's blob as
    // another pair's files hold it for a record at the same point, len
    // bytes. Only the blob's header and widths are read: its values are
    // taken to be the footprint's.
    void add_coded(const uint8_t* record, const uint8_t* blob, uint64_t len) {
        const uint32_t count = checked_count(record);
        BlobHeader h;
        try {
            h = blob_header(blob, len);
            if (!h.escape) blob_values(h, blob, len);
        } catch (const std::runtime_error& e) {
            throw std::runtime_error(std::string(e.what()) + ": " + VERTICES_FILE + ", record " + std::to_string(records_));
        }
        if (h.vertex_count() != count) refuse("has another vertex count than its coded blob");
        grow(h.logical_bytes(), len);
        blobs_(blob, size_t(len));
        place(record, uint32_t(len));
    }

    // The next record, whose footprint is an escape, with the header of its
    // logical blob (LOGICAL_HEADER bytes). The bytes after the header follow
    // through put_escape_bytes, escape_owed() of them, in pieces of any size.
    void begin_escape(const uint8_t* record, const uint8_t* header) {
        const uint32_t count = checked_count(record);
        const uint8_t enc = checked_footprint(header, count);
        if (!is_escape(enc, count)) throw std::logic_error("a footprint a blob holds is no escape");
        escape(record, header, enc, count);
    }
    void put_escape_bytes(const uint8_t* p, size_t n) {
        if (n > owed_) throw std::logic_error("more bytes than the escaped footprint's");
        blobs_(p, n);
        owed_ -= n;
    }
    uint64_t escape_owed() const { return owed_; }

    // Writes the last block and the last index row.
    void finish() {
        if (owed_) throw std::logic_error("an escaped footprint's bytes are not all in");
        if (block_.records) write_block();
        write_row();
        finished_ = true;
    }

    // The records and the bytes of the logical blobs so far, those before
    // the encoder's first record among them; and its own blobs, and the
    // bytes of its own blocks and blobs.
    uint64_t records() const { return records_; }
    uint64_t logical_vertex_bytes() const { return logical_; }
    uint64_t blobs() const { return blobs_count_; }
    uint64_t blocks_bytes() const { return blocks_bytes_; }
    uint64_t blobs_bytes() const { return blobs_bytes_; }

    // The three footers of the pair an encoder took whole, after finish().
    std::array<uint8_t, POINTS_FOOTER_BYTES> points_footer() const { return addr_files::points_footer(records_); }
    std::array<uint8_t, VERTICES_FOOTER_BYTES> vertices_footer() const {
        return addr_files::vertices_footer(logical_, blobs_count_);
    }
    std::array<uint8_t, INDEX_FOOTER_BYTES> index_footer() const { return addr_files::index_footer(records_); }

private:
    [[noreturn]] void refuse(const std::string& what) const {
        malformed(std::string(POINTS_FILE) + " record " + std::to_string(records_) + " " + what);
    }

    // The next record's vertex count, the record checked: 0 for a point.
    uint32_t checked_count(const uint8_t* record) const {
        if (owed_) throw std::logic_error("an escaped footprint's bytes are not all in");
        if (finished_) throw std::logic_error("a record after the encoder's last block");
        check_records(records_ + 1);
        const uint32_t offset = get32(record + VERTEX_OFFSET_AT), count = get32(record + VERTEX_COUNT_AT);
        if (!count && offset != NO_DATA) refuse("has a vertex_offset and no vertices");
        if (count && offset == NO_DATA) refuse("has vertices and no vertex_offset");
        if (count && offset != logical_)
            refuse("has a vertex_offset of " + std::to_string(offset) + ", not the " + std::to_string(logical_) +
                   " bytes of the footprints before it");
        return count;
    }

    // The encoding of the next record's footprint, a record of `count`
    // vertices, from its logical blob's header.
    uint8_t checked_footprint(const uint8_t* logical, uint32_t count) const {
        if (!count) refuse("is a point with a footprint");
        if (logical[0] > 3) refuse("has a footprint in encoding " + std::to_string(logical[0]));
        if (logical[1]) refuse("has a footprint with its pad byte set");
        return logical[0];
    }

    // A footprint more: its logical blob's bytes and its blob's.
    void grow(uint64_t logical, uint64_t len) {
        check_logical_vertex_bytes(logical_ + logical);
        if (len > UINT32_MAX) refuse("has a footprint whose blob is 4 GiB or more");
        logical_ += logical;
        blobs_bytes_ += len;
        blobs_count_++;
    }

    // An escape's head and its logical blob's header out, the rest owed.
    void escape(const uint8_t* record, const uint8_t* header, uint8_t enc, uint32_t count) {
        uint8_t head[ESCAPE_HEAD_MAX];
        const size_t n = escape_head(count, head);
        const uint64_t logical = logical_blob_bytes(enc, count);
        grow(logical, n + logical);
        blobs_(static_cast<const uint8_t*>(head), n);
        blobs_(header, LOGICAL_HEADER);
        owed_ = logical - LOGICAL_HEADER;
        place(record, uint32_t(n + logical));
    }

    // The record into the block at hand, its blob len bytes, which are
    // counted already.
    void place(const uint8_t* record, uint32_t len) {
        const size_t j = block_.records++;
        if (!j) block_blobs_at_ = blobs_bytes_ - len;
        block_.lat[j] = get32(record + LAT_AT);
        block_.lng[j] = get32(record + LNG_AT);
        block_.housenumber[j] = get32(record + HOUSENUMBER_AT);
        block_.street[j] = get32(record + STREET_AT);
        block_.parent[j] = get32(record + PARENT_AT);
        block_.len[j] = len;
        records_++;
        if (block_.records == BLOCK_RECORDS) write_block();
    }

    void write_block() {
        if (!row_bytes_) {
            row_blocks_at_ = blocks_bytes_;
            row_blobs_at_ = block_blobs_at_;
            put64(row_.data(), row_blocks_at_);
            put64(row_.data() + 8, row_blobs_at_);
            row_bytes_ = INDEX_HEAD_BYTES;
        }
        if (block_blobs_at_ - row_blobs_at_ > UINT32_MAX)
            malformed(std::string(VERTICES_FILE) +
                      " would hold 4 GiB or more of a superblock's blobs before the block of record " +
                      std::to_string(records_ - block_.records));
        put16(row_.data() + row_bytes_, uint16_t(blocks_bytes_ - row_blocks_at_));
        put32(row_.data() + row_bytes_ + 2, uint32_t(block_blobs_at_ - row_blobs_at_));
        row_bytes_ += INDEX_ENTRY_BYTES;
        std::array<uint8_t, BLOCK_MAX_BYTES + 8> bytes;
        const size_t n = encode_block(block_, bytes.data());
        blocks_(static_cast<const uint8_t*>(bytes.data()), n);
        blocks_bytes_ += n;
        block_.records = 0;
        if (row_bytes_ == INDEX_ROW_BYTES) write_row();
    }

    void write_row() {
        if (row_bytes_) index_(static_cast<const uint8_t*>(row_.data()), row_bytes_);
        row_bytes_ = 0;
    }

    Blocks blocks_;
    Blobs blobs_;
    Index index_;
    Block block_;
    std::array<uint8_t, INDEX_ROW_BYTES> row_;
    std::vector<uint8_t> blob_;
    uint64_t records_, logical_, blobs_count_ = 0, blocks_bytes_ = 0, blobs_bytes_ = 0, owed_ = 0;
    // Where the block at hand's first blob starts, and where the row at hand's first block and first blob do.
    uint64_t block_blobs_at_ = 0, row_blocks_at_ = 0, row_blobs_at_ = 0;
    size_t row_bytes_ = 0;
    bool finished_ = false;
};

// Moves the offsets of a slice's index rows (as an encoder that started at
// the slice's first record wrote them: whole rows, the last may be a short
// one) on by the bytes of the blocks and of the blobs before the slice.
inline void rebase_index(uint8_t* rows, size_t bytes, uint64_t blocks_before, uint64_t blobs_before) {
    for (size_t at = 0; at < bytes; at += INDEX_ROW_BYTES) {
        put64(rows + at, get64(rows + at) + blocks_before);
        put64(rows + at + 8, get64(rows + at + 8) + blobs_before);
    }
}

// The three files of a logical pair, in memory.
struct Files {
    std::vector<uint8_t> points, vertices, index;
};
// The files of n logical records (LOGICAL_STRIDE bytes each) and their
// logical addr_vertices.bin. Refuses a pair the files can't hold, a
// footprint the logical addr_vertices.bin ends inside and bytes after the
// last one.
inline Files encode(const uint8_t* records, uint64_t n, const uint8_t* vertices, uint64_t vertices_size) {
    Files f;
    auto into = [](std::vector<uint8_t>& v) { return [&v](const uint8_t* p, size_t len) { v.insert(v.end(), p, p + len); }; };
    Encoder enc(into(f.points), into(f.vertices), into(f.index));
    for (uint64_t i = 0; i < n; i++) {
        const uint8_t* record = records + i * LOGICAL_STRIDE;
        const uint64_t offset = get32(record + VERTEX_OFFSET_AT), count = get32(record + VERTEX_COUNT_AT);
        // What the encoder reads of a footprint at its place is there: the logical blob's header,
        // and the vertices of an encoding a blob holds. It refuses any other before it reads it.
        const uint8_t* logical = nullptr;
        if (count && offset == enc.logical_vertex_bytes()) {
            if (vertices_size - std::min(offset, vertices_size) < LOGICAL_HEADER ||
                (vertices[offset] <= 3 && (vertices_size - offset - LOGICAL_HEADER) / vertex_bytes(vertices[offset]) < count))
                malformed(std::string(POINTS_FILE) + " record " + std::to_string(i) +
                          " has a footprint past the end of the logical " + VERTICES_FILE);
            logical = vertices + offset;
        }
        enc.add(record, logical);
    }
    if (enc.logical_vertex_bytes() != vertices_size)
        malformed(std::string("bytes after the last footprint of the logical ") + VERTICES_FILE);
    enc.finish();
    const auto points = enc.points_footer();
    const auto blobs = enc.vertices_footer();
    const auto index = enc.index_footer();
    f.points.insert(f.points.end(), points.begin(), points.end());
    f.vertices.insert(f.vertices.end(), blobs.begin(), blobs.end());
    f.index.insert(f.index.end(), index.begin(), index.end());
    return f;
}

// ------------------------------------------------------------------ decoder
// The logical pair of the three files (each whole, in memory; of the size
// ABSENT when the dir lacks it), record by record: a record, LOGICAL_STRIDE
// bytes, to put_record(const uint8_t*), then its footprint's logical blob,
// when it has one, to put_vertices(const uint8_t*, size_t bytes). Refuses
// files the encoder would not write.
//
// A decoder that takes the files a unit at a time checks what this one
// checks between the units: each block and blob starts where the ones before
// it end and where addr_index.bin says, the last ones end where the footers
// start, and the blobs and their logical bytes are the footer's.
template <class PutRecord, class PutVertices>
void decode(const uint8_t* points, uint64_t points_size, const uint8_t* vertices, uint64_t vertices_size, const uint8_t* index,
            uint64_t index_size, PutRecord put_record, PutVertices put_vertices) {
    // A file's footer, or all of a shorter file; nothing of one that is not there.
    auto end_of = [](const uint8_t* file, uint64_t size, size_t footer) {
        return size == ABSENT ? nullptr : file + (size - std::min<uint64_t>(size, footer));
    };
    const Footers f = read_footers(end_of(points, points_size, POINTS_FOOTER_BYTES), points_size,
                                   end_of(vertices, vertices_size, VERTICES_FOOTER_BYTES), vertices_size,
                                   end_of(index, index_size, INDEX_FOOTER_BYTES), index_size);
    const std::string index_file = INDEX_FILE, vertices_file = VERTICES_FILE;
    const char* const misplaced = " is not where the blocks and the blobs before it end";
    Block block;
    std::vector<uint8_t> out;
    uint64_t blocks_at = 0, blobs_at = 0, row_blocks_at = 0, row_blobs_at = 0, logical = 0, blobs = 0;
    for (uint64_t b = 0; b < f.blocks(); b++) {
        const uint8_t* row = index + (b / SUPERBLOCK_BLOCKS) * INDEX_ROW_BYTES;
        if (b % SUPERBLOCK_BLOCKS == 0) {
            if (get64(row) != blocks_at || get64(row + 8) != blobs_at)
                malformed(index_file + " superblock " + std::to_string(b / SUPERBLOCK_BLOCKS) + misplaced);
            row_blocks_at = blocks_at;
            row_blobs_at = blobs_at;
        }
        const uint8_t* entry = row + INDEX_HEAD_BYTES + (b % SUPERBLOCK_BLOCKS) * INDEX_ENTRY_BYTES;
        if (get16(entry) != blocks_at - row_blocks_at || get32(entry + 2) != blobs_at - row_blobs_at)
            malformed(index_file + " block " + std::to_string(b) + misplaced);
        const size_t n = f.block_records(b);
        blocks_at += decode_block(b, points + blocks_at, points_size - blocks_at, n, block);
        for (size_t j = 0; j < n; j++) {
            const uint64_t i = b * BLOCK_RECORDS + j;
            const uint32_t len = block.len[j];
            uint8_t record[LOGICAL_STRIDE];
            if (!len) {
                logical_record(block, j, NO_DATA, 0, record);
                put_record(static_cast<const uint8_t*>(record));
                continue;
            }
            if (len > f.blobs_bytes - blobs_at) malformed(vertices_file + " ends inside the blob of record " + std::to_string(i));
            try {
                const BlobHeader h = blob_header(vertices + blobs_at, len);
                if (h.logical_bytes() > f.logical_vertex_bytes - logical)
                    malformed("a footprint past the footer's logical vertex stream of " + std::to_string(f.logical_vertex_bytes) +
                              " bytes");
                out.resize(size_t(h.logical_bytes()));
                decode_blob(h, vertices + blobs_at, len, block.lat[j], block.lng[j], out.data());
                logical_record(block, j, uint32_t(logical), uint32_t(h.vertex_count()), record);
            } catch (const std::runtime_error& e) {
                // What a blob is refused for, with whose it is.
                throw std::runtime_error(std::string(e.what()) + ": " + vertices_file + ", record " + std::to_string(i));
            }
            put_record(static_cast<const uint8_t*>(record));
            put_vertices(static_cast<const uint8_t*>(out.data()), out.size());
            blobs_at += len;
            logical += out.size();
            blobs++;
        }
    }
    if (blocks_at != f.blocks_bytes) malformed(std::string(POINTS_FILE) + " has bytes between its last block and its footer");
    if (blobs_at != f.blobs_bytes) malformed(vertices_file + " has bytes between its last blob and its footer");
    if (logical != f.logical_vertex_bytes)
        malformed(vertices_file + " counts " + std::to_string(f.logical_vertex_bytes) +
                  " bytes of logical blobs and its blobs hold " + std::to_string(logical));
    if (blobs != f.blobs)
        malformed(vertices_file + " counts " + std::to_string(f.blobs) + " blobs and holds " + std::to_string(blobs));
}

// The logical pair of the three files, in memory.
struct Logical {
    std::vector<uint8_t> points, vertices;
};
inline Logical decode(const uint8_t* points, uint64_t points_size, const uint8_t* vertices, uint64_t vertices_size,
                      const uint8_t* index, uint64_t index_size) {
    Logical l;
    decode(
        points, points_size, vertices, vertices_size, index, index_size,
        [&](const uint8_t* record) { l.points.insert(l.points.end(), record, record + LOGICAL_STRIDE); },
        [&](const uint8_t* p, size_t n) { l.vertices.insert(l.vertices.end(), p, p + n); });
    return l;
}

}  // namespace addr_files
