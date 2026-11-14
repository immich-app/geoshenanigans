// An address's footprint as it sits on disk, a blob in addr_vertices.bin. The
// builder writes it and the patcher rebuilds it through encode_blob; the
// server reads its vertices in place (server/src/addr_files.rs). The patch
// tools diff and replay the logical form, which the blob holds losslessly:
// the footprint's logical blob in the logical addr_vertices.bin (types.h), a
// LOGICAL_HEADER (the encoding, a zero byte, the bbox minimum's lat and lng
// as f32) and then the vertices, each (q_lat, q_lng), its steps from the bbox
// minimum on the encoding's grid: u16 each in encodings 0 to 2, u32 each in
// encoding 3. The encoding has no choices, so a footprint has one blob, and
// the decoder refuses any other. geocoder/testdata/addr_files.tsv holds blobs
// both sides must write and read.
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
// A logical blob in an encoding past 3, with its zero byte set or of no
// vertices is refused.
// Varints are LEB128 in their fewest bytes.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "coord_keys.h"
#include "types.h"

namespace addr_files {

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
inline void put16(uint8_t* p, uint16_t v) { std::memcpy(p, &v, 2); }
inline void put32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }

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
    const char* const not_len = "a footprint blob that is not its record's len";
    BlobHeader h;
    if (!len) malformed(not_len);
    const size_t avail = size_t(std::min<uint64_t>(len, BLOB_HEAD_MAX));
    const uint8_t tag = p[0];
    size_t at = 1;
    uint64_t count;
    if (tag == ESCAPE_TAG) {
        if (!coord_keys::get_varint(p, avail, at, 5, count) || !count || count > UINT32_MAX)
            malformed("an escaped footprint whose count is not a varint of 1 to 4294967295 vertices");
        if (avail - at < 2) malformed(not_len);
        h.escape = true;
        h.enc = p[at];
        h.stored = uint32_t(count);
        h.bytes = at;
        if (h.enc > 3) malformed("an escaped footprint in encoding " + std::to_string(h.enc));
        if (p[at + 1]) malformed("an escaped footprint with its pad byte set");
        if (!is_escape(h.enc, count)) malformed("an escape of a footprint a blob holds");
        if (len != h.bytes + h.logical_bytes()) malformed(not_len);
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
    const char* const not_len = "a footprint blob that is not its record's len";
    const size_t oc = h.stored, bbox = h.small ? 2 : 8, head = h.bytes + bbox + 1;
    if (len < head) malformed(not_len);
    const uint32_t nl = p[head - 1] & 15, ng = p[head - 1] >> 4, wl = nl == 15 ? 16 : nl, wg = ng == 15 ? 16 : ng;
    const uint64_t bits = uint64_t(oc) * (wl + wg);
    if (len != head + (bits + 7) / 8) malformed(not_len);

    const uint8_t* values = p + head;
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

}  // namespace addr_files
