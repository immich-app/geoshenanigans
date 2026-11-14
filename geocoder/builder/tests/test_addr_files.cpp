// The address files (addr_files.h): a footprint's blob decodes to the logical
// blob it was made from, the choices in it are the format's, a footprint has
// one blob, and the blobs the encoder would not write are refused.
#include <algorithm>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "addr_files.h"
#include "coord_keys.h"
#include "test_framework.h"

namespace af = addr_files;
using gctest::refusal_of;

namespace {

// A record's point, as f32 bits.
struct Point {
    uint32_t lat = 0, lng = 0;
};
Point point(float lat, float lng) { return {coord_keys::bits_of(lat), coord_keys::bits_of(lng)}; }

using Vertices = std::vector<std::pair<uint32_t, uint32_t>>;  // q_lat, q_lng
using Bytes = std::vector<uint8_t>;

void put16(Bytes& out, uint32_t v) {
    for (int k = 0; k < 2; k++) out.push_back(uint8_t(v >> (8 * k)));
}
void put32(Bytes& out, uint32_t v) {
    for (int k = 0; k < 4; k++) out.push_back(uint8_t(v >> (8 * k)));
}
uint32_t get16(const Bytes& b, size_t at) { return uint32_t(b[at]) | uint32_t(b[at + 1]) << 8; }
uint32_t get32(const Bytes& b, size_t at) { return get16(b, at) | get16(b, at + 2) << 16; }
Bytes varint(uint64_t v) {
    Bytes out;
    for (; v >= 0x80; v >>= 7) out.push_back(uint8_t(v) | 0x80);
    out.push_back(uint8_t(v));
    return out;
}
Bytes joined(std::initializer_list<Bytes> parts) {
    Bytes out;
    for (const Bytes& p : parts) out.insert(out.end(), p.begin(), p.end());
    return out;
}

// A logical blob: the encoding, a zero byte, the bbox minimum and the
// vertices, a u16 pair each (a u32 pair in encoding 3).
Bytes logical_blob(uint8_t enc, uint32_t bmin_lat, uint32_t bmin_lng, const Vertices& q) {
    Bytes out = {enc, 0};
    put32(out, bmin_lat);
    put32(out, bmin_lng);
    for (const auto& [lat, lng] : q) {
        if (enc == 3) {
            put32(out, lat);
            put32(out, lng);
        } else {
            put16(out, lat);
            put16(out, lng);
        }
    }
    return out;
}

// The vertices a logical blob holds.
size_t vertices_of(const Bytes& logical) { return (logical.size() - 10) / (logical[0] == 3 ? 8 : 4); }

Bytes encode(const Bytes& logical, Point p) {
    Bytes out;
    af::encode_blob(logical.data(), vertices_of(logical), p.lat, p.lng, out);
    return out;
}

// The logical blob of a blob, which the decoder reads with 8 bytes after it:
// set bits here, which no blob's values may take.
Bytes decode(const Bytes& blob, Point p) {
    Bytes padded = blob;
    padded.resize(blob.size() + 8, 0xFF);
    const af::BlobHeader h = af::blob_header(padded.data(), blob.size());
    Bytes out(size_t(h.logical_bytes()));
    af::decode_blob(h, padded.data(), blob.size(), p.lat, p.lng, out.data());
    return out;
}

af::BlobHeader header_of(const Bytes& blob) { return af::blob_header(blob.data(), blob.size()); }

// What the decoder says of a blob it refuses; "" for one it takes.
std::string refusal(const Bytes& blob, Point p) {
    return refusal_of([&] { decode(blob, p); });
}

using Fields = std::vector<std::pair<uint32_t, uint32_t>>;  // value, width

// The fields a bit at a time, LSB first, the last byte's spare bits zero.
Bytes packed(const Fields& fields) {
    Bytes out;
    uint64_t bit = 0;
    for (const auto& [v, w] : fields)
        for (uint32_t k = 0; k < w; k++, bit++) {
            if (bit % 8 == 0) out.push_back(0);
            out[bit / 8] |= uint8_t(((v >> k) & 1) << (bit % 8));
        }
    return out;
}

// The format's rules, written apart from the encoder.
uint32_t bit_length(uint32_t v) {
    uint32_t n = 0;
    for (; v; v >>= 1) n++;
    return n;
}
// The bits a blob gives values that are `any` ORed together: 16 for 15.
uint32_t width_of(uint32_t any) { return bit_length(any) == 15 ? 16 : bit_length(any); }
// A q's step from the one before, mod 2^16, zigzagged.
uint32_t step(uint32_t q, uint32_t before) {
    const int32_t d = int16_t(uint16_t(q - before));
    return uint32_t(d < 0 ? -2 * d - 1 : 2 * d);
}
// A float's place in the floats' order, and the float at a place.
int64_t place(uint32_t bits) { return bits >> 31 ? int64_t(0xFFFFFFFFu) - bits : int64_t(bits) + 0x80000000ll; }
uint32_t at_place(int64_t k) { return k >= 0x80000000ll ? uint32_t(k - 0x80000000ll) : uint32_t(0xFFFFFFFFll - k); }
const double SCALE[3] = {1e-5, 1e-4, 1e-6};
// Where a footprint's bbox minimum is when its point is its vertices' mean:
// the product before the quotient.
uint32_t predicted(uint32_t point, uint64_t sum, uint64_t n, uint8_t enc) {
    return coord_keys::bits_of(float(double(coord_keys::float_of(point)) - (double(sum) * SCALE[enc]) / double(n)));
}
// The same with the mean taken first, which rounds another way at some
// points. The mean goes through memory, so no compiler fuses the product into
// the difference.
uint32_t predicted_mean_first(uint32_t point, uint64_t sum, uint64_t n, uint8_t enc) {
    volatile double mean = (double(sum) / double(n)) * SCALE[enc];
    return coord_keys::bits_of(float(double(coord_keys::float_of(point)) - mean));
}
bool finite(uint32_t bits) { return std::isfinite(coord_keys::float_of(bits)); }

// A logical blob's q values of one axis (0 lat, 1 lng), and their sum.
std::vector<uint32_t> axis(const Bytes& logical, int a) {
    std::vector<uint32_t> out;
    for (size_t k = 0; k < vertices_of(logical); k++) out.push_back(get16(logical, 10 + 4 * k + 2 * a));
    return out;
}
uint64_t sum_of(const std::vector<uint32_t>& q) {
    uint64_t s = 0;
    for (uint32_t v : q) s += v;
    return s;
}

// What a blob's tag says of it.
struct Shape {
    bool escape = false, closed = false, small = false, delta = false;
    uint32_t wl = 0, wg = 0;
};

// The blob of a logical blob by the format's rules, the values a bit at a
// time; shape, when given, is what it chose.
Bytes reference_blob(const Bytes& logical, Point p, Shape* shape = nullptr) {
    Shape s;
    const uint8_t enc = logical[0];
    const size_t n = vertices_of(logical);
    if (enc == 3 || n > 65535) {
        s.escape = true;
        if (shape) *shape = s;
        return joined({{0x80}, varint(n), logical});
    }
    const std::vector<uint32_t> ql = axis(logical, 0), qg = axis(logical, 1);
    s.closed = n >= 2 && ql[0] == ql[n - 1] && qg[0] == qg[n - 1];
    const size_t oc = n - s.closed;
    auto plain = [&](const std::vector<uint32_t>& q) {
        uint32_t any = 0;
        for (size_t k = 0; k < oc; k++) any |= q[k];
        return width_of(any);
    };
    auto deltas = [&](const std::vector<uint32_t>& q) {
        uint32_t any = q[0];
        for (size_t k = 1; k < oc; k++) any |= step(q[k], q[k - 1]);
        return width_of(any);
    };
    s.delta = deltas(ql) + deltas(qg) < plain(ql) + plain(qg);
    s.wl = s.delta ? deltas(ql) : plain(ql);
    s.wg = s.delta ? deltas(qg) : plain(qg);
    const uint32_t bmin_lat = get32(logical, 2), bmin_lng = get32(logical, 6);
    const uint32_t pred_lat = predicted(p.lat, sum_of(ql), n, enc), pred_lng = predicted(p.lng, sum_of(qg), n, enc);
    const int64_t rl = place(bmin_lat) - place(pred_lat), rg = place(bmin_lng) - place(pred_lng);
    s.small = finite(p.lat) && finite(p.lng) && finite(pred_lat) && finite(pred_lng) && rl >= -127 && rl <= 127 && rg >= -127 &&
              rg <= 127;
    Bytes bbox;
    if (s.small) {
        bbox = {uint8_t(int8_t(rl)), uint8_t(int8_t(rg))};
    } else {
        put32(bbox, bmin_lat);
        put32(bbox, bmin_lng);
    }
    Fields fields;
    for (const auto* q : {&ql, &qg})
        for (size_t k = 0; k < oc; k++)
            fields.push_back({s.delta && k ? step((*q)[k], (*q)[k - 1]) : (*q)[k], q == &ql ? s.wl : s.wg});
    if (shape) *shape = s;
    const uint8_t tag = uint8_t(enc | (s.closed ? 4 : 0) | (s.small ? 8 : 0) | (s.delta ? 16 : 0));
    const uint8_t widths = uint8_t((s.wl == 16 ? 15 : s.wl) | (s.wg == 16 ? 15 : s.wg) << 4);
    return joined({{tag}, varint(oc), bbox, {widths}, packed(fields)});
}

Shape shape_of(const Bytes& logical, Point p) {
    Shape s;
    reference_blob(logical, p, &s);
    return s;
}

// A blob written by hand: the tag, the count, the bbox minimum's bytes, the
// two width nibbles, and the values of each axis at the nibbles' widths.
Bytes hand_blob(uint8_t tag, uint32_t oc, const Bytes& bbox, uint32_t nl, uint32_t ng, const std::vector<uint32_t>& lat,
                const std::vector<uint32_t>& lng) {
    Fields fields;
    for (uint32_t v : lat) fields.push_back({v, nl == 15 ? 16 : nl});
    for (uint32_t v : lng) fields.push_back({v, ng == 15 ? 16 : ng});
    return joined({{tag}, varint(oc), bbox, {uint8_t(nl | ng << 4)}, packed(fields)});
}

// The bbox minimum a residual away from where a footprint of these vertices
// at this point is predicted to have it.
uint32_t bmin_at(uint32_t point, const std::vector<uint32_t>& q, uint8_t enc, int64_t residual) {
    return at_place(place(predicted(point, sum_of(q), q.size(), enc)) + residual);
}

// A footprint at p in encoding enc, its bbox minimum the residuals away from
// the prediction.
Bytes footprint(uint8_t enc, Point p, const Vertices& q, int64_t rl = 0, int64_t rg = 0) {
    std::vector<uint32_t> ql, qg;
    for (const auto& [lat, lng] : q) {
        ql.push_back(lat);
        qg.push_back(lng);
    }
    return logical_blob(enc, bmin_at(p.lat, ql, enc, rl), bmin_at(p.lng, qg, enc, rg), q);
}

struct Case {
    Bytes logical;
    Point p;
};

// A footprint of one of five kinds: a building (a small closed ring, its
// bbox minimum near the prediction); an open path in any encoding, its
// residuals anywhere a byte holds; a long outline in small steps from beside
// the bbox minimum, up from it or around it (a q below it is taken mod 2^16),
// its residuals at the edge of a byte; any 16 bits a value at any point, a
// NaN or an infinity among them; and one vertex, or two that are equal.
Case random_footprint(std::mt19937_64& rng, int kind) {
    // One draw a statement: the same footprints whatever order a compiler evaluates arguments in.
    Case c;
    const float lat = float(int64_t(rng() % 1700001) - 850000) / 10000.0f;
    const float lng = float(int64_t(rng() % 3500001) - 1750000) / 10000.0f;
    c.p = point(lat, lng);
    Vertices q;
    uint8_t enc = 2;
    int64_t rl = 0, rg = 0;
    if (kind == 0) {
        const size_t n = 3 + rng() % 10;
        for (size_t k = 0; k < n; k++) {
            const uint32_t a = uint32_t(rng() % 600);
            const uint32_t b = uint32_t(rng() % 600);
            q.push_back({a, b});
        }
        q.push_back(q[0]);
        rl = int64_t(rng() % 7) - 3;
        rg = int64_t(rng() % 7) - 3;
    } else if (kind == 1) {
        enc = uint8_t(rng() % 3);
        const size_t n = 2 + rng() % 30;
        const uint32_t reach = 1u << (rng() % 17);
        for (size_t k = 0; k < n; k++) {
            const uint32_t a = uint32_t(rng() % reach);
            const uint32_t b = uint32_t(rng() % reach);
            q.push_back({a, b});
        }
        rl = int64_t(rng() % 255) - 127;
        rg = int64_t(rng() % 255) - 127;
    } else if (kind == 2) {
        enc = uint8_t(rng() % 3);
        const size_t n = 20 + rng() % 280;
        const uint32_t reach = 1u << (1 + rng() % 9);
        const uint32_t back = rng() % 2 ? reach / 2 : 0;
        uint32_t a = uint32_t(rng() % reach);
        uint32_t b = uint32_t(rng() % reach);
        for (size_t k = 0; k < n; k++) {
            q.push_back({a, b});
            a = (a + uint32_t(rng() % reach) - back) & 0xFFFF;
            b = (b + uint32_t(rng() % reach) - back) & 0xFFFF;
        }
        if (rng() % 2) q.push_back(q[0]);
        rl = int64_t(120 + rng() % 16);
        rg = int64_t(120 + rng() % 16);
        if (rng() % 2) rl = -rl;
        if (rng() % 2) rg = -rg;
    } else if (kind == 3) {
        enc = uint8_t(rng() % 3);
        const size_t n = 1 + rng() % 40;
        for (size_t k = 0; k < n; k++) {
            const uint32_t a = uint32_t(rng() % 65536);
            const uint32_t b = uint32_t(rng() % 65536);
            q.push_back({a, b});
        }
        const uint64_t odd = rng() % 8;
        if (odd == 0) c.p.lat = 0x7FC00000u;
        if (odd == 1) c.p.lng = 0xFF800000u;
        if (odd == 2) c.p.lat = uint32_t(rng());
        if (odd >= 4) {
            const uint32_t bmin_lat = uint32_t(rng());
            const uint32_t bmin_lng = uint32_t(rng() >> 7);
            c.logical = logical_blob(enc, bmin_lat, bmin_lng, q);
            return c;
        }
    } else {
        uint32_t a = uint32_t(rng() % 65536);
        uint32_t b = uint32_t(rng() % 300);
        if (rng() % 2) a = 0;
        if (rng() % 2) b = 0;
        q.push_back({a, b});
        if (rng() % 2) q.push_back({a, b});
        rl = int64_t(rng() % 5) - 2;
    }
    c.logical = footprint(enc, c.p, q, rl, rg);
    return c;
}

const Point HERE = {0x42517AE1u, 0x409C7AE1u};  // 52.37, 4.89
// A bbox minimum far from any prediction at HERE, in full: 10 and -20.
const Bytes FAR = {0x00, 0x00, 0x20, 0x41, 0x00, 0x00, 0xA0, 0xC1};
const uint32_t FAR_LAT = 0x41200000u, FAR_LNG = 0xC1A00000u;

}  // namespace

TEST(footprint_blobs_decode_to_the_logical_blobs_they_were_made_from) {
    std::mt19937_64 rng(20261114);
    size_t small = 0, full = 0, delta = 0, plain = 0, closed = 0, open = 0;
    for (int kind = 0; kind < 5; kind++)
        for (int t = 0; t < 600; t++) {
            const Case c = random_footprint(rng, kind);
            const Bytes blob = encode(c.logical, c.p);
            CHECK(decode(blob, c.p) == c.logical);
            Shape s;
            CHECK(blob == reference_blob(c.logical, c.p, &s));
            // The header gives the count and the logical length without a value read.
            const af::BlobHeader h = header_of(blob);
            CHECK(!h.escape);
            CHECK_EQ(h.vertex_count(), uint64_t(vertices_of(c.logical)));
            CHECK_EQ(h.logical_bytes(), uint64_t(c.logical.size()));
            CHECK_EQ(h.enc, c.logical[0]);
            CHECK(h.closed == s.closed && h.small == s.small && h.delta == s.delta);
            CHECK(blob.size() <= af::BLOB_MAX_BYTES);
            (s.small ? small : full)++;
            (s.delta ? delta : plain)++;
            (s.closed ? closed : open)++;
        }
    CHECK(small > 500 && full > 500 && delta > 200 && plain > 500 && closed > 500 && open > 500);
}

TEST(a_blob_is_its_tag_its_count_its_bbox_minimum_its_widths_and_its_values_lsb_first) {
    // A closed ring of four corners in encoding 2, the bbox minimum one float above the prediction in
    // lat and two below in lng: small, plain values of 2 bits, the fifth vertex left out.
    const Vertices ring = {{0, 0}, {0, 3}, {2, 3}, {2, 0}, {0, 0}};
    CHECK(encode(footprint(2, HERE, ring, 1, -2), HERE) == (Bytes{0x0E, 0x04, 0x01, 0xFE, 0x22, 0xA0, 0x3C}));
    // An open path in encoding 0 far from its point: the bbox minimum in full, and deltas, which
    // take 7 and 2 bits where the plain values take 8 and 2.
    const Bytes path = logical_blob(0, FAR_LAT, FAR_LNG, {{100, 0}, {130, 1}, {100, 2}});
    CHECK(encode(path, HERE) ==
          joined({{0x10, 0x03}, FAR, {0x27}, packed({{100, 7}, {60, 7}, {59, 7}, {0, 2}, {2, 2}, {2, 2}})}));
    CHECK(encode(path, HERE) ==
          (Bytes{0x10, 0x03, 0x00, 0x00, 0x20, 0x41, 0x00, 0x00, 0xA0, 0xC1, 0x27, 0x64, 0xDE, 0x0E, 0x05}));
    // One vertex at q 0, 0: no bits a value.
    CHECK(encode(footprint(1, HERE, {{0, 0}}), HERE) == (Bytes{0x09, 0x01, 0x00, 0x00, 0x00}));
    // Encoding 3, whose q are u32: the logical blob as it is, after the escape's tag and count.
    const Bytes wide = logical_blob(3, FAR_LAT, FAR_LNG, {{70000, 5}, {1, 4000000000u}});
    CHECK(encode(wide, HERE) == joined({{0x80, 0x02}, wide}));
    CHECK(decode(encode(wide, HERE), HERE) == wide);
    // The blobs written by hand below are the encoder's.
    CHECK(hand_blob(0x10, 3, FAR, 7, 2, {100, 60, 59}, {0, 2, 2}) == encode(path, HERE));
}

TEST(a_bbox_minimum_is_two_residual_bytes_exactly_when_both_are_within_127) {
    const Vertices q = {{10, 20}, {300, 20}, {300, 700}, {10, 20}};
    for (uint8_t enc = 0; enc < 3; enc++)
        for (int64_t rl : {-200, -128, -127, -1, 0, 1, 127, 128, 5000})
            for (int64_t rg : {-128, -127, 0, 127, 128}) {
                const Bytes logical = footprint(enc, HERE, q, rl, rg);
                const Bytes blob = encode(logical, HERE);
                const bool small = rl >= -127 && rl <= 127 && rg >= -127 && rg <= 127;
                CHECK_EQ(header_of(blob).small, small);
                // The tag, the count, 2 or 8 bytes of bbox minimum, the widths, three vertices at 9 + 10 bits.
                CHECK_EQ(blob.size(), size_t(2 + (small ? 2 : 8) + 1 + 8));
                if (small) {
                    CHECK_EQ(int8_t(blob[2]), int8_t(rl));
                    CHECK_EQ(int8_t(blob[3]), int8_t(rg));
                } else {
                    CHECK_EQ(get32(blob, 2), get32(logical, 2));
                    CHECK_EQ(get32(blob, 6), get32(logical, 6));
                }
                CHECK(decode(blob, HERE) == logical);
            }
    // The prediction is the point less the vertices' mean in degrees, the product before the
    // quotient: at 52.37 with a mean of 155 steps of 1e-6.
    CHECK_EQ(af::predicted_bits(HERE.lat, 620, 4, 2), coord_keys::bits_of(float(double(52.37f) - (620.0 * 1e-6) / 4.0)));
    CHECK_EQ(af::predicted_bits(HERE.lat, 620, 4, 0), coord_keys::bits_of(float(double(52.37f) - (620.0 * 1e-5) / 4.0)));
    CHECK_EQ(af::predicted_bits(HERE.lat, 620, 4, 1), coord_keys::bits_of(float(double(52.37f) - (620.0 * 1e-4) / 4.0)));
}

TEST(a_footprint_at_a_point_that_is_not_finite_is_never_small) {
    const float inf = std::numeric_limits<float>::infinity();
    const uint32_t nan = 0x7FC00000u, largest = coord_keys::bits_of(FLT_MAX);
    const Vertices q = {{1, 2}, {3, 4}};
    // At an infinite point the prediction is the infinity, and the largest float is one below it; at
    // a NaN it is a NaN. Either bbox minimum would be a residual a byte holds.
    struct Odd {
        Point p;
        uint32_t bmin_lat, bmin_lng;
    };
    const Odd odds[] = {
        {{coord_keys::bits_of(inf), HERE.lng}, largest, bmin_at(HERE.lng, {2, 4}, 2, 0)},
        {{coord_keys::bits_of(-inf), HERE.lng}, coord_keys::bits_of(-FLT_MAX), bmin_at(HERE.lng, {2, 4}, 2, 0)},
        {{HERE.lat, coord_keys::bits_of(inf)}, bmin_at(HERE.lat, {1, 3}, 2, 0), largest},
        {{nan, HERE.lng}, nan, bmin_at(HERE.lng, {2, 4}, 2, 0)},
        {{HERE.lat, nan | 5}, bmin_at(HERE.lat, {1, 3}, 2, 0), nan | 5},
        {{nan, nan}, nan, nan},
    };
    for (const Odd& o : odds) {
        const Bytes logical = logical_blob(2, o.bmin_lat, o.bmin_lng, q);
        const Bytes blob = encode(logical, o.p);
        CHECK(!header_of(blob).small);
        CHECK_EQ(get32(blob, 2), o.bmin_lat);
        CHECK_EQ(get32(blob, 6), o.bmin_lng);
        CHECK(decode(blob, o.p) == logical);
        // The same blob with two residual bytes is refused, whatever they are.
        for (uint8_t r : {0x00, 0x01, 0xFF}) {
            const Bytes small = hand_blob(0x0A, 2, {r, r}, 2, 3, {1, 3}, {2, 4});
            CHECK_EQ(refusal(small, o.p),
                     std::string("Malformed address files: a small footprint blob at a point that is not finite"));
        }
    }
    // The finite point beside them is small.
    CHECK(header_of(encode(footprint(2, HERE, q), HERE)).small);
    CHECK(encode(footprint(2, HERE, q), HERE) == hand_blob(0x0A, 2, {0, 0}, 2, 3, {1, 3}, {2, 4}));
    // And so is the largest float, whose bbox minimum may be the infinity a float above it: a
    // bbox minimum is any 32 bits.
    const Point edge = {largest, HERE.lng};
    const Bytes above = logical_blob(2, coord_keys::bits_of(inf), bmin_at(HERE.lng, {0}, 2, 0), {{0, 0}});
    CHECK(encode(above, edge) == (Bytes{0x0A, 0x01, 0x01, 0x00, 0x00}));
    CHECK(decode(encode(above, edge), edge) == above);
}

TEST(a_blob_holds_deltas_exactly_when_they_make_its_two_widths_sum_smaller) {
    struct Want {
        Vertices q;
        bool delta;
        uint32_t wl, wg;
    };
    const Want wants[] = {
        // 8 + 2 bits plain, 7 + 2 as deltas.
        {{{100, 0}, {130, 1}, {100, 2}}, true, 7, 2},
        // 7 + 2 either way: plain.
        {{{100, 0}, {120, 1}, {100, 2}}, false, 7, 2},
        // The lat steps take a bit more and the lng steps a bit less: plain.
        {{{0, 200}, {100, 260}, {0, 200}, {100, 260}}, false, 7, 9},
        // A step is taken mod 2^16: from 3 down to 65,535 is -4.
        {{{3, 0}, {65535, 0}, {1, 0}}, true, 3, 0},
        // The lat steps take 15 bits, which are written as 16: 16 + 12 as deltas is no smaller than
        // 14 + 14 plain.
        {{{0x3FFF, 0x800}, {0, 0xFFF}, {0x3FFF, 0x17FE}, {0, 0x1FFD}, {0x3FFF, 0x27FC}}, false, 14, 14},
        // The plain lats take 15 bits, written as 16: 14 + 3 as deltas is smaller than 16 + 2.
        {{{0x2000, 0}, {0x3FFF, 3}, {0x5FFE, 0}}, true, 14, 3},
        // One vertex has no steps.
        {{{40000, 7}}, false, 16, 3},
    };
    for (const Want& w : wants) {
        const Bytes logical = logical_blob(0, FAR_LAT, FAR_LNG, w.q);
        const Bytes blob = encode(logical, HERE);
        const Shape s = shape_of(logical, HERE);
        CHECK_EQ(header_of(blob).delta, w.delta);
        CHECK(s.delta == w.delta && s.wl == w.wl && s.wg == w.wg);
        CHECK_EQ(blob[10], uint8_t((w.wl == 16 ? 15 : w.wl) | (w.wg == 16 ? 15 : w.wg) << 4));
        CHECK(blob == reference_blob(logical, HERE));
        CHECK(decode(blob, HERE) == logical);
    }
}

TEST(a_blob_value_of_15_bits_is_written_in_16) {
    struct Want {
        uint32_t q, nibble, bits;
    };
    for (const Want& w : {Want{0, 0, 0}, Want{1, 1, 1}, Want{0x1FFF, 13, 13}, Want{0x2000, 14, 14}, Want{0x3FFF, 14, 14},
                          Want{0x4000, 15, 16}, Want{0x7FFF, 15, 16}, Want{0x8000, 15, 16}, Want{0xFFFF, 15, 16}})
        for (int a = 0; a < 2; a++) {
            // Two vertices, the other axis 0: the widths byte, and two values of the axis.
            const Vertices q = {{a ? 0 : w.q, a ? w.q : 0}, {a ? 0 : w.q >> 1, a ? w.q >> 1 : 0}};
            const Bytes logical = logical_blob(1, FAR_LAT, FAR_LNG, q);
            const Bytes blob = encode(logical, HERE);
            CHECK_EQ(blob[10], uint8_t(a ? w.nibble << 4 : w.nibble));
            CHECK_EQ(blob.size(), size_t(11 + (2 * w.bits + 7) / 8));
            CHECK(decode(blob, HERE) == logical);
            CHECK(blob == reference_blob(logical, HERE));
        }
}

TEST(a_ring_that_ends_where_it_starts_leaves_its_last_vertex_out) {
    struct Want {
        Vertices q;
        bool closed;
    };
    const Want wants[] = {
        {{{5, 6}}, false},
        {{{5, 6}, {5, 6}}, true},
        {{{5, 6}, {5, 7}}, false},
        {{{5, 6}, {9, 9}, {5, 6}}, true},
        // The first vertex again in the middle, and a last one that shares one axis with the first.
        {{{5, 6}, {9, 9}, {5, 6}, {1, 1}}, false},
        {{{5, 6}, {9, 9}, {5, 7}}, false},
        {{{5, 6}, {9, 9}, {5, 6}, {1, 1}, {5, 6}}, true},
        {{{0, 0}, {0, 0}, {0, 0}}, true},
    };
    for (const Want& w : wants) {
        const Bytes logical = footprint(2, HERE, w.q);
        const Bytes blob = encode(logical, HERE);
        const af::BlobHeader h = header_of(blob);
        CHECK_EQ(h.closed, w.closed);
        CHECK_EQ(h.stored, uint32_t(w.q.size() - w.closed));
        CHECK_EQ(h.vertex_count(), uint64_t(w.q.size()));
        CHECK(decode(blob, HERE) == logical);
        CHECK(blob == reference_blob(logical, HERE));
    }
}

TEST(a_footprint_of_65535_vertices_is_a_blob_and_one_of_more_or_of_encoding_3_an_escape) {
    std::mt19937_64 rng(9);
    auto vertices = [&](size_t n, bool closed) {
        Vertices q;
        for (size_t k = 0; k < n; k++) {
            const uint32_t a = uint32_t(rng() % 65536);
            const uint32_t b = uint32_t(rng() % 65536);
            q.push_back({a, b});
        }
        q.back() = closed ? q[0] : std::pair<uint32_t, uint32_t>{q[0].first ^ 1, q[0].second};
        return q;
    };
    CHECK_EQ(af::MAX_BLOB_VERTICES, uint64_t(65535));
    for (bool closed : {false, true}) {
        // The most a blob holds: 65,535 vertices, of which a closed ring stores 65,534.
        const Bytes most = footprint(2, HERE, vertices(65535, closed), 3, -3);
        const Bytes blob = encode(most, HERE);
        const af::BlobHeader h = header_of(blob);
        CHECK(!h.escape && h.closed == closed);
        CHECK_EQ(h.stored, uint32_t(65535 - closed));
        CHECK_EQ(h.logical_bytes(), uint64_t(10 + 4 * 65535));
        // The count in three bytes, two residuals, and every value at 16 bits.
        CHECK_EQ(blob.size(), size_t(1 + 3 + 2 + 1 + 4 * (65535 - closed)));
        CHECK(blob.size() <= af::BLOB_MAX_BYTES);
        CHECK(decode(blob, HERE) == most);
        CHECK(blob == reference_blob(most, HERE));
        // One more is an escape: the logical blob as it is.
        const Bytes more = footprint(2, HERE, vertices(65536, closed), 3, -3);
        const Bytes escape = encode(more, HERE);
        CHECK(escape == joined({{0x80, 0x80, 0x80, 0x04}, more}));
        const af::BlobHeader e = header_of(escape);
        CHECK(e.escape && e.bytes == 4 && e.enc == 2);
        CHECK_EQ(e.vertex_count(), uint64_t(65536));
        CHECK_EQ(e.logical_bytes(), uint64_t(more.size()));
        CHECK(decode(escape, HERE) == more);
    }
    // The longest blob there is: the bbox minimum in full.
    const Bytes longest = logical_blob(0, FAR_LAT, FAR_LNG, vertices(65535, false));
    CHECK_EQ(encode(longest, HERE).size(), af::BLOB_MAX_BYTES);
    // Encoding 3 is an escape at any count.
    for (size_t n : {size_t(1), size_t(2), size_t(700), size_t(70000)}) {
        Vertices q;
        for (size_t k = 0; k < n; k++) q.push_back({uint32_t(rng()), uint32_t(k)});
        const Bytes wide = logical_blob(3, uint32_t(rng()), FAR_LNG, q);
        const Bytes escape = encode(wide, HERE);
        CHECK(escape == joined({{0x80}, varint(n), wide}));
        const af::BlobHeader e = header_of(escape);
        CHECK(e.escape && e.enc == 3 && e.bytes == 1 + varint(n).size());
        CHECK_EQ(e.vertex_count(), uint64_t(n));
        CHECK_EQ(e.logical_bytes(), uint64_t(10 + 8 * n));
        CHECK(decode(escape, HERE) == wide);
    }
    CHECK(af::is_escape(3, 1) && af::is_escape(0, 65536) && !af::is_escape(0, 65535) && !af::is_escape(2, 1));
    // An escape's head alone, for a blob that is passed on in pieces.
    uint8_t head[af::ESCAPE_HEAD_MAX];
    CHECK_EQ(af::escape_head(65536, head), size_t(4));
    CHECK(Bytes(head, head + 4) == (Bytes{0x80, 0x80, 0x80, 0x04}));
    CHECK_EQ(af::escape_head(0xFFFFFFFFu, head), af::ESCAPE_HEAD_MAX);
}

TEST(the_blob_encoder_refuses_a_footprint_no_blob_holds) {
    const Bytes good = footprint(2, HERE, {{1, 2}, {3, 4}});
    auto refused = [&](Bytes logical, size_t n) {
        return refusal_of([&] {
            Bytes out;
            af::encode_blob(logical.data(), n, HERE.lat, HERE.lng, out);
        });
    };
    auto edited = [](Bytes b, size_t at, uint8_t byte) {
        b[at] = byte;
        return b;
    };
    CHECK_EQ(refused(good, 2), std::string());
    CHECK_EQ(refused(edited(good, 0, 4), 2), std::string("Malformed address files: a footprint in encoding 4"));
    CHECK_EQ(refused(edited(good, 0, 255), 2), std::string("Malformed address files: a footprint in encoding 255"));
    CHECK_EQ(refused(edited(good, 1, 1), 2), std::string("Malformed address files: a footprint with its pad byte set"));
    CHECK_EQ(refused(good, 0), std::string("Malformed address files: a footprint of no vertices"));
    // The same of a footprint that would be an escape.
    const Bytes wide = logical_blob(3, 0, 0, {{1, 2}});
    CHECK_EQ(refused(wide, 1), std::string());
    CHECK_EQ(refused(edited(wide, 1, 0x80), 1), std::string("Malformed address files: a footprint with its pad byte set"));
    CHECK_EQ(refused(wide, 0), std::string("Malformed address files: a footprint of no vertices"));
}

TEST(the_blob_decoder_refuses_each_blob_the_encoder_would_not_write) {
    struct Refused {
        Bytes blob;
        std::string refusal;
    };
    auto cut = [](Bytes b, size_t n) {
        b.resize(b.size() - n);
        return b;
    };
    auto with = [](Bytes b, std::initializer_list<uint8_t> more) {
        b.insert(b.end(), more);
        return b;
    };
    // Two vertices far from the point, plain, 3 and 2 bits; and the same footprint small, were its
    // bbox minimum at the prediction.
    const Bytes good = hand_blob(0x00, 2, FAR, 3, 2, {5, 2}, {0, 3});
    const Bytes small = hand_blob(0x0A, 2, {0x00, 0x00}, 3, 2, {5, 2}, {0, 3});
    const Bytes wide = logical_blob(3, FAR_LAT, FAR_LNG, {{70000, 5}});
    const Bytes many = logical_blob(0, FAR_LAT, FAR_LNG, Vertices(65536, {1, 2}));
    // The bbox minimum of `good` in full, where the prediction is: its residuals fit.
    Bytes near;
    put32(near, bmin_at(HERE.lat, {5, 2}, 0, 100));
    put32(near, bmin_at(HERE.lng, {0, 3}, 0, -127));
    const std::string len = "a footprint blob that is not its record's len";
    const std::string count = "a footprint blob whose count is not a varint of 1 to 65535 vertices";
    const std::string escape_count = "an escaped footprint whose count is not a varint of 1 to 4294967295 vertices";
    const Refused cases[] = {
        // The tag and the count.
        {{}, len},
        {hand_blob(0x20, 2, FAR, 3, 2, {5, 2}, {0, 3}), "a footprint blob with a tag none has"},
        {hand_blob(0x40, 2, FAR, 3, 2, {5, 2}, {0, 3}), "a footprint blob with a tag none has"},
        {hand_blob(0x81, 2, FAR, 3, 2, {5, 2}, {0, 3}), "a footprint blob with a tag none has"},
        {hand_blob(0x03, 2, FAR, 3, 2, {5, 2}, {0, 3}), "a footprint blob with a tag none has"},
        {hand_blob(0x00, 0, FAR, 0, 0, {}, {}), count},
        {joined({{0x00, 0x82, 0x00}, FAR, {0x23, 0x15, 0x06}}), count},
        {joined({{0x00, 0x80, 0x80, 0x04}, FAR, {0x00}}), count},
        {joined({{0x04, 0xFF, 0xFF, 0x03}, FAR, {0x00}}), count},
        {{0x00, 0x82}, count},
        {{0x00}, count},
        // Its length: cut short in the bbox minimum, at the widths, in the values, and a byte too long.
        {cut(good, 3), len},
        {cut(good, 2), len},
        {cut(good, 1), len},
        {with(good, {0x00}), len},
        {cut(small, 2), len},
        {{0x08, 0x02, 0x00}, len},
        // Its values: a width they do not need, the other way of writing them, bits past the last.
        {hand_blob(0x00, 2, FAR, 4, 2, {5, 2}, {0, 3}), "a footprint blob with a width its values do not need"},
        {hand_blob(0x00, 2, FAR, 3, 3, {5, 2}, {0, 3}), "a footprint blob with a width its values do not need"},
        {hand_blob(0x00, 2, FAR, 15, 2, {0x2000, 2}, {0, 3}), "a footprint blob with a width its values do not need"},
        {hand_blob(0x10, 2, FAR, 3, 3, {5, 5}, {0, 6}), "a footprint blob of deltas that make its widths no smaller"},
        {hand_blob(0x00, 3, FAR, 8, 2, {100, 130, 100}, {0, 1, 2}),
         "a footprint blob of plain values where deltas make its widths smaller"},
        {hand_blob(0x10, 3, FAR, 8, 2, {100, 60, 59}, {0, 2, 2}), "a footprint blob with a width its values do not need"},
        {with(cut(good, 1), {0x83}), "a footprint blob with bits set past its last value"},
        // Its ring: a last vertex that is the first, kept.
        {hand_blob(0x00, 3, FAR, 3, 2, {5, 2, 5}, {0, 3, 0}), "an open footprint blob whose last vertex is its first"},
        // Its bbox minimum.
        {hand_blob(0x00, 2, near, 3, 2, {5, 2}, {0, 3}),
         "a footprint blob with its bbox minimum in full where its residuals fit a byte each"},
        {hand_blob(0x0A, 2, {0x80, 0x00}, 3, 2, {5, 2}, {0, 3}), "a small footprint blob with a residual of -128"},
        {hand_blob(0x0A, 2, {0x00, 0x80}, 3, 2, {5, 2}, {0, 3}), "a small footprint blob with a residual of -128"},
        // An escape: its count, its logical blob's header, a footprint a blob holds, its length.
        {joined({{0x80, 0x00}, wide}), escape_count},
        {joined({{0x80, 0x81, 0x00}, wide}), escape_count},
        {joined({{0x80, 0x80, 0x80, 0x80, 0x80, 0x10}, wide}), escape_count},
        {{0x80}, escape_count},
        {{0x80, 0x01}, len},
        {{0x80, 0x01, 0x03}, len},
        {joined({{0x80, 0x01}, logical_blob(4, FAR_LAT, FAR_LNG, {{1, 2}})}), "an escaped footprint in encoding 4"},
        {joined({{0x80, 0x01}, {0x03, 0x01}, Bytes(wide.begin() + 2, wide.end())}), "an escaped footprint with its pad byte set"},
        {joined({{0x80, 0x01}, logical_blob(2, FAR_LAT, FAR_LNG, {{1, 2}})}), "an escape of a footprint a blob holds"},
        {joined({{0x80, 0xFF, 0xFF, 0x03}, cut(many, 4)}), "an escape of a footprint a blob holds"},
        {joined({{0x80, 0x02}, wide}), len},
        {joined({{0x80, 0x01}, cut(wide, 1)}), len},
        {joined({{0x80, 0x01}, with(wide, {0x00})}), len},
        {joined({{0x80, 0x80, 0x80, 0x04}, cut(many, 4)}), len},
    };
    for (const Refused& c : cases) {
        const std::string got = refusal(c.blob, HERE), want = "Malformed address files: " + c.refusal;
        CHECK_EQ(got, want);
        if (got != want) std::printf("    want %s, got %s\n", want.c_str(), got.c_str());
    }
    // The blobs the cases are cut from, and their neighbours that are the encoder's.
    CHECK_EQ(refusal(good, HERE), std::string());
    CHECK_EQ(refusal(small, HERE), std::string());
    CHECK_EQ(refusal(joined({{0x80, 0x01}, wide}), HERE), std::string());
    CHECK_EQ(refusal(joined({{0x80, 0x80, 0x80, 0x04}, many}), HERE), std::string());
    CHECK_EQ(refusal(hand_blob(0x10, 3, FAR, 7, 2, {100, 60, 59}, {0, 2, 2}), HERE), std::string());
    CHECK_EQ(refusal(hand_blob(0x04, 2, FAR, 3, 2, {5, 2}, {0, 3}), HERE), std::string());
    CHECK_EQ(refusal(hand_blob(0x00, 2, near, 3, 2, {5, 2}, {0, 3}), {HERE.lat, HERE.lng ^ 0x00400000u}), std::string());
}

// Every blob a byte changed in, or cut short, either is refused or decodes to
// a footprint whose blob it is: no footprint has two blobs.
TEST(a_footprint_has_one_blob) {
    std::mt19937_64 rng(77);
    size_t refused = 0, decoded = 0;
    for (int kind = 0; kind < 5; kind++)
        for (int f = 0; f < 40; f++) {
            const Case c = random_footprint(rng, kind);
            const Bytes blob = encode(c.logical, c.p);
            for (int t = 0; t < 150; t++) {
                Bytes g = blob;
                Point p = c.p;
                if (t % 25 == 0) {
                    g.resize(rng() % g.size());
                } else if (t % 25 == 1) {
                    // The point the blob is read at moved a float or two: a small bbox minimum moves with it.
                    p.lat = at_place(place(p.lat) + int64_t(rng() % 5) - 2);
                } else {
                    // Mostly where a blob's shape is, its first bytes.
                    const size_t at = rng() % (t % 2 ? std::min<size_t>(g.size(), 14) : g.size());
                    g[at] ^= uint8_t(1u << (rng() % 8));
                }
                // Only the decoder may refuse.
                Bytes logical;
                try {
                    logical = decode(g, p);
                } catch (const std::runtime_error&) {
                    refused++;
                    continue;
                }
                decoded++;
                CHECK(encode(logical, p) == g);
            }
        }
    CHECK(refused > 2000 && decoded > 2000);
}

// The same of blobs written by hand, of any tag, count, widths and values:
// the ones the decoder takes are the ones the encoder writes.
TEST(a_hand_written_blob_decodes_only_where_it_is_the_encoders) {
    std::mt19937_64 rng(78);
    size_t refused = 0, decoded[2] = {0, 0};
    for (int t = 0; t < 120000; t++) {
        const uint8_t enc = uint8_t(rng() % 3);
        const bool closed = rng() % 2;
        const bool small = rng() % 2;
        const bool delta = rng() % 3 == 0;
        const uint32_t oc = 1 + uint32_t(rng() % 5);
        // Nibbles that are mostly the ones a few small values need.
        const uint32_t reach_l = uint32_t(rng() % (t % 7 ? 5 : 17));
        const uint32_t reach_g = uint32_t(rng() % (t % 5 ? 4 : 17));
        const uint32_t nl = reach_l == 16 ? 15 : reach_l, ng = reach_g == 16 ? 15 : reach_g;
        std::vector<uint32_t> lat, lng;
        for (uint32_t k = 0; k < oc; k++) {
            const uint32_t a = uint32_t(rng());
            const uint32_t b = uint32_t(rng());
            lat.push_back(reach_l == 16 ? a & 0xFFFF : a & ((1u << (nl == 15 ? 16 : nl)) - 1));
            lng.push_back(reach_g == 16 ? b & 0xFFFF : b & ((1u << (ng == 15 ? 16 : ng)) - 1));
        }
        Bytes bbox;
        if (small) {
            bbox.push_back(uint8_t(rng() % 5 - 2));
            bbox.push_back(uint8_t(rng() % 5 - 2));
        } else if (rng() % 4 == 0) {
            // In full, and near enough the point that its residuals may fit.
            put32(bbox, at_place(place(HERE.lat) + int64_t(rng() % 400) - 200));
            put32(bbox, at_place(place(HERE.lng) + int64_t(rng() % 400) - 200));
        } else {
            bbox = FAR;
        }
        const uint8_t tag = uint8_t(enc | (closed ? 4 : 0) | (small ? 8 : 0) | (delta ? 16 : 0));
        const Bytes blob = hand_blob(tag, oc, bbox, nl, ng, lat, lng);
        Bytes logical;
        try {
            logical = decode(blob, HERE);
        } catch (const std::runtime_error&) {
            refused++;
            continue;
        }
        decoded[delta]++;
        CHECK(encode(logical, HERE) == blob);
    }
    CHECK(refused > 20000 && decoded[0] > 5000 && decoded[1] > 100);
}

TEST(footprint_blobs_are_the_shared_vectors_byte_for_byte) {
    // geocoder/testdata/addr_files.tsv, which the server's reader and test encoder read too.
    std::ifstream in(std::string(GEOCODER_TESTDATA_DIR) + "/addr_files.tsv");
    REQUIRE(in.good());
    auto bytes = [](const std::string& hex) {
        Bytes out;
        for (size_t i = 0; i + 1 < hex.size(); i += 2) out.push_back(uint8_t(std::stoi(hex.substr(i, 2), nullptr, 16)));
        return out;
    };
    size_t cases = 0, small = 0, full = 0, delta = 0, closed = 0, escapes = 0, boundary = 0, odd_points = 0;
    for (std::string line; std::getline(in, line);) {
        if (line.empty() || line[0] == '#') continue;
        std::vector<std::string> field;
        size_t a = 0;
        for (size_t b; (b = line.find('\t', a)) != std::string::npos; a = b + 1) field.push_back(line.substr(a, b - a));
        field.push_back(line.substr(a));
        if (field[0] != "blob") continue;
        REQUIRE(field.size() == 5);
        unsigned lat, lng;
        REQUIRE(std::sscanf(field[2].c_str(), "%x:%x", &lat, &lng) == 2);
        const Point p = {lat, lng};
        const Bytes logical = bytes(field[3]), want = bytes(field[4]);
        CHECK(encode(logical, p) == want);
        CHECK(decode(want, p) == logical);
        const af::BlobHeader h = header_of(want);
        cases++;
        escapes += h.escape;
        odd_points += !finite(p.lat) || !finite(p.lng);
        if (h.escape) continue;
        (h.small ? small : full)++;
        delta += h.delta;
        closed += h.closed;
        // A prediction that is another float when the mean is taken before the scale: the rows
        // that hold the order of the two to the format's.
        const uint64_t n = vertices_of(logical), sum_lat = sum_of(axis(logical, 0)), sum_lng = sum_of(axis(logical, 1));
        boundary += predicted(p.lat, sum_lat, n, h.enc) != predicted_mean_first(p.lat, sum_lat, n, h.enc) ||
                    predicted(p.lng, sum_lng, n, h.enc) != predicted_mean_first(p.lng, sum_lng, n, h.enc);
    }
    CHECK(cases >= 40);
    CHECK(small >= 10 && full >= 10 && delta >= 5 && closed >= 10 && escapes >= 2 && odd_points >= 2);
    CHECK(boundary >= 3);
}
