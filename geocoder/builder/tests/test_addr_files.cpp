// The address files (addr_files.h): a footprint's blob and a pair's three
// files decode to the logical form they were made from, the choices in them
// are the format's, a footprint has one blob and a logical pair one encoding,
// and what the encoder would not write is refused.
#include <algorithm>
#include <array>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
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
// its residuals at the edge of a byte; any 16 bits a value, the point as
// given or a NaN or an infinity; and one vertex, or two that are equal.
Case random_footprint_at(std::mt19937_64& rng, int kind, Point p) {
    // One draw a statement: the same footprints whatever order a compiler evaluates arguments in.
    Case c;
    c.p = p;
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

// The same at a point anywhere.
Case random_footprint(std::mt19937_64& rng, int kind) {
    const float lat = float(int64_t(rng() % 1700001) - 850000) / 10000.0f;
    const float lng = float(int64_t(rng() % 3500001) - 1750000) / 10000.0f;
    return random_footprint_at(rng, kind, point(lat, lng));
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

// ------------------------------------------------------------------ blocks, index and files
namespace {

void put64(Bytes& out, uint64_t v) {
    for (int k = 0; k < 8; k++) out.push_back(uint8_t(v >> (8 * k)));
}
uint64_t get64(const Bytes& b, size_t at) { return uint64_t(get32(b, at)) | uint64_t(get32(b, at + 4)) << 32; }
void append(Bytes& out, const Bytes& more) { out.insert(out.end(), more.begin(), more.end()); }

// A logical record: its point (f32 bits), its ids, and its footprint's
// logical blob, empty for a point.
struct Record {
    uint32_t lat = 0, lng = 0, housenumber = 0, street = 0, parent = 0;
    Bytes footprint;
    bool operator==(const Record& o) const {
        return lat == o.lat && lng == o.lng && housenumber == o.housenumber && street == o.street && parent == o.parent &&
               footprint == o.footprint;
    }
};
using Records = std::vector<Record>;

// The fill of a dead slot (cell_index.cpp): a point at 0, 0 with string 0 for
// its housenumber and its street, and no parent way.
const Record TOMBSTONE = {0, 0, 0, 0, NO_DATA, {}};

// A logical pair: addr_points.bin and addr_vertices.bin.
struct Pair {
    Bytes points, vertices;
    bool operator==(const Pair& o) const { return points == o.points && vertices == o.vertices; }
};

// A record's 28 bytes, given its vertex_offset and vertex_count.
Bytes record_bytes(const Record& r, uint32_t vertex_offset, uint32_t vertex_count) {
    Bytes out;
    for (uint32_t v : {r.lat, r.lng, r.housenumber, r.street, r.parent, vertex_offset, vertex_count}) put32(out, v);
    return out;
}

// The logical pair of the records: each footprint at the bytes of the ones
// before it.
Pair logical_pair(const Records& records) {
    Pair l;
    for (const Record& r : records) {
        const bool point = r.footprint.empty();
        append(l.points, record_bytes(r, point ? NO_DATA : af::vertex_offset_of(l.vertices.size()),
                                      point ? 0 : uint32_t(vertices_of(r.footprint))));
        append(l.vertices, r.footprint);
    }
    return l;
}

af::Files encode_pair(const Pair& l) {
    return af::encode(l.points.data(), l.points.size() / af::LOGICAL_STRIDE, l.vertices.data(), l.vertices.size());
}
af::Files files_of(const Records& records) { return encode_pair(logical_pair(records)); }

Pair decode_files(const af::Files& f) {
    af::Logical l =
        af::decode(f.points.data(), f.points.size(), f.vertices.data(), f.vertices.size(), f.index.data(), f.index.size());
    return {std::move(l.points), std::move(l.vertices)};
}

bool same(const af::Files& a, const af::Files& b) {
    return a.points == b.points && a.vertices == b.vertices && a.index == b.index;
}

af::Footers footers_of(const af::Files& f) {
    auto end = [](const Bytes& b, size_t footer) { return b.data() + b.size() - std::min(b.size(), footer); };
    return af::read_footers(end(f.points, af::POINTS_FOOTER_BYTES), f.points.size(), end(f.vertices, af::VERTICES_FOOTER_BYTES),
                            f.vertices.size(), end(f.index, af::INDEX_FOOTER_BYTES), f.index.size());
}

// What the decoder says of files it refuses; "" for ones it takes.
std::string files_refusal(const af::Files& f) {
    return refusal_of([&] { decode_files(f); });
}

// An axis's keys by the format's rule: each coordinate's hybrid key with its
// sign bit flipped where every one has a hybrid key, else each one's place in
// the floats' order. Whether they are the latter.
bool axis_keys(const std::vector<uint32_t>& bits, std::vector<uint32_t>& keys) {
    bool hybrid = true;
    for (uint32_t b : bits) {
        uint32_t key = 0;
        hybrid = coord_keys::hybrid_key(b, key) && hybrid;
        keys.push_back(key ^ 0x80000000u);
    }
    if (hybrid) return false;
    keys.clear();
    for (uint32_t b : bits) keys.push_back(uint32_t(place(b)));
    return true;
}

// The least of the values, and the bit length of the largest less it.
std::pair<uint32_t, uint32_t> base_and_width(const std::vector<uint32_t>& v) {
    const uint32_t least = *std::min_element(v.begin(), v.end());
    return {least, bit_length(*std::max_element(v.begin(), v.end()) - least)};
}

// What a block's head says of it.
struct BlockShape {
    bool lat_order = false, lng_order = false, split = false;
    size_t entries = 0;
    std::array<uint32_t, 6> w{};  // lat, lng, housenumber, index, parent, len
};

// The block of n records by the format's rules, the fields a bit at a time,
// given the bytes of each record's blob; shape, when given, is what it chose.
Bytes reference_block(const Record* r, size_t n, const std::vector<uint32_t>& len, BlockShape* shape = nullptr) {
    std::vector<uint32_t> lat, lng, housenumber, parent, streets, pair_index, street_index, lat_keys, lng_keys;
    std::vector<std::pair<uint32_t, uint32_t>> pairs;
    for (size_t j = 0; j < n; j++) {
        lat.push_back(r[j].lat);
        lng.push_back(r[j].lng);
        housenumber.push_back(r[j].housenumber);
        parent.push_back(r[j].parent);
        // The pairs and the streets in the order the records first have them.
        const std::pair<uint32_t, uint32_t> pair = {r[j].street, r[j].parent};
        const auto p = std::find(pairs.begin(), pairs.end(), pair);
        pair_index.push_back(uint32_t(p - pairs.begin()));
        if (p == pairs.end()) pairs.push_back(pair);
        const auto s = std::find(streets.begin(), streets.end(), r[j].street);
        street_index.push_back(uint32_t(s - streets.begin()));
        if (s == streets.end()) streets.push_back(r[j].street);
    }
    BlockShape s;
    s.lat_order = axis_keys(lat, lat_keys);
    s.lng_order = axis_keys(lng, lng_keys);
    const std::pair<uint32_t, uint32_t> lat_span = base_and_width(lat_keys), lng_span = base_and_width(lng_keys),
                                        housenumber_span = base_and_width(housenumber), parent_span = base_and_width(parent);
    const uint32_t pair_bits = bit_length(uint32_t(pairs.size() - 1)), street_bits = bit_length(uint32_t(streets.size() - 1));
    // The streets, a parent base, and a street index and a parent a record, against the pairs and a
    // pair index a record.
    s.split = 32 * streets.size() + 32 + n * (street_bits + parent_span.second) < 64 * pairs.size() + n * pair_bits;
    s.entries = s.split ? streets.size() : pairs.size();
    s.w = {lat_span.second,
           lng_span.second,
           housenumber_span.second,
           s.split ? street_bits : pair_bits,
           s.split ? parent_span.second : 0,
           bit_length(*std::max_element(len.begin(), len.end()))};
    Bytes out = {uint8_t((s.lat_order ? 1 : 0) | (s.lng_order ? 2 : 0) | (s.split ? 4 : 0)), uint8_t(s.entries)};
    Fields widths;
    for (uint32_t w : s.w) widths.push_back({w, 6});
    widths.push_back({0, 4});
    append(out, packed(widths));
    put32(out, lat_span.first);
    put32(out, lng_span.first);
    put32(out, housenumber_span.first);
    if (s.split) {
        for (uint32_t street : streets) put32(out, street);
        put32(out, parent_span.first);
    } else {
        for (const auto& pair : pairs) {
            put32(out, pair.first);
            put32(out, pair.second);
        }
    }
    Fields fields;
    for (size_t j = 0; j < n; j++) {
        fields.push_back({lat_keys[j] - lat_span.first, s.w[0]});
        fields.push_back({lng_keys[j] - lng_span.first, s.w[1]});
        fields.push_back({housenumber[j] - housenumber_span.first, s.w[2]});
        fields.push_back({s.split ? street_index[j] : pair_index[j], s.w[3]});
        fields.push_back({s.split ? parent[j] - parent_span.first : 0, s.w[4]});
        fields.push_back({len[j], s.w[5]});
    }
    append(out, packed(fields));
    if (shape) *shape = s;
    return out;
}

// The three files of the records by the format's rules.
af::Files reference_files(const Records& records) {
    af::Files f;
    Bytes row;
    size_t row_blocks_at = 0, row_blobs_at = 0;
    uint64_t logical = 0;
    uint32_t blobs = 0;
    for (size_t at = 0; at < records.size(); at += 16) {
        const size_t n = std::min<size_t>(16, records.size() - at);
        // A row a superblock of 64 blocks: where its first block and its first blob start, then
        // where each of its blocks and that block's first blob do, from there.
        if (at % 1024 == 0) {
            append(f.index, row);
            row.clear();
            row_blocks_at = f.points.size();
            row_blobs_at = f.vertices.size();
            put64(row, row_blocks_at);
            put64(row, row_blobs_at);
        }
        put16(row, uint32_t(f.points.size() - row_blocks_at));
        put32(row, uint32_t(f.vertices.size() - row_blobs_at));
        std::vector<uint32_t> len;
        for (size_t j = at; j < at + n; j++) {
            const Record& r = records[j];
            const Bytes blob = r.footprint.empty() ? Bytes{} : reference_blob(r.footprint, {r.lat, r.lng});
            append(f.vertices, blob);
            len.push_back(uint32_t(blob.size()));
            logical += r.footprint.size();
            blobs += !r.footprint.empty();
        }
        append(f.points, reference_block(&records[at], n, len));
    }
    append(f.index, row);
    put32(f.points, uint32_t(records.size()));
    append(f.points, {'A', 'D', 'B', '1'});
    put64(f.vertices, logical);
    put32(f.vertices, blobs);
    append(f.vertices, {'A', 'D', 'F', '1'});
    put32(f.index, uint32_t(records.size()));
    append(f.index, {'A', 'D', 'X', '1'});
    return f;
}

// Where block b starts in addr_points.bin and where its records' first blob
// does in addr_vertices.bin, as addr_index.bin has them.
std::pair<uint64_t, uint64_t> block_place(const af::Files& f, uint64_t b) {
    const size_t row = size_t(b / 64) * 400, entry = row + 16 + size_t(b % 64) * 6;
    return {get64(f.index, row) + get16(f.index, entry), get64(f.index, row + 8) + get32(f.index, entry + 2)};
}

// The flags of block b of the files, and its six widths: lat, lng,
// housenumber, index, parent, len.
uint8_t block_flags(const af::Files& f, uint64_t b) { return f.points[size_t(block_place(f, b).first)]; }
std::array<uint32_t, 6> block_widths(const af::Files& f, uint64_t b) {
    const size_t at = size_t(block_place(f, b).first) + 2;
    uint64_t bits = 0;
    for (size_t k = 0; k < 5; k++) bits |= uint64_t(f.points[at + k]) << (8 * k);
    std::array<uint32_t, 6> w{};
    for (size_t k = 0; k < 6; k++) w[k] = uint32_t(bits >> (6 * k)) & 63;
    return w;
}

// Record i as a reader finds it: its block through its superblock's row, and
// its blob where the block's first is plus the len of the block's records
// before it.
Record read_record(const af::Files& f, uint64_t i) {
    const uint64_t b = i / 16;
    const std::pair<uint64_t, uint64_t> at = block_place(f, b);
    af::Block block;
    af::decode_block(b, f.points.data() + at.first, f.points.size() - at.first, footers_of(f).block_records(b), block);
    const size_t j = size_t(i % 16);
    Record r = {block.lat[j], block.lng[j], block.housenumber[j], block.street[j], block.parent[j], {}};
    if (block.len[j]) {
        const uint8_t* blob = f.vertices.data() + at.second + block.blob_bytes_before(j);
        const af::BlobHeader h = af::blob_header(blob, block.len[j]);
        r.footprint.resize(size_t(h.logical_bytes()));
        af::decode_blob(h, blob, block.len[j], r.lat, r.lng, r.footprint.data());
    }
    return r;
}

// n records of one of four kinds: a town (points a few metres apart, streets
// in runs, each under its own parent way or all under ways near each other,
// housenumbers near each other, two in three a building); points within a
// degree of the equator, where few floats have a hybrid key, with runs of
// ones that do and -0 among them; any 32 bits a field, with footprints of
// every kind, escapes among them; and a town with dead slots: tombstones,
// and parents of NO_DATA and far apart.
Records random_records(std::mt19937_64& rng, size_t n, int kind) {
    Records out;
    // One draw a statement: the same records whatever order a compiler evaluates arguments in.
    const double lat0 = double(int64_t(rng() % 1600001) - 800000) / 10000.0;
    const double lng0 = double(int64_t(rng() % 3400001) - 1700000) / 10000.0;
    uint32_t street = uint32_t(rng() % 1000000);
    const uint32_t parent = uint32_t(rng() % 1000000);
    const uint32_t housenumber = uint32_t(rng() % 1000000);
    bool exact = true, own = false;
    for (size_t i = 0; i < n; i++) {
        Record r;
        const uint64_t dice = rng();
        const uint32_t any = uint32_t(rng());
        if (kind == 2) {
            r.lat = uint32_t(rng());
            r.lng = uint32_t(rng());
            r.housenumber = uint32_t(rng());
            // A few streets and parents a block, or any.
            r.street = dice % 4 ? any % 5 : any;
            r.parent = (dice >> 4) % 4 ? (any >> 8) % 7 : any >> 3;
        } else {
            if (dice % 8 == 0) street += 1 + uint32_t((dice >> 8) % 40);
            if (i % 19 == 0) exact = (dice >> 16) % 3 != 0;
            if (i % 23 == 0) own = (dice >> 20) % 2;
            const int64_t step_lat = int64_t(rng() % 4001) - 2000;
            const int64_t step_lng = int64_t(rng() % 4001) - 2000;
            r.lat = coord_keys::bits_of(float(lat0 + double(step_lat) * 1e-6));
            r.lng = coord_keys::bits_of(float(lng0 + double(step_lng) * 1e-6));
            if (kind == 1) {
                // A multiple of 1e-7, which has a hybrid key, or any float below 1.0.
                r.lat =
                    exact ? coord_keys::bits_of(float(double(step_lat * 250) / 1e7)) : (any % 0x3F800000u) | (any & 0x80000000u);
                if ((dice >> 24) % 40 == 0) r.lat = 0x80000000u;
            }
            r.housenumber = housenumber + any % 300;
            r.street = street;
            r.parent = own ? street * 2654435761u : parent + street % 7 + (any >> 12) % 4;
            if (kind == 3) {
                if ((dice >> 28) % 9 == 0) r.parent = NO_DATA;
                if ((dice >> 28) % 9 == 1) r.parent = street * 2654435761u;
                if ((dice >> 32) % 11 == 0) {
                    out.push_back(TOMBSTONE);
                    continue;
                }
            }
        }
        const uint64_t f = (dice >> 40) % 12;
        if (kind == 2 && f == 11) {
            // Encoding 3, which is an escape.
            Vertices q;
            const size_t vertices = 1 + size_t((dice >> 48) % 4);
            for (size_t k = 0; k < vertices; k++) {
                const uint32_t a = uint32_t(rng());
                const uint32_t b = uint32_t(rng());
                q.push_back({a, b});
            }
            r.footprint = logical_blob(3, any, ~any, q);
        } else if (f < 8) {
            const Case c = random_footprint_at(rng, kind == 2 ? int(f % 5) : f < 6 ? 0 : 4, {r.lat, r.lng});
            r.footprint = c.logical;
            r.lat = c.p.lat;
            r.lng = c.p.lng;
        }
        out.push_back(r);
    }
    return out;
}

// HERE's hybrid keys, their sign bits flipped: the floats from 1.0 to each
// coordinate, and 10,000,000.
const uint32_t HERE_LAT_KEY = 0x836A1161u, HERE_LNG_KEY = 0x81B51161u;

// A block written by hand: the flags, the count of dictionary entries, the
// six widths and the four bits after them, the three bases, the dictionary's
// words (a split block's parent base the last) and each record's six fields.
struct HandBlock {
    uint8_t flags = 0;
    uint32_t entries = 1;
    std::array<uint32_t, 6> w{};  // lat, lng, housenumber, index, parent, len
    uint32_t spare = 0;
    uint32_t lat_base = HERE_LAT_KEY, lng_base = HERE_LNG_KEY, housenumber_base = 100;
    std::vector<uint32_t> dictionary = {7, 40};
    std::vector<std::array<uint32_t, 6>> records;
};
Bytes bytes_of(const HandBlock& b) {
    Bytes out = {b.flags, uint8_t(b.entries)};
    Fields widths;
    for (uint32_t w : b.w) widths.push_back({w, 6});
    widths.push_back({b.spare, 4});
    append(out, packed(widths));
    put32(out, b.lat_base);
    put32(out, b.lng_base);
    put32(out, b.housenumber_base);
    for (uint32_t d : b.dictionary) put32(out, d);
    Fields fields;
    for (const auto& r : b.records)
        for (size_t k = 0; k < 6; k++) fields.push_back({r[k], b.w[k]});
    append(out, packed(fields));
    return out;
}

// The three files of blocks written by hand, block b's records' blobs in
// blobs[b]: the index the blocks and the blobs give, and footers of these
// counts.
af::Files hand_files(const std::vector<Bytes>& blocks, uint32_t records, const std::vector<Bytes>& blobs = {},
                     uint64_t logical = 0, uint32_t blob_count = 0) {
    af::Files f;
    Bytes row;
    size_t row_blocks_at = 0, row_blobs_at = 0;
    for (size_t b = 0; b < blocks.size(); b++) {
        if (b % 64 == 0) {
            append(f.index, row);
            row.clear();
            row_blocks_at = f.points.size();
            row_blobs_at = f.vertices.size();
            put64(row, row_blocks_at);
            put64(row, row_blobs_at);
        }
        put16(row, uint32_t(f.points.size() - row_blocks_at));
        put32(row, uint32_t(f.vertices.size() - row_blobs_at));
        append(f.points, blocks[b]);
        if (b < blobs.size()) append(f.vertices, blobs[b]);
    }
    append(f.index, row);
    put32(f.points, records);
    append(f.points, {'A', 'D', 'B', '1'});
    put64(f.vertices, logical);
    put32(f.vertices, blob_count);
    append(f.vertices, {'A', 'D', 'F', '1'});
    put32(f.index, records);
    append(f.index, {'A', 'D', 'X', '1'});
    return f;
}

// The bytes of an escaped footprint after its logical blob's header, which no
// sink here keeps: zeros, a piece at a time.
template <class Encoder>
void feed_escape(Encoder& enc) {
    const Bytes zeros(size_t(1) << 20, 0);
    while (enc.escape_owed()) enc.put_escape_bytes(zeros.data(), size_t(std::min<uint64_t>(enc.escape_owed(), zeros.size())));
}

}  // namespace

TEST(address_files_decode_to_the_logical_pair_they_were_made_from) {
    std::mt19937_64 rng(20261115);
    size_t split = 0, paired = 0, order = 0, hybrid = 0, footprints = 0, escapes = 0, tombstones = 0;
    for (int kind = 0; kind < 4; kind++)
        for (size_t n : {size_t(0), size_t(1), size_t(2), size_t(15), size_t(16), size_t(17), size_t(1023), size_t(1024),
                         size_t(1025), size_t(3000)}) {
            const Records records = random_records(rng, n, kind);
            const Pair logical = logical_pair(records);
            const af::Files f = encode_pair(logical);
            CHECK(decode_files(f) == logical);
            CHECK(same(f, reference_files(records)));
            // The footers count the records, the blobs and the logical bytes, and the index is a
            // row a superblock of 1,024 records and an entry a block of 16.
            const size_t blocks = (n + 15) / 16, superblocks = (blocks + 63) / 64;
            const af::Footers ft = footers_of(f);
            CHECK_EQ(ft.records, uint32_t(n));
            CHECK_EQ(ft.blocks(), uint64_t(blocks));
            CHECK_EQ(ft.superblocks(), uint64_t(superblocks));
            CHECK_EQ(ft.logical_vertex_bytes, uint64_t(logical.vertices.size()));
            CHECK_EQ(ft.blocks_bytes, uint64_t(f.points.size() - 8));
            CHECK_EQ(ft.blobs_bytes, uint64_t(f.vertices.size() - 16));
            CHECK_EQ(f.index.size(), 16 * superblocks + 6 * blocks + 8);
            CHECK_EQ(ft.index_bytes(), uint64_t(f.index.size()));
            const af::LogicalSizes sizes = af::logical_sizes(ft);
            CHECK_EQ(sizes.points, uint64_t(logical.points.size()));
            CHECK_EQ(sizes.vertices, uint64_t(logical.vertices.size()));
            size_t with_footprint = 0;
            for (const Record& r : records) {
                with_footprint += !r.footprint.empty();
                escapes += !r.footprint.empty() && r.footprint[0] == 3;
                tombstones += r == TOMBSTONE;
            }
            CHECK_EQ(ft.blobs, uint32_t(with_footprint));
            footprints += with_footprint;
            for (uint64_t b = 0; b < blocks; b++) {
                const uint8_t flags = block_flags(f, b);
                (flags & 4 ? split : paired)++;
                (flags & 3 ? order : hybrid)++;
                const uint64_t end = b + 1 < blocks ? block_place(f, b + 1).first : ft.blocks_bytes;
                CHECK(end - block_place(f, b).first <= af::BLOCK_MAX_BYTES);
            }
        }
    CHECK(split > 500 && paired > 500 && order > 300 && hybrid > 500 && footprints > 10000 && escapes > 100 && tombstones > 200);
}

TEST(no_records_are_the_three_footers) {
    const af::Files f = files_of({});
    CHECK(f.points == (Bytes{0, 0, 0, 0, 'A', 'D', 'B', '1'}));
    CHECK(f.vertices == (Bytes{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 'A', 'D', 'F', '1'}));
    CHECK(f.index == (Bytes{0, 0, 0, 0, 'A', 'D', 'X', '1'}));
    CHECK_EQ(f.points.size(), af::POINTS_FOOTER_BYTES);
    CHECK_EQ(f.vertices.size(), af::VERTICES_FOOTER_BYTES);
    CHECK_EQ(f.index.size(), af::INDEX_FOOTER_BYTES);
    const Pair none = decode_files(f);
    CHECK(none.points.empty() && none.vertices.empty());
    const af::Footers ft = footers_of(f);
    CHECK(ft.records == 0 && ft.blobs == 0 && ft.blocks() == 0 && ft.superblocks() == 0 && ft.logical_vertex_bytes == 0);
    CHECK(ft.blocks_bytes == 0 && ft.blobs_bytes == 0 && ft.index_bytes() == 8);

    // A dir without the three files holds no records either, and one with some of them is refused.
    const af::Footers absent = af::read_footers(nullptr, af::ABSENT, nullptr, af::ABSENT, nullptr, af::ABSENT);
    CHECK(absent.records == 0 && absent.blobs == 0 && absent.logical_vertex_bytes == 0);
    CHECK(af::logical_sizes(absent).points == 0 && af::logical_sizes(absent).vertices == 0);
    const af::Logical nothing = af::decode(nullptr, af::ABSENT, nullptr, af::ABSENT, nullptr, af::ABSENT);
    CHECK(nothing.points.empty() && nothing.vertices.empty());
    for (int missing = 0; missing < 7; missing++) {
        const std::string said = refusal_of([&] {
            af::decode(f.points.data(), missing & 1 ? af::ABSENT : f.points.size(), f.vertices.data(),
                       missing & 2 ? af::ABSENT : f.vertices.size(), f.index.data(), missing & 4 ? af::ABSENT : f.index.size());
        });
        CHECK_EQ(said, std::string(missing ? "Malformed address files: a dir with some of addr_points.bin, addr_vertices.bin and "
                                             "addr_index.bin and not all three"
                                           : ""));
    }
}

TEST(a_block_is_its_head_its_dictionary_and_its_records_fields_lsb_first) {
    uint32_t key = 0;
    CHECK(coord_keys::hybrid_key(HERE.lat, key) && (key ^ 0x80000000u) == HERE_LAT_KEY);
    CHECK(coord_keys::hybrid_key(HERE.lng, key) && (key ^ 0x80000000u) == HERE_LNG_KEY);
    // Three records a few floats apart, the first with a footprint of one vertex where its point
    // is, on two streets whose parent ways are 2^20 apart: pairs.
    const Bytes vertex = footprint(1, HERE, {{0, 0}});
    Records records = {
        {HERE.lat, HERE.lng, 100, 7, 40, vertex},
        {HERE.lat + 5, HERE.lng - 3, 103, 7, 40, {}},
        {HERE.lat + 2, HERE.lng + 12, 100, 9, 40 + (1u << 20), {}},
    };
    const af::Files paired = files_of(records);
    CHECK(paired.points == joined({{0x00, 0x02},                    // both axes under the hybrid key, pairs; two entries
                                   {0x03, 0x21, 0x04, 0xC0, 0x00},  // 3, 4, 2, 1, 0 and 3 bits a field
                                   {0x61, 0x11, 0x6A, 0x83},        // the least lat key
                                   {0x5E, 0x11, 0xB5, 0x81},        // the least lng key, the second record's
                                   {0x64, 0x00, 0x00, 0x00},        // the least housenumber
                                   {0x07, 0x00, 0x00, 0x00, 0x28, 0x00, 0x00, 0x00},
                                   {0x09, 0x00, 0x00, 0x00, 0x28, 0x00, 0x10, 0x00},
                                   {0x18, 0xB4, 0x30, 0xE8, 0x09},  // three records of 13 bits
                                   {0x03, 0x00, 0x00, 0x00, 'A', 'D', 'B', '1'}}));
    // The blob, then the 14 bytes of its logical blob and the one blob.
    CHECK(paired.vertices ==
          joined({{0x09, 0x01, 0x00, 0x00, 0x00}, {14, 0, 0, 0, 0, 0, 0, 0}, {1, 0, 0, 0}, {'A', 'D', 'F', '1'}}));
    // One superblock, whose first block and first blob start the files, and its one block.
    CHECK(paired.index == joined({Bytes(16, 0), Bytes(6, 0), {0x03, 0x00, 0x00, 0x00, 'A', 'D', 'X', '1'}}));
    CHECK(decode_files(paired) == logical_pair(records));

    // The parent ways next to each other: the streets, the least parent, and a parent a record.
    records[2].parent = 41;
    const af::Files split = files_of(records);
    CHECK(split.points == joined({{0x04, 0x02},
                                  {0x03, 0x21, 0x04, 0xC1, 0x00},  // a bit a parent
                                  {0x61, 0x11, 0x6A, 0x83},
                                  {0x5E, 0x11, 0xB5, 0x81},
                                  {0x64, 0x00, 0x00, 0x00},
                                  {0x07, 0x00, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00},
                                  {0x28, 0x00, 0x00, 0x00},
                                  {0x18, 0x68, 0x61, 0xA0, 0x67, 0x00},  // three records of 14 bits
                                  {0x03, 0x00, 0x00, 0x00, 'A', 'D', 'B', '1'}}));
    CHECK(split.vertices == paired.vertices && split.index == paired.index);
    CHECK(decode_files(split) == logical_pair(records));
}

TEST(a_block_is_split_exactly_when_that_takes_fewer_bits_than_pairs) {
    struct Want {
        Records records;
        bool split;
        size_t entries;
        uint32_t index_bits, parent_bits;
    };
    auto on = [](uint32_t street, uint32_t parent) { return Record{HERE.lat, HERE.lng, 1, street, parent, {}}; };
    // n records, record j on the street street(j) under the parent way parent(j).
    auto block = [&](size_t n, const std::function<uint32_t(size_t)>& street, const std::function<uint32_t(size_t)>& parent) {
        Records out;
        for (size_t j = 0; j < n; j++) out.push_back(on(street(j), parent(j)));
        return out;
    };
    auto one_street = [](size_t) { return uint32_t(7); };
    const Want wants[] = {
        // One record: 64 bits either way, so pairs.
        {block(1, one_street, [](size_t) { return uint32_t(40); }), false, 1, 0, 0},
        // One street under two parent ways, sixteen records: 144 bits paired, and 64 + 16 a parent
        // bit split, which is fewer up to 4 bits a parent.
        {block(16, one_street, [](size_t j) { return uint32_t(100 + 8 * (j % 2)); }), true, 1, 0, 4},
        {block(16, one_street, [](size_t j) { return uint32_t(100 + 16 * (j % 2)); }), false, 2, 1, 0},
        // Two records of one street: 130 bits paired, 128 split with the parents 32 bits apart.
        {block(2, one_street, [](size_t j) { return j ? NO_DATA : 0; }), true, 1, 0, 32},
        // Two records of two streets: 130 bits paired, and 98 + 2 a parent bit split.
        {block(
             2, [](size_t j) { return uint32_t(7 + j); }, [](size_t j) { return uint32_t(40 + 0x4000 * j); }),
         true, 2, 1, 15},
        {block(
             2, [](size_t j) { return uint32_t(7 + j); }, [](size_t j) { return uint32_t(40 + 0x8000 * j); }),
         false, 2, 1, 0},
        // Sixteen streets, each under its own parent way: 1,088 bits paired, 624 split with the
        // parents a bit apart and 1,120 with them 32 bits apart.
        {block(
             16, [](size_t j) { return uint32_t(7 + j); }, [](size_t j) { return uint32_t(40 + j % 2); }),
         true, 16, 4, 1},
        {block(
             16, [](size_t j) { return uint32_t(7 + j); }, [](size_t j) { return uint32_t(j * 0x11111111u); }),
         false, 16, 4, 0},
        // A street a parent way, the same pair through the block: one entry, no bits a record.
        {block(16, one_street, [](size_t) { return uint32_t(40); }), false, 1, 0, 0},
    };
    for (const Want& w : wants) {
        const af::Files f = files_of(w.records);
        CHECK_EQ(bool(block_flags(f, 0) & 4), w.split);
        CHECK_EQ(size_t(f.points[1]), w.entries);
        CHECK_EQ(block_widths(f, 0)[3], w.index_bits);
        CHECK_EQ(block_widths(f, 0)[4], w.parent_bits);
        // The head, the entries (a street and a parent, or a street and the one parent base), and
        // the records' bits.
        const size_t n = w.records.size();
        CHECK_EQ(f.points.size() - 8,
                 19 + (w.split ? 4 * w.entries + 4 : 8 * w.entries) + (n * (w.index_bits + w.parent_bits) + 7) / 8);
        CHECK(same(f, reference_files(w.records)));
        CHECK(decode_files(f) == logical_pair(w.records));
    }
}

TEST(an_axis_is_under_the_hybrid_key_exactly_when_every_coordinate_of_the_block_has_one) {
    // The float nearest 1e-7 has the hybrid key 1 and the float after it has none; nor has -0,
    // whose key would be +0's. A float from 1.0 up has one, a NaN and an infinity too.
    const uint32_t tenth = 0x33D6BF95u, beside = 0x33D6BF96u, minus_zero = 0x80000000u;
    struct Want {
        std::vector<uint32_t> lat, lng;
        uint8_t flags;
        uint32_t lat_base, lng_base, lat_bits, lng_bits;
    };
    const Want wants[] = {
        {{HERE.lat, tenth, 0}, {HERE.lng, HERE.lng, HERE.lng}, 0, 0x80000000u, HERE_LNG_KEY, 26, 0},
        {{HERE.lat, beside, 0}, {HERE.lng, HERE.lng, HERE.lng}, 1, 0x80000000u, HERE_LNG_KEY, 31, 0},
        {{HERE.lat, HERE.lat, HERE.lat}, {HERE.lng, minus_zero, 0}, 2, HERE_LAT_KEY, 0x7FFFFFFFu, 0, 31},
        {{beside, beside, tenth}, {minus_zero, minus_zero, minus_zero}, 3, 0xB3D6BF95u, 0x7FFFFFFFu, 1, 0},
        {{0x7FC00000u, 0xFF800000u, 0x3F800000u},
         {HERE.lng, HERE.lng, HERE.lng},
         0,
         0x80000000u - 0x40000000u - 10000000u,
         HERE_LNG_KEY,
         32,
         0},
        // A coordinate below zero under the hybrid key: the least key is its own.
        {{tenth, tenth | 0x80000000u, 0}, {HERE.lng, HERE.lng, HERE.lng}, 0, 0x7FFFFFFFu, HERE_LNG_KEY, 2, 0},
    };
    for (const Want& w : wants) {
        Records records;
        for (size_t j = 0; j < w.lat.size(); j++) records.push_back({w.lat[j], w.lng[j], 1, 2, 3, {}});
        const af::Files f = files_of(records);
        CHECK_EQ(f.points[0], w.flags);
        CHECK_EQ(get32(f.points, 7), w.lat_base);
        CHECK_EQ(get32(f.points, 11), w.lng_base);
        CHECK_EQ(block_widths(f, 0)[0], w.lat_bits);
        CHECK_EQ(block_widths(f, 0)[1], w.lng_bits);
        CHECK(same(f, reference_files(records)));
        CHECK(decode_files(f) == logical_pair(records));
    }
}

TEST(a_tombstone_record_is_a_record_like_any_other) {
    CHECK(record_bytes(TOMBSTONE, NO_DATA, 0) ==
          (Bytes{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0}));
    std::mt19937_64 rng(5);
    const Records town = random_records(rng, 40, 0);
    for (size_t at : {size_t(0), size_t(7), size_t(15), size_t(16), size_t(39)}) {
        Records records = town;
        records[at] = TOMBSTONE;
        const Pair logical = logical_pair(records);
        const af::Files f = encode_pair(logical);
        CHECK(decode_files(f) == logical);
        CHECK(read_record(f, at) == TOMBSTONE);
        CHECK(same(f, reference_files(records)));
        // The block around it holds its neighbours too, each as it was.
        for (size_t i = at - at % 16; i < std::min<size_t>(40, at - at % 16 + 16); i++) CHECK(read_record(f, i) == records[i]);
    }
    // One alone: the head, the pair of street 0 and no parent way, and no bits a record.
    const af::Files alone = files_of({TOMBSTONE});
    CHECK(alone.points == joined({{0x00, 0x01},
                                  Bytes(5, 0),
                                  {0x00, 0x00, 0x00, 0x80},
                                  {0x00, 0x00, 0x00, 0x80},
                                  {0x00, 0x00, 0x00, 0x00},
                                  {0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF},
                                  {0x01, 0x00, 0x00, 0x00, 'A', 'D', 'B', '1'}}));
    CHECK(decode_files(alone) == logical_pair({TOMBSTONE}));
    // And a block of nothing else is as short.
    const af::Files block = files_of(Records(16, TOMBSTONE));
    CHECK_EQ(block.points.size(), size_t(27 + 8));
    CHECK(decode_files(block) == logical_pair(Records(16, TOMBSTONE)));
}

TEST(the_index_places_each_superblock_and_each_block_and_its_first_blob) {
    std::mt19937_64 rng(6);
    // One record: a block, and a row of one entry.
    const Records lone = random_records(rng, 1, 0);
    const af::Files one = files_of(lone);
    CHECK_EQ(one.index.size(), size_t(16 + 6 + 8));
    CHECK(read_record(one, 0) == lone[0]);
    CHECK(decode_files(one) == logical_pair(lone));

    // 1,025 records: 65 blocks, the last of one record, in two superblocks, the second of that
    // block alone.
    for (int kind : {0, 2}) {
        const Records records = random_records(rng, 1025, kind);
        const af::Files f = files_of(records);
        const af::Footers ft = footers_of(f);
        CHECK(ft.blocks() == 65 && ft.superblocks() == 2 && ft.block_records(0) == 16 && ft.block_records(64) == 1);
        CHECK_EQ(f.index.size(), size_t(2 * 16 + 65 * 6 + 8));
        CHECK_EQ(af::INDEX_ROW_BYTES, size_t(400));
        uint64_t blocks_at = 0, blobs_at = 0, row_blocks_at = 0, row_blobs_at = 0;
        for (uint64_t b = 0; b < 65; b++) {
            const size_t row = size_t(b / 64) * 400, entry = row + 16 + size_t(b % 64) * 6;
            if (b % 64 == 0) {
                CHECK_EQ(get64(f.index, row), blocks_at);
                CHECK_EQ(get64(f.index, row + 8), blobs_at);
                row_blocks_at = blocks_at;
                row_blobs_at = blobs_at;
            }
            CHECK_EQ(uint64_t(get16(f.index, entry)), blocks_at - row_blocks_at);
            CHECK_EQ(uint64_t(get32(f.index, entry + 2)), blobs_at - row_blobs_at);
            af::Block block;
            blocks_at +=
                af::decode_block(b, f.points.data() + blocks_at, f.points.size() - blocks_at, ft.block_records(b), block);
            blobs_at += block.blob_bytes();
        }
        CHECK_EQ(blocks_at, ft.blocks_bytes);
        CHECK_EQ(blobs_at, ft.blobs_bytes);
        CHECK(row_blocks_at > 0 && (kind == 2 || row_blobs_at > 0));
        // Every record is where the index says.
        for (uint64_t i = 0; i < 1025; i++) CHECK(read_record(f, i) == records[size_t(i)]);
    }
}

// The builder encodes slices of whole superblocks on its threads and places
// each after the ones before it; the patcher passes on a blob another set of
// files holds for the same footprint at the same point, and an escaped
// footprint as its bytes come.
TEST(the_files_are_the_same_wherever_slices_are_cut_and_however_a_footprint_comes) {
    std::mt19937_64 rng(11);
    auto into = [](Bytes& v) { return [&v](const uint8_t* p, size_t len) { v.insert(v.end(), p, p + len); }; };
    size_t coded = 0, pieces = 0;
    for (int kind = 0; kind < 4; kind++) {
        const size_t n = 5000 + size_t(rng() % 300);
        const Records records = random_records(rng, n, kind);
        const Pair logical = logical_pair(records);
        const af::Files want = encode_pair(logical);
        // A record's bytes, and its footprint's logical blob, none for a point.
        auto record = [&](size_t i) { return logical.points.data() + 28 * i; };
        auto footprint_of = [&](size_t i) {
            return get32(logical.points, 28 * i + 24)
                       ? logical.vertices.data() + af::vertex_offset_bytes(get32(logical.points, 28 * i + 20))
                       : nullptr;
        };

        // Slices of whole superblocks, each through its own encoder, its index counted from its
        // first block and first blob.
        {
            af::Files f;
            uint64_t blobs = 0, logical_bytes = 0;
            for (size_t at = 0; at < n;) {
                const size_t len = std::min(n - at, 1024 * size_t(1 + rng() % 3));
                af::Files slice;
                af::Encoder enc(into(slice.points), into(slice.vertices), into(slice.index), at, logical_bytes);
                for (size_t i = at; i < at + len; i++) enc.add(record(i), footprint_of(i));
                enc.finish();
                CHECK_EQ(enc.records(), uint64_t(at + len));
                CHECK_EQ(enc.blocks_bytes(), uint64_t(slice.points.size()));
                CHECK_EQ(enc.blobs_bytes(), uint64_t(slice.vertices.size()));
                af::rebase_index(slice.index.data(), slice.index.size(), f.points.size(), f.vertices.size());
                append(f.points, slice.points);
                append(f.vertices, slice.vertices);
                append(f.index, slice.index);
                blobs += enc.blobs();
                logical_bytes = enc.logical_vertex_bytes();
                at += len;
            }
            const auto points = af::points_footer(n);
            const auto vertices = af::vertices_footer(logical_bytes, blobs);
            const auto index = af::index_footer(n);
            f.points.insert(f.points.end(), points.begin(), points.end());
            f.vertices.insert(f.vertices.end(), vertices.begin(), vertices.end());
            f.index.insert(f.index.end(), index.begin(), index.end());
            CHECK(same(f, want));
        }

        // Every other footprint as the blob the files hold for it, and an escape in pieces.
        {
            af::Files f;
            af::Encoder enc(into(f.points), into(f.vertices), into(f.index));
            uint64_t blob_at = 0;
            for (size_t i = 0; i < n; i++) {
                const Bytes blob = records[i].footprint.empty()
                                       ? Bytes{}
                                       : reference_blob(records[i].footprint, {records[i].lat, records[i].lng});
                REQUIRE(blob.empty() ||
                        Bytes(want.vertices.begin() + blob_at, want.vertices.begin() + blob_at + blob.size()) == blob);
                blob_at += blob.size();
                if (blob.empty() || (blob[0] != 0x80 && i % 2)) {
                    enc.add(record(i), footprint_of(i));
                } else if (blob[0] != 0x80) {
                    enc.add_coded(record(i), blob.data(), blob.size());
                    coded++;
                } else {
                    enc.begin_escape(record(i), footprint_of(i));
                    CHECK_EQ(enc.escape_owed(), uint64_t(records[i].footprint.size() - 10));
                    for (const uint8_t* p = footprint_of(i) + 10; enc.escape_owed();) {
                        const size_t k = size_t(std::min<uint64_t>(enc.escape_owed(), 1 + rng() % 7));
                        enc.put_escape_bytes(p, k);
                        p += k;
                        pieces++;
                    }
                }
                CHECK_EQ(enc.records(), uint64_t(i + 1));
                CHECK_EQ(enc.blobs_bytes(), blob_at);
            }
            enc.finish();
            const auto points = enc.points_footer();
            const auto vertices = enc.vertices_footer();
            const auto index = enc.index_footer();
            f.points.insert(f.points.end(), points.begin(), points.end());
            f.vertices.insert(f.vertices.end(), vertices.begin(), vertices.end());
            f.index.insert(f.index.end(), index.begin(), index.end());
            CHECK(same(f, want));
        }
    }
    CHECK(coded > 3000 && pieces > 500);
}

// A footprint's vertex_offset is the bytes of the logical blobs before it in
// units of VERTEX_OFFSET_UNIT, a point's NO_DATA. Bytes that are not whole
// units, or more than a logical addr_vertices.bin is, are no offset: the
// caller's error.
TEST(a_vertex_offset_counts_the_logical_bytes_before_in_units) {
    using Logic = std::logic_error;
    const uint64_t unit = af::VERTEX_OFFSET_UNIT, most = af::MAX_LOGICAL_VERTEX_BYTES;
    CHECK_EQ(unit, uint64_t(2));
    CHECK_EQ(most, unit * 0xFFFFFFFEull);
    // 18 bytes are 9 units, 4 GiB are 2^31, and a footprint of one vertex that
    // ends at the most starts at the largest offset there is.
    CHECK_EQ(af::vertex_offset_of(18), 9u);
    CHECK_EQ(af::vertex_offset_of(0x100000000ull), 0x80000000u);
    CHECK_EQ(af::vertex_offset_of(most - af::logical_blob_bytes(0, 1)), 0xFFFFFFF7u);
    CHECK_EQ(af::vertex_offset_bytes(0xFFFFFFF7u), uint64_t(0x1FFFFFFEEull));
    CHECK(!refusal_of<Logic>([&] { af::vertex_offset_of(17); }).empty());
    for (const uint64_t k : {uint64_t(0), uint64_t(1), uint64_t(9), uint64_t(0x80000000ull), uint64_t(0xFFFFFFFEull)}) {
        CHECK_EQ(af::vertex_offset_of(k * unit), uint32_t(k));
        CHECK_EQ(af::vertex_offset_bytes(uint32_t(k)), k * unit);
    }
    for (uint64_t part = 1; part < unit; part++)
        CHECK(!refusal_of<Logic>([&] { af::vertex_offset_of(18 * unit + part); }).empty());
    // A unit past the most would read as NO_DATA.
    CHECK(!refusal_of<Logic>([&] { af::vertex_offset_of(most + unit); }).empty());
    CHECK(!refusal_of<Logic>([&] { af::vertex_offset_of(uint64_t(NO_DATA)); }).empty());
    CHECK(!refusal_of<Logic>([&] { af::vertex_offset_of(af::NO_FOOTPRINT); }).empty());

    // A logical record holds it: a point's, and a footprint's with 18 units
    // before it. NO_DATA is no count of bytes.
    af::Block block;
    block.records = 1;
    block.lat[0] = HERE.lat;
    block.lng[0] = HERE.lng;
    block.housenumber[0] = 1;
    block.street[0] = 2;
    block.parent[0] = 3;
    const Record here = {HERE.lat, HERE.lng, 1, 2, 3, {}};
    Bytes record(af::LOGICAL_STRIDE);
    af::logical_record(block, 0, af::NO_FOOTPRINT, 0, record.data());
    CHECK(record == record_bytes(here, NO_DATA, 0));
    af::logical_record(block, 0, 18 * unit, 2, record.data());
    CHECK(record == record_bytes(here, 18, 2));
    CHECK(!refusal_of<Logic>([&] { af::logical_record(block, 0, uint64_t(NO_DATA), 0, record.data()); }).empty());

    // An encoder starts at whole units, and takes a footprint at them.
    auto drop = [](const uint8_t*, size_t) {};
    const Bytes ring = footprint(2, HERE, {{1, 2}, {3, 4}});
    for (uint64_t part = 1; part < unit; part++)
        CHECK(!refusal_of<Logic>([&] { af::Encoder enc(drop, drop, drop, 1024, 18 * unit + part); }).empty());
    CHECK(!refusal_of<Logic>([&] { af::Encoder enc(drop, drop, drop, 1024, 37); }).empty());
    af::Encoder enc(drop, drop, drop, 1024, 18 * unit);
    enc.add(record_bytes(here, 18, 2).data(), ring.data());
    CHECK_EQ(enc.logical_vertex_bytes(), 18 * unit + ring.size());
}

TEST(the_encoder_refuses_a_pair_the_files_cant_hold) {
    const Bytes ring = footprint(2, HERE, {{1, 2}, {3, 4}});
    CHECK_EQ(ring.size(), size_t(18));
    const Record here = {HERE.lat, HERE.lng, 1, 2, 3, {}};
    Record building = here;
    building.footprint = ring;
    const Pair good = logical_pair({here, building, building});
    auto refused = [](const Pair& l) { return refusal_of([&] { encode_pair(l); }); };
    // The pair with the u32 at a place of a record's bytes, or a byte of a footprint's, another.
    auto with_word = [](Pair l, size_t record, size_t at, uint32_t v) {
        for (size_t k = 0; k < 4; k++) l.points[28 * record + at + k] = uint8_t(v >> (8 * k));
        return l;
    };
    auto with_byte = [](Pair l, size_t at, uint8_t v) {
        l.vertices[at] = v;
        return l;
    };
    auto with_vertices = [](Pair l, size_t bytes) {
        l.vertices.resize(bytes, 0);
        return l;
    };
    const size_t offset = 20, count = 24;
    struct Refused {
        Pair pair;
        std::string refusal;
    };
    const std::string past = "has a footprint past the end of the logical addr_vertices.bin";
    const std::string after = "bytes after the last footprint of the logical addr_vertices.bin";
    const Refused cases[] = {
        // A vertex_offset without vertices, vertices without one, and one that is not the units
        // of the footprints before it: their bytes among them.
        {with_word(good, 0, offset, 0), "addr_points.bin record 0 has a vertex_offset and no vertices"},
        {with_word(good, 1, offset, NO_DATA), "addr_points.bin record 1 has vertices and no vertex_offset"},
        {with_word(good, 1, offset, 4),
         "addr_points.bin record 1 has a vertex_offset of 4, not the 0 units of 2 bytes of the footprints before it"},
        {with_word(good, 2, offset, 0),
         "addr_points.bin record 2 has a vertex_offset of 0, not the 9 units of 2 bytes of the footprints before it"},
        {with_word(good, 2, offset, 10),
         "addr_points.bin record 2 has a vertex_offset of 10, not the 9 units of 2 bytes of the footprints before it"},
        {with_word(good, 2, offset, 18),
         "addr_points.bin record 2 has a vertex_offset of 18, not the 9 units of 2 bytes of the footprints before it"},
        {with_word(good, 1, count, 1),
         "addr_points.bin record 2 has a vertex_offset of 9, not the 7 units of 2 bytes of the footprints before it"},
        // A footprint no blob holds.
        {with_byte(good, 18, 4), "addr_points.bin record 2 has a footprint in encoding 4"},
        {with_byte(good, 0, 0xFF), "addr_points.bin record 1 has a footprint in encoding 255"},
        {with_byte(good, 1, 1), "addr_points.bin record 1 has a footprint with its pad byte set"},
        // A footprint the logical addr_vertices.bin ends inside, and bytes after the last one.
        {with_word(good, 2, count, 3), "addr_points.bin record 2 " + past},
        {with_vertices(good, 35), "addr_points.bin record 2 " + past},
        {with_vertices(good, 27), "addr_points.bin record 2 " + past},
        {with_vertices(good, 18), "addr_points.bin record 2 " + past},
        {with_vertices(good, 9), "addr_points.bin record 1 " + past},
        {with_vertices(good, 0), "addr_points.bin record 1 " + past},
        {with_word(good, 2, count, 1), after},
        {with_vertices(good, 37), after},
    };
    for (const Refused& c : cases) {
        const std::string got = refused(c.pair), want = "Malformed address files: " + c.refusal;
        CHECK_EQ(got, want);
        if (got != want) std::printf("    want %s, got %s\n", want.c_str(), got.c_str());
    }
    CHECK_EQ(refused(good), std::string());
    // Encoding 3 at the same place is an escape, of twice the bytes a vertex.
    CHECK_EQ(refused(with_vertices(with_byte(good, 18, 3), 18 + 10 + 16)), std::string());

    // A coded blob of another vertex count than its record's, of another length than given,
    // and one no encoder writes.
    auto drop = [](const uint8_t*, size_t) {};
    const Bytes blob = encode(ring, HERE);
    auto coded = [&](uint32_t vertex_count, Bytes b, size_t len) {
        return refusal_of([&] {
            af::Encoder enc(drop, drop, drop);
            b.resize(std::max(b.size(), len) + 8);
            enc.add_coded(record_bytes(here, 0, vertex_count).data(), b.data(), len);
        });
    };
    CHECK_EQ(coded(2, blob, blob.size()), std::string());
    CHECK_EQ(coded(3, blob, blob.size()),
             std::string("Malformed address files: addr_points.bin record 0 has another vertex count than its coded blob"));
    const std::string not_len =
        "Malformed address files: a footprint blob that is not its record's len: addr_vertices.bin, record 0";
    CHECK_EQ(coded(2, blob, blob.size() - 1), not_len);
    CHECK_EQ(coded(2, blob, blob.size() + 1), not_len);
    CHECK_EQ(coded(2, blob, 3), not_len);
    CHECK_EQ(coded(2, blob, 0), not_len);
    CHECK_EQ(coded(2, {0x2A, 0x02, 0x00, 0x00, 0x32, 0xD1, 0x10}, 7),
             std::string("Malformed address files: a footprint blob with a tag none has: addr_vertices.bin, record 0"));
    // An escape passed on whole is its head and its logical blob.
    const Bytes wide = logical_blob(3, FAR_LAT, FAR_LNG, {{70000, 5}});
    CHECK_EQ(coded(1, joined({{0x80, 0x01}, wide}), 2 + wide.size()), std::string());
    CHECK_EQ(coded(1, joined({{0x80, 0x01}, wide}), 1 + wide.size()), not_len);
}

// What breaks the order an encoder is fed in is its caller's error, not the
// data's.
TEST(the_encoder_throws_a_logic_error_where_its_caller_breaks_its_order) {
    auto drop = [](const uint8_t*, size_t) {};
    using Logic = std::logic_error;
    const Record here = {HERE.lat, HERE.lng, 1, 2, 3, {}};
    const Bytes point = record_bytes(here, NO_DATA, 0);
    const Bytes ring = footprint(2, HERE, {{1, 2}, {3, 4}}), wide = logical_blob(3, FAR_LAT, FAR_LNG, {{70000, 5}});
    // A slice starts at a superblock's first record.
    CHECK(!refusal_of<Logic>([&] { af::Encoder enc(drop, drop, drop, 16, 0); }).empty());
    CHECK(!refusal_of<Logic>([&] { af::Encoder enc(drop, drop, drop, 1023, 0); }).empty());
    CHECK(refusal_of<Logic>([&] { af::Encoder enc(drop, drop, drop, 2048, 0); }).empty());
    // Only a footprint that is an escape comes in pieces.
    {
        af::Encoder enc(drop, drop, drop);
        CHECK(!refusal_of<Logic>([&] { enc.begin_escape(record_bytes(here, 0, 2).data(), ring.data()); }).empty());
    }
    // An escape's bytes are all in before the next record, and before the end, and no more.
    {
        af::Encoder enc(drop, drop, drop);
        enc.begin_escape(record_bytes(here, 0, 1).data(), wide.data());
        CHECK_EQ(enc.escape_owed(), uint64_t(8));
        CHECK(!refusal_of<Logic>([&] { enc.add(point.data(), nullptr); }).empty());
        CHECK(!refusal_of<Logic>([&] { enc.finish(); }).empty());
        enc.put_escape_bytes(wide.data() + 10, 5);
        CHECK(!refusal_of<Logic>([&] { enc.put_escape_bytes(wide.data() + 15, 4); }).empty());
        enc.put_escape_bytes(wide.data() + 15, 3);
        CHECK(!refusal_of<Logic>([&] { enc.put_escape_bytes(wide.data(), 1); }).empty());
        enc.add(point.data(), nullptr);
        enc.finish();
        // And no record comes after the end.
        CHECK(!refusal_of<Logic>([&] { enc.add(point.data(), nullptr); }).empty());
        CHECK_EQ(enc.records(), uint64_t(2));
    }
}

TEST(a_logical_vertex_stream_past_8589934588_bytes_is_refused) {
    const uint64_t limit = 0x1FFFFFFFCull;
    CHECK_EQ(af::MAX_LOGICAL_VERTEX_BYTES, limit);
    const std::string past =
        "Malformed address files: a logical addr_vertices.bin of 8589934606 bytes is more than the 8589934588 a patch "
        "can index";
    auto drop = [](const uint8_t*, size_t) {};
    const Record here = {HERE.lat, HERE.lng, 1, 2, 3, {}};
    const Bytes ring = footprint(2, HERE, {{1, 2}, {3, 4}});
    // By the encoder: one that starts 18 bytes short of the most takes a footprint of 18 bytes,
    // at the offset 0xFFFFFFF5, and no other.
    CHECK_EQ(af::vertex_offset_of(limit - 18), 0xFFFFFFF5u);
    {
        af::Encoder enc(drop, drop, drop, 1024, limit - 18);
        enc.add(record_bytes(here, af::vertex_offset_of(limit - 18), 2).data(), ring.data());
        CHECK_EQ(enc.logical_vertex_bytes(), limit);
        enc.add(record_bytes(here, NO_DATA, 0).data(), nullptr);
        CHECK_EQ(refusal_of([&] { enc.add(record_bytes(here, af::vertex_offset_of(limit), 2).data(), ring.data()); }), past);
    }
    {
        af::Encoder enc(drop, drop, drop, 1024, limit - 18);
        CHECK_EQ(refusal_of([&] {
                     enc.add_coded(record_bytes(here, af::vertex_offset_of(limit - 18), 2).data(), encode(ring, HERE).data(), 7);
                 }),
                 std::string());
        CHECK_EQ(refusal_of([&] {
                     enc.add_coded(record_bytes(here, af::vertex_offset_of(limit), 2).data(), encode(ring, HERE).data(), 7);
                 }),
                 past);
    }
    CHECK_EQ(encode(ring, HERE).size(), size_t(7));
    // By the footer the encoder would write.
    CHECK(af::vertices_footer(limit, 1) ==
          (std::array<uint8_t, 16>{0xFC, 0xFF, 0xFF, 0xFF, 1, 0, 0, 0, 1, 0, 0, 0, 'A', 'D', 'F', '1'}));
    CHECK_EQ(refusal_of([&] { af::vertices_footer(limit + 18, 1); }), past);
    CHECK_EQ(refusal_of([&] { af::check_logical_vertex_bytes(limit + 18); }), past);
    CHECK_EQ(refusal_of([&] { af::check_logical_vertex_bytes(limit); }), std::string());

    // By the decoder, on the footer's length, before it reads a block or a blob: the first block
    // here is one no decoder takes.
    Record building = here;
    building.footprint = ring;
    af::Files f = files_of({building, here});
    f.points[0] = 0xFF;
    auto with_length = [](af::Files g, uint64_t length) {
        for (size_t k = 0; k < 8; k++) g.vertices[g.vertices.size() - 16 + k] = uint8_t(length >> (8 * k));
        return g;
    };
    const std::string footer =
        "Malformed address files: a logical addr_vertices.bin of 8589934590 bytes is more than the 8589934588 a "
        "patch can index";
    CHECK_EQ(files_refusal(with_length(f, limit + 2)), footer);
    CHECK_EQ(refusal_of([&] { footers_of(with_length(f, limit + 2)); }), footer);
    CHECK_EQ(
        refusal_of([&] { footers_of(with_length(f, uint64_t(1) << 40)); }),
        std::string(
            "Malformed address files: a logical addr_vertices.bin of 1099511627776 bytes is more than the 8589934588 a patch "
            "can index"));
    // A length of the most passes the footers, and the block is read.
    CHECK_EQ(refusal_of([&] { footers_of(with_length(f, limit)); }), std::string());
    CHECK_EQ(footers_of(with_length(f, limit)).logical_vertex_bytes, limit);
    CHECK_EQ(af::logical_sizes(footers_of(with_length(f, limit))).vertices, limit);
    CHECK_EQ(files_refusal(with_length(f, limit)),
             std::string("Malformed address files: addr_points.bin block 0 has a flag no block has"));
}

// Footprints that start past 4 GiB of logical bytes. An encoder that starts
// 18 bytes short of 2^32 takes four footprints of 18 bytes: the first ends at
// 4 GiB, so the second's vertex_offset is 2^31 units; the third comes as the
// blob another pair's files hold and the fourth as an escape in pieces. A
// record that gives the low 32 bits of the bytes before its footprint, or a
// unit more than their units, is refused. The sinks drop what they are given.
TEST(the_encoder_takes_footprints_past_4_gib_of_logical_bytes) {
    auto drop = [](const uint8_t*, size_t) {};
    const uint64_t four_gib = uint64_t(1) << 32;
    const Record here = {HERE.lat, HERE.lng, 1, 2, 3, {}};
    const Bytes ring = footprint(2, HERE, {{1, 2}, {3, 4}}), blob = encode(ring, HERE);
    const Bytes wide = logical_blob(3, FAR_LAT, FAR_LNG, {{70000, 5}});
    REQUIRE(ring.size() == 18 && wide.size() == 18);
    af::Encoder enc(drop, drop, drop, 1024, four_gib - 18);
    auto refused = [&](uint32_t vertex_offset) {
        return refusal_of([&] { enc.add(record_bytes(here, vertex_offset, 2).data(), ring.data()); });
    };
    CHECK_EQ(refused(0x7FFFFFF7u), std::string());
    CHECK_EQ(enc.logical_vertex_bytes(), four_gib);
    const std::string record = "Malformed address files: addr_points.bin record 1025 has a vertex_offset of ";
    const std::string before = ", not the 2147483648 units of 2 bytes of the footprints before it";
    CHECK_EQ(refused(0), record + "0" + before);
    CHECK_EQ(refused(0x80000001u), record + "2147483649" + before);
    CHECK_EQ(refused(0x80000000u), std::string());
    enc.add_coded(record_bytes(here, 0x80000009u, 2).data(), blob.data(), blob.size());
    enc.begin_escape(record_bytes(here, 0x80000012u, 1).data(), wide.data());
    enc.put_escape_bytes(wide.data() + 10, 3);
    enc.put_escape_bytes(wide.data() + 13, 5);
    enc.finish();
    CHECK_EQ(enc.records(), uint64_t(1028));
    CHECK_EQ(enc.blobs(), uint64_t(4));
    CHECK_EQ(enc.logical_vertex_bytes(), four_gib + 54);
    // The footer holds the bytes in 64 bits.
    CHECK(af::vertices_footer(enc.logical_vertex_bytes(), enc.blobs()) ==
          (std::array<uint8_t, 16>{0x36, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 'A', 'D', 'F', '1'}));

    // A decoder gives a record the units of the bytes before its footprint.
    af::Block block;
    block.records = 1;
    block.lat[0] = HERE.lat;
    block.lng[0] = HERE.lng;
    block.housenumber[0] = 1;
    block.street[0] = 2;
    block.parent[0] = 3;
    Bytes logical(af::LOGICAL_STRIDE);
    af::logical_record(block, 0, four_gib + 18, 2, logical.data());
    CHECK(logical == record_bytes(here, 0x80000009u, 2));
}

TEST(more_records_than_a_patch_indexes_a_blob_of_4_gib_and_a_superblock_of_4_gib_of_blobs_are_refused) {
    auto drop = [](const uint8_t*, size_t) {};
    const Record here = {HERE.lat, HERE.lng, 1, 2, 3, {}};
    const Bytes point = record_bytes(here, NO_DATA, 0);
    const std::string many =
        "Malformed address files: 4294967295 records in addr_points.bin are more than the 4294967294 a patch can index";
    CHECK_EQ(af::MAX_RECORDS, uint64_t(0xFFFFFFFEull));
    // An encoder that starts 1,022 records short of the most takes that many.
    {
        af::Encoder enc(drop, drop, drop, af::MAX_RECORDS - 1022, 0);
        for (int i = 0; i < 1022; i++) enc.add(point.data(), nullptr);
        CHECK_EQ(enc.records(), af::MAX_RECORDS);
        CHECK_EQ(refusal_of([&] { enc.add(point.data(), nullptr); }), many);
        enc.finish();
        CHECK(enc.points_footer() == (std::array<uint8_t, 8>{0xFE, 0xFF, 0xFF, 0xFF, 'A', 'D', 'B', '1'}));
        CHECK(enc.index_footer() == (std::array<uint8_t, 8>{0xFE, 0xFF, 0xFF, 0xFF, 'A', 'D', 'X', '1'}));
    }
    CHECK_EQ(refusal_of([&] { af::points_footer(af::MAX_RECORDS + 1); }), many);
    CHECK_EQ(refusal_of([&] { af::index_footer(af::MAX_RECORDS + 1); }), many);

    // An escape of 1,073,741,819 vertices in encoding 0 is a blob of 4 GiB less 4 bytes: its
    // tag, its count in 5 bytes and a logical blob of 4 GiB less 10. A vertex more is 4 GiB.
    const Bytes header = logical_blob(0, FAR_LAT, FAR_LNG, {});
    {
        af::Encoder enc(drop, drop, drop);
        enc.begin_escape(record_bytes(here, 0, 1073741819).data(), header.data());
        CHECK_EQ(enc.escape_owed(), uint64_t(4294967276ull));
        feed_escape(enc);
        enc.finish();
        CHECK_EQ(enc.blobs_bytes(), uint64_t(4294967292ull));
        CHECK_EQ(enc.logical_vertex_bytes(), uint64_t(4294967286ull));
    }
    {
        af::Encoder enc(drop, drop, drop);
        CHECK_EQ(refusal_of([&] { enc.begin_escape(record_bytes(here, 0, 1073741820).data(), header.data()); }),
                 std::string("Malformed address files: addr_points.bin record 0 has a footprint whose blob is 4 GiB or more"));
    }

    // A block's first blob is placed by a u32 from its superblock's first. An escape of
    // 4,294,967,072 bytes and fifteen footprints of one vertex in the first block: their blobs
    // end a byte short of 4 GiB, and the next block is placed; or a byte past it, and it is not.
    const Bytes wide = logical_blob(0, FAR_LAT, FAR_LNG, {{0xFFFF, 0xFFFF}}),
                narrow = logical_blob(0, FAR_LAT, FAR_LNG, {{0xFF, 0xFF}});
    CHECK(encode(wide, HERE).size() == 15 && encode(narrow, HERE).size() == 13);
    for (bool past : {false, true}) {
        af::Encoder enc(drop, drop, drop);
        enc.begin_escape(record_bytes(here, 0, 1073741764).data(), header.data());
        feed_escape(enc);
        CHECK_EQ(enc.blobs_bytes(), uint64_t(4294967072ull));
        for (int j = 0; j < 15; j++)
            enc.add(record_bytes(here, af::vertex_offset_of(enc.logical_vertex_bytes()), 1).data(),
                    (j < 14 || past ? wide : narrow).data());
        CHECK_EQ(enc.blobs_bytes(), uint64_t(past ? 4294967297ull : 4294967295ull));
        enc.add(point.data(), nullptr);
        CHECK_EQ(
            refusal_of([&] { enc.finish(); }),
            std::string(
                past ? "Malformed address files: addr_vertices.bin would hold 4 GiB or more of a superblock's blobs before the "
                       "block of record 16"
                     : ""));
    }
}

TEST(the_decoder_refuses_each_set_of_files_the_encoder_would_not_write) {
    struct Refused {
        af::Files files;
        std::string refusal;
    };
    auto with = [](HandBlock b, const std::function<void(HandBlock&)>& edit) {
        edit(b);
        return bytes_of(b);
    };
    auto files = [](const Bytes& block, uint32_t records = 2) { return hand_files({block}, records); };
    // The files with a byte of one of them another, and with one of them cut or a byte longer
    // before its footer.
    auto with_byte = [](af::Files f, Bytes af::Files::* file, size_t at, uint8_t byte) {
        (f.*file)[at] = byte;
        return f;
    };
    auto resized = [](af::Files f, Bytes af::Files::* file, size_t bytes) {
        (f.*file).resize(bytes);
        return f;
    };
    auto with_gap = [](af::Files f, Bytes af::Files::* file, size_t footer) {
        (f.*file).insert((f.*file).end() - footer, 0);
        return f;
    };

    // Two records at HERE and three floats north of it, housenumbers 100 and 101, on street 7
    // under the parent way 40: a pair, and 2 + 1 bits a record.
    HandBlock two;
    two.w = {2, 0, 1, 0, 0, 0};
    two.records = {{0, 0, 0, 0, 0, 0}, {3, 0, 1, 0, 0, 0}};
    const Bytes pair = bytes_of(two);
    const Record first = {HERE.lat, HERE.lng, 100, 7, 40, {}};
    Record second = {HERE.lat + 3, HERE.lng, 101, 7, 40, {}};
    CHECK(same(files(pair), files_of({first, second})));
    // The same under the parent ways 40 and 41: split, a parent bit a record more.
    HandBlock split = two;
    split.flags = 4;
    split.w = {2, 0, 1, 0, 1, 0};
    split.records[1][4] = 1;
    second.parent = 41;
    CHECK(same(files(bytes_of(split)), files_of({first, second})));
    // And on the streets 7 and 9 under parent ways 2^20 apart: two pairs, an index bit a record.
    HandBlock pairs = two;
    pairs.entries = 2;
    pairs.w = {2, 0, 1, 1, 0, 0};
    pairs.dictionary = {7, 40, 9, 40 + (1u << 20)};
    pairs.records[1][3] = 1;
    second.street = 9;
    second.parent = 40 + (1u << 20);
    CHECK(same(files(bytes_of(pairs)), files_of({first, second})));
    // Eighteen records in two blocks, the first of sixteen.
    HandBlock sixteen = two;
    sixteen.records.insert(sixteen.records.begin(), 14, std::array<uint32_t, 6>{});
    const af::Files blocks = hand_files({bytes_of(sixteen), pair}, 18);
    CHECK_EQ(files_refusal(blocks), std::string());
    // Sixteen records of the street 7 under the parent ways 100 and 116 in turn: two pairs.
    HandBlock turns;
    turns.entries = 2;
    turns.w = {0, 0, 0, 1, 0, 0};
    turns.dictionary = {7, 100, 7, 116};
    for (uint32_t j = 0; j < 16; j++) turns.records.push_back({0, 0, 0, j % 2, 0, 0});
    CHECK_EQ(files_refusal(files(bytes_of(turns), 16)), std::string());
    // One record at HERE whose footprint is one vertex where the point is, in encoding 1: a blob
    // of 5 bytes for a logical blob of 14.
    HandBlock one;
    one.w = {0, 0, 0, 0, 0, 3};
    one.records = {{0, 0, 0, 0, 0, 5}};
    const Bytes blob = {0x09, 0x01, 0x00, 0x00, 0x00};
    auto with_blob = [&](const Bytes& blobs, uint64_t logical = 14, uint32_t count = 1, uint32_t len = 5) {
        HandBlock b = one;
        b.records[0][5] = len;
        return hand_files({bytes_of(b)}, 1, {blobs}, logical, count);
    };
    Record building = first;
    building.footprint = footprint(1, HERE, {{0, 0}});
    CHECK(same(with_blob(blob), files_of({building})));

    const af::Files good = files(pair);
    const std::string b0 = "addr_points.bin block 0 ", record0 = ": addr_vertices.bin, record 0";
    const std::string misplaced = " is not where the blocks and the blobs before it end";
    const std::string dictionary = "has a dictionary that is not its records' streets and parents in the order they first come";
    const Refused cases[] = {
        // The footers: a file without its magic or shorter than its footer, counts and sizes
        // that disagree.
        {with_byte(good, &af::Files::points, good.points.size() - 1, '2'), "addr_points.bin has no ADB1 footer"},
        {resized(good, &af::Files::points, 7), "addr_points.bin has no ADB1 footer"},
        {resized(good, &af::Files::points, 0), "addr_points.bin has no ADB1 footer"},
        {with_byte(good, &af::Files::vertices, good.vertices.size() - 4, 'a'), "addr_vertices.bin has no ADF1 footer"},
        {resized(good, &af::Files::vertices, 15), "addr_vertices.bin has no ADF1 footer"},
        {with_byte(good, &af::Files::index, good.index.size() - 2, 'Y'), "addr_index.bin has no ADX1 footer"},
        {resized(good, &af::Files::index, 4), "addr_index.bin has no ADX1 footer"},
        {with_byte(good, &af::Files::index, good.index.size() - 8, 3),
         "addr_points.bin and addr_index.bin count different records"},
        {with_byte(good, &af::Files::points, good.points.size() - 8, 17),
         "addr_points.bin and addr_index.bin count different records"},
        {with_gap(good, &af::Files::index, 8), "addr_index.bin is not the rows of its 2 records"},
        {hand_files({pair, pair}, 2), "addr_index.bin is not the rows of its 2 records"},
        {hand_files({pair}, 17), "addr_index.bin is not the rows of its 17 records"},
        {with_byte(good, &af::Files::vertices, good.vertices.size() - 8, 3),
         "addr_vertices.bin counts more blobs than addr_points.bin has records"},
        {hand_files({}, 0xFFFFFFFFu), "4294967295 records in addr_points.bin are more than the 4294967294 a patch can index"},
        // The index: a superblock or a block that is not where the blocks and blobs before it end.
        {with_byte(blocks, &af::Files::index, 0, 1), "addr_index.bin superblock 0" + misplaced},
        {with_byte(blocks, &af::Files::index, 8, 1), "addr_index.bin superblock 0" + misplaced},
        {with_byte(blocks, &af::Files::index, 16, 1), "addr_index.bin block 0" + misplaced},
        {with_byte(blocks, &af::Files::index, 18, 1), "addr_index.bin block 0" + misplaced},
        {with_byte(blocks, &af::Files::index, 22, uint8_t(blocks.index[22] ^ 1)), "addr_index.bin block 1" + misplaced},
        {with_byte(blocks, &af::Files::index, 23, 1), "addr_index.bin block 1" + misplaced},
        {with_byte(blocks, &af::Files::index, 27, 0x80), "addr_index.bin block 1" + misplaced},
        // A block's head.
        {files(with(two, [](HandBlock& b) { b.flags = 0x08; })), b0 + "has a flag no block has"},
        {files(with(two, [](HandBlock& b) { b.flags = 0x80; })), b0 + "has a flag no block has"},
        {files(with(two, [](HandBlock& b) { b.entries = 0; })),
         b0 + "has a dictionary of no entries, or of more than its records"},
        {files(with(two,
                    [](HandBlock& b) {
                        b.entries = 3;
                        b.w[3] = 2;
                        b.dictionary = {7, 40, 8, 40, 9, 40};
                    })),
         b0 + "has a dictionary of no entries, or of more than its records"},
        {files(with(two, [](HandBlock& b) { b.spare = 1; })), b0 + "has bits set past its widths"},
        {files(with(two, [](HandBlock& b) { b.spare = 8; })), b0 + "has bits set past its widths"},
        {files(with(two,
                    [](HandBlock& b) {
                        b.w[0] = 33;
                        b.records.clear();
                    })),
         b0 + "has a width past 32 bits"},
        {files(with(two,
                    [](HandBlock& b) {
                        b.w[5] = 63;
                        b.records.clear();
                    })),
         b0 + "has a width past 32 bits"},
        {files(with(two, [](HandBlock& b) { b.w[3] = 1; })), b0 + "has indexes of another width than its dictionary needs"},
        {files(with(pairs, [](HandBlock& b) { b.w[3] = 2; })), b0 + "has indexes of another width than its dictionary needs"},
        {files(with(two, [](HandBlock& b) { b.w[4] = 1; })), b0 + "has a parent width and no parents of its own"},
        // A block that ends past the blocks: none, one cut short, one of a record more.
        {files(Bytes{}), b0 + "runs past the blocks' end"},
        {files(Bytes(pair.begin(), pair.end() - 1)), b0 + "runs past the blocks' end"},
        {files(Bytes(pair.begin(), pair.begin() + 20)), b0 + "runs past the blocks' end"},
        {files(pair, 3), b0 + "runs past the blocks' end"},
        {hand_files({bytes_of(sixteen), Bytes(pair.begin(), pair.end() - 1)}, 18),
         "addr_points.bin block 1 runs past the blocks' end"},
        // Its records' values: past 32 bits, no coordinate's key, an entry the dictionary lacks.
        {files(with(two, [](HandBlock& b) { b.housenumber_base = NO_DATA; })), b0 + "holds a value past 32 bits"},
        {files(with(split, [](HandBlock& b) { b.dictionary[1] = NO_DATA; })), b0 + "holds a value past 32 bits"},
        {files(with(two, [](HandBlock& b) { b.lat_base = 0xFFFFFFF0u; })), b0 + "holds a hybrid key no coordinate has"},
        {files(with(two, [](HandBlock& b) { b.lng_base = 5; })), b0 + "holds a hybrid key no coordinate has"},
        {hand_files({with(two,
                          [](HandBlock& b) {
                              b.entries = 3;
                              b.w[3] = 2;
                              b.dictionary = {7, 40, 8, 40 + (1u << 20), 9, 40 + (1u << 21)};
                              b.records = {{0, 0, 0, 0, 0, 0}, {3, 0, 1, 1, 0, 0}, {0, 0, 0, 3, 0, 0}};
                          })},
                    3),
         b0 + "has an index past its dictionary"},
        // The choices that are the format's: the key, the bases, the widths, the mode, the
        // dictionary.
        {files(with(two,
                    [](HandBlock& b) {
                        b.flags = 1;
                        b.lat_base = 0xC2517AE1u;
                    })),
         b0 + "is under the order key where every coordinate has a hybrid key"},
        {files(with(two,
                    [](HandBlock& b) {
                        b.flags = 2;
                        b.lng_base = 0xC09C7AE1u;
                    })),
         b0 + "is under the order key where every coordinate has a hybrid key"},
        {files(with(two, [](HandBlock& b) { b.records[0][0] = 1; })), b0 + "has a base none of its records is at"},
        {files(with(two, [](HandBlock& b) { b.records[0][2] = 1; })), b0 + "has a base none of its records is at"},
        {files(with(split, [](HandBlock& b) { b.records[0][4] = 1; })), b0 + "has a base none of its records is at"},
        {files(with(two, [](HandBlock& b) { b.w[0] = 3; })), b0 + "has a width its values do not need"},
        {files(with(two, [](HandBlock& b) { b.w[1] = 1; })), b0 + "has a width its values do not need"},
        {files(with(two, [](HandBlock& b) { b.w[5] = 1; })), b0 + "has a width its values do not need"},
        {files(with(split, [](HandBlock& b) { b.w[4] = 2; })), b0 + "has a width its values do not need"},
        {files(with(split,
                    [](HandBlock& b) {
                        b.w[4] = 0;
                        b.records[1][4] = 0;
                    })),
         b0 + "is split where pairs take no more bits"},
        {files(with(two,
                    [](HandBlock& b) {
                        b.entries = 2;
                        b.w[3] = 1;
                        b.dictionary = {7, 40, 7, 41};
                        b.records[1][3] = 1;
                    })),
         b0 + "is paired where a split takes fewer bits"},
        {files(with(pairs,
                    [](HandBlock& b) {
                        b.dictionary = {9, 40 + (1u << 20), 7, 40};
                        b.records[0][3] = 1;
                        b.records[1][3] = 0;
                    })),
         b0 + dictionary},
        {files(with(pairs, [](HandBlock& b) { b.records[1][3] = 0; })), b0 + dictionary},
        {files(with(pairs,
                    [](HandBlock& b) {
                        b.dictionary = {7, 40, 0, 0};
                        b.records[1][3] = 0;
                    })),
         b0 + dictionary},
        {files(with(pairs, [](HandBlock& b) { b.dictionary = {7, 40, 7, 40}; })), b0 + dictionary},
        {files(with(turns,
                    [](HandBlock& b) {
                        b.dictionary = {7, 116, 7, 100};
                        for (auto& r : b.records) r[3] ^= 1;
                    }),
               16),
         b0 + dictionary},
        // Bits past the last record, and bytes past the last block.
        {with_byte(good, &af::Files::points, pair.size() - 1, uint8_t(pair.back() | 0x80)),
         b0 + "has bits set past its last record"},
        {with_byte(good, &af::Files::points, pair.size() - 1, uint8_t(pair.back() | 0x40)),
         b0 + "has bits set past its last record"},
        {with_gap(good, &af::Files::points, 8), "addr_points.bin has bytes between its last block and its footer"},
        // The blobs: one the file ends inside, one that is not its record's len or not the
        // encoder's, bytes past the last, and counts that are not the blobs'.
        {with_blob({0x09, 0x01, 0x00}), "addr_vertices.bin ends inside the blob of record 0"},
        {with_blob({}), "addr_vertices.bin ends inside the blob of record 0"},
        {with_blob(blob, 14, 1, 4), "a footprint blob that is not its record's len" + record0},
        {with_blob({0x09, 0x01, 0x00, 0x00, 0x00, 0x00}, 14, 1, 6), "a footprint blob that is not its record's len" + record0},
        {with_blob({0x29, 0x01, 0x00, 0x00, 0x00}), "a footprint blob with a tag none has" + record0},
        {with_blob({0x09, 0x01, 0x80, 0x00, 0x00}), "a small footprint blob with a residual of -128" + record0},
        {with_blob({0x09, 0x01, 0x00, 0x00, 0x00, 0x00}), "addr_vertices.bin has bytes between its last blob and its footer"},
        {with_blob(blob, 13), "a footprint past the footer's logical vertex stream of 13 bytes" + record0},
        {with_blob(blob, 0), "a footprint past the footer's logical vertex stream of 0 bytes" + record0},
        {with_blob(blob, 15), "addr_vertices.bin counts 15 bytes of logical blobs and its blobs hold 14"},
        {with_blob(blob, 14, 0), "addr_vertices.bin counts 0 blobs and holds 1"},
        {with_blob(blob, 14, 2), "addr_vertices.bin counts more blobs than addr_points.bin has records"},
        {hand_files({pair}, 2, {}, 0, 1), "addr_vertices.bin counts 1 blobs and holds 0"},
        {hand_files({pair}, 2, {}, 14, 0), "addr_vertices.bin counts 14 bytes of logical blobs and its blobs hold 0"},
    };
    for (const Refused& c : cases) {
        const std::string got = files_refusal(c.files), want = "Malformed address files: " + c.refusal;
        CHECK_EQ(got, want);
        if (got != want) std::printf("    want %s, got %s\n", want.c_str(), got.c_str());
    }
    // The files the cases are made from.
    CHECK_EQ(files_refusal(good), std::string());
    CHECK_EQ(files_refusal(files(bytes_of(split))), std::string());
    CHECK_EQ(files_refusal(files(bytes_of(pairs))), std::string());
    CHECK_EQ(files_refusal(with_blob(blob)), std::string());
}

// Every set of files a byte changed in, or with a file cut short, either is
// refused or decodes to a pair whose encoding it is: no pair has two
// encodings.
TEST(a_logical_pair_has_one_encoding) {
    std::mt19937_64 rng(79);
    size_t refused = 0, decoded[3] = {0, 0, 0};
    for (int kind = 0; kind < 4; kind++) {
        const af::Files f = files_of(random_records(rng, 200, kind));
        for (int t = 0; t < 6000; t++) {
            af::Files g = f;
            Bytes& file = t % 3 == 0 ? g.points : t % 3 == 1 ? g.vertices : g.index;
            if (t % 60 < 3) {
                file.resize(rng() % file.size());
            } else {
                const size_t at = rng() % file.size();
                file[at] ^= uint8_t(1u << (rng() % 8));
            }
            // Only the decoder may refuse.
            Pair logical;
            try {
                logical = decode_files(g);
            } catch (const std::runtime_error&) {
                refused++;
                continue;
            }
            decoded[t % 3]++;
            CHECK(same(encode_pair(logical), g));
        }
    }
    // A changed block or blob is often another pair's; a changed index never is.
    CHECK(refused > 10000 && decoded[0] > 1000 && decoded[1] > 1000 && decoded[2] == 0);
}

// The same of blocks written by hand, of any flags, widths, bases, entries
// and fields: the ones the decoder takes are the ones the encoder writes.
TEST(a_hand_written_block_decodes_only_where_it_is_the_encoders) {
    std::mt19937_64 rng(80);
    size_t refused = 0, decoded[2] = {0, 0}, orders = 0;
    for (int t = 0; t < 300000; t++) {
        HandBlock b;
        const size_t n = 1 + size_t(rng() % (t % 4 ? 3 : 16));
        const uint64_t dice = rng();
        const bool split = dice % 2, lat_order = (dice >> 1) % 4 == 0, lng_order = (dice >> 3) % 4 == 0;
        b.flags = uint8_t((lat_order ? 1 : 0) | (lng_order ? 2 : 0) | (split ? 4 : 0) | ((dice >> 5) % 64 == 0 ? 8 : 0));
        b.entries = 1 + uint32_t((dice >> 11) % std::min<size_t>(n, 3));
        // A bit or two a field, or none; the index mostly at the width its entries need.
        b.w = {uint32_t((dice >> 13) % 3),
               uint32_t((dice >> 15) % 3),
               uint32_t((dice >> 17) % 3),
               bit_length(b.entries - 1) + uint32_t((dice >> 19) % 16 == 0),
               split ? uint32_t((dice >> 23) % 3) : uint32_t((dice >> 23) % 16 == 0),
               uint32_t((dice >> 27) % 8 == 0)};
        // Under the order key, keys around a float without a hybrid key; under the hybrid key,
        // keys around HERE's or around zero's, or, rarely, past the last key.
        const uint32_t hybrid_bases[4] = {HERE_LAT_KEY, 0x7FFFFFFFu, 0x80000000u, 0xFFFFFFF0u};
        b.lat_base =
            lat_order ? 0xB3D6BF94u + uint32_t((dice >> 30) % 2) : hybrid_bases[(dice >> 31) % 13 ? (dice >> 35) % 3 : 3];
        b.lng_base = lng_order ? 0x7FFFFFFEu + uint32_t((dice >> 37) % 2) : hybrid_bases[(dice >> 38) % 3];
        b.housenumber_base = (dice >> 40) % 16 ? uint32_t((dice >> 44) % 100) : NO_DATA - 1;
        // A few streets and parent ways, near each other or far apart.
        b.dictionary.clear();
        for (uint32_t i = 0; i < b.entries; i++) {
            const uint64_t entry = rng();
            b.dictionary.push_back(5 + uint32_t(entry % 3));
            if (!split) b.dictionary.push_back(40 + uint32_t((entry >> 8) % 3) + ((entry >> 16) % 4 ? 0 : 1u << 20));
        }
        if (split) b.dictionary.push_back((dice >> 50) % 16 ? 40 : NO_DATA - 1);
        for (size_t j = 0; j < n; j++) {
            const uint64_t fields = rng();
            std::array<uint32_t, 6> r{};
            for (size_t k = 0; k < 6; k++) r[k] = uint32_t(fields >> (8 * k)) & ((1u << b.w[k]) - 1);
            b.records.push_back(r);
        }
        // The 8 bytes a decoder may read past a block are other blocks', or the footer's.
        const Bytes block = bytes_of(b);
        Bytes padded = block;
        padded.resize(block.size() + 8, 0xFF);
        af::Block out;
        size_t bytes = 0;
        try {
            bytes = af::decode_block(0, padded.data(), padded.size(), n, out);
        } catch (const std::runtime_error&) {
            refused++;
            continue;
        }
        decoded[split]++;
        orders += lat_order || lng_order;
        CHECK_EQ(bytes, block.size());
        uint8_t again[af::BLOCK_MAX_BYTES + 8];
        CHECK(Bytes(again, again + af::encode_block(out, again)) == block);
    }
    CHECK(refused > 100000 && decoded[0] > 2000 && decoded[1] > 500 && orders > 500);
}

namespace {

// The rows of a kind in geocoder/testdata/addr_files.tsv, which the server's
// reader and test encoder read too: each row's fields, the kind first.
std::vector<std::vector<std::string>> vector_rows(const std::string& kind) {
    std::ifstream in(std::string(GEOCODER_TESTDATA_DIR) + "/addr_files.tsv");
    REQUIRE(in.good());
    std::vector<std::vector<std::string>> rows;
    for (std::string line; std::getline(in, line);) {
        if (line.empty() || line[0] == '#') continue;
        std::vector<std::string> field;
        size_t a = 0;
        for (size_t b; (b = line.find('\t', a)) != std::string::npos; a = b + 1) field.push_back(line.substr(a, b - a));
        field.push_back(line.substr(a));
        if (field[0] == kind) rows.push_back(field);
    }
    return rows;
}

// A field's bytes, written as hex: none for "-".
Bytes bytes_of_hex(const std::string& hex) {
    Bytes out;
    for (size_t i = 0; i + 1 < hex.size(); i += 2) out.push_back(uint8_t(std::stoi(hex.substr(i, 2), nullptr, 16)));
    return out;
}

}  // namespace

TEST(footprint_blobs_are_the_shared_vectors_byte_for_byte) {
    size_t cases = 0, small = 0, full = 0, delta = 0, closed = 0, escapes = 0, boundary = 0, odd_points = 0;
    for (const std::vector<std::string>& field : vector_rows("blob")) {
        REQUIRE(field.size() == 5);
        unsigned lat, lng;
        REQUIRE(std::sscanf(field[2].c_str(), "%x:%x", &lat, &lng) == 2);
        const Point p = {lat, lng};
        const Bytes logical = bytes_of_hex(field[3]), want = bytes_of_hex(field[4]);
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

TEST(address_files_are_the_shared_vectors_byte_for_byte) {
    size_t cases = 0, empty = 0, split = 0, paired = 0, order = 0, blobs = 0, escapes = 0;
    uint64_t superblocks = 0;
    for (const std::vector<std::string>& field : vector_rows("files")) {
        REQUIRE(field.size() == 7);
        const Pair logical = {bytes_of_hex(field[2]), bytes_of_hex(field[3])};
        const af::Files want = {bytes_of_hex(field[4]), bytes_of_hex(field[5]), bytes_of_hex(field[6])};
        CHECK(same(encode_pair(logical), want));
        CHECK(decode_files(want) == logical);
        const af::Footers ft = footers_of(want);
        cases++;
        empty += ft.records == 0;
        blobs += ft.blobs;
        superblocks = std::max(superblocks, ft.superblocks());
        for (uint64_t b = 0; b < ft.blocks(); b++) {
            const uint8_t flags = block_flags(want, b);
            (flags & 4 ? split : paired)++;
            order += (flags & 3) != 0;
        }
        for (size_t i = 0; i < ft.records; i++)
            escapes += get32(logical.points, 28 * i + 24) &&
                       logical.vertices[af::vertex_offset_bytes(get32(logical.points, 28 * i + 20))] == 3;
    }
    CHECK(cases >= 14 && empty == 1 && superblocks == 2);
    CHECK(split >= 10 && paired >= 10 && order >= 3 && blobs >= 100 && escapes >= 2);
}
