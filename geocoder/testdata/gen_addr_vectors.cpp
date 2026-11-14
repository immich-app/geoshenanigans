// gen_addr_vectors: writes geocoder/testdata/addr_files.tsv, the footprint blobs the builder's
// encoder (builder/src/addr_files.h) writes for a fixed set of footprints, which the builder's and
// the server's tests both check (the server's reader and its test encoder, addr_files.rs, must
// agree with them byte for byte).
//   g++ -std=c++20 -O1 -I geocoder/builder/src geocoder/testdata/gen_addr_vectors.cpp -o gen
//   ./gen > geocoder/testdata/addr_files.tsv
#include <cfloat>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "addr_files.h"
#include "coord_keys.h"

using coord_keys::bits_of;
using Bytes = std::vector<uint8_t>;
using Vertices = std::vector<std::pair<uint32_t, uint32_t>>;  // q_lat, q_lng

// A record's point, as f32 bits.
struct Point {
    uint32_t lat, lng;
};
static const Point HERE = {bits_of(52.37f), bits_of(4.89f)};
// A bbox minimum far from any prediction at HERE: 10 and -20.
static const uint32_t FAR_LAT = bits_of(10.0f), FAR_LNG = bits_of(-20.0f);

static std::string hex(const Bytes& b) {
    std::string out;
    char c[3];
    for (uint8_t x : b) {
        snprintf(c, sizeof c, "%02x", x);
        out += c;
    }
    return out;
}

static void put(Bytes& out, uint32_t v, int bytes) {
    for (int k = 0; k < bytes; k++) out.push_back(uint8_t(v >> (8 * k)));
}

// A logical blob: the encoding, a zero byte, the bbox minimum and the vertices, a u16 pair each
// (a u32 pair in encoding 3).
static Bytes logical_blob(uint8_t enc, uint32_t bmin_lat, uint32_t bmin_lng, const Vertices& q) {
    Bytes out = {enc, 0};
    put(out, bmin_lat, 4);
    put(out, bmin_lng, 4);
    for (const auto& v : q) {
        put(out, v.first, enc == 3 ? 4 : 2);
        put(out, v.second, enc == 3 ? 4 : 2);
    }
    return out;
}

// The float `residual` floats after the one a footprint of these vertices at this point is
// predicted to have its bbox minimum at, on one axis (0 lat, 1 lng).
static uint32_t bmin_at(uint32_t point, const Vertices& q, int axis, uint8_t enc, int64_t residual) {
    uint64_t sum = 0;
    for (const auto& v : q) sum += axis ? v.second : v.first;
    const uint32_t predicted = addr_files::predicted_bits(point, sum, q.size(), enc);
    return coord_keys::from_order_key(uint32_t(int64_t(coord_keys::order_key(predicted)) + residual));
}

// A footprint at p in encoding enc, its bbox minimum the residuals away from the prediction.
static Bytes footprint(uint8_t enc, Point p, const Vertices& q, int64_t rl = 0, int64_t rg = 0) {
    return logical_blob(enc, bmin_at(p.lat, q, 0, enc, rl), bmin_at(p.lng, q, 1, enc, rg), q);
}

static void blob(const std::string& what, Point p, const Bytes& logical) {
    Bytes coded;
    addr_files::encode_blob(logical.data(), (logical.size() - 10) / (logical[0] == 3 ? 8 : 4), p.lat, p.lng, coded);
    printf("blob\t%s\t%08x:%08x\t%s\t%s\n", what.c_str(), p.lat, p.lng, hex(logical).c_str(), hex(coded).c_str());
}

// The prediction with the mean taken before the scale. The mean goes through memory, so no
// compiler fuses the product into the difference.
static uint32_t predicted_mean_first(uint32_t point, uint64_t sum, uint64_t n, uint8_t enc) {
    volatile double mean = (double(sum) / double(n)) * addr_files::SCALE[enc];
    return bits_of(float(double(coord_keys::float_of(point)) - mean));
}

// A footprint just north of the equator whose lat prediction is another float when the mean is
// taken before the scale: a blob of it holds the order of the two to the format's.
static void blob_at_a_boundary(std::mt19937_64& rng, uint8_t enc) {
    for (int tries = 0; tries < 10000000; tries++) {
        const size_t n = 1 + rng() % 6;
        Vertices q;
        uint64_t sum = 0;
        for (size_t k = 0; k < n; k++) {
            const uint32_t a = uint32_t(rng() % 3000);
            const uint32_t b = uint32_t(rng() % 3000);
            q.push_back({a, b});
            sum += a;
        }
        // The point where the vertices' mean is over a bbox minimum at the equator.
        const Point p = {bits_of(float((double(sum) * addr_files::SCALE[enc]) / double(n))), HERE.lng};
        if (addr_files::predicted_bits(p.lat, sum, n, enc) == predicted_mean_first(p.lat, sum, n, enc)) continue;
        blob("encoding " + std::to_string(enc) + ": a prediction that is another float when the mean is taken first", p,
             footprint(enc, p, q, 2, -1));
        return;
    }
    fprintf(stderr, "gen_addr_vectors: no prediction at a boundary\n");
    exit(1);
}

int main() {
    printf("# Address files as the builder's encoder writes them (builder/src/addr_files.h). A row is of the\n");
    printf("# kind its first field names.\n");
    printf("# blob: what the case covers, the record's point (lat:lng, f32 bits in hex), the footprint's\n");
    printf("# logical blob and its blob in addr_vertices.bin, as hex. The builder's tests encode the logical\n");
    printf("# blob to the blob and decode it back; the server's read the vertices from the blob and its\n");
    printf("# test encoder (addr_files.rs) must write the same blob.\n");
    printf("# Regenerate with geocoder/testdata/gen_addr_vectors.cpp.\n");
    printf("kind\tcase\tpoint\tlogical\tblob\n");

    const Vertices ring = {{0, 40}, {0, 310}, {120, 310}, {120, 180}, {260, 180}, {260, 40}, {0, 40}};
    blob("one vertex where the point is", HERE, footprint(2, HERE, {{0, 0}}));
    blob("a building: a closed ring, its last vertex left out", HERE, footprint(2, HERE, ring, 1, -2));
    blob("the same outline as an open path", HERE, footprint(2, HERE, Vertices(ring.begin(), ring.end() - 1), 1, -2));
    blob("two equal vertices: a closed ring of one", HERE, footprint(2, HERE, {{7, 9}, {7, 9}}));
    blob("the first vertex again in the middle of an open path", HERE, footprint(2, HERE, {{5, 6}, {9, 9}, {5, 6}, {1, 1}}));
    blob("encoding 0: steps of 1e-5", HERE, footprint(0, HERE, ring, -3, 4));
    blob("encoding 1: steps of 1e-4", HERE, footprint(1, HERE, ring, 5, -6));

    // Deltas exactly when they make the two widths' sum smaller.
    blob("deltas: 7 + 2 bits where the plain values take 8 + 2", HERE,
         logical_blob(0, FAR_LAT, FAR_LNG, {{100, 0}, {130, 1}, {100, 2}}));
    blob("a tie of deltas and plain values is plain", HERE, logical_blob(0, FAR_LAT, FAR_LNG, {{100, 0}, {120, 1}, {100, 2}}));
    blob("a step is taken mod 2^16", HERE, logical_blob(0, FAR_LAT, FAR_LNG, {{3, 0}, {65535, 0}, {1, 0}}));
    blob("steps of 15 bits are written in 16, which keeps the values plain", HERE,
         logical_blob(0, FAR_LAT, FAR_LNG, {{0x3FFF, 0x800}, {0, 0xFFF}, {0x3FFF, 0x17FE}, {0, 0x1FFD}, {0x3FFF, 0x27FC}}));
    blob("plain values of 15 bits are written in 16, which makes them deltas", HERE,
         logical_blob(0, FAR_LAT, FAR_LNG, {{0x2000, 0}, {0x3FFF, 3}, {0x5FFE, 0}}));
    for (uint32_t q : {0x1FFFu, 0x2000u, 0x3FFFu, 0x4000u, 0x7FFFu, 0x8000u, 0xFFFFu}) {
        char what[64];
        snprintf(what, sizeof what, "values up to %u", q);
        blob(what, HERE, footprint(1, HERE, {{q, 0}, {q >> 1, q}, {0, q >> 2}}));
    }

    // The bbox minimum is two residual bytes exactly when both are within 127.
    for (int64_t r : {127, 128, -127, -128}) {
        blob("a lat residual of " + std::to_string(r), HERE, footprint(2, HERE, ring, r, 0));
        blob("a lng residual of " + std::to_string(r), HERE, footprint(2, HERE, ring, 0, r));
    }
    blob("both residuals at their ends", HERE, footprint(2, HERE, ring, -127, 127));

    // Points that are not finite are never small; the floats around zero and at the end are.
    const uint32_t nan = 0x7FC00000u, inf = bits_of(std::numeric_limits<float>::infinity()), largest = bits_of(FLT_MAX);
    blob("a NaN lat, the bbox minimum the same NaN", {nan, HERE.lng},
         logical_blob(2, nan, bmin_at(HERE.lng, ring, 1, 2, 0), ring));
    blob("an infinite lng, the bbox minimum the largest float", {HERE.lat, inf},
         logical_blob(2, bmin_at(HERE.lat, ring, 0, 2, 0), largest, ring));
    blob("a lat of the largest float, the bbox minimum the infinity above it", {largest, HERE.lng},
         logical_blob(2, inf, bmin_at(HERE.lng, {{0, 0}}, 1, 2, 0), {{0, 0}}));
    blob("a point at -0, -0 and a bbox minimum at +0, +0", {0x80000000u, 0x80000000u}, logical_blob(2, 0, 0, {{0, 0}, {0, 0}}));
    blob("a point at +0, +0 under its vertices: a prediction below zero", {0, 0}, footprint(2, {0, 0}, ring));
    const Point south_west = {bits_of(-33.87f), bits_of(-70.66f)};
    blob("a point south and west of zero", south_west, footprint(2, south_west, ring, -1, 1));

    // Encoding 3 is an escape: the logical blob as it is.
    blob("encoding 3, one vertex: an escape", HERE, logical_blob(3, FAR_LAT, FAR_LNG, {{70000, 5}}));
    blob("encoding 3, four vertices: an escape", HERE,
         logical_blob(3, bits_of(-90.0f), bits_of(-180.0f), {{0, 0}, {1800000000u, 0}, {1800000000u, 3600000000u}, {0, 0}}));

    std::mt19937_64 rng(20261114);
    for (uint8_t enc = 0; enc < 3; enc++) blob_at_a_boundary(rng, enc);

    // One draw a statement, so the vectors don't depend on the order a compiler evaluates
    // arguments in.
    for (int t = 0; t < 8; t++) {
        const float lat = float(int64_t(rng() % 1700001) - 850000) / 10000.0f;
        const float lng = float(int64_t(rng() % 3500001) - 1750000) / 10000.0f;
        const Point p = {bits_of(lat), bits_of(lng)};
        const uint8_t enc = uint8_t(rng() % 3);

        // A building: a few corners near each other and the first again.
        Vertices building;
        const size_t corners = 3 + rng() % 9;
        for (size_t k = 0; k < corners; k++) {
            const uint32_t a = uint32_t(rng() % 700);
            const uint32_t b = uint32_t(rng() % 700);
            building.push_back({a, b});
        }
        building.push_back(building[0]);
        const int64_t rl = int64_t(rng() % 9) - 4;
        const int64_t rg = int64_t(rng() % 9) - 4;
        blob("a building of " + std::to_string(corners) + " corners", p, footprint(2, p, building, rl, rg));

        // An outline in small steps from beside the bbox minimum, up from it or around it (a q
        // below it is taken mod 2^16), its residuals at the edge of a byte.
        Vertices outline;
        const size_t steps = 20 + rng() % 60;
        const uint32_t reach = 1u << (2 + rng() % 8);
        const uint32_t back = t % 2 ? reach / 2 : 0;
        uint32_t a = uint32_t(rng() % reach);
        uint32_t b = uint32_t(rng() % reach);
        for (size_t k = 0; k < steps; k++) {
            outline.push_back({a, b});
            a = (a + uint32_t(rng() % reach) - back) & 0xFFFF;
            b = (b + uint32_t(rng() % reach) - back) & 0xFFFF;
        }
        if (rng() % 2) outline.push_back(outline[0]);
        const int64_t el = int64_t(124 + rng() % 8);
        const int64_t eg = -int64_t(124 + rng() % 8);
        blob("an outline of " + std::to_string(outline.size()) + " vertices in steps below " + std::to_string(reach) +
                 (back ? " around the bbox minimum" : " up from the bbox minimum"),
             p, footprint(enc, p, outline, el, eg));

        // Any 16 bits a value, far from the point.
        Vertices any;
        const size_t n = 1 + rng() % 12;
        for (size_t k = 0; k < n; k++) {
            const uint32_t c = uint32_t(rng() % 65536);
            const uint32_t d = uint32_t(rng() % 65536);
            any.push_back({c, d});
        }
        const uint32_t bmin_lat = uint32_t(rng());
        const uint32_t bmin_lng = uint32_t(rng());
        blob(std::to_string(n) + " vertices of any 16 bits, the bbox minimum any 32", p,
             logical_blob(enc, bmin_lat, bmin_lng, any));
    }
    return 0;
}
