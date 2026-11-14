// gen_addr_vectors: writes geocoder/testdata/addr_files.tsv, the footprint blobs and the address
// files the builder's encoder (builder/src/addr_files.h) writes for a fixed set of footprints and
// of records, which the builder's and the server's tests both check (the server's reader and its
// test encoder, addr_files.rs, must agree with them byte for byte).
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

// A logical record: its point (f32 bits), its ids, and its footprint's logical blob, empty for a
// point.
struct Record {
    uint32_t lat, lng, housenumber, street, parent;
    Bytes footprint;
};
using Records = std::vector<Record>;

// The fill of a dead slot: a point at 0, 0 with string 0 for its housenumber and its street, and
// no parent way.
static const Record TOMBSTONE = {0, 0, 0, 0, NO_DATA, {}};

static void files(const std::string& what, const Records& records) {
    // The logical pair: each footprint at the bytes of the ones before it.
    Bytes points, vertices;
    for (const Record& r : records) {
        const bool point = r.footprint.empty();
        const uint32_t count = point ? 0 : uint32_t((r.footprint.size() - 10) / (r.footprint[0] == 3 ? 8 : 4));
        for (uint32_t v : {r.lat, r.lng, r.housenumber, r.street, r.parent, point ? NO_DATA : uint32_t(vertices.size()), count})
            put(points, v, 4);
        vertices.insert(vertices.end(), r.footprint.begin(), r.footprint.end());
    }
    const addr_files::Files f = addr_files::encode(points.data(), records.size(), vertices.data(), vertices.size());
    auto field = [](const Bytes& b) { return b.empty() ? std::string("-") : hex(b); };
    printf("files\t%s\t%s\t%s\t%s\t%s\t%s\n", what.c_str(), field(points).c_str(), field(vertices).c_str(), hex(f.points).c_str(),
           hex(f.vertices).c_str(), hex(f.index).c_str());
}

// n records, record j on the street streets[j mod its size] under the parent way parents[j mod its
// size], all at HERE with one housenumber: what a block's dictionary is made from, and nothing
// else.
static Records on(size_t n, const std::vector<uint32_t>& streets, const std::vector<uint32_t>& parents) {
    Records out;
    for (size_t j = 0; j < n; j++)
        out.push_back({HERE.lat, HERE.lng, 1, streets[j % streets.size()], parents[j % parents.size()], {}});
    return out;
}

// Three records at these lats and lngs, with one housenumber on one street.
static Records at(const std::vector<uint32_t>& lat, const std::vector<uint32_t>& lng) {
    Records out;
    for (size_t j = 0; j < 3; j++) out.push_back({lat[j], lng[j], 1, 2, 3, {}});
    return out;
}

// A building at the record's point: a few corners near each other and the first again.
static Bytes building(std::mt19937_64& rng, const Record& r) {
    // One draw a statement, so the vectors don't depend on the order a compiler evaluates
    // arguments in.
    Vertices ring;
    const size_t corners = 3 + rng() % 6;
    for (size_t k = 0; k < corners; k++) {
        const uint32_t a = uint32_t(rng() % 600);
        const uint32_t b = uint32_t(rng() % 600);
        ring.push_back({a, b});
    }
    ring.push_back(ring[0]);
    const int64_t rl = int64_t(rng() % 7) - 3;
    const int64_t rg = int64_t(rng() % 7) - 3;
    return footprint(2, {r.lat, r.lng}, ring, rl, rg);
}

// n records of a town around lat0, lng0, in steps of a degree / unit: points within 2,000 steps
// of it, streets in runs, each under its own parent way (own) or all under ways near each other,
// housenumbers near each other, and `buildings` in 8 of them a building.
static Records town(std::mt19937_64& rng, size_t n, int64_t lat0, int64_t lng0, double unit, bool own, unsigned buildings) {
    Records out;
    uint32_t street = uint32_t(rng() % 1000000);
    const uint32_t parent = uint32_t(rng() % 1000000);
    const uint32_t housenumber = uint32_t(rng() % 1000000);
    for (size_t i = 0; i < n; i++) {
        const uint64_t dice = rng();
        if (dice % 8 == 0) street += 1 + uint32_t((dice >> 8) % 40);
        const int64_t step_lat = int64_t(rng() % 4001) - 2000;
        const int64_t step_lng = int64_t(rng() % 4001) - 2000;
        Record r;
        // One quotient a coordinate: no product feeds a sum.
        r.lat = bits_of(float(double(lat0 + step_lat) / unit));
        r.lng = bits_of(float(double(lng0 + step_lng) / unit));
        r.housenumber = housenumber + uint32_t((dice >> 16) % 300);
        r.street = street;
        r.parent = own ? street * 2654435761u : parent + street % 7 + uint32_t((dice >> 28) % 4);
        if ((dice >> 32) % 8 < buildings) r.footprint = building(rng, r);
        out.push_back(r);
    }
    return out;
}

int main() {
    printf("# Address files as the builder's encoder writes them (builder/src/addr_files.h). A row is of the\n");
    printf("# kind its first field names, and its second field is what the case covers.\n");
    printf("# blob: the record's point (lat:lng, f32 bits in hex), the footprint's logical blob and its blob\n");
    printf("# in addr_vertices.bin, as hex. The builder's tests encode the logical blob to the blob and\n");
    printf("# decode it back; the server's read the vertices from the blob and its test encoder\n");
    printf("# (addr_files.rs) must write the same blob.\n");
    printf("# files: the logical addr_points.bin and the logical addr_vertices.bin (- for one of no bytes),\n");
    printf("# then addr_points.bin, addr_vertices.bin and addr_index.bin, as hex. The builder's tests encode\n");
    printf("# the logical pair to the three files and decode them back; the server's read every record and\n");
    printf("# its footprint from the files and its test encoder must write the same three.\n");
    printf("# Regenerate with geocoder/testdata/gen_addr_vectors.cpp.\n");
    printf("kind\tcase\tfields\n");

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

    // The files. No records are the three footers; a block of one record is its head and one
    // dictionary entry.
    const Record here = {HERE.lat, HERE.lng, 100, 7, 40, {}};
    Record house = here;
    house.footprint = footprint(2, HERE, ring, 1, -2);
    files("no records", {});
    files("one point", {here});
    files("one building", {house});
    files("a dead slot's record alone", {TOMBSTONE});
    files("a block of dead slots", Records(16, TOMBSTONE));
    files("a point, a dead slot and a building", {here, TOMBSTONE, house});

    // A block is split exactly when that takes fewer bits than pairs.
    Record vertex = here;
    vertex.footprint = footprint(1, HERE, {{0, 0}});
    Records three = {
        vertex, {HERE.lat + 5, HERE.lng - 3, 103, 7, 40, {}}, {HERE.lat + 2, HERE.lng + 12, 100, 9, 40 + (1u << 20), {}}};
    files("three records on two streets under parent ways 2^20 apart: pairs", three);
    three[2].parent = 41;
    files("the same under parent ways next to each other: split", three);
    files("one street under two parent ways 8 apart, sixteen records: split, 4 bits a parent", on(16, {7}, {100, 108}));
    files("one street under two parent ways 16 apart, sixteen records: pairs", on(16, {7}, {100, 116}));
    files("two records of one street under no parent way and under way 0: split, 32 bits a parent", on(2, {7}, {NO_DATA, 0}));
    files("two streets under parent ways 0x4000 apart: split", on(2, {7, 8}, {40, 40 + 0x4000}));
    files("two streets under parent ways 0x8000 apart: pairs", on(2, {7, 8}, {40, 40 + 0x8000}));
    {
        std::vector<uint32_t> streets, near, far;
        for (uint32_t j = 0; j < 16; j++) {
            streets.push_back(7 + j);
            near.push_back(40 + j % 2);
            far.push_back(j * 0x11111111u);
        }
        files("sixteen streets, each under one of two parent ways next to each other: split", on(16, streets, near));
        files("sixteen streets under parent ways 32 bits apart: pairs", on(16, streets, far));
    }
    files("seventeen records of one street under one parent way: a block of sixteen and a block of one", on(17, {7}, {40}));
    files("a dictionary in the order its entries first come", on(11, {900, 5, 900, 70000, 5, 5}, {12, 12, 12, 4000000000u}));

    // An axis is under the hybrid key exactly when every coordinate of the block has one. The
    // float nearest 1e-7 has the hybrid key 1 and the float after it has none; nor has -0.
    const uint32_t tenth = 0x33D6BF95u, minus_zero = 0x80000000u;
    files("lats of 52.37, 1e-7 and 0: hybrid keys", at({HERE.lat, tenth, 0}, {HERE.lng, HERE.lng, HERE.lng}));
    files("a lat one float past 1e-7: the lats under the order key",
          at({HERE.lat, tenth + 1, 0}, {HERE.lng, HERE.lng, HERE.lng}));
    files("a lng of -0: the lngs under the order key", at({HERE.lat, HERE.lat, HERE.lat}, {HERE.lng, minus_zero, 0}));
    files("both axes under the order key", at({tenth + 1, tenth + 1, tenth}, {minus_zero, minus_zero, minus_zero}));
    files("lats of a NaN, minus infinity and 1.0: hybrid keys 32 bits apart",
          at({nan, inf | 0x80000000u, 0x3F800000u}, {HERE.lng, HERE.lng, HERE.lng}));
    files("lats either side of zero under the hybrid key", at({tenth, tenth | 0x80000000u, 0}, {HERE.lng, HERE.lng, HERE.lng}));
    files("points south and west of zero", at({south_west.lat, south_west.lat + 9, south_west.lat - 4},
                                              {south_west.lng - 2, south_west.lng, south_west.lng + 30}));
    files("housenumbers of 0 and NO_DATA: 32 bits a housenumber",
          {{HERE.lat, HERE.lng, 0, 7, 40, {}}, {HERE.lat, HERE.lng, NO_DATA, 7, 40, {}}});

    // Footprints: each record's len is its blob's bytes, and an escape is its logical blob.
    {
        Vertices outline;
        for (uint32_t k = 0; k < 90; k++) outline.push_back({(k * 37) % 4000, (k * k * 11) % 5000});
        Record wide = here, far = here, path = here;
        wide.footprint = logical_blob(3, FAR_LAT, FAR_LNG, {{70000, 5}});
        far.footprint =
            logical_blob(3, bits_of(-90.0f), bits_of(-180.0f), {{0, 0}, {1800000000u, 0}, {1800000000u, 3600000000u}, {0, 0}});
        path.footprint = footprint(0, HERE, outline, -40, 90);
        files("a building, a point, an escape, an outline of 90 vertices, a dead slot and an escape",
              {house, here, wide, path, TOMBSTONE, far});
    }

    // Towns of points a few metres apart, in blocks whose last is short, full, and of one record;
    // one with dead slots; one within a metre just north of the equator, where few floats have a
    // hybrid key; and one of two superblocks.
    for (size_t n : {size_t(15), size_t(16), size_t(17), size_t(40)}) {
        const int64_t lat0 = int64_t(rng() % 160000001) - 80000000;
        const int64_t lng0 = int64_t(rng() % 340000001) - 170000000;
        files(std::to_string(n) + " records of a town, streets under ways near each other",
              town(rng, n, lat0, lng0, 1e6, false, 5));
        files(std::to_string(n) + " records of a town, each street under its own way", town(rng, n, lat0, lng0, 1e6, true, 5));
    }
    {
        Records dead = town(rng, 40, 48856600, 2352200, 1e6, true, 4);
        for (size_t i : {size_t(0), size_t(7), size_t(15), size_t(16), size_t(17), size_t(39)}) dead[i] = TOMBSTONE;
        files("40 records of a town, six of them dead slots", dead);
    }
    files("33 records within a metre of each other at 0.0015, 103.8: the lats under the order key",
          town(rng, 33, 1500000, 103800000000, 1e9, false, 3));
    {
        // Any 32 bits a field, every third record a footprint far from its point.
        Records any;
        for (size_t i = 0; i < 48; i++) {
            Record r;
            r.lat = uint32_t(rng());
            r.lng = uint32_t(rng());
            r.housenumber = uint32_t(rng());
            r.street = uint32_t(rng());
            r.parent = uint32_t(rng());
            if (i % 3 == 0) {
                Vertices q;
                const size_t n = 1 + rng() % 9;
                for (size_t k = 0; k < n; k++) {
                    const uint32_t a = uint32_t(rng() % 65536);
                    const uint32_t b = uint32_t(rng() % 65536);
                    q.push_back({a, b});
                }
                const uint32_t bmin_lat = uint32_t(rng());
                const uint32_t bmin_lng = uint32_t(rng());
                r.footprint = logical_blob(uint8_t(i % 9 / 3), bmin_lat, bmin_lng, q);
            }
            any.push_back(r);
        }
        files("48 records of any 32 bits a field", any);
    }
    files("1025 records of a town: two superblocks, the second of one block of one record",
          town(rng, 1025, -33868800, 151209300, 1e6, false, 1));
    return 0;
}
