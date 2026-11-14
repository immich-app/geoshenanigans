// gen_way_vectors: writes geocoder/testdata/way_files.tsv, the way files the builder's encoder
// (builder/src/way_files.h) writes for a fixed set of logical pairs, which the builder's and the
// server's tests both check (the server's reader and its test encoder, way_files.rs, must agree
// with them byte for byte).
//   g++ -std=c++20 -O1 -I geocoder/builder/src geocoder/testdata/gen_way_vectors.cpp -o gen
//   ./gen > geocoder/testdata/way_files.tsv
#include <cfloat>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "coord_keys.h"
#include "way_files.h"

using way_files::INTERPS;
using way_files::Kind;
using way_files::STREETS;

// A way: its nodes are lat bits and lng bits, a pair a node.
struct Way {
    uint32_t name = 0, start = 0, end = 0;
    uint8_t interpolation = 0;
    std::vector<uint32_t> nodes;
};

static std::string hex(const std::vector<uint8_t>& b) {
    std::string out;
    char c[3];
    for (uint8_t x : b) {
        snprintf(c, sizeof c, "%02x", x);
        out += c;
    }
    return out;
}

static void row(const char* what, const Kind& kind, const std::vector<Way>& ways) {
    std::vector<uint8_t> rows, nodes;
    auto into = [](std::vector<uint8_t>& v) { return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); }; };
    way_files::Encoder enc(kind, into(rows), into(nodes));
    std::string logical;
    uint64_t node = 0;
    for (const Way& w : ways) {
        const size_t count = w.nodes.size() / 2;
        uint8_t record[way_files::MAX_STRIDE] = {};
        way_files::put32(record, count ? uint32_t(node) : 0);
        way_files::put16(record + 4, uint16_t(count));
        way_files::put32(record + 8, w.name);
        if (kind.interp) {
            way_files::put32(record + 12, w.start);
            way_files::put32(record + 16, w.end);
            record[20] = w.interpolation;
        }
        enc.add(record, reinterpret_cast<const uint8_t*>(w.nodes.data()));
        node += count;
        char b[64];
        snprintf(b, sizeof b, "%s%u,%u,%u,%u,", logical.empty() ? "" : ";", w.name, w.start, w.end, unsigned(w.interpolation));
        logical += b;
        for (size_t j = 0; j < count; j++) {
            snprintf(b, sizeof b, "%s%08x:%08x", j ? "|" : "", w.nodes[2 * j], w.nodes[2 * j + 1]);
            logical += b;
        }
        if (!count) logical += "-";
    }
    enc.finish();
    const auto ways_footer = enc.ways_footer();
    const auto nodes_footer = enc.nodes_footer();
    rows.insert(rows.end(), ways_footer.begin(), ways_footer.end());
    nodes.insert(nodes.end(), nodes_footer.begin(), nodes_footer.end());
    printf("%s\t%s\t%s\t%s\t%s\n", what, kind.interp ? "interp" : "street", logical.empty() ? "-" : logical.c_str(), hex(rows).c_str(),
           hex(nodes).c_str());
}

static uint32_t bits(float f) { return coord_keys::bits_of(f); }

// The bits of a coordinate of seven decimals.
static uint32_t e7(int64_t v) { return bits(float(double(v) / 1e7)); }

static Way street(uint32_t name, std::vector<uint32_t> nodes) {
    Way w;
    w.name = name;
    w.nodes = std::move(nodes);
    return w;
}

static Way interp(uint32_t street_id, uint32_t start, uint32_t end, uint8_t interpolation, std::vector<uint32_t> nodes) {
    Way w = street(street_id, std::move(nodes));
    w.start = start;
    w.end = end;
    w.interpolation = interpolation;
    return w;
}

// A walk of n nodes over coordinates of seven decimals from (lat, lng), in units of 1e-7.
static std::vector<uint32_t> walk(std::mt19937_64& rng, size_t n, int64_t lat, int64_t lng) {
    std::vector<uint32_t> nodes;
    // One draw a statement, so the vectors don't depend on the order a compiler evaluates
    // operands in.
    int64_t step_lat = int64_t(rng() % 4001) - 2000;
    int64_t step_lng = int64_t(rng() % 4001) - 2000;
    for (size_t j = 0; j < n; j++) {
        nodes.push_back(e7(lat));
        nodes.push_back(e7(lng));
        step_lat += int64_t(rng() % 41) - 20;
        step_lng += int64_t(rng() % 41) - 20;
        lat += step_lat;
        lng += step_lng;
    }
    return nodes;
}

// n ways of a town at (lat, lng), in units of 1e-7: some without nodes, of one node and of two,
// names of every width; `raw` ways in eight are floats between the decimals.
static std::vector<Way> town(std::mt19937_64& rng, const Kind& kind, size_t n, int64_t lat, int64_t lng, uint64_t raw) {
    std::vector<Way> ways;
    for (size_t i = 0; i < n; i++) {
        const uint64_t pick = rng() % 10;
        const size_t more = 3 + size_t(rng() % 9);
        const size_t count = pick == 0 ? 0 : pick == 1 ? 1 : pick == 2 ? 2 : more;
        const int64_t dlat = int64_t(rng() % 400001) - 200000;
        const int64_t dlng = int64_t(rng() % 400001) - 200000;
        Way w;
        w.nodes = walk(rng, count, lat + dlat, lng + dlng);
        if (rng() % 8 < raw)
            for (uint32_t& x : w.nodes) x += uint32_t(rng() % 7);
        const uint64_t shift = 32 + rng() % 32;
        w.name = count ? uint32_t(rng() >> shift) : NO_DATA;
        if (kind.interp) {
            w.start = uint32_t(rng() % 2000);
            w.end = uint32_t(rng() % 2000);
            w.interpolation = uint8_t(rng() % 4);
        }
        ways.push_back(w);
    }
    return ways;
}

int main() {
    printf("# Way files as the builder's encoder writes them (builder/src/way_files.h): what the case covers,\n");
    printf("# the kind (street or interp), the logical ways (name_id or street_id, start_number,\n");
    printf("# end_number, interpolation, and the nodes as lat:lng f32 bits in hex joined by |, - for none;\n");
    printf("# ways joined by ;, - for no ways), and the rows file and the nodes file as hex. A street's\n");
    printf("# numbers are 0 and not stored. The builder's tests encode the ways to the files and decode\n");
    printf("# them back; the server's read the ways from the files and its test encoder (way_files.rs)\n");
    printf("# must write the same files.\n");
    printf("# Regenerate with geocoder/testdata/gen_way_vectors.cpp.\n");
    printf("case\tkind\tways\trows\tnodes\n");
    const uint32_t lat = bits(52.37f), lng = bits(4.89f);
    row("no streets", STREETS, {});
    row("no interpolations", INTERPS, {});
    row("one street without nodes", STREETS, {street(NO_DATA, {})});
    row("one street of one node", STREETS, {street(7, {lat, lng})});
    row("one street of two nodes", STREETS, {street(7, {lat, lng, bits(52.38f), bits(4.9f)})});
    row("a straight street: equal steps", STREETS, {street(0, {e7(523700000), e7(48900000), e7(523701000), e7(48899000), e7(523702000),
                                                              e7(48898000), e7(523703000), e7(48897000), e7(523704000), e7(48896000)})});
    row("one node over and over", STREETS, {street(1, {lat, lng, lat, lng, lat, lng})});
    row("a group whose first street has no nodes", STREETS,
        {street(NO_DATA, {}), street(3, {lat, lng, lat + 9, lng - 4}), street(4, {lat - 1, lng + 2, lat, lng})});
    // The first node of each street after the first sits further from the anchor: in 1, 2, 3 and 4 bytes.
    row("first nodes at every distance from the anchor", STREETS,
        {street(1, {lat, lng}), street(2, {lat + 100, lng - 3}), street(3, {lat - 2, lng + 30000}), street(4, {lat + 5000000, lng}),
         street(5, {bits(-33.87f), bits(151.21f)}), street(6, {lat, lng})});
    row("signed zeros and the corners of the globe: the order key", STREETS,
        {street(9, {bits(-0.0f), bits(0.0f), bits(0.0f), bits(-0.0f), bits(-90.0f), bits(-180.0f), bits(90.0f), bits(180.0f)})});
    row("zero and the smallest decimals either side: the hybrid key", STREETS,
        {street(9, {e7(0), e7(0), e7(1), e7(-1), e7(-2), e7(2), e7(9999999), e7(-9999999)})});
    row("across 1.0, where the hybrid key turns from decimals to floats", STREETS,
        {street(2, {coord_keys::ONE_BITS - 2, coord_keys::ONE_BITS | 0x80000000u, coord_keys::ONE_BITS, (coord_keys::ONE_BITS - 2) | 0x80000000u,
                    coord_keys::ONE_BITS + 1, (coord_keys::ONE_BITS + 1) | 0x80000000u})});
    row("the float after a decimal has no hybrid key", STREETS,
        {street(2, {bits(0.1f), bits(0.5f), bits(0.1f) + 1, bits(0.5f), bits(0.1f), bits(0.5f)}), street(3, {bits(0.1f), bits(0.5f)}),
         street(4, {coord_keys::ONE_BITS - 1, lng})});
    row("infinities, NaNs and the largest floats, then the smallest", STREETS,
        {street(5, {0x7F800000u, 0xFF800000u, 0x7FC00000u, 0xFFC00000u, 0x7FFFFFFFu, 0xFFFFFFFFu, bits(FLT_MAX), bits(-FLT_MAX)}),
         street(6, {1u, 0x80000001u, bits(FLT_MIN) - 1, bits(-FLT_MIN), bits(FLT_MIN), 0u})});
    row("values of 32 bits", STREETS, {street(6, {0u, 0xFFFFFFFFu, 0x7FFFFFFFu, 0u, 0x80000000u, 0x7FFFFFFFu, 0xFFFFFFFFu, 0x80000000u})});
    row("the names 0, NO_DATA and the one before it", STREETS, {street(0, {lat, lng}), street(NO_DATA, {lat, lng}), street(0xFFFFFFFEu, {})});
    row("one interpolation", INTERPS, {interp(12, 1, 99, 2, {lat, lng, lat + 40, lng + 70})});
    row("one interpolation without nodes", INTERPS, {interp(NO_DATA, 0, 0, 0, {})});
    row("an interpolation that ends below its start, and numbers at the edges", INTERPS,
        {interp(1, 100, 2, 1, {lat, lng, lat + 1, lng + 1}), interp(2, 0, NO_DATA, 255, {lat, lng}), interp(3, NO_DATA, 0, 3, {}),
         interp(4, NO_DATA, NO_DATA, 0, {lat + 3, lng - 3, lat, lng})});
    std::mt19937_64 rng(20261114);
    for (size_t n : {size_t(15), size_t(16), size_t(17), size_t(40)}) {
        row((std::to_string(n) + " streets of Amsterdam").c_str(), STREETS, town(rng, STREETS, n, 523700000, 48900000, 0));
        row((std::to_string(n) + " interpolations of Sydney").c_str(), INTERPS, town(rng, INTERPS, n, -338700000, 1512100000, 0));
    }
    row("33 streets where the equator meets the prime meridian, floats between the decimals among them", STREETS,
        town(rng, STREETS, 33, 1000000, -2000000, 4));
    row("20 interpolations there", INTERPS, town(rng, INTERPS, 20, -3000000, 500000, 4));
    {
        // Sixteen streets without nodes between two groups with.
        std::vector<Way> ways = town(rng, STREETS, 16, 407100000, -740100000, 0);
        for (int i = 0; i < 16; i++) ways.push_back(street(NO_DATA, {}));
        const std::vector<Way> more = town(rng, STREETS, 5, 407100000, -740100000, 1);
        ways.insert(ways.end(), more.begin(), more.end());
        row("a group without nodes between two with", STREETS, ways);
    }
    return 0;
}
