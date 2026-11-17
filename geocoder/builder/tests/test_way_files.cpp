// The way files (way_files.h): the encoder's files decode to the logical pair
// it took, a pair has one encoding, and the pairs and files the format can't
// hold are refused.
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "coord_keys.h"
#include "logical_decode.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "types.h"
#include "way_files.h"

using gctest::refusal_of;
using way_files::Footers;
using way_files::INTERPS;
using way_files::Kind;
using way_files::Row;
using way_files::STREETS;

namespace {

constexpr size_t G = way_files::GROUP_WAYS;

// A way of a logical pair: its nodes are lat bits and lng bits, a pair a node.
struct Way {
    uint32_t name = 0, start = 0, end = 0;
    uint8_t interpolation = 0;
    std::vector<uint32_t> nodes;
    size_t count() const { return nodes.size() / 2; }
};

// The logical ways file and nodes file.
struct Pair {
    std::vector<uint8_t> ways, nodes;
    bool operator==(const Pair& o) const { return ways == o.ways && nodes == o.nodes; }
};

// The rows file and the nodes file.
struct Files {
    std::vector<uint8_t> rows, nodes;
    bool operator==(const Files& o) const { return rows == o.rows && nodes == o.nodes; }
};

auto into(std::vector<uint8_t>& v) {
    return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); };
}

void append32(std::vector<uint8_t>& v, uint32_t x) {
    uint8_t b[4];
    way_files::put32(b, x);
    v.insert(v.end(), b, b + 4);
}

// The bytes of a vector, never null: an empty file is a file.
const uint8_t* bytes_of(const std::vector<uint8_t>& v) {
    static const uint8_t none = 0;
    return v.empty() ? &none : v.data();
}

// A way's logical record, with the node_offset given.
std::vector<uint8_t> record(const Kind& kind, const Way& w, uint32_t node_offset) {
    std::vector<uint8_t> r(kind.stride, 0);
    way_files::put32(r.data(), node_offset);
    way_files::put16(r.data() + 4, uint16_t(w.count()));
    way_files::put32(r.data() + 8, w.name);
    if (kind.interp) {
        way_files::put32(r.data() + 12, w.start);
        way_files::put32(r.data() + 16, w.end);
        r[20] = w.interpolation;
    }
    return r;
}

// The logical pair of the ways, each record's node_offset the nodes before
// it, 0 of a way without nodes.
Pair logical(const Kind& kind, const std::vector<Way>& ways) {
    Pair p;
    uint64_t node = 0;
    for (const Way& w : ways) {
        const std::vector<uint8_t> r = record(kind, w, w.count() ? uint32_t(node) : 0);
        p.ways.insert(p.ways.end(), r.begin(), r.end());
        for (uint32_t x : w.nodes) append32(p.nodes, x);
        node += w.count();
    }
    return p;
}

// The pair through the encoder, footers and all. Each way's nodes are taken
// by its count, whatever its node_offset says.
Files encode(const Kind& kind, const Pair& p) {
    Files f;
    way_files::Encoder enc(kind, into(f.rows), into(f.nodes));
    size_t node = 0;
    for (size_t at = 0; at < p.ways.size(); at += kind.stride) {
        enc.add(p.ways.data() + at, bytes_of(p.nodes) + node);
        node += way_files::NODE_BYTES * way_files::get16(p.ways.data() + at + 4);
    }
    enc.finish();
    const auto ways_footer = enc.ways_footer();
    const auto nodes_footer = enc.nodes_footer();
    f.rows.insert(f.rows.end(), ways_footer.begin(), ways_footer.end());
    f.nodes.insert(f.nodes.end(), nodes_footer.begin(), nodes_footer.end());
    return f;
}

Pair decode(const Kind& kind, const Files& f) {
    Pair p;
    way_files::decode(
        kind, bytes_of(f.rows), f.rows.size(), bytes_of(f.nodes), f.nodes.size(),
        [&](const uint8_t* r) { p.ways.insert(p.ways.end(), r, r + kind.stride); },
        [&](const uint8_t* n, size_t bytes) { p.nodes.insert(p.nodes.end(), n, n + bytes); });
    return p;
}

Footers footers(const Kind& kind, const Files& f) {
    return way_files::read_footers(kind, bytes_of(f.rows) + f.rows.size() - std::min(f.rows.size(), way_files::WAYS_FOOTER_BYTES),
                                   f.rows.size(), bytes_of(f.nodes) + f.nodes.size() - std::min(f.nodes.size(), way_files::NODES_FOOTER_BYTES),
                                   f.nodes.size());
}

// The rows of the files, each as decode_row reads it.
std::vector<Row> rows_of(const Kind& kind, const Files& f) {
    const Footers ft = footers(kind, f);
    std::vector<Row> rows;
    size_t at = 0;
    for (uint64_t g = 0; g < ft.groups(); g++) {
        rows.push_back(way_files::decode_row(kind, f.rows.data() + at, ft.group_ways(g)));
        at += kind.row_bytes(ft.group_ways(g));
    }
    return rows;
}

// The pair of the files, read a unit at a time as a stream holds them: a
// row, then each block copied out with 8 bytes after it.
Pair walk(const Kind& kind, const Files& f) {
    Pair p;
    uint64_t at = 0, node = 0;
    std::vector<uint8_t> block, out;
    for (const Row& r : rows_of(kind, f)) {
        CHECK_EQ(uint64_t(r.off), at);
        for (size_t j = 0; j < r.ways; j++) {
            uint8_t rec[way_files::MAX_STRIDE];
            way_files::logical_record(kind, r, j, node, rec);
            p.ways.insert(p.ways.end(), rec, rec + kind.stride);
            if (!r.cnt[j]) continue;
            CHECK_EQ(r.off + r.bytes_before(j), at);
            block.assign(f.nodes.begin() + at, f.nodes.begin() + at + r.block_bytes(j));
            block.resize(block.size() + 8, 0xFF);
            out.resize(way_files::NODE_BYTES * r.cnt[j]);
            way_files::decode_block(block.data(), r.cnt[j], r.code[j], r.anchor_lat, r.anchor_lng, out.data());
            p.nodes.insert(p.nodes.end(), out.begin(), out.end());
            at += r.block_bytes(j);
            node += r.cnt[j];
        }
    }
    CHECK_EQ(at + way_files::NODES_FOOTER_BYTES, uint64_t(f.nodes.size()));
    return p;
}

uint32_t bits(float f) { return coord_keys::bits_of(f); }

// The bits of a coordinate of seven decimals, which has a hybrid key.
uint32_t e7(int64_t v) { return bits(float(double(v) / 1e7)); }

enum Shape { SEVEN_DECIMALS, NEAR_ZERO, ANY_BITS, ONE_NODE_OVER };

// The nodes of a way of n nodes: a walk over coordinates of seven decimals
// (each has a hybrid key), floats near the equator and the prime meridian
// (few have one), any bit patterns, or one node over and over.
std::vector<uint32_t> random_nodes(std::mt19937_64& rng, size_t n, Shape shape) {
    std::vector<uint32_t> nodes;
    // One draw a statement: the same ways whatever order a compiler evaluates operands in.
    int64_t lat = int64_t(rng() % 1700000001) - 850000000;
    int64_t lng = int64_t(rng() % 3600000001) - 1800000000;
    int64_t step_lat = int64_t(rng() % 4001) - 2000;
    int64_t step_lng = int64_t(rng() % 4001) - 2000;
    const uint32_t zero_lat = bits(0.3f) + uint32_t(rng() % 100000);
    const uint32_t zero_lng = (bits(0.6f) + uint32_t(rng() % 100000)) | 0x80000000u;
    for (size_t j = 0; j < n; j++) {
        if (shape == SEVEN_DECIMALS) {
            nodes.push_back(e7(lat));
            nodes.push_back(e7(lng));
            step_lat += int64_t(rng() % 41) - 20;
            step_lng += int64_t(rng() % 41) - 20;
            lat += step_lat;
            lng += step_lng;
        } else if (shape == NEAR_ZERO) {
            const uint32_t dl = uint32_t(rng() % 5000);
            const uint32_t dg = uint32_t(rng() % 5000);
            nodes.push_back(zero_lat + uint32_t(j) * 3000 + dl);
            nodes.push_back(zero_lng - uint32_t(j) * 3000 - dg);
        } else if (shape == ANY_BITS) {
            const uint32_t l = uint32_t(rng());
            const uint32_t g = uint32_t(rng());
            nodes.push_back(l);
            nodes.push_back(g);
        } else {
            nodes.push_back(e7(lat));
            nodes.push_back(e7(lng));
        }
    }
    return nodes;
}

// n ways of the kind: every eighth without nodes, some of one node and of
// two, names of every width, an interpolation's numbers either way round.
std::vector<Way> random_ways(std::mt19937_64& rng, const Kind& kind, size_t n) {
    std::vector<Way> ways;
    for (size_t i = 0; i < n; i++) {
        Way w;
        const uint64_t pick = rng() % 16;
        const size_t more = 3 + size_t(rng() % 40);
        const size_t count = pick < 2 ? 0 : pick == 2 ? 1 : pick == 3 ? 2 : more;
        const uint64_t shape = rng() % 8;
        w.nodes = random_nodes(rng, count, shape < 5 ? SEVEN_DECIMALS : shape == 5 ? NEAR_ZERO : shape == 6 ? ANY_BITS : ONE_NODE_OVER);
        const uint64_t shift = 32 + rng() % 32;
        w.name = count ? uint32_t(rng() >> shift) : NO_DATA;
        if (kind.interp) {
            w.start = uint32_t(rng() % 3000);
            w.end = uint32_t(rng() % 3000);
            w.interpolation = uint8_t(rng() % 4);
        }
        ways.push_back(w);
    }
    return ways;
}

std::string refusal(const Kind& kind, const Pair& p) {
    return refusal_of([&] { encode(kind, p); });
}

std::string refusal(const Kind& kind, const Files& f) {
    return refusal_of([&] { decode(kind, f); });
}

bool says(const std::string& got, const std::string& want) {
    const bool ok = got.find(want) != std::string::npos;
    if (!ok) std::printf("    want %s, got %s\n", want.c_str(), got.c_str());
    return ok;
}

// ---- files made by hand, with blocks no encoder writes among them.

uint16_t code_of(uint32_t wl, uint32_t wg, bool hybrid, uint32_t wf) { return uint16_t(wl | wg << 6 | uint32_t(hybrid) << 12 | wf << 13); }

// A block: the first node's two zigzags in wf bytes each, then the lat
// values at wl bits and the lng values at wg bits, the bits of the last byte
// past them as given.
std::vector<uint8_t> raw_block(uint32_t zl, uint32_t zg, uint32_t wf, const std::vector<uint32_t>& lat, uint32_t wl,
                               const std::vector<uint32_t>& lng, uint32_t wg, uint8_t padding = 0) {
    const uint64_t used = lat.size() * wl + lng.size() * wg;
    std::vector<uint8_t> b(2 * wf + (used + 7) / 8 + 8, 0);
    for (uint32_t k = 0; k < wf; k++) {
        b[k] = uint8_t(zl >> (8 * k));
        b[wf + k] = uint8_t(zg >> (8 * k));
    }
    uint64_t bit = 0;
    for (uint32_t v : lat) {
        coord_keys::put_bits(b.data() + 2 * wf, bit, v, wl);
        bit += wl;
    }
    for (uint32_t v : lng) {
        coord_keys::put_bits(b.data() + 2 * wf, bit, v, wg);
        bit += wg;
    }
    b.resize(b.size() - 8);
    if (used % 8) b.back() |= uint8_t(padding << (used % 8));
    return b;
}

struct RawWay {
    uint16_t cnt = 0, code = 0;
    std::vector<uint8_t> block;
};

// The files of one group of streets: the anchor, each way's count, code and
// block as given, the footers the counts'.
Files raw_files(uint32_t anchor_lat, uint32_t anchor_lng, const std::vector<RawWay>& ways) {
    Files f;
    Row r;
    r.anchor_lat = anchor_lat;
    r.anchor_lng = anchor_lng;
    uint64_t nodes = 0;
    for (const RawWay& w : ways) {
        r.cnt[r.ways] = w.cnt;
        r.code[r.ways] = w.code;
        r.name[r.ways] = uint32_t(100 + r.ways);
        r.ways++;
        f.nodes.insert(f.nodes.end(), w.block.begin(), w.block.end());
        nodes += w.cnt;
    }
    f.rows.resize(STREETS.row_bytes(r.ways));
    way_files::encode_row(STREETS, r, f.rows.data());
    append32(f.rows, uint32_t(ways.size()));
    f.rows.insert(f.rows.end(), STREETS.ways_magic, STREETS.ways_magic + 4);
    append32(f.nodes, uint32_t(nodes));
    append32(f.nodes, uint32_t(nodes >> 32));
    append32(f.nodes, uint32_t(ways.size()));
    f.nodes.insert(f.nodes.end(), STREETS.nodes_magic, STREETS.nodes_magic + 4);
    return f;
}

uint32_t zz(int32_t d) { return coord_keys::zigzag32(uint32_t(d)); }

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    size_t a = 0;
    for (size_t b; (b = s.find(sep, a)) != std::string::npos; a = b + 1) out.push_back(s.substr(a, b - a));
    out.push_back(s.substr(a));
    return out;
}

std::vector<uint8_t> unhex(const std::string& hex) {
    std::vector<uint8_t> out;
    if (hex == "-") return out;
    for (size_t i = 0; i + 1 < hex.size(); i += 2) out.push_back(uint8_t(std::stoul(hex.substr(i, 2), nullptr, 16)));
    return out;
}

}  // namespace

TEST(way_files_decode_to_the_pair_the_encoder_took) {
    std::mt19937_64 rng(20261114);
    size_t hybrid = 0, order = 0, bare = 0, single = 0;
    for (const Kind* kind : {&STREETS, &INTERPS})
        for (size_t n : {size_t(0), size_t(1), size_t(2), size_t(15), size_t(16), size_t(17), size_t(32), size_t(33), size_t(1000)}) {
            const Pair p = logical(*kind, random_ways(rng, *kind, n));
            const Files f = encode(*kind, p);
            CHECK(decode(*kind, f) == p);
            CHECK(encode(*kind, decode(*kind, f)) == f);
            CHECK(walk(*kind, f) == p);
            // A row a group of 16, the last shorter, and the footers' counts.
            CHECK_EQ(f.rows.size(), 12 * ((n + 15) / 16) + (kind->interp ? 17 : 8) * n + 8);
            const Footers ft = footers(*kind, f);
            CHECK_EQ(size_t(ft.ways), n);
            CHECK_EQ(ft.nodes * 8, uint64_t(p.nodes.size()));
            CHECK_EQ(ft.rows_bytes + 8, uint64_t(f.rows.size()));
            CHECK_EQ(ft.blocks_bytes + 16, uint64_t(f.nodes.size()));
            CHECK_EQ(way_files::logical_sizes(*kind, ft).ways, uint64_t(p.ways.size()));
            CHECK_EQ(way_files::logical_sizes(*kind, ft).nodes, uint64_t(p.nodes.size()));
            for (const Row& r : rows_of(*kind, f))
                for (size_t j = 0; j < r.ways; j++) {
                    bare += r.cnt[j] == 0;
                    single += r.cnt[j] == 1;
                    hybrid += r.cnt[j] && way_files::is_hybrid(r.code[j]);
                    order += r.cnt[j] && !way_files::is_hybrid(r.code[j]);
                }
        }
    CHECK(hybrid > 500 && order > 200 && bare > 100 && single > 50);
}

TEST(ways_of_no_one_two_and_the_most_nodes_round_trip) {
    std::mt19937_64 rng(3);
    for (const Kind* kind : {&STREETS, &INTERPS}) {
        std::vector<Way> ways(8);
        const Shape shapes[] = {SEVEN_DECIMALS, SEVEN_DECIMALS, NEAR_ZERO, SEVEN_DECIMALS, ANY_BITS, NEAR_ZERO, ONE_NODE_OVER, ANY_BITS};
        const size_t counts[] = {0, 1, 2, 65535, 65535, 65535, 65535, 1};
        for (size_t i = 0; i < ways.size(); i++) {
            ways[i].nodes = random_nodes(rng, counts[i], shapes[i]);
            ways[i].name = uint32_t(i);
            ways[i].end = uint32_t(7 * i);
        }
        const Pair p = logical(*kind, ways);
        const Files f = encode(*kind, p);
        CHECK(decode(*kind, f) == p);
        CHECK(walk(*kind, f) == p);
        const std::vector<Row> rows = rows_of(*kind, f);
        REQUIRE(rows.size() == 1 && rows[0].ways == 8);
        const Row& r = rows[0];
        CHECK_EQ(r.cnt[0], 0);
        CHECK_EQ(r.code[0], 0);
        // The group's anchor is its first node: the way of one node has no bytes.
        CHECK_EQ(r.anchor_lat, ways[1].nodes[0]);
        CHECK_EQ(r.anchor_lng, ways[1].nodes[1]);
        CHECK_EQ(r.block_bytes(1), size_t(0));
        CHECK(way_files::is_hybrid(r.code[3]) && !way_files::is_hybrid(r.code[4]) && !way_files::is_hybrid(r.code[5]));
        // Any bit patterns fill the widths: the largest block a way has.
        CHECK_EQ(way_files::lat_bits(r.code[4]), 32u);
        CHECK_EQ(way_files::lng_bits(r.code[4]), 32u);
        CHECK_EQ(way_files::first_bytes(r.code[4]), 4u);
        CHECK_EQ(r.block_bytes(4), size_t(8 + 65534 * 8));
        // One node over and over is its first node alone.
        CHECK_EQ(r.block_bytes(6), size_t(2 * way_files::first_bytes(r.code[6])));
        CHECK_EQ(r.nodes(), uint64_t(4 * 65535 + 4));
    }
}

TEST(a_group_without_nodes_starts_where_the_next_block_does_and_has_no_anchor) {
    std::mt19937_64 rng(4);
    for (const Kind* kind : {&STREETS, &INTERPS}) {
        // A group with nodes, one without, and a short one whose first way has none.
        std::vector<Way> ways(35);
        for (size_t i = 0; i < 16; i++) ways[i].nodes = random_nodes(rng, 3 + i, SEVEN_DECIMALS);
        for (size_t i = 16; i < 33; i++) ways[i].name = NO_DATA;
        ways[33].nodes = random_nodes(rng, 4, SEVEN_DECIMALS);
        ways[34].nodes = random_nodes(rng, 2, NEAR_ZERO);
        const Pair p = logical(*kind, ways);
        const Files f = encode(*kind, p);
        CHECK(decode(*kind, f) == p);
        CHECK_EQ(f.rows.size(), 2 * kind->row_bytes(16) + kind->row_bytes(3) + 8);
        const std::vector<Row> rows = rows_of(*kind, f);
        REQUIRE(rows.size() == 3);
        CHECK_EQ(rows[0].off, 0u);
        CHECK_EQ(rows[0].anchor_lat, ways[0].nodes[0]);
        CHECK_EQ(rows[0].anchor_lng, ways[0].nodes[1]);
        CHECK(rows[0].blocks_bytes() > 0);
        CHECK_EQ(uint64_t(rows[1].off), rows[0].blocks_bytes());
        CHECK_EQ(rows[1].anchor_lat, 0u);
        CHECK_EQ(rows[1].anchor_lng, 0u);
        CHECK_EQ(rows[1].blocks_bytes(), uint64_t(0));
        CHECK_EQ(rows[2].off, rows[1].off);
        CHECK_EQ(rows[2].ways, size_t(3));
        CHECK_EQ(rows[2].anchor_lat, ways[33].nodes[0]);
        CHECK_EQ(rows[2].anchor_lng, ways[33].nodes[1]);
        CHECK_EQ(rows[2].off + rows[2].blocks_bytes() + 16, uint64_t(f.nodes.size()));
        // A way's record has the nodes before it, a way without nodes 0.
        CHECK_EQ(way_files::get32(p.ways.data() + 33 * kind->stride), uint32_t(rows[0].nodes()));
        CHECK_EQ(way_files::get32(p.ways.data() + 20 * kind->stride), 0u);
    }
}

TEST(an_interpolations_numbers_and_a_streets_name_come_back_whatever_they_are) {
    Way a, b, c;
    a.nodes = {bits(52.5f), bits(4.5f), bits(52.6f), bits(4.6f)};
    a.name = 0;
    a.start = 10;
    a.end = 2;  // the span wraps
    a.interpolation = 255;
    b.name = NO_DATA;
    b.start = NO_DATA;
    b.end = 0;
    c.nodes = {bits(-0.0f), bits(0.0f)};
    c.name = 0xFFFFFFFEu;
    c.start = 0;
    c.end = NO_DATA;
    c.interpolation = 1;
    for (const Kind* kind : {&STREETS, &INTERPS}) {
        const Pair p = logical(*kind, {a, b, c});
        const Files f = encode(*kind, p);
        CHECK(decode(*kind, f) == p);
        const Row r = rows_of(*kind, f).at(0);
        CHECK_EQ(r.name[1], NO_DATA);
        CHECK_EQ(r.name[2], 0xFFFFFFFEu);
        if (kind->interp) {
            CHECK_EQ(r.span[0], uint32_t(-8));
            CHECK_EQ(r.span[1], 1u);
            CHECK_EQ(r.interpolation[0], 255);
        }
    }
}

TEST(no_ways_are_the_two_footers) {
    for (const Kind* kind : {&STREETS, &INTERPS}) {
        const Files f = encode(*kind, Pair{});
        std::vector<uint8_t> rows(4, 0), nodes(12, 0);
        rows.insert(rows.end(), kind->ways_magic, kind->ways_magic + 4);
        nodes.insert(nodes.end(), kind->nodes_magic, kind->nodes_magic + 4);
        CHECK(f.rows == rows);
        CHECK(f.nodes == nodes);
        CHECK(decode(*kind, f) == Pair{});
        const Footers ft = footers(*kind, f);
        CHECK_EQ(ft.ways, 0u);
        CHECK_EQ(ft.groups(), uint64_t(0));
        CHECK_EQ(way_files::logical_sizes(*kind, ft).ways + way_files::logical_sizes(*kind, ft).nodes, uint64_t(0));
    }
    CHECK_EQ(std::string(STREETS.ways_magic, 4), std::string("SWY1"));
    CHECK_EQ(std::string(STREETS.nodes_magic, 4), std::string("SWN1"));
    CHECK_EQ(std::string(INTERPS.ways_magic, 4), std::string("IWY1"));
    CHECK_EQ(std::string(INTERPS.nodes_magic, 4), std::string("IWN1"));
    CHECK_EQ(std::string(STREETS.ways_file), std::string("street_ways.bin"));
    CHECK_EQ(std::string(STREETS.nodes_file), std::string("street_nodes.bin"));
    CHECK_EQ(std::string(INTERPS.ways_file), std::string("interp_ways.bin"));
    CHECK_EQ(std::string(INTERPS.nodes_file), std::string("interp_nodes.bin"));
}

TEST(a_dir_without_either_file_holds_no_ways_and_one_with_one_of_them_is_refused) {
    for (const Kind* kind : {&STREETS, &INTERPS}) {
        const Files f = encode(*kind, logical(*kind, {Way{}}));
        const uint8_t* rows_end = f.rows.data() + f.rows.size() - 8;
        const uint8_t* nodes_end = f.nodes.data() + f.nodes.size() - 16;
        const Footers none = way_files::read_footers(*kind, nullptr, way_files::ABSENT, nullptr, way_files::ABSENT);
        CHECK_EQ(none.ways, 0u);
        CHECK_EQ(none.nodes + none.rows_bytes + none.blocks_bytes, uint64_t(0));
        CHECK_EQ(way_files::logical_sizes(*kind, none).ways + way_files::logical_sizes(*kind, none).nodes, uint64_t(0));
        size_t put = 0;
        way_files::decode(
            *kind, nullptr, way_files::ABSENT, nullptr, way_files::ABSENT, [&](const uint8_t*) { put++; },
            [&](const uint8_t*, size_t) { put++; });
        CHECK_EQ(put, size_t(0));
        const std::string ways = kind->ways_file, nodes = kind->nodes_file;
        CHECK(says(refusal_of([&] { way_files::read_footers(*kind, rows_end, f.rows.size(), nullptr, way_files::ABSENT); }),
                   ways + " without " + nodes));
        CHECK(says(refusal_of([&] { way_files::read_footers(*kind, nullptr, way_files::ABSENT, nodes_end, f.nodes.size()); }),
                   nodes + " without " + ways));
        // A file of no bytes is a file, and no footer.
        CHECK(says(refusal_of([&] { way_files::read_footers(*kind, nullptr, 0, nullptr, 0); }), ways + " has no"));
        CHECK(says(refusal(*kind, Files{}), ways + " has no"));
        CHECK(says(refusal(*kind, Files{f.rows, {}}), nodes + " has no"));
    }
}

TEST(the_footers_alone_refuse_a_pair_whose_sizes_and_counts_disagree) {
    std::mt19937_64 rng(5);
    const Pair p = logical(STREETS, random_ways(rng, STREETS, 40));
    const Files f = encode(STREETS, p);
    CHECK_EQ(refusal(STREETS, f), std::string());
    auto edited = [&](auto edit) {
        Files g = f;
        edit(g);
        return refusal_of([&] { footers(STREETS, g); });
    };
    CHECK(says(edited([](Files& g) { g.rows.back() = '2'; }), "street_ways.bin has no SWY1 footer"));
    CHECK(says(edited([](Files& g) { g.nodes.back() = '2'; }), "street_nodes.bin has no SWN1 footer"));
    CHECK(says(edited([](Files& g) { g.rows.resize(7); }), "street_ways.bin has no SWY1 footer"));
    CHECK(says(edited([](Files& g) { g.nodes.resize(15); }), "street_nodes.bin has no SWN1 footer"));
    // The interpolations' magics are not the streets'.
    CHECK(says(refusal_of([&] { footers(INTERPS, f); }), "interp_ways.bin has no IWY1 footer"));
    CHECK(says(edited([](Files& g) { g.rows[g.rows.size() - 8]++; }), "street_ways.bin and street_nodes.bin count different ways"));
    CHECK(says(edited([](Files& g) { g.nodes[g.nodes.size() - 8]++; }), "street_ways.bin and street_nodes.bin count different ways"));
    CHECK(says(edited([](Files& g) {
                   g.rows[g.rows.size() - 8]++;
                   g.nodes[g.nodes.size() - 8]++;
               }),
               "street_ways.bin is not the rows of its 41 ways"));
    CHECK(says(edited([](Files& g) { g.rows.erase(g.rows.begin()); }), "street_ways.bin is not the rows of its 40 ways"));
    CHECK(says(edited([](Files& g) { g.rows.insert(g.rows.begin(), 0); }), "street_ways.bin is not the rows of its 40 ways"));
    // 40 ways have at most 40 x 65,535 nodes, and a node at most 8 bytes of block.
    CHECK(says(edited([](Files& g) { way_files::put64(g.nodes.data() + g.nodes.size() - 16, 40 * 65535 + 1); }),
               "street_nodes.bin counts more nodes than its ways can have"));
    CHECK_EQ(edited([](Files& g) { way_files::put64(g.nodes.data() + g.nodes.size() - 16, 40 * 65535); }), std::string());
    CHECK(says(edited([](Files& g) { way_files::put64(g.nodes.data() + g.nodes.size() - 16, (g.nodes.size() - 16) / 8 - 1); }),
               "street_nodes.bin is longer than its nodes can fill"));

    // The sizes alone: the footers of one way, with files of any size.
    uint8_t ways_end[8], nodes_end[16];
    way_files::put32(ways_end, 1);
    std::memcpy(ways_end + 4, "SWY1", 4);
    way_files::put64(nodes_end, 1);
    way_files::put32(nodes_end + 8, 1);
    std::memcpy(nodes_end + 12, "SWN1", 4);
    CHECK_EQ(refusal_of([&] { way_files::read_footers(STREETS, ways_end, 28, nodes_end, 16); }), std::string());
    CHECK(says(refusal_of([&] { way_files::read_footers(STREETS, ways_end, 28, nodes_end, uint64_t(1) << 32); }),
               "street_nodes.bin is 4 GiB or more"));
    CHECK(says(refusal_of([&] { way_files::read_footers(STREETS, ways_end, 28, nodes_end, 25); }),
               "street_nodes.bin is longer than its nodes can fill"));
    // 2^32 - 1 ways: NO_DATA would be the last one's id.
    way_files::put32(ways_end, UINT32_MAX);
    way_files::put32(nodes_end + 8, UINT32_MAX);
    const uint64_t rows = 12 * ((uint64_t(UINT32_MAX) + 15) / 16) + 8 * uint64_t(UINT32_MAX) + 8;
    CHECK(says(refusal_of([&] { way_files::read_footers(STREETS, ways_end, rows, nodes_end, 16); }), "street_ways.bin holds 2^32 - 1 ways or more"));
    way_files::put32(ways_end, UINT32_MAX - 1);
    way_files::put32(nodes_end + 8, UINT32_MAX - 1);
    CHECK_EQ(refusal_of([&] { way_files::read_footers(STREETS, ways_end, rows - 8, nodes_end, 24); }), std::string());
}

TEST(the_encoder_refuses_a_pair_the_files_cant_hold) {
    const std::vector<uint32_t> two = {bits(52.5f), bits(4.5f), bits(52.6f), bits(4.6f)};
    Way with, without;
    with.nodes = two;
    with.name = 7;
    // The records of ways {with, without, with}, the last one's edited.
    auto edited = [&](const Kind& kind, auto edit) {
        Pair p = logical(kind, {with, without, with});
        edit(p.ways.data() + 2 * kind.stride, p.ways.data() + kind.stride);
        return refusal(kind, p);
    };
    for (const Kind* kind : {&STREETS, &INTERPS}) {
        const std::string file = kind->ways_file;
        CHECK_EQ(edited(*kind, [](uint8_t*, uint8_t*) {}), std::string());
        CHECK(says(edited(*kind, [](uint8_t* last, uint8_t*) { way_files::put32(last, 3); }),
                   file + " way 2 has a node_offset of 3, not the 2 nodes before it"));
        CHECK(says(edited(*kind, [](uint8_t* last, uint8_t*) { way_files::put32(last, 0); }),
                   file + " way 2 has a node_offset of 0, not the 2 nodes before it"));
        CHECK(says(edited(*kind, [](uint8_t*, uint8_t* bare) { way_files::put32(bare, 2); }), file + " way 1 has a node_offset of 2, not 0"));
        CHECK(says(edited(*kind, [](uint8_t* last, uint8_t*) { last[6] = 1; }), file + " way 2 has its padding set"));
        CHECK(says(edited(*kind, [](uint8_t* last, uint8_t*) { last[7] = 0x80; }), file + " way 2 has its padding set"));
    }
    for (size_t pad : {size_t(21), size_t(22), size_t(23)})
        CHECK(says(edited(INTERPS, [&](uint8_t*, uint8_t* bare) { bare[pad] = 1; }), "interp_ways.bin way 1 has its padding set"));

    std::vector<uint8_t> rows, blocks;
    // A way's node_offset is a u32: one with nodes can follow 2^32 - 1 nodes, and one without any number.
    {
        way_files::Encoder enc(STREETS, into(rows), into(blocks), 0, uint64_t(UINT32_MAX));
        enc.add(record(STREETS, with, UINT32_MAX).data(), reinterpret_cast<const uint8_t*>(two.data()));
        CHECK_EQ(enc.nodes(), uint64_t(UINT32_MAX) + 2);
        enc.add(record(STREETS, without, 0).data(), nullptr);
        CHECK(says(refusal_of([&] { enc.add(record(STREETS, with, 1).data(), reinterpret_cast<const uint8_t*>(two.data())); }),
                   "street_ways.bin way 2 has nodes after 2^32 nodes or more"));
    }
    // 2^32 - 2 ways at most.
    {
        way_files::Encoder enc(INTERPS, into(rows), into(blocks), 0xFFFFFFF0u, 0);
        for (int k = 0; k < 14; k++) enc.add(record(INTERPS, without, 0).data(), nullptr);
        CHECK_EQ(enc.ways(), uint64_t(UINT32_MAX) - 1);
        CHECK(says(refusal_of([&] { enc.add(record(INTERPS, without, 0).data(), nullptr); }), "interp_ways.bin would hold 2^32 - 1 ways or more"));
        CHECK_EQ(refusal_of([&] { enc.ways_footer(); }), std::string());
    }
    CHECK(says(refusal_of([&] { way_files::ways_footer(STREETS, UINT32_MAX); }), "street_ways.bin would hold 2^32 - 1 ways or more"));
    CHECK(says(refusal_of([&] { way_files::nodes_footer(STREETS, 0, UINT32_MAX, 0); }), "street_ways.bin would hold 2^32 - 1 ways or more"));
    // A nodes file below 4 GiB, its footer counted.
    CHECK_EQ(refusal_of([&] { way_files::nodes_footer(STREETS, 9, 9, uint64_t(UINT32_MAX) - 16); }), std::string());
    CHECK(says(refusal_of([&] { way_files::nodes_footer(STREETS, 9, 9, uint64_t(UINT32_MAX) - 15); }), "street_nodes.bin would be 4 GiB or more"));
    // An encoder does not start inside a group, and takes no way after a short one.
    CHECK(!refusal_of<std::logic_error>([&] { way_files::Encoder(STREETS, into(rows), into(blocks), 8, 0); }).empty());
    {
        way_files::Encoder enc(STREETS, into(rows), into(blocks));
        enc.add(record(STREETS, without, 0).data(), nullptr);
        enc.finish();
        CHECK(!refusal_of<std::logic_error>([&] { enc.add(record(STREETS, without, 0).data(), nullptr); }).empty());
    }
}

TEST(the_encoder_refuses_a_nodes_file_of_4_gib_as_its_blocks_come) {
    // Groups of the largest blocks, taken as coded bytes: 65,535 nodes a way at 32 bits a value.
    Row coded;
    coded.ways = G;
    Way most;
    most.nodes.resize(2 * 65535);
    std::vector<uint8_t> records;
    for (size_t j = 0; j < G; j++) {
        coded.cnt[j] = 65535;
        coded.code[j] = code_of(32, 32, false, j ? 4 : 0);
    }
    const uint64_t group = coded.blocks_bytes();
    CHECK_EQ(group, uint64_t(16 * 65534 * 8 + 15 * 8));
    const std::vector<uint8_t> bytes(group, 0);
    uint64_t rows = 0, blocks = 0;
    way_files::Encoder enc(
        STREETS, [&](const uint8_t*, size_t n) { rows += n; }, [&](const uint8_t*, size_t n) { blocks += n; });
    std::string said;
    size_t groups = 0;
    while (said.empty() && groups < 600) {
        records.clear();
        for (size_t j = 0; j < G; j++) {
            const std::vector<uint8_t> r = record(STREETS, most, uint32_t(enc.nodes() + j * 65535));
            records.insert(records.end(), r.begin(), r.end());
        }
        said = refusal_of([&] {
            enc.put_coded_group(coded, records.data());
            enc.put_coded_bytes(bytes.data(), bytes.size());
        });
        groups += said.empty();
    }
    CHECK(says(said, "street_nodes.bin would be 4 GiB or more"));
    CHECK_EQ(groups, size_t((uint64_t(UINT32_MAX) - 16) / group));
    CHECK_EQ(blocks, groups * group);
    CHECK(blocks + group + 16 > UINT32_MAX);
    CHECK_EQ(rows, groups * STREETS.row_bytes(G));
}

TEST(the_decoder_refuses_files_the_encoder_would_not_write) {
    // One group of streets around an anchor of two floats above 1.0, so every key near it is a hybrid key's.
    const uint32_t al = bits(52.0f), ag = bits(4.0f);
    // Three nodes: the anchor, then steps of +5 and +5 - 3 in lat, -3 and -3 + 1 in lng.
    const std::vector<uint32_t> lat = {zz(5), zz(-3)}, lng = {zz(-3), zz(1)};
    const RawWay first{3, code_of(4, 3, true, 0), raw_block(0, 0, 0, lat, 4, lng, 3)};
    // A second way that starts 3 floats up and 1 float left of the anchor.
    const RawWay second{2, code_of(1, 0, true, 1), raw_block(zz(3), zz(-1), 1, {zz(-1)}, 1, {0}, 0)};
    const RawWay bare{0, 0, {}};
    const Files good = raw_files(al, ag, {first, bare, second});
    CHECK_EQ(refusal(STREETS, good), std::string());
    CHECK(encode(STREETS, decode(STREETS, good)) == good);
    const Pair p = decode(STREETS, good);
    REQUIRE(p.nodes.size() == 5 * 8);
    CHECK_EQ(way_files::get32(p.nodes.data() + 8), al + 5);
    CHECK_EQ(way_files::get32(p.nodes.data() + 12), ag - 3);
    CHECK_EQ(way_files::get32(p.nodes.data() + 16), al + 7);
    CHECK_EQ(way_files::get32(p.nodes.data() + 20), ag - 5);
    CHECK_EQ(way_files::get32(p.nodes.data() + 24), al + 3);
    CHECK_EQ(way_files::get32(p.nodes.data() + 32), al + 2);
    CHECK_EQ(way_files::get32(p.nodes.data() + 36), ag - 1);

    struct Case {
        Files files;
        std::string refusal;
    };
    auto with_row = [&](auto edit) {
        Files f = good;
        Row r = rows_of(STREETS, good).at(0);
        edit(r);
        way_files::encode_row(STREETS, r, f.rows.data());
        return f;
    };
    auto with_files = [&](auto edit) {
        Files f = good;
        edit(f);
        return f;
    };
    const uint32_t no_key = bits(0.1f) + 1, last_key = 0x7FFFFFFFu;
    const Case cases[] = {
        {with_row([](Row& r) { r.off = 1; }), "street_ways.bin group 0 is not where the blocks before it end"},
        // The first way with nodes starts at the anchor, in no bytes.
        {raw_files(al, ag, {RawWay{3, code_of(4, 3, true, 1), raw_block(0, 0, 1, lat, 4, lng, 3)}}),
         "an anchor that is not the first node of its group's first way with nodes"},
        {raw_files(al, 0, {bare, bare}), "an anchor in a group without nodes"},
        {raw_files(0, ag, {bare}), "an anchor in a group without nodes"},
        {with_row([](Row& r) { r.code[1] = 1; }), "a way without nodes that has a code"},
        {with_row([](Row& r) { r.code[2] = code_of(33, 0, true, 1); }), "a code with a width out of range"},
        {with_row([](Row& r) { r.code[2] = code_of(1, 33, true, 1); }), "a code with a width out of range"},
        {with_row([](Row& r) { r.code[2] = code_of(1, 0, true, 5); }), "a code with a width out of range"},
        // Widths wider than the values.
        {raw_files(al, ag, {first, RawWay{2, code_of(1, 0, true, 2), raw_block(zz(3), zz(-1), 2, {zz(-1)}, 1, {0}, 0)}}),
         "a way's first node in more bytes than it needs"},
        {raw_files(al, ag, {RawWay{3, code_of(5, 3, true, 0), raw_block(0, 0, 0, lat, 5, lng, 3)}}),
         "a way's values in more bits than they need"},
        {raw_files(al, ag, {RawWay{3, code_of(4, 4, true, 0), raw_block(0, 0, 0, lat, 4, lng, 4)}}),
         "a way's values in more bits than they need"},
        {raw_files(al, ag, {RawWay{1, code_of(1, 0, true, 0), {}}}), "a way's values in more bits than they need"},
        // The key the flag names is not the one the encoder takes.
        {raw_files(no_key, ag, {first}), "a way under the hybrid key whose first node has none"},
        {raw_files(al, no_key, {RawWay{1, code_of(0, 0, true, 0), {}}}), "a way under the hybrid key whose first node has none"},
        {raw_files(last_key, ag, {first}), "a way under the hybrid key with a key no coordinate has"},
        {raw_files(al, ag, {RawWay{3, code_of(4, 3, false, 0), raw_block(0, 0, 0, lat, 4, lng, 3)}}),
         "a way under the order key whose coordinates all have hybrid keys"},
        {raw_files(al, ag, {RawWay{1, 0, {}}}), "a way under the order key whose coordinates all have hybrid keys"},
        // 2 x 4 + 2 x 3 bits leave two bits of the second byte.
        {raw_files(al, ag, {RawWay{3, code_of(4, 3, true, 0), raw_block(0, 0, 0, lat, 4, lng, 3, 1)}}),
         "bits set past a node block's last value"},
        {raw_files(al, ag, {RawWay{3, code_of(4, 3, true, 0), raw_block(0, 0, 0, lat, 4, lng, 3, 2)}}),
         "bits set past a node block's last value"},
        // The blocks and the footers.
        {with_files([](Files& f) { f.nodes.insert(f.nodes.end() - 16, uint8_t(0)); }),
         "street_nodes.bin has bytes between its last block and its footer"},
        {with_files([](Files& f) { f.nodes.erase(f.nodes.end() - 17); }), "street_nodes.bin ends inside the block of way 2"},
        {with_files([](Files& f) { f.nodes[f.nodes.size() - 16]++; }), "street_nodes.bin counts 6 nodes and its ways have 5"},
        {with_files([](Files& f) { f.nodes[f.nodes.size() - 16]--; }), "street_nodes.bin counts 4 nodes and its ways have 5"},
        {with_files([](Files& f) { f.rows[f.rows.size() - 8]--; }), "street_ways.bin and street_nodes.bin count different ways"},
    };
    for (const Case& c : cases) CHECK(says(refusal(STREETS, c.files), c.refusal));

    // A way with a coordinate that has no hybrid key is under the order key.
    const Files order = raw_files(no_key, ag, {RawWay{1, 0, {}}, RawWay{2, code_of(1, 0, false, 0), raw_block(0, 0, 0, {zz(-1)}, 1, {0}, 0)}});
    CHECK_EQ(refusal(STREETS, order), std::string());
    CHECK(encode(STREETS, decode(STREETS, order)) == order);

    // A way with nodes whose node_offset would not fit, as a unit decoder meets it.
    const Row r = rows_of(STREETS, good).at(0);
    uint8_t rec[way_files::MAX_STRIDE];
    CHECK(says(refusal_of([&] { way_files::logical_record(STREETS, r, 0, uint64_t(1) << 32, rec); }),
               "street_ways.bin has a way with nodes after 2^32 nodes or more"));
    way_files::logical_record(STREETS, r, 1, uint64_t(1) << 32, rec);
    CHECK_EQ(way_files::get32(rec), 0u);
    way_files::logical_record(STREETS, r, 2, UINT32_MAX, rec);
    CHECK_EQ(way_files::get32(rec), UINT32_MAX);
}

// Either file with a bit changed, or cut short, is refused or decodes to a
// pair whose encoding it is: no pair has two encodings.
TEST(a_logical_pair_has_one_encoding) {
    std::mt19937_64 rng(77);
    for (const Kind* kind : {&STREETS, &INTERPS}) {
        const Files f = encode(*kind, logical(*kind, random_ways(rng, *kind, 300)));
        size_t refused = 0, decoded = 0;
        for (int t = 0; t < 4000; t++) {
            Files g = f;
            // A cut goes to the rows and the nodes in turn, a flipped bit to either.
            std::vector<uint8_t>& file = (t % 50 == 0 ? t / 50 : t) % 2 ? g.rows : g.nodes;
            if (t % 50 == 0) {
                file.resize(rng() % file.size());
            } else {
                const size_t at = rng() % file.size();
                file[at] ^= uint8_t(1u << (rng() % 8));
            }
            // Only the decoder may refuse: a pair it hands back that the
            // encoder then refuses fails the test.
            Pair p;
            try {
                p = decode(*kind, g);
            } catch (const std::runtime_error&) {
                refused++;
                continue;
            }
            decoded++;
            CHECK(encode(*kind, p) == g);
        }
        CHECK(refused > 300 && decoded > 300);
    }
}

// A node block goes after what its vector holds, the same bytes and code as
// into an empty one.
TEST(a_node_block_is_appended_to_what_its_vector_holds) {
    std::mt19937_64 rng(20261115);
    const std::vector<uint32_t> way = random_nodes(rng, 9, SEVEN_DECIMALS);
    std::vector<uint8_t> nodes, alone, after = {1, 2, 3};
    for (uint32_t x : way) append32(nodes, x);
    const uint16_t code = way_files::encode_block(nodes.data(), 9, way[0], way[1], alone);
    CHECK_EQ(way_files::encode_block(nodes.data(), 9, way[0], way[1], after), code);
    CHECK_EQ(alone.size(), way_files::block_bytes(9, code));
    REQUIRE(after.size() == 3 + alone.size());
    CHECK(std::vector<uint8_t>(after.begin(), after.begin() + 3) == std::vector<uint8_t>({1, 2, 3}));
    CHECK(std::vector<uint8_t>(after.begin() + 3, after.end()) == alone);
}

TEST(a_group_put_as_coded_bytes_is_the_group_encoded) {
    std::mt19937_64 rng(6);
    for (const Kind* kind : {&STREETS, &INTERPS}) {
        // Yesterday's ways and today's: the same nodes, other names and numbers.
        const std::vector<Way> old_ways = random_ways(rng, *kind, 205);
        std::vector<Way> new_ways = old_ways;
        for (Way& w : new_ways) {
            w.name += 1000;
            w.start += 3;
            w.interpolation ^= 1;
        }
        const Pair before = logical(*kind, old_ways), after = logical(*kind, new_ways);
        const Files old_files = encode(*kind, before), want = encode(*kind, after);
        const std::vector<Row> rows = rows_of(*kind, old_files);
        REQUIRE(rows.size() == 13 && rows[12].ways == 13);
        Files got;
        way_files::Encoder enc(*kind, into(got.rows), into(got.nodes));
        size_t node = 0, coded = 0;
        for (size_t g = 0; g < rows.size(); g++) {
            const Row& r = rows[g];
            const uint8_t* records = after.ways.data() + g * G * kind->stride;
            // Two groups in three taken as bytes, the last, short one among them.
            if (g % 3 != 1) {
                enc.put_coded_group(r, records);
                // In pieces of any size, none at all among them.
                enc.put_coded_bytes(nullptr, 0);
                for (uint64_t at = 0; at < r.blocks_bytes(); at += 7)
                    enc.put_coded_bytes(old_files.nodes.data() + r.off + at, size_t(std::min<uint64_t>(7, r.blocks_bytes() - at)));
                coded++;
            } else {
                size_t in_group = node;
                for (size_t j = 0; j < r.ways; j++) {
                    enc.add(records + j * kind->stride, bytes_of(after.nodes) + in_group);
                    in_group += 8 * r.cnt[j];
                }
            }
            node += 8 * size_t(r.nodes());
        }
        enc.finish();
        const auto ways_footer = enc.ways_footer();
        const auto nodes_footer = enc.nodes_footer();
        got.rows.insert(got.rows.end(), ways_footer.begin(), ways_footer.end());
        got.nodes.insert(got.nodes.end(), nodes_footer.begin(), nodes_footer.end());
        CHECK(got == want);
        CHECK_EQ(coded, size_t(9));
        CHECK(decode(*kind, got) == after);
    }
}

TEST(a_coded_group_is_whole_at_a_group_edge_and_of_its_rows_counts) {
    std::mt19937_64 rng(8);
    const std::vector<Way> ways = random_ways(rng, STREETS, 2 * G);
    const Pair p = logical(STREETS, ways);
    const Files f = encode(STREETS, p);
    const std::vector<Row> rows = rows_of(STREETS, f);
    std::vector<uint8_t> out_rows, out_blocks;
    auto fresh = [&] { return way_files::Encoder(STREETS, into(out_rows), into(out_blocks)); };
    const uint8_t* blocks = f.nodes.data();
    {
        // Inside a group.
        auto enc = fresh();
        enc.add(p.ways.data(), bytes_of(p.nodes));
        CHECK(!refusal_of<std::logic_error>([&] { enc.put_coded_group(rows[0], p.ways.data()); }).empty());
    }
    {
        // Its blocks first: no way, no group and no end before they are all in, and no byte more.
        auto enc = fresh();
        enc.put_coded_group(rows[0], p.ways.data());
        REQUIRE(rows[0].blocks_bytes() > 1);
        enc.put_coded_bytes(blocks, size_t(rows[0].blocks_bytes() - 1));
        CHECK(!refusal_of<std::logic_error>([&] { enc.add(p.ways.data() + G * STREETS.stride, blocks); }).empty());
        CHECK(!refusal_of<std::logic_error>([&] { enc.put_coded_group(rows[1], p.ways.data() + G * STREETS.stride); }).empty());
        CHECK(!refusal_of<std::logic_error>([&] { enc.finish(); }).empty());
        CHECK(!refusal_of<std::logic_error>([&] { enc.put_coded_bytes(blocks, 2); }).empty());
        enc.put_coded_bytes(blocks + rows[0].blocks_bytes() - 1, 1);
        CHECK_EQ(refusal_of<std::logic_error>([&] { enc.put_coded_group(rows[1], p.ways.data() + G * STREETS.stride); }), std::string());
    }
    {
        // A record with another node count than the row's, or a node_offset that is not the nodes before it.
        auto enc = fresh();
        std::vector<uint8_t> records(p.ways.begin(), p.ways.begin() + G * STREETS.stride);
        size_t with_nodes = 0;
        while (!rows[0].cnt[with_nodes]) with_nodes++;
        way_files::put16(records.data() + with_nodes * STREETS.stride + 4, uint16_t(rows[0].cnt[with_nodes] + 1));
        CHECK(says(refusal_of([&] { enc.put_coded_group(rows[0], records.data()); }),
                   "street_ways.bin way " + std::to_string(with_nodes) + " has another node count than its coded group's row"));
        auto other = fresh();
        CHECK(says(refusal_of([&] { other.put_coded_group(rows[1], p.ways.data() + G * STREETS.stride); }), "has a node_offset of"));
    }
    {
        // A row no group has: no ways, or a first way with nodes that does not start at the anchor.
        auto enc = fresh();
        Row short_of_ways = rows[0];
        short_of_ways.ways = 0;
        CHECK(says(refusal_of([&] { enc.put_coded_group(short_of_ways, p.ways.data()); }), "a row of no ways"));
        Row coded = rows[0];
        size_t first = 0;
        while (!coded.cnt[first]) first++;
        coded.code[first] |= 1 << 13;
        CHECK(says(refusal_of([&] { enc.put_coded_group(coded, p.ways.data()); }), "an anchor that is not the first node"));
    }
    {
        // A short group is the last.
        auto enc = fresh();
        Row three = rows[0];
        three.ways = 3;
        size_t nodes = 0;
        for (size_t j = 0; j < 3; j++) nodes += three.cnt[j];
        if (!nodes) three.anchor_lat = three.anchor_lng = 0;
        enc.put_coded_group(three, p.ways.data());
        enc.put_coded_bytes(blocks, size_t(three.blocks_bytes()));
        CHECK(!refusal_of<std::logic_error>([&] { enc.add(p.ways.data() + 3 * STREETS.stride, blocks); }).empty());
        enc.finish();
        CHECK_EQ(enc.ways(), uint64_t(3));
    }
}

TEST(slices_encoded_apart_and_moved_on_are_the_pair_encoded_whole) {
    std::mt19937_64 rng(9);
    for (const Kind* kind : {&STREETS, &INTERPS}) {
        const Pair p = logical(*kind, random_ways(rng, *kind, 1000));
        const Files whole = encode(*kind, p);
        const size_t ways = p.ways.size() / kind->stride;
        for (size_t slice_groups : {size_t(1), size_t(3), size_t(40), size_t(63), size_t(100)}) {
            Files joined;
            uint64_t way = 0, node = 0;
            while (way < ways) {
                std::vector<uint8_t> rows, blocks;
                way_files::Encoder enc(*kind, into(rows), into(blocks), way, node);
                const uint64_t end = std::min<uint64_t>(ways, way + slice_groups * G);
                for (; way < end; way++) {
                    const uint8_t* r = p.ways.data() + way * kind->stride;
                    enc.add(r, bytes_of(p.nodes) + 8 * node);
                    node += way_files::get16(r + 4);
                }
                enc.finish();
                CHECK_EQ(enc.ways(), way);
                CHECK_EQ(enc.nodes(), node);
                CHECK_EQ(enc.blocks_bytes(), uint64_t(blocks.size()));
                way_files::rebase_rows(*kind, rows.data(), rows.size(), joined.nodes.size());
                joined.rows.insert(joined.rows.end(), rows.begin(), rows.end());
                joined.nodes.insert(joined.nodes.end(), blocks.begin(), blocks.end());
            }
            const auto ways_footer = way_files::ways_footer(*kind, way);
            const auto nodes_footer = way_files::nodes_footer(*kind, node, way, joined.nodes.size());
            joined.rows.insert(joined.rows.end(), ways_footer.begin(), ways_footer.end());
            joined.nodes.insert(joined.nodes.end(), nodes_footer.begin(), nodes_footer.end());
            CHECK(joined == whole);
        }
    }
    // Rows moved past what an off holds are refused.
    std::vector<uint8_t> row(STREETS.row_bytes(1), 0);
    way_files::put32(row.data(), 10);
    way_files::rebase_rows(STREETS, row.data(), row.size(), uint64_t(UINT32_MAX) - 16 - 10);
    CHECK_EQ(way_files::get32(row.data()), UINT32_MAX - 16);
    CHECK(says(refusal_of([&] { way_files::rebase_rows(STREETS, row.data(), row.size(), 1); }), "street_nodes.bin would be 4 GiB or more"));
}

TEST(way_files_are_the_shared_vectors_byte_for_byte) {
    // geocoder/testdata/way_files.tsv, which the server's reader and test encoder read too.
    std::ifstream in(std::string(GEOCODER_TESTDATA_DIR) + "/way_files.tsv");
    REQUIRE(in.good());
    size_t cases = 0, streets = 0, hybrid = 0, order = 0;
    bool header = true;
    for (std::string line; std::getline(in, line);) {
        if (line.empty() || line[0] == '#') continue;
        if (header) {
            header = false;
            continue;
        }
        const std::vector<std::string> field = split(line, '\t');
        REQUIRE(field.size() == 5);
        REQUIRE(field[1] == "street" || field[1] == "interp");
        const Kind& kind = field[1] == "street" ? STREETS : INTERPS;
        std::vector<Way> ways;
        if (field[2] != "-")
            for (const std::string& w : split(field[2], ';')) {
                const std::vector<std::string> part = split(w, ',');
                REQUIRE(part.size() == 5);
                Way way;
                way.name = uint32_t(std::stoul(part[0]));
                way.start = uint32_t(std::stoul(part[1]));
                way.end = uint32_t(std::stoul(part[2]));
                way.interpolation = uint8_t(std::stoul(part[3]));
                if (part[4] != "-")
                    for (const std::string& node : split(part[4], '|')) {
                        const std::vector<std::string> ll = split(node, ':');
                        REQUIRE(ll.size() == 2);
                        way.nodes.push_back(uint32_t(std::stoul(ll[0], nullptr, 16)));
                        way.nodes.push_back(uint32_t(std::stoul(ll[1], nullptr, 16)));
                    }
                ways.push_back(way);
            }
        const Pair p = logical(kind, ways);
        const Files want{unhex(field[3]), unhex(field[4])};
        CHECK(encode(kind, p) == want);
        CHECK(decode(kind, want) == p);
        cases++;
        streets += !kind.interp;
        for (const Row& r : rows_of(kind, want))
            for (size_t j = 0; j < r.ways; j++) {
                hybrid += r.cnt[j] && way_files::is_hybrid(r.code[j]);
                order += r.cnt[j] && !way_files::is_hybrid(r.code[j]);
            }
    }
    CHECK(cases >= 20 && streets >= 10 && cases - streets >= 6);
    CHECK(hybrid >= 50 && order >= 20);
}

// ------------------------------------------------------------------ the diff's decode on threads
namespace {

std::vector<uint8_t> file_bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

// What logical_decode::decode_way_files makes of the files on `threads`
// threads, `range` groups a range, its files in dir: the logical pair, and
// the refusal ("" for files it takes).
struct Threaded {
    Pair logical;
    std::string refusal;
};
Threaded threaded_decode(const std::string& dir, const Kind& kind, const Files& f, unsigned threads, uint64_t range) {
    const std::string ways = dir + "/ways", nodes = dir + "/nodes";
    Threaded t;
    {
        logical_decode::ScratchFile wo(ways), no(nodes);
        t.refusal = refusal_of([&] {
            CHECK(logical_decode::decode_way_files(kind, bytes_of(f.rows), f.rows.size(), bytes_of(f.nodes), f.nodes.size(), wo.fd(),
                                                   no.fd(), threads, range));
        });
        CHECK(wo.flush() && no.flush());
    }
    t.logical = {file_bytes(ways), file_bytes(nodes)};
    return t;
}

// What the one decoder makes of the files: the logical pair, and the refusal.
Threaded one_decode(const Kind& kind, const Files& f) {
    Threaded t;
    t.refusal = refusal_of([&] { t.logical = decode(kind, f); });
    return t;
}

}  // namespace

// geocoder-diff decodes the way pairs on threads, a range of groups at a
// time, each range written at its place: the logical pair is the one
// decoder's byte for byte, whatever the threads and the ranges.
TEST(a_scratch_file_that_did_not_open_fails_its_flush_unwritten) {
    ScratchDir dir("scratch-file-open");
    logical_decode::ScratchFile f(dir.path() + "/no-such-dir/file");
    CHECK(!f.flush());
}

TEST(the_threaded_decode_writes_the_one_decoders_way_pair) {
    ScratchDir dir("ways-threaded");
    std::mt19937_64 rng(20261117);
    uint64_t groups = 0;
    for (const Kind* kind : {&STREETS, &INTERPS})
        for (size_t n : {size_t(0), size_t(1), size_t(16), size_t(17), size_t(3000), kind->interp ? size_t(9000) : size_t(70000)}) {
            const Files f = encode(*kind, logical(*kind, random_ways(rng, *kind, n)));
            const Pair want = decode(*kind, f);
            groups = std::max(groups, footers(*kind, f).groups());
            for (unsigned threads : {1u, 2u, 3u, 8u})
                for (uint64_t range : {uint64_t(1), uint64_t(2), uint64_t(7), logical_decode::WAY_RANGE_GROUPS}) {
                    const Threaded t = threaded_decode(dir.path(), *kind, f, threads, range);
                    CHECK_EQ(t.refusal, std::string());
                    CHECK(t.logical == want);
                }
        }
    // Ranges of the default size, and a short last one.
    CHECK_EQ(groups, uint64_t(4375));
}

// A refusal in a later range fails the threaded decode by name, as the one
// decoder refuses the pair: a group's off, its row, a block of group 150 of
// 188, a footer of fewer nodes than the ways have, which no thread can place
// the last ranges' nodes by, and of two refusals the first in the files'
// order.
TEST(the_threaded_decode_refuses_in_a_later_range_what_the_one_decoder_refuses) {
    ScratchDir dir("ways-threaded-refused");
    std::mt19937_64 rng(1117);
    const Files f = encode(STREETS, logical(STREETS, random_ways(rng, STREETS, 3000)));
    REQUIRE(footers(STREETS, f).groups() == 188);
    const std::vector<Row> rows = rows_of(STREETS, f);
    auto edited = [&](const std::function<void(Files&)>& edit) {
        Files g = f;
        edit(g);
        return g;
    };
    auto with_row = [&](uint64_t g, const std::function<void(Row&)>& edit) {
        return edited([&, g](Files& files) {
            Row r = rows[size_t(g)];
            edit(r);
            way_files::encode_row(STREETS, r, files.rows.data() + g * STREETS.row_bytes(G));
        });
    };
    // A bit of a block of group 150 the one decoder refuses the pair for: the first one of the
    // group's blocks that is.
    const Row& r = rows[150];
    Files block = f;
    for (uint64_t at = r.off; at < r.off + r.blocks_bytes() && one_decode(STREETS, block).refusal.empty(); at++)
        for (int bit = 0; bit < 8; bit++) {
            block = f;
            block.nodes[size_t(at)] ^= uint8_t(1u << bit);
            if (!one_decode(STREETS, block).refusal.empty()) break;
        }
    REQUIRE(!one_decode(STREETS, block).refusal.empty());
    const size_t footer = f.nodes.size() - way_files::NODES_FOOTER_BYTES;
    struct Case {
        Files files;
        std::string name;
    };
    const Case cases[] = {
        {with_row(150, [](Row& w) { w.off++; }), "street_ways.bin group 150 is not where the blocks before it end"},
        {with_row(150, [](Row& w) { w.code[0] = w.cnt[0] ? uint16_t(40) : uint16_t(1); }), "Malformed way files: a "},
        {block, "Malformed way files: "},
        {edited([&](Files& g) { way_files::put64(g.nodes.data() + footer, way_files::get64(g.nodes.data() + footer) - 1); }),
         "nodes and its ways have"},
        {edited([&](Files& g) { way_files::put64(g.nodes.data() + footer, way_files::get64(g.nodes.data() + footer) + 1); }),
         "nodes and its ways have"},
        {edited([&](Files& g) {
             Row w = rows[150];
             w.off++;
             way_files::encode_row(STREETS, w, g.rows.data() + 150 * STREETS.row_bytes(G));
             w = rows[40];
             w.off++;
             way_files::encode_row(STREETS, w, g.rows.data() + 40 * STREETS.row_bytes(G));
         }),
         "street_ways.bin group 40 is not where the blocks before it end"},
    };
    for (const Case& c : cases) {
        const std::string want = one_decode(STREETS, c.files).refusal;
        CHECK(says(want, c.name));
        for (unsigned threads : {2u, 5u})
            for (uint64_t range : {uint64_t(1), uint64_t(3)})
                CHECK_EQ(threaded_decode(dir.path(), STREETS, c.files, threads, range).refusal, want);
    }
}

// Every pair a byte changed in, or with a file cut short, the threaded
// decode takes or refuses as the one decoder does: the same logical pair, or
// the same refusal.
TEST(the_threaded_decode_takes_and_refuses_what_the_one_decoder_does) {
    ScratchDir dir("ways-threaded-edits");
    std::mt19937_64 rng(1118);
    size_t refused = 0, decoded = 0;
    for (const Kind* kind : {&STREETS, &INTERPS}) {
        const Files f = encode(*kind, logical(*kind, random_ways(rng, *kind, 400)));
        for (int t = 0; t < 1200; t++) {
            Files g = f;
            // A cut goes to the rows and the nodes in turn, a flipped bit to either.
            std::vector<uint8_t>& file = (t % 50 == 0 ? t / 50 : t) % 2 ? g.rows : g.nodes;
            if (t % 50 == 0) {
                file.resize(rng() % file.size());
            } else {
                const size_t at = rng() % file.size();
                file[at] ^= uint8_t(1u << (rng() % 8));
            }
            const Threaded want = one_decode(*kind, g), got = threaded_decode(dir.path(), *kind, g, 1 + unsigned(t % 4), 1 + uint64_t(t % 3));
            CHECK_EQ(got.refusal, want.refusal);
            if (want.refusal.empty()) CHECK(got.logical == want.logical);
            (want.refusal.empty() ? decoded : refused)++;
        }
    }
    CHECK(refused > 300 && decoded > 1000);
}
