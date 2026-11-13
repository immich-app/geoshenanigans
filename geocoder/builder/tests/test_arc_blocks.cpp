// arc_blocks.h: the arc files' varints, arc blocks and ring blocks, their
// boxes and chunk cuts, and what a decoder refuses.
#include "arc_blocks.h"

#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "test_framework.h"

using namespace arc_blocks;

namespace {

std::vector<uint8_t> varint(uint64_t v) {
    std::vector<uint8_t> out(put_varint(v, nullptr));
    put_varint(v, out.data());
    return out;
}

std::vector<uint8_t> arc_block(const std::vector<PackedLocation>& v, uint8_t k) {
    auto at = [&](size_t i) { return v[i]; };
    std::vector<uint8_t> out(encode_arc(v.size(), at, k, nullptr));
    CHECK_EQ(encode_arc(v.size(), at, k, out.data()), out.size());
    return out;
}

std::vector<uint8_t> ring_block(const std::vector<std::vector<RingRef>>& rings) {
    size_t bytes = 0;
    for (const auto& r : rings) bytes += encode_ring(r.size(), [&](size_t i) { return r[i]; }, nullptr);
    std::vector<uint8_t> out(bytes);
    uint8_t* at = out.data();
    for (const auto& r : rings) at += encode_ring(r.size(), [&](size_t i) { return r[i]; }, at);
    return out;
}

ArcBox box(int32_t min_lng, int32_t max_lng, int32_t max_lat) {
    ArcBox b;
    b.min_lng = min_lng;
    b.max_lng = max_lng;
    b.max_lat = max_lat;
    return b;
}

// A ring of n refs whose planet slots are `planet(i)`, each arc's box a unit
// square at its index.
std::vector<RingRef> ring_of(size_t n, uint32_t (*planet)(size_t)) {
    std::vector<RingRef> ring;
    for (size_t i = 0; i < n; i++) {
        const int32_t x = static_cast<int32_t>(i);
        ring.push_back({static_cast<uint32_t>(1000 + 7 * i), i % 3 == 0, planet(i), box(x, x + 1, -x)});
    }
    return ring;
}

uint32_t identity_planet(size_t i) { return static_cast<uint32_t>(i); }

}  // namespace

TEST(varints_and_zigzag_round_trip_and_a_varint_cut_short_or_past_64_bits_is_refused) {
    for (int64_t v : {int64_t(0), int64_t(-1), int64_t(1), int64_t(-3600000000), int64_t(3600000000), INT64_MIN,
                      INT64_MAX})
        CHECK_EQ(unzigzag(zigzag(v)), v);
    CHECK_EQ(zigzag(-1), 1u);
    CHECK_EQ(zigzag(1), 2u);
    for (uint64_t v : {uint64_t(0), uint64_t(127), uint64_t(128), uint64_t(1) << 35, UINT64_MAX}) {
        const std::vector<uint8_t> bytes = varint(v);
        const uint8_t* p = bytes.data();
        uint64_t back = 0;
        CHECK(get_varint(p, bytes.data() + bytes.size(), back));
        CHECK_EQ(back, v);
        CHECK(p == bytes.data() + bytes.size());
        p = bytes.data();
        CHECK(!get_varint(p, bytes.data() + bytes.size() - 1, back));
    }
    CHECK_EQ(varint(127).size(), size_t(1));
    CHECK_EQ(varint(128).size(), size_t(2));
    CHECK_EQ(varint(UINT64_MAX).size(), size_t(10));
    const std::vector<uint8_t> past = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02};
    const uint8_t* p = past.data();
    uint64_t v = 0;
    CHECK(!get_varint(p, past.data() + past.size(), v));
}

TEST(an_arc_block_round_trips_on_each_grid_and_carries_its_box_from_16_vertices) {
    std::mt19937_64 rng(5);
    std::uniform_int_distribution<int32_t> lat(-900000, 900000), lng(-1800000, 1800000);
    for (uint8_t k : {uint8_t(0), uint8_t(2), uint8_t(3)}) {
        const int32_t step = grid_step(k);
        for (size_t n : {size_t(1), size_t(2), size_t(15), size_t(16), size_t(200)}) {
            std::vector<PackedLocation> v;
            for (size_t i = 0; i < n; i++) v.push_back({lat(rng) * step, lng(rng) * step});
            // The poles and the antimeridian on the grid, at both ends of a jump.
            if (n >= 2) v[n - 1] = {900000000 - 900000000 % step, -1800000000 + 1800000000 % step};
            const std::vector<uint8_t> block = arc_block(v, k);
            DecodedArc d;
            CHECK(decode_arc(block.data(), block.size(), d));
            CHECK(d.vertices == v);
            CHECK_EQ(int(d.k), int(k));
            CHECK_EQ(d.has_box, n >= kArcBboxMinVertices);
            CHECK_EQ(int(block[0] & (kGridMask | kHasBox)), int(k | (n >= kArcBboxMinVertices ? 0x80 : 0)));
            if (d.has_box) {
                ArcBox want;
                for (const PackedLocation& p : v) want.add(p);
                CHECK(d.box == want);
            }
        }
    }
    CHECK_EQ(int(grid_exponent(1)), 0);
    CHECK_EQ(int(grid_exponent(kSimplifiedGridE7)), 2);
    CHECK(gctest::refusal_of<std::invalid_argument>([] { grid_exponent(50); }).find("no power of ten") != std::string::npos);
    CHECK(gctest::refusal_of<std::logic_error>([] { arc_block({{150, 0}}, 2); }).find("off its tier's grid") !=
          std::string::npos);
}

TEST(an_arc_block_that_isnt_well_formed_decodes_to_nothing) {
    // 3 vertices: flags, the first vertex, a header byte and 2 deltas a coordinate at 8 bits or more.
    const std::vector<uint8_t> good = arc_block({{100, 200}, {300, -400}, {300, -500}}, 2);
    DecodedArc d;
    CHECK(decode_arc(good.data(), good.size(), d));
    for (size_t cut = 0; cut < good.size(); cut++) {
        // Short of its first vertex, or ending inside a group (at 9 and 11 the
        // block is a shorter arc's: one vertex, or one delta at 4 + 4 bits).
        if (cut == 9 || cut == 11) continue;
        CHECK(!decode_arc(good.data(), cut, d));
    }
    // A short group whose widths sum to less than 8 can't say its count.
    std::vector<uint8_t> narrow = good;
    narrow.resize(9);
    narrow.push_back(0x21);  // widths 2 and 1
    narrow.push_back(0x05);
    CHECK(!decode_arc(narrow.data(), narrow.size(), d));
    // A short group too short for one vertex: widths 5 and 5 in one byte.
    std::vector<uint8_t> short_group = good;
    short_group.resize(9);
    short_group.push_back(0x55);
    short_group.push_back(0x00);
    CHECK(!decode_arc(short_group.data(), short_group.size(), d));
    // A latitude past i32 once scaled by the grid, and a delta past any.
    std::vector<uint8_t> far = arc_block({{0, 0}}, 3);
    for (int i = 1; i <= 4; i++) far[i] = 0x7f;
    CHECK(!decode_arc(far.data(), far.size(), d));
    std::vector<uint8_t> jump = arc_block({{0, 0}}, 0);
    jump[0] |= 15 << kBaseWidthShift;  // base width 30
    jump.push_back(0xff);              // widths 45 and 45: one vertex in 12 bytes
    for (int i = 0; i < 12; i++) jump.push_back(0xff);
    CHECK(!decode_arc(jump.data(), jump.size(), d));
}

TEST(an_arc_block_packs_its_deltas_in_groups_and_takes_second_order_ones_where_smaller) {
    // Evenly spaced vertices: every second-order delta is 0.
    std::vector<PackedLocation> line;
    for (int i = 0; i < 40; i++) line.push_back({1000000 + i * 12345, -2000000 + i * 6789});
    const std::vector<uint8_t> straight = arc_block(line, 0);
    CHECK((straight[0] & kSecondOrder) != 0);
    DecodedArc d;
    REQUIRE(decode_arc(straight.data(), straight.size(), d));
    CHECK(d.vertices == line);
    // A zigzag on both axes: second-order deltas are twice the first-order ones, so first order wins.
    std::vector<PackedLocation> zig;
    for (int i = 0; i < 40; i++) zig.push_back({i % 2 ? 5000 : 0, i % 2 ? 3000 : 0});
    const std::vector<uint8_t> z = arc_block(zig, 0);
    CHECK((z[0] & kSecondOrder) == 0);
    REQUIRE(decode_arc(z.data(), z.size(), d));
    CHECK(d.vertices == zig);
    // Every vertex count round-trips: full groups, a short last one, and its count from its bytes.
    std::mt19937_64 rng(11);
    std::uniform_int_distribution<int32_t> small(-3, 3), wide(-2000000, 2000000);
    for (size_t n = 1; n <= 70; n++)
        for (int kind = 0; kind < 3; kind++) {
            std::vector<PackedLocation> v{{0, 0}};
            for (size_t i = 1; i < n; i++) {
                const int32_t dl = kind == 0 ? small(rng) : wide(rng), dg = kind == 2 ? small(rng) : wide(rng);
                v.push_back({v.back().lat_e7 + dl, v.back().lon_e7 + dg});
            }
            const std::vector<uint8_t> block = arc_block(v, 0);
            REQUIRE(decode_arc(block.data(), block.size(), d));
            CHECK(d.vertices == v);
        }
}

TEST(an_arc_delta_across_the_whole_globe_round_trips_at_a_wider_base) {
    // From the south-west corner to the north-east and back: deltas of about 2^32 e7, past 30 bits.
    const std::vector<PackedLocation> v{{-900000000, -1800000000}, {900000000, 1800000000}, {-900000000, -1800000000}};
    const std::vector<uint8_t> block = arc_block(v, 0);
    CHECK(((block[0] >> kBaseWidthShift) & 15) > 0);
    DecodedArc d;
    REQUIRE(decode_arc(block.data(), block.size(), d));
    CHECK(d.vertices == v);
}

TEST(arc_blocks_are_the_shared_vectors_byte_for_byte) {
    // geocoder/testdata/arc_blocks.tsv, which the server's decoder and test encoder read too.
    std::ifstream f(std::string(GEOCODER_TESTDATA_DIR) + "/arc_blocks.tsv");
    REQUIRE(f.good());
    size_t rows = 0;
    bool header = true;
    for (std::string line; std::getline(f, line);) {
        if (line.empty() || line[0] == '#') continue;
        if (header) {
            header = false;
            continue;
        }
        std::vector<std::string> field;
        std::stringstream in(line);
        for (std::string x; std::getline(in, x, '\t');) field.push_back(x);
        REQUIRE(field.size() == 4);
        const uint8_t k = static_cast<uint8_t>(std::stoi(field[0]));
        std::vector<PackedLocation> v;
        std::stringstream verts(field[2]);
        for (std::string pair; std::getline(verts, pair, ';');) {
            const size_t comma = pair.find(',');
            v.push_back({std::stoi(pair.substr(0, comma)), std::stoi(pair.substr(comma + 1))});
        }
        std::vector<uint8_t> want;
        for (size_t i = 0; i + 1 < field[3].size(); i += 2) want.push_back(static_cast<uint8_t>(std::stoi(field[3].substr(i, 2), nullptr, 16)));
        CHECK(arc_block(v, k) == want);
        DecodedArc d;
        CHECK(decode_arc(want.data(), want.size(), d));
        CHECK(d.vertices == v);
        rows++;
    }
    CHECK(rows > 100);
}

TEST(a_ring_of_up_to_8_refs_is_one_chunk_without_a_box) {
    for (size_t n : {size_t(1), size_t(8)}) {
        const std::vector<RingRef> ring = ring_of(n, identity_planet);
        const std::vector<uint8_t> block = ring_block({ring});
        std::vector<RingChunk> chunks;
        REQUIRE(decode_ring_block(block.data(), block.size(), chunks));
        REQUIRE(chunks.size() == 1u);
        CHECK(!chunks[0].has_box);
        CHECK(chunks[0].last_in_ring);
        REQUIRE(chunks[0].refs.size() == n);
        for (size_t i = 0; i < n; i++) CHECK(chunks[0].refs[i] == (SlotRef{ring[i].slot, ring[i].reversed}));
        // head, then each ref against the one before it.
        CHECK_EQ(int(block[0]), int(n << 2 | 1));
    }
    // Slots going down too, and a polygon of two rings.
    std::vector<RingRef> down = {{5000, false, 1, {}}, {3, true, 2, {}}, {UINT32_MAX, false, 3, {}}};
    std::vector<std::vector<SlotRef>> rings;
    const std::vector<uint8_t> two = ring_block({down, ring_of(3, identity_planet)});
    REQUIRE(decode_rings(two.data(), two.size(), rings));
    REQUIRE(rings.size() == 2u);
    CHECK(rings[0] == (std::vector<SlotRef>{{5000, false}, {3, true}, {UINT32_MAX, false}}));
    CHECK_EQ(rings[1].size(), size_t(3));
}

TEST(a_longer_ring_is_cut_where_its_planet_slots_say_and_each_chunk_carries_its_arcs_box) {
    const std::vector<RingRef> ring = ring_of(60, identity_planet);
    const std::vector<uint8_t> block = ring_block({ring});
    std::vector<RingChunk> chunks;
    REQUIRE(decode_ring_block(block.data(), block.size(), chunks));
    size_t at = 0;
    for (size_t c = 0; c < chunks.size(); c++) {
        const RingChunk& chunk = chunks[c];
        CHECK(chunk.has_box);
        CHECK_EQ(chunk.last_in_ring, c + 1 == chunks.size());
        const size_t n = chunk.refs.size();
        ArcBox want;
        for (size_t i = 0; i < n; i++) {
            CHECK(chunk.refs[i] == (SlotRef{ring[at + i].slot, ring[at + i].reversed}));
            want.add(ring[at + i].box);
            // No cut before the chunk's last ref.
            if (i + 1 < n) CHECK(!cuts_after(static_cast<uint32_t>(i + 1), ring[at + i].planet_slot));
        }
        CHECK(chunk.box == want);
        if (c + 1 < chunks.size()) {
            CHECK(cuts_after(static_cast<uint32_t>(n), ring[at + n - 1].planet_slot));
            CHECK(n >= kRefChunkMin && n <= kRefChunkMax);
        }
        at += n;
    }
    CHECK_EQ(at, ring.size());
    CHECK(chunks.size() > 3);
    // The hash cuts: after 4 refs or more where mix64's low 3 bits are set,
    // at 16 whatever the hash.
    uint32_t cutting = 0, quiet = 0;
    while ((mix64(cutting) & kRefChunkCutMask) != kRefChunkCutMask) cutting++;
    while ((mix64(quiet) & kRefChunkCutMask) == kRefChunkCutMask) quiet++;
    CHECK(cuts_after(4, cutting) && !cuts_after(3, cutting));
    CHECK(!cuts_after(15, quiet) && cuts_after(16, quiet));
    // A ring that never hashes a cut is cut every 16.
    const std::vector<uint8_t> flat = ring_block({ring_of(40, [](size_t) { return uint32_t(0); })});
    REQUIRE(decode_ring_block(flat.data(), flat.size(), chunks));
    REQUIRE(chunks.size() == 3u);
    CHECK(chunks[0].refs.size() == 16u && chunks[1].refs.size() == 16u && chunks[2].refs.size() == 8u);
}

TEST(a_ref_put_into_a_ring_changes_only_its_own_chunk) {
    // Planet slots whose cuts fall every few refs, none forced at 16.
    std::vector<RingRef> ring;
    uint32_t planet = 0;
    for (size_t i = 0; i < 48; i++) {
        const bool cut = i % 6 == 5;
        while (((mix64(planet) & kRefChunkCutMask) == kRefChunkCutMask) != cut) planet++;
        const int32_t x = static_cast<int32_t>(i);
        ring.push_back({static_cast<uint32_t>(10 * i), false, planet++, box(x, x + 1, x)});
    }
    uint32_t extra = planet;
    while ((mix64(extra) & kRefChunkCutMask) == kRefChunkCutMask) extra++;
    std::vector<RingRef> grown = ring;
    grown.insert(grown.begin() + 20, RingRef{999, true, extra, box(-5, 100, 7)});
    std::vector<RingChunk> before, after;
    const std::vector<uint8_t> a = ring_block({ring}), b = ring_block({grown});
    REQUIRE(decode_ring_block(a.data(), a.size(), before));
    REQUIRE(decode_ring_block(b.data(), b.size(), after));
    REQUIRE(before.size() == after.size());
    size_t changed = 0;
    for (size_t c = 0; c < before.size(); c++) {
        const bool same = before[c].refs == after[c].refs && before[c].box == after[c].box;
        if (!same) {
            changed++;
            CHECK_EQ(c, size_t(3));  // refs 18-23
            CHECK_EQ(after[c].refs.size(), before[c].refs.size() + 1);
        }
    }
    CHECK_EQ(changed, size_t(1));
}

TEST(a_ring_block_that_isnt_well_formed_decodes_to_nothing) {
    const std::vector<uint8_t> good = ring_block({ring_of(30, identity_planet)});
    std::vector<RingChunk> chunks;
    CHECK(decode_ring_block(good.data(), good.size(), chunks));
    CHECK(decode_ring_block(good.data(), 0, chunks) && chunks.empty());
    for (size_t cut = 1; cut < good.size(); cut++) CHECK(!decode_ring_block(good.data(), cut, chunks));
    // A chunk of no refs, or of more than 16.
    for (uint64_t head : {uint64_t(0) << 2 | 1, uint64_t(17) << 2 | 1}) {
        const std::vector<uint8_t> bad = varint(head);
        CHECK(!decode_ring_block(bad.data(), bad.size(), chunks));
    }
    // A box whose byte count runs past its refs.
    std::vector<uint8_t> boxed = ring_block({ring_of(9, [](size_t) { return uint32_t(0); })});
    REQUIRE(decode_ring_block(boxed.data(), boxed.size(), chunks));
    boxed[1 + 12] += 1;
    CHECK(!decode_ring_block(boxed.data(), boxed.size(), chunks));
}
