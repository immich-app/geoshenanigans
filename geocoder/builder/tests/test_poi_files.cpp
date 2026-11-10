// The POI files' codec (src/poi_files.h): every record shape and polygon
// round trips through R2 and V2 to its logical bytes, the builder's writer
// and the whole-set encoder write the same files, a small set's files are
// the bytes the format gives (server/src/poi_files.rs reads the same ones),
// each dictionary holds 65,535 entries and refuses the next, and the codec
// refuses each logical set it can't hold and each file it never writes.
#include "poi_files.h"

#include <cmath>
#include <cstring>
#include <functional>
#include <random>
#include <string>
#include <vector>

#include "test_framework.h"
#include "types.h"

using gctest::refusal_of;

namespace {

struct Polygon {
    uint8_t encoding;
    uint32_t bmin_lat, bmin_lng;  // f32 bit patterns
    std::vector<std::pair<uint32_t, uint32_t>> q;
};

// A logical POI set as the builder holds it before writing.
struct Set {
    std::vector<uint8_t> recs, verts;
    void add(PoiRecord r, const Polygon* p = nullptr) {
        if (p) {
            r.vertex_offset = uint32_t(verts.size());
            r.vertex_count = uint32_t(p->q.size());
            const size_t at = verts.size();
            verts.resize(at + poi_files::LOGICAL_HEADER + p->q.size() * poi_files::vertex_bytes(p->encoding));
            verts[at] = p->encoding;
            verts[at + 1] = 0;
            poi_files::wr32(&verts[at + 2], p->bmin_lat);
            poi_files::wr32(&verts[at + 6], p->bmin_lng);
            for (size_t k = 0; k < p->q.size(); k++)
                poi_files::put_q(&verts[at + poi_files::LOGICAL_HEADER + k * poi_files::vertex_bytes(p->encoding)], p->encoding,
                                 p->q[k].first, p->q[k].second);
        } else {
            r.vertex_offset = NO_DATA;
            r.vertex_count = 0;
        }
        const uint8_t* b = reinterpret_cast<const uint8_t*>(&r);
        recs.insert(recs.end(), b, b + sizeof r);
    }
    uint64_t n() const { return recs.size() / poi_files::LOGICAL_STRIDE; }
};

uint32_t bits(float f) {
    uint32_t b;
    memcpy(&b, &f, 4);
    return b;
}

PoiRecord record(float lat, float lng, uint32_t name, uint8_t category = 7, uint8_t importance = 0) {
    PoiRecord r{};
    r.lat = lat;
    r.lng = lng;
    r.name_id = name;
    r.category = category;
    r.tier = 2;
    r.flags = 1;
    r.importance = importance;
    r.parent_street_id = 1000 + name;
    r.parent_postcode_id = 2000;
    r.parent_poly_id = 3000;
    return r;
}

poi_files::Files encode(const Set& s) { return poi_files::encode(s.recs.data(), s.n(), s.verts.data(), s.verts.size()); }

poi_files::Logical decode(const poi_files::Files& f) {
    return poi_files::decode(f.records.data(), f.records.size(), f.vertices.data(), f.vertices.size());
}

bool round_trips(const Set& s) {
    const poi_files::Logical l = decode(encode(s));
    return std::vector<uint8_t>(l.records.begin(), l.records.end()) == s.recs &&
           std::vector<uint8_t>(l.vertices.begin(), l.vertices.end()) == s.verts;
}

std::vector<uint8_t> hex(const char* h) {
    std::vector<uint8_t> out;
    for (const char* p = h; *p;) {
        if (*p == ' ') {
            p++;
            continue;
        }
        out.push_back(uint8_t(std::stoul(std::string(p, 2), nullptr, 16)));
        p += 2;
    }
    return out;
}

// A point and a closed triangle (poi_files_write_a_small_set_as_the_format_gives).
Set small_set() {
    Set s;
    PoiRecord a{};
    a.lat = 1.5f;
    a.lng = -2.25f;
    a.name_id = 100;
    a.category = 7;
    a.tier = 1;
    a.flags = 2;
    a.importance = 200;
    a.parent_street_id = NO_DATA;
    a.parent_postcode_id = 500;
    a.parent_poly_id = NO_DATA;
    s.add(a);
    PoiRecord b = a;
    b.lat = 1.0f;
    b.lng = 2.0f;
    b.name_id = 101;
    b.parent_street_id = 300;
    b.parent_poly_id = 0x123456;
    const Polygon tri{0, bits(1.0f), bits(1.5f), {{0, 5}, {3, 0}, {1, 7}, {0, 5}}};
    s.add(b, &tri);
    return s;
}
// Its files, as the format gives them (poi_files.h), and as poi_files.rs's
// test holds them.
const char* SMALL_R2 =
    "0000c03f 000010c0 00000000 64000000 ffffffff ffffff 0000 0000"
    "0000803f 00000040 00000000 65000000 2c010000 563412 0000 0100"
    "07 01 02 c8 00  07 01 02 c8 01"
    "f4010000"
    "02000000 02000000 01000000 1a00000000000000 504f5232";
const char* SMALL_V2 = "04 03 00 ffffff03 02 03 7474  0000000000000000";

// The small set's files with edit applied: refused with `refusal` (a
// prefix of the error's text).
std::string refusal_of_edit(const std::function<void(std::vector<uint8_t>&, std::vector<uint8_t>&)>& edit) {
    std::vector<uint8_t> r2 = hex(SMALL_R2), v2 = hex(SMALL_V2);
    edit(r2, v2);
    return refusal_of([&] { poi_files::decode(r2.data(), r2.size(), v2.data(), v2.size()); });
}

// The small set's files with its block replaced by `block` (the record at
// lat 1.0, lng 2.0) and the footer's logical size by `logical`.
std::string refusal_of_block(const std::vector<uint8_t>& block, uint64_t logical = 26) {
    return refusal_of_edit([&](std::vector<uint8_t>& r2, std::vector<uint8_t>& v2) {
        v2 = block;
        v2.resize(v2.size() + poi_files::VERTEX_PAD, 0);
        poi_files::wr64(&r2[r2.size() - 12], logical);
    });
}

bool starts_with(const std::string& s, const std::string& p) { return s.compare(0, p.size(), p) == 0; }

}  // namespace

TEST(poi_files_round_trip_every_record_shape_and_polygon) {
    std::mt19937 rng(20261110);
    Set s;
    // Points: every field set; none of street, postcode, admin polygon; a
    // tombstone; the largest admin polygon R2 holds.
    s.add(record(47.37f, 8.54f, 12));
    PoiRecord bare = record(-33.9f, 151.2f, 24);
    bare.parent_street_id = bare.parent_postcode_id = bare.parent_poly_id = NO_DATA;
    s.add(bare);
    PoiRecord dead{};
    dead.name_id = NO_DATA;
    s.add(dead);
    PoiRecord far = record(0.0f, -0.0f, 36);
    far.parent_poly_id = 0xFFFFFE;
    s.add(far);
    // Polygons: one vertex; two equal ones (closed, one stored); two that
    // differ (open); closed and open rings; all-zero vertices (no bits); u32
    // vertices at full width (64 bits a vertex) and at 63 bits; a bbox far
    // from its record (5-byte varints); a tombstone keeping its block.
    const std::vector<Polygon> polys = {
        {0, bits(47.0f), bits(8.0f), {{5, 9}}},
        {1, bits(47.0f), bits(8.0f), {{5, 9}, {5, 9}}},
        {2, bits(47.0f), bits(8.0f), {{5, 9}, {9, 5}}},
        {0, bits(47.0f), bits(8.0f), {{0, 0}, {65535, 3}, {7, 65535}, {12, 400}, {0, 0}}},
        {1, bits(47.0f), bits(8.0f), {{0, 0}, {300, 3}, {7, 1}, {0, 1}}},
        {2, bits(47.0f), bits(8.0f), {{0, 0}, {0, 0}, {0, 0}}},
        {3, bits(47.0f), bits(8.0f), {{0xFFFFFFFFu, 0xFFFFFFFFu}, {0, 1}, {0x80000000u, 0x7FFFFFFFu}}},
        {3, bits(47.0f), bits(8.0f), {{0xFFFFFFFFu, 0x7FFFFFFFu}, {1, 2}, {3, 4}, {0xFFFFFFFFu, 0x7FFFFFFFu}}},
        {0, 0x7FFFFFFFu, 0x80000000u, {{1, 2}, {3, 4}}},
    };
    for (size_t i = 0; i < polys.size(); i++) s.add(record(-47.0f, -8.0f, uint32_t(48 + 12 * i), 3, uint8_t(i)), &polys[i]);
    s.add(dead, &polys[3]);
    // A ring of random vertices per encoding, closed and open.
    for (uint8_t e = 0; e < 4; e++)
        for (bool closed : {false, true}) {
            Polygon p{e, uint32_t(rng()), uint32_t(rng()), {}};
            for (int k = 0; k < 1000; k++) p.q.push_back({uint32_t(rng() >> (e == 3 ? 0 : 16)), uint32_t(rng() >> (e == 3 ? 3 : 18))});
            if (closed) p.q.push_back(p.q[0]);
            s.add(record(1.0f, 2.0f, 600, 9), &p);
        }
    CHECK(round_trips(s));
    // The builder's writer, a record at a time, writes the whole set's files.
    poi_files::Files streamed;
    poi_files::Writer w([&](const uint8_t* p, size_t n) { streamed.records.insert(streamed.records.end(), p, p + n); },
                        [&](const uint8_t* p, size_t n) { streamed.vertices.insert(streamed.vertices.end(), p, p + n); });
    for (uint64_t i = 0; i < s.n(); i++) {
        const uint8_t* rec = &s.recs[i * poi_files::LOGICAL_STRIDE];
        const uint32_t vc = poi_files::rd32(rec + 12);
        w.add(rec, vc ? &s.verts[poi_files::rd32(rec + 8)] : nullptr);
    }
    w.finish();
    const poi_files::Files whole = encode(s);
    CHECK(streamed.records == whole.records);
    CHECK(streamed.vertices == whole.vertices);
    // No records: a footer and a pad.
    const poi_files::Files none = encode(Set{});
    CHECK_EQ(none.records.size(), poi_files::FOOTER_BYTES);
    CHECK(none.vertices == std::vector<uint8_t>(poi_files::VERTEX_PAD, 0));
    CHECK(round_trips(Set{}));
}

TEST(poi_files_write_a_small_set_as_the_format_gives) {
    const poi_files::Files f = encode(small_set());
    CHECK(f.records == hex(SMALL_R2));
    CHECK(f.vertices == hex(SMALL_V2));
    CHECK(round_trips(small_set()));
}

TEST(poi_files_dictionaries_hold_65535_entries_and_refuse_the_next) {
    // 65,535 postcodes, then one more; 65,535 combos (importance and flags),
    // then one more.
    Set postcodes, combos;
    for (uint32_t i = 0; i < 65535; i++) {
        PoiRecord r = record(1.0f, 2.0f, 12);
        r.parent_postcode_id = 12 * i;
        postcodes.add(r);
        PoiRecord c = record(1.0f, 2.0f, 12);
        c.importance = uint8_t(i);
        c.flags = uint8_t(i >> 8);
        combos.add(c);
    }
    CHECK(round_trips(postcodes));
    CHECK(round_trips(combos));
    PoiRecord r = record(1.0f, 2.0f, 12);
    r.parent_postcode_id = 12 * 65535;
    postcodes.add(r);
    PoiRecord c = record(1.0f, 2.0f, 12);
    c.importance = c.flags = 255;
    c.category = 8;
    combos.add(c);
    CHECK_EQ(refusal_of([&] { encode(postcodes); }), std::string("POI files can't hold more than 65535 distinct postcodes"));
    CHECK_EQ(refusal_of([&] { encode(combos); }), std::string("POI files can't hold more than 65535 distinct combos"));
    // A footer giving 65,536 entries.
    CHECK_EQ(refusal_of_edit([](std::vector<uint8_t>& r2, std::vector<uint8_t>&) { poi_files::wr32(&r2[r2.size() - 20], 65536); }),
             std::string("Malformed POI files: a dictionary of more than 65535 entries"));
}

TEST(poi_files_refuse_logical_sets_they_cant_hold) {
    const Polygon one{0, bits(1.0f), bits(2.0f), {{1, 1}}};
    struct Case {
        const char* what;
        std::function<void(Set&)> make;
        const char* refusal;
    };
    const Case cases[] = {
        {"a polygon count without an offset",
         [](Set& s) {
             s.add(record(1, 2, 12));
             poi_files::wr32(&s.recs[12], 3);
         },
         "POI files can't hold record 0: vertex_count 3 with vertex_offset 4294967295"},
        {"an offset without a polygon count",
         [&](Set& s) {
             s.add(record(1, 2, 12), &one);
             poi_files::wr32(&s.recs[12], 0);
         },
         "POI files can't hold record 0: vertex_count 0 with vertex_offset 0"},
        {"the admin polygon NO_POLY",
         [](Set& s) {
             PoiRecord r = record(1, 2, 12);
             r.parent_poly_id = 0xFFFFFF;
             s.add(r);
         },
         "POI files can't hold parent_poly_id 16777215"},
        {"encoding 4",
         [&](Set& s) {
             s.add(record(1, 2, 12), &one);
             s.verts[0] = 4;
         },
         "POI files can't hold vertex encoding 4"},
        {"a non-zero pad byte",
         [&](Set& s) {
             s.add(record(1, 2, 12), &one);
             s.verts[1] = 1;
         },
         "POI files can't hold a polygon header with a non-zero pad byte"},
        {"polygons not back to back",
         [&](Set& s) {
             s.add(record(1, 2, 12), &one);
             s.add(record(1, 2, 12), &one);
             s.verts.insert(s.verts.begin() + 14, 0);
             poi_files::wr32(&s.recs[36 + 8], 15);
         },
         "POI files can't hold record 1: a polygon at 15 where the last one ended at 14"},
        {"vertex bytes past the last polygon",
         [&](Set& s) {
             s.add(record(1, 2, 12), &one);
             s.verts.push_back(0);
         },
         "POI files can't hold vertex bytes past the last polygon"},
    };
    for (const Case& c : cases) {
        Set s;
        c.make(s);
        const std::string got = refusal_of([&] { encode(s); });
        if (got != c.refusal) std::printf("    %s: %s\n", c.what, got.c_str());
        CHECK_EQ(got, std::string(c.refusal));
    }
}

TEST(poi_files_decoder_refuses_each_file_the_encoder_never_writes) {
    using Bytes = std::vector<uint8_t>;
    struct Case {
        const char* refusal;  // the error's text up to its end or a ':' it ends with
        std::function<std::string()> run;
    };
    auto edit = [](std::function<void(Bytes&, Bytes&)> e) { return [e] { return refusal_of_edit(e); }; };
    auto block = [](Bytes b, uint64_t logical = 26) { return [b, logical] { return refusal_of_block(b, logical); }; };
    const Case cases[] = {
        {"Malformed POI files: poi_records.bin is shorter than its footer", edit([](Bytes& r, Bytes&) { r.resize(23); })},
        {"Malformed POI files: poi_records.bin is not R2", edit([](Bytes& r, Bytes&) { r.back() = '3'; })},
        {"Malformed POI files: poi_records.bin is 92 bytes, its footer gives 119",
         edit([](Bytes& r, Bytes&) { poi_files::wr32(&r[r.size() - 24], 3); })},
        {"Malformed POI files: combo 1 has polygon 2", edit([](Bytes& r, Bytes&) { r[54 + 9] = 2; })},
        {"Malformed POI files: combo 1 repeats an entry", edit([](Bytes& r, Bytes&) { r[54 + 9] = 0; })},
        {"Malformed POI files: postcode 1 repeats an entry",
         edit([](Bytes& r, Bytes&) {
             r.insert(r.begin() + 68, {0xf4, 0x01, 0, 0});
             poi_files::wr32(&r[r.size() - 16], 2);
         })},
        {"Malformed POI files: a row uses combo 1 with 0 used before it of 2",
         edit([](Bytes& r, Bytes&) {
             r[25] = 1;
             r[27 + 25] = 0;
         })},
        {"Malformed POI files: a dictionary entry no row uses",
         edit([](Bytes& r, Bytes&) {
             r.insert(r.begin() + 68, {9, 0, 0, 0});
             poi_files::wr32(&r[r.size() - 16], 2);
         })},
        {"Malformed POI files: row 1: voff 1 where the blocks are at 0", edit([](Bytes& r, Bytes&) { r[27 + 8] = 1; })},
        {"Malformed POI files: poi_vertices.bin is shorter than its pad", edit([](Bytes&, Bytes& v) { v.resize(7); })},
        {"Malformed POI files: poi_vertices.bin's pad is not zero", edit([](Bytes&, Bytes& v) { v.back() = 1; })},
        {"Malformed POI files: poi_vertices.bin has bytes past its last block",
         edit([](Bytes&, Bytes& v) { v.insert(v.begin() + 11, 0); })},
        {"Malformed POI files: the vertex stream is 26 bytes, the footer gives 27", block(hex("04 03 00 ffffff03 02 03 7474"), 27)},
        {"Malformed POI files: a block tag of 8", block(hex("08 03 00 ffffff03 02 03 7474"))},
        {"Malformed POI files: a block of no vertices", block(hex("04 00 00 ffffff03 02 03"))},
        {"Malformed POI files: a block header's varint", block(hex("04 8300 00 ffffff03 02 03 7474"))},
        {"Malformed POI files: a block's vertex width", block(hex("00 01 00 ffffff03 11 03 000000"))},
        {"Malformed POI files: a block's bbox past 32 bits", block(hex("00 01 00 8080808020 00 00"))},
        {"Malformed POI files: row 1: a block past the end of poi_vertices.bin", block(hex("04 03 00 ffffff03 02 03 74"))},
        // 100,000 stored vertices of no bits (a closed block of 100,001, all
        // equal): 11 bytes of poi_vertices.bin, 400,014 logical bytes where
        // the footer gives 26. Refused at the block, not once it is decoded.
        {"Malformed POI files: row 1: a block past the footer's vertex stream of 26 bytes",
         block(hex("04 a08d06 00 ffffff03 00 00"))},
        // One vertex (1, 1) at widths 2 and 1, not 1 and 1.
        {"Malformed POI files: a block's vertex width is not its largest vertex's", block(hex("00 01 00 ffffff03 02 01 05"), 14)},
        // Two vertices (1, 1) and (1, 1) stored open.
        {"Malformed POI files: an open block whose last vertex is its first", block(hex("00 02 00 ffffff03 01 01 0f"), 18)},
        // One vertex (1, 1) at widths 1 and 1, a bit set past it.
        {"Malformed POI files: a block's last byte has bits past its vertices", block(hex("00 01 00 ffffff03 01 01 07"), 14)},
    };
    for (const Case& c : cases) {
        const std::string got = c.run();
        if (!starts_with(got, c.refusal)) std::printf("    want %s, got %s\n", c.refusal, got.c_str());
        CHECK(starts_with(got, c.refusal));
    }
    // The files as written decode.
    CHECK_EQ(refusal_of_edit([](Bytes&, Bytes&) {}), std::string());
}
