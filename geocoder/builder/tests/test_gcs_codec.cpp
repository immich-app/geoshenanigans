// Unit tests for the low-memory mode's in-process codecs (tools/gcs_codec.h):
// today's-format bytes written to an encoder in odd-sized pieces decode back
// byte for byte, and malformed encoded sets fail with an error instead of
// decoding wrong bytes or exhausting memory.
#include "gcs_codec.h"
#include "gc_addr.h"

#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <random>
#include <stdexcept>
#include <string>
#include <sys/resource.h>
#include <unistd.h>
#include <vector>

#include "child_process.h"
#include "scratch_dir.h"
#include "test_framework.h"

using namespace gcs;
using gctest::exit_status_in_child;

namespace {

// Feeds bytes to one port in pieces of 1..max_piece bytes.
void write_in_pieces(Encoder& enc, int port, const uint8_t* bytes, size_t n, std::mt19937& rng, size_t max_piece) {
    for (size_t o = 0; o < n;) {
        const size_t c = std::min<size_t>(n - o, 1 + rng() % max_piece);
        enc.write(port, bytes + o, c);
        o += c;
    }
}
void write_in_pieces(Encoder& enc, int port, const std::vector<uint8_t>& bytes, std::mt19937& rng, size_t max_piece) {
    write_in_pieces(enc, port, bytes.data(), bytes.size(), rng, max_piece);
}

// Every port closed and pumped, then the manifest.
Manifest finish_all(Encoder& enc) {
    for (auto& s : enc.state) s = 2;
    enc.pump();
    Manifest m;
    enc.finish(m);
    return m;
}

// The decoder's bytes per port, decoded to the end.
std::vector<std::vector<uint8_t>> decode_all(Decoder& dec) {
    for (auto& s : dec.state) s = 1;
    while (dec.step()) {}
    std::vector<std::vector<uint8_t>> out;
    for (auto& q : dec.out) out.emplace_back(q.data(), q.data() + q.size());
    return out;
}

std::vector<uint8_t> u32s(const std::vector<uint32_t>& v) {
    std::vector<uint8_t> b(v.size() * 4);
    for (size_t i = 0; i < v.size(); i++) wr32(&b[4 * i], v[i]);
    return b;
}

// n little-endian bytes of v.
std::vector<uint8_t> le(uint64_t v, int n) {
    std::vector<uint8_t> b;
    for (int k = 0; k < n; k++) b.push_back(uint8_t(v >> (8 * k)));
    return b;
}

// LEB128, as the encoded columns hold counts.
void uv(std::vector<uint8_t>& o, uint64_t v) {
    while (v >= 0x80) {
        o.push_back(uint8_t(v) | 0x80);
        v >>= 7;
    }
    o.push_back(uint8_t(v));
}

std::vector<char> slurp(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::vector<char>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

void write_bytes(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), std::streamsize(bytes.size()));
}

// Every file of a dir, by name.
std::map<std::string, std::vector<char>> dir_files(const std::string& dir) {
    std::map<std::string, std::vector<char>> files;
    for (const auto& e : std::filesystem::directory_iterator(dir)) files[e.path().filename().string()] = slurp(e.path().string());
    return files;
}

bool throws_runtime(const std::function<void()>& fn, const char* message) {
    try {
        fn();
    } catch (const std::runtime_error& e) {
        return std::string(e.what()).find(message) != std::string::npos;
    }
    return false;
}

// In a child: at most `headroom` more bytes of address space, so a size taken
// from an unchecked count fails at once (none under ASan, whose shadow
// mappings need far more).
void cap_address_space(size_t headroom) {
#ifndef __SANITIZE_ADDRESS__
    long pages = 0;
    if (FILE* f = std::fopen("/proc/self/statm", "r")) {
        if (std::fscanf(f, "%ld", &pages) != 1) pages = 0;
        std::fclose(f);
    }
    const rlim_t cap = rlim_t(pages) * rlim_t(sysconf(_SC_PAGESIZE)) + headroom;
    struct rlimit as{cap, cap};
    setrlimit(RLIMIT_AS, &as);
#else
    (void)headroom;
#endif
}

// 2 if body throws a runtime_error naming `message`, 3 (the error printed) if
// another one, 0 if none.
int error_code(const std::function<void()>& body, const char* message) {
    try {
        body();
    } catch (const std::runtime_error& e) {
        if (std::string(e.what()).find(message) != std::string::npos) return 2;
        std::fprintf(stderr, "    unexpected error: %s\n", e.what());
        return 3;
    }
    return 0;
}

// A synthetic POI set: points, small and big polygons, closed and open rings,
// both vertex widths (records of 36 bytes, vertex blocks of a 10-byte header
// and vc vertices). Polygons of 9000 to 70000 vertices span several decoder
// steps (VERTS_PER_STEP) and, when open, several 64 KiB pieces of the
// encoder's plane relayout.
struct PoiSet {
    std::vector<uint8_t> recs, verts;
};
PoiSet poi_set(std::mt19937& rng) {
    PoiSet s;
    struct Spec { uint32_t vc; int w; bool closed; };
    const Spec specs[] = {{0, 0, false},    {3000, 2, false},  {5, 2, true},      {2000, 4, true},   {0, 0, false},
                          {7, 4, false},    {4096, 2, true},   {1, 2, false},     {2500, 4, false},  {0, 0, false},
                          {2, 2, true},     {70000, 2, false}, {17000, 2, true},  {70000, 4, false}, {17000, 4, true},
                          {9000, 2, false}, {65538, 2, false}, {65537, 4, false}, {65536, 2, false}};
    for (const Spec& sp : specs) {
        uint8_t r[36];
        for (auto& b : r) b = uint8_t(rng());
        if (!sp.vc) {
            wr32(r + 8, NO);
            wr32(r + 12, 0);
        } else {
            wr32(r + 8, uint32_t(s.verts.size()));
            wr32(r + 12, sp.vc);
            uint8_t h[10];
            for (auto& b : h) b = uint8_t(rng());
            h[0] = sp.w == 4 ? 3 : uint8_t(rng() % 3);
            h[1] = 0;
            s.verts.insert(s.verts.end(), h, h + 10);
            const size_t vs = 2 * size_t(sp.w);
            const size_t first = s.verts.size();
            for (uint32_t k = 0; k < sp.vc; k++) {
                uint8_t v[8];
                for (auto& b : v) b = uint8_t(rng());
                if (k + 1 == sp.vc && sp.closed && sp.vc > 1) memcpy(v, &s.verts[first], vs);
                if (k + 1 == sp.vc && !sp.closed && sp.vc > 1 && memcmp(v, &s.verts[first], vs) == 0) v[0] ^= 1;
                s.verts.insert(s.verts.end(), v, v + vs);
            }
        }
        s.recs.insert(s.recs.end(), r, r + 36);
    }
    return s;
}

// Encodes a POI set record by record (each record, then its vertex block, in
// odd-sized pieces) into dir; returns the manifest.
Manifest encode_poi(const PoiSet& s, const std::string& dir, std::mt19937& rng) {
    poly::PoiEnc enc(dir);
    enc.state = {1, 1};
    size_t vp = 0;
    for (size_t i = 0; i < s.recs.size() / 36; i++) {
        const std::vector<uint8_t> rec(s.recs.begin() + 36 * i, s.recs.begin() + 36 * (i + 1));
        write_in_pieces(enc, 0, rec, rng, 13);
        const uint32_t vc = rd32(&rec[12]);
        if (!vc) continue;
        const size_t bs = 10 + size_t(vc) * (s.verts[vp] == 3 ? 8 : 4);
        write_in_pieces(enc, 1, &s.verts[vp], bs, rng, 997);
        vp += bs;
    }
    return finish_all(enc);
}

// A synthetic admin set: admin_polygons.bin records (24 bytes, the vertex
// offset and count first) and their vertex blocks (a 10-byte header and vc
// vertices of w u16s).
struct AdminSpec {
    uint32_t vc;
    int w;
};
struct AdminSet {
    std::vector<uint8_t> polys, verts;
};
AdminSet admin_set(const std::vector<AdminSpec>& specs, std::mt19937& rng) {
    AdminSet s;
    for (const AdminSpec& sp : specs) {
        uint8_t r[24];
        for (auto& b : r) b = uint8_t(rng());
        if (!sp.vc) {
            wr32(r, NO);
            wr32(r + 4, 0);
        } else {
            wr32(r, uint32_t(s.verts.size()));
            wr32(r + 4, sp.vc);
            uint8_t h[10];
            for (auto& b : h) b = uint8_t(rng());
            h[0] = sp.w == 4 ? 3 : uint8_t(rng() % 3);
            s.verts.insert(s.verts.end(), h, h + 10);
            for (uint32_t k = 0; k < sp.vc; k++) {
                uint8_t v[8];
                for (auto& b : v) b = uint8_t(rng());
                s.verts.insert(s.verts.end(), v, v + 2 * sp.w);
            }
        }
        s.polys.insert(s.polys.end(), r, r + 24);
    }
    return s;
}

// Writes the set's admin_polygons.bin into dir and encodes its vertices, fed
// in odd-sized pieces.
void encode_admin(const AdminSet& s, const std::string& dir, std::mt19937& rng) {
    {
        Writer pw(dir + "/admin_polygons.bin");
        pw.put(s.polys);
        pw.finish();
    }
    poly::AdminEnc enc(dir);
    enc.state = {1};
    write_in_pieces(enc, 0, s.verts, rng, 997);
    finish_all(enc);
}

// Sets big_block() for one scope.
struct BigBlock {
    size_t saved = big_block();
    explicit BigBlock(size_t b) { big_block() = b; }
    ~BigBlock() { big_block() = saved; }
};

uint32_t f32_bits(float f) {
    uint32_t b;
    memcpy(&b, &f, 4);
    return b;
}

// Today's street (12-byte) or interpolation (24-byte) ways of 0 to 3 nodes,
// node_offset the running node count (0 for a way without nodes).
struct WaySet {
    std::vector<uint8_t> ways, nodes;
};
WaySet way_set(bool street) {
    WaySet s;
    uint32_t off = 0;
    for (uint32_t w = 0; w < 40; w++) {
        const uint16_t cnt = uint16_t(w % 4);
        std::vector<uint8_t> r = le(cnt ? off : 0, 4);
        for (uint8_t b : le(cnt, 2)) r.push_back(b);
        r.resize(8, 0);
        for (uint8_t b : le(1000 + w, 4)) r.push_back(b);  // name, or the interpolation's street
        if (!street) {
            for (uint8_t b : le(2 * w, 4)) r.push_back(b);      // start
            for (uint8_t b : le(2 * w + 9, 4)) r.push_back(b);  // end
            r.push_back(uint8_t(w % 3));
            r.resize(24, 0);
        }
        s.ways.insert(s.ways.end(), r.begin(), r.end());
        for (uint16_t j = 0; j < cnt; j++) {
            for (uint8_t b : le(f32_bits(47.0f + float(w) * 1e-4f + float(j) * 1e-5f), 4)) s.nodes.push_back(b);
            for (uint8_t b : le(f32_bits(8.0f + float(w) * 1e-4f - float(j) * 1e-5f), 4)) s.nodes.push_back(b);
        }
        off += cnt;
    }
    return s;
}

}  // namespace

TEST(gcs_way_parents_round_trip_with_no_data) {
    ScratchDir dir("gcs-test");
    std::mt19937 rng(7);
    const std::vector<uint32_t> values = {0, 1, NO, 0xFFFFFE, 12345, NO, 0x10000};
    side::U24Enc enc(dir.path());
    enc.state = {1};
    write_in_pieces(enc, 0, u32s(values), rng, 7);
    const Manifest m = finish_all(enc);
    CHECK_EQ(m.num("way_parents.n"), uint64_t(values.size()));
    CHECK_EQ(slurp(dir.path() + "/way_parents.u24").size(), 3 * values.size());
    side::U24Dec dec(dir.path(), m);
    CHECK_EQ(dec.decoded_size(0), uint64_t(4 * values.size()));
    CHECK(decode_all(dec)[0] == u32s(values));
}

TEST(gcs_way_parents_rejects_a_value_past_24_bits) {
    ScratchDir dir("gcs-test");
    side::U24Enc enc(dir.path());
    enc.state = {1};
    const std::vector<uint8_t> bytes = u32s({0xFFFFFF});
    CHECK(throws_runtime([&] { enc.write(0, bytes.data(), bytes.size()); }, "does not fit 24 bits"));
}

TEST(gcs_dictionary_round_trip_renumbers_in_value_order) {
    ScratchDir dir("gcs-test");
    std::mt19937 rng(11);
    // First seen out of value order, repeats, NO_DATA, and enough distinct
    // values to grow the table past its first size.
    std::vector<uint32_t> values = {900, 5, NO, 900, 7, 5};
    for (uint32_t i = 0; i < 5000; i++) values.push_back(100000 - (i * 7919) % 6000);
    side::DictEnc enc(dir.path(), "way_postcodes");
    enc.state = {1};
    write_in_pieces(enc, 0, u32s(values), rng, 4096);
    const Manifest m = finish_all(enc);
    const std::vector<char> dict = slurp(dir.path() + "/way_postcodes.dict");
    bool ascending = true;
    for (size_t i = 4; i + 4 <= dict.size(); i += 4)
        ascending &= rd32(reinterpret_cast<const uint8_t*>(&dict[i - 4])) < rd32(reinterpret_cast<const uint8_t*>(&dict[i]));
    CHECK(ascending);
    CHECK_EQ(m.num("way_postcodes.dict"), uint64_t(dict.size() / 4));
    side::DictDec dec(dir.path(), "way_postcodes", m);
    CHECK(decode_all(dec)[0] == u32s(values));
}

TEST(gcs_poi_round_trip_in_memory_and_by_planes) {
    // Blocks up to big_block() are laid out in memory, larger ones plane by
    // plane through positional writers (the open-ring relayout included):
    // both paths must write the same files and decode to the input.
    std::mt19937 rng(20261018);
    const PoiSet s = poi_set(rng);
    ScratchDir mem("gcs-test"), planes("gcs-test");
    Manifest m_mem, m_planes;
    {
        std::mt19937 feed(1);
        m_mem = encode_poi(s, mem.path(), feed);
    }
    {
        BigBlock small(64);
        std::mt19937 feed(2);
        m_planes = encode_poi(s, planes.path(), feed);
    }
    CHECK(m_mem.kv == m_planes.kv);
    for (const char* f : {"poi_records.r3", "poi_vertices.bopen"}) {
        const std::vector<char> a = slurp(mem.path() + "/" + f), b = slurp(planes.path() + "/" + f);
        CHECK(!a.empty());
        CHECK(a == b);
    }
    for (size_t b : {big_block(), size_t(64)}) {
        BigBlock bb(b);
        poly::PoiDec dec(mem.path(), m_mem);
        const auto out = decode_all(dec);
        CHECK(out[0] == s.recs);
        CHECK(out[1] == s.verts);
    }
}

TEST(gcs_admin_round_trip_in_memory_and_by_planes) {
    // Blocks past the default big_block() (40000 x 8 B, 70000 x 4 B and
    // 33000 x 8 B) go plane by plane (the header through the sequential
    // writer, the planes through positional writers on the shared fd, the
    // sequential writer moved past them): between small blocks, and first,
    // last and back to back. Both paths write the same files and decode to
    // the input.
    const std::vector<std::vector<AdminSpec>> orders = {
        {{0, 0}, {3, 2}, {40000, 4}, {5, 4}, {0, 0}, {70000, 2}, {1, 2}, {17, 4}, {33000, 4}, {2, 2}, {0, 0}, {9, 2}},
        {{40000, 4}, {3, 2}, {70000, 2}, {70000, 2}, {0, 0}, {33000, 4}}};
    std::mt19937 rng(20261027);
    for (const auto& specs : orders) {
        const AdminSet s = admin_set(specs, rng);
        ScratchDir mem("gcs-test"), planes("gcs-test");
        {
            std::mt19937 feed(1);
            encode_admin(s, mem.path(), feed);
        }
        {
            BigBlock small(64);
            std::mt19937 feed(2);
            encode_admin(s, planes.path(), feed);
        }
        const auto files = dir_files(mem.path());
        CHECK(!files.at("admin_vertices.B").empty());
        CHECK(files == dir_files(planes.path()));
        for (size_t b : {big_block(), size_t(64)}) {
            BigBlock bb(b);
            poly::AdminDec dec(mem.path());
            CHECK(decode_all(dec)[0] == s.verts);
        }
    }
}

TEST(gcs_addr_footprints_past_the_abs2_bound_round_trip) {
    // Footprints of more than ABS2_MAX_VERTICES vertices (70000 open, 70001
    // closed, which is 70000 once the closing vertex is dropped, 80000,
    // 120000) go through the raw escape; 69999 still fits abs2. Narrow (u8)
    // and wide (u16) values, every encoding. The encoder (fed in odd pieces)
    // writes geocoder-gc's files, and its decoder reads them back.
    std::mt19937 rng(20261027);
    struct Spec { uint32_t vc; uint8_t enc; bool closed; uint32_t span; };
    const Spec specs[] = {{0, 0, false, 0},        {70000, 0, false, 65536}, {5, 1, true, 200},          {69999, 2, false, 65536},
                          {70001, 0, true, 65536}, {0, 0, false, 0},         {70000, 1, false, 256 - 1}, {3, 3, false, 65536},
                          {80000, 3, false, 65536}, {120000, 2, true, 200}};
    std::vector<uint8_t> points, verts;
    for (const Spec& sp : specs) {
        uint32_t rec[7] = {f32_bits(float(rng() % 1800000) / 1e4f - 90.f), f32_bits(float(rng() % 3600000) / 1e4f - 180.f),
                           uint32_t(rng()), uint32_t(rng()), uint32_t(rng()), NO, 0};
        if (sp.vc) {
            rec[5] = uint32_t(verts.size());
            rec[6] = sp.vc;
            uint8_t h[10] = {sp.enc, 0};
            wr32(h + 2, f32_bits(float(rng() % 1800000) / 1e4f - 90.f));
            wr32(h + 6, f32_bits(float(rng() % 3600000) / 1e4f - 180.f));
            verts.insert(verts.end(), h, h + 10);
            const size_t vs = sp.enc == 3 ? 8 : 4, first = verts.size();
            for (uint32_t k = 0; k < sp.vc; k++) {
                uint8_t v[8];
                if (vs == 8) {
                    for (auto& b : v) b = uint8_t(rng());
                } else {
                    wr16(v, uint16_t(rng() % sp.span));
                    wr16(v + 2, uint16_t(rng() % sp.span));
                }
                if (k + 1 == sp.vc && sp.closed) memcpy(v, &verts[first], vs);
                verts.insert(verts.end(), v, v + vs);
            }
        }
        const uint8_t* r = reinterpret_cast<const uint8_t*>(rec);
        points.insert(points.end(), r, r + 28);
    }
    ScratchDir dir("gcs-test");
    addr::Enc enc(dir.path(), points.size());
    enc.state = {1, 1};
    size_t vp = 0;
    for (size_t i = 0; i < points.size() / 28; i++) {
        enc.write(0, &points[28 * i], 28);
        const uint32_t vc = rd32(&points[28 * i + 24]);
        if (!vc) continue;
        const size_t bs = 10 + size_t(vc) * (verts[vp] == 3 ? 8 : 4);
        write_in_pieces(enc, 1, &verts[vp], bs, rng, 65521);
        vp += bs;
    }
    const Manifest m = finish_all(enc);
    addr::Dec dec(dir.path(), m);
    const auto out = decode_all(dec);
    CHECK(out[0] == points);
    CHECK(out[1] == verts);
    // The files geocoder-gc encodes from the same today's files, byte for byte.
    const std::string today = dir.path() + "/today", gc = dir.path() + "/gc";
    std::filesystem::create_directories(today);
    std::filesystem::create_directories(gc);
    write_bytes(today + "/addr_points.bin", points);
    write_bytes(today + "/addr_vertices.bin", verts);
    gcset::Manifest gm;
    gcset::addr::encode(today, gc, false, gm);
    for (const char* f : {"/addr_points.col", "/addr_vertices.abs2"}) CHECK(slurp(gc + f) == slurp(dir.path() + f));
}

TEST(gcs_addr_decoder_rejects_a_footprint_count_past_its_record) {
    // One record whose footprint is an abs2 blob (tag 0x04: 2 + 2 count bytes
    // per vertex after an 8-byte bmin) or the raw escape (tag 0x20: today's
    // block, 10 + 4 count bytes), whose count must fit the record's u32.
    ScratchDir dir("gcs-test");
    write_bytes(dir.path() + "/addr_points.col", std::vector<uint8_t>(24));
    for (const uint8_t tag : {uint8_t(0x04), uint8_t(0x20)})
        for (const uint64_t count : {uint64_t(5), (uint64_t(1) << 32) + 5, (uint64_t(1) << 63) + 5}) {
            std::vector<uint8_t> bytes{tag};
            uv(bytes, count);
            bytes.resize(bytes.size() + (tag == 0x20 ? 30 : 28));
            write_bytes(dir.path() + "/addr_vertices.abs2", bytes);
            const int status = exit_status_in_child(
                [&] {
                    return error_code(
                        [&] {
                            Manifest m;
                            m.set("addr.n", 1);
                            m.set("addr.layout", "col");
                            addr::Dec dec(dir.path(), m);
                            while (dec.step()) {}
                        },
                        "bad count");
                },
                5);
            CHECK_EQ(status, count == 5 ? 0 : 2);
        }
}

TEST(gcs_addr_decoder_refuses_a_footprint_past_the_end_before_reading_it) {
    // An escaped footprint (tag 0x20) whose count fits the record's u32 but
    // not the file is refused as a truncated file, and the reader's buffer
    // never grows to the footprint: 4 MiB, or 16 GiB at the largest count
    // (each case in a child whose address space can't hold that).
    ScratchDir dir("gcs-test");
    write_bytes(dir.path() + "/addr_points.col", std::vector<uint8_t>(24));
    for (const uint64_t count : {uint64_t(1) << 20, uint64_t(0xFFFFFFFF)}) {
        std::vector<uint8_t> bytes{0x20};
        uv(bytes, count);
        bytes.resize(bytes.size() + 30);
        write_bytes(dir.path() + "/addr_vertices.abs2", bytes);
        const int status = exit_status_in_child(
            [&] {
                cap_address_space(size_t(1) << 30);
                Manifest m;
                m.set("addr.n", 1);
                m.set("addr.layout", "col");
                addr::Dec dec(dir.path(), m);
                const int code = error_code([&] { while (dec.step()) {} }, "unexpected end of file");
                return dec.A->buf.size() > READ_BUF + 64 ? 4 : code;
            },
            5);

        if (status != 2) std::printf("    count %llu: exit %d\n", (unsigned long long)count, status);
        CHECK_EQ(status, 2);
    }
}

TEST(gcs_gxc_decoder_rejects_list_and_address_counts_before_using_them) {
    // A one-row encoded set per case: malformed street list and address
    // counts fail before any list is sized or filled from them.
    const uint64_t MAX = ~uint64_t(0);  // LEB128 FF x9 01: count - 1 + 1 wraps to 0
    struct Case {
        int s, a;                      // street list state, address state of the row
        std::vector<uint64_t> street;  // gxc_street.bin varints
        std::vector<uint64_t> addr;    // address payload varints
        uint64_t addr_ids;             // next address id after the row
        const char* error;             // nullptr: decodes
    };
    const Case cases[] = {
        {1, 1, {1, 4, 0}, {2}, 2, nullptr},                                                    // a valid row
        {1, 0, {MAX, 0}, {}, 0, "list too long"},                                              // street count - 1 = 2^64 - 1
        {3, 0, {0xFFFF, 0}, {}, 0, "list too long"},                                           // street count 65536
        {0, 1, {}, {uint64_t(1) << 40}, uint64_t(1) << 40, "bad address list"},                // state 1, 2^40 ids
        {0, 1, {}, {0}, 0, "bad address list"},                                                // state 1, no ids
        {0, 2, {}, {3, 1, uint64_t(1) << 40, 0}, 3, "bad address list"},                       // state 2, index gap 2^40
        {0, 2, {}, {1, 1, 0, uint64_t(1) << 40}, 1, "bad address list"},                       // state 2, 2^40 copies
        {0, 2, {}, {1, 2, 0, 0, 0, 0}, 1, "bad address list"},                                 // state 2, more repeats than ids
        {0, 3, {}, {1, MAX, 0}, 1, "bad address list"},                                        // state 3, count - 1 = 2^64 - 1
    };
    for (const Case& c : cases) {
        ScratchDir dir("gcs-test");
        uint8_t top[32] = {};
        wr64(top, uint64_t(1) << 40);
        wr32(top + 12, NO);  // no street / interp list in effect at the block start
        wr32(top + 16, NO);
        write_bytes(dir.path() + "/gxc_top.bin", std::vector<uint8_t>(top, top + 32));
        std::vector<uint8_t> rows{uint8_t(c.s | c.a << 2)}, street;
        for (uint64_t v : c.addr) uv(rows, v);
        for (uint64_t v : c.street) uv(street, v);
        write_bytes(dir.path() + "/gxc_rows.bin", rows);
        write_bytes(dir.path() + "/gxc_street.bin", street);
        write_bytes(dir.path() + "/gxc_interp.bin", {});
        const int status = exit_status_in_child(
            [&] {
                cap_address_space(size_t(4) << 30);
                return error_code(
                    [&] {
                        Manifest m;
                        m.set("gxc.rows", 1);
                        m.set("gxc.B", 128);
                        m.set("gxc.addr_ids", c.addr_ids);
                        for (const char* e : gxc::ENTRY) m.set(std::string("gxc.") + e, 0);
                        gxc::Dec dec(dir.path(), m);
                        for (auto& st : dec.state) st = 1;
                        while (dec.step()) {}
                    },
                    c.error ? c.error : "");
            },
            20);
        CHECK_EQ(status, c.error ? 2 : 0);
    }
}

TEST(gcs_ways_decoder_rejects_codes_wider_than_the_encoder_writes) {
    // One street way of two nodes, its block 2 wf bytes then (wl + wg) bits:
    // a first coordinate past 4 bytes or deltas past 32 bits are refused
    // instead of shifting a u32 past its width; the widest valid code decodes.
    struct Case { int wf, wl, wg; bool valid; };
    const Case cases[] = {{4, 32, 32, true}, {0, 0, 0, true}, {5, 0, 0, false}, {7, 0, 0, false}, {0, 33, 0, false}, {4, 32, 63, false}};
    for (const Case& c : cases) {
        ScratchDir dir("gcs-test");
        const uint16_t cnt = 2, code = uint16_t(c.wl | c.wg << 6 | c.wf << 13);
        write_bytes(dir.path() + "/street.groups", std::vector<uint8_t>(12));
        write_bytes(dir.path() + "/street.nodes2", std::vector<uint8_t>(ways::block_size(cnt, code), 0x5A));
        write_bytes(dir.path() + "/street.cnt", le(cnt, 2));
        write_bytes(dir.path() + "/street.code", le(code, 2));
        write_bytes(dir.path() + "/street.name", le(7, 4));
        const int status = exit_status_in_child(
            [&] {
                return error_code(
                    [&] {
                        Manifest m;
                        m.set("street.ways", 1);
                        m.set("street.nodes", cnt);
                        m.set("street.G", 16);
                        ways::Dec dec(dir.path(), true, m);
                        dec.state = {1, 1};
                        while (dec.step()) {}
                    },
                    "bad code");
            },
            5);
        CHECK_EQ(status, c.valid ? 0 : 2);
    }
}

TEST(gcs_ways_decoder_rejects_bytes_past_the_end_of_a_column) {
    // A street or interpolation set with a byte appended to any one of its
    // columns fails instead of decoding and dropping it; the set as encoded
    // decodes to its input.
    for (bool street : {true, false}) {
        const WaySet s = way_set(street);
        ScratchDir enc_dir("gcs-test");
        Manifest m;
        {
            std::mt19937 rng(5);
            ways::Enc enc(enc_dir.path(), street);
            enc.state = {1, 1};
            const size_t stride = street ? 12 : 24;
            size_t np = 0;
            for (size_t w = 0; w < s.ways.size() / stride; w++) {
                write_in_pieces(enc, 0, &s.ways[stride * w], stride, rng, 5);
                const size_t nb = 8 * size_t(rd16(&s.ways[stride * w + 4]));
                if (nb) write_in_pieces(enc, 1, &s.nodes[np], nb, rng, 7);
                np += nb;
            }
            m = finish_all(enc);
        }
        {
            ways::Dec dec(enc_dir.path(), street, m);
            const auto out = decode_all(dec);
            CHECK(out[0] == s.ways);
            CHECK(out[1] == s.nodes);
        }
        const std::string fam = street ? "street" : "interp";
        const std::vector<std::string> columns = street ? std::vector<std::string>{"cnt", "code", "name"}
                                                        : std::vector<std::string>{"cntcode", "street", "start", "span", "interp"};
        for (const std::string& column : columns) {
            ScratchDir bad("gcs-test");
            std::filesystem::copy(enc_dir.path(), bad.path(),
                                  std::filesystem::copy_options::recursive | std::filesystem::copy_options::overwrite_existing);
            {
                std::ofstream f(bad.path() + "/" + fam + "." + column, std::ios::binary | std::ios::app);
                f.put('\x5a');
            }
            const int status = exit_status_in_child(
                [&] {
                    return error_code(
                        [&] {
                            ways::Dec dec(bad.path(), street, m);
                            for (auto& st : dec.state) st = 1;
                            while (dec.step())
                                for (auto& q : dec.out) q.pop(q.size());
                        },
                        "not fully consumed");
                },
                20);
            if (status != 2) std::printf("    %s.%s: exit %d\n", fam.c_str(), column.c_str(), status);
            CHECK_EQ(status, 2);
        }
    }
}

namespace {

std::vector<uint32_t> u24_values() {
    std::vector<uint32_t> v(15000);  // 45000 bytes of u24: one buffer (48 KiB), written at finish
    for (size_t j = 0; j < v.size(); j++) v[j] = uint32_t(j * 7919 % 0xFFFFFF);
    return v;
}

// 0: the error reached the catch naming the file (mode 3, no limit: every
// byte is in the file); 3: an error without the file's path; 4: no error;
// 5, 6: mode 3's file is wrong; 8: an error in mode 3.
int writer_case(int mode, const std::string& dir) {
    try {
        if (mode == 0 || mode == 3) {  // U24Enc: its last buffer goes out in finish()
            side::U24Enc enc(dir);
            const std::vector<uint32_t> v = u24_values();
            enc.state[0] = 1;
            enc.write(0, v.data(), v.size() * 4);
            enc.state[0] = 2;
            Manifest m;
            enc.finish(m);
            if (mode == 3) {
                Reader r(dir + "/way_parents.u24");
                if (r.size() != 3 * v.size()) return 5;
                for (uint32_t x : v) {
                    const uint8_t* b = r.take(3);
                    if ((uint32_t(b[0]) | uint32_t(b[1]) << 8 | uint32_t(b[2]) << 16) != x) return 6;
                }
                return 0;
            }
        } else {  // a writer whose finish() fails (1), or whose put() fails writing straight through (2)
            Writer w(dir + "/output", 48 << 10);
            std::vector<uint8_t> bytes(mode == 1 ? 40000 : 100000);
            w.put(bytes.data(), bytes.size());
            w.finish();
        }
    } catch (const std::runtime_error& e) {
        if (mode == 3) return 8;
        return std::string(e.what()).find(dir) != std::string::npos ? 0 : 3;
    }
    return 4;
}

}  // namespace

TEST(gcs_writer_errors_reach_the_caller) {
    // A failed last write of an encoder's output reaches the caller as an
    // error (the patcher's sink close, then main), instead of terminating the
    // process from a destructor. A 16 KiB file size limit stands in for a
    // full disk; mode 3 runs without one and must not fail.
    ScratchDir dir("gcs-test");
    for (int mode = 0; mode < 4; mode++) {
        const int status = exit_status_in_child(
            [&] {
                if (mode != 3) {
                    signal(SIGXFSZ, SIG_IGN);
                    struct rlimit limit{16384, 16384};
                    if (setrlimit(RLIMIT_FSIZE, &limit)) return 7;
                }
                return writer_case(mode, dir.path());
            },
            20);
        if (status != 0) std::printf("    mode %d: exit %d\n", mode, status);
        CHECK_EQ(status, 0);
    }
}
