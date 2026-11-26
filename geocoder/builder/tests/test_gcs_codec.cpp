// Unit tests for the patcher's in-process codecs (tools/gcs_codec.h):
// today's-format bytes written to an encoder in odd-sized pieces decode back
// byte for byte, and malformed encoded files fail with an error instead of
// decoding wrong bytes or exhausting memory.
#include "gcs_codec.h"

#include <cerrno>
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
using gctest::refusal_of;

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

// Every port closed and pumped, then the encoder finished.
void finish_all(Encoder& enc) {
    for (auto& s : enc.state) s = 2;
    enc.pump();
    enc.finish();
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

// For a child's exit status: 2 if body is refused with `refusal`, 3 (the
// refusal printed) if with another, 0 if it returns.
int refusal_code(const std::function<void()>& body, const std::string& refusal) {
    const std::string got = refusal_of(body);
    if (got.empty()) return 0;
    if (got == refusal) return 2;
    std::fprintf(stderr, "    unexpected refusal: %s\n", got.c_str());
    return 3;
}

// A synthetic POI set: points, small and big polygons, closed and open rings,
// both vertex widths (records of 36 bytes, vertex blocks of a 10-byte header
// and vc vertices). Polygons of 9000 to 70000 vertices span several decoder
// steps (VERTS_PER_STEP) and, when open, several 64 KiB pieces of the
// encoder's plane relayout.
struct PoiSet {
    std::vector<uint8_t> recs, verts;
};
// A POI: a point (vc 0), or a polygon of vc vertices of w u16s, its ring
// closed (the last vertex the first) or open.
struct PoiSpec {
    uint32_t vc;
    int w;
    bool closed;
};
PoiSet poi_set(const std::vector<PoiSpec>& specs, std::mt19937& rng) {
    PoiSet s;
    for (const PoiSpec& sp : specs) {
        uint8_t r[36];
        for (auto& b : r) b = uint8_t(rng());
        wr32(r + 32, rng() % 0xFFFFFF);  // parent_poly_id: R2 holds 24 bits
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
PoiSet poi_set(std::mt19937& rng) {
    return poi_set({{0, 0, false},    {3000, 2, false},  {5, 2, true},      {2000, 4, true},   {0, 0, false},
                    {7, 4, false},    {4096, 2, true},   {1, 2, false},     {2500, 4, false},  {0, 0, false},
                    {2, 2, true},     {70000, 2, false}, {17000, 2, true},  {70000, 4, false}, {17000, 4, true},
                    {9000, 2, false}, {65538, 2, false}, {65537, 4, false}, {65536, 2, false}},
                   rng);
}

// Encodes a POI set record by record (each record, then its vertex block, in
// odd-sized pieces) into dir.
void encode_poi(const PoiSet& s, const std::string& dir, std::mt19937& rng) {
    poi::Enc enc(dir);
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
    finish_all(enc);
}

// The POI files the builder writes for a set (poi_files::Writer).
std::map<std::string, std::vector<char>> poi_files_of(const PoiSet& s) {
    const poi_files::Files f = poi_files::encode(s.recs.data(), s.recs.size() / 36, s.verts.data(), s.verts.size());
    return {{"poi_records.bin", std::vector<char>(f.records.begin(), f.records.end())},
            {"poi_vertices.bin", std::vector<char>(f.vertices.begin(), f.vertices.end())}};
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

// A logical address pair (addr_files.h): the points' records, 28 bytes each
// with the vertex offset and count last, and their footprints' logical blobs
// back to back (a 10-byte header: the encoding, a zero byte, the bbox
// minimum's latitude and longitude bits; then 4 bytes a vertex, 8 in encoding
// 3). sizes: each record's footprint bytes (0: none).
struct AddrSet {
    std::vector<uint8_t> points, verts;
    std::vector<size_t> sizes;
};
// A record: a point (vc 0), a dead slot, or a footprint of vc vertices in an
// encoding, 8 random bytes each in encoding 3, else two u16 below span; a
// closed one repeats its first vertex last.
struct AddrSpec {
    uint32_t vc;
    uint8_t enc;
    bool closed;
    uint32_t span;
    bool dead = false;
};
// The pair of specs' records, the points anywhere, a few streets and parent
// ways among them. Half the footprints have their bbox minimum at their
// point, so a narrow one's goes as its residuals; the others' is anywhere.
AddrSet addr_set(const std::vector<AddrSpec>& specs, std::mt19937& rng) {
    AddrSet s;
    for (const AddrSpec& sp : specs) {
        const uint32_t lat = f32_bits(float(rng() % 1800000) / 1e4f - 90.f);
        const uint32_t lng = f32_bits(float(rng() % 3600000) / 1e4f - 180.f);
        const uint32_t street = 1000 + uint32_t(rng() % 6);
        uint32_t rec[7] = {lat, lng, uint32_t(rng() % 5000), street, rng() % 4 ? street + 7 : uint32_t(rng()), NO, 0};
        if (sp.dead) rec[0] = rec[1] = rec[2] = rec[3] = 0, rec[4] = NO;
        const size_t first = s.verts.size();
        if (sp.vc) {
            rec[5] = addr_files::vertex_offset_of(first);
            rec[6] = sp.vc;
            uint8_t h[10] = {sp.enc, 0};
            const bool at_point = rng() % 2;
            wr32(h + 2, at_point ? lat : f32_bits(float(rng() % 1800000) / 1e4f - 90.f));
            wr32(h + 6, at_point ? lng : f32_bits(float(rng() % 3600000) / 1e4f - 180.f));
            s.verts.insert(s.verts.end(), h, h + 10);
            const size_t vs = sp.enc == 3 ? 8 : 4, first_vertex = s.verts.size();
            for (uint32_t k = 0; k < sp.vc; k++) {
                uint8_t v[8];
                if (vs == 8) {
                    for (auto& b : v) b = uint8_t(rng());
                } else {
                    wr16(v, uint16_t(rng() % sp.span));
                    wr16(v + 2, uint16_t(rng() % sp.span));
                }
                if (k + 1 == sp.vc && sp.closed) memcpy(v, &s.verts[first_vertex], vs);
                s.verts.insert(s.verts.end(), v, v + vs);
            }
        }
        const uint8_t* r = reinterpret_cast<const uint8_t*>(rec);
        s.points.insert(s.points.end(), r, r + 28);
        s.sizes.push_back(s.verts.size() - first);
    }
    return s;
}

// n records: a point in five, a dead slot in seventeen, an escape in forty
// (encoding 3, of 1 to 6 vertices), the others footprints of 1 to 12 vertices
// in encodings 0 to 2, closed and open, narrow and wide.
std::vector<AddrSpec> addr_specs(size_t n, std::mt19937& rng) {
    std::vector<AddrSpec> specs(n);
    for (size_t i = 0; i < n; i++) {
        const uint32_t vc = 1 + uint32_t(rng() % 12);
        const uint32_t spans[] = {40, 300, 65536};
        specs[i] = i % 5 == 0    ? AddrSpec{0, 0, false, 0}
                   : i % 17 == 3 ? AddrSpec{0, 0, false, 0, true}
                   : i % 40 == 39 ? AddrSpec{1 + uint32_t(rng() % 6), 3, false, 0}
                                  : AddrSpec{vc, uint8_t(rng() % 3), vc >= 2 && rng() % 2 == 0, spans[rng() % 3]};
    }
    return specs;
}

// The three files the builder writes of a pair (addr_files::encode), by name.
std::map<std::string, std::vector<char>> addr_files_of(const AddrSet& s) {
    const addr_files::Files f = addr_files::encode(s.points.data(), s.points.size() / 28, s.verts.data(), s.verts.size());
    return {{addr_files::POINTS_FILE, std::vector<char>(f.points.begin(), f.points.end())},
            {addr_files::VERTICES_FILE, std::vector<char>(f.vertices.begin(), f.vertices.end())},
            {addr_files::INDEX_FILE, std::vector<char>(f.index.begin(), f.index.end())}};
}

// Encodes a pair record by record into dir, each record and its footprint in
// pieces of up to max_piece bytes, every third record's footprint before its
// record.
void encode_addrs(const AddrSet& s, const std::string& dir, std::mt19937& rng, size_t max_piece) {
    addr::Enc enc(dir);
    enc.state = {1, 1};
    size_t vp = 0;
    for (size_t i = 0; i < s.sizes.size(); i++) {
        if (i % 3 != 2) write_in_pieces(enc, 0, &s.points[28 * i], 28, rng, max_piece);
        if (s.sizes[i]) write_in_pieces(enc, 1, &s.verts[vp], s.sizes[i], rng, max_piece);
        if (i % 3 == 2) write_in_pieces(enc, 0, &s.points[28 * i], 28, rng, max_piece);
        vp += s.sizes[i];
    }
    finish_all(enc);
}

// A pair read back as the patcher reads it: each record, then its footprint
// in views of 64 KiB. Whether the decoder gave every byte and then ended.
bool read_addrs(addr::Dec& dec, const AddrSet& s, AddrSet& out) {
    for (auto& st : dec.state) st = 1;
    std::vector<char> view(size_t(64) << 10);
    auto read = [&](int port, size_t n, std::vector<uint8_t>& to) {
        while (n) {
            const size_t k = dec.read(port, view.data(), std::min(n, view.size()));
            if (!k) return false;
            to.insert(to.end(), view.data(), view.data() + k);
            n -= k;
        }
        return true;
    };
    bool complete = true;
    for (const size_t size : s.sizes) complete &= read(0, 28, out.points) && read(1, size, out.verts);
    return complete && dec.read(0, view.data(), 1) == 0 && dec.read(1, view.data(), 1) == 0;
}

// The address files of one record at 48, 11 whose blob is `blob` and then
// zeros up to len bytes (a hole in the file), their footers giving `logical`
// bytes of footprints: files no encoder writes, block and index as one would.
void write_one_address(const std::string& dir, const std::vector<uint8_t>& blob, uint32_t len, uint64_t logical) {
    addr_files::Block b;
    b.records = 1;
    b.lat[0] = f32_bits(48.0f);
    b.lng[0] = f32_bits(11.0f);
    b.len[0] = len;
    std::vector<uint8_t> points(addr_files::BLOCK_MAX_BYTES + 8);
    std::vector<uint8_t> index(addr_files::INDEX_HEAD_BYTES + addr_files::INDEX_ENTRY_BYTES, 0);
    points.resize(addr_files::encode_block(b, points.data()));
    const auto points_footer = addr_files::points_footer(1);
    const auto vertices_footer = addr_files::vertices_footer(logical, 1);
    const auto index_footer = addr_files::index_footer(1);
    points.insert(points.end(), points_footer.begin(), points_footer.end());
    index.insert(index.end(), index_footer.begin(), index_footer.end());
    write_bytes(dir + "/" + addr_files::POINTS_FILE, points);
    write_bytes(dir + "/" + addr_files::INDEX_FILE, index);
    const std::string vertices = dir + "/" + addr_files::VERTICES_FILE;
    write_bytes(vertices, blob);
    std::filesystem::resize_file(vertices, len);
    std::ofstream out(vertices, std::ios::binary | std::ios::app);
    out.write(reinterpret_cast<const char*>(vertices_footer.data()), std::streamsize(vertices_footer.size()));
}

// The families of the four side files, each <family>.bin.
const char* const SIDE_FAMILIES[] = {"way_parents", "way_postcodes", "interp_postcodes", "addr_postcodes", "addr_cities"};

// n side values in stretches of 100, which cut the blocks of 64 anywhere: one
// value, a few values in turn with NO_DATA among them (a dictionary), a
// narrow range and any u32 (both FOR).
std::vector<uint32_t> side_values(size_t n, std::mt19937& rng) {
    std::vector<uint32_t> v(n);
    for (size_t j = 0; j < n; j++) {
        const uint32_t stretch = uint32_t(j / 100);
        switch (stretch % 4) {
        case 0: v[j] = 7000 + stretch; break;
        case 1: v[j] = rng() % 4 ? 900000 * (rng() % 3) : NO; break;
        case 2: v[j] = 100000 + rng() % 500; break;
        default: v[j] = uint32_t(rng()); break;
        }
    }
    return v;
}

// The file the builder writes of a side family's values (side_blocks.h).
std::vector<char> side_file_of(const std::string& family, const std::vector<uint32_t>& values) {
    const std::vector<uint8_t> f = side_blocks::encode(family + ".bin", values.data(), values.size());
    return std::vector<char>(f.begin(), f.end());
}

// A logical way pair (way_files.h): the ways' records, 12 bytes a street and
// 24 an interpolation, and their nodes back to back, 8 bytes a node.
struct WaySet {
    std::vector<uint8_t> ways, nodes;
};

// The pair of ways of these node counts, each record's node_offset the nodes
// before it (0 of a way without nodes), its name and numbers anything. A
// way's nodes walk in steps of up to 1e-4 from a start of its own, coordinates
// of seven decimals (the hybrid key); every seventh way's are any bits (the
// order key, but for a few).
WaySet way_set(const way_files::Kind& kind, const std::vector<uint32_t>& counts, std::mt19937& rng) {
    WaySet s;
    uint32_t before = 0;
    for (size_t w = 0; w < counts.size(); w++) {
        const uint32_t cnt = counts[w];
        uint8_t r[way_files::MAX_STRIDE] = {};
        wr32(r, cnt ? before : 0);
        wr16(r + 4, uint16_t(cnt));
        wr32(r + 8, uint32_t(rng()));
        if (kind.interp) {
            const uint32_t start = uint32_t(rng());
            wr32(r + 12, start);
            wr32(r + 16, rng() % 4 ? start + uint32_t(rng() % 200) : uint32_t(rng()));
            r[20] = uint8_t(rng());
        }
        s.ways.insert(s.ways.end(), r, r + kind.stride);
        int64_t lat = int64_t(rng() % 1800000001u) - 900000000, lng = int64_t(rng() % 3600000001u) - 1800000000;
        for (uint32_t j = 0; j < cnt; j++) {
            uint8_t node[8];
            if (w % 7 == 6) {
                wr32(node, uint32_t(rng()));
                wr32(node + 4, uint32_t(rng()));
            } else {
                wr32(node, f32_bits(float(double(lat) / 1e7)));
                wr32(node + 4, f32_bits(float(double(lng) / 1e7)));
                lat += int64_t(rng() % 2001) - 1000;
                lng += int64_t(rng() % 2001) - 1000;
            }
            s.nodes.insert(s.nodes.end(), node, node + 8);
        }
        before += cnt;
    }
    return s;
}

// The node counts of n ways: 0 to 5, but 3000 of a way in forty and none in
// ways 32 to 47, a group without nodes.
std::vector<uint32_t> way_counts(size_t n, std::mt19937& rng) {
    std::vector<uint32_t> counts(n);
    for (size_t w = 0; w < n; w++) counts[w] = w >= 32 && w < 48 ? 0 : w % 40 == 39 ? 3000 : uint32_t(rng() % 6);
    return counts;
}

// The nodes' bytes of the way whose record is at r.
size_t node_bytes(const uint8_t* r) { return way_files::NODE_BYTES * size_t(rd16(r + way_files::NODE_COUNT_AT)); }

// The two files the builder writes of a pair (way_files::Encoder, a way at a
// time), by name.
std::map<std::string, std::vector<char>> way_files_of(const way_files::Kind& kind, const WaySet& s) {
    std::vector<char> rows, blocks;
    auto into = [](std::vector<char>& v) {
        return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); };
    };
    way_files::Encoder enc(kind, into(rows), into(blocks));
    size_t np = 0;
    for (size_t at = 0; at < s.ways.size(); at += kind.stride) {
        enc.add(&s.ways[at], s.nodes.data() + np);
        np += node_bytes(&s.ways[at]);
    }
    enc.finish();
    const auto ways_footer = enc.ways_footer();
    const auto nodes_footer = enc.nodes_footer();
    rows.insert(rows.end(), ways_footer.begin(), ways_footer.end());
    blocks.insert(blocks.end(), nodes_footer.begin(), nodes_footer.end());
    return {{kind.ways_file, rows}, {kind.nodes_file, blocks}};
}

// Encodes a pair way by way into dir, each way's record and its nodes in
// pieces of up to max_piece bytes, every third way's nodes before its record.
void encode_ways(const way_files::Kind& kind, const WaySet& s, const std::string& dir, std::mt19937& rng, size_t max_piece) {
    ways::Enc enc(dir, kind);
    enc.state = {1, 1};
    size_t np = 0;
    for (size_t at = 0, w = 0; at < s.ways.size(); at += kind.stride, w++) {
        const size_t nb = node_bytes(&s.ways[at]);
        if (w % 3 != 2) write_in_pieces(enc, 0, &s.ways[at], kind.stride, rng, max_piece);
        if (nb) write_in_pieces(enc, 1, &s.nodes[np], nb, rng, max_piece);
        if (w % 3 == 2) write_in_pieces(enc, 0, &s.ways[at], kind.stride, rng, max_piece);
        np += nb;
    }
    finish_all(enc);
}

const way_files::Kind* const WAY_KINDS[] = {&way_files::STREETS, &way_files::INTERPS};

}  // namespace

TEST(gcs_side_blocks_round_trip_in_odd_pieces_and_equal_the_builders_encoder) {
    // Values written in pieces that cut them anywhere (of 1 to 7 bytes, and of
    // up to 5000) give the builder's file, whatever their count: none (the
    // footer alone), one, a block less one, a block, a block and one, and
    // many blocks of every kind. The table's scratch file is gone from the
    // dir. The decoder gives the values back, and the decoded size is the
    // footer's without a decoder. Each of the four side files in turn.
    std::mt19937 rng(20261114);
    size_t k = 0;
    for (const size_t n : {size_t(0), size_t(1), size_t(63), size_t(64), size_t(65), size_t(5000)})
        for (const size_t max_piece : {size_t(7), size_t(5000)}) {
            const std::string family = SIDE_FAMILIES[k++ % std::size(SIDE_FAMILIES)];
            const std::vector<uint32_t> values = side_values(n, rng);
            ScratchDir dir("gcs-test");
            side::Enc enc(dir.path(), family);
            enc.state = {1};
            write_in_pieces(enc, 0, u32s(values), rng, max_piece);
            finish_all(enc);

            CHECK(dir_files(dir.path()) == (std::map<std::string, std::vector<char>>{{family + ".bin", side_file_of(family, values)}}));
            CHECK_EQ(side::decoded_size(dir.path(), family), uint64_t(4 * n));
            side::Dec dec(dir.path(), family);
            CHECK_EQ(dec.decoded_size(0), uint64_t(4 * n));
            CHECK(decode_all(dec)[0] == u32s(values));
        }
}

TEST(gcs_side_decoder_takes_a_dir_without_its_file_as_no_values) {
    // A side file is written only for a day that has a value: a dir without
    // it decodes to none, beside another family's file.
    ScratchDir dir("gcs-test");
    write_bytes(dir.path() + "/way_parents.bin", std::vector<uint8_t>(side_file_of("way_parents", {1, 2, 3}).size(), 0x5A));

    CHECK_EQ(side::decoded_size(dir.path(), "way_postcodes"), uint64_t(0));
    side::Dec dec(dir.path(), "way_postcodes");
    CHECK_EQ(dec.decoded_size(0), uint64_t(0));
    CHECK(decode_all(dec)[0].empty());
}

TEST(gcs_side_decoder_refuses_a_malformed_block_or_table) {
    // Three blocks: 64 values of 7 (FOR, 6 bytes at 0), 64 of NO_DATA and 5 in
    // turn (a dictionary, 19 bytes at 6) and 22 from 1000 up (FOR at 5 bits,
    // 20 bytes at 25); their table at 45 and the footer at 57. The file
    // decodes as written. With a block where the table does not put it, a
    // byte between the last block and the table, a block the encoder would
    // not write or cut short, or a footer of another file or count, it is
    // refused by name, by the decoder and, for the footer, by the decoded
    // size alone.
    std::vector<uint32_t> values(64, 7);
    for (uint32_t j = 0; j < 64; j++) values.push_back(j % 2 ? 5 : NO);
    for (uint32_t j = 0; j < 22; j++) values.push_back(1000 + j);
    const std::vector<char> written = side_file_of("way_postcodes", values);
    REQUIRE(written.size() == 65 && rd32(reinterpret_cast<const uint8_t*>(&written[49])) == 6);
    struct Case {
        std::function<void(std::vector<uint8_t>&)> edit;
        const char* refusal;  // nullptr: decodes
        bool footer = false;
    };
    const Case cases[] = {
        {[](std::vector<uint8_t>&) {}, nullptr},
        {[](std::vector<uint8_t>& f) { f[49] = 7; }, "block 1 is not where the blocks before it end"},
        {[](std::vector<uint8_t>& f) { f.insert(f.begin() + 45, 0); }, "bytes between its blocks and its table"},
        {[](std::vector<uint8_t>& f) { f.erase(f.begin() + 44); }, "block 2 runs past the blocks' end"},
        {[](std::vector<uint8_t>& f) { f[0] = 2; }, "block 0 is neither FOR nor a dictionary"},
        {[](std::vector<uint8_t>& f) { f[17] = 0x55; }, "block 1 has an index out of first-use order, or past its dictionary"},
        {[](std::vector<uint8_t>& f) { f[44] |= 0x80; }, "block 2 has bits set past its last value"},
        {[](std::vector<uint8_t>& f) { f.back() = '2'; }, "no SDB1 in its footer", true},
        {[](std::vector<uint8_t>& f) { f.resize(7); }, "shorter than its footer", true},
        {[](std::vector<uint8_t>& f) { wr32(&f[57], 100000); }, "shorter than its block table", true},
    };
    for (const Case& c : cases) {
        ScratchDir dir("gcs-test");
        std::vector<uint8_t> file(written.begin(), written.end());
        c.edit(file);
        write_bytes(dir.path() + "/way_postcodes.bin", file);
        const std::string refusal = c.refusal ? std::string("Malformed way_postcodes.bin: ") + c.refusal : "";
        std::vector<uint8_t> decoded;

        CHECK_EQ(refusal_of([&] {
                     side::Dec dec(dir.path(), "way_postcodes");
                     decoded = decode_all(dec)[0];
                 }),
                 refusal);
        CHECK_EQ(refusal_of([&] { side::decoded_size(dir.path(), "way_postcodes"); }), c.footer ? refusal : "");
        if (!c.refusal) CHECK(decoded == u32s(values));
    }
}

TEST(gcs_side_encoder_refuses_a_stream_ending_inside_a_value) {
    // A full replacement whose payload is not whole u32s: refused at the
    // finish, not written as a file of the whole ones.
    ScratchDir dir("gcs-test");
    side::Enc enc(dir.path(), "way_parents");
    enc.state = {1};
    const std::vector<uint8_t> bytes = u32s({1, 2, 3});
    enc.write(0, bytes.data(), bytes.size() - 1);

    CHECK_EQ(refusal_of([&] { finish_all(enc); }), std::string("gcs: way_parents.bin: the stream ends 3 bytes into a value"));
}

TEST(gcs_side_codec_holds_one_block) {
    // 4 MiB of values in one write (as the patcher's FILE passes a full
    // replacement), read back as the patcher reads them (64 KiB views). The
    // encoder queues none of the write and keeps no table entry of a block it
    // has written; the decoder's queue holds what a read asks ahead and one
    // block's values, and its readers their buffers. The file is the
    // builder's.
    std::mt19937 rng(20261114);
    const std::vector<uint32_t> values = side_values(size_t(1) << 20, rng);
    const std::vector<uint8_t> bytes = u32s(values);
    ScratchDir dir("gcs-test");
    side::Enc enc(dir.path(), "addr_postcodes");
    enc.state = {1};
    enc.write(0, bytes.data(), bytes.size());
    const size_t encoder_peak = enc.peak(0), table_held = enc.blocks.table().size();
    finish_all(enc);

    side::Dec dec(dir.path(), "addr_postcodes");
    dec.state = {1};
    std::vector<uint8_t> out;
    std::vector<char> view(size_t(64) << 10);
    size_t readers = 0;
    for (size_t got; (got = dec.read(0, view.data(), view.size())) != 0;) {
        out.insert(out.end(), view.data(), view.data() + got);
        if (dec.B) readers = std::max(readers, dec.B->buf.size() + dec.T->buf.size());
    }
    std::printf("    %zu values: encoder queue peak %zu B, decoder queue peak %zu B, readers %zu B\n", values.size(), encoder_peak,
                dec.peak(0), readers);

    CHECK_EQ(encoder_peak, size_t(0));
    CHECK_EQ(table_held, size_t(0));
    CHECK(dec.peak(0) < Decoder::READ_AHEAD + 4 * side_blocks::BLOCK_VALUES);
    CHECK(readers > 0 && readers <= side::BLOCKS_BUF + side::TABLE_BUF + 2 * READ_SLACK);
    CHECK(out == bytes);
    CHECK(slurp(dir.path() + "/addr_postcodes.bin") == side_file_of("addr_postcodes", values));
}

TEST(gcs_poi_round_trip_whole_and_in_pieces) {
    // Blocks up to big_block() are encoded and decoded whole, larger ones a
    // piece at a time (the encoder's packed in place in the new file): both
    // ways write the builder's files and decode to the input.
    std::mt19937 rng(20261018);
    const PoiSet s = poi_set(rng);
    ScratchDir whole("gcs-test"), pieces("gcs-test");
    {
        std::mt19937 feed(1);
        encode_poi(s, whole.path(), feed);
    }
    {
        BigBlock small(64);
        std::mt19937 feed(2);
        encode_poi(s, pieces.path(), feed);
    }
    CHECK(dir_files(whole.path()) == poi_files_of(s));
    CHECK(dir_files(pieces.path()) == poi_files_of(s));
    for (size_t b : {big_block(), size_t(64)}) {
        BigBlock bb(b);
        poi::Dec dec(whole.path());
        const auto out = decode_all(dec);
        CHECK(out[0] == s.recs);
        CHECK(out[1] == s.verts);
    }
}

TEST(gcs_poi_decoder_refuses_blocks_the_encoder_never_writes) {
    // One polygon record whose block is all of poi_vertices.bin but its pad:
    // a block of no stored vertex, one of 2^32 stored vertices (more than the
    // record's u32 vertex_count holds), one whose vertices run past the
    // file, and one of 65,535 stored vertices of no bits (a closed block, all
    // equal: past big_block(), in 8 bytes of the file) whose logical bytes run
    // past the footer's 14. Refused by the header before the block is sized
    // or the record emitted; a block of one vertex decodes.
    struct Case {
        std::vector<uint8_t> block;
        const char* error;  // nullptr: decodes
    };
    const Case cases[] = {{{0, 1, 0, 0, 1, 2, 0b101}, nullptr},
                          {{0, 0, 0, 0, 16, 16}, "Malformed POI files: a block of no vertices"},
                          {{0, 0x80, 0x80, 0x80, 0x80, 0x10, 0, 0, 16, 16}, "Malformed POI files: a block of more vertices than a record holds"},
                          {{0, 0xFF, 0xFF, 0x03, 0, 0, 16, 16}, "Malformed POI files: row 0: a block past the end of poi_vertices.bin"},
                          {{4, 0xFF, 0xFF, 0x03, 0, 0, 0, 0}, "Malformed POI files: row 0: a block past the footer's vertex stream of 14 bytes"}};
    for (const Case& c : cases) {
        ScratchDir dir("gcs-test");
        std::vector<uint8_t> row(27, 0), tail;
        wr32(&row[12], 7);                          // name_id
        row[20] = row[21] = row[22] = 0xFF;         // no parent polygon
        row[23] = row[24] = 0xFF;                   // no postcode
        for (uint8_t b : {0, 0, 0, 0, 1}) tail.push_back(b);  // the one combo: a polygon
        for (uint8_t b : le(1, 4)) tail.push_back(b);
        for (uint8_t b : le(1, 4)) tail.push_back(b);
        for (uint8_t b : le(0, 4)) tail.push_back(b);
        for (uint8_t b : le(14, 8)) tail.push_back(b);
        for (uint8_t b : {'P', 'O', 'R', '2'}) tail.push_back(b);
        row.insert(row.end(), tail.begin(), tail.end());
        write_bytes(dir.path() + "/poi_records.bin", row);
        std::vector<uint8_t> v = c.block;
        v.resize(v.size() + 8, 0);
        write_bytes(dir.path() + "/poi_vertices.bin", v);
        poi::Dec dec(dir.path());
        dec.state = {1, 1};

        if (!c.error) {
            CHECK(decode_all(dec)[1].size() == 14);
            continue;
        }
        CHECK_EQ(refusal_of([&] { dec.step(); }), std::string(c.error));
        CHECK(dec.out[0].size() == 0);
    }
}

TEST(gcs_encoder_queues_one_large_write_a_piece_at_a_time) {
    // poi_vertices.bin of 1 MiB and of 10 MiB, each in one write after the
    // records (as the patcher's FILE passes a long write), of small blocks,
    // blocks just under big_block() (taken whole) and just past it (packed in
    // place): at both sizes the vertices' queue peaks at no more than a piece
    // of the write (64 KiB) and a block taken whole (256 KiB), and the files
    // are those of a write in odd-sized pieces. The patch's file sizes are
    // given, so a unit is checked against the bytes left from where it starts.
    const uint32_t whole = uint32_t((big_block() - 10) / 8), in_place = whole + 2;
    std::mt19937 rng(20261030);
    for (const size_t mib : {1, 10}) {
        std::vector<PoiSpec> specs;
        for (size_t k = 0, bytes = 0; bytes < (mib << 20); k++) {
            const PoiSpec sp = k % 64 == 7    ? PoiSpec{whole, 4, false}
                               : k % 64 == 40 ? PoiSpec{in_place, 4, k % 128 == 40}
                                              : PoiSpec{1 + uint32_t(rng() % 500), k % 2 ? 4 : 2, k % 3 == 0};
            specs.push_back(sp);
            bytes += 10 + size_t(sp.vc) * 2 * size_t(sp.w);
        }
        const PoiSet s = poi_set(specs, rng);
        ScratchDir one("gcs-test"), pieces("gcs-test");
        poi::Enc enc(one.path());
        enc.state = {1, 1};
        enc.expect = {s.recs.size(), s.verts.size()};
        enc.write(0, s.recs.data(), s.recs.size());
        enc.write(1, s.verts.data(), s.verts.size());
        std::printf("    %zu MiB in one write: queue peak %zu B\n", mib, enc.peak(1));
        CHECK(enc.peak(1) <= size_t(320) << 10);
        finish_all(enc);
        std::mt19937 feed(1);
        encode_poi(s, pieces.path(), feed);
        CHECK(dir_files(one.path()) == dir_files(pieces.path()));
    }
}

TEST(gcs_addr_round_trip_in_odd_pieces_and_equal_the_builders_encoder) {
    // Address points written a record and its footprint at a time, in pieces
    // that cut them anywhere (of 1 to 7 bytes, and of up to 5000), give the
    // builder's three files, whatever their count: none (the three footers
    // alone), one, a block less one, a block, a block and one, and two
    // superblocks, with points, dead slots, footprints of every encoding and
    // escapes among them. The decoder gives the pair back, and the decoded
    // sizes are the footers' without a decoder.
    std::mt19937 rng(20261114);
    for (const size_t n : {size_t(0), size_t(1), size_t(15), size_t(16), size_t(17), size_t(1100)})
        for (const size_t max_piece : {size_t(7), size_t(5000)}) {
            const AddrSet s = addr_set(addr_specs(n, rng), rng);
            ScratchDir dir("gcs-test");
            encode_addrs(s, dir.path(), rng, max_piece);

            const auto files = dir_files(dir.path());
            CHECK(files == addr_files_of(s));
            if (n == 0)
                CHECK(files.at("addr_points.bin").size() == 8 && files.at("addr_vertices.bin").size() == 16 &&
                      files.at("addr_index.bin").size() == 8);
            CHECK_EQ(addr::decoded_size(dir.path(), 0), uint64_t(s.points.size()));
            CHECK_EQ(addr::decoded_size(dir.path(), 1), uint64_t(s.verts.size()));
            addr::Dec dec(dir.path());
            CHECK_EQ(dec.decoded_size(0), uint64_t(s.points.size()));
            CHECK_EQ(dec.decoded_size(1), uint64_t(s.verts.size()));
            const auto out = decode_all(dec);
            CHECK(out[0] == s.points);
            CHECK(out[1] == s.verts);
        }
}

TEST(gcs_addr_decoder_takes_a_dir_without_its_files_as_no_addresses_and_refuses_part_of_them) {
    // A variant without addresses has none of the three files: it decodes to
    // no records, beside another family's file. A dir with two of the three
    // is refused by name, by the decoder and by the decoded size alone.
    std::mt19937 rng(20261114);
    ScratchDir dir("gcs-test");
    write_bytes(dir.path() + "/addr_postcodes.bin", std::vector<uint8_t>(40, 0x5A));

    for (int port = 0; port < 2; port++) CHECK_EQ(addr::decoded_size(dir.path(), port), uint64_t(0));
    addr::Dec none(dir.path());
    CHECK_EQ(none.decoded_size(0) + none.decoded_size(1), uint64_t(0));
    const auto out = decode_all(none);
    CHECK(out[0].empty() && out[1].empty());

    const std::string refusal =
        "Malformed address files: a dir with some of addr_points.bin, addr_vertices.bin and addr_index.bin and not all three";
    for (const char* gone : {"addr_points.bin", "addr_vertices.bin", "addr_index.bin"}) {
        ScratchDir part("gcs-test");
        encode_addrs(addr_set(addr_specs(20, rng), rng), part.path(), rng, 100);
        REQUIRE(std::filesystem::remove(part.path() + "/" + gone));

        CHECK_EQ(refusal_of([&] { addr::Dec dec(part.path()); }), refusal);
        CHECK_EQ(refusal_of([&] { addr::decoded_size(part.path(), 0); }), refusal);
    }
}

TEST(gcs_addr_codec_refuses_a_footprint_stream_past_the_limit_by_the_footers_alone) {
    // The logical addr_vertices.bin is a stream a patch indexes by its 2-byte
    // units in a u32 below NO_DATA, as a vertex_offset does. Files whose
    // footer gives a unit more are refused by the file's name and the bytes,
    // by the decoded size and by the decoder as it is made: before a block is
    // read, though the first is one no encoder writes. At the limit the
    // footer passes, and the decoder refuses that first block.
    std::mt19937 rng(20261114);
    ScratchDir dir("gcs-test");
    encode_addrs(addr_set(addr_specs(20, rng), rng), dir.path(), rng, 100);
    std::vector<char> points = slurp(dir.path() + "/addr_points.bin"), vertices = slurp(dir.path() + "/addr_vertices.bin");
    points[0] = char(0xFF);
    write_bytes(dir.path() + "/addr_points.bin", std::vector<uint8_t>(points.begin(), points.end()));
    auto with_logical_bytes = [&](uint64_t bytes) {
        wr64(reinterpret_cast<uint8_t*>(&vertices[vertices.size() - 16]), bytes);
        write_bytes(dir.path() + "/addr_vertices.bin", std::vector<uint8_t>(vertices.begin(), vertices.end()));
    };
    const std::string past =
        "Malformed address files: a logical addr_vertices.bin of 8589934590 bytes is more than the 8589934588 a patch can index";

    with_logical_bytes(0x1FFFFFFFEull);
    CHECK_EQ(refusal_of([&] { addr::decoded_size(dir.path(), 1); }), past);
    CHECK_EQ(refusal_of([&] { addr::Dec dec(dir.path()); }), past);

    with_logical_bytes(0x1FFFFFFFCull);
    CHECK_EQ(addr::decoded_size(dir.path(), 1), uint64_t(0x1FFFFFFFCull));
    CHECK_EQ(refusal_of([&] {
                 addr::Dec dec(dir.path());
                 decode_all(dec);
             }),
             std::string("Malformed address files: addr_points.bin block 0 has a flag no block has"));
}

TEST(gcs_addr_decoder_refuses_what_the_strict_decoder_refuses) {
    // The three files of 40 records, damaged 600 times: bytes of the blocks,
    // of the blobs, of the index or of a footer overwritten, a file's first
    // byte cut, a byte added or cut before a footer, a file gone or its last
    // byte cut, a block or a superblock a byte further in the index, a byte or
    // a blob more in the footer's counts, and every twelfth time nothing. The
    // decoder, which reads a record at a time, refuses each with the message
    // of addr_files::decode, which reads the three files whole, and gives
    // that one's pair where it takes the files. The decoded size alone
    // refuses what the three footers show.
    std::mt19937 rng(20261114);
    auto u8 = [](const std::vector<char>& v) { return std::vector<uint8_t>(v.begin(), v.end()); };
    const AddrSet s = addr_set(addr_specs(40, rng), rng);
    const auto written = addr_files_of(s);
    const std::vector<uint8_t> blocks = u8(written.at("addr_points.bin")), blobs = u8(written.at("addr_vertices.bin")),
                               rows = u8(written.at("addr_index.bin"));
    size_t refused = 0, taken = 0, by_footers = 0, differ = 0;
    for (int round = 0; round < 600; round++) {
        std::vector<uint8_t> p = blocks, v = blobs, x = rows;
        bool has[3] = {true, true, true};
        auto overwrite = [&](std::vector<uint8_t>& f) { f[rng() % f.size()] = uint8_t(rng()); };
        switch (round % 12) {
        case 0: overwrite(p); break;
        case 1: overwrite(v); break;
        case 2: overwrite(x); break;
        case 3:
            for (int k = 0; k < 3; k++) overwrite(rng() % 3 == 0 ? p : rng() % 2 ? v : x);
            break;
        case 4: {
            std::vector<uint8_t>& f = rng() % 2 ? p : v;
            f.erase(f.begin());
            break;
        }
        case 5:
            if (round % 24 < 12) v.insert(v.end() - 16, uint8_t(rng()));
            else v.erase(v.end() - 17);
            break;
        case 6:
            if (round % 24 < 12) p.insert(p.end() - 8, uint8_t(rng()));
            else p.erase(p.end() - 9);
            break;
        case 7: {
            const size_t k = rng() % 3;
            if (round % 24 < 12) has[k] = false;
            else (k == 0 ? p : k == 1 ? v : x).pop_back();
            break;
        }
        case 8: x[16 + 6 * (1 + rng() % 2)]++; break;
        case 9: x[round % 24 < 12 ? 0 : 8]++; break;
        case 10: v[v.size() - (round % 24 < 12 ? 16 : 8)]++; break;
        default: break;
        }
        ScratchDir dir("gcs-test");
        if (has[0]) write_bytes(dir.path() + "/addr_points.bin", p);
        if (has[1]) write_bytes(dir.path() + "/addr_vertices.bin", v);
        if (has[2]) write_bytes(dir.path() + "/addr_index.bin", x);
        const uint64_t ps = has[0] ? p.size() : addr_files::ABSENT, vs = has[1] ? v.size() : addr_files::ABSENT,
                       xs = has[2] ? x.size() : addr_files::ABSENT;

        AddrSet whole, stepped;
        const std::string strict = refusal_of([&] {
            addr_files::decode(
                p.data(), ps, v.data(), vs, x.data(), xs,
                [&](const uint8_t* record) { whole.points.insert(whole.points.end(), record, record + 28); },
                [&](const uint8_t* q, size_t n) { whole.verts.insert(whole.verts.end(), q, q + n); });
        });
        const std::string ours = refusal_of([&] {
            addr::Dec dec(dir.path());
            auto out = decode_all(dec);
            stepped.points = std::move(out[0]);
            stepped.verts = std::move(out[1]);
        });
        // Each file's footer, or all of a shorter file.
        auto end_of = [](const std::vector<uint8_t>& f, size_t footer) {
            return f.data() + (f.size() - std::min(f.size(), footer));
        };
        const std::string footers =
            refusal_of([&] { addr_files::read_footers(end_of(p, 8), ps, end_of(v, 16), vs, end_of(x, 8), xs); });
        const std::string sized = refusal_of([&] { addr::decoded_size(dir.path(), 0); });

        const bool same = ours == strict && sized == footers &&
                          (!strict.empty() || (stepped.points == whole.points && stepped.verts == whole.verts));
        if (!same && differ++ < 3)
            std::printf("    round %d: decode \"%s\", the codec \"%s\"; footers \"%s\", the size \"%s\"\n", round, strict.c_str(),
                        ours.c_str(), footers.c_str(), sized.c_str());
        refused += !strict.empty();
        taken += strict.empty();
        by_footers += !footers.empty();
        if (round % 12 == 11) CHECK(strict.empty() && stepped.points == s.points && stepped.verts == s.verts);
        if (round % 12 == 8 || round % 12 == 9) CHECK(!strict.empty() && footers.empty());
    }
    std::printf("    %zu refused (%zu by the footers), %zu taken\n", refused, by_footers, taken);
    CHECK_EQ(differ, size_t(0));
    CHECK(refused > 300 && taken >= 50 && by_footers >= 50);
}

TEST(gcs_addr_decoder_refuses_a_blob_that_is_not_its_len_before_reading_it) {
    // One record whose blob the block gives a gigabyte, most of it a hole in
    // the file: a footprint of five vertices whose widths give it 21 bytes,
    // an escape whose count gives it 36, and an escape whose count is past a
    // record's u32. Each is refused from the blob's first bytes, the reader's
    // buffer never growing to the len (each case in a child whose address
    // space can't hold that). The same footprints at their own len decode.
    const uint32_t gigabyte = uint32_t(1) << 30;
    struct Case {
        std::vector<uint8_t> blob;
        uint32_t len;
        uint64_t logical;
        const char* refusal;  // nullptr: decodes
    };
    // Five vertices of 8 + 8 bits after an 8-byte bbox minimum, any values but
    // a last vertex that is the first; and three 8-byte vertices raw.
    std::vector<uint8_t> five = {0, 5}, escape = {addr_files::ESCAPE_TAG, 3, 3, 0}, wide = {addr_files::ESCAPE_TAG};
    for (const uint8_t b : le(0x42400000, 4)) five.push_back(b);
    for (const uint8_t b : le(0x41300000, 4)) five.push_back(b);
    five.push_back(0x88);
    for (const uint8_t q : {200, 3, 255, 4, 5, 6, 7, 128, 9, 10}) five.push_back(q);
    escape.resize(4 + 8 + 24, 7);
    uv(wide, (uint64_t(1) << 32) + 5);
    wide.resize(wide.size() + 40, 0);
    const Case cases[] = {
        {five, uint32_t(five.size()), 30, nullptr},
        {five, gigabyte, 30, "a footprint blob that is not its record's len"},
        {escape, uint32_t(escape.size()), 34, nullptr},
        {escape, gigabyte, 34, "a footprint blob that is not its record's len"},
        {wide, gigabyte, 34, "an escaped footprint whose count is not a varint of 1 to 4294967295 vertices"},
    };
    for (const Case& c : cases) {
        ScratchDir dir("gcs-test");
        write_one_address(dir.path(), c.blob, c.len, c.logical);
        const std::string refusal =
            c.refusal ? std::string("Malformed address files: ") + c.refusal + ": addr_vertices.bin, record 0" : std::string();
        const int status = exit_status_in_child(
            [&] {
                cap_address_space(size_t(256) << 20);
                addr::Dec dec(dir.path());
                for (auto& st : dec.state) st = 1;
                size_t reader = 0;
                const int code = refusal_code(
                    [&] {
                        while (dec.step()) reader = std::max(reader, dec.V ? dec.V->buf.size() : 0);
                    },
                    refusal);
                if (dec.V) reader = std::max(reader, dec.V->buf.size());
                if (reader > addr::BLOBS_BUF + READ_SLACK) return 4;
                if (!c.refusal && dec.out[1].size() != c.logical) return 6;
                return code;
            },
            20);

        if (status != (c.refusal ? 2 : 0))
            std::printf("    a blob of %zu bytes at len %u: exit %d\n", c.blob.size(), c.len, status);
        CHECK_EQ(status, c.refusal ? 2 : 0);
    }
}

TEST(gcs_addr_codecs_copy_an_escaped_footprint_through_in_pieces) {
    // An escaped footprint in encoding 3 of 80K and of 800K vertices, 640 KB
    // and 6.4 MB, and one escaped for a vertex more than a blob holds (65,536
    // in encoding 1, 262 KB), among small ones, each stream in one write (as
    // the patcher's FILE passes a long stretch) and read back as the patcher
    // reads them (a record, then its footprint in 64 KiB views): at both
    // sizes the encoder's queue holds at most a piece and the decoder's what
    // a read asks ahead and a piece of the blobs' buffer, each with the
    // largest footprint here either takes whole (the one of 12 vertices, 10
    // + 4 x 12 B); the files are the builder's and decode back to the same
    // bytes.
    const size_t whole = 10 + 4 * 12;
    std::mt19937 rng(20261030);
    for (const uint32_t big : {80000u, 800000u}) {
        const AddrSet s = addr_set({{5, 1, true, 200},
                                    {big, 3, false, 0},
                                    {0, 0, false, 0},
                                    {uint32_t(addr_files::MAX_BLOB_VERTICES) + 1, 1, false, 65536},
                                    {12, 2, false, 65536},
                                    {9, 0, true, 256 - 1}},
                                   rng);
        ScratchDir dir("gcs-test");
        addr::Enc enc(dir.path());
        enc.state = {1, 1};
        enc.write(0, s.points.data(), s.points.size());
        enc.write(1, s.verts.data(), s.verts.size());
        const size_t encoder_peak = std::max(enc.peak(0), enc.peak(1));
        finish_all(enc);

        addr::Dec dec(dir.path());
        AddrSet out;
        const bool complete = read_addrs(dec, s, out);
        const size_t decoder_peak = std::max(dec.peak(0), dec.peak(1));
        std::printf("    %u vertices: encoder queue peak %zu B, decoder queue peak %zu B\n", big, encoder_peak, decoder_peak);

        CHECK(encoder_peak <= ENCODER_PIECE + whole);
        CHECK(decoder_peak <= Decoder::READ_AHEAD + addr::BLOBS_BUF + whole);
        CHECK(complete);
        CHECK(out.points == s.points);
        CHECK(out.verts == s.verts);
        CHECK(dir_files(dir.path()) == addr_files_of(s));
    }
}

TEST(gcs_addr_codec_holds_a_piece_and_one_footprint_of_the_most_vertices) {
    // Footprints of the most vertices a blob holds (65,535: 262,150 B), four
    // of them in one block, in one write of each stream (as the patcher's
    // FILE passes an INSERT stretch), which the encoder takes a piece (64
    // KiB) at a time: its queue peaks at no more than a piece and one such
    // footprint, which it takes whole. Read back as the patcher reads them (a
    // record, then its footprint in 64 KiB views), the decoder's queue holds
    // what a read asks ahead and one footprint, not its block's four, and its
    // blob reader the largest blob. The files are the builder's.
    const uint32_t most = uint32_t(addr_files::MAX_BLOB_VERTICES);
    const size_t largest = 10 + 4 * size_t(most);
    std::mt19937 rng(20261030);
    const AddrSet s = addr_set({{5000, 1, false, 65536},
                                {6914, 2, true, 200},
                                {most, 0, false, 65536},
                                {0, 0, false, 0},
                                {most, 1, true, 256 - 1},
                                {most, 2, false, 65536},
                                {most, 0, true, 65536},
                                {3, 0, false, 40}},
                               rng);
    REQUIRE(s.sizes[2] == largest);
    ScratchDir dir("gcs-test");
    addr::Enc enc(dir.path());
    enc.state = {1, 1};
    enc.write(0, s.points.data(), s.points.size());
    enc.write(1, s.verts.data(), s.verts.size());
    const size_t encoder_records = enc.peak(0), encoder_footprints = enc.peak(1);
    finish_all(enc);

    addr::Dec dec(dir.path());
    dec.state = {1, 1};
    AddrSet out;
    std::vector<char> view(size_t(64) << 10);
    size_t reader = 0;
    auto read = [&](int port, size_t n, std::vector<uint8_t>& to) {
        while (n) {
            const size_t k = dec.read(port, view.data(), std::min(n, view.size()));
            if (!k) return false;
            if (dec.V) reader = std::max(reader, dec.V->buf.size());
            to.insert(to.end(), view.data(), view.data() + k);
            n -= k;
        }
        return true;
    };
    bool complete = true;
    for (const size_t size : s.sizes) complete &= read(0, 28, out.points) && read(1, size, out.verts);
    std::printf("    %zu B of footprints: encoder queue peaks %zu %zu B, decoder queue peaks %zu %zu B, blob reader %zu B\n",
                s.verts.size(), encoder_records, encoder_footprints, dec.peak(0), dec.peak(1), reader);

    CHECK(encoder_records <= s.points.size());
    CHECK(encoder_footprints >= largest && encoder_footprints <= ENCODER_PIECE + largest);
    CHECK(dec.peak(0) <= Decoder::READ_AHEAD);
    CHECK(dec.peak(1) >= largest && dec.peak(1) <= Decoder::READ_AHEAD + largest);
    CHECK(reader > largest / 2 && reader <= addr_files::BLOB_MAX_BYTES + 8 + READ_SLACK);
    CHECK(complete);
    CHECK(dec.read(0, view.data(), 1) == 0);
    CHECK(out.points == s.points);
    CHECK(out.verts == s.verts);
    CHECK(dir_files(dir.path()) == addr_files_of(s));
}

TEST(gcs_addr_encoder_refuses_a_stream_that_is_not_whole_records_and_their_footprints) {
    // Four records: a point, a footprint of two vertices, a footprint of
    // three, and an escape of three in encoding 3. A record whose
    // vertex_offset is not the footprints' units before it, and a footprint
    // in an encoding none has or with its zero byte set, are refused as they
    // come, by the record, the footprint's vertices unread; a stream that
    // ends inside a footprint, inside an escaped one (which the encoder took
    // as it came, its record already in the block) or inside a record, or
    // holds a byte past the last footprint, at the finish, not written as
    // shorter files.
    std::mt19937 rng(20261114);
    const AddrSet s = addr_set({{0, 0, false, 0}, {2, 0, false, 300}, {3, 1, true, 40}, {3, 3, false, 0}}, rng);
    REQUIRE(s.verts.size() == 18 + 22 + 34);
    struct Case {
        size_t points, verts;                // the bytes of each stream written
        std::function<void(AddrSet&)> edit;  // of the pair, before it is written
        const char* refusal;
    };
    const Case cases[] = {
        {112, 74, [](AddrSet&) {}, ""},
        {112, 74, [](AddrSet& e) { wr32(&e.points[2 * 28 + 20], 10); },
         "Malformed address files: addr_points.bin record 2 has a vertex_offset of 10, not the 9 units of 2 bytes of the "
         "footprints before it"},
        {112, 28, [](AddrSet& e) { e.verts[18] = 4; },
         "Malformed address files: addr_points.bin record 2 has a footprint in encoding 4"},
        {112, 28, [](AddrSet& e) { e.verts[19] = 1; },
         "Malformed address files: addr_points.bin record 2 has a footprint with its pad byte set"},
        {112, 39, [](AddrSet&) {}, "gcs: addr: 56 bytes left over on input 0"},
        {112, 73, [](AddrSet&) {}, "gcs: addr_vertices.bin: the stream ends 1 bytes before its last footprint does"},
        {111, 40, [](AddrSet&) {}, "gcs: addr: 27 bytes left over on input 0"},
        {84, 41, [](AddrSet&) {}, "gcs: addr: 1 bytes left over on input 1"},
    };
    for (const Case& c : cases) {
        ScratchDir dir("gcs-test");
        AddrSet e = s;
        c.edit(e);
        addr::Enc enc(dir.path());
        enc.state = {1, 1};

        CHECK_EQ(refusal_of([&] {
                     enc.write(1, e.verts.data(), c.verts);
                     enc.write(0, e.points.data(), c.points);
                     finish_all(enc);
                 }),
                 std::string(c.refusal));
        if (!*c.refusal) CHECK(dir_files(dir.path()) == addr_files_of(s));
    }
}

TEST(gcs_encoders_refuse_a_unit_past_the_end_of_its_file) {
    // A footprint of 2^31 vertices or a POI block of 2^30 (10 + 4 x 2^31 and
    // 10 + 4 x 2^30 bytes, which a 32-bit size_t wraps to 10) whose stream is
    // ten bytes: refused as those bytes come, not waited for while the queue
    // takes the rest of the file. The footprint is one of more bytes than a
    // patch indexes, the POI block one past the end of its file as the patch
    // gives it.
    ScratchDir dir("gcs-test");
    const std::vector<uint8_t> ten(10, 0);
    {
        std::vector<uint8_t> rec(28, 0);
        wr32(&rec[20], 0);
        wr32(&rec[24], 0x80000000);
        addr::Enc enc(dir.path());
        enc.state = {1, 1};
        enc.write(0, rec.data(), rec.size());

        CHECK_EQ(refusal_of([&] { enc.write(1, ten.data(), ten.size()); }),
                 std::string("Malformed address files: a logical addr_vertices.bin of 8589934602 bytes is more than the "
                             "8589934588 a patch can index"));
    }
    {
        std::vector<uint8_t> rec(36, 0);
        wr32(&rec[8], 0);
        wr32(&rec[12], 0x40000000);
        poi::Enc enc(dir.path());
        enc.state = {1, 1};
        enc.expect = {rec.size(), ten.size()};
        enc.write(0, rec.data(), rec.size());

        CHECK_EQ(refusal_of([&] { enc.write(1, ten.data(), ten.size()); }),
                 std::string("gcs: poi: a unit of 10 + 1073741824 x 4 bytes at 0 runs past the end of input 1 (10 bytes left)"));
    }
}

TEST(gcs_reader_rejects_a_varint_past_64_bits) {
    // A varint's tenth byte holds bit 63 alone: one past 1 would lose its
    // other bits off the u64 (FF x9 02 read as 2^63 - 1), so it is rejected;
    // FF x9 01 is 2^64 - 1. Through the reader's buffered path (the varint
    // already in the buffer, after a byte read first) and its byte-by-byte one
    // (a fresh reader).
    ScratchDir dir("gcs-test");
    const std::string path = dir.path() + "/v";
    for (const uint8_t last : {uint8_t(0x01), uint8_t(0x02), uint8_t(0x7F)}) {
        for (const bool buffered : {false, true}) {
            std::vector<uint8_t> bytes;
            if (buffered) bytes.push_back(0);
            bytes.insert(bytes.end(), 9, 0xFF);
            bytes.push_back(last);
            write_bytes(path, bytes);
            Reader r(path);
            if (buffered) r.u8();

            if (last == 1) {
                CHECK(r.uv() == ~uint64_t(0));
                CHECK(r.eof());
            } else {
                CHECK_EQ(refusal_of([&] { r.uv(); }), "gcs: " + path + ": bad varint");
            }
        }
    }
}

TEST(gcs_ways_round_trip_in_odd_pieces_and_equal_the_builders_encoder) {
    // Ways written a record and its nodes at a time, in pieces that cut them
    // anywhere (of 1 to 7 bytes, and of up to 5000), give the builder's two
    // files, whatever their count: none (the two footers alone), one, a group
    // less one, a group, a group and one, and many groups, one of them without
    // a node. The decoder gives the pair back, and the decoded sizes are the
    // footers' without a decoder. Streets and interpolations.
    std::mt19937 rng(20261114);
    for (const way_files::Kind* kind : WAY_KINDS)
        for (const size_t n : {size_t(0), size_t(1), size_t(15), size_t(16), size_t(17), size_t(1000)})
            for (const size_t max_piece : {size_t(7), size_t(5000)}) {
                const WaySet s = way_set(*kind, way_counts(n, rng), rng);
                ScratchDir dir("gcs-test");
                encode_ways(*kind, s, dir.path(), rng, max_piece);

                const auto files = dir_files(dir.path());
                CHECK(files == way_files_of(*kind, s));
                if (n == 0) CHECK(files.at(kind->ways_file).size() == 8 && files.at(kind->nodes_file).size() == 16);
                CHECK_EQ(ways::decoded_size(dir.path(), *kind, 0), uint64_t(s.ways.size()));
                CHECK_EQ(ways::decoded_size(dir.path(), *kind, 1), uint64_t(s.nodes.size()));
                ways::Dec dec(dir.path(), *kind);
                CHECK_EQ(dec.decoded_size(0), uint64_t(s.ways.size()));
                CHECK_EQ(dec.decoded_size(1), uint64_t(s.nodes.size()));
                const auto out = decode_all(dec);
                CHECK(out[0] == s.ways);
                CHECK(out[1] == s.nodes);
            }
}

TEST(gcs_ways_decoder_takes_a_dir_without_its_files_as_no_ways_and_refuses_one_of_them) {
    // A variant without streets has neither street file: it decodes to no
    // ways, beside the interpolations' pair. A dir with one file of a pair is
    // refused by name, by the decoder and by the decoded size alone.
    std::mt19937 rng(20261114);
    ScratchDir dir("gcs-test");
    encode_ways(way_files::INTERPS, way_set(way_files::INTERPS, way_counts(20, rng), rng), dir.path(), rng, 100);

    for (int port = 0; port < 2; port++) CHECK_EQ(ways::decoded_size(dir.path(), way_files::STREETS, port), uint64_t(0));
    ways::Dec none(dir.path(), way_files::STREETS);
    CHECK_EQ(none.decoded_size(0) + none.decoded_size(1), uint64_t(0));
    const auto out = decode_all(none);
    CHECK(out[0].empty() && out[1].empty());

    REQUIRE(std::filesystem::remove(dir.path() + "/interp_nodes.bin"));
    const std::string refusal = "Malformed way files: interp_ways.bin without interp_nodes.bin";
    CHECK_EQ(refusal_of([&] { ways::Dec dec(dir.path(), way_files::INTERPS); }), refusal);
    CHECK_EQ(refusal_of([&] { ways::decoded_size(dir.path(), way_files::INTERPS, 0); }), refusal);
}

TEST(gcs_ways_decoder_refuses_what_the_strict_decoder_refuses) {
    // A pair of 40 ways, damaged 400 times a kind: bytes of the rows, of the
    // blocks or of a footer overwritten, a byte cut or added before a footer,
    // a file gone, the second group's blocks a byte further than the first's
    // end, a node more in the footer's count, and every tenth time nothing.
    // The decoder, which reads a way at a time, refuses each with the message
    // of way_files::decode, which reads the two files whole, and gives that
    // one's pair where it takes the files. The decoded size alone refuses
    // what the two footers show.
    std::mt19937 rng(20261114);
    auto u8 = [](const std::vector<char>& v) { return std::vector<uint8_t>(v.begin(), v.end()); };
    for (const way_files::Kind* kind : WAY_KINDS) {
        const WaySet s = way_set(*kind, way_counts(40, rng), rng);
        const auto written = way_files_of(*kind, s);
        const std::vector<uint8_t> rows = u8(written.at(kind->ways_file)), blocks = u8(written.at(kind->nodes_file));
        size_t refused = 0, taken = 0, by_footers = 0, differ = 0;
        for (int round = 0; round < 400; round++) {
            std::vector<uint8_t> r = rows, b = blocks;
            bool has_rows = true, has_blocks = true;
            auto overwrite = [&](std::vector<uint8_t>& f) { f[rng() % f.size()] = uint8_t(rng()); };
            switch (round % 10) {
            case 0: overwrite(r); break;
            case 1: overwrite(b); break;
            case 2:
                for (int k = 0; k < 3; k++) overwrite(rng() % 2 ? r : b);
                break;
            case 3: b.erase(b.end() - 17); break;
            case 4: b.insert(b.end() - 16, uint8_t(rng())); break;
            case 5: r.erase(r.begin() + long(rng() % (r.size() - 8))); break;
            case 6: (round % 20 < 10 ? has_rows : has_blocks) = false; break;
            case 7: r[kind->row_bytes(way_files::GROUP_WAYS)]++; break;
            case 8: b[b.size() - 16]++; break;
            default: break;
            }
            ScratchDir dir("gcs-test");
            if (has_rows) write_bytes(dir.path() + "/" + kind->ways_file, r);
            if (has_blocks) write_bytes(dir.path() + "/" + kind->nodes_file, b);
            const uint64_t rows_size = has_rows ? r.size() : way_files::ABSENT, blocks_size = has_blocks ? b.size() : way_files::ABSENT;

            WaySet whole, stepped;
            const std::string strict = refusal_of([&] {
                way_files::decode(
                    *kind, r.data(), rows_size, b.data(), blocks_size,
                    [&](const uint8_t* record) { whole.ways.insert(whole.ways.end(), record, record + kind->stride); },
                    [&](const uint8_t* p, size_t n) { whole.nodes.insert(whole.nodes.end(), p, p + n); });
            });
            const std::string ours = refusal_of([&] {
                ways::Dec dec(dir.path(), *kind);
                auto out = decode_all(dec);
                stepped.ways = std::move(out[0]);
                stepped.nodes = std::move(out[1]);
            });
            const std::string footers = refusal_of([&] {
                way_files::read_footers(*kind, r.data() + (r.size() - 8), rows_size, b.data() + (b.size() - 16), blocks_size);
            });
            const std::string sized = refusal_of([&] { ways::decoded_size(dir.path(), *kind, 0); });

            const bool same = ours == strict && sized == footers &&
                              (!strict.empty() || (stepped.ways == whole.ways && stepped.nodes == whole.nodes));
            if (!same && differ++ < 3)
                std::printf("    round %d: decode \"%s\", the codec \"%s\"; footers \"%s\", the size \"%s\"\n", round, strict.c_str(),
                            ours.c_str(), footers.c_str(), sized.c_str());
            refused += !strict.empty();
            taken += strict.empty();
            by_footers += !footers.empty();
            if (round % 10 == 9) CHECK(strict.empty() && stepped.ways == s.ways && stepped.nodes == s.nodes);
            if (round % 10 == 7 || round % 10 == 8) CHECK(!strict.empty() && footers.empty());
        }
        std::printf("    %s: %zu refused (%zu by the footers), %zu taken\n", kind->ways_file, refused, by_footers, taken);
        CHECK_EQ(differ, size_t(0));
        CHECK(refused > 200 && taken > 60 && by_footers > 60);
    }
}

TEST(gcs_ways_decoder_refuses_a_way_starting_past_node_u32) {
    // A way's node_offset is a u32: ways whose nodes sum past it are refused
    // at the first way that starts past node 2^32 - 1 (narrowed, it read as a
    // small offset). 40 street ways decoded from node 2^32 - 1: the first way
    // with nodes starts there, the next one past it.
    std::mt19937 rng(5);
    const WaySet s = way_set(way_files::STREETS, way_counts(40, rng), rng);
    ScratchDir dir("gcs-test");
    encode_ways(way_files::STREETS, s, dir.path(), rng, 7);
    uint64_t second_with_nodes = 0;
    for (size_t w = 0, with_nodes = 0; w < 40 && with_nodes < 2; w++)
        if (node_bytes(&s.ways[12 * w]) && ++with_nodes == 2) second_with_nodes = w;
    REQUIRE(second_with_nodes > 0);

    ways::Dec dec(dir.path(), way_files::STREETS);
    dec.node = UINT32_MAX;
    for (auto& st : dec.state) st = 1;
    const std::string refusal = refusal_of([&] {
        while (dec.step())
            for (auto& q : dec.out) q.pop(q.size());
    });

    CHECK_EQ(refusal, std::string("Malformed way files: street_ways.bin has a way with nodes after 2^32 nodes or more"));
    CHECK_EQ(dec.way, second_with_nodes);
}

TEST(gcs_ways_encoder_refuses_a_stream_that_is_not_whole_ways_and_their_nodes) {
    // Three street ways of 2, 0 and 1 nodes. A record whose node_offset is
    // not the nodes before it is refused as it comes, by the way; a stream
    // that ends inside a way's nodes or inside a record, or holds a node past
    // the last way's, at the finish, not written as a shorter pair.
    std::mt19937 rng(20261114);
    const WaySet s = way_set(way_files::STREETS, {2, 0, 1}, rng);
    struct Case {
        size_t ways, nodes;  // the bytes of each stream written
        uint32_t offset;     // the last way's node_offset
        const char* refusal;
    };
    const Case cases[] = {{36, 24, 2, ""},
                          {36, 24, 3, "Malformed way files: street_ways.bin way 2 has a node_offset of 3, not the 2 nodes before it"},
                          {36, 23, 2, "gcs: streets: 12 bytes left over on input 0"},
                          {35, 16, 2, "gcs: streets: 11 bytes left over on input 0"},
                          {24, 24, 2, "gcs: streets: 8 bytes left over on input 1"}};
    for (const Case& c : cases) {
        ScratchDir dir("gcs-test");
        std::vector<uint8_t> ways = s.ways;
        wr32(&ways[24], c.offset);
        ways::Enc enc(dir.path(), way_files::STREETS);
        enc.state = {1, 1};

        CHECK_EQ(refusal_of([&] {
                     enc.write(1, s.nodes.data(), c.nodes);
                     enc.write(0, ways.data(), c.ways);
                     finish_all(enc);
                 }),
                 std::string(c.refusal));
        if (!*c.refusal) CHECK(dir_files(dir.path()) == way_files_of(way_files::STREETS, s));
    }
}

TEST(gcs_ways_codec_holds_one_way) {
    // 3,000 interpolations of 20 to 200 nodes, two of them of the most nodes
    // a way has (65,535: 524,280 B, one's of any bits, whose block is as
    // long), 3.6 MB of nodes: each way's record and then its nodes in one
    // write (as the patcher's FILE passes a long write), read back as the
    // patcher reads them (a record, then its nodes in 64 KiB views). The
    // encoder's queues hold the record and the nodes of the way in flight;
    // the decoder's what a read asks ahead and one way; the rows' reader its
    // buffer, and the blocks' the largest block. The files are the builder's.
    const way_files::Kind& kind = way_files::INTERPS;
    const size_t most = way_files::NODE_BYTES * size_t(way_files::MAX_WAY_NODES);
    std::mt19937 rng(20261114);
    std::vector<uint32_t> counts(3000);
    for (size_t w = 0; w < counts.size(); w++)
        counts[w] = w == 699 || w == 2100 ? uint32_t(way_files::MAX_WAY_NODES) : 20 + uint32_t(rng() % 181);
    const WaySet s = way_set(kind, counts, rng);
    ScratchDir dir("gcs-test");
    ways::Enc enc(dir.path(), kind);
    enc.state = {1, 1};
    size_t np = 0;
    for (size_t at = 0; at < s.ways.size(); at += kind.stride) {
        enc.write(0, &s.ways[at], kind.stride);
        enc.write(1, &s.nodes[np], node_bytes(&s.ways[at]));
        np += node_bytes(&s.ways[at]);
    }
    const size_t encoder_records = enc.peak(0), encoder_nodes = enc.peak(1);
    finish_all(enc);

    ways::Dec dec(dir.path(), kind);
    dec.state = {1, 1};
    WaySet out;
    std::vector<char> view(size_t(64) << 10);
    size_t rows_reader = 0, blocks_reader = 0;
    auto read = [&](int port, size_t n, std::vector<uint8_t>& to) {
        while (n) {
            const size_t k = dec.read(port, view.data(), std::min(n, view.size()));
            if (!k) return false;
            if (dec.R) {
                rows_reader = std::max(rows_reader, dec.R->buf.size());
                blocks_reader = std::max(blocks_reader, dec.B->buf.size());
            }
            to.insert(to.end(), view.data(), view.data() + k);
            n -= k;
        }
        return true;
    };
    bool complete = true;
    for (size_t at = 0; at < s.ways.size(); at += kind.stride)
        complete &= read(0, kind.stride, out.ways) && read(1, node_bytes(&s.ways[at]), out.nodes);
    std::printf("    %zu B of nodes: encoder queue peaks %zu %zu B, decoder queue peaks %zu %zu B, readers %zu %zu B\n", s.nodes.size(),
                encoder_records, encoder_nodes, dec.peak(0), dec.peak(1), rows_reader, blocks_reader);

    CHECK(s.nodes.size() > 6 * most);
    CHECK_EQ(encoder_records, kind.stride);
    CHECK_EQ(encoder_nodes, most);
    CHECK(dec.peak(0) <= Decoder::READ_AHEAD);
    CHECK(dec.peak(1) <= Decoder::READ_AHEAD + most);
    CHECK(rows_reader <= ways::ROWS_BUF + READ_SLACK);
    CHECK(blocks_reader > most && blocks_reader <= most + 8 + READ_SLACK);
    CHECK(complete);
    CHECK(dec.read(0, view.data(), 1) == 0);
    CHECK(out.ways == s.ways);
    CHECK(out.nodes == s.nodes);
    CHECK(dir_files(dir.path()) == way_files_of(kind, s));
}

namespace {

// 128,000 values, one value a block: 2,000 blocks of 6 bytes, 12 KB that
// wait in the encoder's buffer for finish(), which adds their table (8 KB)
// and the footer.
std::vector<uint32_t> one_value_blocks() {
    std::vector<uint32_t> v(128000);
    for (size_t j = 0; j < v.size(); j++) v[j] = uint32_t(j / 64 * 7919 % 0xFFFFFF);
    return v;
}

// 0: the write is refused with the error naming its file, too large for
// the limit (mode 3, no limit: the file holds the values); 3: another
// refusal; 4: none; 5, 6: mode 3's file is wrong; 8: a refusal in mode 3.
int writer_case(int mode, const std::string& dir) {
    const bool side = mode == 0 || mode == 3;
    const std::string file = dir + (side ? "/way_parents.bin" : "/output");
    const std::vector<uint32_t> v = one_value_blocks();
    const std::string refusal = refusal_of([&] {
        if (side) {  // side::Enc: its blocks' last buffer, their table and the footer go out in finish()
            side::Enc enc(dir, "way_parents");
            enc.state[0] = 1;
            enc.write(0, v.data(), v.size() * 4);
            enc.state[0] = 2;
            enc.finish();
        } else {  // a writer whose finish() fails (1), or whose put() fails writing straight through (2)
            Writer w(file, 48 << 10);
            std::vector<uint8_t> bytes(mode == 1 ? 40000 : 100000);
            w.put(bytes.data(), bytes.size());
            w.finish();
        }
    });
    if (mode != 3) {
        if (refusal.empty()) return 4;
        return refusal == "gcs: write " + file + ": " + std::strerror(EFBIG) ? 0 : 3;
    }
    if (!refusal.empty()) return 8;
    const std::vector<char> bytes = slurp(file);
    std::vector<uint32_t> values;
    if (!refusal_of([&] {
             side_blocks::decode("way_parents.bin", reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(),
                                 [&](const uint32_t* p, size_t n) { values.insert(values.end(), p, p + n); });
         }).empty())
        return 5;
    return values == v ? 0 : 6;
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

TEST(gcs_reader_rejects_a_unit_past_what_its_buffer_holds) {
    // A unit past what the reader's buffer holds with its slack is rejected
    // before the buffer grows: a footprint of 0x3FFFFFFD four-byte vertices
    // after its header, 0xFFFFFFFE bytes, in a sparse file of as many. Only a
    // 32-bit build has such a unit (its buffer holds 2 GiB, its size_t the
    // unit), where cap + 64 wrapped to a 62-byte buffer that the file was then
    // read into.
    if (sizeof(size_t) > 4) return;
    ScratchDir dir("gcs-test");
    const std::string path = dir.path() + "/addr_vertices.bin";
    write_bytes(path, {});
    std::filesystem::resize_file(path, 0xFFFFFFFE);

    const int status = exit_status_in_child(
        [&] {
            return refusal_code(
                [&] {
                    Reader r(path);
                    r.take(r.unit_size(10, 0x3FFFFFFD, 4));
                },
                "gcs: " + path + ": a unit of 4294967294 bytes at 0 does not fit in memory");
        },
        20);

    CHECK_EQ(status, 2);
}

namespace {

// A port's stream read as the patcher reads an old file whose decoded size
// it knows: views of up to 64 KiB and none past that size, the other ports
// closed. What it gave.
std::vector<uint8_t> read_to_its_size(Decoder& dec, int port) {
    for (size_t p = 0; p < dec.state.size(); p++) dec.state[p] = int(p) == port ? 1 : 2;
    const uint64_t size = dec.decoded_size(port);
    std::vector<uint8_t> out;
    std::vector<char> view(size_t(64) << 10);
    while (out.size() < size) {
        const size_t k = dec.read(port, view.data(), size_t(std::min<uint64_t>(view.size(), size - out.size())));
        if (!k) break;
        out.insert(out.end(), view.data(), view.data() + k);
    }
    return out;
}

using Files = std::map<std::string, std::vector<char>>;

void write_files(const std::string& dir, const Files& files) {
    for (const auto& [name, bytes] : files) write_bytes(dir + "/" + name, std::vector<uint8_t>(bytes.begin(), bytes.end()));
}

const uint8_t* u8(const std::vector<char>& v) { return reinterpret_cast<const uint8_t*>(v.data()); }

// The three place files of `visible` nodes, three a cell, and `hidden`
// hidden ones after them (place_files::Encoder).
Files place_files_of(uint32_t visible, uint32_t hidden) {
    std::vector<char> cells, blocks, hidden_rows;
    auto into = [](std::vector<char>& v) {
        return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); };
    };
    place_files::Encoder enc(into(cells), into(blocks), into(hidden_rows));
    for (uint32_t i = 0; i < visible + hidden; i++) {
        PlaceNode n{};
        n.lat = 47.0f + float(i) * 1e-3f;
        n.lng = 8.0f;
        n.name_id = 16 * i;
        n.place_type = 1;
        n.rank_address = 16;
        if (i >= visible) {
            n._pad1 = PLACE_FLAG_HIDDEN;
            n.rank_address = 0;
        }
        enc.add({n, i < visible ? i / 3 : NO_DATA});
    }
    enc.finish();
    for (const auto& e : enc.table()) blocks.insert(blocks.end(), e.begin(), e.end());
    const auto footer = enc.footer();
    blocks.insert(blocks.end(), footer.begin(), footer.end());
    return {{place_files::CELLS_FILE, cells}, {place_files::NODES_FILE, blocks}, {place_files::HIDDEN_FILE, hidden_rows}};
}

// n postcode centroids in cell order, nine to a cell, every third of a
// second country, half at four decimals: their logical rows, and their two
// files as the builder writes them (postcode_files::Encoder).
std::vector<postcode_files::LogicalCentroid> centroid_rows(uint32_t n) {
    std::vector<postcode_files::LogicalCentroid> rows;
    for (uint32_t i = 0; i < n; i++) {
        postcode_files::LogicalCentroid c;
        c.lat = i % 2 ? 51.5f + float(i % 9) * 1e-3f : float(515000 + i % 9) / 1e4f;
        c.lng = -0.1f + float(i % 9) * 7e-4f;
        c.postcode_id = 3 * i;
        c.country_code = i % 3 ? uint16_t('G' << 8 | 'B') : uint16_t('I' << 8 | 'E');
        c.cell_key = uint32_t(cell_pairs::cell_of(4000 + i / 9, 10) >> 32);
        rows.push_back(c);
    }
    return rows;
}
Files postcode_files_of(const std::vector<postcode_files::LogicalCentroid>& rows) {
    std::vector<char> centroids, cells;
    auto into = [](std::vector<char>& v) {
        return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); };
    };
    postcode_files::Encoder enc(into(centroids), into(cells));
    for (const postcode_files::LogicalCentroid& c : rows) enc.add(c);
    enc.finish();
    for (const auto& e : enc.table()) centroids.insert(centroids.end(), e.begin(), e.end());
    const auto footer = enc.footer();
    centroids.insert(centroids.end(), footer.begin(), footer.end());
    return {{postcode_files::CENTROIDS_FILE, centroids}, {postcode_files::CELLS_FILE, cells}};
}
std::vector<uint8_t> logical_of(const std::vector<postcode_files::LogicalCentroid>& rows) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(rows.data());
    return std::vector<uint8_t>(p, p + rows.size() * postcode_files::LOGICAL_STRIDE);
}

// A family's files as the builder writes them, with its decoder over a dir
// that holds them, the ports whose stream ends with the files' last unit
// (the records' unless the last record's child bytes come in pieces after
// it, the child's when the last record has some), the whole-file decoder's
// refusal of a set ("" when it takes it), and files a byte or a count away
// from the written ones where they end.
struct FilesEnd {
    std::string name;
    Files files;
    std::function<std::unique_ptr<Decoder>(const std::string& dir)> decoder;
    std::vector<int> ports;
    std::function<std::string(const Files&)> strict;
    struct Damage {
        std::string what, file;
        std::function<void(std::vector<char>&)> edit;
    };
    std::vector<Damage> damages;
};

// A byte put in `before` bytes from the file's end.
std::function<void(std::vector<char>&)> a_byte_before_the_last(size_t before) {
    return [before](std::vector<char>& f) { f.insert(f.end() - std::ptrdiff_t(before), char(0)); };
}
// The byte `before` bytes from the file's end, one more.
std::function<void(std::vector<char>&)> one_more_at(size_t before) {
    return [before](std::vector<char>& f) { f[f.size() - before]++; };
}

std::vector<FilesEnd> files_ends(std::mt19937& rng) {
    std::vector<FilesEnd> ends;
    for (const size_t n : {size_t(100), size_t(2048), size_t(4096)}) {
        const std::string family = "way_postcodes", file = family + ".bin";
        const size_t table = 4 * ((n + 63) / 64) + 8;
        ends.push_back({"side file of " + std::to_string(n) + " values",
                        {{file, side_file_of(family, side_values(n, rng))}},
                        [family](const std::string& dir) { return std::unique_ptr<Decoder>(new side::Dec(dir, family)); },
                        {0},
                        [file](const Files& f) {
                            return refusal_of([&] { side_blocks::decode(file, u8(f.at(file)), f.at(file).size(), [](const uint32_t*, size_t) {}); });
                        },
                        {{"a byte between the blocks and the table", file, a_byte_before_the_last(table)}}});
    }
    for (const way_files::Kind* kind : WAY_KINDS) {
        // Ways that end without nodes (the records' stream alone ends with the last way), and ways that all have some.
        for (const bool nodes_last : {false, true}) {
            const std::vector<uint32_t> counts = nodes_last ? std::vector<uint32_t>(50, 3) : way_counts(40, rng);
            ends.push_back({std::string(kind->ways_file) + (nodes_last ? ", every way with nodes" : ", the last ways without nodes"),
                            way_files_of(*kind, way_set(*kind, counts, rng)),
                            [kind](const std::string& dir) { return std::unique_ptr<Decoder>(new ways::Dec(dir, *kind)); },
                            nodes_last ? std::vector<int>{0, 1} : std::vector<int>{0},
                            [kind](const Files& f) {
                                const std::vector<char>&rows = f.at(kind->ways_file), &blocks = f.at(kind->nodes_file);
                                return refusal_of([&] {
                                    way_files::decode(
                                        *kind, u8(rows), rows.size(), u8(blocks), blocks.size(), [](const uint8_t*) {}, [](const uint8_t*, size_t) {});
                                });
                            },
                            {{"a byte between the last block and the footer", kind->nodes_file, a_byte_before_the_last(way_files::NODES_FOOTER_BYTES)},
                             {"a node more in the footer", kind->nodes_file, one_more_at(way_files::NODES_FOOTER_BYTES)}}});
        }
    }
    for (const bool escape_last : {false, true}) {
        // Addresses that end with points, and ones whose last footprint is an escape, which the decoder gives in pieces.
        std::vector<AddrSpec> specs = addr_specs(40, rng);
        specs.push_back(escape_last ? AddrSpec{9000, 3, false, 0} : AddrSpec{0, 0, false, 0});
        const auto strict = [](const Files& f) {
            const std::vector<char>&points = f.at(addr_files::POINTS_FILE), &vertices = f.at(addr_files::VERTICES_FILE),
                              &index = f.at(addr_files::INDEX_FILE);
            return refusal_of([&] { addr_files::decode(u8(points), points.size(), u8(vertices), vertices.size(), u8(index), index.size()); });
        };
        ends.push_back({escape_last ? "addresses, the last an escape" : "addresses, the last a point",
                        addr_files_of(addr_set(specs, rng)),
                        [](const std::string& dir) { return std::unique_ptr<Decoder>(new addr::Dec(dir)); },
                        {escape_last ? 1 : 0},
                        strict,
                        {{"a byte between the last block and the footer", addr_files::POINTS_FILE, a_byte_before_the_last(addr_files::POINTS_FOOTER_BYTES)},
                         {"a byte between the last blob and the footer", addr_files::VERTICES_FILE,
                          a_byte_before_the_last(addr_files::VERTICES_FOOTER_BYTES)},
                         {"a blob more in the footer", addr_files::VERTICES_FILE, one_more_at(8)}}});
    }
    for (const bool polygon_last : {false, true}) {
        // POIs that end with a point, and ones whose last polygon is past big_block() (set to 4 KiB: decoded in pieces).
        std::vector<PoiSpec> specs = {{0, 0, false}, {30, 2, false}, {5, 4, true}, {0, 0, false}, {700, 2, true}, {12, 2, false}};
        specs.push_back(polygon_last ? PoiSpec{20000, 2, false} : PoiSpec{0, 0, false});
        ends.push_back({polygon_last ? "POIs, the last a polygon in pieces" : "POIs, the last a point",
                        poi_files_of(poi_set(specs, rng)),
                        [](const std::string& dir) { return std::unique_ptr<Decoder>(new poi::Dec(dir)); },
                        {polygon_last ? 1 : 0},
                        [](const Files& f) {
                            const std::vector<char>&r2 = f.at("poi_records.bin"), &v2 = f.at("poi_vertices.bin");
                            return refusal_of([&] { poi_files::decode(u8(r2), r2.size(), u8(v2), v2.size()); });
                        },
                        {{"a byte between the last block and the pad", "poi_vertices.bin", a_byte_before_the_last(poi_files::VERTEX_PAD)},
                         {"a pad that is not zero", "poi_vertices.bin", one_more_at(1)},
                         {"a vertex byte more in the footer", "poi_records.bin", one_more_at(12)}}});
    }
    for (const uint32_t hidden : {0u, 300u}) {
        // Place nodes that end with a block of visible ones, and with a run of hidden ones.
        ends.push_back({hidden ? "places, the last hidden" : "places, all visible",
                        place_files_of(500, hidden),
                        [](const std::string& dir) { return std::unique_ptr<Decoder>(new place::Dec(dir)); },
                        {0},
                        [](const Files& f) {
                            const std::vector<char>&cells = f.at(place_files::CELLS_FILE), &nodes = f.at(place_files::NODES_FILE),
                                              &hidden_rows = f.at(place_files::HIDDEN_FILE);
                            return refusal_of([&] {
                                place_files::decode(u8(cells), cells.size(), u8(nodes), nodes.size(), u8(hidden_rows), hidden_rows.size(),
                                                    [](const place_files::LogicalPlace&) {});
                            });
                        },
                        {{"a byte between the blocks and the table", place_files::NODES_FILE,
                          a_byte_before_the_last(place_files::TABLE_ENTRY_BYTES * ((500 + place_files::BLOCK_ROWS - 1) / place_files::BLOCK_ROWS) +
                                                 place_files::FOOTER_BYTES)},
                         {"a cell past the last visible node", place_files::CELLS_FILE,
                          [](std::vector<char>& f) {
                              // The next key, from the row past the last.
                              uint8_t row[place_files::CELL_ROW_BYTES];
                              wr32(row, rd32(u8(f) + f.size() - place_files::CELL_ROW_BYTES) + 1);
                              wr32(row + 4, 500);
                              f.insert(f.end(), row, row + sizeof row);
                          }}}});
    }
    for (const uint32_t n : {320u, 333u}) {
        // Postcode centroids whose last block is whole, and one of 13 rows.
        const size_t blocks = (n + postcode_files::BLOCK_ROWS - 1) / postcode_files::BLOCK_ROWS;
        ends.push_back({"postcode centroids, " + std::to_string(n),
                        postcode_files_of(centroid_rows(n)),
                        [](const std::string& dir) { return std::unique_ptr<Decoder>(new postcodes::Dec(dir)); },
                        {0},
                        [](const Files& f) {
                            const std::vector<char>&c = f.at(postcode_files::CENTROIDS_FILE), &k = f.at(postcode_files::CELLS_FILE);
                            return refusal_of([&] {
                                postcode_files::decode(u8(c), c.size(), u8(k), k.size(), [](const postcode_files::LogicalCentroid&) {});
                            });
                        },
                        {{"a byte between the blocks and the table", postcode_files::CENTROIDS_FILE,
                          a_byte_before_the_last(postcode_files::TABLE_ENTRY_BYTES * blocks + postcode_files::FOOTER_BYTES)},
                         {"a row more in the footer", postcode_files::CENTROIDS_FILE, one_more_at(postcode_files::FOOTER_BYTES)}}});
    }
    return ends;
}

}  // namespace

TEST(gcs_decoders_check_how_their_files_end_with_the_last_unit_they_decode) {
    // A reader that knows an old file's decoded size asks a decoder for no
    // byte past it, so no step follows the one that decodes the files' last
    // unit: that step makes the checks of how the files end. Each family's
    // files as written, a port that ends with the last unit read to its
    // decoded size, give its stream and leave the decoder at its end. With a
    // byte between the last unit and what follows it, or a footer that
    // counts one more, the read that reaches the last unit refuses them in
    // the whole-file decoder's words: side files of less than a read ahead,
    // of one and of two; way pairs that end with ways without nodes and with
    // ways that have them; addresses that end with a point and with an
    // escape, given in pieces; POIs that end with a point and with a polygon
    // past big_block(), decoded in pieces; places that end with visible
    // nodes and with hidden ones; postcode centroids whose last block is
    // whole and one that is not.
    std::mt19937 rng(20261115);
    BigBlock small(4096);
    for (const FilesEnd& end : files_ends(rng)) {
        const int failures_before = gctest::failures();
        REQUIRE(end.strict(end.files).empty());
        {
            ScratchDir dir("gcs-test");
            write_files(dir.path(), end.files);
            for (const int port : end.ports) {
                const std::unique_ptr<Decoder> dec = end.decoder(dir.path());
                const uint64_t size = dec->decoded_size(port);
                std::vector<uint8_t> out;
                CHECK_EQ(refusal_of([&] { out = read_to_its_size(*dec, port); }), std::string());
                CHECK(out.size() == size && !out.empty());
                // The decoder is at its end: it has nothing more for any port.
                CHECK(!dec->step());
            }
        }
        for (const FilesEnd::Damage& damage : end.damages) {
            Files damaged = end.files;
            damage.edit(damaged.at(damage.file));
            const std::string strict = end.strict(damaged);
            CHECK(!strict.empty());
            ScratchDir dir("gcs-test");
            write_files(dir.path(), damaged);
            for (const int port : end.ports) {
                const std::string ours = refusal_of([&] {
                    const std::unique_ptr<Decoder> dec = end.decoder(dir.path());
                    read_to_its_size(*dec, port);
                });
                if (ours != strict) std::printf("    %s, %s, port %d: \"%s\", not \"%s\"\n", end.name.c_str(), damage.what.c_str(), port, ours.c_str(), strict.c_str());
                CHECK_EQ(ours, strict);
            }
        }
        if (gctest::failures() > failures_before) std::printf("    in: %s\n", end.name.c_str());
    }
}

namespace {

// A two-file family's encoder into a dir, and a record of it that has child
// bytes, with the least of them.
struct CoupledEncoder {
    const char* family;
    std::function<std::unique_ptr<Encoder>(const std::string& dir)> encoder;
    std::vector<uint8_t> record, child;
};

std::vector<CoupledEncoder> coupled_encoders() {
    // A street way of one node; an address with a footprint of one vertex; a POI with a polygon of one vertex.
    std::vector<uint8_t> way(12, 0), address(28, 0), poi(36, 0);
    wr16(&way[4], 1);
    wr32(&address[24], 1);
    wr32(&poi[12], 1);
    return {{"streets", [](const std::string& dir) { return std::unique_ptr<Encoder>(new ways::Enc(dir, way_files::STREETS)); }, way,
             std::vector<uint8_t>(8, 0)},
            {"addr", [](const std::string& dir) { return std::unique_ptr<Encoder>(new addr::Enc(dir)); }, address, std::vector<uint8_t>(14, 0)},
            {"poi", [](const std::string& dir) { return std::unique_ptr<Encoder>(new poi::Enc(dir)); }, poi, std::vector<uint8_t>(14, 0)}};
}

}  // namespace

TEST(gcs_coupled_encoders_refuse_bytes_no_unit_takes_as_they_come) {
    // A two-file family's encoder takes a record once its child bytes are
    // in, and child bytes once their record is. Bytes of one file that wait
    // for the other's are refused as they come when the other file is
    // closed, as no unit will take them: child bytes after the records' file
    // closed without a record for them, and a record with child bytes after
    // the child file closed without them. While both files are open one may
    // run ahead of the other by what a replay's buffers put between them,
    // and no further: 4 MiB of child bytes without a record, and 4 MiB of
    // records without their child bytes, are refused once the queue holds
    // MAX_ENCODER_SKEW, not held until the files close. The way pair, the
    // address files and the POI files.
    const size_t skew = MAX_ENCODER_SKEW;
    for (const CoupledEncoder& c : coupled_encoders()) {
        const int failures_before = gctest::failures();
        const std::string family = c.family;
        {
            ScratchDir dir("gcs-test");
            const std::unique_ptr<Encoder> enc = c.encoder(dir.path());
            enc->state = {2, 1};
            CHECK_EQ(refusal_of([&] { enc->write(1, c.child.data(), c.child.size()); }),
                     "gcs: " + family + ": " + std::to_string(c.child.size()) + " bytes left over on input 1");
        }
        {
            ScratchDir dir("gcs-test");
            const std::unique_ptr<Encoder> enc = c.encoder(dir.path());
            enc->state = {1, 2};
            CHECK_EQ(refusal_of([&] { enc->write(0, c.record.data(), c.record.size()); }),
                     "gcs: " + family + ": " + std::to_string(c.record.size()) + " bytes left over on input 0");
        }
        for (const int port : {0, 1}) {
            // Whole units, 64 KiB of them a write.
            const std::vector<uint8_t>& unit = port == 0 ? c.record : c.child;
            std::vector<uint8_t> piece;
            while (piece.size() + unit.size() <= ENCODER_PIECE) piece.insert(piece.end(), unit.begin(), unit.end());
            ScratchDir dir("gcs-test");
            const std::unique_ptr<Encoder> enc = c.encoder(dir.path());
            enc->state = {1, 1};
            size_t written = 0;
            const std::string refusal = refusal_of([&] {
                for (; written < (size_t(4) << 20); written += piece.size()) enc->write(port, piece.data(), piece.size());
            });
            const size_t held = (skew / piece.size() + 1) * piece.size();
            CHECK_EQ(refusal, "gcs: " + family + ": input " + std::to_string(port) + " runs " + std::to_string(held) + " bytes ahead of input " +
                                  std::to_string(1 - port));
            CHECK(enc->peak(port) <= skew + ENCODER_PIECE);
        }
        if (gctest::failures() > failures_before) std::printf("    in: %s\n", c.family);
    }
}

// Postcode centroids written a row at a time, in pieces that cut rows
// anywhere (of 1 to 7 bytes, and of up to 5000), give the builder's two
// files, whatever their count: none (the footer alone and an empty cells
// file), one, a block less one, a block, a block and one, and many blocks
// over many cells. The decoder gives the rows back, and the decoded size is
// the footer's without a decoder.
TEST(gcs_postcodes_round_trip_in_odd_pieces_and_equal_the_builders_encoder) {
    std::mt19937 rng(20261117);
    for (const uint32_t n : {0u, 1u, 31u, 32u, 33u, 3000u})
        for (const size_t max_piece : {size_t(7), size_t(5000)}) {
            const std::vector<postcode_files::LogicalCentroid> rows = centroid_rows(n);
            const std::vector<uint8_t> logical = logical_of(rows);
            ScratchDir dir("gcs-test");
            {
                postcodes::Enc enc(dir.path());
                enc.state = {1};
                write_in_pieces(enc, 0, logical, rng, max_piece);
                finish_all(enc);
            }
            const auto files = dir_files(dir.path());
            CHECK(files == postcode_files_of(rows));
            if (n == 0) CHECK(files.at(postcode_files::CENTROIDS_FILE).size() == 8 && files.at(postcode_files::CELLS_FILE).empty());
            CHECK_EQ(postcodes::decoded_size(dir.path()), uint64_t(logical.size()));
            postcodes::Dec dec(dir.path());
            CHECK_EQ(dec.decoded_size(0), uint64_t(logical.size()));
            const auto out = decode_all(dec);
            CHECK(out[0] == logical);
        }
}

// A dir without the two files decodes to no centroids; one with either of
// them alone is refused by name, by the decoder and by the decoded size
// alone. The encoder refuses a stream that ends inside a row, and a row the
// files can't hold.
TEST(gcs_postcodes_decoder_takes_a_dir_without_its_files_as_none_and_refuses_one_of_them) {
    ScratchDir dir("gcs-test");
    CHECK_EQ(postcodes::decoded_size(dir.path()), uint64_t(0));
    postcodes::Dec none(dir.path());
    CHECK_EQ(none.decoded_size(0), uint64_t(0));
    CHECK(decode_all(none)[0].empty());
    write_files(dir.path(), postcode_files_of(centroid_rows(50)));
    for (const char* gone : {postcode_files::CELLS_FILE, postcode_files::CENTROIDS_FILE}) {
        ScratchDir one("gcs-test");
        Files f = postcode_files_of(centroid_rows(50));
        f.erase(gone);
        write_files(one.path(), f);
        const std::string refusal = "gcs: " + one.path() + " has one of postcode_centroids.bin and postcode_centroid_cells.bin";
        CHECK_EQ(refusal_of([&] { postcodes::Dec dec(one.path()); }), refusal);
        CHECK_EQ(refusal_of([&] { postcodes::decoded_size(one.path()); }), refusal);
    }
    const std::vector<uint8_t> logical = logical_of(centroid_rows(5));
    {
        ScratchDir out("gcs-test");
        postcodes::Enc enc(out.path());
        enc.state = {1};
        enc.write(0, logical.data(), logical.size() - 3);
        enc.state = {2};
        CHECK_EQ(refusal_of([&] { enc.finish(); }), std::string("gcs: postcodes: the stream ends 17 bytes into a row"));
    }
    {
        ScratchDir out("gcs-test");
        postcodes::Enc enc(out.path());
        enc.state = {1};
        std::vector<uint8_t> twice = logical;
        twice.insert(twice.end(), logical.begin(), logical.begin() + postcode_files::LOGICAL_STRIDE);
        CHECK_EQ(refusal_of([&] { enc.write(0, twice.data(), twice.size()); }),
                 std::string("Malformed postcode centroid files: centroids out of order in a cell"));
    }
}

// Files of no rows with a byte before the footer, which no block decodes, are
// refused by the decoder and by the decoded size alone, from the footer.
TEST(gcs_postcodes_refuse_block_bytes_beside_no_rows) {
    ScratchDir dir("gcs-test");
    Files f = postcode_files_of({});
    std::vector<char>& c = f.at(postcode_files::CENTROIDS_FILE);
    c.insert(c.begin(), char(0));
    write_files(dir.path(), f);
    const std::string refusal = "Malformed postcode centroid files: postcode_centroids.bin has block bytes but no rows";
    CHECK_EQ(refusal_of([&] { postcodes::decoded_size(dir.path()); }), refusal);
    CHECK_EQ(refusal_of([&] {
                 postcodes::Dec dec(dir.path());
                 decode_all(dec);
             }),
             refusal);
}

// A block longer than its rows can be, within the file (a MiB between the
// blocks and the table, which makes the last block, row 32 alone, that long),
// is refused in the strict decoder's words before the reader takes it: its
// buffer stays the size it opened with.
TEST(gcs_postcodes_refuse_a_block_longer_than_its_rows_before_reading_it) {
    ScratchDir dir("gcs-test");
    Files f = postcode_files_of(centroid_rows(33));
    std::vector<char>& c = f.at(postcode_files::CENTROIDS_FILE);
    c.insert(c.end() - std::ptrdiff_t(2 * postcode_files::TABLE_ENTRY_BYTES + postcode_files::FOOTER_BYTES), size_t(1) << 20, char(0));
    const std::vector<char>& k = f.at(postcode_files::CELLS_FILE);
    const std::string strict = refusal_of([&] {
        postcode_files::decode(u8(c), c.size(), u8(k), k.size(), [](const postcode_files::LogicalCentroid&) {});
    });
    CHECK_EQ(strict, std::string("Malformed postcode centroid files: block 1 has more bytes than its rows can take"));
    write_files(dir.path(), f);
    postcodes::Dec dec(dir.path());
    CHECK_EQ(refusal_of([&] { decode_all(dec); }), strict);
    REQUIRE(dec.N);
    CHECK_EQ(dec.N->cap, postcodes::BLOCKS_BUF);
}
