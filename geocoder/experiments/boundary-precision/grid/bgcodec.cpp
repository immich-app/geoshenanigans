// Layout "Bg" of /ssd/shrink/stack/layouts.py (layout B with a per-polygon
// divisor), re-implemented in C++ so it runs on the uncapped-size files
// (layouts.py needs ~150 bytes of RAM per vertex). Must be byte-identical to
// `layouts.py encode Bg` (checked on the smaller files by bg_check.sh).
//
//   bgcodec encode <set_dir> <today_admin_vertices.bin> <out.bin>
//   bgcodec decode <set_dir> <in.bin> <out_today_admin_vertices.bin>
//
// Per polygon block at the same offset and size as today's: the 10-byte header
// verbatim except the pad byte, which holds g = gcd of all the polygon's q
// values (lat and lng; 0 -> 1; > 255 -> its largest divisor <= 255); then the
// q / g values as wrapping zigzag deltas at the polygon's own width (first
// vertex absolute), stored as byte planes: lat b0 | lat b1 [| b2 | b3] | lng b0 | ...
// decode reads only <set_dir>/admin_polygons.bin and <in.bin>.
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

static const uint32_t NO_DATA = 0xFFFFFFFF;
static const size_t H = 10;
struct Poly { uint32_t off, cnt, name; uint8_t lvl, pto, p2, p3; float area; uint16_t cc, p4; };
static_assert(sizeof(Poly) == 24, "AdminPolygon is 24 bytes");

static std::vector<uint8_t> slurp(const std::string& p) {
    std::ifstream f(p, std::ios::binary | std::ios::ate);
    if (!f) { std::cerr << "cannot open " << p << "\n"; std::exit(1); }
    std::vector<uint8_t> b(static_cast<size_t>(f.tellg()));
    f.seekg(0); f.read(reinterpret_cast<char*>(b.data()), b.size());
    return b;
}
static void die(const char* m) { std::cerr << m << "\n"; std::exit(1); }

int main(int argc, char** argv) {
    if (argc != 5) die("usage: bgcodec encode|decode <set_dir> <in> <out>");
    std::string mode = argv[1];
    bool enc_mode = mode == "encode";
    if (!enc_mode && mode != "decode") die("mode must be encode or decode");
    auto pb = slurp(std::string(argv[2]) + "/admin_polygons.bin");
    std::vector<Poly> polys(pb.size() / sizeof(Poly));
    std::memcpy(polys.data(), pb.data(), pb.size());
    auto in = slurp(argv[3]);
    std::vector<uint8_t> out(in.size(), 0);
    size_t expect = 0;
    uint64_t gcount[256] = {0};
    for (const Poly& p : polys) {
        if (p.cnt == 0 || p.off == NO_DATA) continue;
        if (p.off != expect) die("blocks not back to back in polygon order");
        const size_t off = p.off, cnt = p.cnt;
        const uint8_t enc = in[off];
        if (enc > 3) die("unknown vertex encoding");
        const size_t w = enc == 3 ? 4 : 2;
        const size_t end = off + H + cnt * 2 * w;
        if (end > in.size()) die("block past end of file");
        const uint64_t mod = 1ull << (8 * w), half = mod >> 1;
        std::memcpy(&out[off], &in[off], H);
        const uint8_t* body = &in[off + H];
        uint8_t* ob = &out[off + H];
        std::vector<uint64_t> q[2];
        if (enc_mode) {
            if (in[off + 1] != 0) die("header pad byte in use");
            for (int c = 0; c < 2; c++) {
                q[c].resize(cnt);
                for (size_t k = 0; k < cnt; k++) {
                    uint64_t v = 0;
                    for (size_t b = 0; b < w; b++) v |= uint64_t(body[k * 2 * w + c * w + b]) << (8 * b);
                    q[c][k] = v;
                }
            }
            uint64_t g = 0;
            for (int c = 0; c < 2; c++) for (uint64_t v : q[c]) g = std::gcd(g, v);
            if (g == 0) g = 1;
            if (g > 255) { uint64_t x = 255; while (g % x) x--; g = x; }
            out[off + 1] = static_cast<uint8_t>(g);
            gcount[g]++;
            for (int c = 0; c < 2; c++) {
                uint64_t prev = 0;
                for (size_t k = 0; k < cnt; k++) {
                    uint64_t v = q[c][k] / g;
                    uint64_t d = (v - prev) & (mod - 1);
                    int64_t s = d >= half ? int64_t(d) - int64_t(mod) : int64_t(d);
                    uint64_t z = s >= 0 ? uint64_t(s) << 1 : (uint64_t(-s) << 1) - 1;
                    for (size_t b = 0; b < w; b++) ob[(c * w + b) * cnt + k] = uint8_t(z >> (8 * b));
                    prev = v;
                }
            }
        } else {
            uint64_t g = in[off + 1];
            if (g == 0) die("divisor 0 in a Bg block");
            out[off + 1] = 0;
            for (int c = 0; c < 2; c++) {
                uint64_t prev = 0;
                for (size_t k = 0; k < cnt; k++) {
                    uint64_t z = 0;
                    for (size_t b = 0; b < w; b++) z |= uint64_t(body[(c * w + b) * cnt + k]) << (8 * b);
                    int64_t s = (z & 1) ? -int64_t((z + 1) >> 1) : int64_t(z >> 1);
                    uint64_t v = (prev + uint64_t(s)) & (mod - 1);
                    prev = v;
                    uint64_t qv = v * g;
                    if (qv >= mod) die("decoded value exceeds the polygon's width");
                    for (size_t b = 0; b < w; b++) ob[k * 2 * w + c * w + b] = uint8_t(qv >> (8 * b));
                }
            }
        }
        expect = end;
    }
    if (expect != in.size()) die("blocks do not cover the whole file");
    std::ofstream(argv[4], std::ios::binary).write(reinterpret_cast<const char*>(out.data()), out.size());
    if (enc_mode) {
        std::printf("encode Bg: %zu bytes; divisors (g: polygons)", out.size());
        for (int g = 1; g < 256; g++) if (gcount[g]) std::printf(" %d:%llu", g, (unsigned long long)gcount[g]);
        std::printf("\n");
    } else {
        std::printf("decode Bg: %zu bytes\n", out.size());
    }
    return 0;
}
