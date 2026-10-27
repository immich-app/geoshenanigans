// test_addr_big: address footprints of more than 69999 vertices (abs2's bound, which the decoders
// hold to) round trip through the raw escape, byte for byte: gc's encoder in one pass (both layouts)
// and in its two streaming passes, gc's decoders (whole, points only, vertices only), and the
// patcher's Enc and Dec.
#include "gcs_codec.h"
#include "../gc/fam_addr.h"
#include <filesystem>
#include <fstream>
#include <iterator>
#include <random>

static std::vector<uint8_t> read_bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(in), {});
}

static void write_bytes(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(bytes.data()), std::streamsize(bytes.size()));
}

static uint32_t f32_bits(float f) {
    uint32_t b;
    memcpy(&b, &f, 4);
    return b;
}

static bool ok = true;
static void expect_same(const std::string& what, const std::vector<uint8_t>& got, const std::vector<uint8_t>& want) {
    if (got == want) return;
    fprintf(stderr, "FAIL %s: %zu bytes, expected %zu\n", what.c_str(), got.size(), want.size());
    ok = false;
}

int main() {
    const char* tmp = std::getenv("TMPDIR");
    std::string root = std::string(tmp ? tmp : "/tmp") + "/addr-big-XXXXXX";
    if (!mkdtemp(root.data())) return 1;
    const std::string src = root + "/src";
    std::filesystem::create_directories(src);
    std::mt19937 rng(20261027);
    struct Spec { uint32_t vc; uint8_t enc; bool closed; uint32_t span; };
    // 69999 vertices still fit abs2; 70000 open and 70001 closed (70000 once the closing vertex is
    // dropped) do not. Narrow (u8) and wide (u16) values, every encoding.
    const Spec specs[] = {{0, 0, false, 0},       {70000, 0, false, 65536}, {5, 1, true, 200},        {69999, 2, false, 65536},
                          {70001, 0, true, 65536}, {0, 0, false, 0},         {70000, 1, false, 256 - 1}, {3, 3, false, 65536},
                          {80000, 3, false, 65536}, {120000, 2, true, 200}};
    std::vector<uint8_t> points, verts;
    for (const Spec& sp : specs) {
        uint32_t rec[7] = {f32_bits(float(rng() % 1800000) / 1e4f - 90.f), f32_bits(float(rng() % 3600000) / 1e4f - 180.f),
                           uint32_t(rng()), uint32_t(rng()), uint32_t(rng()), gcs::NO, 0};
        if (sp.vc) {
            rec[5] = uint32_t(verts.size());
            rec[6] = sp.vc;
            uint8_t h[10] = {sp.enc, 0};
            gcs::wr32(h + 2, f32_bits(float(rng() % 1800000) / 1e4f - 90.f));
            gcs::wr32(h + 6, f32_bits(float(rng() % 3600000) / 1e4f - 180.f));
            verts.insert(verts.end(), h, h + 10);
            const size_t vs = sp.enc == 3 ? 8 : 4, first = verts.size();
            for (uint32_t k = 0; k < sp.vc; k++) {
                uint8_t v[8];
                if (vs == 8) {
                    for (auto& b : v) b = uint8_t(rng());
                } else {
                    gcs::wr16(v, uint16_t(rng() % sp.span));
                    gcs::wr16(v + 2, uint16_t(rng() % sp.span));
                }
                if (k + 1 == sp.vc && sp.closed) memcpy(v, &verts[first], vs);
                verts.insert(verts.end(), v, v + vs);
            }
        }
        const uint8_t* r = reinterpret_cast<const uint8_t*>(rec);
        points.insert(points.end(), r, r + 28);
    }
    write_bytes(src + "/addr_points.bin", points);
    write_bytes(src + "/addr_vertices.bin", verts);

    // gc, one pass: col and pax170, decoded whole
    for (const bool pax : {false, true}) {
        const std::string enc = root + (pax ? "/pax" : "/col"), dec = enc + ".dec";
        std::filesystem::create_directories(enc);
        std::filesystem::create_directories(dec);
        Manifest m;
        addr::encode(src, enc, pax, m);
        addr::decode(enc, dec, m);
        expect_same(std::string("gc ") + (pax ? "pax170" : "col") + " addr_points.bin", read_bytes(dec + "/addr_points.bin"), points);
        expect_same(std::string("gc ") + (pax ? "pax170" : "col") + " addr_vertices.bin", read_bytes(dec + "/addr_vertices.bin"), verts);
    }
    const std::string col = root + "/col";
    Manifest m;  // the keys the decoders read
    m.set("addr.n", uint64_t(points.size() / 28));
    m.set("addr.layout", "col");
    // gc, the two streaming passes: the same files as one pass
    {
        const std::string enc = root + "/split";
        std::filesystem::create_directories(enc);
        Manifest ms;
        want_files() = {"addr_points.bin"};
        addr::encode(src, enc, false, ms);
        want_files() = {"addr_vertices.bin"};
        addr::encode(src, enc, false, ms);
        want_files().clear();
        for (const char* f : {"/addr_points.col", "/addr_vertices.abs2"})
            expect_same(std::string("gc split pass ") + (f + 1), read_bytes(enc + f), read_bytes(col + f));
    }
    // gc, streaming decodes: points alone, vertices alone
    for (const char* f : {"addr_points.bin", "addr_vertices.bin"}) {
        const std::string dec = root + "/only." + f;
        std::filesystem::create_directories(dec);
        want_files() = {f};
        addr::decode(col, dec, m);
        want_files().clear();
        expect_same(std::string("gc decode of ") + f + " alone", read_bytes(dec + "/" + f), f[5] == 'p' ? points : verts);
    }
    // the patcher's encoder (fed in odd pieces) writes gc's files; its decoder reads them back
    {
        const std::string enc = root + "/patcher";
        std::filesystem::create_directories(enc);
        gcs::addr::Enc e(enc, points.size());
        e.state = {1, 1};
        size_t vp = 0;
        for (size_t i = 0; i < points.size() / 28; i++) {
            e.write(0, &points[28 * i], 28);
            const uint32_t vc = gcs::rd32(&points[28 * i + 24]);
            if (!vc) continue;
            const size_t bs = 10 + size_t(vc) * (verts[vp] == 3 ? 8 : 4);
            for (size_t o = 0; o < bs;) {
                const size_t c = std::min<size_t>(bs - o, 1 + rng() % 65521);
                e.write(1, &verts[vp + o], c);
                o += c;
            }
            vp += bs;
        }
        e.state = {2, 2};
        e.pump();
        gcs::Manifest gm;
        e.finish(gm);
        for (const char* f : {"/addr_points.col", "/addr_vertices.abs2"})
            expect_same(std::string("patcher Enc ") + (f + 1), read_bytes(enc + f), read_bytes(col + f));
        gcs::Manifest dm;
        dm.set("addr.n", uint64_t(points.size() / 28));
        dm.set("addr.layout", "col");
        gcs::addr::Dec d(col, dm);
        d.state = {1, 1};
        while (d.step()) {}
        expect_same("patcher Dec addr_points.bin", std::vector<uint8_t>(d.out[0].data(), d.out[0].data() + d.out[0].size()), points);
        expect_same("patcher Dec addr_vertices.bin", std::vector<uint8_t>(d.out[1].data(), d.out[1].data() + d.out[1].size()), verts);
    }
    std::filesystem::remove_all(root);
    fprintf(stderr, ok ? "PASS\n" : "FAIL\n");
    return ok ? 0 : 1;
}
