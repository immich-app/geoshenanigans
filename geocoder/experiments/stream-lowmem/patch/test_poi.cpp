// test_poi DIR: encodes a synthetic POI set (points, small and big polygons, closed and open rings,
// both vertex widths) with PoiEnc, fed in odd-sized pieces, decodes it with PoiDec and checks the
// round trip byte for byte. Polygons of 9000 to 70000 vertices span several decoder steps
// (VERTS_PER_STEP) and, when open, several 64 KiB pieces of the encoder's plane relayout. Run it
// with and without GCS_BIG_BLOCK=64 and cmp the two DIRs: the big block path (planes through
// positional writers, the open-ring relayout) must give the same files.
#include <random>
#include <sys/stat.h>
#include "gcs_codec.h"

using namespace gcs;

int main(int argc, char** argv) {
    if (argc < 2) return 2;
    const std::string dir = argv[1];
    mkdir(dir.c_str(), 0755);
    std::mt19937 rng(20261018);
    std::vector<uint8_t> recs, verts;
    struct Spec { uint32_t vc; int w; bool closed; };
    const Spec specs[] = {{0, 0, false},      {3000, 2, false},  {5, 2, true},       {2000, 4, true},   {0, 0, false},
                          {7, 4, false},      {4096, 2, true},   {1, 2, false},      {2500, 4, false},  {0, 0, false},
                          {2, 2, true},       {70000, 2, false}, {17000, 2, true},   {70000, 4, false}, {17000, 4, true},
                          {9000, 2, false},   {65538, 2, false}, {65537, 4, false},  {65536, 2, false}};
    for (const Spec& sp : specs) {
        uint8_t r[36];
        for (auto& b : r) b = uint8_t(rng());
        if (!sp.vc) {
            wr32(r + 8, NO);
            wr32(r + 12, 0);
        } else {
            wr32(r + 8, uint32_t(verts.size()));
            wr32(r + 12, sp.vc);
            uint8_t h[10];
            for (auto& b : h) b = uint8_t(rng());
            h[0] = sp.w == 4 ? 3 : uint8_t(rng() % 3);
            h[1] = 0;
            verts.insert(verts.end(), h, h + 10);
            const size_t vs = 2 * size_t(sp.w);
            const size_t first = verts.size();
            for (uint32_t k = 0; k < sp.vc; k++) {
                uint8_t v[8];
                for (auto& b : v) b = uint8_t(rng());
                if (k + 1 == sp.vc && sp.closed && sp.vc > 1) memcpy(v, &verts[first], vs);
                if (k + 1 == sp.vc && !sp.closed && sp.vc > 1 && memcmp(v, &verts[first], vs) == 0) v[0] ^= 1;
                verts.insert(verts.end(), v, v + vs);
            }
        }
        recs.insert(recs.end(), r, r + 36);
    }
    // encode, record by record, each piece cut into odd sizes
    poly::PoiEnc enc(dir);
    enc.state = {1, 1};
    size_t vp = 0;
    for (size_t i = 0; i < recs.size() / 36; i++) {
        const uint8_t* r = &recs[36 * i];
        for (size_t o = 0; o < 36;) {
            const size_t c = std::min<size_t>(36 - o, 1 + rng() % 13);
            enc.write(0, r + o, c);
            o += c;
        }
        const uint32_t vc = rd32(r + 12);
        if (!vc) continue;
        const size_t bs = 10 + size_t(vc) * (verts[vp] == 3 ? 8 : 4);
        for (size_t o = 0; o < bs;) {
            const size_t c = std::min<size_t>(bs - o, 1 + rng() % 997);
            enc.write(1, &verts[vp + o], c);
            o += c;
        }
        vp += bs;
    }
    enc.state = {2, 2};
    enc.pump();
    Manifest m;
    enc.finish(m);
    m.save(dir + "/gc.manifest");
    // decode
    poly::PoiDec dec(dir, m);
    dec.state = {1, 1};
    while (dec.step()) {}
    const bool ok_r = dec.out[0].size() == recs.size() && memcmp(dec.out[0].data(), recs.data(), recs.size()) == 0;
    const bool ok_v = dec.out[1].size() == verts.size() && memcmp(dec.out[1].data(), verts.data(), verts.size()) == 0;
    printf("big_block %zu: records %s, vertices %s (%zu B), bopen %s B\n", big_block(), ok_r ? "ok" : "DIFFER", ok_v ? "ok" : "DIFFER",
           verts.size(), m.get("poi.bopen_bytes").c_str());
    return ok_r && ok_v ? 0 : 1;
}
