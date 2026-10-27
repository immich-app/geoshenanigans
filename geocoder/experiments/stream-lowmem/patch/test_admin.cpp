// test_admin DIR: encodes synthetic admin sets (polygons without vertices, small blocks and blocks
// past big_block() in both vertex widths) with AdminEnc, fed in odd-sized pieces, decodes them with
// AdminDec and checks the round trip byte for byte. Two orders: big blocks between small ones, and
// big blocks first, last and back to back. Run it with and without GCS_BIG_BLOCK=64 and cmp the two
// DIRs: the big block path (the header through the sequential writer, the planes through positional
// writers on the shared fd, the sequential writer moved past them) must give the same files.
#include <random>
#include <sys/stat.h>
#include "gcs_codec.h"

using namespace gcs;

struct Spec {
    uint32_t vc;
    int w;  // vertex width in u16s (2 or 4)
};

static bool round_trip(const std::string& dir, const std::vector<Spec>& specs, std::mt19937& rng) {
    mkdir(dir.c_str(), 0755);
    std::vector<uint8_t> polys, verts;
    for (const Spec& sp : specs) {
        uint8_t r[24];
        for (auto& b : r) b = uint8_t(rng());
        if (!sp.vc) {
            wr32(r, NO);
            wr32(r + 4, 0);
        } else {
            wr32(r, uint32_t(verts.size()));
            wr32(r + 4, sp.vc);
            uint8_t h[10];
            for (auto& b : h) b = uint8_t(rng());
            h[0] = sp.w == 4 ? 3 : uint8_t(rng() % 3);
            verts.insert(verts.end(), h, h + 10);
            for (uint32_t k = 0; k < sp.vc; k++) {
                uint8_t v[8];
                for (auto& b : v) b = uint8_t(rng());
                verts.insert(verts.end(), v, v + 2 * sp.w);
            }
        }
        polys.insert(polys.end(), r, r + 24);
    }
    {
        Writer pw(dir + "/admin_polygons.bin");
        pw.put(polys);
        pw.finish();
    }
    {
        poly::AdminEnc enc(dir);
        enc.state = {1};
        for (size_t o = 0; o < verts.size();) {
            const size_t c = std::min<size_t>(verts.size() - o, 1 + rng() % 997);
            enc.write(0, &verts[o], c);
            o += c;
        }
        enc.state = {2};
        enc.pump();
        Manifest m;
        enc.finish(m);
    }
    poly::AdminDec dec(dir);
    dec.state = {1};
    while (dec.step()) {}
    const bool ok = dec.out[0].size() == verts.size() && memcmp(dec.out[0].data(), verts.data(), verts.size()) == 0;
    struct stat st;
    stat((dir + "/admin_vertices.B").c_str(), &st);
    printf("big_block %zu, %s: vertices %s (%zu B), admin_vertices.B %lld B\n", big_block(), dir.c_str(), ok ? "ok" : "DIFFER",
           verts.size(), (long long)st.st_size);
    return ok;
}

int main(int argc, char** argv) {
    if (argc < 2) return 2;
    const std::string dir = argv[1];
    mkdir(dir.c_str(), 0755);
    std::mt19937 rng(20261027);
    // (40000 x 8 B, 70000 x 4 B and 33000 x 8 B blocks are past the default 256 KiB big_block)
    const bool mixed = round_trip(dir + "/mixed",
                                  {{0, 0}, {3, 2}, {40000, 4}, {5, 4}, {0, 0}, {70000, 2}, {1, 2}, {17, 4}, {33000, 4}, {2, 2}, {0, 0}, {9, 2}},
                                  rng);
    const bool edges = round_trip(dir + "/edges", {{40000, 4}, {3, 2}, {70000, 2}, {70000, 2}, {0, 0}, {33000, 4}}, rng);
    return mixed && edges ? 0 : 1;
}
