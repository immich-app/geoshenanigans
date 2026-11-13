// gen_arc_vectors: writes geocoder/testdata/arc_blocks.tsv, arc blocks the builder's encode_arc
// (builder/src/arc_blocks.h) gives for a fixed set of arcs, which the builder's and the server's
// tests both check (the server's decoder and its test encoder must agree with them byte for byte).
//   g++ -std=c++20 -O1 -I geocoder/builder/src geocoder/testdata/gen_arc_vectors.cpp -o gen
//   ./gen > geocoder/testdata/arc_blocks.tsv
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "arc_blocks.h"

using namespace arc_blocks;

static void row(uint8_t k, const std::vector<PackedLocation>& v, const char* what) {
    auto at = [&](size_t i) { return v[i]; };
    std::vector<uint8_t> out(encode_arc(v.size(), at, k, nullptr));
    encode_arc(v.size(), at, k, out.data());
    std::string verts, hex;
    for (size_t i = 0; i < v.size(); i++) {
        if (i) verts += ';';
        verts += std::to_string(v[i].lat_e7) + "," + std::to_string(v[i].lon_e7);
    }
    char b[3];
    for (uint8_t c : out) {
        snprintf(b, sizeof b, "%02x", c);
        hex += b;
    }
    printf("%d\t%s\t%s\t%s\n", int(k), what, verts.c_str(), hex.c_str());
}

int main() {
    printf("# Arc blocks as the builder's encode_arc writes them (builder/src/arc_blocks.h): the grid exponent k,\n");
    printf("# what the case covers, the vertices in e7 units (lat,lng;...), and the block as hex. The builder's\n");
    printf("# tests encode each to the block and decode it back; the server's decode it to the vertices and its\n");
    printf("# test encoder (arc_fixture.rs) must write the same block. Regenerate with geocoder/testdata/gen_arc_vectors.cpp.\n");
    printf("k\tcase\tvertices\tblock\n");
    std::mt19937_64 rng(20261113);
    for (uint8_t k : {uint8_t(0), uint8_t(2), uint8_t(3)}) {
        const int32_t step = grid_step(k);
        auto on_grid = [&](int64_t c) { return static_cast<int32_t>(c / step * step); };
        for (size_t n : {size_t(1), size_t(2), size_t(3), size_t(15), size_t(16), size_t(17), size_t(31), size_t(33), size_t(80)}) {
            for (int kind = 0; kind < 5; kind++) {
                std::vector<PackedLocation> v;
                int32_t lat = on_grid(int64_t(rng() % 1600000000) - 800000000), lng = on_grid(int64_t(rng() % 3400000000ull) - 1700000000);
                const char* what = "";
                for (size_t i = 0; i < n; i++) {
                    v.push_back({lat, lng});
                    int64_t dl = 0, dg = 0;
                    switch (kind) {
                        case 0: what = "short steps"; dl = int64_t(rng() % 41) - 20; dg = int64_t(rng() % 41) - 20; break;
                        case 1: what = "wide steps"; dl = int64_t(rng() % 4000001) - 2000000; dg = int64_t(rng() % 4000001) - 2000000; break;
                        case 2: what = "straight line"; dl = 137; dg = -59; break;
                        case 3: what = "zigzag"; dl = (i % 2) ? 500 : -500; dg = 3; break;
                        default: what = "one axis"; dl = 0; dg = int64_t(rng() % 9) - 4; break;
                    }
                    lat = static_cast<int32_t>(std::clamp<int64_t>(lat + dl * step, -900000000, 900000000) / step * step);
                    lng = static_cast<int32_t>(std::clamp<int64_t>(lng + dg * step, -1800000000, 1800000000) / step * step);
                }
                row(k, v, what);
            }
        }
        // The globe's corners: deltas past 30 bits at a wider base width.
        row(k, {{on_grid(-900000000), on_grid(-1800000000)}, {on_grid(900000000), on_grid(1800000000)}, {on_grid(-900000000), on_grid(-1800000000)}}, "across the globe");
    }
    return 0;
}
