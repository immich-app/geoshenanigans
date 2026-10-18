// Time deriving the place-node L10 cell runs at load time (place_cells.bin replacement) from
// a place_nodes.bin already sorted by cell. usage: place_cell_bench <place_nodes.bin> <place_cells.bin>
#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <vector>

#pragma pack(push, 1)
struct PlaceNode { float lat, lng; uint32_t name_id; uint8_t t, p1; uint16_t p2; uint32_t parent; };
#pragma pack(pop)

int main(int argc, char** argv) {
    std::ifstream f(argv[1], std::ios::binary);
    std::vector<char> raw((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    std::ifstream fc(argv[2], std::ios::binary);
    std::vector<char> craw((std::istreambuf_iterator<char>(fc)), std::istreambuf_iterator<char>());
    size_t n = raw.size() / sizeof(PlaceNode);
    const PlaceNode* p = reinterpret_cast<const PlaceNode*>(raw.data());
    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::pair<uint64_t, uint32_t>> runs;  // (cell, first node)
    uint64_t prev = 0;
    for (size_t i = 0; i < n; i++) {
        uint64_t c = S2CellId(S2LatLng::FromDegrees(p[i].lat, p[i].lng)).parent(10).id();
        if (i == 0 || c != prev) runs.push_back({c, uint32_t(i)});
        prev = c;
    }
    double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    size_t ncells = craw.size() / 12, match = 0;
    for (size_t k = 0; k < ncells && k < runs.size(); k++) {
        uint64_t id; std::memcpy(&id, craw.data() + k * 12, 8);
        if (id == runs[k].first) match++;
    }
    std::printf("%zu nodes -> %zu runs in %.3fs single thread (%.0f ns/node); shipped-format cells %zu, matching ids %zu\n",
                n, runs.size(), s, s * 1e9 / n, ncells, match);
}
