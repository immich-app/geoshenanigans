// gen_place_vectors: writes geocoder/testdata/place_files.tsv, the place files the builder's
// encoder (builder/src/place_files.h) writes for a fixed set of logical streams, which the
// builder's and the server's tests both check (the server's reader and its test encoder,
// place_files.rs, must agree with them byte for byte).
//   g++ -std=c++20 -O1 -I geocoder/builder/src geocoder/testdata/gen_place_vectors.cpp -o gen
//   ./gen > geocoder/testdata/place_files.tsv
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "place_files.h"

using place_files::LogicalPlace;

static std::string hex(const std::vector<uint8_t>& b) {
    std::string out;
    char c[3];
    for (uint8_t x : b) {
        snprintf(c, sizeof c, "%02x", x);
        out += c;
    }
    return out;
}

static void row(const char* what, const std::vector<LogicalPlace>& rows) {
    std::vector<uint8_t> cells, blocks, hidden;
    auto into = [](std::vector<uint8_t>& v) { return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); }; };
    place_files::Encoder enc(into(cells), into(blocks), into(hidden));
    for (const LogicalPlace& p : rows) enc.add(p);
    enc.finish();
    for (const auto& e : enc.table()) blocks.insert(blocks.end(), e.begin(), e.end());
    const auto footer = enc.footer();
    blocks.insert(blocks.end(), footer.begin(), footer.end());
    std::string logical;
    for (const LogicalPlace& p : rows) {
        char b[96];
        snprintf(b, sizeof b, "%s%08x,%08x,%u,%u,%u,%u,%u", logical.empty() ? "" : ";", place_files::bits_of(p.node.lat),
                 place_files::bits_of(p.node.lng), p.node.name_id, unsigned(p.node.place_type), unsigned(p.node._pad1),
                 p.node.parent_poly_id, p.cell_key);
        logical += b;
    }
    printf("%s\t%s\t%s\t%s\t%s\n", what, logical.c_str(), hex(cells).c_str(), hex(blocks).c_str(), hex(hidden).c_str());
}

static LogicalPlace visible(float lat, float lng, uint32_t name, uint8_t type, uint32_t key) {
    LogicalPlace p;
    p.node = PlaceNode{};
    p.node.lat = lat;
    p.node.lng = lng;
    p.node.name_id = name;
    p.node.place_type = type;
    p.cell_key = key;
    return p;
}

static LogicalPlace hidden(float lat, float lng, uint32_t name, uint8_t type, uint32_t parent) {
    LogicalPlace p = visible(lat, lng, name, type, NO_DATA);
    p.node._pad1 = PLACE_FLAG_HIDDEN | (parent == NO_DATA ? 0 : PLACE_FLAG_LABEL_OF);
    p.node.parent_poly_id = parent;
    return p;
}

int main() {
    printf("# Place files as the builder's encoder writes them (builder/src/place_files.h): what the case\n");
    printf("# covers, the logical rows (lat and lng f32 bits in hex, name_id, place_type, flags,\n");
    printf("# parent_poly_id, cell key; ...), and place_cells.bin, place_nodes.bin and place_hidden.bin as\n");
    printf("# hex. The builder's tests encode the rows to the files and decode them back; the server's read\n");
    printf("# the rows from the files and its test encoder (place_files.rs) must write the same files.\n");
    printf("# Regenerate with geocoder/testdata/gen_place_vectors.cpp.\n");
    printf("case\trows\tcells\tnodes\thidden\n");
    row("no nodes", {});
    row("one visible node", {visible(52.37f, 4.89f, 7, 2, 0x47c60000)});
    row("one hidden label", {hidden(52.37f, 4.89f, 7, 0, 12)});
    row("two cells, a label and a hidden node",
        {visible(52.37f, 4.89f, 3, 1, 0x47c60000), visible(52.38f, 4.90f, 900, 2, 0x47c60000),
         visible(-33.87f, 151.21f, 0, 6, 0x6b120000), hidden(52.37f, 4.89f, 12, 0, 5), hidden(0.0f, -0.0f, 1, 4, NO_DATA)});
    row("signed zeros and the edges of the globe",
        {visible(-0.0f, 0.0f, 1, 0, 0x10000000), visible(0.0f, -0.0f, 1, 0, 0x10000000), visible(-90.0f, -180.0f, 2, 3, 0x20000000),
         visible(90.0f, 180.0f, (1u << 31) + 5, 5, 0x30000000), hidden(-90.0f, 180.0f, (1u << 28) - 1, 7, 0xFFFFFFFEu)});
    std::mt19937_64 rng(20261113);
    for (size_t n : {size_t(63), size_t(64), size_t(65), size_t(200)}) {
        std::vector<LogicalPlace> rows;
        uint32_t key = 0x01000000;
        for (size_t i = 0; i < n; i++) {
            if (rng() % 5 == 0) key += 1 + uint32_t(rng() % 1000);
            const float lat = float(int64_t(rng() % 1700000) - 850000) / 10000.0f, lng = float(int64_t(rng() % 3500000) - 1750000) / 10000.0f;
            rows.push_back(visible(lat, lng, uint32_t(rng() % 4000000) >> (rng() % 22), uint8_t(rng() % 7), key));
        }
        for (size_t i = 0; i < n / 7; i++)
            rows.push_back(hidden(float(i), float(i) / 3.0f, uint32_t(rng() % (1u << 28)), uint8_t(rng() % 7),
                                  rng() % 2 ? uint32_t(rng() % 100000) : NO_DATA));
        row((std::to_string(n) + " visible nodes, " + std::to_string(n / 7) + " hidden").c_str(), rows);
    }
    return 0;
}
