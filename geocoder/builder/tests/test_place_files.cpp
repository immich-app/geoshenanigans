// The place files (place_files.h): the encoder's files decode to the logical
// stream it took, one stream has one encoding, and the streams and files the
// format can't hold are refused.
#include <cstdint>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "place_files.h"
#include "test_framework.h"
#include "types.h"

using place_files::LogicalPlace;

namespace {

struct Files {
    std::vector<uint8_t> cells, nodes, hidden;
    bool operator==(const Files& o) const { return cells == o.cells && nodes == o.nodes && hidden == o.hidden; }
};

Files encode(const std::vector<LogicalPlace>& rows) {
    Files f;
    auto into = [](std::vector<uint8_t>& v) { return [&v](const uint8_t* p, size_t n) { v.insert(v.end(), p, p + n); }; };
    place_files::Encoder enc(into(f.cells), into(f.nodes), into(f.hidden));
    for (const LogicalPlace& p : rows) enc.add(p);
    enc.finish();
    for (const auto& e : enc.table()) f.nodes.insert(f.nodes.end(), e.begin(), e.end());
    const auto footer = enc.footer();
    f.nodes.insert(f.nodes.end(), footer.begin(), footer.end());
    return f;
}

std::vector<LogicalPlace> decode(const Files& f) {
    std::vector<LogicalPlace> out;
    place_files::decode(f.cells.data(), f.cells.size(), f.nodes.data(), f.nodes.size(), f.hidden.data(), f.hidden.size(),
                        [&](const LogicalPlace& p) { out.push_back(p); });
    return out;
}

bool same(const std::vector<LogicalPlace>& a, const std::vector<LogicalPlace>& b) {
    return a.size() == b.size() && (a.empty() || std::memcmp(a.data(), b.data(), a.size() * sizeof(LogicalPlace)) == 0);
}

LogicalPlace visible(float lat, float lng, uint32_t name, uint8_t type, uint32_t key) {
    LogicalPlace p;
    p.node = PlaceNode{};
    p.node.lat = lat;
    p.node.lng = lng;
    p.node.name_id = name;
    p.node.place_type = type;
    p.cell_key = key;
    return p;
}

LogicalPlace hidden(float lat, float lng, uint32_t name, uint8_t type, uint32_t parent) {
    LogicalPlace p = visible(lat, lng, name, type, NO_DATA);
    p.node._pad1 = PLACE_FLAG_HIDDEN | (parent == NO_DATA ? 0 : PLACE_FLAG_LABEL_OF);
    p.node.parent_poly_id = parent;
    return p;
}

// n visible nodes in cells of ascending keys (several to a cell, some cells
// far apart), names of every width, then n / 5 hidden ones, half of them
// labels.
std::vector<LogicalPlace> random_stream(std::mt19937_64& rng, size_t n) {
    std::vector<LogicalPlace> rows;
    uint32_t key = uint32_t(rng() % 1000);
    for (size_t i = 0; i < n; i++) {
        if (rng() % 4 == 0) key += 1 + uint32_t(rng() % (rng() % 2 ? 3 : 100000));
        const float lat = float(int64_t(rng() % 1800001) - 900000) / 10000.0f, lng = float(int64_t(rng() % 3600001) - 1800000) / 10000.0f;
        rows.push_back(visible(rng() % 13 ? lat : -0.0f, lng, uint32_t(rng() >> (32 + rng() % 32)), uint8_t(rng() % 8), key));
    }
    for (size_t i = 0; i < n / 5; i++)
        rows.push_back(hidden(float(i) * 0.01f, -float(i), uint32_t(rng() % (1u << 28)), uint8_t(rng() % 8),
                              rng() % 2 ? uint32_t(rng() % 50000) : NO_DATA));
    return rows;
}

std::string refusal(const std::vector<LogicalPlace>& rows) {
    try {
        encode(rows);
    } catch (const std::runtime_error& e) {
        return e.what();
    }
    return "";
}

}  // namespace

TEST(place_files_decode_to_the_stream_the_encoder_took) {
    std::mt19937_64 rng(20261113);
    for (size_t n : {size_t(0), size_t(1), size_t(63), size_t(64), size_t(65), size_t(128), size_t(129), size_t(3000)}) {
        const std::vector<LogicalPlace> rows = random_stream(rng, n);
        const Files f = encode(rows);
        CHECK(same(decode(f), rows));
        CHECK(encode(decode(f)) == f);
        // The footer counts the visible rows, the table an entry a block of 64.
        const size_t visible = n, blocks = (visible + 63) / 64;
        CHECK_EQ(place_files::get32(f.nodes.data() + f.nodes.size() - 8), uint32_t(visible));
        CHECK_EQ(f.hidden.size(), (n / 5) * place_files::HIDDEN_ROW_BYTES);
        CHECK(f.nodes.size() >= blocks * place_files::TABLE_ENTRY_BYTES + place_files::FOOTER_BYTES);
    }
}

TEST(a_cell_row_lists_each_cell_with_its_first_row) {
    const Files f = encode({visible(1, 1, 0, 0, 5), visible(2, 2, 0, 0, 5), visible(3, 3, 0, 0, 9), hidden(0, 0, 1, 0, NO_DATA)});
    REQUIRE(f.cells.size() == 2 * place_files::CELL_ROW_BYTES);
    CHECK_EQ(place_files::get32(f.cells.data()), 5u);
    CHECK_EQ(place_files::get32(f.cells.data() + 4), 0u);
    CHECK_EQ(place_files::get32(f.cells.data() + 8), 9u);
    CHECK_EQ(place_files::get32(f.cells.data() + 12), 2u);
}

TEST(the_encoder_refuses_a_stream_the_files_cant_hold) {
    const LogicalPlace v = visible(1, 1, 3, 1, 10), h = hidden(1, 1, 3, 1, NO_DATA);
    auto with = [](LogicalPlace p, auto edit) {
        edit(p);
        return p;
    };
    struct Case {
        std::vector<LogicalPlace> rows;
        std::string refusal;
    };
    const Case cases[] = {
        {{h, v}, "a visible node after a hidden one"},
        {{visible(1, 1, 3, 1, 11), v}, "visible nodes out of cell order"},
        {{with(v, [](LogicalPlace& p) { p.cell_key = NO_DATA; })}, "a visible node without a cell"},
        {{with(h, [](LogicalPlace& p) { p.cell_key = 4; })}, "a hidden node with a cell"},
        {{with(v, [](LogicalPlace& p) { p.node.name_id = NO_DATA; })}, "a node without a name"},
        {{with(v, [](LogicalPlace& p) { p.node.place_type = 8; })}, "a place type past 3 bits"},
        {{with(v, [](LogicalPlace& p) { p.node._pad2 = 1; })}, "a node with its reserved bytes set"},
        {{with(h, [](LogicalPlace& p) { p.node._pad1 |= PLACE_FLAG_LABEL_OF; })}, "a hidden node's parent without its label"},
        {{with(h, [](LogicalPlace& p) { p.node.parent_poly_id = 3; })}, "a hidden node's parent without its label"},
        {{with(h, [](LogicalPlace& p) { p.node.name_id = 1u << 28; })}, "a hidden node's name_id past 28 bits"},
        {{with(h, [](LogicalPlace& p) { p.node._pad1 = PLACE_FLAG_HIDDEN | 0x10; })}, "a node neither visible nor hidden"},
        {{with(h, [](LogicalPlace& p) { p.node._pad1 = PLACE_FLAG_LABEL_OF; p.node.parent_poly_id = 2; })},
         "a node neither visible nor hidden"},
    };
    for (const Case& c : cases) {
        const std::string got = refusal(c.rows);
        CHECK(got.find(c.refusal) != std::string::npos);
        if (got.find(c.refusal) == std::string::npos) std::printf("    want %s, got %s\n", c.refusal.c_str(), got.c_str());
    }
    CHECK_EQ(refusal({v, h}), std::string());
}

// Every file a byte changed in, or cut short, either is refused or decodes
// to a stream whose encoding it is: no stream has two encodings.
TEST(a_logical_stream_has_one_encoding) {
    std::mt19937_64 rng(77);
    const Files f = encode(random_stream(rng, 300));
    size_t refused = 0, decoded = 0;
    for (int t = 0; t < 4000; t++) {
        Files g = f;
        std::vector<uint8_t>& file = t % 3 == 0 ? g.cells : t % 3 == 1 ? g.nodes : g.hidden;
        if (file.empty()) continue;
        if (t % 50 == 0) {
            file.resize(rng() % file.size());
        } else {
            const size_t at = rng() % file.size();
            file[at] ^= uint8_t(1u << (rng() % 8));
        }
        try {
            const std::vector<LogicalPlace> rows = decode(g);
            decoded++;
            CHECK(encode(rows) == g);
        } catch (const std::runtime_error&) {
            refused++;
        }
    }
    CHECK(refused > 0 && decoded > 0);
}

TEST(place_files_are_the_shared_vectors_byte_for_byte) {
    // geocoder/testdata/place_files.tsv, which the server's reader and test encoder read too.
    std::ifstream in(std::string(GEOCODER_TESTDATA_DIR) + "/place_files.tsv");
    REQUIRE(in.good());
    auto bytes = [](const std::string& hex) {
        std::vector<uint8_t> out;
        for (size_t i = 0; i + 1 < hex.size(); i += 2) out.push_back(uint8_t(std::stoi(hex.substr(i, 2), nullptr, 16)));
        return out;
    };
    size_t cases = 0;
    bool header = true;
    for (std::string line; std::getline(in, line);) {
        if (line.empty() || line[0] == '#') continue;
        if (header) {
            header = false;
            continue;
        }
        std::vector<std::string> field;
        size_t a = 0;
        for (size_t b; (b = line.find('\t', a)) != std::string::npos; a = b + 1) field.push_back(line.substr(a, b - a));
        field.push_back(line.substr(a));
        REQUIRE(field.size() == 5);
        std::vector<LogicalPlace> rows;
        std::stringstream rs(field[1]);
        for (std::string r; std::getline(rs, r, ';');) {
            unsigned lat, lng, name, type, flags, parent, key;
            REQUIRE(std::sscanf(r.c_str(), "%x,%x,%u,%u,%u,%u,%u", &lat, &lng, &name, &type, &flags, &parent, &key) == 7);
            LogicalPlace p;
            p.node = PlaceNode{};
            p.node.lat = place_files::float_of(lat);
            p.node.lng = place_files::float_of(lng);
            p.node.name_id = name;
            p.node.place_type = uint8_t(type);
            p.node._pad1 = uint8_t(flags);
            p.node.parent_poly_id = parent;
            p.cell_key = key;
            rows.push_back(p);
        }
        const Files want{bytes(field[2]), bytes(field[3]), bytes(field[4])};
        CHECK(encode(rows) == want);
        CHECK(same(decode(want), rows));
        cases++;
    }
    CHECK(cases >= 9);
}
