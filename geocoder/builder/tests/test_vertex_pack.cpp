// Polygon vertex packing (vertex_pack.h): the bytes are part of the admin,
// POI and addr vertex files, so they must match the packers it replaced.
#include "vertex_pack.h"

#include <random>
#include <utility>
#include <vector>

#include "test_framework.h"

namespace {

// The admin packer vertex_pack.h replaced (POI and addr had copies of it).
uint32_t reference_pack(std::vector<uint8_t>& out, const std::vector<std::pair<double, double>>& verts) {
    double min_lat = verts[0].first, max_lat = verts[0].first;
    double min_lng = verts[0].second, max_lng = verts[0].second;
    for (size_t k = 1; k < verts.size(); k++) {
        if (verts[k].first < min_lat) min_lat = verts[k].first;
        if (verts[k].first > max_lat) max_lat = verts[k].first;
        if (verts[k].second < min_lng) min_lng = verts[k].second;
        if (verts[k].second > max_lng) max_lng = verts[k].second;
    }
    double max_span = std::max(max_lat - min_lat, max_lng - min_lng);
    VertexEncoding enc;
    double scale;
    if (max_span < 65535.0 * 1e-6) { enc = VertexEncoding::U16_011M; scale = 1e-6; }
    else if (max_span < 65535.0 * 1e-5) { enc = VertexEncoding::U16_1M; scale = 1e-5; }
    else if (max_span < 65535.0 * 1e-4) { enc = VertexEncoding::U16_11M; scale = 1e-4; }
    else { enc = VertexEncoding::U32_1CM; scale = 1e-7; }

    uint32_t offset = static_cast<uint32_t>(out.size());
    out.push_back(static_cast<uint8_t>(enc));
    out.push_back(0);
    float bml = static_cast<float>(min_lat), bmg = static_cast<float>(min_lng);
    auto* lp = reinterpret_cast<const uint8_t*>(&bml);
    out.insert(out.end(), lp, lp + 4);
    auto* gp = reinterpret_cast<const uint8_t*>(&bmg);
    out.insert(out.end(), gp, gp + 4);
    for (const auto& [lat, lng] : verts) {
        if (enc == VertexEncoding::U32_1CM) {
            uint32_t a = static_cast<uint32_t>(std::lround((lat - min_lat) / scale));
            uint32_t b = static_cast<uint32_t>(std::lround((lng - min_lng) / scale));
            out.insert(out.end(), reinterpret_cast<uint8_t*>(&a), reinterpret_cast<uint8_t*>(&a) + 4);
            out.insert(out.end(), reinterpret_cast<uint8_t*>(&b), reinterpret_cast<uint8_t*>(&b) + 4);
        } else {
            uint16_t a = static_cast<uint16_t>(std::lround((lat - min_lat) / scale));
            uint16_t b = static_cast<uint16_t>(std::lround((lng - min_lng) / scale));
            out.insert(out.end(), reinterpret_cast<uint8_t*>(&a), reinterpret_cast<uint8_t*>(&a) + 2);
            out.insert(out.end(), reinterpret_cast<uint8_t*>(&b), reinterpret_cast<uint8_t*>(&b) + 2);
        }
    }
    return offset;
}

}  // namespace

TEST(pack_polygon_matches_the_replaced_packer_for_every_encoding) {
    std::mt19937_64 rng(9);
    std::uniform_real_distribution<float> lat0(-80, 80), lng0(-179, 179);
    // Spans that land in each encoding, and the edges between them.
    const double spans[] = {0, 1e-5, 0.03, 65535.0 * 1e-6, 0.3, 65535.0 * 1e-5, 4.0, 65535.0 * 1e-4, 40.0};
    std::vector<uint8_t> want, got_float, got_double;
    for (int round = 0; round < 300; round++) {
        const double span = spans[round % 9];
        std::uniform_real_distribution<double> step(0, span);
        size_t n = 1 + rng() % 40;
        float base_lat = lat0(rng), base_lng = lng0(rng);
        std::vector<NodeCoord> nodes(n);
        std::vector<std::pair<double, double>> pairs(n);
        for (size_t k = 0; k < n; k++) {
            nodes[k] = {static_cast<float>(base_lat + step(rng)), static_cast<float>(base_lng + step(rng))};
            pairs[k] = {nodes[k].lat, nodes[k].lng};
        }
        uint32_t want_off = reference_pack(want, pairs);
        for (auto* got : {&got_float, &got_double}) {
            CHECK_EQ(got->size(), size_t(want_off));
            got->resize(want.size());
        }
        CHECK_EQ(plan_polygon(nodes.data(), n).bytes, want.size() - want_off);
        CHECK_EQ(pack_polygon_at(got_float.data() + want_off, nodes.data(), n), want.size() - want_off);
        CHECK_EQ(pack_polygon_at(got_double.data() + want_off, pairs.data(), n), want.size() - want_off);
    }
    CHECK(got_float == want);
    CHECK(got_double == want);
}

TEST(plan_polygon_sizes_the_header_and_vertex_stride) {
    const std::vector<std::pair<double, double>> small = {{10.0, 20.0}, {10.001, 20.001}};
    const std::vector<std::pair<double, double>> huge = {{-40.0, -60.0}, {30.0, 90.0}, {0.0, 0.0}};
    PolygonPacking p = plan_polygon(small.data(), small.size());
    CHECK(p.enc == VertexEncoding::U16_011M);
    CHECK_EQ(p.bytes, POLY_HEADER_BYTES + 2 * 4);
    p = plan_polygon(huge.data(), huge.size());
    CHECK(p.enc == VertexEncoding::U32_1CM);
    CHECK_EQ(p.bytes, POLY_HEADER_BYTES + 3 * 8);
}
