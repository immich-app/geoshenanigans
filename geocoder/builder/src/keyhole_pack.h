// Packing an admin polygon's keyhole range (polygon_rings.h) so that the
// rings the reader decodes still nest. plan_polygon's grid rounds every
// vertex on its own, by up to half a step (5.6 m on the 1e-4 grid), so a hole
// vertex that close to a slanted outer edge, or to a bordering hole, can
// round across it, and the crossing-number test then counts the sliver beyond
// as inside. A range whose decoded rings stop nesting is packed on the 1 cm
// grid instead, where a vertex decodes to the f32 it was (any of magnitude 1
// or more; within an f32 step below that).
#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "polygon_rings.h"
#include "types.h"
#include "vertex_pack.h"

// The grid pack_keyhole put a range on.
enum class KeyholeGrid : uint8_t {
    Planned,   // plan_polygon's: the decoded rings nest, or there are no holes
    Unnested,  // plan_polygon's: the rings don't nest unrounded either
    Finest,    // the 1 cm grid, as plan_polygon's rounded the rings out of nesting
    Broken,    // the 1 cm grid, the decoded rings still not nesting
};
inline constexpr size_t kKeyholeGrids = 4;

// Whether the keyhole range v[0, n) with `holes` excursions splits into its
// rings and they nest (rings_nest).
template <class P>
bool range_nests(const P* v, uint32_t n, uint16_t holes) {
    std::vector<RingSpan> spans;
    if (!split_rings(v, n, holes, spans)) return false;
    auto ring = [&](const RingSpan& r) {
        std::vector<LatLng> out;
        out.reserve(r.count);
        for (uint32_t k = 0; k < r.count; k++) out.emplace_back(ring_lat(v[r.begin + k]), ring_lng(v[r.begin + k]));
        return out;
    };
    std::vector<std::vector<LatLng>> hole_rings;
    for (size_t r = 1; r < spans.size(); r++) hole_rings.push_back(ring(spans[r]));
    return rings_nest(ring(spans[0]), hole_rings);
}

// A keyhole range's packed bytes and the grid they're on.
struct PackedKeyhole {
    std::vector<uint8_t> bytes;
    KeyholeGrid grid = KeyholeGrid::Planned;
};

// Packs the keyhole range `range` (1 or more vertices, `holes` excursions)
// on plan_polygon's grid, or on the 1 cm grid when the rings decoded from
// that one stop nesting where the given rings nest.
inline PackedKeyhole pack_keyhole(const std::vector<LatLng>& range, uint16_t holes) {
    const auto n = static_cast<uint32_t>(range.size());
    PolygonPacking p = plan_polygon(range.data(), n);
    PackedKeyhole out;
    auto pack = [&] {
        out.bytes.assign(p.bytes, 0);
        pack_polygon(out.bytes.data(), p, range.data(), n);
    };
    auto decoded_nests = [&] {
        const std::vector<NodeCoord> decoded = unpack_polygon(out.bytes.data(), n);
        return range_nests(decoded.data(), n, holes);
    };
    pack();
    if (holes == 0 || decoded_nests()) return out;

    if (!range_nests(range.data(), n, holes)) {
        out.grid = KeyholeGrid::Unnested;
        return out;
    }
    out.grid = KeyholeGrid::Broken;
    if (p.enc == VertexEncoding::U32_1CM) return out;
    p.enc = VertexEncoding::U32_1CM;
    p.scale = VERTEX_SCALE[static_cast<uint8_t>(p.enc)];
    p.bytes = POLY_HEADER_BYTES + size_t{n} * VERTEX_STRIDE[static_cast<uint8_t>(p.enc)];
    pack();
    if (decoded_nests()) out.grid = KeyholeGrid::Finest;
    return out;
}

// How many polygons pack_keyhole put on each grid, for the build log.
struct KeyholeGridCounts {
    uint64_t of[kKeyholeGrids] = {};

    void add(KeyholeGrid grid) { of[static_cast<uint8_t>(grid)]++; }
    // "<a> packed on the 1 cm grid to keep nesting, <b> not nesting there either, <c> not nesting unrounded"
    std::string summary() const {
        auto count = [&](KeyholeGrid grid) { return std::to_string(of[static_cast<uint8_t>(grid)]); };
        return count(KeyholeGrid::Finest) + " packed on the 1 cm grid to keep nesting, " + count(KeyholeGrid::Broken) +
               " not nesting there either, " + count(KeyholeGrid::Unnested) + " not nesting unrounded";
    }
};
