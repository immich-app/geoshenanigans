// A region's water layer as the builder holds it from the arc stage to the
// write (water_layer.h): its records, their slot identities, its names, its
// encoded arcs and its cell index. Here, apart from the build steps, so that
// ParsedData can hold one.
#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "admin_arcs.h"
#include "id_allocator.h"
#include "types.h"
#include "water_types.h"

namespace water {

// A region's water: one record per polygon (an outer ring with its holes,
// in polygon order), each with its slot identity, its fallback key and
// whether the major tier holds it; its names; its arcs (the stage's
// encoding, the planet's or a region's share with its own slots) and its
// cell index over the polygons, interior where both tiers' rings hold the
// whole cell.
struct Layer {
    Layer() { arcs.stage = &kWaterArcStage; }

    std::shared_ptr<const TypeTable> types = std::make_shared<TypeTable>();
    // name_id: an offset into `names`; country_code: the type code; area:
    // the object's; ring_offset 0.
    std::vector<AdminPolygon> records;
    std::vector<uint64_t> identities;  // IdAllocator's packed key (ObjectType << 56 | stable id)
    std::vector<uint64_t> fallbacks;   // gc::id_alloc::kNoFallbackKey for none
    std::vector<uint8_t> major;
    std::vector<char> names;  // NUL-terminated, back to back, alphabetical
    AdminArcBlocks arcs;
    std::vector<CellItemPair> cells;

    size_t size() const { return records.size(); }
    bool empty() const { return records.empty(); }
    uint64_t bytes() const {
        auto held = [](const auto& v) { return uint64_t(v.size()) * sizeof(v[0]); };
        return held(records) + held(identities) + held(fallbacks) + held(major) + held(names) + held(cells) +
               arcs.bytes();
    }
};

// The slot identity of a closed way's polygon, and of a relation's outer
// ring (or a piece of one, water_split.h) by the smallest vertex of the ring
// (vertex_key): another outer ring added to the relation leaves it as it
// is. Its fallback key, the relation and the ring's place among the
// relation's outer rings (and the piece's cut), keeps the slot when that
// vertex moves.
inline uint64_t way_identity(int64_t way) {
    return gc::id_alloc::make_key(gc::id_alloc::ObjectType::OSM_WAY, static_cast<uint64_t>(way));
}
inline uint64_t relation_ring_identity(int64_t relation, uint64_t smallest_vertex) {
    const uint64_t h = mix64(mix64(static_cast<uint64_t>(relation) ^ 0x5741544552ull) ^ smallest_vertex);
    return gc::id_alloc::make_key(gc::id_alloc::ObjectType::SYNTHETIC, h);
}
inline uint64_t relation_ring_fallback(int64_t relation, uint32_t outer) {
    return mix64(static_cast<uint64_t>(relation) * 0x10001ull + outer + 0x52494e47ull) & 0x00FFFFFFFFFFFFFFull;
}
// A piece of a ring cut along the grid (water_split.h): its fallback key,
// the ring's with the cut's.
inline uint64_t piece_fallback(uint64_t ring_fallback, uint64_t piece) {
    return piece == 0 ? ring_fallback : mix64(ring_fallback ^ mix64(piece)) & 0x00FFFFFFFFFFFFFFull;
}

}  // namespace water
