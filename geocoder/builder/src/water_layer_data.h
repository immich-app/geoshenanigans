// A region's water layer as the builder holds it from the arc stage to the
// write (water_layer.h): its records, their slot identities, its names, its
// encoded arcs and its cell index. Here, apart from the build steps, so that
// ParsedData can hold one.
#pragma once

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "admin_arcs.h"
#include "id_allocator.h"
#include "types.h"
#include "water_types.h"

namespace water {

// A region's water: one record per polygon (an outer ring with its holes,
// in polygon order), each with its slot identity, its fallback key, whether
// the major tier holds it, whether it is a marine area's (marine.h) and its
// object; its names; its arcs (the stage's encoding, the planet's or a
// region's share with its own slots) and its cell index over the polygons,
// interior where both tiers' rings hold the whole cell.
struct Layer {
    Layer() { arcs.stage = &kWaterArcStage; }

    std::shared_ptr<const TypeTable> types = std::make_shared<TypeTable>();
    // name_id: an offset into `names`; country_code: the type code; area:
    // the object's; ring_offset 0.
    std::vector<AdminPolygon> records;
    std::vector<uint64_t> identities;  // IdAllocator's packed key (ObjectType << 56 | stable id)
    std::vector<uint64_t> fallbacks;   // gc::id_alloc::kNoFallbackKey for none
    std::vector<uint8_t> major;
    std::vector<uint8_t> marine;
    std::vector<uint32_t> objects;  // the planet's numbering of the objects
    std::vector<char> names;  // NUL-terminated, back to back, alphabetical
    AdminArcBlocks arcs;
    std::vector<CellItemPair> cells;

    size_t size() const { return records.size(); }
    bool empty() const { return records.empty(); }
    uint64_t bytes() const {
        auto held = [](const auto& v) { return uint64_t(v.size()) * sizeof(v[0]); };
        return held(records) + held(identities) + held(fallbacks) + held(major) + held(marine) + held(objects) +
               held(names) + held(cells) + arcs.bytes();
    }
};

// A region's water arcs, or the slots of them a build's sidecars hold: the
// all set's, and the major tier's.
struct ArcCounts {
    uint64_t all = 0, major = 0;
};

// The slot identity of a closed way's polygon, and of a relation's outer
// ring (or a piece of one, water_split.h) by the ring's corner (ring_corner):
// another outer ring added to the relation leaves it as it is. Its fallback
// key, the relation and the ring's place among the relation's outer rings
// (and the piece's cut path), keeps the slot when that corner moves.
inline uint64_t way_identity(int64_t way) {
    return gc::id_alloc::make_key(gc::id_alloc::ObjectType::OSM_WAY, static_cast<uint64_t>(way));
}
inline uint64_t relation_ring_identity(int64_t relation, uint64_t corner) {
    const uint64_t h = mix64(mix64(static_cast<uint64_t>(relation) ^ 0x5741544552ull) ^ corner);
    return gc::id_alloc::make_key(gc::id_alloc::ObjectType::SYNTHETIC, h);
}
inline uint64_t relation_ring_fallback(int64_t relation, uint32_t outer) {
    return mix64(static_cast<uint64_t>(relation) * 0x10001ull + outer + 0x52494e47ull) & 0x00FFFFFFFFFFFFFFull;
}
// A marine area's polygon (marine.h): its slot identity by the area's id
// and its outer ring's corner (ring_corner), and its fallback key, the area
// and the polygon's place among the area's, which keeps the slot when that
// corner moves (a land snapshot of another day).
inline uint64_t marine_identity(uint64_t area, uint64_t corner) {
    const uint64_t h = mix64(mix64(area ^ 0x4d4152494e45ull) ^ corner);
    return gc::id_alloc::make_key(gc::id_alloc::ObjectType::SYNTHETIC, h);
}
inline uint64_t marine_fallback(uint64_t area, uint32_t polygon) {
    return mix64(area * 0x10001ull + polygon + 0x4d415249ull) & 0x00FFFFFFFFFFFFFFull;
}
// A ring's corner: its smallest vertex (vertex_key) with the two beside it,
// either way round, and the cut of a piece (water_split.h; 0 for none), as
// one key. Two rings of one object that meet at their smallest vertex (outer
// rings touching at a point, pieces at a corner of the grid) have other
// vertices beside it, so their keys differ. `v` holds `count` vertices, the
// first repeated at the end.
inline uint64_t ring_corner(const PackedLocation* v, uint32_t count, uint64_t piece) {
    const uint32_t n = count - 1;
    uint32_t s = 0;
    for (uint32_t i = 1; i < n; i++)
        if (vertex_key(v[i]) < vertex_key(v[s])) s = i;
    uint64_t before = vertex_key(v[(s + n - 1) % n]), after = vertex_key(v[(s + 1) % n]);
    if (before > after) std::swap(before, after);
    const uint64_t corner = mix64(mix64(vertex_key(v[s]) ^ 0x434f524e4552ull) ^ before) ^ after;
    return piece == 0 ? corner : mix64(corner ^ mix64(piece));
}
// A piece of a ring cut along the grid (water_split.h): its fallback key,
// the ring's with its cut's path (SplitPolygon::path), which an edit of the
// shore that leaves the cuts as they were doesn't move.
inline uint64_t piece_fallback(uint64_t ring_fallback, uint64_t path) {
    return path == 0 ? ring_fallback : mix64(ring_fallback ^ mix64(path)) & 0x00FFFFFFFFFFFFFFull;
}

}  // namespace water
