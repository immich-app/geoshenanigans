// The marine areas (build-index --marine): the seas and oceans of the IHO's
// sea areas, as tools/marine_import.py writes them from Marine Regions'
// EEZ-IHO intersect cut to the water off a pinned snapshot of OSM's land
// polygons. Each is an object of the water layer (water_layer.h) of its own
// id space, the sea area's MRGID, apart from OSM's ways and relations; the
// file is the same from one day to the next, so the areas are too.
//
// marine_areas.bin, little-endian: "GCMARINE", u32 version (1), u32 object
// count; then each object, by id ascending: u64 id, u8 kind (1 sea, 2
// ocean), u16 name bytes, the name (UTF-8), u32 polygon count; each polygon:
// u32 ring count (its outer ring, then its holes), each ring: u32 vertex
// count (at least 3, the first not repeated at the end), then its vertices
// as i32 latitude and i32 longitude in e7 units, the first as they are and
// each next as its difference from the one before.
//
// A marine ring has no member ways to cut its arcs at, so its own vertices
// say where: one in kMarineCutEvery, by a hash of its coordinates, starts a
// stretch, and each stretch is a way of its own (a negative id from its
// vertices) for the arc stage (ArcCuts::JunctionsAndWayEnds). A cut so drawn
// falls on the same vertex in every ring through it, whichever way round
// the ring runs, and a long coast is cut into arcs of a few hundred vertices
// that a lookup decodes only where its ray crosses.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "admin_arcs.h"
#include "packed_location.h"
#include "polygon_rings.h"

namespace marine {

inline constexpr char kMagic[8] = {'G', 'C', 'M', 'A', 'R', 'I', 'N', 'E'};
inline constexpr uint32_t kVersion = 1;
inline constexpr uint8_t kSea = 1;
inline constexpr uint8_t kOcean = 2;

// One vertex in this many starts a stretch.
inline constexpr uint64_t kMarineCutEvery = 256;

// One marine object: its id (the sea area's MRGID), kind and name, and its
// polygons, each its rings (outer first, then holes), each ring closed (its
// first vertex repeated at its end).
struct Area {
    uint64_t id = 0;
    uint8_t kind = kSea;
    std::string name;
    std::vector<std::vector<std::vector<PackedLocation>>> polygons;
};

// The areas of a marine_areas.bin, in its order; throws, naming the place,
// on a file of another kind or version, one cut short or with bytes past its
// last object, ids not ascending, an unknown kind, an empty name, a polygon
// of no rings, a ring of fewer than 3 vertices or a vertex off the world.
inline std::vector<Area> read_areas(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    if (!f) throw std::runtime_error("--marine: can't open " + path);
    std::vector<char> data((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    size_t at = 0;
    auto fail = [&](const std::string& why) {
        throw std::runtime_error("--marine: " + path + ": " + why + " (byte " + std::to_string(at) + ")");
    };
    auto take = [&](void* out, size_t n) {
        if (data.size() - at < n) fail("cut short");
        std::memcpy(out, data.data() + at, n);
        at += n;
    };
    auto u32 = [&] {
        uint32_t v;
        take(&v, 4);
        return v;
    };
    // A count of records of at least `least` bytes each, refused past what
    // the rest of the file holds.
    auto count_of = [&](size_t least) {
        const uint32_t n = u32();
        if ((data.size() - at) / least < n) fail("cut short");
        return n;
    };
    constexpr size_t kRingBytes = 4 + 3 * 8, kPolygonBytes = 4 + kRingBytes, kAreaBytes = 8 + 1 + 2 + 1 + 4;
    char magic[8];
    take(magic, 8);
    if (std::memcmp(magic, kMagic, 8) != 0) fail("not a marine areas file");
    if (u32() != kVersion) fail("not of version " + std::to_string(kVersion));
    const uint32_t count = count_of(kAreaBytes);
    std::vector<Area> out;
    out.reserve(count);
    for (uint32_t k = 0; k < count; k++) {
        Area a;
        take(&a.id, 8);
        take(&a.kind, 1);
        uint16_t name_bytes;
        take(&name_bytes, 2);
        if (name_bytes == 0) fail("an object of no name");
        a.name.resize(name_bytes);
        take(a.name.data(), name_bytes);
        if (a.kind != kSea && a.kind != kOcean) fail("object " + std::to_string(a.id) + " of kind " + std::to_string(a.kind));
        if (!out.empty() && a.id <= out.back().id) fail("object " + std::to_string(a.id) + " out of order");
        a.polygons.resize(count_of(kPolygonBytes));
        for (auto& rings : a.polygons) {
            rings.resize(count_of(kRingBytes));
            if (rings.empty()) fail("a polygon of no rings");
            for (auto& ring : rings) {
                const uint32_t n = u32();
                if (n < 3) fail("a ring of " + std::to_string(n) + " vertices");
                if ((data.size() - at) / 8 < n) fail("cut short");
                ring.resize(n + 1);
                int64_t lat = 0, lon = 0;
                for (uint32_t i = 0; i < n; i++) {
                    int32_t step[2];
                    take(step, 8);
                    lat = i == 0 ? step[0] : lat + step[0];
                    lon = i == 0 ? step[1] : lon + step[1];
                    if (lat < -900000000 || lat > 900000000 || lon < -1800000000 || lon > 1800000000)
                        fail("a vertex off the world");
                    ring[i] = {static_cast<int32_t>(lat), static_cast<int32_t>(lon)};
                }
                ring[n] = ring[0];
            }
        }
        out.push_back(std::move(a));
    }
    if (at != data.size()) fail(std::to_string(data.size() - at) + " bytes past the last object");
    return out;
}

// Throws as read_areas does unless `path` reads whole as a marine areas
// file: a check before the build's passes, which read it again once they
// are done. The areas it reads are let go at once, so the passes' peak
// holds none of them.
inline void check_file(const std::string& path) { read_areas(path); }

// Whether a stretch starts at the vertex.
inline bool starts_stretch(PackedLocation v) {
    return (mix64(vertex_key(v) ^ 0x4d4152494e45ull) & (kMarineCutEvery - 1)) == 0;
}

// The way id of a stretch from vertex a, through b and y, to vertex z (a
// and z its cut vertices, b and y the ones next to them inside it): the
// same either way round, negative.
inline int64_t stretch_way(PackedLocation a, PackedLocation b, PackedLocation y, PackedLocation z) {
    uint64_t ends[2] = {vertex_key(a), vertex_key(z)}, inner[2] = {vertex_key(b), vertex_key(y)};
    if (ends[0] > ends[1] || (ends[0] == ends[1] && inner[0] > inner[1])) {
        std::swap(ends[0], ends[1]);
        std::swap(inner[0], inner[1]);
    }
    const uint64_t h = mix64(mix64(mix64(ends[0] ^ 0x5354524554434855ull) ^ ends[1]) ^ mix64(inner[0] * 3 + inner[1]));
    return -static_cast<int64_t>((h >> 2) + 1);
}

// A closed ring's way runs: a stretch from each vertex starts_stretch picks
// to the next, or the whole ring one stretch when it picks none (its way id
// from its smallest vertex and those beside it), in the ring's order from
// its first vertex (the stretch through it split there, both its runs one
// way).
inline std::vector<WayRun> ring_runs(const std::vector<PackedLocation>& ring) {
    if (ring.size() < 4 || ring.front() != ring.back()) throw std::logic_error("marine ring_runs: not a closed ring");
    const uint32_t n = static_cast<uint32_t>(ring.size() - 1);
    std::vector<uint32_t> cuts;
    for (uint32_t i = 0; i < n; i++)
        if (starts_stretch(ring[i])) cuts.push_back(i);
    if (cuts.empty()) {
        uint32_t s = 0;
        for (uint32_t i = 1; i < n; i++)
            if (vertex_key(ring[i]) < vertex_key(ring[s])) s = i;
        return {{stretch_way(ring[s], ring[(s + 1) % n], ring[(s + n - 1) % n], ring[s]), n}};
    }
    auto way_from = [&](size_t j) {
        const uint32_t from = cuts[j], to = cuts[(j + 1) % cuts.size()];
        return stretch_way(ring[from], ring[(from + 1) % n], ring[(to + n - 1) % n], ring[to]);
    };
    std::vector<WayRun> runs;
    const int64_t wrap = way_from(cuts.size() - 1);
    if (cuts[0] > 0) runs.push_back({wrap, cuts[0]});
    for (size_t j = 0; j + 1 < cuts.size(); j++) runs.push_back({way_from(j), cuts[j + 1] - cuts[j]});
    runs.push_back({wrap, n - cuts.back()});
    // Two runs in a row of one way (a ring of one cut starting there) are one.
    std::vector<WayRun> merged;
    for (const WayRun& r : runs) {
        if (!merged.empty() && merged.back().way == r.way) merged.back().edges += r.edges;
        else merged.push_back(r);
    }
    return merged;
}

}  // namespace marine
