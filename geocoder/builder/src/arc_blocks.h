// The blocks of the arc files: arc_vertices.bin holds one arc block per arc
// slot, admin_rings.bin one ring block per polygon slot (arcs-design §4.2,
// §4.4). Integers are little-endian, varints LEB128, and signed values go
// through zigzag.
//
// An arc block is a flags byte (bits 0-1 the grid exponent k: coordinates
// are in units of 10^k e7; bit 7 a box follows), the first vertex (i32
// latitude, i32 longitude), the box of its vertices when it has
// kArcBboxMinVertices or more (i32 min longitude, max longitude, max
// latitude), then each later vertex as varint zz(Δlat), varint zz(Δlng).
// Its vertex count is implied by its length.
//
// A ring block is a polygon's rings, its outer ring first, back to back. A
// ring is one or more chunks of refs to arc slots: a head varint
// (refs << 2 | has_box << 1 | last_in_ring), with a box the i32 min
// longitude, max longitude and max latitude of its arcs' raw vertices in e7
// units and a varint of the bytes of refs after it, then each ref as
// varint(zz(slot - the slot before it in the chunk, 0 for the first) << 1 |
// reversed). A ring of at most kChunkBoxMinRefs refs is one chunk without a
// box; a longer one is cut where its refs' planet slots say (kRefChunkMin,
// kRefChunkMax, kRefChunkCutMask), so a ref put in or taken out changes only
// the chunk around it and the cuts are the planet's in every region.
#pragma once

#include <climits>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "packed_location.h"
#include "types.h"

// splitmix64's finalizer: a bijection of 64-bit values that spreads every bit.
inline uint64_t mix64(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ull;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebull;
    x ^= x >> 31;
    return x;
}

// An extent in e7 units, the three sides a north ray's skip reads.
struct ArcBox {
    int32_t min_lng = INT32_MAX;
    int32_t max_lng = INT32_MIN;
    int32_t max_lat = INT32_MIN;

    void add(PackedLocation p) {
        if (p.lon_e7 < min_lng) min_lng = p.lon_e7;
        if (p.lon_e7 > max_lng) max_lng = p.lon_e7;
        if (p.lat_e7 > max_lat) max_lat = p.lat_e7;
    }
    void add(const ArcBox& b) {
        if (b.min_lng < min_lng) min_lng = b.min_lng;
        if (b.max_lng > max_lng) max_lng = b.max_lng;
        if (b.max_lat > max_lat) max_lat = b.max_lat;
    }
};
inline bool operator==(const ArcBox& a, const ArcBox& b) {
    return a.min_lng == b.min_lng && a.max_lng == b.max_lng && a.max_lat == b.max_lat;
}

namespace arc_blocks {

inline uint64_t zigzag(int64_t v) { return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63); }
inline int64_t unzigzag(uint64_t z) { return static_cast<int64_t>(z >> 1) ^ -static_cast<int64_t>(z & 1); }

// The encoders write at `out` and return how many bytes they wrote; given a
// null `out` they write nothing and return how many they would.
inline size_t put_varint(uint64_t v, uint8_t* out) {
    size_t n = 0;
    do {
        const uint8_t byte = static_cast<uint8_t>(v & 0x7f) | (v >= 0x80 ? 0x80 : 0);
        if (out) out[n] = byte;
        n++;
        v >>= 7;
    } while (v);
    return n;
}

inline size_t put_i32(int32_t v, uint8_t* out) {
    if (out) {
        const uint32_t u = static_cast<uint32_t>(v);
        for (int i = 0; i < 4; i++) out[i] = static_cast<uint8_t>(u >> (8 * i));
    }
    return 4;
}

// The decoders read from `p` up to `end` and say whether what they read is
// well formed: none reads past `end`.
inline bool get_varint(const uint8_t*& p, const uint8_t* end, uint64_t& v) {
    v = 0;
    for (int shift = 0; shift < 64; shift += 7) {
        if (p == end) return false;
        const uint8_t byte = *p++;
        if (shift == 63 && byte > 1) return false;  // past 64 bits
        v |= static_cast<uint64_t>(byte & 0x7f) << shift;
        if (!(byte & 0x80)) return true;
    }
    return false;
}

inline bool get_i32(const uint8_t*& p, const uint8_t* end, int32_t& v) {
    if (end - p < 4) return false;
    uint32_t u = 0;
    for (int i = 0; i < 4; i++) u |= static_cast<uint32_t>(p[i]) << (8 * i);
    v = static_cast<int32_t>(u);
    p += 4;
    return true;
}

constexpr uint8_t kGridMask = 0x03;
constexpr uint8_t kHasBox = 0x80;

// The grid exponent k of a tier whose grid step is grid_e7 = 10^k e7 units.
inline uint8_t grid_exponent(int32_t grid_e7) {
    int32_t step = 1;
    for (uint8_t k = 0; k <= kGridMask; k++, step *= 10)
        if (step == grid_e7) return k;
    throw std::invalid_argument("an arc grid step of " + std::to_string(grid_e7) + " e7 units is no power of ten up to 1000");
}

inline int32_t grid_step(uint8_t k) {
    int32_t step = 1;
    while (k--) step *= 10;
    return step;
}

// The arc block of the n vertices vertex(0) ... vertex(n - 1) (PackedLocation
// values, each a multiple of 10^k e7 units), n >= 1.
template <class VertexAt>
size_t encode_arc(size_t n, VertexAt vertex, uint8_t k, uint8_t* out) {
    const int32_t step = grid_step(k);
    auto unit = [&](int32_t c) {
        if (c % step != 0) throw std::logic_error("an arc vertex off its tier's grid");
        return c / step;
    };
    const bool boxed = n >= kArcBboxMinVertices;
    size_t at = 0;
    auto put = [&](size_t bytes) { at += bytes; };
    if (out) out[0] = static_cast<uint8_t>(k | (boxed ? kHasBox : 0));
    put(1);
    const PackedLocation first = vertex(0);
    put(put_i32(unit(first.lat_e7), out ? out + at : nullptr));
    put(put_i32(unit(first.lon_e7), out ? out + at : nullptr));
    if (boxed) {
        ArcBox box;
        for (size_t i = 0; i < n; i++) box.add(vertex(i));
        put(put_i32(unit(box.min_lng), out ? out + at : nullptr));
        put(put_i32(unit(box.max_lng), out ? out + at : nullptr));
        put(put_i32(unit(box.max_lat), out ? out + at : nullptr));
    }
    int64_t lat = unit(first.lat_e7), lng = unit(first.lon_e7);
    for (size_t i = 1; i < n; i++) {
        const PackedLocation v = vertex(i);
        const int64_t vlat = unit(v.lat_e7), vlng = unit(v.lon_e7);
        put(put_varint(zigzag(vlat - lat), out ? out + at : nullptr));
        put(put_varint(zigzag(vlng - lng), out ? out + at : nullptr));
        lat = vlat;
        lng = vlng;
    }
    return at;
}

// An arc block's vertices in e7 units, its grid exponent and its box (an
// empty box when it carries none).
struct DecodedArc {
    uint8_t k = 0;
    bool has_box = false;
    ArcBox box;  // e7 units
    std::vector<PackedLocation> vertices;
};

inline bool decode_arc(const uint8_t* data, size_t size, DecodedArc& out) {
    const uint8_t* p = data;
    const uint8_t* end = data + size;
    out.vertices.clear();
    out.box = {};
    if (p == end) return false;
    const uint8_t flags = *p++;
    if (flags & ~(kGridMask | kHasBox)) return false;
    out.k = flags & kGridMask;
    out.has_box = (flags & kHasBox) != 0;
    const int64_t step = grid_step(out.k);
    auto e7 = [&](int64_t units, int32_t& c) {
        const int64_t v = units * step;
        if (v < INT32_MIN || v > INT32_MAX) return false;
        c = static_cast<int32_t>(v);
        return true;
    };
    int32_t lat0 = 0, lng0 = 0;
    if (!get_i32(p, end, lat0) || !get_i32(p, end, lng0)) return false;
    if (out.has_box) {
        int32_t b[3];
        for (int32_t& v : b)
            if (!get_i32(p, end, v)) return false;
        if (!e7(b[0], out.box.min_lng) || !e7(b[1], out.box.max_lng) || !e7(b[2], out.box.max_lat)) return false;
    }
    int64_t lat = lat0, lng = lng0;
    PackedLocation v;
    if (!e7(lat, v.lat_e7) || !e7(lng, v.lon_e7)) return false;
    out.vertices.push_back(v);
    // A delta past the coordinates' whole range is no arc's, and summing one
    // could overflow.
    constexpr int64_t kMaxDelta = int64_t(1) << 33;
    while (p != end) {
        uint64_t dlat = 0, dlng = 0;
        if (!get_varint(p, end, dlat) || !get_varint(p, end, dlng)) return false;
        const int64_t a = unzigzag(dlat), b = unzigzag(dlng);
        if (a > kMaxDelta || a < -kMaxDelta || b > kMaxDelta || b < -kMaxDelta) return false;
        lat += a;
        lng += b;
        if (!e7(lat, v.lat_e7) || !e7(lng, v.lon_e7)) return false;
        out.vertices.push_back(v);
    }
    return true;
}

// A ring's ref as its block stores it, and what its chunking reads: the
// planet slot its cuts hash and the raw box of its arc.
struct RingRef {
    uint32_t slot = 0;
    bool reversed = false;
    uint32_t planet_slot = 0;
    ArcBox box;
};

// Whether a chunk of `refs` refs ends after a ref of planet slot
// `planet_slot`, the ring going on past it.
inline bool cuts_after(uint32_t refs, uint32_t planet_slot) {
    return refs == kRefChunkMax || (refs >= kRefChunkMin && (mix64(planet_slot) & kRefChunkCutMask) == kRefChunkCutMask);
}

// The chunks of one ring of n refs, ref(0) ... ref(n - 1), n >= 1.
template <class RefAt>
size_t encode_ring(size_t n, RefAt ref, uint8_t* out) {
    size_t at = 0;
    auto sub = [&]() { return out ? out + at : nullptr; };
    auto chunk = [&](size_t first, size_t count, bool boxed, bool last) {
        const uint64_t head = (uint64_t(count) << 2) | (boxed ? 2u : 0u) | (last ? 1u : 0u);
        at += put_varint(head, sub());
        size_t body = 0;
        int64_t prev = 0;
        for (size_t i = first; i < first + count; i++) {
            const RingRef r = ref(i);
            body += put_varint((zigzag(int64_t(r.slot) - prev) << 1) | (r.reversed ? 1u : 0u), nullptr);
            prev = r.slot;
        }
        if (boxed) {
            ArcBox box;
            for (size_t i = first; i < first + count; i++) box.add(ref(i).box);
            at += put_i32(box.min_lng, sub());
            at += put_i32(box.max_lng, sub());
            at += put_i32(box.max_lat, sub());
            at += put_varint(body, sub());
        }
        prev = 0;
        for (size_t i = first; i < first + count; i++) {
            const RingRef r = ref(i);
            at += put_varint((zigzag(int64_t(r.slot) - prev) << 1) | (r.reversed ? 1u : 0u), sub());
            prev = r.slot;
        }
    };
    if (n <= kChunkBoxMinRefs) {
        chunk(0, n, false, true);
        return at;
    }
    size_t first = 0;
    for (size_t i = 0; i < n; i++) {
        const size_t count = i - first + 1;
        if (i + 1 == n || cuts_after(static_cast<uint32_t>(count), ref(i).planet_slot)) {
            chunk(first, count, true, i + 1 == n);
            first = i + 1;
        }
    }
    return at;
}

struct SlotRef {
    uint32_t slot = 0;
    bool reversed = false;
};
inline bool operator==(const SlotRef& a, const SlotRef& b) { return a.slot == b.slot && a.reversed == b.reversed; }

struct RingChunk {
    std::vector<SlotRef> refs;
    bool has_box = false;
    ArcBox box;
    bool last_in_ring = false;
};

// A ring block's chunks in order; false for one that isn't well formed (a
// truncated or overlong varint, a chunk of 0 or more than kRefChunkMax refs,
// a box whose byte count disagrees with its refs, a slot outside u32, or a
// block whose last chunk ends no ring).
inline bool decode_ring_block(const uint8_t* data, size_t size, std::vector<RingChunk>& out) {
    out.clear();
    const uint8_t* p = data;
    const uint8_t* end = data + size;
    while (p != end) {
        RingChunk c;
        uint64_t head = 0;
        if (!get_varint(p, end, head)) return false;
        const uint64_t count = head >> 2;
        if (count == 0 || count > kRefChunkMax) return false;
        c.has_box = (head & 2) != 0;
        c.last_in_ring = (head & 1) != 0;
        const uint8_t* body_end = end;
        if (c.has_box) {
            uint64_t body = 0;
            if (!get_i32(p, end, c.box.min_lng) || !get_i32(p, end, c.box.max_lng) || !get_i32(p, end, c.box.max_lat) ||
                !get_varint(p, end, body) || body > uint64_t(end - p))
                return false;
            body_end = p + body;
        }
        int64_t prev = 0;
        for (uint64_t i = 0; i < count; i++) {
            uint64_t v = 0;
            if (!get_varint(p, body_end, v)) return false;
            const int64_t slot = prev + unzigzag(v >> 1);
            if (slot < 0 || slot > int64_t(UINT32_MAX)) return false;
            c.refs.push_back({static_cast<uint32_t>(slot), (v & 1) != 0});
            prev = slot;
        }
        if (c.has_box && p != body_end) return false;
        out.push_back(std::move(c));
    }
    return out.empty() || out.back().last_in_ring;
}

// The rings of a ring block, each its refs in order.
inline bool decode_rings(const uint8_t* data, size_t size, std::vector<std::vector<SlotRef>>& rings) {
    std::vector<RingChunk> chunks;
    if (!decode_ring_block(data, size, chunks)) return false;
    rings.clear();
    bool open = false;
    for (const RingChunk& c : chunks) {
        if (!open) rings.emplace_back();
        rings.back().insert(rings.back().end(), c.refs.begin(), c.refs.end());
        open = !c.last_in_ring;
    }
    return true;
}

}  // namespace arc_blocks
