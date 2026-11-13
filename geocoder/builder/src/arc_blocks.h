// The blocks of the arc files: arc_vertices.bin holds one arc block per arc
// slot, admin_rings.bin one ring block per polygon slot (arcs-design §4.2,
// §4.4). Integers are little-endian, varints LEB128, and signed values go
// through zigzag.
//
// An arc block is a flags byte (bits 0-1 the grid exponent k: coordinates
// are in units of 10^k e7; bit 2 second-order deltas; bits 3-6 the base
// width b / 2; bit 7 a box follows), the first vertex (i32 latitude, i32
// longitude), the box of its vertices when it has kArcBboxMinVertices or
// more (i32 min longitude, max longitude, max latitude), then its deltas in
// bit-packed groups of up to kArcGroup vertices. A delta is zz(Δ) of the
// vertex's coordinate and the one before it, or with second-order deltas
// zz(Δ - the Δ before it) (0 before the first), whichever block is smaller.
// A group is a header byte dl << 4 | dg, then its kArcGroup (fewer in the
// last group) latitude deltas at b + dl bits each, then its longitude
// deltas at b + dg bits, LSB first, padded to a byte. A short last group's
// two widths sum to at least 8 bits, so its vertex count follows from its
// bytes, and so the arc's from its block's length.
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

#include <algorithm>
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
constexpr uint8_t kSecondOrder = 0x04;
constexpr int kBaseWidthShift = 3;  // bits 3-6: the base width b / 2
constexpr uint8_t kHasBox = 0x80;
// Vertices (deltas) per bit-packed group.
constexpr size_t kArcGroup = 16;
// The widest delta a group stores: b <= 30 and b + 15.
constexpr int kArcMaxDeltaBits = 45;

inline int bit_width(uint64_t v) { return v ? 64 - __builtin_clzll(v) : 0; }

// The deltas of an arc's vertices `lat`/`lng` (grid units) as zz values,
// first-order (Δ) or second-order (Δ minus the Δ before it).
inline void arc_deltas(const std::vector<int64_t>& lat, const std::vector<int64_t>& lng, bool second_order,
                       std::vector<uint64_t>& zl, std::vector<uint64_t>& zg) {
    const size_t m = lat.size() - 1;
    zl.resize(m);
    zg.resize(m);
    int64_t prev_lat = 0, prev_lng = 0;
    for (size_t i = 0; i < m; i++) {
        const int64_t dl = lat[i + 1] - lat[i], dg = lng[i + 1] - lng[i];
        zl[i] = zigzag(second_order ? dl - prev_lat : dl);
        zg[i] = zigzag(second_order ? dg - prev_lng : dg);
        prev_lat = dl;
        prev_lng = dg;
    }
}

// The base width (even, at most 30) of an arc's zz deltas: the narrowest
// from which every group's widths are at most 15 bits wider.
inline int arc_base_width(const std::vector<uint64_t>& zl, const std::vector<uint64_t>& zg) {
    int widest = 0;
    for (size_t i = 0; i < zl.size(); i++) widest = std::max({widest, bit_width(zl[i]), bit_width(zg[i])});
    if (widest > kArcMaxDeltaBits) throw std::logic_error("an arc delta past 45 bits");
    const int over = std::max(0, widest - 15);
    return 2 * ((over + 1) / 2);
}

// The bytes of an arc's delta groups at base width b; writes them at `out`
// unless it is null.
inline size_t put_arc_groups(const std::vector<uint64_t>& zl, const std::vector<uint64_t>& zg, int b, uint8_t* out) {
    const size_t m = zl.size();
    size_t at = 0;
    for (size_t g = 0; g < m; g += kArcGroup) {
        const size_t c = std::min(kArcGroup, m - g);
        int wl = b, wg = b;
        for (size_t i = g; i < g + c; i++) {
            wl = std::max(wl, bit_width(zl[i]));
            wg = std::max(wg, bit_width(zg[i]));
        }
        // A short group's widths sum to 8 or more, so its count follows from its bytes.
        if (c < kArcGroup)
            while (wl + wg < 8) {
                if (wl - b < 15) wl++;
                else wg++;
            }
        if (out) out[at] = static_cast<uint8_t>((wl - b) << 4 | (wg - b));
        at++;
        const size_t bytes = (c * static_cast<size_t>(wl + wg) + 7) / 8;
        if (out) {
            uint64_t acc = 0;
            int n = 0;
            size_t o = at;
            auto put = [&](uint64_t v, int w) {
                // w <= 45 and n < 8 before, so acc holds every bit: a 45-bit value
                // goes in as two parts.
                while (w > 0) {
                    const int take = std::min(w, 56 - n);
                    acc |= (v & ((uint64_t(1) << take) - 1)) << n;
                    n += take;
                    v >>= take;
                    w -= take;
                    while (n >= 8) {
                        out[o++] = static_cast<uint8_t>(acc);
                        acc >>= 8;
                        n -= 8;
                    }
                }
            };
            for (size_t i = g; i < g + c; i++) put(zl[i], wl);
            for (size_t i = g; i < g + c; i++) put(zg[i], wg);
            if (n) out[o++] = static_cast<uint8_t>(acc);
            if (o != at + bytes) throw std::logic_error("an arc group's bytes don't add up");
        }
        at += bytes;
    }
    return at;
}

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
    // Per thread, so the builder's workers each reuse theirs.
    thread_local std::vector<int64_t> lat, lng;
    thread_local std::vector<uint64_t> z1l, z1g, z2l, z2g;
    lat.resize(n);
    lng.resize(n);
    ArcBox box;
    for (size_t i = 0; i < n; i++) {
        const PackedLocation v = vertex(i);
        lat[i] = unit(v.lat_e7);
        lng[i] = unit(v.lon_e7);
        if (boxed) box.add(v);
    }
    // First- or second-order deltas, whichever takes fewer bytes (first on a tie).
    arc_deltas(lat, lng, false, z1l, z1g);
    arc_deltas(lat, lng, true, z2l, z2g);
    const int b1 = arc_base_width(z1l, z1g), b2 = arc_base_width(z2l, z2g);
    const size_t bytes1 = put_arc_groups(z1l, z1g, b1, nullptr), bytes2 = put_arc_groups(z2l, z2g, b2, nullptr);
    const bool second = bytes2 < bytes1;
    const int b = second ? b2 : b1;
    size_t at = 0;
    auto put = [&](size_t bytes) { at += bytes; };
    if (out)
        out[0] = static_cast<uint8_t>(k | (second ? kSecondOrder : 0) | (b / 2) << kBaseWidthShift | (boxed ? kHasBox : 0));
    put(1);
    put(put_i32(static_cast<int32_t>(lat[0]), out ? out + at : nullptr));
    put(put_i32(static_cast<int32_t>(lng[0]), out ? out + at : nullptr));
    if (boxed) {
        put(put_i32(unit(box.min_lng), out ? out + at : nullptr));
        put(put_i32(unit(box.max_lng), out ? out + at : nullptr));
        put(put_i32(unit(box.max_lat), out ? out + at : nullptr));
    }
    put(second ? put_arc_groups(z2l, z2g, b, out ? out + at : nullptr)
               : put_arc_groups(z1l, z1g, b, out ? out + at : nullptr));
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
    out.k = flags & kGridMask;
    out.has_box = (flags & kHasBox) != 0;
    const bool second_order = (flags & kSecondOrder) != 0;
    const int b = 2 * ((flags >> kBaseWidthShift) & 15);
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
        int32_t bx[3];
        for (int32_t& c : bx)
            if (!get_i32(p, end, c)) return false;
        if (!e7(bx[0], out.box.min_lng) || !e7(bx[1], out.box.max_lng) || !e7(bx[2], out.box.max_lat)) return false;
    }
    int64_t lat = lat0, lng = lng0;
    PackedLocation v;
    if (!e7(lat, v.lat_e7) || !e7(lng, v.lon_e7)) return false;
    out.vertices.push_back(v);
    // Each vertex stays in range, so the sums below can't overflow: a delta is
    // at most 45 bits and a first-order one is the difference of two vertices.
    int64_t prev_lat = 0, prev_lng = 0;
    while (p != end) {
        const uint8_t h = *p++;
        const int wl = b + (h >> 4), wg = b + (h & 15), w = wl + wg;
        const size_t rem = static_cast<size_t>(end - p);
        size_t c = kArcGroup;
        if (rem < kArcGroup * static_cast<size_t>(w) / 8) {
            // A short last group: its widths sum to 8 or more.
            if (w < 8) return false;
            c = rem * 8 / static_cast<size_t>(w);
        }
        const size_t bytes = (c * static_cast<size_t>(w) + 7) / 8;
        if (c == 0 || bytes > rem || (c < kArcGroup && bytes != rem)) return false;
        size_t bit = 0;
        auto get = [&](int width) {
            uint64_t z = 0;
            for (int i = 0; i < width; i++, bit++) z |= static_cast<uint64_t>((p[bit >> 3] >> (bit & 7)) & 1) << i;
            return z;
        };
        int64_t dl[kArcGroup], dg[kArcGroup];
        for (size_t i = 0; i < c; i++) dl[i] = unzigzag(get(wl));
        for (size_t i = 0; i < c; i++) dg[i] = unzigzag(get(wg));
        p += bytes;
        for (size_t i = 0; i < c; i++) {
            const int64_t a = second_order ? prev_lat + dl[i] : dl[i];
            const int64_t g = second_order ? prev_lng + dg[i] : dg[i];
            prev_lat = a;
            prev_lng = g;
            lat += a;
            lng += g;
            if (!e7(lat, v.lat_e7) || !e7(lng, v.lon_e7)) return false;
            out.vertices.push_back(v);
        }
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
