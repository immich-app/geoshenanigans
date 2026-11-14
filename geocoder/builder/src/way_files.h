// The street and interpolation ways as they sit on disk, a pair of files a
// kind: the rows (street_ways.bin, interp_ways.bin) and the node blocks
// (street_nodes.bin, interp_nodes.bin). The builder writes them and the
// patcher rebuilds them through Encoder; the server reads them in place
// (server/src/way_files.rs). The patch tools diff and replay the logical
// form, which the files hold losslessly: a WayHeader or an InterpWay
// (types.h) a way in the logical ways file, and the ways' nodes back to back
// in the logical nodes file, NODE_BYTES each, lat then lng (f32). The
// encoding has no choices, so a logical pair has one encoding, and the
// decoder refuses any other. geocoder/testdata/way_files.tsv holds pairs both
// sides must write and read.
//
// A way's id is its place in the logical ways file. The ways sit in groups
// of GROUP_WAYS, the last group shorter.
//
// The rows file, a row a group, back to back:
//   off (u32): where the group's first node block starts in the nodes file,
//     which is where the next block starts when none of its ways has nodes;
//   anchor_lat, anchor_lng (u32): the f32 bits of the first node of the
//     group's first way with nodes; 0, 0 when none has;
//   a way: cnt (u16), its nodes, and code (u16), how its block is packed;
//   interpolations only, a way: start_number (u32), span (u32: end_number -
//     start_number mod 2^32) and interpolation (u8);
//   a way: name (u32), a street's name_id, an interpolation's street_id.
//   Footer, WAYS_FOOTER_BYTES: the ways (u32), "SWY1" (streets) or "IWY1".
//   So a row is ROW_HEAD_BYTES and 8 bytes a way, 17 an interpolation, and
//   the file is its rows and its footer to the byte.
// The nodes file: the node blocks of the ways with nodes, in way order, back
//   to back. Footer, NODES_FOOTER_BYTES: the nodes (u64), the ways (u32),
//   "SWN1" or "IWN1". A bit reader's 8-byte loads past the last block land
//   in the footer.
//
// A way's node block:
//   the first node: zl = zigzag32(order_key(lat) - order_key(anchor_lat)) and
//     zg, the same of the lngs, each in wf bytes, wf the byte length of the
//     larger (0 to 4);
//   the nodes after it, under one key for the way: the hybrid key when every
//     coordinate of the way has one, else the order key (coord_keys.h). Per
//     axis, node 1's value is its key less node 0's, a later node's is its
//     step from the node before less that node's step, both mod 2^32,
//     zigzagged; wl is the bit length of the lat values ORed together, wg
//     that of the lng values;
//   the cnt - 1 lat values at wl bits each, then the lng values at wg bits,
//     one stream, LSB first, its last byte filled with zero bits.
//   code = wl | wg << 6 | hybrid << 12 | wf << 13, and the block is 2 wf +
//   ceil((cnt - 1)(wl + wg) / 8) bytes. A way without nodes has code 0 and
//   no block.
//
// Way i is way i % GROUP_WAYS of row i / GROUP_WAYS. Its block starts at the
// row's off plus the bytes of the blocks of the row's ways before it, which
// their cnt and code give.
//
// What the files leave out of a logical record is derived: node_offset is
// the nodes of the ways before it (0 of a way without nodes), end_number is
// start_number + span, and the pads are 0. The encoder refuses a pair in
// which they are anything else, and one the files can't hold: 2^32 - 1 ways
// or more (NO_DATA is no way's id), a way with nodes after 2^32 nodes or
// more (node_offset is a u32), a nodes file of 4 GiB or more (off is a u32).
#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "coord_keys.h"
#include "types.h"

namespace way_files {

inline constexpr size_t GROUP_WAYS = 16, ROW_HEAD_BYTES = 12, WAYS_FOOTER_BYTES = 8, NODES_FOOTER_BYTES = 16, NODE_BYTES = 8;
// The most ways a pair holds, the most nodes a way has, and the most bytes a
// nodes file is.
inline constexpr uint64_t MAX_WAYS = UINT32_MAX - 1, MAX_WAY_NODES = UINT16_MAX, MAX_NODES_FILE_BYTES = UINT32_MAX;
// The size to give for a file the dir does not have. A file of no bytes is
// a file, and a malformed one.
inline constexpr uint64_t ABSENT = ~uint64_t(0);

// A kind of way: its two files and their magics, the bytes of its logical
// record and of a way in a row.
struct Kind {
    const char* ways_file;
    const char* nodes_file;
    char ways_magic[4], nodes_magic[4];
    size_t stride, row_way_bytes;
    bool interp;
    constexpr size_t row_bytes(size_t ways) const { return ROW_HEAD_BYTES + ways * row_way_bytes; }
};
inline constexpr Kind STREETS{"street_ways.bin", "street_nodes.bin", {'S', 'W', 'Y', '1'}, {'S', 'W', 'N', '1'}, sizeof(WayHeader), 8, false};
inline constexpr Kind INTERPS{"interp_ways.bin", "interp_nodes.bin", {'I', 'W', 'Y', '1'}, {'I', 'W', 'N', '1'}, sizeof(InterpWay), 17, true};
inline constexpr size_t MAX_STRIDE = sizeof(InterpWay), MAX_ROW_BYTES = INTERPS.row_bytes(GROUP_WAYS);

// Where a logical record's fields sit: an InterpWay's first three where a
// WayHeader's are.
inline constexpr size_t NODE_OFFSET_AT = 0, NODE_COUNT_AT = 4, PAD_AT = 6, NAME_AT = 8, START_AT = 12, END_AT = 16, INTERPOLATION_AT = 20,
                        INTERP_PAD_AT = 21;
static_assert(offsetof(WayHeader, node_offset) == NODE_OFFSET_AT && offsetof(WayHeader, node_count) == NODE_COUNT_AT &&
                  offsetof(WayHeader, _pad) == PAD_AT && offsetof(WayHeader, name_id) == NAME_AT && sizeof(WayHeader) == 12,
              "a street's logical record is a WayHeader");
static_assert(offsetof(InterpWay, node_offset) == NODE_OFFSET_AT && offsetof(InterpWay, node_count) == NODE_COUNT_AT &&
                  offsetof(InterpWay, _pad1) == PAD_AT && offsetof(InterpWay, street_id) == NAME_AT &&
                  offsetof(InterpWay, start_number) == START_AT && offsetof(InterpWay, end_number) == END_AT &&
                  offsetof(InterpWay, interpolation) == INTERPOLATION_AT && offsetof(InterpWay, _pad4) == INTERP_PAD_AT &&
                  sizeof(InterpWay) == 24,
              "an interpolation's logical record is an InterpWay");
static_assert(sizeof(NodeCoord) == NODE_BYTES && offsetof(NodeCoord, lng) == 4, "a logical node is a NodeCoord");

[[noreturn]] inline void malformed(const std::string& what) { throw std::runtime_error("Malformed way files: " + what); }

inline uint16_t get16(const uint8_t* p) {
    uint16_t v;
    std::memcpy(&v, p, 2);
    return v;
}
inline uint32_t get32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}
inline uint64_t get64(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    return v;
}
inline void put16(uint8_t* p, uint16_t v) { std::memcpy(p, &v, 2); }
inline void put32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
inline void put64(uint8_t* p, uint64_t v) { std::memcpy(p, &v, 8); }

// ------------------------------------------------------------------ node blocks
// A code's parts: wl, wg, the hybrid flag and wf.
constexpr uint32_t lat_bits(uint16_t code) { return code & 63; }
constexpr uint32_t lng_bits(uint16_t code) { return (code >> 6) & 63; }
constexpr bool is_hybrid(uint16_t code) { return (code >> 12) & 1; }
constexpr uint32_t first_bytes(uint16_t code) { return code >> 13; }

// The bytes of the block of a way of cnt nodes packed as code says: none of
// a way without nodes.
constexpr size_t block_bytes(uint16_t cnt, uint16_t code) {
    return cnt ? 2 * size_t(first_bytes(code)) + ((size_t(cnt) - 1) * (lat_bits(code) + lng_bits(code)) + 7) / 8 : 0;
}

// Whether a way of cnt nodes can have the code: none without nodes, else
// widths of at most 32 bits and 4 bytes.
constexpr bool code_fits(uint16_t cnt, uint16_t code) {
    return cnt ? lat_bits(code) <= 32 && lng_bits(code) <= 32 && first_bytes(code) <= 4 : code == 0;
}

// The values of a way's nodes 1 to cnt - 1 under key(bits, key&), to
// each(node, lat value, lng value). False, having stopped, at a coordinate
// that key gives no key for.
template <class Key, class Each>
bool for_values(const uint8_t* nodes, size_t cnt, Key key, Each each) {
    uint32_t last_lat = 0, last_lng = 0, step_lat = 0, step_lng = 0;
    for (size_t j = 0; j < cnt; j++) {
        uint32_t lat, lng;
        if (!key(get32(nodes + NODE_BYTES * j), lat) || !key(get32(nodes + NODE_BYTES * j + 4), lng)) return false;
        if (j) {
            const uint32_t dl = lat - last_lat, dg = lng - last_lng;
            each(j, coord_keys::zigzag32(dl - step_lat), coord_keys::zigzag32(dg - step_lng));
            step_lat = dl;
            step_lng = dg;
        }
        last_lat = lat;
        last_lng = lng;
    }
    return true;
}
struct HybridKey {
    bool operator()(uint32_t bits, uint32_t& key) const { return coord_keys::hybrid_key(bits, key); }
};
struct OrderKey {
    bool operator()(uint32_t bits, uint32_t& key) const {
        key = coord_keys::order_key(bits);
        return true;
    }
};

// The node block of a way's cnt nodes (at least one; NODE_BYTES each, at any
// alignment) under its group's anchor, appended to out; returns its code.
inline uint16_t encode_block(const uint8_t* nodes, size_t cnt, uint32_t anchor_lat, uint32_t anchor_lng, std::vector<uint8_t>& out) {
    using namespace coord_keys;
    if (!cnt || cnt > MAX_WAY_NODES) throw std::logic_error("a node block holds 1 to 65,535 nodes");
    const uint32_t zl = zigzag32(order_key(get32(nodes)) - order_key(anchor_lat)),
                   zg = zigzag32(order_key(get32(nodes + 4)) - order_key(anchor_lng));
    const uint32_t wf = std::max(byte_length(zl), byte_length(zg));
    // The widths under the hybrid key, where every coordinate has one, else
    // under the order key. The keys are made again to pack them, so no way's
    // keys are held.
    uint32_t any_lat = 0, any_lng = 0;
    auto widen = [&](size_t, uint32_t vl, uint32_t vg) {
        any_lat |= vl;
        any_lng |= vg;
    };
    const bool hybrid = for_values(nodes, cnt, HybridKey{}, widen);
    if (!hybrid) {
        any_lat = any_lng = 0;
        for_values(nodes, cnt, OrderKey{}, widen);
    }
    const uint32_t wl = bit_length(any_lat), wg = bit_length(any_lng);
    const uint16_t code = uint16_t(wl | wg << 6 | uint32_t(hybrid) << 12 | wf << 13);
    const size_t at = out.size(), bytes = block_bytes(uint16_t(cnt), code);
    // Zeroed, with the 8 bytes to spare put_bits asks for.
    out.resize(at + bytes + 8);
    uint8_t* p = out.data() + at;
    for (uint32_t b = 0; b < wf; b++) {
        p[b] = uint8_t(zl >> (8 * b));
        p[wf + b] = uint8_t(zg >> (8 * b));
    }
    uint8_t* bits = p + 2 * wf;
    const uint64_t lng_at = uint64_t(cnt - 1) * wl;
    auto pack = [&](size_t j, uint32_t vl, uint32_t vg) {
        put_bits(bits, uint64_t(j - 1) * wl, vl, wl);
        put_bits(bits, lng_at + uint64_t(j - 1) * wg, vg, wg);
    };
    if (hybrid) for_values(nodes, cnt, HybridKey{}, pack);
    else for_values(nodes, cnt, OrderKey{}, pack);
    out.resize(at + bytes);
    return code;
}

// A way's cnt nodes (at least one) to out (NODE_BYTES each) from its block
// at p, block_bytes(cnt, code) bytes with 8 more after them that can be
// read. Refuses a block the encoder would not write for its nodes.
inline void decode_block(const uint8_t* p, uint16_t cnt, uint16_t code, uint32_t anchor_lat, uint32_t anchor_lng, uint8_t* out) {
    using namespace coord_keys;
    if (!cnt || !code_fits(cnt, code)) malformed("a node block with no nodes or a width out of range");
    const uint32_t wl = lat_bits(code), wg = lng_bits(code), wf = first_bytes(code);
    const bool hybrid = is_hybrid(code);
    uint32_t zl = 0, zg = 0;
    for (uint32_t b = 0; b < wf; b++) {
        zl |= uint32_t(p[b]) << (8 * b);
        zg |= uint32_t(p[wf + b]) << (8 * b);
    }
    if (std::max(byte_length(zl), byte_length(zg)) != wf) malformed("a way's first node in more bytes than it needs");
    const uint32_t lat0 = from_order_key(order_key(anchor_lat) + unzigzag32(zl)),
                   lng0 = from_order_key(order_key(anchor_lng) + unzigzag32(zg));
    put32(out, lat0);
    put32(out + 4, lng0);
    // Whether every coordinate so far has a hybrid key: the encoder's choice.
    uint32_t key_lat = 0, key_lng = 0;
    bool all_hybrid = hybrid_key(lat0, key_lat) && hybrid_key(lng0, key_lng);
    if (hybrid && !all_hybrid) malformed("a way under the hybrid key whose first node has none");
    if (!hybrid) {
        key_lat = order_key(lat0);
        key_lng = order_key(lng0);
    }
    const uint8_t* bits = p + 2 * wf;
    const uint64_t lng_at = uint64_t(cnt - 1) * wl;
    uint32_t step_lat = 0, step_lng = 0, any_lat = 0, any_lng = 0;
    for (size_t j = 1; j < cnt; j++) {
        const uint32_t vl = get_bits(bits, uint64_t(j - 1) * wl, wl), vg = get_bits(bits, lng_at + uint64_t(j - 1) * wg, wg);
        any_lat |= vl;
        any_lng |= vg;
        step_lat += unzigzag32(vl);
        step_lng += unzigzag32(vg);
        key_lat += step_lat;
        key_lng += step_lng;
        uint32_t lat, lng;
        if (hybrid) {
            if (!is_hybrid_key(key_lat) || !is_hybrid_key(key_lng)) malformed("a way under the hybrid key with a key no coordinate has");
            lat = from_hybrid_key(key_lat);
            lng = from_hybrid_key(key_lng);
        } else {
            lat = from_order_key(key_lat);
            lng = from_order_key(key_lng);
            uint32_t unused;
            all_hybrid = all_hybrid && hybrid_key(lat, unused) && hybrid_key(lng, unused);
        }
        put32(out + NODE_BYTES * j, lat);
        put32(out + NODE_BYTES * j + 4, lng);
    }
    if (bit_length(any_lat) != wl || bit_length(any_lng) != wg) malformed("a way's values in more bits than they need");
    if (!hybrid && all_hybrid) malformed("a way under the order key whose coordinates all have hybrid keys");
    if (!padding_is_zero(bits, uint64_t(cnt - 1) * (wl + wg))) malformed("bits set past a node block's last value");
}

// ------------------------------------------------------------------ rows
// A group's row: where its blocks start, its anchor and its ways' fields
// (the last three an interpolation's, 0 of a street).
struct Row {
    uint32_t off = 0, anchor_lat = 0, anchor_lng = 0;
    size_t ways = 0;
    std::array<uint16_t, GROUP_WAYS> cnt{}, code{};
    std::array<uint32_t, GROUP_WAYS> name{}, start{}, span{};
    std::array<uint8_t, GROUP_WAYS> interpolation{};

    // The bytes of way j's block, of the blocks of the ways before it, and of
    // all the group's blocks.
    size_t block_bytes(size_t j) const { return way_files::block_bytes(cnt[j], code[j]); }
    uint64_t bytes_before(size_t j) const {
        uint64_t n = 0;
        for (size_t k = 0; k < j; k++) n += block_bytes(k);
        return n;
    }
    uint64_t blocks_bytes() const { return bytes_before(ways); }
    // The group's nodes.
    uint64_t nodes() const {
        uint64_t n = 0;
        for (size_t k = 0; k < ways; k++) n += cnt[k];
        return n;
    }
};

// Refuses a row no group has: no ways or more than GROUP_WAYS, a code its
// way's nodes can't have, an anchor that is not the first node of the first
// way with nodes (that way's first node takes no bytes) or is not 0, 0
// without one.
inline void check_row(const Row& r) {
    if (!r.ways || r.ways > GROUP_WAYS) malformed("a row of no ways, or of more than a group's");
    bool anchored = false;
    for (size_t j = 0; j < r.ways; j++) {
        if (!code_fits(r.cnt[j], r.code[j])) malformed(r.cnt[j] ? "a code with a width out of range" : "a way without nodes that has a code");
        if (r.cnt[j] && !anchored) {
            if (first_bytes(r.code[j])) malformed("an anchor that is not the first node of its group's first way with nodes");
            anchored = true;
        }
    }
    if (!anchored && (r.anchor_lat || r.anchor_lng)) malformed("an anchor in a group without nodes");
}

// The row's bytes to out, kind.row_bytes(r.ways) of them.
inline void encode_row(const Kind& kind, const Row& r, uint8_t* out) {
    put32(out, r.off);
    put32(out + 4, r.anchor_lat);
    put32(out + 8, r.anchor_lng);
    uint8_t* p = out + ROW_HEAD_BYTES;
    for (size_t j = 0; j < r.ways; j++, p += 4) {
        put16(p, r.cnt[j]);
        put16(p + 2, r.code[j]);
    }
    if (kind.interp)
        for (size_t j = 0; j < r.ways; j++, p += 9) {
            put32(p, r.start[j]);
            put32(p + 4, r.span[j]);
            p[8] = r.interpolation[j];
        }
    for (size_t j = 0; j < r.ways; j++, p += 4) put32(p, r.name[j]);
}

// The row of a group of `ways` ways from its bytes, kind.row_bytes(ways) of
// them. Refuses what check_row refuses.
inline Row decode_row(const Kind& kind, const uint8_t* p, size_t ways) {
    Row r;
    r.ways = ways;
    if (!ways || ways > GROUP_WAYS) malformed("a row of no ways, or of more than a group's");
    r.off = get32(p);
    r.anchor_lat = get32(p + 4);
    r.anchor_lng = get32(p + 8);
    p += ROW_HEAD_BYTES;
    for (size_t j = 0; j < ways; j++, p += 4) {
        r.cnt[j] = get16(p);
        r.code[j] = get16(p + 2);
    }
    if (kind.interp)
        for (size_t j = 0; j < ways; j++, p += 9) {
            r.start[j] = get32(p);
            r.span[j] = get32(p + 4);
            r.interpolation[j] = p[8];
        }
    for (size_t j = 0; j < ways; j++, p += 4) r.name[j] = get32(p);
    check_row(r);
    return r;
}

// The logical record of the row's way j, kind.stride bytes to out, given the
// nodes of the ways before it. Refuses a way with nodes whose node_offset
// would pass a u32.
inline void logical_record(const Kind& kind, const Row& r, size_t j, uint64_t nodes_before, uint8_t* out) {
    if (r.cnt[j] && nodes_before > UINT32_MAX) malformed(std::string(kind.ways_file) + " has a way with nodes after 2^32 nodes or more");
    std::memset(out, 0, kind.stride);
    put32(out + NODE_OFFSET_AT, r.cnt[j] ? uint32_t(nodes_before) : 0);
    put16(out + NODE_COUNT_AT, r.cnt[j]);
    put32(out + NAME_AT, r.name[j]);
    if (kind.interp) {
        put32(out + START_AT, r.start[j]);
        put32(out + END_AT, r.start[j] + r.span[j]);
        out[INTERPOLATION_AT] = r.interpolation[j];
    }
}

// ------------------------------------------------------------------ footers
// The rows file's footer for this many ways.
inline std::array<uint8_t, WAYS_FOOTER_BYTES> ways_footer(const Kind& kind, uint64_t ways) {
    if (ways > MAX_WAYS) malformed(std::string(kind.ways_file) + " would hold 2^32 - 1 ways or more: NO_DATA is no way's id");
    std::array<uint8_t, WAYS_FOOTER_BYTES> f;
    put32(f.data(), uint32_t(ways));
    std::memcpy(f.data() + 4, kind.ways_magic, 4);
    return f;
}

// The nodes file's footer for this many nodes and ways, after blocks_bytes
// of blocks. Refuses a file of 4 GiB or more.
inline std::array<uint8_t, NODES_FOOTER_BYTES> nodes_footer(const Kind& kind, uint64_t nodes, uint64_t ways, uint64_t blocks_bytes) {
    if (ways > MAX_WAYS) malformed(std::string(kind.ways_file) + " would hold 2^32 - 1 ways or more: NO_DATA is no way's id");
    if (blocks_bytes > MAX_NODES_FILE_BYTES - NODES_FOOTER_BYTES) malformed(std::string(kind.nodes_file) + " would be 4 GiB or more");
    std::array<uint8_t, NODES_FOOTER_BYTES> f;
    put64(f.data(), nodes);
    put32(f.data() + 8, uint32_t(ways));
    std::memcpy(f.data() + 12, kind.nodes_magic, 4);
    return f;
}

// What the two footers say of a pair: its ways and nodes, and the bytes of
// its rows and of its blocks.
struct Footers {
    uint32_t ways = 0;
    uint64_t nodes = 0, rows_bytes = 0, blocks_bytes = 0;
    uint64_t groups() const { return (uint64_t(ways) + GROUP_WAYS - 1) / GROUP_WAYS; }
    // The ways of group g.
    size_t group_ways(uint64_t g) const { return size_t(std::min<uint64_t>(GROUP_WAYS, ways - g * GROUP_WAYS)); }
};

// The footers of a pair, each file given by its size and its last bytes (its
// footer; all of a shorter file), or by the size ABSENT when the dir has no
// such file. A dir with neither file holds no ways. Refuses one file without
// the other, a footer without its magic, footers that count different ways
// or more nodes than those ways can have, a rows file that is not its ways'
// rows, and a nodes file of 4 GiB or more or longer than its nodes can fill.
inline Footers read_footers(const Kind& kind, const uint8_t* ways_end, uint64_t ways_size, const uint8_t* nodes_end, uint64_t nodes_size) {
    Footers f;
    if (ways_size == ABSENT && nodes_size == ABSENT) return f;
    const std::string ways_file = kind.ways_file, nodes_file = kind.nodes_file;
    if (ways_size == ABSENT) malformed(nodes_file + " without " + ways_file);
    if (nodes_size == ABSENT) malformed(ways_file + " without " + nodes_file);
    if (ways_size < WAYS_FOOTER_BYTES || std::memcmp(ways_end + 4, kind.ways_magic, 4) != 0)
        malformed(ways_file + " has no " + std::string(kind.ways_magic, 4) + " footer");
    if (nodes_size < NODES_FOOTER_BYTES || std::memcmp(nodes_end + 12, kind.nodes_magic, 4) != 0)
        malformed(nodes_file + " has no " + std::string(kind.nodes_magic, 4) + " footer");
    f.ways = get32(ways_end);
    f.nodes = get64(nodes_end);
    f.rows_bytes = ways_size - WAYS_FOOTER_BYTES;
    f.blocks_bytes = nodes_size - NODES_FOOTER_BYTES;
    if (get32(nodes_end + 8) != f.ways) malformed(ways_file + " and " + nodes_file + " count different ways");
    if (f.ways > MAX_WAYS) malformed(ways_file + " holds 2^32 - 1 ways or more: NO_DATA is no way's id");
    if (f.rows_bytes != f.groups() * ROW_HEAD_BYTES + uint64_t(f.ways) * kind.row_way_bytes)
        malformed(ways_file + " is not the rows of its " + std::to_string(f.ways) + " ways");
    if (nodes_size > MAX_NODES_FILE_BYTES) malformed(nodes_file + " is 4 GiB or more");
    if (f.nodes > uint64_t(f.ways) * MAX_WAY_NODES) malformed(nodes_file + " counts more nodes than its ways can have");
    if (f.blocks_bytes > f.nodes * NODE_BYTES) malformed(nodes_file + " is longer than its nodes can fill");
    return f;
}

// The bytes of the logical ways file and of the logical nodes file.
struct LogicalSizes {
    uint64_t ways = 0, nodes = 0;
};
inline LogicalSizes logical_sizes(const Kind& kind, const Footers& f) { return {uint64_t(f.ways) * kind.stride, f.nodes * NODE_BYTES}; }

// ------------------------------------------------------------------ encoder
// The logical pair in, a way at a time, and the two files out through the
// sinks (called with bytes and a length): a way's block as the way comes, a
// group's row when its last way has. The caller writes the footers after
// finish(). Holds one row and one way's block. Refuses a pair the files
// can't hold.
//
// An encoder may start at any group's first way, given the ways and the
// nodes before it, and encode a slice of a pair: its rows count their offs
// from the slice's first block (rebase_rows moves them on), and the footers
// are made from the pair's counts (ways_footer, nodes_footer).
template <class RowSink, class BlockSink>
class Encoder {
public:
    Encoder(const Kind& kind, RowSink rows, BlockSink blocks, uint64_t ways_before = 0, uint64_t nodes_before = 0)
        : kind_(kind), rows_(rows), blocks_(blocks), ways_(ways_before), nodes_(nodes_before) {
        if (ways_before % GROUP_WAYS) throw std::logic_error("a way encoder starts at a group's first way");
    }

    // The next way: its logical record (kind.stride bytes) and its nodes
    // (NODE_BYTES each, at any alignment; not read for a way without nodes).
    void add(const uint8_t* record, const uint8_t* nodes) {
        const uint16_t cnt = checked_count(record);
        if (!row_.ways) open_row();
        uint16_t code = 0;
        if (cnt) {
            if (!anchored_) {
                row_.anchor_lat = get32(nodes);
                row_.anchor_lng = get32(nodes + 4);
                anchored_ = true;
            }
            block_.clear();
            code = encode_block(nodes, cnt, row_.anchor_lat, row_.anchor_lng, block_);
            grow(block_.size());
            blocks_(static_cast<const uint8_t*>(block_.data()), block_.size());
        }
        place(record, cnt, code);
        if (row_.ways == GROUP_WAYS) write_row();
    }

    // The next group, whole (GROUP_WAYS ways, or fewer as the pair's last),
    // taken as another pair of files has it coded: its ways' logical
    // records, back to back, and its row there, whose anchor, counts and
    // codes the row written here keeps. The group's blocks follow through
    // put_coded_bytes, coded.blocks_bytes() of them, as they are there. No
    // node is read: the codes and the bytes are the encoder's when those
    // files are, and the records' nodes are taken to be the ones the blocks
    // hold.
    void put_coded_group(const Row& coded, const uint8_t* records) {
        if (row_.ways || owed_) throw std::logic_error("a coded group starts at a group's first way, after the blocks of the one before");
        check_row(coded);
        open_row();
        for (size_t j = 0; j < coded.ways; j++) {
            const uint8_t* record = records + j * kind_.stride;
            if (checked_count(record) != coded.cnt[j]) refuse("has another node count than its coded group's row");
            place(record, coded.cnt[j], coded.code[j]);
        }
        row_.anchor_lat = coded.anchor_lat;
        row_.anchor_lng = coded.anchor_lng;
        owed_ = coded.blocks_bytes();
        grow(owed_);
        write_row();
    }

    // The next bytes of the blocks of the group put_coded_group took.
    void put_coded_bytes(const uint8_t* p, size_t n) {
        if (n > owed_) throw std::logic_error("more bytes than the coded group's blocks");
        blocks_(p, n);
        owed_ -= n;
    }

    // Writes the last group's row.
    void finish() {
        if (owed_) throw std::logic_error("a coded group's blocks are not all in");
        if (row_.ways) write_row();
        closed_ = true;
    }

    // The ways and nodes so far, those before the encoder's first way among
    // them, and the bytes of its own blocks.
    uint64_t ways() const { return ways_; }
    uint64_t nodes() const { return nodes_; }
    uint64_t blocks_bytes() const { return blocks_bytes_; }

    // The two footers of the pair an encoder took whole, after finish().
    std::array<uint8_t, WAYS_FOOTER_BYTES> ways_footer() const { return way_files::ways_footer(kind_, ways_); }
    std::array<uint8_t, NODES_FOOTER_BYTES> nodes_footer() const { return way_files::nodes_footer(kind_, nodes_, ways_, blocks_bytes_); }

private:
    [[noreturn]] void refuse(const std::string& what) const {
        malformed(std::string(kind_.ways_file) + " way " + std::to_string(ways_) + " " + what);
    }

    // The next record's node count, the record checked.
    uint16_t checked_count(const uint8_t* record) const {
        if (owed_) throw std::logic_error("a coded group's blocks are not all in");
        if (closed_) throw std::logic_error("a way after the pair's last group");
        if (ways_ >= MAX_WAYS) malformed(std::string(kind_.ways_file) + " would hold 2^32 - 1 ways or more: NO_DATA is no way's id");
        if (get16(record + PAD_AT) || (kind_.interp && (record[INTERP_PAD_AT] | record[INTERP_PAD_AT + 1] | record[INTERP_PAD_AT + 2])))
            refuse("has its padding set");
        const uint32_t offset = get32(record + NODE_OFFSET_AT);
        const uint16_t cnt = get16(record + NODE_COUNT_AT);
        if (cnt && nodes_ > UINT32_MAX) refuse("has nodes after 2^32 nodes or more");
        if (offset != (cnt ? uint32_t(nodes_) : 0))
            refuse("has a node_offset of " + std::to_string(offset) + ", not " + (cnt ? "the " + std::to_string(nodes_) + " nodes before it" : "0"));
        return cnt;
    }

    // A row for the next group, which starts where the blocks so far end.
    void open_row() {
        row_.off = uint32_t(blocks_bytes_);
        row_.anchor_lat = row_.anchor_lng = 0;
        anchored_ = false;
    }

    // The way into the row.
    void place(const uint8_t* record, uint16_t cnt, uint16_t code) {
        const size_t j = row_.ways++;
        row_.cnt[j] = cnt;
        row_.code[j] = code;
        row_.name[j] = get32(record + NAME_AT);
        if (kind_.interp) {
            row_.start[j] = get32(record + START_AT);
            row_.span[j] = get32(record + END_AT) - row_.start[j];
            row_.interpolation[j] = record[INTERPOLATION_AT];
        }
        ways_++;
        nodes_ += cnt;
    }

    // n more bytes of blocks.
    void grow(uint64_t n) {
        if (blocks_bytes_ + n > MAX_NODES_FILE_BYTES - NODES_FOOTER_BYTES) malformed(std::string(kind_.nodes_file) + " would be 4 GiB or more");
        blocks_bytes_ += n;
    }

    void write_row() {
        uint8_t bytes[MAX_ROW_BYTES];
        encode_row(kind_, row_, bytes);
        rows_(static_cast<const uint8_t*>(bytes), kind_.row_bytes(row_.ways));
        closed_ = row_.ways < GROUP_WAYS;
        row_.ways = 0;
    }

    const Kind& kind_;
    RowSink rows_;
    BlockSink blocks_;
    Row row_;
    std::vector<uint8_t> block_;
    uint64_t ways_, nodes_, blocks_bytes_ = 0, owed_ = 0;
    bool anchored_ = false, closed_ = false;
};

// Moves the offs of a slice's rows (as an encoder that started at the
// slice's first way wrote them: whole rows, the last may be a short one) on
// by the bytes of the blocks before the slice.
inline void rebase_rows(const Kind& kind, uint8_t* rows, size_t bytes, uint64_t blocks_before) {
    for (size_t at = 0; at < bytes; at += kind.row_bytes(GROUP_WAYS)) {
        const uint64_t off = blocks_before + get32(rows + at);
        if (off > MAX_NODES_FILE_BYTES - NODES_FOOTER_BYTES) malformed(std::string(kind.nodes_file) + " would be 4 GiB or more");
        put32(rows + at, uint32_t(off));
    }
}

// ------------------------------------------------------------------ decoder
// The logical pair of the two files (each whole, in memory; of the size
// ABSENT when the dir lacks it), way by way: a way's record, kind.stride
// bytes, to put_way(const uint8_t*), then its nodes, when it has any, to
// put_nodes(const uint8_t*, size_t bytes). Refuses files the encoder would
// not write.
//
// A decoder that takes the files a unit at a time checks what this one
// checks between the units: each row's off is where the blocks before it
// end, the last block ends where the footer starts, and the ways' nodes are
// the footer's.
template <class PutWay, class PutNodes>
void decode(const Kind& kind, const uint8_t* ways, uint64_t ways_size, const uint8_t* nodes, uint64_t nodes_size, PutWay put_way,
            PutNodes put_nodes) {
    // A file's footer, or all of a shorter file; nothing of one that is not there.
    auto end_of = [](const uint8_t* file, uint64_t size, size_t footer) {
        return size == ABSENT ? nullptr : file + (size - std::min<uint64_t>(size, footer));
    };
    const Footers f = read_footers(kind, end_of(ways, ways_size, WAYS_FOOTER_BYTES), ways_size, end_of(nodes, nodes_size, NODES_FOOTER_BYTES),
                                   nodes_size);
    std::vector<uint8_t> out;
    uint64_t at = 0, node = 0;
    const uint8_t* row = ways;
    for (uint64_t g = 0; g < f.groups(); g++) {
        const size_t n = f.group_ways(g);
        const Row r = decode_row(kind, row, n);
        row += kind.row_bytes(n);
        if (r.off != at) malformed(std::string(kind.ways_file) + " group " + std::to_string(g) + " is not where the blocks before it end");
        for (size_t j = 0; j < n; j++) {
            uint8_t record[MAX_STRIDE];
            logical_record(kind, r, j, node, record);
            put_way(static_cast<const uint8_t*>(record));
            if (!r.cnt[j]) continue;
            const size_t bytes = r.block_bytes(j);
            if (bytes > f.blocks_bytes - at)
                malformed(std::string(kind.nodes_file) + " ends inside the block of way " + std::to_string(g * GROUP_WAYS + j));
            out.resize(NODE_BYTES * size_t(r.cnt[j]));
            decode_block(nodes + at, r.cnt[j], r.code[j], r.anchor_lat, r.anchor_lng, out.data());
            put_nodes(static_cast<const uint8_t*>(out.data()), out.size());
            at += bytes;
            node += r.cnt[j];
        }
    }
    if (at != f.blocks_bytes) malformed(std::string(kind.nodes_file) + " has bytes between its last block and its footer");
    if (node != f.nodes)
        malformed(std::string(kind.nodes_file) + " counts " + std::to_string(f.nodes) + " nodes and its ways have " + std::to_string(node));
}

}  // namespace way_files
