// geocoder-diff v3: Fully custom patch format using merge-sequence encoding.
//
// For each data file: walk old (string-remapped) and new in parallel,
// match records by content, emit MATCH/INSERT/DELETE operations.
// For entry/cell files: include changed cell entry data directly.
// geo_cells rebuilt by patch tool from entries (not delta-patched).
//
// Patch format: custom binary, zstd-compressed as a whole for transport.
//
// Usage: geocoder-diff <old-dir> <new-dir> -o <patch-file>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <dirent.h>
#include <fstream>
#include <functional>
#include <future>
#include <iomanip>
#include <iostream>
#include <malloc.h>
#include <mutex>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <deque>

#include "merge_sequence.h"
#include "patch_format.h"

// Sentinel meaning "no offset / no data / unmapped id" in cell offset fields
// and id-remap tables. Emitted/compared as a raw uint32_t.
static constexpr uint32_t NO_DATA = 0xFFFFFFFFu;

static size_t get_rss_mb() {
    FILE* f = fopen("/proc/self/statm", "r");
    if (!f) return 0;
    size_t dummy, rss; if (fscanf(f, "%zu %zu", &dummy, &rss) != 2) rss = 0; fclose(f);
    return rss * 4096 / (1024*1024);
}

// --- String remap ---

static std::unordered_map<uint32_t, uint32_t> build_string_remap(
    const char* old_pool, size_t old_size, const char* new_pool, size_t new_size)
{
    std::unordered_map<std::string, uint32_t> new_idx;
    size_t pos = 0;
    while (pos < new_size) {
        const char* s = new_pool + pos;
        size_t len = strlen(s);
        new_idx[std::string(s, len)] = static_cast<uint32_t>(pos);
        pos += len + 1;
    }
    std::unordered_map<uint32_t, uint32_t> remap;
    pos = 0;
    while (pos < old_size) {
        const char* s = old_pool + pos;
        size_t len = strlen(s);
        auto it = new_idx.find(std::string(s, len));
        if (it != new_idx.end())
            remap[static_cast<uint32_t>(pos)] = it->second;
        pos += len + 1;
    }
    return remap;
}

// --- Remap + fixup helpers ---

static void remap_addr_points(char* data, size_t size, const std::unordered_map<uint32_t,uint32_t>& rm) {
    // AddrPoint: 28 bytes (lat:4 + lng:4 + housenumber_id:4 + street_id:4 +
    //                      parent_way_id:4 + vertex_offset:4 + vertex_count:4)
    // String fields at offsets 8, 12 only; parent_way_id/vertex fields are not strings.
    constexpr size_t stride = 28;
    for (size_t i = 0; i + stride <= size; i += stride) {
        for (size_t off : {8, 12}) {
            uint32_t v; memcpy(&v, data + i + off, 4);
            auto it = rm.find(v); if (it != rm.end()) memcpy(data + i + off, &it->second, 4);
        }
    }
}
static void remap_field(char* data, size_t size, size_t stride, size_t field_off,
                         const std::unordered_map<uint32_t,uint32_t>& rm) {
    for (size_t i = 0; i + stride <= size; i += stride) {
        uint32_t v; memcpy(&v, data + i + field_off, 4);
        auto it = rm.find(v); if (it != rm.end()) memcpy(data + i + field_off, &it->second, 4);
    }
}

// Content matching for ways (by name + nodes, ignoring node_offset)
static uint64_t fnv_mix(uint64_t h, uint64_t v) { h ^= v; h *= 1099511628211ULL; return h; }

static void fixup_way_offsets(char* old_ways, size_t old_ways_size,
                                const char* old_nodes, size_t old_nodes_size,
                                const char* new_ways, size_t new_ways_size,
                                const char* new_nodes, size_t new_nodes_size,
                                size_t stride) {
    // If the way records AND node coords are byte-identical between builds,
    // the fixup is a no-op at best and a corruptor at worst: a way_hash
    // collision can match an unchanged record to a different same-hash record
    // and rewrite its node_offset to the wrong value, perturbing OLD so
    // build_merge_seq classifies unchanged records as DELETE+INSERT and the
    // derived id_remap goes non-identity — emitting thousands of spurious
    // entry corrections (street_entries) for data that did not change. Skip.
    if (old_ways_size == new_ways_size && old_nodes_size == new_nodes_size &&
        old_ways_size > 0 &&
        memcmp(old_ways, new_ways, old_ways_size) == 0 &&
        memcmp(old_nodes, new_nodes, old_nodes_size) == 0)
        return;
    size_t name_off = (stride == 12) ? 8 : 5;
    size_t old_n = old_ways_size / stride, new_n = new_ways_size / stride;
    size_t old_nc = old_nodes_size / 8, new_nc = new_nodes_size / 8;

    auto way_hash = [&](const char* w, const char* nodes, size_t max_n) -> uint64_t {
        uint32_t node_offset, name_id;
        memcpy(&node_offset, w, 4); memcpy(&name_id, w + name_off, 4);
        uint32_t node_count = record_node_count(w, stride, WAY_HEADER_STRIDE_PACKED);
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, name_id); h = fnv_mix(h, node_count);
        for (uint32_t j = 0; j < node_count && (node_offset + j) < max_n; j++) {
            float lat, lng;
            size_t off = node_byte_offset(node_offset + j);
            memcpy(&lat, nodes + off, 4);
            memcpy(&lng, nodes + off + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    // FIFO per hash: duplicate ways (identical name + geometry, e.g.
    // stacked OSM ways) must pair in slot order — see fixup_v15_offsets.
    std::unordered_map<uint64_t, std::deque<uint32_t>> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map[way_hash(new_ways + i * stride, new_nodes, new_nc)].push_back(i);

    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = way_hash(old_ways + i * stride, old_nodes, old_nc);
        auto it = new_map.find(h);
        if (it != new_map.end() && !it->second.empty()) {
            uint32_t new_node_off;
            memcpy(&new_node_off, new_ways + it->second.front() * stride, 4);
            memcpy(old_ways + i * stride, &new_node_off, 4);
            it->second.pop_front();
        }
    }
}

// v15 admin/POI vertex_offset fixup. Identifies polygons across builds
// by (name_id, level, cc, vert_count, hash-of-vertex-bytes) and rewrites
// the old polygon's vert_offset to match the new build's byte offset.
// After this pass, unchanged polygons have byte-identical records, so
// build_merge_seq finds them as MATCH — which is what build_vertex_byte_merge
// needs to emit short MATCH ops on the byte stream rather than full INSERT.
//
// Vertex byte block for polygon i runs from `vert_offset` until the
// next polygon's `vert_offset` (or end of file for the last polygon).
// Unlike v14, we can't decode vertices without parsing the inline
// header — so we just hash the raw byte block (head bytes are enough
// since unchanged polygons produce byte-identical block content).
static void fixup_v15_offsets(char* old_polys, size_t old_polys_size,
                              const char* old_verts, size_t old_verts_size,
                              const char* new_polys, size_t new_polys_size,
                              const char* new_verts, size_t new_verts_size,
                              size_t stride, size_t off_field_pos,
                              uint64_t (*key_fn)(const char* rec)) {
    // Skip when polygon records AND vertex bytes are byte-identical (see
    // fixup_way_offsets): the content-hash match can otherwise rewrite an
    // unchanged polygon's vert_offset to a colliding polygon's offset,
    // perturbing OLD and producing spurious non-identity id_remaps / admin_
    // and addr_entries corrections on same-build diffs.
    if (old_polys_size == new_polys_size && old_verts_size == new_verts_size &&
        old_polys_size > 0 &&
        memcmp(old_polys, new_polys, old_polys_size) == 0 &&
        memcmp(old_verts, new_verts, old_verts_size) == 0)
        return;
    size_t old_n = old_polys_size / stride;
    size_t new_n = new_polys_size / stride;
    auto compute_blocks = [&](const char* polys, size_t parent_size, size_t verts_size) {
        size_t total_n = parent_size / stride;
        std::vector<uint32_t> offsets(total_n);
        std::vector<uint32_t> sizes(total_n);
        for (size_t i = 0; i < total_n; i++)
            memcpy(&offsets[i], polys + i * stride + off_field_pos, 4);
        uint32_t next_off = static_cast<uint32_t>(verts_size);
        for (size_t i = total_n; i-- > 0; ) {
            uint32_t off = offsets[i];
            if (off == NO_DATA || (size_t)off > verts_size) {
                sizes[i] = 0;
            } else {
                sizes[i] = (next_off >= off) ? (next_off - off) : 0;
                next_off = off;
            }
        }
        return std::make_pair(std::move(offsets), std::move(sizes));
    };
    auto old_blocks = compute_blocks(old_polys, old_polys_size, old_verts_size);
    auto new_blocks = compute_blocks(new_polys, new_polys_size, new_verts_size);
    auto block_off_size = [&](bool is_new, size_t i) -> std::pair<uint32_t, size_t> {
        const auto& v = is_new ? new_blocks : old_blocks;
        if (i >= v.first.size() || v.second[i] == 0) return {v.first.empty() ? 0u : v.first[i], 0};
        return {v.first[i], v.second[i]};
    };
    auto block_hash = [&](const char* verts, size_t verts_size, uint32_t off, size_t bsize) -> uint64_t {
        uint64_t h = 14695981039346656037ULL;
        size_t end = std::min((size_t)off + bsize, verts_size);
        size_t hash_len = std::min(bsize, (size_t)64); // first 64 bytes are enough — header (10) + ~13 vertices
        for (size_t j = 0; j < hash_len && (size_t)off + j < end; j++) {
            h ^= (uint8_t)verts[off + j];
            h *= 1099511628211ULL;
        }
        h = fnv_mix(h, (uint64_t)bsize); // distinguish blocks of same head bytes but different lengths
        return h;
    };

    // Build new index: (record_key, block_hash) → new record indices in
    // slot order. Content-identical duplicates (e.g. one building polygon
    // copied for every unit's addr_point) MUST pair positionally: an
    // unordered_multimap's find() returns an arbitrary duplicate, which
    // rewrote old voffs to a DIFFERENT copy's position — the record then
    // mismatched its true counterpart and whole duplicate groups were
    // re-serialized (1.27 GB of unchanged addr records in an 11-day
    // planet diff). Records keep stable slot order across chained
    // builds, so k-th old ↔ k-th new within a group is the true pairing.
    std::unordered_map<uint64_t, std::deque<uint32_t>> new_map;
    new_map.reserve(new_n);
    auto compose_key = [&](uint64_t rec_key, uint64_t bhash) -> uint64_t {
        // FNV-mix the two together to make a single hashtable key.
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, rec_key);
        h = fnv_mix(h, bhash);
        return h;
    };
    for (uint32_t i = 0; i < new_n; i++) {
        const char* rec = new_polys + i * stride;
        auto bos = block_off_size(true, i);
        uint64_t rec_key = key_fn(rec);
        uint64_t bhash = block_hash(new_verts, new_verts_size, bos.first, bos.second);
        new_map[compose_key(rec_key, bhash)].push_back(i);
    }

    for (uint32_t i = 0; i < old_n; i++) {
        const char* rec = old_polys + i * stride;
        auto bos = block_off_size(false, i);
        uint64_t rec_key = key_fn(rec);
        uint64_t bhash = block_hash(old_verts, old_verts_size, bos.first, bos.second);
        auto it = new_map.find(compose_key(rec_key, bhash));
        if (it != new_map.end() && !it->second.empty()) {
            uint32_t new_off;
            memcpy(&new_off, new_polys + it->second.front() * stride + off_field_pos, 4);
            memcpy(old_polys + i * stride + off_field_pos, &new_off, 4);
            it->second.pop_front();
        }
    }
}

// Same for admin polygons (vertex_offset)
static void fixup_admin_offsets(char* old_polys, size_t old_polys_size,
                                  const char* old_verts, size_t old_verts_size,
                                  const char* new_polys, size_t new_polys_size,
                                  const char* new_verts, size_t new_verts_size,
                                  size_t stride) {
    size_t old_n = old_polys_size / stride, new_n = new_polys_size / stride;
    size_t old_vc = old_verts_size / 8, new_vc = new_verts_size / 8;

    auto poly_hash = [&](const char* p, const char* verts, size_t max_v) -> uint64_t {
        uint32_t vert_offset, vert_count, name_id;
        memcpy(&vert_offset, p, 4); memcpy(&vert_count, p + 4, 4); memcpy(&name_id, p + 8, 4);
        uint8_t level = (uint8_t)p[12]; uint16_t cc; memcpy(&cc, p + 20, 2);
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, name_id); h = fnv_mix(h, level); h = fnv_mix(h, cc); h = fnv_mix(h, vert_count);
        for (uint32_t j = 0; j < std::min(vert_count, 10u) && (vert_offset + j) < max_v; j++) {
            float lat, lng;
            size_t off = node_byte_offset(vert_offset + j);
            memcpy(&lat, verts + off, 4); memcpy(&lng, verts + off + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    // FIFO per hash: content-identical duplicates pair in slot order
    // (see fixup_v15_offsets).
    std::unordered_map<uint64_t, std::deque<uint32_t>> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map[poly_hash(new_polys + i * stride, new_verts, new_vc)].push_back(i);

    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = poly_hash(old_polys + i * stride, old_verts, old_vc);
        auto it = new_map.find(h);
        if (it != new_map.end() && !it->second.empty()) {
            uint32_t new_vert_off;
            memcpy(&new_vert_off, new_polys + it->second.front() * stride, 4);
            memcpy(old_polys + i * stride, &new_vert_off, 4);
            it->second.pop_front();
        }
    }
}

// Same for POI records (vertex_offset at byte 8, vertex_count at byte 12)
// POI record layout: lat(f32:0), lng(f32:4), vertex_offset(u32:8), vertex_count(u32:12),
//                    name_id(u32:16), category(u8:20), tier(u8:21), flags(u8:22), pad(u8:23)
static void fixup_poi_offsets(char* old_pois, size_t old_pois_size,
                                const char* old_verts, size_t old_verts_size,
                                const char* new_pois, size_t new_pois_size,
                                const char* new_verts, size_t new_verts_size,
                                size_t stride) {
    size_t old_n = old_pois_size / stride, new_n = new_pois_size / stride;
    size_t old_vc = old_verts_size / 8, new_vc = new_verts_size / 8;

    auto poi_hash = [&](const char* p, const char* verts, size_t max_v) -> uint64_t {
        uint32_t vert_offset, vert_count, name_id;
        memcpy(&vert_offset, p + 8, 4); memcpy(&vert_count, p + 12, 4); memcpy(&name_id, p + 16, 4);
        uint8_t category = (uint8_t)p[20];
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, name_id); h = fnv_mix(h, category); h = fnv_mix(h, vert_count);
        if (vert_count == 0) {
            // Point POI — use lat/lng
            float lat, lng;
            memcpy(&lat, p, 4); memcpy(&lng, p + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        } else {
            // Polygon POI — use first vertices
            for (uint32_t j = 0; j < std::min(vert_count, 10u) && (vert_offset + j) < max_v; j++) {
                float lat, lng;
                size_t off = node_byte_offset(vert_offset + j);
                memcpy(&lat, verts + off, 4); memcpy(&lng, verts + off + 4, 4);
                h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
            }
        }
        return h;
    };

    // FIFO per hash: content-identical duplicates pair in slot order
    // (see fixup_v15_offsets).
    std::unordered_map<uint64_t, std::deque<uint32_t>> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map[poi_hash(new_pois + i * stride, new_verts, new_vc)].push_back(i);

    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = poi_hash(old_pois + i * stride, old_verts, old_vc);
        auto it = new_map.find(h);
        if (it != new_map.end() && !it->second.empty()) {
            uint32_t new_vert_off;
            memcpy(&new_vert_off, new_pois + it->second.front() * stride + 8, 4);
            memcpy(old_pois + i * stride + 8, &new_vert_off, 4);
            it->second.pop_front();
        }
    }
}

// Same for interp ways
static void fixup_interp_offsets(char* old_data, size_t old_size,
                                   const char* old_nodes, size_t old_nodes_size,
                                   const char* new_data, size_t new_size,
                                   const char* new_nodes, size_t new_nodes_size,
                                   size_t stride) {
    size_t street_off = (stride >= 20) ? 8 : 5;
    size_t old_n = old_size / stride, new_n = new_size / stride;
    size_t old_nc = old_nodes_size / 8, new_nc = new_nodes_size / 8;

    auto ihash = [&](const char* p, const char* nodes, size_t max_n) -> uint64_t {
        uint32_t node_offset, street_id, start, end; uint8_t itype;
        memcpy(&node_offset, p, 4);
        uint32_t count = record_node_count(p, stride, INTERP_WAY_STRIDE_PACKED);
        memcpy(&street_id, p + street_off, 4); memcpy(&start, p + street_off + 4, 4);
        memcpy(&end, p + street_off + 8, 4);
        itype = (street_off + 12 < stride) ? (uint8_t)p[street_off + 12] : 0;
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, street_id); h = fnv_mix(h, start); h = fnv_mix(h, end);
        h = fnv_mix(h, itype); h = fnv_mix(h, count);
        for (uint32_t j = 0; j < count && (node_offset + j) < max_n; j++) {
            float lat, lng;
            size_t off = node_byte_offset(node_offset + j);
            memcpy(&lat, nodes + off, 4); memcpy(&lng, nodes + off + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    // FIFO per hash: content-identical duplicates pair in slot order
    // (see fixup_v15_offsets).
    std::unordered_map<uint64_t, std::deque<uint32_t>> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map[ihash(new_data + i * stride, new_nodes, new_nc)].push_back(i);
    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = ihash(old_data + i * stride, old_nodes, old_nc);
        auto it = new_map.find(h);
        if (it != new_map.end() && !it->second.empty()) {
            uint32_t new_off; memcpy(&new_off, new_data + it->second.front() * stride, 4);
            memcpy(old_data + i * stride, &new_off, 4);
            it->second.pop_front();
        }
    }
}

// The offsets a fixup pass above rewrote at `field` of each old record;
// old_offsets holds the field from before the pass.
static OffsetFixups collect_offset_fixups(
        const std::vector<uint32_t>& old_offsets, const char* fixed, size_t stride, size_t field) {
    return encode_offset_fixups(old_offsets, [&](uint32_t i) {
        uint32_t off; memcpy(&off, fixed + (size_t)i * stride + field, 4);
        return off;
    });
}

// --- Secondary matching for modified records ---
// After the merge sequence is built, match DELETE'd and INSERT'd records by a
// relaxed key to recover ID mappings for records that changed (e.g. geometry edit)
// but represent the same logical entity. Only matches when the key group has
// equal counts on both sides to avoid mismatches.
template<typename KeyFn>
static std::unordered_map<uint32_t, uint32_t> secondary_match_from_merge(
    const MergeSequence& seq,
    const char* old_data, size_t old_size,
    const char* new_data, size_t new_size,
    size_t stride,
    KeyFn key_fn)
{
    std::vector<uint32_t> del_indices, ins_indices;
    size_t pos = 0, old_rec = 0, new_rec = 0;
    while (pos < seq.data.size()) {
        uint8_t op = static_cast<uint8_t>(seq.data[pos]); pos++;
        uint32_t count; memcpy(&count, seq.data.data() + pos, 4); pos += 4;
        if (op == OP_MATCH_RUN) {
            old_rec += count; new_rec += count;
        } else if (op == OP_INSERT_RUN) {
            for (uint32_t k = 0; k < count; k++)
                ins_indices.push_back(static_cast<uint32_t>(new_rec + k));
            pos += count * stride;
            new_rec += count;
        } else if (op == OP_DELETE_RUN) {
            for (uint32_t k = 0; k < count; k++)
                del_indices.push_back(static_cast<uint32_t>(old_rec + k));
            old_rec += count;
        }
    }

    std::unordered_map<uint64_t, std::vector<uint32_t>> del_by_key, ins_by_key;
    for (uint32_t di : del_indices) {
        if ((size_t)(di + 1) * stride > old_size) continue;
        del_by_key[key_fn(old_data + di * stride)].push_back(di);
    }
    for (uint32_t ii : ins_indices) {
        if ((size_t)(ii + 1) * stride > new_size) continue;
        ins_by_key[key_fn(new_data + ii * stride)].push_back(ii);
    }

    std::unordered_map<uint32_t, uint32_t> remap;
    for (auto& [key, del_vec] : del_by_key) {
        auto it = ins_by_key.find(key);
        if (it == ins_by_key.end()) continue;
        auto& ins_vec = it->second;
        if (del_vec.size() == ins_vec.size()) {
            std::sort(del_vec.begin(), del_vec.end());
            std::sort(ins_vec.begin(), ins_vec.end());
            for (size_t i = 0; i < del_vec.size(); i++)
                remap[del_vec[i]] = ins_vec[i];
        }
    }
    return remap;
}

// --- Derive ID remap from merge sequence (same logic as patch tool) ---
static std::vector<uint32_t> derive_id_remap_from_merge(
    const MergeSequence& seq, size_t old_count, size_t stride)
{
    std::vector<uint32_t> id_map(old_count, NO_DATA);
    size_t pos = 0, old_rec = 0, new_rec = 0;
    while (pos < seq.data.size()) {
        uint8_t op = static_cast<uint8_t>(seq.data[pos]); pos++;
        uint32_t count; memcpy(&count, seq.data.data() + pos, 4); pos += 4;
        if (op == OP_MATCH_RUN) {
            for (uint32_t k = 0; k < count; k++)
                if (old_rec + k < id_map.size())
                    id_map[old_rec + k] = static_cast<uint32_t>(new_rec + k);
            old_rec += count; new_rec += count;
        } else if (op == OP_INSERT_RUN) {
            pos += count * stride;
            new_rec += count;
        } else if (op == OP_DELETE_RUN) {
            old_rec += count;
        }
    }
    return id_map;
}

// --- Per-file merge result (computed in parallel, serialized sequentially) ---
struct FileMergeResult {
    PatchFileId id;
    std::string name;
    size_t stride;
    uint64_t old_size, new_size;
    MergeSequence seq;
    OffsetFixups fixups;
    std::unordered_map<uint32_t,uint32_t> secondary_matches; // soft old→new ID map
    std::vector<uint32_t> id_remap; // derived old→new ID remap (with secondary merged in)
};

// Emit a COPY_OLD_STRIDE section (standard 6-field header, no data) iff the new
// file at new_path is byte-identical to old_path. Returns true if emitted, so
// callers do `if (try_emit_copy_old(...)) return;`. Single source of truth for
// the copy-old wire format shared by emit_raw / emit_sparse_delta /
// serialize_merge. Never short-circuits a missing or zero-size file (mmap
// returns null → same=false), so the patcher's copy from cur_dir always has a
// real source. memcmp via mmap is cheap relative to the bytes saved.
//
// Safe even for data files that feed entry corrections: byte-identity of the
// source forces an identity id_remap (the fixup passes are skipped for identical
// files), so the patcher reconstructs dependents with the same identity remap.
static bool try_emit_copy_old(std::vector<char>& patch, uint32_t fid,
                              const std::string& old_path, const std::string& new_path,
                              uint64_t old_size, uint64_t new_size, const char* log_name) {
    if (old_size != new_size || new_size == 0) return false;
    auto om = mmap_file(old_path);
    auto nm = mmap_file(new_path);
    bool same = om.data && nm.data &&
                memcmp(om.data, nm.data, (size_t)new_size) == 0;
    if (om.data) unmap_file(om);
    if (nm.data) unmap_file(nm);
    if (!same) return false;
    uint32_t stride = COPY_OLD_STRIDE, nfix = 0;
    uint64_t ds = 0;
    auto w = [&](const void* d, size_t s) {
        patch.insert(patch.end(), (const char*)d, (const char*)d + s);
    };
    w(&fid, 4); w(&stride, 4); w(&old_size, 8); w(&new_size, 8); w(&nfix, 4); w(&ds, 8);
    std::cerr << "  " << log_name << ": unchanged (copy old, "
              << new_size << " bytes saved)" << std::endl;
    return true;
}

// Serialize a pre-computed merge result into the patch buffer
static void serialize_merge(std::vector<char>& patch, const FileMergeResult& r,
                            const std::string& old_dir = "",
                            const std::string& new_dir = "") {
    auto wv = [&](const void* data, size_t size) {
        patch.insert(patch.end(), (const char*)data, (const char*)data + size);
    };
    // Unchanged short-circuit: emit a copy-old marker instead of the merge
    // sequence when the file is byte-identical to old. The merge runs after
    // upstream fixups (string-pool remap, vertex_offset rewrites) that can
    // perturb the byte-merge into emitting ops for a file that is in fact
    // identical (addr_vertices / street_nodes were byte-equal yet encoded
    // ~0.1 MiB each on a same-PBF planet diff). Skipped automatically when
    // either file isn't present at <dir>/<name> (mmap returns null); admin_*
    // live at a fallback path during apply but the diff still finds them under
    // old_dir/new_dir here, so they DO participate.
    if (!old_dir.empty() &&
        try_emit_copy_old(patch, static_cast<uint32_t>(r.id),
                          old_dir + "/" + r.name, new_dir + "/" + r.name,
                          r.old_size, r.new_size, r.name.c_str()))
        return;
    uint32_t fid = static_cast<uint32_t>(r.id);
    uint32_t st = static_cast<uint32_t>(r.stride);
    const OffsetFixups& f = r.fixups;
    uint32_t runs_size = static_cast<uint32_t>(f.runs.size());
    uint32_t values_size = static_cast<uint32_t>(f.values.size());
    wv(&fid, 4); wv(&st, 4); wv(&r.old_size, 8); wv(&r.new_size, 8);
    wv(&f.n_runs, 4); wv(&f.n_values, 4); wv(&runs_size, 4); wv(&values_size, 4);
    patch.insert(patch.end(), f.runs.begin(), f.runs.end());
    patch.insert(patch.end(), f.values.begin(), f.values.end());
    uint64_t ss = r.seq.data.size();
    wv(&ss, 8);
    patch.insert(patch.end(), r.seq.data.begin(), r.seq.data.end());
}

static std::mutex log_mutex;
static void log_merge(const FileMergeResult& r) {
    std::lock_guard<std::mutex> lock(log_mutex);
    std::cerr << "  " << r.name << ": seq=" << r.seq.data.size()
              << " fixup_runs=" << r.fixups.n_runs << " fixup_values=" << r.fixups.n_values
              << " (" << std::fixed << std::setprecision(2)
              << (r.new_size > 0 ? r.seq.data.size() * 100.0 / r.new_size : 0) << "%)" << std::endl;
}

// --- Write helpers ---
static void wval(std::vector<char>& buf, const void* data, size_t size) {
    buf.insert(buf.end(), (const char*)data, (const char*)data + size);
}

// The client files of a variant dir (see CLIENT_FILES_MARKER), sorted by name.
// Fails on a file no patch section can reproduce, so a new builder output can
// never be silently dropped from patched installs.
static std::vector<ClientFile> list_client_files(const std::string& dir) {
    std::unordered_set<std::string> section_files(std::begin(patch_file_names), std::end(patch_file_names));
    section_files.erase("strings.bin");  // legacy single pool, never emitted
    std::vector<ClientFile> files;
    DIR* d = opendir(dir.c_str());
    if (!d) throw std::runtime_error("Cannot read " + dir);
    while (struct dirent* e = readdir(d)) {
        std::string name = e->d_name;
        if (!is_client_file(name)) continue;
        std::string path = dir + "/" + name;
        struct stat st;
        if (stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) continue;
        ClientFile f{name, static_cast<uint64_t>(st.st_size), {}};
        if (!is_inline_client_file(name)) {
            if (!section_files.count(name)) { closedir(d); throw std::runtime_error("No patch section for " + path); }
        } else if (!has_suffix(name, ".json") || f.size > MAX_INLINE_CLIENT_FILE_BYTES) {
            closedir(d);
            throw std::runtime_error("Cannot carry " + path + " in a patch");
        } else {
            f.bytes = read_file(path);
            if (f.bytes.size() != f.size) { closedir(d); throw std::runtime_error("Short read: " + path); }
        }
        files.push_back(std::move(f));
    }
    closedir(d);
    std::sort(files.begin(), files.end(), [](const ClientFile& a, const ClientFile& b) { return a.name < b.name; });
    return files;
}

static int run(int argc, char* argv[]) {
    if (argc < 5 || std::string(argv[3]) != "-o") {
        std::cerr << "Usage: geocoder-diff <old-dir> <new-dir> -o <patch-file>" << std::endl;
        return 1;
    }
    std::string old_dir = argv[1], new_dir = argv[2], patch_path = argv[4];

    // Build string remap (mmap each tier's pool — read-only sequential
    // scan). Virtual "concat pool" reproduces the global-offset layout
    // that record name_ids point into, so build_string_remap works
    // unchanged. Tier bases are the cumulative sizes of the tiers found;
    // a tier found nowhere is empty (strings_poi.bin beside full/).
    std::cerr << "Building string remap... (RSS=" << get_rss_mb() << " MiB)" << std::endl;
    static const char* kStrTierFilenames[5] = {
        "strings_core.bin", "strings_street.bin", "strings_addr.bin",
        "strings_postcode.bin", "strings_poi.bin"
    };
    std::array<MappedFile, 5> old_tier_maps{}, new_tier_maps{};
    std::vector<char> old_concat, new_concat;
    std::array<uint32_t, 6> old_tier_bases{}, new_tier_bases{};
    // String tiers may be absent from the input dir when geocoder-diff
    // is invoked on a per-variant subdir (e.g. <region>/quality/<q>/
    // or <region>/poi/<tier>/) — strings_*.bin live under the sibling
    // <region>/full/ directory. Fall back upward through one or two
    // path levels to find them. Without this fallback the str_remap
    // ends up empty and records that should match (after name_id
    // re-tier) get classified as INSERT/DELETE → byte-block walker
    // produces patches the size of the whole vertex stream.
    auto try_load_tier = [&](const std::string& dir, const char* fname) -> MappedFile {
        MappedFile m = mmap_file(dir + "/" + fname);
        if (m.size > 0 || m.data != nullptr) return m;
        m = mmap_file(dir + "/../full/" + fname);
        if (m.size > 0 || m.data != nullptr) return m;
        return mmap_file(dir + "/../../full/" + fname);
    };
    // Generic fallback loader for shared-across-variants files. Used so
    // t_admin/t_street can compute the parent-id remap that t_poi needs
    // even when geocoder-diff is invoked on a per-variant subdir
    // (e.g. <region>/poi/<tier>/) where admin_polygons.bin lives in
    // <region>/quality/q2.5/ and street_ways.bin lives in <region>/full/.
    // Returns the load path on success (so caller can detect fallback
    // and suppress emitting a merge section that would target the
    // wrong output directory).
    auto try_load_with_fallback = [&](const std::string& dir, const char* fname,
                                       std::initializer_list<const char*> fallbacks)
            -> std::pair<MappedFile, bool> {
        MappedFile m = mmap_file(dir + "/" + fname);
        if (m.size > 0) return {m, false};
        for (const char* fb : fallbacks) {
            if (m.data) unmap_file(m);
            m = mmap_file(dir + "/" + fb + fname);
            if (m.size > 0) return {m, true};
        }
        return {m, false};
    };
    auto try_load_with_fallback_rw = [&](const std::string& dir, const char* fname,
                                          std::initializer_list<const char*> fallbacks)
            -> std::pair<MappedFileRW, bool> {
        MappedFileRW m = mmap_file_rw(dir + "/" + fname);
        if (m.size > 0) return {m, false};
        for (const char* fb : fallbacks) {
            if (m.data) unmap_file(m);
            m = mmap_file_rw(dir + "/" + fb + fname);
            if (m.size > 0) return {m, true};
        }
        return {m, false};
    };
    for (int t = 0; t < 5; t++) {
        old_tier_maps[t] = try_load_tier(old_dir, kStrTierFilenames[t]);
        new_tier_maps[t] = try_load_tier(new_dir, kStrTierFilenames[t]);
        old_tier_bases[t] = static_cast<uint32_t>(old_concat.size());
        new_tier_bases[t] = static_cast<uint32_t>(new_concat.size());
        if (old_tier_maps[t].size > 0)
            old_concat.insert(old_concat.end(), old_tier_maps[t].data,
                              old_tier_maps[t].data + old_tier_maps[t].size);
        if (new_tier_maps[t].size > 0)
            new_concat.insert(new_concat.end(), new_tier_maps[t].data,
                              new_tier_maps[t].data + new_tier_maps[t].size);
    }
    old_tier_bases[5] = static_cast<uint32_t>(old_concat.size());
    new_tier_bases[5] = static_cast<uint32_t>(new_concat.size());
    auto str_remap = build_string_remap(old_concat.data(), old_concat.size(),
                                         new_concat.data(), new_concat.size());

    // Detect strides (stat only, no data loaded)
    auto detect = [](const std::string& path, std::initializer_list<size_t> cs) -> size_t {
        return detect_stride_from_file(path, cs);
    };
    size_t way_stride = detect(old_dir + "/street_ways.bin", {12, 9});
    size_t interp_stride = detect(old_dir + "/interp_ways.bin", {24, 20, 18});
    size_t admin_stride = detect(old_dir + "/admin_polygons.bin", {24, 20, 19});
    // PoiRecord: 24B (build_version<=3), 28B (build_version 4-9,
    // added parent_street_id), 32B (build_version==10, added
    // parent_postcode_id), 36B (build_version>=11, added
    // parent_poly_id). Detect from file stride so old caches still
    // work for diff generation.
    size_t poi_stride = detect(old_dir + "/poi_records.bin", {36, 32, 28, 24});
    // PlaceNode: 16B (build_version<=3) or 20B (build_version>=4,
    // added parent_poly_id).
    size_t place_stride = detect(old_dir + "/place_nodes.bin", {20, 16});

    // Build patch data (uncompressed, will be zstd-compressed at the end)
    std::vector<char> patch;
    patch.insert(patch.end(), GCPATCH_MAGIC, GCPATCH_MAGIC + 8);
    uint32_t ver = GCPATCH_VERSION, flags = 0; // custom merge-sequence format
    wval(patch, &ver, 4); wval(patch, &flags, 4);
    {
        auto client_files = list_client_files(new_dir);
        append_client_files(patch, client_files);
        std::cerr << "  Client files: " << client_files.size() << std::endl;
    }

    // --- Section: Per-file merge sequences (computed in parallel) ---
    // Stored merge sequences for ID remap derivation

    // Per-tier string-level diffs. Each tier is independently sorted
    // alphabetically, so we diff tier-by-tier. Wire format: one header
    // marker 0xFFFFFFF6 followed by 5 blocks of {n_added, n_deleted,
    // added_strings..., deleted_indices...}. The patch tool applies
    // each block to the matching tier file in cur_dir.
    //
    // MUST come before the explicit-remap marker below — the patcher
    // checks for the tiered marker first; if it sees the explicit
    // marker first, it consumes that section and exits (never reading
    // the tiered marker that follows), leaving strings_*.bin unwritten.
    {
        uint32_t tiered_marker = STRINGS_TIERED_MARKER;
        wval(patch, &tiered_marker, 4);
        for (int t = 0; t < 5; t++) {
            TierStamp s;
            s.old_size = static_cast<uint32_t>(old_tier_maps[t].size);
            s.new_size = static_cast<uint32_t>(new_tier_maps[t].size);
            s.old_hash = content_hash(old_tier_maps[t].data, old_tier_maps[t].size);
            s.new_hash = content_hash(new_tier_maps[t].data, new_tier_maps[t].size);
            wval(patch, &s.old_size, 4); wval(patch, &s.new_size, 4);
            wval(patch, &s.old_hash, 8); wval(patch, &s.new_hash, 8);
        }
        for (int t = 0; t < 5; t++) {
            std::vector<std::string> old_strs, new_strs;
            {
                size_t p = 0;
                while (p < old_tier_maps[t].size) {
                    old_strs.push_back(old_tier_maps[t].data + p);
                    p += strlen(old_tier_maps[t].data + p) + 1;
                }
            }
            {
                size_t p = 0;
                while (p < new_tier_maps[t].size) {
                    new_strs.push_back(new_tier_maps[t].data + p);
                    p += strlen(new_tier_maps[t].data + p) + 1;
                }
            }
            std::vector<std::string> added_strings;
            std::vector<uint32_t> deleted_indices;
            size_t oi = 0, ni = 0;
            while (oi < old_strs.size() && ni < new_strs.size()) {
                int c = old_strs[oi].compare(new_strs[ni]);
                if (c == 0) { oi++; ni++; }
                else if (c < 0) { deleted_indices.push_back(oi); oi++; }
                else { added_strings.push_back(new_strs[ni]); ni++; }
            }
            while (oi < old_strs.size()) { deleted_indices.push_back(oi); oi++; }
            while (ni < new_strs.size()) { added_strings.push_back(new_strs[ni]); ni++; }
            uint32_t n_added = added_strings.size(), n_deleted = deleted_indices.size();
            wval(patch, &n_added, 4); wval(patch, &n_deleted, 4);
            for (auto& s : added_strings) { patch.insert(patch.end(), s.begin(), s.end()); patch.push_back('\0'); }
            for (auto idx : deleted_indices) wval(patch, &idx, 4);
            std::cerr << "  " << kStrTierFilenames[t] << ": +" << n_added << " -" << n_deleted << " strings" << std::endl;
        }
    }

    // Explicit-remap section follows the tiered diff. Same-tier remaps
    // (where old and new offsets are in the matching tier file) are
    // derived by the patcher walking each tier's old/new pool side by
    // side. CROSS-TIER moves — a string that lived in addr in the
    // previous build and now lives in street — aren't visible to the
    // per-tier walk, so they have to be transmitted explicitly here.
    // Without this, records pointing at the moved string keep their old
    // (now stale) global offset after patch (verified: 566 oceania
    // addr_points, ~thousands of street_ways across regions when a
    // numeric/short string flips tier between consecutive builds).
    {
        auto tier_of = [](uint32_t global, const std::array<uint32_t, 6>& bases) -> int {
            for (int t = 0; t < 5; t++) if (global >= bases[t] && global < bases[t + 1]) return t;
            return -1;
        };
        std::vector<std::pair<uint32_t, uint32_t>> cross_tier;
        cross_tier.reserve(str_remap.size() / 8);
        for (const auto& [og, ng] : str_remap) {
            int ot = tier_of(og, old_tier_bases);
            int nt = tier_of(ng, new_tier_bases);
            if (ot != nt) cross_tier.push_back({og, ng});
        }
        std::sort(cross_tier.begin(), cross_tier.end());
        uint32_t marker = STRINGS_CROSS_TIER_REMAP_MARKER;
        uint32_t count = static_cast<uint32_t>(cross_tier.size());
        wval(patch, &marker, 4); wval(patch, &count, 4);
        for (auto& [og, ng] : cross_tier) { wval(patch, &og, 4); wval(patch, &ng, 4); }
        std::cerr << "  String remap: " << count << " cross-tier explicit entries" << std::endl;
    }

    // Build merge sequences for all data files in parallel (4 groups)
    // Group 1: addr_points (independent)
    // Group 2: street_ways → street_nodes (sequential within group)
    // Group 3: interp_ways → interp_nodes (sequential within group)
    // Group 4: admin_polygons → admin_vertices (sequential within group)
    FileMergeResult res_addr, res_addr_v, res_ways, res_nodes, res_interp_w, res_interp_n, res_admin_p, res_admin_v, res_poi_r, res_poi_v, res_place_n;

    // Byte-block merge for the v15 variable-stride vertex stream
    // (admin_vertices / poi_vertices / addr_vertices / postal_vertices). The
    // vertex bytes for polygon i live at the polygon's `vertex_offset` and
    // run until the next polygon's `vertex_offset` (or end-of-file for the
    // last). Replaces the previous "emit the whole new file as raw" path
    // that made every quality patch ~size-of-file.
    //
    // Correctness depends on: (a) simplification being deterministic
    // (verified — the existing patch verify is byte-identical), and
    // (b) polygon byte blocks landing at the same byte offset in old
    // and new admin_vertices when the polygon record is a MATCH in
    // the parent sequence (verified by the same byte-identical pass —
    // if blocks shifted, MATCH polygons in admin_polygons.bin would
    // have stale vertex_offset values).
    //
    // off_field_pos: byte offset of vertex_offset on the parent record.
    auto build_vertex_byte_merge = [](const MergeSequence& parent_seq,
                                       const char* old_parent, size_t old_parent_size,
                                       const char* new_parent, size_t new_parent_size,
                                       const char* old_verts, size_t old_verts_size,
                                       const char* new_verts, size_t new_verts_size,
                                       size_t parent_stride,
                                       size_t off_field_pos) -> MergeSequence {
        // Vertex byte block for polygon i runs from vert_offset until
        // the next *non-NO_DATA* polygon's vert_offset (POIs can be
        // points with vert_offset==NO_DATA interspersed between polygon
        // POIs; their blocks have zero size). Precompute block sizes
        // O(n) so the walker's per-record lookup is O(1).
        auto compute_block_sizes = [&](const char* parent, size_t parent_size, size_t verts_size) {
            size_t total_n = parent_size / parent_stride;
            std::vector<uint32_t> offsets(total_n);
            std::vector<uint32_t> sizes(total_n);
            for (size_t i = 0; i < total_n; i++)
                memcpy(&offsets[i], parent + i * parent_stride + off_field_pos, 4);
            // Walk from end so each i finds its next non-NO_DATA neighbour cheaply.
            uint32_t next_off = static_cast<uint32_t>(verts_size);
            for (size_t i = total_n; i-- > 0; ) {
                uint32_t off = offsets[i];
                if (off == NO_DATA || (size_t)off > verts_size) {
                    sizes[i] = 0;
                } else {
                    sizes[i] = (next_off >= off) ? (next_off - off) : 0;
                    next_off = off;
                }
            }
            return std::make_pair(std::move(offsets), std::move(sizes));
        };
        auto old_off_sz = compute_block_sizes(old_parent, old_parent_size, old_verts_size);
        auto new_off_sz = compute_block_sizes(new_parent, new_parent_size, new_verts_size);
        auto block_of = [](const std::pair<std::vector<uint32_t>, std::vector<uint32_t>>& v) {
            return [&v](size_t i) -> ChildBlock {
                if (i >= v.first.size() || v.second[i] == 0) return {0, 0};
                return {v.first[i], v.second[i]};
            };
        };
        return merge_child_blocks(parent_seq, parent_stride, 1, old_verts, old_verts_size,
                                  new_verts, new_verts_size, block_of(old_off_sz), block_of(new_off_sz));
    };

    // Node merge of street_ways / interp_ways from the parent way merge:
    // node_count is read with record_node_count (u16 at byte 4; u8 in the
    // legacy packed strides), off_field_pos is the byte offset of
    // node_offset (0 for both).
    auto build_child_merge = [](const MergeSequence& parent_seq,
                                 const char* old_parent, size_t /*old_parent_size*/,
                                 const char* new_parent, size_t /*new_parent_size*/,
                                 const char* old_child, size_t old_child_size,
                                 const char* new_child, size_t new_child_size,
                                 size_t parent_stride, size_t packed_stride,
                                 size_t off_field_pos = 0) -> MergeSequence {
        auto block_of = [=](const char* parent) {
            return [=](size_t i) -> ChildBlock {
                const char* rec = parent + i * parent_stride;
                uint32_t off; memcpy(&off, rec + off_field_pos, 4);
                // node_byte_offset widens before `* 8`: planet street_nodes
                // passes 2^29 nodes, where 32-bit math wraps.
                return {node_byte_offset(off), (size_t)record_node_count(rec, parent_stride, packed_stride) * 8};
            };
        };
        return merge_child_blocks(parent_seq, parent_stride, 8, old_child, old_child_size,
                                  new_child, new_child_size, block_of(old_parent), block_of(new_parent));
    };

    // --- Timing helper ---
    auto now_ms = []() -> double {
        struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
        return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
    };
    auto log_time = [&](const char* label, double start) {
        std::lock_guard<std::mutex> lock(log_mutex);
        std::cerr << "  [" << std::fixed << std::setprecision(1) << (now_ms() - start) / 1000.0 << "s] " << label << std::endl;
    };

    double merge_start = now_ms();
    std::cerr << "Building merge sequences (parallel)..." << std::endl;

    // t_poi needs the full admin + street id_remaps to rewrite old
    // PoiRecord parent ids before pr_seq is built. t_admin and t_street
    // signal completion via these promises so t_poi can run in parallel
    // with t_addr / t_interp / t_place / t_cells (only blocking on the
    // narrow window where the remaps are consumed).
    std::promise<void> admin_remap_ready, street_remap_ready;
    std::shared_future<void> admin_remap_future = admin_remap_ready.get_future().share();
    std::shared_future<void> street_remap_future = street_remap_ready.get_future().share();

    // Group 1: addr_points (old=COW mmap for string remap, new=read-only mmap)
    std::thread t_addr([&]() {
        double gs = now_ms();
        auto old_m = mmap_file_rw(old_dir + "/addr_points.bin");
        auto new_m = mmap_file(new_dir + "/addr_points.bin");
        // Stride must be detected per-file. Known sizes: 16 (pre-postcode),
        // 20 (+parent_way_id), 28 (+vertex_offset/count). We can only run
        // the merge if old and new have the same stride; cross-stride
        // patches are not supported (the manifest's build_version bump
        // forces clients to fresh-install instead).
        auto detect_addr_stride = [](size_t sz) -> size_t {
            if (sz % 28 == 0 && sz > 0) return 28;
            if (sz % 20 == 0 && sz > 0) return 20;
            return 16;
        };
        size_t old_stride = detect_addr_stride(old_m.size);
        size_t new_stride = detect_addr_stride(new_m.size);
        if (old_stride != new_stride) {
            std::cerr << "  addr_points: stride mismatch (old=" << old_stride
                      << ", new=" << new_stride << ") — cannot patch" << std::endl;
            std::exit(2);
        }
        size_t addr_stride = new_stride;
        remap_addr_points(old_m.data, old_m.size, str_remap);
        // parent_way_id (byte 16-19) is a foreign id into street_ways.bin.
        // It shifts day-over-day whenever the way ordering changes (which
        // is on every build). Without rewriting it to the new id-space,
        // every addr_point's bytes differ between old and new even when
        // the underlying address didn't change — build_merge_seq would
        // classify all 174M planet addr_points as DELETE+INSERT, which is
        // exactly what we observed (4.89 GB merge sequence on a 4.89 GB
        // file). Wait on t_street's id_remap and apply before merge_seq.
        if (addr_stride >= 20 && old_m.size > 0) {
            street_remap_future.wait();
            const auto& way_rm = res_ways.id_remap;
            if (!way_rm.empty()) {
                size_t n = old_m.size / addr_stride;
                for (size_t i = 0; i < n; i++) {
                    char* rec = old_m.data + i * addr_stride;
                    uint32_t pw; memcpy(&pw, rec + 16, 4);
                    if (pw != NO_DATA && pw < way_rm.size() && way_rm[pw] != NO_DATA && way_rm[pw] != pw)
                        memcpy(rec + 16, &way_rm[pw], 4);
                }
            }
        }
        // Identify polygon-bearing addr_points across builds by polygon
        // VERTEX BYTE CONTENT (purely content-hash; key_fn returns 0 so
        // the match is independent of any other addr_point field). This
        // rewrites old_m's byte-20 vertex_offset to point at the matching
        // polygon's byte position in NEW addr_vertices, so build_merge_seq
        // below classifies stable-polygon addr_points as MATCH instead of
        // DELETE+INSERT — without depending on slot stability.
        //
        // For point-only addr_points (vertex_offset==NO_DATA, block_size
        // 0), the fixup is a no-op (NO_DATA→NO_DATA). Their identity is
        // recovered downstream by str_remap + the secondary-match path.
        MappedFile old_av{}, new_av{};
        std::vector<uint32_t> old_vert_offsets;
        if (addr_stride >= 28) {
            old_av = mmap_file(old_dir + "/addr_vertices.bin");
            new_av = mmap_file(new_dir + "/addr_vertices.bin");
            size_t n_old = old_m.size / addr_stride;
            old_vert_offsets.resize(n_old);
            for (size_t i = 0; i < n_old; i++)
                memcpy(&old_vert_offsets[i],
                       old_m.data + i * addr_stride + 20, 4);
            fixup_v15_offsets(old_m.data, old_m.size,
                              old_av.data ? old_av.data : "", old_av.size,
                              new_m.data, new_m.size,
                              new_av.data ? new_av.data : "", new_av.size,
                              addr_stride, /*off_field_pos*/ 20,
                              /*key_fn*/ [](const char*) -> uint64_t { return 0; });
        }

        auto seq = build_merge_seq(old_m.data, old_m.size, new_m.data, new_m.size, addr_stride);
        auto soft = secondary_match_from_merge(seq, old_m.data, old_m.size, new_m.data, new_m.size, addr_stride,
            [](const char* rec) -> uint64_t {
                uint32_t hn_id, st_id;
                memcpy(&hn_id, rec + 8, 4); memcpy(&st_id, rec + 12, 4);
                return ((uint64_t)st_id << 32) | hn_id;
            });
        auto id_rm = derive_id_remap_from_merge(seq, old_m.size / addr_stride, addr_stride);
        for (auto& [o,n] : soft) if (o < id_rm.size()) id_rm[o] = n;
        // Capture vertex_offset (byte 20) fixups for the merge so the
        // patch tool can rewrite byte 20 of each MATCH-replayed record
        // (it reads raw OLD from disk; without the fixup the reconstructed
        // bytes would carry OLD's vertex_offset instead of NEW's, mismatching
        // the new file).
        auto addr_fixups = collect_offset_fixups(old_vert_offsets, old_m.data, addr_stride,
                                                 ADDR_POINT_VERTEX_OFFSET_OFF);
        res_addr = {PatchFileId::ADDR_POINTS, "addr_points.bin", addr_stride,
                    old_m.size, new_m.size, std::move(seq), std::move(addr_fixups),
                    std::move(soft), std::move(id_rm)};
        log_merge(res_addr);

        // Byte-block merge for addr_vertices. Walk the addr_points parent
        // merge sequence: MATCH parent slots contribute MATCH ops on the
        // polygon byte block (since fixup_v15_offsets above made stable
        // polygons share a byte position in BOTH old and new), DELETE
        // parent slots drop their old block, INSERT parent slots emit
        // new block bytes. Mirrors build_vertex_byte_merge usage for
        // admin_vertices and poi_vertices.
        if (addr_stride >= 28) {
            // Restore old's original vertex_offsets so build_vertex_byte_merge
            // reads each polygon's old block from the *original* byte
            // position (it was rewritten by fixup_v15_offsets to match new).
            size_t n_old = old_m.size / addr_stride;
            for (size_t i = 0; i < n_old; i++)
                memcpy(old_m.data + i * addr_stride + 20,
                       &old_vert_offsets[i], 4);
            auto av_seq = build_vertex_byte_merge(res_addr.seq,
                old_m.data, old_m.size,
                new_m.data, new_m.size,
                old_av.data ? old_av.data : "", old_av.size,
                new_av.data ? new_av.data : "", new_av.size,
                addr_stride, /*off_field_pos*/ 20);
            res_addr_v = {PatchFileId::ADDR_VERTICES, "addr_vertices.bin", 1,
                          old_av.size, new_av.size, std::move(av_seq), {}};
            log_merge(res_addr_v);
        } else {
            // Pre-v15 (no inline polygon header) — addr_vertices is fixed-
            // stride NodeCoord array. Fall through to emit_raw later.
            res_addr_v = {PatchFileId::ADDR_VERTICES, "addr_vertices.bin", 0,
                          0, 0, MergeSequence{}, {}};
        }
        if (old_av.data) unmap_file(old_av);
        if (new_av.data) unmap_file(new_av);
        unmap_file(old_m); unmap_file(new_m);
        log_time("  group:addr_points", gs);
        std::cerr << "  RSS after addr_points: " << get_rss_mb() << " MiB" << std::endl;
    });

    // Group 2: street_ways → street_nodes
    // old_w = COW mmap (needs string remap + offset fixup), new_w = read-only mmap
    // nodes = read-only mmap (huge files, never modified — biggest memory win)
    bool street_from_fallback = false;
    std::thread t_street([&]() {
        double gs = now_ms();
        auto [old_w, ow_fb] = try_load_with_fallback_rw(old_dir, "street_ways.bin",
                                                         {"../full/", "../../full/"});
        auto [new_w, nw_fb] = try_load_with_fallback(new_dir, "street_ways.bin",
                                                      {"../full/", "../../full/"});
        street_from_fallback = (ow_fb || nw_fb);
        remap_field(old_w.data, old_w.size, way_stride, way_stride == 12 ? 8 : 5, str_remap);

        // Variants without their own street_ways (admin, admin-minimal,
        // poi tiers) borrow the sibling /full/ copy only for the string
        // remap. They emit no street records and carry no addr_points,
        // the only consumer of a street id remap, so skip the street
        // pipeline entirely (it costs ~10 GiB per planet invocation).
        if (street_from_fallback) {
            res_ways = {PatchFileId::STREET_WAYS, "street_ways.bin", way_stride,
                        0, 0, MergeSequence{}, {}};
            res_nodes = {PatchFileId::STREET_NODES, "street_nodes.bin", 8,
                         0, 0, MergeSequence{}, {}};
            log_merge(res_ways);
            log_merge(res_nodes);
            street_remap_ready.set_value();
            unmap_file(old_w); unmap_file(new_w);
            log_time("  group:streets (fallback)", gs);
            std::cerr << "  RSS after streets: " << get_rss_mb() << " MiB" << std::endl;
            return;
        }

        auto [old_n, _on_fb] = try_load_with_fallback(old_dir, "street_nodes.bin",
                                                       {"../full/", "../../full/"});
        auto [new_n, _nn_fb] = try_load_with_fallback(new_dir, "street_nodes.bin",
                                                       {"../full/", "../../full/"});
        madvise(const_cast<char*>(old_n.data), old_n.size, MADV_SEQUENTIAL);
        madvise(const_cast<char*>(new_n.data), new_n.size, MADV_SEQUENTIAL);
        // Fixup node_offsets
        size_t wn = old_w.size / way_stride;
        std::vector<uint32_t> old_offsets(wn);
        for (size_t i = 0; i < wn; i++) memcpy(&old_offsets[i], old_w.data + i * way_stride, 4);
        fixup_way_offsets(old_w.data, old_w.size, old_n.data, old_n.size,
                          new_w.data, new_w.size, new_n.data, new_n.size, way_stride);
        auto fixups = collect_offset_fixups(old_offsets, old_w.data, way_stride, 0);
        auto way_seq = build_merge_seq(old_w.data, old_w.size, new_w.data, new_w.size, way_stride);
        size_t way_name_off = (way_stride == 12) ? 8 : 5;
        auto soft = secondary_match_from_merge(way_seq, old_w.data, old_w.size, new_w.data, new_w.size, way_stride,
            [way_name_off, way_stride](const char* rec) -> uint64_t {
                uint32_t name_id; memcpy(&name_id, rec + way_name_off, 4);
                uint32_t nc = record_node_count(rec, way_stride, WAY_HEADER_STRIDE_PACKED);
                return ((uint64_t)name_id << 16) | nc;
            });
        auto id_rm = derive_id_remap_from_merge(way_seq, old_w.size / way_stride, way_stride);
        for (auto& [o,n] : soft) if (o < id_rm.size()) id_rm[o] = n;
        res_ways = {PatchFileId::STREET_WAYS, "street_ways.bin", way_stride,
                    old_w.size, new_w.size, way_seq, std::move(fixups), std::move(soft), std::move(id_rm)};
        log_merge(res_ways);
        // Signal t_poi that res_ways.id_remap is ready. Anything that
        // happens after this point in t_street (node merge, suppression,
        // unmaps) doesn't affect the data t_poi reads.
        street_remap_ready.set_value();
        // Restore original node_offsets for child merge
        for (size_t i = 0; i < wn; i++) memcpy(old_w.data + i * way_stride, &old_offsets[i], 4);
        if (old_n.data && new_n.data) {
            auto node_seq = build_child_merge(way_seq, old_w.data, old_w.size, new_w.data, new_w.size,
                                               old_n.data, old_n.size, new_n.data, new_n.size, way_stride,
                                               WAY_HEADER_STRIDE_PACKED);
            res_nodes = {PatchFileId::STREET_NODES, "street_nodes.bin", 8,
                         old_n.size, new_n.size, std::move(node_seq), {}};
        } else {
            res_nodes = {PatchFileId::STREET_NODES, "street_nodes.bin", 8,
                         0, 0, MergeSequence{}, {}};
        }
        log_merge(res_nodes);
        // Should not happen now that the fallback branch returns early,
        // but kept as a safety net for any future code path that loads
        // ways from a fallback dir without taking the fast path above.
        if (street_from_fallback) {
            res_ways.seq = MergeSequence{};
            res_ways.old_size = 0;
            res_ways.new_size = 0;
            res_ways.fixups = {};
            res_nodes.seq = MergeSequence{};
            res_nodes.old_size = 0;
            res_nodes.new_size = 0;
        }
        unmap_file(old_w); unmap_file(new_w); unmap_file(old_n); unmap_file(new_n);
        log_time("  group:streets", gs);
        std::cerr << "  RSS after streets: " << get_rss_mb() << " MiB" << std::endl;
    });

    // Group 3: interp_ways → interp_nodes
    std::thread t_interp([&]() {
        double gs = now_ms();
        auto old_data = mmap_file_rw(old_dir + "/interp_ways.bin");
        auto new_data = mmap_file_rw(new_dir + "/interp_ways.bin");
        remap_field(old_data.data, old_data.size, interp_stride, interp_stride >= 20 ? 8 : 5, str_remap);
        auto old_n = mmap_file(old_dir + "/interp_nodes.bin");
        auto new_n = mmap_file(new_dir + "/interp_nodes.bin");
        size_t n = old_data.size / interp_stride;
        std::vector<uint32_t> old_offsets(n);
        for (size_t i = 0; i < n; i++) memcpy(&old_offsets[i], old_data.data + i * interp_stride, 4);
        fixup_interp_offsets(old_data.data, old_data.size, old_n.data, old_n.size,
                             new_data.data, new_data.size, new_n.data, new_n.size, interp_stride);
        auto fixups = collect_offset_fixups(old_offsets, old_data.data, interp_stride, 0);
        auto iw_seq = build_merge_seq(old_data.data, old_data.size, new_data.data, new_data.size, interp_stride);
        size_t ist_off = (interp_stride >= 20) ? 8 : 5;
        auto soft = secondary_match_from_merge(iw_seq, old_data.data, old_data.size, new_data.data, new_data.size, interp_stride,
            [ist_off](const char* rec) -> uint64_t {
                uint32_t street_id, start;
                memcpy(&street_id, rec + ist_off, 4);
                memcpy(&start, rec + ist_off + 4, 4);
                return ((uint64_t)street_id << 32) | start;
            });
        auto id_rm = derive_id_remap_from_merge(iw_seq, old_data.size / interp_stride, interp_stride);
        for (auto& [o,n] : soft) if (o < id_rm.size()) id_rm[o] = n;
        res_interp_w = {PatchFileId::INTERP_WAYS, "interp_ways.bin", interp_stride,
                        old_data.size, new_data.size, iw_seq, std::move(fixups), std::move(soft), std::move(id_rm)};
        log_merge(res_interp_w);
        for (size_t i = 0; i < n; i++) memcpy(old_data.data + i * interp_stride, &old_offsets[i], 4);
        auto in_seq = build_child_merge(iw_seq, old_data.data, old_data.size, new_data.data, new_data.size,
                                         old_n.data, old_n.size, new_n.data, new_n.size, interp_stride,
                                         INTERP_WAY_STRIDE_PACKED);
        res_interp_n = {PatchFileId::INTERP_NODES, "interp_nodes.bin", 8,
                        old_n.size, new_n.size, std::move(in_seq), {}};
        log_merge(res_interp_n);
        unmap_file(old_data); unmap_file(new_data); unmap_file(old_n); unmap_file(new_n);
        log_time("  group:interp", gs);
        std::cerr << "  RSS after interp: " << get_rss_mb() << " MiB" << std::endl;
    });

    // Group 4: admin_polygons → admin_vertices
    bool admin_from_fallback = false;
    std::thread t_admin([&]() {
        double gs = now_ms();
        auto [old_data, old_fb] = try_load_with_fallback_rw(old_dir, "admin_polygons.bin",
                                                             {"../../quality/q2.5/", "../quality/q2.5/"});
        auto [new_data, new_fb] = try_load_with_fallback_rw(new_dir, "admin_polygons.bin",
                                                             {"../../quality/q2.5/", "../quality/q2.5/"});
        admin_from_fallback = (old_fb || new_fb);
        if (admin_stride == 24) {
            // Zero only actual padding bytes (14-15), preserve place_type_override at byte 13
            for (size_t i = 0; i + admin_stride <= old_data.size; i += admin_stride)
                memset(old_data.data + i + 14, 0, 2);
            for (size_t i = 0; i + admin_stride <= new_data.size; i += admin_stride)
                memset(new_data.data + i + 14, 0, 2);
        }
        remap_field(old_data.data, old_data.size, admin_stride, 8, str_remap);
        // admin_vertices.bin lives next to admin_polygons.bin in every
        // build mode, so it follows the same fallback resolution.
        auto [old_v, _ov_fb] = try_load_with_fallback(old_dir, "admin_vertices.bin",
                                                       {"../../quality/q2.5/", "../quality/q2.5/"});
        auto [new_v, _nv_fb] = try_load_with_fallback(new_dir, "admin_vertices.bin",
                                                       {"../../quality/q2.5/", "../quality/q2.5/"});
        madvise(const_cast<char*>(old_v.data), old_v.size, MADV_SEQUENTIAL);
        madvise(const_cast<char*>(new_v.data), new_v.size, MADV_SEQUENTIAL);
        size_t n = old_data.size / admin_stride;
        std::vector<uint32_t> old_offsets(n);
        for (size_t i = 0; i < n; i++) memcpy(&old_offsets[i], old_data.data + i * admin_stride, 4);
        // v15-aware fixup: rewrite old polygon vertex_offset to match
        // the new build's byte offset for the same logical polygon.
        // After this pass, unchanged polygons have byte-identical
        // records, so build_merge_seq classifies them as MATCH —
        // which is what build_vertex_byte_merge needs to emit
        // efficient byte-stream MATCH ops below.
        fixup_v15_offsets(old_data.data, old_data.size, old_v.data, old_v.size,
                          new_data.data, new_data.size, new_v.data, new_v.size,
                          admin_stride, /*off_field_pos=*/0,
                          [](const char* rec) -> uint64_t {
                              uint32_t name_id; memcpy(&name_id, rec + 8, 4);
                              uint8_t level = static_cast<uint8_t>(rec[12]);
                              uint16_t cc; memcpy(&cc, rec + 20, 2);
                              uint32_t vert_count; memcpy(&vert_count, rec + 4, 4);
                              uint64_t k = ((uint64_t)name_id << 24) | ((uint64_t)level << 16) | cc;
                              return k ^ ((uint64_t)vert_count << 40);
                          });
        auto fixups = collect_offset_fixups(old_offsets, old_data.data, admin_stride, 0);
        auto ap_seq = build_merge_seq(old_data.data, old_data.size, new_data.data, new_data.size, admin_stride);
        auto soft = secondary_match_from_merge(ap_seq, old_data.data, old_data.size, new_data.data, new_data.size, admin_stride,
            [](const char* rec) -> uint64_t {
                uint32_t name_id; memcpy(&name_id, rec + 8, 4);
                uint8_t level = static_cast<uint8_t>(rec[12]);
                uint16_t cc; memcpy(&cc, rec + 20, 2);
                return ((uint64_t)name_id << 24) | ((uint64_t)level << 16) | cc;
            });
        // Populate id_remap up-front so t_poi can consume the full
        // primary+secondary admin remap before pr_seq is built.
        auto admin_id_rm = derive_id_remap_from_merge(ap_seq, old_data.size / admin_stride, admin_stride);
        for (auto& [o,n] : soft) if (o < admin_id_rm.size()) admin_id_rm[o] = n;
        res_admin_p = {PatchFileId::ADMIN_POLYGONS, "admin_polygons.bin", admin_stride,
                       old_data.size, new_data.size, ap_seq, std::move(fixups), std::move(soft), std::move(admin_id_rm)};
        log_merge(res_admin_p);
        // Signal t_poi that res_admin_p.id_remap is ready. Vertex merge
        // below restores byte 0 of each old record but t_poi only reads
        // res_admin_p.id_remap which is already populated.
        admin_remap_ready.set_value();
        // Restore original old vertex_offsets — build_vertex_byte_merge
        // reads each polygon's bytes from old_v at the *original* old
        // offset, not the post-fixup new offset. (build_merge_seq above
        // used the fixed-up offsets to find MATCH runs.)
        for (size_t i = 0; i < n; i++) memcpy(old_data.data + i * admin_stride, &old_offsets[i], 4);
        // Skip the vertex byte-block merge entirely when admin came from
        // fallback — its output goes straight into the suppression block
        // below, so emitting it just wastes a few hundred MiB of memory
        // (av_seq holds INSERT data for every changed block) and a few
        // seconds of CPU per parallel poi diff.
        if (admin_from_fallback) {
            res_admin_p.seq = MergeSequence{};
            res_admin_p.old_size = 0;
            res_admin_p.new_size = 0;
            res_admin_p.fixups = {};
            res_admin_v = {PatchFileId::ADMIN_VERTICES, "admin_vertices.bin", 1,
                           0, 0, MergeSequence{}, {}, {}, {}};
            log_merge(res_admin_v);
        } else {
            auto av_seq = build_vertex_byte_merge(ap_seq,
                old_data.data, old_data.size, new_data.data, new_data.size,
                old_v.data, old_v.size, new_v.data, new_v.size, admin_stride, 0);
            res_admin_v = {PatchFileId::ADMIN_VERTICES, "admin_vertices.bin", 1,
                           old_v.size, new_v.size, std::move(av_seq), {}, {}, {}};
            log_merge(res_admin_v);
        }
        unmap_file(old_data); unmap_file(new_data); unmap_file(old_v); unmap_file(new_v);
        log_time("  group:admin", gs);
        std::cerr << "  RSS after admin: " << get_rss_mb() << " MiB" << std::endl;
    });

    // Group 5: poi_records → poi_vertices (may not exist in old builds)
    std::thread t_poi([&]() {
        double gs = now_ms();
        auto old_data = mmap_file_rw(old_dir + "/poi_records.bin");
        auto new_data = mmap_file(new_dir + "/poi_records.bin");
        // Handle missing POI files gracefully (new feature, old builds may not have them)
        if (new_data.size == 0 && old_data.size == 0) {
            res_poi_r = {PatchFileId::POI_RECORDS, "poi_records.bin", poi_stride, 0, 0, MergeSequence{}, {}};
            res_poi_v = {PatchFileId::POI_VERTICES, "poi_vertices.bin", 1, 0, 0, MergeSequence{}, {}};
            if (old_data.data) unmap_file(old_data);
            if (new_data.data) unmap_file(new_data);
            log_time("  group:poi (empty)", gs);
            return;
        }
        // String remap on the three string-offset fields:
        //   byte 16 = name_id           (all strides)
        //   byte 24 = parent_street_id  (string offset of nearest street name, v9+ / stride>=28)
        //   byte 28 = parent_postcode_id(string offset of POI postcode, v10+ / stride>=32)
        // All three are string-pool offsets and shift day-over-day exactly
        // like name_id, so they get the same str_remap. (Previously bytes
        // 24/28 were wrongly remapped via the way-index / postcode-centroid
        // tables — see the parent-id block below, now removed.)
        if (old_data.size > 0) {
            remap_field(old_data.data, old_data.size, poi_stride, 16, str_remap);
            if (poi_stride >= 28)
                remap_field(old_data.data, old_data.size, poi_stride, 24, str_remap);
            if (poi_stride >= 32)
                remap_field(old_data.data, old_data.size, poi_stride, 28, str_remap);
        }

        // Remap old PoiRecord parent ids into the new build's id-space so
        // unchanged POIs don't differ day-over-day (otherwise pr_seq
        // classifies most POIs as INSERT/DELETE and the byte-block walker
        // emits INSERT for nearly every vertex block):
        //   bytes 16/24/28 (name_id, parent_street_id, parent_postcode_id)
        //     — string offsets, already remapped via str_remap above.
        //   byte 32 (parent_poly_id) — admin polygon index, remapped below.
        // Block on t_admin so the admin remap is ready before fixup runs.
        admin_remap_future.wait();
        street_remap_future.wait();

        // Parent-poly remap: byte 32 (parent_poly_id) is a genuine admin
        // polygon index and must be remapped from the old build's admin
        // id-space into the new one. (Bytes 24/28 are string offsets,
        // handled by str_remap above — NOT here.)
        if (old_data.size > 0 && poi_stride >= 36) {
            const auto& adm_rm = res_admin_p.id_remap;
            size_t pn = old_data.size / poi_stride;
            for (size_t i = 0; i < pn; i++) {
                char* rec = old_data.data + i * poi_stride;
                if (!adm_rm.empty()) {
                    uint32_t pp; memcpy(&pp, rec + 32, 4);
                    if (pp != NO_DATA && pp < adm_rm.size() && adm_rm[pp] != NO_DATA && adm_rm[pp] != pp)
                        memcpy(rec + 32, &adm_rm[pp], 4);
                }
            }
        }

        auto old_v = mmap_file(old_dir + "/poi_vertices.bin");
        auto new_v = mmap_file(new_dir + "/poi_vertices.bin");
        if (old_v.data) madvise(const_cast<char*>(old_v.data), old_v.size, MADV_SEQUENTIAL);
        if (new_v.data) madvise(const_cast<char*>(new_v.data), new_v.size, MADV_SEQUENTIAL);
        size_t n = old_data.size / poi_stride;
        std::vector<uint32_t> old_offsets(n);
        for (size_t i = 0; i < n; i++) memcpy(&old_offsets[i], old_data.data + i * poi_stride + 8, 4);
        // v15-aware fixup: PoiRecord's vertex_offset lives at byte 8.
        // Same byte-block hash strategy as admin polygons.
        if (old_data.size > 0 && new_data.size > 0) {
            fixup_v15_offsets(old_data.data, old_data.size, old_v.data, old_v.size,
                              new_data.data, new_data.size, new_v.data, new_v.size,
                              poi_stride, /*off_field_pos=*/8,
                              [](const char* rec) -> uint64_t {
                                  uint32_t name_id; memcpy(&name_id, rec + 16, 4);
                                  uint8_t cat = static_cast<uint8_t>(rec[20]);
                                  uint32_t vert_count; memcpy(&vert_count, rec + 12, 4);
                                  return ((uint64_t)name_id << 16) | ((uint64_t)cat << 8) | (vert_count & 0xff);
                              });
        }
        auto fixups = collect_offset_fixups(old_offsets, old_data.data, poi_stride, POI_RECORD_VERTEX_OFFSET_OFF);
        auto pr_seq = build_merge_seq(old_data.data, old_data.size,
                                       new_data.data, new_data.size, poi_stride);
        auto soft = secondary_match_from_merge(pr_seq, old_data.data, old_data.size,
            new_data.data, new_data.size, poi_stride,
            [](const char* rec) -> uint64_t {
                uint32_t name_id; memcpy(&name_id, rec + 16, 4);
                uint8_t category = static_cast<uint8_t>(rec[20]);
                return ((uint64_t)name_id << 8) | category;
            });
        res_poi_r = {PatchFileId::POI_RECORDS, "poi_records.bin", poi_stride,
                     old_data.size, new_data.size, pr_seq, std::move(fixups), std::move(soft), {}};
        log_merge(res_poi_r);
        // Byte-block delta over poi_vertices.bin — same shape as the
        // admin_vertices path. Restore original old vert_offsets first
        // (build_vertex_byte_merge reads each POI's bytes from old_v
        // at the *original* old offset; build_merge_seq above used the
        // fixed-up offsets to find MATCH runs).
        for (size_t i = 0; i < n; i++) memcpy(old_data.data + i * poi_stride + 8, &old_offsets[i], 4);
        auto pv_seq = build_vertex_byte_merge(pr_seq,
            old_data.data, old_data.size, new_data.data, new_data.size,
            old_v.data ? old_v.data : "", old_v.size,
            new_v.data ? new_v.data : "", new_v.size,
            poi_stride, /*off_field_pos=*/8);
        res_poi_v = {PatchFileId::POI_VERTICES, "poi_vertices.bin", 1,
                     old_v.size, new_v.size, std::move(pv_seq), {}, {}, {}};
        log_merge(res_poi_v);
        if (old_data.data) unmap_file(old_data);
        unmap_file(new_data);
        if (old_v.data) unmap_file(old_v);
        if (new_v.data) unmap_file(new_v);
        log_time("  group:poi", gs);
        std::cerr << "  RSS after poi: " << get_rss_mb() << " MiB" << std::endl;
    });

    // Group 6: cell changes
    // Uses sorted vectors + set_difference instead of unordered_sets — much faster for 15M cells
    std::vector<uint64_t> g_added, g_removed, a_added, a_removed, p_added, p_removed, pl_added, pl_removed;
    std::thread t_cells([&]() {
        double gs = now_ms();
        auto diff_cells = [](const std::string& old_path, const std::string& new_path,
                             size_t stride, std::vector<uint64_t>& added, std::vector<uint64_t>& removed) {
            auto old_m = mmap_file(old_path);
            auto new_m = mmap_file(new_path);
            std::vector<uint64_t> old_ids(old_m.size / stride), new_ids(new_m.size / stride);
            for (size_t i = 0; i < old_ids.size(); i++) memcpy(&old_ids[i], old_m.data+i*stride, 8);
            for (size_t i = 0; i < new_ids.size(); i++) memcpy(&new_ids[i], new_m.data+i*stride, 8);
            std::sort(old_ids.begin(), old_ids.end());
            std::sort(new_ids.begin(), new_ids.end());
            std::set_difference(new_ids.begin(), new_ids.end(), old_ids.begin(), old_ids.end(), std::back_inserter(added));
            std::set_difference(old_ids.begin(), old_ids.end(), new_ids.begin(), new_ids.end(), std::back_inserter(removed));
            unmap_file(old_m); unmap_file(new_m);
        };
        diff_cells(old_dir + "/geo_cells.bin", new_dir + "/geo_cells.bin", 20, g_added, g_removed);
        diff_cells(old_dir + "/admin_cells.bin", new_dir + "/admin_cells.bin", 12, a_added, a_removed);
        diff_cells(old_dir + "/poi_cells.bin", new_dir + "/poi_cells.bin", 12, p_added, p_removed);
        diff_cells(old_dir + "/place_cells.bin", new_dir + "/place_cells.bin", 12, pl_added, pl_removed);
        log_time("  group:cell_changes", gs);
    });

    // Group 7: place_nodes (no child files, simple fixed-stride merge)
    std::thread t_place([&]() {
        double gs = now_ms();
        auto old_data = mmap_file_rw(old_dir + "/place_nodes.bin");
        auto new_data = mmap_file(new_dir + "/place_nodes.bin");
        // Handle missing place files gracefully (new feature, old builds may not have them)
        if (new_data.size == 0 && old_data.size == 0) {
            res_place_n = {PatchFileId::PLACE_NODES, "place_nodes.bin", place_stride, 0, 0, MergeSequence{}, {}};
            if (old_data.data) unmap_file(old_data);
            if (new_data.data) unmap_file(new_data);
            log_time("  group:place (empty)", gs);
            return;
        }
        // String remap on name_id field (offset 8 in 16-byte stride)
        if (old_data.size > 0)
            remap_field(old_data.data, old_data.size, place_stride, 8, str_remap);
        // parent_poly_id (byte 16-19, only present in 20-byte stride) is a
        // foreign id into admin_polygons.bin and shifts day-over-day with
        // the admin id-space. Without rewriting it the merge sees
        // byte-different records for every place_node whose containing
        // admin polygon's id moved.
        if (place_stride >= 20 && old_data.size > 0) {
            admin_remap_future.wait();
            const auto& adm_rm = res_admin_p.id_remap;
            if (!adm_rm.empty()) {
                size_t n = old_data.size / place_stride;
                for (size_t i = 0; i < n; i++) {
                    char* rec = old_data.data + i * place_stride;
                    uint32_t pp; memcpy(&pp, rec + 16, 4);
                    if (pp != NO_DATA && pp < adm_rm.size() && adm_rm[pp] != NO_DATA && adm_rm[pp] != pp)
                        memcpy(rec + 16, &adm_rm[pp], 4);
                }
            }
        }
        // Positional merge: strategy-2 keeps each place in its slot, so the
        // files are in slot order, not (type, name, lat, lng) order. The
        // key-sorted merge this replaced re-sent nearly the whole file daily
        // (planet: 3.1M of 4.1M places secondary-matched).
        auto pn_seq = build_merge_seq(old_data.data, old_data.size,
                                      new_data.data, new_data.size, place_stride);
        auto soft = secondary_match_from_merge(pn_seq, old_data.data, old_data.size,
            new_data.data, new_data.size, place_stride,
            [](const char* rec) -> uint64_t {
                uint32_t name_id; memcpy(&name_id, rec + 8, 4);
                uint8_t place_type = static_cast<uint8_t>(rec[12]);
                return ((uint64_t)name_id << 8) | place_type;
            });
        auto id_rm = derive_id_remap_from_merge(pn_seq, old_data.size / place_stride, place_stride);
        for (auto& [o,n] : soft) if (o < id_rm.size()) id_rm[o] = n;
        res_place_n = {PatchFileId::PLACE_NODES, "place_nodes.bin", place_stride,
                       old_data.size, new_data.size, pn_seq, {}, std::move(soft), std::move(id_rm)};
        log_merge(res_place_n);
        if (old_data.data) unmap_file(old_data);
        if (new_data.data) unmap_file(new_data);
        log_time("  group:place", gs);
        std::cerr << "  RSS after place: " << get_rss_mb() << " MiB" << std::endl;
    });

    t_addr.join(); t_street.join(); t_interp.join(); t_admin.join(); t_poi.join(); t_place.join(); t_cells.join();
    log_time("All merge sequences + cell changes built", merge_start);
    // Free string pools (no longer needed after all merges complete).
    // str_remap is kept alive for emit_sparse_delta below — it consumes
    // the unordered_map directly to decide which addr_postcodes /
    // way_postcodes / postcode_centroids positions actually shifted vs
    // just got a string-tier remap. Freeing it here previously caused
    // sparse_delta to see an empty map → no remap applied during diff,
    // patch applied the full remap → verify mismatch
    // (e.g. oceania/full addr_postcodes.bin first_diff=267889).
    for (int t = 0; t < 5; t++) {
        unmap_file(old_tier_maps[t]);
        unmap_file(new_tier_maps[t]);
    }
    { std::vector<char>().swap(old_concat); }
    { std::vector<char>().swap(new_concat); }
    std::cerr << "  RSS after merge phase: " << get_rss_mb() << " MiB" << std::endl;

    // Parent-id remap section. Emitted FIRST so it's loaded before any
    // merge section that consumes it during MATCH replay:
    //   - addr_points.bin: byte 16 parent_way_id (uses street pairs)
    //   - poi_records.bin: bytes 24/28/32 (street/postcode/admin pairs)
    //   - place_nodes.bin: byte 16 parent_poly_id (uses admin pairs)
    //   - way_parents/admin_parents (sparse delta, admin id remap)
    //   - way_postcodes/addr_postcodes (sparse delta, string remap)
    //   - postcode_centroids (sparse delta, string remap on postcode_id)
    // Previous ordering emitted this AFTER addr_points, so addr_points'
    // pw remap saw an empty poi_street_remap → every addr_point that
    // matched via the street-shifted secondary path kept its OLD
    // parent_way_id in the verify output, producing byte mismatches
    // like the oceania/south-america addr_points failures we saw.
    // Without this section, files that reference foreign IDs that
    // shift day-over-day classify nearly every record as DELETE+INSERT
    // (e.g. planet/full's addr_points was 4.89 GiB emitted as INSERT —
    // the entire file). Quality/postal-only variants skip the marker
    // entirely so they don't carry hundreds of MiB of remap pairs they'll
    // never apply.
    bool has_poi = res_poi_r.old_size > 0 || res_poi_r.new_size > 0;
    bool has_place = res_place_n.old_size > 0 || res_place_n.new_size > 0;
    bool has_addr = res_addr.old_size > 0 || res_addr.new_size > 0;
    // Sparse-delta files also need the remap pairs to reconstruct the
    // old → new id mapping during replay.
    auto sparse_file_present = [&](const char* fname) -> bool {
        struct stat st;
        return stat((new_dir + "/" + fname).c_str(), &st) == 0;
    };
    bool has_sparse_delta_files =
        sparse_file_present("way_parents.bin")    ||
        sparse_file_present("way_postcodes.bin")  ||
        sparse_file_present("addr_postcodes.bin") ||
        sparse_file_present("admin_parents.bin")  ||
        sparse_file_present("postcode_centroids.bin");
    bool has_admin_entries = sparse_file_present("admin_entries.bin");
    if (has_poi || has_place || has_addr || has_sparse_delta_files || has_admin_entries) {
        uint32_t marker = POI_PARENT_REMAP_MARKER;
        wval(patch, &marker, 4);
        auto emit_pairs = [&](const std::vector<uint32_t>& rm) {
            uint32_t n_pairs = 0;
            for (uint32_t i = 0; i < rm.size(); i++)
                if (rm[i] != NO_DATA && rm[i] != i) n_pairs++;
            wval(patch, &n_pairs, 4);
            for (uint32_t i = 0; i < rm.size(); i++) {
                if (rm[i] != NO_DATA && rm[i] != i) {
                    uint32_t o = i, nn = rm[i];
                    wval(patch, &o, 4);
                    wval(patch, &nn, 4);
                }
            }
            return n_pairs;
        };
        uint32_t na = emit_pairs(res_admin_p.id_remap);
        // The patcher applies street pairs only to AddrPoint.parent_way_id.
        uint32_t ns = emit_pairs(has_addr ? res_ways.id_remap : std::vector<uint32_t>{});
        // Reserved postcode leg: never populated (parent_postcode_id holds a
        // string offset, remapped via str_remap, not a centroid index). Kept
        // as a literal 0 on the wire for format compatibility.
        uint32_t np = 0;
        wval(patch, &np, 4);
        std::cerr << "  POI parent-id remap: admin_pairs=" << na
                  << " street_pairs=" << ns
                  << " postcode_pairs=" << np << std::endl;
    }

    // Serialize merge results to patch in canonical order, freeing as we go.
    serialize_merge(patch, res_addr, old_dir, new_dir);
    // res_addr_v is built in t_addr only when addr_stride >= 28 (v15+);
    // older variants leave stride=0 and old/new sizes 0, which we treat as
    // "not built" → fall through to the emit_raw(ADDR_VERTICES) call later
    // in the section pipeline. stride=1 marks a byte-merge ready to ship.
    if (res_addr_v.stride == 1) {
        serialize_merge(patch, res_addr_v, old_dir, new_dir);
        { std::vector<char>().swap(res_addr_v.seq.data); }
    }
    serialize_merge(patch, res_ways, old_dir, new_dir);
    serialize_merge(patch, res_nodes, old_dir, new_dir);
    { std::vector<char>().swap(res_nodes.seq.data); }
    serialize_merge(patch, res_interp_w, old_dir, new_dir);
    serialize_merge(patch, res_interp_n, old_dir, new_dir);
    { std::vector<char>().swap(res_interp_n.seq.data); }
    serialize_merge(patch, res_admin_p, old_dir, new_dir);
    serialize_merge(patch, res_admin_v, old_dir, new_dir);
    { std::vector<char>().swap(res_admin_v.seq.data); }
    serialize_merge(patch, res_poi_r, old_dir, new_dir);
    serialize_merge(patch, res_poi_v, old_dir, new_dir);
    { std::vector<char>().swap(res_poi_v.seq.data); }
    serialize_merge(patch, res_place_n, old_dir, new_dir);
    malloc_trim(0); // return freed heap to OS
    std::cerr << "  RSS after serialize: " << get_rss_mb() << " MiB" << std::endl;
    double t0 = now_ms();

    // ID remaps were pre-computed in the parallel merge groups.
    // Cell changes were computed in parallel with merge groups.
    // Just need to compute admin remap (small, kept as hash map).
    auto& w_rm_v = res_ways.id_remap;
    auto& a_rm_v = res_addr.id_remap;
    auto& i_rm_v = res_interp_w.id_remap;
    // The admin leg of the parent-id remap, which is what the patcher
    // rebuilds admin_entries from: the full remap even for variants whose
    // polygons live in quality/q2.5, so applying needs no sibling.
    std::unordered_map<uint32_t,uint32_t> ad_rm_d;
    for (uint32_t i = 0; i < res_admin_p.id_remap.size(); i++) {
        uint32_t n = res_admin_p.id_remap[i];
        if (n != NO_DATA && n != i) ad_rm_d[i] = n;
    }
    std::unordered_map<uint32_t,uint32_t> poi_rm_d;
    if (res_poi_r.old_size > 0 || res_poi_r.new_size > 0) {
        auto vec = derive_id_remap_from_merge(
            res_poi_r.seq,
            res_poi_r.old_size / poi_stride, poi_stride);
        poi_rm_d.reserve(vec.size());
        for (uint32_t i = 0; i < vec.size(); i++)
            if (vec[i] != NO_DATA) poi_rm_d[i] = vec[i];
        for (auto& [o,n] : res_poi_r.secondary_matches) poi_rm_d[o] = n;
    }
    std::unordered_map<uint32_t,uint32_t> place_rm_d;
    if (res_place_n.old_size > 0 || res_place_n.new_size > 0) {
        auto vec = derive_id_remap_from_merge(
            res_place_n.seq,
            res_place_n.old_size / place_stride, place_stride);
        place_rm_d.reserve(vec.size());
        for (uint32_t i = 0; i < vec.size(); i++)
            if (vec[i] != NO_DATA) place_rm_d[i] = vec[i];
        for (auto& [o,n] : res_place_n.secondary_matches) place_rm_d[o] = n;
    }
    log_time("Admin+POI+Place remap", t0);

    // old_geo no longer needed as a vector — streaming corrections use mmap directly

    // Emit cell changes
    { uint32_t marker = CELL_CHANGES_GEO_MARKER, na = g_added.size(), nr = g_removed.size();
      wval(patch, &marker, 4); wval(patch, &na, 4); wval(patch, &nr, 4);
      for (auto c : g_added) wval(patch, &c, 8); for (auto c : g_removed) wval(patch, &c, 8);
      std::cerr << "  Geo cell changes: +" << na << " -" << nr << std::endl; }
    { uint32_t marker = CELL_CHANGES_ADMIN_MARKER, na = a_added.size(), nr = a_removed.size();
      wval(patch, &marker, 4); wval(patch, &na, 4); wval(patch, &nr, 4);
      for (auto c : a_added) wval(patch, &c, 8); for (auto c : a_removed) wval(patch, &c, 8);
      std::cerr << "  Admin cell changes: +" << na << " -" << nr << std::endl; }
    { uint32_t marker = CELL_CHANGES_POI_MARKER, na = p_added.size(), nr = p_removed.size();
      wval(patch, &marker, 4); wval(patch, &na, 4); wval(patch, &nr, 4);
      for (auto c : p_added) wval(patch, &c, 8); for (auto c : p_removed) wval(patch, &c, 8);
      std::cerr << "  POI cell changes: +" << na << " -" << nr << std::endl; }
    { uint32_t marker = CELL_CHANGES_PLACE_MARKER, na = pl_added.size(), nr = pl_removed.size();
      wval(patch, &marker, 4); wval(patch, &na, 4); wval(patch, &nr, 4);
      for (auto c : pl_added) wval(patch, &c, 8); for (auto c : pl_removed) wval(patch, &c, 8);
      std::cerr << "  Place cell changes: +" << na << " -" << nr << std::endl; }

    // Emit secondary remap section (from merge results, no re-reads)
    std::cerr << "  Secondary matches: ways=" << res_ways.secondary_matches.size()
              << " addr=" << res_addr.secondary_matches.size()
              << " interp=" << res_interp_w.secondary_matches.size()
              << " admin=" << res_admin_p.secondary_matches.size()
              << " poi=" << res_poi_r.secondary_matches.size()
              << " place=" << res_place_n.secondary_matches.size() << std::endl;
    { uint32_t marker = SECONDARY_REMAP_MARKER; wval(patch, &marker, 4);
      auto emit_remap = [&](PatchFileId fid, const std::unordered_map<uint32_t,uint32_t>& rm) {
          uint32_t file = static_cast<uint32_t>(fid), count = rm.size();
          wval(patch, &file, 4); wval(patch, &count, 4);
          for (auto& [o,n] : rm) { wval(patch, &o, 4); wval(patch, &n, 4); }
      };
      // Admin pairs travel in the parent-id remap instead.
      uint32_t n_files = 5; wval(patch, &n_files, 4);
      emit_remap(PatchFileId::STREET_WAYS, res_ways.secondary_matches);
      emit_remap(PatchFileId::ADDR_POINTS, res_addr.secondary_matches);
      emit_remap(PatchFileId::INTERP_WAYS, res_interp_w.secondary_matches);
      emit_remap(PatchFileId::POI_RECORDS, res_poi_r.secondary_matches);
      emit_remap(PatchFileId::PLACE_NODES, res_place_n.secondary_matches);
    }

    // --- Streaming entry corrections ---
    // Instead of materializing all 15M cells' entries in memory (rebuild_geo_from_remap_vec),
    // merge-walk old and new geo_cells in parallel. For each old cell, remap its entry IDs
    // on the fly and compare with new entries. This uses O(1) memory per cell.
    double t1 = now_ms();

    // Free merge sequences (already serialized to patch)
    { std::vector<char>().swap(res_addr.seq.data); std::vector<char>().swap(res_ways.seq.data);
      std::vector<char>().swap(res_interp_w.seq.data); std::vector<char>().swap(res_admin_p.seq.data);
      std::vector<char>().swap(res_nodes.seq.data); std::vector<char>().swap(res_interp_n.seq.data);
      std::vector<char>().swap(res_admin_v.seq.data);
      std::vector<char>().swap(res_poi_r.seq.data); std::vector<char>().swap(res_poi_v.seq.data);
      std::vector<char>().swap(res_place_n.seq.data); }

    // mmap all geo/entry files (old + new)
    auto old_geo_m = mmap_file(old_dir + "/geo_cells.bin");
    auto old_se_m = mmap_file(old_dir + "/street_entries.bin");
    auto old_ae_m = mmap_file(old_dir + "/addr_entries.bin");
    auto old_ie_m = mmap_file(old_dir + "/interp_entries.bin");
    auto new_geo_m = mmap_file(new_dir + "/geo_cells.bin");
    auto new_se_m = mmap_file(new_dir + "/street_entries.bin");
    auto new_ae_m = mmap_file(new_dir + "/addr_entries.bin");
    auto new_ie_m = mmap_file(new_dir + "/interp_entries.bin");

    // Shared cell-entry-list parser: reads a uint16 count at `off` followed by
    // `count` uint32 ids. Used for both raw mmap pointers and std::vector<char>
    // buffers (call with .data()/.size()).
    auto parse_ids = [](const char* data, size_t data_size, uint32_t off) -> std::vector<uint32_t> {
        if (off == NO_DATA || off + 2 > data_size) return {};
        uint16_t count; memcpy(&count, data + off, 2);
        if (off + 2 + count * 4 > data_size) return {};
        std::vector<uint32_t> ids(count);
        memcpy(ids.data(), data + off + 2, count * 4);
        return ids;
    };

    // Remap IDs in-place using vector remap (O(1) per ID)
    auto remap_ids_vec = [](std::vector<uint32_t>& ids, const std::vector<uint32_t>& rm) {
        for (auto& id : ids)
            if (id < rm.size() && rm[id] != NO_DATA) id = rm[id];
        std::sort(ids.begin(), ids.end());
    };

    // Build sorted list of old cell_ids to handle added/removed cells
    size_t old_nc = old_geo_m.size / 20;
    size_t new_nc = new_geo_m.size / 20;

    // Build effective old cell set: old cells - removed + added (with empty entries)
    // Both old and new geo_cells are sorted by cell_id. Added/removed cells are also sorted.
    // We merge-walk: old_cells + added - removed vs new_cells
    std::unordered_set<uint64_t> removed_set(g_removed.begin(), g_removed.end());
    // added_cells are sorted — we'll merge them in during the walk
    size_t ai = 0; // index into g_added

    std::cerr << "  Streaming entry corrections: " << old_nc << " old cells, " << new_nc
              << " new cells, +" << g_added.size() << " -" << g_removed.size() << std::endl;
    std::cerr << "  RSS before corrections: " << get_rss_mb() << " MiB" << std::endl;

    // Single-pass merge walk over effective-old vs new cells.
    // For each entry type, compute derived entries on the fly and compare with new.
    auto streaming_corrections = [&](PatchFileId fid, const std::string& fname,
                                      const MappedFile& old_entries, size_t geo_off_pos,
                                      const MappedFile& new_entries,
                                      const std::vector<uint32_t>& id_rm) -> std::vector<char> {
        std::vector<char> buf; buf.resize(12, 0); uint32_t dc = 0;

        // Merge-walk: effective old (old - removed + added) vs new.
        // Both are sorted by cell_id. Walk old and new geo_cells arrays in
        // lockstep, applying added/removed on the old side.
        size_t oi = 0, ni = 0, added_i = 0;
        buf.resize(12, 0); dc = 0;

        while (oi < old_nc || ni < new_nc || added_i < g_added.size()) {
            // Determine next effective-old cell_id
            uint64_t eff_old = UINT64_MAX;
            bool is_added = false;
            // Skip removed old cells
            while (oi < old_nc) {
                memcpy(&eff_old, old_geo_m.data + oi * 20, 8);
                if (!removed_set.count(eff_old)) break;
                oi++;
                eff_old = UINT64_MAX;
            }
            // Check if an added cell comes before
            if (added_i < g_added.size() && g_added[added_i] < eff_old) {
                eff_old = g_added[added_i];
                is_added = true;
            }

            uint64_t n_cid = UINT64_MAX;
            if (ni < new_nc) memcpy(&n_cid, new_geo_m.data + ni * 20, 8);

            if (eff_old == UINT64_MAX && n_cid == UINT64_MAX) break;

            if (eff_old == n_cid) {
                // Cell in both — compute derived and compare
                std::vector<uint32_t> d_ids;
                if (!is_added) {
                    uint32_t off; memcpy(&off, old_geo_m.data + oi * 20 + geo_off_pos, 4);
                    d_ids = parse_ids(old_entries.data, old_entries.size, off);
                    remap_ids_vec(d_ids, id_rm);
                    oi++;
                } else {
                    added_i++;
                }
                uint32_t n_off; memcpy(&n_off, new_geo_m.data + ni * 20 + geo_off_pos, 4);
                auto n_ids = parse_ids(new_entries.data, new_entries.size, n_off);
                bool differs = (d_ids.size() != n_ids.size()) ||
                    (!d_ids.empty() && memcmp(d_ids.data(), n_ids.data(), d_ids.size() * 4) != 0);
                if (differs) {
                    wval(buf, &eff_old, 8); uint16_t c = n_ids.size(); wval(buf, &c, 2);
                    if (!n_ids.empty()) buf.insert(buf.end(), (const char*)n_ids.data(), (const char*)n_ids.data() + n_ids.size() * 4);
                    dc++;
                }
                ni++;
            } else if (eff_old < n_cid) {
                // Cell only in effective-old — correct to empty
                std::vector<uint32_t> d_ids;
                if (!is_added) {
                    uint32_t off; memcpy(&off, old_geo_m.data + oi * 20 + geo_off_pos, 4);
                    d_ids = parse_ids(old_entries.data, old_entries.size, off);
                    remap_ids_vec(d_ids, id_rm);
                    oi++;
                } else {
                    added_i++;
                }
                if (!d_ids.empty()) {
                    wval(buf, &eff_old, 8); uint16_t c = 0; wval(buf, &c, 2);
                    dc++;
                }
            } else {
                // Cell only in new — emit new entries
                uint32_t n_off; memcpy(&n_off, new_geo_m.data + ni * 20 + geo_off_pos, 4);
                auto n_ids = parse_ids(new_entries.data, new_entries.size, n_off);
                if (!n_ids.empty()) {
                    wval(buf, &n_cid, 8); uint16_t c = n_ids.size(); wval(buf, &c, 2);
                    buf.insert(buf.end(), (const char*)n_ids.data(), (const char*)n_ids.data() + n_ids.size() * 4);
                    dc++;
                }
                ni++;
            }
        }
        uint32_t marker = ENTRY_CORRECTION_MARKER, file = static_cast<uint32_t>(fid);
        memcpy(buf.data(), &marker, 4); memcpy(buf.data() + 4, &file, 4); memcpy(buf.data() + 8, &dc, 4);
        std::cerr << "  " << fname << ": " << dc << " cell corrections (" << buf.size() - 12 << " bytes)" << std::endl;
        return buf;
    };

    auto corr_se = streaming_corrections(PatchFileId::STREET_ENTRIES, "street_entries.bin",
        old_se_m, 8, new_se_m, w_rm_v);
    patch.insert(patch.end(), corr_se.begin(), corr_se.end()); { std::vector<char>().swap(corr_se); }

    auto corr_ae = streaming_corrections(PatchFileId::ADDR_ENTRIES, "addr_entries.bin",
        old_ae_m, 12, new_ae_m, a_rm_v);
    patch.insert(patch.end(), corr_ae.begin(), corr_ae.end()); { std::vector<char>().swap(corr_ae); }

    auto corr_ie = streaming_corrections(PatchFileId::INTERP_ENTRIES, "interp_entries.bin",
        old_ie_m, 16, new_ie_m, i_rm_v);
    patch.insert(patch.end(), corr_ie.begin(), corr_ie.end()); { std::vector<char>().swap(corr_ie); }

    // Free remaps and entry mmaps
    { std::vector<uint32_t>().swap(w_rm_v); std::vector<uint32_t>().swap(a_rm_v);
      std::vector<uint32_t>().swap(i_rm_v); }
    unmap_file(old_se_m); unmap_file(old_ae_m); unmap_file(old_ie_m);
    unmap_file(new_se_m); unmap_file(new_ae_m); unmap_file(new_ie_m);
    malloc_trim(0);
    log_time("Streaming entry corrections", t1);
    std::cerr << "  RSS after geo corrections: " << get_rss_mb() << " MiB" << std::endl;

    // Corrections for a cell index the patcher rebuilds from an id remap
    // (admin / POI / place): a per-cell delta from the rebuilt lists, or, for
    // a layout write_cell_lists can't reproduce, every cell whose rebuilt
    // list differs with its new list.
    auto emit_cell_corrections = [&](PatchFileId entries_fid, const std::string& prefix,
                                     const std::unordered_map<uint32_t,uint32_t>& rm,
                                     const std::vector<uint64_t>& added, const std::vector<uint64_t>& removed) {
        auto old_c = read_file(old_dir + "/" + prefix + "_cells.bin");
        auto old_e = read_file(old_dir + "/" + prefix + "_entries.bin");
        auto derived = rebuild_cells_from_remap(old_c, old_e, rm, added, removed);
        auto new_c = read_file(new_dir + "/" + prefix + "_cells.bin");
        auto new_e = read_file(new_dir + "/" + prefix + "_entries.bin");
        auto new_lists = parse_cell_lists(new_c, new_e);
        if (write_cell_lists(new_lists) == std::make_pair(new_c, new_e)) {
            std::vector<char> payload;
            append_cell_list_delta(payload, parse_cell_lists(derived.cells_data, derived.entries_data), new_lists);
            uint32_t marker = CELL_INDEX_DELTA_MARKER, file = static_cast<uint32_t>(entries_fid);
            uint64_t size = payload.size();
            wval(patch, &marker, 4); wval(patch, &file, 4); wval(patch, &size, 8);
            patch.insert(patch.end(), payload.begin(), payload.end());
            std::cerr << "  " << prefix << "_entries.bin: cell index delta (" << size << " bytes)" << std::endl;
            return;
        }
        auto parse = [&](const std::vector<char>& cells, const std::vector<char>& entries)
            -> std::unordered_map<uint64_t, std::vector<uint32_t>> {
            std::unordered_map<uint64_t, std::vector<uint32_t>> m;
            for (size_t i = 0; i < cells.size() / 12; i++) {
                uint64_t cid; memcpy(&cid, cells.data()+i*12, 8);
                uint32_t off; memcpy(&off, cells.data()+i*12+8, 4);
                m[cid] = parse_ids(entries.data(), entries.size(), off);
            }
            return m;
        };
        auto dm = parse(derived.cells_data, derived.entries_data);
        auto nm = parse(new_c, new_e);
        std::vector<char> buf; buf.resize(12, 0); uint32_t dc = 0;
        for (auto& [cid, nids] : nm) {
            auto it = dm.find(cid); auto* dids = it != dm.end() ? &it->second : nullptr;
            bool differs = !dids ? !nids.empty() : (dids->size() != nids.size()) ||
                          (!dids->empty() && memcmp(dids->data(), nids.data(), dids->size()*4) != 0);
            if (differs) { wval(buf, &cid, 8); uint16_t c = nids.size(); wval(buf, &c, 2);
                if (!nids.empty()) buf.insert(buf.end(), (const char*)nids.data(), (const char*)nids.data()+nids.size()*4); dc++; }
        }
        for (auto& [cid, dids] : dm) { if (!nm.count(cid) && !dids.empty()) { wval(buf, &cid, 8); uint16_t c = 0; wval(buf, &c, 2); dc++; } }
        uint32_t marker = ENTRY_CORRECTION_MARKER, file = static_cast<uint32_t>(entries_fid);
        memcpy(buf.data(), &marker, 4); memcpy(buf.data()+4, &file, 4); memcpy(buf.data()+8, &dc, 4);
        patch.insert(patch.end(), buf.begin(), buf.end());
        std::cerr << "  " << prefix << "_entries.bin: " << dc << " cell corrections (" << buf.size()-12 << " bytes)" << std::endl;
    };
    emit_cell_corrections(PatchFileId::ADMIN_ENTRIES, "admin", ad_rm_d, a_added, a_removed);
    if (res_poi_r.old_size > 0 || res_poi_r.new_size > 0)
        emit_cell_corrections(PatchFileId::POI_ENTRIES, "poi", poi_rm_d, p_added, p_removed);
    if (res_place_n.old_size > 0 || res_place_n.new_size > 0)
        emit_cell_corrections(PatchFileId::PLACE_ENTRIES, "place", place_rm_d, pl_added, pl_removed);

    log_time("Entry corrections", t1);
    // No CELL_FLAGS section: every cell whose street / addr / interp list
    // appears or empties already carries an entry correction, which decides
    // the patcher's output on its own (planet: 0.09 MiB/day and a hash map
    // of every old geo cell).
    unmap_file(old_geo_m); unmap_file(new_geo_m);

    // --- Full-replacement sections for secondary files ---
    // Files without record-level diff logic (parallel postcode/parent
    // arrays, postal boundary index, postcode centroids). The patch
    // tool's stride==0 path writes them byte-for-byte from the patch
    // stream. Old-build size is looked up so the `old_size` header
    // field is correct; missing old files (first-time addition) show
    // 0. The whole patch is zstd-compressed at the end of the diff
    // tool, so embedding raw file bytes here does not inflate the
    // final .gcpatch much.
    auto emit_raw = [&](PatchFileId fid, const std::string& fname) {
        std::string new_path = new_dir + "/" + fname;
        std::string old_path = old_dir + "/" + fname;
        struct stat nst;
        // Skip only if the new file doesn't exist on disk. Empty files
        // (size 0) still need an entry so the patch tool creates an
        // empty file in the verify dir — otherwise cmp sees a missing
        // verify file vs an empty new file and reports a mismatch.
        if (stat(new_path.c_str(), &nst) != 0) return;
        uint64_t new_size = (uint64_t)nst.st_size;
        struct stat ost;
        uint64_t old_size = (stat(old_path.c_str(), &ost) == 0) ? (uint64_t)ost.st_size : 0;

        // Unchanged short-circuit: emit a copy-old marker instead of dumping
        // the whole file when it's byte-identical to old. Several raw-emitted
        // files (postcode_centroid_cells/entries, postal_*) are fully
        // deterministic and identical day-over-day; full-replacing them wasted
        // ~5.8 MiB/planet.
        if (try_emit_copy_old(patch, static_cast<uint32_t>(fid), old_path, new_path,
                              old_size, new_size, fname.c_str()))
            return;

        uint32_t fid_u = static_cast<uint32_t>(fid);
        uint32_t stride = 0;  // sentinel: full replacement
        uint32_t nfix = 0;
        uint64_t ds = new_size;

        wval(patch, &fid_u, 4);
        wval(patch, &stride, 4);
        wval(patch, &old_size, 8);
        wval(patch, &new_size, 8);
        wval(patch, &nfix, 4);
        wval(patch, &ds, 8);
        if (new_size > 0) {
            auto new_m = mmap_file(new_path);
            if (new_m.data) {
                patch.insert(patch.end(), new_m.data, new_m.data + new_size);
                unmap_file(new_m);
            }
        }
        std::cerr << "  " << fname << ": full replace " << new_size << " bytes" << std::endl;
    };

    // Sparse position-keyed delta for files where strategy-2 keeps the
    // record index stable but the stored *value* needs a remap to align
    // old & new (admin polygon ids shift via res_admin_p.id_remap;
    // postcode string offsets shift via str_remap). For unchanged
    // entries (vast majority day-over-day), the diff after remap matches
    // byte-for-byte and contributes 0 to the patch. For changed entries,
    // each emits 4 + value_stride bytes. Replaces full_replace which
    // dumped the entire file every day (planet was 666 MiB way_parents,
    // 208 MiB addr_postcodes, 208 MiB admin_parents, 63 MiB
    // postcode_centroids, 4 MiB way_postcodes — about 1.1 GiB
    // uncompressed cut from the patch).
    //
    // remap_kind:
    //   0 = none                  (raw byte compare)
    //   1 = admin polygon idx     (res_admin_p.id_remap, NO_DATA passthrough)
    //   2 = string pool offset    (str_remap)
    //   3 = postcode_centroid     (16B struct, str_remap on bytes 8-11 only)
    auto emit_sparse_delta = [&](PatchFileId fid, const char* fname,
                                  uint32_t value_stride, uint32_t remap_kind) {
        std::string new_path = new_dir + "/" + std::string(fname);
        std::string old_path = old_dir + "/" + std::string(fname);
        struct stat nst;
        if (stat(new_path.c_str(), &nst) != 0) return;
        uint64_t new_size = (uint64_t)nst.st_size;
        struct stat ost;
        uint64_t old_size = (stat(old_path.c_str(), &ost) == 0) ? (uint64_t)ost.st_size : 0;
        // Unchanged short-circuit: emit a copy-old marker when byte-identical.
        // The sparse delta remaps OLD values via an id_remap before comparing,
        // so when an upstream array is non-deterministic the remap can be
        // spuriously non-identity and emit thousands of changes for a file that
        // is in fact byte-identical (observed: 30k way_parents entries on a
        // same-PBF planet diff).
        if (try_emit_copy_old(patch, static_cast<uint32_t>(fid), old_path, new_path,
                              old_size, new_size, fname))
            return;
        if (new_size > 0 && (new_size % value_stride != 0 || old_size == 0)) {
            // Stride mismatch OR introduction day (no old file): fall back to
            // full_replace. A sparse delta against an absent old file would
            // encode EVERY position as an 8-byte (pos,value) pair — 2x the
            // raw file — where a raw replacement compresses far better.
            if (old_size == 0)
                std::cerr << "  " << fname << ": no old file, using full replace ("
                          << new_size << " bytes)" << std::endl;
            else
            std::cerr << "  " << fname << ": stride mismatch (size " << new_size
                      << " not divisible by " << value_stride << "), using full replace" << std::endl;
            uint32_t fid_u = (uint32_t)fid;
            uint32_t stride = 0;
            uint32_t nfix = 0;
            uint64_t ds = new_size;
            wval(patch, &fid_u, 4);
            wval(patch, &stride, 4);
            wval(patch, &old_size, 8);
            wval(patch, &new_size, 8);
            wval(patch, &nfix, 4);
            wval(patch, &ds, 8);
            if (new_size > 0) {
                auto new_m = mmap_file(new_path);
                if (new_m.data) {
                    patch.insert(patch.end(), new_m.data, new_m.data + new_size);
                    unmap_file(new_m);
                }
            }
            return;
        }

        auto old_m = (old_size > 0) ? mmap_file(old_path) : MappedFile{0, 0};
        auto new_m = (new_size > 0) ? mmap_file(new_path) : MappedFile{0, 0};

        size_t old_n = (old_m.data ? old_m.size : 0) / value_stride;
        size_t new_n = (new_m.data ? new_m.size : 0) / value_stride;

        std::vector<char> delta_buf;
        delta_buf.reserve(std::min(new_n * (size_t)(4 + value_stride),
                                    (size_t)64 * 1024));
        uint32_t n_changes = 0;
        // Run-length stats — tells us whether changes cluster (run-length
        // encoding would shrink the fallback) or are scattered (only a
        // structural fix to slot-position stability helps).
        uint32_t n_runs = 0;
        uint32_t cur_run_len = 0;
        uint32_t max_run_len = 0;
        uint64_t sum_runs_len_sq = 0; // for mean-of-squares (run-length-aware size)
        uint32_t last_change_pos = UINT32_MAX;
        auto close_run = [&]() {
            if (cur_run_len > 0) {
                if (cur_run_len > max_run_len) max_run_len = cur_run_len;
                sum_runs_len_sq += (uint64_t)cur_run_len * cur_run_len;
                cur_run_len = 0;
            }
        };
        auto emit_change = [&](uint32_t pos, const char* val_ptr) {
            delta_buf.insert(delta_buf.end(),
                             reinterpret_cast<const char*>(&pos),
                             reinterpret_cast<const char*>(&pos) + 4);
            delta_buf.insert(delta_buf.end(), val_ptr, val_ptr + value_stride);
            n_changes++;
            if (last_change_pos != UINT32_MAX && pos == last_change_pos + 1) {
                cur_run_len++;
            } else {
                close_run();
                cur_run_len = 1;
                n_runs++;
            }
            last_change_pos = pos;
        };

        if (remap_kind == 3) {
            // 16-byte postcode_centroid: str_remap on bytes 8-11 (postcode_id),
            // raw compare on bytes 0-7 (lat/lng) and 12-15 (cc + pad).
            for (size_t pos = 0; pos < new_n; pos++) {
                const char* np = new_m.data + pos * 16;
                if (pos >= old_n) { emit_change((uint32_t)pos, np); continue; }
                const char* op = old_m.data + pos * 16;
                bool d = (memcmp(op, np, 8) != 0) || (memcmp(op + 12, np + 12, 4) != 0);
                if (!d) {
                    uint32_t old_pid; memcpy(&old_pid, op + 8, 4);
                    uint32_t new_pid; memcpy(&new_pid, np + 8, 4);
                    if (old_pid != NO_DATA) {
                        auto it = str_remap.find(old_pid);
                        if (it != str_remap.end()) old_pid = it->second;
                    }
                    d = (old_pid != new_pid);
                }
                if (d) emit_change((uint32_t)pos, np);
            }
        } else {
            // 4-byte uint32 array with optional remap on the value.
            for (size_t pos = 0; pos < new_n; pos++) {
                uint32_t new_val; memcpy(&new_val, new_m.data + pos * 4, 4);
                bool d = false;
                if (pos >= old_n) {
                    d = true;
                } else {
                    uint32_t old_val; memcpy(&old_val, old_m.data + pos * 4, 4);
                    uint32_t remapped = old_val;
                    if (old_val != NO_DATA) {
                        if (remap_kind == 1 && old_val < res_admin_p.id_remap.size()) {
                            uint32_t r = res_admin_p.id_remap[old_val];
                            if (r != NO_DATA) remapped = r;
                        } else if (remap_kind == 2) {
                            auto it = str_remap.find(old_val);
                            if (it != str_remap.end()) remapped = it->second;
                        }
                    }
                    d = (remapped != new_val);
                }
                if (d) emit_change((uint32_t)pos, reinterpret_cast<const char*>(&new_val));
            }
        }

        close_run();
        // Hypothetical run-length-encoded size for diagnostic comparison.
        // Format would be (start_pos:u32, run_len:u32, values:run_len*vs).
        // Per-run overhead is 8 bytes (pos+len); per-value cost is vs bytes.
        size_t rle_payload_bytes = (size_t)n_runs * 8 + (size_t)n_changes * value_stride;
        double mean_run_len = n_runs ? (double)n_changes / (double)n_runs : 0.0;
        size_t sparse_section_bytes = 4 /*stride*/ + 8 /*old_size*/ + 8 /*new_size*/
                                       + 4 /*value_stride*/ + 4 /*remap_kind*/
                                       + 4 /*n_changes*/ + delta_buf.size();
        size_t full_section_bytes = 4 /*stride*/ + 8 + 8 + 4 /*nfix*/ + 8 /*ds*/ + (size_t)new_size;
        // NOTE: we used to fall back to FULL_REPLACE when sparse ≥ full.
        // That branch hid the underlying defect — sparse only grows past
        // full when >50% of positions appear "changed" after remap, which
        // means strategy-2 slot stability is broken upstream (the file
        // contents at slot[i] genuinely diverge day-over-day instead of
        // mostly matching via str_remap / admin_id_remap / postcode remap).
        // Per project direction: never silently fall back. Always emit
        // the sparse format and let the patch size expose the real cost
        // of any instability so the build-side fix can be tracked.
        uint32_t fid_u = (uint32_t)fid;

        uint32_t stride = SPARSE_DELTA_STRIDE;
        wval(patch, &fid_u, 4);
        wval(patch, &stride, 4);
        wval(patch, &old_size, 8);
        wval(patch, &new_size, 8);
        wval(patch, &value_stride, 4);
        wval(patch, &remap_kind, 4);
        wval(patch, &n_changes, 4);
        patch.insert(patch.end(), delta_buf.begin(), delta_buf.end());

        if (old_m.data) unmap_file(old_m);
        if (new_m.data) unmap_file(new_m);

        std::cerr << "  " << fname << ": sparse delta " << n_changes << "/"
                  << new_n << " (" << sparse_section_bytes
                  << " bytes vs " << new_size << " full"
                  << " runs=" << n_runs << " mean_run=" << mean_run_len
                  << " max_run=" << max_run_len
                  << " rle_payload=" << rle_payload_bytes << ")" << std::endl;
    };

    emit_sparse_delta(PatchFileId::ADDR_POSTCODES,       "addr_postcodes.bin",  4,  2);
    emit_sparse_delta(PatchFileId::ADMIN_PARENTS,        "admin_parents.bin",   4,  1);
    emit_sparse_delta(PatchFileId::WAY_PARENTS,          "way_parents.bin",     4,  1);
    emit_sparse_delta(PatchFileId::WAY_POSTCODES,        "way_postcodes.bin",   4,  2);
    emit_sparse_delta(PatchFileId::INTERP_POSTCODES,     "interp_postcodes.bin", 4, 2);
    emit_sparse_delta(PatchFileId::POSTCODE_CENTROIDS,   "postcode_centroids.bin", 16, 3);
    // A cell index whose ids are strategy-2 slots barely changes day to day:
    // send the cells that lost or gained ids instead of both files.
    // Full-replacing postcode_centroid_* cost ~3.7 MiB per planet mode dir.
    auto emit_cell_index = [&](PatchFileId cells_fid, const std::string& cells_name,
                               PatchFileId entries_fid, const std::string& entries_name) {
        struct stat st;
        bool present = stat((old_dir + "/" + cells_name).c_str(), &st) == 0 &&
                       stat((old_dir + "/" + entries_name).c_str(), &st) == 0 &&
                       stat((new_dir + "/" + cells_name).c_str(), &st) == 0 &&
                       stat((new_dir + "/" + entries_name).c_str(), &st) == 0;
        if (present) {
            auto old_c = read_file(old_dir + "/" + cells_name), old_e = read_file(old_dir + "/" + entries_name);
            auto new_c = read_file(new_dir + "/" + cells_name), new_e = read_file(new_dir + "/" + entries_name);
            auto new_lists = parse_cell_lists(new_c, new_e);
            bool unchanged = old_c == new_c && old_e == new_e;
            // Only a layout write_cell_lists reproduces byte for byte can be rebuilt.
            if (!unchanged && write_cell_lists(new_lists) == std::make_pair(new_c, new_e)) {
                std::vector<char> payload;
                uint64_t new_entries_size = new_e.size();
                payload.insert(payload.end(), (const char*)&new_entries_size, (const char*)&new_entries_size + 8);
                append_cell_list_delta(payload, parse_cell_lists(old_c, old_e), new_lists);
                uint32_t fid = static_cast<uint32_t>(cells_fid), stride = CELL_LIST_DELTA_STRIDE;
                uint64_t old_size = old_c.size(), new_size = new_c.size(), payload_size = payload.size();
                wval(patch, &fid, 4); wval(patch, &stride, 4);
                wval(patch, &old_size, 8); wval(patch, &new_size, 8); wval(patch, &payload_size, 8);
                patch.insert(patch.end(), payload.begin(), payload.end());
                std::cerr << "  " << cells_name << " + " << entries_name << ": cell list delta "
                          << payload_size << " bytes vs " << new_c.size() + new_e.size() << " full" << std::endl;
                return;
            }
        }
        emit_raw(cells_fid, cells_name);
        emit_raw(entries_fid, entries_name);
    };
    emit_cell_index(PatchFileId::POSTCODE_CENTROID_CELLS, "postcode_centroid_cells.bin",
                    PatchFileId::POSTCODE_CENTROID_ENTRIES, "postcode_centroid_entries.bin");
    // Postal polygons are the admin_level 11 copies of admin polygons, in
    // slot order with their own packed vertex stream, so they merge like
    // admin_polygons / admin_vertices. Full-replacing them re-sent ~92 MiB
    // of planet/quality/uncapped every day.
    {
        auto old_p = mmap_file_rw(old_dir + "/postal_polygons.bin");
        auto new_p = mmap_file(new_dir + "/postal_polygons.bin");
        auto old_v = mmap_file(old_dir + "/postal_vertices.bin");
        auto new_v = mmap_file(new_dir + "/postal_vertices.bin");
        const size_t stride = admin_stride;
        bool mergeable = stride == 24 && old_p.size > 0 && new_p.size > 0 && old_v.size > 0 && new_v.size > 0
                         && old_p.size % stride == 0 && new_p.size % stride == 0;
        if (mergeable) {
            remap_field(old_p.data, old_p.size, stride, ADMIN_POLYGON_NAME_ID_OFF, str_remap);
            size_t n = old_p.size / stride;
            std::vector<uint32_t> old_offsets(n);
            for (size_t i = 0; i < n; i++) memcpy(&old_offsets[i], old_p.data + i * stride, 4);
            fixup_v15_offsets(old_p.data, old_p.size, old_v.data, old_v.size,
                              new_p.data, new_p.size, new_v.data, new_v.size, stride, /*off_field_pos=*/0,
                              [](const char* rec) -> uint64_t {
                                  uint32_t name_id; memcpy(&name_id, rec + 8, 4);
                                  uint8_t level = static_cast<uint8_t>(rec[12]);
                                  uint16_t cc; memcpy(&cc, rec + 20, 2);
                                  uint32_t vert_count; memcpy(&vert_count, rec + 4, 4);
                                  uint64_t k = ((uint64_t)name_id << 24) | ((uint64_t)level << 16) | cc;
                                  return k ^ ((uint64_t)vert_count << 40);
                              });
            auto fixups = collect_offset_fixups(old_offsets, old_p.data, stride, 0);
            auto seq = build_merge_seq(old_p.data, old_p.size, new_p.data, new_p.size, stride);
            // The vertex merge reads each polygon's bytes at its original offset.
            for (size_t i = 0; i < n; i++) memcpy(old_p.data + i * stride, &old_offsets[i], 4);
            auto vseq = build_vertex_byte_merge(seq, old_p.data, old_p.size, new_p.data, new_p.size,
                                                old_v.data, old_v.size, new_v.data, new_v.size, stride, 0);
            FileMergeResult rp = {PatchFileId::POSTAL_POLYGONS, "postal_polygons.bin", stride,
                                  old_p.size, new_p.size, std::move(seq), std::move(fixups), {}, {}};
            FileMergeResult rv = {PatchFileId::POSTAL_VERTICES, "postal_vertices.bin", 1,
                                  old_v.size, new_v.size, std::move(vseq), {}, {}, {}};
            log_merge(rp); log_merge(rv);
            serialize_merge(patch, rp, old_dir, new_dir);
            serialize_merge(patch, rv, old_dir, new_dir);
        }
        unmap_file(old_p); unmap_file(new_p); unmap_file(old_v); unmap_file(new_v);
        if (!mergeable) {
            emit_raw(PatchFileId::POSTAL_POLYGONS, "postal_polygons.bin");
            emit_raw(PatchFileId::POSTAL_VERTICES, "postal_vertices.bin");
        }
    }
    // ADDR_VERTICES was already serialized as a byte-merge for v15+
    // (addr_stride >= 28) just after res_addr. Only fall back to FULL_REPLACE
    // here for older variants where t_addr left res_addr_v.stride == 0.
    if (res_addr_v.stride != 1) {
        emit_raw(PatchFileId::ADDR_VERTICES, "addr_vertices.bin");
    }

    // Now safe to free str_remap — all consumers (sparse_delta above)
    // have finished using it.
    { std::unordered_map<uint32_t,uint32_t>().swap(str_remap); }

    // End marker
    uint32_t end_marker = SECTION_END_MARKER;
    wval(patch, &end_marker, 4);

    std::cerr << "\nUncompressed patch: " << patch.size() << " bytes ("
              << patch.size() / 1024 / 1024 << " MiB)"
              << " RSS=" << get_rss_mb() << " MiB" << std::endl;

    // Compress for transport, streaming straight into zstd: no staged
    // uncompressed copy, and a short write or zstd failure fails the diff.
    {
        double tc = now_ms();
        std::string cmd = "zstd -19 -T0 -q -f -o '" + patch_path + "'";
        FILE* z = popen(cmd.c_str(), "w");
        size_t put = z ? fwrite(patch.data(), 1, patch.size(), z) : 0;
        int rc = z ? pclose(z) : -1;
        if (put != patch.size() || rc == -1 || !WIFEXITED(rc) || WEXITSTATUS(rc) != 0) {
            std::cerr << "Patch compression failed (wrote " << put << " of " << patch.size()
                      << " bytes, zstd rc=" << rc << ")" << std::endl;
            return 1;
        }
        struct stat cst; stat(patch_path.c_str(), &cst);
        std::cerr << "Compressed patch: " << cst.st_size << " bytes ("
                  << cst.st_size / 1024 / 1024 << " MiB)" << std::endl;
        log_time("zstd -19 -T0 compression", tc);
    }
    return 0;
}

// The one error edge for run().
int main(int argc, char* argv[]) {
    try {
        return run(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}
