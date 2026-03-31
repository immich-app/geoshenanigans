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
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "patch_format.h"

// --- Merge sequence encoding ---
// Walk two sorted arrays (old remapped, new), matching by record bytes.
// Emit: MATCH(n) = copy n records from old, INSERT(n,data) = add n new records,
//       DELETE(n) = skip n old records.

enum MergeOp : uint8_t { OP_MATCH_RUN = 0, OP_INSERT_RUN = 1, OP_DELETE_RUN = 2 };

struct MergeSequence {
    std::vector<char> data; // serialized ops

    void add_match(uint32_t count) {
        uint8_t op = OP_MATCH_RUN;
        data.insert(data.end(), (char*)&op, (char*)&op + 1);
        data.insert(data.end(), (char*)&count, (char*)&count + 4);
    }
    void add_delete(uint32_t count) {
        uint8_t op = OP_DELETE_RUN;
        data.insert(data.end(), (char*)&op, (char*)&op + 1);
        data.insert(data.end(), (char*)&count, (char*)&count + 4);
    }
    void add_insert(const char* records, uint32_t count, size_t stride) {
        uint8_t op = OP_INSERT_RUN;
        data.insert(data.end(), (char*)&op, (char*)&op + 1);
        data.insert(data.end(), (char*)&count, (char*)&count + 4);
        data.insert(data.end(), records, records + count * stride);
    }
};

// Build merge sequence for sorted data files (no hash table needed).
// Both old (after string remap) and new are in deterministic sort order.
// cmp(a, b) returns <0 if a sorts before b, >0 if after, 0 if same key.
// When cmp returns 0 but bytes differ, the record was modified (DELETE+INSERT).
template<typename CmpFn>
static MergeSequence build_merge_seq_sorted(
    const std::vector<char>& old_data, const std::vector<char>& new_data,
    size_t stride, CmpFn cmp)
{
    size_t old_n = old_data.size() / stride;
    size_t new_n = new_data.size() / stride;
    size_t oi = 0, ni = 0;
    MergeSequence seq;
    uint32_t match_run = 0, del_run = 0;
    auto flush_match = [&]() { if (match_run > 0) { seq.add_match(match_run); match_run = 0; } };
    auto flush_del = [&]() { if (del_run > 0) { seq.add_delete(del_run); del_run = 0; } };

    while (oi < old_n && ni < new_n) {
        const char* op = old_data.data() + oi * stride;
        const char* np = new_data.data() + ni * stride;
        if (memcmp(op, np, stride) == 0) {
            flush_del(); match_run++; oi++; ni++;
        } else {
            int c = cmp(op, np);
            if (c < 0) { flush_match(); del_run++; oi++; }               // old-only → deleted
            else if (c > 0) { flush_match(); flush_del(); seq.add_insert(np, 1, stride); ni++; } // new-only → inserted
            else { flush_match(); del_run++; oi++; flush_del(); seq.add_insert(np, 1, stride); ni++; } // same key, different bytes → modified
        }
    }
    flush_match(); flush_del();
    if (oi < old_n) seq.add_delete(old_n - oi);
    if (ni < new_n) seq.add_insert(new_data.data() + ni * stride, new_n - ni, stride);
    return seq;
}

// Build merge sequence for a data file.
// old_data has string remap already applied.
// Records are compared by byte equality (stride bytes).
static MergeSequence build_merge_seq(
    const std::vector<char>& old_data, const std::vector<char>& new_data,
    size_t stride)
{
    size_t old_n = old_data.size() / stride;
    size_t new_n = new_data.size() / stride;
    size_t oi = 0, ni = 0;
    MergeSequence seq;
    uint32_t match_run = 0, del_run = 0;

    auto flush_match = [&]() { if (match_run > 0) { seq.add_match(match_run); match_run = 0; } };
    auto flush_del = [&]() { if (del_run > 0) { seq.add_delete(del_run); del_run = 0; } };

    // For small strides (<=8), records aren't unique enough for hash matching.
    // Use simple sequential scan instead.
    bool use_hash = (stride > 8);

    // Pre-build hash index of new records for fast mismatch resolution
    auto record_hash = [&](const char* p, size_t s) -> uint64_t {
        uint64_t h = 14695981039346656037ULL;
        for (size_t i = 0; i < s; i++) { h ^= (uint8_t)p[i]; h *= 1099511628211ULL; }
        return h;
    };
    std::unordered_multimap<uint64_t, uint32_t> new_hash;
    if (use_hash) {
        new_hash.reserve(new_n);
        for (uint32_t i = 0; i < new_n; i++)
            new_hash.emplace(record_hash(new_data.data() + i * stride, stride), i);
    }

    while (oi < old_n && ni < new_n) {
        const char* op = old_data.data() + oi * stride;
        const char* np = new_data.data() + ni * stride;

        if (memcmp(op, np, stride) == 0) {
            flush_del();
            match_run++;
            oi++; ni++;
        } else if (use_hash) {
            // Hash-based mismatch resolution (for records with unique content)
            uint64_t oh = record_hash(op, stride);
            auto range = new_hash.equal_range(oh);
            uint32_t best_ni = UINT32_MAX;
            for (auto it = range.first; it != range.second; ++it) {
                if (it->second >= ni && it->second < ni + 10000 &&
                    memcmp(op, new_data.data() + it->second * stride, stride) == 0) {
                    if (it->second < best_ni) best_ni = it->second;
                }
            }
            if (best_ni != UINT32_MAX && best_ni > ni) {
                flush_match(); flush_del();
                seq.add_insert(new_data.data() + ni * stride, best_ni - ni, stride);
                ni = best_ni;
            } else if (best_ni == ni) {
                flush_del(); match_run++; oi++; ni++;
            } else {
                flush_match(); del_run++; oi++;
            }
        } else {
            // Sequential scan (for coordinate files where records aren't unique)
            size_t lookahead = std::min((size_t)200, std::min(old_n - oi, new_n - ni));
            bool found = false;
            for (size_t k = 1; k <= lookahead; k++) {
                if (ni + k < new_n && memcmp(op, new_data.data() + (ni + k) * stride, stride) == 0) {
                    flush_match(); flush_del();
                    seq.add_insert(new_data.data() + ni * stride, k, stride);
                    ni += k; found = true; break;
                }
                if (oi + k < old_n && memcmp(np, old_data.data() + (oi + k) * stride, stride) == 0) {
                    flush_match(); del_run += k; oi += k; found = true; break;
                }
            }
            if (!found) { flush_match(); del_run++; oi++; flush_del(); seq.add_insert(np, 1, stride); ni++; }
        }
    }

    flush_match(); flush_del();
    // Remaining old records: deletions
    if (oi < old_n) seq.add_delete(old_n - oi);
    // Remaining new records: insertions
    if (ni < new_n) seq.add_insert(new_data.data() + ni * stride, new_n - ni, stride);

    return seq;
}

// --- String remap ---

static std::unordered_map<uint32_t, uint32_t> build_string_remap(
    const std::vector<char>& old_pool, const std::vector<char>& new_pool)
{
    std::unordered_map<std::string, uint32_t> new_idx;
    size_t pos = 0;
    while (pos < new_pool.size()) {
        const char* s = new_pool.data() + pos;
        size_t len = strlen(s);
        new_idx[std::string(s, len)] = static_cast<uint32_t>(pos);
        pos += len + 1;
    }
    std::unordered_map<uint32_t, uint32_t> remap;
    pos = 0;
    while (pos < old_pool.size()) {
        const char* s = old_pool.data() + pos;
        size_t len = strlen(s);
        auto it = new_idx.find(std::string(s, len));
        if (it != new_idx.end())
            remap[static_cast<uint32_t>(pos)] = it->second;
        pos += len + 1;
    }
    return remap;
}

// --- Remap + fixup helpers ---

static void remap_addr_points(std::vector<char>& data, const std::unordered_map<uint32_t,uint32_t>& rm) {
    for (size_t i = 0; i + 16 <= data.size(); i += 16)
        for (size_t off : {8, 12}) {
            uint32_t v; memcpy(&v, data.data() + i + off, 4);
            auto it = rm.find(v); if (it != rm.end()) memcpy(data.data() + i + off, &it->second, 4);
        }
}
static void remap_field(std::vector<char>& data, size_t stride, size_t field_off,
                         const std::unordered_map<uint32_t,uint32_t>& rm) {
    for (size_t i = 0; i + stride <= data.size(); i += stride) {
        uint32_t v; memcpy(&v, data.data() + i + field_off, 4);
        auto it = rm.find(v); if (it != rm.end()) memcpy(data.data() + i + field_off, &it->second, 4);
    }
}

// Content matching for ways (by name + nodes, ignoring node_offset)
static uint64_t fnv_mix(uint64_t h, uint64_t v) { h ^= v; h *= 1099511628211ULL; return h; }

static void fixup_way_offsets(std::vector<char>& old_ways, const std::vector<char>& old_nodes,
                                const std::vector<char>& new_ways, const std::vector<char>& new_nodes,
                                size_t stride) {
    size_t name_off = (stride == 12) ? 8 : 5;
    size_t old_n = old_ways.size() / stride, new_n = new_ways.size() / stride;
    size_t old_nc = old_nodes.size() / 8, new_nc = new_nodes.size() / 8;

    auto way_hash = [&](const char* w, const char* nodes, size_t max_n) -> uint64_t {
        uint32_t node_offset, name_id; uint8_t node_count;
        memcpy(&node_offset, w, 4); node_count = (uint8_t)w[4]; memcpy(&name_id, w + name_off, 4);
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, name_id); h = fnv_mix(h, node_count);
        for (uint8_t j = 0; j < node_count && (node_offset + j) < max_n; j++) {
            float lat, lng;
            memcpy(&lat, nodes + (node_offset + j) * 8, 4);
            memcpy(&lng, nodes + (node_offset + j) * 8 + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    std::unordered_multimap<uint64_t, uint32_t> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map.emplace(way_hash(new_ways.data() + i * stride, new_nodes.data(), new_nc), i);

    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = way_hash(old_ways.data() + i * stride, old_nodes.data(), old_nc);
        auto it = new_map.find(h);
        if (it != new_map.end()) {
            // Fix node_offset to match new way
            uint32_t new_node_off;
            memcpy(&new_node_off, new_ways.data() + it->second * stride, 4);
            memcpy(old_ways.data() + i * stride, &new_node_off, 4);
            new_map.erase(it);
        }
    }
}

// Same for admin polygons (vertex_offset)
static void fixup_admin_offsets(std::vector<char>& old_polys, const std::vector<char>& old_verts,
                                  const std::vector<char>& new_polys, const std::vector<char>& new_verts,
                                  size_t stride) {
    size_t old_n = old_polys.size() / stride, new_n = new_polys.size() / stride;
    size_t old_vc = old_verts.size() / 8, new_vc = new_verts.size() / 8;

    auto poly_hash = [&](const char* p, const char* verts, size_t max_v) -> uint64_t {
        uint32_t vert_offset, vert_count, name_id;
        memcpy(&vert_offset, p, 4); memcpy(&vert_count, p + 4, 4); memcpy(&name_id, p + 8, 4);
        uint8_t level = (uint8_t)p[12]; uint16_t cc; memcpy(&cc, p + 20, 2);
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, name_id); h = fnv_mix(h, level); h = fnv_mix(h, cc); h = fnv_mix(h, vert_count);
        for (uint32_t j = 0; j < std::min(vert_count, 10u) && (vert_offset + j) < max_v; j++) {
            float lat, lng;
            memcpy(&lat, verts + (vert_offset + j) * 8, 4); memcpy(&lng, verts + (vert_offset + j) * 8 + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    std::unordered_multimap<uint64_t, uint32_t> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map.emplace(poly_hash(new_polys.data() + i * stride, new_verts.data(), new_vc), i);

    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = poly_hash(old_polys.data() + i * stride, old_verts.data(), old_vc);
        auto it = new_map.find(h);
        if (it != new_map.end()) {
            uint32_t new_vert_off;
            memcpy(&new_vert_off, new_polys.data() + it->second * stride, 4);
            memcpy(old_polys.data() + i * stride, &new_vert_off, 4);
            new_map.erase(it);
        }
    }
}

// Same for interp ways
static void fixup_interp_offsets(std::vector<char>& old_data, const std::vector<char>& old_nodes,
                                   const std::vector<char>& new_data, const std::vector<char>& new_nodes,
                                   size_t stride) {
    size_t street_off = (stride >= 20) ? 8 : 5;
    size_t old_n = old_data.size() / stride, new_n = new_data.size() / stride;
    size_t old_nc = old_nodes.size() / 8, new_nc = new_nodes.size() / 8;

    auto ihash = [&](const char* p, const char* nodes, size_t max_n) -> uint64_t {
        uint32_t node_offset, street_id, start, end; uint8_t count, itype;
        memcpy(&node_offset, p, 4); count = (uint8_t)p[4];
        memcpy(&street_id, p + street_off, 4); memcpy(&start, p + street_off + 4, 4);
        memcpy(&end, p + street_off + 8, 4);
        itype = (street_off + 12 < stride) ? (uint8_t)p[street_off + 12] : 0;
        uint64_t h = 14695981039346656037ULL;
        h = fnv_mix(h, street_id); h = fnv_mix(h, start); h = fnv_mix(h, end);
        h = fnv_mix(h, itype); h = fnv_mix(h, count);
        for (uint8_t j = 0; j < count && (node_offset + j) < max_n; j++) {
            float lat, lng;
            memcpy(&lat, nodes + (node_offset + j) * 8, 4); memcpy(&lng, nodes + (node_offset + j) * 8 + 4, 4);
            h = fnv_mix(h, to_grid(lat)); h = fnv_mix(h, to_grid(lng));
        }
        return h;
    };

    std::unordered_multimap<uint64_t, uint32_t> new_map;
    new_map.reserve(new_n);
    for (uint32_t i = 0; i < new_n; i++)
        new_map.emplace(ihash(new_data.data() + i * stride, new_nodes.data(), new_nc), i);
    for (uint32_t i = 0; i < old_n; i++) {
        uint64_t h = ihash(old_data.data() + i * stride, old_nodes.data(), old_nc);
        auto it = new_map.find(h);
        if (it != new_map.end()) {
            uint32_t new_off; memcpy(&new_off, new_data.data() + it->second * stride, 4);
            memcpy(old_data.data() + i * stride, &new_off, 4);
            new_map.erase(it);
        }
    }
}

// --- Secondary matching for modified records ---
// After the merge sequence is built, match DELETE'd and INSERT'd records by a
// relaxed key to recover ID mappings for records that changed (e.g. geometry edit)
// but represent the same logical entity. Only matches when the key group has
// equal counts on both sides to avoid mismatches.
template<typename KeyFn>
static std::unordered_map<uint32_t, uint32_t> secondary_match_from_merge(
    const MergeSequence& seq,
    const std::vector<char>& old_data,
    const std::vector<char>& new_data,
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

    // Group by key
    std::unordered_map<uint64_t, std::vector<uint32_t>> del_by_key, ins_by_key;
    for (uint32_t di : del_indices) {
        if ((size_t)(di + 1) * stride > old_data.size()) continue;
        del_by_key[key_fn(old_data.data() + di * stride)].push_back(di);
    }
    for (uint32_t ii : ins_indices) {
        if ((size_t)(ii + 1) * stride > new_data.size()) continue;
        ins_by_key[key_fn(new_data.data() + ii * stride)].push_back(ii);
    }

    // Match only when equal counts per key (safe pairwise match in index order)
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
    std::vector<uint32_t> id_map(old_count, 0xFFFFFFFF);
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
    std::vector<std::pair<uint32_t,uint32_t>> fixups; // (record_idx, new_offset)
    std::unordered_map<uint32_t,uint32_t> secondary_matches; // soft old→new ID map
    std::vector<uint32_t> id_remap; // derived old→new ID remap (with secondary merged in)
};

// Serialize a pre-computed merge result into the patch buffer
static void serialize_merge(std::vector<char>& patch, const FileMergeResult& r) {
    auto wv = [&](const void* data, size_t size) {
        patch.insert(patch.end(), (const char*)data, (const char*)data + size);
    };
    uint32_t fid = static_cast<uint32_t>(r.id);
    uint32_t st = static_cast<uint32_t>(r.stride);
    uint32_t n_fixups = static_cast<uint32_t>(r.fixups.size());
    wv(&fid, 4); wv(&st, 4); wv(&r.old_size, 8); wv(&r.new_size, 8);
    wv(&n_fixups, 4);
    if (n_fixups > 0) {
        std::vector<char> delta_buf;
        uint32_t prev_idx = 0, prev_val = 0;
        for (auto& [idx, val] : r.fixups) {
            write_varint(delta_buf, idx - prev_idx);
            write_varint(delta_buf, val - prev_val);
            prev_idx = idx; prev_val = val;
        }
        uint32_t delta_size = static_cast<uint32_t>(delta_buf.size());
        wv(&delta_size, 4);
        patch.insert(patch.end(), delta_buf.begin(), delta_buf.end());
    }
    uint64_t ss = r.seq.data.size();
    wv(&ss, 8);
    patch.insert(patch.end(), r.seq.data.begin(), r.seq.data.end());
}

static std::mutex log_mutex;
static void log_merge(const FileMergeResult& r) {
    std::lock_guard<std::mutex> lock(log_mutex);
    std::cerr << "  " << r.name << ": seq=" << r.seq.data.size()
              << " fixups=" << r.fixups.size()
              << " (" << std::fixed << std::setprecision(2)
              << (r.new_size > 0 ? r.seq.data.size() * 100.0 / r.new_size : 0) << "%)" << std::endl;
}

// --- Write helpers ---
static void wval(std::vector<char>& buf, const void* data, size_t size) {
    buf.insert(buf.end(), (const char*)data, (const char*)data + size);
}

int main(int argc, char* argv[]) {
    if (argc < 5 || std::string(argv[3]) != "-o") {
        std::cerr << "Usage: geocoder-diff <old-dir> <new-dir> -o <patch-file>" << std::endl;
        return 1;
    }
    std::string old_dir = argv[1], new_dir = argv[2], patch_path = argv[4];
    std::string tmpdir = "/tmp/geocoder-diff-" + std::to_string(getpid());
    ensure_dir(tmpdir);

    // Build string remap
    std::cerr << "Building string remap..." << std::endl;
    auto old_strings = read_file(old_dir + "/strings.bin");
    auto new_strings = read_file(new_dir + "/strings.bin");
    auto str_remap = build_string_remap(old_strings, new_strings);

    // Detect strides
    auto detect = [](const std::string& path, std::vector<size_t> cs) -> size_t {
        auto d = read_file(path);
        for (size_t s : cs) if (d.size() % s == 0 && d.size() > 0) return s;
        return cs[0];
    };
    size_t way_stride = detect(old_dir + "/street_ways.bin", {12, 9});
    size_t interp_stride = detect(old_dir + "/interp_ways.bin", {24, 20, 18});
    size_t admin_stride = detect(old_dir + "/admin_polygons.bin", {24, 20, 19});

    // Build patch data (uncompressed, will be zstd-compressed at the end)
    std::vector<char> patch;
    patch.insert(patch.end(), GCPATCH_MAGIC, GCPATCH_MAGIC + 8);
    uint32_t ver = 2, flags = 0; // version 2 = custom format
    wval(patch, &ver, 4); wval(patch, &flags, 4);

    // --- Section: String remap ---
    // When string-level diff is present, the remap is derived by the patch tool.
    // Only include explicit remap if string diff is NOT used.
    // For now, always include empty remap (string diff handles everything).
    {
        uint32_t marker = 0xFFFFFFFE, count = 0;
        wval(patch, &marker, 4); wval(patch, &count, 4);
        std::cerr << "  String remap: derived from string diff (0 explicit entries)" << std::endl;
    }

    // --- Section: Per-file merge sequences (computed in parallel) ---
    // Stored merge sequences for ID remap derivation
    std::unordered_map<uint32_t, MergeSequence> stored_merges;

    // strings.bin: string-level diff (both pools are sorted alphabetically)
    {
        std::vector<std::string> old_strs, new_strs;
        { size_t p = 0; while (p < old_strings.size()) { old_strs.push_back(old_strings.data()+p); p += strlen(old_strings.data()+p)+1; } }
        { size_t p = 0; while (p < new_strings.size()) { new_strs.push_back(new_strings.data()+p); p += strlen(new_strings.data()+p)+1; } }
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
        uint32_t str_diff_marker = 0xFFFFFFF7;
        uint32_t n_added = added_strings.size(), n_deleted = deleted_indices.size();
        wval(patch, &str_diff_marker, 4); wval(patch, &n_added, 4); wval(patch, &n_deleted, 4);
        for (auto& s : added_strings) { patch.insert(patch.end(), s.begin(), s.end()); patch.push_back('\0'); }
        for (auto idx : deleted_indices) wval(patch, &idx, 4);
        std::cerr << "  strings.bin: +" << n_added << " -" << n_deleted << " strings" << std::endl;
    }

    // Build merge sequences for all data files in parallel (4 groups)
    // Group 1: addr_points (independent)
    // Group 2: street_ways → street_nodes (sequential within group)
    // Group 3: interp_ways → interp_nodes (sequential within group)
    // Group 4: admin_polygons → admin_vertices (sequential within group)
    FileMergeResult res_addr, res_ways, res_nodes, res_interp_w, res_interp_n, res_admin_p, res_admin_v;

    // Helper to build parent-aware node merge from a parent way merge
    // count_u32: true for admin_polygons (vertex_count is uint32_t at offset 4)
    //            false for street_ways/interp_ways (node_count is uint8_t at offset 4)
    auto build_child_merge = [](const MergeSequence& parent_seq,
                                 const std::vector<char>& old_parent, const std::vector<char>& new_parent,
                                 const std::vector<char>& old_child, const std::vector<char>& new_child,
                                 size_t parent_stride, bool count_u32) -> MergeSequence {
        MergeSequence seq;
        auto read_count = [&](const char* rec) -> uint32_t {
            if (count_u32) { uint32_t v; memcpy(&v, rec + 4, 4); return v; }
            return static_cast<uint32_t>(static_cast<uint8_t>(rec[4]));
        };
        size_t p_oi = 0, p_ni = 0, ppos = 0;
        while (ppos < parent_seq.data.size()) {
            uint8_t op = static_cast<uint8_t>(parent_seq.data[ppos]); ppos++;
            uint32_t count; memcpy(&count, parent_seq.data.data() + ppos, 4); ppos += 4;
            if (op == OP_MATCH_RUN) {
                for (uint32_t k = 0; k < count; k++) {
                    uint32_t old_off; memcpy(&old_off, old_parent.data() + (p_oi+k)*parent_stride, 4);
                    uint32_t vc = read_count(old_parent.data() + (p_oi+k)*parent_stride);
                    uint32_t new_off; memcpy(&new_off, new_parent.data() + (p_ni+k)*parent_stride, 4);
                    if (vc > 0) {
                        bool match = (old_off + vc) * 8 <= old_child.size() &&
                                    (new_off + vc) * 8 <= new_child.size() &&
                                    memcmp(old_child.data() + old_off*8, new_child.data() + new_off*8, vc*8) == 0;
                        if (match) seq.add_match(vc);
                        else { seq.add_delete(vc); seq.add_insert(new_child.data() + new_off*8, vc, 8); }
                    }
                }
                p_oi += count; p_ni += count;
            } else if (op == OP_INSERT_RUN) {
                for (uint32_t k = 0; k < count; k++) {
                    const char* rec = parent_seq.data.data() + ppos + k * parent_stride;
                    uint32_t off; memcpy(&off, rec, 4);
                    uint32_t vc = read_count(rec);
                    if (vc > 0 && (off + vc) * 8 <= new_child.size())
                        seq.add_insert(new_child.data() + off*8, vc, 8);
                }
                ppos += count * parent_stride;
                p_ni += count;
            } else if (op == OP_DELETE_RUN) {
                for (uint32_t k = 0; k < count; k++) {
                    uint32_t vc = read_count(old_parent.data() + (p_oi+k)*parent_stride);
                    if (vc > 0) seq.add_delete(vc);
                }
                p_oi += count;
            }
        }
        return seq;
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

    // Group 1: addr_points
    std::thread t_addr([&]() {
        double gs = now_ms();
        auto old_data = read_file(old_dir + "/addr_points.bin");
        remap_addr_points(old_data, str_remap);
        auto new_data = read_file(new_dir + "/addr_points.bin");
        auto seq = build_merge_seq(old_data, new_data, 16);
        auto soft = secondary_match_from_merge(seq, old_data, new_data, 16,
            [](const char* rec) -> uint64_t {
                uint32_t hn_id, st_id;
                memcpy(&hn_id, rec + 8, 4); memcpy(&st_id, rec + 12, 4);
                return ((uint64_t)st_id << 32) | hn_id;
            });
        auto id_rm = derive_id_remap_from_merge(seq, old_data.size() / 16, 16);
        for (auto& [o,n] : soft) if (o < id_rm.size()) id_rm[o] = n;
        res_addr = {PatchFileId::ADDR_POINTS, "addr_points.bin", 16,
                    old_data.size(), new_data.size(), std::move(seq), {}, std::move(soft), std::move(id_rm)};
        log_merge(res_addr);
        log_time("  group:addr_points", gs);
    });

    // Group 2: street_ways → street_nodes
    std::thread t_street([&]() {
        double gs = now_ms();
        auto old_w = read_file(old_dir + "/street_ways.bin");
        remap_field(old_w, way_stride, way_stride == 12 ? 8 : 5, str_remap);
        auto new_w = read_file(new_dir + "/street_ways.bin");
        auto old_n = read_file(old_dir + "/street_nodes.bin");
        auto new_n = read_file(new_dir + "/street_nodes.bin");
        // Fixup node_offsets
        size_t wn = old_w.size() / way_stride;
        std::vector<uint32_t> old_offsets(wn);
        for (size_t i = 0; i < wn; i++) memcpy(&old_offsets[i], old_w.data() + i * way_stride, 4);
        fixup_way_offsets(old_w, old_n, new_w, new_n, way_stride);
        std::vector<std::pair<uint32_t,uint32_t>> fixups;
        for (size_t i = 0; i < wn; i++) {
            uint32_t new_off; memcpy(&new_off, old_w.data() + i * way_stride, 4);
            if (new_off != old_offsets[i]) fixups.push_back({static_cast<uint32_t>(i), new_off});
        }
        auto way_seq = build_merge_seq(old_w, new_w, way_stride);
        size_t way_name_off = (way_stride == 12) ? 8 : 5;
        auto soft = secondary_match_from_merge(way_seq, old_w, new_w, way_stride,
            [way_name_off](const char* rec) -> uint64_t {
                uint32_t name_id; memcpy(&name_id, rec + way_name_off, 4);
                uint8_t nc = static_cast<uint8_t>(rec[4]);
                return ((uint64_t)name_id << 8) | nc;
            });
        auto id_rm = derive_id_remap_from_merge(way_seq, old_w.size() / way_stride, way_stride);
        for (auto& [o,n] : soft) if (o < id_rm.size()) id_rm[o] = n;
        res_ways = {PatchFileId::STREET_WAYS, "street_ways.bin", way_stride,
                    old_w.size(), new_w.size(), way_seq, std::move(fixups), std::move(soft), std::move(id_rm)};
        log_merge(res_ways);
        // Now build parent-aware node merge from way merge
        // Restore original node_offsets (before fixup) — avoids re-reading the file
        for (size_t i = 0; i < wn; i++) memcpy(old_w.data() + i * way_stride, &old_offsets[i], 4);
        auto node_seq = build_child_merge(way_seq, old_w, new_w, old_n, new_n, way_stride, false);
        res_nodes = {PatchFileId::STREET_NODES, "street_nodes.bin", 8,
                     old_n.size(), new_n.size(), std::move(node_seq), {}};
        log_merge(res_nodes);
        log_time("  group:streets", gs);
    });

    // Group 3: interp_ways → interp_nodes
    std::thread t_interp([&]() {
        double gs = now_ms();
        auto old_data = read_file(old_dir + "/interp_ways.bin");
        auto new_data = read_file(new_dir + "/interp_ways.bin");
        if (interp_stride == 24) {
            for (size_t i = 0; i + interp_stride <= old_data.size(); i += interp_stride)
                { memset(old_data.data()+i+5, 0, 3); memset(old_data.data()+i+21, 0, 3); }
            for (size_t i = 0; i + interp_stride <= new_data.size(); i += interp_stride)
                { memset(new_data.data()+i+5, 0, 3); memset(new_data.data()+i+21, 0, 3); }
        }
        remap_field(old_data, interp_stride, interp_stride >= 20 ? 8 : 5, str_remap);
        auto old_n = read_file(old_dir + "/interp_nodes.bin");
        auto new_n = read_file(new_dir + "/interp_nodes.bin");
        size_t n = old_data.size() / interp_stride;
        std::vector<uint32_t> old_offsets(n);
        for (size_t i = 0; i < n; i++) memcpy(&old_offsets[i], old_data.data() + i * interp_stride, 4);
        fixup_interp_offsets(old_data, old_n, new_data, new_n, interp_stride);
        std::vector<std::pair<uint32_t,uint32_t>> fixups;
        for (size_t i = 0; i < n; i++) {
            uint32_t new_off; memcpy(&new_off, old_data.data() + i * interp_stride, 4);
            if (new_off != old_offsets[i]) fixups.push_back({static_cast<uint32_t>(i), new_off});
        }
        auto iw_seq = build_merge_seq(old_data, new_data, interp_stride);
        size_t ist_off = (interp_stride >= 20) ? 8 : 5;
        auto soft = secondary_match_from_merge(iw_seq, old_data, new_data, interp_stride,
            [ist_off](const char* rec) -> uint64_t {
                uint32_t street_id, start;
                memcpy(&street_id, rec + ist_off, 4);
                memcpy(&start, rec + ist_off + 4, 4);
                return ((uint64_t)street_id << 32) | start;
            });
        auto id_rm = derive_id_remap_from_merge(iw_seq, old_data.size() / interp_stride, interp_stride);
        for (auto& [o,n] : soft) if (o < id_rm.size()) id_rm[o] = n;
        res_interp_w = {PatchFileId::INTERP_WAYS, "interp_ways.bin", interp_stride,
                        old_data.size(), new_data.size(), iw_seq, std::move(fixups), std::move(soft), std::move(id_rm)};
        log_merge(res_interp_w);
        for (size_t i = 0; i < n; i++) memcpy(old_data.data() + i * interp_stride, &old_offsets[i], 4);
        auto in_seq = build_child_merge(iw_seq, old_data, new_data, old_n, new_n, interp_stride, false);
        res_interp_n = {PatchFileId::INTERP_NODES, "interp_nodes.bin", 8,
                        old_n.size(), new_n.size(), std::move(in_seq), {}};
        log_merge(res_interp_n);
        log_time("  group:interp", gs);
    });

    // Group 4: admin_polygons → admin_vertices
    std::thread t_admin([&]() {
        double gs = now_ms();
        auto old_data = read_file(old_dir + "/admin_polygons.bin");
        auto new_data = read_file(new_dir + "/admin_polygons.bin");
        if (admin_stride == 24) {
            for (size_t i = 0; i + admin_stride <= old_data.size(); i += admin_stride)
                memset(old_data.data() + i + 13, 0, 3);
            for (size_t i = 0; i + admin_stride <= new_data.size(); i += admin_stride)
                memset(new_data.data() + i + 13, 0, 3);
        }
        remap_field(old_data, admin_stride, 8, str_remap);
        auto old_v = read_file(old_dir + "/admin_vertices.bin");
        auto new_v = read_file(new_dir + "/admin_vertices.bin");
        size_t n = old_data.size() / admin_stride;
        std::vector<uint32_t> old_offsets(n);
        for (size_t i = 0; i < n; i++) memcpy(&old_offsets[i], old_data.data() + i * admin_stride, 4);
        fixup_admin_offsets(old_data, old_v, new_data, new_v, admin_stride);
        std::vector<std::pair<uint32_t,uint32_t>> fixups;
        for (size_t i = 0; i < n; i++) {
            uint32_t new_off; memcpy(&new_off, old_data.data() + i * admin_stride, 4);
            if (new_off != old_offsets[i]) fixups.push_back({static_cast<uint32_t>(i), new_off});
        }
        auto ap_seq = build_merge_seq(old_data, new_data, admin_stride);
        auto soft = secondary_match_from_merge(ap_seq, old_data, new_data, admin_stride,
            [](const char* rec) -> uint64_t {
                uint32_t name_id; memcpy(&name_id, rec + 8, 4);
                uint8_t level = static_cast<uint8_t>(rec[12]);
                uint16_t cc; memcpy(&cc, rec + 20, 2);
                return ((uint64_t)name_id << 24) | ((uint64_t)level << 16) | cc;
            });
        res_admin_p = {PatchFileId::ADMIN_POLYGONS, "admin_polygons.bin", admin_stride,
                       old_data.size(), new_data.size(), ap_seq, std::move(fixups), std::move(soft), {}};
        log_merge(res_admin_p);
        for (size_t i = 0; i < n; i++) memcpy(old_data.data() + i * admin_stride, &old_offsets[i], 4);
        auto av_seq = build_child_merge(ap_seq, old_data, new_data, old_v, new_v, admin_stride, true);
        res_admin_v = {PatchFileId::ADMIN_VERTICES, "admin_vertices.bin", 8,
                       old_v.size(), new_v.size(), std::move(av_seq), {}};
        log_merge(res_admin_v);
        log_time("  group:admin", gs);
    });

    // Group 5: cell changes (independent of merges, runs in parallel with them)
    // Uses sorted vectors + set_difference instead of unordered_sets — much faster for 15M cells
    std::vector<uint64_t> g_added, g_removed, a_added, a_removed;
    std::thread t_cells([&]() {
        double gs = now_ms();
        auto diff_cells = [](const std::string& old_path, const std::string& new_path,
                             size_t stride, std::vector<uint64_t>& added, std::vector<uint64_t>& removed) {
            auto old_data = read_file(old_path);
            auto new_data = read_file(new_path);
            std::vector<uint64_t> old_ids(old_data.size() / stride), new_ids(new_data.size() / stride);
            for (size_t i = 0; i < old_ids.size(); i++) memcpy(&old_ids[i], old_data.data()+i*stride, 8);
            for (size_t i = 0; i < new_ids.size(); i++) memcpy(&new_ids[i], new_data.data()+i*stride, 8);
            std::sort(old_ids.begin(), old_ids.end());
            std::sort(new_ids.begin(), new_ids.end());
            std::set_difference(new_ids.begin(), new_ids.end(), old_ids.begin(), old_ids.end(), std::back_inserter(added));
            std::set_difference(old_ids.begin(), old_ids.end(), new_ids.begin(), new_ids.end(), std::back_inserter(removed));
        };
        diff_cells(old_dir + "/geo_cells.bin", new_dir + "/geo_cells.bin", 20, g_added, g_removed);
        diff_cells(old_dir + "/admin_cells.bin", new_dir + "/admin_cells.bin", 12, a_added, a_removed);
        log_time("  group:cell_changes", gs);
    });

    t_addr.join(); t_street.join(); t_interp.join(); t_admin.join(); t_cells.join();
    log_time("All merge sequences + cell changes built", merge_start);

    // Serialize merge results to patch in canonical order, then free heavy data
    serialize_merge(patch, res_addr);
    serialize_merge(patch, res_ways);
    serialize_merge(patch, res_nodes);
    { std::vector<char>().swap(res_nodes.seq.data); } // free ~264 MiB node merge sequence
    serialize_merge(patch, res_interp_w);
    serialize_merge(patch, res_interp_n);
    { std::vector<char>().swap(res_interp_n.seq.data); }
    serialize_merge(patch, res_admin_p);
    serialize_merge(patch, res_admin_v);
    { std::vector<char>().swap(res_admin_v.seq.data); }
    double t0 = now_ms();

    // ID remaps were pre-computed in the parallel merge groups.
    // Cell changes were computed in parallel with merge groups.
    // Just need to compute admin remap (small, kept as hash map).
    auto& w_rm_v = res_ways.id_remap;
    auto& a_rm_v = res_addr.id_remap;
    auto& i_rm_v = res_interp_w.id_remap;
    std::unordered_map<uint32_t,uint32_t> ad_rm_d;
    {
        auto vec = derive_id_remap_from_merge(
            res_admin_p.seq,
            res_admin_p.old_size / admin_stride, admin_stride);
        ad_rm_d.reserve(vec.size());
        for (uint32_t i = 0; i < vec.size(); i++)
            if (vec[i] != 0xFFFFFFFF) ad_rm_d[i] = vec[i];
        for (auto& [o,n] : res_admin_p.secondary_matches) ad_rm_d[o] = n;
    }
    log_time("Admin remap", t0);

    auto old_geo = read_file(old_dir + "/geo_cells.bin");

    // Emit cell changes
    { uint32_t marker = CELL_CHANGES_GEO_MARKER, na = g_added.size(), nr = g_removed.size();
      wval(patch, &marker, 4); wval(patch, &na, 4); wval(patch, &nr, 4);
      for (auto c : g_added) wval(patch, &c, 8); for (auto c : g_removed) wval(patch, &c, 8);
      std::cerr << "  Geo cell changes: +" << na << " -" << nr << std::endl; }
    { uint32_t marker = CELL_CHANGES_ADMIN_MARKER, na = a_added.size(), nr = a_removed.size();
      wval(patch, &marker, 4); wval(patch, &na, 4); wval(patch, &nr, 4);
      for (auto c : a_added) wval(patch, &c, 8); for (auto c : a_removed) wval(patch, &c, 8);
      std::cerr << "  Admin cell changes: +" << na << " -" << nr << std::endl; }

    // Emit secondary remap section (from merge results, no re-reads)
    std::cerr << "  Secondary matches: ways=" << res_ways.secondary_matches.size()
              << " addr=" << res_addr.secondary_matches.size()
              << " interp=" << res_interp_w.secondary_matches.size()
              << " admin=" << res_admin_p.secondary_matches.size() << std::endl;
    { uint32_t marker = SECONDARY_REMAP_MARKER; wval(patch, &marker, 4);
      auto emit_remap = [&](PatchFileId fid, const std::unordered_map<uint32_t,uint32_t>& rm) {
          uint32_t file = static_cast<uint32_t>(fid), count = rm.size();
          wval(patch, &file, 4); wval(patch, &count, 4);
          for (auto& [o,n] : rm) { wval(patch, &o, 4); wval(patch, &n, 4); }
      };
      uint32_t n_files = 4; wval(patch, &n_files, 4);
      emit_remap(PatchFileId::STREET_WAYS, res_ways.secondary_matches);
      emit_remap(PatchFileId::ADDR_POINTS, res_addr.secondary_matches);
      emit_remap(PatchFileId::INTERP_WAYS, res_interp_w.secondary_matches);
      emit_remap(PatchFileId::ADMIN_POLYGONS, res_admin_p.secondary_matches);
    }

    // --- Entry rebuild + corrections ---
    // Use the proven rebuild_geo_from_remap_vec approach, then compare sequentially.
    // Sequential corrections avoid OOM on planet (parallel used too much memory).
    double t1 = now_ms();

    // Free merge sequences (already serialized to patch)
    { std::vector<char>().swap(res_addr.seq.data); std::vector<char>().swap(res_ways.seq.data);
      std::vector<char>().swap(res_interp_w.seq.data); std::vector<char>().swap(res_admin_p.seq.data);
      std::vector<char>().swap(res_nodes.seq.data); std::vector<char>().swap(res_interp_n.seq.data);
      std::vector<char>().swap(res_admin_v.seq.data); }

    auto old_se = read_file(old_dir + "/street_entries.bin");
    auto old_ae = read_file(old_dir + "/addr_entries.bin");
    auto old_ie = read_file(old_dir + "/interp_entries.bin");
    auto derived = rebuild_geo_from_remap_vec(old_geo, old_se, old_ae, old_ie,
                                               w_rm_v, a_rm_v, i_rm_v, g_added, g_removed);
    // Free inputs no longer needed
    { std::vector<uint32_t>().swap(w_rm_v); std::vector<uint32_t>().swap(a_rm_v);
      std::vector<uint32_t>().swap(i_rm_v); }
    { std::vector<char>().swap(old_se); std::vector<char>().swap(old_ae);
      std::vector<char>().swap(old_ie); } // keep old_geo for flag corrections
    log_time("Geo entry rebuild", t1);

    // Compare derived entries with new entries, one file at a time (sequential to save memory)
    double t2 = now_ms();
    auto new_geo_data = read_file(new_dir + "/geo_cells.bin");
    auto parse_ids = [](const std::vector<char>& data, uint32_t off) -> std::vector<uint32_t> {
        if (off == 0xFFFFFFFF || off + 2 > data.size()) return {};
        uint16_t count; memcpy(&count, data.data() + off, 2);
        if (off + 2 + count * 4 > data.size()) return {};
        std::vector<uint32_t> ids(count);
        memcpy(ids.data(), data.data() + off + 2, count * 4);
        return ids;
    };
    // Pre-parse new entries by cell (shared across parallel correction threads)
    size_t d_nc = derived.geo_cells_data.size() / 20;
    size_t n_nc = new_geo_data.size() / 20;
    auto new_se_data = read_file(new_dir + "/street_entries.bin");
    auto new_ae_data = read_file(new_dir + "/addr_entries.bin");
    auto new_ie_data = read_file(new_dir + "/interp_entries.bin");

    // Build shared new cell maps: cell_id → (street_ids, addr_ids, interp_ids)
    struct NewCellEntries { std::vector<uint32_t> streets, addrs, interps; };
    std::unordered_map<uint64_t, NewCellEntries> new_cell_map;
    new_cell_map.reserve(n_nc);
    for (size_t i = 0; i < n_nc; i++) {
        uint64_t cid; memcpy(&cid, new_geo_data.data()+i*20, 8);
        uint32_t so, ao, io;
        memcpy(&so, new_geo_data.data()+i*20+8, 4);
        memcpy(&ao, new_geo_data.data()+i*20+12, 4);
        memcpy(&io, new_geo_data.data()+i*20+16, 4);
        new_cell_map[cid] = {parse_ids(new_se_data, so), parse_ids(new_ae_data, ao), parse_ids(new_ie_data, io)};
    }
    { std::vector<char>().swap(new_se_data); std::vector<char>().swap(new_ae_data);
      std::vector<char>().swap(new_ie_data); }

    // Build shared derived cell set
    std::unordered_set<uint64_t> d_set;
    d_set.reserve(d_nc);
    for (size_t i = 0; i < d_nc; i++) {
        uint64_t cid; memcpy(&cid, derived.geo_cells_data.data()+i*20, 8);
        d_set.insert(cid);
    }
    log_time("Pre-parse new entries", t2);

    // Parallel correction: each thread reads derived entries + shared new_cell_map
    auto compute_geo_correction = [&](PatchFileId fid, const std::string& fname,
                                       const std::vector<char>& d_entries,
                                       size_t d_off_pos,
                                       auto get_new_ids) -> std::vector<char> {
        std::vector<char> buf; buf.resize(12, 0); uint32_t dc = 0;
        for (size_t i = 0; i < d_nc; i++) {
            uint64_t cid; memcpy(&cid, derived.geo_cells_data.data()+i*20, 8);
            uint32_t off; memcpy(&off, derived.geo_cells_data.data()+i*20+d_off_pos, 4);
            auto d_ids = parse_ids(d_entries, off);
            auto nit = new_cell_map.find(cid);
            const std::vector<uint32_t>* n_ids = nullptr;
            if (nit != new_cell_map.end()) n_ids = &get_new_ids(nit->second);
            bool differs = !n_ids ? !d_ids.empty() : (d_ids.size() != n_ids->size()) ||
                          (!d_ids.empty() && memcmp(d_ids.data(), n_ids->data(), d_ids.size()*4) != 0);
            if (differs) {
                wval(buf, &cid, 8); uint16_t c = n_ids ? n_ids->size() : 0; wval(buf, &c, 2);
                if (n_ids && !n_ids->empty()) buf.insert(buf.end(), (const char*)n_ids->data(), (const char*)n_ids->data()+n_ids->size()*4);
                dc++;
            }
        }
        for (auto& [cid, ne] : new_cell_map) {
            if (!d_set.count(cid)) {
                auto& ids = get_new_ids(ne);
                if (!ids.empty()) {
                    wval(buf, &cid, 8); uint16_t c = ids.size(); wval(buf, &c, 2);
                    buf.insert(buf.end(), (const char*)ids.data(), (const char*)ids.data()+ids.size()*4);
                    dc++;
                }
            }
        }
        uint32_t marker = ENTRY_CORRECTION_MARKER, file = static_cast<uint32_t>(fid);
        memcpy(buf.data(), &marker, 4); memcpy(buf.data()+4, &file, 4); memcpy(buf.data()+8, &dc, 4);
        { std::lock_guard<std::mutex> lock(log_mutex);
          std::cerr << "  " << fname << ": " << dc << " cell corrections (" << buf.size()-12 << " bytes)" << std::endl; }
        return buf;
    };

    std::vector<char> corr_se, corr_ae, corr_ie;
    {
        std::thread tc1([&]() { corr_se = compute_geo_correction(PatchFileId::STREET_ENTRIES, "street_entries.bin",
            derived.street_entries_data, 8, [](const NewCellEntries& e) -> const std::vector<uint32_t>& { return e.streets; }); });
        std::thread tc2([&]() { corr_ae = compute_geo_correction(PatchFileId::ADDR_ENTRIES, "addr_entries.bin",
            derived.addr_entries_data, 12, [](const NewCellEntries& e) -> const std::vector<uint32_t>& { return e.addrs; }); });
        std::thread tc3([&]() { corr_ie = compute_geo_correction(PatchFileId::INTERP_ENTRIES, "interp_entries.bin",
            derived.interp_entries_data, 16, [](const NewCellEntries& e) -> const std::vector<uint32_t>& { return e.interps; }); });
        tc1.join(); tc2.join(); tc3.join();
    }
    patch.insert(patch.end(), corr_se.begin(), corr_se.end()); { std::vector<char>().swap(corr_se); }
    patch.insert(patch.end(), corr_ae.begin(), corr_ae.end()); { std::vector<char>().swap(corr_ae); }
    patch.insert(patch.end(), corr_ie.begin(), corr_ie.end()); { std::vector<char>().swap(corr_ie); }
    derived = RebuiltGeo{}; new_cell_map.clear(); d_set.clear();

    // Admin corrections
    {
        auto old_admc = read_file(old_dir + "/admin_cells.bin");
        auto old_adme = read_file(old_dir + "/admin_entries.bin");
        auto admin_derived = rebuild_admin_from_remap(old_admc, old_adme, ad_rm_d, a_added, a_removed);
        auto new_admc = read_file(new_dir + "/admin_cells.bin");
        auto new_adme = read_file(new_dir + "/admin_entries.bin");
        auto parse_admin = [&](const std::vector<char>& cells, const std::vector<char>& entries)
            -> std::unordered_map<uint64_t, std::vector<uint32_t>> {
            std::unordered_map<uint64_t, std::vector<uint32_t>> m;
            for (size_t i = 0; i < cells.size() / 12; i++) {
                uint64_t cid; memcpy(&cid, cells.data()+i*12, 8);
                uint32_t off; memcpy(&off, cells.data()+i*12+8, 4);
                m[cid] = parse_ids(entries, off);
            }
            return m;
        };
        auto dm = parse_admin(admin_derived.admin_cells_data, admin_derived.admin_entries_data);
        auto nm = parse_admin(new_admc, new_adme);
        std::vector<char> buf; buf.resize(12, 0); uint32_t dc = 0;
        for (auto& [cid, nids] : nm) {
            auto it = dm.find(cid); auto* dids = it != dm.end() ? &it->second : nullptr;
            bool differs = !dids ? !nids.empty() : (dids->size() != nids.size()) ||
                          (!dids->empty() && memcmp(dids->data(), nids.data(), dids->size()*4) != 0);
            if (differs) { wval(buf, &cid, 8); uint16_t c = nids.size(); wval(buf, &c, 2);
                if (!nids.empty()) buf.insert(buf.end(), (const char*)nids.data(), (const char*)nids.data()+nids.size()*4); dc++; }
        }
        for (auto& [cid, dids] : dm) { if (!nm.count(cid) && !dids.empty()) { wval(buf, &cid, 8); uint16_t c = 0; wval(buf, &c, 2); dc++; } }
        uint32_t marker = ENTRY_CORRECTION_MARKER, file = static_cast<uint32_t>(PatchFileId::ADMIN_ENTRIES);
        memcpy(buf.data(), &marker, 4); memcpy(buf.data()+4, &file, 4); memcpy(buf.data()+8, &dc, 4);
        patch.insert(patch.end(), buf.begin(), buf.end());
        std::cerr << "  admin_entries.bin: " << dc << " cell corrections (" << buf.size()-12 << " bytes)" << std::endl;
    }
    log_time("Entry corrections", t2);

    // Cell flag corrections
    {
        auto new_geo_data = read_file(new_dir + "/geo_cells.bin");
        std::unordered_map<uint64_t, uint8_t> old_cell_flags;
        for (size_t i = 0; i < old_geo.size() / 20; i++) {
            uint64_t cid; memcpy(&cid, old_geo.data()+i*20, 8);
            uint32_t s, a, ip; memcpy(&s, old_geo.data()+i*20+8, 4);
            memcpy(&a, old_geo.data()+i*20+12, 4); memcpy(&ip, old_geo.data()+i*20+16, 4);
            old_cell_flags[cid] = (s != 0xFFFFFFFF ? 1 : 0) | (a != 0xFFFFFFFF ? 2 : 0) | (ip != 0xFFFFFFFF ? 4 : 0);
        }
        std::vector<std::pair<uint64_t, uint8_t>> flag_corrections;
        for (size_t i = 0; i < new_geo_data.size() / 20; i++) {
            uint64_t cid; memcpy(&cid, new_geo_data.data()+i*20, 8);
            uint32_t s, a, ip; memcpy(&s, new_geo_data.data()+i*20+8, 4);
            memcpy(&a, new_geo_data.data()+i*20+12, 4); memcpy(&ip, new_geo_data.data()+i*20+16, 4);
            uint8_t nf = (s != 0xFFFFFFFF ? 1 : 0) | (a != 0xFFFFFFFF ? 2 : 0) | (ip != 0xFFFFFFFF ? 4 : 0);
            auto it = old_cell_flags.find(cid);
            if (nf != (it != old_cell_flags.end() ? it->second : 0))
                flag_corrections.push_back({cid, nf});
        }
        uint32_t fm = CELL_FLAGS_MARKER, fc = flag_corrections.size();
        wval(patch, &fm, 4); wval(patch, &fc, 4);
        for (auto& [cid, flags] : flag_corrections) { wval(patch, &cid, 8); wval(patch, &flags, 1); }
        std::cerr << "  Cell flag corrections: " << fc << " cells" << std::endl;
    }

    // End marker
    uint32_t end_marker = 0xFFFFFFFF;
    wval(patch, &end_marker, 4);

    std::cerr << "\nUncompressed patch: " << patch.size() << " bytes ("
              << patch.size() / 1024 / 1024 << " MiB)" << std::endl;

    // Compress whole patch with zstd for transport
    {
        double tc = now_ms();
        std::string raw_path = tmpdir + "/patch.raw";
        write_file(raw_path, patch);
        std::string cmd = "zstd -19 -T0 '" + raw_path + "' -o '" + patch_path + "' -f --quiet 2>/dev/null";
        system(cmd.c_str());
        auto compressed = read_file(patch_path);
        std::cerr << "Compressed patch: " << compressed.size() << " bytes ("
                  << compressed.size() / 1024 / 1024 << " MiB)" << std::endl;
        log_time("zstd -19 -T0 compression", tc);
        remove(raw_path.c_str());
    }

    std::string rm_cmd = "rm -rf '" + tmpdir + "'";
    system(rm_cmd.c_str());
    return 0;
}
