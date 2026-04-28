// geocoder-patch v4: Low-memory streaming patch application.
//
// Key design: mmap old files (zero copy), stream output via fwrite (never accumulate),
// process entry pipeline cell-by-cell. Target: <1 GiB peak RSS for planet.
//
// Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir>

#include <algorithm>
#include <array>
#include <cstdint>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <malloc.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <vector>

#include "patch_format.h"

enum MergeOp : uint8_t { OP_MATCH_RUN = 0, OP_INSERT_RUN = 1, OP_DELETE_RUN = 2 };

// --- Helpers ---
static double now_ms() {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}
static size_t get_rss_mb() {
    FILE* f = fopen("/proc/self/statm", "r");
    if (!f) return 0;
    size_t dummy, rss; fscanf(f, "%zu %zu", &dummy, &rss); fclose(f);
    return rss * 4096 / (1024*1024);
}
static void log_phase(const char* label, double start) {
    std::cerr << "  [" << std::fixed << std::setprecision(1) << (now_ms() - start) / 1000.0
              << "s, " << get_rss_mb() << " MiB] " << label << std::endl;
}

// MappedFile + mmap_file + unmap_file are now in patch_format.h

// Detect stride from file size
static size_t detect_stride(const std::string& path, std::initializer_list<size_t> candidates) {
    struct stat st; if (stat(path.c_str(), &st) != 0) return *candidates.begin();
    for (size_t s : candidates) if (st.st_size % s == 0 && st.st_size > 0) return s;
    return *candidates.begin();
}

int main(int argc, char* argv[]) {
    if (argc < 5 || std::string(argv[3]) != "-o") {
        std::cerr << "Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir>" << std::endl;
        return 1;
    }
    std::string cur_dir = argv[1], patch_path = argv[2], out_dir = argv[4];
    ensure_dir(out_dir);
    std::string tmpdir = "/tmp/geocoder-patch-" + std::to_string(getpid());
    ensure_dir(tmpdir);
    double t_start = now_ms();

    // --- Phase 1: Decompress + mmap patch ---
    std::string raw_path = tmpdir + "/patch.raw";
    {
        std::string cmd = "zstd -d '" + patch_path + "' -o '" + raw_path + "' -f --quiet 2>/dev/null";
        if (system(cmd.c_str()) != 0) { std::cerr << "Failed to decompress" << std::endl; return 1; }
    }
    MappedFile patch_map = mmap_file(raw_path);
    if (!patch_map.data) { std::cerr << "Failed to mmap patch" << std::endl; return 1; }
    // Don't remove temp file yet (mmap needs backing file for page eviction under memory pressure)
    // madvise random — we access sections non-sequentially but release pages after each section
    madvise(const_cast<char*>(patch_map.data), patch_map.size, MADV_RANDOM);
    const char* P = patch_map.data;
    size_t patch_size = patch_map.size;
    std::cerr << "Patch: " << patch_size << " bytes" << std::endl;
    log_phase("Decompress", t_start);

    size_t pos = 0;
    auto ru32 = [&]() -> uint32_t { uint32_t v; memcpy(&v, P+pos, 4); pos += 4; return v; };
    auto ru64 = [&]() -> uint64_t { uint64_t v; memcpy(&v, P+pos, 8); pos += 8; return v; };

    // Header
    if (memcmp(P, GCPATCH_MAGIC, 8) != 0) { std::cerr << "Bad magic" << std::endl; return 1; }
    pos = 8;
    uint32_t ver = ru32(); if (ver != 2) { std::cerr << "Bad version" << std::endl; return 1; }
    ru32(); // flags

    // --- Phase 2: String rebuild ---
    // Sorted vector remap: (old_offset, new_offset) pairs sorted by old_offset.
    // Offsets are global across all tiers (tier 0 occupies [0, base[1]),
    // tier N at [base[N], base[N+1])) so a single remap covers everything.
    static const char* kStrTierFilenames[5] = {
        "strings_core.bin", "strings_street.bin", "strings_addr.bin",
        "strings_postcode.bin", "strings_poi.bin"
    };
    static const char* kStrTierNames[5] = {"core", "street", "addr", "postcode", "poi"};
    std::vector<std::pair<uint32_t, uint32_t>> str_remap_vec;
    std::array<uint32_t, 6> new_tier_bases{};
    {
        uint32_t marker = ru32();
        if (marker == 0xFFFFFFF6) {
            // Tiered format — 5 independent per-tier diffs.  For each tier:
            //   1. Read its n_added/n_deleted block from the patch.
            //   2. Merge-write the old tier file + added (minus deleted)
            //      into the new tier file.
            //   3. Walk old/new to extend str_remap_vec using global offsets.
            uint32_t old_global_base = 0;
            uint32_t new_global_base = 0;
            uint32_t total_added = 0, total_deleted = 0;
            for (int t = 0; t < 5; t++) {
                uint32_t n_added = ru32(), n_deleted = ru32();
                total_added += n_added; total_deleted += n_deleted;
                std::vector<std::string> added;
                for (uint32_t i = 0; i < n_added; i++) { const char* s = P+pos; added.push_back(s); pos += strlen(s)+1; }
                std::vector<uint32_t> del_idx(n_deleted);
                for (uint32_t i = 0; i < n_deleted; i++) del_idx[i] = ru32();
                std::unordered_set<uint32_t> del_set(del_idx.begin(), del_idx.end());

                // Fallback to ../full/ when applying a patch on a per-
                // variant subdir (quality, poi, admin-minimal). Strings
                // live under <region>/full/, so the str_remap_vec built
                // here has to walk those instead of the (missing)
                // strings_*.bin in cur_dir.  Without this, the str_remap
                // is empty and admin_polygons.bin name_id MATCH replays
                // keep the old name_id while the new build expects the
                // re-tiered offset → byte-mismatch on verify.
                MappedFile old_pool = mmap_file(cur_dir + "/" + kStrTierFilenames[t]);
                if (old_pool.size == 0 && old_pool.data == nullptr) {
                    old_pool = mmap_file(cur_dir + "/../full/" + kStrTierFilenames[t]);
                }
                if (old_pool.size == 0 && old_pool.data == nullptr) {
                    old_pool = mmap_file(cur_dir + "/../../full/" + kStrTierFilenames[t]);
                }
                // Phase A: write new tier file via alphabetical merge.
                {
                    FILE* fp = fopen((out_dir + "/" + kStrTierFilenames[t]).c_str(), "wb");
                    size_t sp = 0; uint32_t idx = 0; size_t ai = 0;
                    std::sort(added.begin(), added.end());
                    while (sp < old_pool.size || ai < added.size()) {
                        const char* old_s = nullptr;
                        while (sp < old_pool.size) {
                            if (!del_set.count(idx)) { old_s = old_pool.data + sp; break; }
                            sp += strlen(old_pool.data + sp) + 1; idx++;
                        }
                        const char* add_s = ai < added.size() ? added[ai].c_str() : nullptr;
                        if (old_s && add_s) {
                            int c = strcmp(old_s, add_s);
                            if (c <= 0) {
                                size_t l = strlen(old_s) + 1; fwrite(old_s, 1, l, fp);
                                sp += l; idx++;
                                if (c == 0) ai++;
                            } else {
                                size_t l = added[ai].size() + 1; fwrite(add_s, 1, l, fp);
                                ai++;
                            }
                        } else if (old_s) {
                            size_t l = strlen(old_s) + 1; fwrite(old_s, 1, l, fp);
                            sp += l; idx++;
                        } else if (add_s) {
                            size_t l = added[ai].size() + 1; fwrite(add_s, 1, l, fp);
                            ai++;
                        } else break;
                    }
                    fclose(fp);
                }
                { std::vector<std::string>().swap(added); std::unordered_set<uint32_t>().swap(del_set);
                  std::vector<uint32_t>().swap(del_idx); }

                // Phase B: extend remap by merge-walking old vs new in this tier.
                MappedFile new_pool = mmap_file(out_dir + "/" + kStrTierFilenames[t]);
                {
                    size_t old_pos = 0, new_pos = 0;
                    while (old_pos < old_pool.size && new_pos < new_pool.size) {
                        const char* os = old_pool.data + old_pos;
                        const char* ns = new_pool.data + new_pos;
                        int c = strcmp(os, ns);
                        if (c == 0) {
                            uint32_t old_global = old_global_base + static_cast<uint32_t>(old_pos);
                            uint32_t new_global = new_global_base + static_cast<uint32_t>(new_pos);
                            if (old_global != new_global)
                                str_remap_vec.push_back({old_global, new_global});
                            old_pos += strlen(os) + 1;
                            new_pos += strlen(ns) + 1;
                        } else if (c < 0) {
                            old_pos += strlen(os) + 1;
                        } else {
                            new_pos += strlen(ns) + 1;
                        }
                    }
                }
                old_global_base += static_cast<uint32_t>(old_pool.size);
                new_global_base += static_cast<uint32_t>(new_pool.size);
                new_tier_bases[t + 1] = new_global_base;
                unmap_file(old_pool);
                unmap_file(new_pool);
            }
            malloc_trim(0);
            std::sort(str_remap_vec.begin(), str_remap_vec.end());
            std::cerr << "  Strings (tiered): +" << total_added << " -" << total_deleted
                      << ", " << str_remap_vec.size() << " remapped ("
                      << str_remap_vec.size() * 8 / 1024 / 1024 << " MiB)" << std::endl;

            // Write strings_layout.json (new tier bases).
            {
                std::ofstream f(out_dir + "/strings_layout.json");
                f << "{\n  \"tiers\": [\n";
                for (int t = 0; t < 5; t++) {
                    f << "    {\"name\": \"" << kStrTierNames[t]
                      << "\", \"file\": \"" << kStrTierFilenames[t]
                      << "\", \"start\": " << new_tier_bases[t]
                      << ", \"end\": " << new_tier_bases[t + 1] << "}";
                    if (t < 4) f << ",";
                    f << "\n";
                }
                f << "  ]\n}\n";
            }

            marker = ru32();
        }
        if (marker == 0xFFFFFFFE) {
            uint32_t c = ru32();
            for (uint32_t i = 0; i < c; i++) { uint32_t a = ru32(), b = ru32(); str_remap_vec.push_back({a, b}); }
            std::sort(str_remap_vec.begin(), str_remap_vec.end());
        } else pos -= 4;
    }
    // Release patch pages read so far (string section)
    madvise(const_cast<char*>(patch_map.data), pos, MADV_DONTNEED);
    log_phase("Strings", t_start);

    // Detect strides
    size_t way_stride = detect_stride(cur_dir + "/street_ways.bin", {12, 9});
    size_t interp_stride = detect_stride(cur_dir + "/interp_ways.bin", {24, 20, 18});
    size_t admin_stride = detect_stride(cur_dir + "/admin_polygons.bin", {24, 20, 19});

    // String remap lookup via sorted vector + binary search
    auto str_remap_lookup = [&](uint32_t old_off) -> uint32_t {
        auto it = std::lower_bound(str_remap_vec.begin(), str_remap_vec.end(),
            std::make_pair(old_off, (uint32_t)0));
        if (it != str_remap_vec.end() && it->first == old_off) return it->second;
        return old_off; // no remap
    };

    // --- Phase 3: Merge replays (streaming output) ---
    // ID remaps: file_id → vector<uint32_t> where remap[old_idx] = new_idx
    std::unordered_map<uint32_t, std::vector<uint32_t>> id_remaps;
    std::vector<uint64_t> geo_added, geo_removed, admin_added, admin_removed, poi_added, poi_removed, place_added, place_removed;
    std::unordered_map<uint64_t, uint8_t> flag_corrections;
    struct CellCorr { uint64_t cell_id; std::vector<uint32_t> ids; };
    std::unordered_map<uint32_t, std::vector<CellCorr>> entry_corrections;

    while (pos < patch_size) {
        uint32_t file_id = ru32();
        if (file_id == 0xFFFFFFFF) break;

        // --- Metadata sections ---
        if (file_id == 0xFFFFFFF7) {
            // String diff in main loop — same memory-efficient approach as Phase 2
            uint32_t n_added = ru32(), n_deleted = ru32();
            std::vector<std::string> added;
            for (uint32_t i = 0; i < n_added; i++) { const char* s = P+pos; added.push_back(s); pos += strlen(s)+1; }
            std::vector<uint32_t> del_idx(n_deleted);
            for (uint32_t i = 0; i < n_deleted; i++) del_idx[i] = ru32();
            std::unordered_set<uint32_t> del_set(del_idx.begin(), del_idx.end());
            MappedFile old_pool = mmap_file(cur_dir + "/strings.bin");
            struct OS { uint32_t idx; uint32_t off; const char* str; };
            std::vector<OS> old_strs;
            { size_t sp = 0; uint32_t ix = 0;
              while (sp < old_pool.size) { old_strs.push_back({ix++, (uint32_t)sp, old_pool.data+sp}); sp += strlen(old_pool.data+sp)+1; }
            }
            std::vector<const char*> merged;
            for (auto& os : old_strs) if (!del_set.count(os.idx)) merged.push_back(os.str);
            for (auto& s : added) merged.push_back(s.c_str());
            std::sort(merged.begin(), merged.end(), [](const char* a, const char* b) { return strcmp(a,b) < 0; });
            FILE* fp = fopen((out_dir + "/strings.bin").c_str(), "wb");
            uint32_t wpos = 0;
            for (const char* s : merged) { size_t l = strlen(s)+1; fwrite(s, 1, l, fp); wpos += l; }
            fclose(fp);
            // Build remap via merge-walk
            MappedFile new_pool = mmap_file(out_dir + "/strings.bin");
            std::vector<std::pair<const char*, uint32_t>> new_strs;
            { size_t sp = 0;
              while (sp < new_pool.size) { new_strs.push_back({new_pool.data+sp, (uint32_t)sp}); sp += strlen(new_pool.data+sp)+1; }
            }
            size_t oi2 = 0, ni2 = 0;
            while (oi2 < old_strs.size() && ni2 < new_strs.size()) {
                int c = strcmp(old_strs[oi2].str, new_strs[ni2].first);
                if (c == 0) {
                    if (old_strs[oi2].off != new_strs[ni2].second)
                        str_remap_vec.push_back({old_strs[oi2].off, new_strs[ni2].second});
                    oi2++; ni2++;
                } else if (c < 0) { oi2++; } else { ni2++; }
            }
            std::sort(str_remap_vec.begin(), str_remap_vec.end());
            unmap_file(old_pool); unmap_file(new_pool);
            std::cerr << "  Strings (loop): +" << n_added << " -" << n_deleted << " → " << merged.size()
                      << ", " << str_remap_vec.size() << " remapped" << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_GEO_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            geo_added.resize(na); geo_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&geo_added[i], P+pos, 8); pos += 8; }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&geo_removed[i], P+pos, 8); pos += 8; }
            std::cerr << "  Geo cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_ADMIN_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            admin_added.resize(na); admin_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&admin_added[i], P+pos, 8); pos += 8; }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&admin_removed[i], P+pos, 8); pos += 8; }
            std::cerr << "  Admin cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_POI_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            poi_added.resize(na); poi_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&poi_added[i], P+pos, 8); pos += 8; }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&poi_removed[i], P+pos, 8); pos += 8; }
            std::cerr << "  POI cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_PLACE_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            place_added.resize(na); place_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&place_added[i], P+pos, 8); pos += 8; }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&place_removed[i], P+pos, 8); pos += 8; }
            std::cerr << "  Place cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_FLAGS_MARKER) {
            uint32_t c = ru32();
            for (uint32_t i = 0; i < c; i++) { uint64_t cid; memcpy(&cid, P+pos, 8); pos += 8; flag_corrections[cid] = P[pos++]; }
            std::cerr << "  Flag corrections: " << c << std::endl;
            continue;
        }
        if (file_id == SECONDARY_REMAP_MARKER) {
            // Apply secondary remaps directly to the temp remap files on disk
            uint32_t nf = ru32();
            for (uint32_t f = 0; f < nf; f++) {
                uint32_t fid = ru32(), np = ru32();
                std::string remap_path = tmpdir + "/remap_" + std::to_string(fid) + ".bin";
                // Open remap file for read-write
                int rfd = open(remap_path.c_str(), O_RDWR);
                if (rfd >= 0) {
                    struct stat st; fstat(rfd, &st);
                    size_t remap_size = st.st_size;
                    uint32_t* remap_data = static_cast<uint32_t*>(
                        mmap(nullptr, remap_size, PROT_READ | PROT_WRITE, MAP_SHARED, rfd, 0));
                    size_t remap_count = remap_size / 4;
                    for (uint32_t i = 0; i < np; i++) {
                        uint32_t o = ru32(), n = ru32();
                        if (o < remap_count) remap_data[o] = n + 1; // +1 encoding
                    }
                    munmap(remap_data, remap_size);
                    close(rfd);
                } else {
                    // Skip data if file doesn't exist
                    for (uint32_t i = 0; i < np; i++) { ru32(); ru32(); }
                }
                std::cerr << "  Secondary remap " << fid << ": " << np << " pairs" << std::endl;
            }
            continue;
        }
        if (file_id == ENTRY_CORRECTION_MARKER) {
            uint32_t fid = ru32(), c = ru32();
            auto& list = entry_corrections[fid];
            for (uint32_t i = 0; i < c; i++) {
                uint64_t cid; memcpy(&cid, P+pos, 8); pos += 8;
                uint16_t ec; memcpy(&ec, P+pos, 2); pos += 2;
                std::vector<uint32_t> ids(ec);
                if (ec > 0) { memcpy(ids.data(), P+pos, ec*4); pos += ec*4; }
                list.push_back({cid, std::move(ids)});
            }
            std::cerr << "  Entry corrections " << fid << ": " << c << std::endl;
            continue;
        }

        // --- Merge sequence replay (streaming) ---
        uint32_t stride = ru32();
        uint64_t old_size = ru64(), new_size = ru64();
        if (file_id >= (uint32_t)PatchFileId::COUNT) { std::cerr << "Unknown file " << file_id << std::endl; break; }
        const char* fname = patch_file_names[file_id];

        if (stride == 0) {
            // Full replacement — write directly from patch mmap
            uint32_t nf = ru32(); (void)nf; uint64_t ds = ru64();
            FILE* fp = fopen((out_dir + "/" + fname).c_str(), "wb");
            fwrite(P+pos, 1, ds, fp); fclose(fp);
            pos += ds;
            std::cerr << "  " << fname << ": full replace " << ds << " bytes" << std::endl;
            continue;
        }
        if (stride == 0xFE) { uint32_t nf = ru32(); (void)nf; uint64_t ds = ru64(); pos += ds; continue; }

        size_t actual_stride = stride;
        if (file_id == (uint32_t)PatchFileId::STREET_WAYS) actual_stride = way_stride;
        else if (file_id == (uint32_t)PatchFileId::INTERP_WAYS) actual_stride = interp_stride;
        else if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS) actual_stride = admin_stride;

        // Determine if this file needs in-memory modifications
        bool needs_remap = (file_id == (uint32_t)PatchFileId::ADDR_POINTS ||
                            file_id == (uint32_t)PatchFileId::STREET_WAYS ||
                            file_id == (uint32_t)PatchFileId::INTERP_WAYS ||
                            file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS ||
                            file_id == (uint32_t)PatchFileId::POI_RECORDS ||
                            file_id == (uint32_t)PatchFileId::PLACE_NODES);
        bool needs_padding = (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS && actual_stride == 24) ||
                             (file_id == (uint32_t)PatchFileId::INTERP_WAYS && actual_stride == 24);

        // Read fixup data — decoded lazily during merge replay (zero allocation)
        uint32_t n_fixups = ru32();
        size_t fixup_data_pos = pos;
        if (n_fixups > 0) {
            uint32_t delta_size = ru32();
            fixup_data_pos = pos;
            pos += delta_size;
        }
        // Lazy fixup decoder state (reads from patch mmap on demand)
        struct FixupDecoder {
            const char* data; size_t read_pos;
            uint32_t prev_idx = 0, prev_val = 0;
            uint32_t cur_idx = UINT32_MAX, cur_val = 0;
            uint32_t remaining;
            void init(const char* d, size_t p, uint32_t count) {
                data = d; read_pos = p; remaining = count;
                if (remaining > 0) advance();
            }
            void advance() {
                if (remaining == 0) { cur_idx = UINT32_MAX; return; }
                cur_idx = prev_idx + read_varint(data, read_pos);
                cur_val = prev_val + read_varint(data, read_pos);
                prev_idx = cur_idx; prev_val = cur_val;
                remaining--;
            }
            // Advance past all fixups with index < target
            bool lookup(uint32_t target_idx, uint32_t& out_val) {
                while (cur_idx < target_idx && (remaining > 0 || cur_idx != UINT32_MAX)) advance();
                if (cur_idx == target_idx) { out_val = cur_val; advance(); return true; }
                return false;
            }
        } fixup_dec;
        fixup_dec.init(P, fixup_data_pos, n_fixups);

        // Get string remap field offsets for this file type
        std::vector<size_t> remap_offs;
        if (!str_remap_vec.empty() && needs_remap) {
            // AddrPoint string fields live at {8, 12} (housenumber_id,
            // street_id). Offset 16 is parent_way_id, which is a WAY id
            // — not a string pool offset — so it must NOT be rewritten
            // by the string remap. The previous {8, 12, 16} mapping
            // corrupted parent_way_id for every record that sat in a
            // MATCH run, producing the "first_diff=17" (first byte of
            // parent_way_id) mismatches we saw in patch verify.
            if (file_id == (uint32_t)PatchFileId::ADDR_POINTS) remap_offs = {8, 12};
            else if (file_id == (uint32_t)PatchFileId::STREET_WAYS) remap_offs = {(actual_stride == 12) ? 8ul : 5ul};
            else if (file_id == (uint32_t)PatchFileId::INTERP_WAYS) remap_offs = {(actual_stride >= 20) ? 8ul : 5ul};
            else if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS) remap_offs = {8};
            else if (file_id == (uint32_t)PatchFileId::POI_RECORDS) remap_offs = {16};
            else if (file_id == (uint32_t)PatchFileId::PLACE_NODES) remap_offs = {8};
        }

        // mmap old file read-only (zero allocation)
        MappedFile old_mmap = mmap_file(cur_dir + "/" + std::string(fname));
        madvise(const_cast<char*>(old_mmap.data), old_mmap.size, MADV_SEQUENTIAL);
        size_t n_old_records = old_mmap.size / actual_stride;

        // Replay merge sequence — stream output, apply remap/fixups per-record inline
        uint64_t seq_size = ru64();
        size_t seq_end = pos + seq_size;
        FILE* outf = fopen((out_dir + "/" + fname).c_str(), "wb");

        bool track = needs_remap; // track ID remap only for data files
        // File-backed id_map: create temp file, fill with 0xFF, mmap read-write
        int remap_fd = -1;
        uint32_t* id_map_ptr = nullptr;
        std::string remap_path;
        if (track) {
            remap_path = tmpdir + "/remap_" + std::to_string(file_id) + ".bin";
            remap_fd = open(remap_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
            size_t remap_bytes = n_old_records * 4;
            ftruncate(remap_fd, remap_bytes); // zero-filled by OS (sparse)
            id_map_ptr = static_cast<uint32_t*>(
                mmap(nullptr, remap_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, remap_fd, 0));
            // File is zero-filled. We store new_index+1 so that 0 means "unmapped".
            // The entry pipeline reads these and subtracts 1.
        }

        // Per-record buffer for applying remap/fixups inline (avoid copying entire file)
        std::vector<char> rec_buf(actual_stride);

        size_t old_rec = 0, new_rec = 0, old_bytes = 0, written = 0;
        while (pos < seq_end) {
            uint8_t op = P[pos++];
            uint32_t count; memcpy(&count, P+pos, 4); pos += 4;
            if (op == OP_MATCH_RUN) {
                for (uint32_t k = 0; k < count; k++) {
                    size_t rec_off = old_bytes + k * actual_stride;
                    if (rec_off + actual_stride > old_mmap.size) break;

                    bool modified = false;
                    // Check if this record needs any modification
                    if (!remap_offs.empty() || needs_padding) modified = true;
                    // Check fixup using lazy decoder (reads from patch mmap, zero allocation)
                    uint32_t fixup_val = 0;
                    bool has_fixup = fixup_dec.lookup(old_rec + k, fixup_val);
                    if (has_fixup) modified = true;

                    if (modified) {
                        memcpy(rec_buf.data(), old_mmap.data + rec_off, actual_stride);
                        // Apply padding zeroing
                        if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS && actual_stride == 24)
                            memset(rec_buf.data() + 14, 0, 2); // preserve place_type_override at byte 13
                        if (file_id == (uint32_t)PatchFileId::INTERP_WAYS && actual_stride == 24) {
                            memset(rec_buf.data() + 5, 0, 3);
                            memset(rec_buf.data() + 21, 0, 3);
                        }
                        // Apply string remap
                        for (size_t off : remap_offs) {
                            uint32_t v; memcpy(&v, rec_buf.data() + off, 4);
                            uint32_t nv = str_remap_lookup(v);
                            if (nv != v) memcpy(rec_buf.data() + off, &nv, 4);
                        }
                        // Apply fixup (node_offset/vertex_offset — at byte 0 for most types, byte 8 for POI records)
                        if (has_fixup) {
                            size_t fixup_off = (file_id == (uint32_t)PatchFileId::POI_RECORDS) ? 8 : 0;
                            memcpy(rec_buf.data() + fixup_off, &fixup_val, 4);
                        }
                        fwrite(rec_buf.data(), 1, actual_stride, outf);
                    } else {
                        // No modification needed — write directly from mmap
                        fwrite(old_mmap.data + rec_off, 1, actual_stride, outf);
                    }
                    if (track && old_rec + k < n_old_records) id_map_ptr[old_rec + k] = (uint32_t)(new_rec + k) + 1; // +1 so 0 means unmapped
                }
                written += count * actual_stride;
                old_rec += count; new_rec += count; old_bytes += count * actual_stride;
            } else if (op == OP_INSERT_RUN) {
                size_t bytes = count * actual_stride;
                fwrite(P+pos, 1, bytes, outf);
                written += bytes; pos += bytes; new_rec += count;
            } else if (op == OP_DELETE_RUN) {
                old_rec += count; old_bytes += count * actual_stride;
            }
        }
        fclose(outf);
        unmap_file(old_mmap);

        if (track) {
            // Sync and munmap the file-backed remap (data is already on disk)
            size_t remap_bytes = n_old_records * 4;
            msync(id_map_ptr, remap_bytes, MS_SYNC);
            munmap(id_map_ptr, remap_bytes);
            close(remap_fd);
            id_map_ptr = nullptr;
            id_remaps[file_id] = {}; // empty placeholder
            std::cerr << "  " << fname << ": " << written << " bytes (remap " << n_old_records << " records to disk)" << std::endl;
        } else {
            std::cerr << "  " << fname << ": " << written << " bytes" << std::endl;
        }
        // Release patch pages used by this section
        madvise(const_cast<char*>(patch_map.data), pos, MADV_DONTNEED);
        malloc_trim(0);
    }
    log_phase("Merge replays", t_start);

    // Copy POI/place data files from old if not present in patch (these sections are optional)
    for (auto fid : {PatchFileId::POI_RECORDS, PatchFileId::POI_VERTICES, PatchFileId::PLACE_NODES}) {
        std::string fname2 = patch_file_names[(uint32_t)fid];
        std::string out_path = out_dir + "/" + fname2;
        struct stat st;
        if (stat(out_path.c_str(), &st) != 0) {
            // Not written by patch — copy from old if it exists
            std::string old_path = cur_dir + "/" + fname2;
            auto data = read_file(old_path);
            if (!data.empty()) {
                write_file(out_path, data);
                std::cerr << "  " << fname2 << ": copied from old (" << data.size() << " bytes)" << std::endl;
            }
        }
    }

    // Free string remap + release all processed patch pages
    { std::vector<std::pair<uint32_t,uint32_t>>().swap(str_remap_vec); }
    madvise(const_cast<char*>(patch_map.data), pos, MADV_DONTNEED);
    malloc_trim(0);

    // --- Phase 4: Streaming entry pipeline ---
    // Walk old geo_cells, remap IDs, apply corrections, write output directly to files.
    {
        double t_entry = now_ms();
        std::cerr << "Entry pipeline (lean streaming)..." << std::endl;

        // mmap remap files from disk (only pages accessed are in RSS)
        auto mmap_remap = [&](PatchFileId fid) -> MappedFile {
            std::string path = tmpdir + "/remap_" + std::to_string((uint32_t)fid) + ".bin";
            auto m = mmap_file(path);
            if (m.data) madvise(const_cast<char*>(m.data), m.size, MADV_SEQUENTIAL);
            return m;
        };
        // Helper: look up remap value from mmap'd file
        // Remap values stored as new_index+1 (0 means unmapped)
        struct RemapRef {
            const uint32_t* data;
            size_t count;
            uint32_t operator[](size_t i) const {
                if (i >= count) return 0xFFFFFFFF;
                uint32_t v = data[i];
                return v == 0 ? 0xFFFFFFFF : v - 1;
            }
            size_t size() const { return count; }
        };
        MappedFile m_w_rm = mmap_remap(PatchFileId::STREET_WAYS);
        MappedFile m_a_rm = mmap_remap(PatchFileId::ADDR_POINTS);
        MappedFile m_i_rm = mmap_remap(PatchFileId::INTERP_WAYS);
        RemapRef w_rm = {(const uint32_t*)m_w_rm.data, m_w_rm.size / 4};
        RemapRef a_rm = {(const uint32_t*)m_a_rm.data, m_a_rm.size / 4};
        RemapRef i_rm = {(const uint32_t*)m_i_rm.data, m_i_rm.size / 4};

        // mmap old files (zero RSS until pages are accessed, then only working set)
        MappedFile m_geo = mmap_file(cur_dir + "/geo_cells.bin");
        MappedFile m_se = mmap_file(cur_dir + "/street_entries.bin");
        MappedFile m_ae = mmap_file(cur_dir + "/addr_entries.bin");
        MappedFile m_ie = mmap_file(cur_dir + "/interp_entries.bin");
        madvise(const_cast<char*>(m_se.data), m_se.size, MADV_SEQUENTIAL);
        madvise(const_cast<char*>(m_ae.data), m_ae.size, MADV_SEQUENTIAL);
        madvise(const_cast<char*>(m_ie.data), m_ie.size, MADV_SEQUENTIAL);
        size_t n_old = m_geo.size / 20;

        // Build sorted correction vectors + removed set (better cache locality than hash maps)
        std::unordered_set<uint64_t> rm_set(geo_removed.begin(), geo_removed.end());
        struct CorrEntry { uint64_t cid; const std::vector<uint32_t>* ids; };
        std::vector<CorrEntry> cs, ca, ci_map;
        for (auto& [fid, list] : entry_corrections) {
            auto* v = (fid == (uint32_t)PatchFileId::STREET_ENTRIES) ? &cs :
                      (fid == (uint32_t)PatchFileId::ADDR_ENTRIES) ? &ca :
                      (fid == (uint32_t)PatchFileId::INTERP_ENTRIES) ? &ci_map : nullptr;
            if (v) for (auto& c : list) v->push_back({c.cell_id, &c.ids});
        }
        auto corr_cmp = [](const CorrEntry& a, const CorrEntry& b) { return a.cid < b.cid; };
        std::sort(cs.begin(), cs.end(), corr_cmp);
        std::sort(ca.begin(), ca.end(), corr_cmp);
        std::sort(ci_map.begin(), ci_map.end(), corr_cmp);
        // Binary search helper
        auto corr_find = [](const std::vector<CorrEntry>& v, uint64_t cid) -> const std::vector<uint32_t>* {
            auto it = std::lower_bound(v.begin(), v.end(), CorrEntry{cid, nullptr},
                [](const CorrEntry& a, const CorrEntry& b) { return a.cid < b.cid; });
            return (it != v.end() && it->cid == cid) ? it->ids : nullptr;
        };
        // Added cells sorted
        std::sort(geo_added.begin(), geo_added.end());
        log_phase("  Setup", t_entry);

        // Open 4 output files
        FILE* f_geo = fopen((out_dir + "/geo_cells.bin").c_str(), "wb");
        FILE* f_se = fopen((out_dir + "/street_entries.bin").c_str(), "wb");
        FILE* f_ae = fopen((out_dir + "/addr_entries.bin").c_str(), "wb");
        FILE* f_ie = fopen((out_dir + "/interp_entries.bin").c_str(), "wb");
        constexpr uint32_t NO = 0xFFFFFFFF;

        // Reusable buffer (one per entry type to avoid aliasing issues)
        std::vector<uint32_t> buf;
        buf.reserve(4096);

        // Helper: parse IDs from mmap'd entry file
        auto parse = [&buf](const MappedFile& f, uint32_t off) {
            buf.clear();
            if (off == NO || off + 2 > f.size) return;
            uint16_t c; memcpy(&c, f.data + off, 2);
            if (off + 2 + (size_t)c * 4 > f.size) return;
            buf.resize(c);
            memcpy(buf.data(), f.data + off + 2, c * 4);
        };
        auto remap = [](std::vector<uint32_t>& ids, const RemapRef& rm) {
            constexpr uint32_t NO2 = 0xFFFFFFFF;
            for (auto& id : ids) if (id < rm.size() && rm[id] != NO2) id = rm[id];
            std::sort(ids.begin(), ids.end());
        };
        // Write entry and return offset, or NO if empty
        auto emit = [&NO](FILE* f, const uint32_t* ids, size_t n) -> uint32_t {
            if (n == 0) return NO;
            uint32_t off = (uint32_t)ftell(f);
            uint16_t c = (uint16_t)n;
            fwrite(&c, 2, 1, f); fwrite(ids, 4, n, f);
            return off;
        };

        // Merge-walk: old cells + added cells in sorted order
        // Both are sorted by cell_id. Merge them, skip removed.
        size_t old_i = 0, add_i = 0;
        size_t cells_written = 0;

        auto process_cell = [&](uint64_t cid, int32_t oi) {
            // For each entry type: check correction → remap old → write
            auto do_entry = [&](const MappedFile& old_e, size_t geo_off, const RemapRef& rm,
                                const std::vector<CorrEntry>& corr,
                                FILE* outf, uint8_t flag_bit) -> uint32_t {
                // Check flag
                bool has = false;
                if (oi >= 0 && (size_t)oi * 20 + geo_off + 4 <= m_geo.size) {
                    uint32_t off; memcpy(&off, m_geo.data + oi * 20 + geo_off, 4);
                    has = (off != NO);
                } else if (oi >= 0) {
                    std::cerr << "OOB: oi=" << oi << " geo_off=" << geo_off << " m_geo.size=" << m_geo.size << std::endl;
                    return NO;
                }
                auto fc = flag_corrections.find(cid);
                if (fc != flag_corrections.end()) has = (fc->second & flag_bit) != 0;
                auto* corr_ids = corr_find(corr, cid);
                if (corr_ids) has = true;
                if (!has) return NO;

                if (corr_ids) return emit(outf, corr_ids->data(), corr_ids->size());

                if (oi < 0) return NO;
                if ((size_t)oi * 20 + geo_off + 4 > m_geo.size) return NO;
                uint32_t off; memcpy(&off, m_geo.data + oi * 20 + geo_off, 4);
                parse(old_e, off);
                if (buf.empty()) return NO;
                remap(buf, rm);
                return emit(outf, buf.data(), buf.size());
            };

            uint32_t so = do_entry(m_se, 8, w_rm, cs, f_se, 1);
            uint32_t ao = do_entry(m_ae, 12, a_rm, ca, f_ae, 2);
            uint32_t io = do_entry(m_ie, 16, i_rm, ci_map, f_ie, 4);
            fwrite(&cid, 8, 1, f_geo); fwrite(&so, 4, 1, f_geo); fwrite(&ao, 4, 1, f_geo); fwrite(&io, 4, 1, f_geo);
            cells_written++;
        };

        while (old_i < n_old || add_i < geo_added.size()) {
            uint64_t old_cid = UINT64_MAX, add_cid = UINT64_MAX;
            if (old_i < n_old) memcpy(&old_cid, m_geo.data + old_i * 20, 8);
            if (add_i < geo_added.size()) add_cid = geo_added[add_i];

            if (old_cid <= add_cid) {
                if (!rm_set.count(old_cid))
                    process_cell(old_cid, (int32_t)old_i);
                old_i++;
                if (old_cid == add_cid) add_i++; // skip duplicate add
            } else {
                process_cell(add_cid, -1);
                add_i++;
            }

            if (cells_written % 10000000 == 0 && cells_written > 0) {
                std::cerr << "    " << cells_written << " cells, se=" << ftell(f_se)/1024/1024
                          << "M ae=" << ftell(f_ae)/1024/1024 << "M rss=" << get_rss_mb() << "M" << std::endl;
            }
        }

        fclose(f_geo); fclose(f_se); fclose(f_ae); fclose(f_ie);
        unmap_file(m_geo); unmap_file(m_se); unmap_file(m_ae); unmap_file(m_ie);
        unmap_file(m_w_rm); unmap_file(m_a_rm); unmap_file(m_i_rm);
        std::cerr << "  Geo: " << cells_written << " cells written" << std::endl;
        malloc_trim(0);
        log_phase("  Geo entries complete", t_entry);

        // Admin: small, use existing rebuild + corrections
        {
            uint32_t no_data = 0xFFFFFFFF;
            std::unordered_map<uint32_t,uint32_t> ad_rm;
            MappedFile m_ad_rm = mmap_remap(PatchFileId::ADMIN_POLYGONS);
            if (m_ad_rm.data) {
                const uint32_t* ad_vec = (const uint32_t*)m_ad_rm.data;
                size_t ad_count = m_ad_rm.size / 4;
                for (uint32_t i = 0; i < ad_count; i++)
                    if (ad_vec[i] != 0) ad_rm[i] = ad_vec[i] - 1; // decode +1 encoding
                unmap_file(m_ad_rm);
            }
            auto old_ac = read_file(cur_dir + "/admin_cells.bin");
            auto old_adme = read_file(cur_dir + "/admin_entries.bin");
            auto admin = rebuild_admin_from_remap(old_ac, old_adme, ad_rm, admin_added, admin_removed);

            auto ecit = entry_corrections.find((uint32_t)PatchFileId::ADMIN_ENTRIES);
            if (ecit != entry_corrections.end()) {
                std::unordered_map<uint64_t, const std::vector<uint32_t>*> ac_corr;
                for (auto& c : ecit->second) ac_corr[c.cell_id] = &c.ids;
                size_t n = admin.admin_cells_data.size() / 12;
                FILE* fac = fopen((out_dir + "/admin_cells.bin").c_str(), "wb");
                FILE* fae = fopen((out_dir + "/admin_entries.bin").c_str(), "wb");
                uint32_t ae_wpos = 0;
                for (size_t i = 0; i < n; i++) {
                    uint64_t cid; memcpy(&cid, admin.admin_cells_data.data()+i*12, 8);
                    fwrite(&cid, 8, 1, fac);
                    auto cit = ac_corr.find(cid);
                    if (cit != ac_corr.end()) {
                        uint32_t off = cit->second->empty() ? no_data : ae_wpos;
                        fwrite(&off, 4, 1, fac);
                        if (!cit->second->empty()) {
                            uint16_t c = cit->second->size();
                            fwrite(&c, 2, 1, fae); fwrite(cit->second->data(), 4, c, fae);
                            ae_wpos += 2 + c*4;
                        }
                    } else {
                        uint32_t old_off; memcpy(&old_off, admin.admin_cells_data.data()+i*12+8, 4);
                        if (old_off != no_data && old_off+2 <= admin.admin_entries_data.size()) {
                            uint32_t new_off = ae_wpos; fwrite(&new_off, 4, 1, fac);
                            uint16_t c; memcpy(&c, admin.admin_entries_data.data()+old_off, 2);
                            fwrite(admin.admin_entries_data.data()+old_off, 1, 2+c*4, fae);
                            ae_wpos += 2+c*4;
                        } else {
                            fwrite(&no_data, 4, 1, fac);
                        }
                    }
                }
                fclose(fac); fclose(fae);
                std::cerr << "  Admin: " << n << " cells, " << ac_corr.size() << " corrections" << std::endl;
            } else {
                write_file(out_dir + "/admin_cells.bin", admin.admin_cells_data);
                write_file(out_dir + "/admin_entries.bin", admin.admin_entries_data);
            }
            log_phase("  Admin", t_entry);
        }

        // POI: same pattern as admin — rebuild from remap + corrections
        {
            uint32_t no_data = 0xFFFFFFFF;
            std::unordered_map<uint32_t,uint32_t> poi_rm;
            MappedFile m_poi_rm = mmap_remap(PatchFileId::POI_RECORDS);
            if (m_poi_rm.data) {
                const uint32_t* poi_vec = (const uint32_t*)m_poi_rm.data;
                size_t poi_count = m_poi_rm.size / 4;
                for (uint32_t i = 0; i < poi_count; i++)
                    if (poi_vec[i] != 0) poi_rm[i] = poi_vec[i] - 1; // decode +1 encoding
                unmap_file(m_poi_rm);
            }
            auto old_pc = read_file(cur_dir + "/poi_cells.bin");
            auto old_pe = read_file(cur_dir + "/poi_entries.bin");

            if (!old_pc.empty() || !poi_added.empty() || !poi_removed.empty() || !poi_rm.empty()) {
                // Rebuild POI cells/entries using same logic as admin
                // POI cells: 12 bytes each (u64 cell_id + u32 entry_offset)
                // POI entries: variable (u16 count + u32[] ids), no flag masking needed
                size_t n_poi_cells = old_pc.size() / 12;
                struct PoiCellData { uint64_t cell_id; std::vector<uint32_t> ids; };
                std::vector<PoiCellData> poi_cells(n_poi_cells);
                for (size_t i = 0; i < n_poi_cells; i++) {
                    memcpy(&poi_cells[i].cell_id, old_pc.data() + i * 12, 8);
                    uint32_t off; memcpy(&off, old_pc.data() + i * 12 + 8, 4);
                    if (off != no_data && off + 2 <= old_pe.size()) {
                        uint16_t count; memcpy(&count, old_pe.data() + off, 2);
                        if (off + 2 + count * 4 <= old_pe.size()) {
                            poi_cells[i].ids.resize(count);
                            memcpy(poi_cells[i].ids.data(), old_pe.data() + off + 2, count * 4);
                            for (auto& id : poi_cells[i].ids) {
                                auto it = poi_rm.find(id);
                                if (it != poi_rm.end()) id = it->second;
                            }
                            std::sort(poi_cells[i].ids.begin(), poi_cells[i].ids.end());
                        }
                    }
                }
                // Apply cell changes
                if (!poi_removed.empty()) {
                    std::unordered_set<uint64_t> removed_set(poi_removed.begin(), poi_removed.end());
                    poi_cells.erase(std::remove_if(poi_cells.begin(), poi_cells.end(),
                        [&](const PoiCellData& c) { return removed_set.count(c.cell_id); }), poi_cells.end());
                }
                if (!poi_added.empty()) {
                    for (uint64_t cid : poi_added) {
                        PoiCellData cd; cd.cell_id = cid;
                        poi_cells.push_back(cd);
                    }
                    std::sort(poi_cells.begin(), poi_cells.end(),
                        [](const PoiCellData& a, const PoiCellData& b) { return a.cell_id < b.cell_id; });
                }

                // Apply entry corrections
                auto ecit = entry_corrections.find((uint32_t)PatchFileId::POI_ENTRIES);
                if (ecit != entry_corrections.end()) {
                    std::unordered_map<uint64_t, const std::vector<uint32_t>*> pc_corr;
                    for (auto& c : ecit->second) pc_corr[c.cell_id] = &c.ids;
                    FILE* fpc = fopen((out_dir + "/poi_cells.bin").c_str(), "wb");
                    FILE* fpe = fopen((out_dir + "/poi_entries.bin").c_str(), "wb");
                    uint32_t pe_wpos = 0;
                    for (size_t i = 0; i < poi_cells.size(); i++) {
                        uint64_t cid = poi_cells[i].cell_id;
                        fwrite(&cid, 8, 1, fpc);
                        auto cit = pc_corr.find(cid);
                        if (cit != pc_corr.end()) {
                            uint32_t off = cit->second->empty() ? no_data : pe_wpos;
                            fwrite(&off, 4, 1, fpc);
                            if (!cit->second->empty()) {
                                uint16_t c = cit->second->size();
                                fwrite(&c, 2, 1, fpe); fwrite(cit->second->data(), 4, c, fpe);
                                pe_wpos += 2 + c*4;
                            }
                        } else {
                            if (!poi_cells[i].ids.empty()) {
                                uint32_t new_off = pe_wpos; fwrite(&new_off, 4, 1, fpc);
                                uint16_t c = poi_cells[i].ids.size();
                                fwrite(&c, 2, 1, fpe); fwrite(poi_cells[i].ids.data(), 4, c, fpe);
                                pe_wpos += 2 + c*4;
                            } else {
                                fwrite(&no_data, 4, 1, fpc);
                            }
                        }
                    }
                    fclose(fpc); fclose(fpe);
                    std::cerr << "  POI: " << poi_cells.size() << " cells, " << pc_corr.size() << " corrections" << std::endl;
                } else {
                    // No corrections — write rebuilt data directly
                    std::vector<char> pc_data, pe_data;
                    std::unordered_map<uint64_t, uint32_t> offsets;
                    for (auto& c : poi_cells) {
                        if (c.ids.empty()) continue;
                        offsets[c.cell_id] = static_cast<uint32_t>(pe_data.size());
                        uint16_t count = static_cast<uint16_t>(c.ids.size());
                        pe_data.insert(pe_data.end(), (const char*)&count, (const char*)&count + 2);
                        pe_data.insert(pe_data.end(), (const char*)c.ids.data(), (const char*)c.ids.data() + c.ids.size() * 4);
                    }
                    for (auto& c : poi_cells) {
                        pc_data.insert(pc_data.end(), (const char*)&c.cell_id, (const char*)&c.cell_id + 8);
                        auto it = offsets.find(c.cell_id);
                        uint32_t off = it != offsets.end() ? it->second : no_data;
                        pc_data.insert(pc_data.end(), (const char*)&off, (const char*)&off + 4);
                    }
                    write_file(out_dir + "/poi_cells.bin", pc_data);
                    write_file(out_dir + "/poi_entries.bin", pe_data);
                    std::cerr << "  POI: " << poi_cells.size() << " cells rebuilt" << std::endl;
                }
            } else {
                // No POI data in old or patch — copy old files if they exist (graceful)
                auto pc = read_file(cur_dir + "/poi_cells.bin");
                auto pe = read_file(cur_dir + "/poi_entries.bin");
                if (!pc.empty()) write_file(out_dir + "/poi_cells.bin", pc);
                if (!pe.empty()) write_file(out_dir + "/poi_entries.bin", pe);
            }
            log_phase("  POI", t_entry);
        }

        // Place: same pattern as POI — rebuild from remap + corrections
        {
            uint32_t no_data = 0xFFFFFFFF;
            std::unordered_map<uint32_t,uint32_t> place_rm;
            MappedFile m_place_rm = mmap_remap(PatchFileId::PLACE_NODES);
            if (m_place_rm.data) {
                const uint32_t* place_vec = (const uint32_t*)m_place_rm.data;
                size_t place_count = m_place_rm.size / 4;
                for (uint32_t i = 0; i < place_count; i++)
                    if (place_vec[i] != 0) place_rm[i] = place_vec[i] - 1; // decode +1 encoding
                unmap_file(m_place_rm);
            }
            auto old_plc = read_file(cur_dir + "/place_cells.bin");
            auto old_ple = read_file(cur_dir + "/place_entries.bin");

            if (!old_plc.empty() || !place_added.empty() || !place_removed.empty() || !place_rm.empty()) {
                // Rebuild place cells/entries using same logic as POI
                // Place cells: 12 bytes each (u64 cell_id + u32 entry_offset)
                // Place entries: variable (u16 count + u32[] ids)
                size_t n_place_cells = old_plc.size() / 12;
                struct PlaceCellData { uint64_t cell_id; std::vector<uint32_t> ids; };
                std::vector<PlaceCellData> place_cells(n_place_cells);
                for (size_t i = 0; i < n_place_cells; i++) {
                    memcpy(&place_cells[i].cell_id, old_plc.data() + i * 12, 8);
                    uint32_t off; memcpy(&off, old_plc.data() + i * 12 + 8, 4);
                    if (off != no_data && off + 2 <= old_ple.size()) {
                        uint16_t count; memcpy(&count, old_ple.data() + off, 2);
                        if (off + 2 + count * 4 <= old_ple.size()) {
                            place_cells[i].ids.resize(count);
                            memcpy(place_cells[i].ids.data(), old_ple.data() + off + 2, count * 4);
                            for (auto& id : place_cells[i].ids) {
                                auto it = place_rm.find(id);
                                if (it != place_rm.end()) id = it->second;
                            }
                            std::sort(place_cells[i].ids.begin(), place_cells[i].ids.end());
                        }
                    }
                }
                // Apply cell changes
                if (!place_removed.empty()) {
                    std::unordered_set<uint64_t> removed_set(place_removed.begin(), place_removed.end());
                    place_cells.erase(std::remove_if(place_cells.begin(), place_cells.end(),
                        [&](const PlaceCellData& c) { return removed_set.count(c.cell_id); }), place_cells.end());
                }
                if (!place_added.empty()) {
                    for (uint64_t cid : place_added) {
                        PlaceCellData cd; cd.cell_id = cid;
                        place_cells.push_back(cd);
                    }
                    std::sort(place_cells.begin(), place_cells.end(),
                        [](const PlaceCellData& a, const PlaceCellData& b) { return a.cell_id < b.cell_id; });
                }

                // Apply entry corrections
                auto ecit = entry_corrections.find((uint32_t)PatchFileId::PLACE_ENTRIES);
                if (ecit != entry_corrections.end()) {
                    std::unordered_map<uint64_t, const std::vector<uint32_t>*> plc_corr;
                    for (auto& c : ecit->second) plc_corr[c.cell_id] = &c.ids;
                    FILE* fplc = fopen((out_dir + "/place_cells.bin").c_str(), "wb");
                    FILE* fple = fopen((out_dir + "/place_entries.bin").c_str(), "wb");
                    uint32_t ple_wpos = 0;
                    for (size_t i = 0; i < place_cells.size(); i++) {
                        uint64_t cid = place_cells[i].cell_id;
                        fwrite(&cid, 8, 1, fplc);
                        auto cit = plc_corr.find(cid);
                        if (cit != plc_corr.end()) {
                            uint32_t off = cit->second->empty() ? no_data : ple_wpos;
                            fwrite(&off, 4, 1, fplc);
                            if (!cit->second->empty()) {
                                uint16_t c = cit->second->size();
                                fwrite(&c, 2, 1, fple); fwrite(cit->second->data(), 4, c, fple);
                                ple_wpos += 2 + c*4;
                            }
                        } else {
                            if (!place_cells[i].ids.empty()) {
                                uint32_t new_off = ple_wpos; fwrite(&new_off, 4, 1, fplc);
                                uint16_t c = place_cells[i].ids.size();
                                fwrite(&c, 2, 1, fple); fwrite(place_cells[i].ids.data(), 4, c, fple);
                                ple_wpos += 2 + c*4;
                            } else {
                                fwrite(&no_data, 4, 1, fplc);
                            }
                        }
                    }
                    fclose(fplc); fclose(fple);
                    std::cerr << "  Place: " << place_cells.size() << " cells, " << plc_corr.size() << " corrections" << std::endl;
                } else {
                    // No corrections — write rebuilt data directly
                    std::vector<char> plc_data, ple_data;
                    std::unordered_map<uint64_t, uint32_t> offsets;
                    for (auto& c : place_cells) {
                        if (c.ids.empty()) continue;
                        offsets[c.cell_id] = static_cast<uint32_t>(ple_data.size());
                        uint16_t count = static_cast<uint16_t>(c.ids.size());
                        ple_data.insert(ple_data.end(), (const char*)&count, (const char*)&count + 2);
                        ple_data.insert(ple_data.end(), (const char*)c.ids.data(), (const char*)c.ids.data() + c.ids.size() * 4);
                    }
                    for (auto& c : place_cells) {
                        plc_data.insert(plc_data.end(), (const char*)&c.cell_id, (const char*)&c.cell_id + 8);
                        auto it = offsets.find(c.cell_id);
                        uint32_t off = it != offsets.end() ? it->second : no_data;
                        plc_data.insert(plc_data.end(), (const char*)&off, (const char*)&off + 4);
                    }
                    write_file(out_dir + "/place_cells.bin", plc_data);
                    write_file(out_dir + "/place_entries.bin", ple_data);
                    std::cerr << "  Place: " << place_cells.size() << " cells rebuilt" << std::endl;
                }
            } else {
                // No place data in old or patch — copy old files if they exist (graceful)
                auto plc = read_file(cur_dir + "/place_cells.bin");
                auto ple = read_file(cur_dir + "/place_entries.bin");
                if (!plc.empty()) write_file(out_dir + "/place_cells.bin", plc);
                if (!ple.empty()) write_file(out_dir + "/place_entries.bin", ple);
            }
            log_phase("  Place", t_entry);
        }
    }

    // Cleanup
    unmap_file(patch_map);
    remove(raw_path.c_str());
    { std::string cmd = "rm -rf '" + tmpdir + "'"; system(cmd.c_str()); }
    log_phase("Total", t_start);
    std::cerr << "Patch applied. Output in " << out_dir << std::endl;
    return 0;
}
