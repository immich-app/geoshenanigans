// geocoder-patch v4: Low-memory streaming patch application.
//
// Key design: old files stream front to back through pread (SequentialFileReader);
// the string tiers and the decompressed patch, read at random, stay mmapped.
// Output streams via fwrite (never accumulate), the entry pipeline goes
// cell-by-cell. Target: <1 GiB peak RSS for planet.
//
// Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir>

#include <algorithm>
#include <array>
#include <climits>
#include <cstdint>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <malloc.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <vector>

#include "merge_sequence.h"
#include "patch_format.h"
#include "scratch_dir.h"
#include "sequential_file_reader.h"


// --- Helpers ---
static double now_ms() {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}
// Anonymous resident memory (heap, buffers). Unlike statm's RSS it leaves
// out the page cache of mapped old files, which the kernel reclaims freely.
static size_t get_rss_anon_mb() {
    FILE* f = fopen("/proc/self/status", "r");
    if (!f) return 0;
    char line[128];
    size_t kb = 0;
    while (fgets(line, sizeof line, f))
        if (sscanf(line, "RssAnon: %zu kB", &kb) == 1) break;
    fclose(f);
    return kb / 1024;
}
// Page faults and storage reads so far. A read taken inside a page fault
// counts in majflt, a pread only in read_bytes, so a phase needs both to
// show where its old-file I/O went.
struct IoCounters { uint64_t majflt = 0, minflt = 0, read_bytes = 0; };
static IoCounters get_io_counters() {
    IoCounters c;
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) { c.majflt = ru.ru_majflt; c.minflt = ru.ru_minflt; }
    if (FILE* f = fopen("/proc/self/io", "r")) {
        char line[128];
        unsigned long long v;
        while (fgets(line, sizeof line, f))
            if (sscanf(line, "read_bytes: %llu", &v) == 1) { c.read_bytes = v; break; }
        fclose(f);
    }
    return c;
}
// "maj 12, min 3400, read 1.5 MiB" between two snapshots.
static std::string io_delta(const IoCounters& from, const IoCounters& to) {
    char s[96];
    snprintf(s, sizeof s, "maj %llu, min %llu, read %.1f MiB",
             (unsigned long long)(to.majflt - from.majflt), (unsigned long long)(to.minflt - from.minflt),
             (to.read_bytes - from.read_bytes) / 1048576.0);
    return s;
}
// Elapsed time, anonymous RSS, and the faults and reads since the previous
// phase line.
static void log_phase(const char* label, double start) {
    static IoCounters last;
    IoCounters now = get_io_counters();
    std::cerr << "  [" << std::fixed << std::setprecision(1) << (now_ms() - start) / 1000.0
              << "s, " << get_rss_anon_mb() << " MiB anon, " << io_delta(last, now) << "] " << label << std::endl;
    last = now;
}
// Every old-file read is forward only on today's layouts; a rewind means a
// layout changed and the file is being read more than once.
static void log_rewinds(const SequentialFileReader& r) {
    if (r.rewinds()) std::cerr << "  " << r.path() << ": read backwards " << r.rewinds() << " times" << std::endl;
}

// Detect stride from file size
static size_t detect_stride(const std::string& path, std::initializer_list<size_t> candidates) {
    struct stat st; if (stat(path.c_str(), &st) != 0) return *candidates.begin();
    for (size_t s : candidates) if (st.st_size % s == 0 && st.st_size > 0) return s;
    return *candidates.begin();
}

// Resolve a string tier with fallback to the region's full/ dir, where the
// tiers live for variants that don't ship them. Mirrors geocoder-diff's
// try_load_tier; the tier stamps prove both found the same file.
static std::string resolve_with_fallback(const std::string& cur_dir, const std::string& fname,
                                          std::initializer_list<const char*> fallbacks) {
    std::string primary = cur_dir + "/" + fname;
    struct stat st;
    if (stat(primary.c_str(), &st) == 0 && st.st_size > 0) return primary;
    for (const char* fb : fallbacks) {
        std::string p = cur_dir + "/" + fb + fname;
        if (stat(p.c_str(), &st) == 0 && st.st_size > 0) return p;
    }
    return primary;
}

static int run(int argc, char* argv[]) {
    if (argc < 5 || std::string(argv[3]) != "-o") {
        std::cerr << "Usage: geocoder-patch <current-dir> <patch-file> -o <output-dir>" << std::endl;
        return 1;
    }
    std::string cur_dir = argv[1], patch_path = argv[2], out_dir = argv[4];
    ensure_dir(out_dir);
    // Output files are truncated while the current files are still
    // read as the patch base, so in-place application corrupts both.
    {
        char cur_real[PATH_MAX], out_real[PATH_MAX];
        if (realpath(cur_dir.c_str(), cur_real) && realpath(out_dir.c_str(), out_real) &&
            std::strcmp(cur_real, out_real) == 0) {
            std::cerr << "Output dir must differ from the current dir: " << out_real << std::endl;
            return 1;
        }
    }
    // The output is exactly the patch's file set, so stale files can't stay.
    if (!std::filesystem::is_empty(out_dir)) throw std::runtime_error("Output dir is not empty: " + out_dir);
    ScratchDir scratch("geocoder-patch");
    const std::string& tmpdir = scratch.path();
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
    // Bounds-checked read of n bytes at the current position. Fails loudly
    // (rather than reading past the mmap) if a malformed/truncated patch would
    // run the cursor off the end. On the success path pos+n <= patch_size
    // always holds, so this never fires and the read bytes are unchanged.
    auto require_bytes = [&](size_t n, const char* what) {
        if (n > patch_size - pos)
            throw std::runtime_error("Truncated patch: need " + std::to_string(n) + " bytes for " +
                                     what + " at offset " + std::to_string(pos) +
                                     " (size " + std::to_string(patch_size) + ")");
    };
    auto ru32 = [&]() -> uint32_t { require_bytes(4, "u32"); uint32_t v; memcpy(&v, P+pos, 4); pos += 4; return v; };
    auto ru64 = [&]() -> uint64_t { require_bytes(8, "u64"); uint64_t v; memcpy(&v, P+pos, 8); pos += 8; return v; };
    auto take = [&](size_t n, const char* what) -> const char* {
        require_bytes(n, what);
        const char* p = P + pos;
        pos += n;
        return p;
    };
    auto rstr = [&]() -> std::string {
        const void* end = memchr(P + pos, '\0', patch_size - pos);
        if (!end) throw std::runtime_error("Truncated patch: unterminated string");
        std::string s(P + pos, static_cast<const char*>(end));
        pos += s.size() + 1;
        return s;
    };

    // Header
    if (memcmp(P, GCPATCH_MAGIC, 8) != 0) { std::cerr << "Bad magic" << std::endl; return 1; }
    pos = 8;
    uint32_t ver = ru32(); if (ver < GCPATCH_MIN_READ_VERSION || ver > GCPATCH_VERSION) { std::cerr << "Bad version" << std::endl; return 1; }
    ru32(); // flags

    // The exact file set out_dir must hold after the apply. Outputs the
    // variant does not ship (string tiers it only remaps through, sections
    // for files it lacks) are built in scratch instead.
    const std::vector<ClientFile> client_files = parse_client_files(P, patch_size, pos);
    std::unordered_set<std::string> listed;
    for (const auto& f : client_files) {
        listed.insert(f.name);
        if (is_inline_client_file(f.name) && !write_file(out_dir + "/" + f.name, f.bytes))
            throw std::runtime_error("Cannot write " + f.name);
    }
    auto out_path = [&](const std::string& name) {
        return (listed.count(name) ? out_dir : tmpdir) + "/" + name;
    };
    auto open_out = [&](const std::string& name) -> FILE* {
        std::string p = out_path(name);
        FILE* f = fopen(p.c_str(), "wb");
        if (!f) throw std::runtime_error("Cannot write " + p);
        return f;
    };

    // --- Phase 2: String rebuild ---
    // Sorted vector remap: (old_offset, new_offset) pairs sorted by old_offset.
    // Offsets are global across all tiers (tier 0 occupies [0, base[1]),
    // tier N at [base[N], base[N+1])) so a single remap covers everything.
    static const char* kStrTierFilenames[5] = {
        "strings_core.bin", "strings_street.bin", "strings_addr.bin",
        "strings_postcode.bin", "strings_poi.bin"
    };
    StringRemap str_remap;
    // The old tiers stay mapped until the replays are done: str_remap's runs
    // check string starts against them.
    std::vector<MappedFile> old_str_pools;
    {
        uint32_t marker = ru32();
        if (marker == STRINGS_TIERED_MARKER) {
            // Tiered format — 5 independent per-tier diffs.  For each tier:
            //   1. Read its n_added/n_deleted block from the patch.
            //   2. Merge-write the old tier file + added (minus deleted)
            //      into the new tier file.
            //   3. Walk old/new to extend str_remap using global offsets.
            uint32_t old_global_base = 0;
            uint32_t new_global_base = 0;
            uint32_t total_added = 0, total_deleted = 0;
            // A variant without a tier of its own (quality, poi, or a mode
            // dir's unshipped tiers) resolves the old one under <region>/full/.
            auto old_tier_path = [&](int t) {
                return resolve_with_fallback(cur_dir, kStrTierFilenames[t], {"../full/", "../../full/"});
            };
            // String offsets are global (cumulative tier sizes), so every
            // old tier must be exactly the one the diff saw, even tiers this
            // variant doesn't ship; a missing or stale tier would shift every
            // later offset.
            std::array<TierStamp, 5> stamps;
            for (auto& s : stamps) {
                s.old_size = ru32(); s.new_size = ru32();
                s.old_hash = ru64(); s.new_hash = ru64();
            }
            for (int t = 0; t < 5; t++) {
                uint32_t n_added = ru32(), n_deleted = ru32();
                total_added += n_added; total_deleted += n_deleted;
                std::vector<std::string> added;
                for (uint32_t i = 0; i < n_added; i++) added.push_back(rstr());
                require_bytes((size_t)n_deleted * 4, "deleted string indices");
                std::vector<uint32_t> del_idx(n_deleted);
                for (uint32_t i = 0; i < n_deleted; i++) del_idx[i] = ru32();
                std::unordered_set<uint32_t> del_set(del_idx.begin(), del_idx.end());

                MappedFile old_pool = mmap_file(old_tier_path(t));
                if (old_pool.size != stamps[t].old_size ||
                    content_hash(old_pool.data, old_pool.size) != stamps[t].old_hash)
                    throw std::runtime_error(std::string("Old ") + kStrTierFilenames[t] + " (" +
                                             std::to_string(old_pool.size) + " bytes at " + old_tier_path(t) +
                                             ") is not the one the patch was made from");
                // Phase A: write new tier file via alphabetical merge.
                {
                    FILE* fp = open_out(kStrTierFilenames[t]);
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
                MappedFile new_pool = mmap_file(out_path(kStrTierFilenames[t]));
                if (new_pool.size != stamps[t].new_size ||
                    content_hash(new_pool.data, new_pool.size) != stamps[t].new_hash)
                    throw std::runtime_error(std::string("Rebuilt ") + kStrTierFilenames[t] +
                                             " does not match the new build");
                str_remap.add_tier(old_pool.data, old_pool.size, old_global_base,
                                   new_pool.data, new_pool.size, new_global_base);
                old_global_base += static_cast<uint32_t>(old_pool.size);
                new_global_base += static_cast<uint32_t>(new_pool.size);
                old_str_pools.push_back(old_pool);
                unmap_file(new_pool);
            }
            malloc_trim(0);
            std::cerr << "  Strings (tiered): +" << total_added << " -" << total_deleted
                      << ", " << str_remap.run_count() << " remap runs" << std::endl;

            marker = ru32();
        }
        if (marker == STRINGS_CROSS_TIER_REMAP_MARKER) {
            uint32_t c = ru32();
            for (uint32_t i = 0; i < c; i++) { uint32_t a = ru32(), b = ru32(); str_remap.add_pair(a, b); }
        } else pos -= 4;
        str_remap.finish();
    }
    // Release patch pages read so far (string section)
    madvise(const_cast<char*>(patch_map.data), pos, MADV_DONTNEED);
    log_phase("Strings", t_start);

    size_t way_stride = detect_stride(cur_dir + "/street_ways.bin", {12, 9});
    size_t interp_stride = detect_stride(cur_dir + "/interp_ways.bin", {24, 20, 18});
    size_t admin_stride = detect_stride(cur_dir + "/admin_polygons.bin", {24, 20, 19});

    auto str_remap_lookup = [&](uint32_t old_off) { return str_remap.lookup(old_off); };

    // --- Phase 3: Merge replays (streaming output) ---
    // ID remaps: file_id → vector<uint32_t> where remap[old_idx] = new_idx
    // Old → new record ids of the replayed files the entry pipeline remaps by.
    std::unordered_map<uint32_t, RecordRemap> record_remaps;
    std::vector<uint64_t> geo_added, geo_removed, admin_added, admin_removed, poi_added, poi_removed, place_added, place_removed;
    // Street / addr / interp per-cell corrections by entries file id.
    std::unordered_map<uint32_t, std::vector<GeoListDelta>> geo_deltas;
    struct CellCorr { uint64_t cell_id; std::vector<uint32_t> ids; };
    std::unordered_map<uint32_t, std::vector<CellCorr>> entry_corrections;
    // CELL_INDEX_DELTA payloads by entries file id (admin / POI / place).
    std::unordered_map<uint32_t, std::vector<char>> cell_index_deltas;
    // POI parent-id remap (from POI_PARENT_REMAP_MARKER). Applied during
    // POI_RECORDS MATCH replay to bytes 24/28/32 of each record alongside
    // the str_remap on byte 16. Sorted by old_id for binary-search lookup.
    std::vector<std::pair<uint32_t,uint32_t>> poi_admin_remap;
    std::vector<std::pair<uint32_t,uint32_t>> poi_street_remap;
    auto poi_remap_lookup = [](const std::vector<std::pair<uint32_t,uint32_t>>& v,
                               uint32_t old_id) -> uint32_t {
        if (v.empty()) return old_id;
        auto it = std::lower_bound(v.begin(), v.end(),
            std::make_pair(old_id, (uint32_t)0));
        if (it != v.end() && it->first == old_id) return it->second;
        return old_id;
    };

    bool saw_end = false;
    while (pos < patch_size) {
        uint32_t file_id = ru32();
        if (file_id == SECTION_END_MARKER) { saw_end = true; break; }

        // --- Metadata sections ---
        if (file_id == CELL_CHANGES_GEO_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            require_bytes(((size_t)na + nr) * 8, "cell changes");
            geo_added.resize(na); geo_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&geo_added[i], take(8, "cell id"), 8); }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&geo_removed[i], take(8, "cell id"), 8); }
            std::cerr << "  Geo cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_ADMIN_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            require_bytes(((size_t)na + nr) * 8, "cell changes");
            admin_added.resize(na); admin_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&admin_added[i], take(8, "cell id"), 8); }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&admin_removed[i], take(8, "cell id"), 8); }
            std::cerr << "  Admin cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_POI_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            require_bytes(((size_t)na + nr) * 8, "cell changes");
            poi_added.resize(na); poi_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&poi_added[i], take(8, "cell id"), 8); }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&poi_removed[i], take(8, "cell id"), 8); }
            std::cerr << "  POI cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == CELL_CHANGES_PLACE_MARKER) {
            uint32_t na = ru32(), nr = ru32();
            require_bytes(((size_t)na + nr) * 8, "cell changes");
            place_added.resize(na); place_removed.resize(nr);
            for (uint32_t i = 0; i < na; i++) { memcpy(&place_added[i], take(8, "cell id"), 8); }
            for (uint32_t i = 0; i < nr; i++) { memcpy(&place_removed[i], take(8, "cell id"), 8); }
            std::cerr << "  Place cells: +" << na << " -" << nr << std::endl;
            continue;
        }
        if (file_id == GEO_ENTRY_DELTA_MARKER) {
            uint32_t fid = ru32(), c = ru32();
            geo_deltas[fid] = parse_geo_list_deltas(P, patch_size, pos, c);
            std::cerr << "  Geo entry deltas " << fid << ": " << c << " cells" << std::endl;
            continue;
        }
        if (file_id == SECONDARY_REMAP_MARKER) {
            // Secondary matches extend the remap of a file already replayed
            // (the diff emits them after the merges); a file sent unchanged
            // has no remap and keeps its ids.
            uint32_t nf = ru32();
            for (uint32_t f = 0; f < nf; f++) {
                uint32_t fid = ru32(), np = ru32();
                require_bytes((size_t)np * 8, "secondary remap");
                auto it = record_remaps.find(fid);
                for (uint32_t i = 0; i < np; i++) {
                    uint32_t o = ru32(), n = ru32();
                    if (it != record_remaps.end()) it->second.add_pair(o, n);
                }
                std::cerr << "  Secondary remap " << fid << ": " << np << " pairs" << std::endl;
            }
            continue;
        }
        if (file_id == POI_PARENT_REMAP_MARKER) {
            // Format: n_admin(u32), [(o,n):u32]*na, n_street(u32), [(o,n):u32]*ns,
            //         n_postcode(u32), [(o,n):u32]*np
            uint32_t na = ru32();
            require_bytes((size_t)na * 8, "admin remap");
            poi_admin_remap.resize(na);
            for (uint32_t i = 0; i < na; i++) {
                uint32_t o = ru32(), n = ru32();
                poi_admin_remap[i] = {o, n};
            }
            uint32_t ns = ru32();
            require_bytes((size_t)ns * 8, "street remap");
            poi_street_remap.resize(ns);
            for (uint32_t i = 0; i < ns; i++) {
                uint32_t o = ru32(), n = ru32();
                poi_street_remap[i] = {o, n};
            }
            // Reserved postcode leg — always 0 pairs on the wire; skip.
            uint32_t np = ru32();
            for (uint32_t i = 0; i < np; i++) { ru32(); ru32(); }
            std::sort(poi_admin_remap.begin(), poi_admin_remap.end());
            std::sort(poi_street_remap.begin(), poi_street_remap.end());
            std::cerr << "  POI parent-id remap: admin_pairs=" << na
                      << " street_pairs=" << ns
                      << " postcode_pairs=" << np << std::endl;
            continue;
        }
        if (file_id == CELL_INDEX_DELTA_MARKER) {
            uint32_t fid = ru32();
            uint64_t n = ru64();
            const char* p = take(n, "cell index delta");
            cell_index_deltas[fid].assign(p, p + n);
            std::cerr << "  Cell index delta " << fid << ": " << n << " bytes" << std::endl;
            continue;
        }
        if (file_id == ENTRY_CORRECTION_MARKER) {
            uint32_t fid = ru32(), c = ru32();
            auto& list = entry_corrections[fid];
            for (uint32_t i = 0; i < c; i++) {
                uint64_t cid; memcpy(&cid, take(8, "cell id"), 8);
                uint16_t ec; memcpy(&ec, take(2, "entry count"), 2);
                std::vector<uint32_t> ids(ec);
                if (ec > 0) memcpy(ids.data(), take((size_t)ec * 4, "entry ids"), (size_t)ec * 4);
                list.push_back({cid, std::move(ids)});
            }
            std::cerr << "  Entry corrections " << fid << ": " << c << std::endl;
            continue;
        }

        // --- Merge sequence replay (streaming) ---
        const IoCounters section_start = get_io_counters();
        auto section_io = [&] { return " [" + io_delta(section_start, get_io_counters()) + "]"; };
        uint32_t stride = ru32();
        uint64_t old_size = ru64(), new_size = ru64();
        if (file_id >= (uint32_t)PatchFileId::COUNT) { std::cerr << "Unknown file " << file_id << std::endl; return 1; }
        const char* fname = patch_file_names[file_id];

        if (stride == 0) {
            // Full replacement — write directly from patch mmap
            uint32_t nf = ru32(); (void)nf; uint64_t ds = ru64();
            const char* data = take(ds, "full replacement");
            if (!write_file(out_path(fname), data, ds)) throw std::runtime_error(std::string("Cannot write ") + fname);
            std::cerr << "  " << fname << ": full replace " << ds << " bytes" << std::endl;
            continue;
        }
        if (stride == COPY_OLD_STRIDE) {
            // Unchanged file: the diff verified old==new and emitted no
            // data. Reproduce by copying the old (current) file verbatim.
            uint32_t nf = ru32(); (void)nf; take(ru64(), "copy-old payload"); // empty
            SequentialFileReader in(cur_dir + "/" + fname);
            if (!in.is_open()) {
                // The diff only emits COPY_OLD for a file that existed (and was
                // byte-identical) at diff time, so a missing source here is a
                // real error — surface it loudly instead of silently writing a
                // truncated/empty file and logging a false success.
                std::cerr << "  ERROR: " << fname << ": copy-old failed (src MISSING)" << std::endl;
                return 1;
            }
            FILE* out = open_out(fname);
            size_t written = 0;
            in.stream(0, in.size(), [&](const char* p, size_t n) { written += fwrite(p, 1, n, out); });
            fclose(out);
            std::cerr << "  " << fname << ": unchanged (copied " << written
                      << " bytes from old)" << section_io() << std::endl;
            continue;
        }
        if (stride == LEGACY_SKIP_STRIDE) { uint32_t nf = ru32(); (void)nf; take(ru64(), "skipped section"); continue; }
        if (stride == CELL_LIST_DELTA_STRIDE) {
            // A cell index patched per cell; the paired entries file is
            // rebuilt from the same lists.
            std::string cells_name = fname;
            const std::string suffix = "_cells.bin";
            if (cells_name.size() < suffix.size() || cells_name.compare(cells_name.size() - suffix.size(), suffix.size(), suffix) != 0)
                throw std::runtime_error("Cell list delta for " + cells_name);
            std::string entries_name = cells_name.substr(0, cells_name.size() - suffix.size()) + "_entries.bin";
            uint64_t payload_size = ru64();
            if (payload_size < 8) throw std::runtime_error("Malformed cell list delta");
            const char* payload = take(payload_size, "cell list delta");
            uint64_t new_entries_size; memcpy(&new_entries_size, payload, 8);
            SequentialFileReader old_c(cur_dir + "/" + cells_name);
            SequentialFileReader old_e(cur_dir + "/" + entries_name);
            FILE* fc = open_out(cells_name);
            FILE* fe = open_out(entries_name);
            uint64_t cells_written = 0;
            uint64_t entries_written = stream_cell_list_delta(
                old_c, old_e, payload + 8, payload_size - 8,
                [&](const char* p, size_t n) { fwrite(p, 1, n, fc); cells_written += n; },
                [&](const char* p, size_t n) { fwrite(p, 1, n, fe); });
            bool ok = !ferror(fc) && !ferror(fe);
            fclose(fc); fclose(fe);
            log_rewinds(old_c);
            log_rewinds(old_e);
            if (!ok) throw std::runtime_error("Cannot write " + cells_name);
            if (cells_written != new_size || entries_written != new_entries_size)
                throw std::runtime_error("Rebuilt " + cells_name + " does not match the new build");
            std::cerr << "  " << cells_name << " + " << entries_name << ": cell list delta, "
                      << cells_written / 12 << " cells" << section_io() << std::endl;
            continue;
        }
        if (stride == SPARSE_DELTA_STRIDE) {
            // SPARSE_DELTA: position-keyed delta. Format already decoded
            // old_size + new_size above; next fields are value_stride,
            // remap_kind, n_changes, then the (pos, value) pairs.
            uint32_t value_stride = ru32();
            uint32_t remap_kind = ru32();
            uint32_t n_changes = ru32();
            if (value_stride != 4 && value_stride != 16) throw std::runtime_error("Malformed sparse delta");
            const size_t change_size = 4 + value_stride;
            const char* changes = take((size_t)n_changes * change_size, "sparse changes");

            // OLD bytes with the same remap the diff applied before computing
            // the delta, then each (pos, value) pair overwritten, give NEW.
            // Streamed through a 1 MiB buffer (the changes come sorted by
            // position): holding the whole file cost ~700 MiB of client
            // memory for planet addr_postcodes.
            SequentialFileReader old(cur_dir + "/" + std::string(fname));
            const size_t copy_n = std::min<uint64_t>({old_size, new_size, old.size()});
            constexpr uint32_t NO_DATA_VAL = 0xFFFFFFFFu;
            auto remap_value = [&](uint32_t v) -> uint32_t {
                if (v == NO_DATA_VAL) return v;
                if (remap_kind == 1) return poi_remap_lookup(poi_admin_remap, v);
                return str_remap_lookup(v);
            };
            // Field of each record the remap applies to (postcode_id at
            // byte 8 of a 16-byte postcode_centroid), or none.
            const bool remapped = (remap_kind == 1 && !poi_admin_remap.empty() && value_stride == 4) ||
                                  (remap_kind == 2 && !str_remap.empty() && value_stride == 4) ||
                                  (remap_kind == 3 && !str_remap.empty() && value_stride == 16);
            const size_t field = value_stride == 16 ? 8 : 0;

            FILE* out = open_out(fname);
            constexpr size_t CHUNK = 1 << 20;  // a multiple of both value strides
            std::vector<char> buf(CHUNK);
            uint32_t ci = 0;
            uint64_t prev_pos = 0;
            for (size_t at = 0; at < (size_t)new_size; at += CHUNK) {
                size_t n = std::min(CHUNK, (size_t)new_size - at);
                size_t from_old = at < copy_n ? std::min(n, copy_n - at) : 0;
                old.read(at, buf.data(), from_old);
                if (from_old < n) memset(buf.data() + from_old, 0, n - from_old);
                if (remapped) {
                    for (size_t r = 0; r + value_stride <= from_old; r += value_stride) {
                        uint32_t v; memcpy(&v, buf.data() + r + field, 4);
                        uint32_t nv = remap_value(v);
                        if (nv != v) memcpy(buf.data() + r + field, &nv, 4);
                    }
                }
                for (; ci < n_changes; ci++) {
                    uint32_t arr_pos; memcpy(&arr_pos, changes + (size_t)ci * change_size, 4);
                    if (ci > 0 && arr_pos <= prev_pos) throw std::runtime_error("Malformed sparse delta");
                    size_t byte_off = (size_t)arr_pos * value_stride;
                    if (byte_off >= at + n) break;
                    prev_pos = arr_pos;
                    memcpy(buf.data() + (byte_off - at), changes + (size_t)ci * change_size + 4, value_stride);
                }
                fwrite(buf.data(), 1, n, out);
            }
            bool ok = !ferror(out);
            fclose(out);
            if (!ok) throw std::runtime_error(std::string("Cannot write ") + fname);

            std::cerr << "  " << fname << ": sparse delta " << n_changes
                      << " changes (" << new_size << " bytes)" << section_io() << std::endl;
            continue;
        }

        size_t actual_stride = stride;
        if (file_id == (uint32_t)PatchFileId::STREET_WAYS) actual_stride = way_stride;
        else if (file_id == (uint32_t)PatchFileId::INTERP_WAYS) actual_stride = interp_stride;
        else if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS) actual_stride = admin_stride;

        // Determine if this file needs in-memory modifications
        bool needs_remap = (file_id == (uint32_t)PatchFileId::ADDR_POINTS ||
                            file_id == (uint32_t)PatchFileId::STREET_WAYS ||
                            file_id == (uint32_t)PatchFileId::INTERP_WAYS ||
                            file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS ||
                            file_id == (uint32_t)PatchFileId::POSTAL_POLYGONS ||
                            file_id == (uint32_t)PatchFileId::POI_RECORDS ||
                            file_id == (uint32_t)PatchFileId::PLACE_NODES);
        bool needs_padding = file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS && actual_stride == 24;

        // Offset fixups, decoded lazily during merge replay (zero allocation).
        uint32_t n_fixup_runs = ru32(), n_fixup_values = ru32();
        uint32_t fixup_runs_size = ru32(), fixup_values_size = ru32();
        const char* fixup_runs = take(fixup_runs_size, "fixup runs");
        const char* fixup_values = take(fixup_values_size, "fixup values");
        OffsetFixupReader fixups(fixup_runs, fixup_runs_size, n_fixup_runs,
                                 fixup_values, fixup_values_size, n_fixup_values);
        const bool has_fixups = n_fixup_runs > 0 || n_fixup_values > 0;
        // node_offset / vertex_offset: byte 0 for most types, byte 8 for POI
        // records, byte 20 for AddrPoint (after lat/lng/hn_id/st_id/pw_id).
        const size_t fixup_off =
            (file_id == (uint32_t)PatchFileId::POI_RECORDS) ? POI_RECORD_VERTEX_OFFSET_OFF :
            (file_id == (uint32_t)PatchFileId::ADDR_POINTS)  ? ADDR_POINT_VERTEX_OFFSET_OFF : (size_t)0;
        if (has_fixups && fixup_off + 4 > actual_stride) throw std::runtime_error("Malformed fixups");

        // Get string remap field offsets for this file type
        std::vector<size_t> remap_offs;
        if (!str_remap.empty() && needs_remap) {
            // AddrPoint string fields live at {8, 12} (housenumber_id,
            // street_id). Offset 16 is parent_way_id, which is a WAY id
            // — not a string pool offset — so it must NOT be rewritten
            // by the string remap. The previous {8, 12, 16} mapping
            // corrupted parent_way_id for every record that sat in a
            // MATCH run, producing the "first_diff=17" (first byte of
            // parent_way_id) mismatches we saw in patch verify.
            if (file_id == (uint32_t)PatchFileId::ADDR_POINTS) remap_offs = {ADDR_POINT_HOUSENUMBER_ID_OFF, ADDR_POINT_STREET_ID_OFF};
            else if (file_id == (uint32_t)PatchFileId::STREET_WAYS) remap_offs = {(actual_stride == 12) ? WAY_HEADER_NAME_ID_OFF_PADDED : WAY_HEADER_NAME_ID_OFF_PACKED};
            else if (file_id == (uint32_t)PatchFileId::INTERP_WAYS) remap_offs = {(actual_stride >= 20) ? INTERP_WAY_STREET_ID_OFF_PADDED : INTERP_WAY_STREET_ID_OFF_PACKED};
            else if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS ||
                     file_id == (uint32_t)PatchFileId::POSTAL_POLYGONS) remap_offs = {ADMIN_POLYGON_NAME_ID_OFF};
            else if (file_id == (uint32_t)PatchFileId::POI_RECORDS) {
                // byte 16 = name_id; byte 24 = parent_street_id;
                // byte 28 = parent_postcode_id — all string offsets, all
                // remapped via str_remap (mirrors geocoder_diff.cpp).
                remap_offs = {POI_RECORD_NAME_ID_OFF};
                if (actual_stride >= 28) remap_offs.push_back(POI_RECORD_PARENT_STREET_ID_OFF);
                if (actual_stride >= 32) remap_offs.push_back(POI_RECORD_PARENT_POSTCODE_ID_OFF);
            }
            else if (file_id == (uint32_t)PatchFileId::PLACE_NODES) remap_offs = {PLACE_NODE_NAME_ID_OFF};
        }

        // The old file, read front to back (DELETE runs skip forward).
        SequentialFileReader old(cur_dir + "/" + std::string(fname));
        size_t n_old_records = old.size() / actual_stride;

        // Replay merge sequence — stream output, apply remap/fixups per-record inline
        uint64_t seq_size = ru64();
        require_bytes(seq_size, "merge sequence");
        size_t seq_end = pos + seq_size;
        FILE* outf = open_out(fname);

        // Record the old → new record ids of the files whose remap the entry
        // pipeline reads back (geo, POI and place indexes).
        bool track = file_id == (uint32_t)PatchFileId::STREET_WAYS ||
                     file_id == (uint32_t)PatchFileId::ADDR_POINTS ||
                     file_id == (uint32_t)PatchFileId::INTERP_WAYS ||
                     file_id == (uint32_t)PatchFileId::POI_RECORDS ||
                     file_id == (uint32_t)PatchFileId::PLACE_NODES;
        RecordRemap remap(track ? static_cast<uint32_t>(n_old_records) : 0);

        // Per-record buffer for applying remap/fixups inline (avoid copying entire file)
        std::vector<char> rec_buf(actual_stride);

        size_t old_rec = 0, new_rec = 0, old_bytes = 0, written = 0;
        while (pos < seq_end) {
            if (pos + 5 > seq_end) throw std::runtime_error(std::string("Malformed merge sequence: ") + fname);
            uint8_t op = P[pos++];
            uint32_t count; memcpy(&count, P+pos, 4); pos += 4;
            if (op == OP_MATCH_RUN) {
                // Fast path: files with no per-record transform (the
                // *_vertices byte streams, street_nodes / interp_nodes)
                // copy the whole run from old in window-sized fwrites. The
                // record loop below costs one fwrite per record: one per
                // byte of planet/full's 3.4 GiB addr_vertices, one per node
                // of its 600M street_nodes.
                size_t run_bytes = (size_t)count * actual_stride;
                if (!needs_remap && !needs_padding && remap_offs.empty() && !has_fixups
                    && old_bytes + run_bytes <= old.size()) {
                    old.stream(old_bytes, run_bytes, [&](const char* p, size_t n) { fwrite(p, 1, n, outf); });
                    written += run_bytes;
                    old_rec += count; new_rec += count; old_bytes += run_bytes;
                    continue;
                }
                for (uint32_t k = 0; k < count; k++) {
                    size_t rec_off = old_bytes + k * actual_stride;
                    if (rec_off + actual_stride > old.size()) break;
                    const char* rec = old.at(rec_off, actual_stride);

                    bool modified = false;
                    // Check if this record needs any modification
                    if (!remap_offs.empty() || needs_padding) modified = true;
                    // POI records: byte-32 parent_poly_id shifts with the
                    // admin id-space. (name_id/parent_street_id/parent_postcode_id
                    // are string offsets handled by remap_offs, which already
                    // set modified=true above.)
                    if (file_id == (uint32_t)PatchFileId::POI_RECORDS &&
                        !poi_admin_remap.empty())
                        modified = true;
                    // PlaceNode parent_poly_id (byte 16) shifts with admin id-space.
                    if (file_id == (uint32_t)PatchFileId::PLACE_NODES &&
                        actual_stride >= 20 && !poi_admin_remap.empty())
                        modified = true;
                    // AddrPoint parent_way_id (byte 16) shifts with street id-space.
                    if (file_id == (uint32_t)PatchFileId::ADDR_POINTS &&
                        actual_stride >= 20 && !poi_street_remap.empty())
                        modified = true;
                    uint32_t old_off = 0, new_off = 0;
                    if (has_fixups) {
                        memcpy(&old_off, rec + fixup_off, 4);
                        new_off = fixups.apply(static_cast<uint32_t>(old_rec + k), old_off);
                    }
                    bool has_fixup = new_off != old_off;
                    if (has_fixup) modified = true;

                    if (modified) {
                        memcpy(rec_buf.data(), rec, actual_stride);
                        // Apply padding zeroing
                        if (file_id == (uint32_t)PatchFileId::ADMIN_POLYGONS && actual_stride == 24)
                            memset(rec_buf.data() + 14, 0, 2); // preserve place_type_override at byte 13
                        // Apply string remap
                        for (size_t off : remap_offs) {
                            uint32_t v; memcpy(&v, rec_buf.data() + off, 4);
                            uint32_t nv = str_remap_lookup(v);
                            if (nv != v) memcpy(rec_buf.data() + off, &nv, 4);
                        }
                        // Apply POI parent_poly_id remap (byte 32, admin
                        // polygon index). Bytes 24/28 (parent_street_id,
                        // parent_postcode_id) are string offsets and are
                        // handled by the str_remap pass via remap_offs
                        // above — NOT here. Mirrors geocoder_diff.cpp.
                        if (file_id == (uint32_t)PatchFileId::POI_RECORDS) {
                            constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
                            if (actual_stride >= 36 && !poi_admin_remap.empty()) {
                                uint32_t pp; memcpy(&pp, rec_buf.data() + POI_RECORD_PARENT_POLY_ID_OFF, 4);
                                if (pp != NO_DATA) {
                                    uint32_t npp = poi_remap_lookup(poi_admin_remap, pp);
                                    if (npp != pp) memcpy(rec_buf.data() + POI_RECORD_PARENT_POLY_ID_OFF, &npp, 4);
                                }
                            }
                        }
                        // PlaceNode parent_poly_id at byte 16 (20-byte stride).
                        if (file_id == (uint32_t)PatchFileId::PLACE_NODES &&
                            actual_stride >= 20 && !poi_admin_remap.empty()) {
                            constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
                            uint32_t pp; memcpy(&pp, rec_buf.data() + PLACE_NODE_PARENT_POLY_ID_OFF, 4);
                            if (pp != NO_DATA) {
                                uint32_t npp = poi_remap_lookup(poi_admin_remap, pp);
                                if (npp != pp) memcpy(rec_buf.data() + PLACE_NODE_PARENT_POLY_ID_OFF, &npp, 4);
                            }
                        }
                        // AddrPoint parent_way_id at byte 16 (20+ stride).
                        if (file_id == (uint32_t)PatchFileId::ADDR_POINTS &&
                            actual_stride >= 20 && !poi_street_remap.empty()) {
                            constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
                            uint32_t pw; memcpy(&pw, rec_buf.data() + ADDR_POINT_PARENT_WAY_ID_OFF, 4);
                            if (pw != NO_DATA) {
                                uint32_t npw = poi_remap_lookup(poi_street_remap, pw);
                                if (npw != pw) memcpy(rec_buf.data() + ADDR_POINT_PARENT_WAY_ID_OFF, &npw, 4);
                            }
                        }
                        if (has_fixup) memcpy(rec_buf.data() + fixup_off, &new_off, 4);
                        fwrite(rec_buf.data(), 1, actual_stride, outf);
                    } else {
                        // No modification needed: the record as read
                        fwrite(rec, 1, actual_stride, outf);
                    }
                }
                if (track) remap.add_match(static_cast<uint32_t>(old_rec), static_cast<uint32_t>(new_rec), count);
                written += count * actual_stride;
                old_rec += count; new_rec += count; old_bytes += count * actual_stride;
            } else if (op == OP_INSERT_RUN) {
                size_t bytes = count * actual_stride;
                if (pos + bytes > seq_end) throw std::runtime_error(std::string("Malformed merge sequence: ") + fname);
                fwrite(P+pos, 1, bytes, outf);
                written += bytes; pos += bytes; new_rec += count;
            } else if (op == OP_DELETE_RUN) {
                old_rec += count; old_bytes += count * actual_stride;
            } else {
                throw std::runtime_error(std::string("Malformed merge sequence: ") + fname);
            }
        }
        fclose(outf);
        log_rewinds(old);

        if (track) {
            record_remaps[file_id] = std::move(remap);
            std::cerr << "  " << fname << ": " << written << " bytes (remap of " << n_old_records << " records)"
                      << section_io() << std::endl;
        } else {
            std::cerr << "  " << fname << ": " << written << " bytes" << section_io() << std::endl;
        }
        // Release patch pages used by this section
        madvise(const_cast<char*>(patch_map.data), pos, MADV_DONTNEED);
        malloc_trim(0);
    }
    if (!saw_end) throw std::runtime_error("Truncated patch: no end marker");
    log_phase("Merge replays", t_start);

    // Free string remap + release all processed patch pages
    str_remap = StringRemap();
    for (auto& m : old_str_pools) if (m.data) unmap_file(m);
    old_str_pools.clear();
    madvise(const_cast<char*>(patch_map.data), pos, MADV_DONTNEED);
    malloc_trim(0);

    // --- Phase 4: Streaming entry pipeline ---
    // Walk old geo_cells, remap IDs, apply corrections, write output directly to files.
    {
        double t_entry = now_ms();
        std::cerr << "Entry pipeline (lean streaming)..." << std::endl;

        // A file sent unchanged has no remap: its ids keep their values.
        for (auto& kv : record_remaps) kv.second.finish();
        const RecordRemap no_remap;
        auto remap_of = [&](PatchFileId fid) -> const RecordRemap& {
            auto it = record_remaps.find((uint32_t)fid);
            return it != record_remaps.end() ? it->second : no_remap;
        };
        const RecordRemap& w_rm = remap_of(PatchFileId::STREET_WAYS);
        const RecordRemap& a_rm = remap_of(PatchFileId::ADDR_POINTS);
        const RecordRemap& i_rm = remap_of(PatchFileId::INTERP_WAYS);

        // The old geo index and its entries files, each read front to back:
        // the builder writes every list in cell order, removed cells and
        // NO_DATA lists only leave gaps.
        SequentialFileReader old_geo(cur_dir + "/geo_cells.bin");
        SequentialFileReader old_se(cur_dir + "/street_entries.bin");
        SequentialFileReader old_ae(cur_dir + "/addr_entries.bin");
        SequentialFileReader old_ie(cur_dir + "/interp_entries.bin");
        size_t n_old = old_geo.size() / 20;

        // Corrections and removed cells, sorted by cell id and read through
        // forward cursors: the walk below visits cells in ascending id
        // order, and a hash lookup plus three binary searches per cell cost
        // ~17 s of client CPU on planet's ~380M cells.
        std::sort(geo_removed.begin(), geo_removed.end());
        size_t removed_i = 0;
        auto is_removed = [&](uint64_t cid) {
            while (removed_i < geo_removed.size() && geo_removed[removed_i] < cid) removed_i++;
            return removed_i < geo_removed.size() && geo_removed[removed_i] == cid;
        };
        // The diff's per-cell corrections (sorted by cell id when parsed).
        struct DeltaCursor {
            const std::vector<GeoListDelta>& v;
            size_t i = 0;
            const GeoListDelta* at(uint64_t cid) {
                while (i < v.size() && v[i].cell_id < cid) i++;
                return i < v.size() && v[i].cell_id == cid ? &v[i] : nullptr;
            }
        };
        const std::vector<GeoListDelta> no_deltas;
        auto deltas_of = [&](PatchFileId fid) -> const std::vector<GeoListDelta>& {
            auto it = geo_deltas.find((uint32_t)fid);
            return it != geo_deltas.end() ? it->second : no_deltas;
        };
        DeltaCursor cur_s{deltas_of(PatchFileId::STREET_ENTRIES)};
        DeltaCursor cur_a{deltas_of(PatchFileId::ADDR_ENTRIES)};
        DeltaCursor cur_i{deltas_of(PatchFileId::INTERP_ENTRIES)};
        std::vector<uint32_t> scratch;
        // Added cells sorted
        std::sort(geo_added.begin(), geo_added.end());
        log_phase("  Setup", t_entry);

        // Open 4 output files
        FILE* f_geo = open_out("geo_cells.bin");
        struct EntriesOut { FILE* f; uint64_t written; };
        EntriesOut o_se{open_out("street_entries.bin"), 0};
        EntriesOut o_ae{open_out("addr_entries.bin"), 0};
        EntriesOut o_ie{open_out("interp_entries.bin"), 0};
        constexpr uint32_t NO = 0xFFFFFFFF;

        // Reusable buffer (one per entry type to avoid aliasing issues)
        std::vector<uint32_t> buf;
        buf.reserve(4096);

        auto remap = [](std::vector<uint32_t>& ids, const RecordRemap& rm) {
            constexpr uint32_t NO2 = 0xFFFFFFFF;
            for (auto& id : ids) if (id < rm.size() && rm[id] != NO2) id = rm[id];
            std::sort(ids.begin(), ids.end());
        };
        // Write entry and return offset, or NO if empty. Offsets come from a
        // running count per output: ftell per list cost ~40 s of client CPU
        // on planet (~460M lists).
        auto emit = [&NO](EntriesOut& out, const uint32_t* ids, size_t n) -> uint32_t {
            if (n == 0) return NO;
            uint32_t off = (uint32_t)out.written;
            uint16_t c = (uint16_t)n;
            fwrite(&c, 2, 1, out.f); fwrite(ids, 4, n, out.f);
            out.written += 2 + n * 4;
            return off;
        };

        // Merge-walk: old cells + added cells in sorted order
        // Both are sorted by cell_id. Merge them, skip removed.
        size_t old_i = 0, add_i = 0;
        size_t cells_written = 0;

        // The cell's derived list (old list, ids remapped), then its
        // correction from the diff, if it sent one.
        auto do_entry = [&](uint64_t cid, SequentialFileReader& old_e, uint32_t old_off, const RecordRemap& rm,
                            DeltaCursor& deltas, EntriesOut& outf) -> uint32_t {
            read_entry_list(old_e, old_off, buf);
            if (!buf.empty()) remap(buf, rm);
            if (const GeoListDelta* d = deltas.at(cid)) d->apply(buf, scratch);
            if (buf.empty()) return NO;
            return emit(outf, buf.data(), buf.size());
        };
        // old_offs: the cell's street / addr / interp list offsets in the
        // old entries files, NO for a new cell.
        using ListOffsets = std::array<uint32_t, 3>;
        auto process_cell = [&](uint64_t cid, const ListOffsets& old_offs) {
            uint32_t so = do_entry(cid, old_se, old_offs[0], w_rm, cur_s, o_se);
            uint32_t ao = do_entry(cid, old_ae, old_offs[1], a_rm, cur_a, o_ae);
            uint32_t io = do_entry(cid, old_ie, old_offs[2], i_rm, cur_i, o_ie);
            fwrite(&cid, 8, 1, f_geo); fwrite(&so, 4, 1, f_geo); fwrite(&ao, 4, 1, f_geo); fwrite(&io, 4, 1, f_geo);
            cells_written++;
        };
        const ListOffsets added_cell = {NO, NO, NO};

        while (old_i < n_old || add_i < geo_added.size()) {
            uint64_t old_cid = UINT64_MAX, add_cid = UINT64_MAX;
            ListOffsets old_offs = added_cell;
            if (old_i < n_old) {
                // One 20-byte record: cell id, then the three list offsets.
                // size_t: planet has ~380M cells and `old_i * 20` passes 2^31.
                const char* rec = old_geo.at((uint64_t)old_i * 20, 20);
                memcpy(&old_cid, rec, 8);
                memcpy(old_offs.data(), rec + 8, 12);
            }
            if (add_i < geo_added.size()) add_cid = geo_added[add_i];

            if (old_cid <= add_cid) {
                if (!is_removed(old_cid))
                    process_cell(old_cid, old_offs);
                old_i++;
                if (old_cid == add_cid) add_i++; // skip duplicate add
            } else {
                process_cell(add_cid, added_cell);
                add_i++;
            }

            if (cells_written % 10000000 == 0 && cells_written > 0) {
                std::cerr << "    " << cells_written << " cells, se=" << o_se.written/1024/1024
                          << "M ae=" << o_ae.written/1024/1024 << "M anon=" << get_rss_anon_mb() << "M" << std::endl;
            }
        }

        fclose(f_geo); fclose(o_se.f); fclose(o_ae.f); fclose(o_ie.f);
        for (const SequentialFileReader* r : {&old_geo, &old_se, &old_ae, &old_ie}) log_rewinds(*r);
        std::cerr << "  Geo: " << cells_written << " cells written" << std::endl;
        malloc_trim(0);
        log_phase("  Geo entries complete", t_entry);

        // Writes a rebuilt cell index; a cell the diff corrected takes the
        // shipped id list instead of its rebuilt one.
        auto write_corrected_cells = [&](const char* label, PatchFileId entries_fid,
                                         const std::vector<char>& cells, const std::vector<char>& entries,
                                         const std::string& cells_name, const std::string& entries_name) {
            constexpr uint32_t no_data = 0xFFFFFFFF;
            size_t n = cells.size() / 12;
            auto ecit = entry_corrections.find((uint32_t)entries_fid);
            if (ecit == entry_corrections.end()) {
                write_file(out_path(cells_name), cells);
                write_file(out_path(entries_name), entries);
                std::cerr << "  " << label << ": " << n << " cells rebuilt" << std::endl;
                return;
            }
            std::unordered_map<uint64_t, const std::vector<uint32_t>*> corr;
            for (auto& c : ecit->second) corr[c.cell_id] = &c.ids;
            FILE* fc = open_out(cells_name);
            FILE* fe = open_out(entries_name);
            uint32_t wpos = 0;
            for (size_t i = 0; i < n; i++) {
                uint64_t cid; memcpy(&cid, cells.data() + i * 12, 8);
                fwrite(&cid, 8, 1, fc);
                auto cit = corr.find(cid);
                if (cit != corr.end()) {
                    uint32_t off = cit->second->empty() ? no_data : wpos;
                    fwrite(&off, 4, 1, fc);
                    if (!cit->second->empty()) {
                        uint16_t c = cit->second->size();
                        fwrite(&c, 2, 1, fe); fwrite(cit->second->data(), 4, c, fe);
                        wpos += 2 + c * 4;
                    }
                } else {
                    uint32_t old_off; memcpy(&old_off, cells.data() + i * 12 + 8, 4);
                    if (old_off != no_data && old_off + 2 <= entries.size()) {
                        fwrite(&wpos, 4, 1, fc);
                        uint16_t c; memcpy(&c, entries.data() + old_off, 2);
                        fwrite(entries.data() + old_off, 1, 2 + c * 4, fe);
                        wpos += 2 + c * 4;
                    } else {
                        fwrite(&no_data, 4, 1, fc);
                    }
                }
            }
            fclose(fc); fclose(fe);
            std::cerr << "  " << label << ": " << n << " cells, " << corr.size() << " corrections" << std::endl;
        };

        // An admin / POI / place index the diff sent a cell index delta for
        // (every current build) is rebuilt and corrected in one streaming
        // pass over the old files, holding one cell at a time. Returns false
        // when there is no delta and the materialised rebuild + corrections
        // has to run instead.
        auto stream_rebuild = [&](const char* label, PatchFileId entries_fid, const std::string& prefix,
                                  const std::vector<uint64_t>& added, const std::vector<uint64_t>& removed,
                                  auto remap) -> bool {
            auto dit = cell_index_deltas.find((uint32_t)entries_fid);
            if (dit == cell_index_deltas.end()) return false;
            SequentialFileReader old_c(cur_dir + "/" + prefix + "_cells.bin");
            SequentialFileReader old_e(cur_dir + "/" + prefix + "_entries.bin");
            FILE* fc = open_out(prefix + "_cells.bin");
            FILE* fe = open_out(prefix + "_entries.bin");
            size_t n_out = 0;
            stream_cell_index(old_c, old_e, added, removed, remap,
                              dit->second.data(), dit->second.size(),
                              [&](const char* p, size_t len) { fwrite(p, 1, len, fc); n_out += len; },
                              [&](const char* p, size_t len) { fwrite(p, 1, len, fe); });
            bool ok = !ferror(fc) && !ferror(fe);
            fclose(fc); fclose(fe);
            log_rewinds(old_c);
            log_rewinds(old_e);
            if (!ok) throw std::runtime_error("Cannot write " + prefix + "_cells.bin");
            std::cerr << "  " << label << ": " << n_out / 12 << " cells, cell index delta" << std::endl;
            return true;
        };

        // Admin: small, use existing rebuild + corrections
        {
            // The admin leg of the parent-id remap, as the diff used it.
            auto admin_remap = [&](uint32_t id) { return poi_remap_lookup(poi_admin_remap, id); };
            if (!stream_rebuild("Admin", PatchFileId::ADMIN_ENTRIES, "admin", admin_added, admin_removed, admin_remap)) {
                std::unordered_map<uint32_t,uint32_t> ad_rm(poi_admin_remap.begin(), poi_admin_remap.end());
                auto old_ac = read_file(cur_dir + "/admin_cells.bin");
                auto old_adme = read_file(cur_dir + "/admin_entries.bin");
                auto admin = rebuild_cells_from_remap(old_ac, old_adme, ad_rm, admin_added, admin_removed);
                write_corrected_cells("Admin", PatchFileId::ADMIN_ENTRIES, admin.cells_data,
                                      admin.entries_data, "admin_cells.bin", "admin_entries.bin");
            }
            log_phase("  Admin", t_entry);
        }

        // POI and place: ids move by the replay's record remap file, read in
        // place by the streaming pass (by id, so not as a sequential stream).
        auto rebuild_by_record_remap = [&](const char* label, PatchFileId records_fid, PatchFileId entries_fid,
                                           const std::string& prefix, const std::vector<uint64_t>& added,
                                           const std::vector<uint64_t>& removed) {
            const RecordRemap& rm = remap_of(records_fid);
            auto record_remap = [&rm](uint32_t id) { uint32_t v = rm[id]; return v == 0xFFFFFFFFu ? id : v; };
            if (!stream_rebuild(label, entries_fid, prefix, added, removed, record_remap)) {
                std::unordered_map<uint32_t,uint32_t> rm_map;
                for (uint32_t i = 0; i < rm.size(); i++)
                    if (rm[i] != 0xFFFFFFFFu) rm_map[i] = rm[i];
                auto old_c = read_file(cur_dir + "/" + prefix + "_cells.bin");
                auto old_e = read_file(cur_dir + "/" + prefix + "_entries.bin");
                if (!old_c.empty() || !added.empty() || !removed.empty() || !rm_map.empty()) {
                    auto rebuilt = rebuild_cells_from_remap(old_c, old_e, rm_map, added, removed);
                    write_corrected_cells(label, entries_fid, rebuilt.cells_data, rebuilt.entries_data,
                                          prefix + "_cells.bin", prefix + "_entries.bin");
                }
            }
            log_phase((std::string("  ") + label).c_str(), t_entry);
        };
        rebuild_by_record_remap("POI", PatchFileId::POI_RECORDS, PatchFileId::POI_ENTRIES, "poi", poi_added, poi_removed);
        rebuild_by_record_remap("Place", PatchFileId::PLACE_NODES, PatchFileId::PLACE_ENTRIES, "place", place_added, place_removed);
    }

    unmap_file(patch_map);

    for (const auto& f : client_files) {
        std::string p = out_dir + "/" + f.name;
        struct stat st;
        if (stat(p.c_str(), &st) != 0) {
            if (f.size == 0 && write_file(p, nullptr, 0)) continue;
            throw std::runtime_error("Patch did not produce " + f.name);
        }
        if (static_cast<uint64_t>(st.st_size) != f.size)
            throw std::runtime_error("Output size mismatch: " + f.name + " is " + std::to_string(st.st_size) +
                                     " bytes, the patch lists " + std::to_string(f.size));
    }
    log_phase("Total", t_start);
    std::cerr << "Patch applied. Output in " << out_dir << std::endl;
    return 0;
}

// The one error edge: a throw anywhere in run() unwinds its scratch dir.
int main(int argc, char* argv[]) {
    try {
        return run(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}
