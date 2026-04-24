#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <iomanip>
#include <thread>
#include <unistd.h>
#include <sys/stat.h>

#include "types.h"
#include "string_pool.h"

// --- Directory creation ---

inline void ensure_dir(const std::string& path) {
    // Recursive mkdir — create all parent directories
    for (size_t i = 1; i < path.size(); i++) {
        if (path[i] == '/') {
            mkdir(path.substr(0, i).c_str(), 0755);
        }
    }
    mkdir(path.c_str(), 0755);
}

// --- CPU tick reading (all threads via getrusage) ---

#include <sys/resource.h>

struct CpuTicks {
    long long process_us = 0;  // user+system microseconds (all threads)

    static CpuTicks now() {
        CpuTicks ct;
        struct rusage ru;
        if (getrusage(RUSAGE_SELF, &ru) == 0) {
            ct.process_us = (long long)ru.ru_utime.tv_sec * 1000000 + ru.ru_utime.tv_usec
                          + (long long)ru.ru_stime.tv_sec * 1000000 + ru.ru_stime.tv_usec;
        }
        return ct;
    }
};

// --- Phase timer with CPU utilization ---

inline long get_rss_mb() {
    long rss_pages = 0;
    FILE* f = fopen("/proc/self/statm", "r");
    if (f) {
        long size;
        if (fscanf(f, "%ld %ld", &size, &rss_pages) != 2) rss_pages = 0;
        fclose(f);
    }
    return rss_pages * sysconf(_SC_PAGESIZE) / (1024 * 1024);
}

inline void log_phase(const char* name, std::chrono::steady_clock::time_point& t,
                       CpuTicks& prev_cpu) {
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - t).count();
    auto cpu_now = CpuTicks::now();
    double cpu_sec = (cpu_now.process_us - prev_cpu.process_us) / 1e6;
    double wall_sec = ms / 1000.0;
    double cores_used = (wall_sec > 0.1) ? cpu_sec / wall_sec : 0;
    unsigned ncpu = std::thread::hardware_concurrency();
    int pct = (wall_sec > 0.1 && ncpu > 0) ? (int)(cores_used * 100.0 / ncpu) : 0;
    long rss = get_rss_mb();
    std::cerr << "  [" << ms/1000 << "." << (ms%1000)/100 << "s"
              << " " << std::fixed << std::setprecision(1) << cores_used << "/" << ncpu << "cores"
              << " " << pct << "%"
              << " " << rss << "MiB] " << name << std::endl;
    t = now;
    prev_cpu = cpu_now;
}

// Backward compat overload (no CPU tracking)
inline void log_phase(const char* name, std::chrono::steady_clock::time_point& t) {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t).count();
    std::cerr << "  [" << ms/1000 << "." << (ms%1000)/100 << "s] " << name << std::endl;
    t = std::chrono::steady_clock::now();
}

// --- String pool tiers ---
//
// At build finalization, every interned string is assigned to exactly one
// tier based on which record types reference it. Clients download the tier
// files they need and the remaining get_string() lookups for missing tiers
// fall back to an empty string. Rule: a string's home tier is the lowest
// set bit of its consumer-mask, so e.g. a name used by both streets and
// POIs lives in strings_street.bin (POI clients always have streets).
constexpr uint8_t STR_TIER_BIT_CORE     = 1 << 0;  // admin_polygons, place_nodes
constexpr uint8_t STR_TIER_BIT_STREET   = 1 << 1;  // ways, addr_point.street_id, interp, poi.parent_street_id
constexpr uint8_t STR_TIER_BIT_ADDR     = 1 << 2;  // addr housenumbers
constexpr uint8_t STR_TIER_BIT_POSTCODE = 1 << 3;  // postcode strings (any consumer)
constexpr uint8_t STR_TIER_BIT_POI      = 1 << 4;  // poi_records.name_id

constexpr size_t STR_TIER_COUNT = 5;
constexpr const char* STR_TIER_FILENAMES[STR_TIER_COUNT] = {
    "strings_core.bin",
    "strings_street.bin",
    "strings_addr.bin",
    "strings_postcode.bin",
    "strings_poi.bin",
};
constexpr const char* STR_TIER_NAMES[STR_TIER_COUNT] = {
    "core", "street", "addr", "postcode", "poi"
};

// --- Parsed data container ---

struct ParsedData {
    StringPool string_pool;
    std::vector<WayHeader> ways;
    std::vector<NodeCoord> street_nodes;
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_ways;
    std::vector<AddrPoint> addr_points;
    std::vector<NodeCoord> addr_vertices;  // polygon vertices for building addr_points
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_addrs;
    std::vector<InterpWay> interp_ways;
    std::vector<NodeCoord> interp_nodes;
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_interps;
    std::vector<AdminPolygon> admin_polygons;
    std::vector<NodeCoord> admin_vertices;
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_admin;

    // Deferred work for parallel S2 computation (ways + interps)
    std::vector<DeferredWay> deferred_ways;
    std::vector<DeferredInterp> deferred_interps;

    // Sorted (cell_id, item_id) pairs — kept for direct entry writing
    std::vector<CellItemPair> sorted_way_cells;
    std::vector<CellItemPair> sorted_addr_cells;
    std::vector<CellItemPair> sorted_interp_cells;

    // Collected data for parallel admin assembly
    std::vector<CollectedRelation> collected_relations;
    struct WayGeometry {
        std::vector<std::pair<double,double>> coords;
        int64_t first_node_id;
        int64_t last_node_id;
    };
    std::unordered_map<int64_t, WayGeometry> way_geometries;

    // Place nodes (settlements)
    std::vector<PlaceNode> place_nodes;
    std::vector<CellItemPair> sorted_place_cells;

    // POI data
    std::vector<PoiRecord> poi_records;
    std::vector<NodeCoord> poi_vertices;
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_pois;
    std::vector<CellItemPair> sorted_poi_cells;
    std::vector<DeferredPoi> deferred_pois;
    std::vector<CollectedPoiRelation> collected_poi_relations;

    // Parent chain: per-way and per-polygon parent IDs for the
    // Nominatim-style address walk. Parallel arrays indexed by
    // way_id / polygon_id respectively.
    std::vector<uint32_t> way_orig_name_ids;  // way → original name (before name:en pref), build-time only
    std::vector<uint32_t> way_parent_ids;    // way → smallest containing admin poly
    std::vector<uint32_t> admin_parent_ids;  // poly → next-larger containing admin poly
    std::vector<uint32_t> way_postcode_ids;  // way → postcode string from containing postal boundary
    std::vector<uint32_t> addr_postcode_ids; // per-addr_point postcode (optional separate file)

    // Postcode centroid collector: (postcode_string_id → sum_lat, sum_lng, count)
    // Built during addr_point extraction, converted to PostcodeCentroid[] at write time.
    struct PostcodeAccum { double sum_lat = 0; double sum_lng = 0; uint64_t count = 0; uint16_t country_code = 0; };
    std::unordered_map<uint32_t, PostcodeAccum> postcode_accum;
    std::unique_ptr<std::mutex> postcode_mutex = std::make_unique<std::mutex>();

    // boundary=census relations with postal_code tags — build-time only,
    // used for postcode inheritance then discarded. Not written to disk.
    std::vector<CdpPostcodeRelation> cdp_postcode_relations;
    std::vector<CdpPostcodePoly> cdp_postcode_polys;

    // Post-canonical-sort: strings partitioned into per-consumer tiers.
    // Global offset space is contiguous: tier N occupies
    // [strings_tier_bases[N], strings_tier_bases[N+1]). Record name_ids
    // are global offsets.  string_pool.data() is cleared after partition
    // to free memory.
    std::array<std::vector<char>, STR_TIER_COUNT> strings_tiers;
    std::array<uint32_t, STR_TIER_COUNT + 1> strings_tier_bases{};

    // Look up a string by its global offset (post-partition).  Returns
    // nullptr if the offset is NO_DATA / 0xFFFFFFFF or out of range.
    const char* get_string(uint32_t off) const {
        if (off == NO_DATA || off == 0xFFFFFFFFu) return nullptr;
        for (size_t t = 0; t < STR_TIER_COUNT; t++) {
            if (off < strings_tier_bases[t + 1]) {
                uint32_t local = off - strings_tier_bases[t];
                return strings_tiers[t].data() + local;
            }
        }
        return nullptr;
    }
};

// --- Deduplicate IDs per cell ---

template<typename Map>
inline void deduplicate(Map& cell_map) {
    for (auto& [cell_id, ids] : cell_map) {
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    }
}
