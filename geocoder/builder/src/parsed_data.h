#pragma once

#include <algorithm>
#include <chrono>
#include <iostream>
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

// --- Parsed data container ---

struct ParsedData {
    StringPool string_pool;
    std::vector<WayHeader> ways;
    std::vector<NodeCoord> street_nodes;
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_ways;
    std::vector<AddrPoint> addr_points;
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
};

// --- Deduplicate IDs per cell ---

template<typename Map>
inline void deduplicate(Map& cell_map) {
    for (auto& [cell_id, ids] : cell_map) {
        std::sort(ids.begin(), ids.end());
        ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
    }
}
