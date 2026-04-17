#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <s2/s2cell_id.h>

#include "types.h"
#include "geometry.h"
#include "parsed_data.h"

// --- S2 cell level globals (set by main arg parsing) ---

extern int kStreetCellLevel;
extern int kAdminCellLevel;
extern int kMaxAdminLevel;

// --- Simplification globals ---

extern SimplifyMode kSimplifyMode;
extern double kSimplifyEpsilonOverride; // 0 = use per-level defaults

// --- S2 helpers ---

void cover_edge(double lat1, double lng1, double lat2, double lng2,
                std::vector<S2CellId>& out);

std::vector<S2CellId> cover_edge(double lat1, double lng1, double lat2, double lng2);

S2CellId point_to_cell(double lat, double lng);

std::vector<std::pair<S2CellId, bool>> cover_polygon(
    const std::vector<std::pair<double,double>>& vertices);

// --- Thread pool for concurrent admin polygon S2 covering ---

class AdminCoverPool {
public:
    struct WorkItem {
        uint32_t poly_id;
        std::vector<std::pair<double,double>> vertices;
    };

    explicit AdminCoverPool(size_t num_threads);
    ~AdminCoverPool();

    void submit(uint32_t poly_id, std::vector<std::pair<double,double>>&& vertices);
    std::unordered_map<uint64_t, std::vector<uint32_t>> drain();

private:
    void worker_loop();

    std::vector<std::thread> workers_;
    std::deque<WorkItem> queue_;
    std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>> thread_results_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable done_cv_;
    size_t active_workers_ = 0;
    bool stop_;
};

// --- Simplification dispatch ---

// Simplify a polygon using the current global mode, taking admin_level into account.
// In MaxVertices mode: caps at MAX_POLYGON_VERTICES (500).
// In ErrorBounded mode: uses per-level epsilon in meters, converted to degrees.
inline std::vector<std::pair<double,double>> simplify_admin_polygon(
    const std::vector<std::pair<double,double>>& pts, uint8_t admin_level) {
    if (kSimplifyMode == SimplifyMode::ErrorBounded) {
        double eps_m = kSimplifyEpsilonOverride > 0
            ? kSimplifyEpsilonOverride
            : admin_epsilon_meters(admin_level);
        // Estimate latitude from first vertex for degree conversion
        double lat = pts.empty() ? 0.0 : pts[0].first;
        double eps_deg = meters_to_degrees(eps_m, lat);
        return simplify_polygon_epsilon(pts, eps_deg);
    }
    return simplify_polygon(pts);
}

// --- Helper functions that depend on S2 ---

void add_addr_point(ParsedData& data, double lat, double lng,
                    const char* housenumber, const char* street,
                    const char* postcode,
                    uint64_t& addr_count_total,
                    const std::vector<std::pair<double,double>>* polygon_vertices = nullptr);

void add_admin_polygon(ParsedData& data,
                       const std::vector<std::pair<double,double>>& vertices_in,
                       const char* name, uint8_t admin_level,
                       const char* country_code,
                       AdminCoverPool* admin_pool = nullptr,
                       uint8_t place_type_override = 0);
