#include "s2_helpers.h"
#include "geometry.h"

#include <algorithm>
#include <cmath>
#include <iostream>
#include <memory>

#include <s2/s2latlng.h>
#include <s2/s2region_coverer.h>
#include <s2/s2polyline.h>
#include <s2/s2polygon.h>
#include <s2/s2loop.h>

// --- S2 cell level globals ---

int kStreetCellLevel = 17;
int kAdminCellLevel = 10;
int kMaxAdminLevel = 0;  // 0 means no filtering

SimplifyMode kSimplifyMode = SimplifyMode::MaxVertices;
double kSimplifyEpsilonOverride = 0;

// --- cover_edge ---

void cover_edge(double lat1, double lng1, double lat2, double lng2,
                std::vector<S2CellId>& out) {
    out.clear();

    S2CellId c1 = S2CellId(S2LatLng::FromDegrees(lat1, lng1)).parent(kStreetCellLevel);
    S2CellId c2 = S2CellId(S2LatLng::FromDegrees(lat2, lng2)).parent(kStreetCellLevel);

    // Same cell — most common case for short edges
    if (c1 == c2) {
        out.push_back(c1);
        return;
    }

    // Adjacent cells — check midpoint
    S2CellId cm = S2CellId(S2LatLng::FromDegrees((lat1+lat2)/2, (lng1+lng2)/2)).parent(kStreetCellLevel);
    if (cm == c1 || cm == c2) {
        out.push_back(c1);
        out.push_back(c2);
        return;
    }

    // Longer edge — use S2RegionCoverer for correctness (handles geodesic curves)
    thread_local S2RegionCoverer coverer = []() {
        S2RegionCoverer::Options options;
        options.set_fixed_level(kStreetCellLevel);
        return S2RegionCoverer(options);
    }();

    S2Point p1 = S2LatLng::FromDegrees(lat1, lng1).ToPoint();
    S2Point p2 = S2LatLng::FromDegrees(lat2, lng2).ToPoint();
    if (p1 == p2) { out.push_back(c1); return; }

    std::vector<S2Point> points = {p1, p2};
    S2Polyline polyline(points);
    S2CellUnion covering = coverer.GetCovering(polyline);
    out = std::move(covering.cell_ids());
}

std::vector<S2CellId> cover_edge(double lat1, double lng1, double lat2, double lng2) {
    std::vector<S2CellId> result;
    cover_edge(lat1, lng1, lat2, lng2, result);
    return result;
}

S2CellId point_to_cell(double lat, double lng) {
    return S2CellId(S2LatLng::FromDegrees(lat, lng)).parent(kStreetCellLevel);
}

// --- cover_polygon ---

std::vector<std::pair<S2CellId, bool>> cover_polygon(
    const std::vector<std::pair<double,double>>& vertices) {
    std::vector<S2Point> points;
    points.reserve(vertices.size());
    for (const auto& [lat, lng] : vertices) {
        S2Point p = S2LatLng::FromDegrees(lat, lng).ToPoint();
        if (!points.empty() && points.back() == p) continue;
        points.push_back(p);
    }
    if (points.size() > 1 && points.front() == points.back()) {
        points.pop_back();
    }
    if (points.size() < 3) return {};

    S2Error error;
    auto loop = std::make_unique<S2Loop>(points, S2Debug::DISABLE);
    loop->Normalize();
    if (loop->FindValidationError(&error)) return {};

    S2Polygon polygon(std::move(loop));

    S2RegionCoverer::Options options;
    options.set_max_level(kAdminCellLevel);
    options.set_max_cells(MAX_S2_CELLS_PER_POLY);

    S2RegionCoverer coverer(options);
    S2CellUnion covering = coverer.GetCovering(polygon);
    S2CellUnion interior = coverer.GetInteriorCovering(polygon);

    std::unordered_set<uint64_t> interior_set;
    for (const auto& cell : interior.cell_ids()) {
        if (cell.level() <= kAdminCellLevel) {
            auto begin = cell.range_min().parent(kAdminCellLevel);
            auto end = cell.range_max().parent(kAdminCellLevel);
            for (auto c = begin; c != end; c = c.next()) {
                interior_set.insert(c.id());
            }
            interior_set.insert(end.id());
        } else {
            interior_set.insert(cell.parent(kAdminCellLevel).id());
        }
    }

    std::vector<std::pair<S2CellId, bool>> result;
    for (const auto& cell : covering.cell_ids()) {
        if (cell.level() <= kAdminCellLevel) {
            auto begin = cell.range_min().parent(kAdminCellLevel);
            auto end = cell.range_max().parent(kAdminCellLevel);
            for (auto c = begin; c != end; c = c.next()) {
                result.emplace_back(c, interior_set.count(c.id()) > 0);
            }
            result.emplace_back(end, interior_set.count(end.id()) > 0);
        } else {
            auto parent = cell.parent(kAdminCellLevel);
            result.emplace_back(parent, interior_set.count(parent.id()) > 0);
        }
    }

    std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });
    auto it = result.begin();
    for (auto curr = result.begin(); curr != result.end(); ) {
        auto next = curr + 1;
        bool is_interior = curr->second;
        while (next != result.end() && next->first == curr->first) {
            is_interior = is_interior || next->second;
            ++next;
        }
        *it = {curr->first, is_interior};
        ++it;
        curr = next;
    }
    result.erase(it, result.end());
    return result;
}

// --- AdminCoverPool ---

AdminCoverPool::AdminCoverPool(size_t num_threads) : stop_(false) {
    for (size_t i = 0; i < num_threads; i++) {
        workers_.emplace_back([this]() { worker_loop(); });
    }
}

AdminCoverPool::~AdminCoverPool() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) t.join();
}

void AdminCoverPool::submit(uint32_t poly_id, std::vector<std::pair<double,double>>&& vertices) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push_back({poly_id, std::move(vertices)});
    }
    cv_.notify_one();
}

std::unordered_map<uint64_t, std::vector<uint32_t>> AdminCoverPool::drain() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        done_cv_.wait(lock, [this]() { return queue_.empty() && active_workers_ == 0; });
    }
    std::unordered_map<uint64_t, std::vector<uint32_t>> merged;
    for (auto& local : thread_results_) {
        for (auto& [cell_id, ids] : local) {
            auto& target = merged[cell_id];
            target.insert(target.end(), ids.begin(), ids.end());
        }
    }
    return merged;
}

void AdminCoverPool::worker_loop() {
    size_t my_idx;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        my_idx = thread_results_.size();
        thread_results_.emplace_back();
    }

    while (true) {
        WorkItem item;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() { return stop_ || !queue_.empty(); });
            if (stop_ && queue_.empty()) return;
            item = std::move(queue_.front());
            queue_.pop_front();
            active_workers_++;
        }

        auto cell_ids = cover_polygon(item.vertices);
        auto& local = thread_results_[my_idx];
        for (const auto& [cell_id, is_interior] : cell_ids) {
            uint32_t entry = is_interior ? (item.poly_id | INTERIOR_FLAG) : item.poly_id;
            local[cell_id.id()].push_back(entry);
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            active_workers_--;
        }
        done_cv_.notify_all();
    }
}

// --- add_addr_point ---

void add_addr_point(ParsedData& data, double lat, double lng,
                    const char* housenumber, const char* street,
                    uint64_t& addr_count_total) {
    uint32_t addr_id = static_cast<uint32_t>(data.addr_points.size());
    data.addr_points.push_back({
        static_cast<float>(lat),
        static_cast<float>(lng),
        data.string_pool.intern(housenumber),
        data.string_pool.intern(street)
    });

    S2CellId cell = point_to_cell(lat, lng);
    data.cell_to_addrs[cell.id()].push_back(addr_id);

    addr_count_total++;
    if (addr_count_total % 1000000 == 0) {
        std::cerr << "Collected " << addr_count_total / 1000000 << "M addresses..." << std::endl;
    }
}

// --- add_admin_polygon ---

void add_admin_polygon(ParsedData& data,
                       const std::vector<std::pair<double,double>>& vertices_in,
                       const char* name, uint8_t admin_level,
                       const char* country_code,
                       AdminCoverPool* admin_pool) {
    auto vertices = vertices_in;
    if (vertices.size() >= 4 &&
        std::fabs(vertices.front().first - vertices.back().first) < 1e-7 &&
        std::fabs(vertices.front().second - vertices.back().second) < 1e-7) {
        vertices.pop_back();
        auto min_it = std::min_element(vertices.begin(), vertices.end());
        std::rotate(vertices.begin(), min_it, vertices.end());
        vertices.push_back(vertices.front());
    }

    auto simplified = simplify_admin_polygon(vertices, admin_level);
    if (simplified.size() < 3) return;

    uint32_t poly_id = static_cast<uint32_t>(data.admin_polygons.size());
    uint32_t vertex_offset = static_cast<uint32_t>(data.admin_vertices.size());

    for (const auto& [lat, lng] : simplified) {
        data.admin_vertices.push_back({static_cast<float>(lat), static_cast<float>(lng)});
    }

    AdminPolygon poly;
    memset(&poly, 0, sizeof(poly)); // zero ALL bytes including padding
    poly.vertex_offset = vertex_offset;
    poly.vertex_count = static_cast<uint32_t>(simplified.size());
    poly.name_id = data.string_pool.intern(name);
    poly.admin_level = admin_level;
    poly.area = polygon_area(simplified);
    poly.country_code = (country_code && country_code[0] && country_code[1])
        ? static_cast<uint16_t>((country_code[0] << 8) | country_code[1])
        : 0;
    data.admin_polygons.push_back(poly);

    if (admin_pool) {
        admin_pool->submit(poly_id, std::move(simplified));
    }
}
