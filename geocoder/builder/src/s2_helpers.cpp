#include "s2_helpers.h"
#include "geometry.h"
#include "country_code.h"
#include "parallel.h"
#include "polygon_rings.h"
#include "segment_rect.h"

#include <algorithm>
#include <cmath>
#include <iostream>
#include <memory>

#include <s2/s2cell.h>
#include <s2/s2coords.h>
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

// The cells a fixed-level S2RegionCoverer covers the edge p1-p2 with: its
// descent, from the same starting cells, into each child the edge
// MayIntersect()s down to the street level. The coverer spends most of an
// edge's time in MayIntersect, so here the cell's face (u,v), where the edge
// is a straight segment, answers it (segment_meets_rect), and the S2 test
// runs only where rounding could flip the answer.
static void cover_long_edge(const S2Point& p1, const S2Point& p2, std::vector<S2CellId>& out) {
    thread_local S2RegionCoverer start_coverer = []() {
        // What S2RegionCoverer::GetInitialCandidates starts from.
        S2RegionCoverer::Options options;
        options.set_max_cells(4);
        options.set_max_level(kStreetCellLevel);
        return S2RegionCoverer(options);
    }();
    // A face's (u,v) is a central projection, well conditioned only for
    // points well inside the face's hemisphere.
    static constexpr double kMinFaceCosine = 0.1;

    std::vector<S2Point> points = {p1, p2};
    S2Polyline polyline(points);
    thread_local std::vector<S2CellId> start;
    start_coverer.GetFastCovering(polyline, &start);

    struct Cell { int i, j, level; };
    thread_local std::vector<Cell> pending;
    for (S2CellId id : start) {
        int i, j;
        const int face = id.ToFaceIJOrientation(&i, &j, nullptr);
        const S2Point norm = S2::GetNorm(face);
        const bool planar = p1.DotProd(norm) > kMinFaceCosine && p2.DotProd(norm) > kMinFaceCosine;
        R2Point a, b;
        if (planar) {
            S2::ValidFaceXYZtoUV(face, p1, &a);
            S2::ValidFaceXYZtoUV(face, p2, &b);
        }
        const int size = S2CellId::GetSizeIJ(id.level());
        pending.assign(1, Cell{i & -size, j & -size, id.level()});
        while (!pending.empty()) {
            const Cell c = pending.back();
            pending.pop_back();
            const int cell_size = S2CellId::GetSizeIJ(c.level);
            SegmentRect meets = SegmentRect::Unsure;
            if (planar) {
                meets = segment_meets_rect(a[0], a[1], b[0], b[1],
                    S2::STtoUV(S2::IJtoSTMin(c.i)), S2::STtoUV(S2::IJtoSTMin(c.i + cell_size)),
                    S2::STtoUV(S2::IJtoSTMin(c.j)), S2::STtoUV(S2::IJtoSTMin(c.j + cell_size)));
            }
            if (meets == SegmentRect::Disjoint) continue;
            auto cell_id = [&] { return S2CellId::FromFaceIJ(face, c.i, c.j).parent(c.level); };
            if (meets == SegmentRect::Unsure && !polyline.MayIntersect(S2Cell(cell_id()))) continue;
            if (c.level >= kStreetCellLevel) {
                out.push_back(cell_id());
                continue;
            }
            const int half = cell_size >> 1;
            for (int k = 0; k < 4; k++)
                pending.push_back({c.i + (k & 1) * half, c.j + (k >> 1) * half, c.level + 1});
        }
    }
    // The coverer's answer is a normalized cell union: sorted, each cell once.
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
}

void cover_edge(S2CellId c1, S2CellId c2, double lat1, double lng1, double lat2, double lng2,
                std::vector<S2CellId>& out) {
    out.clear();

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

    // Longer edge — cover the geodesic as S2RegionCoverer does
    S2Point p1 = S2LatLng::FromDegrees(lat1, lng1).ToPoint();
    S2Point p2 = S2LatLng::FromDegrees(lat2, lng2).ToPoint();
    if (p1 == p2) { out.push_back(c1); return; }
    cover_long_edge(p1, p2, out);
}


S2CellId point_to_cell(double lat, double lng) {
    return S2CellId(S2LatLng::FromDegrees(lat, lng)).parent(kStreetCellLevel);
}

// --- cover_polygon ---

// A closed ring's S2 loop, normalized to enclose less than half the sphere;
// null below 3 distinct vertices.
static std::unique_ptr<S2Loop> ring_loop(const std::pair<double,double>* ring, size_t count) {
    std::vector<S2Point> points;
    points.reserve(count);
    for (size_t k = 0; k < count; k++) {
        S2Point p = S2LatLng::FromDegrees(ring[k].first, ring[k].second).ToPoint();
        if (!points.empty() && points.back() == p) continue;
        points.push_back(p);
    }
    if (points.size() > 1 && points.front() == points.back()) {
        points.pop_back();
    }
    if (points.size() < 3) return nullptr;
    auto loop = std::make_unique<S2Loop>(points, S2Debug::DISABLE);
    loop->Normalize();
    return loop;
}

std::vector<std::pair<S2CellId, bool>> cover_polygon(
    const std::vector<std::pair<double,double>>& vertices, uint16_t holes) {
    std::vector<RingSpan> rings;
    split_rings(vertices.data(), static_cast<uint32_t>(vertices.size()), holes, rings);
    auto outer = ring_loop(vertices.data() + rings[0].begin, rings[0].count);
    if (!outer) return {};

    S2Error error;
    // Validate through the polygon: for one loop of 3+ vertices
    // S2Polygon::FindValidationError runs exactly S2Loop's checks (the
    // empty-loop, orientation and nesting checks cannot fire), on the shape
    // index the coverer reuses instead of a second index for the loop alone.
    std::unique_ptr<S2Polygon> polygon;
    if (rings.size() > 1) {
        // Holes nest in the outer loop; a polygon whose simplified holes
        // cross it (or each other) is covered by its outer loop alone, a
        // superset the reader's ring test narrows.
        std::vector<std::unique_ptr<S2Loop>> loops;
        loops.emplace_back(outer->Clone());
        for (size_t r = 1; r < rings.size(); r++)
            if (auto hole = ring_loop(vertices.data() + rings[r].begin, rings[r].count))
                loops.push_back(std::move(hole));
        polygon = std::make_unique<S2Polygon>();
        polygon->set_s2debug_override(S2Debug::DISABLE);
        polygon->InitNested(std::move(loops));
        if (polygon->FindValidationError(&error)) polygon.reset();
    }
    if (!polygon) polygon = std::make_unique<S2Polygon>(std::move(outer), S2Debug::DISABLE);
    if (polygon->FindValidationError(&error)) return {};

    S2RegionCoverer::Options options;
    options.set_max_level(kAdminCellLevel);
    options.set_max_cells(MAX_S2_CELLS_PER_POLY);

    S2RegionCoverer coverer(options);
    S2CellUnion covering = coverer.GetCovering(*polygon);
    S2CellUnion interior = coverer.GetInteriorCovering(*polygon);

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

void AdminCoverPool::submit_all(std::vector<WorkItem>&& items) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& item : items) queue_.push_back(std::move(item));
    }
    cv_.notify_all();
}

void AdminCoverPool::wait_idle() {
    std::unique_lock<std::mutex> lock(mutex_);
    done_cv_.wait(lock, [this]() { return queue_.empty() && active_workers_ == 0; });
}

std::unordered_map<uint64_t, std::vector<uint32_t>> AdminCoverPool::drain() {
    wait_idle();
    // Merge the per-worker maps on all workers, each cell in the one shard
    // its id hashes to (splitmix64: level-10 ids end in 40 zero bits).
    size_t shards = std::max<size_t>(1, thread_results_.size());
    auto shard_of = [shards](uint64_t cell_id) {
        uint64_t z = cell_id;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
        return static_cast<size_t>((z ^ (z >> 31)) % shards);
    };
    std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>> parts(shards);
    parallel_for(shards, [&](size_t begin, size_t end, unsigned) {
        for (size_t s = begin; s < end; s++) {
            auto& part = parts[s];
            for (const auto& local : thread_results_) {
                for (const auto& [cell_id, ids] : local) {
                    if (shard_of(cell_id) != s) continue;
                    auto& target = part[cell_id];
                    target.insert(target.end(), ids.begin(), ids.end());
                }
            }
            // Determinism: the order in which worker threads pick items off the
            // queue is thread-scheduling dependent, so each cell's merged vector
            // can end up in a different order across runs. Downstream code that
            // iterates these vectors (e.g. the "first admin_level=2 polygon"
            // country lookup in the postcode centroid writer) must see a stable
            // ordering or its output becomes non-deterministic and breaks
            // incremental patching. Sort each vector by its entry value (which
            // includes the INTERIOR_FLAG bit + poly_id).
            for (auto& [cell_id, ids] : part) {
                std::sort(ids.begin(), ids.end());
            }
        }
    }, static_cast<unsigned>(shards));
    std::unordered_map<uint64_t, std::vector<uint32_t>> merged;
    size_t cells = 0;
    for (const auto& part : parts) cells += part.size();
    merged.reserve(cells);
    for (auto& part : parts) {
        for (auto& [cell_id, ids] : part) merged.emplace(cell_id, std::move(ids));
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

        auto cell_ids = cover_polygon(item.vertices, item.holes);
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
                    const char* postcode,
                    uint64_t& addr_count_total,
                    uint64_t osm_id_packed,
                    const NodeCoord* polygon_vertices,
                    uint32_t polygon_vertex_count) {
    uint32_t street_id = (street && street[0])
        ? data.string_pool.intern(street)
        : NO_DATA;
    uint32_t housenumber_id = data.string_pool.intern(housenumber);
    uint32_t pc_id = (postcode && postcode[0])
        ? data.string_pool.intern(postcode) : NO_DATA;
    append_addr_point(data, lat, lng, housenumber_id, street_id, pc_id,
                      point_to_cell(lat, lng), osm_id_packed,
                      polygon_vertices, polygon_vertex_count);

    addr_count_total++;
    if (addr_count_total % 1000000 == 0) {
        std::cerr << "Collected " << addr_count_total / 1000000 << "M addresses..." << std::endl;
    }
}

void append_addr_point(ParsedData& data, double lat, double lng,
                       uint32_t housenumber_id, uint32_t street_id, uint32_t postcode_id,
                       S2CellId cell, uint64_t osm_id_packed,
                       const NodeCoord* polygon_vertices,
                       uint32_t polygon_vertex_count) {
    uint32_t vertex_offset = NO_DATA;
    uint32_t vertex_count = 0;
    if (polygon_vertices && polygon_vertex_count > 0) {
        vertex_offset = static_cast<uint32_t>(data.addr_vertices.size());
        vertex_count = polygon_vertex_count;
        data.addr_vertices.insert(data.addr_vertices.end(),
            polygon_vertices, polygon_vertices + polygon_vertex_count);
    }
    put_addr_point(data, grow_addr_points(data, 1), static_cast<float>(lat), static_cast<float>(lng),
                   housenumber_id, street_id, postcode_id, cell.id(), osm_id_packed,
                   vertex_offset, vertex_count);
}

// --- add_admin_polygon ---

void add_admin_polygon(ParsedData& data,
                       const std::vector<std::pair<double,double>>& vertices_in,
                       const char* name, uint8_t admin_level,
                       const char* country_code,
                       AdminCoverPool* admin_pool,
                       uint8_t place_type_override,
                       int64_t osm_way_id) {
    auto vertices = vertices_in;
    canonicalize_ring_rotation(vertices);

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
    poly.place_type_override = place_type_override;
    poly.area = polygon_area(simplified);
    poly.country_code = (country_code && country_code[0] && country_code[1])
        ? pack_country_code(country_code[0], country_code[1])
        : 0;
    data.admin_polygons.push_back(poly);
    // Strategy-2 stable identity, packed like addr/poi: top 8 bits =
    // ObjectType, bottom 56 bits = stable id. A closed-way admin's
    // stable identity is its OSM way id (OSM_WAY). Without a unique id
    // every closed-way admin previously collided on (SYNTHETIC, 0) in
    // the IdAllocator, so only one could ever reuse a prev slot and the
    // rest tombstoned + re-allocated on every build — the dominant
    // source of admin_entries / way_parents churn at planet scale
    // (~84k spurious tombstones). A way id (OSM_WAY type) makes each
    // unique and stable. Fall back to 0 (treated as SYNTHETIC) only
    // if no way id was supplied.
    if (osm_way_id != 0) {
        data.admin_osm_ids.push_back(
            (static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_WAY) << 56) |
            (static_cast<uint64_t>(osm_way_id) & 0x00FFFFFFFFFFFFFFull));
    } else {
        data.admin_osm_ids.push_back(0);
    }

    if (admin_pool) {
        admin_pool->submit(poly_id, std::move(simplified));
    }
}
