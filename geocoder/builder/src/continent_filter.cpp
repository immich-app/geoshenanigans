#include "continent_filter.h"
#include "continent_boundaries.h"
#include "parsed_data.h"

#include <algorithm>
#include <cstring>
#include <future>
#include <unordered_set>

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

const ContinentBBox kContinents[] = {
    {"africa",            -35.0,  37.5,  -25.0,  55.0},
    {"asia",              -12.0,  82.0,   25.0, 180.0},
    {"europe",             35.0,  72.0,  -25.0,  45.0},
    {"north-america",       7.0,  84.0, -170.0, -50.0},
    {"south-america",     -56.0,  13.0,  -82.0, -34.0},
    {"oceania",           -50.0,   0.0,  110.0, 180.0},
    {"central-america",     7.0,  23.5, -120.0, -57.0},
    {"antarctica",        -90.0, -60.0, -180.0, 180.0},
};

const size_t kContinentCount = sizeof(kContinents) / sizeof(kContinents[0]);

static bool cell_in_bbox(uint64_t cell_id, const ContinentBBox& bbox) {
    S2CellId cell(cell_id);
    S2LatLng center = cell.ToLatLng();
    double lat = center.lat().degrees();
    double lng = center.lng().degrees();
    return lat >= bbox.min_lat && lat <= bbox.max_lat &&
           lng >= bbox.min_lng && lng <= bbox.max_lng;
}

ParsedData filter_by_bbox(const ParsedData& full, const ContinentBBox& bbox) {
    ParsedData out;
    auto _ft = std::chrono::steady_clock::now();
    auto _fc = CpuTicks::now();

    std::cerr << "    data: ways_sorted=" << full.sorted_way_cells.size()
              << " addrs_sorted=" << full.sorted_addr_cells.size()
              << " interps_sorted=" << full.sorted_interp_cells.size()
              << " ways_map=" << full.cell_to_ways.size()
              << " addrs_map=" << full.cell_to_addrs.size()
              << " interps_map=" << full.cell_to_interps.size()
              << " admin_map=" << full.cell_to_admin.size()
              << std::endl;

    // Filter from hash map
    auto filter_cells_map = [&](const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map,
                                bool mask_interior = false) {
        std::cerr << "    DEBUG filter_cells_map: size=" << cell_map.size() << std::endl;
        std::unordered_set<uint32_t> ids;
        for (const auto& [cell_id, cell_ids] : cell_map) {
            if (cell_in_bbox(cell_id, bbox)) {
                for (uint32_t id : cell_ids)
                    ids.insert(mask_interior ? (id & ID_MASK) : id);
            }
        }
        return ids;
    };

    // Filter from sorted pairs — parallel chunked scan.
    // Split the sorted array into N chunks at cell boundaries, each thread
    // builds its own ID set, then merge.
    auto filter_cells_sorted = [&](const std::vector<CellItemPair>& sorted,
                                   bool mask_interior = false) {
        if (sorted.empty()) return std::unordered_set<uint32_t>{};

        unsigned nthreads = std::max(1u, std::thread::hardware_concurrency() / 4);
        size_t chunk = (sorted.size() + nthreads - 1) / nthreads;
        std::cerr << "    DEBUG filter_cells_sorted: size=" << sorted.size()
                  << " nthreads=" << nthreads << " chunk=" << chunk << std::endl;

        // Find chunk boundaries at cell_id transitions
        std::vector<size_t> bounds = {0};
        for (unsigned t = 1; t < nthreads; t++) {
            size_t target = t * chunk;
            if (target >= sorted.size()) break;
            while (target < sorted.size() && sorted[target].cell_id == sorted[target-1].cell_id)
                target++;
            if (target < sorted.size()) bounds.push_back(target);
        }
        bounds.push_back(sorted.size());

        std::vector<std::unordered_set<uint32_t>> thread_ids(bounds.size() - 1);
        std::vector<std::thread> threads;
        for (size_t t = 0; t + 1 < bounds.size(); t++) {
            threads.emplace_back([&, t]() {
                auto& local = thread_ids[t];
                for (size_t i = bounds[t]; i < bounds[t+1]; ) {
                    uint64_t cell_id = sorted[i].cell_id;
                    bool match = cell_in_bbox(cell_id, bbox);
                    while (i < bounds[t+1] && sorted[i].cell_id == cell_id) {
                        if (match) {
                            uint32_t id = sorted[i].item_id;
                            local.insert(mask_interior ? (id & ID_MASK) : id);
                        }
                        i++;
                    }
                }
            });
        }
        std::cerr << "    DEBUG: spawned " << threads.size() << " threads, "
                  << bounds.size() - 1 << " chunks" << std::endl;
        for (auto& t : threads) t.join();

        // Merge thread-local sets
        size_t largest = 0;
        for (size_t t = 0; t < thread_ids.size(); t++)
            if (thread_ids[t].size() > thread_ids[largest].size()) largest = t;
        auto& merged = thread_ids[largest];
        for (size_t t = 0; t < thread_ids.size(); t++) {
            if (t == largest) continue;
            for (auto id : thread_ids[t]) merged.insert(id);
        }
        return std::move(merged);
    };

    // Use sorted pairs where available (faster linear scan), fall back to hash maps
    auto f_ways = std::async(std::launch::async, [&]{
        return !full.sorted_way_cells.empty()
            ? filter_cells_sorted(full.sorted_way_cells)
            : filter_cells_map(full.cell_to_ways);
    });
    auto f_addrs = std::async(std::launch::async, [&]{
        return !full.sorted_addr_cells.empty()
            ? filter_cells_sorted(full.sorted_addr_cells)
            : filter_cells_map(full.cell_to_addrs);
    });
    auto f_interps = std::async(std::launch::async, [&]{
        return !full.sorted_interp_cells.empty()
            ? filter_cells_sorted(full.sorted_interp_cells)
            : filter_cells_map(full.cell_to_interps);
    });
    auto f_admin = std::async(std::launch::async, [&]{
        return filter_cells_map(full.cell_to_admin, true);
    });

    auto used_way_ids = f_ways.get();
    auto used_addr_ids = f_addrs.get();
    auto used_interp_ids = f_interps.get();
    auto used_admin_ids = f_admin.get();
    log_phase("      filter: ID collection", _ft, _fc);

    // Remap all 4 data types in parallel — each builds its own vectors and remap table
    std::unordered_map<uint32_t, uint32_t> way_remap, addr_remap, interp_remap, admin_remap;

    // Ways
    auto f_remap_ways = std::async(std::launch::async, [&]() {
        std::vector<uint32_t> sorted_ids(used_way_ids.begin(), used_way_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        std::vector<WayHeader> ways;
        std::vector<NodeCoord> nodes;
        ways.reserve(sorted_ids.size());
        nodes.reserve(sorted_ids.size() * 5);
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            remap[old_id] = static_cast<uint32_t>(ways.size());
            const auto& w = full.ways[old_id];
            WayHeader nw = w;
            nw.node_offset = static_cast<uint32_t>(nodes.size());
            ways.push_back(nw);
            for (uint8_t n = 0; n < w.node_count; n++)
                nodes.push_back(full.street_nodes[w.node_offset + n]);
        }
        return std::make_tuple(std::move(remap), std::move(ways), std::move(nodes));
    });

    // Addrs
    auto f_remap_addrs = std::async(std::launch::async, [&]() {
        std::vector<uint32_t> sorted_ids(used_addr_ids.begin(), used_addr_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        std::vector<AddrPoint> addrs;
        addrs.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            remap[old_id] = static_cast<uint32_t>(addrs.size());
            addrs.push_back(full.addr_points[old_id]);
        }
        return std::make_tuple(std::move(remap), std::move(addrs));
    });

    // Interps
    auto f_remap_interps = std::async(std::launch::async, [&]() {
        std::vector<uint32_t> sorted_ids(used_interp_ids.begin(), used_interp_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        std::vector<InterpWay> iways;
        std::vector<NodeCoord> inodes;
        iways.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            remap[old_id] = static_cast<uint32_t>(iways.size());
            const auto& iw = full.interp_ways[old_id];
            InterpWay niw = iw;
            niw.node_offset = static_cast<uint32_t>(inodes.size());
            iways.push_back(niw);
            for (uint8_t n = 0; n < iw.node_count; n++)
                inodes.push_back(full.interp_nodes[iw.node_offset + n]);
        }
        return std::make_tuple(std::move(remap), std::move(iways), std::move(inodes));
    });

    // Admins
    auto f_remap_admins = std::async(std::launch::async, [&]() {
        std::vector<uint32_t> sorted_ids(used_admin_ids.begin(), used_admin_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        std::vector<AdminPolygon> polys;
        std::vector<NodeCoord> verts;
        polys.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            remap[old_id] = static_cast<uint32_t>(polys.size());
            const auto& ap = full.admin_polygons[old_id];
            AdminPolygon nap = ap;
            nap.vertex_offset = static_cast<uint32_t>(verts.size());
            polys.push_back(nap);
            for (uint32_t v = 0; v < ap.vertex_count; v++)
                verts.push_back(full.admin_vertices[ap.vertex_offset + v]);
        }
        return std::make_tuple(std::move(remap), std::move(polys), std::move(verts));
    });

    // Collect results
    {
        auto [wr, ways, nodes] = f_remap_ways.get();
        way_remap = std::move(wr);
        out.ways = std::move(ways);
        out.street_nodes = std::move(nodes);
    }
    {
        auto [ar, addrs] = f_remap_addrs.get();
        addr_remap = std::move(ar);
        out.addr_points = std::move(addrs);
    }
    {
        auto [ir, iways, inodes] = f_remap_interps.get();
        interp_remap = std::move(ir);
        out.interp_ways = std::move(iways);
        out.interp_nodes = std::move(inodes);
    }
    {
        auto [ar, polys, verts] = f_remap_admins.get();
        admin_remap = std::move(ar);
        out.admin_polygons = std::move(polys);
        out.admin_vertices = std::move(verts);
    }
    log_phase("      filter: data remap", _ft, _fc);

    // Remap cell maps — use sorted pairs where available (parallel), hash maps otherwise
    auto remap_cells_map = [&](const std::unordered_map<uint64_t, std::vector<uint32_t>>& src,
                               const std::unordered_map<uint32_t, uint32_t>& remap,
                               std::unordered_map<uint64_t, std::vector<uint32_t>>& dst,
                               bool handle_flags = false) {
        for (const auto& [cell_id, ids] : src) {
            if (!cell_in_bbox(cell_id, bbox)) continue;
            std::vector<uint32_t> new_ids;
            for (uint32_t id : ids) {
                uint32_t raw_id = handle_flags ? (id & ID_MASK) : id;
                uint32_t flags = handle_flags ? (id & INTERIOR_FLAG) : 0;
                auto it = remap.find(raw_id);
                if (it != remap.end()) new_ids.push_back(it->second | flags);
            }
            if (!new_ids.empty()) dst[cell_id] = std::move(new_ids);
        }
    };

    // Remap from sorted pairs — builds output cell map by scanning sorted pairs.
    // Uses same parallel chunked pattern as filter_cells_sorted.
    auto remap_cells_sorted = [&](const std::vector<CellItemPair>& sorted,
                                   const std::unordered_map<uint32_t, uint32_t>& remap,
                                   std::unordered_map<uint64_t, std::vector<uint32_t>>& dst) {
        if (sorted.empty()) return;
        auto _rt = std::chrono::steady_clock::now();
        auto _rc = CpuTicks::now();

        unsigned nthreads = std::max(1u, std::thread::hardware_concurrency() / 4);
        size_t chunk = (sorted.size() + nthreads - 1) / nthreads;

        // Find chunk boundaries at cell_id transitions
        std::vector<size_t> bounds = {0};
        for (unsigned t = 1; t < nthreads; t++) {
            size_t target = t * chunk;
            if (target >= sorted.size()) break;
            while (target < sorted.size() && sorted[target].cell_id == sorted[target-1].cell_id)
                target++;
            if (target < sorted.size()) bounds.push_back(target);
        }
        bounds.push_back(sorted.size());

        std::cerr << "        remap_sorted: " << sorted.size() << " pairs, "
                  << bounds.size()-1 << " chunks, " << nthreads << " threads" << std::endl;

        // Each thread builds its own partial map
        std::vector<std::unordered_map<uint64_t, std::vector<uint32_t>>> partial(bounds.size() - 1);
        std::vector<std::thread> threads;
        for (size_t t = 0; t + 1 < bounds.size(); t++) {
            threads.emplace_back([&, t]() {
                auto& local = partial[t];
                for (size_t i = bounds[t]; i < bounds[t+1]; ) {
                    uint64_t cell_id = sorted[i].cell_id;
                    bool match = cell_in_bbox(cell_id, bbox);
                    std::vector<uint32_t> new_ids;
                    while (i < bounds[t+1] && sorted[i].cell_id == cell_id) {
                        if (match) {
                            auto it = remap.find(sorted[i].item_id);
                            if (it != remap.end()) new_ids.push_back(it->second);
                        }
                        i++;
                    }
                    if (!new_ids.empty()) local[cell_id] = std::move(new_ids);
                }
            });
        }
        for (auto& t : threads) t.join();
        log_phase("        remap_sorted: scan", _rt, _rc);

        // Merge partial maps (no conflicts — cell boundaries are clean splits)
        size_t total = 0;
        for (auto& p : partial) total += p.size();
        dst.reserve(total);
        for (auto& p : partial) {
            for (auto& [k, v] : p) dst[k] = std::move(v);
        }
        log_phase("        remap_sorted: merge", _rt, _rc);
    };

    // Build sorted pairs for continent by filtering + remapping in one pass.
    // Much faster than building hash maps — write_index uses sorted pairs directly.
    auto filter_and_remap_sorted = [&](const std::vector<CellItemPair>& sorted,
                                        const std::unordered_map<uint32_t, uint32_t>& remap,
                                        std::vector<CellItemPair>& dst) {
        if (sorted.empty()) return;
        auto _rt = std::chrono::steady_clock::now();
        auto _rc = CpuTicks::now();

        unsigned nthreads = std::max(1u, std::thread::hardware_concurrency() / 4);
        size_t chunk = (sorted.size() + nthreads - 1) / nthreads;

        std::vector<size_t> bounds = {0};
        for (unsigned t = 1; t < nthreads; t++) {
            size_t target = t * chunk;
            if (target >= sorted.size()) break;
            while (target < sorted.size() && sorted[target].cell_id == sorted[target-1].cell_id)
                target++;
            if (target < sorted.size()) bounds.push_back(target);
        }
        bounds.push_back(sorted.size());

        // Each thread produces a local vector of filtered+remapped pairs
        std::vector<std::vector<CellItemPair>> thread_pairs(bounds.size() - 1);
        std::vector<std::thread> threads;
        for (size_t t = 0; t + 1 < bounds.size(); t++) {
            threads.emplace_back([&, t]() {
                auto& local = thread_pairs[t];
                for (size_t i = bounds[t]; i < bounds[t+1]; ) {
                    uint64_t cell_id = sorted[i].cell_id;
                    bool match = cell_in_bbox(cell_id, bbox);
                    while (i < bounds[t+1] && sorted[i].cell_id == cell_id) {
                        if (match) {
                            auto it = remap.find(sorted[i].item_id);
                            if (it != remap.end())
                                local.push_back({cell_id, it->second});
                        }
                        i++;
                    }
                }
            });
        }
        for (auto& t : threads) t.join();
        log_phase("        sorted remap: scan", _rt, _rc);

        // Concatenate (already in sorted cell_id order since chunks are at cell boundaries)
        size_t total = 0;
        for (auto& v : thread_pairs) total += v.size();
        dst.reserve(total);
        for (auto& v : thread_pairs) dst.insert(dst.end(), v.begin(), v.end());
        log_phase("        sorted remap: concat", _rt, _rc);
    };

    {
        // Build sorted pairs for ways/addrs/interps (write_index uses these directly)
        auto f1 = std::async(std::launch::async, [&]{ filter_and_remap_sorted(full.sorted_way_cells, way_remap, out.sorted_way_cells); });
        auto f2 = std::async(std::launch::async, [&]{ filter_and_remap_sorted(full.sorted_addr_cells, addr_remap, out.sorted_addr_cells); });
        auto f3 = std::async(std::launch::async, [&]{ filter_and_remap_sorted(full.sorted_interp_cells, interp_remap, out.sorted_interp_cells); });
        // Admin still uses hash map (has interior flags)
        auto f4 = std::async(std::launch::async, [&]{ remap_cells_map(full.cell_to_admin, admin_remap, out.cell_to_admin, true); });
        f1.get(); f2.get(); f3.get(); f4.get();
    }
    log_phase("      filter: cell map remap", _ft, _fc);

    // Rebuild compact string pool. Skip NO_DATA sentinels — addr_points
    // with unresolved parent streets keep NO_DATA rather than a string
    // offset, and must not be inserted into the pool.
    std::unordered_set<uint32_t> used_offsets;
    auto add_used = [&](uint32_t off) { if (off != NO_DATA) used_offsets.insert(off); };
    for (const auto& w : out.ways) add_used(w.name_id);
    for (const auto& a : out.addr_points) { add_used(a.housenumber_id); add_used(a.street_id); }
    for (const auto& iw : out.interp_ways) add_used(iw.street_id);
    for (const auto& ap : out.admin_polygons) add_used(ap.name_id);

    const auto& old_sp = full.string_pool.data();
    std::unordered_map<uint32_t, uint32_t> string_remap;
    auto& new_sp = out.string_pool.mutable_data();
    new_sp.clear();

    std::vector<uint32_t> sorted_offsets(used_offsets.begin(), used_offsets.end());
    std::sort(sorted_offsets.begin(), sorted_offsets.end());
    for (uint32_t old_off : sorted_offsets) {
        uint32_t new_off = static_cast<uint32_t>(new_sp.size());
        string_remap[old_off] = new_off;
        const char* str = old_sp.data() + old_off;
        size_t len = std::strlen(str);
        new_sp.insert(new_sp.end(), str, str + len + 1);
    }
    // NO_DATA maps to itself so addr_points without a resolved street
    // keep the sentinel after remapping.
    auto remap_or_sentinel = [&](uint32_t off) -> uint32_t {
        return off == NO_DATA ? NO_DATA : string_remap[off];
    };

    for (auto& w : out.ways) w.name_id = remap_or_sentinel(w.name_id);
    for (auto& a : out.addr_points) { a.housenumber_id = remap_or_sentinel(a.housenumber_id); a.street_id = remap_or_sentinel(a.street_id); }
    for (auto& iw : out.interp_ways) iw.street_id = remap_or_sentinel(iw.street_id);
    for (auto& ap : out.admin_polygons) ap.name_id = remap_or_sentinel(ap.name_id);
    log_phase("      filter: string pool rebuild", _ft, _fc);

    return out;
}

ParsedData filter_by_bbox_masked(const ParsedData& full, const ContinentBBox& bbox,
    uint8_t continent_bit,
    const std::vector<uint8_t>& way_masks,
    const std::vector<uint8_t>& addr_masks,
    const std::vector<uint8_t>& interp_masks,
    const std::vector<std::pair<double,double>>* polygon) {

    ParsedData out;
    auto _ft = std::chrono::steady_clock::now();
    auto _fc = CpuTicks::now();

    // Fast mask-based filter using bitset → sorted vector directly
    // Eliminates hash set entirely. O(1) bit set, O(n) scan to extract sorted IDs.
    auto filter_sorted_masked = [&](const std::vector<CellItemPair>& sorted,
                                     const std::vector<uint8_t>& masks,
                                     uint32_t max_id) {
        std::vector<uint32_t> result;
        if (sorted.empty()) return result;

        unsigned nthreads = std::max(1u, std::thread::hardware_concurrency() / 4);
        size_t bitset_bytes = (max_id + 8) / 8;
        size_t chunk = (sorted.size() + nthreads - 1) / nthreads;
        std::vector<size_t> bounds = {0};
        for (unsigned t = 1; t < nthreads; t++) {
            size_t target = t * chunk;
            if (target >= sorted.size()) break;
            while (target < sorted.size() && sorted[target].cell_id == sorted[target-1].cell_id) target++;
            if (target < sorted.size()) bounds.push_back(target);
        }
        bounds.push_back(sorted.size());

        // Per-thread bitsets to avoid race conditions on concurrent byte writes
        std::vector<std::vector<uint8_t>> thread_bitsets(bounds.size() - 1);
        std::vector<std::thread> threads;
        for (size_t t = 0; t + 1 < bounds.size(); t++) {
            threads.emplace_back([&, t]() {
                auto& bs = thread_bitsets[t];
                bs.resize(bitset_bytes, 0);
                for (size_t i = bounds[t]; i < bounds[t+1]; i++) {
                    if (masks[i] & continent_bit) {
                        uint32_t id = sorted[i].item_id;
                        if (id < max_id)
                            bs[id / 8] |= (1 << (id % 8));
                    }
                }
            });
        }
        for (auto& t : threads) t.join();

        // Merge with OR
        std::vector<uint8_t> bitset(bitset_bytes, 0);
        for (auto& bs : thread_bitsets) {
            for (size_t i = 0; i < bitset_bytes; i++) bitset[i] |= bs[i];
        }

        // Extract sorted IDs from bitset (already in ascending order)
        for (uint32_t id = 0; id < max_id; id++) {
            if (bitset[id / 8] & (1 << (id % 8))) result.push_back(id);
        }
        return result;
    };

    // Filter from hash map (admin only — no precomputed masks)
    auto filter_cells_map = [&](const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map,
                                bool mask_interior = false) {
        std::unordered_set<uint32_t> ids;
        for (const auto& [cell_id, cell_ids] : cell_map) {
            if (cell_in_bbox(cell_id, bbox)) {
                for (uint32_t id : cell_ids)
                    ids.insert(mask_interior ? (id & ID_MASK) : id);
            }
        }
        return ids;
    };

    // Pass max_id for each data type to size the bitset
    uint32_t max_way_id = static_cast<uint32_t>(full.ways.size());
    uint32_t max_addr_id = static_cast<uint32_t>(full.addr_points.size());
    uint32_t max_interp_id = static_cast<uint32_t>(full.interp_ways.size());

    auto f_ways = std::async(std::launch::async, [&]{ return filter_sorted_masked(full.sorted_way_cells, way_masks, max_way_id); });
    auto f_addrs = std::async(std::launch::async, [&]{ return filter_sorted_masked(full.sorted_addr_cells, addr_masks, max_addr_id); });
    auto f_interps = std::async(std::launch::async, [&]{ return filter_sorted_masked(full.sorted_interp_cells, interp_masks, max_interp_id); });
    auto f_admin = std::async(std::launch::async, [&]{ return filter_cells_map(full.cell_to_admin, true); });

    auto used_way_ids = f_ways.get();     // sorted vector<uint32_t>
    auto used_addr_ids = f_addrs.get();   // sorted vector<uint32_t>
    auto used_interp_ids = f_interps.get(); // sorted vector<uint32_t>
    auto used_admin_ids = f_admin.get();  // unordered_set<uint32_t> (admin still uses hash map)
    log_phase("      filter: ID collection (masked)", _ft, _fc);

    // Reuse the same remap + cell remap + string pool logic from filter_by_bbox
    // (inline the rest — same as filter_by_bbox from the remap section onwards)
    std::unordered_map<uint32_t, uint32_t> way_remap, addr_remap, interp_remap, admin_remap;

    auto f_remap_ways = std::async(std::launch::async, [&]() {
        auto& sorted_ids = used_way_ids;
        std::vector<WayHeader> ways; std::vector<NodeCoord> nodes;
        ways.reserve(sorted_ids.size()); nodes.reserve(sorted_ids.size() * 5);
        std::unordered_map<uint32_t, uint32_t> remap; remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            const auto& w = full.ways[old_id];
            // Coordinate-level polygon test — include way if ANY node is inside
            if (polygon && w.node_count > 0) {
                bool any_inside = false;
                for (uint8_t n = 0; n < w.node_count; n++) {
                    const auto& nd = full.street_nodes[w.node_offset + n];
                    if (point_in_polygon(nd.lat, nd.lng, *polygon)) { any_inside = true; break; }
                }
                if (!any_inside) continue;
            }
            remap[old_id] = static_cast<uint32_t>(ways.size());
            WayHeader nw = w;
            nw.node_offset = static_cast<uint32_t>(nodes.size()); ways.push_back(nw);
            for (uint8_t n = 0; n < w.node_count; n++) nodes.push_back(full.street_nodes[w.node_offset + n]);
        }
        return std::make_tuple(std::move(remap), std::move(ways), std::move(nodes));
    });
    auto f_remap_addrs = std::async(std::launch::async, [&]() {
        auto& sorted_ids = used_addr_ids;
        std::vector<AddrPoint> addrs; addrs.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap; remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            const auto& a = full.addr_points[old_id];
            // Coordinate-level polygon test on address point
            if (polygon && !point_in_polygon(a.lat, a.lng, *polygon)) continue;
            remap[old_id] = static_cast<uint32_t>(addrs.size()); addrs.push_back(a);
        }
        return std::make_tuple(std::move(remap), std::move(addrs));
    });
    auto f_remap_interps = std::async(std::launch::async, [&]() {
        auto& sorted_ids = used_interp_ids;
        std::vector<InterpWay> iways; std::vector<NodeCoord> inodes;
        iways.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap; remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            remap[old_id] = static_cast<uint32_t>(iways.size());
            const auto& iw = full.interp_ways[old_id]; InterpWay niw = iw;
            niw.node_offset = static_cast<uint32_t>(inodes.size()); iways.push_back(niw);
            for (uint8_t n = 0; n < iw.node_count; n++) inodes.push_back(full.interp_nodes[iw.node_offset + n]);
        }
        return std::make_tuple(std::move(remap), std::move(iways), std::move(inodes));
    });
    auto f_remap_admins = std::async(std::launch::async, [&]() {
        std::vector<uint32_t> sorted_ids(used_admin_ids.begin(), used_admin_ids.end());
        std::sort(sorted_ids.begin(), sorted_ids.end());
        std::vector<AdminPolygon> polys; std::vector<NodeCoord> verts;
        polys.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap; remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            remap[old_id] = static_cast<uint32_t>(polys.size());
            const auto& ap = full.admin_polygons[old_id]; AdminPolygon nap = ap;
            nap.vertex_offset = static_cast<uint32_t>(verts.size()); polys.push_back(nap);
            for (uint32_t v = 0; v < ap.vertex_count; v++) verts.push_back(full.admin_vertices[ap.vertex_offset + v]);
        }
        return std::make_tuple(std::move(remap), std::move(polys), std::move(verts));
    });

    { auto [wr, ways, nodes] = f_remap_ways.get(); way_remap = std::move(wr); out.ways = std::move(ways); out.street_nodes = std::move(nodes); }
    { auto [ar, addrs] = f_remap_addrs.get(); addr_remap = std::move(ar); out.addr_points = std::move(addrs); }
    { auto [ir, iways, inodes] = f_remap_interps.get(); interp_remap = std::move(ir); out.interp_ways = std::move(iways); out.interp_nodes = std::move(inodes); }
    { auto [ar, polys, verts] = f_remap_admins.get(); admin_remap = std::move(ar); out.admin_polygons = std::move(polys); out.admin_vertices = std::move(verts); }
    log_phase("      filter: data remap (masked)", _ft, _fc);

    // Remap sorted pairs using mask for fast filtering
    auto remap_sorted_masked = [&](const std::vector<CellItemPair>& sorted,
                                    const std::vector<uint8_t>& masks,
                                    const std::unordered_map<uint32_t, uint32_t>& remap,
                                    std::vector<CellItemPair>& dst) {
        if (sorted.empty()) return;
        unsigned nthreads = std::max(1u, std::thread::hardware_concurrency() / 4);
        size_t chunk = (sorted.size() + nthreads - 1) / nthreads;
        std::vector<size_t> bounds = {0};
        for (unsigned t = 1; t < nthreads; t++) {
            size_t target = t * chunk;
            if (target >= sorted.size()) break;
            while (target < sorted.size() && sorted[target].cell_id == sorted[target-1].cell_id) target++;
            if (target < sorted.size()) bounds.push_back(target);
        }
        bounds.push_back(sorted.size());
        std::vector<std::vector<CellItemPair>> thread_pairs(bounds.size() - 1);
        std::vector<std::thread> threads;
        for (size_t t = 0; t + 1 < bounds.size(); t++) {
            threads.emplace_back([&, t]() {
                auto& local = thread_pairs[t];
                for (size_t i = bounds[t]; i < bounds[t+1]; i++) {
                    if (masks[i] & continent_bit) {
                        auto it = remap.find(sorted[i].item_id);
                        if (it != remap.end()) local.push_back({sorted[i].cell_id, it->second});
                    }
                }
            });
        }
        for (auto& t : threads) t.join();
        size_t total = 0;
        for (auto& v : thread_pairs) total += v.size();
        dst.reserve(total);
        for (auto& v : thread_pairs) dst.insert(dst.end(), v.begin(), v.end());
    };

    // Admin remap still uses cell_in_bbox (no precomputed masks for admin hash map)
    auto remap_cells_map = [&](const std::unordered_map<uint64_t, std::vector<uint32_t>>& src,
                               const std::unordered_map<uint32_t, uint32_t>& remap,
                               std::unordered_map<uint64_t, std::vector<uint32_t>>& dst,
                               bool handle_flags = false) {
        for (const auto& [cell_id, ids] : src) {
            if (!cell_in_bbox(cell_id, bbox)) continue;
            std::vector<uint32_t> new_ids;
            for (uint32_t id : ids) {
                uint32_t raw_id = handle_flags ? (id & ID_MASK) : id;
                uint32_t flags = handle_flags ? (id & INTERIOR_FLAG) : 0;
                auto it = remap.find(raw_id);
                if (it != remap.end()) new_ids.push_back(it->second | flags);
            }
            if (!new_ids.empty()) dst[cell_id] = std::move(new_ids);
        }
    };

    {
        auto f1 = std::async(std::launch::async, [&]{ remap_sorted_masked(full.sorted_way_cells, way_masks, way_remap, out.sorted_way_cells); });
        auto f2 = std::async(std::launch::async, [&]{ remap_sorted_masked(full.sorted_addr_cells, addr_masks, addr_remap, out.sorted_addr_cells); });
        auto f3 = std::async(std::launch::async, [&]{ remap_sorted_masked(full.sorted_interp_cells, interp_masks, interp_remap, out.sorted_interp_cells); });
        auto f4 = std::async(std::launch::async, [&]{ remap_cells_map(full.cell_to_admin, admin_remap, out.cell_to_admin, true); });
        f1.get(); f2.get(); f3.get(); f4.get();
    }
    log_phase("      filter: cell map remap (masked)", _ft, _fc);

    // Rebuild compact string pool. Skip NO_DATA sentinels (see unmasked
    // filter_by_bbox above for the same logic + rationale).
    std::unordered_set<uint32_t> used_offsets;
    auto add_used_m = [&](uint32_t off) { if (off != NO_DATA) used_offsets.insert(off); };
    for (const auto& w : out.ways) add_used_m(w.name_id);
    for (const auto& a : out.addr_points) { add_used_m(a.housenumber_id); add_used_m(a.street_id); }
    for (const auto& iw : out.interp_ways) add_used_m(iw.street_id);
    for (const auto& ap : out.admin_polygons) add_used_m(ap.name_id);

    const auto& old_sp = full.string_pool.data();
    std::unordered_map<uint32_t, uint32_t> string_remap;
    auto& new_sp = out.string_pool.mutable_data();
    new_sp.clear();
    std::vector<uint32_t> sorted_offsets(used_offsets.begin(), used_offsets.end());
    std::sort(sorted_offsets.begin(), sorted_offsets.end());
    for (uint32_t old_off : sorted_offsets) {
        uint32_t new_off = static_cast<uint32_t>(new_sp.size());
        string_remap[old_off] = new_off;
        const char* str = old_sp.data() + old_off;
        size_t len = std::strlen(str);
        new_sp.insert(new_sp.end(), str, str + len + 1);
    }
    auto remap_or_sentinel_m = [&](uint32_t off) -> uint32_t {
        return off == NO_DATA ? NO_DATA : string_remap[off];
    };
    for (auto& w : out.ways) w.name_id = remap_or_sentinel_m(w.name_id);
    for (auto& a : out.addr_points) { a.housenumber_id = remap_or_sentinel_m(a.housenumber_id); a.street_id = remap_or_sentinel_m(a.street_id); }
    for (auto& iw : out.interp_ways) iw.street_id = remap_or_sentinel_m(iw.street_id);
    for (auto& ap : out.admin_polygons) ap.name_id = remap_or_sentinel_m(ap.name_id);
    log_phase("      filter: string pool rebuild (masked)", _ft, _fc);

    return out;
}
