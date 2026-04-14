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

ParsedData filter_by_bbox_masked(const ParsedData& full, const ContinentBBox& bbox,
    uint8_t continent_bit,
    const std::vector<uint8_t>& way_masks,
    const std::vector<uint8_t>& addr_masks,
    const std::vector<uint8_t>& interp_masks,
    const std::vector<uint8_t>& poi_masks,
    const std::vector<uint8_t>& place_masks,
    const std::vector<std::pair<double,double>>* polygon) {

    ParsedData out;
    auto _ft = std::chrono::steady_clock::now();
    auto _fc = CpuTicks::now();

    // Fast mask-based filter: bitset indexed by raw item_id.
    // INTERIOR_FLAG is stripped during indexing so flagged items (e.g. POIs)
    // are captured — the flag is re-applied during sorted-pair remap below.
    auto filter_sorted_masked = [&](const std::vector<CellItemPair>& sorted,
                                     const std::vector<uint8_t>& masks,
                                     uint32_t max_id) {
        std::vector<uint32_t> result;
        if (sorted.empty() || max_id == 0) return result;

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

        std::vector<std::vector<uint8_t>> thread_bitsets(bounds.size() - 1);
        std::vector<std::thread> threads;
        for (size_t t = 0; t + 1 < bounds.size(); t++) {
            threads.emplace_back([&, t]() {
                auto& bs = thread_bitsets[t];
                bs.resize(bitset_bytes, 0);
                for (size_t i = bounds[t]; i < bounds[t+1]; i++) {
                    if (masks[i] & continent_bit) {
                        uint32_t id = sorted[i].item_id & ID_MASK;
                        if (id < max_id)
                            bs[id / 8] |= (1 << (id % 8));
                    }
                }
            });
        }
        for (auto& t : threads) t.join();

        std::vector<uint8_t> bitset(bitset_bytes, 0);
        for (auto& bs : thread_bitsets) {
            for (size_t i = 0; i < bitset_bytes; i++) bitset[i] |= bs[i];
        }

        for (uint32_t id = 0; id < max_id; id++) {
            if (bitset[id / 8] & (1 << (id % 8))) result.push_back(id);
        }
        return result;
    };

    // Filter admin IDs from hash map (admin still has no precomputed masks)
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

    const uint32_t max_way_id = static_cast<uint32_t>(full.ways.size());
    const uint32_t max_addr_id = static_cast<uint32_t>(full.addr_points.size());
    const uint32_t max_interp_id = static_cast<uint32_t>(full.interp_ways.size());
    const uint32_t max_poi_id = static_cast<uint32_t>(full.poi_records.size());
    const uint32_t max_place_id = static_cast<uint32_t>(full.place_nodes.size());

    auto f_ways    = std::async(std::launch::async, [&]{ return filter_sorted_masked(full.sorted_way_cells,    way_masks,    max_way_id); });
    auto f_addrs   = std::async(std::launch::async, [&]{ return filter_sorted_masked(full.sorted_addr_cells,   addr_masks,   max_addr_id); });
    auto f_interps = std::async(std::launch::async, [&]{ return filter_sorted_masked(full.sorted_interp_cells, interp_masks, max_interp_id); });
    auto f_pois    = std::async(std::launch::async, [&]{ return filter_sorted_masked(full.sorted_poi_cells,    poi_masks,    max_poi_id); });
    auto f_places  = std::async(std::launch::async, [&]{ return filter_sorted_masked(full.sorted_place_cells,  place_masks,  max_place_id); });
    auto f_admin   = std::async(std::launch::async, [&]{ return filter_cells_map(full.cell_to_admin, true); });

    auto used_way_ids    = f_ways.get();
    auto used_addr_ids   = f_addrs.get();
    auto used_interp_ids = f_interps.get();
    auto used_poi_ids    = f_pois.get();
    auto used_place_ids  = f_places.get();
    auto used_admin_ids  = f_admin.get();
    log_phase("      filter: ID collection (masked)", _ft, _fc);

    std::unordered_map<uint32_t, uint32_t> way_remap, addr_remap, interp_remap,
                                            admin_remap, poi_remap, place_remap;

    // Ways — with coordinate-level polygon refinement if provided
    auto f_remap_ways = std::async(std::launch::async, [&]() {
        auto& sorted_ids = used_way_ids;
        std::vector<WayHeader> ways;
        std::vector<NodeCoord> nodes;
        ways.reserve(sorted_ids.size());
        nodes.reserve(sorted_ids.size() * 5);
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            const auto& w = full.ways[old_id];
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
            nw.node_offset = static_cast<uint32_t>(nodes.size());
            ways.push_back(nw);
            for (uint8_t n = 0; n < w.node_count; n++)
                nodes.push_back(full.street_nodes[w.node_offset + n]);
        }
        return std::make_tuple(std::move(remap), std::move(ways), std::move(nodes));
    });

    // Addrs — coord polygon refinement
    auto f_remap_addrs = std::async(std::launch::async, [&]() {
        auto& sorted_ids = used_addr_ids;
        std::vector<AddrPoint> addrs;
        addrs.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            const auto& a = full.addr_points[old_id];
            if (polygon && !point_in_polygon(a.lat, a.lng, *polygon)) continue;
            remap[old_id] = static_cast<uint32_t>(addrs.size());
            addrs.push_back(a);
        }
        return std::make_tuple(std::move(remap), std::move(addrs));
    });

    // Interps
    auto f_remap_interps = std::async(std::launch::async, [&]() {
        auto& sorted_ids = used_interp_ids;
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

    // Admin
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

    // POIs — coord polygon refinement, preserve vertex arrays for polygon POIs
    auto f_remap_pois = std::async(std::launch::async, [&]() {
        auto& sorted_ids = used_poi_ids;
        std::vector<PoiRecord> pois;
        std::vector<NodeCoord> verts;
        pois.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            const auto& p = full.poi_records[old_id];
            if (polygon && !point_in_polygon(p.lat, p.lng, *polygon)) continue;
            remap[old_id] = static_cast<uint32_t>(pois.size());
            PoiRecord np = p;
            uint32_t old_voff = p.vertex_offset;
            uint32_t vc = p.vertex_count;
            np.vertex_offset = static_cast<uint32_t>(verts.size());
            if (vc > 0 && old_voff != NO_DATA) {
                for (uint32_t v = 0; v < vc; v++)
                    verts.push_back(full.poi_vertices[old_voff + v]);
            }
            pois.push_back(np);
        }
        return std::make_tuple(std::move(remap), std::move(pois), std::move(verts));
    });

    // Place nodes — parent_poly_id remapped in a second pass below, after
    // admin_remap is ready. Keeps the async pipeline free of dependencies.
    auto f_remap_places = std::async(std::launch::async, [&]() {
        auto& sorted_ids = used_place_ids;
        std::vector<PlaceNode> places;
        places.reserve(sorted_ids.size());
        std::unordered_map<uint32_t, uint32_t> remap;
        remap.reserve(sorted_ids.size());
        for (uint32_t old_id : sorted_ids) {
            const auto& pn = full.place_nodes[old_id];
            if (polygon && !point_in_polygon(pn.lat, pn.lng, *polygon)) continue;
            remap[old_id] = static_cast<uint32_t>(places.size());
            places.push_back(pn);
        }
        return std::make_tuple(std::move(remap), std::move(places));
    });

    { auto [wr, ways, nodes] = f_remap_ways.get();    way_remap    = std::move(wr); out.ways           = std::move(ways); out.street_nodes = std::move(nodes); }
    { auto [ar, addrs]       = f_remap_addrs.get();   addr_remap   = std::move(ar); out.addr_points    = std::move(addrs); }
    { auto [ir, iways, inds] = f_remap_interps.get(); interp_remap = std::move(ir); out.interp_ways    = std::move(iways); out.interp_nodes = std::move(inds); }
    { auto [ar, polys, vts]  = f_remap_admins.get();  admin_remap  = std::move(ar); out.admin_polygons = std::move(polys); out.admin_vertices = std::move(vts); }
    { auto [pr, pois, vts]   = f_remap_pois.get();    poi_remap    = std::move(pr); out.poi_records    = std::move(pois); out.poi_vertices = std::move(vts); }
    { auto [plr, places]     = f_remap_places.get();  place_remap  = std::move(plr); out.place_nodes   = std::move(places); }
    log_phase("      filter: data remap (masked)", _ft, _fc);

    // --- Project parent chains through admin_remap ---
    // way_parent_ids: parallel to full.ways, values are old admin_poly_ids
    if (!full.way_parent_ids.empty()) {
        out.way_parent_ids.assign(out.ways.size(), NO_DATA);
        for (const auto& [old_wid, new_wid] : way_remap) {
            if (old_wid >= full.way_parent_ids.size()) continue;
            uint32_t old_parent = full.way_parent_ids[old_wid];
            if (old_parent == NO_DATA) continue;
            auto it = admin_remap.find(old_parent);
            out.way_parent_ids[new_wid] = (it != admin_remap.end()) ? it->second : NO_DATA;
        }
    }

    // admin_parent_ids: parallel to full.admin_polygons, values are old admin_poly_ids
    if (!full.admin_parent_ids.empty()) {
        out.admin_parent_ids.assign(out.admin_polygons.size(), NO_DATA);
        for (const auto& [old_pid, new_pid] : admin_remap) {
            if (old_pid >= full.admin_parent_ids.size()) continue;
            uint32_t old_parent = full.admin_parent_ids[old_pid];
            if (old_parent == NO_DATA) continue;
            auto it = admin_remap.find(old_parent);
            out.admin_parent_ids[new_pid] = (it != admin_remap.end()) ? it->second : NO_DATA;
        }
    }

    // place_nodes.parent_poly_id — second pass after admin_remap is available
    for (auto& pn : out.place_nodes) {
        if (pn.parent_poly_id != NO_DATA) {
            auto it = admin_remap.find(pn.parent_poly_id);
            pn.parent_poly_id = (it != admin_remap.end()) ? it->second : NO_DATA;
        }
    }

    // --- Project postcode-id parallel arrays ---
    // Values are string offsets in full.string_pool — remapped during string
    // compaction below.
    if (!full.way_postcode_ids.empty()) {
        out.way_postcode_ids.assign(out.ways.size(), NO_DATA);
        for (const auto& [old_wid, new_wid] : way_remap) {
            if (old_wid >= full.way_postcode_ids.size()) continue;
            out.way_postcode_ids[new_wid] = full.way_postcode_ids[old_wid];
        }
    }
    if (!full.addr_postcode_ids.empty()) {
        out.addr_postcode_ids.assign(out.addr_points.size(), NO_DATA);
        for (const auto& [old_aid, new_aid] : addr_remap) {
            if (old_aid >= full.addr_postcode_ids.size()) continue;
            out.addr_postcode_ids[new_aid] = full.addr_postcode_ids[old_aid];
        }
    }

    // --- Copy postcode_accum ---
    // The write-time centroid construction (cell_index.cpp:431-485) validates
    // entries against out.cell_to_admin, so TIGER/GeoNames accumulator entries
    // outside the continent naturally drop out. Keys are string offsets in the
    // FULL pool — remapped via string_remap below.
    out.postcode_accum = full.postcode_accum;

    log_phase("      filter: parent + postcode projection", _ft, _fc);

    // --- Remap sorted cell arrays for all 5 types (preserving INTERIOR_FLAG) ---
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
                        uint32_t raw   = sorted[i].item_id & ID_MASK;
                        uint32_t flags = sorted[i].item_id & INTERIOR_FLAG;
                        auto it = remap.find(raw);
                        if (it != remap.end())
                            local.push_back({sorted[i].cell_id, it->second | flags});
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
        auto f1 = std::async(std::launch::async, [&]{ remap_sorted_masked(full.sorted_way_cells,    way_masks,    way_remap,    out.sorted_way_cells); });
        auto f2 = std::async(std::launch::async, [&]{ remap_sorted_masked(full.sorted_addr_cells,   addr_masks,   addr_remap,   out.sorted_addr_cells); });
        auto f3 = std::async(std::launch::async, [&]{ remap_sorted_masked(full.sorted_interp_cells, interp_masks, interp_remap, out.sorted_interp_cells); });
        auto f4 = std::async(std::launch::async, [&]{ remap_sorted_masked(full.sorted_poi_cells,    poi_masks,    poi_remap,    out.sorted_poi_cells); });
        auto f5 = std::async(std::launch::async, [&]{ remap_sorted_masked(full.sorted_place_cells,  place_masks,  place_remap,  out.sorted_place_cells); });
        auto f6 = std::async(std::launch::async, [&]{ remap_cells_map(full.cell_to_admin, admin_remap, out.cell_to_admin, true); });
        f1.get(); f2.get(); f3.get(); f4.get(); f5.get(); f6.get();
    }
    log_phase("      filter: cell map remap (masked)", _ft, _fc);

    // --- String pool compaction ---
    // Collect every surviving offset from ways, addrs, interps, admin polygons,
    // place nodes, POI records, way/addr postcode arrays, and postcode_accum keys.
    std::unordered_set<uint32_t> used_offsets;
    auto add_used = [&](uint32_t off) { if (off != NO_DATA) used_offsets.insert(off); };
    for (const auto& w : out.ways) add_used(w.name_id);
    for (const auto& a : out.addr_points) { add_used(a.housenumber_id); add_used(a.street_id); }
    for (const auto& iw : out.interp_ways) add_used(iw.street_id);
    for (const auto& ap : out.admin_polygons) add_used(ap.name_id);
    for (const auto& pn : out.place_nodes) add_used(pn.name_id);
    for (const auto& pr : out.poi_records) { add_used(pr.name_id); add_used(pr.parent_street_id); }
    for (uint32_t off : out.way_postcode_ids) add_used(off);
    for (uint32_t off : out.addr_postcode_ids) add_used(off);
    for (const auto& [pc_id, _acc] : out.postcode_accum) add_used(pc_id);

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
    auto remap_or_sentinel = [&](uint32_t off) -> uint32_t {
        if (off == NO_DATA) return NO_DATA;
        auto it = string_remap.find(off);
        return it == string_remap.end() ? NO_DATA : it->second;
    };

    for (auto& w : out.ways) w.name_id = remap_or_sentinel(w.name_id);
    for (auto& a : out.addr_points) {
        a.housenumber_id = remap_or_sentinel(a.housenumber_id);
        a.street_id = remap_or_sentinel(a.street_id);
    }
    for (auto& iw : out.interp_ways) iw.street_id = remap_or_sentinel(iw.street_id);
    for (auto& ap : out.admin_polygons) ap.name_id = remap_or_sentinel(ap.name_id);
    for (auto& pn : out.place_nodes) pn.name_id = remap_or_sentinel(pn.name_id);
    for (auto& pr : out.poi_records) {
        pr.name_id = remap_or_sentinel(pr.name_id);
        pr.parent_street_id = remap_or_sentinel(pr.parent_street_id);
    }
    for (auto& off : out.way_postcode_ids) off = remap_or_sentinel(off);
    for (auto& off : out.addr_postcode_ids) off = remap_or_sentinel(off);

    // Rebuild postcode_accum with remapped keys (drop entries whose string didn't survive)
    {
        std::unordered_map<uint32_t, ParsedData::PostcodeAccum> remapped;
        remapped.reserve(out.postcode_accum.size());
        for (auto& [old_pc_id, acc] : out.postcode_accum) {
            uint32_t new_pc_id = remap_or_sentinel(old_pc_id);
            if (new_pc_id == NO_DATA) continue;
            auto& dst = remapped[new_pc_id];
            dst.sum_lat += acc.sum_lat;
            dst.sum_lng += acc.sum_lng;
            dst.count += acc.count;
            dst.country_code = acc.country_code;
        }
        out.postcode_accum = std::move(remapped);
    }

    log_phase("      filter: string pool rebuild (masked)", _ft, _fc);

    std::cerr << "    subset: ways=" << out.ways.size()
              << " addrs=" << out.addr_points.size()
              << " interps=" << out.interp_ways.size()
              << " pois=" << out.poi_records.size()
              << " places=" << out.place_nodes.size()
              << " admins=" << out.admin_polygons.size()
              << " postcodes=" << out.postcode_accum.size()
              << std::endl;

    return out;
}
