#include "continent_filter.h"
#include "continent_boundaries.h"
#include "parsed_data.h"
#include "parallel.h"
#include "subset_copy.h"

#include <algorithm>
#include <cstring>
#include <future>

#include <s2/s2cell_id.h>
#include <s2/s2latlng.h>

// Workers per sorted-pair pass: the record classes' passes run at once.
static unsigned pair_pass_threads() {
    return std::max(1u, parallel_threads() / 4);
}

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

// Cell membership test used for the admin cell maps: polygon-based when a
// boundary polygon is available (same semantics as precompute_masks for the
// record classes), hand-written bbox fallback otherwise. The bbox-only test
// dropped admin data for zones the bboxes miss but the Geofabrik polygons
// cover (Guam, Adak, the Azores, island territories).
static bool cell_in_continent(uint64_t cell_id, const ContinentBBox& bbox,
                              const std::vector<std::pair<double,double>>* polygon) {
    if (!polygon) return cell_in_bbox(cell_id, bbox);
    S2CellId cell(cell_id);
    S2LatLng center = cell.ToLatLng();
    return point_in_polygon(center.lat().degrees(), center.lng().degrees(), *polygon);
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

        size_t bitset_bytes = (max_id + 8) / 8;
        const unsigned threads = pair_pass_threads();
        std::vector<std::vector<uint8_t>> thread_bitsets(threads);
        parallel_for_runs(sorted.size(), same_cell(sorted), [&](size_t begin, size_t end, unsigned w) {
            auto& bs = thread_bitsets[w];
            bs.resize(bitset_bytes, 0);
            for (size_t i = begin; i < end; i++) {
                if (masks[i] & continent_bit) {
                    uint32_t id = sorted[i].item_id & ID_MASK;
                    if (id < max_id)
                        bs[id / 8] |= (1 << (id % 8));
                }
            }
        }, threads);

        std::vector<uint8_t> bitset(bitset_bytes, 0);
        for (auto& bs : thread_bitsets) {
            if (bs.empty()) continue;
            for (size_t i = 0; i < bitset_bytes; i++) bitset[i] |= bs[i];
        }

        for (uint32_t id = 0; id < max_id; id++) {
            if (bitset[id / 8] & (1 << (id % 8))) result.push_back(id);
        }
        return result;
    };

    // Admin ids of the continent's cells (admin has no precomputed masks),
    // ascending.
    auto filter_admin_cells = [&](const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map) {
        std::vector<char> used(full.admin_polygons.size(), 0);
        for (const auto& [cell_id, cell_ids] : cell_map) {
            if (cell_in_continent(cell_id, bbox, polygon)) {
                for (uint32_t id : cell_ids) used.at(id & ID_MASK) = 1;
            }
        }
        std::vector<uint32_t> ids;
        for (uint32_t id = 0; id < used.size(); id++)
            if (used[id]) ids.push_back(id);
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
    auto f_admin   = std::async(std::launch::async, [&]{ return filter_admin_cells(full.cell_to_admin); });

    auto used_way_ids    = f_ways.get();
    auto used_addr_ids   = f_addrs.get();
    auto used_interp_ids = f_interps.get();
    auto used_poi_ids    = f_pois.get();
    auto used_place_ids  = f_places.get();
    auto used_admin_ids  = f_admin.get();
    log_phase(("      " + std::string(bbox.name) + " filter: ID collection (masked)").c_str(), _ft, _fc);

    // Planet id -> continent id, NO_DATA for records the continent drops.
    std::vector<uint32_t> way_remap, addr_remap, interp_remap, admin_remap, poi_remap, place_remap;
    auto remap_id = [](const std::vector<uint32_t>& remap, uint32_t id) {
        return id < remap.size() ? remap[id] : NO_DATA;
    };

    // Ways — with coordinate-level polygon refinement if provided.
    // Strategy-2: also forward osm_ids (parallel to ways) so the
    // continent's IdAllocator pass can run with stable identity.
    auto f_remap_ways = std::async(std::launch::async, [&]() {
        way_remap = copy_subset(used_way_ids, full.ways, full.street_nodes,
            [&](uint32_t id) {
                const auto& w = full.ways[id];
                if (!polygon || w.node_count == 0) return true;
                for (uint16_t n = 0; n < w.node_count; n++) {
                    const auto& nd = full.street_nodes[w.node_offset + n];
                    if (point_in_polygon(nd.lat, nd.lng, *polygon)) return true;
                }
                return false;
            },
            [&](uint32_t id) { return CoordRun{full.ways[id].node_offset, full.ways[id].node_count}; },
            [](WayHeader& w, uint32_t offset, uint32_t) { w.node_offset = offset; },
            out.ways, out.street_nodes);
        out.way_osm_ids = subset_osm_ids(used_way_ids, way_remap, out.ways.size(), full.way_osm_ids);
    });

    // Addrs — coord polygon refinement.  Also carries through the
    // addr_vertices buffer for addr_points with building footprints,
    // remapping vertex_offset to the new buffer.  Without this, the
    // continent's data.addr_vertices stays empty and any polygon-aware
    // downstream consumer (e.g. cell_index.cpp's addr_vertices packer
    // added in v15) crashes on out-of-bounds access.
    auto f_remap_addrs = std::async(std::launch::async, [&]() {
        addr_remap = copy_subset(used_addr_ids, full.addr_points, full.addr_vertices,
            [&](uint32_t id) {
                const auto& a = full.addr_points[id];
                return !polygon || point_in_polygon(a.lat, a.lng, *polygon);
            },
            [&](uint32_t id) {
                const auto& a = full.addr_points[id];
                bool footprint = a.vertex_count > 0 && a.vertex_offset != NO_DATA
                    && (size_t)a.vertex_offset + a.vertex_count <= full.addr_vertices.size();
                return footprint ? CoordRun{a.vertex_offset, a.vertex_count} : CoordRun{0, 0};
            },
            [](AddrPoint& a, uint32_t offset, uint32_t count) {
                a.vertex_offset = count > 0 ? offset : NO_DATA;
                a.vertex_count = count;
            },
            out.addr_points, out.addr_vertices);
        out.addr_osm_ids = subset_osm_ids(used_addr_ids, addr_remap, out.addr_points.size(), full.addr_osm_ids);
    });

    // Interps
    auto f_remap_interps = std::async(std::launch::async, [&]() {
        interp_remap = copy_subset(used_interp_ids, full.interp_ways, full.interp_nodes,
            [](uint32_t) { return true; },
            [&](uint32_t id) { return CoordRun{full.interp_ways[id].node_offset, full.interp_ways[id].node_count}; },
            [](InterpWay& iw, uint32_t offset, uint32_t) { iw.node_offset = offset; },
            out.interp_ways, out.interp_nodes);
        out.interp_osm_ids = subset_osm_ids(used_interp_ids, interp_remap, out.interp_ways.size(), full.interp_osm_ids);
    });

    // Admin
    auto f_remap_admins = std::async(std::launch::async, [&]() {
        admin_remap = copy_subset(used_admin_ids, full.admin_polygons, full.admin_vertices,
            [](uint32_t) { return true; },
            [&](uint32_t id) {
                return CoordRun{full.admin_polygons[id].vertex_offset, full.admin_polygons[id].vertex_count};
            },
            [](AdminPolygon& ap, uint32_t offset, uint32_t) { ap.vertex_offset = offset; },
            out.admin_polygons, out.admin_vertices);
        out.admin_osm_ids = subset_osm_ids(used_admin_ids, admin_remap, out.admin_polygons.size(), full.admin_osm_ids);
    });

    // POIs — coord polygon refinement, preserve vertex arrays for polygon POIs
    // A point POI's vertex_offset also moves to the current end of the
    // vertex array, as it always has.
    auto f_remap_pois = std::async(std::launch::async, [&]() {
        poi_remap = copy_subset(used_poi_ids, full.poi_records, full.poi_vertices,
            [&](uint32_t id) {
                const auto& p = full.poi_records[id];
                return !polygon || point_in_polygon(p.lat, p.lng, *polygon);
            },
            [&](uint32_t id) {
                const auto& p = full.poi_records[id];
                return p.vertex_count > 0 && p.vertex_offset != NO_DATA ? CoordRun{p.vertex_offset, p.vertex_count}
                                                                        : CoordRun{0, 0};
            },
            [](PoiRecord& p, uint32_t offset, uint32_t) { p.vertex_offset = offset; },
            out.poi_records, out.poi_vertices);
        out.poi_osm_ids = subset_osm_ids(used_poi_ids, poi_remap, out.poi_records.size(), full.poi_osm_ids);
    });

    // Place nodes — parent_poly_id remapped in a second pass below, after
    // admin_remap is ready. Keeps the async pipeline free of dependencies.
    auto f_remap_places = std::async(std::launch::async, [&]() {
        const std::vector<NodeCoord> no_coords;
        std::vector<NodeCoord> unused;
        place_remap = copy_subset(used_place_ids, full.place_nodes, no_coords,
            [&](uint32_t id) {
                const auto& pn = full.place_nodes[id];
                return !polygon || point_in_polygon(pn.lat, pn.lng, *polygon);
            },
            [](uint32_t) { return CoordRun{0, 0}; },
            [](PlaceNode&, uint32_t, uint32_t) {},
            out.place_nodes, unused);
        out.place_osm_ids = subset_osm_ids(used_place_ids, place_remap, out.place_nodes.size(), full.place_osm_ids);
    });

    f_remap_ways.get();
    f_remap_addrs.get();
    f_remap_interps.get();
    f_remap_admins.get();
    f_remap_pois.get();
    f_remap_places.get();
    log_phase(("      " + std::string(bbox.name) + " filter: data remap (masked)").c_str(), _ft, _fc);

    // --- Project parent chains through admin_remap ---
    // way_parent_ids: parallel to full.ways, values are old admin_poly_ids
    if (!full.way_parent_ids.empty()) {
        out.way_parent_ids.assign(out.ways.size(), NO_DATA);
        parallel_each(used_way_ids, [&](uint32_t old_wid) {
            uint32_t new_wid = way_remap[old_wid];
            if (new_wid == NO_DATA || old_wid >= full.way_parent_ids.size()) return;
            uint32_t old_parent = full.way_parent_ids[old_wid];
            if (old_parent == NO_DATA) return;
            out.way_parent_ids[new_wid] = remap_id(admin_remap, old_parent);
        });
    }

    // admin_parent_ids: parallel to full.admin_polygons, values are old admin_poly_ids
    if (!full.admin_parent_ids.empty()) {
        out.admin_parent_ids.assign(out.admin_polygons.size(), NO_DATA);
        for (uint32_t old_pid : used_admin_ids) {
            uint32_t new_pid = admin_remap[old_pid];
            if (old_pid >= full.admin_parent_ids.size()) continue;
            uint32_t old_parent = full.admin_parent_ids[old_pid];
            if (old_parent == NO_DATA) continue;
            out.admin_parent_ids[new_pid] = remap_id(admin_remap, old_parent);
        }
    }

    // Foreign ids copied from the planet records still point into the
    // planet's id space. Project them into this continent's; a target
    // the continent split filtered out becomes NO_DATA (the server
    // treats a missing parent as unknown).
    auto project = [&](uint32_t& id, const std::vector<uint32_t>& remap) { id = remap_id(remap, id); };
    parallel_each(out.place_nodes, [&](PlaceNode& pn) { project(pn.parent_poly_id, admin_remap); });
    parallel_each(out.poi_records, [&](PoiRecord& pr) { project(pr.parent_poly_id, admin_remap); });
    // Street-won housenumber refinement matches addr points to the
    // winning way by this id.
    parallel_each(out.addr_points, [&](AddrPoint& ap) { project(ap.parent_way_id, way_remap); });

    // --- Project postcode-id parallel arrays ---
    // Values are string offsets in full.string_pool — remapped during string
    // compaction below.
    if (!full.way_postcode_ids.empty()) {
        out.way_postcode_ids.assign(out.ways.size(), NO_DATA);
        parallel_each(used_way_ids, [&](uint32_t old_wid) {
            uint32_t new_wid = way_remap[old_wid];
            if (new_wid == NO_DATA || old_wid >= full.way_postcode_ids.size()) return;
            out.way_postcode_ids[new_wid] = full.way_postcode_ids[old_wid];
        });
    }
    if (!full.interp_postcode_ids.empty()) {
        out.interp_postcode_ids.assign(out.interp_ways.size(), NO_DATA);
        bool any_zip = false;
        for (uint32_t old_iid : used_interp_ids) {
            uint32_t new_iid = interp_remap[old_iid];
            if (old_iid >= full.interp_postcode_ids.size()) continue;
            out.interp_postcode_ids[new_iid] = full.interp_postcode_ids[old_iid];
            if (out.interp_postcode_ids[new_iid] != NO_DATA) any_zip = true;
        }
        // Continents outside TIGER coverage would otherwise ship (and patch,
        // forever) a pure-NO_DATA file that can never produce a postcode.
        if (!any_zip) out.interp_postcode_ids.clear();
    }
    if (!full.addr_postcode_ids.empty()) {
        out.addr_postcode_ids.assign(out.addr_points.size(), NO_DATA);
        parallel_each(used_addr_ids, [&](uint32_t old_aid) {
            uint32_t new_aid = addr_remap[old_aid];
            if (new_aid == NO_DATA || old_aid >= full.addr_postcode_ids.size()) return;
            out.addr_postcode_ids[new_aid] = full.addr_postcode_ids[old_aid];
        });
    }

    // --- Copy postcode_accum (spatially filtered) ---
    // External entries carry their country in the key, so without a spatial
    // filter every continent would ship the full global centroid set (~4.5M
    // entries). Filter by centroid; the string part of each key is an offset
    // in the FULL pool — remapped via string_remap below.
    out.postcode_accum.reserve(full.postcode_accum.size());
    for (const auto& [key, acc] : full.postcode_accum) {
        if (acc.count == 0) continue;
        double alat = acc.lat(), alng = acc.lng();
        bool inside = polygon
            ? point_in_polygon(alat, alng, *polygon)
            : (alat >= bbox.min_lat && alat <= bbox.max_lat &&
               alng >= bbox.min_lng && alng <= bbox.max_lng);
        if (!inside) continue;
        out.postcode_accum.emplace(key, acc);
    }

    log_phase(("      " + std::string(bbox.name) + " filter: parent + postcode projection").c_str(), _ft, _fc);

    // --- Remap sorted cell arrays for all 5 types (preserving INTERIOR_FLAG) ---
    auto remap_sorted_masked = [&](const std::vector<CellItemPair>& sorted,
                                    const std::vector<uint8_t>& masks,
                                    const std::vector<uint32_t>& remap,
                                    std::vector<CellItemPair>& dst) {
        if (sorted.empty()) return;
        const unsigned threads = pair_pass_threads();
        std::vector<std::vector<CellItemPair>> thread_pairs(threads);
        parallel_for_runs(sorted.size(), same_cell(sorted), [&](size_t begin, size_t end, unsigned w) {
            auto& local = thread_pairs[w];
            for (size_t i = begin; i < end; i++) {
                if (masks[i] & continent_bit) {
                    uint32_t raw   = sorted[i].item_id & ID_MASK;
                    uint32_t flags = sorted[i].item_id & INTERIOR_FLAG;
                    uint32_t id = remap_id(remap, raw);
                    if (id != NO_DATA)
                        local.push_back({sorted[i].cell_id, id | flags});
                }
            }
        }, threads);
        size_t total = 0;
        for (auto& v : thread_pairs) total += v.size();
        dst.reserve(total);
        for (auto& v : thread_pairs) dst.insert(dst.end(), v.begin(), v.end());
    };

    auto remap_cells_map = [&](const std::unordered_map<uint64_t, std::vector<uint32_t>>& src,
                               const std::vector<uint32_t>& remap,
                               std::unordered_map<uint64_t, std::vector<uint32_t>>& dst,
                               bool handle_flags = false) {
        for (const auto& [cell_id, ids] : src) {
            if (!cell_in_continent(cell_id, bbox, polygon)) continue;
            std::vector<uint32_t> new_ids;
            for (uint32_t id : ids) {
                uint32_t raw_id = handle_flags ? (id & ID_MASK) : id;
                uint32_t flags = handle_flags ? (id & INTERIOR_FLAG) : 0;
                uint32_t new_id = remap_id(remap, raw_id);
                if (new_id != NO_DATA) new_ids.push_back(new_id | flags);
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
    log_phase(("      " + std::string(bbox.name) + " filter: cell map remap (masked)").c_str(), _ft, _fc);

    // --- String pool compaction ---
    // Collect every surviving offset from ways, addrs, interps, admin polygons,
    // place nodes, POI records, way/addr postcode arrays, and postcode_accum keys,
    // as bits over the planet pool: ascending set bits are the kept strings.
    const auto& old_sp = full.string_pool.data();
    std::vector<uint64_t> used_bits(old_sp.size() / 64 + 1, 0);
    auto add_used = [&](uint32_t off) {
        if (off < old_sp.size()) used_bits[off / 64] |= uint64_t(1) << (off % 64);
    };
    for (const auto& w : out.ways) add_used(w.name_id);
    for (const auto& a : out.addr_points) { add_used(a.housenumber_id); add_used(a.street_id); }
    for (const auto& iw : out.interp_ways) add_used(iw.street_id);
    for (const auto& ap : out.admin_polygons) add_used(ap.name_id);
    for (const auto& pn : out.place_nodes) add_used(pn.name_id);
    for (const auto& pr : out.poi_records) { add_used(pr.name_id); add_used(pr.parent_street_id); add_used(pr.parent_postcode_id); }
    for (uint32_t off : out.way_postcode_ids) add_used(off);
    for (uint32_t off : out.interp_postcode_ids) add_used(off);
    for (uint32_t off : out.addr_postcode_ids) add_used(off);
    for (const auto& [key, _acc] : out.postcode_accum) add_used(postcode_key_pc(key));

    std::vector<uint32_t> kept_offsets;  // ascending planet offsets
    std::vector<uint32_t> new_offsets;   // each one's offset in the continent pool
    auto& new_sp = out.string_pool.mutable_data();
    new_sp.clear();
    for (size_t word = 0; word < used_bits.size(); word++) {
        for (uint64_t bits = used_bits[word]; bits; bits &= bits - 1) {
            uint32_t old_off = static_cast<uint32_t>(word * 64 + __builtin_ctzll(bits));
            kept_offsets.push_back(old_off);
            new_offsets.push_back(static_cast<uint32_t>(new_sp.size()));
            const char* str = old_sp.data() + old_off;
            size_t len = std::strlen(str);
            new_sp.insert(new_sp.end(), str, str + len + 1);
        }
    }
    std::vector<uint64_t>().swap(used_bits);
    auto remap_or_sentinel = [&](uint32_t off) -> uint32_t {
        auto it = std::lower_bound(kept_offsets.begin(), kept_offsets.end(), off);
        if (it == kept_offsets.end() || *it != off) return NO_DATA;
        return new_offsets[it - kept_offsets.begin()];
    };
    parallel_each(out.ways, [&](WayHeader& w) { w.name_id = remap_or_sentinel(w.name_id); });
    parallel_each(out.addr_points, [&](AddrPoint& a) {
        a.housenumber_id = remap_or_sentinel(a.housenumber_id);
        a.street_id = remap_or_sentinel(a.street_id);
    });
    parallel_each(out.interp_ways, [&](InterpWay& iw) { iw.street_id = remap_or_sentinel(iw.street_id); });
    parallel_each(out.admin_polygons, [&](AdminPolygon& ap) { ap.name_id = remap_or_sentinel(ap.name_id); });
    parallel_each(out.place_nodes, [&](PlaceNode& pn) { pn.name_id = remap_or_sentinel(pn.name_id); });
    parallel_each(out.poi_records, [&](PoiRecord& pr) {
        pr.name_id = remap_or_sentinel(pr.name_id);
        pr.parent_street_id = remap_or_sentinel(pr.parent_street_id);
        pr.parent_postcode_id = remap_or_sentinel(pr.parent_postcode_id);
    });
    auto remap_offset = [&](uint32_t& off) { off = remap_or_sentinel(off); };
    parallel_each(out.way_postcode_ids, remap_offset);
    parallel_each(out.interp_postcode_ids, remap_offset);
    parallel_each(out.addr_postcode_ids, remap_offset);

    // Rebuild postcode_accum with remapped keys (drop entries whose string didn't survive)
    {
        std::unordered_map<uint64_t, ParsedData::PostcodeAccum> remapped;
        remapped.reserve(out.postcode_accum.size());
        for (auto& [key, acc] : out.postcode_accum) {
            uint32_t new_pc_id = remap_or_sentinel(postcode_key_pc(key));
            if (new_pc_id == NO_DATA) continue;
            remapped[postcode_key(postcode_key_cc(key), new_pc_id)] = acc;
        }
        out.postcode_accum = std::move(remapped);
    }

    log_phase(("      " + std::string(bbox.name) + " filter: string pool rebuild (masked)").c_str(), _ft, _fc);

    // Partition the continent's flat pool into the same 5 tier files
    // the full planet produces, so write_index can emit them per-mode.
    // Records get remapped a second time (flat-continent offsets →
    // tiered-continent offsets) which is correct.
    partition_strings_into_tiers(out);

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
