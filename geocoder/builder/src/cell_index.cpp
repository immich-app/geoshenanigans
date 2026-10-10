#include "cell_index.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <unistd.h>

#include "geometry.h"
#include "id_allocator.h"
#include "parallel.h"
#include "postcode_validation.h"
#include "ring_contains.h"
#include "s2_helpers.h"
#include "strategy2_remap.h"
#include "vertex_pack.h"

#include <s2/s2latlng.h>
#include <limits>
#include <memory>

// Write strings_layout.json — records each tier's [start, end) in the
// global string-offset space so the server can route a global offset to
// the right tier file. Same content regardless of which subset of tier
// files a given dir contains.
void write_strings_layout(const std::string& dir, const ParsedData& data) {
    std::ofstream f(dir + "/strings_layout.json");
    f << "{\n  \"tiers\": [\n";
    for (size_t t = 0; t < STR_TIER_COUNT; t++) {
        f << "    {\"name\": \"" << STR_TIER_NAMES[t]
          << "\", \"file\": \"" << STR_TIER_FILENAMES[t]
          << "\", \"start\": " << data.strings_tier_bases[t]
          << ", \"end\": " << data.strings_tier_bases[t + 1] << "}";
        if (t + 1 < STR_TIER_COUNT) f << ",";
        f << "\n";
    }
    f << "  ]\n}\n";
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + dir + "/strings_layout.json");
}

// "<region>/<mode>" of an output dir, to tell apart the phase lines of
// regions and modes written at once.
static std::string dir_label(const std::string& dir) {
    size_t last = dir.find_last_of('/');
    if (last == std::string::npos || last == 0) return dir;
    size_t prev = dir.find_last_of('/', last - 1);
    return prev == std::string::npos ? dir : dir.substr(prev + 1);
}

std::vector<uint32_t> write_entries(
    const std::string& path,
    const std::vector<uint64_t>& sorted_cells,
    const std::unordered_map<uint64_t, std::vector<uint32_t>>& cell_map
) {
    struct CellRef { uint64_t cell_id; const std::vector<uint32_t>* ids; };
    std::vector<CellRef> sorted_refs;
    sorted_refs.reserve(cell_map.size());
    for (auto& [id, ids] : cell_map) sorted_refs.push_back({id, &ids});
    std::sort(sorted_refs.begin(), sorted_refs.end(),
        [](const CellRef& a, const CellRef& b) { return a.cell_id < b.cell_id; });

    std::vector<uint32_t> offsets(sorted_cells.size(), NO_DATA);
    size_t total_size = 0;
    for (auto& r : sorted_refs) total_size += sizeof(uint16_t) + r.ids->size() * sizeof(uint32_t);

    std::vector<char> buf;
    buf.reserve(total_size);
    uint32_t current = 0;
    size_t ri = 0;
    size_t max_count = 0;
    for (uint32_t si = 0; si < sorted_cells.size() && ri < sorted_refs.size(); si++) {
        if (sorted_cells[si] < sorted_refs[ri].cell_id) continue;
        if (sorted_cells[si] > sorted_refs[ri].cell_id) { si--; ri++; continue; }
        offsets[si] = current;
        const auto& ids = *sorted_refs[ri].ids;
        uint16_t count = checked_entry_count(ids.size(), path);
        if (ids.size() > max_count) max_count = ids.size();
        buf.insert(buf.end(), reinterpret_cast<const char*>(&count),
                   reinterpret_cast<const char*>(&count) + sizeof(count));
        buf.insert(buf.end(), reinterpret_cast<const char*>(ids.data()),
                   reinterpret_cast<const char*>(ids.data()) + ids.size() * sizeof(uint32_t));
        current += sizeof(uint16_t) + ids.size() * sizeof(uint32_t);
        ri++;
    }
    std::cerr << "  " << path << ": max entries/cell = " << max_count << std::endl;
    write_binary_file(path, buf.data(), buf.size());
    return offsets;
}

std::vector<uint32_t> write_entries_from_sorted(
    const std::string& path,
    const std::vector<uint64_t>& sorted_cells,
    const std::vector<CellItemPair>& sorted_pairs
) {
    std::vector<uint32_t> offsets(sorted_cells.size(), NO_DATA);
    if (sorted_pairs.empty()) {
        // Still create (truncate) the file, just empty.
        write_binary_file(path, nullptr, 0);
        return offsets;
    }

    // Each worker packs its cells into its own buffer with offsets local to
    // it; the offsets are then rebased and the buffers written in order.
    struct ChunkResult {
        std::vector<char> buf;
        size_t cell_start = 0, cell_end = 0;
        uint32_t local_size = 0;
        size_t max_count = 0;  // largest per-cell entry count seen (overflow checked after join)
    };
    const unsigned threads = parallel_threads();
    std::vector<ChunkResult> chunks(threads);
    parallel_for(sorted_cells.size(), [&](size_t cs, size_t ce, unsigned t) {
        auto& chunk = chunks[t];
        chunk.cell_start = cs;
        chunk.cell_end = ce;

        size_t pi = std::lower_bound(sorted_pairs.begin(), sorted_pairs.end(),
            sorted_cells[cs], [](const CellItemPair& p, uint64_t id) {
                return p.cell_id < id;
            }) - sorted_pairs.begin();

        for (size_t si = cs; si < ce && pi < sorted_pairs.size(); si++) {
            if (sorted_cells[si] < sorted_pairs[pi].cell_id) continue;
            while (pi < sorted_pairs.size() && sorted_pairs[pi].cell_id < sorted_cells[si]) pi++;
            if (pi >= sorted_pairs.size() || sorted_pairs[pi].cell_id != sorted_cells[si]) continue;

            offsets[si] = chunk.local_size;
            size_t start = pi;
            while (pi < sorted_pairs.size() && sorted_pairs[pi].cell_id == sorted_cells[si]) pi++;
            // Can't throw from a worker thread; record the max and let the
            // post-join check below fail the build on overflow.
            if (pi - start > chunk.max_count) chunk.max_count = pi - start;
            uint16_t count = static_cast<uint16_t>(pi - start);
            size_t entry_size = sizeof(uint16_t) + (pi - start) * sizeof(uint32_t);
            size_t buf_pos = chunk.buf.size();
            chunk.buf.resize(buf_pos + entry_size);
            memcpy(chunk.buf.data() + buf_pos, &count, sizeof(count));
            for (size_t k = start; k < pi; k++) {
                memcpy(chunk.buf.data() + buf_pos + sizeof(uint16_t) + (k - start) * sizeof(uint32_t),
                       &sorted_pairs[k].item_id, sizeof(uint32_t));
            }
            chunk.local_size += entry_size;
        }
    }, threads);

    size_t max_count = 0;
    for (auto& chunk : chunks)
        if (chunk.max_count > max_count) max_count = chunk.max_count;
    checked_entry_count(max_count, path);
    std::cerr << "  " << path << ": max entries/cell = " << max_count << std::endl;

    std::vector<uint32_t> chunk_base(threads, 0);
    for (unsigned t = 1; t < threads; t++) chunk_base[t] = chunk_base[t - 1] + chunks[t - 1].local_size;
    parallel_for(threads, [&](size_t b, size_t e, unsigned) {
        for (size_t t = b; t < e; t++) {
            if (chunk_base[t] == 0) continue;
            for (size_t si = chunks[t].cell_start; si < chunks[t].cell_end; si++)
                if (offsets[si] != NO_DATA) offsets[si] += chunk_base[t];
        }
    }, threads);

    std::ofstream f(path, std::ios::binary);
    for (auto& chunk : chunks) {
        f.write(chunk.buf.data(), static_cast<std::streamsize>(chunk.buf.size()));
        std::vector<char>().swap(chunk.buf);
    }
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + path);
    return offsets;
}

// Strategy-2 persistent dense IDs for street_ways.
//
// Loads <prev_dir>/full/street_ways.osm_ids (if it exists), allocates
// a new idx for each current way preferring the previous build's slot,
// reorders data.ways + parallel arrays, applies the resulting old→new
// remap to every reference site (cell_to_ways, sorted_way_cells,
// addr_points.parent_way_id, poi_records.parent_street_id), and stores
// the new sidecar bytes on data.way_sidecar_blob for write_index to emit.
//
// On miss (no prev sidecar / first build), each way gets a fresh
// sequential idx — equivalent to today's behavior, with a sidecar
// emitted so the NEXT build can stabilize against this one.
// Finalize an IdAllocator pass: log the slot summary and move the slot table
// into the sidecar blob. Shared by all six apply_strategy2_* passes (both the
// identity fast-path and the post-reorder finalize).
static void finalize_strategy2(gc::id_alloc::IdAllocator& alloc, uint32_t n_new, size_t n_old,
                               std::vector<gc::id_alloc::SidecarSlot>& sidecar_blob, const char* label,
                               bool no_shifts) {
    (void)n_old;
    alloc.finalize();
    std::cerr << "  strategy2 " << label << ": " << alloc.live_count() << " live, "
              << alloc.tombstone_count() << " tombstones, "
              << n_new << " total slots ("
              << (no_shifts ? "no shifts" : "remap applied")
              << ")" << std::endl;
    sidecar_blob = alloc.take_slots();
}

// A record identity packed as (ObjectType << 56 | 56-bit id).
static gc::id_alloc::SlotIdentity unpack_identity(uint64_t packed, uint8_t tier = 0) {
    return {static_cast<gc::id_alloc::ObjectType>(packed >> 56), packed & 0x00FFFFFFFFFFFFFFull, tier};
}

// admin_osm_ids are packed like the others: OSM_RELATION for relation
// rings, OSM_WAY for closed-way polygons (stable id = way id). A bare 0 is
// the legacy "no stable id" sentinel — SYNTHETIC, so it never reuses a slot.
static gc::id_alloc::SlotIdentity admin_identity(uint64_t packed) {
    if (packed == 0) return {gc::id_alloc::ObjectType::SYNTHETIC, 0};

    return unpack_identity(packed);
}

// Whether every record keeps its index.
static bool is_identity(const std::vector<uint32_t>& remap) {
    for (size_t i = 0; i < remap.size(); i++)
        if (remap[i] != static_cast<uint32_t>(i)) return false;
    return true;
}

static void apply_strategy2_streets(ParsedData& data, const std::string& prev_dir) {
    using namespace gc::id_alloc;
    if (data.ways.empty()) return;
    // Belt-and-braces: continent_filter.cpp does preserve way_osm_ids
    // for subsets these days, but if any path ever produces a desynced
    // sidecar, skip strategy-2 cleanly rather than mis-keying slots.
    if (data.way_osm_ids.size() != data.ways.size()) return;

    IdAllocator alloc;
    if (!prev_dir.empty()) {
        // Streets live in /full/ for non-admin-only builds. Try the
        // canonical path; missing-file is fine (first build).
        alloc.load_previous(prev_dir + "/full/street_ways.osm_ids");
    }

    const size_t n_old = data.ways.size();
    const std::vector<uint32_t> remap = alloc.allocate_all(n_old, [&](size_t i) {
        return SlotIdentity{ObjectType::OSM_WAY, static_cast<uint64_t>(data.way_osm_ids[i])};
    });
    const bool identity = is_identity(remap);

    const uint32_t n_new = alloc.total_slots();

    // Fast path: no actual reorder needed (first build / fresh allocation
    // / coincidentally-stable). Skip allocating shadow vectors entirely —
    // saves ~3 GiB peak RSS on planet for the way arrays alone, which
    // was running the self-hosted runner OOM during step 9.
    if (identity && n_new == n_old) {
        finalize_strategy2(alloc, n_new, n_old, data.way_sidecar_blob, "streets", /*no_shifts=*/true);
        return;
    }

    // Reorder data.ways into a tombstoned dense layout indexed by remap[i].
    WayHeader tomb_way{};
    tomb_way.node_offset = 0;
    tomb_way.node_count  = 0;
    tomb_way.name_id     = NO_DATA;
    reorder_by_remap(data.ways, remap, n_new, tomb_way);
    reorder_by_remap(data.way_osm_ids, remap, n_new, int64_t(0));
    // Build-time only, and already dropped by the string-tier pass.
    if (!data.way_orig_name_ids.empty()) reorder_by_remap(data.way_orig_name_ids, remap, n_new, NO_DATA);
    if (!data.way_parent_ids.empty())    reorder_by_remap(data.way_parent_ids, remap, n_new, NO_DATA);
    if (!data.way_postcode_ids.empty())  reorder_by_remap(data.way_postcode_ids, remap, n_new, NO_DATA);

    // Rebuild street_nodes in way order with sequential offsets. Before
    // this pass, each new_ways[k].node_offset still points at the OLD
    // parse-order position in data.street_nodes (we only reshuffled
    // the WayHeader array, not the node array). The patch tool replays
    // node merge ops sequentially — it cannot follow out-of-order
    // node_offsets — so without this rebuild verify reads from the
    // wrong byte positions and the patched street_nodes.bin diverges
    // from the build's at byte 0 (cf. africa/full first_diff=1).
    repack_nodes(data.ways, data.street_nodes);

    // Apply remap to every reference site that points into ways[].
    remap_cell_map(data.cell_to_ways, [&](uint32_t& v) { remap_index(v, remap); });
    remap_cell_pairs(data.sorted_way_cells, [&](uint32_t& v) { remap_index(v, remap); });
    parallel_each(data.addr_points, [&](AddrPoint& ap) { remap_index(ap.parent_way_id, remap); });
    // NOTE: PoiRecord::parent_street_id is NOT a way index — it holds the
    // string offset of the nearest street's name (w.name_id, set at
    // build_index.cpp's POI parent-street linking). String offsets are
    // assigned by the deterministic-ordering/string-partition pass that
    // runs BEFORE strategy-2, and are not perturbed by the way reorder
    // here. Remapping it through the way-index `remap[]` (as this code
    // previously did) corrupted it whenever name_id happened to be < n_ways.
    // The day-over-day string-offset shift is handled by str_remap in the
    // diff/patch tools, exactly like PoiRecord::name_id.

    finalize_strategy2(alloc, n_new, n_old, data.way_sidecar_blob, "streets", /*no_shifts=*/false);
}

// Strategy-2 stable IDs for admin_polygons.
// Stable identity: (relation_id<<16 | ring_index) packed into uint64_t.
// Closed-way admin polygons (non-relation-sourced) carry stable_id=0
// and get fresh IDs each build (acceptable: <1% of admin polygons).
//
// Reference sites updated:
//   - data.cell_to_admin (map values)
//   - data.admin_parent_ids (parallel array — both reorder AND value remap)
//   - data.way_parent_ids (values point to admin polys — value remap)
//   - data.poi_records[*].parent_poly_id
//   - data.place_nodes[*].parent_poly_id
//
// admin_polygons are written to /full/admin_polygons.bin in the admin
// variant and to /quality/q*/ in quality variants; sidecar is canonical
// at /admin/admin_polygons.osm_ids (admin variant always has it).
static void apply_strategy2_admins(ParsedData& data, const std::string& prev_dir) {
    using namespace gc::id_alloc;
    if (data.admin_polygons.empty()) return;
    if (data.admin_osm_ids.size() != data.admin_polygons.size()) return;

    IdAllocator alloc;
    if (!prev_dir.empty()) {
        // admin_polygons.osm_ids canonical paths, in priority order:
        // quality/uncapped (admin_polygons.bin written for every quality
        // tier including uncapped, sidecars are identical), then quality/q2.5.
        // Try each — only one needs to load successfully. (Not /admin/:
        // admin_polygons.bin is not written there. Not admin-minimal/: its
        // sidecar numbers only the kept polygons, in its own slots.)
        if (!alloc.load_previous(prev_dir + "/quality/uncapped/admin_polygons.osm_ids"))
        if (!alloc.load_previous(prev_dir + "/quality/q2.5/admin_polygons.osm_ids"))
            { /* fall through: no prev — fresh allocation */ }
    }

    const size_t n_old = data.admin_polygons.size();
    const std::vector<uint32_t> remap = alloc.allocate_all(n_old, [&](size_t i) {
        return admin_identity(data.admin_osm_ids[i]);
    });
    const bool identity = is_identity(remap);

    const uint32_t n_new = alloc.total_slots();

    if (identity && n_new == n_old) {
        finalize_strategy2(alloc, n_new, n_old, data.admin_sidecar_blob, "admins", /*no_shifts=*/true);
        return;
    }

    reorder_by_remap(data.admin_polygons, remap, n_new, admin_polygon_tombstone());
    reorder_by_remap(data.admin_osm_ids, remap, n_new, uint64_t(0));
    if (!data.admin_parent_ids.empty()) reorder_by_remap(data.admin_parent_ids, remap, n_new, NO_DATA);

    // Apply remap to every reference site that points into admin_polygons[].
    // cell_to_admin entries carry the high-bit INTERIOR_FLAG (set during
    // the admin S2-covering pass for cells fully inside a polygon). A
    // flagged value is ≥ 2^31, so the plain remap_index's `v < remap.size()`
    // guard would skip it and leave a stale polygon index after reorder, so
    // use remap_index_flagged which masks the flag, remaps, then re-ORs.
    remap_cell_map(data.cell_to_admin, [&](uint32_t& v) { remap_index_flagged(v, remap); });
    // admin_parent_ids and way_parent_ids hold admin polygon IDs (parent chain).
    // admin_parent_ids was reordered above; now value-remap each entry.
    // These are plain indices (no INTERIOR_FLAG), so plain remap_index is correct.
    parallel_each(data.admin_parent_ids, [&](uint32_t& v) { remap_index(v, remap); });
    parallel_each(data.way_parent_ids, [&](uint32_t& v) { remap_index(v, remap); });
    parallel_each(data.poi_records, [&](PoiRecord& pr) { remap_index(pr.parent_poly_id, remap); });
    parallel_each(data.place_nodes, [&](PlaceNode& pn) { remap_index(pn.parent_poly_id, remap); });

    finalize_strategy2(alloc, n_new, n_old, data.admin_sidecar_blob, "admins", /*no_shifts=*/false);
}

// Strategy-2 stable IDs for addr_points.
// Stable identity comes pre-packed in data.addr_osm_ids: top 8 bits
// = ObjectType (NODE / WAY / SYNTHETIC), bottom 56 bits = the id.
// Reference sites updated:
//   - data.cell_to_addrs (map values)
//   - data.sorted_addr_cells (item_id)
//   - data.addr_postcode_ids (parallel array — reorder)
// addr_vertices.bin is keyed by byte offset, not idx, so no remap.
static void apply_strategy2_addrs(ParsedData& data, const std::string& prev_dir) {
    using namespace gc::id_alloc;
    if (data.addr_points.empty()) return;
    if (data.addr_osm_ids.size() != data.addr_points.size()) return;

    IdAllocator alloc;
    if (!prev_dir.empty()) {
        alloc.load_previous(prev_dir + "/full/addr_points.osm_ids");
    }

    const size_t n_old = data.addr_points.size();
    const std::vector<uint32_t> remap = alloc.allocate_all(n_old, [&](size_t i) {
        return unpack_identity(data.addr_osm_ids[i]);
    });
    const bool identity = is_identity(remap);
    const uint32_t n_new = alloc.total_slots();

    if (identity && n_new == n_old) {
        finalize_strategy2(alloc, n_new, n_old, data.addr_sidecar_blob, "addrs", /*no_shifts=*/true);
        return;
    }

    AddrPoint tomb_addr{};
    std::memset(&tomb_addr, 0, sizeof(tomb_addr));
    tomb_addr.parent_way_id = NO_DATA;
    tomb_addr.vertex_offset = NO_DATA;
    reorder_by_remap(data.addr_points, remap, n_new, tomb_addr);
    reorder_by_remap(data.addr_osm_ids, remap, n_new, uint64_t(0));
    if (!data.addr_postcode_ids.empty()) reorder_by_remap(data.addr_postcode_ids, remap, n_new, NO_DATA);

    remap_cell_map(data.cell_to_addrs, [&](uint32_t& v) { remap_index(v, remap); });
    remap_cell_pairs(data.sorted_addr_cells, [&](uint32_t& v) { remap_index(v, remap); });

    finalize_strategy2(alloc, n_new, n_old, data.addr_sidecar_blob, "addrs", /*no_shifts=*/false);
}

// Strategy-2 stable IDs for place_nodes.
// place_nodes are referenced from cell_to_place / sorted_place_cells.
// They aren't sorted by content for binary search (verified — server
// uses cell-driven lookup); reordering is safe.
static void apply_strategy2_places(ParsedData& data, const std::string& prev_dir) {
    using namespace gc::id_alloc;
    if (data.place_nodes.empty()) return;
    if (data.place_osm_ids.size() != data.place_nodes.size()) return;

    IdAllocator alloc;
    if (!prev_dir.empty()) {
        // place_nodes live in /admin/ (and /full/, /no-addresses/).
        // Use /admin/ as canonical since it's always present when
        // place_nodes exist.
        alloc.load_previous(prev_dir + "/admin/place_nodes.osm_ids");
    }

    const size_t n_old = data.place_nodes.size();
    const std::vector<uint32_t> remap = alloc.allocate_all(n_old, [&](size_t i) {
        return unpack_identity(data.place_osm_ids[i]);
    });
    const bool identity = is_identity(remap);
    const uint32_t n_new = alloc.total_slots();

    if (identity && n_new == n_old) {
        finalize_strategy2(alloc, n_new, n_old, data.place_sidecar_blob, "places", /*no_shifts=*/true);
        return;
    }

    PlaceNode tomb_pn{};
    std::memset(&tomb_pn, 0, sizeof(tomb_pn));
    tomb_pn.name_id = NO_DATA;
    reorder_by_remap(data.place_nodes, remap, n_new, tomb_pn);
    reorder_by_remap(data.place_osm_ids, remap, n_new, uint64_t(0));

    remap_cell_pairs(data.sorted_place_cells, [&](uint32_t& v) { remap_index(v, remap); });

    finalize_strategy2(alloc, n_new, n_old, data.place_sidecar_blob, "places", /*no_shifts=*/false);
}

// Strategy-2 stable IDs for poi_records.
// Reference sites updated:
//   - data.cell_to_pois (map values)
//   - data.sorted_poi_cells (item_id)
// poi_vertices.bin is byte-offset keyed (no remap needed).
static void apply_strategy2_pois(ParsedData& data, const std::string& prev_dir) {
    using namespace gc::id_alloc;
    if (data.poi_records.empty()) return;
    if (data.poi_osm_ids.size() != data.poi_records.size()) return;

    IdAllocator alloc;
    if (!prev_dir.empty()) {
        // POIs are in /poi/all/ canonically (always has the full set).
        alloc.load_previous(prev_dir + "/poi/all/poi_records.osm_ids");
    }

    const size_t n_old = data.poi_records.size();
    const std::vector<uint32_t> remap = alloc.allocate_all(n_old, [&](size_t i) {
        return unpack_identity(data.poi_osm_ids[i], data.poi_records[i].tier);
    });
    const bool identity = is_identity(remap);
    const uint32_t n_new = alloc.total_slots();

    if (identity && n_new == n_old) {
        finalize_strategy2(alloc, n_new, n_old, data.poi_sidecar_blob, "pois", /*no_shifts=*/true);
        return;
    }

    PoiRecord tomb_pr{};
    std::memset(&tomb_pr, 0, sizeof(tomb_pr));
    tomb_pr.name_id = NO_DATA;
    tomb_pr.vertex_offset = NO_DATA;
    tomb_pr.parent_street_id = NO_DATA;
    tomb_pr.parent_postcode_id = NO_DATA;
    tomb_pr.parent_poly_id = NO_DATA;
    reorder_by_remap(data.poi_records, remap, n_new, tomb_pr);
    reorder_by_remap(data.poi_osm_ids, remap, n_new, uint64_t(0));

    // cell_to_pois and sorted_poi_cells item_ids carry the high-bit
    // INTERIOR_FLAG (set during POI cell covering at build_index.cpp). A
    // flagged value is ≥ 2^31, so a plain `v < remap.size()` guard would
    // skip it and leave a stale POI index after reorder. remap_index_flagged
    // masks, remaps, re-ORs — mirroring the deterministic-sort pass.
    remap_cell_map(data.cell_to_pois, [&](uint32_t& v) { remap_index_flagged(v, remap); });
    remap_cell_pairs(data.sorted_poi_cells, [&](uint32_t& v) { remap_index_flagged(v, remap); });

    finalize_strategy2(alloc, n_new, n_old, data.poi_sidecar_blob, "pois", /*no_shifts=*/false);
}

// Strategy-2 stable IDs for interp_ways.
// References: data.cell_to_interps + data.sorted_interp_cells.
static void apply_strategy2_interps(ParsedData& data, const std::string& prev_dir) {
    using namespace gc::id_alloc;
    if (data.interp_ways.empty()) return;
    if (data.interp_osm_ids.size() != data.interp_ways.size()) return;

    IdAllocator alloc;
    if (!prev_dir.empty()) {
        alloc.load_previous(prev_dir + "/full/interp_ways.osm_ids");
    }

    const size_t n_old = data.interp_ways.size();
    const std::vector<uint32_t> remap = alloc.allocate_all(n_old, [&](size_t i) {
        return unpack_identity(data.interp_osm_ids[i]);
    });
    const bool identity = is_identity(remap);
    const uint32_t n_new = alloc.total_slots();

    if (identity && n_new == n_old) {
        finalize_strategy2(alloc, n_new, n_old, data.interp_sidecar_blob, "interps", /*no_shifts=*/true);
        return;
    }

    InterpWay tomb_iw{};
    std::memset(&tomb_iw, 0, sizeof(tomb_iw));
    tomb_iw.street_id = NO_DATA;
    // Strict parallel-array gate, matching reorder_deterministically: a
    // desynced sidecar is skipped whole rather than partially copied.
    const bool have_pc = (data.interp_postcode_ids.size() == n_old);
    reorder_by_remap(data.interp_ways, remap, n_new, tomb_iw);
    reorder_by_remap(data.interp_osm_ids, remap, n_new, uint64_t(0));
    if (have_pc) reorder_by_remap(data.interp_postcode_ids, remap, n_new, NO_DATA);

    // Rebuild interp_nodes in interp_way order with sequential offsets —
    // same reason as the street_nodes rebuild above. The patch tool
    // replays child node merges sequentially; if node_offsets are
    // shuffled (which is what we get after reordering interp_ways
    // without touching interp_nodes), the patch reads from the wrong
    // bytes and verify diverges from the build's interp_nodes.bin at
    // byte 0.
    repack_nodes(data.interp_ways, data.interp_nodes);

    remap_cell_map(data.cell_to_interps, [&](uint32_t& v) { remap_index(v, remap); });
    remap_cell_pairs(data.sorted_interp_cells, [&](uint32_t& v) { remap_index(v, remap); });

    finalize_strategy2(alloc, n_new, n_old, data.interp_sidecar_blob, "interps", /*no_shifts=*/false);
}

// Helper: emit a strategy-2 sidecar from a sidecar_blob (post-remap)
// or from a parallel osm_ids vector (fallback when strategy-2 wasn't
// applied). Wraps the count + magic header. No-op if both are empty.
void emit_strategy2_sidecar(const std::string& path,
                            const std::vector<gc::id_alloc::SidecarSlot>& blob,
                            const std::vector<uint64_t>& osm_ids_fallback) {
    using namespace gc::id_alloc;
    if (!blob.empty()) {
        IdAllocator::write_sidecar(path, blob);
        return;
    }
    if (osm_ids_fallback.empty()) return;
    std::vector<SidecarSlot> slots(osm_ids_fallback.size());
    for (size_t i = 0; i < osm_ids_fallback.size(); i++) {
        uint64_t packed = osm_ids_fallback[i];
        slots[i].object_type = static_cast<uint8_t>(packed >> 56);
        slots[i].flags = 0;
        slots[i].reserved = 0;
        slots[i].stable_id = packed & 0x00FFFFFFFFFFFFFFull;
    }
    IdAllocator::write_sidecar(path, slots);
}

void apply_strategy2_remaps(ParsedData& data, const std::string& prev_dir) {
    const std::string label = "      strategy2 " + prev_dir.substr(prev_dir.find_last_of('/') + 1) + ": ";
    timed_phase(label + "streets", [&] { apply_strategy2_streets(data, prev_dir); });
    timed_phase(label + "admins", [&] { apply_strategy2_admins(data, prev_dir); });
    timed_phase(label + "addrs", [&] { apply_strategy2_addrs(data, prev_dir); });
    timed_phase(label + "places", [&] { apply_strategy2_places(data, prev_dir); });
    timed_phase(label + "pois", [&] { apply_strategy2_pois(data, prev_dir); });
    timed_phase(label + "interps", [&] { apply_strategy2_interps(data, prev_dir); });
    // postcode_centroids: their stable identity is (country_code,
    // postcode_string) and the centroid records get materialized from
    // the unordered_map<postcode_id, PostcodeAccum> at write time, AFTER
    // strategy-2 runs. Stability there is handled by deterministic
    // sorting at write time + the existing postcode_id remap pipeline
    // in geocoder-diff; revisit once the other types are validated.
}

// Find the level-2 (country) admin polygon covering (lat, lng) and return
// its country_code, or 0 if none is found. Resolves the point to its
// kAdminCellLevel S2 cell and walks that cell's admin entries. Single-
// country cells return immediately; border cells (multiple countries in
// the entry list) resolve by point-in-polygon, with the ring test supplied
// as contains(polygon id, vertices, count, plat, plng).
template <class Contains>
static uint16_t country_at(const ParsedData& data, double lat, double lng, Contains contains) {
    S2CellId cell = S2CellId(S2LatLng::FromDegrees(lat, lng)).parent(kAdminCellLevel);
    auto it = data.cell_to_admin.find(cell.id());
    if (it == data.cell_to_admin.end()) return 0;

    // Selection must be independent of the entry-vector order: the on-disk
    // cell index is sorted at write time, but the IN-MEMORY cell_to_admin
    // vectors reflect covering completion order, which is not guaranteed
    // across runs. "First candidate wins" here made a few hundred
    // borderline/offshore postcode centroids flip country between
    // otherwise-identical builds (same-PBF chain patch blew up from 251 B
    // to 6 MB via strategy-2 identity churn).
    //
    // Rules, all order-independent:
    //  - exactly one distinct candidate country: return it (fast path)
    //  - several: point-in-polygon; among CONTAINING candidates pick the
    //    smallest area (most specific claim), ties by lowest country code
    //  - none containing (offshore aggregates): lowest country code among
    //    the candidates — arbitrary but deterministic; these are garbage
    //    multi-country string aggregates that pattern validation mostly
    //    rejects downstream anyway.
    uint16_t single_cc = 0;
    bool multiple = false;
    for (uint32_t raw_id : it->second) {
        uint32_t pid = raw_id & ID_MASK;
        if (pid >= data.admin_polygons.size()) continue;
        const auto& poly = data.admin_polygons[pid];
        if (poly.admin_level != 2 || poly.country_code == 0) continue;
        if (single_cc == 0) {
            single_cc = poly.country_code;
        } else if (poly.country_code != single_cc) {
            multiple = true;
            break;
        }
    }
    if (single_cc == 0) return 0;
    if (!multiple) return single_cc;

    float plat = static_cast<float>(lat);
    float plng = static_cast<float>(lng);
    float best_area = std::numeric_limits<float>::max();
    uint16_t best_cc = 0;        // smallest containing polygon's country
    uint16_t fallback_cc = 0;    // lowest cc among all candidates
    for (uint32_t raw_id : it->second) {
        uint32_t pid = raw_id & ID_MASK;
        if (pid >= data.admin_polygons.size()) continue;
        const auto& poly = data.admin_polygons[pid];
        if (poly.admin_level != 2 || poly.country_code == 0) continue;
        if (fallback_cc == 0 || poly.country_code < fallback_cc)
            fallback_cc = poly.country_code;
        uint32_t off = poly.vertex_offset;
        uint32_t cnt = poly.vertex_count;
        if (off + cnt > data.admin_vertices.size() || cnt < 3) continue;
        if (contains(pid, &data.admin_vertices[off], cnt, plat, plng)) {
            if (poly.area < best_area ||
                (poly.area == best_area && poly.country_code < best_cc)) {
                best_area = poly.area;
                best_cc = poly.country_code;
            }
        }
    }
    return best_cc != 0 ? best_cc : fallback_cc;
}

uint16_t country_code_at_point(const ParsedData& data, double lat, double lng) {
    return country_at(data, lat, lng, [](uint32_t, const NodeCoord* verts, uint32_t cnt, float plat, float plng) {
        return ring_contains(verts, cnt, plat, plng);
    });
}

// Edge indexes of the country rings that border cells (several countries
// in the entry list) test, by polygon id; null for the rest. Planet country
// rings run to millions of vertices.
static std::vector<std::unique_ptr<RingEdgeIndex>> index_border_rings(const ParsedData& data) {
    auto country_ring = [&](uint32_t raw_id) {
        uint32_t pid = raw_id & ID_MASK;
        return pid < data.admin_polygons.size() && data.admin_polygons[pid].admin_level == 2
            && data.admin_polygons[pid].country_code != 0;
    };
    std::vector<char> tested(data.admin_polygons.size(), 0);
    for (const auto& [cell_id, ids] : data.cell_to_admin) {
        uint16_t first_cc = 0;
        bool border = false;
        for (uint32_t raw_id : ids) {
            if (!country_ring(raw_id)) continue;
            uint16_t cc = data.admin_polygons[raw_id & ID_MASK].country_code;
            if (first_cc == 0) first_cc = cc;
            else if (cc != first_cc) border = true;
        }
        if (!border) continue;
        for (uint32_t raw_id : ids)
            if (country_ring(raw_id)) tested[raw_id & ID_MASK] = 1;
    }
    std::vector<uint32_t> pids;
    for (uint32_t pid = 0; pid < tested.size(); pid++) {
        const auto& poly = data.admin_polygons[pid];
        if (tested[pid] && poly.vertex_offset + poly.vertex_count <= data.admin_vertices.size()
            && poly.vertex_count >= 3)
            pids.push_back(pid);
    }
    std::vector<std::unique_ptr<RingEdgeIndex>> rings(data.admin_polygons.size());
    parallel_for_dynamic(pids.size(), 1, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++) {
            const auto& poly = data.admin_polygons[pids[k]];
            rings[pids[k]] = std::make_unique<RingEdgeIndex>(&data.admin_vertices[poly.vertex_offset],
                                                             poly.vertex_count);
        }
    });
    return rings;
}

void collect_postcode_centroids(ParsedData& data) {
    using Accums = std::unordered_map<uint64_t, ParsedData::PostcodeAccum>;
    size_t n = std::min(data.addr_points.size(), data.addr_postcode_ids.size());
    const auto rings = index_border_rings(data);
    auto contains = [&](uint32_t pid, const NodeCoord* verts, uint32_t cnt, float plat, float plng) {
        return rings[pid] ? rings[pid]->contains(plat, plng) : ring_contains(verts, cnt, plat, plng);
    };
    // Border cells cost a point-in-polygon each, so workers take small
    // ranges as they free up. The sums are integers: merging the workers'
    // maps in any order gives the same totals.
    std::vector<Accums> parts(parallel_threads());
    parallel_for_dynamic(n, 1 << 14, [&](size_t begin, size_t end, unsigned w) {
        for (size_t i = begin; i < end; i++) {
            uint32_t pc_id = data.addr_postcode_ids[i];
            if (pc_id == NO_DATA) continue;
            const auto& ap = data.addr_points[i];
            uint16_t cc = country_at(data, ap.lat, ap.lng, contains);
            if (cc == 0) continue;
            parts[w][postcode_key(cc, pc_id)].add(ap.lat, ap.lng);
        }
    });
    Accums osm;
    for (auto& part : parts) {
        for (const auto& [key, acc] : part) {
            auto& sum = osm[key];
            sum.sum_lat_e7 += acc.sum_lat_e7;
            sum.sum_lng_e7 += acc.sum_lng_e7;
            sum.count += acc.count;
        }
        Accums().swap(part);
    }
    // External centroids (TIGER, GeoNames) fill only the (country,
    // postcode) pairs OSM has none for, as Nominatim's
    // _update_from_external does.
    size_t external = 0;
    for (const auto& [key, acc] : data.postcode_accum) {
        if (osm.emplace(key, acc).second) external++;
    }
    std::cerr << "Postcode centroids: " << osm.size() - external << " from OSM addresses, "
              << external << " external" << std::endl;
    data.postcode_accum = std::move(osm);
}

void write_index(const ParsedData& data, const std::string& output_dir, IndexMode mode) {
    ensure_dir(output_dir);
    const std::string label = "      " + dir_label(output_dir) + ": ";
    auto _wt = std::chrono::steady_clock::now();
    auto _wc = CpuTicks::now();
    auto _total_t = _wt;
    auto _total_c = _wc;

    bool write_streets = (mode != IndexMode::AdminOnly);
    bool write_addresses = (mode == IndexMode::Full);

    if (write_streets) {
        std::vector<uint64_t> sorted_geo_cells;
        {
            auto extract_unique_cells = [](const std::vector<CellItemPair>& pairs) {
                std::vector<uint64_t> cells;
                cells.reserve(pairs.size() / 2);
                for (size_t i = 0; i < pairs.size(); ) {
                    cells.push_back(pairs[i].cell_id);
                    uint64_t cur = pairs[i].cell_id;
                    while (i < pairs.size() && pairs[i].cell_id == cur) i++;
                }
                return cells;
            };
            auto extract_from_map = [](const std::unordered_map<uint64_t, std::vector<uint32_t>>& m) {
                std::vector<uint64_t> cells;
                cells.reserve(m.size());
                for (auto& [id, _] : m) cells.push_back(id);
                std::sort(cells.begin(), cells.end());
                return cells;
            };

            std::vector<uint64_t> way_cells, addr_cells, interp_cells;
            {
                auto f1 = std::async(std::launch::async, [&] {
                    return !data.sorted_way_cells.empty()
                        ? extract_unique_cells(data.sorted_way_cells)
                        : extract_from_map(data.cell_to_ways);
                });
                if (write_addresses) {
                    auto f2 = std::async(std::launch::async, [&] {
                        return !data.sorted_addr_cells.empty()
                            ? extract_unique_cells(data.sorted_addr_cells)
                            : extract_from_map(data.cell_to_addrs);
                    });
                    auto f3 = std::async(std::launch::async, [&] {
                        return !data.sorted_interp_cells.empty()
                            ? extract_unique_cells(data.sorted_interp_cells)
                            : extract_from_map(data.cell_to_interps);
                    });
                    addr_cells = f2.get();
                    interp_cells = f3.get();
                }
                way_cells = f1.get();
            }

            // Interp cells go in by an in-place merge: its buffer is the
            // short interp side, not another copy of the whole list.
            const size_t way_addr = way_cells.size() + addr_cells.size();
            sorted_geo_cells.resize(way_addr + interp_cells.size());
            std::merge(way_cells.begin(), way_cells.end(), addr_cells.begin(), addr_cells.end(),
                       sorted_geo_cells.begin());
            std::vector<uint64_t>().swap(way_cells);
            std::vector<uint64_t>().swap(addr_cells);
            std::copy(interp_cells.begin(), interp_cells.end(), sorted_geo_cells.begin() + way_addr);
            std::vector<uint64_t>().swap(interp_cells);
            std::inplace_merge(sorted_geo_cells.begin(), sorted_geo_cells.begin() + way_addr,
                               sorted_geo_cells.end());
            sorted_geo_cells.erase(std::unique(sorted_geo_cells.begin(), sorted_geo_cells.end()),
                                    sorted_geo_cells.end());
        }

        std::vector<uint32_t> street_offsets, addr_offsets, interp_offsets;
        {
            auto f1 = std::async(std::launch::async, [&]() {
                if (!data.sorted_way_cells.empty())
                    return write_entries_from_sorted(output_dir + "/street_entries.bin", sorted_geo_cells, data.sorted_way_cells);
                return write_entries(output_dir + "/street_entries.bin", sorted_geo_cells, data.cell_to_ways);
            });
            if (write_addresses) {
                auto f2 = std::async(std::launch::async, [&]() {
                    if (!data.sorted_addr_cells.empty())
                        return write_entries_from_sorted(output_dir + "/addr_entries.bin", sorted_geo_cells, data.sorted_addr_cells);
                    return write_entries(output_dir + "/addr_entries.bin", sorted_geo_cells, data.cell_to_addrs);
                });
                auto f3 = std::async(std::launch::async, [&]() {
                    if (!data.sorted_interp_cells.empty())
                        return write_entries_from_sorted(output_dir + "/interp_entries.bin", sorted_geo_cells, data.sorted_interp_cells);
                    return write_entries(output_dir + "/interp_entries.bin", sorted_geo_cells, data.cell_to_interps);
                });
                addr_offsets = f2.get();
                interp_offsets = f3.get();
            }
            street_offsets = f1.get();
        }

        log_phase((label + "entry files").c_str(), _wt, _wc);
        {
            // Filled and written a piece at a time: the whole file is 20
            // bytes per cell (7 GiB on planet). No addr/interp offsets
            // (no-addresses mode) write as NO_DATA.
            constexpr size_t kRowsPerPiece = size_t(1) << 22;
            const size_t n = sorted_geo_cells.size();
            const size_t row_size = sizeof(uint64_t) + 3 * sizeof(uint32_t);
            auto offset_at = [](const std::vector<uint32_t>& offsets, size_t i) {
                return offsets.empty() ? NO_DATA : offsets[i];
            };
            std::vector<char> piece(std::min(n, kRowsPerPiece) * row_size);
            const std::string path = output_dir + "/geo_cells.bin";
            std::ofstream f(path, std::ios::binary);
            for (size_t first = 0; first < n; first += kRowsPerPiece) {
                const size_t rows = std::min(kRowsPerPiece, n - first);
                parallel_for(rows, [&](size_t begin, size_t end, unsigned) {
                    char* ptr = piece.data() + begin * row_size;
                    for (size_t i = first + begin; i < first + end; i++) {
                        const uint32_t offsets[3] = {street_offsets[i], offset_at(addr_offsets, i),
                                                     offset_at(interp_offsets, i)};
                        memcpy(ptr, &sorted_geo_cells[i], sizeof(uint64_t));
                        memcpy(ptr + sizeof(uint64_t), offsets, sizeof(offsets));
                        ptr += row_size;
                    }
                });
                f.write(piece.data(), static_cast<std::streamsize>(rows * row_size));
            }
            f.flush();
            if (!f) throw std::runtime_error("failed to write " + path);
        }

        std::cerr << "geo index: " << sorted_geo_cells.size() << " cells ("
                  << data.ways.size() << " ways, " << data.addr_points.size() << " addrs, "
                  << data.interp_ways.size() << " interps)" << std::endl;
    }

    log_phase((label + "geo_cells.bin").c_str(), _wt, _wc);
    auto admin_future = std::async(std::launch::async, [&] {
        timed_phase(label + "admin cells", [&] {
            write_cell_index(output_dir + "/admin_cells.bin", output_dir + "/admin_entries.bin", data.cell_to_admin);
        });
        std::cerr << "admin index: " << data.cell_to_admin.size() << " cells, " << data.admin_polygons.size() << " polygons" << std::endl;
    });

    std::vector<std::future<void>> write_futures;
    if (write_streets) {
        write_futures.push_back(std::async(std::launch::async, [&] {
            write_binary_file(output_dir + "/street_ways.bin",
                              reinterpret_cast<const char*>(data.ways.data()),
                              data.ways.size() * sizeof(WayHeader));
        }));
        // Strategy-2 sidecar (cached only — workflow excludes *.osm_ids
        // from user-facing upload). If apply_strategy2_remaps ran,
        // data.way_sidecar_blob holds the post-remap slot table. Else
        // fall back to deriving slots from data.way_osm_ids.
        write_futures.push_back(std::async(std::launch::async, [&] {
            // way_osm_ids is int64_t but pack-as-OSM_WAY happens implicitly
            // when blob is empty; build a uint64_t fallback view.
            std::vector<uint64_t> packed_fallback;
            if (data.way_sidecar_blob.empty() && !data.way_osm_ids.empty()) {
                packed_fallback.resize(data.way_osm_ids.size());
                for (size_t i = 0; i < data.way_osm_ids.size(); i++) {
                    packed_fallback[i] = (static_cast<uint64_t>(gc::id_alloc::ObjectType::OSM_WAY) << 56) |
                                         (static_cast<uint64_t>(data.way_osm_ids[i]) & 0x00FFFFFFFFFFFFFFull);
                }
            }
            emit_strategy2_sidecar(output_dir + "/street_ways.osm_ids",
                                    data.way_sidecar_blob, packed_fallback);
        }));
        write_futures.push_back(std::async(std::launch::async, [&] {
            write_binary_file(output_dir + "/street_nodes.bin",
                              reinterpret_cast<const char*>(data.street_nodes.data()),
                              data.street_nodes.size() * sizeof(NodeCoord));
        }));
        // Way parent chain (parallel array indexed by way_id)
        if (!data.way_parent_ids.empty()) {
            write_futures.push_back(std::async(std::launch::async, [&] {
                write_binary_file(output_dir + "/way_parents.bin",
                                  reinterpret_cast<const char*>(data.way_parent_ids.data()),
                                  data.way_parent_ids.size() * sizeof(uint32_t));
            }));
        }
        // Admin parent chain (parallel array indexed by polygon_id)
        if (!data.admin_parent_ids.empty()) {
            write_futures.push_back(std::async(std::launch::async, [&] {
                write_binary_file(output_dir + "/admin_parents.bin",
                                  reinterpret_cast<const char*>(data.admin_parent_ids.data()),
                                  data.admin_parent_ids.size() * sizeof(uint32_t));
            }));
        }
        // Per-way postcode (parallel array indexed by way_id)
        if (!data.way_postcode_ids.empty()) {
            write_futures.push_back(std::async(std::launch::async, [&] {
                write_binary_file(output_dir + "/way_postcodes.bin",
                                  reinterpret_cast<const char*>(data.way_postcode_ids.data()),
                                  data.way_postcode_ids.size() * sizeof(uint32_t));
            }));
        }
        if (write_addresses) {
            // Pack addr_point polygon footprints with the same encoding
            // scheme used for admin/POI vertices.  Most addr_points are
            // single-coord (vertex_count == 0) and contribute nothing
            // to the byte stream; the ~5% with building footprints get
            // a 10-byte header + delta-encoded vertices.  Cuts the
            // 5 GB planet addr_vertices.bin to ~2.5 GB.
            write_futures.push_back(std::async(std::launch::async, [&] {
                auto _at = std::chrono::steady_clock::now();
                auto _ac = CpuTicks::now();
                // Point address, polygon footprint missing, or out-of-range
                // index (defends against caller passing addr_points without
                // matching addr_vertices) all pack as points.
                auto footprint = [&](const AddrPoint& ap) -> const NodeCoord* {
                    if (ap.vertex_count == 0 || ap.vertex_offset == NO_DATA
                        || (size_t)ap.vertex_offset + ap.vertex_count > data.addr_vertices.size())
                        return nullptr;
                    return &data.addr_vertices[ap.vertex_offset];
                };
                std::vector<AddrPoint> packed_points(data.addr_points.size());
                std::vector<uint8_t> packed_bytes;
                parallel_prefix_fill(data.addr_points.size(),
                    [&](size_t i) -> size_t {
                        const AddrPoint& ap = data.addr_points[i];
                        const NodeCoord* verts = footprint(ap);
                        return verts ? plan_polygon(verts, ap.vertex_count).bytes : 0;
                    },
                    [&](size_t total) { packed_bytes.resize(total); },
                    [&](size_t i, size_t offset) -> size_t {
                        AddrPoint ap = data.addr_points[i];
                        const NodeCoord* verts = footprint(ap);
                        size_t bytes = 0;
                        if (verts) {
                            bytes = pack_polygon_at(packed_bytes.data() + offset, verts, ap.vertex_count);
                            ap.vertex_offset = static_cast<uint32_t>(offset);
                        } else {
                            ap.vertex_offset = NO_DATA;
                            ap.vertex_count = 0;
                        }
                        packed_points[i] = ap;
                        return bytes;
                    });
                write_binary_file(output_dir + "/addr_points.bin",
                                  reinterpret_cast<const char*>(packed_points.data()),
                                  packed_points.size() * sizeof(AddrPoint));
                write_binary_file(output_dir + "/addr_vertices.bin",
                                  reinterpret_cast<const char*>(packed_bytes.data()),
                                  packed_bytes.size());
                emit_strategy2_sidecar(output_dir + "/addr_points.osm_ids",
                                        data.addr_sidecar_blob, data.addr_osm_ids);
                log_phase((label + "addr points").c_str(), _at, _ac);
            }));
            // Per-addr postcode (optional separate file, parallel to addr_points)
            if (!data.addr_postcode_ids.empty()) {
                write_futures.push_back(std::async(std::launch::async, [&] {
                    write_binary_file(output_dir + "/addr_postcodes.bin",
                                      reinterpret_cast<const char*>(data.addr_postcode_ids.data()),
                                      data.addr_postcode_ids.size() * sizeof(uint32_t));
                }));
            }
            write_futures.push_back(std::async(std::launch::async, [&] {
                write_binary_file(output_dir + "/interp_ways.bin",
                                  reinterpret_cast<const char*>(data.interp_ways.data()),
                                  data.interp_ways.size() * sizeof(InterpWay));
                emit_strategy2_sidecar(output_dir + "/interp_ways.osm_ids",
                                        data.interp_sidecar_blob, data.interp_osm_ids);
            }));
            if (!data.interp_postcode_ids.empty()) {
                write_futures.push_back(std::async(std::launch::async, [&] {
                    write_binary_file(output_dir + "/interp_postcodes.bin",
                                      reinterpret_cast<const char*>(data.interp_postcode_ids.data()),
                                      data.interp_postcode_ids.size() * sizeof(uint32_t));
                }));
            }
            write_futures.push_back(std::async(std::launch::async, [&] {
                write_binary_file(output_dir + "/interp_nodes.bin",
                                  reinterpret_cast<const char*>(data.interp_nodes.data()),
                                  data.interp_nodes.size() * sizeof(NodeCoord));
            }));
        }
    }
    // Per-tier strings files. Each mode writes the tiers its records can
    // reference (see string_home_tier): admin → CORE+POSTCODE, no-addresses
    // → +STREET, full → +ADDR. The postcode tier also holds house numbers
    // and street names that double as postcodes, so it ships even in a
    // region without postcode centroids. The POI tier (4) is written with
    // the POI variants.
    auto write_tier = [&](size_t tier_idx) {
        const auto& buf = data.strings_tiers[tier_idx];
        write_futures.push_back(std::async(std::launch::async, [&, tier_idx] {
            write_binary_file(output_dir + "/" + STR_TIER_FILENAMES[tier_idx],
                              buf.data(), buf.size());
        }));
    };
    write_tier(0);  // core — always
    write_tier(3);  // postcode — always (may be empty)
    if (write_streets) write_tier(1);   // street
    if (write_addresses) write_tier(2); // addr

    // strings_layout.json records each tier's start/end offset in the
    // global string-offset space so the server can translate a record's
    // name_id into the correct tier file. Written unconditionally into
    // every directory that contains a strings_*.bin so the server can
    // load from any mode/opt-in dir without cross-dir discovery.
    write_strings_layout(output_dir, data);

    // Write postcode centroid index (optional files)
    // Validate each centroid's postcode against the country's pattern
    // (matching Nominatim's clean_postcodes sanitizer).
    // Centroids per (country, postcode) were collected once on the full
    // data (collect_postcode_centroids); continents carry the entries
    // whose centroid lies inside them.
    if (!data.postcode_accum.empty()) {
        write_futures.push_back(std::async(std::launch::async, [&] {
            auto _pt = std::chrono::steady_clock::now();
            auto _pc = CpuTicks::now();
            auto get_str = [&](uint32_t off) -> const char* {
                return data.get_string(off);
            };

            // Build centroid vector with validation
            std::vector<PostcodeCentroid> centroids;
            centroids.reserve(data.postcode_accum.size());
            uint32_t rejected = 0;
            for (const auto& [pk, acc] : data.postcode_accum) {
                if (acc.count == 0) continue;
                uint16_t cc = postcode_key_cc(pk);
                uint32_t pc_id = postcode_key_pc(pk);
                char cc_str[3] = {
                    static_cast<char>(std::tolower(cc >> 8)),
                    static_cast<char>(std::tolower(cc & 0xFF)),
                    0
                };
                const char* pc_str = get_str(pc_id);
                if (!validate_postcode_for_country(cc_str, pc_str)) {
                    rejected++;
                    continue;
                }
                PostcodeCentroid c{};
                c.lat = static_cast<float>(acc.lat());
                c.lng = static_cast<float>(acc.lng());
                c.postcode_id = pc_id;
                c.country_code = cc;
                centroids.push_back(c);
            }
            std::cerr << "Postcode centroids: " << centroids.size() << " valid, "
                      << rejected << " rejected by country pattern"
                      << " (from " << data.postcode_accum.size() << " country+postcode pairs)" << std::endl;
            // Determinism: iterate-then-sort over `postcode_accum` (an
            // unordered_map) produces a run-dependent insertion order.
            // Sorting on postcode_id alone leaves collisions (same pc_id
            // in different countries, e.g. "90012" in US and FR) in an
            // undefined relative order — their lat/lng bits end up
            // run-dependent. Tiebreak by country_code then by lat/lng
            // for a total order.
            std::sort(centroids.begin(), centroids.end(),
                [](const PostcodeCentroid& a, const PostcodeCentroid& b) {
                    if (a.postcode_id != b.postcode_id) return a.postcode_id < b.postcode_id;
                    if (a.country_code != b.country_code) return a.country_code < b.country_code;
                    uint32_t la, lb, ga, gb;
                    std::memcpy(&la, &a.lat, 4); std::memcpy(&lb, &b.lat, 4);
                    if (la != lb) return la < lb;
                    std::memcpy(&ga, &a.lng, 4); std::memcpy(&gb, &b.lng, 4);
                    return ga < gb;
                });

            // Strategy-2 stable IDs for postcode_centroids.
            //
            // Centroids aren't OSM entities — they're computed aggregates
            // per (country_code, postcode_string) bucket. Their stable
            // identity is that pair, which is exactly the ObjectType::POSTCODE
            // case the IdAllocator was designed for. Without strategy-2
            // here, the sorted-by-postcode_id ordering shifts day-over-day
            // (postcode_id is a string offset that moves with string-pool
            // reorganization, and any postcode insertion/deletion cascades
            // subsequent indices), and postcode_centroid_entries.bin
            // inflates the patch with shifted IDs.
            //
            // The cell index below is built AFTER reordering, so cell
            // entries naturally point into the post-reorder layout — no
            // separate cell remap needed.
            //
            // Identity = FNV-1a of (country_code, postcode_string). Pad
            // to 56 bits and tag with ObjectType::POSTCODE.
            {
                using namespace gc::id_alloc;
                IdAllocator alloc;
                std::string prev_path;
                // Locate previous-build sidecar by reversing the prev-vs-out
                // dir mapping. write_index doesn't get prev_dir directly —
                // strategy-2 ran in apply_strategy2_remaps with a per-region
                // dir. Use the canonical /full/ subdir of the same region's
                // previous output. output_dir ends in "/<region>/<mode>"; we
                // need "<prev>/<region>/full". Synthesize via env var the
                // workflow sets.
                if (const char* prev_root = std::getenv("GC_PREV_OUTPUT_ROOT")) {
                    // output_dir like ".../planet/full" → strip trailing
                    // mode segment to get region root.
                    std::string od = output_dir;
                    auto last = od.find_last_of('/');
                    if (last != std::string::npos) {
                        std::string region = od.substr(0, last);
                        auto reg_last = region.find_last_of('/');
                        if (reg_last != std::string::npos) {
                            std::string region_name = region.substr(reg_last + 1);
                            prev_path = std::string(prev_root) + "/" + region_name +
                                        "/full/postcode_centroids.osm_ids";
                        }
                    }
                }
                if (!prev_path.empty()) alloc.load_previous(prev_path);

                auto fnv = [](const char* s, uint16_t cc) -> uint64_t {
                    uint64_t h = FNV1A_OFFSET_BASIS;
                    h ^= static_cast<uint64_t>(cc); h *= FNV1A_PRIME;
                    if (s) for (; *s; s++) { h ^= static_cast<uint8_t>(*s); h *= FNV1A_PRIME; }
                    return h & 0x00FFFFFFFFFFFFFFull;
                };

                const size_t n_old = centroids.size();
                const std::vector<uint32_t> remap = alloc.allocate_all(n_old, [&](size_t i) {
                    return SlotIdentity{ObjectType::POSTCODE,
                                        fnv(get_str(centroids[i].postcode_id), centroids[i].country_code)};
                });
                const bool identity = is_identity(remap);
                const uint32_t n_new = alloc.total_slots();

                if (!(identity && n_new == n_old)) {
                    PostcodeCentroid tomb_pc{};
                    tomb_pc.postcode_id = NO_DATA;
                    std::vector<PostcodeCentroid> reordered(n_new, tomb_pc);
                    for (size_t i = 0; i < n_old; i++) reordered[remap[i]] = centroids[i];
                    centroids = std::move(reordered);
                }

                std::cerr << "  strategy2 postcodes: " << alloc.live_count()
                          << " live, " << alloc.tombstone_count()
                          << " tombstones, " << n_new << " total" << std::endl;

                // Emit sidecar (cached, not user-facing — same exclusion
                // patterns as the others).
                alloc.finalize();
                IdAllocator::write_sidecar(
                    output_dir + "/postcode_centroids.osm_ids",
                    alloc.slots());
            }

            // Write flat centroid array (post-strategy-2 layout — slots in
            // stable-idx order, with tombstones for indices whose previous
            // occupant was deleted this build).
            write_binary_file(output_dir + "/postcode_centroids.bin",
                              reinterpret_cast<const char*>(centroids.data()),
                              centroids.size() * sizeof(PostcodeCentroid));

            // Build S2 cell index for spatial lookup. Skip tombstones —
            // they have postcode_id = NO_DATA and bogus lat/lng (0,0)
            // which would mis-cell. Cell entries only ever reference
            // live indices, so the diff doesn't see cascade-shift on
            // postcode_centroid_entries.bin.
            std::unordered_map<uint64_t, std::vector<uint32_t>> centroid_cells;
            for (uint32_t i = 0; i < centroids.size(); i++) {
                if (centroids[i].postcode_id == NO_DATA) continue;
                S2CellId cell = S2CellId(S2LatLng::FromDegrees(
                    centroids[i].lat, centroids[i].lng)).parent(kAdminCellLevel);
                centroid_cells[cell.id()].push_back(i);
            }
            write_cell_index(output_dir + "/postcode_centroid_cells.bin",
                             output_dir + "/postcode_centroid_entries.bin",
                             centroid_cells);

            std::cerr << "postcode centroids: " << centroids.size() << " entries, "
                      << centroid_cells.size() << " cells" << std::endl;
            log_phase((label + "postcode centroids").c_str(), _pt, _pc);
        }));
    }

    admin_future.get();
    for (auto& f : write_futures) f.get();
    log_phase((label + "total").c_str(), _total_t, _total_c);
}

// Simplify one polygon's vertices at a given epsilon (or pass through if
// epsilon_scale == 0). Helper used by both quality variant + admin-minimal.
static std::vector<std::pair<double,double>>
simplify_admin_polygon(const ParsedData& data,
                       const AdminPolygon& ap,
                       double epsilon_scale) {
    std::vector<std::pair<double,double>> pts;
    pts.reserve(ap.vertex_count);
    for (uint32_t j = 0; j < ap.vertex_count; j++) {
        const auto& v = data.admin_vertices[ap.vertex_offset + j];
        pts.emplace_back(v.lat, v.lng);
    }
    if (epsilon_scale <= 0) return pts;
    double eps_m = admin_epsilon_meters(ap.admin_level) * epsilon_scale;
    double lat = pts.empty() ? 0.0 : pts[0].first;
    double eps_deg = meters_to_degrees(eps_m, lat);
    return simplify_polygon_epsilon(pts, eps_deg);
}

void write_quality_variant(const ParsedData& data, const std::string& source_dir,
                           const std::string& output_dir, double epsilon_scale) {
    ensure_dir(output_dir);
    const std::string label = "      " + dir_label(output_dir) + ": ";
    auto _qt = std::chrono::steady_clock::now();
    auto _qc = CpuTicks::now();

    // Re-simplify admin polygons at the given epsilon scale
    std::vector<AdminPolygon> new_polys;
    std::vector<NodeCoord> new_verts;
    new_polys.reserve(data.admin_polygons.size());

    // Parallel simplification
    struct SimplifiedPoly {
        std::vector<std::pair<double,double>> verts;
    };
    std::vector<SimplifiedPoly> simplified(data.admin_polygons.size());

    parallel_for_dynamic(data.admin_polygons.size(), 1, [&](size_t begin, size_t end, unsigned) {
        for (size_t i = begin; i < end; i++)
            simplified[i].verts = simplify_admin_polygon(data, data.admin_polygons[i], epsilon_scale);
    });
    log_phase((label + "simplify").c_str(), _qt, _qc);

    // Sequential: build new polygon/vertex arrays. Postal boundaries
    // (admin_level=11) are kept in the main arrays (cell index references
    // them by ID) AND also written to separate optional files.
    std::vector<AdminPolygon> postal_polys;
    std::vector<uint8_t> new_verts_bytes;       // packed: variable stride per polygon
    std::vector<uint8_t> postal_verts_bytes;
    // Don't pre-reserve verts — overestimates lead to large unused
    // allocations across multiple concurrent continent writers and can
    // OOM the GH runner.  Vector growth is amortized cheap.

    // admin_polygons.bin MUST stay index-aligned with data.admin_polygons:
    // the cell index (admin_cells/admin_entries, written by the full/admin
    // variants) references slot `i` in this space, and the server's
    // admin_polygon(idx) accessor reads admin_polygons[idx] directly with
    // no remap. Build new_polys at full size and place each simplified
    // polygon at its own index `i`. Slots that have no usable geometry —
    // strategy-2 tombstones (a polygon that existed in the previous build
    // but not this one) and any polygon that simplifies to <3 vertices —
    // are written as NO_DATA tombstone records, which the accessor skips.
    // (Previously this loop push_back-compacted; with strategy-2 tombstones
    // present that shifted every slot past the first gap and corrupted
    // admin lookups.)
    new_polys.assign(data.admin_polygons.size(), admin_polygon_tombstone());

    // Slot i stays a NO_DATA tombstone when fewer than 3 vertices survive.
    auto drawn_bytes = [&](size_t i) -> size_t {
        const auto& sv = simplified[i].verts;
        return sv.size() < 3 ? 0 : plan_polygon(sv.data(), sv.size()).bytes;
    };
    parallel_prefix_fill(data.admin_polygons.size(), drawn_bytes,
        [&](size_t total) { new_verts_bytes.resize(total); },
        [&](size_t i, size_t offset) -> size_t {
            const auto& sv = simplified[i].verts;
            if (sv.size() < 3) return 0;

            AdminPolygon np = data.admin_polygons[i];
            size_t bytes = pack_polygon_at(new_verts_bytes.data() + offset, sv.data(), sv.size());
            np.vertex_offset = static_cast<uint32_t>(offset);
            np.vertex_count = static_cast<uint32_t>(sv.size());
            np.area = polygon_area(sv);
            new_polys[i] = np;
            return bytes;
        });

    // Postal also go into separate files (for optional loading)
    std::vector<uint32_t> postal_idx;
    for (size_t i = 0; i < new_polys.size(); i++)
        if (simplified[i].verts.size() >= 3 && new_polys[i].admin_level == 11)
            postal_idx.push_back(static_cast<uint32_t>(i));
    postal_polys.resize(postal_idx.size());
    parallel_prefix_fill(postal_idx.size(), [&](size_t k) { return drawn_bytes(postal_idx[k]); },
        [&](size_t total) { postal_verts_bytes.resize(total); },
        [&](size_t k, size_t offset) -> size_t {
            const auto& sv = simplified[postal_idx[k]].verts;
            AdminPolygon pp = new_polys[postal_idx[k]];
            pp.vertex_offset = static_cast<uint32_t>(offset);
            postal_polys[k] = pp;
            return pack_polygon_at(postal_verts_bytes.data() + offset, sv.data(), sv.size());
        });

    std::cerr << "Quality " << epsilon_scale << "x: " << new_polys.size()
              << " admin polygons, " << new_verts_bytes.size() / 1024 / 1024
              << " MiB packed vertices, " << postal_polys.size() << " postal polygons" << std::endl;

    // Write admin files (excluding postal)
    write_binary_file(output_dir + "/admin_polygons.bin",
                      reinterpret_cast<const char*>(new_polys.data()),
                      new_polys.size() * sizeof(AdminPolygon));
    write_binary_file(output_dir + "/admin_vertices.bin",
                      reinterpret_cast<const char*>(new_verts_bytes.data()),
                      new_verts_bytes.size());
    // Strategy-2 sidecar for admin polygons (cached only). Same content
    // across full/no-addresses/admin since they share the same polygon
    // set. data.admin_osm_ids is already packed (ObjectType<<56 |
    // stable56) — OSM_RELATION for relation rings, OSM_WAY for closed
    // ways — so the fallback passes it straight through. A bare 0 is the
    // legacy "no stable id" sentinel → emit as a SYNTHETIC slot.
    {
        std::vector<uint64_t> packed_fallback;
        if (data.admin_sidecar_blob.empty() && !data.admin_osm_ids.empty()) {
            packed_fallback.resize(data.admin_osm_ids.size());
            for (size_t i = 0; i < data.admin_osm_ids.size(); i++) {
                packed_fallback[i] = data.admin_osm_ids[i] != 0
                    ? data.admin_osm_ids[i]
                    : (static_cast<uint64_t>(gc::id_alloc::ObjectType::SYNTHETIC) << 56);
            }
        }
        emit_strategy2_sidecar(output_dir + "/admin_polygons.osm_ids",
                                data.admin_sidecar_blob, packed_fallback);
    }

    // Write postal boundary files (optional, admin_level=11 only)
    if (!postal_polys.empty()) {
        write_binary_file(output_dir + "/postal_polygons.bin",
                          reinterpret_cast<const char*>(postal_polys.data()),
                          postal_polys.size() * sizeof(AdminPolygon));
        write_binary_file(output_dir + "/postal_vertices.bin",
                          reinterpret_cast<const char*>(postal_verts_bytes.data()),
                          postal_verts_bytes.size());
    }

    log_phase((label + "pack + write").c_str(), _qt, _qc);
    // Quality directories only contain the files that change.
    // Shared files (admin_cells.bin, admin_entries.bin, strings.bin)
    // stay in the parent directory.
}

void write_admin_minimal_polygons(const ParsedData& data,
                                  const std::string& output_dir,
                                  const std::string& prev_dir,
                                  double epsilon_scale,
                                  std::vector<uint32_t>& id_remap) {
    ensure_dir(output_dir);

    // Collect old indices we want to keep (admin_level in [2, 8]).
    id_remap.assign(data.admin_polygons.size(), NO_DATA);
    std::vector<uint32_t> kept_idx;
    kept_idx.reserve(data.admin_polygons.size() / 2);
    for (size_t i = 0; i < data.admin_polygons.size(); i++) {
        uint8_t lvl = data.admin_polygons[i].admin_level;
        if (lvl >= 2 && lvl <= 8) kept_idx.push_back(static_cast<uint32_t>(i));
    }

    // Parallel simplification of just the kept polygons.
    struct SimplifiedPoly { std::vector<std::pair<double,double>> verts; };
    std::vector<SimplifiedPoly> simplified(kept_idx.size());
    parallel_for_dynamic(kept_idx.size(), 1, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++)
            simplified[k].verts = simplify_admin_polygon(
                data, data.admin_polygons[kept_idx[k]], epsilon_scale);
    });

    // Survivors keep stable slots in admin-minimal's own numbering and
    // sidecar, like the full set's (apply_strategy2_admins). A dense
    // renumbering shifted every later id whenever one kept polygon came or
    // went, re-sending ~459K parent ids (3.5 MiB raw) every planet day.
    using namespace gc::id_alloc;
    IdAllocator alloc;
    if (!prev_dir.empty()) alloc.load_previous(prev_dir + "/admin-minimal/admin_polygons.osm_ids");
    std::vector<size_t> drawn;  // kept polygons with a shape left after simplifying
    for (size_t k = 0; k < kept_idx.size(); k++)
        if (simplified[k].verts.size() >= 3) drawn.push_back(k);
    const std::vector<uint32_t> drawn_slots = alloc.allocate_all(drawn.size(), [&](size_t j) {
        size_t old_id = kept_idx[drawn[j]];
        return admin_identity(old_id < data.admin_osm_ids.size() ? data.admin_osm_ids[old_id] : 0);
    });
    alloc.finalize();
    std::vector<size_t> kept_at_slot(alloc.total_slots(), SIZE_MAX);
    for (size_t j = 0; j < drawn.size(); j++) kept_at_slot[drawn_slots[j]] = drawn[j];

    // Vertex bytes in slot order, so offsets ascend with ids.
    std::vector<AdminPolygon> new_polys(alloc.total_slots(), admin_polygon_tombstone());
    std::vector<uint8_t> new_verts_bytes;
    const size_t kept = drawn.size();
    parallel_prefix_fill(kept_at_slot.size(),
        [&](size_t slot) -> size_t {
            size_t k = kept_at_slot[slot];
            if (k == SIZE_MAX) return 0;
            const auto& sv = simplified[k].verts;
            return plan_polygon(sv.data(), sv.size()).bytes;
        },
        [&](size_t total) { new_verts_bytes.resize(total); },
        [&](size_t slot, size_t offset) -> size_t {
            size_t k = kept_at_slot[slot];
            if (k == SIZE_MAX) return 0;
            const auto& sv = simplified[k].verts;
            AdminPolygon np = data.admin_polygons[kept_idx[k]];
            size_t bytes = pack_polygon_at(new_verts_bytes.data() + offset, sv.data(), sv.size());
            np.vertex_offset = static_cast<uint32_t>(offset);
            np.vertex_count = static_cast<uint32_t>(sv.size());
            np.area = polygon_area(sv);
            id_remap[kept_idx[k]] = static_cast<uint32_t>(slot);
            new_polys[slot] = np;
            return bytes;
        });

    write_binary_file(output_dir + "/admin_polygons.bin",
                      reinterpret_cast<const char*>(new_polys.data()),
                      new_polys.size() * sizeof(AdminPolygon));
    write_binary_file(output_dir + "/admin_vertices.bin",
                      reinterpret_cast<const char*>(new_verts_bytes.data()),
                      new_verts_bytes.size());
    IdAllocator::write_sidecar(output_dir + "/admin_polygons.osm_ids", alloc.slots());

    std::cerr << "  Admin-minimal polygons: " << kept << " kept in " << new_polys.size()
              << " slots (of " << data.admin_polygons.size() << "), "
              << new_verts_bytes.size() / 1024 / 1024 << " MiB packed vertices"
              << std::endl;
}
