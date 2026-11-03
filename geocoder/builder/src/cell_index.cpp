#include "cell_index.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <numeric>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "admin_cover.h"
#include "admin_rank_config.h"
#include "geometry.h"
#include "id_allocator.h"
#include "keyhole_pack.h"
#include "parallel.h"
#include "place_claims.h"
#include "polygon_rings.h"
#include "postcode_validation.h"
#include "ranked_bits.h"
#include "s2_helpers.h"
#include "strategy2_remap.h"
#include "string_subset.h"
#include "vertex_pack.h"
#include "write_schedule.h"

#include <s2/s2latlng.h>
#include <functional>
#include <limits>
#include <memory>

// Write strings_layout.json — records each tier's [start, end) in the
// global string-offset space so the server can route a global offset to
// the right tier file. Same content in every dir of the shared strings,
// whichever of the tier files it holds.
void write_strings_layout(const std::string& dir, const std::array<uint32_t, STR_TIER_COUNT + 1>& bases) {
    std::ofstream f(dir + "/strings_layout.json");
    f << "{\n  \"tiers\": [\n";
    for (size_t t = 0; t < STR_TIER_COUNT; t++) {
        f << "    {\"name\": \"" << STR_TIER_NAMES[t]
          << "\", \"file\": \"" << STR_TIER_FILENAMES[t]
          << "\", \"start\": " << bases[t]
          << ", \"end\": " << bases[t + 1] << "}";
        if (t + 1 < STR_TIER_COUNT) f << ",";
        f << "\n";
    }
    f << "  ]\n}\n";
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + dir + "/strings_layout.json");
}

// The layout of a dir whose strings are a core of its own: every other tier
// empty at the core's end, so no offset into another dir's strings resolves
// against it.
static std::array<uint32_t, STR_TIER_COUNT + 1> core_only_strings_layout(size_t core_size) {
    std::array<uint32_t, STR_TIER_COUNT + 1> bases;
    bases.fill(static_cast<uint32_t>(core_size));
    bases[0] = 0;
    return bases;
}

// "<region>/<mode>" of an output dir, to tell apart the phase lines of
// regions and modes written at once.
static std::string dir_label(const std::string& dir) {
    size_t last = dir.find_last_of('/');
    if (last == std::string::npos || last == 0) return dir;
    size_t prev = dir.find_last_of('/', last - 1);
    return prev == std::string::npos ? dir : dir.substr(prev + 1);
}

// Strategy-2 persistent dense IDs for street_ways.
//
// Loads <prev_dir>/full/street_ways.osm_ids (if it exists), allocates
// a new idx for each current way preferring the previous build's slot,
// reorders data.ways + parallel arrays, applies the resulting old→new
// remap to every reference site (sorted_way_cells,
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

// Returns the old -> new way id remap when the ways moved (empty when not):
// the addr points' parent ways still need it.
static std::vector<uint32_t> apply_strategy2_streets(ParsedData& data, const std::string& prev_dir) {
    using namespace gc::id_alloc;
    if (data.ways.empty()) return {};
    // Belt-and-braces: continent_filter.cpp does preserve way_osm_ids
    // for subsets these days, but if any path ever produces a desynced
    // sidecar, skip strategy-2 cleanly rather than mis-keying slots.
    if (data.way_osm_ids.size() != data.ways.size()) return {};

    IdAllocator alloc;
    if (!prev_dir.empty()) {
        // Streets live in /full/ for non-admin-only builds. Try the
        // canonical path; missing-file is fine (first build).
        alloc.load_previous(prev_dir + "/full/street_ways.osm_ids");
    }

    const size_t n_old = data.ways.size();
    std::vector<uint32_t> remap = alloc.allocate_all(n_old, [&](size_t i) {
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
        return {};
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
    remap_cell_pairs(data.sorted_way_cells, [&](uint32_t& v) { remap_index(v, remap); });
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
    return remap;
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

    auto& arc_polygons = data.admin_arcs.polygons;
    if (!arc_polygons.empty() && arc_polygons.size() != n_old)
        throw std::logic_error("admin arc polygons out of step with the polygons");
    reorder_by_remap(data.admin_polygons, remap, n_new, admin_polygon_tombstone());
    reorder_by_remap(data.admin_osm_ids, remap, n_new, uint64_t(0));
    if (!data.admin_planet_ids.empty()) reorder_by_remap(data.admin_planet_ids, remap, n_new, NO_DATA);
    // A tombstone has no rings.
    if (!arc_polygons.empty()) reorder_by_remap(arc_polygons, remap, n_new, AdminArcPolygon{});
    if (!data.admin_parent_ids.empty()) reorder_by_remap(data.admin_parent_ids, remap, n_new, NO_DATA);

    // Apply remap to every reference site that points into admin_polygons[].
    // cell_to_admin entries carry the high-bit INTERIOR_FLAG (set by
    // admin_candidate_cells for cells fully inside a polygon). A
    // flagged value is ≥ 2^31, so the plain remap_index's `v < remap.size()`
    // guard would skip it and leave a stale polygon index after reorder, so
    // use remap_index_flagged which masks the flag, remaps, then re-ORs.
    remap_cell_map(data.cell_to_admin, [&](uint32_t& v) { remap_index_flagged(v, remap); });
    remap_cell_pairs(data.admin_index_cells, [&](uint32_t& v) { remap_index_flagged(v, remap); });
    remap_cell_pairs(data.admin_minimal_index_cells, [&](uint32_t& v) { remap_index_flagged(v, remap); });
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
    tomb_pn.parent_poly_id = NO_DATA;
    reorder_by_remap(data.place_nodes, remap, n_new, tomb_pn);
    reorder_by_remap(data.place_osm_ids, remap, n_new, uint64_t(0));

    remap_cell_pairs(data.sorted_place_cells, [&](uint32_t& v) { remap_index(v, remap); });

    finalize_strategy2(alloc, n_new, n_old, data.place_sidecar_blob, "places", /*no_shifts=*/false);
}

// Strategy-2 stable IDs for poi_records.
// Reference sites updated:
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

    // sorted_poi_cells item_ids carry the high-bit
    // INTERIOR_FLAG (set during POI cell covering at build_index.cpp). A
    // flagged value is ≥ 2^31, so a plain `v < remap.size()` guard would
    // skip it and leave a stale POI index after reorder. remap_index_flagged
    // masks, remaps, re-ORs — mirroring the deterministic-sort pass.
    remap_cell_pairs(data.sorted_poi_cells, [&](uint32_t& v) { remap_index_flagged(v, remap); });

    finalize_strategy2(alloc, n_new, n_old, data.poi_sidecar_blob, "pois", /*no_shifts=*/false);
}

// Strategy-2 stable IDs for interp_ways.
// References: data.sorted_interp_cells.
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

void apply_strategy2_remaps(ParsedData& data, const std::string& prev_dir, RunOrder order, MemoryBudget* budget) {
    const std::string label = "      strategy2 " + prev_dir.substr(prev_dir.find_last_of('/') + 1) + ": ";
    // Admins first: their remap reaches into the way, POI and place arrays
    // the other passes reorder. The other passes touch
    // disjoint arrays and run at once; the addr points' parent ways follow
    // the streets afterwards, which commutes with the addrs' reorder (an
    // addr tombstone's parent way is NO_DATA).
    timed_phase(label + "admins", [&] { apply_strategy2_admins(data, prev_dir); });
    const KindValues costs = strategy2_pass_costs(data);
    std::vector<uint32_t> way_remap;
    std::vector<std::future<void>> passes;
    auto start = [&](RecordKind kind, const char* name, std::function<void()> pass) {
        auto run = [&, kind, name, pass] {
            std::optional<BudgetLease> lease;
            if (budget) lease.emplace(*budget, at(costs, kind));
            timed_phase(label + name, pass);
        };
        if (order == RunOrder::Serial) return run();
        passes.push_back(std::async(std::launch::async, run));
    };
    start(RecordKind::Ways, "streets", [&] { way_remap = apply_strategy2_streets(data, prev_dir); });
    start(RecordKind::Addrs, "addrs", [&] { apply_strategy2_addrs(data, prev_dir); });
    start(RecordKind::Places, "places", [&] { apply_strategy2_places(data, prev_dir); });
    start(RecordKind::Pois, "pois", [&] { apply_strategy2_pois(data, prev_dir); });
    start(RecordKind::Interps, "interps", [&] { apply_strategy2_interps(data, prev_dir); });
    for (auto& f : passes) f.get();
    if (!way_remap.empty())
        parallel_each(data.addr_points, [&](AddrPoint& ap) { remap_index(ap.parent_way_id, way_remap); });
    // postcode_centroids: their stable identity is (country_code,
    // postcode_string) and the centroid records get materialized from
    // the unordered_map<postcode_id, PostcodeAccum> at write time, AFTER
    // strategy-2 runs. Stability there is handled by deterministic
    // sorting at write time + the existing postcode_id remap pipeline
    // in geocoder-diff; revisit once the other types are validated.
}

// Find the level-2 (country) admin polygon covering (lat, lng) and return
// its country_code, or 0 if none is found. Rounds the point to e7 and walks
// its own cell's admin entries. Single-country cells return immediately;
// border cells (multiple countries in the entry list) resolve by the exact
// point-in-polygon test of `countries`.
static uint16_t country_at(const ParsedData& data, const AdminRings& countries, double lat, double lng) {
    const PackedLocation p = point_e7(lat, lng);
    auto it = data.cell_to_admin.find(admin_link_cell(p).id());
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
        if ((raw_id & INTERIOR_FLAG) || countries.contains(pid, p)) {
            if (poly.area < best_area ||
                (poly.area == best_area && poly.country_code < best_cc)) {
                best_area = poly.area;
                best_cc = poly.country_code;
            }
        }
    }
    return best_cc != 0 ? best_cc : fallback_cc;
}

uint16_t country_code_at_point(const ParsedData& data, const AdminRings& countries, double lat, double lng) {
    return country_at(data, countries, lat, lng);
}

AdminRings border_country_rings(const ParsedData& data, unsigned int threads) {
    auto country_ring = [&](uint32_t raw_id) {
        uint32_t pid = raw_id & ID_MASK;
        return pid < data.admin_polygons.size() && data.admin_polygons[pid].admin_level == 2
            && data.admin_polygons[pid].country_code != 0;
    };
    const size_t polygons = data.admin_polygons.size();
    std::vector<std::atomic<uint64_t>> tested(polygons / 64 + 1);
    const auto& cells = data.cell_to_admin;
    parallel_for(cells.bucket_count(), [&](size_t b0, size_t b1, unsigned) {
        for (size_t b = b0; b < b1; b++)
            for (auto it = cells.begin(b); it != cells.end(b); ++it) {
                const auto& ids = it->second;
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
                    if (country_ring(raw_id)) mark_bit(tested, raw_id & ID_MASK);
            }
    }, threads);
    const RankedBits marked(polygons, tested);
    return AdminRings(data.admin_exact_rings, threads,
                      [&](uint32_t pid) { return marked.index(pid) != RankedBits::npos; });
}

void collect_postcode_centroids(ParsedData& data) {
    using Accums = std::unordered_map<uint64_t, ParsedData::PostcodeAccum>;
    size_t n = std::min(data.addr_points.size(), data.addr_postcode_ids.size());
    const AdminRings countries = border_country_rings(data);
    // Border cells cost a point-in-polygon each, so workers take small
    // ranges as they free up. The sums are integers: merging the workers'
    // maps in any order gives the same totals.
    std::vector<Accums> parts(parallel_threads());
    parallel_for_dynamic(n, 1 << 14, [&](size_t begin, size_t end, unsigned w) {
        for (size_t i = begin; i < end; i++) {
            uint32_t pc_id = data.addr_postcode_ids[i];
            if (pc_id == NO_DATA) continue;
            const auto& ap = data.addr_points[i];
            uint16_t cc = country_at(data, countries, ap.lat, ap.lng);
            if (cc == 0) continue;
            parts[w][postcode_key(cc, pc_id)].add(ap.lat, ap.lng);
        }
    });
    // The workers' maps summed a shard of the keys per thread.
    constexpr size_t kShards = 64;
    std::vector<Accums> shards(kShards);
    parallel_for_each(kShards, [&](size_t s, unsigned) {
        for (const auto& part : parts)
            for (const auto& [key, acc] : part) {
                if (key % kShards != s) continue;
                auto& sum = shards[s][key];
                sum.sum_lat_e7 += acc.sum_lat_e7;
                sum.sum_lng_e7 += acc.sum_lng_e7;
                sum.count += acc.count;
            }
    });
    parallel_for_each(parts.size(), [&](size_t w, unsigned) { Accums().swap(parts[w]); });
    Accums osm;
    for (auto& shard : shards) osm.merge(shard);
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

    auto admin_future = std::async(std::launch::async, [&] {
        timed_phase(label + "admin cells", [&] {
            write_cell_index_sorted(output_dir + "/admin_cells.bin", output_dir + "/admin_entries.bin",
                                    data.admin_index_cells);
        });
        std::cerr << "admin index: " << count_cells(data.admin_index_cells) << " cells, " << data.admin_polygons.size() << " polygons" << std::endl;
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
        // Admin parent chain (parallel array indexed by polygon_id), empty
        // for a region without admin polygons: the full and no-addresses
        // configurations list it for every region.
        write_futures.push_back(std::async(std::launch::async, [&] {
            write_binary_file(output_dir + "/admin_parents.bin",
                              reinterpret_cast<const char*>(data.admin_parent_ids.data()),
                              data.admin_parent_ids.size() * sizeof(uint32_t));
        }));
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
                // Packed and written a slice at a time: the packed copies
                // run to 8.7 GB on planet.
                constexpr size_t kPointsPerSlice = size_t(1) << 16;
                struct Slice {
                    std::vector<AddrPoint> points;
                    std::vector<uint8_t> bytes;
                };
                const std::string points_path = output_dir + "/addr_points.bin";
                const std::string bytes_path = output_dir + "/addr_vertices.bin";
                std::ofstream points_file(points_path, std::ios::binary);
                std::ofstream bytes_file(bytes_path, std::ios::binary);
                parallel_prefix_stream(data.addr_points.size(), kPointsPerSlice,
                    [&](size_t i) -> size_t {
                        const AddrPoint& ap = data.addr_points[i];
                        const NodeCoord* verts = footprint(ap);
                        return verts ? plan_polygon(verts, ap.vertex_count).bytes : 0;
                    },
                    [&](size_t begin, size_t end, size_t offset, size_t bytes) {
                        Slice out;
                        out.points.resize(end - begin);
                        out.bytes.resize(bytes);
                        size_t at = 0;
                        for (size_t i = begin; i < end; i++) {
                            AddrPoint ap = data.addr_points[i];
                            if (const NodeCoord* verts = footprint(ap)) {
                                ap.vertex_offset = static_cast<uint32_t>(offset + at);
                                at += pack_polygon_at(out.bytes.data() + at, verts, ap.vertex_count);
                            } else {
                                ap.vertex_offset = NO_DATA;
                                ap.vertex_count = 0;
                            }
                            out.points[i - begin] = ap;
                        }
                        return out;
                    },
                    [&](Slice&& slice) {
                        points_file.write(reinterpret_cast<const char*>(slice.points.data()),
                                          static_cast<std::streamsize>(slice.points.size() * sizeof(AddrPoint)));
                        bytes_file.write(reinterpret_cast<const char*>(slice.bytes.data()),
                                         static_cast<std::streamsize>(slice.bytes.size()));
                    }, 0, parallel_threads());
                points_file.flush();
                if (!points_file) throw std::runtime_error("failed to write " + points_path);
                bytes_file.flush();
                if (!bytes_file) throw std::runtime_error("failed to write " + bytes_path);
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
    write_strings_layout(output_dir, data.strings_tier_bases);

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
                // need "<prev>/<region>/full". Synthesize via the env var
                // build-index sets from --prev-output.
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

    // The geo index goes last, beside the files above.
    if (write_streets) {
        GeoTables tables{&data.sorted_way_cells, nullptr, nullptr};
        if (write_addresses) {
            tables[1] = &data.sorted_addr_cells;
            tables[2] = &data.sorted_interp_cells;
        }
        size_t cells = write_geo_index(output_dir, tables);
        std::cerr << "geo index: " << cells << " cells ("
                  << data.ways.size() << " ways, " << data.addr_points.size() << " addrs, "
                  << data.interp_ways.size() << " interps)" << std::endl;
    }

    log_phase((label + "geo index").c_str(), _wt, _wc);

    admin_future.get();
    for (auto& f : write_futures) f.get();
    log_phase((label + "total").c_str(), _total_t, _total_c);
}

// One admin polygon re-simplified at epsilon_scale (0 = as is) and packed on
// its own; no bytes when fewer than 3 vertices survive. Packed bytes don't
// depend on where they land, so the writers lay these out back to back
// instead of holding every simplified vertex list (6 GiB of doubles for the
// planet's uncapped variant).
struct PackedPolygon {
    std::vector<uint8_t> bytes;
    uint32_t vertex_count = 0;
    float area = 0;
    uint16_t hole_count = 0;
    uint8_t halvings = 0;  // SimplifiedRings::halvings
    bool nests = true;
    KeyholeGrid grid = KeyholeGrid::Planned;  // pack_keyhole's
};

// The build log line of how simplify_rings and pack_keyhole fared over
// packed polygons.
static std::string hole_topology(const std::vector<PackedPolygon>& packed) {
    HoleTopologyCounts counts;
    KeyholeGridCounts grids;
    for (const PackedPolygon& p : packed) {
        counts.add(p.halvings, p.nests);
        grids.add(p.grid);
    }
    return counts.summary() + "; " + grids.summary();
}
static PackedPolygon pack_admin_polygon(const ParsedData& data, const AdminPolygon& ap,
                                        double epsilon_scale) {
    if (ap.vertex_count < 3) return {};  // tombstone, or no area to draw
    const NodeCoord* verts = data.admin_vertices.data() + ap.vertex_offset;
    auto points = [&](const RingSpan& r) {
        std::vector<std::pair<double,double>> pts;
        pts.reserve(r.count);
        for (uint32_t j = 0; j < r.count; j++) pts.emplace_back(verts[r.begin + j].lat, verts[r.begin + j].lng);
        return pts;
    };
    std::vector<RingSpan> rings;
    const bool parsed = split_rings(verts, ap.vertex_count, ap.hole_count, rings);
    std::vector<std::pair<double,double>> pts;
    uint16_t holes = parsed ? ap.hole_count : 0;
    PackedPolygon out;
    if (epsilon_scale > 0) {
        // Each ring on its own: simplifying the keyhole range as one ring
        // would cut its connectors.
        double eps_m = admin_epsilon_meters(ap.admin_level) * epsilon_scale;
        std::vector<std::vector<std::pair<double,double>>> hole_rings;
        for (size_t r = 1; r < rings.size(); r++) hole_rings.push_back(points(rings[r]));
        SimplifiedRings s = simplify_rings(points(rings[0]), hole_rings,
            [&](const std::vector<std::pair<double,double>>& ring, double scale) {
                double lat = ring.empty() ? 0.0 : ring[0].first;
                return simplify_polygon_epsilon(ring, meters_to_degrees(eps_m * scale, lat));
            });
        pts = std::move(s.range);
        holes = s.holes;
        out.halvings = s.halvings;
        out.nests = s.nests;
    } else {
        pts = points(RingSpan{0, ap.vertex_count});
    }
    if (pts.size() < 3) return out;

    PackedKeyhole keyhole = pack_keyhole(pts, holes);
    out.bytes = std::move(keyhole.bytes);
    out.grid = keyhole.grid;
    out.vertex_count = static_cast<uint32_t>(pts.size());
    out.area = polygon_area(pts);
    out.hole_count = holes;
    return out;
}

void compute_admin_index_cells(ParsedData& data, const std::vector<double>& scales,
                               std::optional<double> minimal_scale) {
    const unsigned threads = parallel_threads();
    struct Found {
        std::vector<CellItemPair> shared, minimal;
    };
    std::vector<Found> found(threads);
    parallel_for_dynamic(data.admin_polygons.size(), 1, [&](size_t begin, size_t end, unsigned worker) {
        Found& out = found[worker];
        for (size_t i = begin; i < end; i++) {
            const AdminPolygon& ap = data.admin_polygons[i];
            if (ap.vertex_count == 0 || ap.vertex_offset == NO_DATA) continue;
            // The cells of the ring a variant at `scale` writes, none when it
            // writes no ring.
            auto ring_cells = [&](double scale) {
                const PackedPolygon pp = pack_admin_polygon(data, ap, scale);
                if (pp.bytes.empty()) return AdminRingCells();
                const std::vector<NodeCoord> ring = unpack_polygon(pp.bytes.data(), pp.vertex_count);
                return admin_ring_cells(ring.data(), pp.vertex_count, kAdminCellLevel, ADMIN_COVER_REACH_RAD);
            };
            const bool minimal = minimal_scale && admin_minimal_keeps(ap.admin_level, ap.place_type_override);
            std::optional<AdminRingCells> shared, own_minimal;
            for (double scale : scales) {
                AdminRingCells cells = ring_cells(scale);
                if (minimal && scale == *minimal_scale) own_minimal = cells;
                if (cells.empty()) continue;
                shared = shared ? merge_admin_ring_cells(*shared, cells) : std::move(cells);
            }
            if (minimal && !own_minimal) own_minimal = ring_cells(*minimal_scale);
            const uint32_t id = static_cast<uint32_t>(i);
            if (shared)
                for (const auto& [cell, interior] : *shared) out.shared.push_back({cell, interior ? id | INTERIOR_FLAG : id});
            if (own_minimal)
                for (const auto& [cell, interior] : *own_minimal) out.minimal.push_back({cell, interior ? id | INTERIOR_FLAG : id});
        }
    }, threads);

    auto gather = [&](std::vector<CellItemPair> Found::*part) {
        std::vector<size_t> at(threads + 1, 0);
        for (unsigned w = 0; w < threads; w++) at[w + 1] = at[w] + (found[w].*part).size();
        std::vector<CellItemPair> pairs;
        parallel_resize(pairs, at[threads]);
        parallel_for(threads, [&](size_t b, size_t e, unsigned) {
            for (size_t w = b; w < e; w++) {
                std::copy((found[w].*part).begin(), (found[w].*part).end(), pairs.begin() + at[w]);
                found[w].*part = {};
            }
        });
        // Each (cell, polygon) once: the order is the workers' schedule's
        // until sorted.
        parallel_sort(pairs.begin(), pairs.end(), cell_item_less);
        return pairs;
    };
    data.admin_index_cells = gather(&Found::shared);
    data.admin_minimal_index_cells = gather(&Found::minimal);

    auto interior = [](const std::vector<CellItemPair>& pairs) {
        return parallel_count(pairs.size(), [&](size_t k) { return (pairs[k].item_id & INTERIOR_FLAG) != 0; });
    };
    std::cerr << "Admin index cells: " << count_cells(data.admin_index_cells) << " cells, "
              << data.admin_index_cells.size() << " entries (" << interior(data.admin_index_cells)
              << " interior) over " << scales.size() << " variants; admin-minimal "
              << count_cells(data.admin_minimal_index_cells) << " cells, "
              << data.admin_minimal_index_cells.size() << " entries ("
              << interior(data.admin_minimal_index_cells) << " interior)" << std::endl;
}

// Writes packed polygons back to back: bytes_of(j) for j in [0, n), null to
// skip one.
template <class BytesOf>
static void write_packed_polygons(const std::string& path, size_t n, BytesOf bytes_of) {
    std::ofstream f(path, std::ios::binary);
    for (size_t j = 0; j < n; j++)
        if (const std::vector<uint8_t>* bytes = bytes_of(j))
            f.write(reinterpret_cast<const char*>(bytes->data()), static_cast<std::streamsize>(bytes->size()));
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + path);
}

// A continent's polygons packed as in the planet's finished variant at the
// same scale (planet_variant): the same vertices and level pack to the same
// bytes, so each takes its planet polygon's record and bytes rather than
// being simplified again. False when the planet's files can't be read.
static bool copy_planet_polygons(const ParsedData& data, const std::string& planet_variant,
                                 std::vector<PackedPolygon>& packed) {
    std::ifstream records(planet_variant + "/admin_polygons.bin", std::ios::binary | std::ios::ate);
    if (!records) return false;
    const auto polygon_bytes = static_cast<size_t>(records.tellg());
    if (polygon_bytes % sizeof(AdminPolygon) != 0) return false;
    std::vector<AdminPolygon> planet(polygon_bytes / sizeof(AdminPolygon));
    records.seekg(0);
    records.read(reinterpret_cast<char*>(planet.data()), static_cast<std::streamsize>(polygon_bytes));
    if (!records) return false;

    const int fd = ::open((planet_variant + "/admin_vertices.bin").c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return false;
    struct stat st{};
    const bool sized = ::fstat(fd, &st) == 0;
    const size_t file_bytes = sized ? static_cast<size_t>(st.st_size) : 0;
    // The records' 32-bit offsets locate bytes only in a file under 4 GiB.
    const bool addressable = sized && file_bytes <= UINT32_MAX;
    void* map = addressable && file_bytes ? ::mmap(nullptr, file_bytes, PROT_READ, MAP_PRIVATE, fd, 0) : nullptr;
    ::close(fd);
    if (!addressable || map == MAP_FAILED) return false;
    const uint8_t* bytes = static_cast<const uint8_t*>(map);

    // A polygon's bytes run to where the next packed one's start, or the
    // file's end: the variant lays them back to back in id order.
    std::vector<size_t> end(planet.size());
    for (size_t p = planet.size(), next = file_bytes; p-- > 0;) {
        end[p] = next;
        if (planet[p].vertex_count > 0) next = planet[p].vertex_offset;
    }
    std::atomic<bool> fits{true};
    parallel_for(packed.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t j = b; j < e; j++) {
            const uint32_t p = data.admin_planet_ids[j];
            if (p >= planet.size() || planet[p].vertex_count == 0) continue;
            if (planet[p].vertex_offset > end[p] || end[p] > file_bytes) {
                fits = false;
                continue;
            }
            packed[j].bytes.assign(bytes + planet[p].vertex_offset, bytes + end[p]);
            packed[j].vertex_count = planet[p].vertex_count;
            packed[j].area = planet[p].area;
            packed[j].hole_count = planet[p].hole_count;
        }
    });
    if (file_bytes) ::munmap(map, file_bytes);
    return fits;
}

void write_quality_variant(const ParsedData& data, const std::string& source_dir,
                           const std::string& output_dir, double epsilon_scale,
                           const std::string& planet_variant) {
    ensure_dir(output_dir);
    const std::string label = "      " + dir_label(output_dir) + ": ";
    auto _qt = std::chrono::steady_clock::now();
    auto _qc = CpuTicks::now();

    const size_t n = data.admin_polygons.size();
    std::vector<PackedPolygon> packed(n);
    const bool copied = !planet_variant.empty() && data.admin_planet_ids.size() == n &&
                        copy_planet_polygons(data, planet_variant, packed);
    if (!copied) {
        packed.assign(n, PackedPolygon{});
        parallel_for_dynamic(n, 1, [&](size_t begin, size_t end, unsigned) {
            for (size_t i = begin; i < end; i++)
                packed[i] = pack_admin_polygon(data, data.admin_polygons[i], epsilon_scale);
        });
    }
    log_phase((label + (copied ? "copy from the planet" : "simplify")).c_str(), _qt, _qc);
    if (!copied)
        std::cerr << "Quality " << epsilon_scale << "x hole topology (" << output_dir << "): " << hole_topology(packed)
                  << std::endl;

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
    // admin lookups.) Postal boundaries (admin_level=11) are kept in the
    // main arrays (cell index references them by ID) AND also written to
    // separate optional files.
    std::vector<AdminPolygon> new_polys(n, admin_polygon_tombstone());
    std::vector<AdminPolygon> postal_polys;
    std::vector<uint32_t> postal_idx;
    size_t vertex_bytes = 0, postal_bytes = 0;
    for (size_t i = 0; i < n; i++) {
        const PackedPolygon& pp = packed[i];
        if (pp.bytes.empty()) continue;

        AdminPolygon np = data.admin_polygons[i];
        np.vertex_offset = static_cast<uint32_t>(vertex_bytes);
        np.vertex_count = pp.vertex_count;
        np.area = pp.area;
        np.hole_count = pp.hole_count;
        new_polys[i] = np;
        vertex_bytes += pp.bytes.size();
        if (np.admin_level != 11) continue;
        np.vertex_offset = static_cast<uint32_t>(postal_bytes);
        postal_polys.push_back(np);
        postal_idx.push_back(static_cast<uint32_t>(i));
        postal_bytes += pp.bytes.size();
    }

    std::cerr << "Quality " << epsilon_scale << "x: " << new_polys.size()
              << " admin polygons, " << vertex_bytes / 1024 / 1024
              << " MiB packed vertices, " << postal_polys.size() << " postal polygons" << std::endl;

    // Write admin files (excluding postal)
    write_binary_file(output_dir + "/admin_polygons.bin",
                      reinterpret_cast<const char*>(new_polys.data()),
                      new_polys.size() * sizeof(AdminPolygon));
    write_packed_polygons(output_dir + "/admin_vertices.bin", n,
                          [&](size_t i) { return &packed[i].bytes; });
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
        write_packed_polygons(output_dir + "/postal_vertices.bin", postal_idx.size(),
                              [&](size_t k) { return &packed[postal_idx[k]].bytes; });
    }

    log_phase((label + "pack + write").c_str(), _qt, _qc);
    // Quality directories only contain the files that change.
    // Shared files (admin_cells.bin, admin_entries.bin, strings.bin)
    // stay in the parent directory.
}

// --- admin-minimal ---

// The polygons admin-minimal keeps (admin_minimal_keeps) that keep a shape at
// its scale: each one's id in the shared slots, ascending, and its bytes.
struct AdminMinimalPolygons {
    std::vector<uint32_t> ids;
    std::vector<PackedPolygon> packed;
};

static AdminMinimalPolygons pack_admin_minimal_polygons(const ParsedData& data, double epsilon_scale,
                                                        const std::string& region_dir) {
    std::vector<uint32_t> kept;
    kept.reserve(data.admin_polygons.size() / 2);
    for (size_t i = 0; i < data.admin_polygons.size(); i++) {
        const auto& p = data.admin_polygons[i];
        if (admin_minimal_keeps(p.admin_level, p.place_type_override)) kept.push_back(static_cast<uint32_t>(i));
    }
    std::vector<PackedPolygon> packed(kept.size());
    parallel_for_dynamic(kept.size(), 1, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++)
            packed[k] = pack_admin_polygon(data, data.admin_polygons[kept[k]], epsilon_scale);
    });
    std::cerr << "  Admin-minimal hole topology (" << region_dir << "): " << hole_topology(packed) << std::endl;

    AdminMinimalPolygons out;
    for (size_t k = 0; k < kept.size(); k++) {
        if (packed[k].bytes.empty()) continue;
        out.ids.push_back(kept[k]);
        out.packed.push_back(std::move(packed[k]));
    }
    return out;
}

// Where a dir puts admin-minimal's polygons: slot_of[shared id] (NO_DATA for
// one it leaves out) among `count` slots.
struct AdminMinimalSlots {
    std::vector<uint32_t> slot_of;
    size_t count = 0;
};

// admin-minimal/'s own stable slots, carried through its own sidecar like the
// full set's (apply_strategy2_admins). A dense renumbering shifted every later
// id whenever one kept polygon came or went, re-sending ~459K parent ids
// (3.5 MiB raw) every planet day.
// `kept` holds the shared ids of the polygons it keeps, ascending.
static AdminMinimalSlots own_admin_minimal_slots(const ParsedData& data, const std::vector<uint32_t>& kept,
                                                 const std::string& region_prev, const std::string& dir) {
    using namespace gc::id_alloc;
    IdAllocator alloc;
    if (!region_prev.empty()) alloc.load_previous(region_prev + "/admin-minimal/admin_polygons.osm_ids");
    const std::vector<uint32_t> slots = alloc.allocate_all(kept.size(), [&](size_t k) {
        const uint32_t id = kept[k];
        return admin_identity(id < data.admin_osm_ids.size() ? data.admin_osm_ids[id] : 0);
    });
    alloc.finalize();
    IdAllocator::write_sidecar(dir + "/admin_polygons.osm_ids", alloc.slots());

    AdminMinimalSlots out{std::vector<uint32_t>(data.admin_polygons.size(), NO_DATA), alloc.total_slots()};
    for (size_t k = 0; k < kept.size(); k++) out.slot_of[kept[k]] = slots[k];
    return out;
}

// The shared slots of the quality dirs, which POI parent_poly_ids name: each
// polygon at its own id.
static AdminMinimalSlots shared_admin_minimal_slots(const ParsedData& data, const std::vector<uint32_t>& kept) {
    AdminMinimalSlots out{std::vector<uint32_t>(data.admin_polygons.size(), NO_DATA), data.admin_polygons.size()};
    for (uint32_t id : kept) out.slot_of[id] = id;
    return out;
}

// The core strings a dir ships, their layout, and the subset its records'
// names move into (none: they keep their shared offsets).
struct AdminMinimalStrings {
    const std::vector<char>& core;
    std::array<uint32_t, STR_TIER_COUNT + 1> bases;
    const StringSubset* subset = nullptr;

    uint32_t name(uint32_t shared) const { return subset ? subset->offset_of(shared) : shared; }
};

// The core strings admin-minimal's records name: its polygons' and its place
// nodes', in the order of the shared core.
static StringSubset admin_minimal_names(const ParsedData& data, const std::vector<uint32_t>& kept) {
    const std::vector<char>& core = data.strings_tiers[0];
    std::vector<std::atomic<uint64_t>> used(core.size() / 64 + 1);
    auto use = [&](uint32_t name) {
        if (name == NO_DATA) return;
        if (name >= core.size())
            throw std::logic_error("admin-minimal names string " + std::to_string(name) + " outside the core strings (" +
                                   std::to_string(core.size()) + " bytes)");
        mark_bit(used, name);
    };
    parallel_for(kept.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++) use(data.admin_polygons[kept[k]].name_id);
    });
    parallel_for(data.place_nodes.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++)
            if (admin_minimal_keeps_place(data.place_nodes[i].place_type)) use(data.place_nodes[i].name_id);
    });
    return StringSubset(core, used);
}

// One admin-minimal dir: the polygons at their slots, the place nodes it
// keeps with parents in those slots, the admin cell index over them, and its
// core strings.
static void write_admin_minimal_dir(const ParsedData& data, const std::string& dir, const AdminMinimalPolygons& kept,
                                    const AdminMinimalSlots& slots, const AdminMinimalStrings& strings) {
    // Vertex bytes in slot order, so offsets ascend with ids; the builder's
    // tombstone (the reader's "no polygon") at a slot without one.
    std::vector<size_t> kept_at_slot(slots.count, SIZE_MAX);
    for (size_t k = 0; k < kept.ids.size(); k++) kept_at_slot[slots.slot_of[kept.ids[k]]] = k;
    std::vector<AdminPolygon> polygons(slots.count, admin_polygon_tombstone());
    size_t vertex_bytes = 0;
    for (size_t slot = 0; slot < slots.count; slot++) {
        const size_t k = kept_at_slot[slot];
        if (k == SIZE_MAX) continue;

        AdminPolygon p = data.admin_polygons[kept.ids[k]];
        p.vertex_offset = static_cast<uint32_t>(vertex_bytes);
        p.vertex_count = kept.packed[k].vertex_count;
        p.area = kept.packed[k].area;
        p.hole_count = kept.packed[k].hole_count;
        p.name_id = strings.name(p.name_id);
        polygons[slot] = p;
        vertex_bytes += kept.packed[k].bytes.size();
    }
    write_binary_file(dir + "/admin_polygons.bin", reinterpret_cast<const char*>(polygons.data()),
                      polygons.size() * sizeof(AdminPolygon));
    write_packed_polygons(dir + "/admin_vertices.bin", slots.count, [&](size_t slot) -> const std::vector<uint8_t>* {
        return kept_at_slot[slot] == SIZE_MAX ? nullptr : &kept.packed[kept_at_slot[slot]].bytes;
    });

    // Parents in this dir's slots (NO_DATA when the parent polygon isn't
    // kept), like every other id here; a node whose claimant isn't kept is
    // shown.
    std::vector<PlaceNode> places;
    places.reserve(data.place_nodes.size());
    for (const PlaceNode& node : data.place_nodes) {
        if (!admin_minimal_keeps_place(node.place_type)) continue;
        PlaceNode pn = node;
        remap_place_parent_unhiding_orphans(pn, slots.slot_of);
        pn.name_id = strings.name(pn.name_id);
        places.push_back(pn);
    }
    write_binary_file(dir + "/place_nodes.bin", reinterpret_cast<const char*>(places.data()),
                      places.size() * sizeof(PlaceNode));

    // Its admin cell index (compute_admin_index_cells), in its slots, each
    // id's INTERIOR_FLAG kept.
    std::vector<CellItemPair> cells = parallel_collect<CellItemPair>(data.admin_minimal_index_cells.size(),
        [&](size_t k, std::vector<CellItemPair>& out) {
            const CellItemPair& p = data.admin_minimal_index_cells[k];
            const uint32_t id = p.item_id & ID_MASK;
            const uint32_t slot = id < slots.slot_of.size() ? slots.slot_of[id] : NO_DATA;
            if (slot != NO_DATA) out.push_back({p.cell_id, slot | (p.item_id & INTERIOR_FLAG)});
        });
    // Own slots renumber the ids: each cell's back in canonical order (the
    // shared slots keep it).
    parallel_sort(cells.begin(), cells.end(), cell_item_less);
    write_cell_index_sorted(dir + "/admin_cells.bin", dir + "/admin_entries.bin", cells);

    write_binary_file(dir + "/" + STR_TIER_FILENAMES[0], strings.core.data(), strings.core.size());
    write_strings_layout(dir, strings.bases);

    std::cerr << "  " << dir_label(dir) << ": " << kept.ids.size() << " polygons in " << slots.count
              << " slots (of " << data.admin_polygons.size() << "), " << vertex_bytes / 1024 / 1024
              << " MiB packed vertices, " << places.size() << " place nodes (of " << data.place_nodes.size()
              << "), " << count_cells(cells) << " cells (of " << count_cells(data.admin_index_cells) << "), "
              << strings.core.size() << " bytes of core strings" << std::endl;
}

void write_admin_minimal(const ParsedData& data, const std::string& region_dir, const std::string& region_prev,
                         double epsilon_scale) {
    const AdminMinimalPolygons kept = pack_admin_minimal_polygons(data, epsilon_scale, region_dir);

    const std::string own_dir = region_dir + "/admin-minimal";
    ensure_dir(own_dir);
    const StringSubset names = admin_minimal_names(data, kept.ids);
    write_admin_minimal_dir(data, own_dir, kept, own_admin_minimal_slots(data, kept.ids, region_prev, own_dir),
                            {names.bytes(), core_only_strings_layout(names.bytes().size()), &names});

    const std::string poi_dir = region_dir + "/" + kAdminMinimalPoiDir;
    ensure_dir(poi_dir);
    write_admin_minimal_dir(data, poi_dir, kept, shared_admin_minimal_slots(data, kept.ids),
                            {data.strings_tiers[0], data.strings_tier_bases});
}

void write_arc_dirs(const ParsedData& data, const std::string& region_dir, const std::string& region_prev) {
    const AdminArcBlocks& arcs = data.admin_arcs;
    const size_t n = data.admin_polygons.size();
    if (arcs.polygons.size() != n) throw std::logic_error("admin arc polygons out of step with the polygons");
    // A dir's records: polygon i at slot slots.slot_of[i] (none at NO_DATA),
    // named name(its shared name), and a tombstone at every other slot.
    struct Records {
        std::vector<AdminPolygon> at_slot;
        std::vector<uint32_t> polygon_of;
    };
    auto records_at = [&](const AdminMinimalSlots& slots, auto name) {
        Records out{std::vector<AdminPolygon>(slots.count, admin_polygon_tombstone()),
                    std::vector<uint32_t>(slots.count, NO_DATA)};
        for (size_t i = 0; i < n; i++) {
            const uint32_t slot = slots.slot_of[i];
            if (slot == NO_DATA) continue;
            out.at_slot[slot] = data.admin_polygons[i];
            out.at_slot[slot].name_id = name(data.admin_polygons[i].name_id);
            out.polygon_of[slot] = static_cast<uint32_t>(i);
        }
        return out;
    };
    auto shared_name = [](uint32_t name) { return name; };
    std::ostringstream summary;
    summary << "  Arc dirs of " << region_dir << ":";
    auto write_dir = [&](const std::string& dir, size_t tier, const Records& records) {
        const ArcSet set = kArcTiers[tier].set;
        ensure_dir(dir);
        const size_t ring_bytes = write_ring_files(dir, arcs, set, records.at_slot, records.polygon_of);
        write_arc_files(dir, arcs.set(set), arcs.tiers[tier]);
        summary << " " << dir_label(dir) << " " << records.at_slot.size() << " slots, " << ring_bytes
                << " ring bytes, " << arcs.tiers[tier].bytes.size() << " arc bytes;";
    };
    // Live polygons (no strategy-2 tombstone) whose rings `set` holds.
    auto holding = [&](ArcSet set) {
        std::vector<uint32_t> ids;
        for (size_t i = 0; i < n; i++)
            if (data.admin_polygons[i].vertex_offset != NO_DATA &&
                (set == ArcSet::Full || arcs.polygons[i].blocks[static_cast<size_t>(set)].size > 0))
                ids.push_back(static_cast<uint32_t>(i));
        return ids;
    };

    // The full set's tiers, in the shared slots.
    const Records shared = records_at(shared_admin_minimal_slots(data, holding(ArcSet::Full)), shared_name);
    for (size_t t = 0; t < kArcTierCount; t++)
        if (kArcTiers[t].set == ArcSet::Full) write_dir(region_dir + "/" + kArcTiers[t].dir, t, shared);
    gc::id_alloc::IdAllocator::write_sidecar(arc_sidecar_path(region_dir, ArcSet::Full), arcs.set(ArcSet::Full).sidecar);

    // The minimal set's, in admin-minimal's own slots (and names) and in the
    // shared ones.
    const std::vector<uint32_t> kept = holding(ArcSet::Minimal);
    for (size_t t = 0; t < kArcTierCount; t++) {
        if (kArcTiers[t].set != ArcSet::Minimal) continue;
        const std::string own_dir = region_dir + "/" + kArcTiers[t].dir;
        ensure_dir(own_dir);
        const StringSubset names = admin_minimal_names(data, kept);
        write_dir(own_dir, t, records_at(own_admin_minimal_slots(data, kept, region_prev, own_dir),
                                         [&](uint32_t name) { return names.offset_of(name); }));
        write_dir(region_dir + "/" + kAdminMinimalPoiDir, t, records_at(shared_admin_minimal_slots(data, kept), shared_name));
    }
    gc::id_alloc::IdAllocator::write_sidecar(arc_sidecar_path(region_dir, ArcSet::Minimal),
                                             arcs.set(ArcSet::Minimal).sidecar);
    std::cerr << summary.str() << std::endl;
}
