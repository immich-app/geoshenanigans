// How much of the write phase runs at once: the bytes each region and stage
// needs, estimated from its records, against the memory the host allows.
// No S2 here, so the unit tests can check the estimates.
#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <vector>

#include "parsed_data.h"

// The record kinds a region holds; the continent split shares out each one
// by its cell pairs.
enum class RecordKind { Ways, Addrs, Interps, Pois, Places, Admins, Waters };
constexpr size_t RECORD_KIND_COUNT = 7;

// Per record kind: bytes held (sizes, not capacities: room a vector never
// filled costs no memory) or records.
using KindValues = std::array<uint64_t, RECORD_KIND_COUNT>;

inline uint64_t& at(KindValues& v, RecordKind kind) { return v[static_cast<size_t>(kind)]; }
inline uint64_t at(const KindValues& v, RecordKind kind) { return v[static_cast<size_t>(kind)]; }

inline uint64_t sum(const KindValues& v) {
    uint64_t total = 0;
    for (uint64_t x : v) total += x;
    return total;
}

template <class T>
inline uint64_t vector_bytes(const std::vector<T>& v) { return v.size() * sizeof(T); }

// A region's records and the structures that run parallel to them, by kind.
// Strings, postcode centroids, the encoded arcs and the admin cell indexes
// count with the admins: every region holds them whole or nearly so.
inline KindValues record_bytes(const ParsedData& d) {
    KindValues b{};
    at(b, RecordKind::Ways) = vector_bytes(d.ways) + vector_bytes(d.way_osm_ids) + vector_bytes(d.way_sidecar_blob)
        + vector_bytes(d.way_parent_ids) + vector_bytes(d.way_postcode_ids) + vector_bytes(d.way_orig_name_ids)
        + vector_bytes(d.street_nodes) + vector_bytes(d.sorted_way_cells);
    at(b, RecordKind::Addrs) = vector_bytes(d.addr_points) + vector_bytes(d.addr_osm_ids)
        + vector_bytes(d.addr_sidecar_blob) + vector_bytes(d.addr_postcode_ids) + vector_bytes(d.addr_city_ids)
        + vector_bytes(d.addr_vertices) + vector_bytes(d.sorted_addr_cells);
    at(b, RecordKind::Interps) = vector_bytes(d.interp_ways) + vector_bytes(d.interp_osm_ids)
        + vector_bytes(d.interp_sidecar_blob) + vector_bytes(d.interp_postcode_ids) + vector_bytes(d.interp_nodes)
        + vector_bytes(d.sorted_interp_cells);
    at(b, RecordKind::Pois) = vector_bytes(d.poi_records) + vector_bytes(d.poi_osm_ids)
        + vector_bytes(d.poi_sidecar_blob) + vector_bytes(d.poi_vertices) + vector_bytes(d.sorted_poi_cells);
    at(b, RecordKind::Places) = vector_bytes(d.place_nodes) + vector_bytes(d.place_osm_ids)
        + vector_bytes(d.sorted_place_cells);
    uint64_t strings = d.string_pool.data().size();
    for (const auto& tier : d.strings_tiers) strings += tier.size();
    at(b, RecordKind::Admins) = vector_bytes(d.admin_polygons) + vector_bytes(d.admin_osm_ids)
        + vector_bytes(d.admin_sidecar_blob) + vector_bytes(d.admin_parent_ids) + vector_bytes(d.admin_index_cells)
        + vector_bytes(d.admin_minimal_index_cells) + map_bytes_approx(d.postcode_accum) + strings
        + d.admin_arcs.bytes();
    // The water layer: its records, names, encoded arcs and cells.
    at(b, RecordKind::Waters) = d.water.bytes();
    return b;
}

inline KindValues record_counts(const ParsedData& d) {
    KindValues n{};
    at(n, RecordKind::Ways) = d.ways.size();
    at(n, RecordKind::Addrs) = d.addr_points.size();
    at(n, RecordKind::Interps) = d.interp_ways.size();
    at(n, RecordKind::Pois) = d.poi_records.size();
    at(n, RecordKind::Places) = d.place_nodes.size();
    at(n, RecordKind::Admins) = d.admin_polygons.size();
    return n;
}

// The cell pairs of each kind a region's records sit in: the waters' their
// cell index's entries, which the continent split routes; admins have none.
// The strategy-2 passes and the ordering steps never see water records:
// record_counts leaves them out.
inline KindValues pair_counts(const ParsedData& d) {
    KindValues n{};
    at(n, RecordKind::Ways) = d.sorted_way_cells.size();
    at(n, RecordKind::Addrs) = d.sorted_addr_cells.size();
    at(n, RecordKind::Interps) = d.sorted_interp_cells.size();
    at(n, RecordKind::Pois) = d.sorted_poi_cells.size();
    at(n, RecordKind::Places) = d.sorted_place_cells.size();
    at(n, RecordKind::Waters) = d.water.cells.size();
    return n;
}

inline uint64_t largest(const KindValues& v) { return *std::max_element(v.begin(), v.end()); }

// --- Estimates (bytes held beyond a region's records) ---
//
// Fitted to the per-step peaks of a planet build run one step at a time
// (planet-261006): each overestimates what it measured.

// Free memory the schedule leaves untouched: page cache for the files being
// written, allocator slack and estimation error. A fraction of the limit,
// at least kMinReserveBytes.
constexpr uint64_t kMinReserveBytes = uint64_t(4) << 30;
constexpr uint64_t kReserveFraction = 16;

// The strategy-2 passes' working set per record: the allocator's keys and
// slots, then each pass's reordered copies (55 B measured on North America,
// 40 B on Europe).
constexpr uint64_t kStrategy2BytesPerRecord = 56;

// The continent split's planet-sized scratch: an id map per record (4 B)
// and, while ids are collected, 3 bits per record (the bit every worker
// marks, and its RankedBits blocks of 16 B per 64 records); beside it, the
// workers' copies of a share of the subset's cell pairs. The water split
// reads the planet's water masks (a byte a water cell entry, held with the
// other kinds' masks while the continents run), and marks each of the
// planet's water polygons, and beside those each of its arcs, as the
// records are marked: 3 bits each, with no id map.
constexpr uint64_t kFilterMapBytesPerRecord = 4;
constexpr uint64_t kFilterMarkBitsPerRecord = 3;
constexpr uint64_t kFilterPairCopyDivisor = 2;

// Per mode dir, write_index's slices in flight, postcode centroids and admin
// cell copies (planet: 0.8 GiB for the three). The slices are the geo
// index's and those of the way, address and side file writers
// (record_files_io.h), each of which holds at most kSliceWindow slices of
// 65,536 records however many cores encode them: a way writer their rows and
// node blocks, the address writer their blocks, blobs and index rows, a side
// writer their values' blocks.
constexpr uint64_t kModeBytes = uint64_t(256) << 20;

// Stage working sets as fractions of the records they read: the boundary
// tiers' records and slot maps, and admin-minimal's records, place nodes,
// cell index and strings for each of its dirs, against the admins (planet
// 261018b, one stage at a time: at most 0.04 and 0.70 GiB of 2.82 GiB of
// admins; the boundary tiers' 0.11 GiB of records, slot maps and sidecar is
// below what RSS shows of a stage), a POI tier's filtered records, vertices
// and cells (planet: 2.7 GiB of 7.8).
constexpr uint64_t kAdminTierAdminDivisor = 16;
constexpr uint64_t kAdminMinimalAdminDivisor = 3;
constexpr uint64_t kPoiTierPoiTenths = 4;

// The water stage's working set against the layer it reads: the records in
// their slots for each set, the names a set uses, and the major tier's
// share of the arcs and ring blocks, its slots and the sidecars (planet
// 261020: about half of it); and beside the layer's cells, the copy of them
// renumbered to a set's slots, collected in the workers' parts beside it and
// sorted through a buffer as large (three times the cells).
constexpr uint64_t kWaterStageWaterTenths = 6;
constexpr uint64_t kWaterStageCellTimes = 3;

// The water stage's arrays by slot, which a set's slot table sizes,
// tombstones and all: the records at their slots, each slot's polygon and
// the records it writes. The share of the layer above counts those of
// today's polygons; a previous build whose slots outnumber them (water
// gone since) leaves the rest to these.
constexpr uint64_t kWaterSlotBytes = sizeof(AdminPolygon) + sizeof(uint32_t) + sizeof(AdminPolygonRecord);

// What a water set's polygon slot allocation (polygon_slots) holds for each
// slot of the previous build's sidecar it loads, tombstones among them,
// before the arrays above exist: the slot as loaded, its key among the
// previous keys and the sort's copy of them, a fallback pass's key of it
// and the sort's copy, its fallback key, and its marks.
constexpr uint64_t kWaterSlotAllocationBytes =
    sizeof(gc::id_alloc::SidecarSlot) + 4 * sizeof(gc::id_alloc::KeyIdx) + sizeof(uint64_t) + 2;

// Each slot of the previous build's water dirs: the more of the two.
constexpr uint64_t kWaterPreviousSlotBytes = std::max(kWaterSlotBytes, kWaterSlotAllocationBytes);

// What a water set's arc slot allocation (allocate_arc_slots) holds for
// each slot of the previous build's sidecar it loads, tombstones among
// them: the slot as loaded and the key it can be claimed by, its key among
// the previous keys and the sort's copy of them, its fallback keys, a
// fallback pass's key of it and the sort's copy, and its marks; past those
// the write's offset and arc of a slot (write_arc_files) are less. The
// share of the layer above counts the slots of today's arcs; a previous
// build whose slots outnumber them (water gone since) leaves the rest to
// these.
constexpr uint64_t kWaterArcSlotBytes = sizeof(gc::id_alloc::SidecarSlot) + sizeof(uint64_t) +
                                        4 * sizeof(gc::id_alloc::KeyIdx) + sizeof(ArcFallbackKeys) + 2;

// The previous build's arc slots of a region's water dirs past its arcs
// today (water::previous_arc_slots, water::arc_counts): the larger of the
// all set's and, `with_major` (a build that writes the major tier), the
// major tier's, whose allocations run one after the other (none past where
// today's are as many or more).
inline uint64_t water_arc_slots_past(const water::ArcCounts& previous, const water::ArcCounts& today,
                                     bool with_major = true) {
    auto past = [](uint64_t slots, uint64_t arcs) { return slots > arcs ? slots - arcs : 0; };
    return std::max(past(previous.all, today.all), with_major ? past(previous.major, today.major) : 0);
}

// What write_place_files holds beside the nodes it is given, as a multiple
// of their bytes (20 a node): each node's cell key, the order and the sort's
// scratch (4 bytes each), and the three files in memory until they are
// written (a visible row is under 13 bytes, its cell row 8, a block's table
// entry 16 for 64 rows; a hidden node 16), in vectors that hold up to three
// times their bytes while they grow: at most 74 bytes a node.
constexpr uint64_t kPlaceWriterTimes = 4;

// A continent's records can run past its share of the planet's cell pairs
// (Europe's by 5%, North America's addrs by 16%); the share estimates carry
// this margin.
constexpr uint64_t kShareMarginPercent = 110;

// The budget the write phase schedules against: the limit less what the build
// holds and the reserve. No known limit: no bound.
inline uint64_t write_budget(uint64_t limit, uint64_t in_use) {
    if (limit == 0) return UINT64_MAX;

    uint64_t reserve = std::max(kMinReserveBytes, limit / kReserveFraction);
    return limit > in_use + reserve ? limit - in_use - reserve : 0;
}

// What the strategy-2 passes hold: every kind's at once, or the largest
// alone.
inline uint64_t strategy2_cost(const KindValues& counts, RunOrder order) {
    return (order == RunOrder::Serial ? largest(counts) : sum(counts)) * kStrategy2BytesPerRecord;
}

// What each strategy-2 pass holds while it runs: the allocator's working set
// per record, and the copy of the nodes the street and interpolation passes
// repack.
inline KindValues strategy2_pass_costs(const ParsedData& d) {
    KindValues c = record_counts(d);
    for (uint64_t& v : c) v *= kStrategy2BytesPerRecord;
    at(c, RecordKind::Ways) += vector_bytes(d.street_nodes);
    at(c, RecordKind::Interps) += vector_bytes(d.interp_nodes);
    return c;
}

// The ordering steps' working set per record beside the copy of the records
// they move them into: the sort's keys, its merge copy, the order, the run
// starts and old_to_new. The cell order's (regroup_by_cell: each record's
// least cell, the runs' keys and their merge copy, the sorted runs, beside
// the order and starts) comes once the sort's keys are gone and is no more.
constexpr uint64_t kReorderBytesPerRecord = 48;

// What the ordering steps of the addrs, ways, POIs, places and interps hold
// running at once: a copy of each one's records, nodes and parallel arrays
// (their cell pairs are renumbered in place), and kReorderBytesPerRecord a
// record.
inline uint64_t ordering_cost(const ParsedData& d) {
    const uint64_t copies = vector_bytes(d.ways) + vector_bytes(d.street_nodes) + vector_bytes(d.way_osm_ids)
        + vector_bytes(d.way_parent_ids) + vector_bytes(d.way_postcode_ids)
        + vector_bytes(d.addr_points) + vector_bytes(d.addr_vertices) + vector_bytes(d.addr_osm_ids)
        + vector_bytes(d.addr_postcode_ids) + vector_bytes(d.addr_city_ids)
        + vector_bytes(d.poi_records) + vector_bytes(d.poi_vertices) + vector_bytes(d.poi_osm_ids)
        + vector_bytes(d.place_nodes) + vector_bytes(d.place_osm_ids)
        + vector_bytes(d.interp_ways) + vector_bytes(d.interp_nodes) + vector_bytes(d.interp_osm_ids)
        + vector_bytes(d.interp_postcode_ids);
    const KindValues counts = record_counts(d);
    return copies + (sum(counts) - at(counts, RecordKind::Admins)) * kReorderBytesPerRecord;
}

// What the continent split holds beyond the subset it builds, of a planet
// of `water_polygons` water polygons over `water_arcs` arcs, which
// record_counts leaves out.
inline uint64_t filter_cost(const KindValues& planet_counts, const KindValues& continent_pairs,
                            uint64_t water_polygons = 0, uint64_t water_arcs = 0) {
    const uint64_t records = sum(planet_counts);
    return records * kFilterMapBytesPerRecord
         + (records + water_polygons + water_arcs) * kFilterMarkBitsPerRecord / 8
         + sum(continent_pairs) * sizeof(CellItemPair) / kFilterPairCopyDivisor;
}

// What each write stage holds while it runs: every stage a region's write
// runs, the boundary tiers and both admin-minimal dirs (which write the arc
// geometry) among them.
struct StageCosts {
    uint64_t modes = 0, admin_tiers = 0, places = 0, admin_minimal = 0, poi_tiers = 0, water = 0;

    uint64_t total() const { return modes + admin_tiers + places + admin_minimal + poi_tiers + water; }
    uint64_t largest() const { return std::max({modes, admin_tiers, places, admin_minimal, poi_tiers, water}); }
};

// The place stage writes its dirs one after another, each through the place
// writer; an admin-minimal dir writes the nodes it keeps, a copy of at most
// all of them, through the same writer. `water_pairs` are the water cell
// index's entries, among the waters' bytes; `water_slots` the previous
// build's slots of the water dirs (water::previous_slots), each of which
// the slot allocation loads and the write keeps; `water_arc_slots` the
// previous build's arc slots past today's arcs (water_arc_slots_past),
// which the slot allocations load.
inline StageCosts stage_costs(const KindValues& bytes, uint64_t water_pairs = 0, uint64_t water_slots = 0,
                              uint64_t water_arc_slots = 0) {
    StageCosts c;
    const uint64_t admins = at(bytes, RecordKind::Admins), places = at(bytes, RecordKind::Places);
    c.modes = 3 * kModeBytes;
    c.admin_tiers = admins / kAdminTierAdminDivisor;
    c.places = places * kPlaceWriterTimes;
    c.admin_minimal = admins / kAdminMinimalAdminDivisor + places * (1 + kPlaceWriterTimes);
    c.poi_tiers = at(bytes, RecordKind::Pois) * kPoiTierPoiTenths / 10;
    const uint64_t waters = at(bytes, RecordKind::Waters);
    const uint64_t cells = std::min<uint64_t>(waters, water_pairs * sizeof(CellItemPair));
    c.water = (waters - cells) * kWaterStageWaterTenths / 10 + cells * kWaterStageCellTimes +
              water_slots * kWaterPreviousSlotBytes + water_arc_slots * kWaterArcSlotBytes;
    return c;
}

// How a region's write runs and the bytes it holds at its peak: its records
// plus the most any step holds beside them.
struct RegionCost {
    uint64_t bytes;
    RunOrder order;
};

inline uint64_t region_peak(const KindValues& bytes, const KindValues& counts, uint64_t filter_bytes,
                            RunOrder order, uint64_t water_pairs = 0, uint64_t water_slots = 0,
                            uint64_t water_arc_slots = 0) {
    const StageCosts stages = stage_costs(bytes, water_pairs, water_slots, water_arc_slots);
    const uint64_t steps = order == RunOrder::Serial ? stages.largest() : stages.total();
    return sum(bytes) + std::max({filter_bytes, strategy2_cost(counts, order), steps});
}

// A continent runs its steps at once when that fits the budget, else one
// after another. It holds its whole reservation: its write takes no stage
// leases of its own.
inline RegionCost continent_cost(const KindValues& bytes, const KindValues& counts, uint64_t filter_bytes,
                                 uint64_t capacity, uint64_t water_pairs = 0, uint64_t water_slots = 0,
                                 uint64_t water_arc_slots = 0) {
    const uint64_t at_once =
        region_peak(bytes, counts, filter_bytes, RunOrder::Concurrent, water_pairs, water_slots, water_arc_slots);
    if (at_once <= capacity) return {at_once, RunOrder::Concurrent};

    return {region_peak(bytes, counts, filter_bytes, RunOrder::Serial, water_pairs, water_slots, water_arc_slots),
            RunOrder::Serial};
}

// A continent's share of the planet's values, kind by kind, from its share
// of each kind's cell pairs, with kShareMarginPercent. Admins have no pairs:
// they take the share the values of the kinds of records (all but the
// waters) add up to. The waters take `waters`: a polygon its cells list
// comes whole, with its arcs, however few of its cells those are, so the
// split's share of them is counted polygon by polygon (water::share_bytes).
inline KindValues continent_share(const KindValues& planet, const KindValues& planet_pairs,
                                  const KindValues& continent_pairs, uint64_t waters = 0) {
    KindValues out{};
    double shared = 0, total = 0;
    for (size_t k = 0; k < RECORD_KIND_COUNT; k++) {
        if (planet_pairs[k] == 0 || k == static_cast<size_t>(RecordKind::Waters)) continue;
        double share = std::min(1.0, static_cast<double>(continent_pairs[k]) / static_cast<double>(planet_pairs[k]));
        out[k] = static_cast<uint64_t>(static_cast<double>(planet[k]) * share * kShareMarginPercent / 100);
        shared += static_cast<double>(planet[k]) * share;
        total += static_cast<double>(planet[k]);
    }
    const double admin_share = total > 0 ? shared / total : 0;
    const double admins = static_cast<double>(at(planet, RecordKind::Admins));
    at(out, RecordKind::Admins) = static_cast<uint64_t>(admins * admin_share * kShareMarginPercent / 100);
    at(out, RecordKind::Waters) = waters;
    return out;
}
