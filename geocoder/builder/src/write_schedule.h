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
enum class RecordKind { Ways, Addrs, Interps, Pois, Places, Admins };
constexpr size_t RECORD_KIND_COUNT = 6;

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
// Strings and postcode centroids count with the admins: every region holds
// them whole or nearly so.
inline KindValues record_bytes(const ParsedData& d) {
    KindValues b{};
    at(b, RecordKind::Ways) = vector_bytes(d.ways) + vector_bytes(d.way_osm_ids) + vector_bytes(d.way_sidecar_blob)
        + vector_bytes(d.way_parent_ids) + vector_bytes(d.way_postcode_ids) + vector_bytes(d.way_orig_name_ids)
        + vector_bytes(d.street_nodes) + vector_bytes(d.sorted_way_cells);
    at(b, RecordKind::Addrs) = vector_bytes(d.addr_points) + vector_bytes(d.addr_osm_ids)
        + vector_bytes(d.addr_sidecar_blob) + vector_bytes(d.addr_postcode_ids) + vector_bytes(d.addr_vertices)
        + vector_bytes(d.sorted_addr_cells);
    at(b, RecordKind::Interps) = vector_bytes(d.interp_ways) + vector_bytes(d.interp_osm_ids)
        + vector_bytes(d.interp_sidecar_blob) + vector_bytes(d.interp_postcode_ids) + vector_bytes(d.interp_nodes)
        + vector_bytes(d.sorted_interp_cells);
    at(b, RecordKind::Pois) = vector_bytes(d.poi_records) + vector_bytes(d.poi_osm_ids)
        + vector_bytes(d.poi_sidecar_blob) + vector_bytes(d.poi_vertices) + vector_bytes(d.sorted_poi_cells);
    at(b, RecordKind::Places) = vector_bytes(d.place_nodes) + vector_bytes(d.place_osm_ids)
        + vector_bytes(d.place_sidecar_blob) + vector_bytes(d.sorted_place_cells);
    uint64_t strings = d.string_pool.data().size();
    for (const auto& tier : d.strings_tiers) strings += tier.size();
    at(b, RecordKind::Admins) = vector_bytes(d.admin_polygons) + vector_bytes(d.admin_osm_ids)
        + vector_bytes(d.admin_sidecar_blob) + vector_bytes(d.admin_parent_ids) + vector_bytes(d.admin_vertices)
        + map_bytes_approx(d.cell_to_admin) + map_bytes_approx(d.postcode_accum) + strings;
    for (const auto& [cell, ids] : d.cell_to_admin) at(b, RecordKind::Admins) += vector_bytes(ids);
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

// The cell pairs of each kind a region's records sit in (admins have none).
inline KindValues pair_counts(const ParsedData& d) {
    KindValues n{};
    at(n, RecordKind::Ways) = d.sorted_way_cells.size();
    at(n, RecordKind::Addrs) = d.sorted_addr_cells.size();
    at(n, RecordKind::Interps) = d.sorted_interp_cells.size();
    at(n, RecordKind::Pois) = d.sorted_poi_cells.size();
    at(n, RecordKind::Places) = d.sorted_place_cells.size();
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
// and, while ids are collected, a bitset per record on each worker; beside
// it, the workers' copies of a share of the subset's cell pairs.
constexpr uint64_t kFilterMapBytesPerRecord = 4;
constexpr unsigned kFilterBitsetWorkerDivisor = 4;
constexpr uint64_t kFilterPairCopyDivisor = 2;

// Per mode dir, write_index's slices in flight (geo index and packed addr
// points), postcode centroids and admin cell copies (planet: 0.8 GiB for
// the three).
constexpr uint64_t kModeBytes = uint64_t(256) << 20;

// Stage working sets as fractions of the records they read: the quality
// variants' packed polygons (three at once) and admin-minimal's copies of
// the admins (planet: 1.0 and 0.4 GiB of 4.4), a POI tier's filtered
// records, vertices and cells (planet: 2.7 GiB of 7.8).
constexpr uint64_t kQualityAdminDivisor = 2;
constexpr uint64_t kAdminMinimalAdminDivisor = 4;
constexpr uint64_t kPoiTierPoiTenths = 4;

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

// What the continent split holds beyond the subset it builds.
inline uint64_t filter_cost(const KindValues& planet_counts, const KindValues& continent_pairs, unsigned threads) {
    const uint64_t records = sum(planet_counts);
    const uint64_t bitset_workers = std::max(1u, threads / kFilterBitsetWorkerDivisor);
    return records * kFilterMapBytesPerRecord + records / 8 * bitset_workers
         + sum(continent_pairs) * sizeof(CellItemPair) / kFilterPairCopyDivisor;
}

// What each write stage holds while it runs.
struct StageCosts {
    uint64_t modes = 0, quality = 0, places = 0, admin_minimal = 0, poi_tiers = 0;

    uint64_t total() const { return modes + quality + places + admin_minimal + poi_tiers; }
    uint64_t largest() const { return std::max({modes, quality, places, admin_minimal, poi_tiers}); }
};

// The place files are written straight from the records.
inline StageCosts stage_costs(const KindValues& bytes) {
    StageCosts c;
    const uint64_t admins = at(bytes, RecordKind::Admins);
    c.modes = 3 * kModeBytes;
    c.quality = admins / kQualityAdminDivisor;
    c.admin_minimal = admins / kAdminMinimalAdminDivisor;
    c.poi_tiers = at(bytes, RecordKind::Pois) * kPoiTierPoiTenths / 10;
    return c;
}

// How a region's write runs and the bytes it holds at its peak: its records
// plus the most any step holds beside them.
struct RegionCost {
    uint64_t bytes;
    RunOrder order;
};

inline uint64_t region_peak(const KindValues& bytes, const KindValues& counts, uint64_t filter_bytes,
                            RunOrder order) {
    const StageCosts stages = stage_costs(bytes);
    const uint64_t steps = order == RunOrder::Serial ? stages.largest() : stages.total();
    return sum(bytes) + std::max({filter_bytes, strategy2_cost(counts, order), steps});
}

// A continent runs its steps at once when that fits the budget, else one
// after another.
inline RegionCost continent_cost(const KindValues& bytes, const KindValues& counts, uint64_t filter_bytes,
                                 uint64_t capacity) {
    const uint64_t at_once = region_peak(bytes, counts, filter_bytes, RunOrder::Concurrent);
    if (at_once <= capacity) return {at_once, RunOrder::Concurrent};

    return {region_peak(bytes, counts, filter_bytes, RunOrder::Serial), RunOrder::Serial};
}

// A continent's share of the planet's values, kind by kind, from its share
// of each kind's cell pairs, with kShareMarginPercent. Admins have no pairs:
// they take the share the other kinds' values add up to.
inline KindValues continent_share(const KindValues& planet, const KindValues& planet_pairs,
                                  const KindValues& continent_pairs) {
    KindValues out{};
    double shared = 0, total = 0;
    for (size_t k = 0; k < RECORD_KIND_COUNT; k++) {
        if (planet_pairs[k] == 0) continue;
        double share = std::min(1.0, static_cast<double>(continent_pairs[k]) / static_cast<double>(planet_pairs[k]));
        shared += static_cast<double>(planet[k]) * share;
        total += static_cast<double>(planet[k]);
        out[k] = static_cast<uint64_t>(static_cast<double>(planet[k]) * share * kShareMarginPercent / 100);
    }
    const double admin_share = total > 0 ? shared / total : 0;
    at(out, RecordKind::Admins) = static_cast<uint64_t>(
        static_cast<double>(at(planet, RecordKind::Admins)) * admin_share * kShareMarginPercent / 100);
    return out;
}
