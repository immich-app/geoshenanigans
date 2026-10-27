#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <iomanip>
#include <sstream>
#include <thread>
#include <unistd.h>
#include <sys/stat.h>

#include "packed_location.h"
#include "types.h"
#include "string_pool.h"
#include "id_allocator.h"
#include "place_claims.h"
#include "parallel.h"
#include "ranked_bits.h"

// The records one POI tier's files carry, in record order, and how many of
// them are tombstones. A tombstone only goes to the tiers that held its
// record (poi_shipped_tier). `slots` is the strategy-2 slot table, parallel
// to `records`, or empty when strategy 2 did not run.
struct PoiTierSelection {
    std::vector<uint32_t> indices;
    size_t tombstones = 0;
};

inline PoiTierSelection select_poi_tier(const std::vector<PoiRecord>& records,
                                        const std::vector<gc::id_alloc::SidecarSlot>& slots,
                                        uint8_t max_tier) {
    if (!slots.empty() && slots.size() != records.size())
        throw std::runtime_error("POI slot table not parallel to poi_records");

    auto slot_of = [&](size_t i) { return slots.empty() ? nullptr : &slots[i]; };
    PoiTierSelection out;
    out.indices = parallel_filter(records.size(), [&](size_t i) {
        return gc::id_alloc::poi_shipped_tier(records[i].tier, slot_of(i)) <= max_tier;
    });
    out.tombstones = parallel_count(out.indices.size(), [&](size_t k) {
        const gc::id_alloc::SidecarSlot* slot = slot_of(out.indices[k]);
        return slot && gc::id_alloc::is_tombstone(*slot);
    });
    return out;
}

// Categories a POI tier's poi_meta.json lists: those of the live records in
// the tier's selection. A tombstone is memset to category 0 (MUSEUM), so
// counting it would list museums in a tier that holds none.
inline std::set<uint8_t> poi_meta_categories(const std::vector<PoiRecord>& records,
                                             const std::vector<gc::id_alloc::SidecarSlot>& slots,
                                             const PoiTierSelection& selection) {
    constexpr size_t kCategories = 256;
    std::vector<std::array<bool, kCategories>> seen(parallel_threads(), std::array<bool, kCategories>{});
    const size_t blocks = parallel_blocks(selection.indices.size(), [&](size_t b, size_t begin, size_t end) {
        for (size_t k = begin; k < end; k++) {
            const uint32_t i = selection.indices[k];
            if (!slots.empty() && gc::id_alloc::is_tombstone(slots[i])) continue;

            seen[b][records[i].category] = true;
        }
    });
    std::set<uint8_t> cats;
    for (size_t b = 0; b < blocks; b++)
        for (size_t c = 0; c < kCategories; c++)
            if (seen[b][c]) cats.insert(static_cast<uint8_t>(c));
    return cats;
}

// --- Directory creation ---

inline void ensure_dir(const std::string& path) {
    // Recursive mkdir — create all parent directories
    for (size_t i = 1; i < path.size(); i++) {
        if (path[i] == '/') {
            mkdir(path.substr(0, i).c_str(), 0755);
        }
    }
    mkdir(path.c_str(), 0755);
}

// --- External postcode keys: (country 'XX' packed uppercase, string id) ---

inline uint64_t postcode_key(uint16_t cc, uint32_t pc_id) { return (static_cast<uint64_t>(cc) << 32) | pc_id; }
inline uint16_t postcode_key_cc(uint64_t key) { return static_cast<uint16_t>(key >> 32); }
inline uint32_t postcode_key_pc(uint64_t key) { return static_cast<uint32_t>(key); }

// --- CPU tick reading (all threads via getrusage) ---

#include <sys/resource.h>
#include <cmath>

struct CpuTicks {
    long long process_us = 0;  // user+system microseconds (all threads)

    static CpuTicks now() {
        CpuTicks ct;
        struct rusage ru;
        if (getrusage(RUSAGE_SELF, &ru) == 0) {
            ct.process_us = (long long)ru.ru_utime.tv_sec * 1000000 + ru.ru_utime.tv_usec
                          + (long long)ru.ru_stime.tv_sec * 1000000 + ru.ru_stime.tv_usec;
        }
        return ct;
    }
};

// --- Phase timer with CPU utilization ---

inline long get_rss_mb() {
    long rss_pages = 0;
    FILE* f = fopen("/proc/self/statm", "r");
    if (f) {
        long size;
        if (fscanf(f, "%ld %ld", &size, &rss_pages) != 2) rss_pages = 0;
        fclose(f);
    }
    return rss_pages * sysconf(_SC_PAGESIZE) / (1024 * 1024);
}

inline void log_phase(const char* name, std::chrono::steady_clock::time_point& t,
                       CpuTicks& prev_cpu) {
    auto now = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - t).count();
    auto cpu_now = CpuTicks::now();
    double cpu_sec = (cpu_now.process_us - prev_cpu.process_us) / 1e6;
    double wall_sec = ms / 1000.0;
    double cores_used = (wall_sec > 0.1) ? cpu_sec / wall_sec : 0;
    unsigned ncpu = std::thread::hardware_concurrency();
    int pct = (wall_sec > 0.1 && ncpu > 0) ? (int)(cores_used * 100.0 / ncpu) : 0;
    long rss = get_rss_mb();
    // Formatted here and written in one call: phases end on many threads at
    // once, and manipulators on the shared std::cerr would race.
    std::ostringstream line;
    line << "  [" << ms/1000 << "." << (ms%1000)/100 << "s"
         << " " << std::fixed << std::setprecision(1) << cores_used << "/" << ncpu << "cores"
         << " " << pct << "%"
         << " " << rss << "MiB] " << name << "\n";
    std::cerr << line.str();
    t = now;
    prev_cpu = cpu_now;
}

// Backward compat overload (no CPU tracking)
inline void log_phase(const char* name, std::chrono::steady_clock::time_point& t) {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t).count();
    std::cerr << "  [" << ms/1000 << "." << (ms%1000)/100 << "s] " << name << std::endl;
    t = std::chrono::steady_clock::now();
}

// Runs fn() and logs it as one phase line. Steps that overlap others still
// report the whole process's cores.
template <class Fn>
inline void timed_phase(const std::string& name, Fn&& fn) {
    auto t = std::chrono::steady_clock::now();
    auto cpu = CpuTicks::now();
    fn();
    log_phase(name.c_str(), t, cpu);
}

// Approximate heap bytes held by a vector (size + capacity slack).
template <class V>
inline size_t vec_bytes(const V& v) {
    return v.capacity() * sizeof(typename V::value_type);
}
// unordered_map: bucket array + node payload. Approx — close enough for
// "which structure is fat" decisions; not a precise audit.
template <class M>
inline size_t map_bytes_approx(const M& m) {
    using K = typename M::key_type;
    using V = typename M::mapped_type;
    return m.bucket_count() * sizeof(void*)
         + m.size() * (sizeof(K) + sizeof(V) + 2 * sizeof(void*));
}
inline void log_mem(const char* label, size_t bytes) {
    if (bytes < (1ull << 20)) return;  // skip noise <1 MiB
    std::cerr << "    [mem] " << label << ": "
              << (bytes / (1024*1024)) << " MiB" << std::endl;
}

// --- String pool tiers ---
//
// At build finalization, every interned string is assigned to exactly one
// tier based on which record types reference it. Clients download the tier
// files their mode needs and lookups into a missing tier return an empty
// string, so a string shared by several consumers must live in a tier every
// one of their modes downloads. See string_home_tier.
constexpr uint8_t STR_TIER_BIT_CORE     = 1 << 0;  // admin_polygons, place_nodes
constexpr uint8_t STR_TIER_BIT_STREET   = 1 << 1;  // ways, addr_point.street_id, interp, poi.parent_street_id
constexpr uint8_t STR_TIER_BIT_ADDR     = 1 << 2;  // addr housenumbers
constexpr uint8_t STR_TIER_BIT_POSTCODE = 1 << 3;  // postcode strings (any consumer)
constexpr uint8_t STR_TIER_BIT_POI      = 1 << 4;  // poi_records.name_id
// Strings of POI candidates no POI tier ships (tier > POI_MAX_SHIPPED_TIER).
constexpr uint8_t STR_TIER_BIT_UNSHIPPED = 1 << 5;
// string_home_tier for a string only unshipped records use: it isn't written
// (planet: 2.4M names, 53 MiB of strings_poi.bin no output referenced).
constexpr uint8_t STR_TIER_NONE = 0xFF;

constexpr size_t STR_TIER_COUNT = 5;
constexpr const char* STR_TIER_FILENAMES[STR_TIER_COUNT] = {
    "strings_core.bin",
    "strings_street.bin",
    "strings_addr.bin",
    "strings_postcode.bin",
    "strings_poi.bin",
};
constexpr const char* STR_TIER_NAMES[STR_TIER_COUNT] = {
    "core", "street", "addr", "postcode", "poi"
};
// The POI names' tier, the one tier not in alphabetical order (see
// order_poi_strings_by_record).
constexpr size_t STR_TIER_POI = 4;

// Home tier of a string from its consumer mask. The mode tiers nest by who
// downloads them (core: every mode; postcode: admin and up; street:
// no-addresses and up; addr: full only), so the most widely downloaded
// consumer tier serves every consumer. The poi tier ships with any mode, so a
// string a POI shares with anything outside core needs core.
inline uint8_t string_home_tier(uint8_t mask) {
    if (mask == STR_TIER_BIT_UNSHIPPED) return STR_TIER_NONE;
    mask &= static_cast<uint8_t>(~STR_TIER_BIT_UNSHIPPED);
    if (mask == 0 || (mask & STR_TIER_BIT_CORE)) return 0;
    if (mask & STR_TIER_BIT_POI) return (mask & ~STR_TIER_BIT_POI) ? 0 : 4;
    if (mask & STR_TIER_BIT_POSTCODE) return 3;
    if (mask & STR_TIER_BIT_STREET) return 1;
    return 2;
}

// Coordinates of one way within a shared buffer.
struct CoordSpan {
    const PackedLocation* first = nullptr;
    size_t count = 0;

    const PackedLocation* begin() const { return first; }
    const PackedLocation* end() const { return first + count; }
    size_t size() const { return count; }
    const PackedLocation& front() const { return first[0]; }
    const PackedLocation& back() const { return first[count - 1]; }
    const PackedLocation& operator[](size_t i) const { return first[i]; }
};

// The admin and POI relation member ways ring assembly reads, by way id,
// their coordinates back to back in one buffer.
struct WayGeometries {
    struct Way {
        size_t offset;  // into coords
        uint32_t count;
        int64_t first_node_id;
        int64_t last_node_id;
    };
    std::vector<PackedLocation> coords;
    std::unordered_map<int64_t, Way> ways;

    const Way* find(int64_t way_id) const {
        auto it = ways.find(way_id);
        return it == ways.end() ? nullptr : &it->second;
    }
    CoordSpan coords_of(const Way& way) const { return {coords.data() + way.offset, way.count}; }
};

// --- Parsed data container ---

struct ParsedData {
    StringPool string_pool;
    std::vector<WayHeader> ways;
    // Stable identity per way, parallel to `ways`. The IdAllocator
    // (strategy 2) consumes these alongside the previous build's
    // sidecar to assign the same dense way_id to each osm_way_id
    // across builds — eliminating cascade-shift in addr_points'
    // parent_way_id, poi_records' parent_street_id, cell entries,
    // and so on. Build-time only; not written to user-facing files.
    std::vector<int64_t> way_osm_ids;
    // Filled by apply_strategy2_remaps after the remap+reorder pass.
    // Indexed by the post-remap dense way_id; tombstone entries (slots
    // owned by a previous-build way that no longer exists in this
    // build) have object_type=NONE + flags|=1. write_index emits these
    // as the street_ways.osm_ids sidecar. Empty when strategy 2
    // wasn't applied (e.g. fresh build, no prev_dir) — in that case
    // write_index falls back to deriving slots from way_osm_ids.
    // Filled by apply_strategy2_remaps; moved out of the IdAllocator's
    // internal table with no copy. write_index emits this directly.
    std::vector<gc::id_alloc::SidecarSlot> way_sidecar_blob;
    std::vector<NodeCoord> street_nodes;
    std::vector<AddrPoint> addr_points;
    // Parallel to addr_points. Packed: top 8 bits = ObjectType
    // (OSM_NODE for node-sourced, OSM_WAY for closed-way buildings,
    // SYNTHETIC for TIGER imports). Bottom 56 bits = osm id (or
    // synthetic hash). Strategy-2 IdAllocator uses this.
    std::vector<uint64_t> addr_osm_ids;
    std::vector<NodeCoord> addr_vertices;  // polygon vertices for building addr_points
    // Street cell of each addr point, parallel to addr_points until a parse
    // turns it into sorted_addr_cells.
    std::vector<uint64_t> addr_cells;
    std::vector<InterpWay> interp_ways;
    // Parallel to interp_ways: packed (OSM_WAY, osm_way_id).
    std::vector<uint64_t> interp_osm_ids;
    // Parallel to interp_ways: postcode string offset for TIGER-sourced
    // segments (the CSV's per-segment ZIP), NO_DATA for OSM interpolations.
    // Empty when the build has no TIGER data — interp_postcodes.bin is then
    // not written. Mirrors Nominatim's location_property_tiger.postcode.
    std::vector<uint32_t> interp_postcode_ids;
    std::vector<NodeCoord> interp_nodes;
    std::vector<AdminPolygon> admin_polygons;
    // Parallel to admin_polygons. Packed (ObjectType<<56 | 56-bit id):
    // OSM_RELATION + (relation_id<<16 | ring_index) for relation-sourced
    // polygons, OSM_WAY + way_id for closed-way polygons (stable ids since
    // #105). Strategy-2 IdAllocator keys on this.
    std::vector<uint64_t> admin_osm_ids;
    // A continent's admin polygons' planet ids, parallel to admin_polygons
    // (NO_DATA for a strategy-2 tombstone); empty for the planet. Its
    // quality variants take each polygon from the planet's.
    std::vector<uint32_t> admin_planet_ids;
    std::vector<gc::id_alloc::SidecarSlot> admin_sidecar_blob;
    std::vector<gc::id_alloc::SidecarSlot> addr_sidecar_blob;
    std::vector<gc::id_alloc::SidecarSlot> place_sidecar_blob;
    std::vector<gc::id_alloc::SidecarSlot> poi_sidecar_blob;
    std::vector<gc::id_alloc::SidecarSlot> interp_sidecar_blob;
    std::vector<NodeCoord> admin_vertices;
    // The builder's own admin candidates per cell: the S2 covering of each
    // polygon's ring, which the parent and country passes (and the continent
    // split's polygon set) read.
    std::unordered_map<uint64_t, std::vector<uint32_t>> cell_to_admin;
    // The admin cell index the reader reads (admin_cells.bin), in canonical
    // order: (cell, poly_id | INTERIOR_FLAG) for every cell from which any
    // written variant's ring of the polygon contains a point (see
    // compute_admin_index_cells), and admin-minimal's alike for its rings.
    std::vector<CellItemPair> admin_index_cells;
    std::vector<CellItemPair> admin_minimal_index_cells;

    // Deferred work for parallel S2 computation (ways + interps)
    std::vector<DeferredWay> deferred_ways;
    std::vector<DeferredInterp> deferred_interps;

    // Sorted (cell_id, item_id) pairs — kept for direct entry writing
    std::vector<CellItemPair> sorted_way_cells;
    std::vector<CellItemPair> sorted_addr_cells;
    std::vector<CellItemPair> sorted_interp_cells;

    // Collected data for parallel admin assembly
    std::vector<CollectedRelation> collected_relations;
    WayGeometries way_geometries;

    // Place nodes (settlements)
    std::vector<PlaceNode> place_nodes;
    // Parallel: packed (OSM_NODE, osm_node_id). Strategy-2 IdAllocator uses this.
    std::vector<uint64_t> place_osm_ids;
    std::vector<CellItemPair> sorted_place_cells;

    // POI data
    std::vector<PoiRecord> poi_records;
    // Parallel: packed (object_type, osm_id). object_type ∈ {OSM_NODE,
    // OSM_WAY, OSM_RELATION} since POIs come from any of three OSM
    // entity kinds. Strategy-2 IdAllocator uses this.
    std::vector<uint64_t> poi_osm_ids;
    std::vector<NodeCoord> poi_vertices;
    std::vector<CellItemPair> sorted_poi_cells;
    std::vector<CollectedPoiRelation> collected_poi_relations;

    // Parent chain: per-way and per-polygon parent IDs for the
    // Nominatim-style address walk. Parallel arrays indexed by
    // way_id / polygon_id respectively.
    std::vector<uint32_t> way_orig_name_ids;  // way → original name (before name:en pref), build-time only
    std::vector<uint32_t> way_parent_ids;    // way → smallest containing admin poly
    std::vector<uint32_t> admin_parent_ids;  // poly → next-larger containing admin poly
    std::vector<uint32_t> way_postcode_ids;  // way → postcode string from containing postal boundary
    std::vector<uint32_t> addr_postcode_ids; // per-addr_point postcode (optional separate file)

    // External postcode centroids (TIGER segments, GeoNames rows) keyed by
    // postcode_key(country, postcode string id), like Nominatim's
    // location_postcode: the same string in two countries is two postcodes
    // (US ZIP 01069 is not Dresden's 01069). OSM addr:postcode centroids are
    // re-accumulated per country from the addr points at write time, so
    // they are not collected here.
    // Sums are 1e7-scaled integers, not doubles: contributions arrive from
    // dynamically-scheduled parse workers, so the merge's addition order
    // varies run-to-run and double sums wobble in the last ULP — enough to
    // make the postcode files nondeterministic across identical builds.
    // Integer addition is order-independent; 1e-7° ≈ 1 cm matches OSM's
    // native coordinate precision.
    struct PostcodeAccum {
        int64_t sum_lat_e7 = 0; int64_t sum_lng_e7 = 0; uint64_t count = 0;
        void add(double lat, double lng) {
            sum_lat_e7 += llround(lat * 1e7);
            sum_lng_e7 += llround(lng * 1e7);
            count++;
        }
        double lat() const { return (sum_lat_e7 / 1e7) / static_cast<double>(count); }
        double lng() const { return (sum_lng_e7 / 1e7) / static_cast<double>(count); }
    };
    std::unordered_map<uint64_t, PostcodeAccum> postcode_accum;

    // Boundaries' label/wikidata claims on place nodes (build-time only,
    // consumed by link_places_by_name to hide the nodes like Nominatim's
    // linked_place_id and name their claimant); filled from parallel
    // relation workers under the mutex.
    std::vector<PlaceClaim> place_claims;
    std::unique_ptr<std::mutex> linked_pn_mutex = std::make_unique<std::mutex>();

    // Post-canonical-sort: strings partitioned into per-consumer tiers.
    // Global offset space is contiguous: tier N occupies
    // [strings_tier_bases[N], strings_tier_bases[N+1]). Record name_ids
    // are global offsets.  string_pool.data() is rebuilt as the tiers
    // back to back, so a global offset still resolves into it.
    std::array<std::vector<char>, STR_TIER_COUNT> strings_tiers;
    std::array<uint32_t, STR_TIER_COUNT + 1> strings_tier_bases{};
    // Per POI tier t, the bytes at the head of the POI string tier that hold
    // every name a POI of tier <= t uses (order_poi_strings_by_record puts
    // them first; the whole tier until it runs).
    std::array<uint32_t, POI_MAX_SHIPPED_TIER + 1> poi_tier_string_ends{};

    // Look up a string by its global offset (post-partition).  Returns
    // nullptr if the offset is NO_DATA or out of range.
    const char* get_string(uint32_t off) const {
        if (off == NO_DATA) return nullptr;
        for (size_t t = 0; t < STR_TIER_COUNT; t++) {
            if (off < strings_tier_bases[t + 1]) {
                uint32_t local = off - strings_tier_bases[t];
                return strings_tiers[t].data() + local;
            }
        }
        return nullptr;
    }
};

// Frees v's storage now (clear() keeps it).
template <class V>
inline void free_storage(V& v) {
    v = V();
}

// Makes room for n more elements in one step. Capacity doubles past the new
// size, as push_back growth would leave it, so the appends that follow a
// merge (the next merge, POI relations) grow v in place instead of copying
// it whole; capacity never written costs no memory.
template <class T>
inline void reserve_for(std::vector<T>& v, size_t n) {
    if (v.size() + n > v.capacity()) v.reserve(2 * (v.size() + n));
}

// Grows v by n elements for a fill in place; returns the first new index.
template <class T>
inline size_t grow_by(std::vector<T>& v, size_t n) {
    size_t at = v.size();
    reserve_for(v, n);
    parallel_resize(v, at + n);
    return at;
}

// Grows addr_points and the arrays parallel to it by n points, which
// put_addr_point then fills (from many threads at once); returns the first.
inline size_t grow_addr_points(ParsedData& data, size_t n) {
    // put_addr_point writes one index into all four arrays.
    const size_t size = data.addr_points.size();
    if (data.addr_osm_ids.size() != size || data.addr_postcode_ids.size() != size || data.addr_cells.size() != size)
        throw std::logic_error("addr_points and its parallel arrays differ in length");
    grow_by(data.addr_osm_ids, n);
    grow_by(data.addr_postcode_ids, n);
    grow_by(data.addr_cells, n);
    return grow_by(data.addr_points, n);
}

// Stores address point i, its strings already interned (NO_DATA for a
// missing street or postcode); `cell` is point_to_cell() of its location. A
// building's polygon already sits at addr_vertices[vertex_offset].
inline void put_addr_point(ParsedData& data, size_t i, float lat, float lng,
                           uint32_t housenumber_id, uint32_t street_id, uint32_t postcode_id,
                           uint64_t cell, uint64_t osm_id_packed,
                           uint32_t vertex_offset = NO_DATA, uint32_t vertex_count = 0) {
    // parent_way_id is filled in during the nearest-street sweep.
    data.addr_points[i] = {lat, lng, housenumber_id, street_id, NO_DATA, vertex_offset, vertex_count};
    data.addr_osm_ids[i] = osm_id_packed;
    data.addr_postcode_ids[i] = postcode_id;
    data.addr_cells[i] = cell;
}

// Offset -> string index over a pool of NUL-terminated strings: a bit per
// pool byte where a string starts. Marking and remapping visit every string
// reference of the planet (~1e9). A pool under 4 GiB holds fewer than 2^32
// strings.
inline RankedBits string_starts(const std::vector<char>& pool, unsigned threads = 0) {
    const char* p = pool.data();
    const size_t size = pool.size();
    return RankedBits(size, [&](size_t b) {
        uint64_t bits = 0;
        for (size_t off = b * 64, end = std::min(size, off + 64); off < end; off++)
            if (off == 0 || p[off - 1] == '\0') bits |= uint64_t(1) << (off & 63);
        return bits;
    }, threads);
}

// Partition the string pool in `data` into 5 per-consumer tiers, sorting
// alphabetically within each (order_poi_strings_by_record later lays the POI
// tier out by record), and remap all record name_id fields to the
// new globally-contiguous offset space. Used by both the canonical sort
// step (applied to the full planet pool) and continent filter (applied
// per-continent subset after a flat pool is rebuilt).  Each string goes
// to string_home_tier of its consumer mask.  On
// return, `data.strings_tiers` + `data.strings_tier_bases` are populated
// and `data.string_pool.mutable_data()` is replaced with a concat view
// of the tier buffers (so legacy `string_pool.data().data() + off` reads
// continue to work for global offsets).
inline void partition_strings_into_tiers(ParsedData& data, unsigned threads = 0) {
    auto& pool_data = data.string_pool.mutable_data();
    if (pool_data.empty()) {
        data.strings_tiers = {};
        data.strings_tier_bases = {};
        return;
    }
    const char* pool = pool_data.data();
    const RankedBits starts = string_starts(pool_data, threads);
    const size_t n = starts.count();

    std::vector<uint8_t> tier(n);
    {
        std::vector<std::atomic<uint8_t>> mask(n);
        auto mark = [&](uint32_t off, uint8_t bit) {
            if (off == NO_DATA) return;
            size_t s = starts.index(off);
            if (s == RankedBits::npos) return;
            // Load first: a hot string (house number "1") would otherwise
            // bounce its cache line between every core.
            if (!(mask[s].load(std::memory_order_relaxed) & bit))
                mask[s].fetch_or(bit, std::memory_order_relaxed);
        };
        auto mark_each = [&](size_t count, auto&& mark_one) {
            parallel_for(count, [&](size_t b, size_t e, unsigned) {
                for (size_t i = b; i < e; i++) mark_one(i);
            }, threads);
        };
        auto shipped = [](const PoiRecord& pr) { return pr.tier <= POI_MAX_SHIPPED_TIER; };

        mark_each(data.admin_polygons.size(), [&](size_t i) { mark(data.admin_polygons[i].name_id, STR_TIER_BIT_CORE); });
        mark_each(data.place_nodes.size(), [&](size_t i) { mark(data.place_nodes[i].name_id, STR_TIER_BIT_CORE); });

        mark_each(data.ways.size(), [&](size_t i) { mark(data.ways[i].name_id, STR_TIER_BIT_STREET); });
        mark_each(data.way_orig_name_ids.size(), [&](size_t i) { mark(data.way_orig_name_ids[i], STR_TIER_BIT_STREET); });
        mark_each(data.addr_points.size(), [&](size_t i) {
            mark(data.addr_points[i].street_id, STR_TIER_BIT_STREET);
            mark(data.addr_points[i].housenumber_id, STR_TIER_BIT_ADDR);
        });
        mark_each(data.interp_ways.size(), [&](size_t i) { mark(data.interp_ways[i].street_id, STR_TIER_BIT_STREET); });

        mark_each(data.way_postcode_ids.size(), [&](size_t i) { mark(data.way_postcode_ids[i], STR_TIER_BIT_POSTCODE); });
        mark_each(data.interp_postcode_ids.size(), [&](size_t i) { mark(data.interp_postcode_ids[i], STR_TIER_BIT_POSTCODE); });
        mark_each(data.addr_postcode_ids.size(), [&](size_t i) { mark(data.addr_postcode_ids[i], STR_TIER_BIT_POSTCODE); });
        for (const auto& [key, _acc] : data.postcode_accum) mark(postcode_key_pc(key), STR_TIER_BIT_POSTCODE);

        mark_each(data.poi_records.size(), [&](size_t i) {
            const auto& pr = data.poi_records[i];
            mark(pr.parent_street_id, shipped(pr) ? STR_TIER_BIT_STREET : STR_TIER_BIT_UNSHIPPED);
            mark(pr.parent_postcode_id, shipped(pr) ? STR_TIER_BIT_POSTCODE : STR_TIER_BIT_UNSHIPPED);
            mark(pr.name_id, shipped(pr) ? STR_TIER_BIT_POI : STR_TIER_BIT_UNSHIPPED);
        });

        parallel_for(n, [&](size_t b, size_t e, unsigned) {
            for (size_t s = b; s < e; s++) tier[s] = string_home_tier(mask[s].load(std::memory_order_relaxed));
        }, threads);
    }

    // Each tier's string offsets in alphabetical order. Strings only
    // unshipped records use aren't written; their references become NO_DATA
    // below like any offset missing from the layout.
    std::array<std::vector<uint32_t>, STR_TIER_COUNT> sorted;
    {
        const std::vector<uint32_t> offsets = starts.ones(threads);
        // The sort orders each string's first 8 bytes beside its offset, so
        // most comparisons never reach into the pool. Big-endian, the bytes
        // compare as strcmp's unsigned chars; equal heads that hold the
        // terminator are equal strings.
        struct StrKey {
            uint64_t head;
            uint32_t off;
        };
        auto key_less = [pool](const StrKey& a, const StrKey& b) {
            if (a.head != b.head) return a.head < b.head;
            return (a.head & 0xFF) != 0 && strcmp(pool + a.off + 8, pool + b.off + 8) < 0;
        };
        std::vector<StrKey> keys;
        bool unique = true;
        for (size_t t = 0; t < STR_TIER_COUNT && unique; t++) {
            auto& list = sorted[t];
            list = parallel_filter(n, [&](size_t s) { return tier[s] == t; }, threads);
            keys.resize(list.size());
            parallel_for(list.size(), [&](size_t b, size_t e, unsigned) {
                for (size_t k = b; k < e; k++) {
                    const uint32_t off = offsets[list[k]];
                    uint64_t head = 0;
                    for (int c = 0; c < 8 && pool[off + c]; c++)
                        head |= uint64_t(static_cast<unsigned char>(pool[off + c])) << (56 - 8 * c);
                    keys[k] = {head, off};
                }
            }, threads);
            parallel_sort(keys.begin(), keys.end(), key_less, threads);
            unique = !parallel_any(keys.size() > 1 ? keys.size() - 1 : 0, [&](size_t k) {
                return !key_less(keys[k], keys[k + 1]);
            }, threads);
            parallel_for(list.size(), [&](size_t b, size_t e, unsigned) {
                for (size_t k = b; k < e; k++) list[k] = keys[k].off;
            }, threads);
        }
        keys = {};
        if (!unique) {
            // Interning keeps the pool unique, but should a string repeat,
            // std::sort's tie order is part of the layout: sort exactly as
            // the serial pass always has.
            std::vector<uint32_t> all = parallel_filter(n, [&](size_t s) { return tier[s] != STR_TIER_NONE; }, threads);
            for (auto& s : all) s = offsets[s];
            auto tier_of = [&](uint32_t off) { return tier[starts.index(off)]; };
            std::sort(all.begin(), all.end(), [&](uint32_t a, uint32_t b) {
                uint8_t ta = tier_of(a), tb = tier_of(b);
                if (ta != tb) return ta < tb;
                return strcmp(pool + a, pool + b) < 0;
            });
            auto at = all.begin();
            for (size_t t = 0; t < STR_TIER_COUNT; t++) {
                auto end = std::find_if(at, all.end(), [&](uint32_t off) { return tier_of(off) != t; });
                sorted[t].assign(at, end);
                at = end;
            }
        }
    }

    std::array<std::vector<char>, STR_TIER_COUNT> tier_bufs;
    std::array<uint32_t, STR_TIER_COUNT + 1> bases{};
    std::vector<uint32_t> new_off(n);
    for (size_t t = 0; t < STR_TIER_COUNT; t++) {
        const auto& list = sorted[t];
        auto at = parallel_offsets<size_t>(list.size(), [&](size_t k) { return strlen(pool + list[k]) + 1; }, threads);
        tier_bufs[t].resize(at.back());
        parallel_for(list.size(), [&](size_t b, size_t e, unsigned) {
            for (size_t k = b; k < e; k++) {
                std::memcpy(tier_bufs[t].data() + at[k], pool + list[k], at[k + 1] - at[k]);
                new_off[starts.index(list[k])] = bases[t] + static_cast<uint32_t>(at[k]);
            }
        }, threads);
        bases[t + 1] = bases[t] + static_cast<uint32_t>(at.back());
    }
    const uint32_t global_off = bases[STR_TIER_COUNT];

    data.strings_tiers = std::move(tier_bufs);
    data.strings_tier_bases = bases;
    data.poi_tier_string_ends.fill(static_cast<uint32_t>(data.strings_tiers[STR_TIER_POI].size()));

    // Rebuild pool_data as concat-of-tiers so legacy reads (pool_data.data()
    // + global_offset) still resolve correctly. Cheap vs. updating every
    // call site.
    pool_data.clear();
    pool_data.reserve(global_off);
    for (size_t t = 0; t < STR_TIER_COUNT; t++) {
        pool_data.insert(pool_data.end(),
            data.strings_tiers[t].begin(), data.strings_tiers[t].end());
    }

    auto new_offset = [&](uint32_t off) -> uint32_t {
        if (off == NO_DATA) return NO_DATA;
        size_t s = starts.index(off);
        return s == RankedBits::npos || tier[s] == STR_TIER_NONE ? NO_DATA : new_off[s];
    };
    auto remap_each = [&](size_t count, auto&& remap_one) {
        parallel_for(count, [&](size_t b, size_t e, unsigned) {
            for (size_t i = b; i < e; i++) remap_one(i);
        }, threads);
    };
    auto remap_vec = [&](std::vector<uint32_t>& v) {
        remap_each(v.size(), [&](size_t i) { v[i] = new_offset(v[i]); });
    };
    remap_each(data.ways.size(), [&](size_t i) { data.ways[i].name_id = new_offset(data.ways[i].name_id); });
    data.way_orig_name_ids = {};
    remap_each(data.addr_points.size(), [&](size_t i) {
        auto& a = data.addr_points[i];
        a.housenumber_id = new_offset(a.housenumber_id);
        a.street_id = new_offset(a.street_id);
    });
    remap_each(data.interp_ways.size(), [&](size_t i) {
        data.interp_ways[i].street_id = new_offset(data.interp_ways[i].street_id);
    });
    remap_each(data.admin_polygons.size(), [&](size_t i) {
        data.admin_polygons[i].name_id = new_offset(data.admin_polygons[i].name_id);
    });
    remap_each(data.poi_records.size(), [&](size_t i) {
        auto& pr = data.poi_records[i];
        pr.name_id = new_offset(pr.name_id);
        pr.parent_street_id = new_offset(pr.parent_street_id);
        pr.parent_postcode_id = new_offset(pr.parent_postcode_id);
    });
    remap_each(data.place_nodes.size(), [&](size_t i) {
        data.place_nodes[i].name_id = new_offset(data.place_nodes[i].name_id);
    });
    remap_vec(data.way_postcode_ids);
    remap_vec(data.addr_postcode_ids);
    remap_vec(data.interp_postcode_ids);
    {
        // Re-keys the nodes rather than copying them: a planet's worth of
        // node allocations and frees on one core takes seconds.
        auto& accum = data.postcode_accum;
        std::unordered_map<uint64_t, ParsedData::PostcodeAccum> remapped;
        remapped.reserve(accum.size());
        for (auto it = accum.begin(); it != accum.end();) {
            auto node = accum.extract(it++);
            uint32_t pc = new_offset(postcode_key_pc(node.key()));
            if (pc == NO_DATA) continue;
            node.key() = postcode_key(postcode_key_cc(node.key()), pc);
            auto placed = remapped.insert(std::move(node));
            if (!placed.inserted) placed.position->second = placed.node.mapped();
        }
        accum = std::move(remapped);
    }
}

// Lays the POI tier out in POI record order, so a reader walking the records
// (the place search build) reads their names front to back instead of all
// over the tier. A name's home is the lowest POI tier of a shipped record
// using it; the names of each home tier sit together, lowest tier first, each
// where the first record of its home tier using it sits in the records'
// final order. So the names of the POIs of tier <= t are the head of the
// tier (poi_tier_string_ends[t]). Run once the records stop moving (strategy
// 2). Each name keeps its one copy and the tier its size, so no offset outside
// the tier moves; a name no shipped record uses (none in a build) goes last,
// in its old order.
inline void order_poi_strings_by_record(ParsedData& data, unsigned threads = 0) {
    std::vector<char>& tier = data.strings_tiers[STR_TIER_POI];
    if (tier.empty()) return;

    const uint32_t base = data.strings_tier_bases[STR_TIER_POI];
    std::vector<char>& pool = data.string_pool.mutable_data();
    if (pool.size() != data.strings_tier_bases[STR_TIER_COUNT] ||
        base + tier.size() != data.strings_tier_bases[STR_TIER_POI + 1])
        throw std::logic_error("order_poi_strings_by_record: the pool is not the tiers back to back");

    const RankedBits starts = string_starts(tier, threads);
    const size_t n = starts.count();
    auto name_of = [&](uint32_t off) {
        return off == NO_DATA || off < base ? RankedBits::npos : starts.index(off - base);
    };
    std::vector<PoiRecord>& records = data.poi_records;

    std::vector<std::atomic<uint8_t>> home(n);
    std::vector<std::atomic<uint32_t>> first(n);
    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        for (size_t s = b; s < e; s++) {
            home[s].store(0xFF, std::memory_order_relaxed);
            first[s].store(NO_DATA, std::memory_order_relaxed);
        }
    }, threads);
    auto shipped_name = [&](size_t i) {
        return records[i].tier <= POI_MAX_SHIPPED_TIER ? name_of(records[i].name_id) : RankedBits::npos;
    };
    parallel_for(records.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) {
            const size_t s = shipped_name(i);
            if (s == RankedBits::npos) continue;
            uint8_t seen = home[s].load(std::memory_order_relaxed);
            while (records[i].tier < seen && !home[s].compare_exchange_weak(seen, records[i].tier, std::memory_order_relaxed)) {}
        }
    }, threads);
    parallel_for(records.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) {
            const size_t s = shipped_name(i);
            if (s == RankedBits::npos || records[i].tier != home[s].load(std::memory_order_relaxed)) continue;
            uint32_t seen = first[s].load(std::memory_order_relaxed);
            while (i < seen && !first[s].compare_exchange_weak(seen, static_cast<uint32_t>(i), std::memory_order_relaxed)) {}
        }
    }, threads);
    std::vector<uint32_t> order;
    std::array<size_t, POI_MAX_SHIPPED_TIER + 1> group_end{};
    for (uint8_t t = 0; t <= POI_MAX_SHIPPED_TIER; t++) {
        const std::vector<uint32_t> group = parallel_collect<uint32_t>(records.size(), [&](size_t i, std::vector<uint32_t>& out) {
            const size_t s = name_of(records[i].name_id);
            if (s != RankedBits::npos && first[s].load(std::memory_order_relaxed) == i &&
                home[s].load(std::memory_order_relaxed) == t)
                out.push_back(static_cast<uint32_t>(s));
        }, threads);
        order.insert(order.end(), group.begin(), group.end());
        group_end[t] = order.size();
    }
    const std::vector<uint32_t> unused =
        parallel_filter(n, [&](size_t s) { return first[s].load(std::memory_order_relaxed) == NO_DATA; }, threads);
    order.insert(order.end(), unused.begin(), unused.end());
    free_storage(first);
    free_storage(home);

    const std::vector<uint32_t> old_at = starts.ones(threads);
    const std::vector<uint32_t> at = parallel_offsets<uint32_t>(n, [&](size_t k) {
        const uint32_t s = order[k];
        return (s + 1 < n ? old_at[s + 1] : static_cast<uint32_t>(tier.size())) - old_at[s];
    }, threads);
    std::vector<char> laid;
    parallel_resize(laid, tier.size());
    std::vector<uint32_t> new_off(n);
    parallel_for(n, [&](size_t b, size_t e, unsigned) {
        for (size_t k = b; k < e; k++) {
            std::memcpy(laid.data() + at[k], tier.data() + old_at[order[k]], at[k + 1] - at[k]);
            new_off[order[k]] = base + at[k];
        }
    }, threads);
    parallel_for(records.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) {
            const size_t s = name_of(records[i].name_id);
            if (s != RankedBits::npos) records[i].name_id = new_off[s];
        }
    }, threads);
    std::memcpy(pool.data() + base, laid.data(), laid.size());
    tier = std::move(laid);
    for (size_t t = 0; t < POI_MAX_SHIPPED_TIER; t++) data.poi_tier_string_ends[t] = at[group_end[t]];
    data.poi_tier_string_ends[POI_MAX_SHIPPED_TIER] = static_cast<uint32_t>(tier.size());
}

// The POI string tier as the dir of the POIs of tier <= max_tier ships it:
// the head that holds their names.
inline std::string_view poi_dir_strings(const ParsedData& data, uint8_t max_tier) {
    const std::vector<char>& tier = data.strings_tiers[STR_TIER_POI];
    return {tier.data(), std::min<size_t>(data.poi_tier_string_ends.at(max_tier), tier.size())};
}

// --- Deduplicate IDs per cell ---

// Every cell of a cell map with its id list, in bucket order. The buckets are
// walked on every core: following the map's node list is a cache miss per
// cell on one core, most of a minute for the planet's addr cells.
template<typename Map>
inline std::vector<std::pair<uint64_t, std::vector<uint32_t>*>> cell_lists(Map& cell_map, unsigned threads = 0) {
    using Cell = std::pair<uint64_t, std::vector<uint32_t>*>;
    return parallel_collect<Cell>(cell_map.bucket_count(), [&](size_t b, std::vector<Cell>& out) {
        for (auto it = cell_map.begin(b); it != cell_map.end(b); ++it) out.push_back({it->first, &it->second});
    }, threads);
}

// Sorts and dedups each cell's ids, on every core.
inline void deduplicate_lists(const std::vector<std::pair<uint64_t, std::vector<uint32_t>*>>& cells,
                              unsigned threads = 0) {
    parallel_for(cells.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t c = b; c < e; c++) {
            auto& ids = *cells[c].second;
            std::sort(ids.begin(), ids.end());
            ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        }
    }, threads);
}

template<typename Map>
inline void deduplicate(Map& cell_map, unsigned threads = 0) {
    deduplicate_lists(cell_lists(cell_map, threads), threads);
}

// (cell_id, item_id) pairs for items that each sit in one cell, cells[item],
// ordered by cell, then item.
inline std::vector<CellItemPair> sorted_item_cells(const std::vector<uint64_t>& cells, unsigned threads = 0) {
    std::vector<CellItemPair> pairs;
    parallel_resize(pairs, cells.size());
    parallel_for(cells.size(), [&](size_t b, size_t e, unsigned) {
        for (size_t i = b; i < e; i++) pairs[i] = {cells[i], static_cast<uint32_t>(i)};
    }, threads);
    // Items are unique, so the order is too.
    parallel_sort(pairs.begin(), pairs.end(), cell_item_less, threads);
    return pairs;
}
