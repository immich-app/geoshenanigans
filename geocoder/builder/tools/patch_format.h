#pragma once
// Shared definitions for the geocoder patch system.
// Used by geocoder-canonicalize, geocoder-diff, and geocoder-patch.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_set>
#include <vector>

// --- Memory-mapped file helpers ---

struct MappedFile { const char* data; size_t size; };

// mmap a file read-only (PROT_READ, MAP_PRIVATE). Pages paged in on demand.
inline MappedFile mmap_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return {nullptr, 0};
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    if (sz == 0) { close(fd); return {nullptr, 0}; }
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) return {nullptr, 0};
    return {static_cast<const char*>(p), sz};
}

// mmap a file with copy-on-write (PROT_READ|PROT_WRITE, MAP_PRIVATE).
// Writes create private copies of modified pages; unmodified pages share physical memory.
struct MappedFileRW { char* data; size_t size; };
inline MappedFileRW mmap_file_rw(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return {nullptr, 0};
    struct stat st; fstat(fd, &st);
    size_t sz = st.st_size;
    if (sz == 0) { close(fd); return {nullptr, 0}; }
    void* p = mmap(nullptr, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) return {nullptr, 0};
    return {static_cast<char*>(p), sz};
}

inline void unmap_file(MappedFile& f) {
    if (f.data) { munmap(const_cast<char*>(f.data), f.size); f.data = nullptr; f.size = 0; }
}
inline void unmap_file(MappedFileRW& f) {
    if (f.data) { munmap(f.data, f.size); f.data = nullptr; f.size = 0; }
}

// Detect stride from file size (avoids loading entire file)
inline size_t detect_stride_from_file(const std::string& path, std::initializer_list<size_t> candidates) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) return *candidates.begin();
    size_t sz = st.st_size;
    for (size_t s : candidates)
        if (sz % s == 0 && sz / s > 0) return s;
    return *candidates.begin();
}

// --- Binary record structs (must match types.h and server) ---

#pragma pack(push, 1)
struct PatchWayHeader {
    uint32_t node_offset;
    uint8_t node_count;
    uint32_t name_id;
};
static_assert(sizeof(PatchWayHeader) == 9, "WayHeader must be 9 bytes packed");

struct PatchAddrPoint {
    float lat;
    float lng;
    uint32_t housenumber_id;
    uint32_t street_id;
    uint32_t parent_way_id;
};
static_assert(sizeof(PatchAddrPoint) == 20, "AddrPoint must be 20 bytes");

struct PatchNodeCoord {
    float lat;
    float lng;
};
static_assert(sizeof(PatchNodeCoord) == 8, "NodeCoord must be 8 bytes");
#pragma pack(pop)

// InterpWay and AdminPolygon have compiler-dependent padding.
// Read them field-by-field instead of struct-casting.

struct PatchInterpWay {
    uint32_t node_offset;
    uint8_t node_count;
    uint32_t street_id;
    uint32_t start_number;
    uint32_t end_number;
    uint8_t interpolation;
};

struct PatchAdminPolygon {
    uint32_t vertex_offset;
    uint32_t vertex_count;
    uint32_t name_id;
    uint8_t admin_level;
    uint8_t place_type_override;
    float area;
    uint16_t country_code;
};

struct PatchPoiRecord {
    float lat;
    float lng;
    uint32_t vertex_offset;
    uint32_t vertex_count;
    uint32_t name_id;
    uint8_t category;
    uint8_t tier;
    uint8_t flags;
    uint8_t importance;
};

// --- File I/O helpers ---

inline std::vector<char> read_file(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) return {};
    auto size = f.tellg();
    f.seekg(0);
    std::vector<char> data(size);
    f.read(data.data(), size);
    return data;
}

inline bool write_file(const std::string& path, const char* data, size_t size) {
    std::ofstream f(path, std::ios::binary);
    if (!f) return false;
    f.write(data, size);
    return f.good();
}

inline bool write_file(const std::string& path, const std::vector<char>& data) {
    return write_file(path, data.data(), data.size());
}

inline const char* get_string(const std::vector<char>& pool, uint32_t offset) {
    if (offset >= pool.size()) return "";
    return pool.data() + offset;
}

// Read WayHeader array — detect stride from file size vs known struct sizes
inline std::vector<PatchWayHeader> read_ways(const std::string& path) {
    auto data = read_file(path);
    // Determine stride: builder writes sizeof(WayHeader) which may be 9 or 12
    // depending on compiler padding. Try 12 first (common), then 9.
    size_t stride = 12;
    if (data.size() % 12 != 0 || data.size() / 12 == 0) {
        stride = 9;
    }
    size_t count = data.size() / stride;
    std::vector<PatchWayHeader> result(count);
    for (size_t i = 0; i < count; i++) {
        const char* p = data.data() + i * stride;
        memcpy(&result[i].node_offset, p, 4);
        result[i].node_count = static_cast<uint8_t>(p[4]);
        // name_id is at offset 5 (packed) or 8 (padded to 4-byte alignment)
        size_t name_off = (stride == 12) ? 8 : 5;
        memcpy(&result[i].name_id, p + name_off, 4);
    }
    return result;
}

inline size_t detect_way_stride(const std::string& path) {
    auto data = read_file(path);
    return (data.size() % 12 == 0 && data.size() / 12 > 0) ? 12 : 9;
}

template<typename T>
inline std::vector<T> read_structs(const std::string& path) {
    auto data = read_file(path);
    size_t count = data.size() / sizeof(T);
    std::vector<T> result(count);
    if (count > 0) memcpy(result.data(), data.data(), count * sizeof(T));
    return result;
}

// Read InterpWay with field-by-field parsing to handle padding
inline std::vector<PatchInterpWay> read_interps(const std::string& path) {
    auto data = read_file(path);
    // Determine record size from file: try sizeof(InterpWay) as compiled by the builder
    // The builder uses the same struct, so sizeof should match.
    size_t rec_size = sizeof(PatchInterpWay);
    // But the builder's InterpWay has padding. Read with the builder's size.
    // From types.h the struct is: u32 + u8 + u32 + u32 + u32 + u8 = 18 bytes raw
    // but compiler pads to 20 bytes typically. Check by trying both.
    size_t count_20 = data.size() / 20;
    size_t count_18 = data.size() / 18;
    // Use whichever divides evenly
    size_t stride = (data.size() % 20 == 0 && count_20 > 0) ? 20 :
                    (data.size() % 18 == 0 && count_18 > 0) ? 18 : 20;
    size_t count = data.size() / stride;
    std::vector<PatchInterpWay> result(count);
    for (size_t i = 0; i < count; i++) {
        const char* p = data.data() + i * stride;
        memcpy(&result[i].node_offset, p, 4); p += 4;
        result[i].node_count = *reinterpret_cast<const uint8_t*>(p); p += 1;
        // Skip padding to align street_id
        if (stride == 20) p += 3; // 3 bytes padding after node_count
        memcpy(&result[i].street_id, p, 4); p += 4;
        memcpy(&result[i].start_number, p, 4); p += 4;
        memcpy(&result[i].end_number, p, 4); p += 4;
        result[i].interpolation = *reinterpret_cast<const uint8_t*>(p);
    }
    return result;
}

// Read AdminPolygon with field-by-field parsing
inline std::vector<PatchAdminPolygon> read_admin_polygons(const std::string& path) {
    auto data = read_file(path);
    // AdminPolygon: u32 + u32 + u32 + u8 + f32 + u16 = 19 raw bytes
    // Compiler likely pads to 20 or 24 bytes. Try common sizes.
    size_t stride = 0;
    for (size_t s : {24, 20, 19}) {
        if (data.size() % s == 0 && data.size() / s > 0) { stride = s; break; }
    }
    if (stride == 0) stride = 24; // fallback
    size_t count = data.size() / stride;
    std::vector<PatchAdminPolygon> result(count);
    for (size_t i = 0; i < count; i++) {
        const char* p = data.data() + i * stride;
        memcpy(&result[i].vertex_offset, p, 4); p += 4;
        memcpy(&result[i].vertex_count, p, 4); p += 4;
        memcpy(&result[i].name_id, p, 4); p += 4;
        result[i].admin_level = *reinterpret_cast<const uint8_t*>(p); p += 1;
        result[i].place_type_override = *reinterpret_cast<const uint8_t*>(p); p += 1;
        // remaining padding for area alignment
        if (stride >= 20) {
            size_t skip = (stride == 24) ? 2 : (stride == 20 ? 2 : 0);
            p += skip;
        }
        memcpy(&result[i].area, p, 4); p += 4;
        memcpy(&result[i].country_code, p, 2);
    }
    return result;
}

// Read PoiRecord with field-by-field parsing
inline std::vector<PatchPoiRecord> read_poi_records(const std::string& path) {
    auto data = read_file(path);
    size_t stride = 24;
    if (data.size() % stride != 0) return {};
    size_t count = data.size() / stride;
    std::vector<PatchPoiRecord> result(count);
    for (size_t i = 0; i < count; i++) {
        const char* p = data.data() + i * stride;
        memcpy(&result[i].lat, p, 4); p += 4;
        memcpy(&result[i].lng, p, 4); p += 4;
        memcpy(&result[i].vertex_offset, p, 4); p += 4;
        memcpy(&result[i].vertex_count, p, 4); p += 4;
        memcpy(&result[i].name_id, p, 4); p += 4;
        result[i].category = *reinterpret_cast<const uint8_t*>(p); p += 1;
        result[i].tier = *reinterpret_cast<const uint8_t*>(p); p += 1;
        result[i].flags = *reinterpret_cast<const uint8_t*>(p); p += 1;
        result[i].importance = *reinterpret_cast<const uint8_t*>(p);
    }
    return result;
}

// --- Patch file format ---

static constexpr char GCPATCH_MAGIC[8] = {'G','C','P','A','T','C','H','\0'};
static constexpr uint32_t GCPATCH_VERSION = 1;

enum class PatchFileId : uint32_t {
    STRINGS = 0,
    STREET_WAYS = 1,
    STREET_NODES = 2,
    ADDR_POINTS = 3,
    INTERP_WAYS = 4,
    INTERP_NODES = 5,
    ADMIN_POLYGONS = 6,
    ADMIN_VERTICES = 7,
    GEO_CELLS = 8,
    STREET_ENTRIES = 9,
    ADDR_ENTRIES = 10,
    INTERP_ENTRIES = 11,
    ADMIN_CELLS = 12,
    ADMIN_ENTRIES = 13,
    POI_RECORDS = 14,
    POI_VERTICES = 15,
    POI_CELLS = 16,
    POI_ENTRIES = 17,
    PLACE_NODES = 18,
    PLACE_CELLS = 19,
    PLACE_ENTRIES = 20,
    // Secondary parallel arrays + postcode/postal indexes. These don't
    // have record-level diff logic yet — the diff tool emits them as
    // full-replacement sections (stride=0) which the patch tool writes
    // verbatim. Patch cost is only the size of the changed files.
    ADDR_POSTCODES = 21,
    ADMIN_PARENTS = 22,
    WAY_PARENTS = 23,
    WAY_POSTCODES = 24,
    POSTCODE_CENTROIDS = 25,
    POSTCODE_CENTROID_CELLS = 26,
    POSTCODE_CENTROID_ENTRIES = 27,
    POSTAL_POLYGONS = 28,
    POSTAL_VERTICES = 29,
    COUNT = 30
};

static const char* patch_file_names[] = {
    "strings.bin", "street_ways.bin", "street_nodes.bin", "addr_points.bin",
    "interp_ways.bin", "interp_nodes.bin", "admin_polygons.bin", "admin_vertices.bin",
    "geo_cells.bin", "street_entries.bin", "addr_entries.bin", "interp_entries.bin",
    "admin_cells.bin", "admin_entries.bin",
    "poi_records.bin", "poi_vertices.bin", "poi_cells.bin", "poi_entries.bin",
    "place_nodes.bin", "place_cells.bin", "place_entries.bin",
    "addr_postcodes.bin", "admin_parents.bin", "way_parents.bin", "way_postcodes.bin",
    "postcode_centroids.bin", "postcode_centroid_cells.bin", "postcode_centroid_entries.bin",
    "postal_polygons.bin", "postal_vertices.bin"
};

// Encoding types for each section
enum class PatchEncoding : uint32_t {
    RAW_REPLACE = 0,  // Full replacement (zlib compressed)
    ZSTD_DELTA = 1,   // Single zstd delta frame (zstd --patch-from on remapped old)
};

// Patch file header
struct PatchHeader {
    char magic[8];
    uint32_t version;
    uint32_t flags;
    // Per-file sections follow
};

// Section header (one per file)
struct PatchSection {
    uint32_t file_id;       // PatchFileId
    uint32_t encoding;      // PatchEncoding
    uint64_t old_size;      // Expected size of old file
    uint64_t new_size;      // Size of reconstructed file
    uint64_t data_size;     // Size of compressed section data
};

// COPY_INSERT opcodes
enum : uint8_t {
    OP_COPY = 0,    // Copy from old file: [u8 tag][u64 offset][u32 length]
    OP_INSERT = 1,  // Insert new data:    [u8 tag][u32 length][data...]
};

// Offset fixup section marker: 0xFFFFFFFD
// Format: uint32_t marker, uint32_t file_id, uint32_t stride,
//         uint32_t count, [(uint32_t record_index, uint32_t new_offset_value)] * count
// Applied to byte offset 0 of each record (node_offset/vertex_offset field).

static constexpr uint32_t FIXUP_MARKER = 0xFFFFFFFD;

// Cell changes section marker: 0xFFFFFFFB
// Format: marker, num_added(u32), num_removed(u32),
//         [added_cell_id(u64)] * num_added, [removed_cell_id(u64)] * num_removed
// For both geo and admin cell sets.
static constexpr uint32_t CELL_CHANGES_GEO_MARKER = 0xFFFFFFFB;
static constexpr uint32_t CELL_CHANGES_ADMIN_MARKER = 0xFFFFFFFA;
static constexpr uint32_t CELL_CHANGES_POI_MARKER = 0xFFFFFFF5;
static constexpr uint32_t CELL_CHANGES_PLACE_MARKER = 0xFFFFFFF4;

// Entry correction marker: 0xFFFFFFF8
// Cell-level diff of entries: lists cells whose entries differ between derived and new.
// Format: marker(4), file_id(4), count(4),
//   for each: cell_index(4), entry_count(2), [id(4)] * entry_count
// cell_index is the position in the geo_cells/admin_cells array.
static constexpr uint32_t ENTRY_CORRECTION_MARKER = 0xFFFFFFF8;

// Cell flag corrections marker: 0xFFFFFFF9
// Format: marker, count(u32), [(cell_id:u64, flags:u8)] × count
// flags: bit 0 = has_street, bit 1 = has_addr, bit 2 = has_interp
static constexpr uint32_t CELL_FLAGS_MARKER = 0xFFFFFFF9;

// Secondary ID remap marker: 0xFFFFFFF6
// Additional old→new ID mappings for modified records (recovered by relaxed key matching).
// Both diff and patch tools must apply these to their derived remaps for consistent results.
// Format: marker(4), n_files(4),
//   for each: file_id(4), n_pairs(4), [(old_id:u32, new_id:u32)] × n_pairs
static constexpr uint32_t SECONDARY_REMAP_MARKER = 0xFFFFFFF6;

// --- Shared entry rebuild logic ---
// Used by both diff and patch tools to produce identical rebuilt entries.
// Takes old geo_cells + old entries + ID remap → produces rebuilt entries + geo_cells.

struct RebuiltGeo {
    std::vector<char> geo_cells_data;
    std::vector<char> street_entries_data;
    std::vector<char> addr_entries_data;
    std::vector<char> interp_entries_data;
};

inline RebuiltGeo rebuild_geo_from_remap(
    const std::vector<char>& old_geo,
    const std::vector<char>& old_se, const std::vector<char>& old_ae, const std::vector<char>& old_ie,
    const std::unordered_map<uint32_t,uint32_t>& way_rm,
    const std::unordered_map<uint32_t,uint32_t>& addr_rm,
    const std::unordered_map<uint32_t,uint32_t>& interp_rm,
    const std::vector<uint64_t>& added_cells = {},
    const std::vector<uint64_t>& removed_cells = {})
{
    size_t n_cells = old_geo.size() / 20;

    auto parse_entry = [](const std::vector<char>& data, uint32_t off) -> std::vector<uint32_t> {
        if (off == 0xFFFFFFFF || off + 2 > data.size()) return {};
        uint16_t count; memcpy(&count, data.data() + off, 2);
        if (off + 2 + count * 4 > data.size()) return {};
        std::vector<uint32_t> ids(count);
        memcpy(ids.data(), data.data() + off + 2, count * 4);
        return ids;
    };

    auto remap_ids = [](std::vector<uint32_t>& ids, const std::unordered_map<uint32_t,uint32_t>& rm) {
        for (auto& id : ids) {
            auto it = rm.find(id);
            if (it != rm.end()) id = it->second;
        }
        std::sort(ids.begin(), ids.end());
    };

    // Parse all cells and remap IDs
    struct CellData {
        uint64_t cell_id;
        std::vector<uint32_t> streets, addrs, interps;
    };
    std::vector<CellData> cells(n_cells);
    for (size_t i = 0; i < n_cells; i++) {
        memcpy(&cells[i].cell_id, old_geo.data() + i * 20, 8);
        uint32_t s_off, a_off, i_off;
        memcpy(&s_off, old_geo.data() + i * 20 + 8, 4);
        memcpy(&a_off, old_geo.data() + i * 20 + 12, 4);
        memcpy(&i_off, old_geo.data() + i * 20 + 16, 4);
        cells[i].streets = parse_entry(old_se, s_off);
        cells[i].addrs = parse_entry(old_ae, a_off);
        cells[i].interps = parse_entry(old_ie, i_off);
        remap_ids(cells[i].streets, way_rm);
        remap_ids(cells[i].addrs, addr_rm);
        remap_ids(cells[i].interps, interp_rm);
    }

    // Apply cell set changes (add new cells, remove old cells)
    if (!removed_cells.empty()) {
        std::unordered_set<uint64_t> removed_set(removed_cells.begin(), removed_cells.end());
        cells.erase(std::remove_if(cells.begin(), cells.end(),
            [&](const CellData& c) { return removed_set.count(c.cell_id); }), cells.end());
    }
    if (!added_cells.empty()) {
        for (uint64_t cid : added_cells) {
            CellData cd; cd.cell_id = cid;
            // New cell entry data is provided via the new_cell_entries parameter (if available)
            cells.push_back(cd);
        }
        // Re-sort to maintain cell_id order
        std::sort(cells.begin(), cells.end(),
            [](const CellData& a, const CellData& b) { return a.cell_id < b.cell_id; });
    }

    // Write rebuilt files
    RebuiltGeo result;
    uint32_t no_data = 0xFFFFFFFF;

    auto write_entries = [&](std::vector<char>& buf, const auto& getter) -> std::unordered_map<uint64_t, uint32_t> {
        std::unordered_map<uint64_t, uint32_t> offsets;
        for (auto& c : cells) {
            const auto& ids = getter(c);
            if (ids.empty()) continue;
            offsets[c.cell_id] = static_cast<uint32_t>(buf.size());
            uint16_t count = static_cast<uint16_t>(ids.size());
            buf.insert(buf.end(), (const char*)&count, (const char*)&count + 2);
            buf.insert(buf.end(), (const char*)ids.data(), (const char*)ids.data() + ids.size() * 4);
        }
        return offsets;
    };

    auto s_off = write_entries(result.street_entries_data, [](const CellData& c) -> const std::vector<uint32_t>& { return c.streets; });
    auto a_off = write_entries(result.addr_entries_data, [](const CellData& c) -> const std::vector<uint32_t>& { return c.addrs; });
    auto i_off = write_entries(result.interp_entries_data, [](const CellData& c) -> const std::vector<uint32_t>& { return c.interps; });

    // Write geo_cells
    for (auto& c : cells) {
        result.geo_cells_data.insert(result.geo_cells_data.end(), (const char*)&c.cell_id, (const char*)&c.cell_id + 8);
        auto get = [&](const auto& m) -> uint32_t {
            auto it = m.find(c.cell_id); return it != m.end() ? it->second : no_data;
        };
        uint32_t sv = get(s_off), av = get(a_off), iv = get(i_off);
        result.geo_cells_data.insert(result.geo_cells_data.end(), (const char*)&sv, (const char*)&sv + 4);
        result.geo_cells_data.insert(result.geo_cells_data.end(), (const char*)&av, (const char*)&av + 4);
        result.geo_cells_data.insert(result.geo_cells_data.end(), (const char*)&iv, (const char*)&iv + 4);
    }

    return result;
}

// Vector-based overload: much faster for diff tool where IDs are dense sequential indices.
// remap[old_id] = new_id, or 0xFFFFFFFF if unmapped.
inline RebuiltGeo rebuild_geo_from_remap_vec(
    const std::vector<char>& old_geo,
    const std::vector<char>& old_se, const std::vector<char>& old_ae, const std::vector<char>& old_ie,
    const std::vector<uint32_t>& way_rm,
    const std::vector<uint32_t>& addr_rm,
    const std::vector<uint32_t>& interp_rm,
    const std::vector<uint64_t>& added_cells = {},
    const std::vector<uint64_t>& removed_cells = {})
{
    size_t n_cells = old_geo.size() / 20;
    auto parse_entry = [](const std::vector<char>& data, uint32_t off) -> std::vector<uint32_t> {
        if (off == 0xFFFFFFFF || off + 2 > data.size()) return {};
        uint16_t count; memcpy(&count, data.data() + off, 2);
        if (off + 2 + count * 4 > data.size()) return {};
        std::vector<uint32_t> ids(count);
        memcpy(ids.data(), data.data() + off + 2, count * 4);
        return ids;
    };
    auto remap_ids_vec = [](std::vector<uint32_t>& ids, const std::vector<uint32_t>& rm) {
        for (auto& id : ids)
            if (id < rm.size() && rm[id] != 0xFFFFFFFF) id = rm[id];
        std::sort(ids.begin(), ids.end());
    };
    struct CellData { uint64_t cell_id; std::vector<uint32_t> streets, addrs, interps; };
    std::vector<CellData> cells(n_cells);
    for (size_t i = 0; i < n_cells; i++) {
        memcpy(&cells[i].cell_id, old_geo.data() + i * 20, 8);
        uint32_t s_off, a_off, i_off;
        memcpy(&s_off, old_geo.data() + i * 20 + 8, 4);
        memcpy(&a_off, old_geo.data() + i * 20 + 12, 4);
        memcpy(&i_off, old_geo.data() + i * 20 + 16, 4);
        cells[i].streets = parse_entry(old_se, s_off);
        cells[i].addrs = parse_entry(old_ae, a_off);
        cells[i].interps = parse_entry(old_ie, i_off);
        remap_ids_vec(cells[i].streets, way_rm);
        remap_ids_vec(cells[i].addrs, addr_rm);
        remap_ids_vec(cells[i].interps, interp_rm);
    }
    if (!removed_cells.empty()) {
        std::unordered_set<uint64_t> rs(removed_cells.begin(), removed_cells.end());
        cells.erase(std::remove_if(cells.begin(), cells.end(), [&](const CellData& c) { return rs.count(c.cell_id); }), cells.end());
    }
    if (!added_cells.empty()) {
        for (uint64_t cid : added_cells) { CellData cd; cd.cell_id = cid; cells.push_back(cd); }
        std::sort(cells.begin(), cells.end(), [](const CellData& a, const CellData& b) { return a.cell_id < b.cell_id; });
    }
    RebuiltGeo result;
    uint32_t no_data = 0xFFFFFFFF;
    auto write_entries = [&](std::vector<char>& buf, const auto& getter) -> std::unordered_map<uint64_t, uint32_t> {
        std::unordered_map<uint64_t, uint32_t> offsets;
        for (auto& c : cells) {
            const auto& ids = getter(c);
            if (ids.empty()) continue;
            offsets[c.cell_id] = static_cast<uint32_t>(buf.size());
            uint16_t count = static_cast<uint16_t>(ids.size());
            buf.insert(buf.end(), (const char*)&count, (const char*)&count + 2);
            buf.insert(buf.end(), (const char*)ids.data(), (const char*)ids.data() + ids.size() * 4);
        }
        return offsets;
    };
    auto s_off = write_entries(result.street_entries_data, [](const CellData& c) -> const std::vector<uint32_t>& { return c.streets; });
    auto a_off = write_entries(result.addr_entries_data, [](const CellData& c) -> const std::vector<uint32_t>& { return c.addrs; });
    auto i_off = write_entries(result.interp_entries_data, [](const CellData& c) -> const std::vector<uint32_t>& { return c.interps; });
    for (auto& c : cells) {
        result.geo_cells_data.insert(result.geo_cells_data.end(), (const char*)&c.cell_id, (const char*)&c.cell_id + 8);
        auto get = [&](const auto& m) -> uint32_t { auto it = m.find(c.cell_id); return it != m.end() ? it->second : no_data; };
        uint32_t sv = get(s_off), av = get(a_off), iv = get(i_off);
        result.geo_cells_data.insert(result.geo_cells_data.end(), (const char*)&sv, (const char*)&sv + 4);
        result.geo_cells_data.insert(result.geo_cells_data.end(), (const char*)&av, (const char*)&av + 4);
        result.geo_cells_data.insert(result.geo_cells_data.end(), (const char*)&iv, (const char*)&iv + 4);
    }
    return result;
}

struct RebuiltAdmin {
    std::vector<char> admin_cells_data;
    std::vector<char> admin_entries_data;
};

inline RebuiltAdmin rebuild_admin_from_remap(
    const std::vector<char>& old_ac, const std::vector<char>& old_ae,
    const std::unordered_map<uint32_t,uint32_t>& admin_rm,
    const std::vector<uint64_t>& added_cells = {},
    const std::vector<uint64_t>& removed_cells = {})
{
    size_t n_cells = old_ac.size() / 12;

    struct CellData { uint64_t cell_id; std::vector<uint32_t> ids; };
    std::vector<CellData> cells(n_cells);
    for (size_t i = 0; i < n_cells; i++) {
        memcpy(&cells[i].cell_id, old_ac.data() + i * 12, 8);
        uint32_t off; memcpy(&off, old_ac.data() + i * 12 + 8, 4);
        if (off != 0xFFFFFFFF && off + 2 <= old_ae.size()) {
            uint16_t count; memcpy(&count, old_ae.data() + off, 2);
            if (off + 2 + count * 4 <= old_ae.size()) {
                cells[i].ids.resize(count);
                memcpy(cells[i].ids.data(), old_ae.data() + off + 2, count * 4);
                for (auto& id : cells[i].ids) {
                    uint32_t flags = id & 0x80000000u;
                    uint32_t masked = id & 0x7FFFFFFFu;
                    auto it = admin_rm.find(masked);
                    if (it != admin_rm.end()) id = it->second | flags;
                }
                std::sort(cells[i].ids.begin(), cells[i].ids.end());
            }
        }
    }

    // Apply cell changes (add/remove)
    if (!removed_cells.empty()) {
        std::unordered_set<uint64_t> removed_set(removed_cells.begin(), removed_cells.end());
        cells.erase(std::remove_if(cells.begin(), cells.end(),
            [&](const CellData& c) { return removed_set.count(c.cell_id); }), cells.end());
    }
    if (!added_cells.empty()) {
        for (uint64_t cid : added_cells) {
            CellData cd; cd.cell_id = cid;
            cells.push_back(cd);
        }
        std::sort(cells.begin(), cells.end(),
            [](const CellData& a, const CellData& b) { return a.cell_id < b.cell_id; });
    }

    RebuiltAdmin result;
    uint32_t no_data = 0xFFFFFFFF;
    std::unordered_map<uint64_t, uint32_t> offsets;
    for (auto& c : cells) {
        if (c.ids.empty()) continue;
        offsets[c.cell_id] = static_cast<uint32_t>(result.admin_entries_data.size());
        uint16_t count = static_cast<uint16_t>(c.ids.size());
        result.admin_entries_data.insert(result.admin_entries_data.end(), (const char*)&count, (const char*)&count + 2);
        result.admin_entries_data.insert(result.admin_entries_data.end(), (const char*)c.ids.data(), (const char*)c.ids.data() + c.ids.size() * 4);
    }
    for (auto& c : cells) {
        result.admin_cells_data.insert(result.admin_cells_data.end(), (const char*)&c.cell_id, (const char*)&c.cell_id + 8);
        auto it = offsets.find(c.cell_id);
        uint32_t off = it != offsets.end() ? it->second : no_data;
        result.admin_cells_data.insert(result.admin_cells_data.end(), (const char*)&off, (const char*)&off + 4);
    }
    return result;
}

// POI cell index rebuild (same structure as admin: 12-byte stride, cell_id(u64) + entry_offset(u32))
struct RebuiltPoi {
    std::vector<char> poi_cells_data;
    std::vector<char> poi_entries_data;
};

inline RebuiltPoi rebuild_poi_from_remap(
    const std::vector<char>& old_pc, const std::vector<char>& old_pe,
    const std::unordered_map<uint32_t,uint32_t>& poi_rm,
    const std::vector<uint64_t>& added_cells = {},
    const std::vector<uint64_t>& removed_cells = {})
{
    size_t n_cells = old_pc.size() / 12;

    struct CellData { uint64_t cell_id; std::vector<uint32_t> ids; };
    std::vector<CellData> cells(n_cells);
    for (size_t i = 0; i < n_cells; i++) {
        memcpy(&cells[i].cell_id, old_pc.data() + i * 12, 8);
        uint32_t off; memcpy(&off, old_pc.data() + i * 12 + 8, 4);
        if (off != 0xFFFFFFFF && off + 2 <= old_pe.size()) {
            uint16_t count; memcpy(&count, old_pe.data() + off, 2);
            if (off + 2 + count * 4 <= old_pe.size()) {
                cells[i].ids.resize(count);
                memcpy(cells[i].ids.data(), old_pe.data() + off + 2, count * 4);
                for (auto& id : cells[i].ids) {
                    auto it = poi_rm.find(id);
                    if (it != poi_rm.end()) id = it->second;
                }
                std::sort(cells[i].ids.begin(), cells[i].ids.end());
            }
        }
    }

    if (!removed_cells.empty()) {
        std::unordered_set<uint64_t> removed_set(removed_cells.begin(), removed_cells.end());
        cells.erase(std::remove_if(cells.begin(), cells.end(),
            [&](const CellData& c) { return removed_set.count(c.cell_id); }), cells.end());
    }
    if (!added_cells.empty()) {
        for (uint64_t cid : added_cells) { CellData cd; cd.cell_id = cid; cells.push_back(cd); }
        std::sort(cells.begin(), cells.end(),
            [](const CellData& a, const CellData& b) { return a.cell_id < b.cell_id; });
    }

    RebuiltPoi result;
    uint32_t no_data = 0xFFFFFFFF;
    std::unordered_map<uint64_t, uint32_t> offsets;
    for (auto& c : cells) {
        if (c.ids.empty()) continue;
        offsets[c.cell_id] = static_cast<uint32_t>(result.poi_entries_data.size());
        uint16_t count = static_cast<uint16_t>(c.ids.size());
        result.poi_entries_data.insert(result.poi_entries_data.end(), (const char*)&count, (const char*)&count + 2);
        result.poi_entries_data.insert(result.poi_entries_data.end(), (const char*)c.ids.data(), (const char*)c.ids.data() + c.ids.size() * 4);
    }
    for (auto& c : cells) {
        result.poi_cells_data.insert(result.poi_cells_data.end(), (const char*)&c.cell_id, (const char*)&c.cell_id + 8);
        auto it = offsets.find(c.cell_id);
        uint32_t off = it != offsets.end() ? it->second : no_data;
        result.poi_cells_data.insert(result.poi_cells_data.end(), (const char*)&off, (const char*)&off + 4);
    }
    return result;
}

// Place cell index rebuild (same structure as POI: 12-byte stride, cell_id(u64) + entry_offset(u32))
struct RebuiltPlace {
    std::vector<char> place_cells_data;
    std::vector<char> place_entries_data;
};

inline RebuiltPlace rebuild_place_from_remap(
    const std::vector<char>& old_pc, const std::vector<char>& old_pe,
    const std::unordered_map<uint32_t,uint32_t>& place_rm,
    const std::vector<uint64_t>& added_cells = {},
    const std::vector<uint64_t>& removed_cells = {})
{
    size_t n_cells = old_pc.size() / 12;

    struct CellData { uint64_t cell_id; std::vector<uint32_t> ids; };
    std::vector<CellData> cells(n_cells);
    for (size_t i = 0; i < n_cells; i++) {
        memcpy(&cells[i].cell_id, old_pc.data() + i * 12, 8);
        uint32_t off; memcpy(&off, old_pc.data() + i * 12 + 8, 4);
        if (off != 0xFFFFFFFF && off + 2 <= old_pe.size()) {
            uint16_t count; memcpy(&count, old_pe.data() + off, 2);
            if (off + 2 + count * 4 <= old_pe.size()) {
                cells[i].ids.resize(count);
                memcpy(cells[i].ids.data(), old_pe.data() + off + 2, count * 4);
                for (auto& id : cells[i].ids) {
                    auto it = place_rm.find(id);
                    if (it != place_rm.end()) id = it->second;
                }
                std::sort(cells[i].ids.begin(), cells[i].ids.end());
            }
        }
    }

    if (!removed_cells.empty()) {
        std::unordered_set<uint64_t> removed_set(removed_cells.begin(), removed_cells.end());
        cells.erase(std::remove_if(cells.begin(), cells.end(),
            [&](const CellData& c) { return removed_set.count(c.cell_id); }), cells.end());
    }
    if (!added_cells.empty()) {
        for (uint64_t cid : added_cells) { CellData cd; cd.cell_id = cid; cells.push_back(cd); }
        std::sort(cells.begin(), cells.end(),
            [](const CellData& a, const CellData& b) { return a.cell_id < b.cell_id; });
    }

    RebuiltPlace result;
    uint32_t no_data = 0xFFFFFFFF;
    std::unordered_map<uint64_t, uint32_t> offsets;
    for (auto& c : cells) {
        if (c.ids.empty()) continue;
        offsets[c.cell_id] = static_cast<uint32_t>(result.place_entries_data.size());
        uint16_t count = static_cast<uint16_t>(c.ids.size());
        result.place_entries_data.insert(result.place_entries_data.end(), (const char*)&count, (const char*)&count + 2);
        result.place_entries_data.insert(result.place_entries_data.end(), (const char*)c.ids.data(), (const char*)c.ids.data() + c.ids.size() * 4);
    }
    for (auto& c : cells) {
        result.place_cells_data.insert(result.place_cells_data.end(), (const char*)&c.cell_id, (const char*)&c.cell_id + 8);
        auto it = offsets.find(c.cell_id);
        uint32_t off = it != offsets.end() ? it->second : no_data;
        result.place_cells_data.insert(result.place_cells_data.end(), (const char*)&off, (const char*)&off + 4);
    }
    return result;
}

// --- Varint encoding for delta-compressed fixup tables ---

inline void write_varint(std::vector<char>& buf, uint32_t value) {
    while (value >= 128) {
        buf.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buf.push_back(static_cast<char>(value));
}

inline uint32_t read_varint(const char* data, size_t& pos) {
    uint32_t result = 0, shift = 0;
    while (true) {
        uint8_t byte = static_cast<uint8_t>(data[pos++]);
        result |= (uint32_t)(byte & 0x7F) << shift;
        if (!(byte & 0x80)) break;
        shift += 7;
    }
    return result;
}

// --- Grid coordinate for fingerprinting ---

inline int to_grid(float v) {
    return (int)(v * 1e5f + (v >= 0 ? 0.5f : -0.5f));
}

// --- Directory creation ---
inline void ensure_dir(const std::string& path) {
    std::string cmd = "mkdir -p '" + path + "'";
    system(cmd.c_str());
}
