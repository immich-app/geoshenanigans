#pragma once
// Shared definitions for the geocoder patch system.
// Used by geocoder-canonicalize, geocoder-diff, and geocoder-patch.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
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
// name_id byte offset within a street WayHeader record. The builder writes
// the padded layout (stride 12, name_id at 8); legacy builds wrote it packed
// (stride 9, name_id at 5). Pick by the detected stride.
static constexpr size_t WAY_HEADER_NAME_ID_OFF_PADDED = 8;
static constexpr size_t WAY_HEADER_NAME_ID_OFF_PACKED = 5;
static constexpr size_t WAY_HEADER_STRIDE_PACKED = 9;

struct PatchAddrPoint {
    float lat;
    float lng;
    uint32_t housenumber_id;
    uint32_t street_id;
    uint32_t parent_way_id;
};
static_assert(sizeof(PatchAddrPoint) == 20, "AddrPoint must be 20 bytes");
// Byte offsets within an AddrPoint record (used by the patch tool's per-record
// remap/fixup passes). housenumber_id/street_id are string-pool offsets;
// parent_way_id is a WAY id (street id-space), NOT a string offset; the
// vertex_offset (polygon footprint) lives after the record's 20 fixed bytes.
static constexpr size_t ADDR_POINT_HOUSENUMBER_ID_OFF = 8;
static constexpr size_t ADDR_POINT_STREET_ID_OFF = 12;
static constexpr size_t ADDR_POINT_PARENT_WAY_ID_OFF = 16;
static constexpr size_t ADDR_POINT_VERTEX_OFFSET_OFF = 20;

struct PatchNodeCoord {
    float lat;
    float lng;
};
static_assert(sizeof(PatchNodeCoord) == 8, "NodeCoord must be 8 bytes");
#pragma pack(pop)

// InterpWay and AdminPolygon have compiler-dependent padding.
// Read them field-by-field instead of struct-casting.

// street_id byte offset within an InterpWay record. node_count is followed by
// padding at stride>=20 (street_id at 8) or no padding at stride 18
// (street_id at 5).
static constexpr size_t INTERP_WAY_STREET_ID_OFF_PADDED = 8;
static constexpr size_t INTERP_WAY_STREET_ID_OFF_PACKED = 5;
static constexpr size_t INTERP_WAY_STRIDE_PACKED = 18;

// node_count of a WayHeader / InterpWay record: u16 at byte 4 in the padded
// layouts the builder writes, u8 in the legacy packed layouts (stride 9 / 18).
inline uint32_t record_node_count(const char* rec, size_t stride, size_t packed_stride) {
    if (stride == packed_stride) return static_cast<uint8_t>(rec[4]);
    uint16_t n;
    memcpy(&n, rec + 4, 2);
    return n;
}

struct PatchAdminPolygon {
    uint32_t vertex_offset;
    uint32_t vertex_count;
    uint32_t name_id;
    uint8_t admin_level;
    uint8_t place_type_override;
    float area;
    uint16_t country_code;
};
// name_id byte offset within an AdminPolygon record (string-pool offset).
static constexpr size_t ADMIN_POLYGON_NAME_ID_OFF = 8;

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
// Byte offsets within a PoiRecord, keyed off the on-disk stride (which grew
// across build versions). vertex_offset is the fixup target. name_id /
// parent_street_id / parent_postcode_id are string-pool offsets; parent_poly_id
// is an admin polygon index (admin id-space).
//   stride 24: lat,lng,vertex_offset,vertex_count,name_id,4×u8
//   stride 28: + parent_street_id at 24
//   stride 32: + parent_postcode_id at 28
//   stride 36: + parent_poly_id at 32
static constexpr size_t POI_RECORD_VERTEX_OFFSET_OFF = 8;
static constexpr size_t POI_RECORD_NAME_ID_OFF = 16;
static constexpr size_t POI_RECORD_PARENT_STREET_ID_OFF = 24;
static constexpr size_t POI_RECORD_PARENT_POSTCODE_ID_OFF = 28;
static constexpr size_t POI_RECORD_PARENT_POLY_ID_OFF = 32;

// PlaceNode byte offsets (no dedicated Patch struct — parsed by stride).
// name_id is a string-pool offset; parent_poly_id (present at stride>=20) is an
// admin polygon index.
static constexpr size_t PLACE_NODE_NAME_ID_OFF = 8;
static constexpr size_t PLACE_NODE_PARENT_POLY_ID_OFF = 16;

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

// --- Patch file format ---

static constexpr char GCPATCH_MAGIC[8] = {'G','C','P','A','T','C','H','\0'};
// Custom merge-sequence patch format. Emitted by geocoder-diff, checked by
// geocoder-patch. v3 added INTERP_POSTCODES (fid 36). v4 lists the variant's
// client files (CLIENT_FILES_MARKER) right after the header, carries its JSON
// files verbatim, and records the string tiers it was diffed against; the
// patcher reproduces exactly that file set. v5 carries offset fixups as runs
// of one shift (OffsetFixups). Version-gated so older appliers reject the
// whole patch upfront ("Bad version").
static constexpr uint32_t GCPATCH_VERSION = 5;
// v4 patches list their offset fixups one by one, which a v5 patcher no
// longer reads.
static constexpr uint32_t GCPATCH_MIN_READ_VERSION = 5;

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
    // addr_points polygon footprints introduced by commit aaf050b
    // (build_version bump 8→9). Emitted as a full-replacement section
    // like the other secondary files — the diff tool doesn't yet have
    // record-level logic for this file.
    ADDR_VERTICES = 30,
    // Per-tier strings files (build_version 14). Replaces the single
    // STRINGS=0 slot. Each is a raw-replacement section — the pool
    // contents are globally ordered so small changes produce small
    // diffs in a single tier file even without record-level logic.
    STRINGS_CORE = 31,
    STRINGS_STREET = 32,
    STRINGS_ADDR = 33,
    STRINGS_POSTCODE = 34,
    STRINGS_POI = 35,
    // Per-segment TIGER ZIP sidecar (u32 string offset per interp way,
    // NO_DATA for OSM interpolations; absent on builds without TIGER).
    INTERP_POSTCODES = 36,
    COUNT = 37
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
    "postal_polygons.bin", "postal_vertices.bin",
    "addr_vertices.bin",
    "strings_core.bin", "strings_street.bin", "strings_addr.bin",
    "strings_postcode.bin", "strings_poi.bin",
    "interp_postcodes.bin"
};

// Offset fixup section marker: 0xFFFFFFFD
// Format: uint32_t marker, uint32_t file_id, uint32_t stride,
//         uint32_t count, [(uint32_t record_index, uint32_t new_offset_value)] * count
// Applied to byte offset 0 of each record (node_offset/vertex_offset field).

static constexpr uint32_t FIXUP_MARKER = 0xFFFFFFFD;

// Terminator written after the last patch section; the patcher breaks its
// section loop when it reads this as a file_id and refuses a patch that ends
// without it. Same bytes as NO_DATA, but a distinct concept.
static constexpr uint32_t SECTION_END_MARKER = 0xFFFFFFFFu;

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
//   for each: cell_id(8), entry_count(2), [id(4)] * entry_count
// cell_id is the S2 cell id (u64), NOT an array position — the patcher
// binary-searches the cells file for it.
static constexpr uint32_t ENTRY_CORRECTION_MARKER = 0xFFFFFFF8;

// Cell index delta marker: 0xFFFFFFF7
// An admin / POI / place cell index the patcher rebuilds from its id remap,
// corrected per cell instead of by ENTRY_CORRECTION: a corrected cell costs
// the ids it lost and gained, not its whole list. Sent when write_cell_lists
// reproduces the new index byte for byte.
// Format: marker(4), file_id(4) = the entries file, payload_size(u64),
//   append_cell_list_delta(rebuilt → new) bytes.
static constexpr uint32_t CELL_INDEX_DELTA_MARKER = 0xFFFFFFF7;

// Cell flag corrections marker: 0xFFFFFFF9
// Format: marker, count(u32), [(cell_id:u64, flags:u8)] × count
// flags: bit 0 = has_street, bit 1 = has_addr, bit 2 = has_interp
// No longer emitted (entry corrections already decide every flipped cell);
// the patcher still accepts it from older v5 patches.
static constexpr uint32_t CELL_FLAGS_MARKER = 0xFFFFFFF9;

// Secondary ID remap marker: 0xFFFFFFF6
// Additional old→new ID mappings for modified records (recovered by relaxed key matching).
// Both diff and patch tools must apply these to their derived remaps for consistent results.
// Format: marker(4), n_files(4),
//   for each: file_id(4), n_pairs(4), [(old_id:u32, new_id:u32)] × n_pairs
static constexpr uint32_t SECONDARY_REMAP_MARKER = 0xFFFFFFF6;

// String-section markers, read by geocoder-patch during the Phase-2 string
// rebuild. These intentionally share their
// raw values with markers above (e.g. STRINGS_TIERED_MARKER reuses 0xFFFFFFF6,
// the SECONDARY_REMAP_MARKER value) because they are disambiguated by read
// position, not by value — the string markers are consumed before the main
// section loop where SECONDARY_REMAP_MARKER appears.
//
//   STRINGS_TIERED_MARKER  — tiered per-tier strings diff (5 blocks of
//                            {n_added, n_deleted, added..., deleted_idx...}).
//                            Emitted by geocoder-diff as `tiered_marker`.
//   STRINGS_CROSS_TIER_REMAP_MARKER — explicit cross-tier (old_off,new_off)
//                            string remap pairs. Emitted by geocoder-diff.
static constexpr uint32_t STRINGS_TIERED_MARKER = 0xFFFFFFF6;
static constexpr uint32_t STRINGS_CROSS_TIER_REMAP_MARKER = 0xFFFFFFFE;

// 64-bit content hash, 8 bytes per step. Not cryptographic: it proves a file
// is the one the diff saw, against stale or mismatched inputs.
inline uint64_t content_hash(const char* data, size_t n) {
    uint64_t h = 0x9E3779B97F4A7C15ull ^ n;
    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        uint64_t w;
        memcpy(&w, data + i, 8);
        h = (h ^ w) * 0xFF51AFD7ED558CCDull;
        h ^= h >> 32;
    }
    uint64_t tail = 0;
    if (n > i) memcpy(&tail, data + i, n - i);
    h = (h ^ tail) * 0xC4CEB9FE1A85EC53ull;
    return h ^ (h >> 29);
}

// The old and new string tier the diff worked from, one per tier, right after
// STRINGS_TIERED_MARKER: 5 × {old_size u32, new_size u32, old_hash u64,
// new_hash u64}. The patcher refuses old tiers (often found through ../full)
// that differ from what the diff saw and checks every tier it rebuilds, so a
// stale or already-patched sibling fails loudly instead of skewing offsets.
struct TierStamp {
    uint32_t old_size = 0, new_size = 0;
    uint64_t old_hash = 0, new_hash = 0;
};

// Sparse position-keyed delta. Stride sentinel that signals the section
// payload is a list of (position, value) pairs for the positions where
// new differs from old. Used for files where strategy-2 keeps the index
// stable but values still need remap (e.g. way_parents holds admin
// polygon ids that shift when closed-way polygons get fresh ids; the
// diff applies an admin or string remap to the loaded old array before
// computing the delta, and the patcher applies the same remap before
// overwriting the changed positions). Daily delta drops from a full
// replace (hundreds of MiB per file) to tens of KiB.
//
// Section format:
//   file_id(u32),
//   stride = SPARSE_DELTA_STRIDE (u32),
//   old_size(u64), new_size(u64),
//   value_stride(u32) — bytes per record (4 for uint32 arrays, 16 for postcode_centroid),
//   remap_kind(u32)  — 0=none, 1=admin idx, 2=string offset, 3=postcode_centroid struct,
//   n_changes(u32),
//   [(pos:u32, value:value_stride bytes)] × n_changes.
//
// remap_kind 3 means a 16-byte postcode_centroid record: lat(4) lng(4)
// postcode_id(4) cc(2) pad(2). Only the postcode_id field uses str_remap.
constexpr uint32_t SPARSE_DELTA_STRIDE = 0xFC;

// Stride sentinel: the new file is byte-identical to the old build. The section
// carries the standard 6-field header (fid, stride, old_size, new_size, nfix=0,
// ds=0) with NO data; the patcher reproduces the file by copying it verbatim
// from cur_dir. Emitted by emit_raw / emit_sparse_delta / serialize_merge (via
// try_emit_copy_old) when memcmp(old,new)==0. Safe for data files that feed
// entry corrections because byte-identity implies an identity id_remap (the
// fixup passes are skipped for identical files), so the patcher's identity
// reconstruction matches.
constexpr uint32_t COPY_OLD_STRIDE = 0xFD;

// Legacy/reserved stride sentinel: skip the section (read header + data, write
// nothing). Consumed by geocoder-patch; not emitted by the current diff.
constexpr uint32_t LEGACY_SKIP_STRIDE = 0xFE;

// Stride sentinel: a cell index (<name>_cells.bin + <name>_entries.bin)
// patched per cell. Strategy-2 ids keep most lists unchanged day to day, so
// only the cells that lost or gained ids travel. The section names the cells
// file and the patcher writes both files.
//   file_id(u32), stride = CELL_LIST_DELTA_STRIDE (u32),
//   old_size(u64), new_size(u64) of the cells file,
//   payload_size(u64), new entries size(u64), append_cell_list_delta bytes.
constexpr uint32_t CELL_LIST_DELTA_STRIDE = 0xFB;

// POI parent-id remap marker: 0xFFFFFFF3
// Carries the full primary+secondary admin-polygon, street-way, and
// postcode-centroid old→new ID remap that the diff side applied to old
// PoiRecord bytes 24/28/32 before computing pr_seq + the byte-block
// delta over poi_vertices.bin. The patcher applies the same remap
// during POI_RECORDS MATCH replay so the reconstructed bytes match the
// new file. Emitted only when the patch contains POI_RECORDS — quality,
// admin, full, etc. variants without POI files would otherwise carry
// hundreds of MiB of remap pairs that are never applied.
// Format: marker(4),
//   n_admin_pairs(4),    [(old_id:u32, new_id:u32)] × n_admin_pairs,
//   n_street_pairs(4),   [(old_id:u32, new_id:u32)] × n_street_pairs,
//   n_postcode_pairs(4), [(old_id:u32, new_id:u32)] × n_postcode_pairs.
// Only entries where old_id != new_id are transmitted.
static constexpr uint32_t POI_PARENT_REMAP_MARKER = 0xFFFFFFF3;

// --- Client file set (v4) ---
// The files a client holds for a variant: everything in the variant dir except
// strategy-2 sidecars (*.osm_ids, server-side cache only), transport artifacts
// (*.gcpatch, *.zst) and dotfiles. Read by position right after the header:
//   marker(4), n(4), n × {name_len(u16), name, size(u64), inline(u8), [bytes]}
// sorted by name. Non-.bin files (strings_layout.json, poi_meta.json) are
// inline; the patcher writes them verbatim and every listed file must exist at
// its listed size after the apply. Files it builds that are not listed (string
// tiers the variant only uses for remapping) stay in its scratch dir.
static constexpr uint32_t CLIENT_FILES_MARKER = 0xFFFFFFF2;
static constexpr size_t MAX_INLINE_CLIENT_FILE_BYTES = 1 << 20;

struct ClientFile {
    std::string name;
    uint64_t size = 0;
    std::vector<char> bytes;  // contents, inline files only
};

inline bool has_suffix(const std::string& s, const char* suffix) {
    size_t n = strlen(suffix);
    return s.size() >= n && s.compare(s.size() - n, n, suffix) == 0;
}

inline bool is_client_file(const std::string& name) {
    if (name.empty() || name[0] == '.' || name.find('/') != std::string::npos) return false;
    return !has_suffix(name, ".osm_ids") && !has_suffix(name, ".gcpatch") && !has_suffix(name, ".zst");
}

inline bool is_inline_client_file(const std::string& name) { return !has_suffix(name, ".bin"); }

inline void append_client_files(std::vector<char>& out, const std::vector<ClientFile>& files) {
    auto put = [&](const void* p, size_t n) { out.insert(out.end(), (const char*)p, (const char*)p + n); };
    uint32_t marker = CLIENT_FILES_MARKER, n = static_cast<uint32_t>(files.size());
    put(&marker, 4); put(&n, 4);
    for (const auto& f : files) {
        uint16_t len = static_cast<uint16_t>(f.name.size());
        uint8_t is_inline = is_inline_client_file(f.name) ? 1 : 0;
        put(&len, 2); put(f.name.data(), len); put(&f.size, 8); put(&is_inline, 1);
        if (is_inline) put(f.bytes.data(), f.bytes.size());
    }
}

// Parses the section at data[pos, size) and advances pos past it.
inline std::vector<ClientFile> parse_client_files(const char* data, size_t size, size_t& pos) {
    auto take = [&](void* dst, size_t n) {
        if (pos + n > size) throw std::runtime_error("Truncated client file list");
        memcpy(dst, data + pos, n);
        pos += n;
    };
    uint32_t marker = 0, n = 0;
    take(&marker, 4);
    if (marker != CLIENT_FILES_MARKER) throw std::runtime_error("Patch has no client file list");
    take(&n, 4);
    if ((size_t)n * 11 > size - pos) throw std::runtime_error("Truncated client file list");  // 11 = smallest entry
    std::vector<ClientFile> files(n);
    for (auto& f : files) {
        uint16_t len = 0;
        take(&len, 2);
        f.name.resize(len);
        take(f.name.data(), len);
        if (!is_client_file(f.name)) throw std::runtime_error("Bad client file name: " + f.name);
        uint8_t is_inline = 0;
        take(&f.size, 8);
        take(&is_inline, 1);
        if (is_inline != (is_inline_client_file(f.name) ? 1 : 0))
            throw std::runtime_error("Bad client file entry: " + f.name);
        if (!is_inline) continue;
        if (f.size > MAX_INLINE_CLIENT_FILE_BYTES) throw std::runtime_error("Inline client file too large: " + f.name);
        f.bytes.resize(f.size);
        take(f.bytes.data(), f.size);
    }
    return files;
}

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

// Admin, POI and place cell index rebuild: 12-byte cells (cell_id u64,
// entry_offset u32) over (count u16, ids u32[]) entries. An id's top bit is
// INTERIOR_FLAG, so the remap is keyed by the bare id and the flag carried over.
struct RebuiltCells {
    std::vector<char> cells_data;
    std::vector<char> entries_data;
};

inline RebuiltCells rebuild_cells_from_remap(
    const std::vector<char>& old_cells, const std::vector<char>& old_entries,
    const std::unordered_map<uint32_t,uint32_t>& rm,
    const std::vector<uint64_t>& added_cells = {},
    const std::vector<uint64_t>& removed_cells = {})
{
    size_t n_cells = old_cells.size() / 12;

    struct CellData { uint64_t cell_id; std::vector<uint32_t> ids; };
    std::vector<CellData> cells(n_cells);
    for (size_t i = 0; i < n_cells; i++) {
        memcpy(&cells[i].cell_id, old_cells.data() + i * 12, 8);
        uint32_t off; memcpy(&off, old_cells.data() + i * 12 + 8, 4);
        if (off != 0xFFFFFFFF && off + 2 <= old_entries.size()) {
            uint16_t count; memcpy(&count, old_entries.data() + off, 2);
            if (off + 2 + count * 4 <= old_entries.size()) {
                cells[i].ids.resize(count);
                memcpy(cells[i].ids.data(), old_entries.data() + off + 2, count * 4);
                for (auto& id : cells[i].ids) {
                    uint32_t flags = id & 0x80000000u;
                    uint32_t masked = id & 0x7FFFFFFFu;
                    auto it = rm.find(masked);
                    if (it != rm.end()) id = it->second | flags;
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

    RebuiltCells result;
    uint32_t no_data = 0xFFFFFFFF;
    std::unordered_map<uint64_t, uint32_t> offsets;
    for (auto& c : cells) {
        if (c.ids.empty()) continue;
        offsets[c.cell_id] = static_cast<uint32_t>(result.entries_data.size());
        uint16_t count = static_cast<uint16_t>(c.ids.size());
        result.entries_data.insert(result.entries_data.end(), (const char*)&count, (const char*)&count + 2);
        result.entries_data.insert(result.entries_data.end(), (const char*)c.ids.data(), (const char*)c.ids.data() + c.ids.size() * 4);
    }
    for (auto& c : cells) {
        result.cells_data.insert(result.cells_data.end(), (const char*)&c.cell_id, (const char*)&c.cell_id + 8);
        auto it = offsets.find(c.cell_id);
        uint32_t off = it != offsets.end() ? it->second : no_data;
        result.cells_data.insert(result.cells_data.end(), (const char*)&off, (const char*)&off + 4);
    }
    return result;
}

// --- Cell index as per-cell id lists (CELL_LIST_DELTA_STRIDE) ---

// cell_id → its ids (sorted), in cell order.
using CellLists = std::map<uint64_t, std::vector<uint32_t>>;

inline CellLists parse_cell_lists(const std::vector<char>& cells, const std::vector<char>& entries) {
    CellLists out;
    for (size_t i = 0; i + 12 <= cells.size(); i += 12) {
        uint64_t cid; uint32_t off;
        memcpy(&cid, cells.data() + i, 8);
        memcpy(&off, cells.data() + i + 8, 4);
        auto& ids = out[cid];
        if (off == 0xFFFFFFFF) continue;
        uint16_t n;
        if ((size_t)off + 2 > entries.size()) throw std::runtime_error("Malformed cell index");
        memcpy(&n, entries.data() + off, 2);
        if ((size_t)off + 2 + (size_t)n * 4 > entries.size()) throw std::runtime_error("Malformed cell index");
        ids.resize(n);
        memcpy(ids.data(), entries.data() + off + 2, (size_t)n * 4);
    }
    return out;
}

// The bytes write_cell_index writes for these lists: (cells, entries).
inline std::pair<std::vector<char>, std::vector<char>> write_cell_lists(const CellLists& lists) {
    std::pair<std::vector<char>, std::vector<char>> out;
    auto& [cells, entries] = out;
    for (const auto& [cid, ids] : lists) {
        if (ids.size() > 0xFFFF) throw std::runtime_error("Cell holds more than 65535 entries");
        uint32_t off = static_cast<uint32_t>(entries.size());
        uint16_t n = static_cast<uint16_t>(ids.size());
        cells.insert(cells.end(), (const char*)&cid, (const char*)&cid + 8);
        cells.insert(cells.end(), (const char*)&off, (const char*)&off + 4);
        entries.insert(entries.end(), (const char*)&n, (const char*)&n + 2);
        entries.insert(entries.end(), (const char*)ids.data(), (const char*)ids.data() + ids.size() * 4);
    }
    return out;
}

// Removed cells (u32 n, u64 each), then every cell that is new or whose list
// changed (u32 n, then u64 cell, u32 n_lost, u32 n_gained, lost ids, gained ids).
inline void append_cell_list_delta(std::vector<char>& out, const CellLists& old_lists, const CellLists& new_lists) {
    auto put = [&](const void* p, size_t n) { out.insert(out.end(), (const char*)p, (const char*)p + n); };
    std::vector<uint64_t> removed;
    for (const auto& kv : old_lists)
        if (!new_lists.count(kv.first)) removed.push_back(kv.first);
    uint32_t n_removed = static_cast<uint32_t>(removed.size());
    put(&n_removed, 4);
    put(removed.data(), removed.size() * 8);

    const std::vector<uint32_t> none;
    std::vector<char> sets;
    uint32_t n_set = 0;
    for (const auto& [cid, ids] : new_lists) {
        auto it = old_lists.find(cid);
        if (it != old_lists.end() && it->second == ids) continue;
        const auto& was = it != old_lists.end() ? it->second : none;
        std::vector<uint32_t> lost, gained;
        std::set_difference(was.begin(), was.end(), ids.begin(), ids.end(), std::back_inserter(lost));
        std::set_difference(ids.begin(), ids.end(), was.begin(), was.end(), std::back_inserter(gained));
        uint32_t nl = static_cast<uint32_t>(lost.size()), ng = static_cast<uint32_t>(gained.size());
        sets.insert(sets.end(), (const char*)&cid, (const char*)&cid + 8);
        sets.insert(sets.end(), (const char*)&nl, (const char*)&nl + 4);
        sets.insert(sets.end(), (const char*)&ng, (const char*)&ng + 4);
        sets.insert(sets.end(), (const char*)lost.data(), (const char*)lost.data() + lost.size() * 4);
        sets.insert(sets.end(), (const char*)gained.data(), (const char*)gained.data() + gained.size() * 4);
        n_set++;
    }
    put(&n_set, 4);
    out.insert(out.end(), sets.begin(), sets.end());
}

// Rebuilds a cell index and applies append_cell_list_delta bytes to it in one
// pass, writing the result in write_cell_lists layout through
// out_cells(bytes, n) / out_entries(bytes, n). The source is the old index
// (cells sorted by id) plus `added` cell ids with empty lists, minus
// `removed`; every old id moves by remap(id without INTERIOR_FLAG), flag
// kept, and each list is sorted: the same lists rebuild_cells_from_remap
// gives. Holds one cell's list at a time: the patcher runs on client
// machines, where materialising planet poi/all (1.1M cells, 25M ids, a 23M
// entry remap map) cost over 1 GiB. Returns the entries bytes written.
template <typename Remap, typename OutCells, typename OutEntries>
uint64_t stream_cell_index(const char* cells, size_t cells_size, const char* entries, size_t entries_size,
                           std::vector<uint64_t> added, std::vector<uint64_t> removed, Remap remap,
                           const char* delta, size_t delta_size, OutCells out_cells, OutEntries out_entries) {
    auto malformed = [] { throw std::runtime_error("Malformed cell list delta"); };
    auto bad_index = [] { throw std::runtime_error("Malformed cell index"); };
    std::sort(added.begin(), added.end());
    std::sort(removed.begin(), removed.end());

    size_t pos = 0;
    auto take = [&](void* dst, size_t n) {
        if (delta_size - pos < n) malformed();
        memcpy(dst, delta + pos, n);
        pos += n;
    };
    uint32_t n_gone;
    take(&n_gone, 4);
    if ((delta_size - pos) / 8 < n_gone) malformed();
    const char* gone = delta + pos;
    pos += (size_t)n_gone * 8;
    uint32_t n_set, set_i = 0, gone_i = 0;
    take(&n_set, 4);

    // The next changed cell from the delta, read in cell order.
    constexpr uint64_t NONE = ~0ull;
    uint64_t set_cid = NONE, prev_set = 0;
    std::vector<uint32_t> lost, gained;
    auto next_set = [&] {
        if (set_i == n_set) { set_cid = NONE; return; }
        uint32_t nl, ng;
        take(&set_cid, 8); take(&nl, 4); take(&ng, 4);
        if ((set_i > 0 && set_cid <= prev_set) || set_cid == NONE) malformed();
        if ((delta_size - pos) / 4 < (size_t)nl + ng) malformed();
        lost.resize(nl); gained.resize(ng);
        take(lost.data(), (size_t)nl * 4);
        take(gained.data(), (size_t)ng * 4);
        prev_set = set_cid;
        set_i++;
    };
    auto delta_drops = [&](uint64_t cid) {
        uint64_t g = 0;
        while (gone_i < n_gone) {
            memcpy(&g, gone + (size_t)gone_i * 8, 8);
            if (g >= cid) break;
            gone_i++;
        }
        return gone_i < n_gone && g == cid;
    };

    // The next rebuilt cell: old cells minus `removed`, merged with `added`.
    size_t n_cells = cells_size / 12, i = 0, a = 0, r = 0;
    uint64_t prev_old = 0;
    std::vector<uint32_t> ids;
    auto next_source = [&](uint64_t& cid) -> bool {
        for (;;) {
            uint64_t old_cid = NONE;
            if (i < n_cells) {
                memcpy(&old_cid, cells + i * 12, 8);
                if (i > 0 && old_cid <= prev_old) bad_index();
            }
            uint64_t add_cid = a < added.size() ? added[a] : NONE;
            if (old_cid == NONE && add_cid == NONE) return false;
            if (add_cid < old_cid) {
                cid = add_cid; a++;
                ids.clear();
                return true;
            }
            if (add_cid == old_cid) bad_index();
            prev_old = old_cid;
            uint32_t off;
            memcpy(&off, cells + i * 12 + 8, 4);
            i++;
            while (r < removed.size() && removed[r] < old_cid) r++;
            if (r < removed.size() && removed[r] == old_cid) continue;
            cid = old_cid;
            ids.clear();
            if (off != 0xFFFFFFFF) {
                uint16_t n;
                if ((size_t)off + 2 > entries_size) bad_index();
                memcpy(&n, entries + off, 2);
                if ((size_t)off + 2 + (size_t)n * 4 > entries_size) bad_index();
                ids.resize(n);
                memcpy(ids.data(), entries + off + 2, (size_t)n * 4);
                for (auto& id : ids) id = remap(id & 0x7FFFFFFFu) | (id & 0x80000000u);
                std::sort(ids.begin(), ids.end());
            }
            return true;
        }
    };

    uint64_t written = 0;
    std::vector<uint32_t> kept, merged;
    auto emit = [&](uint64_t cid, const std::vector<uint32_t>& list) {
        if (list.size() > 0xFFFF) throw std::runtime_error("Cell holds more than 65535 entries");
        if (written > 0xFFFFFFFFull) throw std::runtime_error("Cell index entries past 4 GiB");
        uint32_t off = static_cast<uint32_t>(written);
        uint16_t n = static_cast<uint16_t>(list.size());
        out_cells((const char*)&cid, 8);
        out_cells((const char*)&off, 4);
        out_entries((const char*)&n, 2);
        out_entries((const char*)list.data(), list.size() * 4);
        written += 2 + list.size() * 4;
    };

    next_set();
    uint64_t cid = NONE;
    bool have = next_source(cid);
    while (have || set_cid != NONE) {
        uint64_t src = have ? cid : NONE;
        if (set_cid < src) {  // a cell the rebuilt index doesn't have
            emit(set_cid, gained);
            next_set();
            continue;
        }
        if (src == set_cid) {
            kept.clear();
            std::set_difference(ids.begin(), ids.end(), lost.begin(), lost.end(), std::back_inserter(kept));
            merged.clear();
            std::merge(kept.begin(), kept.end(), gained.begin(), gained.end(), std::back_inserter(merged));
            emit(src, merged);
            next_set();
        } else if (!delta_drops(src)) {
            emit(src, ids);
        }
        have = next_source(cid);
    }
    if (pos != delta_size) malformed();
    return written;
}

// stream_cell_index for an index whose ids don't move (the postcode centroid
// index): the old index plus the delta.
template <typename OutCells, typename OutEntries>
uint64_t stream_cell_list_delta(const char* cells, size_t cells_size, const char* entries, size_t entries_size,
                                const char* delta, size_t delta_size, OutCells out_cells, OutEntries out_entries) {
    return stream_cell_index(cells, cells_size, entries, entries_size, {}, {}, [](uint32_t id) { return id; },
                             delta, delta_size, out_cells, out_entries);
}

// --- String offset remap (patcher) ---

// The patcher's old → new string offset map. Between two edits every
// surviving string of a tier moves by one amount, so the map is held as runs
// over old offsets: a few thousand on a planet day instead of one pair per
// moved string (20.6M pairs, 157 MiB on every client). Like the diff's
// per-string pairs, only a string's start moves; the run checks that against
// the old pool, where the byte before a string start is its predecessor's NUL.
// Strings that changed tier stay explicit pairs.
class StringRemap {
public:
    // Merge-walks one tier's old and new pools (both sorted) and records the
    // surviving strings that moved. The old pool must stay mapped for lookups.
    void add_tier(const char* old_pool, size_t old_size, uint32_t old_base,
                  const char* new_pool, size_t new_size, uint32_t new_base) {
        size_t o = 0, n = 0;
        bool open = false;
        while (o < old_size && n < new_size) {
            const char* os = old_pool + o;
            const char* ns = new_pool + n;
            size_t ol = strnlen(os, old_size - o) + 1, nl = strnlen(ns, new_size - n) + 1;
            int c = strcmp(os, ns);
            if (c == 0) {
                uint32_t old_off = old_base + static_cast<uint32_t>(o);
                uint32_t shift = new_base + static_cast<uint32_t>(n) - old_off;
                if (shift == 0) {
                    open = false;
                } else if (open && runs_.back().end == old_off && runs_.back().shift == shift) {
                    runs_.back().end += static_cast<uint32_t>(ol);
                } else {
                    runs_.push_back({old_off, old_off + static_cast<uint32_t>(ol), shift, os});
                    open = true;
                }
                o += ol; n += nl;
            } else if (c < 0) {
                o += ol; open = false;  // deleted: its offsets keep mapping to themselves
            } else {
                n += nl; open = false;
            }
        }
    }
    void add_pair(uint32_t old_off, uint32_t new_off) { pairs_.push_back({old_off, new_off}); }
    void finish() { std::sort(pairs_.begin(), pairs_.end()); }
    bool empty() const { return runs_.empty() && pairs_.empty(); }
    size_t run_count() const { return runs_.size(); }
    size_t pair_count() const { return pairs_.size(); }

    uint32_t lookup(uint32_t off) const {
        if (!pairs_.empty()) {
            auto p = std::lower_bound(pairs_.begin(), pairs_.end(), std::make_pair(off, 0u));
            if (p != pairs_.end() && p->first == off) return p->second;
        }
        auto it = std::upper_bound(runs_.begin(), runs_.end(), off,
                                   [](uint32_t v, const Run& r) { return v < r.start; });
        if (it == runs_.begin()) return off;
        --it;
        if (off >= it->end) return off;
        if (off != it->start && it->old_bytes[off - it->start - 1] != '\0') return off;
        return off + it->shift;
    }

private:
    struct Run { uint32_t start, end, shift; const char* old_bytes; };
    std::vector<Run> runs_;
    std::vector<std::pair<uint32_t, uint32_t>> pairs_;
};

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

// --- Offset fixups ---
//
// The diff's fixup passes rewrite an old record's offset field (node_offset
// or vertex_offset) to where the same block sits in the new build, so
// unchanged records merge as MATCH, and the patcher must write the same
// values. One edit shifts every later block by the same amount, so the moved
// offsets come in long runs of one shift. They travel as runs of (records
// skipped since the previous run, run length, zigzag change of the shift)
// and the patcher adds the run's shift to each old offset in it. Records
// whose old offset is NO_DATA own no block: runs pass over them, and the
// rare one a pass gives an offset is listed as (index delta, offset).
struct OffsetFixups {
    uint32_t n_runs = 0, n_values = 0;
    std::vector<char> runs, values;
    bool empty() const { return n_runs == 0 && n_values == 0; }
};

inline uint32_t zigzag32(uint32_t v) { return (v << 1) ^ (0u - (v >> 31)); }
inline uint32_t unzigzag32(uint32_t v) { return (v >> 1) ^ (0u - (v & 1)); }

// fixed_at(i): record i's offset after the fixup passes.
template <typename FixedAt>
OffsetFixups encode_offset_fixups(const std::vector<uint32_t>& old_offsets, FixedAt fixed_at) {
    constexpr uint32_t NO_DATA = 0xFFFFFFFFu;
    OffsetFixups out;
    uint32_t prev_end = 0, prev_shift = 0, prev_value = 0;
    uint32_t start = 0, end = 0, shift = 0;
    bool open = false;
    auto close_run = [&]() {
        write_varint(out.runs, start - prev_end);
        write_varint(out.runs, end - start);
        write_varint(out.runs, zigzag32(shift - prev_shift));
        out.n_runs++;
        prev_end = end;
        prev_shift = shift;
        open = false;
    };
    for (uint32_t i = 0; i < old_offsets.size(); i++) {
        uint32_t old = old_offsets[i], fixed = fixed_at(i);
        if (old == NO_DATA) {
            if (fixed != NO_DATA) {
                write_varint(out.values, i - prev_value);
                write_varint(out.values, fixed);
                out.n_values++;
                prev_value = i;
            }
            continue;
        }
        uint32_t s = fixed - old;
        if (open && s == shift) { end = i + 1; continue; }
        if (open) close_run();
        if (s != 0) { open = true; start = i; end = i + 1; shift = s; }
    }
    if (open) close_run();
    return out;
}

// Applies encoded OffsetFixups to old records visited in ascending order.
class OffsetFixupReader {
public:
    OffsetFixupReader(const char* runs, size_t runs_size, uint32_t n_runs,
                      const char* values, size_t values_size, uint32_t n_values)
        : runs_(runs), runs_size_(runs_size), runs_left_(n_runs),
          values_(values), values_size_(values_size), values_left_(n_values) {
        next_run();
        next_value();
    }

    // Record idx's offset after the fixups, given its old offset.
    uint32_t apply(uint32_t idx, uint32_t old) {
        while (value_idx_ < idx) next_value();
        if (value_idx_ == idx) return value_;
        if (old == END) return old;
        while (run_end_ <= idx) next_run();
        return idx >= run_start_ ? old + shift_ : old;
    }

private:
    static constexpr uint32_t END = 0xFFFFFFFFu;  // also NO_DATA

    static uint32_t read(const char* data, size_t& pos, size_t size) {
        uint32_t result = 0;
        for (uint32_t bit = 0; bit < 35; bit += 7) {
            if (pos >= size) throw std::runtime_error("Malformed fixups");
            uint8_t byte = static_cast<uint8_t>(data[pos++]);
            result |= static_cast<uint32_t>(byte & 0x7F) << bit;
            if (!(byte & 0x80)) return result;
        }
        throw std::runtime_error("Malformed fixups");
    }
    void next_run() {
        if (runs_left_ == 0) { run_start_ = run_end_ = END; return; }
        runs_left_--;
        run_start_ = run_end_ + read(runs_, runs_pos_, runs_size_);
        run_end_ = run_start_ + read(runs_, runs_pos_, runs_size_);
        shift_ += unzigzag32(read(runs_, runs_pos_, runs_size_));
    }
    void next_value() {
        if (values_left_ == 0) { value_idx_ = END; return; }
        values_left_--;
        value_idx_ = (value_idx_ == END ? 0 : value_idx_) + read(values_, values_pos_, values_size_);
        value_ = read(values_, values_pos_, values_size_);
    }

    const char* runs_;
    size_t runs_size_, runs_pos_ = 0;
    uint32_t runs_left_, run_start_ = 0, run_end_ = 0, shift_ = 0;
    const char* values_;
    size_t values_size_, values_pos_ = 0;
    uint32_t values_left_, value_idx_ = END, value_ = 0;
};

// --- Grid coordinate for fingerprinting ---

inline int to_grid(float v) {
    return (int)(v * 1e5f + (v >= 0 ? 0.5f : -0.5f));
}

// Byte offset of an 8-byte NodeCoord. Widened before the multiply:
// planet node files pass 2^29 records, where 32-bit `idx * 8` wraps.
inline size_t node_byte_offset(uint32_t node_index) {
    return (size_t)node_index * 8;
}

// --- Directory creation ---
inline void ensure_dir(const std::string& path) {
    std::string cmd = "mkdir -p '" + path + "'";
    system(cmd.c_str());
}
