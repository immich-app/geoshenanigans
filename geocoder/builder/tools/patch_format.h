#pragma once
// Shared definitions for the geocoder patch system.
// Used by geocoder-diff and geocoder-patch.

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <tuple>
#include <type_traits>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cell_pairs.h"
#include "checked_size.h"
#include "sequential_file_reader.h"
#include "string_files.h"

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

// --- Binary record layouts (must match types.h and server) ---

// Strides of the record files the patch merges record by record.
static constexpr size_t WAY_HEADER_STRIDE = 12;
static constexpr size_t ADDR_POINT_STRIDE = 28;
static constexpr size_t INTERP_WAY_STRIDE = 24;
static constexpr size_t ADMIN_POLYGON_STRIDE = 24;
static constexpr size_t ARC_INDEX_STRIDE = 4;
static constexpr size_t POI_RECORD_STRIDE = 36;
static constexpr size_t PLACE_NODE_STRIDE = 20;

// name_id byte offset within a street WayHeader record.
static constexpr size_t WAY_HEADER_NAME_ID_OFF = 8;

// Byte offsets within an AddrPoint record (used by the patch tool's per-record
// remap/fixup passes). housenumber_id/street_id are string-pool offsets;
// parent_way_id is a WAY id (street id-space), NOT a string offset;
// vertex_offset points at the polygon footprint.
static constexpr size_t ADDR_POINT_HOUSENUMBER_ID_OFF = 8;
static constexpr size_t ADDR_POINT_STREET_ID_OFF = 12;
static constexpr size_t ADDR_POINT_PARENT_WAY_ID_OFF = 16;
static constexpr size_t ADDR_POINT_VERTEX_OFFSET_OFF = 20;

// street_id byte offset within an InterpWay record (after node_count and
// its padding).
static constexpr size_t INTERP_WAY_STREET_ID_OFF = 8;

// node_count of a WayHeader / InterpWay record: u16 at byte 4.
inline uint32_t record_node_count(const char* rec) {
    uint16_t n;
    memcpy(&n, rec + 4, 2);
    return n;
}

// name_id byte offset within an AdminPolygon record (string-pool offset).
static constexpr size_t ADMIN_POLYGON_NAME_ID_OFF = 8;

// Byte offsets within a PoiRecord (lat, lng, vertex_offset, vertex_count,
// name_id, 4×u8, parent_street_id, parent_postcode_id, parent_poly_id).
// vertex_offset is the fixup target. name_id / parent_street_id /
// parent_postcode_id are string-pool offsets; parent_poly_id is an admin
// slot, which a polygon keeps.
static constexpr size_t POI_RECORD_VERTEX_OFFSET_OFF = 8;
static constexpr size_t POI_RECORD_NAME_ID_OFF = 16;
static constexpr size_t POI_RECORD_PARENT_STREET_ID_OFF = 24;
static constexpr size_t POI_RECORD_PARENT_POSTCODE_ID_OFF = 28;
static constexpr size_t POI_RECORD_PARENT_POLY_ID_OFF = 32;

// PlaceNode byte offsets. name_id is a string-pool offset (parent_poly_id, at
// 16, is an admin slot).
static constexpr size_t PLACE_NODE_NAME_ID_OFF = 8;

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

// Every patch section names the size of the old file it was made from; the
// diff writes 0 for a file the old dir lacks. Checking it is the only thing
// that refuses a dir from another day for variants that ship no string tier
// (quality dirs): every other read clamps to what is there.
inline void require_old_file_size(const std::string& dir, const std::string& name, uint64_t expected) {
    // Low-memory mode: an encoded family's old file is decoded in process.
    if (virtual_size_hook() && virtual_size_hook()(dir, name, expected)) {
        virtual_file_sizes()[dir + "/" + name] = expected;
        return;
    }
    struct stat st;
    uint64_t actual = stat((dir + "/" + name).c_str(), &st) == 0 ? static_cast<uint64_t>(st.st_size) : 0;
    if (actual != expected)
        throw std::runtime_error("Old " + name + " is not the one the patch was made from (" +
                                 std::to_string(actual) + " bytes, the patch expects " +
                                 std::to_string(expected) + ")");
}

// --- Patch file format ---

static constexpr char GCPATCH_MAGIC[8] = {'G','C','P','A','T','C','H','\0'};
// Custom merge-sequence patch format. Emitted by geocoder-diff, checked by
// geocoder-patch. v3 added INTERP_POSTCODES (fid 36). v4 lists the variant's
// client files (CLIENT_FILES_MARKER) right after the header, carries its JSON
// files verbatim, and records the string tiers it was diffed against; the
// patcher reproduces exactly that file set. v5 carries offset fixups as runs
// of one shift (OffsetFixups). v6 corrects street / addr / interp lists per
// cell (GEO_ENTRY_DELTA_MARKER) and drops cell flags. v7 patches every
// variant dir from its own files: string tiers the dir doesn't ship travel as
// shift runs instead of string diffs (see STRINGS_TIERED_MARKER).
// Version-gated so older appliers reject the whole patch upfront ("Bad
// version").
// v8: street / addr / interp records can match across days by a server-side
// identity sidecar (geocoder-diff --identity) instead of by content, which
// suits both stable-slot days and days kept in a canonical content order; the
// merge gains REPLACE (identity kept, new bytes); the street leg of the
// parent-id remap and the secondary remap carry only the moved records' pairs
// (everything else is derived from the replays, streets replayed before
// addresses); old ids the remap doesn't map drop out of the street / addr /
// interp cell lists; and the per-record side files follow their parent's
// merge (PARALLEL_DELTA_STRIDE).
// v9: string tiers in any order. Each shipped tier's diff carries an order
// word: a sorted tier's added strings go where they sort, an indexed one sends
// each added string's new index, and a string its order moves travels as a
// pair.
// v10: no entry corrections. Every admin / POI / place cell index the old dir
// holds or the new one lists comes as a cell index delta, and the diff refuses
// a new index the patcher couldn't rebuild byte for byte.
// v11: each file's secondary remap pairs come ascending by old id, each id
// once, and the street ways' moved pairs come in the parent-id remap or the
// secondary remap, never both, so the patcher reads every remap pair where it
// sits in the patch.
// v12: every merge section carries string fixups after its offset fixups,
// the new values of the string fields of its MATCH records that the patcher's
// string remap would leave wrong, read in record order. The strings section
// carries at most STRING_PAIRS_MAX moved-string pairs: a patch that moves more
// strings sends none, and fixups instead.
// v13: no place cell index. No dir ships one (a client derives it from
// place_nodes.bin when it loads the dir), so no patch carries its cell changes,
// its cell index delta or the place nodes' secondary remap pairs, which only
// that index's rebuild read; the file ids after PLACE_NODES move down two.
// v14: admin geometry is arcs. File ids 7, 26 and 27 name admin_rings.bin,
// arc_index.bin and arc_vertices.bin (admin_vertices.bin and the postal files
// are gone). The four admin geometry files merge by slot: arc_index.bin and
// admin_polygons.bin record by record, arc_vertices.bin and admin_rings.bin a
// block per record along them, as plain merge sections. Admin polygons keep
// their slots, so nothing is remapped by an admin id: the parent-id remap
// is its street leg alone, the sparse and parallel deltas lose remap kind 1,
// and the admin cell index is rebuilt from the old one's ids as they are.
// v15: a cell index delta (CELL_INDEX_DELTA_MARKER, CELL_LIST_DELTA_STRIDE) is
// runs: the source cells the new index drops by rank, the cells new to it by
// id, then the runs of consecutive cells of the new index that lose or gain an
// id, in varints (append_cell_list_delta). A polygon whose boundary moved costs
// a run, not its id in every cell, and the patcher holds only the ids of the
// runs over its cell, at most 0xFFFF each way. The geo index's GXT2 layout
// (build 19, geo_index.h) keeps v15's sections: file id 35 names its blocks
// file, geo_entries.bin, beside its top file geo_cells.bin (8), and ids 9 to
// 11 name its street, address and interpolation lists, which the geo entry
// deltas correct, not files of their own.
static constexpr uint32_t GCPATCH_VERSION = 15;
// The zstd window a patch is compressed with (8 MiB, level 19's own): a decoder that runs the zstd
// command holds the window, so the diff pins it and the patcher refuses a larger one.
static constexpr int PATCH_ZSTD_WINDOW_LOG = 23;

enum class PatchFileId : uint32_t {
    STRINGS = 0,
    STREET_WAYS = 1,
    STREET_NODES = 2,
    ADDR_POINTS = 3,
    INTERP_WAYS = 4,
    INTERP_NODES = 5,
    ADMIN_POLYGONS = 6,
    ADMIN_RINGS = 7,
    // The geo index: its top file, and (no files) its lists by table.
    GEO_CELLS = 8,
    STREET_LISTS = 9,
    ADDR_LISTS = 10,
    INTERP_LISTS = 11,
    ADMIN_CELLS = 12,
    ADMIN_ENTRIES = 13,
    POI_RECORDS = 14,
    POI_VERTICES = 15,
    POI_CELLS = 16,
    POI_ENTRIES = 17,
    PLACE_NODES = 18,
    // Secondary parallel arrays (SPARSE_DELTA_FILES), the postcode
    // centroid index (a cell list delta) and the admin arcs.
    ADDR_POSTCODES = 19,
    ADMIN_PARENTS = 20,
    WAY_PARENTS = 21,
    WAY_POSTCODES = 22,
    POSTCODE_CENTROIDS = 23,
    POSTCODE_CENTROID_CELLS = 24,
    POSTCODE_CENTROID_ENTRIES = 25,
    ARC_INDEX = 26,
    ARC_VERTICES = 27,
    // addr_points polygon footprints introduced by commit aaf050b
    // (build_version bump 8→9), byte-merged along addr_points like the
    // other vertex streams.
    ADDR_VERTICES = 28,
    // Per-tier strings files (build_version 14). Replaces the single
    // STRINGS=0 slot. They travel in the strings section
    // (STRINGS_TIERED_MARKER); a tier the old dir lacks comes whole in a
    // full-replacement section.
    STRINGS_CORE = 29,
    STRINGS_STREET = 30,
    STRINGS_ADDR = 31,
    STRINGS_POSTCODE = 32,
    STRINGS_POI = 33,
    // Per-segment TIGER ZIP sidecar (u32 string offset per interp way,
    // NO_DATA for OSM interpolations; absent on builds without TIGER).
    INTERP_POSTCODES = 34,
    // The geo index's blocks, beside its top file (GEO_CELLS).
    GEO_ENTRIES = 35,
    COUNT = 36
};

static const char* patch_file_names[] = {
    "strings.bin", "street_ways.bin", "street_nodes.bin", "addr_points.bin",
    "interp_ways.bin", "interp_nodes.bin", "admin_polygons.bin", "admin_rings.bin",
    "geo_cells.bin", "geo street lists", "geo address lists", "geo interpolation lists",
    "admin_cells.bin", "admin_entries.bin",
    "poi_records.bin", "poi_vertices.bin", "poi_cells.bin", "poi_entries.bin",
    "place_nodes.bin",
    "addr_postcodes.bin", "admin_parents.bin", "way_parents.bin", "way_postcodes.bin",
    "postcode_centroids.bin", "postcode_centroid_cells.bin", "postcode_centroid_entries.bin",
    "arc_index.bin", "arc_vertices.bin",
    "addr_vertices.bin",
    "strings_core.bin", "strings_street.bin", "strings_addr.bin",
    "strings_postcode.bin", "strings_poi.bin",
    "interp_postcodes.bin",
    "geo_entries.bin"
};

// The string offset fields of a record file the patch merges, as byte
// offsets in its record; empty for files without any.
// AddrPoint byte 16 is parent_way_id, a way id, not a string offset.
inline std::vector<size_t> string_field_offsets(PatchFileId fid) {
    switch (fid) {
    case PatchFileId::ADDR_POINTS: return {ADDR_POINT_HOUSENUMBER_ID_OFF, ADDR_POINT_STREET_ID_OFF};
    case PatchFileId::STREET_WAYS: return {WAY_HEADER_NAME_ID_OFF};
    case PatchFileId::INTERP_WAYS: return {INTERP_WAY_STREET_ID_OFF};
    case PatchFileId::ADMIN_POLYGONS: return {ADMIN_POLYGON_NAME_ID_OFF};
    case PatchFileId::POI_RECORDS:
        return {POI_RECORD_NAME_ID_OFF, POI_RECORD_PARENT_STREET_ID_OFF, POI_RECORD_PARENT_POSTCODE_ID_OFF};
    case PatchFileId::PLACE_NODES: return {PLACE_NODE_NAME_ID_OFF};
    default: return {};
    }
}

// The files sent as SPARSE_DELTA_STRIDE sections, in emission order, with
// the value each position holds:
//   remap_kind 0 = raw, 2 = string offset,
//   3 = postcode_centroid (16 bytes, string offset at byte 8).
// Admin parents are admin slots, which polygons keep, so they go raw; kind 1
// (an admin id the patch remapped) names no file.
struct SparseDeltaFile { PatchFileId fid; uint32_t value_stride, remap_kind; };
static constexpr SparseDeltaFile SPARSE_DELTA_FILES[] = {
    {PatchFileId::ADDR_POSTCODES, 4, 2},
    {PatchFileId::ADMIN_PARENTS, 4, 0},
    {PatchFileId::WAY_PARENTS, 4, 0},
    {PatchFileId::WAY_POSTCODES, 4, 2},
    {PatchFileId::INTERP_POSTCODES, 4, 2},
    {PatchFileId::POSTCODE_CENTROIDS, 16, 3},
};
static constexpr size_t POSTCODE_CENTROID_POSTCODE_ID_OFF = 8;

inline const SparseDeltaFile* sparse_delta_file(PatchFileId fid) {
    for (const auto& f : SPARSE_DELTA_FILES)
        if (f.fid == fid) return &f;
    return nullptr;
}

// Refuses a sparse delta section unless its file, value stride and remap kind
// are a row of SPARSE_DELTA_FILES, before a byte of it is used.
inline void check_sparse_delta_header(uint32_t fid, uint32_t value_stride, uint32_t remap_kind) {
    const SparseDeltaFile* file = fid < uint32_t(PatchFileId::COUNT) ? sparse_delta_file(static_cast<PatchFileId>(fid)) : nullptr;
    if (!file || value_stride != file->value_stride || remap_kind != file->remap_kind)
        throw std::runtime_error("Malformed sparse delta");
}

// The record files with string fields (string_field_offsets) and their
// strides.
struct StringRecordFile {
    PatchFileId fid;
    size_t stride;
};
static constexpr StringRecordFile STRING_RECORD_FILES[] = {
    {PatchFileId::ADDR_POINTS, ADDR_POINT_STRIDE},
    {PatchFileId::STREET_WAYS, WAY_HEADER_STRIDE},
    {PatchFileId::INTERP_WAYS, INTERP_WAY_STRIDE},
    {PatchFileId::ADMIN_POLYGONS, ADMIN_POLYGON_STRIDE},
    {PatchFileId::POI_RECORDS, POI_RECORD_STRIDE},
    {PatchFileId::PLACE_NODES, PLACE_NODE_STRIDE},
};

// The admin geometry files by family, in the order a patch sends them: a file
// of slots of `stride` bytes, each record's u32 at byte 0 the offset of its
// slot's block in the byte stream after it (a dead arc's block empty, a dead
// polygon's offset NO_DATA). The diff merges each family by slot
// (build_slot_merge_seq), the stream a block per record along its slots.
struct AdminSlotFamily {
    PatchFileId slots, blocks;
    size_t stride;
};
static constexpr AdminSlotFamily ADMIN_SLOT_FAMILIES[] = {
    {PatchFileId::ARC_INDEX, PatchFileId::ARC_VERTICES, ARC_INDEX_STRIDE},
    {PatchFileId::ADMIN_POLYGONS, PatchFileId::ADMIN_RINGS, ADMIN_POLYGON_STRIDE},
};

// Terminator written after the last patch section; the patcher breaks its
// section loop when it reads this as a file_id and refuses a patch that ends
// without it. Same bytes as NO_DATA, but a distinct concept.
static constexpr uint32_t SECTION_END_MARKER = 0xFFFFFFFFu;

// Cell changes section marker: 0xFFFFFFFB
// Format: marker, num_added(u32), num_removed(u32),
//         [added_cell_id(u64)] * num_added, [removed_cell_id(u64)] * num_removed
// For both geo and admin cell sets. Each list ascends (equal ids allowed:
// diff_cell_ids gives multisets); the patcher reads it in place (AscendingIds).
static constexpr uint32_t CELL_CHANGES_GEO_MARKER = 0xFFFFFFFB;
static constexpr uint32_t CELL_CHANGES_ADMIN_MARKER = 0xFFFFFFFA;
static constexpr uint32_t CELL_CHANGES_POI_MARKER = 0xFFFFFFF5;

// Cell index delta marker: 0xFFFFFFF7
// An admin / POI cell index the patcher rebuilds from the old one through
// its id remap (the POI records' replay; admin slots stay as they are),
// corrected by runs of cells that lose or gain an id (v15): a polygon costs
// the runs its cells form in the index, not an id per cell. Sent for every
// index the old or the new dir holds; the patcher writes the new index
// through a cell_pairs::Writer, so the diff refuses a new index that writer
// doesn't reproduce byte for byte.
// Format: marker(4), file_id(4) = the entries file, payload_size(u64),
//   append_cell_list_delta(rebuilt → new) bytes.
// At most one per file of CELL_INDEX_ENTRIES_FILES.
static constexpr uint32_t CELL_INDEX_DELTA_MARKER = 0xFFFFFFF7;

// Geo entry delta marker: 0xFFFFFFF0
// The street / addr / interp lists the patcher derives from the old index
// and the record remaps, corrected per cell (v6; full new lists before).
// Format: marker(4), file_id(4), count(4), then per cell, in cell order:
//   cell_id(u64), n_lost(u16), n_gained(u16), lost ids, gained ids.
// n_lost == GEO_LIST_REPLACE: the gained ids are the cell's whole new list
// (a list that isn't sorted, or a delta that wouldn't fit).
// At most one per file of GEO_ENTRIES_FILES.
static constexpr uint32_t GEO_ENTRY_DELTA_MARKER = 0xFFFFFFF0;
static constexpr uint16_t GEO_LIST_REPLACE = 0xFFFF;

// The geo index's lists the geo walk derives and corrects (in its table
// order, geo_index.h), and the entries files of the cell indexes rebuilt
// from an id remap.
template <size_t N>
using EntriesFiles = std::array<PatchFileId, N>;
static constexpr EntriesFiles<3> GEO_ENTRIES_FILES = {PatchFileId::STREET_LISTS, PatchFileId::ADDR_LISTS,
                                                      PatchFileId::INTERP_LISTS};
static constexpr EntriesFiles<2> CELL_INDEX_ENTRIES_FILES = {PatchFileId::ADMIN_ENTRIES, PatchFileId::POI_ENTRIES};

// Whether a file id names one of the geo index's lists (GEO_ENTRIES_FILES):
// no file, so no file section's.
inline bool is_geo_list_id(uint32_t id) {
    return std::find(GEO_ENTRIES_FILES.begin(), GEO_ENTRIES_FILES.end(), static_cast<PatchFileId>(id)) !=
           GEO_ENTRIES_FILES.end();
}

// Secondary ID remap marker: 0xFFFFFFF6
// Additional old→new ID mappings for modified records (recovered by relaxed key matching).
// Both diff and patch tools must apply these to their derived remaps for consistent results.
// Format: marker(4), n_files(4),
//   for each: file_id(4), n_pairs(4), [(old_id:u32, new_id:u32)] × n_pairs
// Each file's pairs ascending by old_id, each id once (v11); the patcher
// reads them where they sit and refuses a file's pairs out of order or with
// an old id twice, pairs for a file not in REMAPPED_RECORD_FILES, a second
// list with pairs for a file (replayed or sent unchanged), and street_ways
// pairs when the parent-id remap's street leg has some.
static constexpr uint32_t SECONDARY_REMAP_MARKER = 0xFFFFFFF6;

// The record files whose old → new record ids the patcher keeps (RecordRemap)
// for the entry pipeline's geo and POI indexes: the secondary remap gives
// pairs for these and no other (admin polygons keep their slots).
static constexpr PatchFileId REMAPPED_RECORD_FILES[] = {PatchFileId::STREET_WAYS, PatchFileId::ADDR_POINTS,
                                                        PatchFileId::INTERP_WAYS, PatchFileId::POI_RECORDS};
inline bool is_remapped_record_file(uint32_t fid) {
    return std::find(std::begin(REMAPPED_RECORD_FILES), std::end(REMAPPED_RECORD_FILES), static_cast<PatchFileId>(fid)) !=
           std::end(REMAPPED_RECORD_FILES);
}

// The strings section, read by position right after the old file sizes
// (UNSECTIONED_OLD_FILES). Its marker shares 0xFFFFFFF6 with
// SECONDARY_REMAP_MARKER: position, not value, tells them apart.
//   marker STRINGS_TIERED_MARKER,
//   5 × {old_size u32, new_size u32}: every tier of the build, so global
//     offsets (tier t at the sum of the sizes before it) need no tier file,
//   shipped u32: bit t = the dir ships tier t and rebuilds it from its own
//     old file (a tier the old dir lacked is unshipped here and its client
//     file comes whole in a per-file section),
//   sent u32: bit t = runs follow for tier t, which the dir doesn't ship but
//     its own old files reference,
//   per tier t in order:
//     shipped: old_hash u64, new_hash u64, n_added u32, n_deleted u32,
//              order u32 (STRING_TIER_SORTED: the new pool sorts and the
//              added strings go where they sort; STRING_TIER_INDEXED: their
//              new string indices follow them), added strings
//              (NUL-terminated), indexed only: their new string indices
//              (u32, ascending), deleted old string indices (u32,
//              ascending); the patcher rebuilds the tier and derives its
//              shift runs from the same walk (rebuild_string_tier),
//     sent: n_runs u32, runs_size u32, the tier's shift runs
//           (encode_string_tier_runs),
//   n_pairs u32, (old u32, new u32) × n_pairs: surviving strings no tier's
//     edit keeps, which changed tier or order (ascending by old, each string
//     once), for every old tier the dir ships or references; at most
//     STRING_PAIRS_MAX. With more, n_pairs is 0 and the records that hold
//     them carry their new values (string fixups, StringFixupReader).
// The patcher reads every list where it sits in the patch.
// A tier neither shipped nor referenced sends nothing: no lookup lands in it.
static constexpr uint32_t STRINGS_TIERED_MARKER = 0xFFFFFFF6;

// 64-bit content hash, 8 bytes per step, fed piece by piece: seeded with the
// total length n, it carries a step's bytes left over from one piece to the
// next, so hash() of n bytes fed in any split is content_hash of them. Not
// cryptographic: it proves a file is the one the diff saw, against stale or
// mismatched inputs.
class ContentHasher {
public:
    explicit ContentHasher(uint64_t n) : h_(0x9E3779B97F4A7C15ull ^ n) {}
    void update(const char* p, size_t n) {
        if (n == 0) return;
        if (carried_ > 0) {
            const size_t k = std::min(n, sizeof carry_ - carried_);
            memcpy(carry_ + carried_, p, k);
            carried_ += k;
            p += k;
            n -= k;
            if (carried_ < sizeof carry_) return;
            h_ = step(h_, carry_);
            carried_ = 0;
        }
        uint64_t h = h_;
        for (; n >= sizeof carry_; p += sizeof carry_, n -= sizeof carry_) h = step(h, p);
        h_ = h;
        memcpy(carry_, p, n);
        carried_ = n;
    }
    uint64_t hash() const {
        uint64_t tail = 0;
        memcpy(&tail, carry_, carried_);
        const uint64_t h = (h_ ^ tail) * 0xC4CEB9FE1A85EC53ull;
        return h ^ (h >> 29);
    }

private:
    static uint64_t step(uint64_t h, const char* p) {
        uint64_t w;
        memcpy(&w, p, sizeof w);
        h = (h ^ w) * 0xFF51AFD7ED558CCDull;
        return h ^ (h >> 32);
    }

    uint64_t h_;
    char carry_[8];
    size_t carried_ = 0;
};

inline uint64_t content_hash(const char* data, size_t n) {
    ContentHasher h(n);
    h.update(data, n);
    return h.hash();
}

// Sparse position-keyed delta. Stride sentinel that signals the section
// payload is a list of (position, value) pairs for the positions where
// new differs from old. Used for files where strategy-2 keeps the index
// stable and the values are slots or string offsets (e.g. postcode
// string offsets shift with the string tiers; the diff applies the string
// remap to the loaded old array before computing the delta, and the
// patcher applies the same remap before overwriting the changed
// positions). Daily delta drops from a full replace (hundreds of MiB per
// file) to tens of KiB.
//
// Section format:
//   file_id(u32),
//   stride = SPARSE_DELTA_STRIDE (u32),
//   old_size(u64), new_size(u64),
//   value_stride(u32) — bytes per record (4 for uint32 arrays, 16 for postcode_centroid),
//   remap_kind(u32)  — 0=none, 2=string offset, 3=postcode_centroid struct (SPARSE_DELTA_FILES),
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

// Stride sentinel: a cell index (<name>_cells.bin + <name>_entries.bin)
// patched by runs (v15). Strategy-2 ids keep most lists unchanged day to day,
// so only the runs of cells that lost or gained ids travel. The section names
// the cells file and the patcher writes both files.
//   file_id(u32), stride = CELL_LIST_DELTA_STRIDE (u32),
//   old_size(u64), new_size(u64) of the cells file,
//   payload_size(u64), new entries size(u64), append_cell_list_delta bytes.
constexpr uint32_t CELL_LIST_DELTA_STRIDE = 0xFB;

// Stride sentinel (v8): a per-record side file (one value per record of a
// parent file: way_parents / way_postcodes per street way, addr_postcodes per
// address, interp_postcodes per interpolation) rebuilt along the parent's
// merge sequence: MATCH and REPLACE copy the old record's value (remapped like
// the sparse delta's remap_kind), INSERT takes the next inline value, DELETE
// skips one; then (pos, value) pairs at new positions overwrite the values
// that changed. A parent sent unchanged counts as one MATCH of every record.
//   file_id(u32), stride = PARALLEL_DELTA_STRIDE (u32), old_size(u64), new_size(u64),
//   parent_file_id(u32), value_stride(u32) = 4, remap_kind(u32),
//   n_inserted(u32) + [value:u32] x n_inserted,
//   n_changes(u32) + [(pos:u32, value:u32)] x n_changes (pos ascending)
constexpr uint32_t PARALLEL_DELTA_STRIDE = 0xFA;
// The widest record a merge section may give (the patcher sizes a record buffer by it): the
// widest real record is 36 bytes, and every stride from 0xFA up to here is a section kind.
static constexpr uint32_t MAX_RECORD_STRIDE = 0xFF;
// The most records a merge section's old or new file may hold: record indices are u32s below
// NO_DATA (0xFFFFFFFF) in the diff's id remaps and secondary matches and in the patcher's
// RecordRemap, so a file of more would wrap them. A byte stream's records are its bytes. Lower for
// tests (--file-records-max).
static constexpr uint32_t FILE_RECORDS_MAX = 0xFFFFFFFE;

// Refuses a merge of `file` whose old or new file (of old_size and new_size bytes) holds more than
// max records of stride bytes: the diff before it diffs the file, the patcher before it replays the
// section.
inline void require_merge_records(const std::string& file, uint64_t old_size, uint64_t new_size, uint64_t stride,
                                  uint32_t max) {
    for (const auto& [side, size] : {std::pair<const char*, uint64_t>{"Old", old_size}, {"New", new_size}})
        if (size / stride > max)
            throw std::runtime_error(std::string(side) + " " + file + " holds " + std::to_string(size / stride) +
                                     " records, more than the " + std::to_string(max) + " a patch can index");
}

// A tool option's value of 0 to max, in out; false (said) when it is not one.
inline bool u32_option(const std::string& option, const std::string& n, uint32_t max, uint32_t& out) {
    if (n.empty() || n.size() > 10 || n.find_first_not_of("0123456789") != std::string::npos || std::stoull(n) > max) {
        std::cerr << option << " takes 0 to " << max << ", not " << n << std::endl;
        return false;
    }
    out = static_cast<uint32_t>(std::stoull(n));
    return true;
}
// The patcher writes a MATCH run's rewritten records this many bytes at a time.
static constexpr size_t RECORD_BATCH_BYTES = size_t(16) << 10;
static_assert(RECORD_BATCH_BYTES >= MAX_RECORD_STRIDE, "a batch holds at least one record");
struct ParallelDeltaFile { PatchFileId fid, parent; uint32_t value_stride, remap_kind; };
static constexpr ParallelDeltaFile PARALLEL_DELTA_FILES[] = {
    {PatchFileId::ADDR_POSTCODES, PatchFileId::ADDR_POINTS, 4, 2},
    {PatchFileId::WAY_PARENTS, PatchFileId::STREET_WAYS, 4, 0},
    {PatchFileId::WAY_POSTCODES, PatchFileId::STREET_WAYS, 4, 2},
    {PatchFileId::INTERP_POSTCODES, PatchFileId::INTERP_WAYS, 4, 2},
};
inline const ParallelDeltaFile* parallel_delta_file(PatchFileId fid) {
    for (const auto& f : PARALLEL_DELTA_FILES)
        if (f.fid == fid) return &f;
    return nullptr;
}

// Validate a side-file section before using its parent as an array index.
inline void check_parallel_delta_header(uint32_t fid, uint32_t parent_fid,
                                       uint32_t value_stride, uint32_t remap_kind) {
    const auto* file = fid < uint32_t(PatchFileId::COUNT)
        ? parallel_delta_file(static_cast<PatchFileId>(fid)) : nullptr;
    if (!file || parent_fid != uint32_t(file->parent) || value_stride != file->value_stride ||
        remap_kind != file->remap_kind)
        throw std::runtime_error("Malformed parallel delta");
}

// Closes outputs, every one even when another fails, then throws naming those
// whose writes or close failed: a buffered write can succeed even when the
// final flush fails, and an encoded output's encoder takes its last bytes in
// the close.
inline void close_patch_outputs(std::initializer_list<std::pair<FILE*, std::string>> outs) {
    std::string bad;
    for (const auto& [f, name] : outs) {
        const bool ok = !ferror(f);
        if (fclose(f) != 0 || !ok) bad += (bad.empty() ? "" : ", ") + name;
    }
    if (!bad.empty()) throw std::runtime_error("Cannot write " + bad);
}

// POI parent-id remap marker: 0xFFFFFFF3
// The street leg (v14: the only one): the old → new ids of the street ways no
// MATCH or REPLACE run of their replay maps (moved or secondary-matched ways),
// which complete the street remap the address replay moves parent_way_id by.
// Sent, before the street ways' section, only by a dir with address points;
// any other dir's moved ways go in the secondary remap.
// Format: marker(4), n_street_pairs(4), [(old_id:u32, new_id:u32)] × n_street_pairs,
// ascending by old_id, each id once; the patcher reads them where they sit
// (parse_parent_id_remap).
static constexpr uint32_t POI_PARENT_REMAP_MARKER = 0xFFFFFFF3;

// --- Reading a part of the patch ---

// How a SectionReader words its refusal of a read past its end.
enum class PastEnd {
    TRUNCATED,     // "Truncated <name>"
    MALFORMED,     // "Malformed <name>", as the part's other faults are
    TRUNCATED_AT,  // "Truncated <name>: need <n> bytes for <what> at offset <pos> (size <size>)"
};

// Reads a part of the patch at data[pos, size) front to back, moving the
// caller's pos past each value it reads, and refuses a read that would pass
// size. A length is checked in 64 bits before it narrows to size_t: a 32-bit
// build would wrap a length the patch gives (a u64, or a count times its item
// size) into one the part has.
class SectionReader {
public:
    SectionReader(const char* data, size_t size, size_t& pos, const char* name, PastEnd past_end = PastEnd::TRUNCATED)
        : data_(data), size_(size), pos_(pos), name_(name), past_end_(past_end) {}

    // Refuses unless the next n bytes are there.
    void require(uint64_t n, const char* what = "bytes") const {
        if (pos_ > size_ || n > size_ - pos_) refuse_past_end(name_, past_end_, n, what, pos_, size_);
    }
    // The next n bytes, where they sit.
    const char* take(uint64_t n, const char* what = "bytes") {
        require(n, what);
        const char* p = data_ + pos_;
        pos_ += static_cast<size_t>(n);
        return p;
    }
    // The next count items of item bytes each, where they sit.
    const char* take_items(uint32_t count, size_t item, const char* what = "items") {
        return take(uint64_t{count} * item, what);
    }
    uint8_t u8() { return value<uint8_t>("u8"); }
    uint16_t u16() { return value<uint16_t>("u16"); }
    uint32_t u32() { return value<uint32_t>("u32"); }
    uint64_t u64() { return value<uint64_t>("u64"); }
    // A u32 as a varint (write_varint). One longer than 5 bytes, or of 2^32
    // or more, is malformed.
    uint32_t varint() {
        uint32_t v = 0;
        for (uint32_t bit = 0; bit < 35; bit += 7) {
            if (pos_ >= size_) refuse_past_end(name_, past_end_, 1, "varint", pos_, size_);
            const uint8_t byte = static_cast<uint8_t>(data_[pos_++]);
            if (bit == 28 && (byte & 0x70)) malformed();
            v |= static_cast<uint32_t>(byte & 0x7F) << bit;
            if (!(byte & 0x80)) return v;
        }
        malformed();
    }
    // A u64 as a varint (write_varint64). One longer than 10 bytes, or of 2^64
    // or more, is malformed.
    uint64_t varint64() {
        uint64_t v = 0;
        for (uint32_t bit = 0; bit < 70; bit += 7) {
            if (pos_ >= size_) refuse_past_end(name_, past_end_, 1, "varint", pos_, size_);
            const uint8_t byte = static_cast<uint8_t>(data_[pos_++]);
            if (bit == 63 && (byte & 0x7E)) malformed();
            v |= static_cast<uint64_t>(byte & 0x7F) << bit;
            if (!(byte & 0x80)) return v;
        }
        malformed();
    }
    // A NUL-terminated string, where it sits.
    const char* cstring() {
        const void* nul = pos_ < size_ ? memchr(data_ + pos_, '\0', size_ - pos_) : nullptr;
        if (!nul) refuse_past_end(name_, past_end_, uint64_t{left()} + 1, "string", pos_, size_);
        const char* s = data_ + pos_;
        pos_ = static_cast<size_t>(static_cast<const char*>(nul) - data_) + 1;
        return s;
    }
    size_t left() const { return pos_ < size_ ? size_ - pos_ : 0; }
    bool at_end() const { return pos_ == size_; }
    [[noreturn]] void malformed() const { refuse_malformed(name_); }

private:
    template <typename T>
    T value(const char* what) {
        T v;
        memcpy(&v, take(sizeof v, what), sizeof v);
        return v;
    }
    // The refusals are out of line and given the reader's fields rather than
    // the reader, so a read that fits keeps them in registers (an offset
    // fixup's varints are read per record).
    [[noreturn, gnu::cold, gnu::noinline]] static void refuse_malformed(const char* name) {
        throw std::runtime_error(std::string("Malformed ") + name);
    }
    [[noreturn, gnu::cold, gnu::noinline]] static void refuse_past_end(const char* name, PastEnd past_end, uint64_t n,
                                                                        const char* what, size_t pos, size_t size) {
        if (past_end == PastEnd::MALFORMED) refuse_malformed(name);
        std::string refusal = std::string("Truncated ") + name;
        if (past_end == PastEnd::TRUNCATED_AT)
            refusal += ": need " + std::to_string(n) + " bytes for " + what + " at offset " + std::to_string(pos) +
                       " (size " + std::to_string(size) + ")";
        throw std::runtime_error(refusal);
    }

    const char* data_;
    size_t size_;
    size_t& pos_;
    const char* name_;
    PastEnd past_end_;
};

// --- Client file set (v4) ---
// The files a client holds for a variant: everything in the variant dir except
// strategy-2 sidecars (*.osm_ids, server-side cache only), transport artifacts
// (*.gcpatch, *.zst) and dotfiles. Read by position right after the header:
//   marker(4), n(4), n × {name_len(u16), name, size(u64), inline(u8), [bytes]}
// sorted by name. Non-.bin files (strings_layout.json, poi_meta.json) are
// inline; the patcher writes them verbatim and every listed file must exist at
// its listed size after the apply. Files it builds that are not listed (sections
// for files the variant lacks) stay in its scratch dir.
static constexpr uint32_t CLIENT_FILES_MARKER = 0xFFFFFFF2;
static constexpr size_t MAX_INLINE_CLIENT_FILE_BYTES = 1 << 20;
// The most files a client file list may name (a variant holds a few dozen), and the longest
// name (a file name's NAME_MAX).
static constexpr uint32_t MAX_CLIENT_FILES = 1024;
static constexpr size_t MAX_CLIENT_FILE_NAME = 255;

struct ClientFile {
    std::string name;
    uint64_t size = 0;
    const char* bytes = nullptr;  // an inline file's contents: its size bytes, where they sit
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

// Appends the section for files, a list parse_client_files takes back: it refuses, in
// parse_client_files' words and before appending a byte, more than MAX_CLIENT_FILES files, a name
// longer than MAX_CLIENT_FILE_NAME bytes or not a client file's, a name out of order or listed
// twice, and an inline file larger than MAX_INLINE_CLIENT_FILE_BYTES. A diff never writes a patch
// the patcher would refuse.
inline void append_client_files(std::vector<char>& out, const std::vector<ClientFile>& files) {
    if (files.size() > MAX_CLIENT_FILES) throw std::runtime_error("Too many client files: " + std::to_string(files.size()));
    for (size_t i = 0; i < files.size(); i++) {
        const ClientFile& f = files[i];
        if (f.name.size() > MAX_CLIENT_FILE_NAME)
            throw std::runtime_error("Client file name too long: " + std::to_string(f.name.size()) + " bytes");
        if (!is_client_file(f.name)) throw std::runtime_error("Bad client file name: " + f.name);
        if (i > 0 && !(files[i - 1].name < f.name)) throw std::runtime_error("Client file listed out of order or twice: " + f.name);
        if (is_inline_client_file(f.name) && f.size > MAX_INLINE_CLIENT_FILE_BYTES)
            throw std::runtime_error("Inline client file too large: " + f.name);
    }
    auto put = [&](const void* p, size_t n) { out.insert(out.end(), (const char*)p, (const char*)p + n); };
    uint32_t marker = CLIENT_FILES_MARKER, n = static_cast<uint32_t>(files.size());
    put(&marker, 4); put(&n, 4);
    for (const auto& f : files) {
        uint16_t len = static_cast<uint16_t>(f.name.size());
        uint8_t is_inline = is_inline_client_file(f.name) ? 1 : 0;
        put(&len, 2); put(f.name.data(), len); put(&f.size, 8); put(&is_inline, 1);
        if (is_inline) put(f.bytes, f.size);
    }
}

// Parses the section at data[pos, size) and advances pos past it. The names come sorted, each
// once; an inline file's bytes stay where they sit.
inline std::vector<ClientFile> parse_client_files(const char* data, size_t size, size_t& pos) {
    SectionReader in(data, size, pos, "client file list");
    if (in.u32() != CLIENT_FILES_MARKER) throw std::runtime_error("Patch has no client file list");
    const uint32_t n = in.u32();
    if (n > MAX_CLIENT_FILES) throw std::runtime_error("Too many client files: " + std::to_string(n));
    in.require(uint64_t{n} * 11);  // 11 = smallest entry
    std::vector<ClientFile> files(n);
    for (size_t i = 0; i < files.size(); i++) {
        ClientFile& f = files[i];
        const uint16_t len = in.u16();
        if (len > MAX_CLIENT_FILE_NAME) throw std::runtime_error("Client file name too long: " + std::to_string(len) + " bytes");
        f.name.assign(in.take(len), len);
        if (!is_client_file(f.name)) throw std::runtime_error("Bad client file name: " + f.name);
        if (i > 0 && !(files[i - 1].name < f.name)) throw std::runtime_error("Client file listed out of order or twice: " + f.name);
        f.size = in.u64();
        const uint8_t is_inline = in.u8();
        if (is_inline != (is_inline_client_file(f.name) ? 1 : 0))
            throw std::runtime_error("Bad client file entry: " + f.name);
        if (!is_inline) continue;
        if (f.size > MAX_INLINE_CLIENT_FILE_BYTES) throw std::runtime_error("Inline client file too large: " + f.name);
        f.bytes = in.take(f.size);
    }
    return files;
}

// --- Old sizes of the files no section names (v7) ---
// Every section names the size of the old file it replays. The patcher also
// reads old files no section names: the cell indexes of the entry pipeline
// and the entries file beside a cell list delta. Right after the client
// files, the diff records their sizes in the old dir (0 when absent):
//   n(4), n × {file_id(4), old_size(8)}
// and the patcher checks them before it reads anything else.
static constexpr PatchFileId UNSECTIONED_OLD_FILES[] = {
    PatchFileId::GEO_CELLS, PatchFileId::GEO_ENTRIES,
    PatchFileId::ADMIN_CELLS, PatchFileId::ADMIN_ENTRIES, PatchFileId::POI_CELLS, PatchFileId::POI_ENTRIES,
    PatchFileId::POSTCODE_CENTROID_ENTRIES,
};

inline void append_old_file_sizes(std::vector<char>& out, const std::string& old_dir) {
    auto put = [&](const void* p, size_t n) { out.insert(out.end(), (const char*)p, (const char*)p + n); };
    uint32_t n = static_cast<uint32_t>(std::size(UNSECTIONED_OLD_FILES));
    put(&n, 4);
    for (PatchFileId fid : UNSECTIONED_OLD_FILES) {
        struct stat st;
        uint32_t id = static_cast<uint32_t>(fid);
        uint64_t size = stat((old_dir + "/" + patch_file_names[id]).c_str(), &st) == 0 ? static_cast<uint64_t>(st.st_size) : 0;
        put(&id, 4); put(&size, 8);
    }
}

// Reads the sizes at data[pos, size), advances pos past them and checks each
// against cur_dir (require_old_file_size).
inline void check_old_file_sizes(const char* data, size_t size, size_t& pos, const std::string& cur_dir) {
    SectionReader in(data, size, pos, "old file sizes");
    const uint32_t n = in.u32();
    for (uint32_t i = 0; i < n; i++) {
        const uint32_t id = in.u32();
        const uint64_t old_size = in.u64();
        if (id >= static_cast<uint32_t>(PatchFileId::COUNT)) in.malformed();
        require_old_file_size(cur_dir, patch_file_names[id], old_size);
    }
}

// --- Patch sections read in place ---

// Where a section's bytes sit in the mapped patch.
struct PatchRef {
    size_t pos, size;
};

// The sections of one kind the diff sends at most once per entries file
// (GEO_ENTRIES_FILES, CELL_INDEX_ENTRIES_FILES), in one fixed slot per file:
// a section for another file, or a second one for a file, is malformed.
template <size_t N>
class EntriesFileSections {
public:
    EntriesFileSections(const char* kind, const EntriesFiles<N>& files) : kind_(kind), files_(files) {}

    void add(uint32_t fid, PatchRef ref) {
        const size_t slot = slot_of(fid);
        if (slot == files_.size()) throw std::runtime_error(std::string("Malformed ") + kind_ + " for file " + std::to_string(fid));
        if (refs_[slot]) throw std::runtime_error(std::string("Malformed ") + kind_ + ": a second one for " + patch_file_names[fid]);
        refs_[slot] = ref;
    }
    // The file's section, or null when the patch sent none.
    const PatchRef* find(PatchFileId fid) const {
        const size_t slot = slot_of(static_cast<uint32_t>(fid));
        return slot < files_.size() && refs_[slot] ? &*refs_[slot] : nullptr;
    }

private:
    size_t slot_of(uint32_t fid) const {
        return static_cast<size_t>(std::find(files_.begin(), files_.end(), static_cast<PatchFileId>(fid)) - files_.begin());
    }

    const char* kind_;
    EntriesFiles<N> files_;
    std::array<std::optional<PatchRef>, N> refs_;
};

// u64 cell ids read where they sit, front to back by one walk: a cell change
// list or a cell list delta's removed cells. The diff writes them ascending,
// equal ids allowed (diff_cell_ids gives multisets); a step back is refused
// as a malformed `what` when the walk reaches it, or by require_ascending()
// among the ids the walk left. A copy walks on its own.
class AscendingIds {
public:
    AscendingIds() = default;  // none
    AscendingIds(const char* data, size_t count, const char* what) : data_(data), count_(count), what_(what) { load(); }

    const char* data() const { return data_; }
    size_t bytes() const { return count_ * sizeof(uint64_t); }
    bool done() const { return at_ == count_; }
    // The id at the cursor, while not done().
    uint64_t front() const { return id_; }
    void pop() {
        at_++;
        load();
    }
    // Moves past the ids below id: whether the next one is id.
    bool skip_to(uint64_t id) {
        while (!done() && id_ < id) pop();
        return !done() && id_ == id;
    }
    // Reads the ids the walk left (past the last cell it asked for).
    void require_ascending() {
        while (!done()) pop();
    }

private:
    void load() {
        if (done()) return;
        uint64_t id;
        memcpy(&id, data_ + at_ * sizeof id, sizeof id);
        if (at_ > 0 && id < id_) throw std::runtime_error(std::string("Malformed ") + what_);
        id_ = id;
    }

    const char* data_ = nullptr;
    size_t count_ = 0, at_ = 0;
    uint64_t id_ = 0;
    const char* what_ = "";
};

// The keys a SortedTable samples for its directory: 16 KiB at most.
static constexpr size_t TABLE_DIRECTORY_KEYS = 4096;
// A SortedTable's key filter: 2^15 bits, 4 KiB. A normal day's tables (90
// moved strings, a parent id or two) set under 1% of them, so nearly every
// lookup in them ends at the filter.
static constexpr unsigned TABLE_FILTER_BITS_LOG2 = 15;
// The buffer a ScratchTable appends through: 64 KiB.
static constexpr size_t SCRATCH_TABLE_BUF = 64 << 10;
// A shipped string tier's walk gives back its old file's body in steps of at
// least this: 64 KiB.
static constexpr size_t STRING_WALK_PIECE = 64 << 10;
// The most moved-string pairs a strings section carries (512 KiB of them),
// which the patcher looks up at random where they sit. A normal day moves a
// few hundred strings; a day that moves more sends them as string fixups,
// which the replays read in record order.
static constexpr uint32_t STRING_PAIRS_MAX = 65536;

// n fixed-width records read where they sit, sorted strictly ascending by
// the u32 each starts with. The constructor checks the order in one sweep,
// refusing a step back or a repeated key as a malformed `what`, sets each
// key's bit in a fixed filter, which turns away most keys a small table
// lacks, and samples every step-th key into a directory, so a lookup the
// filter passes, or a floor, searches one step of the records.
template <typename Rec>
class SortedTable {
    static_assert(std::is_trivially_copyable<Rec>::value && sizeof(Rec) >= sizeof(uint32_t), "a record starts with its u32 key");

public:
    SortedTable() = default;  // none
    SortedTable(const char* data, size_t count, const char* what) : data_(data), count_(count) {
        step_ = std::max<size_t>(1, (count + TABLE_DIRECTORY_KEYS - 1) / TABLE_DIRECTORY_KEYS);
        directory_.reserve((count + step_ - 1) / step_);
        if (count > 0) filter_.assign((size_t{1} << TABLE_FILTER_BITS_LOG2) / 64, 0);
        for (size_t i = 0; i < count; i++) {
            const uint32_t key = key_at(i);
            if (i > 0 && key <= key_at(i - 1)) throw std::runtime_error(std::string("Malformed ") + what);
            if (i % step_ == 0) directory_.push_back(key);
            const uint32_t bit = filter_bit(key);
            filter_[bit / 64] |= uint64_t{1} << (bit % 64);
        }
    }

    const char* data() const { return data_; }
    size_t size() const { return count_; }
    bool empty() const { return count_ == 0; }
    Rec operator[](size_t i) const {
        Rec r;
        memcpy(&r, data_ + i * sizeof(Rec), sizeof(Rec));
        return r;
    }
    template <typename OnRecord>
    void for_each(OnRecord on_record) const {
        for (size_t i = 0; i < count_; i++) on_record((*this)[i]);
    }
    // Whether the table may hold `key`: true for every key it holds.
    bool might_hold(uint32_t key) const {
        if (count_ == 0) return false;
        const uint32_t bit = filter_bit(key);
        return filter_[bit / 64] >> (bit % 64) & 1;
    }
    // The record of `key`, if the table holds one.
    std::optional<Rec> find(uint32_t key) const {
        if (!might_hold(key)) return std::nullopt;
        const size_t at = search(key);
        if (at == count_) return std::nullopt;
        return (*this)[at];
    }
    // The index of the last record whose key is at most `key`, or size()
    // when every key is above it.
    size_t floor(uint32_t key) const { return floor_of(key).at; }
    // floor, trying the hinted record and the one after it first, then
    // leaving the floor in hint: lookups that move forward through the
    // table skip the search.
    size_t floor(uint32_t key, size_t& hint) const {
        if (hint < count_ && key_at(hint) <= key) {
            if (hint + 1 == count_ || key < key_at(hint + 1)) return hint;
            if (hint + 2 == count_ || key < key_at(hint + 2)) return ++hint;
        }
        return hint = floor(key);
    }

private:
    static uint32_t filter_bit(uint32_t key) {
        return static_cast<uint32_t>((key * 0x9E3779B97F4A7C15ull) >> (64 - TABLE_FILTER_BITS_LOG2));
    }
    // The last record whose key is at most `key`, and that key; at is count_
    // for none. A table of at most TABLE_DIRECTORY_KEYS records keeps every
    // key in its directory, so it reads none of its records.
    struct Floor {
        size_t at;
        uint32_t key;
    };
    Floor floor_of(uint32_t key) const {
        const size_t d = static_cast<size_t>(std::upper_bound(directory_.begin(), directory_.end(), key) - directory_.begin());
        if (d == 0) return {count_, 0};
        Floor f{(d - 1) * step_, directory_[d - 1]};
        if (f.key == key) return f;
        for (size_t hi = std::min(count_, f.at + step_); hi - f.at > 1;) {
            const size_t mid = f.at + (hi - f.at) / 2;
            const uint32_t k = key_at(mid);
            if (k <= key) f = {mid, k};
            else hi = mid;
        }
        return f;
    }
    // The index of the record of `key`, or count_. Out of line, so that a
    // lookup the filter rejects costs its caller no call.
    [[gnu::noinline]] size_t search(uint32_t key) const {
        const Floor f = floor_of(key);
        return f.at < count_ && f.key == key ? f.at : count_;
    }
    uint32_t key_at(size_t i) const {
        uint32_t key;
        memcpy(&key, data_ + i * sizeof(Rec), sizeof key);
        return key;
    }

    const char* data_ = nullptr;
    size_t count_ = 0, step_ = 1;
    std::vector<uint32_t> directory_;  // the key of every step_-th record
    std::vector<uint64_t> filter_;     // a bit per filter_bit, none when empty
};

// How a ScratchTable closes its file: close(), or a test's stand-in that
// fails as a filesystem that flushes late writes (NFS, FUSE) may.
inline int (*&scratch_table_close())(int) {
    static int (*f)(int) = ::close;
    return f;
}

// A SortedTable made record by record: records appended in strictly
// ascending key order go through a SCRATCH_TABLE_BUF buffer to an unnamed
// file in dir (the patcher's scratch dir), and finish() maps the file
// read-only as the table, so the heap holds the buffer while the table
// fills and only the directory and filter after. A key at or below the
// last one is refused as a malformed `what`, and a file that fails to close
// as one it cannot write.
template <typename Rec>
class ScratchTable {
public:
    ScratchTable() = default;  // none: appending throws
    ScratchTable(std::string dir, const char* what) : dir_(std::move(dir)), what_(what) {}
    ScratchTable(ScratchTable&& o) noexcept { swap(o); }
    ScratchTable& operator=(ScratchTable&& o) noexcept {
        swap(o);
        return *this;
    }
    ~ScratchTable() {
        if (fd_ >= 0) close(fd_);
        if (map_) munmap(map_, map_size_);
    }

    void append(const Rec& rec) {
        uint32_t key;
        memcpy(&key, &rec, sizeof key);
        if (count_ > 0 && key <= last_key_) throw std::runtime_error(std::string("Malformed ") + what_);
        if (buf_.empty()) buf_.reserve(SCRATCH_TABLE_BUF / sizeof(Rec));
        buf_.push_back(rec);
        if (buf_.size() == buf_.capacity()) write_buffer();
        last_key_ = key;
        count_++;
    }
    // The records appended so far.
    size_t size() const { return count_; }
    // Ends the appends: maps what the file holds, the table from here on.
    const SortedTable<Rec>& finish() {
        write_buffer();
        std::vector<Rec>().swap(buf_);
        if (fd_ < 0) return table_;
        const std::optional<size_t> size = checked_size(0, count_, sizeof(Rec), UINT64_MAX);
        void* p = size ? mmap(nullptr, *size, PROT_READ, MAP_PRIVATE, fd_, 0) : MAP_FAILED;
        // A filesystem that flushes late may report a failed write only now.
        const int closed = scratch_table_close()(fd_), close_err = errno;
        fd_ = -1;
        if (closed != 0) {
            if (p != MAP_FAILED) munmap(p, *size);
            cannot_write(close_err);
        }
        if (p == MAP_FAILED)
            throw std::runtime_error(std::string("Cannot map ") + what_ + ": " + std::to_string(count_) + " records");
        map_ = p;
        map_size_ = *size;
        table_ = SortedTable<Rec>(static_cast<const char*>(map_), count_, what_);
        return table_;
    }
    // The table finish() mapped; empty before.
    const SortedTable<Rec>& table() const { return table_; }

private:
    void swap(ScratchTable& o) noexcept {
        std::swap(dir_, o.dir_);
        std::swap(what_, o.what_);
        std::swap(buf_, o.buf_);
        std::swap(fd_, o.fd_);
        std::swap(count_, o.count_);
        std::swap(last_key_, o.last_key_);
        std::swap(map_, o.map_);
        std::swap(map_size_, o.map_size_);
        std::swap(table_, o.table_);
    }
    void write_buffer() {
        if (buf_.empty()) return;
        if (fd_ < 0) open_file();
        const char* p = reinterpret_cast<const char*>(buf_.data());
        for (size_t left = buf_.size() * sizeof(Rec); left > 0;) {
            const ssize_t n = write(fd_, p, left);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) cannot_write(n < 0 ? errno : EIO);
            p += n;
            left -= static_cast<size_t>(n);
        }
        buf_.clear();
    }
    // An unnamed file: unlinked at once, it lives as long as the descriptor
    // and then the mapping.
    void open_file() {
        if (dir_.empty()) throw std::runtime_error(std::string("No scratch dir for ") + what_);
        std::string path = dir_ + "/table-XXXXXX";
        fd_ = mkstemp(path.data());
        if (fd_ < 0) cannot_write(errno);
        unlink(path.c_str());
    }
    [[noreturn]] void cannot_write(int err) const {
        throw std::runtime_error(std::string("Cannot write ") + what_ + " to " + dir_ + ": " + std::strerror(err));
    }

    std::string dir_;
    const char* what_ = "scratch table";
    std::vector<Rec> buf_;  // the records not yet written
    int fd_ = -1;
    size_t count_ = 0;
    uint32_t last_key_ = 0;
    void* map_ = nullptr;
    size_t map_size_ = 0;
    SortedTable<Rec> table_;
};

// An old id or string offset and its new one, as the patch carries remap
// pairs: (old u32, new u32).
struct RemapPair {
    uint32_t from, to;
};

// The parent-id remap after its marker (POI_PARENT_REMAP_MARKER), where it
// sits: the street leg. Reads the section at data[pos, size) and advances pos
// past it.
inline SortedTable<RemapPair> parse_parent_id_remap(const char* data, size_t size, size_t& pos) {
    SectionReader in(data, size, pos, "POI parent-id remap");
    const uint32_t n = in.u32();
    return SortedTable<RemapPair>(in.take_items(n, sizeof(RemapPair)), n, "POI parent-id remap");
}

// --- Varint encoding for delta-compressed fixup tables, string runs and cell list deltas ---

inline void write_varint(std::vector<char>& buf, uint32_t value) {
    while (value >= 128) {
        buf.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buf.push_back(static_cast<char>(value));
}

// A u64 as SectionReader::varint64 reads it: 1 to 10 bytes.
inline void write_varint64(std::vector<char>& buf, uint64_t value) {
    while (value >= 128) {
        buf.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buf.push_back(static_cast<char>(value));
}

// The bytes write_varint writes for value: 1 to 5.
inline size_t varint_size(uint32_t value) {
    size_t n = 1;
    for (; value >= 128; value >>= 7) n++;
    return n;
}

inline uint32_t zigzag32(uint32_t v) { return (v << 1) ^ (0u - (v >> 31)); }
inline uint32_t unzigzag32(uint32_t v) { return (v >> 1) ^ (0u - (v & 1)); }

// --- Cell index rebuild ---

// Bytes in memory behind the size() / at(off, n) interface the patcher's
// file readers have (SequentialFileReader), for readers over a buffer.
struct ByteSpan {
    const char* data;
    size_t bytes;
    uint64_t size() const { return bytes; }
    const char* at(uint64_t off, size_t) const { return data + off; }
};

// cell_id → its ids (sorted), in cell order: an admin / POI / postcode
// centroid index (cell_pairs.h) as the patch sees it, whatever the layout of
// its files.
using CellLists = std::map<uint64_t, std::vector<uint32_t>>;

// The lists of a cell pair index of `format` from its files' bytes.
inline CellLists parse_cell_lists(const std::vector<char>& cells, const std::vector<char>& entries,
                                  cell_pairs::Format format) {
    ByteSpan c{cells.data(), cells.size()}, e{entries.data(), entries.size()};
    cell_pairs::Reader reader(c, e, format);
    CellLists out;
    uint64_t cell;
    std::vector<uint32_t> ids;
    while (reader.next(cell, ids)) out.emplace_hint(out.end(), cell, ids);
    return out;
}

// The files cell_pairs::Writer writes for these lists: (cells, entries).
inline std::pair<std::vector<char>, std::vector<char>> write_cell_lists(const CellLists& lists,
                                                                         cell_pairs::Format format) {
    std::pair<std::vector<char>, std::vector<char>> out;
    cell_pairs::Writer writer(
        format, [&](const char* p, size_t n) { out.first.insert(out.first.end(), p, p + n); },
        [&](const char* p, size_t n) { out.second.insert(out.second.end(), p, p + n); });
    for (const auto& [cell, ids] : lists) writer.add(cell, ids.data(), ids.size());
    writer.finish();
    return out;
}

// The admin / POI index the patcher rebuilds, as the diff derives it
// (stream_cell_index gives the same lists): the old index's cells less
// `removed`, with `added` (empty lists), every id moved by rm keyed by the
// bare id, INTERIOR_FLAG carried over, and each list sorted.
inline CellLists rebuild_cell_lists(const CellLists& old, const std::unordered_map<uint32_t, uint32_t>& rm,
                                    const std::vector<uint64_t>& added = {},
                                    const std::vector<uint64_t>& removed = {}) {
    const std::unordered_set<uint64_t> gone(removed.begin(), removed.end());
    CellLists out;
    for (const auto& [cell, ids] : old) {
        if (gone.count(cell)) continue;
        std::vector<uint32_t>& moved = out.emplace_hint(out.end(), cell, ids)->second;
        for (uint32_t& id : moved) {
            auto it = rm.find(id & cell_pairs::BARE);
            if (it != rm.end()) id = it->second | (id & cell_pairs::FLAG);
        }
        std::sort(moved.begin(), moved.end());
    }
    // A cell the old index holds can't be added, as the patcher's walk refuses it.
    for (uint64_t cell : added)
        if (old.count(cell) || !out.emplace(cell, std::vector<uint32_t>()).second)
            throw std::runtime_error("Malformed cell index");
    return out;
}

// --- Cell index deltas as runs (CELL_INDEX_DELTA_MARKER, CELL_LIST_DELTA_STRIDE) ---

// The most ids a cell lists.
static constexpr uint32_t CELL_LIST_MAX = cell_pairs::LIST_MAX;

// A cell list delta (v15) turns one cell index, the source, into the new one: for a postcode centroid index
// (CELL_LIST_DELTA_STRIDE) the old index, for an admin or POI index (CELL_INDEX_DELTA_MARKER) the one the patcher
// rebuilds from it. A source cell has a rank, its place among the source's cells, and so has a cell of the new index
// among the new index's cells. Every number is a varint (write_varint64; the ids' write_varint):
//   n_gone, then the rank of each source cell the new index doesn't hold, ascending, each as the gap from the one
//     after the previous (the first from rank 0);
//   n_new, then the id of each cell new to the index, ascending, each as the gap from the one after the previous
//     (the first from 0), with an empty source list;
//   n_lost, then the runs of consecutive new-index ranks whose cells lose an id; n_gained, then those that gain one.
//     Each run as (start gap, length - 1, bare id << 1 | interior), sorted by (start, id) with the id as the list
//     holds it (INTERIOR_FLAG on top): the start gap is the start rank less the previous run's (the first from 0).
// A cell's new list is its source list less its lost ids, merged with its gained ids. The lists are multisets (a POI
// index rebuilt through a remap can list an id twice and lose both): an id a cell loses k times is in k runs over
// it. A polygon whose boundary moved costs a run or two, not an id per cell; the patcher holds the runs that cover
// the cell it is at (ActiveRuns), at most CELL_LIST_MAX ids each way.

// A run of new-index ranks [start, last] that loses or gains id (the id as the lists hold it).
struct CellRun {
    uint64_t start, last;
    uint32_t id;
};

// The id a run's varint carries: the bare id above the interior flag, so an id costs no 5th byte for its flag.
inline uint32_t cell_run_value(uint32_t id) { return (id & 0x7FFFFFFFu) << 1 | id >> 31; }
inline uint32_t cell_run_id(uint32_t value) { return value >> 1 | value << 31; }

// The runs of (id, rank) pairs: per id, the maximal runs of consecutive ranks it occurs at, one layer of runs per
// occurrence (layer j covers the ranks the id occurs at more than j times), sorted by (start, id, last).
inline std::vector<CellRun> cell_runs(std::vector<std::pair<uint32_t, uint64_t>> pairs) {
    std::sort(pairs.begin(), pairs.end());
    std::vector<CellRun> runs;
    std::vector<std::pair<uint64_t, uint32_t>> counts;  // one id's (rank, occurrences)
    for (size_t i = 0; i < pairs.size();) {
        const uint32_t id = pairs[i].first;
        counts.clear();
        for (; i < pairs.size() && pairs[i].first == id; i++) {
            if (!counts.empty() && counts.back().first == pairs[i].second) counts.back().second++;
            else counts.push_back({pairs[i].second, 1});
        }
        for (uint32_t layer = 0;; layer++) {
            bool any = false, open = false;
            CellRun run{0, 0, id};
            for (const auto& [rank, n] : counts) {
                if (n <= layer) continue;
                any = true;
                if (open && rank == run.last + 1) {
                    run.last = rank;
                    continue;
                }
                if (open) runs.push_back(run);
                run = {rank, rank, id};
                open = true;
            }
            if (open) runs.push_back(run);
            if (!any) break;
        }
    }
    std::sort(runs.begin(), runs.end(), [](const CellRun& a, const CellRun& b) {
        return std::tie(a.start, a.id, a.last) < std::tie(b.start, b.id, b.last);
    });
    return runs;
}

// What a cell list delta holds, for the diff's log.
struct CellListDeltaCounts {
    uint64_t gone = 0, fresh = 0, lost_runs = 0, gained_runs = 0;
};

// Appends the cell list delta from `from` (the source) to `to`, as stream_cell_index reads it. Refuses a new list
// out of order, which no walk rebuilds, and more cells than a run's rank holds (a u32); a source list out of order
// is taken sorted, as the patcher's walk sorts it.
inline CellListDeltaCounts append_cell_list_delta(std::vector<char>& out, const CellLists& from, const CellLists& to) {
    if (to.size() > uint64_t{UINT32_MAX}) throw std::runtime_error("Cell index holds more cells than a cell list delta ranks");
    CellListDeltaCounts counts;
    std::vector<uint64_t> gone, fresh;
    uint64_t rank = 0;
    for (const auto& kv : from) {
        if (!to.count(kv.first)) gone.push_back(rank);
        rank++;
    }
    std::vector<std::pair<uint32_t, uint64_t>> lost, gained;  // (id, new-index rank)
    const std::vector<uint32_t> none;
    std::vector<uint32_t> sorted, d;
    rank = 0;
    for (const auto& [cid, ids] : to) {
        if (!std::is_sorted(ids.begin(), ids.end())) throw std::runtime_error("Cell index list out of order");
        auto it = from.find(cid);
        if (it == from.end()) fresh.push_back(cid);
        const std::vector<uint32_t>* was = it != from.end() ? &it->second : &none;
        if (!std::is_sorted(was->begin(), was->end())) {
            sorted = *was;
            std::sort(sorted.begin(), sorted.end());
            was = &sorted;
        }
        if (*was != ids) {
            d.clear();
            std::set_difference(was->begin(), was->end(), ids.begin(), ids.end(), std::back_inserter(d));
            for (const uint32_t id : d) lost.push_back({id, rank});
            d.clear();
            std::set_difference(ids.begin(), ids.end(), was->begin(), was->end(), std::back_inserter(d));
            for (const uint32_t id : d) gained.push_back({id, rank});
        }
        rank++;
    }
    for (const auto* values : {&gone, &fresh}) {
        write_varint64(out, values->size());
        uint64_t next = 0;
        for (const uint64_t v : *values) {
            write_varint64(out, v - next);
            next = v + 1;
        }
    }
    counts.gone = gone.size();
    counts.fresh = fresh.size();
    for (auto* pairs : {&lost, &gained}) {
        const std::vector<CellRun> runs = cell_runs(std::move(*pairs));
        write_varint64(out, runs.size());
        uint64_t start = 0;
        for (const CellRun& r : runs) {
            write_varint64(out, r.start - start);
            write_varint64(out, r.last - r.start);
            write_varint(out, cell_run_value(r.id));
            start = r.start;
        }
        (pairs == &lost ? counts.lost_runs : counts.gained_runs) = runs.size();
    }
    return counts;
}

// Where a cell list delta's four lists start, and how many each holds: the patcher walks them side by side.
struct CellListDeltaParts {
    uint64_t n_gone, n_new, n_lost, n_gained;
    size_t gone_at, new_at, lost_at, gained_at;
};

// Reads the delta's counts and passes over its items, refusing one that runs past the end or bytes after the last.
inline CellListDeltaParts cell_list_delta_parts(const char* delta, size_t size) {
    size_t pos = 0;
    SectionReader in(delta, size, pos, "cell list delta", PastEnd::MALFORMED);
    CellListDeltaParts p{};
    p.n_gone = in.varint64();
    p.gone_at = pos;
    for (uint64_t i = 0; i < p.n_gone; i++) in.varint64();
    p.n_new = in.varint64();
    p.new_at = pos;
    for (uint64_t i = 0; i < p.n_new; i++) in.varint64();
    for (uint64_t* n : {&p.n_lost, &p.n_gained}) {
        *n = in.varint64();
        (n == &p.n_lost ? p.lost_at : p.gained_at) = pos;
        for (uint64_t i = 0; i < *n; i++) {
            in.varint64();
            in.varint64();
            in.varint();
        }
    }
    if (!in.at_end()) in.malformed();
    return p;
}

[[noreturn, gnu::cold, gnu::noinline]] inline void refuse_cell_list_delta(const char* why) {
    throw std::runtime_error(std::string("Malformed cell list delta: ") + why);
}

// A cell list delta's gone ranks or new cell ids from at, n of them, read in order: each the one after the previous
// plus its gap (the first from 0). A value of NONE or past it is malformed; next() gives NONE once all are read.
class CellGaps {
public:
    static constexpr uint64_t NONE = ~0ull;
    CellGaps(const char* data, size_t size, size_t at, uint64_t n)
        : pos_(at), in_(data, size, pos_, "cell list delta", PastEnd::MALFORMED), left_(n) {}
    CellGaps(const CellGaps&) = delete;
    CellGaps& operator=(const CellGaps&) = delete;
    uint64_t next() {
        if (left_ == 0) return NONE;
        left_--;
        const uint64_t gap = in_.varint64();
        if (gap >= NONE - next_) in_.malformed();
        const uint64_t v = next_ + gap;
        next_ = v + 1;
        return v;
    }

private:
    size_t pos_;
    SectionReader in_;
    uint64_t left_, next_ = 0;
};

// A cell list delta's lost or gained runs from at, n of them, read in order, each checked as it is read: refused as
// "a step back" when its (start, id) is below the previous run's, and as "a run past the last rank" when its last rank
// passes a u32 (no index has as many cells: each lists at least its count in an entries file of u32 offsets) or its
// sum wraps 64 bits. The bounds are computed in 64 bits whatever size_t is.
class CellRunReader {
public:
    CellRunReader(const char* data, size_t size, size_t at, uint64_t n)
        : data_(data), size_(size), pos_(at), in_(data, size, pos_, "cell list delta", PastEnd::MALFORMED), left_(n) {
        load();
    }
    CellRunReader(const CellRunReader&) = delete;
    CellRunReader& operator=(const CellRunReader&) = delete;
    bool done() const { return !have_; }
    // How many runs from the next one on start at rank (stops counting past CELL_LIST_MAX, which no set holds).
    uint64_t starting_at(uint64_t rank) const {
        if (!have_ || next_.start != rank) return 0;
        uint64_t n = 1;
        size_t p = pos_;
        SectionReader peek(data_, size_, p, "cell list delta", PastEnd::MALFORMED);
        for (uint64_t k = 0; k < left_ && n <= CELL_LIST_MAX; k++) {
            if (peek.varint64() != 0) break;
            peek.varint64();
            peek.varint();
            n++;
        }
        return n;
    }
    // The next run, which there is (!done()).
    CellRun next() {
        const CellRun r = next_;
        load();
        return r;
    }

private:
    void load() {
        have_ = left_ > 0;
        if (!have_) return;
        left_--;
        const uint64_t gap = in_.varint64(), length_less_one = in_.varint64();
        const uint32_t id = cell_run_id(in_.varint());
        if (gap > ~0ull - start_) refuse_cell_list_delta("a run past the last rank");
        const uint64_t start = start_ + gap;
        if (length_less_one > uint64_t{UINT32_MAX} || start > uint64_t{UINT32_MAX} - length_less_one)
            refuse_cell_list_delta("a run past the last rank");
        if (start == start_ && id < id_) refuse_cell_list_delta("a step back");
        next_ = {start, start + length_less_one, id};
        start_ = start;
        id_ = id;
    }

    const char* data_;
    size_t size_;
    size_t pos_;
    SectionReader in_;
    uint64_t left_;
    bool have_ = false;
    CellRun next_{0, 0, 0};
    uint64_t start_ = 0;
    uint32_t id_ = 0;
};

// The ids of the lost or gained runs over the cell the walk is at, sorted by id (an id as often as runs of it cover
// the cell), each with its run's last rank: 8 bytes an id, and at most CELL_LIST_MAX ids, since a cell holds every id
// it loses and lists every id it gains, so 512 KiB however many runs the delta holds. Its capacity grows to that at
// most.
class ActiveRuns {
public:
    struct Id {
        uint32_t id, last;
    };
    const std::vector<Id>& ids() const { return ids_; }
    bool empty() const { return ids_.empty(); }
    // Adds the runs that start at rank, merged into the set by id where it sits: refuses a set past CELL_LIST_MAX
    // before it grows.
    void start_at(CellRunReader& runs, uint64_t rank) {
        const uint64_t b = runs.starting_at(rank);
        if (b == 0) return;
        const size_t a = ids_.size();
        if (b > CELL_LIST_MAX - a) refuse_cell_list_delta("an active set above 65535 ids");
        const size_t nb = static_cast<size_t>(b), n = a + nb;
        if (n > ids_.capacity()) ids_.reserve(std::min<size_t>(std::max(n, 2 * ids_.capacity()), CELL_LIST_MAX));
        ids_.resize(n);
        // The set moves to the end; the merge writes from the front, never past what it has still to read.
        std::memmove(ids_.data() + nb, ids_.data(), a * sizeof(Id));
        size_t i = nb, k = 0;
        for (size_t j = 0; j < nb; j++) {
            const CellRun r = runs.next();
            while (i < n && ids_[i].id <= r.id) ids_[k++] = ids_[i++];
            ids_[k++] = {r.id, static_cast<uint32_t>(r.last)};
        }
    }
    // Drops the runs whose last rank is rank.
    void end_at(uint64_t rank) {
        ids_.erase(std::remove_if(ids_.begin(), ids_.end(), [&](const Id& x) { return x.last == rank; }), ids_.end());
    }

private:
    std::vector<Id> ids_;
};

// What a cell index rebuild wrote: its rows and both files' bytes.
struct CellIndexWritten {
    uint64_t rows = 0, cells_bytes = 0, entries_bytes = 0;
};

// Rebuilds a cell index of `format` and applies a cell list delta (append_cell_list_delta) to it in one pass,
// writing the result through a cell_pairs::Writer to out_cells(bytes, n) / out_entries(bytes, n). The source is the
// old index (read by a cell_pairs::Reader) plus `added` cell ids with empty lists, minus `removed`; every old id
// moves by remap(id without INTERIOR_FLAG), flag kept, and each list is sorted: the same lists rebuild_cell_lists
// gives. Holds one block of the old index, one cell's list and the runs over it: the patcher runs on client
// machines, where materialising planet poi/all (1.1M cells, 25M ids, a 23M entry remap map) cost over 1 GiB. The old
// cells and entries are read front to back through size() and at(off, n), a view that lasts until the next call
// (SequentialFileReader); the cell changes and the delta where they sit. Refuses, besides a malformed index or delta:
// a lost id missing from its cell, an active set above CELL_LIST_MAX, a step back (a run's (start, id) below the
// previous one's, or a new cell the source holds) and a run past the last rank (or a gone rank past the last source
// cell).
template <typename Cells, typename Entries, typename Remap, typename OutCells, typename OutEntries>
CellIndexWritten stream_cell_index(Cells&& cells, Entries&& entries, cell_pairs::Format format, AscendingIds added,
                                   AscendingIds removed, Remap remap, const char* delta, size_t delta_size,
                                   OutCells out_cells, OutEntries out_entries) {
    auto bad_index = [] { throw std::runtime_error("Malformed cell index"); };
    constexpr uint64_t NONE = CellGaps::NONE;

    const CellListDeltaParts parts = cell_list_delta_parts(delta, delta_size);
    CellGaps gone(delta, delta_size, parts.gone_at, parts.n_gone), fresh(delta, delta_size, parts.new_at, parts.n_new);
    CellRunReader lost_runs(delta, delta_size, parts.lost_at, parts.n_lost);
    CellRunReader gained_runs(delta, delta_size, parts.gained_at, parts.n_gained);
    ActiveRuns lost, gained;

    // The next source cell and its list: old cells minus `removed`, merged with `added`. The old index's next row
    // waits in (old_cid, old_ids) while an added cell comes first. old_ids is the one copy of an old list the pass
    // holds: as read, or remapped and sorted (old_mapped) once a source cell took it; a row that shares the list of
    // the row before leaves it as it is (Reader::next's repeat), the same list remapped the same way.
    static const std::vector<uint32_t> no_ids;
    cell_pairs::Reader old(cells, entries, format);
    uint64_t old_cid = NONE;
    std::vector<uint32_t> old_ids;
    bool old_mapped = false, old_waiting = false, old_done = false;
    auto next_source = [&](uint64_t& cid, const std::vector<uint32_t>*& list) -> bool {
        for (;;) {
            if (!old_waiting && !old_done) {
                bool repeat = false;
                old_waiting = old.next(old_cid, old_ids, repeat);
                old_done = !old_waiting;
                if (old_waiting && !repeat) old_mapped = false;
            }
            const uint64_t o = old_waiting ? old_cid : NONE;
            const uint64_t add_cid = added.done() ? NONE : added.front();
            if (o == NONE && add_cid == NONE) return false;
            if (add_cid < o) {
                cid = add_cid; added.pop();
                list = &no_ids;
                return true;
            }
            if (add_cid == o) bad_index();
            old_waiting = false;
            if (removed.skip_to(o)) continue;
            cid = o;
            if (!old_mapped) {
                // A remap that keeps the list's order (the admin index's, which moves no id) leaves it sorted: it
                // is sorted only where the remap moved an id before the one ahead of it.
                bool in_order = true;
                uint32_t last = 0;
                for (auto& id : old_ids) {
                    id = remap(id & 0x7FFFFFFFu) | (id & 0x80000000u);
                    in_order &= id >= last;
                    last = id;
                }
                if (!in_order) std::sort(old_ids.begin(), old_ids.end());
                old_mapped = true;
            }
            list = &old_ids;
            return true;
        }
    };

    cell_pairs::Writer out(format, out_cells, out_entries);
    uint64_t rank = 0;
    std::vector<uint32_t> merged;
    // The new index's cell at `rank`: its source list less the lost runs over it, merged with the gained ones.
    auto emit = [&](uint64_t cid, const std::vector<uint32_t>& source) {
        lost.start_at(lost_runs, rank);
        gained.start_at(gained_runs, rank);
        if (lost.empty() && gained.empty()) {
            if (source.size() > CELL_LIST_MAX) throw std::runtime_error("Cell holds more than 65535 entries");
            out.add(cid, source.data(), source.size());
        } else {
            const auto& l = lost.ids();
            const auto& g = gained.ids();
            if (l.size() > source.size()) refuse_cell_list_delta("a lost id missing from its cell");
            if (source.size() - l.size() + g.size() > CELL_LIST_MAX) throw std::runtime_error("Cell holds more than 65535 entries");
            merged.clear();
            size_t li = 0, gi = 0;
            for (const uint32_t id : source) {
                if (li < l.size() && l[li].id < id) refuse_cell_list_delta("a lost id missing from its cell");
                if (li < l.size() && l[li].id == id) {
                    li++;
                    continue;
                }
                for (; gi < g.size() && g[gi].id < id; gi++) merged.push_back(g[gi].id);
                merged.push_back(id);
            }
            if (li < l.size()) refuse_cell_list_delta("a lost id missing from its cell");
            for (; gi < g.size(); gi++) merged.push_back(g[gi].id);
            out.add(cid, merged.data(), merged.size());
        }
        lost.end_at(rank);
        gained.end_at(rank);
        rank++;
    };

    uint64_t source_rank = 0, next_gone = gone.next(), next_new = fresh.next(), cid = NONE;
    const std::vector<uint32_t>* list = &no_ids;
    bool have = next_source(cid, list);
    for (;;) {
        while (have && source_rank == next_gone) {
            source_rank++;
            next_gone = gone.next();
            have = next_source(cid, list);
        }
        if (have && cid < next_new) {
            emit(cid, *list);
            source_rank++;
            have = next_source(cid, list);
        } else if (next_new != NONE && (!have || next_new < cid)) {
            emit(next_new, no_ids);
            next_new = fresh.next();
        } else if (have) {
            refuse_cell_list_delta("a step back");  // a new cell the source holds
        } else {
            break;
        }
    }
    removed.require_ascending();
    if (next_gone != NONE) refuse_cell_list_delta("a gone rank past the last source cell");
    if (!lost_runs.done() || !gained_runs.done() || !lost.empty() || !gained.empty())
        refuse_cell_list_delta("a run past the last rank");
    out.finish();
    return {out.rows(), out.cells_bytes(), out.entries_bytes()};
}

// stream_cell_index for an index whose ids don't move (the postcode centroid
// index): the old index plus the delta.
template <typename Cells, typename Entries, typename OutCells, typename OutEntries>
CellIndexWritten stream_cell_list_delta(Cells&& cells, Entries&& entries, cell_pairs::Format format,
                                        const char* delta, size_t delta_size, OutCells out_cells,
                                        OutEntries out_entries) {
    return stream_cell_index(cells, entries, format, {}, {}, [](uint32_t id) { return id; },
                             delta, delta_size, out_cells, out_entries);
}

// --- Geo entry list deltas (GEO_ENTRY_DELTA_MARKER) ---

// Appends one cell's correction: the ids `derived` (sorted) lost and the
// ids it gained to become `target`, or `target` whole when it isn't sorted or
// the delta wouldn't fit in u16 counts. A delta even where the whole list is
// shorter compresses better on the planet (repeated ids, small values).
inline void append_geo_list_delta(std::vector<char>& out, uint64_t cell_id,
                                  const std::vector<uint32_t>& derived, const std::vector<uint32_t>& target) {
    auto put = [&](const void* p, size_t n) { out.insert(out.end(), (const char*)p, (const char*)p + n); };
    std::vector<uint32_t> lost, gained;
    bool delta = std::is_sorted(target.begin(), target.end());
    if (delta) {
        std::set_difference(derived.begin(), derived.end(), target.begin(), target.end(), std::back_inserter(lost));
        std::set_difference(target.begin(), target.end(), derived.begin(), derived.end(), std::back_inserter(gained));
        delta = lost.size() < GEO_LIST_REPLACE && gained.size() <= 0xFFFF;
    }
    if (!delta) { lost.clear(); gained = target; }
    uint16_t nl = delta ? static_cast<uint16_t>(lost.size()) : GEO_LIST_REPLACE;
    uint16_t ng = static_cast<uint16_t>(gained.size());
    put(&cell_id, 8); put(&nl, 2); put(&ng, 2);
    put(lost.data(), lost.size() * 4);
    put(gained.data(), gained.size() * 4);
}

// Checks `count` cell corrections in data[pos, size), moves pos past them and
// returns where they sit; throws when they are short or out of cell order.
// They stay there: a GeoListDeltaCursor reads each one as the entry walk
// reaches its cell (held as vectors they cost the patcher ~7 MiB of heap on
// planet).
inline PatchRef scan_geo_list_deltas(const char* data, size_t size, size_t& pos, uint32_t count) {
    const size_t start = pos;
    SectionReader in(data, size, pos, "geo entry delta", PastEnd::MALFORMED);
    uint64_t prev = 0;
    for (uint32_t i = 0; i < count; i++) {
        const uint64_t cid = in.u64();
        uint16_t nl = in.u16();
        const uint16_t ng = in.u16();
        if (nl == GEO_LIST_REPLACE) nl = 0;
        if (i > 0 && cid <= prev) in.malformed();
        prev = cid;
        in.take_items(uint32_t{nl} + ng, 4);
    }
    return {start, pos - start};
}

// Forward cursor over the corrections scan_geo_list_deltas checked (at ref
// in data), asked for in ascending cell order; apply() turns a cell's
// derived list into its new one.
class GeoListDeltaCursor {
public:
    GeoListDeltaCursor() = default;  // no corrections
    GeoListDeltaCursor(const char* data, PatchRef ref) : data_(data), pos_(ref.pos), end_(ref.pos + ref.size) { load(); }

    // The cursor holds cell c's correction.
    bool at(uint64_t c) {
        while (have_ && cid_ < c) load();
        return have_ && cid_ == c;
    }
    // The correction replaces the derived list whole (GEO_LIST_REPLACE).
    bool replaces() const { return replace_; }
    // Out of line: few cells carry a correction, and the entry walk that asks for it runs for every cell of
    // the geo index, so its loop stays as small as without one.
    [[gnu::noinline]] void apply(std::vector<uint32_t>& ids, std::vector<uint32_t>& scratch) {
        gained_ids_.resize(ng_);
        if (ng_) memcpy(gained_ids_.data(), data_ + gained_, (size_t)ng_ * 4);
        if (replace_) { ids = gained_ids_; return; }
        lost_ids_.resize(nl_);
        if (nl_) memcpy(lost_ids_.data(), data_ + lost_, (size_t)nl_ * 4);
        scratch.clear();
        std::set_difference(ids.begin(), ids.end(), lost_ids_.begin(), lost_ids_.end(), std::back_inserter(scratch));
        ids.clear();
        std::merge(scratch.begin(), scratch.end(), gained_ids_.begin(), gained_ids_.end(), std::back_inserter(ids));
    }

private:
    void load() {
        if (pos_ == end_) { have_ = false; return; }
        memcpy(&cid_, data_ + pos_, 8); memcpy(&nl_, data_ + pos_ + 8, 2); memcpy(&ng_, data_ + pos_ + 10, 2);
        replace_ = nl_ == GEO_LIST_REPLACE;
        if (replace_) nl_ = 0;
        lost_ = pos_ + 12;
        gained_ = lost_ + (size_t)nl_ * 4;
        pos_ = gained_ + (size_t)ng_ * 4;
        have_ = true;
    }

    const char* data_ = nullptr;
    size_t pos_ = 0, end_ = 0, lost_ = 0, gained_ = 0;
    bool have_ = false, replace_ = false;
    uint64_t cid_ = 0;
    uint16_t nl_ = 0, ng_ = 0;
    std::vector<uint32_t> lost_ids_, gained_ids_;  // the current cell's ids, aligned
};

// --- Record id remap (patcher) ---

// Old → new record ids of a replayed data file: the MATCH runs of its merge
// sequence plus the moved records' pairs (the diff's secondary matches, or
// the street leg of the parent-id remap). Strategy-2 slots keep these
// to long runs of one shift: a few MB where the patcher used to write a
// 4-byte-per-record scratch file (~1 GB for planet/full) and read it at
// random from the entry pipeline, page-cache thrash on a 1 GiB client. The
// runs spill to a ScratchTable as the replay makes them, and the pairs are
// read where they sit in the patch, so a reorder day's many cost the heap
// nothing; lookups start once finish_runs() has mapped the runs.
class RecordRemap {
public:
    static constexpr uint32_t NONE = 0xFFFFFFFFu;

    RecordRemap() = default;  // maps nothing
    RecordRemap(uint32_t n_old, const std::string& scratch_dir) : n_old_(n_old), runs_(scratch_dir, "record runs") {}

    // `count` old records from old_start were kept as new_start onwards.
    // The run stays open while the next match extends it.
    void add_match(uint32_t old_start, uint32_t new_start, uint32_t count) {
        if (old_start >= n_old_) return;
        count = std::min(count, n_old_ - old_start);
        if (count == 0) return;
        if (open_.count > 0 && open_.old_start + open_.count == old_start && open_.new_start + open_.count == new_start) {
            open_.count += count;
            return;
        }
        if (open_.count > 0) runs_.append(open_);
        open_ = {old_start, new_start, count};
    }
    // The replay is over: maps its runs for lookups.
    void finish_runs() {
        if (open_.count > 0) runs_.append(open_);
        open_ = {};
        runs_.finish();
    }
    // The moved records: each pair's old id maps to its new id, whatever
    // the runs say. Ids past the old file map to nothing.
    void set_pairs(SortedTable<RemapPair> pairs) { pairs_ = std::move(pairs); }
    bool has_pairs() const { return !pairs_.empty(); }

    size_t size() const { return n_old_; }
    size_t run_count() const { return runs_.size() + (open_.count > 0); }
    bool empty() const { return run_count() == 0 && pairs_.empty(); }
    // operator[] with a run hint: consecutive lookups that land in the same
    // run or the next (spatially sorted references) skip the search.
    uint32_t lookup(uint32_t o, size_t& hint) const {
        if (o >= n_old_) return NONE;
        if (const std::optional<RemapPair> p = pairs_.find(o)) return p->to;
        return through_run(o, runs_.table().floor(o, hint));
    }
    // The new id of old record `old`, or NONE when it wasn't kept.
    uint32_t operator[](size_t old) const {
        if (old >= n_old_) return NONE;
        const uint32_t o = static_cast<uint32_t>(old);
        if (const std::optional<RemapPair> p = pairs_.find(o)) return p->to;
        return through_run(o, runs_.table().floor(o));
    }

private:
    struct Run { uint32_t old_start, new_start, count; };
    // o's new id by the run at `at`, its floor (the table's size for none).
    uint32_t through_run(uint32_t o, size_t at) const {
        const SortedTable<Run>& t = runs_.table();
        if (at == t.size()) return NONE;
        const Run r = t[at];
        return o - r.old_start < r.count ? r.new_start + (o - r.old_start) : NONE;
    }

    uint32_t n_old_ = 0;
    Run open_{0, 0, 0};  // the run the replay may still extend
    ScratchTable<Run> runs_;
    SortedTable<RemapPair> pairs_;
};

// --- String offset remap (patcher) ---

// The string tier files, in global offset order: tier t's strings sit at
// [sum of the sizes of tiers < t, + its own size). Same as the builder's
// STR_TIER_FILENAMES.
static constexpr int STRING_TIER_COUNT = 5;
static constexpr const char* STRING_TIER_FILES[STRING_TIER_COUNT] = {
    "strings_core.bin", "strings_street.bin", "strings_addr.bin",
    "strings_postcode.bin", "strings_poi.bin"
};

// A tier's edit between two days: the strings both pools hold that keep
// their relative order are kept, a longest such sequence (every common string
// of a sorted tier). Every other old string is deleted and every other new
// one added at its new index, so a string the POI tier's record order moves
// (order_poi_strings_by_record) goes as both, and its offset as a pair.

// The longest strictly increasing subsequence of v, as ascending indices into
// v (patience sorting). Increasing input takes no search.
inline std::vector<uint32_t> longest_increasing_subsequence(const std::vector<uint32_t>& v) {
    constexpr uint32_t NONE = UINT32_MAX;
    std::vector<uint32_t> tails;  // per length, the index that ends a run of it at the least value
    std::vector<uint32_t> prev(v.size());
    for (uint32_t i = 0; i < v.size(); i++) {
        size_t len = tails.size();
        if (len > 0 && !(v[tails.back()] < v[i]))
            len = std::lower_bound(tails.begin(), tails.end(), v[i],
                                   [&](uint32_t t, uint32_t x) { return v[t] < x; }) - tails.begin();
        prev[i] = len > 0 ? tails[len - 1] : NONE;
        if (len == tails.size()) tails.push_back(i);
        else tails[len] = i;
    }
    std::vector<uint32_t> out(tails.size());
    uint32_t i = tails.empty() ? NONE : tails.back();
    for (size_t k = out.size(); k-- > 0; i = prev[i]) out[k] = i;
    return out;
}

// A tier's kept strings as (old, new) starts local to its pools, ascending in
// both, out of `common`: those of every string both pools hold, by old start.
inline std::vector<std::pair<uint32_t, uint32_t>> kept_strings(const std::vector<std::pair<uint32_t, uint32_t>>& common) {
    std::vector<uint32_t> news(common.size());
    for (size_t k = 0; k < common.size(); k++) news[k] = common[k].second;
    std::vector<std::pair<uint32_t, uint32_t>> kept;
    for (uint32_t k : longest_increasing_subsequence(news)) kept.push_back(common[k]);
    return kept;
}

// Joins kept strings, fed in old order as (old start, old end, new start) in
// global offsets, into maximal runs of one shift: on_run(start, end, shift),
// shift = new - old (mod 2^32). A string that doesn't move ends a run without
// opening one, and so does a gap in the old pool (a deleted string), so a run
// covers only bytes of kept strings.
template <typename OnRun>
class StringRunJoiner {
public:
    explicit StringRunJoiner(OnRun on_run) : on_run_(std::move(on_run)) {}
    void kept(uint32_t old_start, uint32_t old_end, uint32_t new_start) {
        const uint32_t shift = new_start - old_start;
        if (open_ && end_ == old_start && shift_ == shift) {
            end_ = old_end;
            return;
        }
        finish();
        if (shift == 0) return;
        open_ = true;
        start_ = old_start;
        end_ = old_end;
        shift_ = shift;
    }
    void finish() {
        if (open_) on_run_(start_, end_, shift_);
        open_ = false;
    }

private:
    OnRun on_run_;
    bool open_ = false;
    uint32_t start_ = 0, end_ = 0, shift_ = 0;
};

// A tier's shift runs as the diff sends them for a tier the client doesn't
// hold: per run, varints of (start - previous end, end - start,
// zigzag(shift - previous shift)), starting from the tier's old base and
// shift 0. kept: the tier's kept strings (kept_strings).
struct StringTierRuns {
    uint32_t n_runs = 0;
    std::vector<char> bytes;
};

inline StringTierRuns encode_string_tier_runs(const char* old_pool, size_t old_size, uint32_t old_base, uint32_t new_base,
                                              const std::vector<std::pair<uint32_t, uint32_t>>& kept) {
    StringTierRuns out;
    uint32_t prev_end = old_base, prev_shift = 0;
    StringRunJoiner joiner([&](uint32_t start, uint32_t end, uint32_t shift) {
        write_varint(out.bytes, start - prev_end);
        write_varint(out.bytes, end - start);
        write_varint(out.bytes, zigzag32(shift - prev_shift));
        out.n_runs++;
        prev_end = end;
        prev_shift = shift;
    });
    for (const auto& [o, n] : kept) {
        const uint32_t len = static_cast<uint32_t>(strnlen(old_pool + o, old_size - o)) + 1;
        joiner.kept(old_base + o, old_base + o + len, new_base + n);
    }
    joiner.finish();
    return out;
}

// A shipped tier's string diff as the diff makes it: the new pool is the old
// one without the strings at `deleted` (indices in old order, ascending),
// with `added` (each NUL-terminated, pointing into the new pool) put in. A
// sorted new pool (every tier but the POI one) puts each added string where
// it sorts among the kept ones, as the tier's order says where; any other
// carries the added strings' new indices `added_at` (ascending).
struct StringTierDiff {
    uint64_t old_hash = 0, new_hash = 0;
    bool sorted = false;
    std::vector<const char*> added;
    std::vector<uint32_t> added_at;  // empty when sorted
    std::vector<uint32_t> deleted;
};

// A shipped tier's order flag in the strings section.
constexpr uint32_t STRING_TIER_SORTED = 0, STRING_TIER_INDEXED = 1;

// Whether every string of the pool sorts strictly before the next.
inline bool string_pool_sorted(const char* pool, size_t size) {
    const char* prev = nullptr;
    for (size_t at = 0; at < size; at += strnlen(pool + at, size - at) + 1) {
        if (prev && strcmp(prev, pool + at) >= 0) return false;
        prev = pool + at;
    }
    return true;
}

// The diff of a shipped tier whose edit keeps `kept` (kept_strings).
inline StringTierDiff string_tier_diff(const char* old_pool, size_t old_size, const char* new_pool, size_t new_size,
                                       const std::vector<std::pair<uint32_t, uint32_t>>& kept) {
    StringTierDiff d;
    d.old_hash = content_hash(old_pool, old_size);
    d.new_hash = content_hash(new_pool, new_size);
    d.sorted = string_pool_sorted(new_pool, new_size);
    size_t k = 0;
    uint32_t i = 0;
    for (size_t o = 0; o < old_size; o += strnlen(old_pool + o, old_size - o) + 1, i++) {
        if (k < kept.size() && kept[k].first == o) k++;
        else d.deleted.push_back(i);
    }
    k = 0;
    i = 0;
    for (size_t n = 0; n < new_size; n += strnlen(new_pool + n, new_size - n) + 1, i++) {
        if (k < kept.size() && kept[k].second == n) {
            k++;
        } else {
            d.added.push_back(new_pool + n);
            if (!d.sorted) d.added_at.push_back(i);
        }
    }
    return d;
}

// Appends d as the strings section carries a shipped tier.
inline void append_string_tier_diff(std::vector<char>& out, const StringTierDiff& d) {
    auto put = [&](const void* p, size_t n) { out.insert(out.end(), (const char*)p, (const char*)p + n); };
    put(&d.old_hash, 8); put(&d.new_hash, 8);
    const uint32_t n_added = static_cast<uint32_t>(d.added.size()), n_deleted = static_cast<uint32_t>(d.deleted.size());
    const uint32_t order = d.sorted ? STRING_TIER_SORTED : STRING_TIER_INDEXED;
    put(&n_added, 4); put(&n_deleted, 4); put(&order, 4);
    for (const char* str : d.added) put(str, strlen(str) + 1);
    for (uint32_t idx : d.added_at) put(&idx, 4);
    for (uint32_t idx : d.deleted) put(&idx, 4);
}

// A shipped tier's diff as the patcher reads it, where it sits in the patch:
// n_added NUL-terminated strings back to back from `added`, and the u32
// lists `added_at` (n_added of them, an indexed tier's only) and `deleted`
// (n_deleted).
struct StringTierEdit {
    uint64_t old_hash = 0, new_hash = 0;
    bool sorted = false;
    const char* added = nullptr;
    uint32_t n_added = 0;
    const char* added_at = nullptr;
    const char* deleted = nullptr;
    uint32_t n_deleted = 0;
    // Past the edit's last byte (the deleted list ends it).
    const char* end() const { return deleted + size_t{n_deleted} * 4; }
};

// Index i of a list of u32 string indices where it sits.
inline uint32_t string_index_at(const char* list, uint32_t i) {
    uint32_t v;
    memcpy(&v, list + size_t{i} * 4, 4);
    return v;
}

// Reads the shipped tier at data[pos, size) where it sits, advancing pos
// past it, and checks it in one sweep: every added string ends in the
// section, a sorted tier's sort strictly, and both index lists ascend
// strictly.
inline StringTierEdit parse_string_tier_edit(const char* data, size_t size, size_t& pos) {
    SectionReader in(data, size, pos, "strings section");
    StringTierEdit e;
    e.old_hash = in.u64();
    e.new_hash = in.u64();
    e.n_added = in.u32();
    e.n_deleted = in.u32();
    const uint32_t order = in.u32();
    if (order != STRING_TIER_SORTED && order != STRING_TIER_INDEXED) in.malformed();
    e.sorted = order == STRING_TIER_SORTED;
    e.added = data + pos;
    const char* prev = nullptr;
    for (uint32_t i = 0; i < e.n_added; i++) {
        const char* added = in.cstring();
        if (e.sorted && prev && strcmp(prev, added) >= 0) in.malformed();
        prev = added;
    }
    auto ascending = [&](uint32_t n) {
        const char* list = in.take_items(n, 4);
        for (uint32_t i = 1; i < n; i++)
            if (string_index_at(list, i) <= string_index_at(list, i - 1)) in.malformed();
        return list;
    };
    if (!e.sorted) e.added_at = ascending(e.n_added);
    e.deleted = ascending(e.n_deleted);
    return e;
}

// No old string: an added one (for_each_new_string).
constexpr size_t NO_OLD_STRING = SIZE_MAX;

// A shipped tier's old pool string by string, as its file's decoder
// (string_files.h) gives it bucket by bucket: str() is the string the walk is
// at, NUL-terminated, until next(); len() its bytes with the NUL, off() its
// offset in the pool. The walk hashes each bucket as it decodes it.
class OldPoolWalk {
public:
    explicit OldPoolWalk(string_files::Decoder& d) : d_(d), hash_(d.raw_size()) { load(); }
    bool at_end() const { return at_ == d_.size(); }
    const char* str() const { return d_.data() + at_; }
    size_t len() const { return len_; }
    size_t off() const { return static_cast<size_t>(d_.raw_start()) + at_; }
    void next() {
        at_ += len_;
        if (at_ == d_.size()) load();
        else len_ = strlen(str()) + 1;
    }
    // The hash of the pool (content_hash) once the walk is at its end.
    uint64_t hash() const { return hash_.hash(); }
    // The file's body bytes the walk has decoded, and its bucket table's.
    uint64_t coded_end() const { return d_.coded_end(); }
    uint64_t table_read() const { return d_.table_read(); }

private:
    void load() {
        at_ = len_ = 0;
        if (!d_.next()) return;
        hash_.update(d_.data(), d_.size());
        len_ = strlen(d_.data()) + 1;
    }
    string_files::Decoder& d_;
    ContentHasher hash_;
    size_t at_ = 0, len_ = 0;
};

// A shipped tier's new pool string by string, from its old pool and edit:
// on_string(p, len, old_off) for each new string in order, with the offset of
// a kept string in the old pool, or NO_OLD_STRING for an added one (p holds
// until the call returns), and on_consumed() as the walk passes each old
// string, kept (after its on_string) or deleted. The edit's indices ascend
// (parse_string_tier_edit). Returns whether the old pool placed every one of
// them: the walk passes the whole old pool either way. A sorted edit puts an
// added string before the next kept one it sorts before.
template <typename OnString, typename OnConsumed>
[[nodiscard]] bool for_each_new_string(OldPoolWalk& old, const StringTierEdit& edit, OnString on_string,
                                       OnConsumed on_consumed) {
    uint32_t oi = 0, ni = 0, d = 0, a = 0;
    const char* added = edit.added;  // the next added string
    auto skip_deleted = [&] {
        for (; !old.at_end() && d < edit.n_deleted && string_index_at(edit.deleted, d) == oi; d++, oi++) {
            old.next();
            on_consumed();
        }
    };
    skip_deleted();
    auto added_next = [&] {
        if (a >= edit.n_added) return false;
        if (!edit.sorted) return string_index_at(edit.added_at, a) == ni;
        return old.at_end() || strcmp(added, old.str()) < 0;
    };
    for (;; ni++) {
        if (added_next()) {
            const size_t len = strlen(added) + 1;
            on_string(added, len, NO_OLD_STRING);
            added += len;
            a++;
        } else if (!old.at_end()) {
            on_string(old.str(), old.len(), old.off());
            old.next();
            oi++;
            on_consumed();
            skip_deleted();
        } else {
            break;
        }
    }
    return a == edit.n_added && d == edit.n_deleted;
}

// The patcher's old → new string offset map. Between two edits every
// surviving string of a tier moves by one amount, so the map is held as runs
// over old offsets: a few thousand on a planet day instead of one pair per
// moved string (20.6M pairs, 157 MiB on every client). A run shifts every
// offset in [start, end), whether the patcher derived it from a tier the
// client holds or the diff sent it for one it doesn't. Only string starts
// are looked up: every stored reference is a string start or NO_DATA, which
// partition_strings_into_tiers guarantees and CI's byte identity check proves
// daily. Surviving strings the edits don't keep (they changed tier, or the
// POI tier's order moved them) are explicit pairs, read where the patch holds
// them. The runs spill to a ScratchTable as they are made, tier by tier in
// offset order, and finish() maps them with the pairs for lookups.
//
// Pairs are looked up first, but the order doesn't matter: a pair's old
// offset starts a string its old tier's edit doesn't keep, runs cover only
// kept strings, and a tier's runs lie inside its own old range, so no run
// covers a pair's offset.
class StringRemap {
public:
    StringRemap() = default;  // maps every offset to itself
    explicit StringRemap(const std::string& scratch_dir) : runs_(scratch_dir, "string runs") {}

    // Adds a run of a shipped tier's kept strings that moved, as its rebuild
    // (rebuild_string_tier) derives them. Lookups don't read the old pool.
    void add_run(uint32_t start, uint32_t end, uint32_t shift) { runs_.append({start, end, shift}); }
    // Decodes the runs encode_string_tier_runs wrote for the tier at old
    // offsets [tier_start, tier_end). Tiers are added in offset order: the
    // table refuses a run that steps back.
    void add_sent_tier(const char* data, size_t size, uint32_t n_runs, uint32_t tier_start, uint32_t tier_end) {
        size_t pos = 0;
        SectionReader in(data, size, pos, "string runs", PastEnd::MALFORMED);
        if (n_runs > size / 3) in.malformed();  // a run is at least 3 bytes
        uint64_t prev_end = tier_start;
        uint32_t shift = 0;
        for (uint32_t i = 0; i < n_runs; i++) {
            const uint64_t start = prev_end + in.varint();
            const uint64_t end = start + in.varint();
            shift += unzigzag32(in.varint());
            if (end == start || end > tier_end) in.malformed();
            runs_.append({static_cast<uint32_t>(start), static_cast<uint32_t>(end), shift});
            prev_end = end;
        }
        if (!in.at_end()) in.malformed();
    }
    // Every tier is in: maps the runs for lookups, with the moved strings'
    // pairs (the strings section's, where it sits).
    void finish(SortedTable<RemapPair> pairs = {}) {
        runs_.finish();
        pairs_ = std::move(pairs);
    }
    bool empty() const { return runs_.size() == 0 && pairs_.empty(); }
    size_t run_count() const { return runs_.size(); }

    uint32_t lookup(uint32_t off) const {
        if (const std::optional<RemapPair> p = pairs_.find(off)) return p->to;
        return through_run(off, runs_.table().floor(off));
    }
    // lookup with a run hint, for offsets that mostly ascend (a record
    // file's names in record order, as the POI tier is).
    uint32_t lookup(uint32_t off, size_t& hint) const {
        if (const std::optional<RemapPair> p = pairs_.find(off)) return p->to;
        return through_run(off, runs_.table().floor(off, hint));
    }

private:
    struct Run { uint32_t start, end, shift; };
    // off shifted by the run at `at`, its floor (the table's size for none).
    uint32_t through_run(uint32_t off, size_t at) const {
        const SortedTable<Run>& t = runs_.table();
        if (at == t.size()) return off;
        const Run r = t[at];
        return off < r.end ? off + r.shift : off;
    }

    ScratchTable<Run> runs_;
    SortedTable<RemapPair> pairs_;
};

// The string tiers that the given fields of a record file reference, as a
// bit per tier. tier_ends holds each tier's end offset (the cumulative
// sizes); NO_DATA references nothing, and any other value past the last tier
// can't be a string start, so it throws.
inline uint32_t referenced_string_tiers(const char* data, size_t size, size_t stride,
                                        const std::vector<size_t>& fields,
                                        const uint32_t (&tier_ends)[STRING_TIER_COUNT], const std::string& name) {
    uint32_t mask = 0;
    for (size_t at = 0; at + stride <= size; at += stride) {
        for (size_t off : fields) {
            uint32_t v;
            memcpy(&v, data + at + off, 4);
            if (v == 0xFFFFFFFFu) continue;
            int t = static_cast<int>(std::upper_bound(tier_ends, tier_ends + STRING_TIER_COUNT, v) - tier_ends);
            if (t == STRING_TIER_COUNT)
                throw std::runtime_error(name + " references string offset " + std::to_string(v) +
                                         " past the old pool (" + std::to_string(tier_ends[STRING_TIER_COUNT - 1]) + " bytes)");
            mask |= 1u << t;
        }
    }
    return mask;
}

// --- The strings section (STRINGS_TIERED_MARKER) ---

// The tiers a variant dir ships: a tier file the new dir holds (even empty)
// is shipped when the old dir holds it too, and the patcher rebuilds it from
// the client's own old file. A tier only the new dir holds is newly shipped:
// the patch carries its file whole in a section of its own, and the strings
// section treats it as unshipped (its runs are sent if the dir references it).
struct ShippedStringTiers { uint32_t shipped = 0, newly_shipped = 0; };

inline ShippedStringTiers shipped_string_tiers(const std::string& old_dir, const std::string& new_dir) {
    ShippedStringTiers out;
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        struct stat st;
        if (stat((new_dir + "/" + STRING_TIER_FILES[t]).c_str(), &st) != 0) continue;
        if (stat((old_dir + "/" + STRING_TIER_FILES[t]).c_str(), &st) == 0) out.shipped |= 1u << t;
        else out.newly_shipped |= 1u << t;
    }
    return out;
}

// The section id of a string tier file.
inline PatchFileId string_tier_file_id(int t) {
    return static_cast<PatchFileId>(static_cast<uint32_t>(PatchFileId::STRINGS_CORE) + t);
}

// One tier's old and new pool as the diff loaded them (the dir's own, or
// borrowed from ../full); null and 0 for a tier found nowhere.
struct StringTierPools {
    const char* old_data = nullptr;
    size_t old_size = 0;
    const char* new_data = nullptr;
    size_t new_size = 0;
};

// What append_strings_section wrote, for the diff's log, and the moved
// strings it didn't send as pairs (more than its cap), ascending by old: the
// records that hold them carry their new values as string fixups.
struct StringsSectionStats {
    uint32_t sent = 0, n_pairs = 0;
    std::array<uint32_t, STRING_TIER_COUNT> n_added{}, n_deleted{}, n_runs{}, runs_bytes{};
    std::vector<std::pair<uint32_t, uint32_t>> unsent_pairs;
};

// The tiers that hold strings but aren't shipped: the only ones whose runs
// can be sent, so the diff scans the old files only when this isn't 0.
inline uint32_t unshipped_string_tiers(const std::array<StringTierPools, STRING_TIER_COUNT>& tiers, uint32_t shipped) {
    uint32_t mask = 0;
    for (int t = 0; t < STRING_TIER_COUNT; t++)
        if (!(shipped >> t & 1) && tiers[t].old_size > 0) mask |= 1u << t;
    return mask;
}

// Appends the strings section of a dir that ships the tiers in `shipped` and
// whose own old files reference the tiers in `referenced`. moved holds the
// (old, new) global start of every surviving string (any iterable of pairs,
// such as the diff's per-string remap); the tiers' edits are made from it, so
// a survivor it misses is deleted and added. Each tier the dir looks up keeps
// what its edit can (kept_strings); the other survivors out of it, which
// changed tier or order, are sent as pairs, unless there are more than
// pairs_max of them: then none is, and they come back as unsent_pairs.
template <typename Moved>
StringsSectionStats append_strings_section(std::vector<char>& out,
                                           const std::array<StringTierPools, STRING_TIER_COUNT>& tiers,
                                           uint32_t shipped, uint32_t referenced, const Moved& moved,
                                           uint32_t pairs_max = STRING_PAIRS_MAX) {
    auto put = [&](const void* p, size_t n) { out.insert(out.end(), (const char*)p, (const char*)p + n); };
    std::array<uint32_t, STRING_TIER_COUNT + 1> old_base{}, new_base{};
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        old_base[t + 1] = old_base[t] + static_cast<uint32_t>(tiers[t].old_size);
        new_base[t + 1] = new_base[t] + static_cast<uint32_t>(tiers[t].new_size);
    }
    StringsSectionStats stats;
    stats.sent = referenced & unshipped_string_tiers(tiers, shipped);
    const uint32_t looked_up = shipped | referenced;

    // The survivors of each looked-up tier, as local (old, new) starts when
    // they stay in it; the ones that leave it are pairs.
    auto tier_of = [](uint32_t global, const std::array<uint32_t, STRING_TIER_COUNT + 1>& base) {
        for (int t = 0; t < STRING_TIER_COUNT; t++)
            if (global >= base[t] && global < base[t + 1]) return t;
        return -1;
    };
    std::array<std::vector<std::pair<uint32_t, uint32_t>>, STRING_TIER_COUNT> common;
    std::vector<std::pair<uint32_t, uint32_t>> pairs;
    for (const auto& [og, ng] : moved) {
        const int ot = tier_of(og, old_base);
        if (ot < 0 || !(looked_up >> ot & 1)) continue;
        if (tier_of(ng, new_base) == ot) common[ot].push_back({og - old_base[ot], ng - new_base[ot]});
        else pairs.push_back({og, ng});
    }

    uint32_t marker = STRINGS_TIERED_MARKER;
    put(&marker, 4);
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        uint32_t old_size = static_cast<uint32_t>(tiers[t].old_size);
        uint32_t new_size = static_cast<uint32_t>(tiers[t].new_size);
        put(&old_size, 4); put(&new_size, 4);
    }
    put(&shipped, 4); put(&stats.sent, 4);
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        if (!(looked_up >> t & 1)) continue;
        std::sort(common[t].begin(), common[t].end());
        const std::vector<std::pair<uint32_t, uint32_t>> kept = kept_strings(common[t]);
        size_t k = 0;
        for (const auto& [o, n] : common[t]) {
            if (k < kept.size() && kept[k].first == o) k++;
            else pairs.push_back({old_base[t] + o, new_base[t] + n});
        }
        common[t] = {};
        const StringTierPools& p = tiers[t];
        if (stats.sent >> t & 1) {
            auto runs = encode_string_tier_runs(p.old_data, p.old_size, old_base[t], new_base[t], kept);
            uint32_t runs_size = static_cast<uint32_t>(runs.bytes.size());
            put(&runs.n_runs, 4); put(&runs_size, 4);
            out.insert(out.end(), runs.bytes.begin(), runs.bytes.end());
            stats.n_runs[t] = runs.n_runs;
            stats.runs_bytes[t] = runs_size;
            continue;
        }
        if (!(shipped >> t & 1)) continue;
        const StringTierDiff d = string_tier_diff(p.old_data, p.old_size, p.new_data, p.new_size, kept);
        append_string_tier_diff(out, d);
        stats.n_added[t] = static_cast<uint32_t>(d.added.size());
        stats.n_deleted[t] = static_cast<uint32_t>(d.deleted.size());
    }

    std::sort(pairs.begin(), pairs.end());
    if (pairs.size() > pairs_max) stats.unsent_pairs.swap(pairs);
    stats.n_pairs = static_cast<uint32_t>(pairs.size());
    put(&stats.n_pairs, 4);
    for (const auto& [og, ng] : pairs) { put(&og, 4); put(&ng, 4); }
    return stats;
}

// A sent tier's runs (encode_string_tier_runs), pointing into the patch.
struct SentStringRuns {
    const char* data = nullptr;
    uint32_t size = 0, n_runs = 0;
};

// Where a shipped tier's walk (rebuild_string_tier) puts its strings: the
// tier's old and new global offsets, and the size of its new pool.
struct StringTierPlace {
    uint32_t old_base = 0, new_base = 0, new_size = 0;
};

struct StringsSection {
    // Tier t sits at old offsets [old_base[t], old_base[t + 1]); new likewise.
    std::array<uint32_t, STRING_TIER_COUNT + 1> old_base{}, new_base{};
    uint32_t shipped = 0, sent = 0;
    std::array<StringTierEdit, STRING_TIER_COUNT> edits;  // shipped tiers
    std::array<SentStringRuns, STRING_TIER_COUNT> runs;   // sent tiers
    SortedTable<RemapPair> pairs;                         // moved strings, by old offset
    uint32_t old_size(int t) const { return old_base[t + 1] - old_base[t]; }
    uint32_t new_size(int t) const { return new_base[t + 1] - new_base[t]; }
    StringTierPlace place(int t) const { return {old_base[t], new_base[t], new_size(t)}; }
};

// Parses the section at data[pos, size) and advances pos past it.
inline StringsSection parse_strings_section(const char* data, size_t size, size_t& pos) {
    SectionReader in(data, size, pos, "strings section");
    if (in.u32() != STRINGS_TIERED_MARKER) throw std::runtime_error("Patch has no strings section");
    StringsSection s;
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        const uint32_t old_size = in.u32(), new_size = in.u32();
        if ((uint64_t)s.old_base[t] + old_size > UINT32_MAX || (uint64_t)s.new_base[t] + new_size > UINT32_MAX)
            in.malformed();
        s.old_base[t + 1] = s.old_base[t] + old_size;
        s.new_base[t + 1] = s.new_base[t] + new_size;
    }
    s.shipped = in.u32();
    s.sent = in.u32();
    if ((s.shipped | s.sent) >> STRING_TIER_COUNT || (s.shipped & s.sent)) in.malformed();
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        if (s.sent >> t & 1) {
            SentStringRuns& r = s.runs[t];
            r.n_runs = in.u32();
            r.size = in.u32();
            r.data = in.take(r.size);
            continue;
        }
        if (s.shipped >> t & 1) s.edits[t] = parse_string_tier_edit(data, size, pos);
    }
    const uint32_t n_pairs = in.u32();
    if (n_pairs > STRING_PAIRS_MAX) in.malformed();
    s.pairs = SortedTable<RemapPair>(in.take_items(n_pairs, sizeof(RemapPair)), n_pairs, "strings section");
    return s;
}

// A shipped tier is a client file the dir rebuilds from its own old one, so
// it must be listed. A listed tier without the bit is one the old dir lacked:
// its file arrives whole through its own section.
inline void require_shipped_tiers_listed(uint32_t shipped, const std::unordered_set<std::string>& listed) {
    for (int t = 0; t < STRING_TIER_COUNT; t++)
        if ((shipped >> t & 1) && !listed.count(STRING_TIER_FILES[t]))
            throw std::runtime_error(std::string("Strings section ships ") + STRING_TIER_FILES[t] +
                                     ", which the client files don't list");
}

// What a shipped tier's walk (rebuild_string_tier) read and wrote: the hash of
// the whole old pool, the size of the new pool written and its hash as the
// place's new_size bytes, and whether the old pool placed every index of the
// edit.
struct StringTierRebuild {
    uint64_t old_hash = 0, new_hash = 0, new_size = 0;
    bool placed = false;
};

// Rebuilds a shipped tier in one walk of its old file and edit: writes the new
// pool (the old pool's kept strings with the added ones at their indices)
// through write(bytes, n), a string at a time, adds the runs of the kept
// strings that moved to remap, and hashes the old pool as the walk decodes it
// and the new one as it writes it. on_passed(coded_off, table_off) once the
// walk has decoded STRING_WALK_PIECE or more of the old file's body past the
// last call: it reads none of the body before coded_off again, nor of the
// bucket table (which follows the body) before table_off bytes into it. The
// walk decodes the whole old file even when the edit doesn't fit
// (require_string_tier_rebuilt), and the decoder refuses a file that is not
// an FCC2 encoding.
template <typename Write, typename OnPassed>
StringTierRebuild rebuild_string_tier(string_files::Decoder& old_file, const StringTierEdit& edit,
                                      const StringTierPlace& at, StringRemap& remap, Write write, OnPassed on_passed) {
    StringTierRebuild r;
    OldPoolWalk old(old_file);
    ContentHasher new_hash(at.new_size);
    StringRunJoiner joiner([&](uint32_t start, uint32_t end, uint32_t shift) { remap.add_run(start, end, shift); });
    uint32_t new_start = at.new_base;
    auto on_string = [&](const char* p, size_t len, size_t old_off) {
        new_hash.update(p, len);
        write(p, len);
        r.new_size += len;
        if (old_off != NO_OLD_STRING) {
            const uint32_t start = at.old_base + static_cast<uint32_t>(old_off);
            joiner.kept(start, start + static_cast<uint32_t>(len), new_start);
        }
        new_start += static_cast<uint32_t>(len);
    };
    uint64_t passed = 0;
    auto on_consumed = [&] {
        if (old.coded_end() - passed < STRING_WALK_PIECE) return;
        passed = old.coded_end();
        on_passed(static_cast<size_t>(passed), static_cast<size_t>(old.table_read()));
    };
    r.placed = for_each_new_string(old, edit, on_string, on_consumed);
    joiner.finish();
    r.old_hash = old.hash();
    r.new_hash = new_hash.hash();
    return r;
}

// Refuses a shipped tier's rebuild by its first fault: an old pool other than
// the one the diff saw, then an edit the old pool can't place, then a new pool
// other than the new build's.
inline void require_string_tier_rebuilt(const StringTierRebuild& r, const StringTierEdit& edit, const StringTierPlace& at,
                                        const std::string& name) {
    if (r.old_hash != edit.old_hash) throw std::runtime_error("Old " + name + " is not the one the patch was made from");
    if (!r.placed) throw std::runtime_error("Malformed strings section");
    if (r.new_size != at.new_size || r.new_hash != edit.new_hash)
        throw std::runtime_error("Rebuilt " + name + " does not match the new build");
}

// Fills remap from the section, tier by tier in offset order: a sent tier
// from its runs, a shipped tier through on_shipped(t), which rebuilds it
// (rebuild_string_tier, which adds its runs). Then it finishes the remap with
// the moved strings.
template <typename OnShipped>
void load_string_remap(const StringsSection& s, StringRemap& remap, OnShipped on_shipped) {
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        if (s.sent >> t & 1) {
            const SentStringRuns& r = s.runs[t];
            remap.add_sent_tier(r.data, r.size, r.n_runs, s.old_base[t], s.old_base[t + 1]);
        } else if (s.shipped >> t & 1) {
            on_shipped(t);
        }
    }
    remap.finish(s.pairs);
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
    // A record past the last u32 record index (0xFFFFFFFE) moved, which no
    // run or value can give: encode_offset_fixups stopped there, and
    // check_offset_fixups refuses them.
    bool past_last_index = false;
    bool empty() const { return n_runs == 0 && n_values == 0; }
};

// The most a merge section's offset fixups may hold: its header gives the
// count and byte size of their runs and of their values as u32s. Lower for
// tests.
struct OffsetFixupLimits {
    uint32_t count = UINT32_MAX;
    uint32_t bytes = UINT32_MAX;
};

// Refuses the offset fixups of the merge of `file` (named in the refusal)
// that its section's header can't give: more runs or values than the limits'
// count, more bytes of runs or of values than their bytes (the header's u32s
// would wrap, and the patcher refuse the section), or a record moved past
// the last u32 record index. The diff checks them as it writes the section,
// on its main thread: they are encoded on the merge threads, where a throw
// would end the process.
inline void check_offset_fixups(const OffsetFixups& f, const std::string& file, OffsetFixupLimits limits = {}) {
    auto refuse = [&](const std::string& why) { throw std::runtime_error("Offset fixups of " + file + ": " + why); };
    if (f.past_last_index) refuse("a record past the last u32 record index moved");
    auto at_most = [&](uint64_t n, uint32_t limit, const char* what) {
        if (n > limit) refuse("more than the section's " + std::to_string(limit) + " " + what);
    };
    at_most(f.n_runs, limits.count, "runs");
    at_most(f.n_values, limits.count, "values");
    at_most(f.runs.size(), limits.bytes, "bytes of runs");
    at_most(f.values.size(), limits.bytes, "bytes of values");
}

// fixed_at(i): record i's offset after the fixup passes, for each record i
// of old_offsets (a size_t: a file may hold more than a u32 indexes). Each
// run starts and each value sits at a record index of its own below
// 0xFFFFFFFF (NO_DATA), so neither count wraps its u32; a record past them
// that moves stops the encoding (past_last_index).
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
    for (size_t at = 0; at < old_offsets.size(); at++) {
        const uint32_t old = old_offsets[at], fixed = fixed_at(at);
        if (at >= NO_DATA && (old == NO_DATA ? fixed != NO_DATA : fixed != old)) {
            out.past_last_index = true;
            break;
        }
        // Below NO_DATA wherever the record moved; a record past it that
        // didn't only closes the open run.
        const uint32_t i = static_cast<uint32_t>(at);
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

// Applies encoded OffsetFixups to old records visited in ascending order,
// read where they sit. Anything encode_offset_fixups never writes is
// malformed: a value index that doesn't ascend (only the first value's delta
// may be 0), a value for a record with an offset (values give NO_DATA
// records theirs), a run of no records, a run or value index past the last
// u32 record index (each bound summed in 64 bits), and, once the replay is
// over (finish), a run or value past the old file's records or bytes past
// the last of either. A record past the last u32 record index (of a file of
// more than 0xFFFFFFFF records) keeps its offset: no run or value reaches
// it, and the encoder refuses one there that moves (past_last_index). The
// replay leaves such records out of apply (reach), once per MATCH run.
class OffsetFixupReader {
public:
    OffsetFixupReader(const char* runs, size_t runs_size, uint32_t n_runs,
                      const char* values, size_t values_size, uint32_t n_values)
        : runs_(runs), runs_size_(runs_size), runs_left_(n_runs),
          values_(values), values_size_(values_size), values_left_(n_values) {
        next_run();
        next_value();
    }

    // How many of a MATCH run's count records, from old record first on,
    // the fixups reach: those below the last u32 record index, which the
    // replay hands apply. The rest keep their offsets: as a u32, the index
    // of one is END (an exhausted value index's) or wraps onto record 0 on,
    // whose value or run apply would give it.
    static uint32_t reach(uint64_t first, uint32_t count) {
        if (first >= END) return 0;
        return END - first < count ? static_cast<uint32_t>(END - first) : count;
    }
    // Record idx's offset after the fixups, given its old offset; idx below
    // END, as reach gives them.
    uint32_t apply(uint32_t idx, uint32_t old) {
        while (value_idx_ < idx) next_value();
        if (value_idx_ == idx) {
            if (old != END) malformed();
            return value_;
        }
        if (old == END) return old;
        while (run_end_ <= idx) next_run();
        return idx >= run_start_ ? old + shift_ : old;
    }
    // The replay is over: the runs and values it didn't reach (a deleted or
    // replaced record's) lie within the old file's n_old_records, and nothing
    // follows the last of either.
    void finish(uint64_t n_old_records) {
        for (; value_idx_ != END; next_value())
            if (value_idx_ >= n_old_records) malformed();
        for (; run_start_ != END; next_run())
            if (run_end_ > n_old_records) malformed();
        if (runs_pos_ != runs_size_ || values_pos_ != values_size_) malformed();
    }

private:
    // NO_DATA, past every record index a run or value gives, and the start
    // of no run: a run may end there (after record END - 1), but no run or
    // value starts there.
    static constexpr uint32_t END = 0xFFFFFFFFu;

    [[noreturn, gnu::cold, gnu::noinline]] static void malformed() { throw std::runtime_error("Malformed offset fixups"); }
    void next_run() {
        if (runs_left_ == 0) { run_start_ = run_end_ = END; return; }
        runs_left_--;
        SectionReader in(runs_, runs_size_, runs_pos_, "offset fixups", PastEnd::MALFORMED);
        const uint64_t start = uint64_t{run_end_} + in.varint();
        const uint32_t length = in.varint();
        if (length == 0 || start + length > END) malformed();
        run_start_ = static_cast<uint32_t>(start);
        run_end_ = static_cast<uint32_t>(start + length);
        shift_ += unzigzag32(in.varint());
    }
    void next_value() {
        if (values_left_ == 0) { value_idx_ = END; return; }
        values_left_--;
        SectionReader in(values_, values_size_, values_pos_, "offset fixups", PastEnd::MALFORMED);
        const bool first = value_idx_ == END;
        const uint32_t delta = in.varint();
        const uint64_t idx = (first ? 0 : uint64_t{value_idx_}) + delta;
        if ((delta == 0 && !first) || idx >= END) malformed();
        value_idx_ = static_cast<uint32_t>(idx);
        value_ = in.varint();
    }

    const char* runs_;
    size_t runs_size_, runs_pos_ = 0;
    uint32_t runs_left_, run_start_ = 0, run_end_ = 0, shift_ = 0;
    const char* values_;
    size_t values_size_, values_pos_ = 0;
    uint32_t values_left_, value_idx_ = END, value_ = 0;
};

// --- String fixups (v12) ---
//
// A moved string the strings section sends no pair for keeps its old offset
// through the patcher's string remap, the tiers' runs. Each merge section
// carries, after its offset fixups, the new value of every string field of
// its MATCH records that the remap would leave wrong: n_string_fixups u32,
// string_fixups_size u32, then per fixup varints of (slots skipped since the
// previous fixup, zigzag change of the value), from slot 0 and value 0. A
// field's slot is its record's old index × the file's string fields
// (string_field_offsets) + its index among them, so a replay reads the fixups
// in order as it rewrites its MATCH records, and a file without string fields
// carries none.

// The most a merge section's string fixups may hold: its header gives their
// count and byte size as u32s. Lower for tests.
struct StringFixupLimits {
    uint32_t count = UINT32_MAX;
    uint32_t bytes = UINT32_MAX;
};

// A merge section's string fixups as the diff writes them, slot by slot in
// ascending order, for the merge of `file` (named in refusals). A fixup that
// would take them past their limits, or skip more slots since the previous
// one than a u32 varint holds, is refused before a byte of it is written: the
// header's u32s would wrap, and the patcher refuse the section.
class StringFixups {
public:
    explicit StringFixups(std::string file = "a merge", StringFixupLimits limits = {})
        : file_(std::move(file)), limits_(limits) {}

    void add(uint64_t slot, uint32_t value) {
        if (slot < from_) throw std::logic_error("String fixups out of slot order");
        if (slot - from_ > UINT32_MAX) refuse("more than " + std::to_string(UINT32_MAX) + " slots skipped between two");
        const uint32_t skipped = static_cast<uint32_t>(slot - from_), change = zigzag32(value - value_);
        if (count_ == limits_.count) refuse("more than the section's " + std::to_string(limits_.count) + " fixups");
        // bytes_ holds at most limits_.bytes, so the room left can't wrap.
        if (varint_size(skipped) + varint_size(change) > limits_.bytes - bytes_.size())
            refuse("more than the section's " + std::to_string(limits_.bytes) + " bytes");
        write_varint(bytes_, skipped);
        write_varint(bytes_, change);
        from_ = slot + 1;
        value_ = value;
        count_++;
    }
    // The count and byte size the section's header gives, within the limits.
    uint32_t count() const { return count_; }
    uint32_t size() const { return static_cast<uint32_t>(bytes_.size()); }
    const std::vector<char>& bytes() const { return bytes_; }

private:
    [[noreturn]] void refuse(const std::string& why) const {
        throw std::runtime_error("String fixups of " + file_ + ": " + why);
    }

    std::string file_;
    StringFixupLimits limits_;
    std::vector<char> bytes_;
    uint32_t count_ = 0, value_ = 0;
    uint64_t from_ = 0;  // the first slot the next fixup may have
};

// Reads a merge section's string fixups where they sit, as its replay
// rewrites the string fields of its MATCH records slot by slot: fixes(slot) is
// whether the next fixup is that slot's, and take() gives its value. A fixup
// for a slot the replay passed without matching (a deleted or replaced
// record's), one left at the end, and bytes past the last are malformed.
class StringFixupReader {
public:
    StringFixupReader(const char* data, size_t size, uint32_t n) : data_(data), size_(size), left_(n) { next(); }

    bool fixes(uint64_t slot) {
        if (slot_ > slot) return false;
        if (slot_ < slot) malformed();
        return true;
    }
    uint32_t take() {
        const uint32_t value = value_;
        next();
        return value;
    }
    // The replay is over: every fixup was taken, and nothing follows them.
    void finish() const {
        if (slot_ != END || pos_ != size_) malformed();
    }

private:
    static constexpr uint64_t END = UINT64_MAX;

    [[noreturn, gnu::cold, gnu::noinline]] static void malformed() { throw std::runtime_error("Malformed string fixups"); }
    void next() {
        if (left_ == 0) {
            slot_ = END;
            return;
        }
        left_--;
        SectionReader in(data_, size_, pos_, "string fixups", PastEnd::MALFORMED);
        slot_ = from_ + in.varint();
        from_ = slot_ + 1;
        value_ += unzigzag32(in.varint());
    }

    const char* data_;
    size_t size_, pos_ = 0;
    uint32_t left_, value_ = 0;
    uint64_t slot_ = END, from_ = 0;
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
// Creates the directory and its missing parents; the path is never shell text.
inline void ensure_dir(const std::string& path) {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) throw std::runtime_error("Cannot create directory " + path + ": " + ec.message());
}
