#pragma once
// Shared definitions for the geocoder patch system.
// Used by geocoder-diff and geocoder-patch.

#include <algorithm>
#include <array>
#include <cstdint>
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
#include <type_traits>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "sequential_file_reader.h"

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
// polygon index (admin id-space).
static constexpr size_t POI_RECORD_VERTEX_OFFSET_OFF = 8;
static constexpr size_t POI_RECORD_NAME_ID_OFF = 16;
static constexpr size_t POI_RECORD_PARENT_STREET_ID_OFF = 24;
static constexpr size_t POI_RECORD_PARENT_POSTCODE_ID_OFF = 28;
static constexpr size_t POI_RECORD_PARENT_POLY_ID_OFF = 32;

// PlaceNode byte offsets. name_id is a string-pool offset; parent_poly_id is
// an admin polygon index.
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
static constexpr uint32_t GCPATCH_VERSION = 9;

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
    // Secondary parallel arrays (SPARSE_DELTA_FILES), the postcode
    // centroid index (a cell list delta) and the postal polygons, which
    // merge like the admin ones.
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
    // (build_version bump 8→9), byte-merged along addr_points like the
    // other vertex streams.
    ADDR_VERTICES = 30,
    // Per-tier strings files (build_version 14). Replaces the single
    // STRINGS=0 slot. They travel in the strings section
    // (STRINGS_TIERED_MARKER); a tier the old dir lacks comes whole in a
    // full-replacement section.
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

// The string offset fields of a record file the patch merges, as byte
// offsets in its record; empty for files without any.
// AddrPoint byte 16 is parent_way_id, a way id, not a string offset.
inline std::vector<size_t> string_field_offsets(PatchFileId fid) {
    switch (fid) {
    case PatchFileId::ADDR_POINTS: return {ADDR_POINT_HOUSENUMBER_ID_OFF, ADDR_POINT_STREET_ID_OFF};
    case PatchFileId::STREET_WAYS: return {WAY_HEADER_NAME_ID_OFF};
    case PatchFileId::INTERP_WAYS: return {INTERP_WAY_STREET_ID_OFF};
    case PatchFileId::ADMIN_POLYGONS:
    case PatchFileId::POSTAL_POLYGONS: return {ADMIN_POLYGON_NAME_ID_OFF};
    case PatchFileId::POI_RECORDS:
        return {POI_RECORD_NAME_ID_OFF, POI_RECORD_PARENT_STREET_ID_OFF, POI_RECORD_PARENT_POSTCODE_ID_OFF};
    case PatchFileId::PLACE_NODES: return {PLACE_NODE_NAME_ID_OFF};
    default: return {};
    }
}

// The files sent as SPARSE_DELTA_STRIDE sections, in emission order, with
// the value each position holds:
//   remap_kind 0 = raw, 1 = admin polygon id, 2 = string offset,
//   3 = postcode_centroid (16 bytes, string offset at byte 8).
struct SparseDeltaFile { PatchFileId fid; uint32_t value_stride, remap_kind; };
static constexpr SparseDeltaFile SPARSE_DELTA_FILES[] = {
    {PatchFileId::ADDR_POSTCODES, 4, 2},
    {PatchFileId::ADMIN_PARENTS, 4, 1},
    {PatchFileId::WAY_PARENTS, 4, 1},
    {PatchFileId::WAY_POSTCODES, 4, 2},
    {PatchFileId::INTERP_POSTCODES, 4, 2},
    {PatchFileId::POSTCODE_CENTROIDS, 16, 3},
};
static constexpr size_t POSTCODE_CENTROID_POSTCODE_ID_OFF = 8;

// The record files with string fields (string_field_offsets) and their
// strides. Postal polygons are admin polygons of admin_level 11, at the admin
// stride.
struct StringRecordFile {
    PatchFileId fid;
    size_t stride;
};
static constexpr StringRecordFile STRING_RECORD_FILES[] = {
    {PatchFileId::ADDR_POINTS, ADDR_POINT_STRIDE},
    {PatchFileId::STREET_WAYS, WAY_HEADER_STRIDE},
    {PatchFileId::INTERP_WAYS, INTERP_WAY_STRIDE},
    {PatchFileId::ADMIN_POLYGONS, ADMIN_POLYGON_STRIDE},
    {PatchFileId::POSTAL_POLYGONS, ADMIN_POLYGON_STRIDE},
    {PatchFileId::POI_RECORDS, POI_RECORD_STRIDE},
    {PatchFileId::PLACE_NODES, PLACE_NODE_STRIDE},
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

// The entries files the geo walk derives and corrects, and those of the
// cell indexes rebuilt from an id remap.
using EntriesFiles = std::array<PatchFileId, 3>;
static constexpr EntriesFiles GEO_ENTRIES_FILES = {PatchFileId::STREET_ENTRIES, PatchFileId::ADDR_ENTRIES,
                                                   PatchFileId::INTERP_ENTRIES};
static constexpr EntriesFiles CELL_INDEX_ENTRIES_FILES = {PatchFileId::ADMIN_ENTRIES, PatchFileId::POI_ENTRIES,
                                                          PatchFileId::PLACE_ENTRIES};

// Secondary ID remap marker: 0xFFFFFFF6
// Additional old→new ID mappings for modified records (recovered by relaxed key matching).
// Both diff and patch tools must apply these to their derived remaps for consistent results.
// Format: marker(4), n_files(4),
//   for each: file_id(4), n_pairs(4), [(old_id:u32, new_id:u32)] × n_pairs
static constexpr uint32_t SECONDARY_REMAP_MARKER = 0xFFFFFFF6;

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
//              shift runs from the same walk (StringRemap::add_tier),
//     sent: n_runs u32, runs_size u32, the tier's shift runs
//           (encode_string_tier_runs),
//   n_pairs u32, (old u32, new u32) × n_pairs: surviving strings no tier's
//     edit keeps, which changed tier or order (ascending by old, each string
//     once), for every old tier the dir ships or references.
// The patcher reads every list where it sits in the patch.
// A tier neither shipped nor referenced sends nothing: no lookup lands in it.
static constexpr uint32_t STRINGS_TIERED_MARKER = 0xFFFFFFF6;

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

// Stride sentinel: a cell index (<name>_cells.bin + <name>_entries.bin)
// patched per cell. Strategy-2 ids keep most lists unchanged day to day, so
// only the cells that lost or gained ids travel. The section names the cells
// file and the patcher writes both files.
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
struct ParallelDeltaFile { PatchFileId fid, parent; uint32_t value_stride, remap_kind; };
static constexpr ParallelDeltaFile PARALLEL_DELTA_FILES[] = {
    {PatchFileId::ADDR_POSTCODES, PatchFileId::ADDR_POINTS, 4, 2},
    {PatchFileId::WAY_PARENTS, PatchFileId::STREET_WAYS, 4, 1},
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
// Only entries where old_id != new_id are transmitted, each leg ascending
// by old_id, each id once; the patcher reads the legs where they sit
// (parse_parent_id_remap).
static constexpr uint32_t POI_PARENT_REMAP_MARKER = 0xFFFFFFF3;

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
        if (n) memcpy(dst, data + pos, n);  // an empty inline file's bytes are null
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

// --- Old sizes of the files no section names (v7) ---
// Every section names the size of the old file it replays. The patcher also
// reads old files no section names: the cell indexes of the entry pipeline
// and the entries file beside a cell list delta. Right after the client
// files, the diff records their sizes in the old dir (0 when absent):
//   n(4), n × {file_id(4), old_size(8)}
// and the patcher checks them before it reads anything else.
static constexpr PatchFileId UNSECTIONED_OLD_FILES[] = {
    PatchFileId::GEO_CELLS, PatchFileId::STREET_ENTRIES, PatchFileId::ADDR_ENTRIES, PatchFileId::INTERP_ENTRIES,
    PatchFileId::ADMIN_CELLS, PatchFileId::ADMIN_ENTRIES, PatchFileId::POI_CELLS, PatchFileId::POI_ENTRIES,
    PatchFileId::PLACE_CELLS, PatchFileId::PLACE_ENTRIES, PatchFileId::POSTCODE_CENTROID_ENTRIES,
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
    auto take = [&](void* dst, size_t n) {
        if (pos > size || n > size - pos) throw std::runtime_error("Truncated old file sizes");
        memcpy(dst, data + pos, n);
        pos += n;
    };
    uint32_t n = 0;
    take(&n, 4);
    for (uint32_t i = 0; i < n; i++) {
        uint32_t id = 0;
        uint64_t old_size = 0;
        take(&id, 4);
        take(&old_size, 8);
        if (id >= static_cast<uint32_t>(PatchFileId::COUNT)) throw std::runtime_error("Malformed old file sizes");
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
class EntriesFileSections {
public:
    EntriesFileSections(const char* kind, const EntriesFiles& files) : kind_(kind), files_(files) {}

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
    EntriesFiles files_;
    std::array<std::optional<PatchRef>, std::tuple_size<EntriesFiles>::value> refs_;
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

// n fixed-width records read where they sit, sorted strictly ascending by
// the u32 each starts with. The constructor checks the order in one sweep,
// refusing a step back or a repeated key as a malformed `what`, and samples
// every step-th key into a directory, so a lookup searches one step of the
// records.
template <typename Rec>
class SortedTable {
    static_assert(std::is_trivially_copyable<Rec>::value && sizeof(Rec) >= sizeof(uint32_t), "a record starts with its u32 key");

public:
    SortedTable() = default;  // none
    SortedTable(const char* data, size_t count, const char* what) : data_(data), count_(count) {
        step_ = std::max<size_t>(1, (count + TABLE_DIRECTORY_KEYS - 1) / TABLE_DIRECTORY_KEYS);
        directory_.reserve((count + step_ - 1) / step_);
        for (size_t i = 0; i < count; i++) {
            const uint32_t key = key_at(i);
            if (i > 0 && key <= key_at(i - 1)) throw std::runtime_error(std::string("Malformed ") + what);
            if (i % step_ == 0) directory_.push_back(key);
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
    // The record of `key`, if the table holds one. A table of at most
    // TABLE_DIRECTORY_KEYS records keeps every key in its directory, so a
    // lookup that misses reads none of its records.
    std::optional<Rec> find(uint32_t key) const {
        const size_t d = static_cast<size_t>(std::upper_bound(directory_.begin(), directory_.end(), key) - directory_.begin());
        if (d == 0) return std::nullopt;
        size_t at = (d - 1) * step_;
        if (directory_[d - 1] != key) {
            const size_t end = std::min(count_, at + step_);
            at++;
            for (size_t hi = end; at < hi;) {
                const size_t mid = at + (hi - at) / 2;
                if (key_at(mid) < key) at = mid + 1;
                else hi = mid;
            }
            if (at == end || key_at(at) != key) return std::nullopt;
        }
        return (*this)[at];
    }

private:
    uint32_t key_at(size_t i) const {
        uint32_t key;
        memcpy(&key, data_ + i * sizeof(Rec), sizeof key);
        return key;
    }

    const char* data_ = nullptr;
    size_t count_ = 0, step_ = 1;
    std::vector<uint32_t> directory_;  // the key of every step_-th record
};

// An old id or string offset and its new one, as the patch carries remap
// pairs: (old u32, new u32).
struct RemapPair {
    uint32_t from, to;
};

// The parent-id remap after its marker (POI_PARENT_REMAP_MARKER), where it
// sits: the admin and street legs. The reserved postcode leg is skipped.
struct ParentIdRemap {
    SortedTable<RemapPair> admin, street;
    uint32_t n_postcode = 0;
};

// Reads the section at data[pos, size) and advances pos past it.
inline ParentIdRemap parse_parent_id_remap(const char* data, size_t size, size_t& pos) {
    auto take = [&](uint64_t n) {
        if (pos > size || n > size - pos) throw std::runtime_error("Truncated POI parent-id remap");
        const char* p = data + pos;
        pos += static_cast<size_t>(n);
        return p;
    };
    auto count = [&] {
        uint32_t n;
        memcpy(&n, take(4), 4);
        return n;
    };
    auto leg = [&] {
        const uint32_t n = count();
        return SortedTable<RemapPair>(take(uint64_t{n} * sizeof(RemapPair)), n, "POI parent-id remap");
    };
    ParentIdRemap r;
    r.admin = leg();
    r.street = leg();
    r.n_postcode = count();
    take(uint64_t{r.n_postcode} * sizeof(RemapPair));
    return r;
}

// --- Cell index rebuild ---

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
                if (count) memcpy(cells[i].ids.data(), old_entries.data() + off + 2, count * 4);
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
        if (n) memcpy(ids.data(), entries.data() + off + 2, (size_t)n * 4);
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
// entry remap map) cost over 1 GiB. The old cells and entries are read
// front to back through size() and at(off, n), a view that lasts until the
// next call (SequentialFileReader); the cell changes and the delta where
// they sit. Returns the entries bytes written.
template <typename Cells, typename Entries, typename Remap, typename OutCells, typename OutEntries>
uint64_t stream_cell_index(Cells&& cells, Entries&& entries, AscendingIds added, AscendingIds removed, Remap remap,
                           const char* delta, size_t delta_size, OutCells out_cells, OutEntries out_entries) {
    auto malformed = [] { throw std::runtime_error("Malformed cell list delta"); };
    auto bad_index = [] { throw std::runtime_error("Malformed cell index"); };

    size_t pos = 0;
    auto take = [&](void* dst, size_t n) {
        if (delta_size - pos < n) malformed();
        if (n) memcpy(dst, delta + pos, n);  // an empty list's data() may be null
        pos += n;
    };
    uint32_t n_gone;
    take(&n_gone, 4);
    if ((delta_size - pos) / 8 < n_gone) malformed();
    AscendingIds gone(delta + pos, n_gone, "cell list delta");
    pos += (size_t)n_gone * 8;
    uint32_t n_set, set_i = 0;
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

    // The next rebuilt cell: old cells minus `removed`, merged with `added`.
    const uint64_t entries_size = entries.size();
    size_t n_cells = cells.size() / 12, i = 0;
    uint64_t prev_old = 0;
    std::vector<uint32_t> ids;
    auto next_source = [&](uint64_t& cid) -> bool {
        for (;;) {
            uint64_t old_cid = NONE;
            uint32_t off = 0;
            if (i < n_cells) {
                const char* rec = cells.at((uint64_t)i * 12, 12);
                memcpy(&old_cid, rec, 8);
                memcpy(&off, rec + 8, 4);
                if (i > 0 && old_cid <= prev_old) bad_index();
            }
            uint64_t add_cid = added.done() ? NONE : added.front();
            if (old_cid == NONE && add_cid == NONE) return false;
            if (add_cid < old_cid) {
                cid = add_cid; added.pop();
                ids.clear();
                return true;
            }
            if (add_cid == old_cid) bad_index();
            prev_old = old_cid;
            i++;
            if (removed.skip_to(old_cid)) continue;
            cid = old_cid;
            ids.clear();
            if (off != 0xFFFFFFFF) {
                uint16_t n;
                if ((uint64_t)off + 2 > entries_size) bad_index();
                memcpy(&n, entries.at(off, 2), 2);
                if ((uint64_t)off + 2 + (uint64_t)n * 4 > entries_size) bad_index();
                ids.resize(n);
                if (n) memcpy(ids.data(), entries.at((uint64_t)off + 2, (size_t)n * 4), (size_t)n * 4);
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
        } else if (!gone.skip_to(src)) {
            emit(src, ids);
        }
        have = next_source(cid);
    }
    removed.require_ascending();
    gone.require_ascending();
    if (pos != delta_size) malformed();
    return written;
}

// stream_cell_index for an index whose ids don't move (the postcode centroid
// index): the old index plus the delta.
template <typename Cells, typename Entries, typename OutCells, typename OutEntries>
uint64_t stream_cell_list_delta(Cells&& cells, Entries&& entries,
                                const char* delta, size_t delta_size, OutCells out_cells, OutEntries out_entries) {
    return stream_cell_index(cells, entries, {}, {}, [](uint32_t id) { return id; },
                             delta, delta_size, out_cells, out_entries);
}

// --- Geo entry list deltas (GEO_ENTRY_DELTA_MARKER) ---

// Bytes in memory behind the size() / at(off, n) interface the patcher's
// file readers have (SequentialFileReader), for readers over a buffer.
struct ByteSpan {
    const char* data;
    size_t bytes;
    uint64_t size() const { return bytes; }
    const char* at(uint64_t off, size_t) const { return data + off; }
};

// Reads the (u16 count, u32 ids) list at `off` of a street / addr / interp
// entries file (size() / at(off, n), like the cell index streams) into `ids`.
// NO_DATA, or a list running past the end of the file, reads as empty; the
// bounds are checked in 64 bits, so an offset near 4 GiB can't wrap.
template <typename Entries>
void read_entry_list(Entries& entries, uint32_t off, std::vector<uint32_t>& ids) {
    ids.clear();
    if (off == 0xFFFFFFFFu || (uint64_t)off + 2 > entries.size()) return;
    uint16_t n;
    memcpy(&n, entries.at(off, 2), 2);
    if ((uint64_t)off + 2 + (uint64_t)n * 4 > entries.size()) return;
    ids.resize(n);
    if (n) memcpy(ids.data(), entries.at((uint64_t)off + 2, (size_t)n * 4), (size_t)n * 4);
}

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
    uint64_t prev = 0;
    for (uint32_t i = 0; i < count; i++) {
        if (size - pos < 12) throw std::runtime_error("Malformed geo entry delta");
        uint64_t cid;
        uint16_t nl, ng;
        memcpy(&cid, data + pos, 8); memcpy(&nl, data + pos + 8, 2); memcpy(&ng, data + pos + 10, 2);
        if (nl == GEO_LIST_REPLACE) nl = 0;
        if (i > 0 && cid <= prev) throw std::runtime_error("Malformed geo entry delta");
        prev = cid;
        pos += 12;
        const size_t ids = ((size_t)nl + ng) * 4;
        if (size - pos < ids) throw std::runtime_error("Malformed geo entry delta");
        pos += ids;
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
    void apply(std::vector<uint32_t>& ids, std::vector<uint32_t>& scratch) {
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
// sequence plus the diff's secondary matches. Strategy-2 slots keep these
// to long runs of one shift, so this holds a few MB where the patcher used
// to write a 4-byte-per-record scratch file (~1 GB for planet/full) and read
// it at random from the entry pipeline: page-cache thrash on a 1 GiB client.
class RecordRemap {
public:
    static constexpr uint32_t NONE = 0xFFFFFFFFu;

    explicit RecordRemap(uint32_t n_old = 0) : n_old_(n_old) {}

    // `count` old records from old_start were kept as new_start onwards.
    void add_match(uint32_t old_start, uint32_t new_start, uint32_t count) {
        if (old_start >= n_old_) return;
        count = std::min(count, n_old_ - old_start);
        if (count == 0) return;
        if (!runs_.empty()) {
            Run& last = runs_.back();
            if (last.old_start + last.count == old_start && last.new_start + last.count == new_start) {
                last.count += count;
                return;
            }
        }
        runs_.push_back({old_start, new_start, count});
    }
    // A secondary match: old_id now maps to new_id; the last one for an id wins.
    void add_pair(uint32_t old_id, uint32_t new_id) {
        if (old_id < n_old_) pairs_.push_back({old_id, new_id});
    }
    void finish() {
        std::stable_sort(pairs_.begin(), pairs_.end(),
                         [](const auto& a, const auto& b) { return a.first < b.first; });
        std::vector<std::pair<uint32_t, uint32_t>> last;
        for (size_t i = 0; i < pairs_.size(); i++)
            if (i + 1 == pairs_.size() || pairs_[i + 1].first != pairs_[i].first) last.push_back(pairs_[i]);
        pairs_.swap(last);
    }

    size_t size() const { return n_old_; }
    bool empty() const { return runs_.empty() && pairs_.empty(); }
    // operator[] with a run hint: consecutive lookups that land in the same
    // run (spatially sorted references) skip the binary search.
    uint32_t lookup(uint32_t o, size_t& hint) const {
        if (o >= n_old_) return NONE;
        if (!pairs_.empty()) {
            auto p = std::lower_bound(pairs_.begin(), pairs_.end(), std::make_pair(o, 0u),
                                      [](const auto& a, const auto& b) { return a.first < b.first; });
            if (p != pairs_.end() && p->first == o) return p->second;
        }
        if (hint < runs_.size() && o >= runs_[hint].old_start && o < runs_[hint].old_start + runs_[hint].count)
            return runs_[hint].new_start + (o - runs_[hint].old_start);
        auto it = std::upper_bound(runs_.begin(), runs_.end(), o,
                                   [](uint32_t v, const Run& r) { return v < r.old_start; });
        if (it == runs_.begin()) return NONE;
        --it;
        if (o >= it->old_start + it->count) return NONE;
        hint = static_cast<size_t>(it - runs_.begin());
        return it->new_start + (o - it->old_start);
    }
    // The new id of old record `old`, or NONE when it wasn't kept.
    uint32_t operator[](size_t old) const {
        if (old >= n_old_) return NONE;
        uint32_t o = static_cast<uint32_t>(old);
        if (!pairs_.empty()) {
            auto p = std::lower_bound(pairs_.begin(), pairs_.end(), std::make_pair(o, 0u),
                                      [](const auto& a, const auto& b) { return a.first < b.first; });
            if (p != pairs_.end() && p->first == o) return p->second;
        }
        auto it = std::upper_bound(runs_.begin(), runs_.end(), o,
                                   [](uint32_t v, const Run& r) { return v < r.old_start; });
        if (it == runs_.begin()) return NONE;
        --it;
        return o < it->old_start + it->count ? it->new_start + (o - it->old_start) : NONE;
    }

private:
    struct Run { uint32_t old_start, new_start, count; };
    uint32_t n_old_;
    std::vector<Run> runs_;
    std::vector<std::pair<uint32_t, uint32_t>> pairs_;
};

// --- Varint encoding for delta-compressed fixup tables and string runs ---

inline void write_varint(std::vector<char>& buf, uint32_t value) {
    while (value >= 128) {
        buf.push_back(static_cast<char>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buf.push_back(static_cast<char>(value));
}

// Reads a varint that stays inside data[0, size): a truncated or overlong
// value, or one of 2^32 or more, throws "Malformed <what>".
inline uint32_t read_varint_bounded(const char* data, size_t& pos, size_t size, const char* what) {
    uint32_t result = 0;
    for (uint32_t bit = 0; bit < 35; bit += 7) {
        if (pos >= size) throw std::runtime_error(std::string("Malformed ") + what);
        uint8_t byte = static_cast<uint8_t>(data[pos++]);
        if (bit == 28 && (byte & 0x70)) throw std::runtime_error(std::string("Malformed ") + what);
        result |= static_cast<uint32_t>(byte & 0x7F) << bit;
        if (!(byte & 0x80)) return result;
    }
    throw std::runtime_error(std::string("Malformed ") + what);
}

inline uint32_t zigzag32(uint32_t v) { return (v << 1) ^ (0u - (v >> 31)); }
inline uint32_t unzigzag32(uint32_t v) { return (v >> 1) ^ (0u - (v & 1)); }

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
    auto malformed = [] { throw std::runtime_error("Malformed strings section"); };
    auto take = [&](uint64_t n) {
        if (pos > size || n > size - pos) throw std::runtime_error("Truncated strings section");
        const char* p = data + pos;
        pos += static_cast<size_t>(n);
        return p;
    };
    auto u32 = [&] { uint32_t v; memcpy(&v, take(4), 4); return v; };
    auto u64 = [&] { uint64_t v; memcpy(&v, take(8), 8); return v; };
    StringTierEdit e;
    e.old_hash = u64();
    e.new_hash = u64();
    e.n_added = u32();
    e.n_deleted = u32();
    const uint32_t order = u32();
    if (order != STRING_TIER_SORTED && order != STRING_TIER_INDEXED) malformed();
    e.sorted = order == STRING_TIER_SORTED;
    e.added = data + pos;
    const char* prev = nullptr;
    for (uint32_t i = 0; i < e.n_added; i++) {
        const void* end = memchr(data + pos, '\0', size - pos);
        if (!end) throw std::runtime_error("Truncated strings section");
        if (e.sorted && prev && strcmp(prev, data + pos) >= 0) malformed();
        prev = data + pos;
        pos = static_cast<size_t>(static_cast<const char*>(end) - data) + 1;
    }
    auto ascending = [&](uint32_t n) {
        const char* list = take(uint64_t{n} * 4);
        for (uint32_t i = 1; i < n; i++)
            if (string_index_at(list, i) <= string_index_at(list, i - 1)) malformed();
        return list;
    };
    if (!e.sorted) e.added_at = ascending(e.n_added);
    e.deleted = ascending(e.n_deleted);
    return e;
}

// No old string: an added one (for_each_new_string).
constexpr size_t NO_OLD_STRING = SIZE_MAX;

// A shipped tier's new pool string by string, from its old pool and edit:
// on_string(p, len, old_off) for each new string in order, with the offset of
// a kept string in the old pool, or NO_OLD_STRING for an added one. The
// edit's indices ascend (parse_string_tier_edit); ones the old pool can't
// place throw. A sorted edit puts an added string before the next kept one
// it sorts before.
template <typename OnString>
void for_each_new_string(const char* old_pool, size_t old_size, const StringTierEdit& edit, OnString on_string) {
    size_t o = 0;
    uint32_t oi = 0, ni = 0, d = 0, a = 0;
    const char* added = edit.added;  // the next added string
    auto old_len = [&] { return strnlen(old_pool + o, old_size - o) + 1; };
    auto skip_deleted = [&] {
        for (; o < old_size && d < edit.n_deleted && string_index_at(edit.deleted, d) == oi; d++, oi++) o += old_len();
    };
    skip_deleted();
    auto added_next = [&] {
        if (a >= edit.n_added) return false;
        if (!edit.sorted) return string_index_at(edit.added_at, a) == ni;
        return o >= old_size || strcmp(added, old_pool + o) < 0;
    };
    for (;; ni++) {
        if (added_next()) {
            const size_t len = strlen(added) + 1;
            on_string(added, len, NO_OLD_STRING);
            added += len;
            a++;
        } else if (o < old_size) {
            const size_t len = old_len();
            on_string(old_pool + o, len, o);
            o += len;
            oi++;
            skip_deleted();
        } else {
            break;
        }
    }
    if (a != edit.n_added || d != edit.n_deleted) throw std::runtime_error("Malformed strings section");
}

// The patcher's old → new string offset map. Between two edits every
// surviving string of a tier moves by one amount, so the map is held as runs
// over old offsets: a few thousand on a planet day instead of one pair per
// moved string (20.6M pairs, 157 MiB on every client). Like the diff's
// per-string pairs, only a string's start moves. A run derived from a tier
// the client holds checks that against the old pool, where the byte before a
// string start is its predecessor's NUL. A run sent for a tier the client
// doesn't hold can't: it trusts that every stored reference is a string start
// or NO_DATA, which partition_strings_into_tiers guarantees and CI's byte
// identity check proves daily. Surviving strings the edits don't keep (they
// changed tier, or the POI tier's order moved them) are explicit pairs, read
// where the patch holds them.
//
// Pairs are looked up first, but the order doesn't matter: a pair's old
// offset starts a string its old tier's edit doesn't keep, runs cover only
// kept strings, and a tier's runs lie inside its own old range, so no run
// covers a pair's offset.
class StringRemap {
public:
    // Records the kept strings that moved, walking a shipped tier's old pool
    // and edit as its rebuild does. The old pool must stay mapped for lookups.
    void add_tier(const char* old_pool, size_t old_size, uint32_t old_base, uint32_t new_base,
                  const StringTierEdit& edit) {
        uint32_t new_start = new_base;
        StringRunJoiner joiner([&](uint32_t start, uint32_t end, uint32_t shift) {
            runs_.push_back({start, end, shift, old_pool + (start - old_base)});
        });
        for_each_new_string(old_pool, old_size, edit, [&](const char*, size_t len, size_t old_off) {
            if (old_off != NO_OLD_STRING) {
                const uint32_t start = old_base + static_cast<uint32_t>(old_off);
                joiner.kept(start, start + static_cast<uint32_t>(len), new_start);
            }
            new_start += static_cast<uint32_t>(len);
        });
        joiner.finish();
    }
    // Decodes the runs encode_string_tier_runs wrote for the tier at old
    // offsets [tier_start, tier_end). Tiers are added in offset order, so the
    // runs stay sorted.
    void add_sent_tier(const char* data, size_t size, uint32_t n_runs, uint32_t tier_start, uint32_t tier_end) {
        auto malformed = [] { throw std::runtime_error("Malformed string runs"); };
        if (!runs_.empty() && runs_.back().end > tier_start) malformed();
        if (n_runs > size / 3) malformed();  // a run is at least 3 bytes
        size_t pos = 0;
        uint64_t prev_end = tier_start;
        uint32_t shift = 0;
        for (uint32_t i = 0; i < n_runs; i++) {
            uint64_t start = prev_end + read_varint_bounded(data, pos, size, "string runs");
            uint64_t end = start + read_varint_bounded(data, pos, size, "string runs");
            shift += unzigzag32(read_varint_bounded(data, pos, size, "string runs"));
            if (end == start || end > tier_end) malformed();
            runs_.push_back({static_cast<uint32_t>(start), static_cast<uint32_t>(end), shift, nullptr});
            prev_end = end;
        }
        if (pos != size) malformed();
    }
    // The moved strings' pairs (the strings section's, where it sits).
    void set_pairs(SortedTable<RemapPair> pairs) { pairs_ = std::move(pairs); }
    bool empty() const { return runs_.empty() && pairs_.empty(); }
    size_t run_count() const { return runs_.size(); }

    uint32_t lookup(uint32_t off) const {
        if (const std::optional<RemapPair> p = pairs_.find(off)) return p->to;
        auto it = std::upper_bound(runs_.begin(), runs_.end(), off,
                                   [](uint32_t v, const Run& r) { return v < r.start; });
        if (it == runs_.begin()) return off;
        --it;
        if (off >= it->end) return off;
        if (it->old_bytes && off != it->start && it->old_bytes[off - it->start - 1] != '\0') return off;
        return off + it->shift;
    }

private:
    // old_bytes: the run's bytes in the old pool, or nullptr for a sent run.
    struct Run { uint32_t start, end, shift; const char* old_bytes; };
    std::vector<Run> runs_;
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

// What append_strings_section wrote, for the diff's log.
struct StringsSectionStats {
    uint32_t sent = 0, n_pairs = 0;
    std::array<uint32_t, STRING_TIER_COUNT> n_added{}, n_deleted{}, n_runs{}, runs_bytes{};
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
// changed tier or order, are sent as pairs.
template <typename Moved>
StringsSectionStats append_strings_section(std::vector<char>& out,
                                           const std::array<StringTierPools, STRING_TIER_COUNT>& tiers,
                                           uint32_t shipped, uint32_t referenced, const Moved& moved) {
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

struct StringsSection {
    // Tier t sits at old offsets [old_base[t], old_base[t + 1]); new likewise.
    std::array<uint32_t, STRING_TIER_COUNT + 1> old_base{}, new_base{};
    uint32_t shipped = 0, sent = 0;
    std::array<StringTierEdit, STRING_TIER_COUNT> edits;  // shipped tiers
    std::array<SentStringRuns, STRING_TIER_COUNT> runs;   // sent tiers
    SortedTable<RemapPair> pairs;                         // moved strings, by old offset
    uint32_t old_size(int t) const { return old_base[t + 1] - old_base[t]; }
    uint32_t new_size(int t) const { return new_base[t + 1] - new_base[t]; }
};

// Parses the section at data[pos, size) and advances pos past it.
inline StringsSection parse_strings_section(const char* data, size_t size, size_t& pos) {
    auto malformed = [] { throw std::runtime_error("Malformed strings section"); };
    auto take = [&](uint64_t n) {
        if (pos > size || n > size - pos) throw std::runtime_error("Truncated strings section");
        const char* p = data + pos;
        pos += static_cast<size_t>(n);
        return p;
    };
    auto u32 = [&] { uint32_t v; memcpy(&v, take(4), 4); return v; };
    if (u32() != STRINGS_TIERED_MARKER) throw std::runtime_error("Patch has no strings section");
    StringsSection s;
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        uint32_t old_size = u32(), new_size = u32();
        if ((uint64_t)s.old_base[t] + old_size > UINT32_MAX || (uint64_t)s.new_base[t] + new_size > UINT32_MAX)
            malformed();
        s.old_base[t + 1] = s.old_base[t] + old_size;
        s.new_base[t + 1] = s.new_base[t] + new_size;
    }
    s.shipped = u32();
    s.sent = u32();
    if ((s.shipped | s.sent) >> STRING_TIER_COUNT || (s.shipped & s.sent)) malformed();
    for (int t = 0; t < STRING_TIER_COUNT; t++) {
        if (s.sent >> t & 1) {
            SentStringRuns& r = s.runs[t];
            r.n_runs = u32();
            r.size = u32();
            r.data = take(r.size);
            continue;
        }
        if (s.shipped >> t & 1) s.edits[t] = parse_string_tier_edit(data, size, pos);
    }
    const uint32_t n_pairs = u32();
    s.pairs = SortedTable<RemapPair>(take(uint64_t{n_pairs} * sizeof(RemapPair)), n_pairs, "strings section");
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

// Writes a shipped tier's new pool through write(bytes, n): the old pool's
// kept strings with the added ones at their indices.
template <typename Write>
void rebuild_string_tier(const char* old_pool, size_t old_size, const StringTierEdit& edit, Write write) {
    for_each_new_string(old_pool, old_size, edit, [&](const char* p, size_t n, size_t) { write(p, n); });
}

// Fills remap from the section, tier by tier in offset order: a sent tier
// from its runs, a shipped tier through on_shipped(t), which rebuilds it and
// calls remap.add_tier with its old pool and edit. Then the moved strings.
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
    remap.set_pairs(s.pairs);
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
        return read_varint_bounded(data, pos, size, "fixups");
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
// Creates the directory and its missing parents; the path is never shell text.
inline void ensure_dir(const std::string& path) {
    std::error_code ec;
    std::filesystem::create_directories(path, ec);
    if (ec) throw std::runtime_error("Cannot create directory " + path + ": " + ec.message());
}
