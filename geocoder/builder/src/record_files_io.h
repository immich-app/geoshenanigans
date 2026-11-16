// The builder's writers of the record files as they sit on disk. Each
// encodes its files in slices of whole units on every core and writes the
// slices in order, so the bytes are its serial encoder's whatever the thread
// count and wherever the slices are cut. No S2 here, so the unit tests can
// check the bytes.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "addr_files.h"
#include "parallel.h"
#include "side_blocks.h"
#include "types.h"
#include "vertex_pack.h"
#include "way_files.h"

// The most slices a writer's threads encode ahead of the one being written:
// two a thread, and kSliceWindow however many threads there are. write_index
// starts every file's writer at once, three mode dirs side by side, and the
// slices they hold are memory the write schedule budgets by the mode dir, not
// by the core.
inline constexpr size_t kSliceWindow = 32;

inline size_t slice_window(unsigned threads) { return std::min(kSliceWindow, size_t(2) * threads); }

// Runs encode(k) for every slice k of [0, slices) on up to `threads` threads
// (0 = every core) and write(k, slice) with each result on the calling
// thread, in order, the encoding no more than the slice window ahead of it.
template <class Encode, class Write>
void encode_slices(size_t slices, Encode&& encode, Write&& write, unsigned threads = 0) {
    if (threads == 0) threads = parallel_threads();
    parallel_ordered(slices, std::forward<Encode>(encode), std::forward<Write>(write), threads, slice_window(threads));
}

// The blocks of a side file's slice: 65,536 values.
inline constexpr size_t kSideSliceBlocks = 1024;

// Writes the side file `file` (side_blocks.h) of values[0, values.size())
// through put(const uint8_t*, size_t): each slice's blocks in one call, then
// the block table, then the footer. `file` names it where the format can't
// hold it.
template <class Values, class Put>
void write_side_blocks(std::string_view file, const Values& values, Put put, unsigned threads = 0,
                       size_t slice_blocks = kSideSliceBlocks) {
    struct Slice {
        std::vector<uint8_t> blocks;
        // Where each block starts, from the slice's first.
        std::vector<uint64_t> starts;
    };
    const size_t n = values.size(), slice = std::max<size_t>(slice_blocks, 1) * side_blocks::BLOCK_VALUES;
    const auto footer = side_blocks::footer(file, n);
    std::vector<uint8_t> table;
    table.reserve((n + side_blocks::BLOCK_VALUES - 1) / side_blocks::BLOCK_VALUES * side_blocks::TABLE_ENTRY_BYTES);
    uint64_t written = 0;
    encode_slices((n + slice - 1) / slice, [&](size_t k) {
        Slice out;
        side_blocks::Encoder enc([&out](const uint8_t* p, size_t len) { out.blocks.insert(out.blocks.end(), p, p + len); });
        for (size_t i = k * slice, end = std::min(n, i + slice); i < end; i++) enc.add(values[i]);
        enc.finish();
        out.starts = enc.table();
        return out;
    }, [&](size_t, Slice&& s) {
        for (uint64_t start : s.starts) {
            uint8_t entry[side_blocks::TABLE_ENTRY_BYTES];
            side_blocks::write_table_entry(file, written + start, entry);
            table.insert(table.end(), entry, entry + side_blocks::TABLE_ENTRY_BYTES);
        }
        put(s.blocks.data(), s.blocks.size());
        written += s.blocks.size();
    }, threads);
    if (!table.empty()) put(table.data(), table.size());
    put(footer.data(), footer.size());
}

// Writes the side file `name` of `dir`: values[i] is that of record i of
// `parent`, which holds `records`. Values that are not one a record are
// refused before the file exists: the server reads a record's value by the
// record's id and loads no side file of another count.
inline void write_side_file(const std::string& dir, const std::string& name, const std::vector<uint32_t>& values,
                            std::string_view parent, uint64_t records, unsigned threads = 0) {
    const std::string path = dir + "/" + name;
    if (values.size() != records)
        throw std::runtime_error(path + ": " + std::to_string(values.size()) + " values for the " +
                                 std::to_string(records) + " records of " + std::string(parent));
    std::ofstream f(path, std::ios::binary);
    write_side_blocks(name, values, [&f](const uint8_t* p, size_t n) {
        f.write(reinterpret_cast<const char*>(p), static_cast<std::streamsize>(n));
    }, threads);
    f.flush();
    if (!f) throw std::runtime_error("failed to write " + path);
}

// The groups of a way slice: 65,536 ways.
inline constexpr size_t kWaySliceGroups = 4096;

// The kind of way (way_files.h) a logical record is of.
inline const way_files::Kind& way_kind(const WayHeader&) { return way_files::STREETS; }
inline const way_files::Kind& way_kind(const InterpWay&) { return way_files::INTERPS; }

// Writes the way files (way_files.h) of a logical pair: ways[i] is way i's
// record, a WayHeader or an InterpWay, and its nodes follow those of the ways
// before it in nodes, which gives each by operator[] as a reference into one
// array. Each slice's rows go through put_rows(const uint8_t*, size_t) in one
// call and its blocks through put_blocks, then each file's footer. Refuses
// what the serial encoder refuses of the pair, in its words, and nodes that
// are not the ways'.
template <class Ways, class Nodes, class PutRows, class PutBlocks>
void write_way_pair(const Ways& ways, const Nodes& nodes, PutRows put_rows, PutBlocks put_blocks, unsigned threads = 0,
                    size_t slice_groups = kWaySliceGroups) {
    struct Slice {
        // The rows' offs count from the slice's first block.
        std::vector<uint8_t> rows, blocks;
    };
    const way_files::Kind& kind = way_kind(std::decay_t<decltype(ways[0])>{});
    const size_t n = ways.size(), slice = std::max<size_t>(slice_groups, 1) * way_files::GROUP_WAYS;
    const size_t slices = (n + slice - 1) / slice;
    // The nodes before each slice, which its encoder starts from, and after
    // the last.
    std::vector<uint64_t> before(slices + 1, 0);
    for (size_t k = 0; k < slices; k++) {
        before[k + 1] = before[k];
        for (size_t i = k * slice, end = std::min(n, i + slice); i < end; i++) before[k + 1] += ways[i].node_count;
    }
    if (before[slices] != nodes.size())
        way_files::malformed(std::string(kind.nodes_file) + " has " + std::to_string(nodes.size()) + " nodes and the ways of " +
                             kind.ways_file + " have " + std::to_string(before[slices]));
    uint64_t blocks_written = 0;
    encode_slices(slices, [&](size_t k) {
        Slice out;
        const size_t first = k * slice, end = std::min(n, first + slice);
        out.rows.reserve((end - first + way_files::GROUP_WAYS - 1) / way_files::GROUP_WAYS * way_files::ROW_HEAD_BYTES +
                         (end - first) * kind.row_way_bytes);
        auto into = [](std::vector<uint8_t>& v) {
            return [&v](const uint8_t* p, size_t len) { v.insert(v.end(), p, p + len); };
        };
        way_files::Encoder enc(kind, into(out.rows), into(out.blocks), first, before[k]);
        uint64_t node = before[k];
        for (size_t i = first; i < end; i++) {
            const auto& way = ways[i];
            enc.add(reinterpret_cast<const uint8_t*>(&way),
                    way.node_count ? reinterpret_cast<const uint8_t*>(&nodes[node]) : nullptr);
            node += way.node_count;
        }
        enc.finish();
        // A slice waits for its turn at the size of its bytes, not at what
        // the blocks grew through.
        out.blocks.shrink_to_fit();
        return out;
    }, [&](size_t, Slice&& s) {
        way_files::rebase_rows(kind, s.rows.data(), s.rows.size(), blocks_written);
        put_rows(s.rows.data(), s.rows.size());
        put_blocks(s.blocks.data(), s.blocks.size());
        blocks_written += s.blocks.size();
    }, threads);
    const auto ways_footer = way_files::ways_footer(kind, n);
    const auto nodes_footer = way_files::nodes_footer(kind, before[slices], n, blocks_written);
    put_rows(ways_footer.data(), ways_footer.size());
    put_blocks(nodes_footer.data(), nodes_footer.size());
}

// Writes the two way files of `dir` for the logical pair: the street files
// of WayHeaders, the interpolation files of InterpWays, the two footers of no
// ways. A pair the writer refuses is refused under the dir's name.
template <class Record>
void write_way_files(const std::string& dir, const std::vector<Record>& ways, const std::vector<NodeCoord>& nodes,
                     unsigned threads = 0) {
    const way_files::Kind& kind = way_kind(Record{});
    const std::string rows_path = dir + "/" + kind.ways_file, blocks_path = dir + "/" + kind.nodes_file;
    std::ofstream rows(rows_path, std::ios::binary), blocks(blocks_path, std::ios::binary);
    if (!rows) throw std::runtime_error("failed to write " + rows_path);
    if (!blocks) throw std::runtime_error("failed to write " + blocks_path);
    auto to = [](std::ofstream& f) {
        return [&f](const uint8_t* p, size_t n) { f.write(reinterpret_cast<const char*>(p), static_cast<std::streamsize>(n)); };
    };
    try {
        write_way_pair(ways, nodes, to(rows), to(blocks), threads);
    } catch (const std::runtime_error& e) {
        throw std::runtime_error(dir + ": " + e.what());
    }
    rows.flush();
    if (!rows) throw std::runtime_error("failed to write " + rows_path);
    blocks.flush();
    if (!blocks) throw std::runtime_error("failed to write " + blocks_path);
}

// The superblocks of an address slice: 65,536 records.
inline constexpr size_t kAddrSliceSuperblocks = 64;

// Writes the address files (addr_files.h) of the builder's addresses:
// points[i] is address i's AddrPoint, a building's vertex_offset the place of
// its footprint's first vertex in vertices and its vertex_count how many
// follow it there; vertices gives each by operator[] as a reference into one
// array. A footprint is written as its packed polygon (vertex_pack.h), the
// logical blob, its record's vertex_offset the bytes of the ones before it in
// the logical record's units (addr_files::vertex_offset_of); a record whose
// footprint the vertices do not hold is written as a point. Each
// slice's blocks go through put_points(const uint8_t*, size_t) in one call,
// its blobs through put_vertices and its index rows through put_index, then
// each file's footer. Refuses, before anything is put, more records than a
// patch indexes and footprints whose logical blobs are more bytes than one
// indexes; and what the serial encoder refuses of the pair, in its words.
// Returns the bytes of the logical addr_vertices.bin.
template <class Points, class Vertices, class PutPoints, class PutVertices, class PutIndex>
uint64_t write_addr_pair(const Points& points, const Vertices& vertices, PutPoints put_points, PutVertices put_vertices,
                         PutIndex put_index, unsigned threads = 0, size_t slice_superblocks = kAddrSliceSuperblocks) {
    struct Slice {
        // The index rows' offsets count from the slice's first block and
        // first blob.
        std::vector<uint8_t> blocks, blobs, index;
        uint64_t footprints = 0;
    };
    const size_t n = points.size(), slice = std::max<size_t>(slice_superblocks, 1) * addr_files::SUPERBLOCK_RECORDS;
    const size_t slices = (n + slice - 1) / slice;
    addr_files::check_records(n);
    // A record's footprint in the vertices: none of a point's, and none
    // where they do not hold it.
    auto footprint = [&vertices](const AddrPoint& p) -> const NodeCoord* {
        if (p.vertex_count == 0 || p.vertex_offset == NO_DATA || size_t(p.vertex_offset) + p.vertex_count > vertices.size())
            return nullptr;
        return &vertices[p.vertex_offset];
    };
    // The bytes of the logical blobs before each slice, which its encoder
    // starts from, and after the last.
    std::vector<uint64_t> before(slices + 1, 0);
    parallel_for(slices, [&](size_t begin, size_t end, unsigned) {
        for (size_t k = begin; k < end; k++) {
            uint64_t bytes = 0;
            for (size_t i = k * slice, last = std::min(n, i + slice); i < last; i++) {
                const AddrPoint p = points[i];
                if (const NodeCoord* ring = footprint(p)) bytes += plan_polygon(ring, p.vertex_count).bytes;
            }
            before[k + 1] = bytes;
        }
    }, threads);
    for (size_t k = 0; k < slices; k++) before[k + 1] += before[k];
    addr_files::check_logical_vertex_bytes(before[slices]);

    uint64_t blocks_written = 0, blobs_written = 0, footprints = 0;
    encode_slices(slices, [&](size_t k) {
        Slice out;
        const size_t first = k * slice, end = std::min(n, first + slice);
        out.index.reserve((end - first + addr_files::SUPERBLOCK_RECORDS - 1) / addr_files::SUPERBLOCK_RECORDS *
                              addr_files::INDEX_HEAD_BYTES +
                          (end - first + addr_files::BLOCK_RECORDS - 1) / addr_files::BLOCK_RECORDS * addr_files::INDEX_ENTRY_BYTES);
        auto into = [](std::vector<uint8_t>& v) {
            return [&v](const uint8_t* p, size_t len) { v.insert(v.end(), p, p + len); };
        };
        addr_files::Encoder enc(into(out.blocks), into(out.blobs), into(out.index), first, before[k]);
        std::vector<uint8_t> logical;
        uint64_t logical_at = before[k];
        for (size_t i = first; i < end; i++) {
            AddrPoint p = points[i];
            if (const NodeCoord* ring = footprint(p)) {
                const PolygonPacking packing = plan_polygon(ring, p.vertex_count);
                logical.resize(packing.bytes);
                pack_polygon(logical.data(), packing, ring, p.vertex_count);
                p.vertex_offset = addr_files::vertex_offset_of(logical_at);
                logical_at += packing.bytes;
            } else {
                p.vertex_offset = NO_DATA;
                p.vertex_count = 0;
            }
            enc.add(reinterpret_cast<const uint8_t*>(&p), logical.data());
        }
        enc.finish();
        out.footprints = enc.blobs();
        // A slice waits for its turn at the size of its bytes, not at what
        // they grew through.
        out.blocks.shrink_to_fit();
        out.blobs.shrink_to_fit();
        return out;
    }, [&](size_t, Slice&& s) {
        addr_files::rebase_index(s.index.data(), s.index.size(), blocks_written, blobs_written);
        put_points(s.blocks.data(), s.blocks.size());
        put_vertices(s.blobs.data(), s.blobs.size());
        put_index(s.index.data(), s.index.size());
        blocks_written += s.blocks.size();
        blobs_written += s.blobs.size();
        footprints += s.footprints;
    }, threads);
    const auto points_footer = addr_files::points_footer(n);
    const auto vertices_footer = addr_files::vertices_footer(before[slices], footprints);
    const auto index_footer = addr_files::index_footer(n);
    put_points(points_footer.data(), points_footer.size());
    put_vertices(vertices_footer.data(), vertices_footer.size());
    put_index(index_footer.data(), index_footer.size());
    return before[slices];
}

// Writes the three address files of `dir` for the builder's addresses, the
// three footers of no addresses. Addresses the writer refuses are refused
// under the dir's name. Returns the bytes of the logical addr_vertices.bin.
inline uint64_t write_addr_files(const std::string& dir, const std::vector<AddrPoint>& points,
                                 const std::vector<NodeCoord>& vertices, unsigned threads = 0) {
    const char* const names[3] = {addr_files::POINTS_FILE, addr_files::VERTICES_FILE, addr_files::INDEX_FILE};
    std::string paths[3];
    std::ofstream files[3];
    for (size_t f = 0; f < 3; f++) {
        paths[f] = dir + "/" + names[f];
        files[f].open(paths[f], std::ios::binary);
        if (!files[f]) throw std::runtime_error("failed to write " + paths[f]);
    }
    auto to = [&files](size_t f) {
        return [&file = files[f]](const uint8_t* p, size_t n) {
            file.write(reinterpret_cast<const char*>(p), static_cast<std::streamsize>(n));
        };
    };
    uint64_t logical_vertex_bytes = 0;
    try {
        logical_vertex_bytes = write_addr_pair(points, vertices, to(0), to(1), to(2), threads);
    } catch (const std::runtime_error& e) {
        throw std::runtime_error(dir + ": " + e.what());
    }
    for (size_t f = 0; f < 3; f++) {
        files[f].flush();
        if (!files[f]) throw std::runtime_error("failed to write " + paths[f]);
    }
    return logical_vertex_bytes;
}
