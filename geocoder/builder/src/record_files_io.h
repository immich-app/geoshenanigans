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

#include "parallel.h"
#include "side_blocks.h"
#include "types.h"
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
