// The builder's writers of the record files as they sit on disk. Each
// encodes its file in slices of whole units on every core and writes the
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
#include <utility>
#include <vector>

#include "parallel.h"
#include "side_blocks.h"

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
