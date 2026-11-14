// The record files' writers (record_files_io.h): a side file is the bytes
// its serial encoder gives, however many threads encode it and wherever its
// slices are cut; the threads encode no more than the slice window ahead of
// the write; and a side array that is not a value a record is refused.
#include "record_files_io.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "scratch_dir.h"
#include "side_blocks.h"
#include "test_framework.h"

using gctest::refusal_of;

namespace {

// NO_DATA, which a side file holds as it holds any value.
constexpr uint32_t NONE = 0xFFFFFFFFu;
const char* const NAME = "way_postcodes.bin";
const char* const PARENT = "street_ways.bin";

// n values in stretches of three kinds, so their blocks are of both modes
// and many widths: a postcode column (runs of one id, NO_DATA among them),
// parents (ids near each other) and any 32 bits.
std::vector<uint32_t> side_values(std::mt19937_64& rng, size_t n) {
    std::vector<uint32_t> out;
    uint32_t run = uint32_t(rng() % 100000);
    int kind = 0;
    // One draw a statement: the same values whatever order a compiler evaluates arguments in.
    for (size_t i = 0; i < n; i++) {
        const uint64_t r = rng();
        if (i % 500 == 0) kind = int((r >> 40) % 3);
        if (kind == 0) {
            if (r % 9 == 0) run = uint32_t((r >> 20) % 100000);
            out.push_back((r >> 8) % 6 == 0 ? NONE : run);
        } else if (kind == 1) {
            run += uint32_t(r % 5);
            out.push_back((r >> 8) % 40 == 0 ? NONE : run - uint32_t((r >> 16) % 3));
        } else {
            out.push_back(uint32_t(r));
        }
    }
    return out;
}

std::vector<uint8_t> serial(const std::vector<uint32_t>& values) {
    return side_blocks::encode(NAME, values.data(), values.size());
}

// What the side writer puts for the values.
std::vector<uint8_t> written(const std::vector<uint32_t>& values, unsigned threads, size_t slice_blocks) {
    std::vector<uint8_t> out;
    write_side_blocks(NAME, values, [&](const uint8_t* p, size_t n) { out.insert(out.end(), p, p + n); }, threads,
                      slice_blocks);
    return out;
}

std::vector<uint8_t> file_bytes(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
}

// A side array that notes, as each value is read, how many slices ahead of
// those written its slice is.
struct Watched {
    const std::vector<uint32_t>& values;
    size_t slice_values;
    const std::atomic<size_t>& written;
    std::atomic<size_t>& furthest;

    size_t size() const { return values.size(); }
    uint32_t operator[](size_t i) const {
        const size_t slice = i / slice_values, done = written.load();
        const size_t ahead = slice > done ? slice - done : 0;
        size_t seen = furthest.load();
        while (ahead > seen && !furthest.compare_exchange_weak(seen, ahead)) {}
        return values[i];
    }
};

}  // namespace

TEST(the_side_writer_puts_the_serial_encoders_bytes_at_any_thread_count_and_slice_size) {
    std::mt19937_64 rng(23);
    // Around a block's edge, and past a slice of the builder's size.
    for (size_t n : {size_t(0), size_t(1), size_t(63), size_t(64), size_t(65), size_t(4000), size_t(70001)}) {
        const std::vector<uint32_t> values = side_values(rng, n);
        const std::vector<uint8_t> want = serial(values);
        for (unsigned threads : {1u, 2u, 7u})
            for (size_t slice_blocks : {size_t(1), size_t(3), kSideSliceBlocks})
                CHECK(written(values, threads, slice_blocks) == want);
    }
    CHECK(written({}, 2, 1) == (std::vector<uint8_t>{0, 0, 0, 0, 'S', 'D', 'B', '1'}));
}

TEST(a_side_file_is_the_serial_encoders_bytes) {
    const ScratchDir dir("side-file");
    std::mt19937_64 rng(29);
    const std::vector<uint32_t> values = side_values(rng, 150000);
    write_side_file(dir.path(), NAME, values, PARENT, values.size());
    CHECK(file_bytes(dir.path() + "/" + NAME) == serial(values));
}

TEST(a_side_array_that_is_not_a_value_a_record_is_refused_before_its_file_exists) {
    const ScratchDir dir("side-count");
    const std::string path = dir.path() + "/" + NAME;
    const std::vector<uint32_t> values(100, 7);
    for (uint64_t records : {uint64_t(99), uint64_t(101), uint64_t(0)}) {
        CHECK_EQ(refusal_of([&] { write_side_file(dir.path(), NAME, values, PARENT, records); }),
                 path + ": 100 values for the " + std::to_string(records) + " records of street_ways.bin");
        CHECK(!std::filesystem::exists(path));
    }
    CHECK_EQ(refusal_of([&] { write_side_file(dir.path(), NAME, {}, PARENT, 3); }),
             path + ": 0 values for the 3 records of street_ways.bin");
    CHECK(!std::filesystem::exists(path));

    // A value a record is written, and a dir that is not there is an error.
    write_side_file(dir.path(), NAME, values, PARENT, 100);
    CHECK(file_bytes(path) == serial(values));
    CHECK_EQ(refusal_of([&] { write_side_file(dir.path() + "/none", NAME, values, PARENT, 100); }),
             "failed to write " + dir.path() + "/none/" + NAME);
}

TEST(a_slice_writer_runs_no_more_than_its_window_ahead_of_the_consumer) {
    // Two slices a thread, and no more than kSliceWindow however many threads.
    CHECK_EQ(slice_window(1), size_t(2));
    CHECK_EQ(slice_window(15), size_t(30));
    CHECK_EQ(slice_window(16), kSliceWindow);
    CHECK_EQ(slice_window(64), kSliceWindow);
    CHECK_EQ(kSliceWindow, size_t(32));

    // 600 slices of a block each, one written every 100 microseconds: the
    // threads encode as far ahead of the write as their window lets them.
    const size_t slices = 600;
    const std::vector<uint32_t> values(slices * side_blocks::BLOCK_VALUES, 7);
    for (unsigned threads : {3u, 64u}) {
        std::atomic<size_t> slices_written{0}, furthest{0};
        const Watched watched{values, side_blocks::BLOCK_VALUES, slices_written, furthest};
        write_side_blocks(NAME, watched, [&](const uint8_t*, size_t) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            slices_written++;
        }, threads, 1);
        // Each slice's blocks, then the table and the footer.
        CHECK_EQ(slices_written.load(), slices + 2);
        CHECK(furthest.load() < slice_window(threads));
    }
}
