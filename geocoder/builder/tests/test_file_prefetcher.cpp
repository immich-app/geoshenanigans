// Unit tests for FilePrefetcher (src/file_prefetcher.h).
#include "file_prefetcher.h"

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "scratch_dir.h"
#include "test_framework.h"

namespace {

std::string make_file(const ScratchDir& dir, size_t bytes) {
    std::string path = dir.path() + "/data.bin";
    std::string data(bytes, 'x');
    std::FILE* f = std::fopen(path.c_str(), "wb");
    std::fwrite(data.data(), 1, data.size(), f);
    std::fclose(f);
    return path;
}

// Waits up to 10 s for the prefetcher to have read `want` bytes.
bool reaches(const FilePrefetcher& sut, uint64_t want) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (sut.bytes_read() < want && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    return sut.bytes_read() == want;
}

}  // namespace

TEST(touching_ranges_merges_neighbours_and_keeps_gaps) {
    const std::vector<uint64_t> offsets = {0, 10, 30, 35, 100};
    const std::vector<uint64_t> sizes = {10, 5, 5, 20, 1};
    auto ranges = touching_ranges(offsets, [&](size_t i) { return sizes[i]; });
    REQUIRE(ranges.size() == 3);
    CHECK_EQ(ranges[0].begin, uint64_t(0));
    CHECK_EQ(ranges[0].end, uint64_t(15));
    CHECK_EQ(ranges[1].begin, uint64_t(30));
    CHECK_EQ(ranges[1].end, uint64_t(55));
    CHECK_EQ(ranges[2].begin, uint64_t(100));
    CHECK_EQ(ranges[2].end, uint64_t(101));
}

TEST(file_prefetcher_reads_every_range_once_and_nothing_else) {
    ScratchDir dir("prefetch-test");
    std::string path = make_file(dir, 1 << 20);
    const std::vector<ByteRange> ranges = {{1000, 300000}, {500000, 1000000}};
    FilePrefetcher sut(path, ranges, uint64_t(1) << 30, 4096, 4);
    CHECK(reaches(sut, (300000 - 1000) + (1000000 - 500000)));
}

TEST(file_prefetcher_stays_within_its_window_of_the_consumers) {
    ScratchDir dir("prefetch-test");
    std::string path = make_file(dir, 1 << 20);
    // Chunks of 1 KiB from 0: with no window only the chunk at the consumers'
    // position may be read until they move on.
    FilePrefetcher sut(path, {{0, 64 << 10}}, 0, 1024, 3);
    CHECK(reaches(sut, 1024));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_EQ(sut.bytes_read(), uint64_t(1024));
    sut.consumed(10 * 1024);
    CHECK(reaches(sut, 11 * 1024));
    sut.consumed(64 << 10);
    CHECK(reaches(sut, 64 << 10));
}

TEST(file_prefetcher_stops_while_held_back) {
    ScratchDir dir("prefetch-test");
    std::string path = make_file(dir, 1 << 20);
    auto start = std::chrono::steady_clock::now();
    {
        FilePrefetcher sut(path, {{0, 1 << 20}}, 0, 1024, 8);
        CHECK(reaches(sut, 1024));
    }
    CHECK(std::chrono::steady_clock::now() - start < std::chrono::seconds(5));
}

TEST(file_prefetcher_without_ranges_or_file_does_nothing) {
    ScratchDir dir("prefetch-test");
    FilePrefetcher none(dir.path() + "/data.bin", {}, 1 << 20, 4096, 4);
    CHECK_EQ(none.bytes_read(), uint64_t(0));
    FilePrefetcher missing(dir.path() + "/missing.bin", {{0, 4096}}, 1 << 20, 4096, 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK_EQ(missing.bytes_read(), uint64_t(0));
}
