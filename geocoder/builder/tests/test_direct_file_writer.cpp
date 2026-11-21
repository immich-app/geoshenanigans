// Unit tests for DirectFileWriter (tools/direct_file_writer.h).
#include "direct_file_writer.h"

#include <cstdio>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include "scratch_dir.h"
#include "test_framework.h"

using gctest::refusal_of;

namespace {

std::string read_file(const std::string& path) {
    std::string out;
    std::FILE* f = std::fopen(path.c_str(), "rb");
    if (!f) return "<missing>";
    char buf[4096];
    for (size_t n; (n = std::fread(buf, 1, sizeof buf, f)) > 0;) out.append(buf, n);
    std::fclose(f);
    return out;
}

std::string numbered_bytes(size_t n, size_t seed) {
    std::string s(n, '\0');
    for (size_t i = 0; i < n; i++) s[i] = char((seed + i) * 7 + (seed + i) / 251);
    return s;
}

// A source file to copy spans of, open for reading.
struct SourceFile {
    ScratchDir dir{"direct-writer-test"};
    std::string path = dir.path() + "/in.bin";
    std::string bytes = numbered_bytes(30000, 99);
    int fd = -1;
    SourceFile() {
        std::FILE* f = std::fopen(path.c_str(), "wb");
        std::fwrite(bytes.data(), 1, bytes.size(), f);
        std::fclose(f);
        fd = open(path.c_str(), O_RDONLY);
    }
    ~SourceFile() { close(fd); }
};

// Appends pieces of the given sizes, a negative size -n a span of n bytes
// of `source` (at a running offset), and returns what the file should hold.
std::string write_pieces(DirectFileWriter& sut, const std::vector<long>& sizes, const SourceFile& source) {
    std::string all;
    size_t from = 0;
    for (long size : sizes) {
        if (size >= 0) {
            std::string piece = numbered_bytes(size_t(size), all.size());
            sut.append(piece);
            all += piece;
        } else {
            size_t n = size_t(-size);
            from = (from + 1237) % (source.bytes.size() - n);
            sut.append_from(source.fd, from, n, source.path);
            all += source.bytes.substr(from, n);
        }
    }
    return all;
}

}  // namespace

TEST(direct_writer_writes_the_appended_bytes_and_spans) {
    SourceFile source;
    REQUIRE(source.fd >= 0);
    const std::vector<std::vector<long>> cases = {
        {},                                     // an empty file
        {1},                                    // a tail only
        {4096},                                 // one aligned tail
        {8192},                                 // exactly one block
        {8192, 8192, 1},                        // whole blocks, then a byte
        {3, 4093, 4097, 1, 12000, 0, 9000},     // pieces straddling blocks
        {50000},                                // one piece over several blocks
        {-20000},                               // one span over several blocks
        {5, -9000, 7, -1, 0, -8192, 3000, -5},  // spans and pieces mixed
    };
    for (bool direct : {false, true}) {
        for (size_t in_flight : {size_t(1), size_t(3)}) {
            for (const auto& sizes : cases) {
                ScratchDir dir("direct-writer-test");
                std::string path = dir.path() + "/out.bin";
                DirectFileWriter sut(path, direct, 8192, in_flight);
                std::string expected = write_pieces(sut, sizes, source);
                CHECK_EQ(sut.finish(), uint64_t(expected.size()));
                CHECK(read_file(path) == expected);
                if (!direct) CHECK(!sut.direct());
            }
        }
    }
}

TEST(direct_writer_rounds_the_block_to_the_alignment) {
    SourceFile source;
    ScratchDir dir("direct-writer-test");
    std::string path = dir.path() + "/out.bin";
    DirectFileWriter sut(path, true, 5000);
    std::string expected = write_pieces(sut, {5000, 5000, 5000}, source);
    CHECK_EQ(sut.finish(), uint64_t(15000));
    CHECK(read_file(path) == expected);
}

TEST(direct_writer_truncates_an_existing_file) {
    SourceFile source;
    ScratchDir dir("direct-writer-test");
    std::string path = dir.path() + "/out.bin";
    std::FILE* f = std::fopen(path.c_str(), "wb");
    std::string old(20000, 'x');
    std::fwrite(old.data(), 1, old.size(), f);
    std::fclose(f);
    DirectFileWriter sut(path, true, 8192);
    std::string expected = write_pieces(sut, {100}, source);
    sut.finish();
    CHECK(read_file(path) == expected);
}

TEST(direct_writer_cuts_the_file_to_its_bytes_whatever_grew_it) {
    // A failed fallocate (ENOSPC) can keep part of its range, the file then
    // longer than any allocation the writer saw succeed.
    SourceFile source;
    ScratchDir dir("direct-writer-test");
    std::string path = dir.path() + "/out.bin";
    DirectFileWriter sut(path, true, 8192);
    std::string expected = write_pieces(sut, {100}, source);
    REQUIRE(truncate(path.c_str(), 1 << 20) == 0);
    CHECK_EQ(sut.finish(), uint64_t(100));
    CHECK(read_file(path) == expected);
}

TEST(direct_writer_reports_a_span_past_the_end_of_its_file) {
    SourceFile source;
    for (long before : {0L, 9000L}) {
        ScratchDir dir("direct-writer-test");
        DirectFileWriter sut(dir.path() + "/out.bin", true, 8192, 2);
        CHECK_EQ(refusal_of([&] {
                     if (before) sut.append(std::string(size_t(before), 'a'));
                     sut.append_from(source.fd, 29990, 11, source.path);
                     sut.append(std::string(20000, 'b'));
                     sut.finish();
                 }),
                 "Unexpected end of " + source.path);
    }
}

// What a reader of the output may rely on, and pbf-apply drops its input's
// pages behind: the bytes written() names are filled from their sources and
// on disk, whole blocks from the start; appended() counts what is laid out.
TEST(direct_writer_written_names_whole_blocks_already_on_disk) {
    SourceFile source;
    for (bool direct : {false, true}) {
        ScratchDir dir("direct-writer-test");
        std::string path = dir.path() + "/out.bin";
        DirectFileWriter sut(path, direct, 4096, 2);
        std::string expected;
        uint64_t last = 0;
        for (size_t from = 0; from + 1500 <= source.bytes.size(); from += 1500) {
            sut.append_from(source.fd, from, 1500, source.path);
            expected += source.bytes.substr(from, 1500);
            CHECK_EQ(sut.appended(), uint64_t(expected.size()));
            uint64_t written = sut.written();
            CHECK_EQ(written % 4096, uint64_t(0));
            CHECK(written >= last);
            CHECK(written <= sut.appended());
            CHECK(read_file(path).substr(0, written) == expected.substr(0, written));
            last = written;
        }
        // 30,000 bytes laid out: with at most 2 blocks in flight, at least 5 of
        // the 7 whole blocks are written, and the rest follow without more appends.
        CHECK(last >= 5 * 4096);
        for (int i = 0; i < 2000 && sut.written() < 7 * 4096; i++) usleep(1000);
        CHECK_EQ(sut.written(), uint64_t(7 * 4096));
        CHECK(read_file(path).substr(0, 7 * 4096) == expected.substr(0, 7 * 4096));
        CHECK_EQ(sut.finish(), uint64_t(expected.size()));
    }
}
