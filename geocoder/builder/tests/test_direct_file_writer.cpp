// Unit tests for DirectFileWriter (tools/direct_file_writer.h).
#include "direct_file_writer.h"

#include <cstdio>
#include <string>
#include <vector>

#include "scratch_dir.h"
#include "test_framework.h"

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

// Appends pieces of the given sizes, bytes numbered across them, and
// returns what the file should hold.
std::string write_pieces(DirectFileWriter& sut, const std::vector<size_t>& sizes) {
    std::string all;
    for (size_t n : sizes) {
        std::string piece;
        for (size_t i = 0; i < n; i++) piece += char((all.size() + i) * 7 + (all.size() + i) / 251);
        sut.append(piece);
        all += piece;
    }
    return all;
}

}  // namespace

TEST(direct_writer_writes_the_appended_bytes_with_and_without_o_direct) {
    const std::vector<std::vector<size_t>> cases = {
        {},                                  // an empty file
        {1},                                 // a tail only
        {4096},                              // one aligned tail
        {8192},                              // exactly one block
        {8192, 8192, 1},                     // whole blocks, then a byte
        {3, 4093, 4097, 1, 12000, 0, 9000},  // pieces straddling blocks
        {50000},                             // one piece over several blocks
    };
    for (bool direct : {false, true}) {
        for (const auto& sizes : cases) {
            ScratchDir dir("direct-writer-test");
            std::string path = dir.path() + "/out.bin";
            DirectFileWriter sut(path, direct, 8192);
            std::string expected = write_pieces(sut, sizes);
            CHECK_EQ(sut.finish(), uint64_t(expected.size()));
            CHECK(read_file(path) == expected);
            if (!direct) CHECK(!sut.direct());
        }
    }
}

TEST(direct_writer_rounds_the_block_to_the_alignment) {
    ScratchDir dir("direct-writer-test");
    std::string path = dir.path() + "/out.bin";
    DirectFileWriter sut(path, true, 5000);
    std::string expected = write_pieces(sut, {5000, 5000, 5000});
    CHECK_EQ(sut.finish(), uint64_t(15000));
    CHECK(read_file(path) == expected);
}

TEST(direct_writer_truncates_an_existing_file) {
    ScratchDir dir("direct-writer-test");
    std::string path = dir.path() + "/out.bin";
    std::FILE* f = std::fopen(path.c_str(), "wb");
    std::string old(20000, 'x');
    std::fwrite(old.data(), 1, old.size(), f);
    std::fclose(f);
    DirectFileWriter sut(path, true, 8192);
    std::string expected = write_pieces(sut, {100});
    sut.finish();
    CHECK(read_file(path) == expected);
}
