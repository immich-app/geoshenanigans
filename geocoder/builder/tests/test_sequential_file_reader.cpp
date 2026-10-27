// Unit tests for SequentialFileReader (tools/sequential_file_reader.h).
#include "sequential_file_reader.h"

#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <cstdio>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include "scratch_dir.h"
#include "test_framework.h"

namespace {

// A file of `n` bytes whose byte i is (i * 7 + i / 251) & 0xFF, so shifted
// views don't match by accident.
struct ByteFile {
    ScratchDir dir{"reader-test"};
    std::string path;
    std::vector<char> bytes;
    explicit ByteFile(size_t n) : path(dir.path() + "/data.bin"), bytes(n) {
        for (size_t i = 0; i < n; i++) bytes[i] = static_cast<char>((i * 7 + i / 251) & 0xFF);
        std::FILE* f = std::fopen(path.c_str(), "wb");
        if (n) std::fwrite(bytes.data(), 1, n, f);
        std::fclose(f);
    }
    bool same(const char* p, uint64_t off, size_t n) const { return std::memcmp(p, bytes.data() + off, n) == 0; }
};

bool throws_runtime(const std::function<void()>& fn, const char* message) {
    try {
        fn();
    } catch (const std::runtime_error& e) {
        return std::string(e.what()).find(message) != std::string::npos;
    }
    return false;
}

}  // namespace

TEST(sequential_reader_walks_records_that_straddle_the_window) {
    ByteFile file(1000);
    SequentialFileReader sut(file.path, 64);
    bool all_same = true;
    for (uint64_t off = 0; off + 12 <= 1000; off += 12) all_same &= file.same(sut.at(off, 12), off, 12);
    CHECK(all_same);
    CHECK_EQ(sut.rewinds(), uint64_t(0));
}

TEST(sequential_reader_skips_forward_inside_and_past_the_window) {
    ByteFile file(1000);
    SequentialFileReader sut(file.path, 64);
    CHECK(file.same(sut.at(0, 10), 0, 10));
    CHECK(file.same(sut.at(40, 10), 40, 10));
    CHECK(file.same(sut.at(500, 30), 500, 30));
    CHECK(file.same(sut.at(990, 10), 990, 10));
    CHECK_EQ(sut.rewinds(), uint64_t(0));
}

TEST(sequential_reader_counts_a_backward_view_as_a_rewind) {
    ByteFile file(1000);
    SequentialFileReader sut(file.path, 64);
    sut.at(500, 10);
    CHECK(file.same(sut.at(100, 10), 100, 10));
    CHECK_EQ(sut.rewinds(), uint64_t(1));
}

TEST(sequential_reader_grows_the_window_for_a_larger_view) {
    ByteFile file(1000);
    SequentialFileReader sut(file.path, 64);
    sut.at(0, 40);
    CHECK(file.same(sut.at(20, 300), 20, 300));
    CHECK(file.same(sut.at(320, 12), 320, 12));
    CHECK_EQ(sut.rewinds(), uint64_t(0));
}

TEST(sequential_reader_reads_to_the_end_and_throws_one_byte_past_it) {
    ByteFile file(1000);
    SequentialFileReader sut(file.path, 64);
    CHECK(file.same(sut.at(988, 12), 988, 12));
    CHECK(throws_runtime([&] { sut.at(989, 12); }, "Read past end"));
    CHECK(throws_runtime([&] { sut.read(1000, nullptr, 1); }, "Read past end"));
}

TEST(sequential_reader_zero_byte_view_at_the_end_needs_no_read) {
    ByteFile file(100);
    SequentialFileReader sut(file.path, 64);
    CHECK(sut.at(100, 0) != nullptr);
}

TEST(sequential_reader_missing_file_is_not_open_and_empty) {
    ScratchDir dir("reader-test");
    SequentialFileReader sut(dir.path() + "/missing.bin");
    CHECK(!sut.is_open());
    CHECK_EQ(sut.size(), uint64_t(0));
    bool called = false;
    sut.stream(0, 0, [&](const char*, size_t) { called = true; });
    CHECK(!called);
}

TEST(sequential_reader_empty_file_is_open_and_empty) {
    ByteFile file(0);
    SequentialFileReader sut(file.path);
    CHECK(sut.is_open());
    CHECK_EQ(sut.size(), uint64_t(0));
    bool called = false;
    sut.stream(0, 0, [&](const char*, size_t) { called = true; });
    CHECK(!called);
}

TEST(sequential_reader_read_copies_without_moving_the_window) {
    ByteFile file(1000);
    SequentialFileReader sut(file.path, 64);
    const char* view = sut.at(100, 10);
    std::vector<char> small(20), large(300);
    sut.read(5, small.data(), small.size());
    sut.read(600, large.data(), large.size());
    CHECK(file.same(small.data(), 5, small.size()));
    CHECK(file.same(large.data(), 600, large.size()));
    CHECK(sut.at(100, 10) == view);
    CHECK(file.same(view, 100, 10));
    CHECK_EQ(sut.rewinds(), uint64_t(0));
}

TEST(sequential_reader_stream_pieces_fit_the_window_and_join_to_the_range) {
    ByteFile file(1000);
    SequentialFileReader sut(file.path, 64);
    std::vector<char> got;
    bool fits = true;
    sut.stream(7, 900, [&](const char* p, size_t n) {
        fits &= n <= 64;
        got.insert(got.end(), p, p + n);
    });
    CHECK(fits);
    REQUIRE(got.size() == 900);
    CHECK(file.same(got.data(), 7, 900));
}

TEST(sequential_reader_copy_range_appends_at_the_output_position) {
    ByteFile file(1000);
    SequentialFileReader sut(file.path, 64);
    std::string out_path = file.dir.path() + "/out.bin";
    int out = ::open(out_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    REQUIRE(out >= 0);
    REQUIRE(::write(out, "x", 1) == 1);
    uint64_t done = sut.copy_range(100, 700, out);
    REQUIRE(::write(out, "y", 1) == 1);
    ::close(out);
    // Where copy_file_range isn't supported nothing is copied; the caller
    // streams the bytes itself.
    REQUIRE(done == 700 || done == 0);
    SequentialFileReader got(out_path);
    if (done == 700) {
        REQUIRE(got.size() == 702);
        CHECK(*got.at(0, 1) == 'x');
        CHECK(file.same(got.at(1, 700), 100, 700));
        CHECK(*got.at(701, 1) == 'y');
    }
    int scratch = ::open(out_path.c_str(), O_WRONLY);
    REQUIRE(scratch >= 0);
    CHECK(throws_runtime([&] { sut.copy_range(990, 11, scratch); }, "Read past end"));
    ::close(scratch);
}

TEST(sequential_reader_copy_range_to_a_pipe_copies_nothing) {
    ByteFile file(1000);
    SequentialFileReader sut(file.path);
    int fds[2];
    REQUIRE(::pipe(fds) == 0);
    CHECK_EQ(sut.copy_range(0, 100, fds[1]), uint64_t(0));
    CHECK(!sut.copy_supported());
    CHECK_EQ(sut.copy_range(0, 100, fds[1]), uint64_t(0));
    ::close(fds[0]);
    ::close(fds[1]);
}

TEST(copy_in_kernel_stops_unsupported_on_errors_that_moved_nothing) {
    struct Case { const char* name; std::vector<ssize_t> script; uint64_t copied; };
    // Each script entry is one call: bytes copied, or -errno.
    const std::vector<Case> cases = {
        {"EXDEV", {-EXDEV}, 0},
        {"EINVAL", {-EINVAL}, 0},
        {"ENOSYS", {-ENOSYS}, 0},
        {"EOPNOTSUPP", {-EOPNOTSUPP}, 0},
        {"EPERM from seccomp", {-EPERM}, 0},
        {"EIO from FUSE or NFS", {-EIO}, 0},
        {"EBADF for an appending output", {-EBADF}, 0},
        {"0 bytes before any copy", {0}, 0},
        {"EPERM after a partial copy", {3, -EPERM}, 3},
        {"0 bytes after a partial copy", {3, 0}, 3},
    };
    for (const auto& c : cases) {
        size_t step = 0;
        auto copy_at = [&](uint64_t, size_t) -> ssize_t {
            ssize_t r = c.script[step++];
            if (r < 0) { errno = static_cast<int>(-r); return -1; }
            return r;
        };
        const KernelCopy got = copy_in_kernel(0, 8, copy_at, "fake");
        CHECK(got.unsupported);
        CHECK_EQ(got.copied, c.copied);
        CHECK_EQ(step, c.script.size());
    }
}

TEST(copy_in_kernel_retries_short_copies_and_interrupts) {
    std::vector<uint64_t> offsets;
    std::vector<ssize_t> script = {3, -EINTR, 5};
    size_t step = 0;
    auto copy_at = [&](uint64_t off, size_t k) -> ssize_t {
        offsets.push_back(off);
        ssize_t r = script[step++];
        if (r < 0) { errno = static_cast<int>(-r); return -1; }
        return std::min(r, static_cast<ssize_t>(k));
    };
    const KernelCopy got = copy_in_kernel(100, 8, copy_at, "fake");
    CHECK(!got.unsupported);
    CHECK_EQ(got.copied, uint64_t(8));
    CHECK(offsets == std::vector<uint64_t>({100, 103, 103}));
}

TEST(copy_in_kernel_throws_on_other_errors) {
    auto no_space = [](uint64_t, size_t) -> ssize_t { errno = ENOSPC; return -1; };
    CHECK(throws_runtime([&] { copy_in_kernel(0, 8, no_space, "fake"); }, "Cannot copy fake"));
}

namespace {

// Runs append_old_range over `runs` of `file` into a stdio stream opened
// with `mode`, with a buffered byte before, between and after the runs.
// Returns what landed in the output file.
std::vector<char> append_runs(const ByteFile& file, const char* mode,
                              const std::vector<std::pair<uint64_t, uint64_t>>& runs, bool* copy_supported) {
    SequentialFileReader old(file.path, 4096);
    const std::string out_path = file.dir.path() + "/appended.bin";
    std::remove(out_path.c_str());
    FILE* out = std::fopen(out_path.c_str(), mode);
    if (!out) return {};
    std::fputc('<', out);
    for (const auto& run : runs) {
        append_old_range(old, out, run.first, run.second, "appended.bin");
        std::fputc('|', out);
    }
    std::fclose(out);
    *copy_supported = old.copy_supported();
    SequentialFileReader got(out_path);
    std::vector<char> bytes(got.size());
    got.read(0, bytes.data(), bytes.size());
    return bytes;
}

}  // namespace

TEST(append_old_range_output_matches_with_and_without_kernel_copy) {
    // 1 MiB: a short run fills the window, a long run starts inside it, and
    // one ends at the end of the file.
    ByteFile file(1 << 20);
    const std::vector<std::pair<uint64_t, uint64_t>> runs = {
        {10, 100}, {200, 400000}, {500000, 300000}, {(1 << 20) - 300000, 300000}};
    std::vector<char> expected = {'<'};
    for (const auto& run : runs) {
        expected.insert(expected.end(), file.bytes.begin() + run.first, file.bytes.begin() + run.first + run.second);
        expected.push_back('|');
    }
    // "ab" opens the output with O_APPEND, which copy_file_range refuses
    // (EBADF): every byte then goes through the window.
    for (const char* mode : {"wb", "ab"}) {
        bool copy_supported = true;
        CHECK(append_runs(file, mode, runs, &copy_supported) == expected);
        if (std::string(mode) == "ab") CHECK(!copy_supported);    }
}

TEST(append_old_range_throws_when_the_flush_before_a_copy_fails) {
    ByteFile file(1 << 20);
    SequentialFileReader old(file.path);
    FILE* out = std::fopen("/dev/full", "wb");
    REQUIRE(out != nullptr);
    std::fputc('<', out);
    CHECK(throws_runtime([&] { append_old_range(old, out, 0, COPY_RANGE_MIN_BYTES, "full.bin"); },
                         "Cannot write full.bin"));
    std::fclose(out);
}

TEST(read_fully_retries_short_reads_and_interrupts) {
    const std::string src = "abcdefgh";
    struct Case { const char* name; std::vector<ssize_t> script; };
    const std::vector<Case> cases = {
        {"one byte per call", {1, 1, 1, 1, 1, 1, 1, 1}},
        {"EINTR once, then the rest", {-EINTR, 8}},
        {"short then the rest", {3, 5}},
    };
    for (const auto& c : cases) {
        size_t step = 0;
        auto read_at = [&](char* dst, size_t n, uint64_t off) -> ssize_t {
            ssize_t r = c.script[step++];
            if (r < 0) { errno = static_cast<int>(-r); return -1; }
            size_t k = std::min(static_cast<size_t>(r), n);
            std::memcpy(dst, src.data() + off, k);
            return static_cast<ssize_t>(k);
        };
        std::string got(8, '\0');
        read_fully(got.data(), 8, 0, read_at, "fake");
        CHECK(got == src);
        CHECK_EQ(step, c.script.size());
    }
}

TEST(read_fully_throws_on_an_early_end_or_an_error) {
    char dst[8];
    auto early_end = [](char*, size_t, uint64_t) -> ssize_t { return 0; };
    auto io_error = [](char*, size_t, uint64_t) -> ssize_t { errno = EIO; return -1; };
    CHECK(throws_runtime([&] { read_fully(dst, 8, 0, early_end, "fake"); }, "Unexpected end of fake"));
    CHECK(throws_runtime([&] { read_fully(dst, 8, 0, io_error, "fake"); }, "Cannot read fake"));
}

TEST(sequential_reader_grows_its_default_window_for_the_largest_cell_list) {
    const size_t list = 2 + 0xFFFF * 4;
    ByteFile file(3 * list);
    SequentialFileReader sut(file.path);
    CHECK(SEQUENTIAL_READ_BUFFER_BYTES < list);
    CHECK(file.same(sut.at(10, 100), 10, 100));
    CHECK(file.same(sut.at(110, list), 110, list));
    CHECK(file.same(sut.at(110 + list, list), 110 + list, list));
    CHECK_EQ(sut.rewinds(), uint64_t(0));
}

// --- stream sources (the low-memory mode's in-process decoders) ---

namespace {

// Serves bytes [0, n) of a ByteFile's pattern in reads of at most `piece`
// bytes, as a decoder does. A skippable one drops skipped bytes unread, as a
// shared decoder does.
struct PatternSource : StreamSource {
    const std::vector<char>& bytes;
    size_t at = 0, piece, read_bytes = 0, skipped = 0;
    bool skippable;
    PatternSource(const std::vector<char>& b, size_t p, bool s) : bytes(b), piece(p), skippable(s) {}
    size_t read(char* dst, size_t max) override {
        const size_t k = std::min({max, piece, bytes.size() - at});
        std::memcpy(dst, bytes.data() + at, k);
        at += k;
        read_bytes += k;
        return k;
    }
    bool skip(uint64_t n) override {
        if (!skippable) return false;
        at += n;
        skipped += n;
        return true;
    }
};

// One virtual file for the test's scope: its path gets a PatternSource and,
// when `registered`, the size require_old_file_size would have registered.
// `source` is the one the last reader opened.
struct VirtualFile {
    std::string path = "/virtual/old.bin";
    static const std::vector<char>* bytes;
    static size_t piece;
    static bool skippable;
    static PatternSource* source;
    VirtualFile(const std::vector<char>& b, size_t p, bool registered = true, bool can_skip = false) {
        bytes = &b;
        piece = p;
        skippable = can_skip;
        stream_source_factory() = [](const std::string& path) -> std::unique_ptr<StreamSource> {
            if (path != "/virtual/old.bin") return nullptr;
            source = new PatternSource(*bytes, piece, skippable);
            return std::unique_ptr<StreamSource>(source);
        };
        if (registered) virtual_file_sizes()[path] = b.size();
    }
    ~VirtualFile() {
        stream_source_factory() = nullptr;
        virtual_file_sizes().erase(path);
    }
};
const std::vector<char>* VirtualFile::bytes = nullptr;
size_t VirtualFile::piece = 0;
bool VirtualFile::skippable = false;
PatternSource* VirtualFile::source = nullptr;

}  // namespace

TEST(sequential_reader_reads_a_stream_source_forward_in_any_read_sizes) {
    ByteFile file(1000);
    for (size_t piece : {size_t(1), size_t(7), size_t(64), size_t(5000)}) {
        VirtualFile v(file.bytes, piece);
        SequentialFileReader sut(v.path, 64);
        CHECK(sut.is_open());
        CHECK_EQ(sut.size(), uint64_t(1000));
        CHECK(!sut.copy_supported());
        CHECK(file.same(sut.at(0, 10), 0, 10));
        CHECK(file.same(sut.at(5, 40), 5, 40));      // overlaps the window
        CHECK(file.same(sut.at(300, 100), 300, 100));  // skips forward, grows the window
        std::vector<char> copy(200);
        sut.read(500, copy.data(), copy.size());
        CHECK(file.same(copy.data(), 500, copy.size()));
        CHECK(file.same(sut.at(990, 10), 990, 10));
    }
}

TEST(sequential_reader_stream_source_reads_only_forward) {
    ByteFile file(1000);
    VirtualFile v(file.bytes, 64);
    SequentialFileReader sut(v.path, 64);
    sut.at(500, 10);
    CHECK(throws_runtime([&] { sut.at(100, 10); }, "Backward read of stream /virtual/old.bin"));
}

TEST(sequential_reader_stream_source_throws_when_it_ends_early) {
    ByteFile file(100);
    VirtualFile v(file.bytes, 64, false);
    virtual_file_sizes()[v.path] = 200;  // the patch expects more than the decoder has
    SequentialFileReader sut(v.path, 64);
    CHECK(file.same(sut.at(0, 100), 0, 100));
    CHECK(throws_runtime([&] { sut.at(100, 50); }, "Unexpected end of /virtual/old.bin"));
}

TEST(sequential_reader_stream_source_needs_a_registered_size) {
    ByteFile file(100);
    VirtualFile v(file.bytes, 64, false);
    CHECK(throws_runtime([&] { SequentialFileReader sut(v.path); }, "No size registered for stream /virtual/old.bin"));
}

TEST(sequential_reader_appends_a_stream_source_range_through_its_window) {
    ByteFile file(1 << 20);
    VirtualFile v(file.bytes, 4096);
    SequentialFileReader old(v.path);
    ScratchDir dir("reader-test");
    const std::string out_path = dir.path() + "/out.bin";
    FILE* out = std::fopen(out_path.c_str(), "wb");
    REQUIRE(out != nullptr);
    append_old_range(old, out, 100, COPY_RANGE_MIN_BYTES * 3, "out.bin");
    std::fclose(out);
    std::FILE* f = std::fopen(out_path.c_str(), "rb");
    std::vector<char> got(COPY_RANGE_MIN_BYTES * 3);
    REQUIRE(std::fread(got.data(), 1, got.size(), f) == got.size());
    std::fclose(f);
    CHECK(file.same(got.data(), 100, got.size()));
}

TEST(sequential_reader_skip_to_drops_a_stream_source_s_bytes_unread) {
    ByteFile file(1000);
    VirtualFile v(file.bytes, 16, true, true);
    SequentialFileReader sut(v.path, 64);
    CHECK(file.same(sut.at(0, 10), 0, 10));
    const size_t consumed = VirtualFile::source->read_bytes;
    sut.skip_to(700);
    CHECK_EQ(VirtualFile::source->skipped, 700 - consumed);
    CHECK(file.same(sut.at(700, 50), 700, 50));
    CHECK(VirtualFile::source->read_bytes - consumed <= 64);  // nothing between 64 and 700 was read
    sut.skip_to(600);  // behind the stream: nothing to drop
    CHECK_EQ(VirtualFile::source->skipped, 700 - consumed);
    CHECK(throws_runtime([&] { sut.skip_to(1001); }, "Read past end of /virtual/old.bin"));
}

TEST(sequential_reader_skip_to_reads_past_bytes_a_source_cannot_drop) {
    ByteFile file(1000);
    VirtualFile v(file.bytes, 16);
    SequentialFileReader sut(v.path, 64);
    sut.at(0, 10);
    sut.skip_to(700);
    CHECK_EQ(VirtualFile::source->skipped, size_t(0));
    CHECK(file.same(sut.at(700, 50), 700, 50));
    CHECK(VirtualFile::source->read_bytes >= 750);
}

TEST(sequential_reader_skip_to_leaves_a_real_file_readable_anywhere) {
    ByteFile file(1000);
    SequentialFileReader sut(file.path, 64);
    CHECK(file.same(sut.at(500, 10), 500, 10));
    sut.skip_to(900);
    CHECK(file.same(sut.at(100, 10), 100, 10));
    CHECK(file.same(sut.at(950, 50), 950, 50));
}
