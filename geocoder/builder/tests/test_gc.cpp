// geocoder-gc, the encoder and decoder of installed sets (tools/gc.cpp and
// tools/gc_io.h), which encodes no family: its readers and writers fail
// through gc's die() (exit 2), each case in a child process, and the tool
// refuses an encoded dir as encode input. (The patcher's decoders:
// tests/test_gcs_codec.cpp.)
#include "gc_io.h"

#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <grp.h>
#include <new>
#include <string>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "child_process.h"
#include "scratch_dir.h"
#include "test_framework.h"
#include "tool_runs.h"

using gctest::exit_status_in_child;

namespace {

void write_bytes(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), std::streamsize(bytes.size()));
}

// n little-endian bytes of v.
std::vector<uint8_t> le(uint64_t v, int n) {
    std::vector<uint8_t> b;
    for (int k = 0; k < n; k++) b.push_back(uint8_t(v >> (8 * k)));
    return b;
}

// A gc run: exit status 0, and its log.
struct GcRun {
    bool ok;
    std::string log;
};

GcRun run_gc(const std::vector<std::string>& args, const std::string& log) {
    std::vector<std::string> words{GEOCODER_GC_BIN};
    words.insert(words.end(), args.begin(), args.end());
    std::filesystem::remove(log);
    GcRun r;
    r.ok = gctest::run(words, log);
    auto text = read_file(log);
    r.log.assign(text.begin(), text.end());
    return r;
}

}  // namespace

// A child whose body throws (a bad_alloc from an unchecked size, say) ends
// there with its own status: the cases here run gc's readers in children.
TEST(exit_status_in_child_ends_a_child_whose_body_throws) {
    CHECK_EQ(exit_status_in_child([]() -> int { throw std::bad_alloc(); }, 5), gctest::EXCEPTION_ESCAPED);
    CHECK_EQ(exit_status_in_child([] { return 7; }, 5), 7);
}

// When a FIFO's pipe can't be enlarged, the grouped FIFO writers fail at once
// instead of deadlocking with their reader. A writer child drops to nobody
// (when run as root), holds pipes until it is past its pipe page limit
// (fs.pipe-user-pages-soft), then writes rows of four streams (a 20-byte
// row, a 100-id list, a 1-id list), each row flushed whole, through four
// FIFOs to a reader of them side by side. gc's die (exit 2) on the pipe size
// passes; a writer still going after 20 s (the deadlock) fails.
TEST(gc_fifo_writers_fail_when_the_pipe_cannot_grow) {
    static const char* const NAMES[4] = {"geo", "street", "addr", "interp"};
    // A 64 KiB window per FIFO, one read() until n bytes are in.
    struct Window {
        int fd = -1;
        std::vector<char> b = std::vector<char>(64 << 10);
        size_t off = 0, len = 0;
        const char* need(size_t n) {
            if (len - off < n) {
                memmove(&b[0], &b[off], len - off);
                len -= off;
                off = 0;
                while (len < n) {
                    const ssize_t r = read(fd, &b[len], b.size() - len);
                    if (r <= 0) _exit(0);  // the writer went away
                    len += size_t(r);
                }
            }
            off += n;
            return &b[off - n];
        }
    };
    ScratchDir scratch("gc-fifo-capacity");
    const std::string& dir = scratch.path();
    chmod(dir.c_str(), 0777);
    for (const char* n : NAMES) {
        REQUIRE(mkfifo((dir + "/" + n).c_str(), 0666) == 0);
        chmod((dir + "/" + n).c_str(), 0666);
    }
    const std::string log = dir + "/writer.log";
    const int log_fd = open(log.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    REQUIRE(log_fd >= 0);
    std::fflush(nullptr);
    const pid_t writer = fork();
    REQUIRE(writer >= 0);
    if (!writer) {
        setpgid(0, 0);  // with its reader: the parent ends both
        dup2(log_fd, 2);
        if (geteuid() == 0 && (setgroups(0, nullptr) || setgid(65534) || setuid(65534))) _exit(5);
        // (The uid change made it undumpable: LeakSanitizer's exit check
        // traces the process, and fails where it can't.)
        prctl(PR_SET_DUMPABLE, 1);
        // Pipes of up to 1 MiB until the user is past its pipe page limit,
        // where a new pipe gets the minimum size and none can grow (stopping
        // at the first refused 1 MiB would leave up to 1 MiB minus a page
        // free, enough for a FIFO's enlargement).
        for (int k = 0;; k++) {
            if (k == 4096) {
                std::fprintf(stderr, "no pipe page limit reached, no refusal to test\n");
                _exit(77);
            }
            int p[2];
            if (pipe(p)) {
                std::fprintf(stderr, "the descriptor limit comes before the pipe page limit\n");
                _exit(77);
            }
            if (fcntl(p[1], F_GETPIPE_SZ) < 65536) break;
            fcntl(p[1], F_SETPIPE_SZ, int(gcset::FIFO_PIPE));
        }
        const pid_t reader = fork();
        if (reader < 0) _exit(6);
        if (!reader) {
            alarm(30);
            Window w[4];
            for (int k = 0; k < 4; k++)
                if ((w[k].fd = open((dir + "/" + NAMES[k]).c_str(), O_RDONLY)) < 0) _exit(7);
            for (;;) {
                w[0].need(20);
                uint16_t n;
                memcpy(&n, w[1].need(2), 2);
                w[1].need(4 * size_t(n));
                memcpy(&n, w[2].need(2), 2);
                w[2].need(4 * size_t(n));
            }
        }
        alarm(20);
        gcset::Writer geo(dir + "/geo"), street(dir + "/street"), addr(dir + "/addr"), interp(dir + "/interp");
        const std::vector<uint32_t> ids(100, 7);
        uint8_t row[20] = {};
        for (int r = 0; r < 20000; r++) {
            street.u16(uint16_t(ids.size()));
            street.put(ids.data(), 4 * ids.size());
            addr.u16(1);
            addr.u32(1);
            geo.put(row, 20);
            for (gcset::Writer* w : {&street, &addr, &geo}) w->flush();
        }
        _exit(0);  // (the pipes were wide after all: no deadlock to look for)
    }
    int status = 0;
    while (waitpid(writer, &status, 0) < 0) {}
    kill(-writer, SIGKILL);
    std::string said(4096, '\0');
    const ssize_t got = pread(log_fd, &said[0], said.size(), 0);
    said.resize(got > 0 ? size_t(got) : 0);
    close(log_fd);
    if (WIFEXITED(status) && WEXITSTATUS(status) == 77) {
        std::printf("    SKIP: %s", said.c_str());
        return;
    }

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 2) std::printf("    writer status %d: %s\n", status, said.c_str());
    CHECK(WIFEXITED(status));
    CHECK_EQ(WEXITSTATUS(status), 2);
    CHECK(said.find("the pipe can't hold") != std::string::npos);
}

// An encoded dir is refused as encode input, whole or one more family with
// --only, before the destination is made: its families have no today's-format
// files, and its gc.manifest would be replaced by one naming none of them.
TEST(gc_refuses_to_encode_an_encoded_dir) {
    ScratchDir dir("gc-encoded-input");
    const std::string& root = dir.path();
    const std::string src = root + "/source", encoded = root + "/encoded", log = root + "/gc.log";
    std::filesystem::create_directories(src);
    write_bytes(src + "/way_parents.bin", le(0x00ABCDEF, 4));
    REQUIRE(run_gc({"encode", src, encoded}, log).ok);
    for (const std::vector<std::string>& extra : {std::vector<std::string>{}, {"--only", "way_parents"}}) {
        const std::string dest = root + "/again" + std::to_string(extra.size());
        std::vector<std::string> args{"encode", encoded, dest};
        args.insert(args.end(), extra.begin(), extra.end());

        auto r = run_gc(args, log);

        CHECK(!r.ok);
        CHECK(r.log.find("is already encoded (it has a gc.manifest)") != std::string::npos);
        CHECK(!std::filesystem::exists(dest));
    }
}

// A varint's tenth byte holds bit 63 alone: one past 1 would lose its other
// bits off the u64 (FF x9 02 read as 2^63 - 1), so it is refused; FF x9 01 is
// 2^64 - 1. Through the reader's buffered path (the varint already in the
// buffer, after a byte read first) and its byte-by-byte one (a fresh reader).
TEST(gc_reader_refuses_a_varint_past_64_bits) {
    ScratchDir dir("gc-varint");
    const std::string path = dir.path() + "/v";
    for (const uint8_t last : {uint8_t(0x01), uint8_t(0x02), uint8_t(0x7F)}) {
        for (const bool buffered : {false, true}) {
            std::vector<uint8_t> bytes;
            if (buffered) bytes.push_back(0);
            bytes.insert(bytes.end(), 9, 0xFF);
            bytes.push_back(last);
            write_bytes(path, bytes);

            const int status = exit_status_in_child(
                [&] {
                    gcset::Reader r(path);
                    if (buffered) r.u8();
                    return r.uv() == ~uint64_t(0) && r.eof() ? 0 : 1;
                },
                5);

            if (status != (last == 1 ? 0 : 2)) std::printf("    tenth byte %#x, %s: exit %d\n", last, buffered ? "buffered" : "byte by byte", status);
            CHECK_EQ(status, last == 1 ? 0 : 2);
        }
    }
}

// A unit past what the reader's buffer holds with its slack is refused before
// the buffer grows: a footprint of 0x3FFFFFFD four-byte vertices after its
// header, 0xFFFFFFFE bytes, in a sparse file of as many. Only a 32-bit build
// has such a unit (its buffer holds 2 GiB, its size_t the unit), where cap +
// 64 wrapped to a 62-byte buffer that the file was then read into.
TEST(gc_reader_refuses_a_unit_past_what_its_buffer_holds) {
    if (sizeof(size_t) > 4) return;
    ScratchDir dir("gc-unit");
    const std::string path = dir.path() + "/addr_vertices.bin";
    write_bytes(path, {});
    std::filesystem::resize_file(path, 0xFFFFFFFE);

    const int status = exit_status_in_child(
        [&] {
            gcset::Reader r(path);
            r.take(r.unit_size(10, 0x3FFFFFFD, 4));
            return 0;
        },
        20);

    CHECK_EQ(status, 2);
}
