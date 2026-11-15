// Unit tests for the patcher's side of its codecs (tools/gcs_patch.h,
// gcs_async.h): a codec thread drains what it was handed, and a run that fails
// with an encoded output still open ends with main's status, without waiting
// on the open file or flushing into destroyed encoders.
#include "gcs_patch.h"

#include <cstdio>
#include <cstdlib>
#include <future>
#include <iostream>
#include <new>
#include <string>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>

#include "child_process.h"
#include "test_framework.h"

using gctest::exit_status_in_child;

namespace {

// Counts and drops what it is handed.
struct CountingEnc : gcs::Encoder {
    uint64_t seen = 0;
    CountingEnc() : Encoder("count", "/nonexistent", 2) {}
    void pump() override {
        for (auto& q : in) {
            seen += q.size();
            q.pop(q.size());
        }
    }
    void finish() override {}
};

// The status a failed run exits with here: main's catch returns 1, but 1 is
// also how ASan and UBSan end a process after a report, so a use-after-free
// as exit() destroys the open sink would pass for it.
constexpr int FAILED_RUN_STATUS = 3;

// A replay error with street_ways.bin open: bytes past the sink's buffer
// (its encoder, and with threads its worker, started), more still buffered,
// then exit(FAILED_RUN_STATUS) as main's catch returns its status.
[[noreturn]] void fail_with_open_sink(bool threads) {
    gcs::Store& s = gcs::store();
    const size_t F = gcs::families().size();
    s.threads = threads;
    s.dec.resize(F);
    s.enc.resize(F);
    s.adec.resize(F);
    s.aenc.resize(F);
    s.file_port["street_ways.bin"] = {0, 0};
    s.file_port["street_nodes.bin"] = {0, 1};
    s.enc[0].reset(new CountingEnc);
    if (threads) s.aenc[0].reset(new gcs::AsyncEnc(s.enc[0].get()));
    FILE* f = s.open_sink("street_ways.bin");
    std::vector<char> b(64 << 10, 1);
    fwrite(b.data(), 1, b.size(), f);
    fwrite(b.data(), 1, 100, f);
    std::cerr << "replay error with street_ways.bin open" << std::endl;
    exit(FAILED_RUN_STATUS);
}

// Refuses whatever it is handed, and counts the bytes handed to it.
struct RefusingEnc : gcs::Encoder {
    uint64_t handed = 0;
    RefusingEnc() : Encoder("refuse", "/nonexistent", 2) {}
    void pump() override {
        handed = got[0] + got[1];
        gcs::die("refuse: no unit takes these bytes");
    }
    void finish() override {}
};

// street_nodes.bin's sink over a RefusingEnc, and 1 MiB written to it in one
// write from bytes that end where a page without access starts. 0 when the
// write fails with the encoder handed one piece of it, and a second write and
// a flush hand it nothing more; a read past the bytes ends the process.
int write_to_a_refusing_sink(bool threads) {
    gcs::Store& s = gcs::store();
    const size_t F = gcs::families().size();
    s.threads = threads;
    s.dec.resize(F);
    s.enc.resize(F);
    s.adec.resize(F);
    s.aenc.resize(F);
    s.file_port["street_ways.bin"] = {0, 0};
    s.file_port["street_nodes.bin"] = {0, 1};
    RefusingEnc* enc = new RefusingEnc;
    s.enc[0].reset(enc);
    if (threads) s.aenc[0].reset(new gcs::AsyncEnc(enc));
    FILE* f = s.open_sink("street_nodes.bin");
    const size_t n = size_t(1) << 20, page = size_t(sysconf(_SC_PAGESIZE));
    char* bytes = static_cast<char*>(mmap(nullptr, n + page, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (bytes == MAP_FAILED || mprotect(bytes + n, page, PROT_NONE)) return 4;
    const size_t written = fwrite(bytes, 1, n, f);
    if (written == n || !ferror(f)) return 5;
    const uint64_t handed = enc->handed;
    if (handed == 0 || handed > gcs::ENCODER_PIECE) return 6;
    fwrite(bytes, 1, n, f);
    fflush(f);
    if (enc->handed != handed) return 7;
    return s.sink_error.find("refuse: no unit takes these bytes") == std::string::npos ? 8 : 0;
}

}  // namespace

// A child whose body throws (a bad_alloc from an unchecked size, say) ends
// there with its own status: the cases here run in children what may hang or
// end the process.
TEST(exit_status_in_child_ends_a_child_whose_body_throws) {
    CHECK_EQ(exit_status_in_child([]() -> int { throw std::bad_alloc(); }, 5), gctest::EXCEPTION_ESCAPED);
    CHECK_EQ(exit_status_in_child([] { return 7; }, 5), 7);
}

TEST(gcs_async_encoder_drains_every_file_before_the_last_close) {
    static char buf[300000] = {};
    CountingEnc e;
    gcs::AsyncEnc a(&e);
    a.write(0, buf, sizeof buf);
    a.write(1, buf, 1234);
    CHECK(!a.close(0));
    CHECK(a.close(1));
    CHECK_EQ(e.seen, uint64_t(sizeof buf + 1234));
}

TEST(gcs_async_encoder_destroyed_with_a_file_open_does_not_wait_for_its_close) {
    // A replay error with a file still open destroys the AsyncEnc.
    const int status = exit_status_in_child(
        [] {
            static char buf[100] = {};
            CountingEnc e;
            {
                gcs::AsyncEnc a(&e);
                a.write(0, buf, sizeof buf);
            }
            return 0;
        },
        5);
    CHECK_EQ(status, 0);
}

TEST(gcs_store_run_failing_with_an_open_sink_exits_with_its_status) {
    for (bool threads : {false, true}) {
        const int status = exit_status_in_child([threads]() -> int { fail_with_open_sink(threads); }, 10);
        if (status != FAILED_RUN_STATUS) std::printf("    threads=%d: status %d\n", int(threads), status);
        CHECK_EQ(status, FAILED_RUN_STATUS);
    }
}

// An encoder that refuses its input fails the write that brought it, in the
// patcher's thread and with --threads: the stream reads nothing past the
// writer's bytes (a write function that answers a failed write with a
// negative number makes stdio go on from past them), the run sees the error
// on the stream and the refusal in sink_error, and no later write or flush
// hands the encoder another byte.
TEST(gcs_store_sink_fails_the_write_its_encoder_refuses_and_takes_no_more) {
    for (bool threads : {false, true}) {
        const int status = exit_status_in_child([threads] { return write_to_a_refusing_sink(threads); }, 10);
        if (status != 0) std::printf("    threads=%d: status %d\n", int(threads), status);
        CHECK_EQ(status, 0);
    }
}
