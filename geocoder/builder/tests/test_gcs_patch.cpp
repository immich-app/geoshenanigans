// Unit tests for the low-memory mode's encoded-set plumbing (tools/gcs_patch.h,
// gcs_async.h): a codec thread drains what it was handed, and a run that fails
// with an encoded output still open ends with main's status, without waiting
// on the open file or flushing into destroyed encoders.
#include "gcs_patch.h"

#include <cstdio>
#include <cstdlib>
#include <future>
#include <iostream>
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
    void finish(gcs::Manifest&) override {}
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
    s.active = true;
    s.threads = threads;
    s.fam_on.assign(F, 0);
    s.dec.resize(F);
    s.enc.resize(F);
    s.adec.resize(F);
    s.aenc.resize(F);
    s.fam_on[0] = 1;
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

}  // namespace

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
