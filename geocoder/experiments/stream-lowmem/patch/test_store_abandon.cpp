// test_store_abandon: a run that fails with an encoded output still open (bytes flushed to its
// encoder, more in the stdio buffer) exits with main's status, with and without --threads: the
// Store's cleanup neither waits for the open file's close nor lets exit's stdio flush reach the
// destroyed encoders. Each case is a child that opens the sink and exits 1, as main's catch does.
#include <iostream>
#include <sys/wait.h>
#include <signal.h>
#include "gcs_patch.h"

struct NullEnc : gcs::Encoder {
    uint64_t seen = 0;
    NullEnc() : Encoder("null", "/nonexistent", 2) {}
    void pump() override {
        for (auto& q : in) { seen += q.size(); q.pop(q.size()); }
    }
    void finish(gcs::Manifest&) override {}
};

static void fail_with_open_sink(bool threads) {
    gcs::Store& s = gcs::store();
    const size_t F = gcs::families().size();
    s.active = true;
    s.threads = threads;
    s.fam_on.assign(F, 0);
    s.dec.resize(F); s.enc.resize(F); s.adec.resize(F); s.aenc.resize(F);
    s.fam_on[0] = 1;
    s.file_port["street_ways.bin"] = {0, 0};
    s.file_port["street_nodes.bin"] = {0, 1};
    s.enc[0].reset(new NullEnc);
    if (threads) s.aenc[0].reset(new gcs::AsyncEnc(s.enc[0].get()));
    FILE* f = s.open_sink("street_ways.bin");
    std::vector<char> b(64 << 10, 1);
    fwrite(b.data(), 1, b.size(), f);  // past the sink's buffer: the encoder (and its worker) start
    fwrite(b.data(), 1, 100, f);       // still buffered when the run fails
    std::cerr << "replay error with street_ways.bin open" << std::endl;
    exit(1);  // main's catch returns 1
}

int main() {
    bool ok = true;
    for (int threads = 0; threads < 2; threads++) {
        const pid_t child = fork();
        if (child < 0) return 1;
        if (!child) {
            alarm(10);
            fail_with_open_sink(threads != 0);
        }
        int status = 0;
        waitpid(child, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 1) {
            fprintf(stderr, "FAIL threads=%d: status %d (%s), want exit 1\n", threads, status,
                    WIFSIGNALED(status) ? strsignal(WTERMSIG(status)) : "exited");
            ok = false;
        }
    }
    fprintf(stderr, ok ? "PASS\n" : "FAIL\n");
    return ok ? 0 : 1;
}
