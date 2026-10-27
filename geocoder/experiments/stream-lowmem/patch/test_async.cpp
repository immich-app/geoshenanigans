#include <future>
#include "gcs_async.h"
using namespace gcs;
struct Sum : Encoder {
    uint64_t seen = 0;
    Sum() : Encoder("sum", "/nonexistent", 2) {}
    void pump() override { for (auto& q : in) { seen += q.size(); q.pop(q.size()); } }
    void finish(Manifest&) override {}
};
int main() {
    char buf[300000] = {};
    {   // every file closed: the worker drains it all
        Sum e;
        AsyncEnc a(&e);
        a.write(0, buf, sizeof buf);
        a.write(1, buf, 1234);
        if (a.close(0)) { fprintf(stderr, "FAIL early all-closed\n"); return 1; }
        if (!a.close(1)) { fprintf(stderr, "FAIL not all-closed\n"); return 1; }
        if (e.seen != sizeof buf + 1234) { fprintf(stderr, "FAIL drained %llu\n", (unsigned long long)e.seen); return 1; }
    }
    {   // a replay error with a file still open: destroying it must not wait for the close
        Sum e;
        auto f = std::async(std::launch::async, [&] {
            AsyncEnc a(&e);
            a.write(0, buf, 100);
        });
        if (f.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
            fprintf(stderr, "FAIL ~AsyncEnc hangs with an open file\n");
            _exit(1);
        }
        try {  // (ready is not done: a throw in the task is only seen here)
            f.get();
        } catch (const std::exception& x) {
            fprintf(stderr, "FAIL the open-file case threw: %s\n", x.what());
            return 1;
        }
    }
    fprintf(stderr, "PASS\n");
    return 0;
}
