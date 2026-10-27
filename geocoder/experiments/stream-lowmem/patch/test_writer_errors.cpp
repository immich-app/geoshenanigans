// test_writer_errors: a failed last write of an encoder's output reaches the caller as an error
// (the patcher's cookie_close / main), instead of terminating the process from a destructor. Each
// case runs in a child with a 16 KiB file size limit standing in for a full disk.
#include "gcs_codec.h"
#include <filesystem>
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>

using namespace gcs;

static std::vector<uint32_t> values() {
    std::vector<uint32_t> v(15000);  // 45000 bytes of u24: one buffer (48 KiB), written at finish
    for (size_t j = 0; j < v.size(); j++) v[j] = uint32_t(j * 7919 % 0xFFFFFF);
    return v;
}

// 0: the error reached the catch; 3: an error without the file's path; 4: no error at all.
static int run_case(int mode, const std::string& dir) {
    try {
        if (mode == 0 || mode == 3) {  // U24Enc: its last buffer goes out in finish()
            side::U24Enc enc(dir);
            const std::vector<uint32_t> v = values();
            enc.state[0] = 1;
            enc.write(0, v.data(), v.size() * 4);
            enc.state[0] = 2;
            Manifest m;
            enc.finish(m);
            if (mode == 3) {  // no limit: every byte is in the file
                Reader r(dir + "/way_parents.u24");
                if (r.size() != 3 * v.size()) return 5;
                for (uint32_t x : v) {
                    const uint8_t* b = r.take(3);
                    if ((uint32_t(b[0]) | uint32_t(b[1]) << 8 | uint32_t(b[2]) << 16) != x) return 6;
                }
                return 0;
            }
        } else {  // a writer whose finish() fails (1), or whose put() fails writing straight through (2)
            Writer w(dir + "/output", 48 << 10);
            std::vector<uint8_t> bytes(mode == 1 ? 40000 : 100000);
            w.put(bytes.data(), bytes.size());
            w.finish();
        }
    } catch (const std::runtime_error& e) {
        return std::string(e.what()).find(dir) != std::string::npos ? 0 : 3;
    }
    return 4;
}

int main() {
    const char* tmp = getenv("TMPDIR");
    std::string dir = std::string(tmp ? tmp : "/tmp") + "/writer-errors-XXXXXX";
    if (!mkdtemp(dir.data())) return 1;
    bool ok = true;
    for (int mode = 0; mode < 4; mode++) {
        const pid_t child = fork();
        if (child < 0) return 1;
        if (!child) {
            if (mode != 3) {
                signal(SIGXFSZ, SIG_IGN);
                struct rlimit limit{16384, 16384};
                if (setrlimit(RLIMIT_FSIZE, &limit)) _exit(7);
            }
            _exit(run_case(mode, dir));
        }
        int status = 0;
        waitpid(child, &status, 0);
        if (!WIFEXITED(status) || WEXITSTATUS(status)) {
            fprintf(stderr, "FAIL mode=%d status=%d\n", mode, status);
            ok = false;
        }
    }
    std::filesystem::remove_all(dir);
    fprintf(stderr, ok ? "PASS\n" : "FAIL\n");
    return ok ? 0 : 1;
}
