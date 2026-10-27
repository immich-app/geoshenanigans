// test_gxc_counts: malformed list and address counts in a gxc row fail through die() in gc's gxc
// decoder, before any list is sized or filled from them. Each case is a one-row encoded set decoded
// in a child process. (The patcher's decoder: geocoder/builder/tests/test_gcs_codec.cpp.)
#include "fam_gxc.h"
#include <filesystem>
#include <fstream>
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>

static void write_bytes(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream out(path, std::ios::binary);
    out.write(reinterpret_cast<const char*>(bytes.data()), std::streamsize(bytes.size()));
}
static void uv(std::vector<uint8_t>& o, uint64_t v) {
    while (v >= 0x80) { o.push_back(uint8_t(v) | 0x80); v >>= 7; }
    o.push_back(uint8_t(v));
}
static const uint64_t MAX = ~uint64_t(0);  // LEB128 FF x9 01: count - 1 + 1 wraps to 0

struct Case {
    const char* name;
    int s, a;                      // street list state, address state of the row
    std::vector<uint64_t> street;  // gxc_street.bin varints
    std::vector<uint64_t> addr;    // address payload varints
    uint64_t addr_ids;             // next address id after the row
    const char* error;             // nullptr: decodes
};

int main() {
    const char* tmp = getenv("TMPDIR");
    std::string root = std::string(tmp ? tmp : "/tmp") + "/gxc-counts-XXXXXX";
    if (!mkdtemp(root.data())) return 1;
    const Case cases[] = {
        {"valid row", 1, 1, {1, 4, 0}, {2}, 2, nullptr},
        {"street count - 1 = 2^64 - 1", 1, 0, {MAX, 0}, {}, 0, "list too long"},
        {"street count 65536", 3, 0, {0xFFFF, 0}, {}, 0, "list too long"},
        {"address state 1, 2^40 ids", 0, 1, {}, {uint64_t(1) << 40}, uint64_t(1) << 40, "bad address list"},
        {"address state 1, no ids", 0, 1, {}, {0}, 0, "bad address list"},
        {"address state 2, index gap 2^40", 0, 2, {}, {3, 1, uint64_t(1) << 40, 0}, 3, "bad address list"},
        {"address state 2, 2^40 copies", 0, 2, {}, {1, 1, 0, uint64_t(1) << 40}, 1, "bad address list"},
        {"address state 2, more repeats than ids", 0, 2, {}, {1, 2, 0, 0, 0, 0}, 1, "bad address list"},
        {"address state 3, count - 1 = 2^64 - 1", 0, 3, {}, {1, MAX, 0}, 1, "bad address list"},
    };
    bool ok = true;
    int n = 0;
    for (const Case& c : cases) {
        const std::string src = root + "/" + std::to_string(n++), out = src + "/out";
        std::filesystem::create_directories(out);
        uint8_t top[32] = {};
        wr64(top, uint64_t(1) << 40);
        wr32(top + 12, 0xFFFFFFFFu);  // no street / interp list in effect at the block start
        wr32(top + 16, 0xFFFFFFFFu);
        write_bytes(src + "/gxc_top.bin", std::vector<uint8_t>(top, top + 32));
        std::vector<uint8_t> rows{uint8_t(c.s | c.a << 2)}, street;
        for (uint64_t v : c.addr) uv(rows, v);
        for (uint64_t v : c.street) uv(street, v);
        write_bytes(src + "/gxc_rows.bin", rows);
        write_bytes(src + "/gxc_street.bin", street);
        write_bytes(src + "/gxc_interp.bin", {});
        const pid_t child = fork();
        if (child < 0) return 1;
        if (!child) {
            alarm(20);
#ifndef __SANITIZE_ADDRESS__
            struct rlimit as{size_t(4) << 30, size_t(4) << 30};  // an unchecked count runs out here
            setrlimit(RLIMIT_AS, &as);
#endif
            Manifest m;
            m.set("gxc.rows", 1);
            m.set("gxc.B", 128);
            m.set("gxc.addr_ids", c.addr_ids);
            gxc::decode(src, out, m);  // gc's die() exits 2
            _exit(0);
        }
        int status = 0;
        waitpid(child, &status, 0);
        const int want = c.error ? 2 : 0;
        if (!WIFEXITED(status) || WEXITSTATUS(status) != want) {
            fprintf(stderr, "FAIL %s: status %d, want exit %d\n", c.name, status, want);
            ok = false;
        }
    }
    std::filesystem::remove_all(root);
    fprintf(stderr, ok ? "PASS\n" : "FAIL\n");
    return ok ? 0 : 1;
}
